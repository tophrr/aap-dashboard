import os
import json
import math
import pickle
import argparse
import sqlite3
import threading
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from river import compose, preprocessing, tree

# Load env variables from parent directory's .env file
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir, '..', '.env')
load_dotenv(dotenv_path)

# MQTT Config
mqtt_url_env = os.getenv('MQTT_URL', 'mqtt://localhost:1883')
parsed_url = urlparse(mqtt_url_env)
MQTT_BROKER = parsed_url.hostname or 'localhost'
MQTT_PORT = parsed_url.port or 1883
MQTT_USER = os.getenv('MQTT_USERNAME', None)
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', None)

STATE_FILE = os.path.join(script_dir, 'model_state.pkl')
EMA_ALPHA = 0.1
DEBOUNCE_THRESHOLD = 15.0
VERBOSE = os.getenv('VERBOSE', '0') == '1'

class MLState:
    def __init__(self):
        self.wait_time_model = compose.Pipeline(
            preprocessing.StandardScaler(),
            tree.HoeffdingTreeRegressor(grace_period=20)
        )
        self.duration_model = compose.Pipeline(
            preprocessing.StandardScaler(),
            tree.HoeffdingTreeRegressor(grace_period=20)
        )
        
        self.previous_features = None
        self.previous_ended_at = None
        self.sample_count = 0
        self.ema_wait = None
        self.ema_duration = None
        self.pending_event = None  # Buffer for fragmented events

def extract_features(timestamp, prev_duration, prev_wait_time):
    dt = datetime.fromtimestamp(timestamp)
    hour = dt.hour + dt.minute / 60.0
    dow = dt.weekday()
    
    return {
        'hour_sin': math.sin(2 * math.pi * hour / 24.0),
        'hour_cos': math.cos(2 * math.pi * hour / 24.0),
        'dow_sin': math.sin(2 * math.pi * dow / 7.0),
        'dow_cos': math.cos(2 * math.pi * dow / 7.0),
        'prev_duration': prev_duration if prev_duration is not None else 0.0,
        'prev_wait_time': prev_wait_time if prev_wait_time is not None else 0.0
    }

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'rb') as f:
                loaded = pickle.load(f)
                if not hasattr(loaded, 'pending_event'):
                    loaded.pending_event = None
                return loaded
        except Exception as e:
            print(f"Error loading state, starting fresh: {e}")
    return MLState()

def save_state(state):
    try:
        with open(STATE_FILE, 'wb') as f:
            pickle.dump(state, f)
    except Exception as e:
        print(f"Error saving state: {e}")

state = None
pending_timer = None

def cancel_pending_timer():
    global pending_timer
    if pending_timer is not None:
        pending_timer.cancel()
        pending_timer = None

def commit_pending_event(state_obj, save_to_disk=True):
    if state_obj.pending_event is None:
        return
        
    pe = state_obj.pending_event
    duration = pe["duration"]
    probing_started_at = pe["probing_started_at"]
    ended_at = pe["ended_at"]
    
    current_wait_time = 0.0
    if state_obj.previous_ended_at is not None:
        current_wait_time = probing_started_at - state_obj.previous_ended_at
        
        # Update EMAs
        if state_obj.ema_wait is None:
            state_obj.ema_wait = current_wait_time
            state_obj.ema_duration = duration
        else:
            state_obj.ema_wait = (EMA_ALPHA * current_wait_time) + ((1 - EMA_ALPHA) * state_obj.ema_wait)
            state_obj.ema_duration = (EMA_ALPHA * duration) + ((1 - EMA_ALPHA) * state_obj.ema_duration)
            
        state_obj.sample_count += 1
        
        # Learn
        if state_obj.previous_features is not None:
            if VERBOSE:
                print(f"[DEBUG] Training models. Actual Wait={current_wait_time:.1f}, Actual Duration={duration:.1f}")
            state_obj.wait_time_model.learn_one(state_obj.previous_features, current_wait_time)
            state_obj.duration_model.learn_one(state_obj.previous_features, duration)
            
    # Extract features for the NEXT prediction
    state_obj.previous_features = extract_features(ended_at, duration, current_wait_time)
    state_obj.previous_ended_at = ended_at
    state_obj.pending_event = None
    
    if save_to_disk:
        save_state(state_obj)

def timer_commit(client):
    global state
    if state.pending_event is not None:
        if VERBOSE:
            print(f"[DEBUG] Timer expired. Committing pending event: duration={state.pending_event['duration']:.1f}s")
        commit_pending_event(state, save_to_disk=True)

def schedule_pending_commit(client):
    global pending_timer
    cancel_pending_timer()
    pending_timer = threading.Timer(DEBOUNCE_THRESHOLD + 1.0, timer_commit, args=[client])
    pending_timer.daemon = True
    pending_timer.start()

def process_event(payload, publish_prediction=True, save_to_disk=True, client=None):
    global state
    if payload.get("event") == "ended":
        duration = payload.get("duration", 0.0)
        probing_started_at = payload.get("probing_started_at")
        ended_at = payload.get("ended_at")
        
        if probing_started_at is None or ended_at is None:
            return
        
        if VERBOSE:
            print(f"\n[DEBUG] Event Payload: duration={duration}, wait_start={probing_started_at}, wait_end={ended_at}")
        
        # Check if this is a fragment of the pending event
        is_fragment = False
        if state.pending_event is not None:
            gap = probing_started_at - state.pending_event["ended_at"]
            if gap < DEBOUNCE_THRESHOLD:
                is_fragment = True
                cancel_pending_timer()
                
                # Merge into pending_event
                state.pending_event["ended_at"] = ended_at
                state.pending_event["duration"] = ended_at - state.pending_event["probing_started_at"]
                if VERBOSE:
                    print(f"[DEBUG] Fragmented event detected. Wait={gap:.1f}s < {DEBOUNCE_THRESHOLD}s. Merging. New pending duration={state.pending_event['duration']:.1f}s")
        
        if not is_fragment:
            # If there was an old pending event that is NOT a fragment, commit it now
            if state.pending_event is not None:
                cancel_pending_timer()
                commit_pending_event(state, save_to_disk=save_to_disk)
                
            # Create new pending event
            state.pending_event = {
                "probing_started_at": probing_started_at,
                "ended_at": ended_at,
                "duration": duration
            }

        # Step D: Predict the FUTURE (preliminary prediction, published immediately)
        features_to_predict = state.previous_features
        if features_to_predict is None:
            # First run fallback features
            features_to_predict = extract_features(ended_at, duration, 0.0)
            
        pred_wait = state.wait_time_model.predict_one(features_to_predict)
        pred_dur = state.duration_model.predict_one(features_to_predict)
        
        # Fallback logic
        final_wait = pred_wait
        final_dur = pred_dur
        
        if state.ema_wait is not None and state.ema_duration is not None:
            if VERBOSE:
                print(f"[DEBUG] Raw ML Pred: Wait={pred_wait:.1f}, Dur={pred_dur:.1f} | EMA: Wait={state.ema_wait:.1f}, Dur={state.ema_duration:.1f}")
            if state.sample_count < 30:
                if VERBOSE:
                    print(f"[DEBUG] Fallback: Sample count {state.sample_count} < 30. Using EMA.")
                final_wait = state.ema_wait
                final_dur = state.ema_duration
            else:
                # Check for > 40% deviation from EMA
                if abs(pred_wait - state.ema_wait) / max(state.ema_wait, 1) > 0.4:
                    if VERBOSE:
                        print(f"[DEBUG] Fallback: ML Wait Pred deviated > 40% from EMA. Using EMA.")
                    final_wait = state.ema_wait
                if abs(pred_dur - state.ema_duration) / max(state.ema_duration, 1) > 0.4:
                    if VERBOSE:
                        print(f"[DEBUG] Fallback: ML Dur Pred deviated > 40% from EMA. Using EMA.")
                    final_dur = state.ema_duration
        elif state.sample_count == 0:
             # No EMA yet, fallback to sensible defaults
             final_wait = 30.0
             final_dur = 10.0

        # Round to 1 decimal place
        final_wait = round(final_wait, 1)
        final_dur = round(final_dur, 1)

        # Publish prediction if requested
        if publish_prediction and client is not None:
            prediction_payload = {
                "predicted_next_wait_time": final_wait,
                "predicted_next_duration": final_dur,
                "ml_active": state.sample_count >= 30
            }
            
            print(f"[{datetime.now().isoformat()}] Predicted next wait: {final_wait}s, duration: {final_dur}s")
            client.publish("crossing/predictions", json.dumps(prediction_payload))
            
            # Schedule commit
            schedule_pending_commit(client)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe("crossing/event")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        process_event(payload, publish_prediction=True, save_to_disk=True, client=client)
    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"Error processing message: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ML Predictor Service")
    parser.add_argument('--start-time', type=int, help='Start UNIX timestamp to reset state and catch up from')
    args = parser.add_argument_group().parse_args() if hasattr(parser.add_argument_group(), 'parse_args') else parser.parse_args()

    print("Starting ML Predictor Service...")
    
    if args.start_time is not None:
        print(f"Start time provided: {args.start_time}. Resetting ML state.")
        state = MLState()
    else:
        state = load_state()
        
    # Catch up on historical events from the logger's database
    db_path = os.path.join(script_dir, '..', 'logger', 'mqtt_logs.db')
    if os.path.exists(db_path):
        print("Checking historical logs in database for catchup...")
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT payload FROM logs WHERE topic = 'crossing/event' ORDER BY id ASC")
            rows = cursor.fetchall()
            
            processed_any = False
            for row in rows:
                try:
                    payload = json.loads(row[0])
                    if payload.get("event") == "ended":
                        ended_at = payload.get("ended_at")
                        if ended_at is not None:
                            if args.start_time is not None:
                                if ended_at >= args.start_time:
                                    process_event(payload, publish_prediction=False, save_to_disk=False)
                                    processed_any = True
                            else:
                                if state.previous_ended_at is None or ended_at > state.previous_ended_at:
                                    process_event(payload, publish_prediction=False, save_to_disk=False)
                                    processed_any = True
                except json.JSONDecodeError:
                    pass
            
            # Commit the final pending event if any
            if state.pending_event is not None:
                if VERBOSE:
                    print(f"[DEBUG] Committing final catchup event: duration={state.pending_event['duration']:.1f}s")
                commit_pending_event(state, save_to_disk=False)
                processed_any = True
                
            if processed_any:
                save_state(state)
                print(f"Caught up on historical logs. New sample count: {state.sample_count}")
            else:
                print("No new historical logs to process.")
                
            conn.close()
        except Exception as e:
            print(f"Error reading historical logs from database: {e}")
    else:
        print("Logger database not found, skipping catchup.")
    
    client = mqtt.Client()
    
    if MQTT_USER and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        
    client.on_connect = on_connect
    client.on_message = on_message
    
    print(f"Connecting to {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nExiting ML Predictor...")
        cancel_pending_timer()
        if state is not None and state.pending_event is not None:
            print(f"[DEBUG] Committing remaining pending event on exit: duration={state.pending_event['duration']:.1f}s")
            commit_pending_event(state, save_to_disk=True)
        client.disconnect()
    except Exception as e:
        print(f"Connection error: {e}")
