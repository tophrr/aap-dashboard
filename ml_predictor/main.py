import os
import json
import math
import pickle
import argparse
import sqlite3
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
                return pickle.load(f)
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
        
        # Step A: Calculate ground truth for the previous prediction
        current_wait_time = 0.0
        if state.previous_ended_at is not None:
            # Wait time is time from PREVIOUS end to CURRENT start
            current_wait_time = probing_started_at - state.previous_ended_at
            
            # Update EMAs
            if state.ema_wait is None:
                state.ema_wait = current_wait_time
                state.ema_duration = duration
            else:
                state.ema_wait = (EMA_ALPHA * current_wait_time) + ((1 - EMA_ALPHA) * state.ema_wait)
                state.ema_duration = (EMA_ALPHA * duration) + ((1 - EMA_ALPHA) * state.ema_duration)
            
            state.sample_count += 1
            
            # Step B: Learn (Self-Correct)
            if state.previous_features is not None:
                if VERBOSE:
                    print(f"[DEBUG] Training models. Actual Wait={current_wait_time:.1f}, Actual Duration={duration:.1f}")
                state.wait_time_model.learn_one(state.previous_features, current_wait_time)
                state.duration_model.learn_one(state.previous_features, duration)

        # Step C: Extract Current Features
        # We use `ended_at` as the timestamp for the current context.
        current_features = extract_features(ended_at, duration, current_wait_time)
        
        # Store state for next time
        state.previous_features = current_features
        state.previous_ended_at = ended_at
        
        # Save state to disk if requested
        if save_to_disk:
            save_state(state)
        
        # Step D: Predict the FUTURE
        pred_wait = state.wait_time_model.predict_one(current_features)
        pred_dur = state.duration_model.predict_one(current_features)
        
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
        client.disconnect()
    except Exception as e:
        print(f"Connection error: {e}")
