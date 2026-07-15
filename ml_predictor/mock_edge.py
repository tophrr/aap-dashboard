import os
import json
import time
import random
from urllib.parse import urlparse
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# Load env variables
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir, '..', '.env')
load_dotenv(dotenv_path)

mqtt_url_env = os.getenv('MQTT_URL', 'mqtt://localhost:1883')
parsed_url = urlparse(mqtt_url_env)
MQTT_BROKER = parsed_url.hostname or 'localhost'
MQTT_PORT = parsed_url.port or 1883
MQTT_USER = os.getenv('MQTT_USERNAME', None)
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', None)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Mock Edge connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
    else:
        print(f"Failed to connect, return code {rc}")

if __name__ == '__main__':
    print("Starting Mock Edge Node (Fast-Forward Mode)...")
    client = mqtt.Client()
    
    if MQTT_USER and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        
    client.on_connect = on_connect
    
    print(f"Connecting to {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        
        # Start simulated time at current time
        simulated_time = time.time()
        
        event_count = 0
        while True:
            # Simulate realistic wait time: 90 to 120 seconds
            wait_time = random.uniform(90.0, 120.0)
            simulated_time += wait_time
            probing_started_at = simulated_time
            
            # Simulate crossing duration: 35 to 45 seconds
            duration = random.uniform(35.0, 45.0)
            simulated_time += duration
            ended_at = simulated_time
            
            payload = {
                "event": "ended",
                "duration": round(duration, 1),
                "probing_started_at": int(probing_started_at),
                "ended_at": int(ended_at)
            }
            
            event_count += 1
            print(f"[Event #{event_count}] Publishing to crossing/event: {json.dumps(payload)}")
            client.publish("crossing/event", json.dumps(payload))
            
            # Sleep a short amount of real time so it pumps out data quickly
            # but is still readable in the console (2 events per second).
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nExiting Mock Edge Node...")
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        print(f"Error: {e}")
