import os
import sqlite3
import json
from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from urllib.parse import urlparse

# Load environment variables from the parent directory's .env file
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir, '..', '.env')
load_dotenv(dotenv_path)

# Configuration
mqtt_url_env = os.getenv('MQTT_URL', 'mqtt://localhost:1883')
parsed_url = urlparse(mqtt_url_env)
MQTT_BROKER = parsed_url.hostname or 'localhost'
MQTT_PORT = parsed_url.port or 1883
MQTT_USER = os.getenv('MQTT_USERNAME', None)
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', None)

DB_PATH = os.path.join(script_dir, 'mqtt_logs.db')

def setup_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            topic TEXT NOT NULL,
            payload TEXT
        )
    ''')
    conn.commit()
    conn.close()

def insert_log(topic, payload):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO logs (topic, payload) VALUES (?, ?)
        ''', (topic, payload))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database error: {e}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        # Subscribe to all crossing topics
        client.subscribe("crossing/#")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8', errors='ignore')
    print(f"[{datetime.now().isoformat()}] Received on {msg.topic}: {payload}")
    insert_log(msg.topic, payload)

if __name__ == '__main__':
    print("Initializing Database...")
    setup_db()
    
    print("Setting up MQTT Client...")
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
        print("\nExiting logger...")
        client.disconnect()
    except Exception as e:
        print(f"Connection error: {e}")
