# Acoustic ATCS Proxy Node Tuner Dashboard

## Overview

The Acoustic ATCS Proxy Node Tuner Dashboard is a real-time tuning and monitoring dashboard for the ATCS Proxy Node (ESP32 edge device). It provides two main modes of interaction:

1. **USB Serial Interface:** Connects directly to the ESP32 node via USB at a baud rate of 460,800 to stream and parse raw DSP frames (frequencies, SNR, RMS) and FSM state transitions, rendering them in real-time charts.
2. **MQTT Interface:** Connects to a central MQTT broker to receive events, heartbeats, and telemetry published by the edge device, and enables remote, live tuning of all node configuration parameters (DSP thresholds, cycle filters, and sleep schedules).

The dashboard is built as a lightweight Node.js application using Express, Socket.IO, SerialPort, and MQTT.js, with a single-page web interface for real-time visualization.

## System Architecture

The diagram below shows how the dashboard acts as the bridge between the physical ESP32 node (via USB Serial), the MQTT broker, and the browser UI:

```mermaid
graph TD
    subgraph PublicWeb ["Public Website (Next.js)"]
        NEXT[Next.js App]
        DISP[Displays Current/Last Event & ML Predictions]
        NEXT --- DISP
    end

    subgraph Browser ["Browser UI (Frontend)"]
        UI[Web Dashboard Page] <-->|Socket.IO| SIO[Socket.IO Client]
        UI -->|Charts| CHARTS[Real-time Scrolling Charts]
        UI -->|Controls| CTRLS[Tuning Sliders & Live Log]
    end

    subgraph DashServer ["Dashboard Server (Node.js)"]
        EXPRESS[Express App] -->|Serves index.html| UI
        SOCKET[Socket.IO Server] <--> SIO
        SP[SerialPort Connection] -->|Reads 460800 Baud| PARSE[Log Line Parser]
        PARSE -->|dsp_data / fsm_data / state_change| SOCKET
        MQTTJS[MQTT.js Client] <--> SOCKET
    end

    subgraph Hardware ["ESP32 Node & Infrastructure"]
        ESP32[ESP32 ATCS Edge Device]
        BROKER[MQTT Broker]
    end

    subgraph ML ["Machine Learning Service"]
        MLPRED["Online ML Predictor (Python)"]
    end

    subgraph Logging ["Data Logging"]
        LOGGER["MQTT Logger (Python)"]
        SQL[(SQLite Database)]
    end

    ESP32 -->|USB Serial Debug Stream| SP
    ESP32 <-->|MQTT Topics| BROKER
    BROKER <-->|MQTT Topics| MQTTJS
    BROKER <-->|crossing/status & predictions| MLPRED
    BROKER -->|crossing/#| LOGGER
    LOGGER -->|Writes events| SQL
    BROKER <-->|"MQTT Topics (Events & Predictions)"| NEXT
```

---

## Configuration & Environment Variables

You can configure the HTTP port and the MQTT broker URL using either command-line arguments or environment variables. Command-line arguments always take precedence.

| Setting | CLI Flag | Environment Variable | Default Value |
| :--- | :--- | :--- | :--- |
| **Server Port** | `--port <number>` or `--port=<number>` | `PORT` | `3000` |
| **MQTT Broker** | `--mqtt-broker <url>` or `--mqtt=<url>` | `MQTT_BROKER` or `MQTT_URL` | `mqtt://localhost:1883` |

*Note: If no protocol (e.g. `mqtt://` or `mqtts://`) is specified in the broker URL, the dashboard will automatically prefix it with `mqtt://`.*

### Example Configurations

**Using CLI Flags:**

```bash
npm start -- --port=8080 --mqtt-broker=192.168.1.150
```

**Using Environment Variables (via `.env` or Shell):**

```bash
PORT=8080 MQTT_BROKER=mqtt://192.168.1.150 npm start
```

---

## Getting Started

### Prerequisites

* Node.js (v18 or higher recommended)
* npm (installed with Node.js)
* A running MQTT Broker (e.g., Mosquitto) if using MQTT remote configuration.

### Installation

1. Navigate to the dashboard directory.
2. Install the required dependencies:

    ```bash
    npm install
    ```

### Running the Dashboard

* **Production / Standard Mode:**

    ```bash
    npm start
    ```

* **Development / Auto-reload Mode:**
    Runs the server using Node's built-in file watcher:

    ```bash
    npm run dev
    ```

Once started, open your web browser and navigate to `http://localhost:3000` (or your configured port) to access the dashboard.

### Machine Learning Service Details & Setup

The repository includes a Python-based Online Machine Learning service (`ml_predictor/`) that predicts future crossing durations and wait times.

#### How It Works

The ML service uses **online machine learning** to continuously update its models in real-time as new data arrives.

1. **Data Ingestion:** Subscribes to the `crossing/event` MQTT topic and listens for `ended` events (which provide timestamp and duration).
2. **Feature Extraction:** Extracts temporal features (sine/cosine of hour and day of week) and historical context (previous duration and wait time).
3. **Online Learning (Self-Correction):** Calculates the actual wait time and duration when an event ends, and immediately trains its models using the previous features (via `river`'s `HoeffdingTreeRegressor`).
4. **Prediction:** Extracts features for the current moment and predicts the *next* wait time and duration.
5. **EMA Fallback:** Maintains an Exponential Moving Average (EMA). If the ML model has seen fewer than 100 samples, or if its prediction deviates from the EMA by >40%, it safely falls back to the EMA baseline.
6. **State Persistence:** Model state is saved locally to `model_state.pkl` after every event to preserve learning.
7. **Publishing:** Predictions are published to `crossing/predictions` for the dashboard UI.

#### Running the Predictor

1. Navigate to the predictor directory: `cd ml_predictor`
2. Install dependencies: `pip install -r requirements.txt`
3. Run the service: `python main.py`
    * *Tip: Enable verbose debug logging by prefixing with `VERBOSE=1` (or `$env:VERBOSE="1"` in PowerShell).*
4. (Optional) For testing, run the mock edge node in a separate terminal: `python mock_edge.py`

### Running the MQTT Logger Service

The repository also includes a Python script (`logger/logger.py`) that subscribes to all `crossing/#` topics and logs raw payloads to a local SQLite database for historical analysis.

1. Navigate to the logger directory: `cd logger`
2. Install dependencies (e.g., `pip install paho-mqtt python-dotenv`)
3. Run the logger: `python logger.py`

---

## Functional Features

### 1. Connection Panel

* **Serial Connection:** Scan for available COM/tty ports, choose the port connected to the ESP32, and click **Connect**. The server will automatically lock to 460,800 baud and begin parsing data.
* **MQTT Connection:** Displays connection status to the configured broker and connects automatically upon launch.

### 2. Live Graphing & Telemetry

* Plots real-time lines for the Goertzel magnitude of target frequencies (1253 Hz, 662 Hz) alongside the ambient noise baseline (900 Hz).
* Plots live SNR values and FSM states (`IDLE`, `PROBING`, `ACTIVE`) so you can visually correlate buzzer pulses with state changes.

### 3. Remote Parameter Tuning

* Provides sliders to adjust DSP thresholds (`main_snr_db`, `sec_snr_db`, `alpha_attack`, `alpha_decay`, `alpha_signal`) and FSM timings (`cycle_target_ms`, `cycle_tolerance_ms`, `required_cycles`).
* Includes a **Persist to NVS** checkbox. When enabled, settings will survive an ESP32 hardware reboot.
* Click **Apply** to publish the changes immediately to the `crossing/config` topic.
* Click **Refresh** to request the node's current configuration from the NVS storage via the `crossing/config/req` topic.

### 4. Interactive Event Log

* Displays a scrolling list of all raw log strings received from the Serial port, as well as incoming and outgoing MQTT payloads (events, heartbeats, and config ACKs).
