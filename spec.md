# Tuning Dashboard — Design Spec

## Overview

A lightweight Node.js + vanilla HTML/JS web dashboard for real-time tuning of the Acoustic ATCS Proxy edge device. Provides serial monitor capture, Plotly graphing, MQTT event monitoring, and parameter slider controls that publish to the ESP32 via MQTT.

## Architecture

```
Browser (HTML/JS + Plotly.js + Socket.IO client)
       ↕ WebSocket (Socket.IO)
Node.js Server (server.js)
  ├── Socket.IO — bidirectional bridge to browser
  ├── serialport — COM port read/write (auto-detect + manual connect)
  └── mqtt.js — MQTT client (subscribe event/heartbeat/telemetry, publish config)
```

## Files

```
dashboard/
  package.json       — dependencies: express, socket.io, serialport, mqtt
  server.js          — Node.js server (serial + MQTT + Socket.IO bridge)
  public/
    index.html       — single-page UI: graph, event log, sliders
```

## Data Flow

### Serial → Browser
1. Server reads lines from COM port → parses [DSP], [FSM], [Event], [STATUS] patterns
2. Parsed DSP/FSM frame data → Socket.IO `dsp_data` / `fsm_data` events → Plotly graph
3. Non-parsed lines (no `#N` frame number) → Socket.IO `event_log` event → Event Log pane
4. State transitions (`[FSM] A → B`) → Socket.IO `state_change` event → graph shading + log

### MQTT → Browser
1. Server subscribes to `crossing/event`, `crossing/heartbeat`, `crossing/telemetry`
2. Incoming MQTT messages → Socket.IO `mqtt_*` events → browser display

### Browser → MQTT (Config)
1. User adjusts sliders → clicks Apply → Socket.IO `config_update` → server
2. Server publishes JSON to `crossing/config` topic

## COM Port Management

- On startup, server lists available serial ports
- Browser receives port list, user selects from dropdown
- Connect/Disconnect buttons control the serial session
- Connection status shown in UI header

## Serial Parser Rules

| Pattern | Action |
|---|---|
| `[DSP] #N \| ... main=... sec=... amb=... main_snr=... sec_snr=...` | Extract: frame#, rms_db, main_db, sec_db, amb_db, main_snr, sec_snr → **graph. NOT in event log** |
| `[FSM] #N \| ... main_snr=... sec_snr=... signal=... pulse=... state=...` | Extract: frame#, main_snr, sec_snr, signal, pulse, state → **graph. NOT in event log** |
| `[FSM] STATE1 → STATE2 (...)` | State transition → graph shading + **event log** |
| `[Event] ...` | Show in **event log** |
| `[STATUS] ...` | Show in **event log** |
| All other lines | Show in **event log** |

## UI Layout

### Header
- Dashboard title + COM port selector (dropdown), Connect/Disconnect buttons
- MQTT connection indicator (green/red dot + broker address)

### Main Area (2-pane, resizable)
- **Left: Plotly Graph** — resizable via draggable divider
- **Right: Event Log** — scrollable text area with timestamps

### Graph Details
- Single Plotly chart (no subplots — lighter)
- Traces: `main_db`, `sec_db`, `amb_db` (solid lines), `main_snr`, `sec_snr` (dashed), `signal` (step), `pulse` (step)
- Background shading: IDLE=green, PROBING=yellow, ACTIVE=red (via Plotly shapes)
- X-axis = elapsed seconds (monotonic counter), Y-axis = dB + binary overlay
- Time window dropup: 5s / 10s / 30s / 60s / 120s (default 30s)
- Auto-scroll toggle (default on): slides time window forward
- Responsive (`responsive: true`), user can pan/zoom when autoscroll off

### Event Log
- Scrollable `<div>` with auto-scroll
- Shows timestamped messages for non-parsed lines
- Clear button, AutoScroll toggle
- Max 5000 lines, oldest trimmed

### Parameters Pane (bottom)
Sliders for all `RuntimeConfig` fields:

| Parameter | Range | Step | Default |
|---|---|---|---|
| `main_snr_db` | 0–50 dB | 0.5 | 25.0 |
| `sec_snr_db` | 0–50 dB | 0.5 | 22.0 |
| `confirm_sec` | 0.1–5.0 s | 0.1 | 0.7 |
| `probing_timeout_sec` | 0.5–10.0 s | 0.1 | 3.0 |
| `active_timeout_sec` | 0.5–10.0 s | 0.1 | 2.5 |

Plus: Debug checkbox (`debug_enabled`)

**Apply button**: publishes all current values as JSON to `crossing/config`

## Dependencies

```json
{
  "express": "^4.18",
  "socket.io": "^4.7",
  "serialport": "^12.0",
  "mqtt": "^5.0"
}
```

Frontend: Plotly.js (from CDN), Socket.IO client (from CDN).

## Performance Considerations

- Ring buffer of max 10,000 data points per trace
- Plotly redraw throttled — batch updates per ~100ms interval
- Serial parser uses single-pass regex, no streaming state machine needed
- Event log trims at 5,000 lines

## Future Considerations

- Record/playback of serial data for offline analysis
- Export graph as PNG
- Dark/light theme toggle