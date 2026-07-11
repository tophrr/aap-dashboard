# Tuning Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lightweight Node.js + vanilla HTML/JS tuning dashboard for the ESP32 ATCS edge device.

**Architecture:** Single Node.js server (`server.js`) handles serial port reading, MQTT client, and Socket.IO bridge. Frontend is a single `index.html` with Plotly.js for graphing, slider controls for parameter tuning, and an event log pane.

**Tech Stack:** Node.js, Express, Socket.IO, serialport, mqtt.js, Plotly.js (CDN)

## Global Constraints

- Serial baud rate: 921600 (matching ESP32 firmware)
- MQTT broker: 10.101.118.108:1883, topics: `crossing/event`, `crossing/heartbeat`, `crossing/telemetry`, `crossing/config`
- Parsed [DSP] #N and [FSM] #N lines go to graph only, NOT event log
- All other lines (no #N frame number) go to event log
- Time window dropdown: 5s/10s/30s/60s/120s, default 30s
- Max 10,000 data points ring buffer per trace
- Event log max 5,000 lines before trimming
- Graph uses Plotly.js with responsive layout and state background shading

---

### Task 1: Initialize project and install dependencies

**Files:**
- Create: `d:\Dev\crossing\aap-edge\dashboard\package.json`
- Create: `d:\Dev\crossing\aap-edge\dashboard\.gitignore`

**Interfaces:**
- Consumes: nothing
- Produces: project scaffold with all npm dependencies declared

- [ ] **Step 1: Create directory and package.json**

```bash
mkdir -p d:\Dev\crossing\aap-edge\dashboard
```

Create `dashboard/package.json`:

```json
{
  "name": "aap-tuner-dashboard",
  "version": "1.0.0",
  "description": "Real-time tuning dashboard for Acoustic ATCS Proxy edge device",
  "main": "server.js",
  "scripts": {
    "start": "node server.js",
    "dev": "node --watch server.js"
  },
  "dependencies": {
    "express": "^4.18.0",
    "socket.io": "^4.7.0",
    "serialport": "^12.0.0",
    "mqtt": "^5.0.0"
  }
}
```

- [ ] **Step 2: Create dashboard/.gitignore**

```
node_modules/
```

- [ ] **Step 3: Install dependencies**

Run: `cd d:\Dev\crossing\aap-edge\dashboard && npm install`

Expected: `node_modules/` created, no errors

- [ ] **Step 4: Create dashboard/public directory**

```bash
mkdir -p d:\Dev\crossing\aap-edge\dashboard\public
```

- [ ] **Step 5: Commit**

```bash
cd d:\Dev\crossing\aap-edge
git add dashboard/
git commit -m "chore: scaffold tuning dashboard project"
```

---

### Task 2: Server — serial port management + Socket.IO setup

**Files:**
- Create: `d:\Dev\crossing\aap-edge\dashboard\server.js`

**Interfaces:**
- Consumes: nothing
- Produces: Express + Socket.IO server on port 3000, serial port listing/connect/disconnect via Socket.IO

- [ ] **Step 1: Write server.js skeleton**

```javascript
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { SerialPort } = require('serialport');
const mqtt = require('mqtt');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 3000;
const MQTT_BROKER = 'mqtt://10.101.118.108:1883';

app.use(express.static('public'));

// ── State ─────────────────────────────────────────────────
let serialPort = null;
let mqttClient = null;
let lineBuffer = '';

// ── Socket.IO ──────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[Socket] Client connected: ${socket.id}`);

  // ── Serial port listing ────────────────────────────────
  socket.on('list_ports', async () => {
    try {
      const ports = await SerialPort.list();
      socket.emit('port_list', ports.map(p => ({
        path: p.path,
        manufacturer: p.manufacturer || ''
      })));
    } catch (err) {
      socket.emit('port_list', []);
    }
  });

  // ── Serial connect ─────────────────────────────────────
  socket.on('serial_connect', (portPath) => {
    if (serialPort && serialPort.isOpen) {
      serialPort.close();
    }
    try {
      serialPort = new SerialPort({
        path: portPath,
        baudRate: 921600,
        autoOpen: true
      });
      serialPort.on('open', () => {
        console.log(`[Serial] Connected: ${portPath}`);
        io.emit('serial_status', { connected: true, port: portPath });
      });
      serialPort.on('data', (data) => {
        lineBuffer += data.toString('utf8');
        const lines = lineBuffer.split('\n');
        lineBuffer = lines.pop(); // keep incomplete line
        for (const line of lines) {
          if (line.trim()) processLine(line.trim(), io);
        }
      });
      serialPort.on('error', (err) => {
        console.error(`[Serial] Error: ${err.message}`);
        io.emit('serial_status', { connected: false, port: portPath, error: err.message });
      });
      serialPort.on('close', () => {
        console.log('[Serial] Disconnected');
        io.emit('serial_status', { connected: false, port: null });
      });
    } catch (err) {
      io.emit('serial_status', { connected: false, port: null, error: err.message });
    }
  });

  // ── Serial disconnect ──────────────────────────────────
  socket.on('serial_disconnect', () => {
    if (serialPort && serialPort.isOpen) {
      serialPort.close();
    }
    serialPort = null;
  });

  // ── Config update (from slider Apply) ──────────────────
  socket.on('config_update', (config) => {
    console.log('[Config] Publishing:', JSON.stringify(config));
    if (mqttClient && mqttClient.connected) {
      mqttClient.publish('crossing/config', JSON.stringify(config));
      io.emit('event_log', {
        time: new Date().toLocaleTimeString(),
        text: `[Config] Published: ${JSON.stringify(config)}`
      });
    } else {
      io.emit('event_log', {
        time: new Date().toLocaleTimeString(),
        text: `[Config] MQTT not connected — config not sent`
      });
    }
  });

  // ── MQTT connection request ────────────────────────────
  socket.on('mqtt_connect', () => {
    if (mqttClient && mqttClient.connected) return;
    connectMQTT();
  });

  socket.on('mqtt_disconnect', () => {
    if (mqttClient) {
      mqttClient.end(true);
      mqttClient = null;
      io.emit('mqtt_status', { connected: false });
    }
  });
});

// ── MQTT Connection ──────────────────────────────────────────
function connectMQTT() {
  mqttClient = mqtt.connect(MQTT_BROKER, {
    clientId: 'aap_tuner_' + Math.random().toString(16).slice(2, 8)
  });

  mqttClient.on('connect', () => {
    console.log('[MQTT] Connected');
    mqttClient.subscribe('crossing/event');
    mqttClient.subscribe('crossing/heartbeat');
    mqttClient.subscribe('crossing/telemetry');
    io.emit('mqtt_status', { connected: true, broker: MQTT_BROKER });
  });

  mqttClient.on('message', (topic, message) => {
    const payload = message.toString();
    io.emit('mqtt_message', { topic, payload, time: Date.now() / 1000 });
    // Also show in event log
    io.emit('event_log', {
      time: new Date().toLocaleTimeString(),
      text: `[MQTT] <${topic}> ${payload}`
    });
  });

  mqttClient.on('close', () => {
    console.log('[MQTT] Disconnected');
    io.emit('mqtt_status', { connected: false });
  });

  mqttClient.on('error', (err) => {
    console.error(`[MQTT] Error: ${err.message}`);
  });
}

// ── Serial Line Parser ───────────────────────────────────────
function processLine(line, io) {
  // Detect [DSP] #N with frame number → graph, NOT event log
  const dspMatch = line.match(/^\[DSP\]\s+#(\d+)\s+\|.*main=(-?\d+\.?\d*)\s+sec=(-?\d+\.?\d*)\s+amb=(-?\d+\.?\d*)\s+\|.*main_snr=(-?\d+\.?\d*)\s+sec_snr=(-?\d+\.?\d*)/);
  if (dspMatch) {
    io.emit('dsp_data', {
      frame: parseInt(dspMatch[1]),
      main_db: parseFloat(dspMatch[2]),
      sec_db: parseFloat(dspMatch[3]),
      amb_db: parseFloat(dspMatch[4]),
      main_snr: parseFloat(dspMatch[5]),
      sec_snr: parseFloat(dspMatch[6]),
      time: Date.now() / 1000
    });
    return;
  }

  // Detect [FSM] #N with frame number → graph, NOT event log
  const fsmMatch = line.match(/^\[FSM\]\s+#(\d+)\s+\|.*main_snr=(-?\d+\.?\d*)\s+sec_snr=(-?\d+\.?\d*)\s+\|.*signal=(\w+)\s+pulse=(\w+)\s+\|.*state=(\w+)/);
  if (fsmMatch) {
    io.emit('fsm_data', {
      frame: parseInt(fsmMatch[1]),
      main_snr: parseFloat(fsmMatch[2]),
      sec_snr: parseFloat(fsmMatch[3]),
      signal: fsmMatch[4] === 'YES' ? 1 : 0,
      pulse: fsmMatch[5] === 'OK' ? 1 : 0,
      state: fsmMatch[6],
      time: Date.now() / 1000
    });
    return;
  }

  // Detect state transitions like [FSM] IDLE → PROBING
  const transitionMatch = line.match(/^\[FSM\]\s+(\w+)\s*→\s*(\w+)/);
  if (transitionMatch) {
    io.emit('state_change', {
      from: transitionMatch[1],
      to: transitionMatch[2],
      time: Date.now() / 1000
    });
  }

  // Everything else → event log
  io.emit('event_log', {
    time: new Date().toLocaleTimeString(),
    text: line
  });
}

// ── Start Server ────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[Server] Tuning dashboard running on http://localhost:${PORT}`);
});
```

- [ ] **Step 2: Verify server starts**

Run: `cd d:\Dev\crossing\aap-edge\dashboard && node server.js`

Expected: `[Server] Tuning dashboard running on http://localhost:3000`

Kill with Ctrl+C.

- [ ] **Step 3: Commit**

```bash
cd d:\Dev\crossing\aap-edge
git add dashboard/server.js
git commit -m "feat: add server with serial, MQTT, Socket.IO bridge"
```

---

### Task 3: Frontend — HTML skeleton, header, COM port management UI

**Files:**
- Create: `d:\Dev\crossing\aap-edge\dashboard\public\index.html`

This task creates the basic HTML structure with the header section including COM port selector and MQTT status.

- [ ] **Step 1: Write index.html basic structure**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Acoustic ATCS Proxy Tuner</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #e0e0e0; height: 100vh; display: flex; flex-direction: column; }
    
    /* Header */
    .header { display: flex; align-items: center; gap: 12px; padding: 8px 16px; background: #16213e; border-bottom: 1px solid #0f3460; flex-shrink: 0; }
    .header h1 { font-size: 16px; font-weight: 600; margin-right: auto; }
    .status-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
    .status-dot.green { background: #4ade80; }
    .status-dot.red { background: #f87171; }
    .status-dot.yellow { background: #facc15; }
    .btn { padding: 4px 12px; border: 1px solid #0f3460; border-radius: 4px; background: #0f3460; color: #e0e0e0; cursor: pointer; font-size: 12px; }
    .btn:hover { background: #1a4a8a; }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn.danger { border-color: #991b1b; background: #7f1d1d; }
    .btn.danger:hover { background: #991b1b; }
    select { padding: 4px 8px; border: 1px solid #0f3460; border-radius: 4px; background: #16213e; color: #e0e0e0; font-size: 12px; }
    
    /* Main area */
    .main-area { display: flex; flex: 1; min-height: 0; }
    .graph-pane { flex: 1; min-width: 0; position: relative; }
    .log-pane { width: 340px; min-width: 200px; border-left: 1px solid #0f3460; display: flex; flex-direction: column; flex-shrink: 0; }
    .divider { width: 4px; cursor: col-resize; background: #0f3460; flex-shrink: 0; }
    .divider:hover { background: #1a4a8a; }
    
    /* Event log */
    .log-header { padding: 6px 10px; border-bottom: 1px solid #0f3460; font-size: 12px; font-weight: 600; display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
    .log-content { flex: 1; overflow-y: auto; font-family: 'Cascadia Code', 'Fira Code', monospace; font-size: 11px; padding: 4px 8px; background: #0d1117; white-space: pre-wrap; word-break: break-all; }
    .log-content div { padding: 1px 0; border-bottom: 1px solid #1a1a2e; }
    
    /* Controls bar */
    .controls-bar { display: flex; align-items: center; gap: 12px; padding: 6px 16px; background: #16213e; border-top: 1px solid #0f3460; border-bottom: 1px solid #0f3460; flex-shrink: 0; font-size: 12px; }
    .controls-bar label { display: flex; align-items: center; gap: 4px; }
    
    /* Parameters pane */
    .params-pane { display: flex; flex-wrap: wrap; gap: 8px; padding: 8px 16px; background: #16213e; flex-shrink: 0; border-top: 1px solid #0f3460; }
    .param-card { background: #1a1a2e; border: 1px solid #0f3460; border-radius: 6px; padding: 8px 12px; min-width: 160px; }
    .param-card label { display: block; font-size: 10px; text-transform: uppercase; color: #94a3b8; margin-bottom: 2px; }
    .param-card .value { font-size: 14px; font-weight: 700; margin-bottom: 4px; }
    .param-card input[type="range"] { width: 100%; height: 4px; appearance: none; background: #0f3460; border-radius: 2px; outline: none; }
    .param-card input[type="range"]::-webkit-slider-thumb { appearance: none; width: 14px; height: 14px; border-radius: 50%; background: #4ade80; cursor: pointer; }
    .param-card input[type="checkbox"] { width: 18px; height: 18px; accent-color: #4ade80; }
    
    .graph-pane #plotly-graph { width: 100%; height: 100%; }
  </style>
</head>
<body>
  <div class="header">
    <h1>⚡ Acoustic ATCS Proxy Tuner</h1>
    
    <div id="serial-section">
      <select id="port-select" style="width:180px"><option value="">(select port)</option></select>
      <button class="btn" id="btn-connect">Connect</button>
      <button class="btn danger" id="btn-disconnect" disabled>Disconnect</button>
      <span id="serial-status" style="font-size:12px;color:#94a3b8">—</span>
    </div>

    <div id="mqtt-section">
      <span class="status-dot red" id="mqtt-dot"></span>
      <span id="mqtt-label" style="font-size:12px">MQTT disconnected</span>
      <button class="btn" id="btn-mqtt-connect">Connect MQTT</button>
      <button class="btn danger" id="btn-mqtt-disconnect" disabled>Disconnect MQTT</button>
    </div>
  </div>

  <div class="main-area">
    <div class="graph-pane">
      <div id="plotly-graph"></div>
    </div>
    <div class="divider" id="divider"></div>
    <div class="log-pane">
      <div class="log-header">
        <span>Event Log</span>
        <button class="btn" id="btn-clear-log" style="margin-left:auto">Clear</button>
        <label><input type="checkbox" id="chk-autoscroll" checked> Auto</label>
      </div>
      <div class="log-content" id="log-content"></div>
    </div>
  </div>

  <div class="controls-bar">
    <label>Time Window:</label>
    <select id="window-select">
      <option value="5">5s</option>
      <option value="10">10s</option>
      <option value="30" selected>30s</option>
      <option value="60">60s</option>
      <option value="120">120s</option>
    </select>
    <label><input type="checkbox" id="chk-autoscroll-graph" checked> AutoScroll</label>
    <span id="data-points" style="color:#94a3b8;margin-left:auto">0 points</span>
  </div>

  <div class="params-pane" id="params-pane"></div>

  <script>
    const socket = io();

    // ── DOM refs ───────────────────────────────────────────
    const portSelect = document.getElementById('port-select');
    const btnConnect = document.getElementById('btn-connect');
    const btnDisconnect = document.getElementById('btn-disconnect');
    const serialStatus = document.getElementById('serial-status');
    const mqttDot = document.getElementById('mqtt-dot');
    const mqttLabel = document.getElementById('mqtt-label');
    const btnMqttConnect = document.getElementById('btn-mqtt-connect');
    const btnMqttDisconnect = document.getElementById('btn-mqtt-disconnect');
    const logContent = document.getElementById('log-content');
    const chkAutoscroll = document.getElementById('chk-autoscroll');
    const btnClearLog = document.getElementById('btn-clear-log');
    const windowSelect = document.getElementById('window-select');
    const chkAutoscrollGraph = document.getElementById('chk-autoscroll-graph');
    const dataPointsSpan = document.getElementById('data-points');
    const divider = document.getElementById('divider');

    // ── Port listing ──────────────────────────────────────
    socket.emit('list_ports');
    setInterval(() => { socket.emit('list_ports'); }, 5000);

    socket.on('port_list', (ports) => {
      const currentVal = portSelect.value;
      portSelect.innerHTML = '<option value="">(select port)</option>' +
        ports.map(p => `<option value="${p.path}">${p.path}${p.manufacturer ? ' — ' + p.manufacturer : ''}</option>`).join('');
      portSelect.value = currentVal;
    });

    // ── Serial connect/disconnect ─────────────────────────
    btnConnect.addEventListener('click', () => {
      const port = portSelect.value;
      if (!port) return;
      btnConnect.disabled = true;
      socket.emit('serial_connect', port);
    });

    btnDisconnect.addEventListener('click', () => {
      socket.emit('serial_disconnect');
    });

    socket.on('serial_status', (status) => {
      btnConnect.disabled = false;
      if (status.connected) {
        serialStatus.textContent = `✓ ${status.port}`;
        serialStatus.style.color = '#4ade80';
        btnConnect.disabled = true;
        btnDisconnect.disabled = false;
      } else {
        serialStatus.textContent = status.error || 'disconnected';
        serialStatus.style.color = status.error ? '#f87171' : '#94a3b8';
        btnConnect.disabled = false;
        btnDisconnect.disabled = true;
      }
    });

    // ── MQTT connect/disconnect ───────────────────────────
    btnMqttConnect.addEventListener('click', () => {
      socket.emit('mqtt_connect');
      btnMqttConnect.disabled = true;
    });

    btnMqttDisconnect.addEventListener('click', () => {
      socket.emit('mqtt_disconnect');
    });

    socket.on('mqtt_status', (status) => {
      btnMqttConnect.disabled = false;
      if (status.connected) {
        mqttDot.className = 'status-dot green';
        mqttLabel.textContent = `MQTT ${status.broker}`;
        btnMqttConnect.disabled = true;
        btnMqttDisconnect.disabled = false;
      } else {
        mqttDot.className = 'status-dot red';
        mqttLabel.textContent = 'MQTT disconnected';
        btnMqttConnect.disabled = false;
        btnMqttDisconnect.disabled = true;
      }
    });

    // ── Event log ─────────────────────────────────────────
    socket.on('event_log', (entry) => {
      const div = document.createElement('div');
      div.textContent = `[${entry.time}] ${entry.text}`;
      logContent.appendChild(div);
      if (chkAutoscroll.checked) {
        logContent.scrollTop = logContent.scrollHeight;
      }
      // Trim to 5000 lines
      while (logContent.children.length > 5000) {
        logContent.removeChild(logContent.firstChild);
      }
    });

    btnClearLog.addEventListener('click', () => {
      logContent.innerHTML = '';
    });

    // ── Divider drag for resizing log pane ──────────────
    let isDragging = false;
    divider.addEventListener('mousedown', (e) => { isDragging = true; e.preventDefault(); });
    document.addEventListener('mousemove', (e) => {
      if (!isDragging) return;
      const logPane = document.querySelector('.log-pane');
      const mainArea = document.querySelector('.main-area');
      const rect = mainArea.getBoundingClientRect();
      const newWidth = Math.max(200, Math.min(rect.width - 200, rect.right - e.clientX));
      logPane.style.width = newWidth + 'px';
    });
    document.addEventListener('mouseup', () => { isDragging = false; });

    // ── Plotly initialization (placeholder, filled in Task 4) ──
    const graphDiv = document.getElementById('plotly-graph');

    // ── Parameter cards (filled in Task 5) ────────────────
    // ── Data buffers (filled in Task 4) ───────────────────
  </script>
</body>
</html>
```

- [ ] **Step 2: Verify static file serving**

Start server: `cd d:\Dev\crossing\aap-edge\dashboard && node server.js`
Open browser to `http://localhost:3000`

Expected: Dashboard header with COM port selector, MQTT section, empty graph area, empty event log, controls bar, empty parameters pane. All visible and styled.

Kill server.

- [ ] **Step 3: Commit**

```bash
cd d:\Dev\crossing\aap-edge
git add dashboard/public/index.html
git commit -m "feat: add dashboard UI skeleton with layout and controls"
```

---

### Task 4: Frontend — Plotly graph with data buffers, auto-scrolling, state shading

**Files:**
- Modify: `d:\Dev\crossing\aap-edge\dashboard\public\index.html` (add graph JS logic)

This task adds the full graph implementation — data buffers, Plotly traces, auto-scroll, time window, state shading.

- [ ] **Step 1: Add data buffer and graph initialization code**

Replace the `// ── Plotly initialization` and `// ── Data buffers (filled in Task 4) ───────────────────` placeholder comments with:

```javascript
    // ── Data Buffers ──────────────────────────────────────
    const MAX_POINTS = 10000;
    const data = {
      time: [],
      main_db: [], sec_db: [], amb_db: [],
      main_snr: [], sec_snr: [],
      signal: [], pulse: [],
      state: [] // 'IDLE', 'PROBING', 'ACTIVE' as strings
    };
    let stateShapes = [];

    function pushData(series, value) {
      data[series].push(value);
      if (data[series].length > MAX_POINTS) {
        data[series].splice(0, data[series].length - MAX_POINTS);
      }
    }

    // ── State tracking ────────────────────────────────────
    let currentState = 'IDLE';

    function addStateChange(fromState, toState, t) {
      // Add a shape rectangle for the completed state span
      if (fromState && toState) {
        stateShapes.push({
          type: 'rect',
          x0: t,
          y0: -120,
          x1: t + 0.1,
          y1: 120,
          fillcolor: stateColor(fromState),
          opacity: 0.15,
          line: { width: 0 },
          layer: 'below'
        });
      }
      currentState = toState;
      pushData('state', toState);
    }

    function stateColor(s) {
      switch (s) {
        case 'IDLE': return '#4ade80';
        case 'PROBING': return '#facc15';
        case 'ACTIVE': return '#f87171';
        default: return '#94a3b8';
      }
    }

    // ── Plotly layout ──────────────────────────────────────
    function getLayout() {
      const windowSec = parseInt(windowSelect.value);
      const now = Date.now() / 1000;
      const xRange = chkAutoscrollGraph.checked ? [now - windowSec, now + 1] : undefined;

      return {
        title: { text: 'Real-time DSP & FSM Data', font: { color: '#e0e0e0', size: 14 } },
        paper_bgcolor: '#0d1117',
        plot_bgcolor: '#0d1117',
        font: { color: '#94a3b8' },
        margin: { l: 50, r: 30, t: 30, b: 40 },
        hovermode: 'x unified',
        legend: { font: { color: '#e0e0e0', size: 10 }, orientation: 'h', y: 1.12 },
        dragmode: 'zoom',
        xaxis: {
          title: 'Time (s)',
          range: xRange,
          color: '#94a3b8',
          gridcolor: '#1a1a2e',
          zerolinecolor: '#1a1a2e'
        },
        yaxis: {
          title: 'dB',
          color: '#94a3b8',
          gridcolor: '#1a1a2e',
          zerolinecolor: '#1a1a2e',
          range: [-120, 60]
        },
        shapes: stateShapes.slice(-200) // show last 200 state segments
      };
    }

    function getTraces() {
      return [
        { name: 'main (dB)', x: data.time, y: data.main_db, type: 'scatter', mode: 'lines', line: { width: 1.5, color: '#4ade80' }, yaxis: 'y' },
        { name: 'sec (dB)', x: data.time, y: data.sec_db, type: 'scatter', mode: 'lines', line: { width: 1.5, color: '#60a5fa' }, yaxis: 'y' },
        { name: 'amb (dB)', x: data.time, y: data.amb_db, type: 'scatter', mode: 'lines', line: { width: 1.5, color: '#94a3b8' }, yaxis: 'y' },
        { name: 'main_snr', x: data.time, y: data.main_snr, type: 'scatter', mode: 'lines', line: { width: 1, dash: 'dash', color: '#4ade80' }, yaxis: 'y' },
        { name: 'sec_snr', x: data.time, y: data.sec_snr, type: 'scatter', mode: 'lines', line: { width: 1, dash: 'dash', color: '#60a5fa' }, yaxis: 'y' },
        { name: 'signal', x: data.time, y: data.signal, type: 'scatter', mode: 'lines', line: { shape: 'hv', width: 1.5, color: '#facc15' }, yaxis: 'y2' },
        { name: 'pulse', x: data.time, y: data.pulse, type: 'scatter', mode: 'lines', line: { shape: 'hv', width: 1.5, color: '#c084fc' }, yaxis: 'y2' }
      ];
    }

    // ── Plotly init ───────────────────────────────────────
    const plotConfig = {
      responsive: true,
      displayModeBar: true,
      modeBarButtonsToRemove: ['lasso2d', 'select2d', 'hoverClosestCartesian', 'hoverCompareCartesian'],
      displaylogo: false
    };

    Plotly.newPlot(graphDiv, [], {
      ...getLayout(),
      yaxis2: { overlaying: 'y', side: 'right', range: [-0.1, 1.1], showgrid: false, visible: false }
    }, plotConfig);

    // ── Update throttle ───────────────────────────────────
    let updatePending = false;
    function requestUpdate() {
      if (updatePending) return;
      updatePending = true;
      requestAnimationFrame(() => {
        updatePending = false;
        const windowSec = parseInt(windowSelect.value);
        const now = Date.now() / 1000;
        const layoutUpdate = {};
        if (chkAutoscrollGraph.checked) {
          layoutUpdate['xaxis.range'] = [now - windowSec, now + 1];
        }
        Plotly.react(graphDiv, getTraces(), {
          ...getLayout(),
          ...layoutUpdate,
          yaxis2: { overlaying: 'y', side: 'right', range: [-0.1, 1.1], showgrid: false, visible: false }
        }, plotConfig);
        dataPointsSpan.textContent = `${data.time.length} points`;
      });
    }

    // ── Socket event handlers ─────────────────────────────
    socket.on('dsp_data', (d) => {
      const t = d.time;
      pushData('time', t);
      pushData('main_db', d.main_db);
      pushData('sec_db', d.sec_db);
      pushData('amb_db', d.amb_db);
      pushData('main_snr', d.main_snr);
      pushData('sec_snr', d.sec_snr);
      // Fill signal/pulse with previous value (or 0)
      pushData('signal', data.signal.length > 0 ? data.signal[data.signal.length - 1] : 0);
      pushData('pulse', data.pulse.length > 0 ? data.pulse[data.pulse.length - 1] : 0);
      requestUpdate();
    });

    socket.on('fsm_data', (d) => {
      const lastTime = data.time.length > 0 ? data.time[data.time.length - 1] : d.time;
      pushData('time', d.time);
      pushData('signal', d.signal);
      pushData('pulse', d.pulse);
      // Extend db/snr traces with last values
      if (data.main_db.length > 0) {
        pushData('main_db', data.main_db[data.main_db.length - 1]);
        pushData('sec_db', data.sec_db[data.sec_db.length - 1]);
        pushData('amb_db', data.amb_db[data.amb_db.length - 1]);
        pushData('main_snr', d.main_snr);
        pushData('sec_snr', d.sec_snr);
      } else {
        pushData('main_db', 0); pushData('sec_db', 0); pushData('amb_db', 0);
        pushData('main_snr', d.main_snr); pushData('sec_snr', d.sec_snr);
      }
      requestUpdate();
    });

    socket.on('state_change', (d) => {
      addStateChange(d.from, d.to, d.time);
      // Also log state change
      const div = document.createElement('div');
      div.textContent = `[${new Date(d.time * 1000).toLocaleTimeString()}] [FSM] ${d.from} → ${d.to}`;
      div.style.color = stateColor(d.to);
      logContent.appendChild(div);
      if (chkAutoscroll.checked) logContent.scrollTop = logContent.scrollHeight;
      while (logContent.children.length > 5000) logContent.removeChild(logContent.firstChild);
    });

    // ── Window / autoscroll changes ───────────────────────
    windowSelect.addEventListener('change', requestUpdate);
    chkAutoscrollGraph.addEventListener('change', () => {
      if (chkAutoscrollGraph.checked) requestUpdate();
    });
```

- [ ] **Step 2: Verify graph loads**

Start server, open browser to `http://localhost:3000`
Expected: Empty Plotly graph visible with dark theme styling, axes labels, legend.

- [ ] **Step 3: Commit**

```bash
cd d:\Dev\crossing\aap-edge
git add dashboard/public/index.html
git commit -m "feat: add Plotly graph with data buffers, auto-scroll, state shading"
```

---

### Task 5: Frontend — Parameter sliders and MQTT config publishing

**Files:**
- Modify: `d:\Dev\crossing\aap-edge\dashboard\public\index.html` (add parameter cards + config update logic)

- [ ] **Step 1: Add parameter card definitions and render code**

Replace the `// ── Parameter cards (filled in Task 5) ────────────────` placeholder with:

```javascript
    // ── Parameter Definitions ──────────────────────────────
    const params = [
      { id: 'main_snr_db', label: 'Main SNR', min: 0, max: 50, step: 0.5, default: 25.0, unit: 'dB' },
      { id: 'sec_snr_db', label: 'Sec SNR', min: 0, max: 50, step: 0.5, default: 22.0, unit: 'dB' },
      { id: 'confirm_sec', label: 'Confirm', min: 0.1, max: 5.0, step: 0.1, default: 0.7, unit: 's' },
      { id: 'probing_timeout_sec', label: 'Probing Timeout', min: 0.5, max: 10.0, step: 0.1, default: 3.0, unit: 's' },
      { id: 'active_timeout_sec', label: 'Active Timeout', min: 0.5, max: 10.0, step: 0.1, default: 2.5, unit: 's' }
    ];

    const paramValues = {};
    const paramPane = document.getElementById('params-pane');

    // Debug checkbox
    const debugCard = document.createElement('div');
    debugCard.className = 'param-card';
    debugCard.innerHTML = `
      <label>Debug</label>
      <div style="display:flex;align-items:center;gap:8px;margin-top:4px">
        <input type="checkbox" id="debug_enabled">
        <span style="font-size:12px">Enable verbose debug</span>
      </div>
    `;
    paramPane.appendChild(debugCard);

    params.forEach(p => {
      paramValues[p.id] = p.default;
      const card = document.createElement('div');
      card.className = 'param-card';
      card.innerHTML = `
        <label>${p.label}</label>
        <div class="value" id="val-${p.id}">${p.default.toFixed(p.step >= 1 ? 0 : 1)} ${p.unit}</div>
        <input type="range" id="slider-${p.id}" min="${p.min}" max="${p.max}" step="${p.step}" value="${p.default}">
      `;
      paramPane.appendChild(card);
      const slider = card.querySelector('input[type="range"]');
      const valDisplay = card.querySelector('.value');
      slider.addEventListener('input', () => {
        const v = parseFloat(slider.value);
        paramValues[p.id] = v;
        valDisplay.textContent = `${v.toFixed(p.step >= 1 ? 0 : 1)} ${p.unit}`;
      });
    });

    // Apply button
    const applyCard = document.createElement('div');
    applyCard.className = 'param-card';
    applyCard.style.display = 'flex';
    applyCard.style.alignItems = 'center';
    applyCard.style.justifyContent = 'center';
    applyCard.innerHTML = `<button class="btn" id="btn-apply" style="font-size:14px;padding:8px 24px">Apply</button>`;
    paramPane.appendChild(applyCard);

    document.getElementById('btn-apply').addEventListener('click', () => {
      const config = { ...paramValues };
      config.debug_enabled = document.getElementById('debug_enabled').checked;
      socket.emit('config_update', config);
      // Visual feedback
      const btn = document.getElementById('btn-apply');
      btn.textContent = '✓ Sent';
      setTimeout(() => { btn.textContent = 'Apply'; }, 1500);
    });
```

- [ ] **Step 2: Verify sliders work**

Start server, open browser.
Expected: 5 parameter cards with labeled sliders, a debug checkbox, and an Apply button. Moving sliders updates the displayed value. Clicking Apply sends the config.

- [ ] **Step 3: Commit**

```bash
cd d:\Dev\crossing\aap-edge
git add dashboard/public/index.html
git commit -m "feat: add parameter sliders and MQTT config publish"
```

---

### Task 6: Frontend — MQTT message display pane

**Files:**
- Modify: `d:\Dev\crossing\aap-edge\dashboard\public\index.html` (add MQTT message display in event log + separate MQTT pane section)

The MQTT messages already flow into the event log from `io.emit('event_log', ...)` in the server's `mqtt.on('message')` handler. This task ensures MQTT messages also get a visual indicator in the header area.

- [ ] **Step 1: No code change needed** — MQTT messages already appear in the event log via the `mqtt_message` and `event_log` socket events from the server.

Verified: `mqtt_message` event handler is already wired in Task 2 server code (the server emits both `mqtt_message` and `event_log` on MQTT receipt).

- [ ] **Step 2: Verify** — Start server, connect MQTT, expect events from ESP32 to appear in the event log pane.

- [ ] **Step 3: Commit** (if changes needed — otherwise skip)

```bash
cd d:\Dev\crossing\aap-edge
git add dashboard/public/index.html
git commit -m "feat: wire MQTT messages into event log"
```

---

### Task 7: Polish — .gitignore, platformio.ini exclude, verify end-to-end

**Files:**
- Modify: `d:\Dev\crossing\aap-edge\.gitignore` (add dashboard node_modules exclusion)

- [ ] **Step 1: Update .gitignore to exclude dashboard/node_modules**

Check current `.gitignore`:

```bash
type d:\Dev\crossing\aap-edge\.gitignore
```

Add if missing:
```
dashboard/node_modules/
```

- [ ] **Step 2: Full end-to-end test**

1. Start dashboard: `cd dashboard && node server.js`
2. Open `http://localhost:3000`
3. Select COM port → Connect
4. Wait for serial data to flow
5. Verify:
   - Graph shows DSP lines (main/sec/amb dB + main_snr/sec_snr)
   - Signal/pulse binary traces appear
   - State shading changes (green→yellow→red)
   - Event log shows non-parsed lines only (no #N lines)
   - State transitions show in event log with color
   - MQTT connect button works
   - Slider values update, Apply publishes to MQTT
   - Time window selector changes visible range
   - AutoScroll toggles correctly
   - Log pane resizable via divider drag

- [ ] **Step 3: Commit final files**

```bash
cd d:\Dev\crossing\aap-edge
git add dashboard/ .gitignore
git commit -m "feat: complete tuning dashboard v1"
```