const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { SerialPort } = require('serialport');
const mqtt = require('mqtt');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Parse port from command line arguments or environment variable
let parsedPort = 3000;
const args = process.argv.slice(2);
const portEqualsIndex = args.findIndex(arg => arg.startsWith('--port='));
if (portEqualsIndex !== -1) {
  parsedPort = parseInt(args[portEqualsIndex].split('=')[1]);
} else {
  const portSpaceIndex = args.indexOf('--port');
  if (portSpaceIndex !== -1 && portSpaceIndex + 1 < args.length) {
    parsedPort = parseInt(args[portSpaceIndex + 1]);
  } else {
    parsedPort = process.env.PORT || 3000;
  }
}
const PORT = parsedPort;
// Parse MQTT broker from command line arguments or environment variable
let parsedMqttBroker = '';
const mqttBrokerEqualsIndex = args.findIndex(arg => arg.startsWith('--mqtt-broker='));
const mqttEqualsIndex = args.findIndex(arg => arg.startsWith('--mqtt='));

if (mqttBrokerEqualsIndex !== -1) {
  parsedMqttBroker = args[mqttBrokerEqualsIndex].split('=')[1];
} else if (mqttEqualsIndex !== -1) {
  parsedMqttBroker = args[mqttEqualsIndex].split('=')[1];
} else {
  const mqttBrokerSpaceIndex = args.indexOf('--mqtt-broker');
  const mqttSpaceIndex = args.indexOf('--mqtt');
  if (mqttBrokerSpaceIndex !== -1 && mqttBrokerSpaceIndex + 1 < args.length) {
    parsedMqttBroker = args[mqttBrokerSpaceIndex + 1];
  } else if (mqttSpaceIndex !== -1 && mqttSpaceIndex + 1 < args.length) {
    parsedMqttBroker = args[mqttSpaceIndex + 1];
  } else {
    parsedMqttBroker = process.env.MQTT_BROKER || process.env.MQTT_URL || 'mqtt://localhost:1883';
  }
}

// Auto-prepend mqtt:// if no protocol is specified
if (!parsedMqttBroker.includes('://')) {
  parsedMqttBroker = 'mqtt://' + parsedMqttBroker;
}
const MQTT_BROKER = parsedMqttBroker;

app.use(express.static('public'));

// ── State ─────────────────────────────────────────────────
let serialPort = null;
let mqttClient = null;
let lineBuffer = '';

// ── Socket.IO ──────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[Socket] Client connected: ${socket.id}`);

  // Emit current connection statuses to newly connected client
  socket.emit('mqtt_status', {
    connected: !!(mqttClient && mqttClient.connected),
    broker: MQTT_BROKER
  });

  socket.emit('serial_status', {
    connected: !!(serialPort && serialPort.isOpen),
    port: serialPort ? serialPort.path : null
  });

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
        baudRate: 460800,
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

  // ── Config request (from Refresh button) ────────────────
  socket.on('config_request', () => {
    console.log('[Config] Requesting current config');
    if (mqttClient && mqttClient.connected) {
      mqttClient.publish('crossing/config/req', '{}');
    }
  });

  // ── MQTT connection request ────────────────────────────
  socket.on('mqtt_connect', () => {
    if (mqttClient && mqttClient.connected) return;
    connectMQTT();
  });

  socket.on('mqtt_disconnect', () => {
    if (mqttClient) {
      console.log('[MQTT] Disconnecting from broker');
      mqttClient.end(true);
      mqttClient = null;
      io.emit('mqtt_status', { connected: false, broker: MQTT_BROKER });
    }
  });
});

// ── MQTT Connection ──────────────────────────────────────────
function connectMQTT() {
  console.log(`[MQTT] Connecting to broker: ${MQTT_BROKER}`);
  mqttClient = mqtt.connect(MQTT_BROKER, {
    clientId: 'aap_tuner_' + Math.random().toString(16).slice(2, 8)
  });

  mqttClient.on('connect', () => {
    console.log(`[MQTT] Connected to ${MQTT_BROKER}`);
    mqttClient.subscribe('crossing/event');
    mqttClient.subscribe('crossing/heartbeat');
    mqttClient.subscribe('crossing/telemetry');
    mqttClient.subscribe('crossing/config/ack');
    mqttClient.subscribe('crossing/rtt');
    mqttClient.subscribe('crossing/log');
    io.emit('mqtt_status', { connected: true, broker: MQTT_BROKER });
  });

  mqttClient.on('message', (topic, message) => {
    const payload = message.toString();

    if (topic === 'crossing/log') {
      processLine(payload, io, true);
      return;
    }

    io.emit('mqtt_message', { topic, payload, time: Date.now() / 1000 });
    // Also show in event log
    io.emit('event_log', {
      time: new Date().toLocaleTimeString(),
      text: `[MQTT] <${topic}> ${payload}`
    });

    if (topic === 'crossing/config/ack') {
      try {
        io.emit('config_ack', JSON.parse(payload));
      } catch (err) {
        console.error('[MQTT] JSON parse error in config ack:', err.message);
      }
    }
  });

  mqttClient.on('close', () => {
    console.log('[MQTT] Disconnected');
    io.emit('mqtt_status', { connected: false, broker: MQTT_BROKER });
  });

  mqttClient.on('error', (err) => {
    console.error(`[MQTT] Error: ${err.message}`);
  });
}

// ── Line Parser ───────────────────────────────────────
function processLine(line, io, isMqtt = false) {
  // Detect [DSP] #N with frame number → graph, NOT event log
  const dspMatch = line.match(/^(?:\[\d{2}[.:]\d{2}[.:]\d{2}\]\s+)?\[DSP\]\s+#(\d+)\s+\|.*RMS=(-?\d+\.?\d*)dB.*main=(-?\d+\.?\d*)\s+sec=(-?\d+\.?\d*)\s+amb=(-?\d+\.?\d*)\s*(?:\(raw=(-?\d+\.?\d*)\))?\s*\|.*main_snr=(-?\d+\.?\d*)\s+sec_snr=(-?\d+\.?\d*)/);
  if (dspMatch) {
    io.emit('dsp_data', {
      frame: parseInt(dspMatch[1]),
      rms_db: parseFloat(dspMatch[2]),
      main_db: parseFloat(dspMatch[3]),
      sec_db: parseFloat(dspMatch[4]),
      amb_db: parseFloat(dspMatch[5]),
      amb_raw_db: dspMatch[6] !== undefined ? parseFloat(dspMatch[6]) : parseFloat(dspMatch[5]),
      main_snr: parseFloat(dspMatch[7]),
      sec_snr: parseFloat(dspMatch[8]),
      time: Date.now() / 1000
    });
    return;
  }

  // Detect [FSM] #N with frame number → graph, NOT event log
  const fsmMatch = line.match(/^(?:\[\d{2}[.:]\d{2}[.:]\d{2}\]\s+)?\[FSM\]\s+#(\d+)\s+\|.*main_snr=(-?\d+\.?\d*)\s+sec_snr=(-?\d+\.?\d*)\s+\|.*signal=(\w+)\s+(?:pulse=(\w+)|cycles=(\d+))\s+\|.*state=(\w+)/);
  if (fsmMatch) {
    let cycles = 0;
    if (fsmMatch[5] !== undefined) {
      cycles = fsmMatch[5] === 'OK' ? 1 : 0;
    } else if (fsmMatch[6] !== undefined) {
      cycles = parseInt(fsmMatch[6]);
    }

    io.emit('fsm_data', {
      frame: parseInt(fsmMatch[1]),
      main_snr: parseFloat(fsmMatch[2]),
      sec_snr: parseFloat(fsmMatch[3]),
      signal: fsmMatch[4] === 'YES' ? 1 : 0,
      cycles: cycles,
      state: fsmMatch[7],
      time: Date.now() / 1000
    });
    return;
  }

  // Detect state transitions like [FSM] IDLE → PROBING
  const transitionMatch = line.match(/^(?:\[\d{2}[.:]\d{2}[.:]\d{2}\]\s+)?\[FSM\]\s+(\w+)\s*→\s*(\w+)/);
  if (transitionMatch) {
    io.emit('state_change', {
      from: transitionMatch[1],
      to: transitionMatch[2],
      time: Date.now() / 1000
    });
  }

  // Everything else → event log or mqtt log
  const eventName = isMqtt ? 'mqtt_log' : 'event_log';
  io.emit(eventName, {
    time: new Date().toLocaleTimeString(),
    text: line
  });
}

// ── Start Server ────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[Server] Tuning dashboard running on http://localhost:${PORT}`);
  console.log(`[Server] Configured MQTT broker: ${MQTT_BROKER}`);
});