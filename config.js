const fs = require('fs');
const path = require('path');

// Load environment variables from .env file if it exists
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  try {
    process.loadEnvFile(envPath);
  } catch (err) {
    console.warn(`[Config] Failed to load .env file dynamically: ${err.message}`);
  }
}

// Simple CLI argument helper
const args = process.argv.slice(2);

function getArgValue(name) {
  // Check for --name=value
  const equalsPrefix = `--${name}=`;
  const eqIndex = args.findIndex(arg => arg.startsWith(equalsPrefix));
  if (eqIndex !== -1) {
    return args[eqIndex].substring(equalsPrefix.length);
  }
  // Check for --name value
  const spacePrefix = `--${name}`;
  const spaceIndex = args.indexOf(spacePrefix);
  if (spaceIndex !== -1 && spaceIndex + 1 < args.length) {
    const val = args[spaceIndex + 1];
    if (!val.startsWith('--')) {
      return val;
    }
  }
  return null;
}

// Parsing config values
const PORT = parseInt(getArgValue('port') || process.env.PORT || '3000', 10);
const UDP_PORT = parseInt(getArgValue('udp-port') || process.env.UDP_PORT || '5001', 10);

let brokerUrl = getArgValue('mqtt-broker') || getArgValue('mqtt') || process.env.MQTT_BROKER || process.env.MQTT_URL || 'mqtt://localhost:1883';
if (!brokerUrl.includes('://')) {
  brokerUrl = 'mqtt://' + brokerUrl;
}
const MQTT_BROKER = brokerUrl;

const MQTT_USERNAME = getArgValue('mqtt-username') || process.env.MQTT_USERNAME || null;
const MQTT_PASSWORD = getArgValue('mqtt-password') || process.env.MQTT_PASSWORD || null;

module.exports = {
  PORT,
  UDP_PORT,
  MQTT_BROKER,
  MQTT_USERNAME,
  MQTT_PASSWORD
};
