# requirements.txt
# -----------------
# paho-mqtt==2.1.0
# amqtt==0.11.3
#
# Install with: pip install -r requirements.txt
# -----------------

import asyncio
import html
import http.server
import json
import logging
import re
import socketserver
import subprocess
import threading
import time
import paho.mqtt.client as mqtt
import datetime
from amqtt.broker import Broker
logging.basicConfig(level=logging.INFO)

MQTT_PROTOCOL = mqtt.MQTTv311
RANDOM_SLIDER_TOPIC = 'GX26301250700800004/RandomSlider'
ALARM_TOPIC = 'GX26301250700800004/alarm'
NUMERIC_VALUE_TOPIC = 'GX26301250700800004/NumericValue'
STRING_TOPIC = 'GX26301250700800004/String'
mqtt_client = None

# Connected clients tracking
connected_clients = {}   # client_id -> {'ip': str, 'connected_at': str}
connected_clients_lock = threading.Lock()

# Broker status info
broker_info = {
    'status': 'starting',
    'address': '0.0.0.0:1883',
    'started_at': '',
    'uptime': '',
}
broker_info_lock = threading.Lock()
broker_start_time = None

# Server Configuration
config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
        },
    },
    'sys_interval': 10,
    'auth': {
        'allow-anonymous': True,
    },
}

# Topic to read from the broker
READ_TOPICS = [
    ('GX26301250700800004/RandomSlider', 0),
    ('GX26301250700800004/alarm', 0),
    ('GX26301250700800004/NumericValue', 0),
    ('GX26301250700800004/String', 0),
]

# Shared state for the web page
latest_values = {
    'RandomSlider': '',
    'Alarm': '',
    'NumericValue': '',
    'String': '',
}
latest_values_lock = threading.Lock()

HTML_PAGE = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SAMKOON GX-070-32MT-4AI2AO-G</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 60%, #16213e 100%);
    --text: #cdd6f4;
    --card-bg: rgba(49,50,68,0.85);
    --card-border: rgba(255,255,255,0.06);
    --input-bg: #1e1e2e;
    --input-color: #cdd6f4;
    --input-border: #45475a;
    --label-color: #a6adc8;
    --topic-color: #585b70;
    --footer-color: #45475a;
    --subtitle-color: #585b70;
  }
  body.light {
    --bg: linear-gradient(135deg, #e8eaf6 0%, #f3f4f8 60%, #eef1f8 100%);
    --text: #1e1e2e;
    --card-bg: rgba(255,255,255,0.92);
    --card-border: rgba(0,0,0,0.08);
    --input-bg: #f5f6fa;
    --input-color: #1e1e2e;
    --input-border: #b0b8d0;
    --label-color: #4a4f68;
    --topic-color: #8888a0;
    --footer-color: #9090a8;
    --subtitle-color: #7070a0;
  }
  body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    padding: 32px 20px 48px;
    transition: background 0.3s, color 0.3s;
  }
  header {
    text-align: center;
    margin-bottom: 40px;
  }
  header h1 {
    font-size: 2rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    background: linear-gradient(90deg, #cba6f7, #89dceb);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }
  header p {
    margin-top: 6px;
    font-size: 0.8rem;
    color: var(--subtitle-color);
    letter-spacing: 0.04em;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 24px;
    max-width: 1100px;
    margin: 0 auto;
  }
  .card {
    background: var(--card-bg);
    border: 1px solid var(--card-border);
    border-radius: 18px;
    padding: 24px 22px 22px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.45);
    backdrop-filter: blur(6px);
    transition: transform 0.15s ease, box-shadow 0.15s ease;
  }
  .card:hover { transform: translateY(-3px); box-shadow: 0 12px 40px rgba(0,0,0,0.6); }
  .card-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 16px;
  }
  .card-icon {
    width: 34px; height: 34px;
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem;
    flex-shrink: 0;
  }
  .icon-slider  { background: rgba(137,220,235,0.15); }
  .icon-random  { background: rgba(166,227,161,0.15); }
  .icon-alarm   { background: rgba(243,139,168,0.15); }
  .card-label {
    font-size: 0.78rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--label-color);
  }
  .card-topic {
    font-size: 0.65rem;
    color: var(--topic-color);
    margin-top: 2px;
    word-break: break-all;
  }
  .card-value {
    font-size: 2.4rem;
    font-weight: 700;
    letter-spacing: -0.01em;
    min-height: 3rem;
    display: flex;
    align-items: center;
  }
  .val-slider  { color: #89dceb; }
  .val-random  { color: #a6e3a1; }
  .val-alarm   { color: #f38ba8; }
  .val-numeric { color: #cba6f7; }

  /* alarm badge */
  .alarm-badge {
    display: inline-block;
    padding: 4px 14px;
    border-radius: 999px;
    font-size: 1rem;
    font-weight: 700;
    letter-spacing: 0.06em;
  }
  .alarm-on  { background: rgba(243,139,168,0.2); color: #f38ba8; border: 1px solid #f38ba8; }
  .alarm-off { background: rgba(166,227,161,0.15); color: #a6e3a1; border: 1px solid #a6e3a1; }

  /* numeric input card */
  .input-row {
    display: flex;
    gap: 10px;
    margin-top: 16px;
  }
  .input-row input {
    flex: 1;
    padding: 10px 12px;
    border-radius: 10px;
    border: 1px solid var(--input-border);
    background: var(--input-bg);
    color: var(--input-color);
    font-size: 1rem;
    outline: none;
    transition: border-color 0.15s;
  }
  .input-row input:focus { border-color: #cba6f7; }
  .input-row button {
    padding: 10px 18px;
    border-radius: 10px;
    border: none;
    background: linear-gradient(135deg, #cba6f7, #89b4fa);
    color: #1e1e2e;
    font-size: 0.95rem;
    font-weight: 700;
    cursor: pointer;
    transition: opacity 0.15s, transform 0.1s;
  }
  .input-row button:hover { opacity: 0.88; }
  .input-row button:active { transform: scale(0.96); }
  .send-status {
    font-size: 0.72rem;
    margin-top: 8px;
    min-height: 1em;
    color: #a6e3a1;
  }
  footer {
    text-align: center;
    margin-top: 44px;
    font-size: 0.72rem;
    color: var(--footer-color);
    letter-spacing: 0.04em;
  }
  .theme-btn {
    position: fixed; top: 14px; right: 20px; z-index: 1000;
    display: flex; gap: 8px;
  }
  .theme-btn button {
    padding: 6px 16px; border-radius: 20px;
    font-size: 0.82rem; font-weight: 700; cursor: pointer;
    transition: opacity 0.15s, transform 0.1s, box-shadow 0.15s;
    box-shadow: 0 2px 8px rgba(0,0,0,0.4);
  }
  .theme-btn button:hover { opacity: 0.85; transform: scale(0.97); }
  .theme-btn button.active { box-shadow: 0 0 0 2px #89dceb; }
  #btnDark  { background: #313244; color: #89dceb; border: 1px solid #89dceb; }
  #btnLight { background: #e8eaf6; color: #3030a0; border: 1px solid #6060c0; }
  .dot {
    display: inline-block;
    width: 7px; height: 7px;
    border-radius: 50%;
    background: #a6e3a1;
    margin-right: 6px;
    animation: pulse 1.4s infinite;
  }
  @keyframes pulse {
    0%,100% { opacity: 1; } 50% { opacity: 0.3; }
  }
</style>
</head>
<body>
<div class="theme-btn">
  <button id="btnDark"  onclick="setTheme(\'dark\')">&#9790; Dark</button>
  <button id="btnLight" onclick="setTheme(\'light\')">&#9728; Light</button>
</div>
<header>
  <div style="font-size:0.7rem;font-weight:600;letter-spacing:0.18em;text-transform:uppercase;color:var(--subtitle-color);margin-bottom:6px;">MQTT Connection with</div>
  <h1>&#9889; SAMKOON GX-070-32MT-4AI2AO-G</h1>
  <p><span class="dot"></span>Live &bull; auto-refresh every 500 ms</p>
</header>

<div class="grid">

  <!-- Random Slider -->
  <div class="card">
    <div class="card-header">
      <div class="card-icon icon-random">&#128256;</div>
      <div>
        <div class="card-label">Random Slider</div>
        <div class="card-topic">GX26301250700800004/RandomSlider</div>
      </div>
    </div>
    <div class="card-value val-random" id="RandomSlider">&mdash;</div>
    <div class="input-row">
      <input type="number" id="sliderInput" min="0" max="65535" placeholder="0 – 65535">
      <button onclick="sendSlider()">Send</button>
    </div>
    <div class="send-status" id="sliderStatus"></div>
  </div>

  <!-- Alarm -->
  <div class="card">
    <div class="card-header">
      <div class="card-icon icon-alarm">&#128680;</div>
      <div>
        <div class="card-label">Alarm</div>
        <div class="card-topic">GX26301250700800004/alarm</div>
      </div>
    </div>
    <div class="card-value val-alarm" id="Alarm">&mdash;</div>
    <div style="margin-top:16px;display:flex;gap:10px;">
      <button id="alarmOnBtn" onclick="setAlarm(\'1\')" style="flex:1;padding:10px;border-radius:10px;border:1px solid #f38ba8;background:rgba(243,139,168,0.15);color:#f38ba8;font-size:0.95rem;font-weight:700;cursor:pointer;transition:background 0.15s;">&#9888; ON</button>
      <button id="alarmOffBtn" onclick="setAlarm(\'0\')" style="flex:1;padding:10px;border-radius:10px;border:1px solid #a6e3a1;background:rgba(166,227,161,0.15);color:#a6e3a1;font-size:0.95rem;font-weight:700;cursor:pointer;transition:background 0.15s;">&#10003; OFF</button>
    </div>
  </div>

  <!-- Numeric Value -->
  <div class="card">
    <div class="card-header">
      <div class="card-icon icon-slider">&#128290;</div>
      <div>
        <div class="card-label">Numeric Value</div>
        <div class="card-topic">GX26301250700800004/NumericValue</div>
      </div>
    </div>
    <div class="card-value val-slider" id="NumericValue">&mdash;</div>
  </div>

  <!-- String -->
  <div class="card">
    <div class="card-header">
      <div class="card-icon icon-random">&#128172;</div>
      <div>
        <div class="card-label">String</div>
        <div class="card-topic">GX26301250700800004/String</div>
      </div>
    </div>
    <div class="card-value val-numeric" id="String" style="font-size:1.4rem;">&mdash;</div>
  </div>


</div>

<!-- Broker Status + Connected Clients -->
<div style="max-width:1100px;margin:32px auto 0;display:grid;grid-template-columns:1fr 2fr;gap:24px;">

  <!-- Broker Status -->
  <div class="card">
    <div class="card-header" style="margin-bottom:16px;">
      <div class="card-icon" style="background:rgba(203,166,247,0.15);">&#128225;</div>
      <div>
        <div class="card-label">Broker Status</div>
        <div class="card-topic">amqtt &bull; local</div>
      </div>
      <span id="brokerStatusBadge" style="margin-left:auto;border-radius:999px;padding:2px 12px;font-size:0.8rem;font-weight:700;">&#8212;</span>
    </div>
    <div style="display:flex;flex-direction:column;gap:10px;font-size:0.85rem;">
      <div style="display:flex;justify-content:space-between;border-bottom:1px solid rgba(255,255,255,0.05);padding-bottom:8px;">
        <span style="color:#585b70;">Address</span>
        <span id="brokerAddress" style="color:#cba6f7;font-family:monospace;">&#8212;</span>
      </div>
      <div style="display:flex;justify-content:space-between;border-bottom:1px solid rgba(255,255,255,0.05);padding-bottom:8px;">
        <span style="color:#585b70;">Started At</span>
        <span id="brokerStartedAt" style="color:#a6adc8;">&#8212;</span>
      </div>
      <div style="display:flex;justify-content:space-between;border-bottom:1px solid rgba(255,255,255,0.05);padding-bottom:8px;">
        <span style="color:#585b70;">Uptime</span>
        <span id="brokerUptime" style="color:#a6e3a1;font-family:monospace;">&#8212;</span>
      </div>
      <div style="display:flex;justify-content:space-between;">
        <span style="color:#585b70;">Active Connections</span>
        <span id="brokerConnections" style="color:#89dceb;font-weight:700;">&#8212;</span>
      </div>
    </div>
  </div>

  <!-- Connected Clients -->
  <div class="card">
    <div class="card-header" style="margin-bottom:12px;">
      <div class="card-icon icon-slider">&#128101;</div>
      <div>
        <div class="card-label">Connected Clients</div>
        <div class="card-topic">MQTT broker &bull; live</div>
      </div>
      <span id="clientCount" style="margin-left:auto;background:rgba(137,220,235,0.15);color:#89dceb;border-radius:999px;padding:2px 12px;font-size:0.85rem;font-weight:700;">0</span>
    </div>
    <table style="width:100%;border-collapse:collapse;font-size:0.85rem;">
      <thead>
        <tr style="color:#585b70;text-transform:uppercase;letter-spacing:0.06em;font-size:0.72rem;">
          <th style="text-align:left;padding:4px 8px;">Client ID</th>
          <th style="text-align:left;padding:4px 8px;">IP Address</th>
          <th style="text-align:left;padding:4px 8px;">Connected At</th>
          <th style="text-align:left;padding:4px 8px;">Ping</th>
        </tr>
      </thead>
      <tbody id="clientBody"></tbody>
    </table>
    <div id="clientEmpty" style="color:#585b70;font-size:0.82rem;padding:8px 8px 0;">No clients connected</div>
  </div>

</div>

<footer>&copy; MQTT Dashboard &bull; GX26301250700800004</footer>

<script>
  async function refresh() {
    try {
      const r = await fetch('/status');
      const d = await r.json();
      const set = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = (val !== undefined && val !== '') ? val : '\\u2014';
      };
      set('RandomSlider', d.RandomSlider);
      set('NumericValue', d.NumericValue);
      set('String',       d.String);
      // Alarm: render badge
      const alarmEl = document.getElementById('Alarm');
      if (alarmEl) {
        const v = d.Alarm !== undefined ? d.Alarm : '';
        if (v === '' ) { alarmEl.textContent = '\\u2014'; alarmEl.className = 'card-value val-alarm'; }
        else if (v === '1') { alarmEl.innerHTML = '<span class=\\'alarm-badge alarm-on\\'>&#9888; ON</span>'; }
        else                { alarmEl.innerHTML = '<span class=\\'alarm-badge alarm-off\\'>&#10003; OFF</span>'; }
      }
    } catch(e) {}
  }
  refresh();
  refreshClients();
  setInterval(refresh, 500);
  setInterval(refreshClients, 2000);

  function setTheme(t) {
    document.body.classList.toggle('light', t === 'light');
    localStorage.setItem('theme', t);
    document.getElementById('btnDark').classList.toggle('active', t === 'dark');
    document.getElementById('btnLight').classList.toggle('active', t === 'light');
  }
  setTheme(localStorage.getItem('theme') || 'dark');

  async function refreshClients() {
    try {
      const [rc, rb] = await Promise.all([fetch('/clients'), fetch('/broker-status')]);
      const clients = await rc.json();
      const broker = await rb.json();

      // Clients table
      const keys = Object.keys(clients);
      document.getElementById('clientCount').textContent = keys.length;
      const body = document.getElementById('clientBody');
      const empty = document.getElementById('clientEmpty');
      if (keys.length === 0) {
        body.innerHTML = '';
        empty.style.display = '';
      } else {
        empty.style.display = 'none';
        body.innerHTML = keys.map(id => {
          const c = clients[id];
          return `<tr style="border-top:1px solid rgba(255,255,255,0.05);">
            <td style="padding:6px 8px;color:#cba6f7;font-family:monospace;">${id}</td>
            <td style="padding:6px 8px;color:#89dceb;">${c.ip || '\u2014'}</td>
            <td style="padding:6px 8px;color:#a6adc8;">${c.connected_at}</td>
            <td style="padding:6px 8px;color:#a6e3a1;font-family:monospace;">${c.ping || '\u2014'}</td>
          </tr>`;
        }).join('');
      }

      // Broker status
      const badge = document.getElementById('brokerStatusBadge');
      if (broker.status === 'running') {
        badge.textContent = '\u25cf Running';
        badge.style.background = 'rgba(166,227,161,0.15)';
        badge.style.color = '#a6e3a1';
        badge.style.border = '1px solid #a6e3a1';
      } else {
        badge.textContent = '\u25cf ' + broker.status;
        badge.style.background = 'rgba(243,139,168,0.15)';
        badge.style.color = '#f38ba8';
        badge.style.border = '1px solid #f38ba8';
      }
      document.getElementById('brokerAddress').textContent = broker.address || '\u2014';
      document.getElementById('brokerStartedAt').textContent = broker.started_at || '\u2014';
      document.getElementById('brokerUptime').textContent = broker.uptime || '\u2014';
      document.getElementById('brokerConnections').textContent = broker.active_connections ?? '\u2014';
    } catch(e) {}
  }

  async function setAlarm(val) {
    try {
      await fetch('/publish-alarm', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({value: val})
      });
    } catch(e) {}
  }

  async function sendSlider() {
    const val = parseInt(document.getElementById('sliderInput').value);
    const status = document.getElementById('sliderStatus');
    if (isNaN(val) || val < 0 || val > 65535) {
      status.style.color = '#f38ba8';
      status.textContent = 'Enter a value between 0 and 65535';
      return;
    }
    try {
      const r = await fetch('/publish', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({topic: 'GX26301250700800004/RandomSlider', value: val})
      });
      if (r.ok) {
        status.style.color = '#a6e3a1';
        status.textContent = '\u2713 Sent: ' + val;
      } else {
        status.style.color = '#f38ba8';
        status.textContent = 'Send failed';
      }
    } catch(e) {
      status.style.color = '#f38ba8';
      status.textContent = 'Error: ' + e;
    }
  }
</script>
</body>
</html>'''

class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if self.path == '/' or self.path == '/index.html':
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(HTML_PAGE.encode('utf-8'))
            elif self.path == '/status':
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                with latest_values_lock:
                    self.wfile.write(json.dumps(latest_values).encode('utf-8'))
            elif self.path == '/clients':
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                with connected_clients_lock:
                    self.wfile.write(json.dumps(connected_clients).encode('utf-8'))
            elif self.path == '/broker-status':
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                with broker_info_lock:
                    self.wfile.write(json.dumps(broker_info).encode('utf-8'))
            else:
                self.send_response(404)
                self.end_headers()
        except Exception:
            pass

    def do_POST(self):
        if self.path == '/publish-alarm':
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
                val = data.get('value', '0')
                if mqtt_client is not None:
                    payload = json.dumps({'alarm': str(val)})
                    mqtt_client.publish(ALARM_TOPIC, payload, qos=0, retain=False)
                    self.send_response(200)
                else:
                    self.send_response(503)
            except Exception:
                self.send_response(500)
            self.end_headers()
        elif self.path == '/publish':
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
                topic = data.get('topic', '')
                value = data.get('value')
                if topic and value is not None and mqtt_client is not None:
                    payload = json.dumps({'RandomSlider': int(value)})
                    mqtt_client.publish(topic, payload, qos=0, retain=False)
                    self.send_response(200)
                else:
                    self.send_response(400)
            except Exception:
                self.send_response(500)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return
    
def start_http_server(host='0.0.0.0', port=8080):
    server = ThreadingHTTPServer((host, port), RequestHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f'HTTP dashboard running on http://{host}:{port}')
    return server

def on_connect(client, userdata, flags, reason_code, properties):
    if not reason_code.is_failure:
        print('Paho MQTT connected to local broker')
        client.subscribe(READ_TOPICS)
        print(f"Subscribed to: {[topic for topic, _ in READ_TOPICS]}")
    else:
        print(f'Paho MQTT connection failed: {reason_code}')

def on_disconnect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f'MQTT disconnected unexpectedly ({reason_code}), reconnecting...')
    else:
        print('MQTT disconnected cleanly')


def on_message(client, userdata, msg):    
    topic = msg.topic
    payload = msg.payload.decode('utf-8', errors='ignore')
    random_slider_value = None
    if topic == NUMERIC_VALUE_TOPIC:
        try:
            data = json.loads(payload)
            numeric_value = data.get('NumericValue')
        except Exception:
            numeric_value = payload.strip()
        with latest_values_lock:
            if numeric_value is not None:
                latest_values['NumericValue'] = html.escape(str(numeric_value))
        print(f"[MQTT] {topic}: {payload}")
        return
    elif topic == STRING_TOPIC:
        try:
            data = json.loads(payload)
            string_value = data.get('String', payload.strip())
        except Exception:
            string_value = payload.strip()
        with latest_values_lock:
            latest_values['String'] = html.escape(str(string_value))
        print(f"[MQTT] {topic}: {payload}")
        return
    elif topic == ALARM_TOPIC:
        try:
            data = json.loads(payload)
            alarm_val = data.get('alarm')
        except Exception:
            alarm_val = payload.strip()
        with latest_values_lock:
            if alarm_val is not None:
                latest_values['Alarm'] = html.escape(str(alarm_val))
        print(f"[MQTT] {topic}: {payload}")
        return
    elif topic == RANDOM_SLIDER_TOPIC:
        try:
            data = json.loads(payload)
            random_slider_value = data.get('RandomSlider')
        except Exception:
            random_slider_value = None
    with latest_values_lock:
        if random_slider_value is not None:
            latest_values['RandomSlider'] = html.escape(str(random_slider_value))
    print(f"[MQTT] {topic}: {payload}")

def start_paho_reader():
    global mqtt_client
    while True:
        try:
            mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, protocol=MQTT_PROTOCOL)
            mqtt_client.on_connect = on_connect
            mqtt_client.on_disconnect = on_disconnect
            mqtt_client.on_message = on_message
            mqtt_client.reconnect_delay_set(min_delay=1, max_delay=10)
            # Wait for broker to be ready before connecting
            while True:
                try:
                    mqtt_client.connect('127.0.0.1', 1883, keepalive=10)
                    break
                except Exception as e:
                    print(f'[MQTT] Waiting for broker: {e}, retrying in 1s...')
                    time.sleep(1)
            print('[MQTT] Starting loop...')
            mqtt_client.loop_forever(retry_first_connection=True)
            print('[MQTT] loop_forever exited, restarting in 3s...')
        except Exception as e:
            print(f'[MQTT] Fatal error in paho loop: {e}, restarting in 3s...')
        time.sleep(3)

def ping_ip(ip):
    """Ping an IP once and return round-trip ms as string, or '--' on failure."""
    if not ip or ip in ('127.0.0.1', '::1'):
        return 'local'
    try:
        result = subprocess.run(
            ['ping', '-n', '1', '-w', '1000', ip],
            capture_output=True, text=True, timeout=2
        )
        for line in result.stdout.splitlines():
            if 'ms' in line and ('Average' in line or 'Minimum' in line or 'time=' in line or 'time<' in line):
                m = re.search(r'time[<=](\d+)ms', line)
                if m:
                    return f'{m.group(1)} ms'
                m = re.search(r'Average = (\d+)ms', line)
                if m:
                    return f'{m.group(1)} ms'
        return 'timeout'
    except Exception:
        return '--'

async def poll_clients(broker):
    """Periodically sync connected_clients and broker_info from the broker."""
    loop = asyncio.get_running_loop()
    while True:
        try:
            snapshot = {}
            for client_id, (session, _handler) in list(broker.sessions.items()):
                ip = str(session.remote_address) if session.remote_address else ''
                with connected_clients_lock:
                    existing = connected_clients.get(client_id, {})
                ping_ms = await loop.run_in_executor(None, ping_ip, ip)
                snapshot[client_id] = {
                    'ip': ip,
                    'connected_at': existing.get('connected_at', datetime.datetime.now().strftime('%H:%M:%S')),
                    'ping': ping_ms,
                }
            with connected_clients_lock:
                connected_clients.clear()
                connected_clients.update(snapshot)
            # Update broker info
            uptime = ''
            if broker_start_time:
                delta = datetime.datetime.now() - broker_start_time
                h, rem = divmod(int(delta.total_seconds()), 3600)
                m, s = divmod(rem, 60)
                uptime = f'{h:02d}:{m:02d}:{s:02d}'
            with broker_info_lock:
                broker_info['status'] = 'running'
                broker_info['uptime'] = uptime
                broker_info['active_connections'] = len(snapshot)
        except Exception as e:
            print(f'[poll_clients] error: {e}')
        await asyncio.sleep(2)
async def run_server():
    global broker_start_time
    # Initialize the Broker object with the config above
    broker = Broker(config)
    try:
        # Start the broker
        await broker.start()
        broker_start_time = datetime.datetime.now()
        with broker_info_lock:
            broker_info['status'] = 'running'
            broker_info['started_at'] = broker_start_time.strftime('%Y-%m-%d %H:%M:%S')
        print("MQTT Broker is running...")
        # Start the HTTP dashboard
        start_http_server(port=8080)
        # Give the broker's event loop a moment to fully settle before paho connects
        await asyncio.sleep(0.5)
        threading.Thread(target=start_paho_reader, daemon=True).start()
        asyncio.ensure_future(poll_clients(broker))
        await asyncio.Event().wait()
    except Exception as e:
        print(f"Server error: {e}")
        raise
    finally:
        await broker.shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("Server stopped by user.")
