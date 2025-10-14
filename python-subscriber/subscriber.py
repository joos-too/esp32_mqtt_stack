import json
import os
import time
import psycopg2
import psycopg2.extras
import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "sensors/esp32/#")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")

PG_CONN_INFO = {
    "host": os.environ.get("PGHOST", "postgres"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "sensors"),
    "user": os.environ.get("PGUSER", "sensors"),
    "password": os.environ.get("PGPASSWORD", "sensors")
}

def pg_connect():
    while True:
        try:
            conn = psycopg2.connect(**PG_CONN_INFO)
            conn.autocommit = True
            print("Postgres connected")
            return conn
        except Exception as e:
            print(f"Postgres connection failed: {e}")
            time.sleep(3)

pg_conn = pg_connect()

INSERT_SQL = (
    """
    INSERT INTO measurements (
      device_id, ts, temp_c, hum_pct, cpu_total_pct, cpu_mp_pct,
      core0_cpu_pct, core1_cpu_pct, mp_used_kb, mp_total_kb, idf_free_kb, raw_payload
    ) VALUES (
      %(device_id)s,
      COALESCE(%(ts)s::timestamptz, NOW()),
      %(temp_c)s, %(hum_pct)s, %(cpu_total_pct)s, %(cpu_mp_pct)s,
      %(core0_cpu_pct)s, %(core1_cpu_pct)s, %(mp_used_kb)s, %(mp_total_kb)s, %(idf_free_kb)s,
      %(raw_payload)s
    )
    """
)

# --- MQTT callbacks ---

def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"Connected to MQTT with result: {reason_code}")
    client.subscribe(MQTT_TOPIC)
    print(f"Subscribed: {MQTT_TOPIC}")


def on_message(client, userdata, msg):
    payload_txt = msg.payload.decode("utf-8", errors="ignore")
    try:
        data = json.loads(payload_txt)
    except Exception:
        print(f"Non-JSON message on {msg.topic}: {payload_txt[:120]}")
        return

    # Normalize fields
    dev = data.get("device_id") or data.get("device") or "unknown"

    # Accept either ISO timestamp string or epoch seconds under key "ts".
    ts_val = None
    if isinstance(data.get("ts"), (int, float)):
        # let Postgres parse epoch
        ts_val = f"to_timestamp({float(data['ts'])})"
        # We'll pass as string only when using string substitution; but here we use parameters.
        # Simpler: just pass ISO, so convert epoch to ISO in Python:
        try:
            ts_val = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(data["ts"])))
        except Exception:
            ts_val = None
    elif isinstance(data.get("ts"), str):
        ts_val = data["ts"]

    row = {
        "device_id": dev,
        "ts": ts_val,  # may be None → defaults to NOW()
        "temp_c": data.get("temp_c"),
        "hum_pct": data.get("hum_pct"),
        "cpu_total_pct": data.get("cpu_total_pct"),
        "cpu_mp_pct": data.get("cpu_mp_pct"),
        "core0_cpu_pct": data.get("cpu_core0_pct"),
        "core1_cpu_pct": data.get("cpu_core1_pct"),
        "mp_used_kb": data.get("mp_used_kb"),
        "mp_total_kb": data.get("mp_total_kb"),
        "idf_free_kb": data.get("idf_free_kb"),
        "raw_payload": psycopg2.extras.Json(data)
    }

    try:
        with pg_conn.cursor() as cur:
            cur.execute(INSERT_SQL, row)
        print(f"Inserted from {dev} @ topic {msg.topic}")
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        # reconnect DB and retry once
        global pg_conn
        pg_conn = pg_connect()
        with pg_conn.cursor() as cur:
            cur.execute(INSERT_SQL, row)
        print(f"Inserted after DB reconnect from {dev}")
    except Exception as e:
        print(f"Failed to insert: {e}")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

client.on_connect = on_connect
client.on_message = on_message

# robust connect loop
while True:
    try:
        print(f"Connecting to MQTT {MQTT_HOST}:{MQTT_PORT}")
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_forever(retry_first_connection=True)
    except Exception as e:
        print(f"MQTT connection error: {e}")
        time.sleep(3)