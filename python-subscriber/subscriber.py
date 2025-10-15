import json
import os
import time
import logging
import psycopg2
import psycopg2.extras
import paho.mqtt.client as mqtt

# --- Configuration -------------------------------------------------------------

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
    "password": os.environ.get("PGPASSWORD", "sensors"),
}

INSERT_SQL = """
    INSERT INTO measurements (
        device_id, ts, temp_c, hum_pct, raw_payload
    ) VALUES (
        %(device_id)s,
        COALESCE(%(ts)s::timestamptz, NOW()),
        %(temp_c)s, %(hum_pct)s, %(raw_payload)s
    );
"""

# --- Logging setup -------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# --- Database helpers ----------------------------------------------------------

def pg_connect():
    """Try to connect to Postgres, retry until success."""
    while True:
        try:
            conn = psycopg2.connect(**PG_CONN_INFO)
            conn.autocommit = True
            logging.info("Connected to Postgres")
            return conn
        except Exception as e:
            logging.warning(f"Postgres connection failed: {e}")
            time.sleep(3)


def insert_measurement(conn, row):
    """Insert a parsed sensor measurement into Postgres."""
    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, row)
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        logging.warning("Postgres connection lost, reconnecting...")
        conn = pg_connect()
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, row)
    return conn  # may be a new connection

# --- MQTT callbacks ------------------------------------------------------------

def on_connect(client, userdata, flags, reason_code, properties=None):
    logging.info(f"Connected to MQTT broker (code {reason_code})")
    client.subscribe(MQTT_TOPIC)
    logging.info(f"Subscribed to topic: {MQTT_TOPIC}")


def on_message(client, userdata, msg):
    conn = userdata["pg_conn"]
    payload_txt = msg.payload.decode("utf-8", errors="ignore")

    try:
        data = json.loads(payload_txt)
    except Exception:
        logging.warning(f"Non-JSON message on {msg.topic}: {payload_txt[:100]}")
        return

    # Normalize fields
    dev = data.get("device_id") or data.get("device") or "unknown"

    # Normalize timestamp (epoch or ISO)
    ts_val = None
    ts = data.get("ts")
    if isinstance(ts, (int, float)):
        try:
            ts_val = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts)))
        except Exception:
            ts_val = None
    elif isinstance(ts, str):
        ts_val = ts

    # Prepare DB row
    row = {
        "device_id": dev,
        "ts": ts_val,
        "temp_c": data.get("temp_c"),
        "hum_pct": data.get("hum_pct"),
        "cpu_total_pct": data.get("cpu_total_pct"),
        "cpu_mp_pct": data.get("cpu_mp_pct"),
        "core0_cpu_pct": data.get("cpu_core0_pct"),
        "core1_cpu_pct": data.get("cpu_core1_pct"),
        "mp_used_kb": data.get("mp_used_kb"),
        "mp_total_kb": data.get("mp_total_kb"),
        "idf_free_kb": data.get("idf_free_kb"),
        "raw_payload": psycopg2.extras.Json(data),
    }

    # Insert data and update connection if reconnected
    new_conn = insert_measurement(conn, row)
    if new_conn != conn:
        userdata["pg_conn"] = new_conn

    logging.info(f"Inserted measurement from {dev} on topic {msg.topic}")

# --- Main loop ----------------------------------------------------------------

def main():
    logging.info(f"Starting subscriber for topic '{MQTT_TOPIC}'")

    conn = pg_connect()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata={"pg_conn": conn})

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

    client.on_connect = on_connect
    client.on_message = on_message

    # Robust MQTT connection loop
    while True:
        try:
            logging.info(f"Connecting to MQTT {MQTT_HOST}:{MQTT_PORT}")
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever(retry_first_connection=True)
        except Exception as e:
            logging.warning(f"MQTT connection error: {e}")
            time.sleep(3)


if __name__ == "__main__":
    main()
