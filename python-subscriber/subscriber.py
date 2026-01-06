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
        device_id, ts, temp_c, hum_pct,
        temp_zscore_anomaly, temp_ewma_anomaly, temp_adaptive_threshold_anomaly,
        hum_zscore_anomaly, hum_ewma_anomaly, hum_adaptive_threshold_anomaly,
        event, window_before, raw_payload
    ) VALUES (
        %(device_id)s,
        COALESCE(%(ts)s::timestamptz, NOW()),
        %(temp_c)s, %(hum_pct)s,
        %(temp_zscore_anomaly)s, %(temp_ewma_anomaly)s, %(temp_adaptive_threshold_anomaly)s,
        %(hum_zscore_anomaly)s, %(hum_ewma_anomaly)s, %(hum_adaptive_threshold_anomaly)s,
        %(event)s, %(window_before)s, %(raw_payload)s
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

def insert_measurements(conn, rows):
    """Insert multiple measurements, reconnecting if needed."""
    for row in rows:
        conn = insert_measurement(conn, row)
    return conn

# --- Payload helpers -----------------------------------------------------------

def to_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "t", "yes", "y", "1", "on"):
            return True
        if normalized in ("false", "f", "no", "n", "0", "off"):
            return False
    return None

def normalize_ts(value):
    if isinstance(value, (int, float)):
        try:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(value)))
        except Exception:
            return None
    if isinstance(value, (list, tuple)) and len(value) >= 6:
        try:
            ts_parts = list(value[:6])
            ts_parts.extend([0, 0, -1])
            return time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(time.mktime(tuple(ts_parts))),
            )
        except Exception:
            return None
    if isinstance(value, str):
        return value
    return None

def normalize_measurement(entry):
    if not isinstance(entry, dict):
        return None
    return {
        "ts": normalize_ts(entry.get("ts")),
        "temp_c": entry.get("temp_c"),
        "hum_pct": entry.get("hum_pct"),
        "temp_zscore_anomaly": to_bool(entry.get("temp_zscore_anomaly")),
        "temp_ewma_anomaly": to_bool(entry.get("temp_ewma_anomaly")),
        "temp_adaptive_threshold_anomaly": to_bool(entry.get("temp_adaptive_threshold_anomaly")),
        "hum_zscore_anomaly": to_bool(entry.get("hum_zscore_anomaly")),
        "hum_ewma_anomaly": to_bool(entry.get("hum_ewma_anomaly")),
        "hum_adaptive_threshold_anomaly": to_bool(entry.get("hum_adaptive_threshold_anomaly")),
    }

def normalize_window_before(raw_window):
    if not isinstance(raw_window, list):
        return None
    cleaned = []
    for entry in raw_window[:15]:
        normalized = normalize_measurement(entry)
        if not normalized or not normalized.get("ts"):
            continue
        cleaned.append(normalized)
    return cleaned or None

def build_row(device_id, measurement, event=None, window_before=None, raw_payload=None):
    return {
        "device_id": device_id,
        "ts": measurement.get("ts"),
        "temp_c": measurement.get("temp_c"),
        "hum_pct": measurement.get("hum_pct"),
        "temp_zscore_anomaly": measurement.get("temp_zscore_anomaly"),
        "temp_ewma_anomaly": measurement.get("temp_ewma_anomaly"),
        "temp_adaptive_threshold_anomaly": measurement.get("temp_adaptive_threshold_anomaly"),
        "hum_zscore_anomaly": measurement.get("hum_zscore_anomaly"),
        "hum_ewma_anomaly": measurement.get("hum_ewma_anomaly"),
        "hum_adaptive_threshold_anomaly": measurement.get("hum_adaptive_threshold_anomaly"),
        "event": event,
        "window_before": psycopg2.extras.Json(window_before) if window_before is not None else None,
        "raw_payload": psycopg2.extras.Json(raw_payload) if raw_payload is not None else None,
    }

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

    measurement = normalize_measurement(data) or {}

    event = data.get("event")
    if isinstance(event, str):
        event = event.strip() or None
    elif event is not None:
        event = str(event)

    window_before = None
    if event == "anomaly":
        window_before = normalize_window_before(data.get("window_before"))

    rows = []
    rows.append(build_row(dev, measurement, event=event, window_before=window_before, raw_payload=data))
    if window_before:
        for entry in window_before:
            entry_payload = dict(entry)
            entry_payload["window_before"] = True
            entry_payload["parent_event"] = event
            rows.append(build_row(dev, entry, raw_payload=entry_payload))

    # Insert data and update connection if reconnected
    new_conn = insert_measurements(conn, rows)
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
