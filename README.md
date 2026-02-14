# MQTT-Stack für Datenvisualisierung

Ein Docker-Compose-Stack, der Sensordaten von ESP32-Geräten entgegennimmt, speichert und visualisiert:

- ESP32 veröffentlicht Messwerte per MQTT (siehe Repository in [Micropython](https://github.com/joos-too/esp32_sensor_data))
- **Mosquitto** (TLS-fähig) nimmt die Nachrichten entgegen
- **python-subscriber** konsumiert MQTT-Nachrichten und schreibt sie in **TimescaleDB/PostgreSQL**
- **Grafana** greift auf die Datenbank zu und stellt Dashboards bereit

```
[ESP32] → MQTT (mosquitto) → MQTT Consumer (python-subscriber) → TimescaleDB → Grafana
```

## Projektstruktur
- `docker-compose.yml` – verbindet alle Services und Umgebungsvariablen
- `mosquitto/` – Broker-Konfiguration, Passwortdatei und Persistenzpfade
- `python-subscriber/` – MQTT→Postgres Ingestor (siehe `subscriber.py`)
- `sql/` – Init-Skripte für TimescaleDB (Tabelle, Retention, Aggregat)
- `grafana/provisioning/` – vordefinierte Datasource und Dashboard

## Wie Docker Compose arbeitet
- **mosquitto**: Exponiert `1883` (LAN) und `8883` (TLS). Zertifikate werden aus `/etc/letsencrypt/...` als Read-Only eingebunden. Persistente Daten/Logs unter `./mosquitto/data` und `./mosquitto/log`.
- **postgres (TimescaleDB)**: Nutzt `pg_data` Volume und führt `./sql/*.sql` beim ersten Start aus.
- **ingestor**: Baut aus `python-subscriber/`, verbindet sich mit Mosquitto und Postgres via Umgebungsvariablen, parst JSON-Nachrichten und schreibt in `measurements`.
- **grafana**: Startet mit vorkonfigurierter Datasource und Dashboard, persistiert unter `grafana_data` und läuft auf Port `3001`.

## Konfiguration (.env)
Lege eine `.env` im Projektverzeichnis an (Compose liest sie automatisch). Mindestbelegung:

```env
POSTGRES_USER=sensors
POSTGRES_PASSWORD=secret
POSTGRES_DB=sensors
PGHOST=postgres
PGPORT=5432
PGDATABASE=sensors
PGUSER=sensors
PGPASSWORD=secret

MQTT_HOST=mosquitto
MQTT_PORT=1883
MQTT_TOPIC=sensors/esp32/#
MQTT_USERNAME=...
MQTT_PASSWORD=...

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=change-me
```

Passe `GF_SERVER_ROOT_URL` in `docker-compose.yml` an, falls Grafana unter einer anderen URL laufen soll. Für TLS auf MQTT-Port 8883 müssen die eingebundenen Let’s-Encrypt-Pfade vorhanden sein oder entfernt werden.

## Stack starten
- Build & Start: `docker compose up -d`
- Logs ansehen: `docker compose logs -f ingestor` (oder `mosquitto`, `grafana`, `postgres`)
- Stack stoppen: `docker compose down`
- Daten behalten: Volumes `pg_data` und `grafana_data` bleiben erhalten; Logs/State für Mosquitto liegen unter `./mosquitto/`

Nach dem Start:
- MQTT: `localhost:1883` (oder `8883` mit TLS) – Benutzer laut `mosquitto/passwordfile`
- Postgres: `localhost:5432` – Tabelle `measurements` wird durch `sql/001_init.sql` erstellt
- Grafana: `http://localhost:3001` (Admin-Creds aus `.env`), vordefiniertes Dashboard unter „Sensors“.
