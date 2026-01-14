## Portfolio Valuator (BNP) – MVP

Dieses Projekt ist ein MVP, um **Portfolios** (ISIN + Stückzahl + Einstandskurs) zu speichern und über **BNP Push / Lightstreamer** per **Bid-Quote** zu bewerten.

### Start (lokal)

```bash
python3 -m pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Danach im Browser öffnen: `http://localhost:8000/`

### Docker (persistente SQLite)

1. Beispiel-Config kopieren:

```bash
cp config/app.env.example config/app.env
```

2. Starten:

```bash
docker compose up --build
```

Die SQLite liegt dann persistent in `./data/app.db` (als Volume nach `/app/data` gemountet).

### Seiten

- Dashboard (Auto-Bewertung): `http://localhost:8000/`
- Portfolio-Pflege (ohne Auto-Refresh): `http://localhost:8000/manage`

### Watchlist (Dashboard)

Im Dashboard kannst du zusätzlich zu Portfolios eine **Watchlist** pflegen (ISIN + optionales Label). Diese Werte werden über den gleichen Lightstreamer-Stream live aktualisiert.

### Persistenz (SQLite)

- Default DB-Pfad: `data/app.db`
- Override via ENV: `DB_PATH=/pfad/zur/app.db`

### BNP / Lightstreamer (ENV)

Die Defaults sind auf die bisherige BNP-Lightstreamer-Konfiguration ausgelegt. Bei Bedarf per ENV anpassen:

- `LS_WSS_URL` (default: `wss://push.bnpparibas.com/lightstreamer`)
- `LS_ADAPTER_SET` (default: `SmarthouseFeed`)
- `LS_DATA_ADAPTER` (default: `MDS5`)
- `LS_ITEM_TEMPLATE` (default: `X0000010800{isin}`)
- `LS_ORIGIN` (default: `https://derivate.bnpparibas.com`, leer setzen zum Deaktivieren)
- `LS_BID_TIMEOUT` (default: `8`)

### MQTT → Home Assistant (Auto-Detect)

Wenn `MQTT_HOST` gesetzt ist, kannst du MQTT im Webinterface (Portfolio-Pflege) **aktivieren**. Default ist **aus**.

- **Discovery Topic**: `homeassistant/sensor/<MQTT_NODE_ID>/<MQTT_OBJECT_ID>/config`
- **State**: `total_market_value` (numerisch, kurz)
- **Attributes**: JSON mit `portfolios`, `watchlist`, `totals`, `updated_at`

Wichtige ENV-Variablen (siehe `config/app.env.example`):
- `MQTT_HOST`, `MQTT_PORT`, optional `MQTT_USERNAME`, `MQTT_PASSWORD`
- `MQTT_NODE_ID`, `MQTT_OBJECT_ID`, `MQTT_BASE_TOPIC`
- `MQTT_DEBOUNCE_MS` (Default 1000ms)
