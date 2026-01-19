## Portfolio Valuator (BNP) – MVP

Dieses Projekt ist ein MVP, um **Portfolios** (ISIN + Stückzahl + Einstandskurs) zu speichern und per **Kursquellen-Prioritaet** (z.B. BNP Lightstreamer, Tradegate) zu bewerten.

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

Im Dashboard kannst du zusätzlich zu Portfolios eine **Watchlist** pflegen (ISIN + optionales Label). Diese Werte werden live ueber die beste verfuegbare Kursquelle aktualisiert.

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

### Kursquellen & Prioritaet

- `QUOTE_SOURCE_PRIORITY` (default: `lightstreamer,tradegate`)
- Reihenfolge = Prioritaet. Pro Titel wird die erste Quelle mit Kurs verwendet.
- ISINs koennen mehrere Quellen haben; X-Items/Indizes bleiben i.d.R. Lightstreamer-only.

### Tradegate (ENV)

- `TRADEGATE_URL_TEMPLATE` (default: `https://www.tradegate.de/refresh.php?isin={isin}`)
- `TRADEGATE_POLL_S` (default: `10`)
- `TRADEGATE_TIMEOUT_S` (default: `5`)
- `TRADEGATE_USER_AGENT` (optional)

### MQTT → Home Assistant (Auto-Detect)

Wenn `MQTT_HOST` gesetzt ist, kannst du MQTT im Webinterface (Portfolio-Pflege) **aktivieren**. Default ist **aus**.

- Es werden **mehrere Sensoren** per Auto-Discovery angelegt:
  - pro **Portfolio**: Wert/Basis/Performance/Performance%
  - pro **Position**: Stück/Kurs/Basis/Wert/Performance/Performance%
  - pro **Watchlist-Eintrag**: Kurs
- Währungswerte werden als **`device_class: monetary`** mit **`unit_of_measurement`** (z.B. EUR/USD) publiziert, damit Home Assistant Historie korrekt führt.

Wichtige ENV-Variablen (siehe `config/app.env.example`):
- `MQTT_HOST`, `MQTT_PORT`, optional `MQTT_USERNAME`, `MQTT_PASSWORD`
- `MQTT_NODE_ID`, `MQTT_BASE_TOPIC`
- `MQTT_DEBOUNCE_MS` (Default 0ms = jeder Push)
