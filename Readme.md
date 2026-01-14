## Portfolio Valuator (BNP) – MVP

Dieses Projekt ist ein MVP, um **Portfolios** (ISIN + Stückzahl + Einstandskurs) zu speichern und über **BNP Push / Lightstreamer** per **Bid-Quote** zu bewerten.

### Start (lokal)

```bash
python3 -m pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Danach im Browser öffnen: `http://localhost:8000/`

### Seiten

- Dashboard (Auto-Bewertung): `http://localhost:8000/`
- Portfolio-Pflege (ohne Auto-Refresh): `http://localhost:8000/manage`

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
