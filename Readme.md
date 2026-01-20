## Portfolio Valuator (BNP) – MVP

Dieses Projekt ist ein MVP, um **Portfolios** ueber **Instrumente** (Basisdaten) zu verwalten und per **Kursquellen-Prioritaet** (z.B. BNP Lightstreamer, Tradegate) zu bewerten.

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
- Uebersicht (Watchlist + Portfolios): `http://localhost:8000/`
- Instrumente: `http://localhost:8000/instruments`
- Kursquellen: `http://localhost:8000/sources`
- Watchlist: `http://localhost:8000/watchlist`
- Positionen: `http://localhost:8000/positions`
- Einstellungen: `http://localhost:8000/settings`

### Datenmodell (Instrumente)

- **Instrumente** enthalten Basisdaten (Code/Name/Waehrung, optional ISIN und LS Item).
- FX-Kurse sind eigene Instrumente (Base/Quote) in `fx_rates` und werden nur fuer Umrechnung genutzt (kein Portfolio-/Watchlist-Einsatz). Sie werden im Dashboard live angezeigt.
- Fuer Umrechnung wird ein FX-Instrument mit `base_currency = Positionswaehrung` und `quote_currency = Portfoliowaehrung` erwartet (direkt oder invers).
- **Positionen** verknuepfen Instrumente mit Portfolios (Menge, Entry, Positionsname).
- **Watchlist** verknuepft Instrumente fuer Live-Kurse.
- Instrument-Codes muessen **nicht** ISINs sein; fuer Kurse nutze optional ISIN/LS Item.
- Lightstreamer nutzt das Item-Template aus den **Kursquellen-Einstellungen** (oder ein explizites `LS Item` am Instrument).
- Tradegate nutzt die ISIN am Instrument (falls gesetzt).
- Kursquellen werden pro Instrument/FX gepflegt (Quelle + Source-ID + Prioritaet).

### Watchlist (Dashboard)

Im Dashboard kannst du zusaetzlich zu Portfolios eine **Watchlist** pflegen (Instrument + optionales Label). Diese Werte werden live ueber die beste verfuegbare Kursquelle aktualisiert.

### Persistenz (SQLite)

- Default DB-Pfad: `data/app.db`
- Override via ENV: `DB_PATH=/pfad/zur/app.db`

### Kursquellen-Konfiguration

Alle Quell-spezifischen Einstellungen (Lightstreamer, Tradegate, Bitfinex, Default-Prioritaet) sind im Tab **Kursquellen** pflegbar.

- Reihenfolge = Prioritaet. Pro Titel wird die erste Quelle mit Kurs verwendet.
- ISINs koennen mehrere Quellen haben; LS Items koennen beliebige Lightstreamer-Item-IDs sein.

### MQTT → Home Assistant (Auto-Detect)

Die MQTT-Konfiguration liegt in **Einstellungen**. MQTT wird dort aktiviert/deaktiviert.

- Es werden **mehrere Sensoren** per Auto-Discovery angelegt:
  - pro **Portfolio**: Wert/Basis/Performance/Performance%
  - pro **Position**: Stück/Kurs/Basis/Wert/Performance/Performance%
  - pro **Watchlist-Eintrag**: Kurs
- Währungswerte werden als **`device_class: monetary`** mit **`unit_of_measurement`** (z.B. EUR/USD) publiziert, damit Home Assistant Historie korrekt führt.

Hinweis: ENV-Werte werden beim ersten Start als Defaults in die Datenbank uebernommen.
