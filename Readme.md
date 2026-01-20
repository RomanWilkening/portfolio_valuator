## Portfolio Valuator

Dieses Projekt verwaltet **Portfolios** auf Basis von **Instrumenten** und bewertet sie ueber **priorisierte Kursquellen** (z.B. BNP Lightstreamer, Tradegate, Bitfinex).

### Start (lokal)

```bash
python3 -m pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Danach im Browser oeffnen: `http://localhost:8000/`

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

- Uebersicht: `http://localhost:8000/`
- Instrumente: `http://localhost:8000/instruments`
- Kursquellen: `http://localhost:8000/sources`
- Watchlist: `http://localhost:8000/watchlist`
- Positionen: `http://localhost:8000/positions`
- Einstellungen: `http://localhost:8000/settings`

### Datenmodell

- **Instrumente** enthalten Basisdaten (Code/Name/Waehrung, optional ISIN und LS Item).
- **FX-Kurse** sind eigene Instrumente (Base/Quote) in `fx_rates` und dienen nur der Umrechnung.
- **Positionen** verknuepfen Instrumente mit Portfolios (Menge, Entry, Positionsname).
- **Watchlist** verknuepft Instrumente fuer Live-Kurse.
- Instrument-Codes muessen **nicht** ISINs sein.

### Kursquellen & Prioritaet

- Kursquellen werden pro Instrument/FX gepflegt (Quelle + Source-ID).
- Reihenfolge = Prioritaet. Pro Titel wird die erste Quelle mit Kurs verwendet.
- Reihenfolge pro Instrument/FX wird per Drag&Drop festgelegt.
- Lightstreamer nutzt das Item-Template aus **Kursquellen** oder ein explizites `LS Item` am Instrument.
- Tradegate nutzt die ISIN am Instrument (falls gesetzt).

### Sortierung

- Portfolios, Positionen und Watchlist lassen sich per Drag&Drop sortieren.
- Die Reihenfolge wird persistent gespeichert.

### Einstellungen (MQTT)

MQTT-Konfiguration und Aktivierung erfolgen im Tab **Einstellungen**.

Es werden **mehrere Sensoren** per Auto-Discovery angelegt:
- pro **Portfolio**: Wert/Basis/Performance/Performance%
- pro **Position**: Stueck/Kurs/Basis/Wert/Performance/Performance%
- pro **Watchlist-Eintrag**: Kurs

Waehrungswerte werden als **`device_class: monetary`** mit **`unit_of_measurement`** (z.B. EUR/USD) publiziert.

### Persistenz

- Default DB-Pfad: `data/app.db`
- Override via ENV: `DB_PATH=/pfad/zur/app.db`
- Konfigurationen werden in der Datenbank gespeichert; ENV dient nur als Initial-Default.
