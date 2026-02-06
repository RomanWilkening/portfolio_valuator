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

### Banking Bridge Integration

Der Portfolio Valuator kann Depots aus einer laufenden [Banking Bridge](https://github.com/RomanWilkening/banking_bridge_3) Instanz importieren und synchronisieren. Die Banking Bridge ruft Wertpapierbestaende ueber FinTS ab.

**Einrichtung:**

1. Banking Bridge URL unter **Einstellungen** konfigurieren (z.B. `http://192.168.1.100:8080`).
2. Verbindung testen und verfuegbare Depots laden.
3. Depot als neues Portfolio importieren (erstellt automatisch Instrumente + Kursquellen).
4. Jederzeit mit **Synchronisieren** aktualisieren (neue Holdings werden hinzugefuegt, entfernte geloescht, Mengen/Entry aktualisiert).

**Oder via ENV (Initial-Default):**

```env
BANKING_BRIDGE_URL=http://192.168.1.100:8080
```

**API-Endpunkte:**

- `GET /api/settings/banking-bridge` – Banking Bridge URL lesen
- `PUT /api/settings/banking-bridge` – Banking Bridge URL setzen
- `GET /api/banking-bridge/status` – Verbindungsstatus
- `GET /api/banking-bridge/depots` – Verfuegbare Depots (inkl. Verknuepfungsstatus)
- `GET /api/banking-bridge/depots/{id}/holdings` – Holdings eines Depots
- `POST /api/banking-bridge/import/{depot_id}` – Depot als Portfolio importieren
- `POST /api/banking-bridge/sync/{portfolio_id}` – Verknuepftes Portfolio synchronisieren
- `POST /api/banking-bridge/link/{portfolio_id}/{depot_id}` – Portfolio manuell verknuepfen
- `POST /api/banking-bridge/unlink/{portfolio_id}` – Verknuepfung aufheben

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
