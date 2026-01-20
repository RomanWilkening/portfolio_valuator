import os
import re
import socket
import sqlite3
from pathlib import Path
from typing import Optional


def get_db_path() -> str:
    # Default: ./data/app.db (relativ zum Repo-Root)
    return os.getenv("DB_PATH", "data/app.db")


def connect_db(path: Optional[str] = None) -> sqlite3.Connection:
    db_path = path or get_db_path()
    p = Path(db_path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS instruments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL,
          currency TEXT NOT NULL DEFAULT 'EUR',
          isin TEXT,
          ls_item TEXT,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS instrument_sources (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          instrument_id INTEGER NOT NULL,
          source TEXT NOT NULL,
          source_code TEXT NOT NULL,
          priority INTEGER NOT NULL DEFAULT 100,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          FOREIGN KEY (instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
          UNIQUE (instrument_id, source, source_code)
        );

        CREATE TABLE IF NOT EXISTS fx_rates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL,
          base_currency TEXT NOT NULL,
          quote_currency TEXT NOT NULL,
          isin TEXT,
          ls_item TEXT,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS fx_rate_sources (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          fx_rate_id INTEGER NOT NULL,
          source TEXT NOT NULL,
          source_code TEXT NOT NULL,
          priority INTEGER NOT NULL DEFAULT 100,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          FOREIGN KEY (fx_rate_id) REFERENCES fx_rates(id) ON DELETE CASCADE,
          UNIQUE (fx_rate_id, source, source_code)
        );

        CREATE TABLE IF NOT EXISTS portfolios (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          currency TEXT NOT NULL DEFAULT 'EUR',
          sort_order INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS positions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          portfolio_id INTEGER NOT NULL,
          instrument_id INTEGER,
          isin TEXT NOT NULL,
          name TEXT,
          quantity REAL NOT NULL,
          entry_price REAL NOT NULL,
          currency TEXT NOT NULL DEFAULT 'EUR',
          sort_order INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE,
          FOREIGN KEY (instrument_id) REFERENCES instruments(id) ON DELETE SET NULL,
          UNIQUE (portfolio_id, isin)
        );

        CREATE TABLE IF NOT EXISTS watchlist (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          label TEXT,
          instrument_id INTEGER,
          isin TEXT NOT NULL UNIQUE,
          currency TEXT NOT NULL DEFAULT 'EUR',
          sort_order INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          FOREIGN KEY (instrument_id) REFERENCES instruments(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS app_settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )

    def _cols(table: str) -> set[str]:
        return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    def _add_column(table: str, column: str, ddl: str) -> None:
        if column not in _cols(table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl};")

    _add_column("instruments", "type", "type TEXT NOT NULL DEFAULT 'asset'")
    _add_column("instruments", "base_currency", "base_currency TEXT")
    _add_column("instruments", "quote_currency", "quote_currency TEXT")
    _add_column("instruments", "isin", "isin TEXT")
    _add_column("instruments", "ls_item", "ls_item TEXT")
    _add_column("positions", "instrument_id", "instrument_id INTEGER")
    _add_column("positions", "name", "name TEXT")
    _add_column("watchlist", "instrument_id", "instrument_id INTEGER")
    _add_column("portfolios", "sort_order", "sort_order INTEGER NOT NULL DEFAULT 0")
    _add_column("positions", "sort_order", "sort_order INTEGER NOT NULL DEFAULT 0")
    _add_column("watchlist", "sort_order", "sort_order INTEGER NOT NULL DEFAULT 0")

    # Minimal-"Migrationen" für bestehende DBs (ALTER TABLE wenn Spalte fehlt)
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(portfolios)").fetchall()}
        if "currency" not in cols:
            conn.execute("ALTER TABLE portfolios ADD COLUMN currency TEXT NOT NULL DEFAULT 'EUR';")
    except Exception:
        pass
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(watchlist)").fetchall()}
        if "currency" not in cols:
            conn.execute("ALTER TABLE watchlist ADD COLUMN currency TEXT NOT NULL DEFAULT 'EUR';")
    except Exception:
        pass
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(positions)").fetchall()}
        if "currency" not in cols:
            conn.execute("ALTER TABLE positions ADD COLUMN currency TEXT NOT NULL DEFAULT 'EUR';")
    except Exception:
        pass

    def _get_setting(key: str) -> Optional[str]:
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
        if not row:
            return None
        return str(row["value"])

    def _set_default_setting(key: str, value: Optional[str]) -> None:
        if value is None:
            return
        if _get_setting(key) is not None:
            return
        conn.execute("INSERT INTO app_settings(key, value) VALUES (?, ?)", (key, str(value)))

    def _seed_sort_order(table: str, flag_key: str) -> None:
        if _get_setting(flag_key) is not None:
            return
        try:
            conn.execute(f"UPDATE {table} SET sort_order = id WHERE sort_order IS NULL OR sort_order = 0;")
        except Exception:
            return
        _set_default_setting(flag_key, "true")

    _seed_sort_order("portfolios", "portfolios_sort_seeded")
    _seed_sort_order("positions", "positions_sort_seeded")
    _seed_sort_order("watchlist", "watchlist_sort_seeded")

    ls_user_agent_default = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    _set_default_setting("ls_wss_url", (os.getenv("LS_WSS_URL") or "").strip() or "wss://push.bnpparibas.com/lightstreamer")
    _set_default_setting("ls_subprotocol", (os.getenv("LS_SUBPROTOCOL") or "").strip() or "TLCP-2.5.0.lightstreamer.com")
    _set_default_setting("ls_adapter_set", (os.getenv("LS_ADAPTER_SET") or "").strip() or "SmarthouseFeed")
    _set_default_setting("ls_data_adapter", (os.getenv("LS_DATA_ADAPTER") or "").strip() or "MDS5")
    _set_default_setting("ls_cid", (os.getenv("LS_CID") or "").strip() or "pcYgxn8m8 feOojyA1V661f3g2.pz482h95IL5h")
    _set_default_setting("ls_item_template", (os.getenv("LS_ITEM_TEMPLATE") or "").strip() or "X0000010800{isin}")
    _set_default_setting("ls_origin", (os.getenv("LS_ORIGIN") or "").strip() or "https://derivate.bnpparibas.com")
    _set_default_setting("ls_user_agent", (os.getenv("LS_USER_AGENT") or "").strip() or ls_user_agent_default)
    _set_default_setting("ls_reconnect_min_s", (os.getenv("LS_RECONNECT_MIN_S") or "").strip() or "1.0")
    _set_default_setting("ls_reconnect_max_s", (os.getenv("LS_RECONNECT_MAX_S") or "").strip() or "30.0")
    _set_default_setting("ls_recv_timeout_s", (os.getenv("LS_RECV_TIMEOUT_S") or "").strip() or "35.0")
    _set_default_setting("ls_stale_restart_s", (os.getenv("LS_STALE_RESTART_S") or "").strip() or "90.0")

    _set_default_setting(
        "quote_source_priority",
        (os.getenv("QUOTE_SOURCE_PRIORITY") or "").strip() or "lightstreamer,tradegate,bitfinex",
    )

    _set_default_setting(
        "tradegate_url_template",
        (os.getenv("TRADEGATE_URL_TEMPLATE") or "").strip() or "https://www.tradegate.de/refresh.php?isin={isin}",
    )
    _set_default_setting("tradegate_poll_s", (os.getenv("TRADEGATE_POLL_S") or "").strip() or "10")
    _set_default_setting("tradegate_timeout_s", (os.getenv("TRADEGATE_TIMEOUT_S") or "").strip() or "5")
    _set_default_setting("tradegate_user_agent", (os.getenv("TRADEGATE_USER_AGENT") or "").strip() or "portfolio-valuator/1.0")

    _set_default_setting("bitfinex_wss_url", (os.getenv("BITFINEX_WSS_URL") or "").strip() or "wss://api-pub.bitfinex.com/ws/2")
    _set_default_setting("bitfinex_reconnect_min_s", (os.getenv("BITFINEX_RECONNECT_MIN_S") or "").strip() or "1.0")
    _set_default_setting("bitfinex_reconnect_max_s", (os.getenv("BITFINEX_RECONNECT_MAX_S") or "").strip() or "30.0")

    node_id_default = (os.getenv("MQTT_NODE_ID") or "").strip() or "portfolio_valuator"
    base_topic_default = (os.getenv("MQTT_BASE_TOPIC") or "").strip() or f"portfolio_valuator/{node_id_default}"
    client_id_default = (os.getenv("MQTT_CLIENT_ID") or "").strip() or f"portfolio-valuator-{socket.gethostname()}"
    _set_default_setting("mqtt_enabled", (os.getenv("MQTT_ENABLED") or "").strip() or "false")
    _set_default_setting("mqtt_host", (os.getenv("MQTT_HOST") or "").strip() or "")
    _set_default_setting("mqtt_port", (os.getenv("MQTT_PORT") or "").strip() or "1883")
    _set_default_setting("mqtt_username", (os.getenv("MQTT_USERNAME") or "").strip() or "")
    _set_default_setting("mqtt_password", (os.getenv("MQTT_PASSWORD") or "").strip() or "")
    _set_default_setting("mqtt_client_id", client_id_default)
    _set_default_setting("mqtt_discovery_prefix", (os.getenv("MQTT_DISCOVERY_PREFIX") or "").strip() or "homeassistant")
    _set_default_setting("mqtt_node_id", node_id_default)
    _set_default_setting("mqtt_base_topic", base_topic_default)
    _set_default_setting("mqtt_qos", (os.getenv("MQTT_QOS") or "").strip() or "0")
    _set_default_setting("mqtt_retain", (os.getenv("MQTT_RETAIN") or "").strip() or "true")
    _set_default_setting("mqtt_debounce_ms", (os.getenv("MQTT_DEBOUNCE_MS") or "").strip() or "0")
    _set_default_setting("mqtt_sanity_skip_zero_price", (os.getenv("MQTT_SANITY_SKIP_ZERO_PRICE") or "").strip() or "true")
    _set_default_setting("mqtt_sanity_max_pct_change", (os.getenv("MQTT_SANITY_MAX_PCT_CHANGE") or "").strip() or "0")
    _set_default_setting(
        "mqtt_sanity_require_price_for_valuation",
        (os.getenv("MQTT_SANITY_REQUIRE_PRICE_FOR_VALUATION") or "").strip() or "true",
    )

    def _is_isin(value: str) -> bool:
        return bool(re.fullmatch(r"[A-Z0-9]{12}", (value or "").strip().upper()))

    def _is_ls_item(value: str) -> bool:
        return bool(re.fullmatch(r"X[0-9A-Z]{6,32}", (value or "").strip().upper()))

    def _get_or_create_instrument(code: str, *, name: str, currency: str, isin: Optional[str], ls_item: Optional[str]) -> int:
        cur = conn.execute("SELECT id, name, currency, isin, ls_item, type FROM instruments WHERE code=?", (code,))
        row = cur.fetchone()
        if row:
            updates = []
            values = []
            if isin and not row["isin"]:
                updates.append("isin=?")
                values.append(isin)
            if ls_item and not row["ls_item"]:
                updates.append("ls_item=?")
                values.append(ls_item)
            if name and (row["name"] or "").strip() == code and name != code:
                updates.append("name=?")
                values.append(name)
            if updates:
                values.append(row["id"])
                conn.execute(f"UPDATE instruments SET {', '.join(updates)} WHERE id=?", tuple(values))
            return int(row["id"])
        conn.execute(
            "INSERT INTO instruments(code, name, currency, type, isin, ls_item) VALUES (?,?,?,?,?,?)",
            (code, name, (currency or "EUR").strip().upper(), "asset", isin, ls_item),
        )
        return int(conn.execute("SELECT id FROM instruments WHERE code=?", (code,)).fetchone()["id"])

    try:
        pos_rows = conn.execute("SELECT id, isin, currency, instrument_id FROM positions").fetchall()
        for row in pos_rows:
            if row["instrument_id"]:
                continue
            code = (row["isin"] or "").strip().upper()
            if not code:
                continue
            isin = code if _is_isin(code) else None
            ls_item = code if _is_ls_item(code) else None
            inst_id = _get_or_create_instrument(code, name=code, currency=row["currency"] or "EUR", isin=isin, ls_item=ls_item)
            conn.execute("UPDATE positions SET instrument_id=?, name=COALESCE(name, '') WHERE id=?", (inst_id, row["id"]))
    except Exception:
        pass

    try:
        watch_rows = conn.execute("SELECT id, isin, label, currency, instrument_id FROM watchlist").fetchall()
        for row in watch_rows:
            if row["instrument_id"]:
                continue
            code = (row["isin"] or "").strip().upper()
            if not code:
                continue
            isin = code if _is_isin(code) else None
            ls_item = code if _is_ls_item(code) else None
            name = (row["label"] or "").strip() or code
            inst_id = _get_or_create_instrument(code, name=name, currency=row["currency"] or "EUR", isin=isin, ls_item=ls_item)
            conn.execute("UPDATE watchlist SET instrument_id=? WHERE id=?", (inst_id, row["id"]))
    except Exception:
        pass

    try:
        conn.execute("UPDATE instruments SET type='asset' WHERE type IS NULL OR type=''")
    except Exception:
        pass

    try:
        fx_rows = conn.execute(
            """
            SELECT id, code, name, base_currency, quote_currency, isin, ls_item
            FROM instruments
            WHERE type='fx' OR (base_currency IS NOT NULL AND quote_currency IS NOT NULL)
            """
        ).fetchall()
        for row in fx_rows:
            base = (row["base_currency"] or "").strip().upper()
            quote = (row["quote_currency"] or "").strip().upper()
            if not base or not quote:
                continue
            exists = conn.execute("SELECT 1 FROM fx_rates WHERE code=?", (row["code"],)).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO fx_rates(code, name, base_currency, quote_currency, isin, ls_item) VALUES (?,?,?,?,?,?)",
                    (
                        row["code"],
                        row["name"] or row["code"],
                        base,
                        quote,
                        row["isin"],
                        row["ls_item"],
                    ),
                )
            used = conn.execute(
                "SELECT 1 FROM positions WHERE instrument_id=? LIMIT 1",
                (row["id"],),
            ).fetchone()
            used_watch = conn.execute(
                "SELECT 1 FROM watchlist WHERE instrument_id=? LIMIT 1",
                (row["id"],),
            ).fetchone()
            if not used and not used_watch:
                conn.execute("DELETE FROM instruments WHERE id=?", (row["id"],))
            else:
                conn.execute(
                    "UPDATE instruments SET type='asset', base_currency=NULL, quote_currency=NULL WHERE id=?",
                    (row["id"],),
                )
    except Exception:
        pass

    def _add_source(table: str, target_id: int, source: str, source_code: str, priority: int) -> None:
        try:
            conn.execute(
                f"INSERT INTO {table}(instrument_id, source, source_code, priority) VALUES (?,?,?,?)"
                if table == "instrument_sources"
                else f"INSERT INTO {table}(fx_rate_id, source, source_code, priority) VALUES (?,?,?,?)",
                (target_id, source, source_code, priority),
            )
        except Exception:
            pass

    def _seed_sources_once() -> None:
        if _get_setting("sources_seeded") is not None:
            return
        has_sources = conn.execute("SELECT 1 FROM instrument_sources LIMIT 1").fetchone()
        has_fx_sources = conn.execute("SELECT 1 FROM fx_rate_sources LIMIT 1").fetchone()
        if has_sources or has_fx_sources:
            _set_default_setting("sources_seeded", "true")
            return
        try:
            instr_rows = conn.execute("SELECT id, code, isin, ls_item FROM instruments").fetchall()
            for row in instr_rows:
                if row["ls_item"]:
                    _add_source("instrument_sources", row["id"], "lightstreamer", row["ls_item"], 10)
                if row["isin"]:
                    _add_source("instrument_sources", row["id"], "lightstreamer", row["isin"], 20)
                    _add_source("instrument_sources", row["id"], "tradegate", row["isin"], 30)
        except Exception:
            pass

        try:
            fx_rows = conn.execute("SELECT id, code, isin, ls_item FROM fx_rates").fetchall()
            for row in fx_rows:
                if row["ls_item"]:
                    _add_source("fx_rate_sources", row["id"], "lightstreamer", row["ls_item"], 10)
                if row["isin"]:
                    _add_source("fx_rate_sources", row["id"], "lightstreamer", row["isin"], 20)
                    _add_source("fx_rate_sources", row["id"], "tradegate", row["isin"], 30)
        except Exception:
            pass
        _set_default_setting("sources_seeded", "true")

    _seed_sources_once()

    conn.commit()
