import os
import re
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

        CREATE TABLE IF NOT EXISTS portfolios (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          currency TEXT NOT NULL DEFAULT 'EUR',
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

    _add_column("instruments", "isin", "isin TEXT")
    _add_column("instruments", "ls_item", "ls_item TEXT")
    _add_column("positions", "instrument_id", "instrument_id INTEGER")
    _add_column("positions", "name", "name TEXT")
    _add_column("watchlist", "instrument_id", "instrument_id INTEGER")

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

    def _is_isin(value: str) -> bool:
        return bool(re.fullmatch(r"[A-Z0-9]{12}", (value or "").strip().upper()))

    def _is_ls_item(value: str) -> bool:
        return bool(re.fullmatch(r"X[0-9A-Z]{6,32}", (value or "").strip().upper()))

    def _get_or_create_instrument(code: str, *, name: str, currency: str, isin: Optional[str], ls_item: Optional[str]) -> int:
        cur = conn.execute("SELECT id, name, currency, isin, ls_item FROM instruments WHERE code=?", (code,))
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
            "INSERT INTO instruments(code, name, currency, isin, ls_item) VALUES (?,?,?,?,?)",
            (code, name, (currency or "EUR").strip().upper(), isin, ls_item),
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

    conn.commit()
