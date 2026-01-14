import os
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
        CREATE TABLE IF NOT EXISTS portfolios (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          currency TEXT NOT NULL DEFAULT 'EUR',
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS positions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          portfolio_id INTEGER NOT NULL,
          isin TEXT NOT NULL,
          quantity REAL NOT NULL,
          entry_price REAL NOT NULL,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE,
          UNIQUE (portfolio_id, isin)
        );

        CREATE TABLE IF NOT EXISTS watchlist (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          label TEXT,
          isin TEXT NOT NULL UNIQUE,
          currency TEXT NOT NULL DEFAULT 'EUR',
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS app_settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )
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
    conn.commit()
