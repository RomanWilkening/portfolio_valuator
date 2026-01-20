import sqlite3
from typing import Optional


def get_setting(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    cur = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,))
    row = cur.fetchone()
    if not row:
        return default
    return str(row["value"])


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    with conn:
        conn.execute(
            "INSERT INTO app_settings(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def get_bool(conn: sqlite3.Connection, key: str, default: bool = False) -> bool:
    v = get_setting(conn, key, None)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def set_bool(conn: sqlite3.Connection, key: str, value: bool) -> None:
    set_setting(conn, key, "true" if value else "false")


def get_int(conn: sqlite3.Connection, key: str, default: int = 0) -> int:
    v = get_setting(conn, key, None)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def get_float(conn: sqlite3.Connection, key: str, default: float = 0.0) -> float:
    v = get_setting(conn, key, None)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(v)
    except Exception:
        return default
