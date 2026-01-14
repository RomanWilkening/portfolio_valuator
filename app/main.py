import asyncio
import logging
import os
import random
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote

import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from websockets.exceptions import ConnectionClosed

from app.db import connect_db, init_db
from app.mqtt_ha import HomeAssistantMqttPublisher, load_mqtt_settings
from app.settings import get_bool, set_bool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("portfolio-valuator")


# ----------------- Konfiguration (ENV) -----------------

LS_WSS_URL = os.getenv("LS_WSS_URL", "wss://push.bnpparibas.com/lightstreamer")
LS_SUBPROTOCOL = os.getenv("LS_SUBPROTOCOL", "TLCP-2.5.0.lightstreamer.com")

LS_ADAPTER_SET = os.getenv("LS_ADAPTER_SET", "SmarthouseFeed")
LS_DATA_ADAPTER = os.getenv("LS_DATA_ADAPTER", "MDS5")

# "Browser-ähnlicher" Client ID (BNP/Lightstreamer kann hier lizenz-/client-typ-spezifisch sein)
LS_CID = os.getenv(
    "LS_CID",
    "pcYgxn8m8 feOojyA1V661f3g2.pz482h95IL5h",
)

# Item-Namensschema (Default: X0000010800<ISIN>)
LS_ITEM_TEMPLATE = os.getenv("LS_ITEM_TEMPLATE", "X0000010800{isin}")

# Origin ist wichtig (Server kann Origin prüfen). Zum Deaktivieren: LS_ORIGIN=""
LS_ORIGIN = os.getenv("LS_ORIGIN", "https://derivate.bnpparibas.com") or None

LS_USER_AGENT = os.getenv(
    "LS_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

# Reconnect / Stabilität
LS_RECONNECT_MIN_S = float(os.getenv("LS_RECONNECT_MIN_S", "1.0"))
LS_RECONNECT_MAX_S = float(os.getenv("LS_RECONNECT_MAX_S", "30.0"))
# If we don't receive any WS message for this long, consider the stream stuck and reconnect.
LS_RECV_TIMEOUT_S = float(os.getenv("LS_RECV_TIMEOUT_S", "35.0"))
LS_STALE_RESTART_S = float(os.getenv("LS_STALE_RESTART_S", "90.0"))

# Für Streaming + Bewertung nutzen wir das „volle“ Schema (wie aus dem Browser beobachtet),
# damit auch andere Item-Typen (z.B. Indizes) sauber funktionieren.
SCHEMA_FIELDS: List[str] = [
    "symbol",
    "bid",
    "bidsize",
    "ask",
    "asksize",
    "reference",
    "last",
    "quotetime",
    "vega",
    "theta",
    "currentleverage",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_isin(isin: str) -> str:
    isin = (isin or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{12}", isin):
        raise ValueError("ISIN muss 12 Zeichen (A-Z/0-9) sein.")
    return isin


def validate_watch_code(code: str) -> str:
    """
    Watchlist kann entweder eine ISIN (12 Zeichen) ODER ein BNP/Lightstreamer Item (z.B. X0000080800586) sein.
    """
    code = (code or "").strip().upper()
    if re.fullmatch(r"[A-Z0-9]{12}", code):
        return code
    if re.fullmatch(r"X[0-9A-Z]{6,32}", code):
        return code
    raise ValueError("Ungültiger Watchlist-Code. Erlaubt: ISIN (12 Zeichen) oder BNP Item-ID (z.B. X0000080800586).")


def code_to_ls_item(code: str) -> str:
    """
    Mappt einen Watch-/ISIN-Code auf Lightstreamer LS_group Item:
    - Wenn bereits X... => direkt verwenden
    - Sonst ISIN => über Template zu X0000010800<ISIN>
    """
    code = validate_watch_code(code)
    if code.startswith("X"):
        return code
    return isin_to_item(code)


def isin_to_item(isin: str) -> str:
    isin = validate_isin(isin)
    tmpl = (LS_ITEM_TEMPLATE or "").strip() or "X0000010800{isin}"
    if "{isin}" not in tmpl:
        raise ValueError("LS_ITEM_TEMPLATE muss '{isin}' enthalten.")
    return tmpl.format(isin=isin)


def _try_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    if v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def decode_field_values(tokens: List[str], fields: List[str], prev: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """
    Decodiert Lightstreamer TLCP Values (| getrennt) mit:
    - "" -> unverändert
    - "#" -> null
    - "$" -> leerstring
    - "^N" -> N Felder unverändert (ab aktueller Position)
    """
    state = dict(prev)
    fi = 0
    ti = 0

    while fi < len(fields) and ti < len(tokens):
        tok = tokens[ti]
        if tok.startswith("^") and tok[1:].isdigit():
            fi += int(tok[1:])
            ti += 1
            continue

        key = fields[fi]
        if tok == "":
            fi += 1
            ti += 1
            continue
        if tok == "#":
            state[key] = None
        elif tok == "$":
            state[key] = ""
        else:
            state[key] = tok
        fi += 1
        ti += 1

    return state


async def _ws_connect() -> Any:
    """
    Robust gegen Unterschiede im Server-Handshake:
    - ggf. ohne UA
    - ggf. ohne Subprotocol
    - permessage-deflate deaktiviert
    """
    subprotocols: List[Optional[str]] = [LS_SUBPROTOCOL, None]
    origins: List[Optional[str]] = [LS_ORIGIN, None]

    last_exc: Optional[BaseException] = None

    for proto in subprotocols:
        for origin in origins:
            for send_ua in (True, False):
                kwargs: Dict[str, Any] = {
                    "ping_interval": None,
                    "compression": None,
                }
                if proto is not None:
                    kwargs["subprotocols"] = [proto]
                if origin is not None:
                    kwargs["origin"] = origin

                headers: Dict[str, str] = {}
                if send_ua:
                    headers["User-Agent"] = LS_USER_AGENT

                try:
                    ws = await websockets.connect(
                        LS_WSS_URL,
                        **kwargs,
                        additional_headers=headers or None,
                    )
                    logger.info("LS WS connected (proto=%s origin=%s ua=%s)", proto or "<none>", origin or "<none>", send_ua)
                    return ws
                except TypeError:
                    # Fallback für ältere websockets Signaturen
                    try:
                        ws = await websockets.connect(
                            LS_WSS_URL,
                            **kwargs,
                            extra_headers=headers or None,
                        )
                        logger.info("LS WS connected (proto=%s origin=%s ua=%s)", proto or "<none>", origin or "<none>", send_ua)
                        return ws
                    except Exception as e:
                        last_exc = e
                except Exception as e:
                    last_exc = e

    assert last_exc is not None
    raise last_exc


class LightstreamerSession:
    def __init__(self) -> None:
        self.websocket: Optional[Any] = None
        self.session_id: Optional[str] = None
        self.sub_id: int = 1
        self.req_id: int = 1
        self.item_state: Dict[int, Dict[str, Optional[str]]] = {}

    async def connect(self) -> None:
        self.websocket = await _ws_connect()

        params = (
            f"LS_adapter_set={quote(LS_ADAPTER_SET)}"
            f"&LS_user="
            f"&LS_cid={quote(LS_CID)}"
            f"&LS_send_sync=false"
            f"&LS_cause=api"
            f"&LS_password="
        )
        msg = "create_session\r\n" + params + "\r\n"
        await self.websocket.send(msg)

        while True:
            raw = await self._recv_text()
            for line in self._split_lines(raw):
                if line.startswith("CONERR,") or line.startswith("ERROR,"):
                    raise RuntimeError(f"Lightstreamer create_session failed: {line}")
                if line.startswith("CONOK,"):
                    parts = line.split(",")
                    if len(parts) >= 2:
                        self.session_id = parts[1].strip()
                        return

    async def subscribe_items(self, items: List[str]) -> None:
        if not self.websocket or not self.session_id:
            raise RuntimeError("Session not connected")
        if not items:
            raise ValueError("items darf nicht leer sein")

        schema = quote(" ".join(SCHEMA_FIELDS))
        group = quote(" ".join(items))

        params = (
            f"LS_reqId={self.req_id}"
            f"&LS_op=add"
            f"&LS_subId={self.sub_id}"
            f"&LS_mode=MERGE"
            f"&LS_group={group}"
            f"&LS_schema={schema}"
            f"&LS_data_adapter={quote(LS_DATA_ADAPTER)}"
            f"&LS_snapshot=true"
            f"&LS_ack=false"
            f"&LS_requested_max_frequency=unfiltered"
            f"&LS_session={quote(self.session_id)}"
        )
        msg = "control\r\n" + params + "\r\n"
        await self.websocket.send(msg)
        self.req_id += 1

    async def close(self) -> None:
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass
        self.websocket = None
        self.session_id = None
        self.item_state.clear()

    async def _recv_text(self) -> str:
        assert self.websocket is not None
        data = await self.websocket.recv()
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        return data

    @staticmethod
    def _split_lines(raw: str) -> List[str]:
        raw = raw.replace("\r", "")
        out: List[str] = []
        for line in raw.split("\n"):
            line = line.strip()
            if not line:
                continue
            if " U," in line:
                parts = line.split(" U,")
                out.append(parts[0].strip())
                for p in parts[1:]:
                    out.append(("U," + p).strip())
            else:
                out.append(line)
        return out

    def handle_update_line(self, line: str, idx_to_key: Dict[int, str]) -> Optional[Dict[str, Any]]:
        """
        Parst U-Updates und liefert ein Event-Dict zurück:
        {type:'quote', key, bid, quotetime, symbol}
        """
        if not line.startswith("U,"):
            return None

        m = re.match(r"^U,(\d+),(\d+),(.*)$", line)
        if not m:
            return None

        item_index = int(m.group(2))
        values_str = m.group(3)
        tokens = values_str.split("|")

        prev = self.item_state.get(item_index, {f: None for f in SCHEMA_FIELDS})
        decoded = decode_field_values(tokens, SCHEMA_FIELDS, prev)
        self.item_state[item_index] = decoded

        key = idx_to_key.get(item_index)
        symbol = decoded.get("symbol")
        if not key and not symbol:
            return None

        bid = _try_float(decoded.get("bid"))
        ask = _try_float(decoded.get("ask"))
        reference = _try_float(decoded.get("reference"))
        last = _try_float(decoded.get("last"))
        qt = decoded.get("quotetime")
        if bid is None and ask is None and reference is None and last is None:
            return None

        # Watchlist-Preis: bei Index-/Underlying-Items ist meist reference relevant (bid/ask/last können 0/# sein)
        def pick_watch_price() -> tuple[Optional[float], Optional[str]]:
            candidates: List[tuple[str, Optional[float]]] = [
                ("reference", reference),
                ("last", last),
                ("bid", bid),
                ("ask", ask),
            ]
            # Prefer non-zero values first (0.0000 ist bei einigen Items nur Platzhalter)
            for name, val in candidates:
                if val is not None and val != 0.0:
                    return val, name
            for name, val in candidates:
                if val is not None:
                    return val, name
            return None, None

        watch_price, watch_field = pick_watch_price()

        # Für Portfolio-Updates ist key typischerweise ISIN. Für Indizes (X...) bleibt key die Item-ID.
        return {
            "type": "quote",
            "key": key or symbol,
            "isin": symbol or key,
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "reference": reference,
            "last": last,
            "watch_price": watch_price,
            "watch_field": watch_field,
            "quotetime": qt,
        }


def _load_portfolios_and_positions(conn) -> tuple[list[dict], dict[int, list[dict]], list[str]]:
    cur = conn.execute("SELECT id, name FROM portfolios ORDER BY id DESC")
    portfolios = [dict(r) for r in cur.fetchall()]
    cur2 = conn.execute(
        """
        SELECT id, portfolio_id, isin, quantity, entry_price
        FROM positions
        ORDER BY portfolio_id DESC, id ASC
        """
    )
    positions_all = [dict(r) for r in cur2.fetchall()]

    by_portfolio: Dict[int, List[Dict[str, Any]]] = {}
    all_isins: List[str] = []
    seen: set[str] = set()
    for p in positions_all:
        pid = int(p["portfolio_id"])
        by_portfolio.setdefault(pid, []).append(p)
        isin = p["isin"]
        if isin not in seen:
            seen.add(isin)
            all_isins.append(isin)

    return portfolios, by_portfolio, all_isins


def compute_all_valuations_from_bids(bids: Dict[str, Optional[float]]) -> List[Dict[str, Any]]:
    portfolios, by_portfolio, _ = _load_portfolios_and_positions(_conn)
    valued_at = now_iso()
    out: List[Dict[str, Any]] = []
    for pf in portfolios:
        pid = int(pf["id"])
        out.append(compute_valuation(pf, by_portfolio.get(pid, []), bids, valued_at=valued_at, timeout_s=0.0))
    return out


def load_watchlist(conn) -> List[Dict[str, Any]]:
    cur = conn.execute("SELECT id, label, isin, currency FROM watchlist ORDER BY id DESC")
    return [dict(r) for r in cur.fetchall()]


def compute_watchlist_from_prices(
    prices: Dict[str, Optional[float]],
    fields: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    items = load_watchlist(_conn)
    out: List[Dict[str, Any]] = []
    for it in items:
        key = it["isin"]
        out.append(
            {
                "id": it["id"],
                "label": it.get("label"),
                "isin": key,
                "key": key,
                "currency": it.get("currency") or "EUR",
                "price": prices.get(key),
                "field": (fields or {}).get(key),
            }
        )
    return out


class StreamManager:
    def __init__(self) -> None:
        self.clients: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._dirty = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

        # Cache: letzte Quotes je Key (ISIN oder X...)
        self.bids: Dict[str, float] = {}
        # Watchlist-Kurse (können reference/last/bid/ask sein)
        self.watch_prices: Dict[str, float] = {}
        self.watch_fields: Dict[str, str] = {}

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run())

    def mark_dirty(self) -> None:
        self._dirty.set()

    async def add_client(self, ws: WebSocket) -> None:
        self.clients.add(ws)

        # Start stream when first client arrives
        self.start()

        # Initial snapshot (valuations + watchlist based on cached bids)
        bids = self._bids_as_optional()
        watch_prices = self._watch_prices_as_optional()
        await ws.send_json(
            {
                "type": "snapshot",
                "valuations": compute_all_valuations_from_bids(bids),
                "watchlist": compute_watchlist_from_prices(watch_prices, self.watch_fields),
            }
        )

    def _bids_as_optional(self) -> Dict[str, Optional[float]]:
        return {k: float(v) for k, v in self.bids.items()}

    def _watch_prices_as_optional(self) -> Dict[str, Optional[float]]:
        return {k: float(v) for k, v in self.watch_prices.items()}

    async def remove_client(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast(self, obj: Dict[str, Any]) -> None:
        dead: List[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_json(obj)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)

    async def _run(self) -> None:
        """
        Background task: subscribes to all current ISINs, emits quote events.
        Restarts subscription when portfolios/positions change (dirty flag).
        """
        last_items: List[str] = []
        backoff_s: float = max(0.1, LS_RECONNECT_MIN_S)

        while True:
            try:
                # Stream läuft, wenn entweder Dashboard-Clients verbunden sind ODER MQTT enabled ist.
                # So kann MQTT auch ohne geöffnetes Frontend 24/7 Updates bekommen.
                need_stream = bool(self.clients) or _mqtt_is_enabled()
                if not need_stream:
                    await asyncio.sleep(0.5)
                    continue

                portfolios, by_portfolio, portfolio_isins = _load_portfolios_and_positions(_conn)
                watch = load_watchlist(_conn)
                watch_codes = [w["isin"] for w in watch]

                # Targets: (ls_item, key). key bleibt stabil fürs Frontend (ISIN oder X...).
                targets: List[tuple[str, str]] = []
                seen_keys: set[str] = set()

                for isin in portfolio_isins:
                    if isin not in seen_keys:
                        seen_keys.add(isin)
                        targets.append((isin_to_item(isin), isin))

                for code in watch_codes:
                    code = validate_watch_code(code)
                    if code not in seen_keys:
                        seen_keys.add(code)
                        targets.append((code_to_ls_item(code), code))

                items = [t[0] for t in targets]
                idx_to_key = {idx + 1: t[1] for idx, t in enumerate(targets)}

                if items == last_items and not self._dirty.is_set():
                    # keep current session
                    await asyncio.sleep(0.5)
                    continue

                # reset dirty
                self._dirty.clear()
                last_items = items

                if not items:
                    await self.broadcast({"type": "status", "level": "warn", "message": "Keine Positionen/Watchlist vorhanden – kein Stream."})
                    await asyncio.sleep(1.0)
                    continue

                await self.broadcast({"type": "status", "level": "info", "message": f"Starte Stream für {len(items)} Item(s)…"})

                sess = LightstreamerSession()
                try:
                    await sess.connect()
                    await sess.subscribe_items(items)
                except Exception as e:
                    # Backoff + retry on connect/subscribe errors
                    wait_s = min(max(LS_RECONNECT_MIN_S, backoff_s), LS_RECONNECT_MAX_S)
                    jitter = random.uniform(0.0, max(0.1, wait_s * 0.1))
                    await self.broadcast(
                        {
                            "type": "status",
                            "level": "error",
                            "message": f"Stream-Verbindung fehlgeschlagen: {e} — Reconnect in {wait_s:.1f}s",
                        }
                    )
                    try:
                        await sess.close()
                    except Exception:
                        pass
                    await asyncio.sleep(wait_s + jitter)
                    backoff_s = min(LS_RECONNECT_MAX_S, max(LS_RECONNECT_MIN_S, backoff_s * 2.0))
                    # force reconnect even if items unchanged
                    last_items = []
                    continue

                backoff_s = max(0.1, LS_RECONNECT_MIN_S)
                await self.broadcast({"type": "status", "level": "success", "message": "Stream verbunden."})

                # Receive loop until dirty flag set -> restart (oder bis wir den Stream nicht mehr brauchen)
                loop = asyncio.get_running_loop()
                last_any_msg = loop.time()
                while not self._dirty.is_set() and (self.clients or _mqtt_is_enabled()):
                    try:
                        raw = await asyncio.wait_for(sess._recv_text(), timeout=max(1.0, LS_RECV_TIMEOUT_S))
                    except ConnectionClosed as e:
                        await self.broadcast({"type": "status", "level": "error", "message": f"Stream getrennt: {e.code} {e.reason}"})
                        break
                    except asyncio.TimeoutError:
                        # We expect regular PROBE frames; if we don't see anything for a while,
                        # restart the connection to avoid silently-stuck sessions.
                        if (loop.time() - last_any_msg) >= max(LS_RECV_TIMEOUT_S, LS_STALE_RESTART_S):
                            await self.broadcast(
                                {
                                    "type": "status",
                                    "level": "warn",
                                    "message": f"Stream ohne Daten seit {(loop.time() - last_any_msg):.0f}s — Reconnect…",
                                }
                            )
                            break
                        continue

                    for line in sess._split_lines(raw):
                        if line == "PROBE":
                            last_any_msg = loop.time()
                            continue
                        if line.startswith("REQERR,") or line.startswith("ERROR,") or line.startswith("CONERR,"):
                            await self.broadcast({"type": "status", "level": "error", "message": f"Lightstreamer: {line}"})
                            # Errors often indicate a broken session/subscription -> reconnect.
                            break

                        last_any_msg = loop.time()

                        evt = sess.handle_update_line(line, idx_to_key)
                        if not evt:
                            continue

                        # update cache and broadcast quote
                        key = evt.get("key") or evt.get("isin")
                        if key:
                            if evt.get("bid") is not None:
                                self.bids[key] = float(evt["bid"])
                            if evt.get("watch_price") is not None and evt.get("watch_field"):
                                self.watch_prices[key] = float(evt["watch_price"])
                                self.watch_fields[key] = str(evt["watch_field"])
                        await self.broadcast(evt)
                        _publish_mqtt_snapshot()
                    else:
                        # no break in for-loop
                        continue
                    # break in for-loop: break out of receive loop too
                    break

                try:
                    await sess.close()
                except Exception:
                    pass

                # On restart, also push a fresh snapshot to align UI state.
                bids = self._bids_as_optional()
                watch_prices = self._watch_prices_as_optional()
                await self.broadcast(
                    {
                        "type": "snapshot",
                        "valuations": compute_all_valuations_from_bids(bids),
                        "watchlist": compute_watchlist_from_prices(watch_prices, self.watch_fields),
                    }
                )
                # If we ended the session without a "dirty" restart request, force reconnect.
                # (Otherwise we'd sit in the "items == last_items" short-circuit and never restart.)
                if not self._dirty.is_set() and (self.clients or _mqtt_is_enabled()):
                    wait_s = min(max(LS_RECONNECT_MIN_S, backoff_s), LS_RECONNECT_MAX_S)
                    jitter = random.uniform(0.0, max(0.1, wait_s * 0.1))
                    await asyncio.sleep(wait_s + jitter)
                    backoff_s = min(LS_RECONNECT_MAX_S, max(LS_RECONNECT_MIN_S, backoff_s * 2.0))
                    last_items = []

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("StreamManager error")
                await self.broadcast({"type": "status", "level": "error", "message": f"StreamManager Fehler: {e}"})
                await asyncio.sleep(2.0)


stream_manager = StreamManager()
_mqtt: Optional[HomeAssistantMqttPublisher] = None


def _publish_mqtt_snapshot() -> None:
    """
    Publishes the full payload (portfolios/positions/watchlist) to a single HA entity.
    Best-effort + debounced inside publisher.
    """
    global _mqtt
    if not _mqtt or not _mqtt_is_enabled():
        return
    bids = stream_manager._bids_as_optional()
    watch_prices = stream_manager._watch_prices_as_optional()
    portfolios = compute_all_valuations_from_bids(bids)
    watchlist = compute_watchlist_from_prices(watch_prices, stream_manager.watch_fields)
    _mqtt.publish_all(portfolios=portfolios, watchlist=watchlist)


def _mqtt_is_enabled() -> bool:
    return get_bool(_conn, "mqtt_enabled", default=False)


def _set_mqtt_enabled(enabled: bool) -> None:
    set_bool(_conn, "mqtt_enabled", enabled)


def _mqtt_connect_if_enabled() -> None:
    global _mqtt
    if not _mqtt_is_enabled():
        return
    if _mqtt:
        return
    s = load_mqtt_settings()
    if not s.host:
        raise RuntimeError("MQTT_HOST ist nicht gesetzt.")
    _mqtt = HomeAssistantMqttPublisher(s)
    _mqtt.connect()
    _publish_mqtt_snapshot()


def _mqtt_disconnect() -> None:
    global _mqtt
    if _mqtt:
        _mqtt.close()
    _mqtt = None


async def fetch_bids(isins: List[str], timeout_s: float = 8.0) -> Dict[str, Optional[float]]:
    """
    Holt für eine ISIN-Liste die aktuellen Bid-Quotes (Snapshot/erste Updates) über Lightstreamer.
    Gibt dict[ISIN] = bid (float) oder None (falls nicht erhalten).
    """
    clean_isins = [validate_isin(i) for i in isins]
    items = [isin_to_item(i) for i in clean_isins]
    idx_to_isin = {idx + 1: isin for idx, isin in enumerate(clean_isins)}

    sess = LightstreamerSession()
    bids: Dict[str, Optional[float]] = {i: None for i in clean_isins}

    try:
        await sess.connect()
        await sess.subscribe_items(items)

        deadline = asyncio.get_running_loop().time() + timeout_s
        while asyncio.get_running_loop().time() < deadline:
            if all(v is not None for v in bids.values()):
                break

            try:
                raw = await asyncio.wait_for(sess._recv_text(), timeout=max(0.2, deadline - asyncio.get_running_loop().time()))
            except asyncio.TimeoutError:
                continue
            except ConnectionClosed as e:
                raise RuntimeError(f"WS closed: code={e.code} reason={e.reason}") from e

            for line in sess._split_lines(raw):
                if line.startswith("REQERR,") or line.startswith("ERROR,"):
                    raise RuntimeError(f"Lightstreamer error: {line}")
                if not line.startswith("U,"):
                    continue

                m = re.match(r"^U,(\d+),(\d+),(.*)$", line)
                if not m:
                    continue
                item_index = int(m.group(2))
                values_str = m.group(3)
                tokens = values_str.split("|")

                prev = sess.item_state.get(item_index, {f: None for f in SCHEMA_FIELDS})
                decoded = decode_field_values(tokens, SCHEMA_FIELDS, prev)
                sess.item_state[item_index] = decoded

                isin = decoded.get("symbol") or idx_to_isin.get(item_index)
                if not isin:
                    continue
                bid = _try_float(decoded.get("bid"))
                if bid is not None:
                    bids[isin] = bid

        return bids
    finally:
        await sess.close()


def compute_valuation(
    portfolio: Dict[str, Any],
    positions: List[Dict[str, Any]],
    bids: Dict[str, Optional[float]],
    valued_at: str,
    timeout_s: float,
) -> Dict[str, Any]:
    out_positions: List[Dict[str, Any]] = []
    total_mv = 0.0
    total_cb = 0.0

    for p in positions:
        isin = p["isin"]
        qty = float(p["quantity"])
        entry = float(p["entry_price"])
        bid = bids.get(isin)

        cost_basis = entry * qty
        market_value = (bid * qty) if bid is not None else None
        pnl = (market_value - cost_basis) if market_value is not None else None
        pnl_pct = (pnl / cost_basis) if (pnl is not None and cost_basis != 0) else None

        if market_value is not None:
            total_mv += market_value
        total_cb += cost_basis

        out_positions.append(
            {
                "id": p.get("id"),
                "isin": isin,
                "quantity": qty,
                "entry_price": entry,
                "bid": bid,
                "market_value": market_value,
                "cost_basis": cost_basis,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
            }
        )

    total_pnl = total_mv - total_cb
    total_pnl_pct = (total_pnl / total_cb) if total_cb else None

    return {
        "portfolio": dict(portfolio),
        "currency": (portfolio or {}).get("currency") or "EUR",
        "valued_at": valued_at,
        "positions": out_positions,
        "totals": {
            "market_value": total_mv,
            "cost_basis": total_cb,
            "pnl": total_pnl,
            "pnl_pct": total_pnl_pct,
        },
        "meta": {"quote_timeout_s": timeout_s},
    }


# ----------------- API Models -----------------


class PortfolioCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    currency: str = Field(default="EUR", min_length=3, max_length=8)


class PositionIn(BaseModel):
    isin: str
    quantity: float = Field(gt=0)
    entry_price: float = Field(gt=0)
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)


class PortfolioOut(BaseModel):
    id: int
    name: str


class PositionOut(BaseModel):
    id: int
    isin: str
    quantity: float
    entry_price: float


class WatchItemCreate(BaseModel):
    isin: str
    label: Optional[str] = Field(default=None, max_length=200)
    currency: str = Field(default="EUR", min_length=3, max_length=8)


class PortfolioUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)


# ----------------- FastAPI App -----------------


app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

_conn = connect_db()
init_db(_conn)

@app.on_event("startup")
async def _startup() -> None:
    # Stream startet erst, wenn ein Dashboard-Client verbunden ist.
    stream_manager.start()
    # MQTT default: aus. Nur verbinden, wenn UI-Setting mqtt_enabled=true ist.
    try:
        _mqtt_connect_if_enabled()
    except Exception as e:
        logger.warning("MQTT connect failed: %s", e)


@app.on_event("shutdown")
async def _shutdown() -> None:
    if stream_manager._task and not stream_manager._task.done():
        stream_manager._task.cancel()
        try:
            await stream_manager._task
        except Exception:
            pass
    _mqtt_disconnect()


@app.get("/")
async def index():
    return FileResponse("app/static/dashboard.html")


@app.get("/manage")
async def manage():
    return FileResponse("app/static/manage.html")


@app.get("/api/portfolios")
async def list_portfolios() -> List[Dict[str, Any]]:
    cur = _conn.execute(
        """
        SELECT p.id, p.name, p.currency, COUNT(pos.id) AS positions_count
        FROM portfolios p
        LEFT JOIN positions pos ON pos.portfolio_id = p.id
        GROUP BY p.id
        ORDER BY p.id DESC
        """
    )
    return [dict(r) for r in cur.fetchall()]


@app.post("/api/portfolios", status_code=201)
async def create_portfolio(body: PortfolioCreate) -> PortfolioOut:
    cur = _conn.execute(
        "INSERT INTO portfolios(name, currency) VALUES (?, ?)",
        (body.name.strip(), (body.currency or "EUR").strip().upper()),
    )
    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return PortfolioOut(id=int(cur.lastrowid), name=body.name.strip())


@app.get("/api/watchlist")
async def list_watchlist() -> List[Dict[str, Any]]:
    return load_watchlist(_conn)


@app.post("/api/watchlist", status_code=201)
async def add_watchlist_item(body: WatchItemCreate) -> Dict[str, Any]:
    isin = validate_watch_code(body.isin)
    label = (body.label or "").strip() or None
    currency = (body.currency or "EUR").strip().upper()
    try:
        cur = _conn.execute("INSERT INTO watchlist(isin, label, currency) VALUES (?,?,?)", (isin, label, currency))
        _conn.commit()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Watchlist-Eintrag konnte nicht gespeichert werden: {e}")

    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return {"id": int(cur.lastrowid), "isin": isin, "label": label, "currency": currency}


@app.delete("/api/watchlist/{item_id}", status_code=204)
async def delete_watchlist_item(item_id: int) -> None:
    with _conn:
        _conn.execute("DELETE FROM watchlist WHERE id=?", (item_id,))
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return None


@app.get("/api/portfolios/valuations")
async def value_all_portfolios() -> List[Dict[str, Any]]:
    """
    Bewertet alle Portfolios in einem Request.
    Wichtig: Wir holen alle Bid-Quotes gesammelt in *einer* Lightstreamer-Session,
    um die Bewertung kontinuierlich (Polling) effizient zu halten.
    """
    cur = _conn.execute("SELECT id, name FROM portfolios ORDER BY id DESC")
    portfolios = [dict(r) for r in cur.fetchall()]
    if not portfolios:
        return []

    cur2 = _conn.execute(
        """
        SELECT id, portfolio_id, isin, quantity, entry_price
        FROM positions
        ORDER BY portfolio_id DESC, id ASC
        """
    )
    positions_all = [dict(r) for r in cur2.fetchall()]

    by_portfolio: Dict[int, List[Dict[str, Any]]] = {}
    all_isins: List[str] = []
    seen: set[str] = set()
    for p in positions_all:
        pid = int(p["portfolio_id"])
        by_portfolio.setdefault(pid, []).append(p)
        isin = p["isin"]
        if isin not in seen:
            seen.add(isin)
            all_isins.append(isin)

    # Für Dashboard: Snapshot aus dem aktuellen Bid-Cache (Stream).
    # Fallback: falls Stream noch nichts gesehen hat, werden bids als None angezeigt.
    stream_manager.start()
    bids = stream_manager._bids_as_optional()
    # asks werden separat für Watchlist im Snapshot per WS genutzt
    valued_at = now_iso()

    out: List[Dict[str, Any]] = []
    for pf in portfolios:
        pid = int(pf["id"])
        out.append(compute_valuation(pf, by_portfolio.get(pid, []), bids, valued_at=valued_at, timeout_s=0.0))
    return out


@app.get("/api/portfolios/{portfolio_id}")
async def get_portfolio(portfolio_id: int) -> Dict[str, Any]:
    cur = _conn.execute("SELECT id, name, currency FROM portfolios WHERE id=?", (portfolio_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    cur2 = _conn.execute(
        "SELECT id, isin, quantity, entry_price, currency FROM positions WHERE portfolio_id=? ORDER BY id ASC",
        (portfolio_id,),
    )
    return {"portfolio": dict(row), "positions": [dict(r) for r in cur2.fetchall()]}


@app.put("/api/portfolios/{portfolio_id}")
async def update_portfolio(portfolio_id: int, body: PortfolioUpdate) -> Dict[str, Any]:
    cur = _conn.execute("SELECT id FROM portfolios WHERE id=?", (portfolio_id,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    fields: List[str] = []
    values: List[Any] = []
    if body.name is not None:
        fields.append("name=?")
        values.append(body.name.strip())
    if body.currency is not None:
        fields.append("currency=?")
        values.append(body.currency.strip().upper())

    if fields:
        values.append(portfolio_id)
        _conn.execute(f"UPDATE portfolios SET {', '.join(fields)} WHERE id=?", tuple(values))
        _conn.commit()
        stream_manager.mark_dirty()
        _publish_mqtt_snapshot()

    return await get_portfolio(portfolio_id)


@app.put("/api/portfolios/{portfolio_id}/positions")
async def replace_positions(portfolio_id: int, positions: List[PositionIn]) -> Dict[str, Any]:
    # Ensure portfolio exists
    cur = _conn.execute("SELECT id, currency FROM portfolios WHERE id=?", (portfolio_id,))
    prow = cur.fetchone()
    if not prow:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")
    portfolio_currency = (prow["currency"] or "EUR").strip().upper()

    cleaned: List[PositionIn] = []
    seen: set[str] = set()
    for p in positions:
        isin = validate_isin(p.isin)
        if isin in seen:
            raise HTTPException(status_code=400, detail=f"Doppelte ISIN im Request: {isin}")
        seen.add(isin)
        currency = (p.currency or portfolio_currency).strip().upper()
        cleaned.append(PositionIn(isin=isin, quantity=p.quantity, entry_price=p.entry_price, currency=currency))

    with _conn:
        _conn.execute("DELETE FROM positions WHERE portfolio_id=?", (portfolio_id,))
        for p in cleaned:
            _conn.execute(
                "INSERT INTO positions(portfolio_id, isin, quantity, entry_price, currency) VALUES (?,?,?,?,?)",
                (portfolio_id, p.isin, p.quantity, p.entry_price, (p.currency or portfolio_currency)),
            )

    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return await get_portfolio(portfolio_id)


@app.delete("/api/portfolios/{portfolio_id}", status_code=204)
async def delete_portfolio(portfolio_id: int) -> None:
    cur = _conn.execute("DELETE FROM portfolios WHERE id=?", (portfolio_id,))
    _conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return None


@app.delete("/api/portfolios/{portfolio_id}/positions/{position_id}", status_code=204)
async def delete_position(portfolio_id: int, position_id: int) -> None:
    cur = _conn.execute("DELETE FROM positions WHERE id=? AND portfolio_id=?", (position_id, portfolio_id))
    _conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Position nicht gefunden")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return None


@app.post("/api/portfolios/{portfolio_id}/value")
async def value_portfolio(portfolio_id: int) -> Dict[str, Any]:
    cur = _conn.execute("SELECT id, name, currency FROM portfolios WHERE id=?", (portfolio_id,))
    portfolio = cur.fetchone()
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    cur2 = _conn.execute(
        "SELECT id, isin, quantity, entry_price, currency FROM positions WHERE portfolio_id=? ORDER BY id ASC",
        (portfolio_id,),
    )
    positions = [dict(r) for r in cur2.fetchall()]
    valued_at = now_iso()
    if not positions:
        return compute_valuation(dict(portfolio), [], {}, valued_at=valued_at, timeout_s=0.0)

    isins = [p["isin"] for p in positions]
    # Einzelbewertung nutzt ebenfalls den Cache (keine eigene LS-Session), damit Streaming „single source of truth“ ist.
    stream_manager.start()
    bids = stream_manager._bids_as_optional()
    return compute_valuation(dict(portfolio), positions, bids, valued_at=valued_at, timeout_s=0.0)


@app.websocket("/ws")
async def ws_dashboard(ws: WebSocket):
    await ws.accept()
    await stream_manager.add_client(ws)
    try:
        while True:
            # Dashboard sendet aktuell keine Commands; wir halten die Verbindung offen.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await stream_manager.remove_client(ws)


@app.get("/api/mqtt")
async def mqtt_status() -> Dict[str, Any]:
    s = load_mqtt_settings()
    enabled = _mqtt_is_enabled()
    return {
        "enabled": enabled,
        "connected": bool(_mqtt),
        "host": s.host,
        "port": s.port,
        "availability_topic": s.availability_topic,
    }


@app.put("/api/mqtt/enabled")
async def mqtt_set_enabled(body: Dict[str, Any]) -> Dict[str, Any]:
    enabled = bool(body.get("enabled"))
    _set_mqtt_enabled(enabled)
    if enabled:
        _mqtt_connect_if_enabled()
        stream_manager.mark_dirty()
    else:
        _mqtt_disconnect()
        stream_manager.mark_dirty()
    return await mqtt_status()
