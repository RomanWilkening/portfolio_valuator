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
from app.quote_sources import (
    SOURCE_LIGHTSTREAMER,
    SOURCE_TRADEGATE,
    QuoteRouter,
    TradegatePoller,
    is_isin,
    load_tradegate_settings,
    parse_source_priority,
)
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

# Kursquellen-Prioritaet (links = hoechste Prioritaet)
QUOTE_SOURCE_PRIORITY = parse_source_priority(os.getenv("QUOTE_SOURCE_PRIORITY"))
_KNOWN_SOURCES = {SOURCE_LIGHTSTREAMER, SOURCE_TRADEGATE}
_UNKNOWN_SOURCES = [s for s in QUOTE_SOURCE_PRIORITY if s not in _KNOWN_SOURCES]
if _UNKNOWN_SOURCES:
    logger.warning("Unbekannte Kursquelle in QUOTE_SOURCE_PRIORITY: %s", ", ".join(_UNKNOWN_SOURCES))

TRADEGATE_SETTINGS = load_tradegate_settings()

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


def is_ls_item(code: str) -> bool:
    return bool(re.fullmatch(r"X[0-9A-Z]{6,32}", (code or "").strip().upper()))




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

        def _valid_price(val: Optional[float]) -> bool:
            return val is not None and val != 0.0

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
                if _valid_price(val):
                    return val, name
            for name, val in candidates:
                if val is not None:
                    return val, name
            return None, None

        watch_price, watch_field = pick_watch_price()

        def pick_valuation_price() -> tuple[Optional[float], Optional[str]]:
            if _valid_price(bid):
                return bid, "bid"
            if bid is not None and ask is not None and bid != 0.0 and ask != 0.0:
                return (bid + ask) / 2.0, "mid"
            if _valid_price(reference):
                return reference, "reference"
            if _valid_price(last):
                return last, "last"
            if _valid_price(ask):
                return ask, "ask"
            return None, None

        price, price_field = pick_valuation_price()

        # Für Portfolio-Updates ist key typischerweise ISIN. Für Indizes (X...) bleibt key die Item-ID.
        return {
            "type": "quote",
            "key": key or symbol,
            "isin": symbol or key,
            "symbol": symbol,
            "bid": bid,
            "price": price,
            "price_field": price_field,
            "ask": ask,
            "reference": reference,
            "last": last,
            "watch_price": watch_price,
            "watch_field": watch_field,
            "quotetime": qt,
        }


def _load_portfolios_and_positions(conn) -> tuple[list[dict], dict[int, list[dict]], list[str]]:
    cur = conn.execute("SELECT id, name, currency FROM portfolios ORDER BY id DESC")
    portfolios = [dict(r) for r in cur.fetchall()]
    cur2 = conn.execute(
        """
        SELECT
            pos.id,
            pos.portfolio_id,
            pos.instrument_id,
            pos.name AS position_name,
            pos.quantity,
            pos.entry_price,
            pos.currency AS position_currency,
            instr.code AS instrument_code,
            instr.name AS instrument_name,
            instr.currency AS instrument_currency,
            instr.isin AS instrument_isin,
            instr.ls_item AS instrument_ls_item
        FROM positions pos
        JOIN instruments instr ON instr.id = pos.instrument_id
        ORDER BY pos.portfolio_id DESC, pos.id ASC
        """
    )
    positions_all = [dict(r) for r in cur2.fetchall()]

    by_portfolio: Dict[int, List[Dict[str, Any]]] = {}
    all_codes: List[str] = []
    seen: set[str] = set()
    for p in positions_all:
        pid = int(p["portfolio_id"])
        by_portfolio.setdefault(pid, []).append(p)
        code = p["instrument_code"]
        if code not in seen:
            seen.add(code)
            all_codes.append(code)

    return portfolios, by_portfolio, all_codes


def compute_all_valuations_from_prices(
    prices: Dict[str, Optional[float]],
    bids: Dict[str, Optional[float]],
    price_sources: Dict[str, str],
) -> List[Dict[str, Any]]:
    portfolios, by_portfolio, _ = _load_portfolios_and_positions(_conn)
    instruments = load_stream_instruments(_conn)
    fx_rates = build_fx_rates(prices, instruments)
    valued_at = now_iso()
    out: List[Dict[str, Any]] = []
    for pf in portfolios:
        pid = int(pf["id"])
        out.append(
            compute_valuation(
                pf,
                by_portfolio.get(pid, []),
                prices,
                bids,
                price_sources,
                fx_rates,
                valued_at=valued_at,
                timeout_s=0.0,
            )
        )
    return out


def load_watchlist(conn) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            w.id,
            w.label,
            w.instrument_id,
            w.currency,
            instr.code AS instrument_code,
            instr.name AS instrument_name,
            instr.currency AS instrument_currency,
            instr.isin AS instrument_isin,
            instr.ls_item AS instrument_ls_item
        FROM watchlist w
        JOIN instruments instr ON instr.id = w.instrument_id
        ORDER BY w.id DESC
        """
    )
    return [dict(r) for r in cur.fetchall()]


def load_fx_rates(conn) -> List[Dict[str, Any]]:
    cur = conn.execute(
        "SELECT id, code, name, base_currency, quote_currency, isin, ls_item FROM fx_rates ORDER BY id DESC"
    )
    return [dict(r) for r in cur.fetchall()]


def load_instrument_sources(conn) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            s.id,
            s.instrument_id,
            s.source,
            s.source_code,
            s.priority,
            i.code AS instrument_code
        FROM instrument_sources s
        JOIN instruments i ON i.id = s.instrument_id
        ORDER BY s.instrument_id, s.priority ASC, s.id ASC
        """
    )
    return [dict(r) for r in cur.fetchall()]


def load_fx_rate_sources(conn) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            s.id,
            s.fx_rate_id,
            s.source,
            s.source_code,
            s.priority,
            fx.code AS fx_code
        FROM fx_rate_sources s
        JOIN fx_rates fx ON fx.id = s.fx_rate_id
        ORDER BY s.fx_rate_id, s.priority ASC, s.id ASC
        """
    )
    return [dict(r) for r in cur.fetchall()]


def build_priority_map(source_rows: List[Dict[str, Any]], key_field: str) -> Dict[str, List[str]]:
    priorities: Dict[str, List[str]] = {}
    for row in source_rows:
        key = row.get(key_field)
        if not key:
            continue
        priorities.setdefault(str(key), []).append(str(row.get("source") or "").lower())
    return priorities


def ls_source_to_item(source_code: str) -> Optional[str]:
    code = (source_code or "").strip()
    if not code:
        return None
    if is_isin(code):
        return isin_to_item(code)
    return code


def load_stream_instruments(conn) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            instr.id AS id,
            instr.code AS code,
            instr.name AS name,
            instr.currency AS currency,
            instr.isin AS isin,
            instr.ls_item AS ls_item,
            'asset' AS type,
            NULL AS base_currency,
            NULL AS quote_currency
        FROM instruments instr
        WHERE instr.id IN (
            SELECT instrument_id FROM positions
            UNION
            SELECT instrument_id FROM watchlist
        )
        UNION
        SELECT
            fx.id AS id,
            fx.code AS code,
            fx.name AS name,
            fx.quote_currency AS currency,
            fx.isin AS isin,
            fx.ls_item AS ls_item,
            'fx' AS type,
            fx.base_currency AS base_currency,
            fx.quote_currency AS quote_currency
        FROM fx_rates fx
        ORDER BY code DESC
        """
    )
    return [dict(r) for r in cur.fetchall()]


def build_fx_rates(
    prices: Dict[str, Optional[float]],
    instruments: List[Dict[str, Any]],
) -> Dict[tuple[str, str], Dict[str, Any]]:
    rates: Dict[tuple[str, str], Dict[str, Any]] = {}
    for instr in instruments:
        if (instr.get("type") or "asset") != "fx":
            continue
        base = _normalize_currency(instr.get("base_currency") or "", default=None)
        quote = _normalize_currency(instr.get("quote_currency") or "", default=None)
        if not base or not quote:
            continue
        code = instr.get("code")
        if not code:
            continue
        price = prices.get(code)
        if price is None:
            continue
        rates[(base, quote)] = {"rate": float(price), "instrument_code": code}
    return rates


def compute_watchlist_from_prices(
    prices: Dict[str, Optional[float]],
    fields: Optional[Dict[str, str]] = None,
    sources: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    items = load_watchlist(_conn)
    out: List[Dict[str, Any]] = []
    for it in items:
        key = it["instrument_code"]
        out.append(
            {
                "id": it["id"],
                "label": it.get("label"),
                "instrument_id": it.get("instrument_id"),
                "instrument_code": key,
                "instrument_name": it.get("instrument_name"),
                "key": key,
                "currency": it.get("currency") or it.get("instrument_currency") or "EUR",
                "price": prices.get(key),
                "field": (fields or {}).get(key),
                "price_source": (sources or {}).get(key),
            }
        )
    return out


def compute_fx_rates_from_prices(
    prices: Dict[str, Optional[float]],
    sources: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    items = load_fx_rates(_conn)
    out: List[Dict[str, Any]] = []
    for it in items:
        code = it.get("code")
        out.append(
            {
                "id": it.get("id"),
                "code": code,
                "name": it.get("name"),
                "base_currency": it.get("base_currency"),
                "quote_currency": it.get("quote_currency"),
                "price": prices.get(code),
                "price_source": (sources or {}).get(code),
            }
        )
    return out


class StreamManager:
    def __init__(self) -> None:
        self.clients: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._dirty = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

        self.quote_router = QuoteRouter(QUOTE_SOURCE_PRIORITY)
        self.lightstreamer_enabled = SOURCE_LIGHTSTREAMER in self.quote_router.priority
        self.tradegate_poller: Optional[TradegatePoller] = None
        self.instrument_by_code: Dict[str, Dict[str, Any]] = {}
        if SOURCE_TRADEGATE in self.quote_router.priority:
            self.tradegate_poller = TradegatePoller(
                settings=TRADEGATE_SETTINGS,
                router=self.quote_router,
                on_best_update=self._on_source_update,
            )

    def start(self) -> None:
        if self._task and not self._task.done():
            if self.tradegate_poller:
                self.tradegate_poller.start()
            return
        self._task = asyncio.create_task(self._run())
        if self.tradegate_poller:
            self.tradegate_poller.start()

    def mark_dirty(self) -> None:
        self._dirty.set()

    async def _on_source_update(self, key: str, quotetime: Optional[str]) -> None:
        await self._broadcast_best_quote(key, quotetime=quotetime)
        _publish_mqtt_snapshot()

    async def _broadcast_best_quote(
        self,
        key: str,
        *,
        quotetime: Optional[str] = None,
        isin: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> None:
        bid = self.quote_router.best_bids.get(key)
        price = self.quote_router.best_prices.get(key)
        watch_price = self.quote_router.best_watch_prices.get(key)
        watch_field = self.quote_router.best_watch_fields.get(key)
        bid_source = self.quote_router.best_bid_source.get(key)
        price_source = self.quote_router.best_price_source.get(key)
        price_field = self.quote_router.best_price_field.get(key)
        watch_source = self.quote_router.best_watch_source.get(key)
        instrument = self.instrument_by_code.get(key, {})
        await self.broadcast(
            {
                "type": "quote",
                "key": key,
                "isin": isin or key,
                "instrument_id": instrument.get("id"),
                "instrument_code": key,
                "instrument_name": instrument.get("name") or instrument.get("instrument_name"),
                "symbol": symbol,
                "bid": bid,
                "price": price,
                "price_field": price_field,
                "bid_source": bid_source,
                "price_source": price_source,
                "watch_price": watch_price,
                "watch_field": watch_field,
                "watch_source": watch_source,
                "quotetime": quotetime or now_iso(),
            }
        )

    async def add_client(self, ws: WebSocket) -> None:
        self.clients.add(ws)

        # Start stream when first client arrives
        self.start()

        # Initial snapshot (valuations + watchlist based on cached bids)
        bids = self._bids_as_optional()
        prices = self._prices_as_optional()
        watch_prices = self._watch_prices_as_optional()
        await ws.send_json(
            {
                "type": "snapshot",
                "valuations": compute_all_valuations_from_prices(
                    prices,
                    bids,
                    self.quote_router.best_price_source,
                ),
                "fx_rates": compute_fx_rates_from_prices(
                    prices,
                    self.quote_router.best_price_source,
                ),
                "watchlist": compute_watchlist_from_prices(
                    watch_prices,
                    self.quote_router.best_watch_fields,
                    self.quote_router.best_watch_source,
                ),
            }
        )

    def _bids_as_optional(self) -> Dict[str, Optional[float]]:
        return {k: float(v) for k, v in self.quote_router.best_bids.items()}

    def _prices_as_optional(self) -> Dict[str, Optional[float]]:
        return {k: float(v) for k, v in self.quote_router.best_prices.items()}

    def _watch_prices_as_optional(self) -> Dict[str, Optional[float]]:
        return {k: float(v) for k, v in self.quote_router.best_watch_prices.items()}

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
                if self.tradegate_poller:
                    self.tradegate_poller.set_enabled(need_stream)
                    if not need_stream:
                        self.tradegate_poller.set_mapping({})
                if not need_stream:
                    last_items = []
                    await asyncio.sleep(0.5)
                    continue

                instruments = load_stream_instruments(_conn)
                self.instrument_by_code = {i["code"]: i for i in instruments if i.get("code")}
                instrument_sources = load_instrument_sources(_conn)
                fx_sources = load_fx_rate_sources(_conn)
                all_sources: List[Dict[str, Any]] = instrument_sources + fx_sources

                priorities = build_priority_map(instrument_sources, "instrument_code")
                fx_priorities = build_priority_map(fx_sources, "fx_code")
                for key, items in fx_priorities.items():
                    priorities.setdefault(key, []).extend(items)
                self.quote_router.set_priorities(priorities)

                # Targets: (ls_item, key). key bleibt stabil fürs Frontend (Instrument-/FX-Code).
                targets: List[tuple[str, str]] = []
                seen_keys: set[str] = {str(i.get("code")) for i in instruments if i.get("code")}
                tradegate_map: Dict[str, str] = {}

                ls_seen: set[str] = set()
                for src in all_sources:
                    source = str(src.get("source") or "").strip().lower()
                    key = (src.get("instrument_code") or src.get("fx_code") or "").strip()
                    if not source or not key:
                        continue
                    source_code = str(src.get("source_code") or "").strip().upper()
                    if source == SOURCE_LIGHTSTREAMER:
                        ls_item = ls_source_to_item(source_code)
                        if ls_item and ls_item not in ls_seen:
                            ls_seen.add(ls_item)
                            targets.append((ls_item, key))
                    elif source == SOURCE_TRADEGATE:
                        if is_isin(source_code) and key not in tradegate_map:
                            tradegate_map[key] = source_code
                        elif source_code and not is_isin(source_code):
                            logger.warning("Tradegate source_code ist keine ISIN: %s", source_code)

                items = [t[0] for t in targets]
                idx_to_key = {idx + 1: t[1] for idx, t in enumerate(targets)}

                if items == last_items and not self._dirty.is_set():
                    # keep current session
                    await asyncio.sleep(0.5)
                    continue

                # reset dirty
                self._dirty.clear()
                last_items = items
                all_keys = list(seen_keys)
                self.quote_router.trim_keys(all_keys)
                if self.tradegate_poller:
                    self.tradegate_poller.set_mapping(tradegate_map)

                if not self.lightstreamer_enabled:
                    if not items:
                        await self.broadcast(
                            {"type": "status", "level": "warn", "message": "Keine Positionen/Watchlist vorhanden – kein Stream."}
                        )
                    bids = self._bids_as_optional()
                    prices = self._prices_as_optional()
                    watch_prices = self._watch_prices_as_optional()
                    await self.broadcast(
                        {
                            "type": "snapshot",
                            "valuations": compute_all_valuations_from_prices(
                                prices,
                                bids,
                                self.quote_router.best_price_source,
                            ),
                            "fx_rates": compute_fx_rates_from_prices(
                                prices,
                                self.quote_router.best_price_source,
                            ),
                            "watchlist": compute_watchlist_from_prices(
                                watch_prices,
                                self.quote_router.best_watch_fields,
                                self.quote_router.best_watch_source,
                            ),
                        }
                    )
                    await asyncio.sleep(0.5)
                    continue

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
                            changed = self.quote_router.update_from_source(
                                source=SOURCE_LIGHTSTREAMER,
                                key=key,
                                bid=evt.get("bid"),
                                price=evt.get("price"),
                                price_field=evt.get("price_field"),
                                watch_price=evt.get("watch_price"),
                                watch_field=evt.get("watch_field"),
                            )
                            if changed:
                                await self._broadcast_best_quote(
                                    key,
                                    quotetime=evt.get("quotetime"),
                                    isin=evt.get("isin"),
                                    symbol=evt.get("symbol"),
                                )
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
                prices = self._prices_as_optional()
                watch_prices = self._watch_prices_as_optional()
                await self.broadcast(
                    {
                        "type": "snapshot",
                        "valuations": compute_all_valuations_from_prices(
                            prices,
                            bids,
                            self.quote_router.best_price_source,
                        ),
                        "fx_rates": compute_fx_rates_from_prices(
                            prices,
                            self.quote_router.best_price_source,
                        ),
                        "watchlist": compute_watchlist_from_prices(
                            watch_prices,
                            self.quote_router.best_watch_fields,
                            self.quote_router.best_watch_source,
                        ),
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
    prices = stream_manager._prices_as_optional()
    watch_prices = stream_manager._watch_prices_as_optional()
    portfolios = compute_all_valuations_from_prices(
        prices,
        bids,
        stream_manager.quote_router.best_price_source,
    )
    watchlist = compute_watchlist_from_prices(
        watch_prices,
        stream_manager.quote_router.best_watch_fields,
        stream_manager.quote_router.best_watch_source,
    )
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
    prices: Dict[str, Optional[float]],
    bids: Dict[str, Optional[float]],
    price_sources: Dict[str, str],
    fx_rates: Dict[tuple[str, str], Dict[str, Any]],
    valued_at: str,
    timeout_s: float,
) -> Dict[str, Any]:
    out_positions: List[Dict[str, Any]] = []
    total_mv = 0.0
    total_cb = 0.0
    total_missing_fx = 0

    portfolio_currency = _normalize_currency((portfolio or {}).get("currency") or "EUR")
    for p in positions:
        code = p.get("instrument_code") or p.get("isin")
        qty = float(p["quantity"])
        entry = float(p["entry_price"])
        bid = bids.get(code)
        price = prices.get(code)
        if price is None and bid is not None:
            price = bid
        price_source = (price_sources or {}).get(code)
        if price_source is None and bid is not None:
            price_source = "bid"
        pos_currency = _normalize_currency(p.get("position_currency") or p.get("currency") or p.get("instrument_currency") or "EUR")

        cost_basis_local = entry * qty
        market_value_local = (price * qty) if price is not None else None
        pnl_local = (market_value_local - cost_basis_local) if market_value_local is not None else None

        fx_rate: Optional[float] = None
        fx_instrument_code: Optional[str] = None
        fx_inverted = False
        fx_missing = False

        if pos_currency != portfolio_currency:
            fx_info = fx_rates.get((pos_currency, portfolio_currency))
            if fx_info:
                fx_rate = float(fx_info["rate"])
                fx_instrument_code = fx_info.get("instrument_code")
            else:
                fx_info = fx_rates.get((portfolio_currency, pos_currency))
                if fx_info and fx_info.get("rate"):
                    fx_rate = 1.0 / float(fx_info["rate"])
                    fx_instrument_code = fx_info.get("instrument_code")
                    fx_inverted = True
            if fx_rate is None:
                fx_missing = True

        market_value = market_value_local
        cost_basis = cost_basis_local
        pnl = pnl_local
        pnl_pct = (pnl / cost_basis) if (pnl is not None and cost_basis) else None

        if pos_currency == portfolio_currency:
            if market_value_local is not None:
                total_mv += market_value_local
            total_cb += cost_basis_local
        elif fx_rate is not None:
            if market_value_local is not None:
                total_mv += market_value_local * fx_rate
            total_cb += cost_basis_local * fx_rate
        else:
            total_missing_fx += 1

        out_positions.append(
            {
                "id": p.get("id"),
                "name": (p.get("position_name") or "").strip() or None,
                "instrument_id": p.get("instrument_id"),
                "instrument_code": code,
                "instrument_name": p.get("instrument_name"),
                "instrument_isin": p.get("instrument_isin"),
                "instrument_ls_item": p.get("instrument_ls_item"),
                "price": price,
                "price_source": price_source,
                "quantity": qty,
                "entry_price": entry,
                "bid": bid,
                "market_value": market_value,
                "market_value_local": market_value_local,
                "cost_basis": cost_basis,
                "cost_basis_local": cost_basis_local,
                "pnl": pnl,
                "pnl_local": pnl_local,
                "pnl_pct": pnl_pct,
                "currency": pos_currency,
                "position_currency": pos_currency,
                "fx_rate": fx_rate,
                "fx_instrument_code": fx_instrument_code,
                "fx_inverted": fx_inverted,
                "fx_missing": fx_missing,
            }
        )

    total_pnl = total_mv - total_cb
    total_pnl_pct = (total_pnl / total_cb) if total_cb else None

    return {
        "portfolio": dict(portfolio),
        "currency": portfolio_currency,
        "valued_at": valued_at,
        "positions": out_positions,
        "totals": {
            "market_value": total_mv,
            "cost_basis": total_cb,
            "pnl": total_pnl,
            "pnl_pct": total_pnl_pct,
        },
        "meta": {"quote_timeout_s": timeout_s, "fx_missing_count": total_missing_fx},
    }


# ----------------- API Models -----------------


class PortfolioCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    currency: str = Field(default="EUR", min_length=3, max_length=8)


class InstrumentCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=200)
    currency: str = Field(default="EUR", min_length=3, max_length=8)
    isin: Optional[str] = Field(default=None, min_length=12, max_length=12)
    ls_item: Optional[str] = Field(default=None, max_length=64)


class InstrumentUpdate(BaseModel):
    code: Optional[str] = Field(default=None, min_length=1, max_length=64)
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)
    isin: Optional[str] = Field(default=None, min_length=12, max_length=12)
    ls_item: Optional[str] = Field(default=None, max_length=64)


class FxRateCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=200)
    base_currency: str = Field(min_length=3, max_length=8)
    quote_currency: str = Field(min_length=3, max_length=8)
    isin: Optional[str] = Field(default=None, min_length=12, max_length=12)
    ls_item: Optional[str] = Field(default=None, max_length=64)


class FxRateUpdate(BaseModel):
    code: Optional[str] = Field(default=None, min_length=1, max_length=64)
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    base_currency: Optional[str] = Field(default=None, min_length=3, max_length=8)
    quote_currency: Optional[str] = Field(default=None, min_length=3, max_length=8)
    isin: Optional[str] = Field(default=None, min_length=12, max_length=12)
    ls_item: Optional[str] = Field(default=None, max_length=64)


class SourceCreate(BaseModel):
    source: str = Field(min_length=1, max_length=32)
    source_code: str = Field(min_length=1, max_length=128)
    priority: int = Field(default=100, ge=0, le=1000)


class SourceUpdate(BaseModel):
    source_code: Optional[str] = Field(default=None, min_length=1, max_length=128)
    priority: Optional[int] = Field(default=None, ge=0, le=1000)


class PositionIn(BaseModel):
    instrument_id: int
    name: Optional[str] = Field(default=None, max_length=200)
    quantity: float = Field(gt=0)
    entry_price: float = Field(gt=0)
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)


class PortfolioOut(BaseModel):
    id: int
    name: str


class WatchItemCreate(BaseModel):
    instrument_id: int
    label: Optional[str] = Field(default=None, max_length=200)
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)


class WatchItemUpdate(BaseModel):
    instrument_id: Optional[int] = None
    label: Optional[str] = Field(default=None, max_length=200)
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)


class PortfolioUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)


# ----------------- FastAPI App -----------------


app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

_conn = connect_db()
init_db(_conn)


def _normalize_code(code: str) -> str:
    code = (code or "").strip()
    if is_isin(code) or is_ls_item(code):
        return code.upper()
    return code


def _normalize_currency(value: Optional[str], default: Optional[str] = "EUR") -> str:
    v = (value or "").strip().upper()
    if v:
        return v
    return (default or "").strip().upper()


def _normalize_source(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_source_code(value: str) -> str:
    return (value or "").strip().upper()




def _get_instrument(instrument_id: int) -> Dict[str, Any]:
    cur = _conn.execute(
        "SELECT id, code, name, currency, type, base_currency, quote_currency, isin, ls_item FROM instruments WHERE id=?",
        (instrument_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Instrument nicht gefunden")
    data = dict(row)
    if (data.get("type") or "asset") == "fx" or (data.get("base_currency") and data.get("quote_currency")):
        raise HTTPException(status_code=400, detail="FX-Instrumente duerfen nicht in Portfolios/Watchlist verwendet werden")
    return data


def _get_fx_rate(fx_id: int) -> Dict[str, Any]:
    cur = _conn.execute(
        "SELECT id, code, name, base_currency, quote_currency, isin, ls_item FROM fx_rates WHERE id=?",
        (fx_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="FX-Instrument nicht gefunden")
    return dict(row)

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
    if stream_manager.tradegate_poller:
        stream_manager.tradegate_poller.stop()
    _mqtt_disconnect()


@app.get("/")
async def index():
    return FileResponse("app/static/dashboard.html")


@app.get("/manage")
async def manage():
    return FileResponse("app/static/manage.html")


@app.get("/api/instruments")
async def list_instruments() -> List[Dict[str, Any]]:
    cur = _conn.execute(
        """
        SELECT id, code, name, currency, isin, ls_item
        FROM instruments
        WHERE (type IS NULL OR type != 'fx')
          AND (base_currency IS NULL OR quote_currency IS NULL)
        ORDER BY id DESC
        """
    )
    return [dict(r) for r in cur.fetchall()]


@app.post("/api/instruments", status_code=201)
async def create_instrument(body: InstrumentCreate) -> Dict[str, Any]:
    code = _normalize_code(body.code)
    if not code:
        raise HTTPException(status_code=400, detail="Instrument-Code darf nicht leer sein")
    name = (body.name or code).strip() or code
    currency = _normalize_currency(body.currency)
    isin = None
    if body.isin:
        isin = validate_isin(body.isin)
    elif is_isin(code):
        isin = code.upper()
    ls_item = (body.ls_item or "").strip()
    if not ls_item and is_ls_item(code):
        ls_item = code
    if ls_item:
        ls_item = ls_item.strip()
    try:
        cur = _conn.execute(
            "INSERT INTO instruments(code, name, currency, type, isin, ls_item) VALUES (?,?,?,?,?,?)",
            (code, name, currency, "asset", isin, ls_item or None),
        )
        _conn.commit()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Instrument konnte nicht gespeichert werden: {e}")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return _get_instrument(int(cur.lastrowid))


@app.put("/api/instruments/{instrument_id}")
async def update_instrument(instrument_id: int, body: InstrumentUpdate) -> Dict[str, Any]:
    instrument = _get_instrument(instrument_id)
    fields: List[str] = []
    values: List[Any] = []
    new_code = instrument["code"]
    if body.code is not None:
        code = _normalize_code(body.code)
        if not code:
            raise HTTPException(status_code=400, detail="Instrument-Code darf nicht leer sein")
        new_code = code
        if code != instrument["code"]:
            conflict = _conn.execute(
                "SELECT 1 FROM watchlist WHERE isin=? AND instrument_id<>?",
                (code, instrument_id),
            ).fetchone()
            if conflict:
                raise HTTPException(status_code=400, detail="Instrument-Code kollidiert mit Watchlist-Eintrag")
        fields.append("code=?")
        values.append(code)
    if body.name is not None:
        fields.append("name=?")
        values.append((body.name or "").strip() or new_code)
    if body.currency is not None:
        fields.append("currency=?")
        values.append(_normalize_currency(body.currency))
    if body.isin is not None:
        if body.isin:
            fields.append("isin=?")
            values.append(validate_isin(body.isin))
        else:
            fields.append("isin=?")
            values.append(None)
    if body.ls_item is not None:
        ls_item = (body.ls_item or "").strip()
        fields.append("ls_item=?")
        values.append(ls_item or None)

    if fields:
        values.append(instrument_id)
        try:
            _conn.execute(f"UPDATE instruments SET {', '.join(fields)} WHERE id=?", tuple(values))
            if new_code != instrument["code"]:
                _conn.execute("UPDATE positions SET isin=? WHERE instrument_id=?", (new_code, instrument_id))
                _conn.execute("UPDATE watchlist SET isin=? WHERE instrument_id=?", (new_code, instrument_id))
            _conn.commit()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Instrument konnte nicht aktualisiert werden: {e}")
        stream_manager.mark_dirty()
        _publish_mqtt_snapshot()

    return _get_instrument(instrument_id)


@app.delete("/api/instruments/{instrument_id}", status_code=204)
async def delete_instrument(instrument_id: int) -> None:
    _get_instrument(instrument_id)
    used_positions = _conn.execute(
        "SELECT 1 FROM positions WHERE instrument_id=? LIMIT 1", (instrument_id,)
    ).fetchone()
    if used_positions:
        raise HTTPException(status_code=400, detail="Instrument wird in Positionen verwendet")
    used_watchlist = _conn.execute(
        "SELECT 1 FROM watchlist WHERE instrument_id=? LIMIT 1", (instrument_id,)
    ).fetchone()
    if used_watchlist:
        raise HTTPException(status_code=400, detail="Instrument ist in der Watchlist")
    _conn.execute("DELETE FROM instruments WHERE id=?", (instrument_id,))
    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return None


@app.get("/api/instruments/{instrument_id}/sources")
async def list_instrument_sources(instrument_id: int) -> List[Dict[str, Any]]:
    _get_instrument(instrument_id)
    cur = _conn.execute(
        "SELECT id, source, source_code, priority FROM instrument_sources WHERE instrument_id=? ORDER BY priority ASC, id ASC",
        (instrument_id,),
    )
    return [dict(r) for r in cur.fetchall()]


@app.post("/api/instruments/{instrument_id}/sources", status_code=201)
async def add_instrument_source(instrument_id: int, body: SourceCreate) -> Dict[str, Any]:
    _get_instrument(instrument_id)
    source = _normalize_source(body.source)
    source_code = _normalize_source_code(body.source_code)
    try:
        cur = _conn.execute(
            "INSERT INTO instrument_sources(instrument_id, source, source_code, priority) VALUES (?,?,?,?)",
            (instrument_id, source, source_code, body.priority),
        )
        _conn.commit()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Kursquelle konnte nicht gespeichert werden: {e}")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return {
        "id": int(cur.lastrowid),
        "source": source,
        "source_code": source_code,
        "priority": body.priority,
    }


@app.put("/api/instruments/{instrument_id}/sources/{source_id}")
async def update_instrument_source(instrument_id: int, source_id: int, body: SourceUpdate) -> Dict[str, Any]:
    _get_instrument(instrument_id)
    row = _conn.execute(
        "SELECT id FROM instrument_sources WHERE id=? AND instrument_id=?",
        (source_id, instrument_id),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Kursquelle nicht gefunden")
    fields: List[str] = []
    values: List[Any] = []
    if body.source_code is not None:
        fields.append("source_code=?")
        values.append(_normalize_source_code(body.source_code))
    if body.priority is not None:
        fields.append("priority=?")
        values.append(body.priority)
    if fields:
        values.extend([source_id, instrument_id])
        try:
            _conn.execute(
                f"UPDATE instrument_sources SET {', '.join(fields)} WHERE id=? AND instrument_id=?",
                tuple(values),
            )
            _conn.commit()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Kursquelle konnte nicht aktualisiert werden: {e}")
        stream_manager.mark_dirty()
        _publish_mqtt_snapshot()
    out = _conn.execute(
        "SELECT id, source, source_code, priority FROM instrument_sources WHERE id=?",
        (source_id,),
    ).fetchone()
    return dict(out)


@app.delete("/api/instruments/{instrument_id}/sources/{source_id}", status_code=204)
async def delete_instrument_source(instrument_id: int, source_id: int) -> None:
    _get_instrument(instrument_id)
    cur = _conn.execute(
        "DELETE FROM instrument_sources WHERE id=? AND instrument_id=?",
        (source_id, instrument_id),
    )
    _conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Kursquelle nicht gefunden")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return None


@app.get("/api/fx-rates")
async def list_fx_rates() -> List[Dict[str, Any]]:
    cur = _conn.execute(
        "SELECT id, code, name, base_currency, quote_currency, isin, ls_item FROM fx_rates ORDER BY id DESC"
    )
    return [dict(r) for r in cur.fetchall()]


@app.post("/api/fx-rates", status_code=201)
async def create_fx_rate(body: FxRateCreate) -> Dict[str, Any]:
    code = _normalize_code(body.code)
    if not code:
        raise HTTPException(status_code=400, detail="FX-Code darf nicht leer sein")
    name = (body.name or code).strip() or code
    base_currency = _normalize_currency(body.base_currency, default=None)
    quote_currency = _normalize_currency(body.quote_currency, default=None)
    if not base_currency or not quote_currency:
        raise HTTPException(status_code=400, detail="FX braucht Base- und Quote-Waehrung")
    isin = None
    if body.isin:
        isin = validate_isin(body.isin)
    elif is_isin(code):
        isin = code.upper()
    ls_item = (body.ls_item or "").strip() or None
    try:
        cur = _conn.execute(
            "INSERT INTO fx_rates(code, name, base_currency, quote_currency, isin, ls_item) VALUES (?,?,?,?,?,?)",
            (code, name, base_currency, quote_currency, isin, ls_item),
        )
        _conn.commit()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"FX-Instrument konnte nicht gespeichert werden: {e}")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return _get_fx_rate(int(cur.lastrowid))


@app.put("/api/fx-rates/{fx_id}")
async def update_fx_rate(fx_id: int, body: FxRateUpdate) -> Dict[str, Any]:
    fx = _get_fx_rate(fx_id)
    fields: List[str] = []
    values: List[Any] = []
    new_code = fx["code"]
    if body.code is not None:
        code = _normalize_code(body.code)
        if not code:
            raise HTTPException(status_code=400, detail="FX-Code darf nicht leer sein")
        new_code = code
        fields.append("code=?")
        values.append(code)
    if body.name is not None:
        fields.append("name=?")
        values.append((body.name or "").strip() or new_code)
    if body.base_currency is not None:
        base_currency = _normalize_currency(body.base_currency, default=None)
        if not base_currency:
            raise HTTPException(status_code=400, detail="Base-Waehrung fehlt")
        fields.append("base_currency=?")
        values.append(base_currency)
    if body.quote_currency is not None:
        quote_currency = _normalize_currency(body.quote_currency, default=None)
        if not quote_currency:
            raise HTTPException(status_code=400, detail="Quote-Waehrung fehlt")
        fields.append("quote_currency=?")
        values.append(quote_currency)
    if body.isin is not None:
        if body.isin:
            fields.append("isin=?")
            values.append(validate_isin(body.isin))
        else:
            fields.append("isin=?")
            values.append(None)
    if body.ls_item is not None:
        ls_item = (body.ls_item or "").strip()
        fields.append("ls_item=?")
        values.append(ls_item or None)
    if fields:
        values.append(fx_id)
        try:
            _conn.execute(f"UPDATE fx_rates SET {', '.join(fields)} WHERE id=?", tuple(values))
            _conn.commit()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"FX-Instrument konnte nicht aktualisiert werden: {e}")
        stream_manager.mark_dirty()
        _publish_mqtt_snapshot()
    return _get_fx_rate(fx_id)


@app.delete("/api/fx-rates/{fx_id}", status_code=204)
async def delete_fx_rate(fx_id: int) -> None:
    _get_fx_rate(fx_id)
    _conn.execute("DELETE FROM fx_rates WHERE id=?", (fx_id,))
    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return None


@app.get("/api/fx-rates/{fx_id}/sources")
async def list_fx_rate_sources(fx_id: int) -> List[Dict[str, Any]]:
    _get_fx_rate(fx_id)
    cur = _conn.execute(
        "SELECT id, source, source_code, priority FROM fx_rate_sources WHERE fx_rate_id=? ORDER BY priority ASC, id ASC",
        (fx_id,),
    )
    return [dict(r) for r in cur.fetchall()]


@app.post("/api/fx-rates/{fx_id}/sources", status_code=201)
async def add_fx_rate_source(fx_id: int, body: SourceCreate) -> Dict[str, Any]:
    _get_fx_rate(fx_id)
    source = _normalize_source(body.source)
    source_code = _normalize_source_code(body.source_code)
    try:
        cur = _conn.execute(
            "INSERT INTO fx_rate_sources(fx_rate_id, source, source_code, priority) VALUES (?,?,?,?)",
            (fx_id, source, source_code, body.priority),
        )
        _conn.commit()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Kursquelle konnte nicht gespeichert werden: {e}")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return {
        "id": int(cur.lastrowid),
        "source": source,
        "source_code": source_code,
        "priority": body.priority,
    }


@app.put("/api/fx-rates/{fx_id}/sources/{source_id}")
async def update_fx_rate_source(fx_id: int, source_id: int, body: SourceUpdate) -> Dict[str, Any]:
    _get_fx_rate(fx_id)
    row = _conn.execute(
        "SELECT id FROM fx_rate_sources WHERE id=? AND fx_rate_id=?",
        (source_id, fx_id),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Kursquelle nicht gefunden")
    fields: List[str] = []
    values: List[Any] = []
    if body.source_code is not None:
        fields.append("source_code=?")
        values.append(_normalize_source_code(body.source_code))
    if body.priority is not None:
        fields.append("priority=?")
        values.append(body.priority)
    if fields:
        values.extend([source_id, fx_id])
        try:
            _conn.execute(
                f"UPDATE fx_rate_sources SET {', '.join(fields)} WHERE id=? AND fx_rate_id=?",
                tuple(values),
            )
            _conn.commit()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Kursquelle konnte nicht aktualisiert werden: {e}")
        stream_manager.mark_dirty()
        _publish_mqtt_snapshot()
    out = _conn.execute(
        "SELECT id, source, source_code, priority FROM fx_rate_sources WHERE id=?",
        (source_id,),
    ).fetchone()
    return dict(out)


@app.delete("/api/fx-rates/{fx_id}/sources/{source_id}", status_code=204)
async def delete_fx_rate_source(fx_id: int, source_id: int) -> None:
    _get_fx_rate(fx_id)
    cur = _conn.execute(
        "DELETE FROM fx_rate_sources WHERE id=? AND fx_rate_id=?",
        (source_id, fx_id),
    )
    _conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Kursquelle nicht gefunden")
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return None


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
    instrument = _get_instrument(body.instrument_id)
    label = (body.label or "").strip() or None
    currency = _normalize_currency(body.currency, default=instrument.get("currency") or "EUR")
    try:
        cur = _conn.execute(
            "INSERT INTO watchlist(instrument_id, isin, label, currency) VALUES (?,?,?,?)",
            (instrument["id"], instrument["code"], label, currency),
        )
        _conn.commit()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Watchlist-Eintrag konnte nicht gespeichert werden: {e}")

    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return [w for w in load_watchlist(_conn) if int(w["id"]) == int(cur.lastrowid)][0]


@app.put("/api/watchlist/{item_id}")
async def update_watchlist_item(item_id: int, body: WatchItemUpdate) -> Dict[str, Any]:
    row = _conn.execute("SELECT id, instrument_id FROM watchlist WHERE id=?", (item_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Watchlist-Eintrag nicht gefunden")
    fields: List[str] = []
    values: List[Any] = []
    instrument = None
    if body.instrument_id is not None:
        instrument = _get_instrument(body.instrument_id)
        fields.append("instrument_id=?")
        values.append(instrument["id"])
        fields.append("isin=?")
        values.append(instrument["code"])
    if body.label is not None:
        label = (body.label or "").strip() or None
        fields.append("label=?")
        values.append(label)
    if body.currency is not None:
        fallback = instrument.get("currency") if instrument else "EUR"
        fields.append("currency=?")
        values.append(_normalize_currency(body.currency, default=fallback))
    if fields:
        values.append(item_id)
        try:
            _conn.execute(f"UPDATE watchlist SET {', '.join(fields)} WHERE id=?", tuple(values))
            _conn.commit()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Watchlist-Eintrag konnte nicht aktualisiert werden: {e}")
        stream_manager.mark_dirty()
        _publish_mqtt_snapshot()
    return [w for w in load_watchlist(_conn) if int(w["id"]) == int(item_id)][0]


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
    # Für Dashboard: Snapshot aus dem aktuellen Kurs-Cache (beste Quelle).
    # Fallback: falls noch kein Kurs gesehen wurde, werden Preise als None angezeigt.
    stream_manager.start()
    bids = stream_manager._bids_as_optional()
    prices = stream_manager._prices_as_optional()
    return compute_all_valuations_from_prices(
        prices,
        bids,
        stream_manager.quote_router.best_price_source,
    )


@app.get("/api/portfolios/{portfolio_id}")
async def get_portfolio(portfolio_id: int) -> Dict[str, Any]:
    cur = _conn.execute("SELECT id, name, currency FROM portfolios WHERE id=?", (portfolio_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    cur2 = _conn.execute(
        """
        SELECT
            pos.id,
            pos.instrument_id,
            pos.name AS position_name,
            pos.quantity,
            pos.entry_price,
            pos.currency AS position_currency,
            instr.code AS instrument_code,
            instr.name AS instrument_name,
            instr.currency AS instrument_currency,
            instr.isin AS instrument_isin,
            instr.ls_item AS instrument_ls_item
        FROM positions pos
        JOIN instruments instr ON instr.id = pos.instrument_id
        WHERE pos.portfolio_id=?
        ORDER BY pos.id ASC
        """,
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
    portfolio_currency = _normalize_currency(prow["currency"] or "EUR")

    cleaned: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for p in positions:
        instr = _get_instrument(p.instrument_id)
        if instr["id"] in seen:
            raise HTTPException(status_code=400, detail=f"Doppeltes Instrument im Request: {instr['code']}")
        seen.add(instr["id"])
        currency = _normalize_currency(p.currency, default=instr.get("currency") or portfolio_currency)
        cleaned.append(
            {
                "instrument_id": instr["id"],
                "instrument_code": instr["code"],
                "name": (p.name or "").strip() or None,
                "quantity": p.quantity,
                "entry_price": p.entry_price,
                "currency": currency,
            }
        )

    with _conn:
        _conn.execute("DELETE FROM positions WHERE portfolio_id=?", (portfolio_id,))
        for p in cleaned:
            _conn.execute(
                "INSERT INTO positions(portfolio_id, instrument_id, isin, name, quantity, entry_price, currency) VALUES (?,?,?,?,?,?,?)",
                (
                    portfolio_id,
                    p["instrument_id"],
                    p["instrument_code"],
                    p["name"],
                    p["quantity"],
                    p["entry_price"],
                    p["currency"],
                ),
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
        """
        SELECT
            pos.id,
            pos.instrument_id,
            pos.name AS position_name,
            pos.quantity,
            pos.entry_price,
            pos.currency AS position_currency,
            instr.code AS instrument_code,
            instr.name AS instrument_name,
            instr.currency AS instrument_currency,
            instr.isin AS instrument_isin,
            instr.ls_item AS instrument_ls_item
        FROM positions pos
        JOIN instruments instr ON instr.id = pos.instrument_id
        WHERE pos.portfolio_id=?
        ORDER BY pos.id ASC
        """,
        (portfolio_id,),
    )
    positions = [dict(r) for r in cur2.fetchall()]
    valued_at = now_iso()
    if not positions:
        return compute_valuation(
            dict(portfolio),
            [],
            {},
            {},
            {},
            {},
            valued_at=valued_at,
            timeout_s=0.0,
        )

    # Einzelbewertung nutzt den Kurs-Cache (keine eigene LS-Session), damit die Quellen-Prioritaet konsistent bleibt.
    stream_manager.start()
    bids = stream_manager._bids_as_optional()
    prices = stream_manager._prices_as_optional()
    fx_rates = build_fx_rates(prices, load_stream_instruments(_conn))
    return compute_valuation(
        dict(portfolio),
        positions,
        prices,
        bids,
        stream_manager.quote_router.best_price_source,
        fx_rates,
        valued_at=valued_at,
        timeout_s=0.0,
    )


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
