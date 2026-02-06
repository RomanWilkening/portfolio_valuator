import asyncio
import logging
import random
import re
from dataclasses import dataclass
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
    SOURCE_BITFINEX,
    SOURCE_LIGHTSTREAMER,
    SOURCE_TRADEGATE,
    BitfinexSettings,
    BitfinexStream,
    QuoteRouter,
    TradegatePoller,
    TradegateSettings,
    is_isin,
    normalize_bitfinex_symbol,
    parse_source_priority,
)
from app.banking_bridge import BankingBridgeClient, BankingBridgeSettings
from app.settings import get_bool, get_float, get_int, get_setting, set_bool, set_setting

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("portfolio-valuator")


# ----------------- Konfiguration (Defaults) -----------------

DEFAULT_LS_WSS_URL = "wss://push.bnpparibas.com/lightstreamer"
DEFAULT_LS_SUBPROTOCOL = "TLCP-2.5.0.lightstreamer.com"
DEFAULT_LS_ADAPTER_SET = "SmarthouseFeed"
DEFAULT_LS_DATA_ADAPTER = "MDS5"
DEFAULT_LS_CID = "pcYgxn8m8 feOojyA1V661f3g2.pz482h95IL5h"
DEFAULT_LS_ITEM_TEMPLATE = "X0000010800{isin}"
DEFAULT_LS_ORIGIN = "https://derivate.bnpparibas.com"
DEFAULT_LS_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_LS_RECONNECT_MIN_S = 1.0
DEFAULT_LS_RECONNECT_MAX_S = 30.0
DEFAULT_LS_RECV_TIMEOUT_S = 35.0
DEFAULT_LS_STALE_RESTART_S = 90.0

DEFAULT_TRADEGATE_URL_TEMPLATE = "https://www.tradegate.de/refresh.php?isin={isin}"
DEFAULT_TRADEGATE_TIMEOUT_S = 5.0
DEFAULT_TRADEGATE_POLL_S = 10.0
DEFAULT_TRADEGATE_USER_AGENT = "portfolio-valuator/1.0"

DEFAULT_BITFINEX_WSS_URL = "wss://api-pub.bitfinex.com/ws/2"
DEFAULT_BITFINEX_RECONNECT_MIN_S = 1.0
DEFAULT_BITFINEX_RECONNECT_MAX_S = 30.0

DEFAULT_QUOTE_SOURCE_PRIORITY = "lightstreamer,tradegate,bitfinex"
_KNOWN_SOURCES = {SOURCE_LIGHTSTREAMER, SOURCE_TRADEGATE, SOURCE_BITFINEX}

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

_conn = connect_db()
init_db(_conn)


@dataclass(frozen=True)
class LightstreamerSettings:
    wss_url: str
    subprotocol: Optional[str]
    adapter_set: str
    data_adapter: str
    cid: str
    item_template: str
    origin: Optional[str]
    user_agent: str
    reconnect_min_s: float
    reconnect_max_s: float
    recv_timeout_s: float
    stale_restart_s: float


def _get_setting_str(key: str, default: str) -> str:
    try:
        return (get_setting(_conn, key, default) or "").strip() or default
    except Exception:
        return default


def load_ls_settings() -> LightstreamerSettings:
    origin = _get_setting_str("ls_origin", DEFAULT_LS_ORIGIN)
    subprotocol = _get_setting_str("ls_subprotocol", DEFAULT_LS_SUBPROTOCOL)
    return LightstreamerSettings(
        wss_url=_get_setting_str("ls_wss_url", DEFAULT_LS_WSS_URL),
        subprotocol=subprotocol or None,
        adapter_set=_get_setting_str("ls_adapter_set", DEFAULT_LS_ADAPTER_SET),
        data_adapter=_get_setting_str("ls_data_adapter", DEFAULT_LS_DATA_ADAPTER),
        cid=_get_setting_str("ls_cid", DEFAULT_LS_CID),
        item_template=_get_setting_str("ls_item_template", DEFAULT_LS_ITEM_TEMPLATE),
        origin=origin or None,
        user_agent=_get_setting_str("ls_user_agent", DEFAULT_LS_USER_AGENT),
        reconnect_min_s=get_float(_conn, "ls_reconnect_min_s", DEFAULT_LS_RECONNECT_MIN_S),
        reconnect_max_s=get_float(_conn, "ls_reconnect_max_s", DEFAULT_LS_RECONNECT_MAX_S),
        recv_timeout_s=get_float(_conn, "ls_recv_timeout_s", DEFAULT_LS_RECV_TIMEOUT_S),
        stale_restart_s=get_float(_conn, "ls_stale_restart_s", DEFAULT_LS_STALE_RESTART_S),
    )


def load_tradegate_settings_db() -> TradegateSettings:
    return TradegateSettings(
        url_template=_get_setting_str("tradegate_url_template", DEFAULT_TRADEGATE_URL_TEMPLATE),
        timeout_s=get_float(_conn, "tradegate_timeout_s", DEFAULT_TRADEGATE_TIMEOUT_S),
        poll_s=get_float(_conn, "tradegate_poll_s", DEFAULT_TRADEGATE_POLL_S),
        user_agent=_get_setting_str("tradegate_user_agent", DEFAULT_TRADEGATE_USER_AGENT),
    )


def load_bitfinex_settings_db() -> BitfinexSettings:
    return BitfinexSettings(
        wss_url=_get_setting_str("bitfinex_wss_url", DEFAULT_BITFINEX_WSS_URL),
        reconnect_min_s=get_float(_conn, "bitfinex_reconnect_min_s", DEFAULT_BITFINEX_RECONNECT_MIN_S),
        reconnect_max_s=get_float(_conn, "bitfinex_reconnect_max_s", DEFAULT_BITFINEX_RECONNECT_MAX_S),
    )


def load_source_priority() -> List[str]:
    raw = _get_setting_str("quote_source_priority", DEFAULT_QUOTE_SOURCE_PRIORITY)
    priorities = parse_source_priority(raw)
    unknown = [s for s in priorities if s not in _KNOWN_SOURCES]
    if unknown:
        logger.warning("Unbekannte Kursquelle in quote_source_priority: %s", ", ".join(unknown))
    return priorities


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
    tmpl = (load_ls_settings().item_template or "").strip() or "X0000010800{isin}"
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


async def _ws_connect(settings: LightstreamerSettings) -> Any:
    """
    Robust gegen Unterschiede im Server-Handshake:
    - ggf. ohne UA
    - ggf. ohne Subprotocol
    - permessage-deflate deaktiviert
    """
    subprotocols: List[Optional[str]] = [settings.subprotocol, None]
    origins: List[Optional[str]] = [settings.origin, None]

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
                    headers["User-Agent"] = settings.user_agent

                try:
                    ws = await websockets.connect(
                        settings.wss_url,
                        **kwargs,
                        additional_headers=headers or None,
                    )
                    logger.info("LS WS connected (proto=%s origin=%s ua=%s)", proto or "<none>", origin or "<none>", send_ua)
                    return ws
                except TypeError:
                    # Fallback für ältere websockets Signaturen
                    try:
                        ws = await websockets.connect(
                            settings.wss_url,
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
    def __init__(self, settings: LightstreamerSettings) -> None:
        self.settings = settings
        self.websocket: Optional[Any] = None
        self.session_id: Optional[str] = None
        self.sub_id: int = 1
        self.req_id: int = 1
        self.item_state: Dict[int, Dict[str, Optional[str]]] = {}

    async def connect(self) -> None:
        self.websocket = await _ws_connect(self.settings)

        params = (
            f"LS_adapter_set={quote(self.settings.adapter_set)}"
            f"&LS_user="
            f"&LS_cid={quote(self.settings.cid)}"
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
            f"&LS_data_adapter={quote(self.settings.data_adapter)}"
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
    cur = conn.execute("SELECT id, name, currency FROM portfolios ORDER BY sort_order ASC, id ASC")
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
        ORDER BY pos.portfolio_id DESC, pos.sort_order ASC, pos.id ASC
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
        ORDER BY w.sort_order ASC, w.id ASC
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
            UNION
            SELECT instrument_id FROM instrument_sources
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
        WHERE fx.id IN (
            SELECT fx_rate_id FROM fx_rate_sources
        )
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

        self.quote_router = QuoteRouter(load_source_priority())
        self.lightstreamer_enabled = SOURCE_LIGHTSTREAMER in self.quote_router.default_priority
        self.tradegate_poller: Optional[TradegatePoller] = None
        self.bitfinex_stream: Optional[BitfinexStream] = BitfinexStream(
            settings=load_bitfinex_settings_db(),
            router=self.quote_router,
            on_best_update=self._on_source_update,
        )
        self.instrument_by_code: Dict[str, Dict[str, Any]] = {}
        self.debug_mappings: Dict[str, Any] = {}
        self.debug_mappings_updated_at: Optional[str] = None
        if SOURCE_TRADEGATE in self.quote_router.default_priority:
            self.tradegate_poller = TradegatePoller(
                settings=load_tradegate_settings_db(),
                router=self.quote_router,
                on_best_update=self._on_source_update,
            )

    def start(self) -> None:
        if self._task and not self._task.done():
            if self.tradegate_poller:
                self.tradegate_poller.start()
            if self.bitfinex_stream:
                self.bitfinex_stream.start()
            return
        self._task = asyncio.create_task(self._run())
        if self.tradegate_poller:
            self.tradegate_poller.start()
        if self.bitfinex_stream:
            self.bitfinex_stream.start()

    def reload_settings(self) -> None:
        priorities = load_source_priority()
        if hasattr(self.quote_router, "set_default_priority"):
            self.quote_router.set_default_priority(priorities)
        else:
            self.quote_router.default_priority = priorities
        self.lightstreamer_enabled = SOURCE_LIGHTSTREAMER in priorities

        if SOURCE_TRADEGATE in priorities:
            if not self.tradegate_poller:
                self.tradegate_poller = TradegatePoller(
                    settings=load_tradegate_settings_db(),
                    router=self.quote_router,
                    on_best_update=self._on_source_update,
                )
                self.tradegate_poller.start()
            else:
                self.tradegate_poller.settings = load_tradegate_settings_db()
        else:
            if self.tradegate_poller:
                self.tradegate_poller.stop()
                self.tradegate_poller = None

        if self.bitfinex_stream:
            self.bitfinex_stream.settings = load_bitfinex_settings_db()

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
        backoff_s: float = max(0.1, load_ls_settings().reconnect_min_s)

        while True:
            try:
                ls_settings = load_ls_settings()
                ls_reconnect_min_s = max(0.1, ls_settings.reconnect_min_s)
                ls_reconnect_max_s = max(ls_reconnect_min_s, ls_settings.reconnect_max_s)
                ls_recv_timeout_s = ls_settings.recv_timeout_s
                ls_stale_restart_s = ls_settings.stale_restart_s
                # Stream läuft, wenn entweder Dashboard-Clients verbunden sind ODER MQTT enabled ist.
                # So kann MQTT auch ohne geöffnetes Frontend 24/7 Updates bekommen.
                need_stream = bool(self.clients) or _mqtt_is_enabled()
                if self.tradegate_poller:
                    self.tradegate_poller.set_enabled(need_stream)
                    if not need_stream:
                        self.tradegate_poller.set_mapping({})
                if self.bitfinex_stream:
                    self.bitfinex_stream.set_enabled(need_stream)
                    if not need_stream:
                        self.bitfinex_stream.set_mapping({})
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
                bitfinex_map: Dict[str, set[str]] = {}

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
                    elif source == SOURCE_BITFINEX:
                        symbol = normalize_bitfinex_symbol(source_code)
                        if symbol:
                            bitfinex_map.setdefault(symbol, set()).add(key)

                items = [t[0] for t in targets]
                idx_to_key = {idx + 1: t[1] for idx, t in enumerate(targets)}

                self.debug_mappings = {
                    "lightstreamer": [{"item": item, "key": key} for item, key in targets],
                    "tradegate": dict(tradegate_map),
                    "bitfinex": {symbol: sorted(list(keys)) for symbol, keys in bitfinex_map.items()},
                }
                self.debug_mappings_updated_at = now_iso()

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
                if self.bitfinex_stream:
                    self.bitfinex_stream.set_mapping(bitfinex_map)

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

                sess = LightstreamerSession(ls_settings)
                try:
                    await sess.connect()
                    await sess.subscribe_items(items)
                except Exception as e:
                    # Backoff + retry on connect/subscribe errors
                    wait_s = min(max(ls_reconnect_min_s, backoff_s), ls_reconnect_max_s)
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
                    backoff_s = min(ls_reconnect_max_s, max(ls_reconnect_min_s, backoff_s * 2.0))
                    # force reconnect even if items unchanged
                    last_items = []
                    continue

                backoff_s = max(0.1, ls_reconnect_min_s)
                await self.broadcast({"type": "status", "level": "success", "message": "Stream verbunden."})

                # Receive loop until dirty flag set -> restart (oder bis wir den Stream nicht mehr brauchen)
                loop = asyncio.get_running_loop()
                last_any_msg = loop.time()
                while not self._dirty.is_set() and (self.clients or _mqtt_is_enabled()):
                    try:
                        raw = await asyncio.wait_for(sess._recv_text(), timeout=max(1.0, ls_recv_timeout_s))
                    except ConnectionClosed as e:
                        await self.broadcast({"type": "status", "level": "error", "message": f"Stream getrennt: {e.code} {e.reason}"})
                        break
                    except asyncio.TimeoutError:
                        # We expect regular PROBE frames; if we don't see anything for a while,
                        # restart the connection to avoid silently-stuck sessions.
                        if (loop.time() - last_any_msg) >= max(ls_recv_timeout_s, ls_stale_restart_s):
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
                    wait_s = min(max(ls_reconnect_min_s, backoff_s), ls_reconnect_max_s)
                    jitter = random.uniform(0.0, max(0.1, wait_s * 0.1))
                    await asyncio.sleep(wait_s + jitter)
                    backoff_s = min(ls_reconnect_max_s, max(ls_reconnect_min_s, backoff_s * 2.0))
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
    s = load_mqtt_settings(_conn)
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


def _mqtt_settings_payload() -> Dict[str, Any]:
    node_id = _get_setting_str("mqtt_node_id", "portfolio_valuator")
    base_topic = _get_setting_str("mqtt_base_topic", f"portfolio_valuator/{node_id}")
    return {
        "host": _get_setting_str("mqtt_host", ""),
        "port": get_int(_conn, "mqtt_port", 1883),
        "username": _get_setting_str("mqtt_username", ""),
        "password": _get_setting_str("mqtt_password", ""),
        "client_id": _get_setting_str("mqtt_client_id", ""),
        "discovery_prefix": _get_setting_str("mqtt_discovery_prefix", "homeassistant"),
        "node_id": node_id,
        "base_topic": base_topic,
        "qos": get_int(_conn, "mqtt_qos", 0),
        "retain": get_bool(_conn, "mqtt_retain", True),
        "debounce_ms": get_int(_conn, "mqtt_debounce_ms", 0),
        "sanity_skip_zero_price": get_bool(_conn, "mqtt_sanity_skip_zero_price", True),
        "sanity_max_pct_change": get_float(_conn, "mqtt_sanity_max_pct_change", 0.0),
        "sanity_require_price_for_valuation": get_bool(_conn, "mqtt_sanity_require_price_for_valuation", True),
    }


def _source_settings_payload() -> Dict[str, Any]:
    ls = load_ls_settings()
    tradegate = load_tradegate_settings_db()
    bitfinex = load_bitfinex_settings_db()
    return {
        "quote_source_priority": _get_setting_str("quote_source_priority", DEFAULT_QUOTE_SOURCE_PRIORITY),
        "ls_wss_url": ls.wss_url,
        "ls_subprotocol": ls.subprotocol or "",
        "ls_adapter_set": ls.adapter_set,
        "ls_data_adapter": ls.data_adapter,
        "ls_cid": ls.cid,
        "ls_item_template": ls.item_template,
        "ls_origin": ls.origin or "",
        "ls_user_agent": ls.user_agent,
        "ls_reconnect_min_s": ls.reconnect_min_s,
        "ls_reconnect_max_s": ls.reconnect_max_s,
        "ls_recv_timeout_s": ls.recv_timeout_s,
        "ls_stale_restart_s": ls.stale_restart_s,
        "tradegate_url_template": tradegate.url_template,
        "tradegate_timeout_s": tradegate.timeout_s,
        "tradegate_poll_s": tradegate.poll_s,
        "tradegate_user_agent": tradegate.user_agent,
        "bitfinex_wss_url": bitfinex.wss_url,
        "bitfinex_reconnect_min_s": bitfinex.reconnect_min_s,
        "bitfinex_reconnect_max_s": bitfinex.reconnect_max_s,
    }


async def fetch_bids(isins: List[str], timeout_s: float = 8.0) -> Dict[str, Optional[float]]:
    """
    Holt für eine ISIN-Liste die aktuellen Bid-Quotes (Snapshot/erste Updates) über Lightstreamer.
    Gibt dict[ISIN] = bid (float) oder None (falls nicht erhalten).
    """
    clean_isins = [validate_isin(i) for i in isins]
    items = [isin_to_item(i) for i in clean_isins]
    idx_to_isin = {idx + 1: isin for idx, isin in enumerate(clean_isins)}

    sess = LightstreamerSession(load_ls_settings())
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
    priority: Optional[int] = Field(default=None, ge=0, le=1000)


class SourceUpdate(BaseModel):
    source_code: Optional[str] = Field(default=None, min_length=1, max_length=128)
    priority: Optional[int] = Field(default=None, ge=0, le=1000)


class OrderUpdate(BaseModel):
    ids: List[int] = Field(default_factory=list)


class MqttSettingsUpdate(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: Optional[str] = None
    discovery_prefix: Optional[str] = None
    node_id: Optional[str] = None
    base_topic: Optional[str] = None
    qos: Optional[int] = Field(default=None, ge=0, le=2)
    retain: Optional[bool] = None
    debounce_ms: Optional[int] = Field(default=None, ge=0)
    sanity_skip_zero_price: Optional[bool] = None
    sanity_max_pct_change: Optional[float] = Field(default=None, ge=0)
    sanity_require_price_for_valuation: Optional[bool] = None


class SourceSettingsUpdate(BaseModel):
    quote_source_priority: Optional[str] = None
    ls_wss_url: Optional[str] = None
    ls_subprotocol: Optional[str] = None
    ls_adapter_set: Optional[str] = None
    ls_data_adapter: Optional[str] = None
    ls_cid: Optional[str] = None
    ls_item_template: Optional[str] = None
    ls_origin: Optional[str] = None
    ls_user_agent: Optional[str] = None
    ls_reconnect_min_s: Optional[float] = Field(default=None, ge=0)
    ls_reconnect_max_s: Optional[float] = Field(default=None, ge=0)
    ls_recv_timeout_s: Optional[float] = Field(default=None, ge=0)
    ls_stale_restart_s: Optional[float] = Field(default=None, ge=0)
    tradegate_url_template: Optional[str] = None
    tradegate_timeout_s: Optional[float] = Field(default=None, ge=0)
    tradegate_poll_s: Optional[float] = Field(default=None, ge=0)
    tradegate_user_agent: Optional[str] = None
    bitfinex_wss_url: Optional[str] = None
    bitfinex_reconnect_min_s: Optional[float] = Field(default=None, ge=0)
    bitfinex_reconnect_max_s: Optional[float] = Field(default=None, ge=0)


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


def _get_source_price(source: str, key: str) -> Optional[float]:
    src = (source or "").strip().lower()
    if not src or not key:
        return None
    price = stream_manager.quote_router.source_prices.get(src, {}).get(key)
    if price is None:
        price = stream_manager.quote_router.source_bids.get(src, {}).get(key)
    return price


def _get_source_last_update(source: str, key: str) -> Optional[str]:
    src = (source or "").strip().lower()
    if not src or not key:
        return None
    return stream_manager.quote_router.source_last_update.get(src, {}).get(key)


def _build_source_stats() -> Dict[str, Any]:
    router = stream_manager.quote_router
    stats: Dict[str, Any] = {}
    for src in sorted(router.known_sources):
        stats[src] = {
            "prices": len(router.source_prices.get(src, {})),
            "bids": len(router.source_bids.get(src, {})),
            "watch_prices": len(router.source_watch_prices.get(src, {})),
            "last_updates": len(router.source_last_update.get(src, {})),
        }
    return stats




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
    if stream_manager.bitfinex_stream:
        stream_manager.bitfinex_stream.stop()
    _mqtt_disconnect()


@app.get("/")
async def index():
    return FileResponse("app/static/dashboard.html")


@app.get("/manage")
async def manage():
    return FileResponse("app/static/positions.html")


@app.get("/positions")
async def positions_page():
    return FileResponse("app/static/positions.html")


@app.get("/watchlist")
async def watchlist_page():
    return FileResponse("app/static/watchlist.html")


@app.get("/sources")
async def sources_page():
    return FileResponse("app/static/sources.html")


@app.get("/settings")
async def settings_page():
    return FileResponse("app/static/settings.html")


@app.get("/instruments")
async def instruments_page():
    return FileResponse("app/static/instruments.html")


@app.get("/api/debug/quote-sources")
async def debug_quote_sources(key: Optional[str] = None) -> Dict[str, Any]:
    router = stream_manager.quote_router
    key = (key or "").strip()
    if key:
        sources: List[Dict[str, Any]] = []
        for src in sorted(router.known_sources):
            sources.append(
                {
                    "source": src,
                    "price": router.source_prices.get(src, {}).get(key),
                    "price_field": router.source_price_fields.get(src, {}).get(key),
                    "bid": router.source_bids.get(src, {}).get(key),
                    "watch_price": router.source_watch_prices.get(src, {}).get(key),
                    "last_update": router.source_last_update.get(src, {}).get(key),
                    "active": router.best_price_source.get(key) == src,
                }
            )
        debug_map = stream_manager.debug_mappings or {}
        ls_items = [
            item.get("item")
            for item in (debug_map.get("lightstreamer") or [])
            if item.get("key") == key
        ]
        tradegate_isin = (debug_map.get("tradegate") or {}).get(key)
        bitfinex_symbols = [
            sym for sym, keys in (debug_map.get("bitfinex") or {}).items() if key in (keys or [])
        ]
        return {
            "key": key,
            "priority": router._get_priority(key),
            "best_price": router.best_prices.get(key),
            "best_price_source": router.best_price_source.get(key),
            "best_bid": router.best_bids.get(key),
            "best_bid_source": router.best_bid_source.get(key),
            "best_watch_price": router.best_watch_prices.get(key),
            "best_watch_source": router.best_watch_source.get(key),
            "sources": sources,
            "mappings": {
                "lightstreamer_items": ls_items,
                "tradegate_isin": tradegate_isin,
                "bitfinex_symbols": bitfinex_symbols,
            },
        }

    return {
        "default_priority": router.default_priority,
        "known_sources": sorted(router.known_sources),
        "source_stats": _build_source_stats(),
        "priority_overrides": len(router.priority_by_key),
        "mappings": stream_manager.debug_mappings,
        "mappings_updated_at": stream_manager.debug_mappings_updated_at,
        "best_prices": len(router.best_prices),
        "best_bids": len(router.best_bids),
        "best_watch_prices": len(router.best_watch_prices),
    }


@app.get("/api/debug/bitfinex")
async def debug_bitfinex() -> Dict[str, Any]:
    if not stream_manager.bitfinex_stream:
        return {"enabled": False}
    return stream_manager.bitfinex_stream.get_status()


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


@app.get("/api/instruments/{instrument_id}/sources/quotes")
async def list_instrument_source_quotes(instrument_id: int) -> List[Dict[str, Any]]:
    instrument = _get_instrument(instrument_id)
    cur = _conn.execute(
        "SELECT id, source, source_code, priority FROM instrument_sources WHERE instrument_id=? ORDER BY priority ASC, id ASC",
        (instrument_id,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    key = instrument["code"]
    out: List[Dict[str, Any]] = []
    for row in rows:
        src = row.get("source")
        out.append(
            {
                **row,
                "price": _get_source_price(src, key),
                "last_update": _get_source_last_update(src, key),
                "active": (stream_manager.quote_router.best_price_source.get(key) == src),
            }
        )
    return out


@app.post("/api/instruments/{instrument_id}/sources", status_code=201)
async def add_instrument_source(instrument_id: int, body: SourceCreate) -> Dict[str, Any]:
    _get_instrument(instrument_id)
    source = _normalize_source(body.source)
    source_code = _normalize_source_code(body.source_code)
    priority = body.priority
    if priority is None:
        max_row = _conn.execute(
            "SELECT COALESCE(MAX(priority), -1) AS max_priority FROM instrument_sources WHERE instrument_id=?",
            (instrument_id,),
        ).fetchone()
        priority = int(max_row["max_priority"] if max_row else -1) + 1
    try:
        cur = _conn.execute(
            "INSERT INTO instrument_sources(instrument_id, source, source_code, priority) VALUES (?,?,?,?)",
            (instrument_id, source, source_code, priority),
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
        "priority": priority,
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


@app.put("/api/instruments/{instrument_id}/sources/order")
async def reorder_instrument_sources(instrument_id: int, body: OrderUpdate) -> List[Dict[str, Any]]:
    _get_instrument(instrument_id)
    ids = [int(i) for i in (body.ids or []) if i]
    if not ids:
        return []
    rows = _conn.execute(
        "SELECT id FROM instrument_sources WHERE instrument_id=?",
        (instrument_id,),
    ).fetchall()
    existing = {int(r["id"]) for r in rows}
    missing = [i for i in ids if i not in existing]
    if missing:
        raise HTTPException(status_code=400, detail="Kursquelle nicht gefunden")
    with _conn:
        for idx, source_id in enumerate(ids):
            _conn.execute(
                "UPDATE instrument_sources SET priority=? WHERE id=? AND instrument_id=?",
                (idx, source_id, instrument_id),
            )
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return await list_instrument_sources(instrument_id)


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


@app.get("/api/fx-rates/{fx_id}/sources/quotes")
async def list_fx_rate_source_quotes(fx_id: int) -> List[Dict[str, Any]]:
    fx = _get_fx_rate(fx_id)
    cur = _conn.execute(
        "SELECT id, source, source_code, priority FROM fx_rate_sources WHERE fx_rate_id=? ORDER BY priority ASC, id ASC",
        (fx_id,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    key = fx["code"]
    out: List[Dict[str, Any]] = []
    for row in rows:
        src = row.get("source")
        out.append(
            {
                **row,
                "price": _get_source_price(src, key),
                "last_update": _get_source_last_update(src, key),
                "active": (stream_manager.quote_router.best_price_source.get(key) == src),
            }
        )
    return out


@app.post("/api/fx-rates/{fx_id}/sources", status_code=201)
async def add_fx_rate_source(fx_id: int, body: SourceCreate) -> Dict[str, Any]:
    _get_fx_rate(fx_id)
    source = _normalize_source(body.source)
    source_code = _normalize_source_code(body.source_code)
    priority = body.priority
    if priority is None:
        max_row = _conn.execute(
            "SELECT COALESCE(MAX(priority), -1) AS max_priority FROM fx_rate_sources WHERE fx_rate_id=?",
            (fx_id,),
        ).fetchone()
        priority = int(max_row["max_priority"] if max_row else -1) + 1
    try:
        cur = _conn.execute(
            "INSERT INTO fx_rate_sources(fx_rate_id, source, source_code, priority) VALUES (?,?,?,?)",
            (fx_id, source, source_code, priority),
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
        "priority": priority,
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


@app.put("/api/fx-rates/{fx_id}/sources/order")
async def reorder_fx_rate_sources(fx_id: int, body: OrderUpdate) -> List[Dict[str, Any]]:
    _get_fx_rate(fx_id)
    ids = [int(i) for i in (body.ids or []) if i]
    if not ids:
        return []
    rows = _conn.execute(
        "SELECT id FROM fx_rate_sources WHERE fx_rate_id=?",
        (fx_id,),
    ).fetchall()
    existing = {int(r["id"]) for r in rows}
    missing = [i for i in ids if i not in existing]
    if missing:
        raise HTTPException(status_code=400, detail="Kursquelle nicht gefunden")
    with _conn:
        for idx, source_id in enumerate(ids):
            _conn.execute(
                "UPDATE fx_rate_sources SET priority=? WHERE id=? AND fx_rate_id=?",
                (idx, source_id, fx_id),
            )
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return await list_fx_rate_sources(fx_id)


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
        SELECT p.id, p.name, p.currency, p.banking_bridge_depot_id, COUNT(pos.id) AS positions_count
        FROM portfolios p
        LEFT JOIN positions pos ON pos.portfolio_id = p.id
        GROUP BY p.id
        ORDER BY p.sort_order ASC, p.id ASC
        """
    )
    return [dict(r) for r in cur.fetchall()]


@app.put("/api/portfolios/order")
async def reorder_portfolios(body: OrderUpdate) -> List[Dict[str, Any]]:
    ids = [int(i) for i in (body.ids or []) if i]
    if not ids:
        return []
    rows = _conn.execute("SELECT id FROM portfolios").fetchall()
    existing = {int(r["id"]) for r in rows}
    missing = [i for i in ids if i not in existing]
    if missing:
        raise HTTPException(status_code=400, detail="Portfolio nicht gefunden")
    with _conn:
        for idx, pid in enumerate(ids):
            _conn.execute("UPDATE portfolios SET sort_order=? WHERE id=?", (idx, pid))
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return await list_portfolios()


@app.post("/api/portfolios", status_code=201)
async def create_portfolio(body: PortfolioCreate) -> PortfolioOut:
    max_row = _conn.execute("SELECT COALESCE(MAX(sort_order), -1) AS max_order FROM portfolios").fetchone()
    next_order = int(max_row["max_order"] if max_row else -1) + 1
    cur = _conn.execute(
        "INSERT INTO portfolios(name, currency, sort_order) VALUES (?, ?, ?)",
        (body.name.strip(), (body.currency or "EUR").strip().upper(), next_order),
    )
    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return PortfolioOut(id=int(cur.lastrowid), name=body.name.strip())


@app.get("/api/watchlist")
async def list_watchlist() -> List[Dict[str, Any]]:
    return load_watchlist(_conn)


@app.put("/api/watchlist/order")
async def reorder_watchlist(body: OrderUpdate) -> List[Dict[str, Any]]:
    ids = [int(i) for i in (body.ids or []) if i]
    if not ids:
        return []
    rows = _conn.execute("SELECT id FROM watchlist").fetchall()
    existing = {int(r["id"]) for r in rows}
    missing = [i for i in ids if i not in existing]
    if missing:
        raise HTTPException(status_code=400, detail="Watchlist-Eintrag nicht gefunden")
    with _conn:
        for idx, wid in enumerate(ids):
            _conn.execute("UPDATE watchlist SET sort_order=? WHERE id=?", (idx, wid))
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()
    return load_watchlist(_conn)


@app.post("/api/watchlist", status_code=201)
async def add_watchlist_item(body: WatchItemCreate) -> Dict[str, Any]:
    instrument = _get_instrument(body.instrument_id)
    label = (body.label or "").strip() or None
    currency = _normalize_currency(body.currency, default=instrument.get("currency") or "EUR")
    max_row = _conn.execute("SELECT COALESCE(MAX(sort_order), -1) AS max_order FROM watchlist").fetchone()
    next_order = int(max_row["max_order"] if max_row else -1) + 1
    try:
        cur = _conn.execute(
            "INSERT INTO watchlist(instrument_id, isin, label, currency, sort_order) VALUES (?,?,?,?,?)",
            (instrument["id"], instrument["code"], label, currency, next_order),
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
    cur = _conn.execute("SELECT id, name, currency, banking_bridge_depot_id FROM portfolios WHERE id=?", (portfolio_id,))
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
        ORDER BY pos.sort_order ASC, pos.id ASC
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
        for idx, p in enumerate(cleaned):
            _conn.execute(
                "INSERT INTO positions(portfolio_id, instrument_id, isin, name, quantity, entry_price, currency, sort_order) VALUES (?,?,?,?,?,?,?,?)",
                (
                    portfolio_id,
                    p["instrument_id"],
                    p["instrument_code"],
                    p["name"],
                    p["quantity"],
                    p["entry_price"],
                    p["currency"],
                    idx,
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
        ORDER BY pos.sort_order ASC, pos.id ASC
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
    s = load_mqtt_settings(_conn)
    enabled = _mqtt_is_enabled()
    return {
        "enabled": enabled,
        "connected": bool(_mqtt),
        "host": s.host,
        "port": s.port,
        "availability_topic": s.availability_topic,
    }


@app.get("/api/settings/mqtt")
async def get_mqtt_settings() -> Dict[str, Any]:
    return _mqtt_settings_payload()


@app.put("/api/settings/mqtt")
async def update_mqtt_settings(body: MqttSettingsUpdate) -> Dict[str, Any]:
    if body.host is not None:
        set_setting(_conn, "mqtt_host", (body.host or "").strip())
    if body.port is not None:
        set_setting(_conn, "mqtt_port", str(body.port))
    if body.username is not None:
        set_setting(_conn, "mqtt_username", (body.username or "").strip())
    if body.password is not None:
        set_setting(_conn, "mqtt_password", (body.password or "").strip())
    if body.client_id is not None:
        set_setting(_conn, "mqtt_client_id", (body.client_id or "").strip())
    if body.discovery_prefix is not None:
        set_setting(_conn, "mqtt_discovery_prefix", (body.discovery_prefix or "").strip())
    if body.node_id is not None:
        set_setting(_conn, "mqtt_node_id", (body.node_id or "").strip())
    if body.base_topic is not None:
        set_setting(_conn, "mqtt_base_topic", (body.base_topic or "").strip())
    if body.qos is not None:
        set_setting(_conn, "mqtt_qos", str(body.qos))
    if body.retain is not None:
        set_setting(_conn, "mqtt_retain", "true" if body.retain else "false")
    if body.debounce_ms is not None:
        set_setting(_conn, "mqtt_debounce_ms", str(body.debounce_ms))
    if body.sanity_skip_zero_price is not None:
        set_setting(_conn, "mqtt_sanity_skip_zero_price", "true" if body.sanity_skip_zero_price else "false")
    if body.sanity_max_pct_change is not None:
        set_setting(_conn, "mqtt_sanity_max_pct_change", str(body.sanity_max_pct_change))
    if body.sanity_require_price_for_valuation is not None:
        set_setting(
            _conn,
            "mqtt_sanity_require_price_for_valuation",
            "true" if body.sanity_require_price_for_valuation else "false",
        )

    if _mqtt_is_enabled():
        _mqtt_disconnect()
        try:
            _mqtt_connect_if_enabled()
        except Exception as exc:
            logger.warning("MQTT reconnect fehlgeschlagen: %s", exc)
    return _mqtt_settings_payload()


@app.get("/api/settings/sources")
async def get_source_settings() -> Dict[str, Any]:
    return _source_settings_payload()


@app.put("/api/settings/sources")
async def update_source_settings(body: SourceSettingsUpdate) -> Dict[str, Any]:
    if body.quote_source_priority is not None:
        raw = (body.quote_source_priority or "").strip()
        raw = raw or DEFAULT_QUOTE_SOURCE_PRIORITY
        parsed = parse_source_priority(raw)
        set_setting(_conn, "quote_source_priority", ",".join(parsed))
    if body.ls_wss_url is not None:
        set_setting(_conn, "ls_wss_url", (body.ls_wss_url or "").strip())
    if body.ls_subprotocol is not None:
        set_setting(_conn, "ls_subprotocol", (body.ls_subprotocol or "").strip())
    if body.ls_adapter_set is not None:
        set_setting(_conn, "ls_adapter_set", (body.ls_adapter_set or "").strip())
    if body.ls_data_adapter is not None:
        set_setting(_conn, "ls_data_adapter", (body.ls_data_adapter or "").strip())
    if body.ls_cid is not None:
        set_setting(_conn, "ls_cid", (body.ls_cid or "").strip())
    if body.ls_item_template is not None:
        value = (body.ls_item_template or "").strip()
        if value and "{isin}" not in value:
            raise HTTPException(status_code=400, detail="LS Item Template muss '{isin}' enthalten")
        set_setting(_conn, "ls_item_template", value)
    if body.ls_origin is not None:
        set_setting(_conn, "ls_origin", (body.ls_origin or "").strip())
    if body.ls_user_agent is not None:
        set_setting(_conn, "ls_user_agent", (body.ls_user_agent or "").strip())
    if body.ls_reconnect_min_s is not None:
        set_setting(_conn, "ls_reconnect_min_s", str(body.ls_reconnect_min_s))
    if body.ls_reconnect_max_s is not None:
        set_setting(_conn, "ls_reconnect_max_s", str(body.ls_reconnect_max_s))
    if body.ls_recv_timeout_s is not None:
        set_setting(_conn, "ls_recv_timeout_s", str(body.ls_recv_timeout_s))
    if body.ls_stale_restart_s is not None:
        set_setting(_conn, "ls_stale_restart_s", str(body.ls_stale_restart_s))

    if body.tradegate_url_template is not None:
        set_setting(_conn, "tradegate_url_template", (body.tradegate_url_template or "").strip())
    if body.tradegate_timeout_s is not None:
        set_setting(_conn, "tradegate_timeout_s", str(body.tradegate_timeout_s))
    if body.tradegate_poll_s is not None:
        set_setting(_conn, "tradegate_poll_s", str(body.tradegate_poll_s))
    if body.tradegate_user_agent is not None:
        set_setting(_conn, "tradegate_user_agent", (body.tradegate_user_agent or "").strip())

    if body.bitfinex_wss_url is not None:
        set_setting(_conn, "bitfinex_wss_url", (body.bitfinex_wss_url or "").strip())
    if body.bitfinex_reconnect_min_s is not None:
        set_setting(_conn, "bitfinex_reconnect_min_s", str(body.bitfinex_reconnect_min_s))
    if body.bitfinex_reconnect_max_s is not None:
        set_setting(_conn, "bitfinex_reconnect_max_s", str(body.bitfinex_reconnect_max_s))

    stream_manager.reload_settings()
    stream_manager.mark_dirty()
    return _source_settings_payload()


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


# ──────────────────────────────────────────────────────────────────────────────
#  Banking Bridge Integration
# ──────────────────────────────────────────────────────────────────────────────


def _bb_client() -> BankingBridgeClient:
    url = _get_setting_str("banking_bridge_url", "")
    return BankingBridgeClient(BankingBridgeSettings(base_url=url))


class BankingBridgeSettingsUpdate(BaseModel):
    url: Optional[str] = None


@app.get("/api/settings/banking-bridge")
async def get_banking_bridge_settings() -> Dict[str, Any]:
    url = _get_setting_str("banking_bridge_url", "")
    return {"url": url}


@app.put("/api/settings/banking-bridge")
async def update_banking_bridge_settings(body: BankingBridgeSettingsUpdate) -> Dict[str, Any]:
    if body.url is not None:
        set_setting(_conn, "banking_bridge_url", (body.url or "").strip())
    url = _get_setting_str("banking_bridge_url", "")
    return {"url": url}


@app.get("/api/banking-bridge/status")
async def banking_bridge_status() -> Dict[str, Any]:
    try:
        client = _bb_client()
        result = client.check_connection()
        return {
            "connected": True,
            "depot_count": result.get("depot_count", 0),
            "url": client.base_url,
        }
    except Exception as e:
        return {
            "connected": False,
            "error": str(e),
            "url": _get_setting_str("banking_bridge_url", ""),
        }


@app.get("/api/banking-bridge/depots")
async def list_banking_bridge_depots() -> Dict[str, Any]:
    client = _bb_client()
    try:
        depots = client.list_depots()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    # Verknuepfungsstatus: welche Depots sind bereits als Portfolio importiert?
    linked: Dict[int, int] = {}
    rows = _conn.execute(
        "SELECT id, banking_bridge_depot_id FROM portfolios WHERE banking_bridge_depot_id IS NOT NULL"
    ).fetchall()
    for r in rows:
        linked[int(r["banking_bridge_depot_id"])] = int(r["id"])

    out = []
    for d in depots:
        out.append({
            "id": d.id,
            "name": d.name,
            "account_number": d.account_number,
            "sub_account": d.sub_account,
            "bank": d.bank,
            "bank_code": d.bank_code,
            "total_value": d.total_value,
            "currency": d.currency,
            "last_update": d.last_update,
            "linked_portfolio_id": linked.get(d.id),
        })
    return {"depots": out, "count": len(out)}


@app.get("/api/banking-bridge/depots/{depot_id}/holdings")
async def get_banking_bridge_holdings(depot_id: int) -> Dict[str, Any]:
    client = _bb_client()
    try:
        holdings = client.get_holdings(depot_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    out = []
    for h in holdings:
        out.append({
            "isin": h.isin,
            "wkn": h.wkn,
            "name": h.name,
            "quantity": h.quantity,
            "currency": h.currency,
            "current_price": h.current_price,
            "purchase_price": h.purchase_price,
            "total_value": h.total_value,
            "profit_loss": h.profit_loss,
            "profit_loss_percent": h.profit_loss_percent,
            "price_date": h.price_date,
            "updated_at": h.updated_at,
        })
    return {"holdings": out, "count": len(out)}


def _get_or_create_instrument_for_holding(
    isin: str,
    name: str,
    currency: str,
) -> int:
    """Findet oder erstellt ein Instrument anhand der ISIN."""
    isin = (isin or "").strip().upper()
    if not isin:
        raise ValueError("ISIN darf nicht leer sein")

    # Suche bestehendes Instrument nach ISIN
    row = _conn.execute(
        "SELECT id FROM instruments WHERE isin=? AND (type IS NULL OR type='asset')",
        (isin,),
    ).fetchone()
    if row:
        return int(row["id"])

    # Suche nach Code = ISIN
    row = _conn.execute(
        "SELECT id FROM instruments WHERE code=? AND (type IS NULL OR type='asset')",
        (isin,),
    ).fetchone()
    if row:
        # ISIN-Feld nachtragen
        _conn.execute("UPDATE instruments SET isin=? WHERE id=? AND isin IS NULL", (isin, row["id"]))
        return int(row["id"])

    # Neues Instrument anlegen
    cur = _conn.execute(
        "INSERT INTO instruments(code, name, currency, type, isin) VALUES (?,?,?,?,?)",
        (isin, name or isin, (currency or "EUR").strip().upper(), "asset", isin),
    )
    instrument_id = int(cur.lastrowid)

    # Auto-Kursquellen: Tradegate (ISIN-basiert) + Lightstreamer
    try:
        _conn.execute(
            "INSERT INTO instrument_sources(instrument_id, source, source_code, priority) VALUES (?,?,?,?)",
            (instrument_id, "lightstreamer", isin, 10),
        )
    except Exception:
        pass
    try:
        _conn.execute(
            "INSERT INTO instrument_sources(instrument_id, source, source_code, priority) VALUES (?,?,?,?)",
            (instrument_id, "tradegate", isin, 20),
        )
    except Exception:
        pass

    return instrument_id


@app.post("/api/banking-bridge/import/{depot_id}")
async def import_depot_as_portfolio(depot_id: int) -> Dict[str, Any]:
    """
    Importiert ein Depot aus der Banking Bridge als neues Portfolio.
    Erstellt automatisch Instrumente fuer alle Holdings (anhand ISIN).
    """
    client = _bb_client()

    # Pruefen ob Depot bereits importiert ist
    existing = _conn.execute(
        "SELECT id, name FROM portfolios WHERE banking_bridge_depot_id=?",
        (depot_id,),
    ).fetchone()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Depot ist bereits als Portfolio '{existing['name']}' (#{existing['id']}) verknuepft",
        )

    try:
        depot = client.get_depot(depot_id)
        holdings = client.get_holdings(depot_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    # Portfolio Name: "Bank - Depotname" (oder nur Depotname)
    portfolio_name = f"{depot.bank} - {depot.name}" if depot.bank else depot.name
    portfolio_currency = (depot.currency or "EUR").strip().upper()

    max_row = _conn.execute("SELECT COALESCE(MAX(sort_order), -1) AS max_order FROM portfolios").fetchone()
    next_order = int(max_row["max_order"] if max_row else -1) + 1

    cur = _conn.execute(
        "INSERT INTO portfolios(name, currency, sort_order, banking_bridge_depot_id) VALUES (?,?,?,?)",
        (portfolio_name, portfolio_currency, next_order, depot_id),
    )
    portfolio_id = int(cur.lastrowid)

    # Positionen importieren
    imported_count = 0
    skipped = []
    for idx, h in enumerate(holdings):
        if not h.isin or h.quantity <= 0:
            skipped.append({"name": h.name, "reason": "Keine ISIN oder Menge <= 0"})
            continue

        try:
            instrument_id = _get_or_create_instrument_for_holding(
                isin=h.isin,
                name=h.name,
                currency=h.currency,
            )
        except Exception as e:
            skipped.append({"name": h.name, "isin": h.isin, "reason": str(e)})
            continue

        entry_price = h.purchase_price if h.purchase_price and h.purchase_price > 0 else 0.01
        pos_currency = (h.currency or portfolio_currency).strip().upper()

        _conn.execute(
            "INSERT INTO positions(portfolio_id, instrument_id, isin, name, quantity, entry_price, currency, sort_order) VALUES (?,?,?,?,?,?,?,?)",
            (portfolio_id, instrument_id, h.isin, h.name, h.quantity, entry_price, pos_currency, idx),
        )
        imported_count += 1

    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()

    return {
        "portfolio_id": portfolio_id,
        "portfolio_name": portfolio_name,
        "depot_id": depot_id,
        "imported_positions": imported_count,
        "skipped": skipped,
        "total_holdings": len(holdings),
    }


@app.post("/api/banking-bridge/sync/{portfolio_id}")
async def sync_portfolio_from_banking_bridge(portfolio_id: int) -> Dict[str, Any]:
    """
    Aktualisiert ein verknuepftes Portfolio mit den aktuellen Depot-Daten
    aus der Banking Bridge. Neue Holdings werden hinzugefuegt, bestehende
    aktualisiert (Menge, Entry-Preis), entfernte geloescht.
    """
    row = _conn.execute(
        "SELECT id, name, currency, banking_bridge_depot_id FROM portfolios WHERE id=?",
        (portfolio_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")
    depot_id = row["banking_bridge_depot_id"]
    if not depot_id:
        raise HTTPException(status_code=400, detail="Portfolio ist nicht mit einem Banking Bridge Depot verknuepft")

    portfolio_currency = (row["currency"] or "EUR").strip().upper()
    client = _bb_client()

    try:
        holdings = client.get_holdings(int(depot_id))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    # Aktuelle Positionen laden
    existing_positions = _conn.execute(
        """
        SELECT pos.id, pos.instrument_id, pos.isin, pos.quantity, pos.entry_price, pos.currency,
               instr.isin AS instrument_isin
        FROM positions pos
        LEFT JOIN instruments instr ON instr.id = pos.instrument_id
        WHERE pos.portfolio_id=?
        """,
        (portfolio_id,),
    ).fetchall()

    # Index: ISIN -> bestehende Position
    existing_by_isin: Dict[str, Dict[str, Any]] = {}
    for p in existing_positions:
        isin_key = (p["instrument_isin"] or p["isin"] or "").strip().upper()
        if isin_key:
            existing_by_isin[isin_key] = dict(p)

    added = 0
    updated = 0
    removed = 0
    skipped = []
    seen_isins: set[str] = set()

    # Max sort_order fuer neue Positionen
    max_sort = _conn.execute(
        "SELECT COALESCE(MAX(sort_order), -1) AS max_order FROM positions WHERE portfolio_id=?",
        (portfolio_id,),
    ).fetchone()
    next_sort = int(max_sort["max_order"] if max_sort else -1) + 1

    for h in holdings:
        isin = (h.isin or "").strip().upper()
        if not isin or h.quantity <= 0:
            skipped.append({"name": h.name, "reason": "Keine ISIN oder Menge <= 0"})
            continue

        seen_isins.add(isin)
        entry_price = h.purchase_price if h.purchase_price and h.purchase_price > 0 else 0.01
        pos_currency = (h.currency or portfolio_currency).strip().upper()

        if isin in existing_by_isin:
            # Bestehende Position aktualisieren
            pos = existing_by_isin[isin]
            changes = []
            values = []
            if abs(pos["quantity"] - h.quantity) > 0.0001:
                changes.append("quantity=?")
                values.append(h.quantity)
            if h.purchase_price and h.purchase_price > 0 and abs(pos["entry_price"] - entry_price) > 0.0001:
                changes.append("entry_price=?")
                values.append(entry_price)
            if pos_currency != (pos["currency"] or "EUR").strip().upper():
                changes.append("currency=?")
                values.append(pos_currency)

            if changes:
                values.append(pos["id"])
                _conn.execute(
                    f"UPDATE positions SET {', '.join(changes)} WHERE id=?",
                    tuple(values),
                )
                updated += 1
        else:
            # Neue Position hinzufuegen
            try:
                instrument_id = _get_or_create_instrument_for_holding(
                    isin=isin,
                    name=h.name,
                    currency=h.currency,
                )
            except Exception as e:
                skipped.append({"name": h.name, "isin": isin, "reason": str(e)})
                continue

            _conn.execute(
                "INSERT INTO positions(portfolio_id, instrument_id, isin, name, quantity, entry_price, currency, sort_order) VALUES (?,?,?,?,?,?,?,?)",
                (portfolio_id, instrument_id, isin, h.name, h.quantity, entry_price, pos_currency, next_sort),
            )
            next_sort += 1
            added += 1

    # Positionen entfernen, die nicht mehr im Depot sind
    for isin_key, pos in existing_by_isin.items():
        if isin_key not in seen_isins:
            _conn.execute("DELETE FROM positions WHERE id=?", (pos["id"],))
            removed += 1

    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()

    return {
        "portfolio_id": portfolio_id,
        "depot_id": depot_id,
        "added": added,
        "updated": updated,
        "removed": removed,
        "skipped": skipped,
        "total_holdings": len(holdings),
    }


@app.post("/api/banking-bridge/link/{portfolio_id}/{depot_id}")
async def link_portfolio_to_depot(portfolio_id: int, depot_id: int) -> Dict[str, Any]:
    """Verknuepft ein bestehendes Portfolio manuell mit einem Banking Bridge Depot."""
    row = _conn.execute("SELECT id, name FROM portfolios WHERE id=?", (portfolio_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    # Pruefen ob Depot-ID bereits vergeben
    conflict = _conn.execute(
        "SELECT id, name FROM portfolios WHERE banking_bridge_depot_id=? AND id<>?",
        (depot_id, portfolio_id),
    ).fetchone()
    if conflict:
        raise HTTPException(
            status_code=409,
            detail=f"Depot ist bereits mit Portfolio '{conflict['name']}' (#{conflict['id']}) verknuepft",
        )

    _conn.execute(
        "UPDATE portfolios SET banking_bridge_depot_id=? WHERE id=?",
        (depot_id, portfolio_id),
    )
    _conn.commit()
    return {"portfolio_id": portfolio_id, "depot_id": depot_id, "linked": True}


@app.post("/api/banking-bridge/unlink/{portfolio_id}")
async def unlink_portfolio_from_depot(portfolio_id: int) -> Dict[str, Any]:
    """Entfernt die Verknuepfung eines Portfolios mit einem Banking Bridge Depot."""
    row = _conn.execute("SELECT id, name FROM portfolios WHERE id=?", (portfolio_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    _conn.execute(
        "UPDATE portfolios SET banking_bridge_depot_id=NULL WHERE id=?",
        (portfolio_id,),
    )
    _conn.commit()
    return {"portfolio_id": portfolio_id, "unlinked": True}


# ──────────────────── Banking Bridge Integration ────────────────────


def _get_bb_client() -> BankingBridgeClient:
    url = (get_setting(_conn, "banking_bridge_url", "") or "").strip()
    return BankingBridgeClient(BankingBridgeSettings(base_url=url))


class BankingBridgeSettingsUpdate(BaseModel):
    url: Optional[str] = None


@app.get("/api/settings/banking-bridge")
async def get_banking_bridge_settings() -> Dict[str, Any]:
    url = (get_setting(_conn, "banking_bridge_url", "") or "").strip()
    return {"url": url}


@app.put("/api/settings/banking-bridge")
async def update_banking_bridge_settings(body: BankingBridgeSettingsUpdate) -> Dict[str, Any]:
    if body.url is not None:
        set_setting(_conn, "banking_bridge_url", (body.url or "").strip())
    url = (get_setting(_conn, "banking_bridge_url", "") or "").strip()
    return {"url": url}


@app.get("/api/banking-bridge/status")
async def banking_bridge_status() -> Dict[str, Any]:
    try:
        client = _get_bb_client()
        result = await asyncio.to_thread(client.check_connection)
        return {"connected": True, **result}
    except Exception as exc:
        return {"connected": False, "error": str(exc)}


@app.get("/api/banking-bridge/depots")
async def banking_bridge_list_depots() -> Dict[str, Any]:
    client = _get_bb_client()
    try:
        depots = await asyncio.to_thread(client.list_depots)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    # Bestehende Verknuepfungen laden
    linked_rows = _conn.execute(
        "SELECT id, banking_bridge_depot_id FROM portfolios WHERE banking_bridge_depot_id IS NOT NULL"
    ).fetchall()
    linked_map: Dict[int, int] = {int(r["banking_bridge_depot_id"]): int(r["id"]) for r in linked_rows}

    result = []
    for d in depots:
        entry: Dict[str, Any] = {
            "id": d.id,
            "name": d.name,
            "account_number": d.account_number,
            "sub_account": d.sub_account,
            "bank": d.bank,
            "bank_code": d.bank_code,
            "total_value": d.total_value,
            "currency": d.currency,
            "last_update": d.last_update,
            "linked_portfolio_id": linked_map.get(d.id),
        }
        result.append(entry)
    return {"depots": result}


@app.get("/api/banking-bridge/depots/{depot_id}/holdings")
async def banking_bridge_depot_holdings(depot_id: int) -> Dict[str, Any]:
    client = _get_bb_client()
    try:
        holdings = await asyncio.to_thread(client.get_holdings, depot_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return {
        "holdings": [
            {
                "isin": h.isin,
                "wkn": h.wkn,
                "name": h.name,
                "quantity": h.quantity,
                "currency": h.currency,
                "current_price": h.current_price,
                "purchase_price": h.purchase_price,
                "total_value": h.total_value,
                "profit_loss": h.profit_loss,
                "profit_loss_percent": h.profit_loss_percent,
                "price_date": h.price_date,
                "updated_at": h.updated_at,
            }
            for h in holdings
        ]
    }


def _get_or_create_instrument_for_holding(
    isin: str, name: str, currency: str, wkn: Optional[str] = None
) -> int:
    """Findet oder erstellt ein Instrument fuer einen Banking Bridge Holding."""
    isin_upper = isin.strip().upper()
    if not isin_upper:
        raise ValueError("ISIN darf nicht leer sein")

    # Suche nach existierendem Instrument mit gleicher ISIN
    row = _conn.execute(
        "SELECT id, code, name FROM instruments WHERE isin=? AND (type IS NULL OR type='asset')",
        (isin_upper,),
    ).fetchone()
    if row:
        # Name aktualisieren, falls nur Code als Name gesetzt
        if name and (dict(row).get("name") or "").strip() == dict(row).get("code", ""):
            _conn.execute("UPDATE instruments SET name=? WHERE id=?", (name, row["id"]))
        return int(row["id"])

    # Suche ueber Code = ISIN
    row = _conn.execute(
        "SELECT id, name FROM instruments WHERE code=? AND (type IS NULL OR type='asset')",
        (isin_upper,),
    ).fetchone()
    if row:
        # ISIN nachsetzen
        _conn.execute("UPDATE instruments SET isin=? WHERE id=? AND (isin IS NULL OR isin='')", (isin_upper, row["id"]))
        if name and (dict(row).get("name") or "").strip() == isin_upper:
            _conn.execute("UPDATE instruments SET name=? WHERE id=?", (name, row["id"]))
        return int(row["id"])

    # Neues Instrument anlegen
    cur = _conn.execute(
        "INSERT INTO instruments(code, name, currency, type, isin, ls_item) VALUES (?,?,?,?,?,NULL)",
        (isin_upper, name or isin_upper, (currency or "EUR").strip().upper(), "asset", isin_upper),
    )
    instrument_id = int(cur.lastrowid)

    # Automatisch Tradegate-Kursquelle anlegen (ISIN-basiert)
    try:
        _conn.execute(
            "INSERT INTO instrument_sources(instrument_id, source, source_code, priority) VALUES (?,?,?,?)",
            (instrument_id, "tradegate", isin_upper, 10),
        )
    except Exception:
        pass
    # Automatisch Lightstreamer-Kursquelle anlegen (ISIN-basiert)
    try:
        _conn.execute(
            "INSERT INTO instrument_sources(instrument_id, source, source_code, priority) VALUES (?,?,?,?)",
            (instrument_id, "lightstreamer", isin_upper, 20),
        )
    except Exception:
        pass

    return instrument_id


class BankingBridgeImportRequest(BaseModel):
    depot_id: int
    portfolio_name: Optional[str] = None


@app.post("/api/banking-bridge/import")
async def banking_bridge_import_depot(body: BankingBridgeImportRequest) -> Dict[str, Any]:
    """
    Importiert ein Banking Bridge Depot als neues Portfolio.
    Erstellt bei Bedarf Instrumente und Positionen.
    """
    client = _get_bb_client()
    depot_id = body.depot_id

    # Pruefen ob bereits verknuepft
    existing = _conn.execute(
        "SELECT id, name FROM portfolios WHERE banking_bridge_depot_id=?", (depot_id,)
    ).fetchone()
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Depot {depot_id} ist bereits mit Portfolio '{existing['name']}' (#{existing['id']}) verknuepft",
        )

    # Depot und Holdings laden
    try:
        depot = await asyncio.to_thread(client.get_depot, depot_id)
        holdings = await asyncio.to_thread(client.get_holdings, depot_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    portfolio_name = (body.portfolio_name or "").strip() or f"{depot.name} ({depot.bank})"
    portfolio_currency = (depot.currency or "EUR").strip().upper()

    # Portfolio anlegen
    max_row = _conn.execute("SELECT COALESCE(MAX(sort_order), -1) AS max_order FROM portfolios").fetchone()
    next_order = int(max_row["max_order"] if max_row else -1) + 1
    cur = _conn.execute(
        "INSERT INTO portfolios(name, currency, sort_order, banking_bridge_depot_id) VALUES (?,?,?,?)",
        (portfolio_name, portfolio_currency, next_order, depot_id),
    )
    portfolio_id = int(cur.lastrowid)

    # Positionen anlegen
    imported_count = 0
    skipped: List[str] = []
    for idx, h in enumerate(holdings):
        if not h.isin or h.quantity <= 0:
            skipped.append(f"{h.name or 'Unbekannt'} (keine ISIN oder Menge<=0)")
            continue
        try:
            instrument_id = _get_or_create_instrument_for_holding(
                isin=h.isin,
                name=h.name,
                currency=h.currency,
                wkn=h.wkn,
            )
            entry_price = h.purchase_price if h.purchase_price and h.purchase_price > 0 else 0.01
            pos_currency = (h.currency or portfolio_currency).strip().upper()
            _conn.execute(
                "INSERT INTO positions(portfolio_id, instrument_id, isin, name, quantity, entry_price, currency, sort_order) VALUES (?,?,?,?,?,?,?,?)",
                (portfolio_id, instrument_id, h.isin.upper(), h.name or None, h.quantity, entry_price, pos_currency, idx),
            )
            imported_count += 1
        except Exception as exc:
            skipped.append(f"{h.name or h.isin}: {exc}")

    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()

    return {
        "portfolio_id": portfolio_id,
        "portfolio_name": portfolio_name,
        "depot_id": depot_id,
        "imported_positions": imported_count,
        "skipped": skipped,
        "total_holdings": len(holdings),
    }


@app.post("/api/banking-bridge/sync/{portfolio_id}")
async def banking_bridge_sync_portfolio(portfolio_id: int) -> Dict[str, Any]:
    """
    Synchronisiert ein verknuepftes Portfolio mit dem Banking Bridge Depot.
    Aktualisiert bestehende Positionen (Menge, Entry) und fuegt neue hinzu.
    Entfernt Positionen, die im Depot nicht mehr vorhanden sind.
    """
    # Portfolio mit BB-Verknuepfung laden
    prow = _conn.execute(
        "SELECT id, name, currency, banking_bridge_depot_id FROM portfolios WHERE id=?",
        (portfolio_id,),
    ).fetchone()
    if not prow:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")
    depot_id = prow["banking_bridge_depot_id"]
    if not depot_id:
        raise HTTPException(status_code=400, detail="Portfolio ist nicht mit einem Banking Bridge Depot verknuepft")
    portfolio_currency = (prow["currency"] or "EUR").strip().upper()

    # Holdings vom Banking Bridge laden
    client = _get_bb_client()
    try:
        holdings = await asyncio.to_thread(client.get_holdings, int(depot_id))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    # Bestehende Positionen laden
    existing_positions = _conn.execute(
        """
        SELECT pos.id, pos.instrument_id, pos.isin, pos.quantity, pos.entry_price, pos.currency,
               instr.isin AS instrument_isin
        FROM positions pos
        LEFT JOIN instruments instr ON instr.id = pos.instrument_id
        WHERE pos.portfolio_id=?
        """,
        (portfolio_id,),
    ).fetchall()

    # Map: ISIN -> bestehende Position
    existing_by_isin: Dict[str, Dict[str, Any]] = {}
    for p in existing_positions:
        isin_key = (p["instrument_isin"] or p["isin"] or "").strip().upper()
        if isin_key:
            existing_by_isin[isin_key] = dict(p)

    # Holdings-ISINs Set
    holding_isins: set[str] = set()
    for h in holdings:
        if h.isin:
            holding_isins.add(h.isin.strip().upper())

    added = 0
    updated = 0
    removed = 0
    skipped: List[str] = []
    max_sort = _conn.execute(
        "SELECT COALESCE(MAX(sort_order), -1) AS mx FROM positions WHERE portfolio_id=?",
        (portfolio_id,),
    ).fetchone()
    next_sort = int(max_sort["mx"] if max_sort else -1) + 1

    for h in holdings:
        if not h.isin or h.quantity <= 0:
            skipped.append(f"{h.name or 'Unbekannt'} (keine ISIN oder Menge<=0)")
            continue

        isin_upper = h.isin.strip().upper()
        entry_price = h.purchase_price if h.purchase_price and h.purchase_price > 0 else 0.01
        pos_currency = (h.currency or portfolio_currency).strip().upper()

        if isin_upper in existing_by_isin:
            # Position aktualisieren
            pos = existing_by_isin[isin_upper]
            changes: List[str] = []
            values: List[Any] = []

            if abs(float(pos["quantity"]) - h.quantity) > 1e-6:
                changes.append("quantity=?")
                values.append(h.quantity)
            if entry_price and abs(float(pos["entry_price"]) - entry_price) > 1e-6:
                changes.append("entry_price=?")
                values.append(entry_price)
            if pos_currency and pos_currency != (pos["currency"] or "EUR").strip().upper():
                changes.append("currency=?")
                values.append(pos_currency)

            if changes:
                values.append(pos["id"])
                _conn.execute(
                    f"UPDATE positions SET {', '.join(changes)} WHERE id=?",
                    tuple(values),
                )
                updated += 1
        else:
            # Neue Position anlegen
            try:
                instrument_id = _get_or_create_instrument_for_holding(
                    isin=h.isin,
                    name=h.name,
                    currency=h.currency,
                    wkn=h.wkn,
                )
                _conn.execute(
                    "INSERT INTO positions(portfolio_id, instrument_id, isin, name, quantity, entry_price, currency, sort_order) VALUES (?,?,?,?,?,?,?,?)",
                    (portfolio_id, instrument_id, isin_upper, h.name or None, h.quantity, entry_price, pos_currency, next_sort),
                )
                next_sort += 1
                added += 1
            except Exception as exc:
                skipped.append(f"{h.name or h.isin}: {exc}")

    # Positionen entfernen, die nicht mehr im Depot sind
    for isin_key, pos in existing_by_isin.items():
        if isin_key not in holding_isins:
            _conn.execute("DELETE FROM positions WHERE id=?", (pos["id"],))
            removed += 1

    _conn.commit()
    stream_manager.mark_dirty()
    _publish_mqtt_snapshot()

    return {
        "portfolio_id": portfolio_id,
        "depot_id": int(depot_id),
        "added": added,
        "updated": updated,
        "removed": removed,
        "skipped": skipped,
        "total_holdings": len(holdings),
    }


@app.post("/api/banking-bridge/unlink/{portfolio_id}")
async def banking_bridge_unlink_portfolio(portfolio_id: int) -> Dict[str, Any]:
    """Entfernt die Verknuepfung eines Portfolios mit einem Banking Bridge Depot."""
    prow = _conn.execute(
        "SELECT id, name, banking_bridge_depot_id FROM portfolios WHERE id=?",
        (portfolio_id,),
    ).fetchone()
    if not prow:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")
    if not prow["banking_bridge_depot_id"]:
        raise HTTPException(status_code=400, detail="Portfolio ist nicht verknuepft")
    _conn.execute("UPDATE portfolios SET banking_bridge_depot_id=NULL WHERE id=?", (portfolio_id,))
    _conn.commit()
    return {"portfolio_id": portfolio_id, "unlinked": True}


@app.post("/api/banking-bridge/link/{portfolio_id}")
async def banking_bridge_link_portfolio(portfolio_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Verknuepft ein bestehendes Portfolio mit einem Banking Bridge Depot."""
    depot_id = body.get("depot_id")
    if not depot_id:
        raise HTTPException(status_code=400, detail="depot_id ist erforderlich")
    depot_id = int(depot_id)

    prow = _conn.execute("SELECT id, name FROM portfolios WHERE id=?", (portfolio_id,)).fetchone()
    if not prow:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    # Pruefen ob Depot bereits verknuepft
    existing = _conn.execute(
        "SELECT id, name FROM portfolios WHERE banking_bridge_depot_id=? AND id<>?",
        (depot_id, portfolio_id),
    ).fetchone()
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Depot {depot_id} ist bereits mit Portfolio '{existing['name']}' (#{existing['id']}) verknuepft",
        )

    _conn.execute("UPDATE portfolios SET banking_bridge_depot_id=? WHERE id=?", (depot_id, portfolio_id))
    _conn.commit()
    return {"portfolio_id": portfolio_id, "depot_id": depot_id, "linked": True}
