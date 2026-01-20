import asyncio
import json
import logging
import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

SOURCE_LIGHTSTREAMER = "lightstreamer"
SOURCE_TRADEGATE = "tradegate"
SOURCE_BITFINEX = "bitfinex"

DEFAULT_SOURCE_PRIORITY = [SOURCE_LIGHTSTREAMER, SOURCE_TRADEGATE, SOURCE_BITFINEX]

logger = logging.getLogger("portfolio-valuator.quote-sources")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def is_isin(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{12}", (value or "").strip().upper()))


def parse_source_priority(raw: Optional[str]) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return list(DEFAULT_SOURCE_PRIORITY)
    items = [s.strip().lower() for s in raw.split(",")]
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out or list(DEFAULT_SOURCE_PRIORITY)


class QuoteRouter:
    def __init__(self, priority: Iterable[str]) -> None:
        self.default_priority = [p.strip().lower() for p in priority if p and p.strip()]
        if not self.default_priority:
            self.default_priority = list(DEFAULT_SOURCE_PRIORITY)
        self.priority_by_key: Dict[str, List[str]] = {}
        self.known_sources: set[str] = set(self.default_priority)

        self.source_bids: Dict[str, Dict[str, float]] = {s: {} for s in self.known_sources}
        self.source_prices: Dict[str, Dict[str, float]] = {s: {} for s in self.known_sources}
        self.source_price_fields: Dict[str, Dict[str, str]] = {s: {} for s in self.known_sources}
        self.source_watch_prices: Dict[str, Dict[str, float]] = {s: {} for s in self.known_sources}
        self.source_watch_fields: Dict[str, Dict[str, str]] = {s: {} for s in self.known_sources}
        self.source_last_update: Dict[str, Dict[str, str]] = {s: {} for s in self.known_sources}

        self.best_bids: Dict[str, float] = {}
        self.best_bid_source: Dict[str, str] = {}
        self.best_prices: Dict[str, float] = {}
        self.best_price_source: Dict[str, str] = {}
        self.best_price_field: Dict[str, str] = {}
        self.best_watch_prices: Dict[str, float] = {}
        self.best_watch_fields: Dict[str, str] = {}
        self.best_watch_source: Dict[str, str] = {}

    def _get_priority(self, key: str) -> List[str]:
        return self.priority_by_key.get(key, self.default_priority)

    def _pick_best(self, source_map: Dict[str, Dict[str, float]], key: str) -> tuple[Optional[float], Optional[str]]:
        for src in self._get_priority(key):
            val = source_map.get(src, {}).get(key)
            if val is not None:
                return val, src
        return None, None

    def _refresh_best_bid(self, key: str) -> bool:
        new_bid, new_src = self._pick_best(self.source_bids, key)
        old_bid = self.best_bids.get(key)
        old_src = self.best_bid_source.get(key)
        if new_bid is None:
            if old_bid is None and old_src is None:
                return False
            self.best_bids.pop(key, None)
            self.best_bid_source.pop(key, None)
            return True
        if old_bid != new_bid or old_src != new_src:
            self.best_bids[key] = float(new_bid)
            assert new_src is not None
            self.best_bid_source[key] = new_src
            return True
        return False

    def _refresh_best_price(self, key: str) -> bool:
        new_price, new_src = self._pick_best(self.source_prices, key)
        old_price = self.best_prices.get(key)
        old_src = self.best_price_source.get(key)
        old_field = self.best_price_field.get(key)
        if new_price is None:
            if old_price is None and old_src is None:
                return False
            self.best_prices.pop(key, None)
            self.best_price_source.pop(key, None)
            self.best_price_field.pop(key, None)
            return True
        assert new_src is not None
        new_field = self.source_price_fields.get(new_src, {}).get(key)
        if old_price != new_price or old_src != new_src or old_field != new_field:
            self.best_prices[key] = float(new_price)
            self.best_price_source[key] = new_src
            if new_field is not None:
                self.best_price_field[key] = new_field
            else:
                self.best_price_field.pop(key, None)
            return True
        return False

    def _refresh_best_watch(self, key: str) -> bool:
        new_price, new_src = self._pick_best(self.source_watch_prices, key)
        old_price = self.best_watch_prices.get(key)
        old_src = self.best_watch_source.get(key)
        old_field = self.best_watch_fields.get(key)
        if new_price is None:
            if old_price is None and old_src is None:
                return False
            self.best_watch_prices.pop(key, None)
            self.best_watch_fields.pop(key, None)
            self.best_watch_source.pop(key, None)
            return True
        assert new_src is not None
        new_field = self.source_watch_fields.get(new_src, {}).get(key)
        if old_price != new_price or old_src != new_src or old_field != new_field:
            self.best_watch_prices[key] = float(new_price)
            self.best_watch_source[key] = new_src
            if new_field is not None:
                self.best_watch_fields[key] = new_field
            else:
                self.best_watch_fields.pop(key, None)
            return True
        return False

    def update_from_source(
        self,
        *,
        source: str,
        key: str,
        bid: Optional[float] = None,
        price: Optional[float] = None,
        price_field: Optional[str] = None,
        watch_price: Optional[float] = None,
        watch_field: Optional[str] = None,
    ) -> bool:
        src = (source or "").strip().lower()
        if not src:
            return False
        if src not in self.known_sources:
            self.known_sources.add(src)
            self.source_bids.setdefault(src, {})
            self.source_prices.setdefault(src, {})
            self.source_price_fields.setdefault(src, {})
            self.source_watch_prices.setdefault(src, {})
            self.source_watch_fields.setdefault(src, {})
        changed = False
        if bid is not None:
            self.source_bids.setdefault(src, {})[key] = float(bid)
            changed = self._refresh_best_bid(key) or changed
        if price is not None:
            self.source_prices.setdefault(src, {})[key] = float(price)
            if price_field is not None:
                self.source_price_fields.setdefault(src, {})[key] = str(price_field)
            changed = self._refresh_best_price(key) or changed
        if watch_price is not None:
            self.source_watch_prices.setdefault(src, {})[key] = float(watch_price)
            if watch_field is not None:
                self.source_watch_fields.setdefault(src, {})[key] = str(watch_field)
            changed = self._refresh_best_watch(key) or changed
        if bid is not None or price is not None or watch_price is not None:
            self.source_last_update.setdefault(src, {})[key] = now_iso()
        return changed

    def trim_keys(self, keep: Iterable[str]) -> bool:
        keep_set = {k for k in keep if k}
        changed = False
        for src in list(self.source_bids.keys()):
            for key in list(self.source_bids.get(src, {}).keys()):
                if key not in keep_set:
                    self.source_bids[src].pop(key, None)
                    changed = True
        for src in list(self.source_prices.keys()):
            for key in list(self.source_prices.get(src, {}).keys()):
                if key not in keep_set:
                    self.source_prices[src].pop(key, None)
                    self.source_price_fields.get(src, {}).pop(key, None)
                    changed = True
        for src in list(self.source_watch_prices.keys()):
            for key in list(self.source_watch_prices.get(src, {}).keys()):
                if key not in keep_set:
                    self.source_watch_prices[src].pop(key, None)
                    self.source_watch_fields.get(src, {}).pop(key, None)
                    changed = True
        for key in list(self.best_prices.keys()):
            if key not in keep_set:
                self.best_prices.pop(key, None)
                self.best_price_source.pop(key, None)
                self.best_price_field.pop(key, None)
                changed = True
        for key in list(self.best_bids.keys()):
            if key not in keep_set:
                self.best_bids.pop(key, None)
                self.best_bid_source.pop(key, None)
                changed = True
        for key in list(self.best_watch_prices.keys()):
            if key not in keep_set:
                self.best_watch_prices.pop(key, None)
                self.best_watch_fields.pop(key, None)
                self.best_watch_source.pop(key, None)
                changed = True
        return changed

    def set_priorities(self, priorities: Dict[str, List[str]]) -> None:
        self.priority_by_key = {}
        for key, sources in (priorities or {}).items():
            clean = [s.strip().lower() for s in sources if s and s.strip()]
            if clean:
                self.priority_by_key[key] = clean
                self.known_sources.update(clean)
        for src in list(self.known_sources):
            self.source_bids.setdefault(src, {})
            self.source_prices.setdefault(src, {})
            self.source_price_fields.setdefault(src, {})
            self.source_watch_prices.setdefault(src, {})
            self.source_watch_fields.setdefault(src, {})
            self.source_last_update.setdefault(src, {})


@dataclass(frozen=True)
class TradegateSettings:
    url_template: str
    timeout_s: float
    poll_s: float
    user_agent: str


def load_tradegate_settings() -> TradegateSettings:
    return TradegateSettings(
        url_template=os.getenv("TRADEGATE_URL_TEMPLATE", "https://www.tradegate.de/refresh.php?isin={isin}"),
        timeout_s=float(os.getenv("TRADEGATE_TIMEOUT_S", "5")),
        poll_s=float(os.getenv("TRADEGATE_POLL_S", "10")),
        user_agent=os.getenv("TRADEGATE_USER_AGENT", "portfolio-valuator/1.0"),
    )


def _parse_tradegate_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        s = s.replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None
    return None


def parse_tradegate_price(payload: Dict[str, Any]) -> tuple[Optional[float], Optional[str]]:
    bid = _parse_tradegate_number(payload.get("bid"))
    last = _parse_tradegate_number(payload.get("last"))
    ask = _parse_tradegate_number(payload.get("ask"))
    for name, val in (("bid", bid), ("last", last), ("ask", ask)):
        if val is not None:
            return val, name
    return None, None


class TradegateClient:
    def __init__(self, settings: TradegateSettings) -> None:
        self.settings = settings

    def fetch(self, isin: str) -> Optional[Dict[str, Any]]:
        url = self.settings.url_template.format(isin=isin)
        headers: Dict[str, str] = {}
        if self.settings.user_agent:
            headers["User-Agent"] = self.settings.user_agent
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.settings.timeout_s) as resp:
                raw = resp.read().decode("utf-8")
        except Exception as exc:
            logger.warning("Tradegate request failed for %s: %s", isin, exc)
            return None
        if not raw or not raw.strip():
            logger.debug("Tradegate empty response for %s", isin)
            return None
        stripped = raw.lstrip()
        if stripped.startswith("<"):
            logger.debug("Tradegate non-json response for %s", isin)
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            snippet = raw.strip().replace("\n", " ")[:120]
            logger.warning("Tradegate JSON decode failed for %s: %s (payload=%s)", isin, exc, snippet)
            return None
        if not isinstance(data, dict):
            logger.warning("Tradegate payload not a dict for %s", isin)
            return None
        return data


class TradegatePoller:
    def __init__(
        self,
        *,
        settings: TradegateSettings,
        router: QuoteRouter,
        on_best_update: Callable[[str, Optional[str]], Awaitable[None]],
    ) -> None:
        self.settings = settings
        self.router = router
        self.on_best_update = on_best_update
        self.client = TradegateClient(settings)
        self._mapping: Dict[str, str] = {}
        self._enabled = False
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run())

    def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = bool(enabled)

    def set_mapping(self, mapping: Dict[str, str]) -> None:
        self._mapping = {k: v for k, v in (mapping or {}).items() if k and v}

    async def _run(self) -> None:
        while True:
            try:
                if not self._enabled or not self._mapping:
                    await asyncio.sleep(0.5)
                    continue

                items = list(self._mapping.items())
                for key, isin in items:
                    payload = await asyncio.to_thread(self.client.fetch, isin)
                    if not payload:
                        continue
                    bid = _parse_tradegate_number(payload.get("bid"))
                    price, field = parse_tradegate_price(payload)
                    if price is None:
                        continue
                    changed = self.router.update_from_source(
                        source=SOURCE_TRADEGATE,
                        key=key,
                        bid=bid,
                        price=price,
                        price_field=field,
                        watch_price=price,
                        watch_field=None,
                    )
                    if changed:
                        await self.on_best_update(key, now_iso())

                await asyncio.sleep(max(0.5, float(self.settings.poll_s)))
            except asyncio.CancelledError:
                self.status = "stopped"
                break
            except Exception:
                logger.exception("TradegatePoller error")
                await asyncio.sleep(2.0)


@dataclass(frozen=True)
class BitfinexSettings:
    wss_url: str
    reconnect_min_s: float
    reconnect_max_s: float


def load_bitfinex_settings() -> BitfinexSettings:
    return BitfinexSettings(
        wss_url=os.getenv("BITFINEX_WSS_URL", "wss://api-pub.bitfinex.com/ws/2"),
        reconnect_min_s=float(os.getenv("BITFINEX_RECONNECT_MIN_S", "1.0")),
        reconnect_max_s=float(os.getenv("BITFINEX_RECONNECT_MAX_S", "30.0")),
    )


def normalize_bitfinex_symbol(symbol: str) -> Optional[str]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    if sym.startswith("T") or sym.startswith("F"):
        return sym
    return "T" + sym


def _bitfinex_pick_price(bid: Optional[float], ask: Optional[float], last: Optional[float]) -> tuple[Optional[float], Optional[str]]:
    def _valid(val: Optional[float]) -> bool:
        return val is not None and val != 0.0

    if _valid(bid):
        return bid, "bid"
    if bid is not None and ask is not None and bid != 0.0 and ask != 0.0:
        return (bid + ask) / 2.0, "mid"
    if _valid(last):
        return last, "last"
    if _valid(ask):
        return ask, "ask"
    return None, None


class BitfinexStream:
    def __init__(
        self,
        *,
        settings: BitfinexSettings,
        router: QuoteRouter,
        on_best_update: Callable[[str, Optional[str]], Awaitable[None]],
    ) -> None:
        self.settings = settings
        self.router = router
        self.on_best_update = on_best_update
        self._enabled = False
        self._task: Optional[asyncio.Task] = None
        self._mapping: Dict[str, set[str]] = {}
        self._version = 0
        self.status = "stopped"
        self.last_error: Optional[str] = None
        self.last_connect_at: Optional[str] = None
        self.last_message_at: Optional[str] = None
        self.subscribed_symbols: set[str] = set()
        self.acked_symbols: set[str] = set()

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run())

    def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
        self.status = "stopped"

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = bool(enabled)

    def set_mapping(self, mapping: Dict[str, set[str]]) -> None:
        self._mapping = {k: set(v) for k, v in (mapping or {}).items() if k and v}
        self._version += 1
        self.subscribed_symbols = set(self._mapping.keys())
        self.acked_symbols = set()

    def get_status(self) -> Dict[str, Any]:
        return {
            "enabled": self._enabled,
            "status": self.status,
            "wss_url": self.settings.wss_url,
            "mapping": {sym: sorted(list(keys)) for sym, keys in self._mapping.items()},
            "subscribed_symbols": sorted(self.subscribed_symbols),
            "acked_symbols": sorted(self.acked_symbols),
            "last_connect_at": self.last_connect_at,
            "last_message_at": self.last_message_at,
            "last_error": self.last_error,
            "version": self._version,
        }

    async def _run(self) -> None:
        import websockets

        backoff_s = max(0.1, self.settings.reconnect_min_s)
        while True:
            try:
                if not self._enabled:
                    self.status = "disabled"
                    await asyncio.sleep(0.5)
                    continue
                if not self._mapping:
                    self.status = "idle"
                    await asyncio.sleep(0.5)
                    continue

                version = self._version
                symbols = list(self._mapping.keys())

                self.status = "connecting"
                self.last_connect_at = now_iso()
                self.last_error = None
                async with websockets.connect(self.settings.wss_url, ping_interval=20, ping_timeout=20) as ws:
                    self.status = "connected"
                    chan_to_symbol: Dict[int, str] = {}
                    for symbol in symbols:
                        await ws.send(json.dumps({"event": "subscribe", "channel": "ticker", "symbol": symbol}))

                    while self._enabled and version == self._version:
                        raw = await ws.recv()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if isinstance(msg, dict):
                            if msg.get("event") == "subscribed" and msg.get("channel") == "ticker":
                                chan_id = msg.get("chanId")
                                sym = msg.get("symbol")
                                if isinstance(chan_id, int) and sym:
                                    chan_to_symbol[chan_id] = sym
                                    self.acked_symbols.add(sym)
                            continue

                        if not isinstance(msg, list) or len(msg) < 2:
                            continue
                        chan_id = msg[0]
                        payload = msg[1]
                        if payload == "hb":
                            continue
                        if not isinstance(chan_id, int) or chan_id not in chan_to_symbol:
                            continue
                        self.last_message_at = now_iso()
                        symbol = chan_to_symbol[chan_id]
                        data = payload if isinstance(payload, list) else None
                        if not data or len(data) < 7:
                            continue

                        bid = _parse_tradegate_number(data[0])
                        ask = _parse_tradegate_number(data[2])
                        last = _parse_tradegate_number(data[6])
                        price, field = _bitfinex_pick_price(bid, ask, last)
                        bid_val = bid if (bid is not None and bid != 0.0) else None

                        keys = self._mapping.get(symbol, set())
                        for key in keys:
                            changed = self.router.update_from_source(
                                source=SOURCE_BITFINEX,
                                key=key,
                                bid=bid_val,
                                price=price,
                                price_field=field,
                                watch_price=price,
                                watch_field=None,
                            )
                            if changed:
                                await self.on_best_update(key, now_iso())

                backoff_s = max(0.1, self.settings.reconnect_min_s)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.status = "error"
                self.last_error = str(exc)
                logger.exception("BitfinexStream error")
                await asyncio.sleep(backoff_s)
                backoff_s = min(self.settings.reconnect_max_s, max(self.settings.reconnect_min_s, backoff_s * 2.0))
