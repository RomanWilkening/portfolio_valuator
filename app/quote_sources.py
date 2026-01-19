import asyncio
import json
import logging
import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

SOURCE_LIGHTSTREAMER = "lightstreamer"
SOURCE_TRADEGATE = "tradegate"

DEFAULT_SOURCE_PRIORITY = [SOURCE_LIGHTSTREAMER, SOURCE_TRADEGATE]

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
        self.priority = [p.strip().lower() for p in priority if p and p.strip()]
        if not self.priority:
            self.priority = list(DEFAULT_SOURCE_PRIORITY)

        self.source_bids: Dict[str, Dict[str, float]] = {s: {} for s in self.priority}
        self.source_prices: Dict[str, Dict[str, float]] = {s: {} for s in self.priority}
        self.source_price_fields: Dict[str, Dict[str, str]] = {s: {} for s in self.priority}
        self.source_watch_prices: Dict[str, Dict[str, float]] = {s: {} for s in self.priority}
        self.source_watch_fields: Dict[str, Dict[str, str]] = {s: {} for s in self.priority}

        self.best_bids: Dict[str, float] = {}
        self.best_bid_source: Dict[str, str] = {}
        self.best_prices: Dict[str, float] = {}
        self.best_price_source: Dict[str, str] = {}
        self.best_price_field: Dict[str, str] = {}
        self.best_watch_prices: Dict[str, float] = {}
        self.best_watch_fields: Dict[str, str] = {}
        self.best_watch_source: Dict[str, str] = {}

    def _pick_best(self, source_map: Dict[str, Dict[str, float]], key: str) -> tuple[Optional[float], Optional[str]]:
        for src in self.priority:
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
        if not src or src not in self.priority:
            return False
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
                break
            except Exception:
                logger.exception("TradegatePoller error")
                await asyncio.sleep(2.0)
