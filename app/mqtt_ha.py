import json
import logging
import os
import re
import socket
import time
from dataclasses import dataclass
from math import isfinite
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import paho.mqtt.client as mqtt

logger = logging.getLogger("portfolio-valuator.mqtt")


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    v = (os.getenv(name) or "").strip()
    if v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "sensor"


@dataclass(frozen=True)
class MqttSettings:
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    client_id: str

    discovery_prefix: str
    node_id: str
    base_topic: str
    qos: int
    retain: bool
    debounce_ms: int
    # Plausibility / Sanity checks (to avoid "0 spikes" etc. in HA history)
    sanity_skip_zero_price: bool
    sanity_max_pct_change: float
    sanity_require_price_for_valuation: bool

    @property
    def availability_topic(self) -> str:
        return f"{self.base_topic}/availability"


def load_mqtt_settings() -> MqttSettings:
    host = (os.getenv("MQTT_HOST") or "").strip()
    node_id = (os.getenv("MQTT_NODE_ID") or "").strip() or "portfolio_valuator"
    base_topic = (os.getenv("MQTT_BASE_TOPIC") or "").strip() or f"portfolio_valuator/{node_id}"

    return MqttSettings(
        host=host,
        port=int(os.getenv("MQTT_PORT", "1883")),
        username=(os.getenv("MQTT_USERNAME") or "").strip() or None,
        password=(os.getenv("MQTT_PASSWORD") or "").strip() or None,
        client_id=(os.getenv("MQTT_CLIENT_ID") or "").strip() or f"portfolio-valuator-{socket.gethostname()}",
        discovery_prefix=(os.getenv("MQTT_DISCOVERY_PREFIX") or "").strip() or "homeassistant",
        node_id=node_id,
        base_topic=base_topic,
        qos=int(os.getenv("MQTT_QOS", "0")),
        retain=_env_bool("MQTT_RETAIN", default=True),
        # Default 0 => publish every push (no debounce)
        debounce_ms=int(os.getenv("MQTT_DEBOUNCE_MS", "0")),
        # Sanity checks
        sanity_skip_zero_price=_env_bool("MQTT_SANITY_SKIP_ZERO_PRICE", default=True),
        # 0 disables jump filtering. Otherwise: max % change per update before skipping the update.
        sanity_max_pct_change=_env_float("MQTT_SANITY_MAX_PCT_CHANGE", default=0.0),
        # If true: don't publish portfolio/position valuation sensors when no price is available.
        sanity_require_price_for_valuation=_env_bool("MQTT_SANITY_REQUIRE_PRICE_FOR_VALUATION", default=True),
    )


class HomeAssistantMqttPublisher:
    """
    Publiziert MQTT-Discovery Sensoren für:
    - alle Portfolios (Wert/Basis/Performance/Performance%)
    - alle Positionen (Stück/Kurs/Basis/Wert/Performance/Performance%)
    - Watchlist (Kurs)

    Home Assistant Auto-Detect per MQTT Discovery.
    """

    def __init__(self, settings: MqttSettings) -> None:
        self.s = settings
        self.client: Optional[mqtt.Client] = None
        self._connected: bool = False
        self._last_publish_ts = 0.0
        self._known_objects: Set[str] = set()
        # Cache discovery payloads so we can republish them after reconnects.
        self._discovery_cache: Dict[str, Dict[str, Any]] = {}
        # Last published state per sensor (for plausibility checks).
        self._last_state: Dict[str, float] = {}

    def connect(self) -> None:
        if not self.s.host:
            raise RuntimeError("MQTT_HOST ist leer.")

        client = mqtt.Client(client_id=self.s.client_id, protocol=mqtt.MQTTv311)
        if self.s.username:
            client.username_pw_set(self.s.username, self.s.password)

        def on_connect(c, userdata, flags, rc, properties=None):  # type: ignore[no-untyped-def]
            logger.info("MQTT connected (rc=%s)", rc)
            self._connected = True
            c.publish(self.s.availability_topic, payload="online", qos=self.s.qos, retain=True)
            # Make sure Home Assistant gets (back) the discovery configs after reconnects
            # (e.g. broker restart, network flap). Discovery topics are retained, but
            # republishing is harmless and helps in edge cases.
            try:
                for oid, payload in list(self._discovery_cache.items()):
                    c.publish(
                        self._discovery_topic(oid),
                        payload=json.dumps(payload, ensure_ascii=False),
                        qos=self.s.qos,
                        retain=True,
                    )
            except Exception as e:
                logger.warning("MQTT discovery republish failed: %s", e)

        def on_disconnect(c, userdata, rc, properties=None):  # type: ignore[no-untyped-def]
            logger.warning("MQTT disconnected (rc=%s)", rc)
            self._connected = False

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.will_set(self.s.availability_topic, payload="offline", qos=self.s.qos, retain=True)
        # Robust reconnect behaviour (no tight loop, automatic backoff).
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        # Use async connect so the app can keep running even if broker is down at startup.
        client.connect_async(self.s.host, self.s.port, keepalive=30)
        client.loop_start()
        self.client = client

    def close(self) -> None:
        if not self.client:
            return
        self._connected = False
        try:
            self.client.publish(self.s.availability_topic, payload="offline", qos=self.s.qos, retain=True)
        except Exception:
            pass
        try:
            self.client.loop_stop()
        except Exception:
            pass
        try:
            self.client.disconnect()
        except Exception:
            pass
        self.client = None

    def _discovery_topic(self, object_id: str) -> str:
        return f"{self.s.discovery_prefix}/sensor/{self.s.node_id}/{object_id}/config"

    def _state_topic(self, object_id: str) -> str:
        return f"{self.s.base_topic}/sensors/{object_id}/state"

    def _attributes_topic(self, object_id: str) -> str:
        return f"{self.s.base_topic}/sensors/{object_id}/attributes"

    def _publish_discovery(
        self,
        *,
        object_id: str,
        name: str,
        unit: Optional[str] = None,
        device_class: Optional[str] = None,
        # Wenn state_class gesetzt ist, zeigt Home Assistant beim Klick oft die Statistik-Ansicht
        # (5-Minuten-Aggregate). Für „jeden Punkt“ in der More-Info-Ansicht lassen wir state_class weg.
        state_class: Optional[str] = None,
        extra_attrs: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.client:
            return

        payload: Dict[str, Any] = {
            "name": name,
            "unique_id": f"{self.s.node_id}_{object_id}",
            "state_topic": self._state_topic(object_id),
            "json_attributes_topic": self._attributes_topic(object_id),
            "availability_topic": self.s.availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "device": {
                "identifiers": [self.s.node_id],
                "name": "Portfolio Valuator",
                "manufacturer": "RomanWilkening/portfolio_valuator",
                "model": "bnpp-ls-portfolio-valuator",
            },
        }
        if unit:
            payload["unit_of_measurement"] = unit
        if device_class:
            payload["device_class"] = device_class
        if state_class:
            payload["state_class"] = state_class
        if extra_attrs:
            payload["json_attributes_template"] = "{{ value_json | tojson }}"

        # Cache for reconnect republish.
        self._discovery_cache[object_id] = payload

        self.client.publish(
            self._discovery_topic(object_id),
            payload=json.dumps(payload, ensure_ascii=False),
            qos=self.s.qos,
            retain=True,
        )

    def _publish_state(self, object_id: str, state_value: Any, attributes: Dict[str, Any]) -> None:
        if not self.client:
            return
        self.client.publish(
            self._state_topic(object_id),
            payload=str(state_value),
            qos=self.s.qos,
            retain=self.s.retain,
        )
        self.client.publish(
            self._attributes_topic(object_id),
            payload=json.dumps(attributes, ensure_ascii=False),
            qos=self.s.qos,
            retain=self.s.retain,
        )

    def _coerce_number(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            v = float(value)
            return v if isfinite(v) else None
        try:
            v = float(str(value))
            return v if isfinite(v) else None
        except Exception:
            return None

    def _should_publish(self, *, oid: str, value: Any, kind: str) -> Tuple[bool, Optional[float]]:
        """
        Returns (publish?, coerced_value).

        kind:
        - price: skip 0.0 (configurable) and apply jump filter (optional)
        - value: apply jump filter (optional)
        - other: no special checks besides numeric coercion
        """
        v = self._coerce_number(value)
        if v is None:
            return False, None

        if kind == "price" and self.s.sanity_skip_zero_price and v == 0.0:
            return False, v

        max_pct = float(self.s.sanity_max_pct_change or 0.0)
        if max_pct > 0.0:
            prev = self._last_state.get(oid)
            if prev is not None:
                # percent change relative to previous value; handle prev==0 separately
                if prev == 0.0:
                    pct = abs(v - prev) * 100.0
                else:
                    pct = abs((v - prev) / prev) * 100.0
                if pct > max_pct:
                    logger.warning("MQTT sanity: skip %s update (%.6g -> %.6g, %.1f%% > %.1f%%)", oid, prev, v, pct, max_pct)
                    return False, v

        return True, v

    def _debounced(self) -> bool:
        now = time.time()
        min_dt = self.s.debounce_ms / 1000.0
        if min_dt > 0 and (now - self._last_publish_ts) < min_dt:
            return True
        self._last_publish_ts = now
        return False

    def publish_all(self, *, portfolios: list[dict], watchlist: list[dict]) -> None:
        if not self.client or not self._connected:
            return
        if self._debounced():
            return

        desired: Set[str] = set()

        def add_sensor(
            object_id: str,
            name: str,
            value: Any,
            unit: Optional[str],
            device_class: Optional[str],
            attrs: Dict[str, Any],
            *,
            kind: str = "other",
        ) -> None:
            oid = _slug(object_id)
            desired.add(oid)
            if oid not in self._known_objects:
                self._publish_discovery(
                    object_id=oid,
                    name=name,
                    unit=unit,
                    device_class=device_class,
                    state_class=None,
                    extra_attrs=attrs,
                )
            ok, coerced = self._should_publish(oid=oid, value=value, kind=kind)
            if not ok:
                return
            assert coerced is not None
            self._publish_state(oid, coerced, attrs)
            self._last_state[oid] = float(coerced)

        # Portfolios + Positionen
        for pf in portfolios:
            pfo = (pf or {}).get("portfolio") or {}
            pid = pfo.get("id")
            currency = (pf or {}).get("currency") or "EUR"
            totals = (pf or {}).get("totals") or {}
            mv = totals.get("market_value")
            cb = totals.get("cost_basis")
            pnl = totals.get("pnl")
            pnl_pct = totals.get("pnl_pct")

            if pid is None:
                continue

            pf_name = (pfo.get("name") or str(pid)).strip()
            base = f"portfolio_{pid}"
            positions = (pf or {}).get("positions") or []
            missing_price = any(((pos.get("price") is None) and (pos.get("bid") is None)) for pos in positions)

            # Basis ist immer stabil (Entry*Qty). Wert/Performance nur, wenn Kursdaten vorhanden sind.
            add_sensor(
                f"{base}_basis",
                f"{pf_name} Basis",
                round(float(cb or 0.0), 2),
                currency,
                "monetary",
                {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency},
                kind="value",
            )
            if (not self.s.sanity_require_price_for_valuation) or (not missing_price):
                add_sensor(
                    f"{base}_wert",
                    f"{pf_name} Wert",
                    None if mv is None else round(float(mv), 2),
                    currency,
                    "monetary",
                    {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency},
                    kind="value",
                )
                add_sensor(
                    f"{base}_performance",
                    f"{pf_name} Performance",
                    None if pnl is None else round(float(pnl), 2),
                    currency,
                    "monetary",
                    {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency},
                    kind="value",
                )
            add_sensor(
                f"{base}_performance_pct",
                f"{pf_name} Performance%",
                None if pnl_pct is None else round(float(pnl_pct) * 100.0, 2),
                "%",
                None,
                {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency},
                kind="other",
            )

            for pos in positions:
                pos_id = pos.get("id")
                if pos_id is None:
                    continue
                isin = pos.get("instrument_code") or pos.get("isin")
                qty = pos.get("quantity")
                price = pos.get("price")
                bid = pos.get("bid")
                price_for_kurs = price if price is not None else bid
                cost_basis = pos.get("cost_basis")
                market_value = pos.get("market_value")
                p_pnl = pos.get("pnl")
                p_pnl_pct = pos.get("pnl_pct")
                pos_currency = (pos.get("currency") or currency or "EUR").strip().upper()

                isin_s = (isin or str(pos_id) or "").strip()
                display_name = (pos.get("instrument_name") or isin_s).strip()
                # object_id muss eindeutig sein, daher Portfolio-ID anhängen (gleiche ISIN kann in mehreren Portfolios vorkommen)
                pbase = f"{isin_s}_p{pid}"
                add_sensor(
                    f"{pbase}_stueck",
                    f"{display_name} Stück",
                    round(float(qty or 0.0), 2),
                    "stk",
                    None,
                    {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency},
                    kind="other",
                )
                add_sensor(
                    f"{pbase}_basis",
                    f"{display_name} Basis",
                    round(float(cost_basis or 0.0), 2),
                    pos_currency,
                    "monetary",
                    {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency},
                    kind="value",
                )
                if (not self.s.sanity_require_price_for_valuation) or (price_for_kurs is not None):
                    add_sensor(
                        f"{pbase}_kurs",
                        f"{display_name} Kurs",
                        None if price_for_kurs is None else round(float(price_for_kurs), 4),
                        pos_currency,
                        "monetary",
                        {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency},
                        kind="price",
                    )
                    add_sensor(
                        f"{pbase}_wert",
                        f"{display_name} Wert",
                        None if market_value is None else round(float(market_value), 2),
                        pos_currency,
                        "monetary",
                        {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency},
                        kind="value",
                    )
                    add_sensor(
                        f"{pbase}_performance",
                        f"{display_name} Performance",
                        None if p_pnl is None else round(float(p_pnl), 2),
                        pos_currency,
                        "monetary",
                        {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency},
                        kind="value",
                    )
                add_sensor(
                    f"{pbase}_performance_pct",
                    f"{display_name} Performance%",
                    None if p_pnl_pct is None else round(float(p_pnl_pct) * 100.0, 2),
                    "%",
                    None,
                    {"id": pos_id, "type": "position", "isin": isin, "portfolio_id": pid, "currency": pos_currency},
                    kind="other",
                )

        # Watchlist
        for w in watchlist:
            wid = w.get("id")
            if wid is None:
                continue
            label = (w.get("label") or "").strip()
            key = w.get("key") or w.get("instrument_code") or w.get("isin")
            currency = (w.get("currency") or "EUR").strip().upper()
            price = w.get("price")
            field = w.get("field")
            key_s = (key or str(wid)).strip()
            # Währung im Sensor-Titel, damit es in HA eindeutig ist
            name = f"{key_s} Kurs {currency}" + (f" ({label})" if label else "")
            add_sensor(
                f"watch_{key_s}_kurs",
                name,
                None if price is None else round(float(price), 4),
                currency,
                "monetary",
                {"id": wid, "type": "watchlist", "key": key, "label": label, "field": field},
                kind="price",
            )

        # Cleanup removed sensors (remove discovery)
        removed = self._known_objects - desired
        for oid in removed:
            self.client.publish(self._discovery_topic(oid), payload="", qos=self.s.qos, retain=True)
            self._discovery_cache.pop(oid, None)
            self._last_state.pop(oid, None)
        self._known_objects = desired

