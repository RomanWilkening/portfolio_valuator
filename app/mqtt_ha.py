import json
import logging
import os
import re
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Set

import paho.mqtt.client as mqtt

logger = logging.getLogger("portfolio-valuator.mqtt")


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "on")


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
        self._last_publish_ts = 0.0
        self._known_objects: Set[str] = set()

    def connect(self) -> None:
        if not self.s.host:
            raise RuntimeError("MQTT_HOST ist leer.")

        client = mqtt.Client(client_id=self.s.client_id, protocol=mqtt.MQTTv311)
        if self.s.username:
            client.username_pw_set(self.s.username, self.s.password)

        def on_connect(c, userdata, flags, rc, properties=None):  # type: ignore[no-untyped-def]
            logger.info("MQTT connected (rc=%s)", rc)
            c.publish(self.s.availability_topic, payload="online", qos=self.s.qos, retain=True)

        def on_disconnect(c, userdata, rc, properties=None):  # type: ignore[no-untyped-def]
            logger.warning("MQTT disconnected (rc=%s)", rc)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.will_set(self.s.availability_topic, payload="offline", qos=self.s.qos, retain=True)
        client.connect(self.s.host, self.s.port, keepalive=30)
        client.loop_start()
        self.client = client

    def close(self) -> None:
        if not self.client:
            return
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
        state_class: Optional[str] = "measurement",
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

    def _debounced(self) -> bool:
        now = time.time()
        min_dt = self.s.debounce_ms / 1000.0
        if min_dt > 0 and (now - self._last_publish_ts) < min_dt:
            return True
        self._last_publish_ts = now
        return False

    def publish_all(self, *, portfolios: list[dict], watchlist: list[dict]) -> None:
        if not self.client:
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
        ) -> None:
            oid = _slug(object_id)
            desired.add(oid)
            if oid not in self._known_objects:
                self._publish_discovery(
                    object_id=oid,
                    name=name,
                    unit=unit,
                    device_class=device_class,
                    state_class="measurement",
                    extra_attrs=attrs,
                )
            self._publish_state(oid, value, attrs)

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
            add_sensor(f"{base}_wert", f"{pf_name} Wert", round(float(mv or 0.0), 2), currency, "monetary", {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency})
            add_sensor(f"{base}_basis", f"{pf_name} Basis", round(float(cb or 0.0), 2), currency, "monetary", {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency})
            add_sensor(f"{base}_performance", f"{pf_name} Performance", round(float(pnl or 0.0), 2), currency, "monetary", {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency})
            add_sensor(
                f"{base}_performance_pct",
                f"{pf_name} Performance%",
                round(float((pnl_pct or 0.0) * 100.0), 2),
                "%",
                None,
                {"id": pid, "type": "portfolio", "name": pf_name, "currency": currency},
            )

            for pos in (pf or {}).get("positions") or []:
                pos_id = pos.get("id")
                if pos_id is None:
                    continue
                isin = pos.get("isin")
                qty = pos.get("quantity")
                bid = pos.get("bid")
                cost_basis = pos.get("cost_basis")
                market_value = pos.get("market_value")
                p_pnl = pos.get("pnl")
                p_pnl_pct = pos.get("pnl_pct")
                pos_currency = (pos.get("currency") or currency or "EUR").strip().upper()

                isin_s = (isin or str(pos_id) or "").strip()
                # object_id muss eindeutig sein, daher Portfolio-ID anhängen (gleiche ISIN kann in mehreren Portfolios vorkommen)
                pbase = f"{isin_s}_p{pid}"
                add_sensor(f"{pbase}_stueck", f"{isin_s} Stück", round(float(qty or 0.0), 2), "stk", None, {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency})
                add_sensor(f"{pbase}_kurs", f"{isin_s} Kurs", round(float(bid or 0.0), 4), pos_currency, "monetary", {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency})
                add_sensor(f"{pbase}_basis", f"{isin_s} Basis", round(float(cost_basis or 0.0), 2), pos_currency, "monetary", {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency})
                add_sensor(f"{pbase}_wert", f"{isin_s} Wert", round(float(market_value or 0.0), 2), pos_currency, "monetary", {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency})
                add_sensor(f"{pbase}_performance", f"{isin_s} Performance", round(float(p_pnl or 0.0), 2), pos_currency, "monetary", {"id": pos_id, "type": "position", "isin": isin_s, "portfolio_id": pid, "currency": pos_currency})
                add_sensor(
                    f"{pbase}_performance_pct",
                    f"{isin_s} Performance%",
                    round(float((p_pnl_pct or 0.0) * 100.0), 2),
                    "%",
                    None,
                    {"id": pos_id, "type": "position", "isin": isin, "portfolio_id": pid, "currency": pos_currency},
                )

        # Watchlist
        for w in watchlist:
            wid = w.get("id")
            if wid is None:
                continue
            label = (w.get("label") or "").strip()
            key = w.get("key") or w.get("isin")
            currency = (w.get("currency") or "EUR").strip().upper()
            price = w.get("price")
            field = w.get("field")
            key_s = (key or str(wid)).strip()
            # Währung im Sensor-Titel, damit es in HA eindeutig ist
            name = f"{key_s} Kurs {currency}" + (f" ({label})" if label else "")
            add_sensor(
                f"watch_{key_s}_kurs",
                name,
                round(float(price or 0.0), 4),
                currency,
                "monetary",
                {"id": wid, "type": "watchlist", "key": key, "label": label, "field": field},
            )

        # Cleanup removed sensors (remove discovery)
        removed = self._known_objects - desired
        for oid in removed:
            self.client.publish(self._discovery_topic(oid), payload="", qos=self.s.qos, retain=True)
        self._known_objects = desired

