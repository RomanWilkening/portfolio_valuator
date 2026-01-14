import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import paho.mqtt.client as mqtt

logger = logging.getLogger("portfolio-valuator.mqtt")


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class MqttSettings:
    enabled: bool
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    client_id: str

    discovery_prefix: str
    node_id: str
    object_id: str

    base_topic: str
    qos: int
    retain: bool
    debounce_ms: int

    @property
    def state_topic(self) -> str:
        return f"{self.base_topic}/state"

    @property
    def attributes_topic(self) -> str:
        return f"{self.base_topic}/attributes"

    @property
    def availability_topic(self) -> str:
        return f"{self.base_topic}/availability"

    @property
    def discovery_topic(self) -> str:
        return f"{self.discovery_prefix}/sensor/{self.node_id}/{self.object_id}/config"


def load_mqtt_settings() -> MqttSettings:
    host = (os.getenv("MQTT_HOST") or "").strip()
    enabled = _env_bool("MQTT_ENABLED", default=bool(host))

    node_id = (os.getenv("MQTT_NODE_ID") or "").strip() or "portfolio_valuator"
    object_id = (os.getenv("MQTT_OBJECT_ID") or "").strip() or "portfolio"
    base_topic = (os.getenv("MQTT_BASE_TOPIC") or "").strip() or f"portfolio_valuator/{node_id}"

    return MqttSettings(
        enabled=enabled,
        host=host,
        port=int(os.getenv("MQTT_PORT", "1883")),
        username=(os.getenv("MQTT_USERNAME") or "").strip() or None,
        password=(os.getenv("MQTT_PASSWORD") or "").strip() or None,
        client_id=(os.getenv("MQTT_CLIENT_ID") or "").strip() or f"portfolio-valuator-{socket.gethostname()}",
        discovery_prefix=(os.getenv("MQTT_DISCOVERY_PREFIX") or "").strip() or "homeassistant",
        node_id=node_id,
        object_id=object_id,
        base_topic=base_topic,
        qos=int(os.getenv("MQTT_QOS", "0")),
        retain=_env_bool("MQTT_RETAIN", default=True),
        debounce_ms=int(os.getenv("MQTT_DEBOUNCE_MS", "1000")),
    )


class HomeAssistantMqttPublisher:
    """
    Publiziert eine einzelne Home-Assistant MQTT Discovery Sensor-Entität:
    - state_topic: ein kompakter numerischer State (Default: total_market_value)
    - attributes_topic: JSON mit Portfolios/Positionen/Watchlist (größerer Payload)
    """

    def __init__(self, settings: MqttSettings) -> None:
        self.s = settings
        self.client: Optional[mqtt.Client] = None
        self._connected = False
        self._last_publish_ts = 0.0

    def connect(self) -> None:
        if not self.s.enabled:
            return
        if not self.s.host:
            raise RuntimeError("MQTT_HOST ist leer, aber MQTT_ENABLED=true.")

        client = mqtt.Client(client_id=self.s.client_id, protocol=mqtt.MQTTv311)
        if self.s.username:
            client.username_pw_set(self.s.username, self.s.password)

        def on_connect(c, userdata, flags, rc, properties=None):  # type: ignore[no-untyped-def]
            self._connected = True
            logger.info("MQTT connected (rc=%s)", rc)
            # Availability online + discovery config (retained)
            c.publish(self.s.availability_topic, payload="online", qos=self.s.qos, retain=True)
            self.publish_discovery()

        def on_disconnect(c, userdata, rc, properties=None):  # type: ignore[no-untyped-def]
            self._connected = False
            logger.warning("MQTT disconnected (rc=%s)", rc)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        # "Last will" for availability
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
        self._connected = False

    def publish_discovery(self) -> None:
        if not self.client:
            return

        # Single sensor with attributes JSON
        unique_id = f"{self.s.node_id}_{self.s.object_id}"
        payload: Dict[str, Any] = {
            "name": "Portfolio Valuator",
            "unique_id": unique_id,
            "state_topic": self.s.state_topic,
            "json_attributes_topic": self.s.attributes_topic,
            "availability_topic": self.s.availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "icon": "mdi:chart-line",
            "device": {
                "identifiers": [self.s.node_id],
                "name": "Portfolio Valuator",
                "manufacturer": "RomanWilkening/portfolio_valuator",
                "model": "bnpp-ls-portfolio-valuator",
            },
        }

        self.client.publish(
            self.s.discovery_topic,
            payload=json.dumps(payload, ensure_ascii=False),
            qos=self.s.qos,
            retain=True,
        )

    def publish_now(self, state_value: Any, attributes: Dict[str, Any]) -> None:
        if not self.client:
            return

        # Debounce in-process (best-effort)
        now = time.time()
        min_dt = self.s.debounce_ms / 1000.0
        if min_dt > 0 and (now - self._last_publish_ts) < min_dt:
            return
        self._last_publish_ts = now

        self.client.publish(
            self.s.state_topic,
            payload=str(state_value),
            qos=self.s.qos,
            retain=self.s.retain,
        )
        self.client.publish(
            self.s.attributes_topic,
            payload=json.dumps(attributes, ensure_ascii=False),
            qos=self.s.qos,
            retain=self.s.retain,
        )


def build_entity_payload(
    *,
    portfolios: list[dict],
    watchlist: list[dict],
    meta: Optional[Dict[str, Any]] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    Returns (state, attributes) for the single HA entity.
    State is numeric total_market_value to keep state short.
    """
    total_mv = 0.0
    total_cb = 0.0
    total_pnl = 0.0

    for pf in portfolios:
        totals = (pf or {}).get("totals") or {}
        mv = totals.get("market_value")
        cb = totals.get("cost_basis")
        pnl = totals.get("pnl")
        if isinstance(mv, (int, float)):
            total_mv += float(mv)
        if isinstance(cb, (int, float)):
            total_cb += float(cb)
        if isinstance(pnl, (int, float)):
            total_pnl += float(pnl)

    attrs: Dict[str, Any] = {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "totals": {
            "market_value": round(total_mv, 2),
            "cost_basis": round(total_cb, 2),
            "pnl": round(total_pnl, 2),
        },
        "portfolios": portfolios,
        "watchlist": watchlist,
    }
    if meta:
        attrs["meta"] = meta

    return round(total_mv, 2), attrs

