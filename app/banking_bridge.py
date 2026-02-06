"""
HTTP-Client fuer die Banking Bridge API.

Die Banking Bridge stellt Depot-Daten ueber FinTS bereit.
Dieser Client ermoeglicht den Import und die Synchronisation
von Depots als Portfolios im Portfolio Valuator.
"""

import json
import logging
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger("portfolio-valuator.banking-bridge")


@dataclass(frozen=True)
class BankingBridgeSettings:
    base_url: str  # z.B. "http://localhost:8080"


@dataclass(frozen=True)
class Depot:
    id: int
    name: str
    account_number: str
    sub_account: Optional[str]
    bank: str
    bank_code: str
    total_value: Optional[float]
    currency: str
    last_update: Optional[str]


@dataclass(frozen=True)
class Holding:
    isin: str
    wkn: Optional[str]
    name: str
    quantity: float
    currency: str
    current_price: Optional[float]
    purchase_price: Optional[float]
    total_value: Optional[float]
    profit_loss: Optional[float]
    profit_loss_percent: Optional[float]
    price_date: Optional[str]
    updated_at: Optional[str]


class BankingBridgeClient:
    """HTTP-Client fuer die Banking Bridge v1 API."""

    def __init__(self, settings: BankingBridgeSettings) -> None:
        self.settings = settings

    @property
    def base_url(self) -> str:
        url = (self.settings.base_url or "").rstrip("/")
        return url

    def _get(self, path: str, timeout: float = 10.0) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            logger.warning("Banking Bridge HTTP %s for %s: %s", exc.code, path, body[:200])
            raise RuntimeError(f"Banking Bridge HTTP {exc.code}: {body[:200]}") from exc
        except urllib.error.URLError as exc:
            logger.warning("Banking Bridge URL error for %s: %s", path, exc.reason)
            raise RuntimeError(f"Banking Bridge nicht erreichbar: {exc.reason}") from exc
        except Exception as exc:
            logger.warning("Banking Bridge request failed for %s: %s", path, exc)
            raise RuntimeError(f"Banking Bridge Fehler: {exc}") from exc

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Banking Bridge: Ungueltiges JSON: {exc}") from exc

        if isinstance(data, dict) and data.get("success") is False:
            msg = data.get("message") or data.get("error") or "Unbekannter Fehler"
            raise RuntimeError(f"Banking Bridge: {msg}")

        return data

    def check_connection(self) -> Dict[str, Any]:
        """Prueft die Verbindung zur Banking Bridge."""
        if not self.base_url:
            raise RuntimeError("Banking Bridge URL ist nicht konfiguriert")
        data = self._get("/api/v1/depots")
        return {
            "success": True,
            "depot_count": data.get("count", len(data.get("depots", []))),
        }

    def list_depots(self) -> List[Depot]:
        """Listet alle verfuegbaren Depots auf."""
        if not self.base_url:
            raise RuntimeError("Banking Bridge URL ist nicht konfiguriert")
        data = self._get("/api/v1/depots")
        depots: List[Depot] = []
        for d in data.get("depots", []):
            depots.append(
                Depot(
                    id=int(d["id"]),
                    name=d.get("name") or "Depot",
                    account_number=d.get("account_number") or "",
                    sub_account=d.get("sub_account"),
                    bank=d.get("bank") or "",
                    bank_code=d.get("bank_code") or "",
                    total_value=d.get("total_value"),
                    currency=d.get("currency") or "EUR",
                    last_update=d.get("last_update"),
                )
            )
        return depots

    def get_depot(self, depot_id: int) -> Depot:
        """Ruft Details zu einem spezifischen Depot ab."""
        if not self.base_url:
            raise RuntimeError("Banking Bridge URL ist nicht konfiguriert")
        data = self._get(f"/api/v1/depots/{depot_id}")
        d = data.get("depot", data)
        return Depot(
            id=int(d["id"]),
            name=d.get("name") or "Depot",
            account_number=d.get("account_number") or "",
            sub_account=d.get("sub_account"),
            bank=d.get("bank") or "",
            bank_code=d.get("bank_code") or "",
            total_value=d.get("total_value"),
            currency=d.get("currency") or "EUR",
            last_update=d.get("last_update"),
        )

    def get_holdings(self, depot_id: int) -> List[Holding]:
        """Ruft die Wertpapierbestaende eines Depots ab."""
        if not self.base_url:
            raise RuntimeError("Banking Bridge URL ist nicht konfiguriert")
        data = self._get(f"/api/v1/depots/{depot_id}/holdings")
        holdings: List[Holding] = []
        for h in data.get("holdings", []):
            holdings.append(
                Holding(
                    isin=h.get("isin") or "",
                    wkn=h.get("wkn"),
                    name=h.get("name") or "",
                    quantity=float(h.get("quantity") or 0),
                    currency=h.get("currency") or "EUR",
                    current_price=h.get("current_price"),
                    purchase_price=h.get("purchase_price"),
                    total_value=h.get("total_value"),
                    profit_loss=h.get("profit_loss"),
                    profit_loss_percent=h.get("profit_loss_percent"),
                    price_date=h.get("price_date"),
                    updated_at=h.get("updated_at"),
                )
            )
        return holdings
