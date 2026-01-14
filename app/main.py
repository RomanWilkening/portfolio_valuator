import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import websockets
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from websockets.exceptions import ConnectionClosed

from app.db import connect_db, init_db

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

# Für Bewertung benötigen wir nur Bid und Symbol + Zeit (optional).
SCHEMA_FIELDS: List[str] = ["symbol", "bid", "quotetime"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_isin(isin: str) -> str:
    isin = (isin or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{12}", isin):
        raise ValueError("ISIN muss 12 Zeichen (A-Z/0-9) sein.")
    return isin


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


class PositionIn(BaseModel):
    isin: str
    quantity: float = Field(gt=0)
    entry_price: float = Field(gt=0)


class PortfolioOut(BaseModel):
    id: int
    name: str


class PositionOut(BaseModel):
    id: int
    isin: str
    quantity: float
    entry_price: float


# ----------------- FastAPI App -----------------


app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

_conn = connect_db()
init_db(_conn)


@app.get("/")
async def index():
    return FileResponse("app/static/index.html")


@app.get("/api/portfolios")
async def list_portfolios() -> List[Dict[str, Any]]:
    cur = _conn.execute(
        """
        SELECT p.id, p.name, COUNT(pos.id) AS positions_count
        FROM portfolios p
        LEFT JOIN positions pos ON pos.portfolio_id = p.id
        GROUP BY p.id
        ORDER BY p.id DESC
        """
    )
    return [dict(r) for r in cur.fetchall()]


@app.post("/api/portfolios", status_code=201)
async def create_portfolio(body: PortfolioCreate) -> PortfolioOut:
    cur = _conn.execute("INSERT INTO portfolios(name) VALUES (?)", (body.name.strip(),))
    _conn.commit()
    return PortfolioOut(id=int(cur.lastrowid), name=body.name.strip())


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

    valued_at = now_iso()
    timeout_s = float(os.getenv("LS_BID_TIMEOUT", "8"))
    bids = await fetch_bids(all_isins, timeout_s=timeout_s) if all_isins else {}

    out: List[Dict[str, Any]] = []
    for pf in portfolios:
        pid = int(pf["id"])
        out.append(
            compute_valuation(
                pf,
                by_portfolio.get(pid, []),
                bids,
                valued_at=valued_at,
                timeout_s=timeout_s,
            )
        )
    return out


@app.get("/api/portfolios/{portfolio_id}")
async def get_portfolio(portfolio_id: int) -> Dict[str, Any]:
    cur = _conn.execute("SELECT id, name FROM portfolios WHERE id=?", (portfolio_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    cur2 = _conn.execute(
        "SELECT id, isin, quantity, entry_price FROM positions WHERE portfolio_id=? ORDER BY id ASC",
        (portfolio_id,),
    )
    return {"portfolio": dict(row), "positions": [dict(r) for r in cur2.fetchall()]}


@app.put("/api/portfolios/{portfolio_id}/positions")
async def replace_positions(portfolio_id: int, positions: List[PositionIn]) -> Dict[str, Any]:
    # Ensure portfolio exists
    cur = _conn.execute("SELECT id FROM portfolios WHERE id=?", (portfolio_id,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    cleaned: List[PositionIn] = []
    seen: set[str] = set()
    for p in positions:
        isin = validate_isin(p.isin)
        if isin in seen:
            raise HTTPException(status_code=400, detail=f"Doppelte ISIN im Request: {isin}")
        seen.add(isin)
        cleaned.append(PositionIn(isin=isin, quantity=p.quantity, entry_price=p.entry_price))

    with _conn:
        _conn.execute("DELETE FROM positions WHERE portfolio_id=?", (portfolio_id,))
        for p in cleaned:
            _conn.execute(
                "INSERT INTO positions(portfolio_id, isin, quantity, entry_price) VALUES (?,?,?,?)",
                (portfolio_id, p.isin, p.quantity, p.entry_price),
            )

    return await get_portfolio(portfolio_id)


@app.post("/api/portfolios/{portfolio_id}/value")
async def value_portfolio(portfolio_id: int) -> Dict[str, Any]:
    cur = _conn.execute("SELECT id, name FROM portfolios WHERE id=?", (portfolio_id,))
    portfolio = cur.fetchone()
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio nicht gefunden")

    cur2 = _conn.execute(
        "SELECT id, isin, quantity, entry_price FROM positions WHERE portfolio_id=? ORDER BY id ASC",
        (portfolio_id,),
    )
    positions = [dict(r) for r in cur2.fetchall()]
    valued_at = now_iso()
    if not positions:
        return compute_valuation(dict(portfolio), [], {}, valued_at=valued_at, timeout_s=0.0)

    isins = [p["isin"] for p in positions]
    timeout_s = float(os.getenv("LS_BID_TIMEOUT", "8"))
    bids = await fetch_bids(isins, timeout_s=timeout_s)
    return compute_valuation(dict(portfolio), positions, bids, valued_at=valued_at, timeout_s=timeout_s)
