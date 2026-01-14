import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from urllib.parse import quote

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bnpp-ls-mvp")

# --- Lightstreamer / BNP Settings (konfigurierbar via ENV) ---
import os

LS_WSS_URL = os.getenv("LS_WSS_URL", "wss://push.bnpparibas.com/lightstreamer")
LS_SUBPROTOCOL = os.getenv("LS_SUBPROTOCOL", "TLCP-2.5.0.lightstreamer.com")

LS_ADAPTER_SET = os.getenv("LS_ADAPTER_SET", "SmarthouseFeed")
LS_DATA_ADAPTER = os.getenv("LS_DATA_ADAPTER", "MDS5")

# "Browser-ähnliche" Defaults, kann angepasst werden.
LS_CID = os.getenv(
    "LS_CID",
    "pcYgxn8m8 feOojyA1V661f3g2.pz482h95IL5h",
)

# Origin ist wichtig (Server kann Origin prüfen).
# Wenn du den Origin-Header testweise deaktivieren willst: LS_ORIGIN="" setzen.
LS_ORIGIN = os.getenv("LS_ORIGIN", "https://derivate.bnpparibas.com") or None

# User-Agent nur kosmetisch/optional
LS_USER_AGENT = os.getenv(
    "LS_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

# Feldschema aus deiner Beobachtung
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


def isin_to_item(isin: str) -> str:
    """
    BNP scheint Produkt-Items so zu benennen: X0000010800<ISIN>.
    Falls du später andere Prefixe brauchst, hier erweitern.
    """
    isin = isin.strip().upper()
    # Minimalvalidierung (ISIN ist typischerweise 12 Zeichen, alphanumerisch)
    if not re.fullmatch(r"[A-Z0-9]{12}", isin):
        raise ValueError("ISIN muss 12 Zeichen (A-Z/0-9) sein.")
    return f"X0000010800{isin}"


def _try_num(v: Optional[str]) -> Any:
    """Konvertiert Zahlstrings zu float, sonst gibt String/None zurück."""
    if v is None:
        return None
    if v == "":
        return ""
    try:
        # BNP sendet meistens Punkt als Dezimaltrenner im Push
        return float(v)
    except Exception:
        return v


def decode_field_values(
    tokens: List[str],
    fields: List[str],
    prev_state: Dict[str, Optional[str]],
) -> Dict[str, Optional[str]]:
    """
    Decodiert Lightstreamer TLCP Values (| getrennt) mit:
    - "" -> unverändert
    - "#" -> null
    - "$" -> leerstring
    - "^N" -> N Felder unverändert (ab aktueller Position)
    """
    state = dict(prev_state)  # copy
    fi = 0  # field index
    ti = 0  # token index

    while fi < len(fields) and ti < len(tokens):
        tok = tokens[ti]

        # ^N: N Felder unverändert
        if tok.startswith("^") and tok[1:].isdigit():
            n = int(tok[1:])
            fi += n
            ti += 1
            continue

        field_name = fields[fi]

        if tok == "":
            # unverändert
            fi += 1
            ti += 1
            continue
        if tok == "#":
            state[field_name] = None
        elif tok == "$":
            state[field_name] = ""
        else:
            # Falls irgendwann ^P/^T auftaucht, lassen wir es als raw string stehen.
            state[field_name] = tok

        fi += 1
        ti += 1

    return state


async def _ws_connect() -> Any:
    """
    websockets hat in neueren Versionen Parameter umbenannt:
    - extra_headers -> additional_headers
    Wir unterstützen beides, damit es mit websockets==14.1 sauber läuft.
    """
    # Häufige Ursache für HTTP 400: Server akzeptiert Subprotocol/Origin nicht.
    # Wir probieren daher ein paar sinnvolle Kombinationen.
    subprotocol_candidates: List[str] = []
    for p in [
        LS_SUBPROTOCOL,
        os.getenv("LS_SUBPROTOCOL_FALLBACK_1", "TLCP-2.4.0.lightstreamer.com"),
        os.getenv("LS_SUBPROTOCOL_FALLBACK_2", "TLCP-2.3.0.lightstreamer.com"),
    ]:
        p = (p or "").strip()
        if p and p not in subprotocol_candidates:
            subprotocol_candidates.append(p)

    origin_candidates: List[Optional[str]] = []
    if LS_ORIGIN not in origin_candidates:
        origin_candidates.append(LS_ORIGIN)
    if None not in origin_candidates:
        origin_candidates.append(None)

    last_exc: Optional[BaseException] = None

    for proto in subprotocol_candidates:
        for origin in origin_candidates:
            for send_ua in (True, False):
                kwargs: Dict[str, Any] = {
                    "subprotocols": [proto],
                    "ping_interval": None,  # Lightstreamer nutzt eigene PROBE
                }
                if origin is not None:
                    kwargs["origin"] = origin

                headers: Dict[str, str] = {}
                if send_ua:
                    headers["User-Agent"] = LS_USER_AGENT

                try:
                    # websockets>=14 nutzt additional_headers
                    ws = await websockets.connect(
                        LS_WSS_URL,
                        **kwargs,
                        additional_headers=headers or None,
                    )
                    logger.info(
                        "WS connected (proto=%s, origin=%s, ua=%s)",
                        proto,
                        origin or "<none>",
                        "on" if send_ua else "off",
                    )
                    return ws
                except TypeError:
                    # Fallback für ältere Signaturen (extra_headers)
                    try:
                        ws = await websockets.connect(
                            LS_WSS_URL,
                            **kwargs,
                            extra_headers=headers or None,
                        )
                        logger.info(
                            "WS connected (proto=%s, origin=%s, ua=%s) [extra_headers]",
                            proto,
                            origin or "<none>",
                            "on" if send_ua else "off",
                        )
                        return ws
                    except Exception as e:
                        last_exc = e
                        logger.warning(
                            "WS connect failed (proto=%s, origin=%s, ua=%s): %s",
                            proto,
                            origin or "<none>",
                            "on" if send_ua else "off",
                            e,
                        )
                except Exception as e:
                    last_exc = e
                    logger.warning(
                        "WS connect failed (proto=%s, origin=%s, ua=%s): %s",
                        proto,
                        origin or "<none>",
                        "on" if send_ua else "off",
                        e,
                    )

    assert last_exc is not None
    raise last_exc


@dataclass
class LightstreamerSession:
    """Eine aktive Lightstreamer WS Session, genau ein Subscription-Set (für MVP)."""

    websocket: Optional[Any] = None
    session_id: Optional[str] = None
    current_isin: Optional[str] = None
    current_item: Optional[str] = None
    sub_id: int = 1
    req_id: int = 1

    # Cache pro itemIndex: letzter decoded state
    item_state: Dict[int, Dict[str, Optional[str]]] = field(default_factory=dict)

    async def connect(self) -> None:
        logger.info("Connecting to Lightstreamer WS...")
        self.websocket = await _ws_connect()

        # create_session (zweizeilig)
        create_params = (
            f"LS_adapter_set={quote(LS_ADAPTER_SET)}"
            f"&LS_user="
            f"&LS_cid={quote(LS_CID)}"
            f"&LS_send_sync=false"
            f"&LS_cause=api"
            f"&LS_password="
        )
        msg = "create_session\n" + create_params + "\n"
        await self.websocket.send(msg)
        logger.info("Sent create_session")

        # Warte auf CONOK und Session ID
        while True:
            raw = await self._recv_text()
            for line in self._split_lines(raw):
                if line.startswith("CONOK,"):
                    # Format: CONOK,<sessionId>,...
                    parts = line.split(",")
                    if len(parts) >= 2:
                        self.session_id = parts[1].strip()
                        logger.info("Lightstreamer session established: %s", self.session_id)
                        return

    async def subscribe_isin(self, isin: str) -> None:
        if not self.websocket or not self.session_id:
            raise RuntimeError("Session not connected")

        self.current_isin = isin.strip().upper()
        self.current_item = isin_to_item(self.current_isin)

        # Subscription control (add)
        # Schema muss URL-encoded sein (Spaces -> %20)
        schema = quote(" ".join(SCHEMA_FIELDS))
        group = quote(self.current_item)

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

        msg = "control\n" + params + "\n"
        await self.websocket.send(msg)
        logger.info("Subscribed to %s (item=%s)", self.current_isin, self.current_item)

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
        """
        DevTools zeigt manchmal mehrere Messages in einer Zeile (z.B. "U,... U,...").
        Wir splitten konservativ:
        - zuerst nach Zeilenumbrüchen
        - dann innerhalb jeder Zeile nach " U," (space+U,)
        """
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

    def handle_line(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Parst U-Updates. Gibt ein Quote-Dict zurück oder None.
        """
        # Keepalive
        if line == "PROBE":
            return None

        # U,<subId>,<itemIndex>,<values>
        if line.startswith("U,"):
            # Split nur die ersten 3 Kommas, Rest ist values
            # Beispiel:
            # U,1,1,DE000...|1.1800|10000|0.0000|0|...
            m = re.match(r"^U,(\d+),(\d+),(.*)$", line)
            if not m:
                return None

            sub_id = int(m.group(1))
            item_index = int(m.group(2))
            values_str = m.group(3)

            # Values sind '|' getrennt
            tokens = values_str.split("|")

            prev = self.item_state.get(item_index, {f: None for f in SCHEMA_FIELDS})
            decoded = decode_field_values(tokens, SCHEMA_FIELDS, prev)
            self.item_state[item_index] = decoded

            # In decoded["symbol"] steht meistens die ISIN
            payload = {
                "type": "quote",
                "sub_id": sub_id,
                "item_index": item_index,
                "isin": decoded.get("symbol") or self.current_isin,
                "item": self.current_item,
                "raw": decoded,
                "parsed": {k: _try_num(v) for k, v in decoded.items()},
            }
            return payload

        return None


# ----------------- FastAPI App -----------------

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

clients: Set[WebSocket] = set()
ls_lock = asyncio.Lock()
ls_session: Optional[LightstreamerSession] = None
ls_task: Optional[asyncio.Task] = None


async def broadcast(obj: Dict[str, Any]) -> None:
    dead: List[WebSocket] = []
    msg = json.dumps(obj, ensure_ascii=False)

    for ws in list(clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)

    for ws in dead:
        clients.discard(ws)


async def run_ls_stream(isin: str) -> None:
    """
    Startet eine LS-Verbindung + Subscription und broadcastet Updates.
    Läuft, bis Task gecancelt wird.
    """
    global ls_session
    sess = LightstreamerSession()

    try:
        await broadcast({"type": "status", "level": "info", "message": "Verbinde zu BNP Push..."})
        await sess.connect()
        await broadcast({"type": "status", "level": "info", "message": "Session OK, subscribe..."})
        await sess.subscribe_isin(isin)
        await broadcast({"type": "status", "level": "success", "message": f"Subscribed: {isin}"})

        ls_session = sess

        while True:
            raw = await sess._recv_text()
            for line in sess._split_lines(raw):
                evt = sess.handle_line(line)
                if evt:
                    await broadcast(evt)

    except asyncio.CancelledError:
        await broadcast({"type": "status", "level": "warn", "message": "Stream gestoppt."})
        raise
    except Exception as e:
        logger.exception("LS stream error")
        await broadcast({"type": "status", "level": "error", "message": f"Fehler im Stream: {e}"})
    finally:
        try:
            await sess.close()
        except Exception:
            pass


async def start_stream(isin: str) -> None:
    """
    Stoppt ggf. laufenden Stream und startet neu mit anderer ISIN.
    """
    global ls_task, ls_session
    async with ls_lock:
        if ls_task and not ls_task.done():
            ls_task.cancel()
            try:
                await ls_task
            except Exception:
                pass

        ls_session = None
        ls_task = asyncio.create_task(run_ls_stream(isin))


@app.get("/")
async def index():
    return FileResponse("app/static/index.html")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)

    # Optional: beim Connect Default-Status senden
    await ws.send_text(json.dumps({
        "type": "status",
        "level": "info",
        "message": "Verbunden. Bitte ISIN eingeben und Subscribe klicken."
    }, ensure_ascii=False))

    try:
        while True:
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
            except Exception:
                await ws.send_text(json.dumps({"type": "status", "level": "error", "message": "Ungültiges JSON."}, ensure_ascii=False))
                continue

            if msg.get("type") == "subscribe":
                isin = (msg.get("isin") or "").strip().upper()
                try:
                    # Validierung (wirft ValueError falls falsch)
                    _ = isin_to_item(isin)
                except Exception as e:
                    await ws.send_text(json.dumps({"type": "status", "level": "error", "message": f"ISIN ungültig: {e}"}, ensure_ascii=False))
                    continue

                await start_stream(isin)

    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
