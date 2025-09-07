import asyncio, json, websockets
from typing import Dict, Any

BYBIT_WS_URL_OB = "wss://stream.bybit.com/v5/public/linear"

def _normalize_ob_msg(raw: Dict[str, Any]) -> Dict[str, Any]:
    ts_ms = int(raw.get("ts") or raw.get("T") or 0)
    bids  = raw.get("b", raw.get("bids", []))
    asks  = raw.get("a", raw.get("asks", []))
    bids = [(float(p), float(q)) for p, q in bids]
    asks = [(float(p), float(q)) for p, q in asks]
    return {"ts_ms": ts_ms, "bids": bids, "asks": asks}

async def stream_orderbook(symbol: str, out_q: asyncio.Queue, *, reconnect_delay: float = 0.5) -> None:
    topic = f"orderbook.50.{symbol}"  # התאם לנושא שאתה משתמש בו
    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL_OB, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
                while True:
                    msg = await ws.recv()
                    payload = json.loads(msg)
                    data = payload.get("data")
                    if not data:
                        continue
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        await out_q.put(_normalize_ob_msg(item))  # דוחף ל-Queue (בלי הדפסה)
        except Exception:
            if reconnect_delay > 0:
                await asyncio.sleep(reconnect_delay)
            continue