import asyncio
import json
import websockets

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

async def stream_trades(symbol: str, out_q: asyncio.Queue, *, reconnect_delay: float = 0.5) -> None:
    """
    מאזין ל-WebSocket של Bybit ומעביר כל טרייד ל-out_q.
    לא מאבד טריידים: משתמשים ב-await out_q.put(...) (Backpressure) – לא put_nowait.
    מבנה פריט טרייד:
    {
      "ts_ms": int,         # מילישניות
      "price": float,       # מחיר
      "qty": float,         # כמות
      "side": "Buy"|"Sell"  # צד
    }
    """
    topic = f"publicTrade.{symbol}"

    while True:  # רה-קונקט במקרה ניתוק/שגיאה
        try:
            async with websockets.connect(BYBIT_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))

                while True:
                    # timeout כדי שלא ניתקע; על timeout נשלח ping ונמשיך
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        try:
                            await ws.ping()
                            continue
                        except Exception:
                            break  # יגרום לרה-קונקט

                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    if "data" not in data:
                        continue

                    for tr in data["data"]:
                        # אין איבוד: ממתינים עד שיש מקום בתור (backpressure)
                        await out_q.put({
                            "ts_ms": tr["T"],
                            "price": float(tr["p"]),
                            "qty": float(tr["v"]),
                            "side": tr["S"],
                        })
                        

        except Exception:
            # רה-קונקט בלבד; אין sleep בלייב הרגיל — רק כאן כדי לא להציף חיבורים בשגיאה.
            if reconnect_delay > 0:
                await asyncio.sleep(reconnect_delay)
            continue
      