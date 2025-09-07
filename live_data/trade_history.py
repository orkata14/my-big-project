#live_data.trade_history.py  (גרסה משודרגת עם ניקיון ב-ingest)-טרייד היסטורי הזה מנקה -המטרה שלו היא למשוך מWEBSOCKET ואז הוא עובר ניקיון של הנתונים ואז מעביר את זה לBUFFER
import asyncio
import json
import math
import websockets

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

# ---------- פונקציות ניקיון/נירמול (INGEST CLEAN) ----------
def normalize_ws_trade(raw: dict) -> dict:
    """
    ממפה הודעת WS של Bybit לשדות אחידים:
    ts_ms:int, price:float, qty:float, side:'buy'|'sell', trade_id_raw:str|None
    (אין תלות ב-pandas כאן; שמים דגש על מהיר וקל.)
    """
    # לפי Bybit v5: T=millis, p=price, v=qty, S='Buy'/'Sell', i=tradeId (לא תמיד קיים?)
    ts_ms = int(raw.get("T") or raw.get("ts") or 0)
    side  = str(raw.get("S") or raw.get("side") or "").strip().lower()  # 'buy'/'sell'
    # כינויי שדות לגמישות:
    price = raw.get("p", raw.get("price"))
    qty   = raw.get("v", raw.get("qty"))

    # המרות בטוחות:
    try:
        price = float(price)
    except (TypeError, ValueError):
        price = math.nan

    try:
        qty = float(qty)
    except (TypeError, ValueError):
        qty = math.nan

    trade_id_raw = raw.get("i") or raw.get("tradeId") or raw.get("id")
    trade_id_raw = str(trade_id_raw) if trade_id_raw is not None else None

    return {
        "ts_ms": ts_ms,
        "price": price,
        "qty": qty,
        "side": side,              # 'buy' או 'sell'
        "trade_id_raw": trade_id_raw,
    }

def is_valid_trade(t: dict) -> bool:
    """
    סינון בסיסי כדי לא להכניס זיהום למערכת:
    - חותמת זמן > 0
    - side חוקי
    - price/qty חיוביים ולא NaN
    """
    if not isinstance(t.get("ts_ms"), int) or t["ts_ms"] <= 0:
        return False
    if t.get("side") not in ("buy", "sell"):
        return False
    price = t.get("price")
    qty   = t.get("qty")
    if price is None or qty is None:
        return False
    if math.isnan(price) or math.isnan(qty):
        return False
    if price <= 0.0 or qty <= 0.0:
        return False
    return True

def build_stable_trade_id(t: dict) -> str:
    """
    בונה מזהה יציב. אם הבורסה סיפקה i (tradeId) – נשתמש בו.
    אחרת נבנה מזהה דטרמיניסטי מהשדות (ts_ms, price, qty, side).
    """
    if t.get("trade_id_raw"):
        return str(t["trade_id_raw"])
    return f"{t['ts_ms']}-{t['price']}-{t['qty']}-{t['side']}"

# ---------- הזרם הראשי (WS → out_q) ----------
async def stream_trades(symbol: str, out_q: asyncio.Queue, *, reconnect_delay: float = 0.5) -> None:
    """
    מאזין ל-WebSocket של Bybit ומעביר כל טרייד נקי ל-out_q (Backpressure בעזרת await put).
    מבנה פריט טרייד שיוצא מה-ingest:
    {
      "symbol": str,
      "ts_ms": int,             # מילישניות UTC
      "price": float,
      "qty": float,
      "side": "buy"|"sell",
      "trade_id": str           # יציב (מהבורסה או סינתטי)
    }
    שים לב: De-dup בפועל נעשה בשכבת TradeBuffer.ingest() ולא כאן.
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

                    # נסה לפרש JSON
                    try:
                        payload = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    # ב-Bybit v5 יכולים להגיע גם הודעות שאינן data (subscribed/heartbeat) – מדלגים
                    data = payload.get("data")
                    if not data:
                        continue

                    # עיבוד כל ה-trades שהגיעו במקבץ
                    for raw_tr in data:
                        t = normalize_ws_trade(raw_tr)
                        if not is_valid_trade(t):
                            continue  # זבל החוצה: לא נכנס למערכת

                        # יצירת מזהה יציב:
                        t_clean = {
                            "symbol": symbol,
                            "ts_ms": t["ts_ms"],
                            "price": t["price"],
                            "qty": t["qty"],
                            "side": t["side"],
                            "trade_id": build_stable_trade_id(t),
                        }

                        # אין איבוד: ממתינים עד שיש מקום בתור (backpressure)
                        await out_q.put(t_clean)

        except Exception:
            # רה-קונקט בלבד; אין sleep בלייב הרגיל — רק כאן כדי לא להציף חיבורים בשגיאה.
            if reconnect_delay > 0:
                await asyncio.sleep(reconnect_delay)
            continue