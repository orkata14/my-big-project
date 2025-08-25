# live_data/pumps.py
import asyncio
import pandas as pd
from typing import Callable, Awaitable, Optional
from .trade_buffer import TradeBuffer

def default_normalize(msg: dict) -> dict:
    """
    ממפה הודעת WS לשדות הסטנדרטיים של הבופר.
    עדכן למיפוי של הבורסה שלך (Bybit/Bitget): id/ts/price/size/side/symbol/best_bid/best_ask.
    """
    return {
        "id":      msg.get("i") or msg.get("trade_id"),
        "ts":      pd.to_datetime(msg.get("T") or msg.get("ts") or msg.get("time"), unit="ms", utc=True),
        "symbol":  msg.get("s") or msg.get("symbol"),
        "price":   msg.get("p") or msg.get("price"),
        "size":    msg.get("q") or msg.get("size"),
        "side":    (msg.get("S") or msg.get("side") or "").lower() or None,  # "buy"/"sell" אם זמין
        "best_bid": msg.get("best_bid"),
        "best_ask": msg.get("best_ask"),
    }

async def pump_trades_to_buffer(
    out_q: "asyncio.Queue[dict]",
    buffer: TradeBuffer,
    *,
    normalize: Callable[[dict], dict] = default_normalize,
    symbol_filter: Optional[str] = None,
) -> None:
    """
    קורא מהרשימה היוצאת של ה-WS (out_q), מנרמל, ודוחף ל-TradeBuffer.
    אין כאן עיבוד אינדיקטורים. אין כפילויות (buffer דואג).
    """
    while True:
        msg = await out_q.get()
        try:
            tr = normalize(msg)
            if symbol_filter and tr.get("symbol") != symbol_filter:
                continue
            buffer.append(tr)
        finally:
            out_q.task_done()
