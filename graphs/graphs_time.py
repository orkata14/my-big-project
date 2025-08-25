import asyncio
from datetime import datetime, timezone

def _floor_to_bucket_ms(ts_ms: int, interval_sec: int) -> int:
    """מעגל timestamp להתחלת אינטרוול (bucket) בגודל נתון בשניות."""
    step = interval_sec * 1000
    return (ts_ms // step) * step

async def build_candles_from_stream(
    trades_q: asyncio.Queue,
    candles_q: asyncio.Queue,
    *,
    interval_sec: int = 30
) -> None:
    """
    קורא מטריידים בפורמט:
      {"ts_ms": int, "price": float, "qty": float, "side": "Buy"|"Sell"}
    ופולט נרות ל-candles_q עם:
      {"start_ms": int, "start_iso": str, "open": float, "high": float, "low": float, "close": float, "volume": float}
    """
    current_bucket = None
    o = h = l = c = None
    vol = 0.0

    while True:
        tr = await trades_q.get()
        ts = tr["ts_ms"]
        price = tr["price"]
        qty = tr["qty"]

        bucket = _floor_to_bucket_ms(ts, interval_sec)

        # טרייד ראשון – פתיחת נר ראשון
        if current_bucket is None:
            current_bucket = bucket
            o = h = l = c = price
            vol = qty
            continue

        # אותו דלי – עדכון הנר
        if bucket == current_bucket:
            c = price
            if price > h:
                h = price
            if price < l:
                l = price
            vol += qty
            continue

        # דלי חדש – סוגרים את הנר הקודם ושולחים לפלט
        await candles_q.put({
            "start_ms": current_bucket,
            "start_iso": datetime.fromtimestamp(current_bucket/1000, tz=timezone.utc).isoformat(),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": vol,
        })

        # נר חדש
        current_bucket = bucket
        o = h = l = c = price
        vol = qty
