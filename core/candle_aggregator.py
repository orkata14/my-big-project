# core/candle_aggregator.py
import pandas as pd
from typing import Optional
from live_data.trade_buffer import TradeBuffer

def get_t1(t0: pd.Timestamp, candle_seconds: int) -> pd.Timestamp:
    return t0 + pd.Timedelta(seconds=candle_seconds)

def finalize_trades_chunk(
    buffer: TradeBuffer,
    t0: pd.Timestamp,
    candle_seconds: int,
    *,
    symbol: Optional[str] = None,
) -> pd.DataFrame:
    """
    מוציא צ'אנק עסקאות לנר [t0, t1) כ-DataFrame "שטוח".
    כאן אנחנו מוסיפים לכל שורה time=t0 (מפתח הנר), בלי חישוב אינדיקטורים.
    """
    t1 = get_t1(t0, candle_seconds)
    df = buffer.slice(t0, t1, symbol)
    if df.empty:
        # ניתן להחזיר ריק; פונקציית ה-VD יודעת להתמודד. נשמור תיאום time/symbol אם תרצה.
        return df
    df = df.copy()
    df["time"] = pd.Timestamp(t0)
    if symbol is not None:
        df["symbol"] = symbol
    return df
