from typing import Dict, Any
import pandas as pd

def build_feature_row(
    ts,
    symbol: str,
    interval: str,
    candle: Dict[str, Any],
    indicators: Dict[str, Any],
    technicals: Dict[str, Any],
    orderbook: Dict[str, Any],
    trade_history: Dict[str, Any],
    volume_delta: Dict[str, Any],
) -> Dict[str, Any]:
    """
    מאחד את כל השכבות לשורה אחת אחידה של פיצ'רים.
    מקבל dict מכל מודול (כבר מחושב) ומחזיר dict לשורה בטבלה.
    שדות העתיד (Targets) מתווספים כאן כ-None ויתמלאו ב-TargetFiller.
    """

    row = {
        # מפתח שורה
        "ts": ts,
        "symbol": symbol,
        "interval": interval,

        # Candle בסיסי
        "open": candle.get("open"),
        "high": candle.get("high"),
        "low": candle.get("low"),
        "close": candle.get("close"),
        "volume": candle.get("volume"),
    }

    # אינדיקטורים (EMA/RSI/BB/VWAP)
    if indicators:
        row.update(indicators)

    # טכניים (סטטוסים: ema_status, vwap_status, bb_status, candle_pattern וכו’)
    if technicals:
        row.update(technicals)

    # OrderBook
    if orderbook:
        row.update(orderbook)

    # Trade History (th_*)
    if trade_history:
        row.update(trade_history)

    # Volume Delta (vd_*)
    if volume_delta:
        row.update(volume_delta)

    # שדות עתידיים (Targets) – יתמלאו אחר כך
    horizons = [30, 60, 90, 120]
    for h in horizons:
        row[f"close_t+{h}s"] = None
        row[f"dpp_{h}s"] = None
        row[f"long_profitable_{h}s"] = None
        row[f"short_profitable_{h}s"] = None

    return row