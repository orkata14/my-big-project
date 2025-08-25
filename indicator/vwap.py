import pandas as pd
import numpy as np

def add_vwap_daily(df: pd.DataFrame, col_name: str = "vwap", tz: str = "UTC") -> pd.DataFrame:
    """
    מחשב VWAP יומי: לכל יום קלנדרי באזור זמן tz נעשה cumsum מחדש.
    מתאים לקריפטו: יום = 00:00 לפי tz (ברירת מחדל UTC).
    הנחות: df מכיל עמודות: high, low, close, volume וגם start_iso או start_ms.
    """
    if df.empty:
        return df

    # זיהוי ציר הזמן (מעדיף start_iso; אם אין – משתמש ב-start_ms)
    if "start_iso" in df.columns:
        t = pd.to_datetime(df["start_iso"], utc=True).dt.tz_convert(tz)
    elif "start_ms" in df.columns:
        t = pd.to_datetime(df["start_ms"], unit="ms", utc=True).dt.tz_convert(tz)
    else:
        raise ValueError("נדרש start_iso או start_ms לחישוב VWAP יומי")

    # מפתח סשן יומי (מתאפס כל חצות ב-tz)
    session_key = t.dt.date

    # מחיר טיפוסי לנר
    tp = (pd.to_numeric(df["high"], errors="coerce")
        + pd.to_numeric(df["low"], errors="coerce")
        + pd.to_numeric(df["close"], errors="coerce")) / 3.0

    vol = pd.to_numeric(df["volume"], errors="coerce")

    # סכומים מצטברים לכל יום בנפרד
    cum_vol   = vol.groupby(session_key).cumsum()
    cum_tpvol = (tp * vol).groupby(session_key).cumsum()

    # VWAP יומי
    vwap_daily = cum_tpvol / cum_vol.replace(0, np.nan)

    # כתיבה לעמודה
    df[col_name] = vwap_daily
    return df