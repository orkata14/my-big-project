from indicator.ema import add_ema
from indicator.rsi import add_rsi
from indicator.bb import add_bollinger
from indicator.vwap import add_vwap_daily  # ← ה־VWAP היומי החדש

def add_all_indicators(df):
    df = add_ema(df, 5)
    df = add_ema(df, 12)
    df = add_ema(df, 21)
    df = add_rsi(df, 14)
    df = add_bollinger(df)
    df = add_vwap_daily(df, col_name="vwap", tz="UTC")  # ← VWAP מתאפס כל יום
    return df

