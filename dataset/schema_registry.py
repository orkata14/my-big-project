import pandas as pd
import numpy as np

DT  = "datetime64[ns, UTC]"
F64 = "float64"
I64 = "Int64"
BOL = "boolean"
STR = "string"

# מינימום עמודות יציבות; הוסף בהמשך אם צריך
SCHEMA = {
    # מטא
    "ts": DT, "symbol": STR, "interval": STR,
    # נר בסיס
    "open": F64, "high": F64, "low": F64, "close": F64, "volume": F64,
    # אינדיקטורים בסיס
    "ema_5": F64, "ema_12": F64, "ema_21": F64,
    "rsi": F64,
    "bb_mid": F64, "bb_up": F64, "bb_low": F64, "bb_width": F64,
    "vwap": F64,
    # דלתא/טרייד היסטורי (דוגמאות שכיחות)
    "th_buy_vol_total": F64, "th_sell_vol_total": F64, "th_total_vol": F64,
    "vd_buy_vol": F64, "vd_sell_vol": F64, "vd_total_vol": F64,
    # OB בסיסי
    "best_bid_price": F64, "best_ask_price": F64, "mid_price": F64, "spread_abs": F64,
    # Targets (נוסיף לפי אופק בריצה)
}

def ensure_target_cols(schema: dict, horizons: list[int]) -> dict:
    sc = dict(schema)
    for h in sorted(set(int(x) for x in horizons)):
        sc[f"close_t+{h}s"] = F64
        sc[f"dpp_{h}s"] = F64
        sc[f"long_profitable_{h}s"]  = BOL
        sc[f"short_profitable_{h}s"] = BOL
        sc[f"filled_at_{h}s"] = DT
    return sc

def empty_df(schema: dict) -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=t) for c, t in schema.items()})

def _coerce_dt_utc(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors="coerce")

def coerce_row(row: dict, schema: dict) -> pd.DataFrame:
    df = pd.DataFrame([row])
    # הוספת עמודות חסרות
    for c, t in schema.items():
        if c not in df.columns:
            if t == DT:
                df[c] = pd.Series([pd.NaT], dtype=DT)
            elif t.startswith("Int"):
                df[c] = pd.Series([pd.NA], dtype=t)
            elif t == BOL:
                df[c] = pd.Series([pd.NA], dtype=BOL)
            elif t == STR:
                df[c] = pd.Series([None], dtype=STR)
            else:
                df[c] = np.nan
    # המרות זמן ל־UTC
    for c, t in schema.items():
        if t == DT:
            df[c] = _coerce_dt_utc(df[c])
    # סדר ו־dtype
    df = df.reindex(columns=list(schema.keys()))
    with pd.option_context("future.no_silent_downcasting", True):
        try:
            df = df.astype(schema, errors="ignore")
        except Exception:
            pass
    return df

def append_row(df_all: pd.DataFrame, row: dict, schema: dict) -> pd.DataFrame:
    incoming = coerce_row(row, schema).dropna(axis=1, how="all")
    return pd.concat([df_all, incoming], ignore_index=True)