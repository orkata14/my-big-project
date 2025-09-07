import os
import pandas as pd

def compute_candle(df: pd.DataFrame) -> dict:
    """
    מצפה ל-columns: ['price','size'] לפחות. אם df ריק – מחזיר ערכים ריקים.
    """
    if df.empty:
        return {"open": float("nan"), "high": float("nan"), "low": float("nan"),
                "close": float("nan"), "volume": 0.0, "trades": 0}
    o = float(df["price"].iloc[0])
    h = float(df["price"].max())
    l = float(df["price"].min())
    c = float(df["price"].iloc[-1])
    v = float(df["size"].sum()) if "size" in df.columns else 0.0
    return {"open": o, "high": h, "low": l, "close": c, "volume": v, "trades": int(len(df))}

def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def append_candle_csv(csv_path: str, t0: pd.Timestamp, symbol: str, candle: dict) -> None:
    _ensure_dir(csv_path)
    row = {"time": pd.Timestamp(t0), "symbol": symbol, **candle}
    header = not os.path.exists(csv_path)
    pd.DataFrame([row]).to_csv(csv_path, mode="a", index=False, header=header)