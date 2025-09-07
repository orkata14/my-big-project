# technical_live/trade_history_technical.py
# פיצ'רים טכניים לנר טרייד-היסטורי (per-candle) + שמירה ל-CSV

from __future__ import annotations
import os
import math
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd


# ---------- אינפרנס side אם חסר ----------
def _infer_side(
    df: pd.DataFrame,
    side_mode: str = "exchange",          # "exchange" | "infer_mid" | "infer_tick"
    price_col: str = "price",
    side_col: str = "side",
    bid_col: str = "best_bid",
    ask_col: str = "best_ask",
    candle_col: str = "time",
) -> pd.Series:
    if side_mode == "exchange" and side_col in df.columns:
        s = df[side_col].astype(str).str.lower().replace({"b": "buy", "s": "sell"})
        s = np.where(pd.Series(s).isin(["buy", "sell"]), s, np.nan)
        return pd.Series(s, index=df.index)

    if side_mode == "infer_mid" and bid_col in df.columns and ask_col in df.columns:
        mid = (pd.to_numeric(df[bid_col], errors="coerce") + pd.to_numeric(df[ask_col], errors="coerce")) / 2.0
        return pd.Series(np.where(pd.to_numeric(df[price_col], errors="coerce") >= mid, "buy", "sell"), index=df.index)

    # fallback: infer_tick — מסדרים לפי time/ts ואז בודקים שינוי מחיר
    sort_key = "ts" if "ts" in df.columns else price_col
    df_sorted = df.sort_values([candle_col, sort_key]).copy()
    prev_price = pd.to_numeric(df_sorted[price_col], errors="coerce").shift(1)
    inferred = np.where(pd.to_numeric(df_sorted[price_col], errors="coerce") > prev_price, "buy", "sell")
    return pd.Series(inferred, index=df_sorted.index).reindex(df.index)


# ---------- עזר: בחירת TOP PCT ----------
def _top_pct_indices(n: int, pct: float) -> int:
    if n <= 0:
        return 0
    k = math.ceil(n * max(0.0, pct) / 100.0)
    return max(1, k)


# ---------- הפונקציה הראשית: שורת פיצ'רים אחת לנר ----------
def compute_trade_history_technical(
    df_chunk: pd.DataFrame,
    t0: pd.Timestamp,
    *,
    side_mode: str = "exchange",          # "exchange" | "infer_mid" | "infer_tick"
    top_pct: float = 3.0,                 # קיר באחוזים לפי גודל טריידים (top pct)
    bps_band: float = 0.001,              # 0.1% לאזורי high/low
    min_trade_size: float = 0.0,          # סינון עסקאות קטנות
    epsilon: float = 1e-9,
    partial_first_flag: int = 0,          # 1 אם זה נר ראשון חלקי (אופציונלי מה-main)
) -> pd.DataFrame:
    # הכנה
    df = df_chunk.copy()

    # טיפוסים בסיסיים
    if "price" not in df.columns:
        df["price"] = np.nan
    if "size" not in df.columns:
        df["size"] = 0.0

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["size"]  = pd.to_numeric(df["size"],  errors="coerce").fillna(0.0)

    # סינון min size
    if min_trade_size > 0:
        df = df[df["size"] >= float(min_trade_size)]

    # הוספת time לנר
    df["time"] = pd.Timestamp(t0)

    # סידור לפי ts אם קיים (כדי להגדיר OPEN/CLOSE)
    sort_key = "ts" if "ts" in df.columns else "price"
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.sort_values([sort_key]).reset_index(drop=True)

    # אם אין טריידים — מחזירים שורה ריקה עם דגלים
    if df.empty:
        row = {
            "time": pd.Timestamp(t0),

            # נפחים / מונים
            "th_buy_vol_total": 0.0, "th_sell_vol_total": 0.0, "th_total_vol": 0.0,
            "th_buy_trades_count": 0, "th_sell_trades_count": 0, "th_trades_count_total": 0,

            # מחיר/טווח
            "th_open": np.nan, "th_high": np.nan, "th_low": np.nan, "th_close": np.nan,
            "th_range": np.nan, "th_body": np.nan, "th_body_ratio": np.nan,
            "th_direction": 0, "th_close_pos_in_range": np.nan,

            # קירות (size-top-pct)
            "th_buy_wall_pct": 0.0, "th_buy_wall_avg_size": 0.0, "th_buy_wall_min_size": 0.0,
            "th_sell_wall_pct": 0.0, "th_sell_wall_avg_size": 0.0, "th_sell_wall_min_size": 0.0,

            # קצוות מחיר
            "th_buy_near_high_pct": 0.0, "th_sell_near_low_pct": 0.0,
            "th_buy_top_price_avg": np.nan, "th_sell_low_price_avg": np.nan,

            # יחסים
            "th_delta_vol": 0.0, "th_delta_ratio": 0.0, "th_count_imbalance": 0.0,

            # טמפ"ו
            "th_duration_sec": 0.0, "th_trades_per_sec": 0.0, "th_max_gap_ms": 0.0,

            # VWAP
            "th_vwap_trades": 0.0,

            # דגלים
            "th_no_trades_flag": 1,
            "th_side_inferred_flag": 0,
            "th_partial_first_flag": int(partial_first_flag),
        }
        return pd.DataFrame([row])

    # אינפרנס side אם צריך
    side_inferred = 0
    if side_mode != "exchange" or "side" not in df.columns:
        df["_th_side"] = _infer_side(df, side_mode, "price", "side", "best_bid", "best_ask", "time")
        side_col = "_th_side"
        side_inferred = 1
    else:
        df["side"] = df["side"].astype(str).str.lower()
        side_col = "side"

    # חלוקה לצדדים
    is_buy  = df[side_col].eq("buy")
    is_sell = df[side_col].eq("sell")

    buy_df  = df.loc[is_buy]
    sell_df = df.loc[is_sell]

    # נפחים / מונים
    buy_vol  = float(buy_df["size"].sum())
    sell_vol = float(sell_df["size"].sum())
    total_vol = buy_vol + sell_vol

    buy_cnt  = int(len(buy_df))
    sell_cnt = int(len(sell_df))
    total_cnt = buy_cnt + sell_cnt

    # מחיר/טווח
    th_open  = float(df["price"].iloc[0])
    th_close = float(df["price"].iloc[-1])
    th_high  = float(df["price"].max())
    th_low   = float(df["price"].min())
    th_range = th_high - th_low
    th_body  = abs(th_close - th_open)
    th_body_ratio = (th_body / (th_range + epsilon)) if th_range == th_range else np.nan  # שומר על NaN אם אין טווח
    th_direction = int(np.sign(th_close - th_open))
    th_close_pos_in_range = (th_close - th_low) / (th_range + epsilon)

    # קירות לפי גודל טריידים (TOP pct) לכל צד
    def _wall_stats(side_df: pd.DataFrame) -> tuple[float, float, float]:
        if side_df.empty:
            return 0.0, 0.0, 0.0
        k = _top_pct_indices(len(side_df), top_pct)
        top = side_df.nlargest(k, "size")
        vol_side = float(side_df["size"].sum())
        wall_sum = float(top["size"].sum())
        wall_pct = (wall_sum / (vol_side + epsilon)) * 100.0 if vol_side > 0 else 0.0
        wall_avg = float(top["size"].mean()) if len(top) else 0.0
        wall_min = float(top["size"].min()) if len(top) else 0.0
        return wall_pct, wall_avg, wall_min

    th_buy_wall_pct, th_buy_wall_avg_size, th_buy_wall_min_size = _wall_stats(buy_df)
    th_sell_wall_pct, th_sell_wall_avg_size, th_sell_wall_min_size = _wall_stats(sell_df)

    # קצוות מחיר: אזור high/low
    near_high_thr = th_high * (1.0 - float(bps_band))
    near_low_thr  = th_low  * (1.0 + float(bps_band))

    buy_near_high_vol = float(buy_df.loc[buy_df["price"] >= near_high_thr, "size"].sum()) if not buy_df.empty else 0.0
    sell_near_low_vol = float(sell_df.loc[sell_df["price"] <= near_low_thr,  "size"].sum()) if not sell_df.empty else 0.0

    th_buy_near_high_pct = (buy_near_high_vol / (buy_vol + epsilon)) * 100.0 if buy_vol > 0 else 0.0
    th_sell_near_low_pct = (sell_near_low_vol / (sell_vol + epsilon)) * 100.0 if sell_vol > 0 else 0.0

    # קצהי מחיר לפי price (top/bottom pct)
    def _extreme_price_avg(side_df: pd.DataFrame, top_by_high: bool) -> float:
        if side_df.empty:
            return np.nan
        k = _top_pct_indices(len(side_df), top_pct)
        if top_by_high:
            pick = side_df.nlargest(k, "price")
        else:
            pick = side_df.nsmallest(k, "price")
        return float(pick["price"].mean()) if len(pick) else np.nan

    th_buy_top_price_avg  = _extreme_price_avg(buy_df, top_by_high=True)
    th_sell_low_price_avg = _extreme_price_avg(sell_df, top_by_high=False)

    # יחסים
    th_delta_vol   = buy_vol - sell_vol
    th_delta_ratio = th_delta_vol / (total_vol + epsilon) if total_vol > 0 else 0.0
    th_count_imbalance = (buy_cnt - sell_cnt) / (total_cnt + epsilon) if total_cnt > 0 else 0.0

    # טמפו/קצב
    if "ts" in df.columns and df["ts"].notna().any():
        ts_sorted = df["ts"].sort_values()
        duration_sec = float((ts_sorted.iloc[-1] - ts_sorted.iloc[0]).total_seconds()) if len(ts_sorted) > 1 else 0.0
        gaps = ts_sorted.diff().dropna().dt.total_seconds() * 1000.0
        max_gap_ms = float(gaps.max()) if len(gaps) else 0.0
    else:
        duration_sec = 0.0
        max_gap_ms = 0.0
    trades_per_sec = (total_cnt / max(duration_sec, 1.0)) if total_cnt > 0 else 0.0

    # VWAP על טריידים
    notional = (df["price"] * df["size"]).sum()
    size_sum = df["size"].sum()
    th_vwap_trades = float(notional / (size_sum + epsilon)) if size_sum > 0 else 0.0

    # בניית שורה
    row: Dict[str, Any] = {
        "time": pd.Timestamp(t0),

        # נפחים/מונה
        "th_buy_vol_total": buy_vol,
        "th_sell_vol_total": sell_vol,
        "th_total_vol": total_vol,
        "th_buy_trades_count": buy_cnt,
        "th_sell_trades_count": sell_cnt,
        "th_trades_count_total": total_cnt,

        # מחיר/טווח
        "th_open": th_open, "th_high": th_high, "th_low": th_low, "th_close": th_close,
        "th_range": th_range, "th_body": th_body, "th_body_ratio": th_body_ratio,
        "th_direction": th_direction, "th_close_pos_in_range": th_close_pos_in_range,

        # קירות (size-top-pct)
        "th_buy_wall_pct": th_buy_wall_pct,
        "th_buy_wall_avg_size": th_buy_wall_avg_size,
        "th_buy_wall_min_size": th_buy_wall_min_size,
        "th_sell_wall_pct": th_sell_wall_pct,
        "th_sell_wall_avg_size": th_sell_wall_avg_size,
        "th_sell_wall_min_size": th_sell_wall_min_size,

        # קצוות מחיר
        "th_buy_near_high_pct": th_buy_near_high_pct,
        "th_sell_near_low_pct": th_sell_near_low_pct,

        # קצהי מחיר לפי price
        "th_buy_top_price_avg": th_buy_top_price_avg,
        "th_sell_low_price_avg": th_sell_low_price_avg,

        # יחסים
        "th_delta_vol": th_delta_vol,
        "th_delta_ratio": th_delta_ratio,
        "th_count_imbalance": th_count_imbalance,

        # טמפו/קצב
        "th_duration_sec": duration_sec,
        "th_trades_per_sec": trades_per_sec,
        "th_max_gap_ms": max_gap_ms,

        # VWAP
        "th_vwap_trades": th_vwap_trades,

        # דגלים
        "th_no_trades_flag": 0,
        "th_side_inferred_flag": int(side_inferred),
        "th_partial_first_flag": int(partial_first_flag),
    }

    return pd.DataFrame([row])


# ---------- Persist ----------
def append_trade_history_tech_csv(csv_path: str, row_df: pd.DataFrame, symbol: str) -> None:
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    if "symbol" not in row_df.columns:
        row_df = row_df.copy()
        row_df["symbol"] = symbol
    header = not os.path.exists(csv_path)
    row_df.to_csv(csv_path, mode="a", index=False, header=header)
