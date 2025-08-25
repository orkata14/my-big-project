# technical_analysis/run_technical.py
from __future__ import annotations
from typing import List, Tuple
import pandas as pd

from technical_analysis.ema_technical import ema_update
from technical_analysis.vwap_technical import vwap_update
from technical_analysis.bb_technical import bb_update
from technical_analysis.candle_technical import candle_technical_update  # ← ייבוא בלבד

def _ema_col_name(df: pd.DataFrame, period: int) -> str:
    no_underscore = f"ema{period}"
    with_underscore = f"ema_{period}"
    if no_underscore in df.columns:
        return no_underscore
    if with_underscore in df.columns:
        return with_underscore
    return no_underscore

def add_all_technical(
    df: pd.DataFrame,
    *,
    mode: str = "stream",
    # VWAP
    vwap_on_tol_pct: float = 0.02,
    # BB
    bb_window: int = 20,
    bb_num_std: float = 2.0,
    # EMA
    ema_pairs: List[Tuple[int, int]] = ((12, 21),),
    # Candles
    include_candles: bool = True,   # אפשר לכבות אם תרצה
) -> pd.DataFrame:
    if df.empty:
        return df

    # ---- VWAP TECH ----
    df = vwap_update(df, mode=mode, on_tol_pct=vwap_on_tol_pct)

    # ---- BB TECH ----
    df = bb_update(df, mode=mode, window=bb_window, num_std=bb_num_std)

    # ---- EMA TECH ----
    for fast, slow in ema_pairs:
        fast_col = _ema_col_name(df, fast)
        slow_col = _ema_col_name(df, slow)
        df = ema_update(
            df,
            mode=mode,
            ema_fast_col=fast_col,
            ema_slow_col=slow_col,
            pair_status_col=f"ema_pair_status_{fast}_{slow}",
            pair_abs_col=f"ema_pair_abs_diff_{fast}_{slow}",
            pair_pct_col=f"ema_pair_spread_pct_{fast}_{slow}",
            pair_score_col=f"ema_pair_score_{fast}_{slow}",
        )
        # יחסי למחיר מול האיטי (למשל 21)
        df = ema_update(
            df,
            mode=mode,
            ema_col=slow_col,
            status_col=f"ema_status_{slow}",
            dist_col=f"ema_dist_pct_{slow}",
            score_col=f"ema_score_{slow}",
            on_tol_pct=0.02,
        )

    # ---- CANDLE TECH ----
    if include_candles:
        df = candle_technical_update(df, mode=mode)  # ← כאן (בתוך הפונקציה), לא למעלה

    return df
