from __future__ import annotations
import numpy as np
import pandas as pd

def vwap_update(df: pd.DataFrame,
                *,
                mode: str = "stream",
                close_col: str = "close",
                vwap_col: str = "vwap",
                status_col: str = "vwap_status",
                dist_col: str = "vwap_dist_pct",
                score_col: str = "vwap_score",
                on_tol_pct: float = 0.02) -> pd.DataFrame:
    if df.empty:
        return df
    if vwap_col not in df.columns:
        raise KeyError(f"'{vwap_col}' חסר. קודם תריץ add_all_indicators(df) שמחשב VWAP בסיס.")
    if mode not in ("stream", "batch"):
        raise ValueError("mode must be 'stream' or 'batch'")

    if mode == "batch":
        c = df[close_col].astype(float)
        v = df[vwap_col].astype(float)
        dist_pct = (c - v).abs() / v.replace(0, np.nan).abs() * 100.0
        score = (100.0 - dist_pct).clip(0, 100)
        prev_above = (c.shift(1) > v.shift(1))
        curr_above = (c > v)
        status = np.select(
            [ (~prev_above) & curr_above, (prev_above) & (~curr_above), curr_above ],
            [ "VWAP = CROSSOVER",          "VWAP = CROSSUNDER",          "VWAP < CLOSE" ],
            default="VWAP > CLOSE"
        )
        score = score.where(dist_pct > on_tol_pct, 100.0)
        df[dist_col] = dist_pct.round(2)
        df[score_col] = score.round(2)
        df[status_col] = status
        bad = ~np.isfinite(v) | ~np.isfinite(c)
        df.loc[bad, [dist_col, score_col]] = np.nan
        return df

    # mode == "stream": רק לנר האחרון
    i = df.index[-1]
    c = float(df.at[i, close_col])
    v = float(df.at[i, vwap_col]) if pd.notna(df.at[i, vwap_col]) else np.nan
    dist_pct = (abs(c - v) / abs(v) * 100.0) if (pd.notna(v) and v != 0.0) else np.nan
    score = 100.0 if (pd.notna(dist_pct) and dist_pct <= on_tol_pct) else max(0.0, 100.0 - (dist_pct if pd.notna(dist_pct) else 100.0))
    score = float(np.clip(score, 0.0, 100.0))
    if len(df) >= 2 and pd.notna(v):
        j = df.index[-2]
        prev_c = float(df.at[j, close_col])
        prev_v = float(df.at[j, vwap_col]) if pd.notna(df.at[j, vwap_col]) else np.nan
        if pd.notna(prev_v):
            prev_above = prev_c > prev_v
            curr_above = c > v
            if (not prev_above) and curr_above:
                status = "VWAP = CROSSOVER"
            elif prev_above and (not curr_above):
                status = "VWAP = CROSSUNDER"
            else:
                status = "VWAP < CLOSE" if curr_above else "VWAP > CLOSE"
        else:
            status = "VWAP < CLOSE" if c > v else "VWAP > CLOSE"
    else:
        status = "VWAP < CLOSE" if (pd.notna(v) and c >= v) else "VWAP > CLOSE"
    df.loc[i, dist_col]  = np.round(dist_pct, 2) if pd.notna(dist_pct) else np.nan
    df.loc[i, score_col] = np.round(score, 2)
    df.loc[i, status_col]= status
    return df


# B) technical_analysis/run_technical.py – לקרוא ל-vwap_update בלי לרענן בסיס
#