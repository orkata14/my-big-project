from __future__ import annotations
import numpy as np
import pandas as pd
from indicator.bb import add_bollinger


def bb_update(df: pd.DataFrame,
              *,
              mode: str = "stream",          # "stream" (נר אחרון) או "batch" (לכל הטבלה)
              window: int = 20,
              num_std: float = 2.0,
              close_col: str = "close",
              mid_col: str = "bb_mid",
              up_col: str = "bb_up",
              low_col: str = "bb_low",
              width_col: str = "bb_width",
              status_col: str = "bb_status",
              score_col: str = "bb_score",
              eps: float = 1e-9,              # מינימום למכנה (נגד חלוקה באפס)
              tol: float = 1e-12              # "כמעט שווה" למרכז
              ) -> pd.DataFrame:
    """
    פלט זהה בשני המצבים:
      - עמודות בסיס: bb_mid, bb_up, bb_low, bb_width  (מהאינדיקטור שלך)
      - עמודות טכניקל: bb_status ∈ {BREAK_UP, BREAK_DOWN, CENTER, UPPER_HALF, LOWER_HALF}
                        bb_score  = |close - bb_mid| / half_side * 100
                        (half_side נבחר לפי צד המחיר: מעל → up-mid, מתחת → mid-low)

    mode="batch": מריץ add_bollinger על כל ה-DF ומחשב טכניקל וקטורי לכל השורות.
    mode="stream": מחשב בסיס רק לנר האחרון דרך tail(window) ואז טכניקל רק לנר האחרון.
    """
    if df.empty:
        return df
    if mode not in ("stream", "batch"):
        raise ValueError("mode must be 'stream' or 'batch'")

    # ---------- שלב A: בסיס BB דרך האינדיקטור הקיים ----------
    if mode == "batch":
        add_bollinger(df, window=window, num_std=num_std,
                      mid_col=mid_col, up_col=up_col, low_col=low_col, width_col=width_col)
    else:
        # STREAM: בסיס רק לנר האחרון ע"י ריצה על חלון tail(window)
        i = df.index[-1]
        if len(df) < window:
            # אין מספיק היסטוריה – נציב NaN ונחזיר CENTER/NaN בהמשך
            df.loc[i, [mid_col, up_col, low_col, width_col]] = [np.nan, np.nan, np.nan, np.nan]
        else:
            tmp = df[[close_col]].iloc[-window:].copy()
            tmp = add_bollinger(tmp, window=window, num_std=num_std,
                                mid_col=mid_col, up_col=up_col, low_col=low_col, width_col=width_col)
            last = tmp.iloc[-1][[mid_col, up_col, low_col, width_col]].values
            df.loc[i, [mid_col, up_col, low_col, width_col]] = last

    # ---------- שלב B: טכניקל (status + score) ----------
    if mode == "batch":
        # וקטורי לכל השורות
        half_up  = (df[up_col]  - df[mid_col]).abs().clip(lower=eps)
        half_low = (df[mid_col] - df[low_col]).abs().clip(lower=eps)
        denom    = half_up.where(df[close_col] >= df[mid_col], half_low)
        dist     = (df[close_col] - df[mid_col]).abs()
        df[score_col] = (dist / denom) * 100.0

        conds = [
            df[close_col] > df[up_col],
            df[close_col] < df[low_col],
            (df[close_col] - df[mid_col]).abs() <= tol,
            df[close_col] > df[mid_col],
        ]
        choices = ["BREAK_UP", "BREAK_DOWN", "CENTER", "UPPER_HALF"]
        df[status_col] = np.select(conds, choices, default="LOWER_HALF")

        # הגנות
        bad = df[[close_col, mid_col, up_col, low_col]].isna().any(axis=1)
        df.loc[bad, status_col] = "CENTER"
        df.loc[bad, score_col]  = np.nan

    else:
        # נר אחרון בלבד
        i = df.index[-1]
        mid = df.at[i, mid_col]
        up  = df.at[i, up_col]
        low = df.at[i, low_col]
        c   = df.at[i, close_col]

        if any(pd.isna(x) for x in (mid, up, low, c)):
            df.loc[i, status_col] = "CENTER"
            df.loc[i, score_col]  = np.nan
            return df

        half_up  = max(abs(up  - mid), eps)
        half_low = max(abs(mid - low), eps)
        denom    = half_up if c >= mid else half_low
        df.loc[i, score_col] = abs(c - mid) / denom * 100.0

        if   c > up:               status = "BREAK_UP"
        elif c < low:              status = "BREAK_DOWN"
        elif abs(c - mid) <= tol:  status = "CENTER"
        elif c > mid:              status = "UPPER_HALF"
        else:                      status = "LOWER_HALF"
        df.loc[i, status_col] = status

    return df