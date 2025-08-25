# technical_analysis/candle_technical.py
from __future__ import annotations
import numpy as np
import pandas as pd


def candle_technical_update(
    df: pd.DataFrame,
    *,
    mode: str = "stream",                 # "stream" = רק הנר האחרון, "batch" = כל הטבלה
    open_col: str = "open",
    high_col: str = "high",
    low_col: str  = "low",
    close_col: str= "close",

    # ---- פרמטרים לתבניות ----
    # DOJI: גוף קטן יחסית לטווח הנר (ברירת מחדל ≤10%)
    doji_body_frac: float = 0.10,
    # כדי לא לזהות DOJI כשכל הטווח ממש זעיר (כמו “רעש”)
    doji_min_range_pct: float = 0.002,    # 0.2% מן המחיר האמצעי

    # HAMMER / SHOOTING STAR
    hammer_shadow_ratio: float = 2.0,     # lower shadow ≥ 2× body
    hammer_upper_to_body: float = 0.2,    # upper shadow ≤ 0.2× body
    star_shadow_ratio: float = 2.0,       # upper shadow ≥ 2× body
    star_lower_to_body: float = 0.2,      # lower shadow ≤ 0.2× body

    # ENGULFING (מקלה) – סבילות קטנה לחפיפה לא מושלמת
    engulf_lenient_tol: float = 0.002,    # ≈0.2%
    min_body_pct_for_engulf: float = 0.05 # גוף ≥5% מן הטווח כדי לא לתפוס רעש
) -> pd.DataFrame:
    """
    מוסיף/מעדכן עמודה אחת: candle_pattern ∈ {
        "SHOOTING_STAR", "HAMMER",
        "BEARISH_ENGULFING", "BULLISH_ENGULFING",
        "DOJI", "RED", "GREEN"
    }
    סדר העדיפויות (גבוה → נמוך):
        1) SHOOTING_STAR
        2) HAMMER
        3) BEARISH_ENGULFING
        4) BULLISH_ENGULFING
        5) DOJI
        6) RED
        7) GREEN
    """
    if df.empty:
        return df
    if mode not in ("stream", "batch"):
        raise ValueError("mode must be 'stream' or 'batch'")

    # טווח העבודה
    idx = df.index[-1:] if mode == "stream" else df.index

    # עמודות מחיר
    o = df.loc[idx, open_col].astype(float)
    h = df.loc[idx, high_col].astype(float)
    l = df.loc[idx, low_col ].astype(float)
    c = df.loc[idx, close_col].astype(float)

    # גדלים בסיסיים לנר
    body = (c - o).abs()
    rng  = (h - l).astype(float)
    rng = rng.where(rng != 0, np.nan)     # להימנע מחלוקה באפס
    upper_shadow = h - pd.concat([o, c], axis=1).max(axis=1)
    lower_shadow = pd.concat([o, c], axis=1).min(axis=1) - l
    mid_price = (h + l) / 2.0

    # בסיס: ירוק/אדום/דו'גי (ללא NONE)
    is_green = c > o
    is_red   = c < o
    is_equal = ~(is_green | is_red)       # close == open

    # DOJI: גוף קטן יחסית לטווח + הטווח לא מיקרוסקופי
    doji_by_body = (body <= doji_body_frac * rng)
    doji_by_range = ((rng / mid_price.replace(0, np.nan)).abs() >= doji_min_range_pct)
    is_doji = (doji_by_body & doji_by_range) | is_equal

    # HAMMER: lower גדול, upper קטן
    is_hammer = (
        (lower_shadow >= hammer_shadow_ratio * body) &
        (upper_shadow <= hammer_upper_to_body * body)
    )

    # SHOOTING STAR: upper גדול, lower קטן
    is_star = (
        (upper_shadow >= star_shadow_ratio * body) &
        (lower_shadow <= star_lower_to_body * body)
    )

    # ---- Engulfing (מקלה) צריך גם נר קודם ----
    prev_o = df[open_col].shift(1).astype(float)
    prev_c = df[close_col].shift(1).astype(float)
    prev_h = df[high_col].shift(1).astype(float)
    prev_l = df[low_col ].shift(1).astype(float)

    # גדלי גוף יחסיים כדי לסנן “רעש”
    prev_rng  = (prev_h - prev_l).replace(0, np.nan)
    prev_body = (prev_c - prev_o).abs()
    body_pct_of_range      = (body / rng).replace([np.inf, -np.inf], np.nan)
    prev_body_pct_of_range = (prev_body / prev_rng).replace([np.inf, -np.inf], np.nan)

    tol = engulf_lenient_tol
    # *שימי לב*: נבחן את תנאי הבליעה על כל ה-DF (כי צריך prev), ואז נשאב רק ל-idx
    cur_is_bull = (df[close_col] > df[open_col])
    cur_is_bear = (df[close_col] < df[open_col])
    prev_is_bull = (prev_c > prev_o)
    prev_is_bear = (prev_c < prev_o)

    # בליעה שורית (מקלה): קודם דובי, עכשיו שורי, והגוף הנוכחי "כמעט בולע" את גוף הקודם
    engulf_bull = (
        prev_is_bear & cur_is_bull &
        (df[close_col] >= prev_o * (1 - tol)) &
        (df[open_col]  <= prev_c * (1 + tol)) &
        ((body / (df[high_col] - df[low_col]).replace(0, np.nan)) >= min_body_pct_for_engulf)
    )

    # בליעה דובית (מקלה): קודם שורי, עכשיו דובי, והגוף הנוכחי "כמעט בולע" את גוף הקודם
    engulf_bear = (
        prev_is_bull & cur_is_bear &
        (df[close_col] <= prev_o * (1 + tol)) &
        (df[open_col]  >= prev_c * (1 - tol)) &
        ((body / (df[high_col] - df[low_col]).replace(0, np.nan)) >= min_body_pct_for_engulf)
    )

    # מצמצמים לאינדקס העבודה בלבד
    engulf_bull = engulf_bull.reindex(idx, fill_value=False)
    engulf_bear = engulf_bear.reindex(idx, fill_value=False)

    # ===== יצירת תווית אחת לפי סדר עדיפויות (אין NONE) =====
    pattern = pd.Series(index=idx, dtype=object)

    # ברירת מחדל תחילה: RED/GREEN/DOJI (כדי שתמיד תהיה תוצאה)
    pattern[is_red]   = "RED"
    pattern[is_green] = "GREEN"
    pattern[is_doji]  = "DOJI"

    # עכשיו דורסים לפי עדיפויות גבוהות יותר:
    # 1) SHOOTING_STAR
    pattern[is_star] = "SHOOTING_STAR"
    # 2) HAMMER
    pattern[is_hammer] = "HAMMER"
    # 3) BEARISH_ENGULFING
    pattern[engulf_bear] = "BEARISH_ENGULFING"
    # 4) BULLISH_ENGULFING
    pattern[engulf_bull] = "BULLISH_ENGULFING"

    # כתיבה ל-DF
    df.loc[idx, "candle_pattern"] = pattern.values

    return df
