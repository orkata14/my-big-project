from __future__ import annotations
import numpy as np
import pandas as pd


def ema_update(
    df: pd.DataFrame,
    *,
    mode: str = "stream",                 # "stream" (נר אחרון) או "batch" (כל הטבלה)
    close_col: str = "close",

    # --- EMA יחיד (מחיר מול EMA) ---
    ema_col: str | None = "ema_21",
    status_col: str = "ema_status",
    dist_col: str = "ema_dist_pct",
    score_col: str = "ema_score",
    on_tol_pct: float = 0.02,             # כמה נחשב "על ה-EMA" → ידביק score ל-100

    # --- זוג EMA (מהיר/איטי) ---
    ema_fast_col: str | None = "ema_12",
    ema_slow_col: str | None = "ema_21",
    pair_status_col: str = "ema_pair_status",
    pair_abs_col: str = "ema_pair_abs_diff",       # ← מרחק אבסולוטי (חדש)
    pair_pct_col: str = "ema_pair_spread_pct",     # אחוזים למידע בלבד (לא ניקוד)
    pair_score_col: str = "ema_pair_score",        # ← יהיה שווה למרחק האבסולוטי
    hebrew_labels: bool = False,                   # לתרגום שמות הסטטוסים
) -> pd.DataFrame:
    """
    פלט:
      (A) EMA יחיד:
          - ema_status ∈ {"EMA = CROSSOVER","EMA = CROSSUNDER","EMA < CLOSE","EMA > CLOSE"}
          - ema_dist_pct = |close - ema| / |ema| * 100
          - ema_score    = 100 - ema_dist_pct (0–100), ואם |dist|<=on_tol_pct → 100
      (B) זוג EMA:
          - ema_pair_status ∈
              * "EMA FAST = CROSSOVER"   (הקצר חצה את הארוך מלמטה)
              * "EMA FAST = CROSSUNDER"  (הקצר חצה את הארוך מלמעלה)
              * "FAST_ABOVE_SLOW"        (קצר מעל ארוך)
              * "FAST_BELOW_SLOW"        (ארוך מעל קצר)
            (אם hebrew_labels=True, הסטטוסים יתורגמו לעברית)
          - ema_pair_abs_diff   = |ema_fast - ema_slow|         ← *** זהו הניקוד ***
          - ema_pair_spread_pct = |ema_fast - ema_slow| / |ema_slow| * 100  (מידע בלבד)
          - ema_pair_score      = ema_pair_abs_diff             ← *** ניקוד = מרחק אבסולוטי ***
    הערות:
      • הפונקציה *לא* מחשבת EMA בבסיס; אם אין עמודות EMA – פשוט מדלגת על אותו חלק.
      • mode="stream" מעדכן רק את הנר האחרון; mode="batch" מחשב לכל הטבלה (וקטורי).
    """
    if df.empty:
        return df
    if mode not in ("stream", "batch"):
        raise ValueError("mode must be 'stream' or 'batch'")

    # =========================================================
    # A) EMA יחיד מול המחיר (כמו קודם)
    # =========================================================
    if ema_col and (ema_col in df.columns):
        if mode == "batch":
            c = df[close_col].astype(float)
            e = df[ema_col].astype(float)

            dist_pct = (c - e).abs() / e.replace(0, np.nan).abs() * 100.0
            score = (100.0 - dist_pct).clip(0, 100)

            prev_above = (c.shift(1) > e.shift(1))
            curr_above = (c > e)
            status = np.select(
                [
                    (~prev_above) & curr_above,     # מלמטה ללמעלה
                    ( prev_above) & (~curr_above),  # מלמעלה למטה
                    curr_above,                     # מעל
                ],
                [
                    "EMA = CROSSOVER",
                    "EMA = CROSSUNDER",
                    "EMA < CLOSE",
                ],
                default="EMA > CLOSE",
            )

            # “על ה-EMA” → score 100
            score = score.where(dist_pct > on_tol_pct, 100.0)

            df[dist_col] = dist_pct.round(2)
            df[score_col] = score.round(2)
            df[status_col] = status

            bad = ~np.isfinite(e) | ~np.isfinite(c)
            df.loc[bad, [dist_col, score_col]] = np.nan

        else:  # stream
            i = df.index[-1]
            c = float(df.at[i, close_col])
            e = float(df.at[i, ema_col]) if pd.notna(df.at[i, ema_col]) else np.nan

            if pd.notna(e) and e != 0.0:
                dist_pct = abs(c - e) / abs(e) * 100.0
                score = 100.0 if dist_pct <= on_tol_pct else max(0.0, 100.0 - dist_pct)
                score = float(np.clip(score, 0.0, 100.0))
            else:
                dist_pct = np.nan
                score = np.nan

            # סטטוס
            if len(df) >= 2 and pd.notna(e):
                j = df.index[-2]
                prev_c = float(df.at[j, close_col])
                prev_e = float(df.at[j, ema_col]) if pd.notna(df.at[j, ema_col]) else np.nan
                if pd.notna(prev_e):
                    prev_above = prev_c > prev_e
                    curr_above = c > e
                    if (not prev_above) and curr_above:
                        status = "EMA = CROSSOVER"
                    elif prev_above and (not curr_above):
                        status = "EMA = CROSSUNDER"
                    else:
                        status = "EMA < CLOSE" if curr_above else "EMA > CLOSE"
                else:
                    status = "EMA < CLOSE" if c > e else "EMA > CLOSE"
            else:
                status = "EMA < CLOSE" if (pd.notna(e) and c >= e) else "EMA > CLOSE"

            df.loc[i, dist_col] = np.round(dist_pct, 2) if pd.notna(dist_pct) else np.nan
            df.loc[i, score_col] = np.round(score, 2) if pd.notna(score) else np.nan
            df.loc[i, status_col] = status

    # =========================================================
    # B) זוג EMA (מהיר/איטי) — ניקוד = מרחק אבסולוטי ביניהם
    # =========================================================
    if (ema_fast_col and ema_slow_col and
        (ema_fast_col in df.columns) and (ema_slow_col in df.columns)):

        if mode == "batch":
            f = df[ema_fast_col].astype(float)
            s = df[ema_slow_col].astype(float)

            abs_diff   = (f - s).abs()                                 # ← מרחק אבסולוטי
            spread_pct = abs_diff / s.replace(0, np.nan).abs() * 100.0 # אחוזים (מידע)

            prev_fast_above = (f.shift(1) > s.shift(1))
            curr_fast_above = (f > s)
            pair_status = np.select(
                [
                    (~prev_fast_above) & curr_fast_above,     # הקצר חצה מלמטה ללמעלה
                    ( prev_fast_above) & (~curr_fast_above),  # הקצר חצה מלמעלה למטה
                    curr_fast_above,                          # קצר מעל ארוך
                ],
                [
                    "EMA FAST = CROSSOVER",
                    "EMA FAST = CROSSUNDER",
                    "FAST_ABOVE_SLOW",
                ],
                default="FAST_BELOW_SLOW",
            )

            if hebrew_labels:
                pair_status = _map_pair_status_to_hebrew(pair_status)

            df[pair_abs_col]  = abs_diff.round(6)            # יותר ספרות כי זה במחיר
            df[pair_pct_col]  = spread_pct.round(2)
            df[pair_score_col]= df[pair_abs_col]             # ← הניקוד הוא ההפרש האבסולוטי

            bad = ~np.isfinite(f) | ~np.isfinite(s)
            df.loc[bad, [pair_abs_col, pair_pct_col, pair_score_col]] = np.nan

        else:  # stream
            i = df.index[-1]
            f = float(df.at[i, ema_fast_col]) if pd.notna(df.at[i, ema_fast_col]) else np.nan
            s = float(df.at[i, ema_slow_col]) if pd.notna(df.at[i, ema_slow_col]) else np.nan

            if pd.notna(f) and pd.notna(s):
                abs_diff = abs(f - s)
                spread_pct = (abs_diff / abs(s) * 100.0) if (s != 0.0) else np.nan
            else:
                abs_diff = np.nan
                spread_pct = np.nan

            # סטטוס חציה/מיקום
            if len(df) >= 2 and pd.notna(f) and pd.notna(s):
                j = df.index[-2]
                pf = float(df.at[j, ema_fast_col]) if pd.notna(df.at[j, ema_fast_col]) else np.nan
                ps = float(df.at[j, ema_slow_col]) if pd.notna(df.at[j, ema_slow_col]) else np.nan
                if pd.notna(pf) and pd.notna(ps):
                    prev_fast_above = pf > ps
                    curr_fast_above = f > s
                    if (not prev_fast_above) and curr_fast_above:
                        pair_status = "EMA FAST = CROSSOVER"
                    elif prev_fast_above and (not curr_fast_above):
                        pair_status = "EMA FAST = CROSSUNDER"
                    else:
                        pair_status = "FAST_ABOVE_SLOW" if curr_fast_above else "FAST_BELOW_SLOW"
                else:
                    pair_status = "FAST_ABOVE_SLOW" if (pd.notna(f) and pd.notna(s) and f >= s) else "FAST_BELOW_SLOW"
            else:
                pair_status = "FAST_ABOVE_SLOW" if (pd.notna(f) and pd.notna(s) and f >= s) else "FAST_BELOW_SLOW"

            if hebrew_labels:
                pair_status = _map_pair_status_to_hebrew(pair_status)

            df.loc[i, pair_abs_col]   = np.round(abs_diff, 6) if pd.notna(abs_diff) else np.nan
            df.loc[i, pair_pct_col]   = np.round(spread_pct, 2) if pd.notna(spread_pct) else np.nan
            df.loc[i, pair_score_col] = df.loc[i, pair_abs_col]  # ← ניקוד = מרחק אבסולוטי
            df.loc[i, pair_status_col]= pair_status

    return df
