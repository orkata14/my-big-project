# dataset/target_filler.py
from __future__ import annotations
from typing import List, Callable, Dict
import pandas as pd
import numpy as np

class TargetFiller:
    """
    ממלא שדות עתיד לכל שורה כשמגיע הזמן:
      • close_t+{h}s
      • dpp_{h}s = (close_t+h - close_t)/close_t * 100
      • long_profitable_{h}s / short_profitable_{h}s (אחרי עלויות)
      • filled_at_{h}s (זמן מילוי בפועל לשקיפות)
    הערות:
      • אין שימוש במידע עתידי לפני הזמן (לא Data Leakage).
      • price_lookup(ts_target) מחזירה close עבור ts_target (או None אם עוד אין).
    """

    def __init__(self,
                 horizons_sec: List[int],
                 commission_bps: float = 5.0,   # 0.05%
                 slippage_bps: float = 2.0):    # 0.02%
        self.h = sorted(set(int(x) for x in horizons_sec))
        self.friction_pct = (float(commission_bps) + float(slippage_bps)) / 100.0
        self.waiting: Dict[int, List[int]] = {h: [] for h in self.h}

    # ─────────────────────────────────────────────────────────────

    def ensure_target_columns(self, df: pd.DataFrame) -> None:
        """יוצר את כל עמודות היעד אם חסרות."""
        for h in self.h:
            for col in (
                f"close_t+{h}s", f"dpp_{h}s",
                f"long_profitable_{h}s", f"short_profitable_{h}s",
                f"filled_at_{h}s",
            ):
                if col not in df.columns:
                    df[col] = np.nan
        if "friction_pct_used" not in df.columns:
            df["friction_pct_used"] = np.nan

    def register_row(self, df: pd.DataFrame, idx: int) -> None:
        """לקרוא מיד אחרי הוספת שורה חדשה ל-DF (כדי לסמן שמחכים לעתיד)."""
        self.ensure_target_columns(df)
        for h in self.h:
            self.waiting[h].append(idx)
        # נרשום את עלות החיכוך ששימשה בעת יצירת השורה (לשקיפות/שחזור)
        df.at[idx, "friction_pct_used"] = self.friction_pct

    # ─────────────────────────────────────────────────────────────

    def on_tick(self,
                df: pd.DataFrame,
                current_ts: pd.Timestamp,
                price_lookup: Callable[[pd.Timestamp], float | None]) -> None:
        """
        לקרוא בכל סגירת נר/טיק חדש:
        מנסה למלא אחורה את כל השורות שהגיע זמנן עבור כל אופק.
        """
        if df.empty:
            return

        for h in self.h:
            still_open: List[int] = []
            for idx in self.waiting[h]:
                base_ts = df.at[idx, "ts"]
                if pd.isna(base_ts):
                    continue
                target_ts = pd.Timestamp(base_ts) + pd.Timedelta(seconds=h)

                # אם עדיין לא הגענו ל-ts העתידי – נשאיר פתוח
                if current_ts < target_ts:
                    still_open.append(idx)
                    continue

                # ננסה להביא close(target_ts)
                close_now = price_lookup(target_ts)
                if close_now is None or not np.isfinite(close_now):
                    # ייתכן שהנר העתידי נסגר מעט מאוחר/חסר – נשאיר פתוח לעוד טיק
                    still_open.append(idx)
                    continue

                # מילוי יעד
                close_base = float(df.at[idx, "close"]) if pd.notna(df.at[idx, "close"]) else np.nan
                if not np.isfinite(close_base) or close_base == 0.0:
                    # אין בסיס – לא נחשב דלתא, רק נרשום close_t+X
                    df.at[idx, f"close_t+{h}s"] = float(close_now)
                    df.at[idx, f"filled_at_{h}s"] = pd.Timestamp(current_ts)
                    continue

                dpp = (float(close_now) - close_base) / close_base * 100.0
                df.at[idx, f"close_t+{h}s"] = float(close_now)
                df.at[idx, f"dpp_{h}s"] = float(dpp)

                # רווחיות אחרי עלויות:
                thr = float(self.friction_pct)
                df.at[idx, f"long_profitable_{h}s"]  = bool(dpp >  +thr)
                df.at[idx, f"short_profitable_{h}s"] = bool(dpp <  -thr)

                # זמן מילוי בפועל (לוג/שקיפות)
                df.at[idx, f"filled_at_{h}s"] = pd.Timestamp(current_ts)

            # רק מי שעדיין לא מולא – נשאר ב-waiting
            self.waiting[h] = still_open