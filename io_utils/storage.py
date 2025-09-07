# io_utils/storage.py
# שמירה/טעינה פשוטה ונקייה לטבלת פיצ'רים+Targets (per symbol+interval)

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Iterable, Optional
import pandas as pd

# ─────────────────────────────────────────────────────────────
# נתיב שמירה סטנדרטי
def parquet_path(symbol: str, interval: str) -> Path:
    base = Path("data/processed")
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{symbol}_{interval}.parquet"


# ─────────────────────────────────────────────────────────────
# טעינה ושמירה
def load_df(symbol: str, interval: str) -> pd.DataFrame:
    p = parquet_path(symbol, interval)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def save_df(df: pd.DataFrame, symbol: str, interval: str) -> None:
    """
    שמירה אטומית: כותב לקובץ זמני ואז מחליף.
    """
    p = parquet_path(symbol, interval)
    tmp = p.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(p)  # atomic-ish move


# ─────────────────────────────────────────────────────────────
# הוספת שורה/ות
def append_row(row: Dict[str, Any], symbol: str, interval: str) -> int:
    """
    מוסיף שורה אחת לקובץ היעד. מחזיר אינדקס השורה לאחר ההוספה.
    הערה: לביצועים עדיף לצבור כמה שורות ואז להשתמש ב-append_rows.
    """
    return append_rows([row], symbol, interval)


def append_rows(rows: Iterable[Dict[str, Any]], symbol: str, interval: str) -> int:
    """
    מוסיף אוסף שורות. מחזיר מספר שורות כולל אחרי ההוספה.
    שומר על סכימה עקבית (מיישר עמודות אם צריך).
    """
    p = parquet_path(symbol, interval)
    new_df = pd.DataFrame(list(rows))

    if p.exists():
        old_df = pd.read_parquet(p)

        # יישור סכימה: מאחדים עמודות, ממלאים NaN לעמודות חסרות בשני הצדדים
        all_cols = list(dict.fromkeys(list(old_df.columns) + list(new_df.columns)))
        old_df = _align_columns(old_df, all_cols)
        new_df = _align_columns(new_df, all_cols)

        out = pd.concat([old_df, new_df], ignore_index=True)
    else:
        out = new_df

    save_df(out, symbol, interval)
    return len(out)


# ─────────────────────────────────────────────────────────────
# עזר: יישור עמודות (Schema Alignment)
def _align_columns(df: pd.DataFrame, all_cols: list[str]) -> pd.DataFrame:
    for c in all_cols:
        if c not in df.columns:
            df[c] = pd.NA
    # לשמור סדר עמודות עקבי
    return df[all_cols]


# ─────────────────────────────────────────────────────────────
# דוגמת שימוש (להסרה אם לא צריך):
#if __name__ == "__main__":
    sym, itv = "BTCUSDT", "30s"
    # 1) טוען קיים או יוצר חדש
    df = load_df(sym, itv)
    print("Loaded rows:", len(df))

    # 2) מוסיף שורה חדשה (למשל מה-Feature Builder)
    row = {
        "ts": pd.Timestamp("2025-09-06T10:00:00Z"),
        "symbol": sym,
        "interval": itv,
        "open": 100, "high": 105, "low": 99, "close": 104, "volume": 1234,
        "ema_12": 102.5, "ema_21": 101.8,
        # שדות נוספים… + Targets (יכולים להיות None/NaN, ימולאו בהמשך ע"י TargetFiller)
        "close_t+30s": None, "dpp_30s": None,
    }
    total = append_row(row, sym, itv)
    print("Total rows after append:", total)