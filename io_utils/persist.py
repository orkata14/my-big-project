# io/persist.py
from __future__ import annotations
import os
import pandas as pd

def ensure_dirs(path: str) -> None:
    """
    יוצר את התיקיות לנתיב אם הן לא קיימות.
    למשל: data/candles.csv -> יוודא שהתיקייה data קיימת.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

def save_csv(df: pd.DataFrame, path: str, *, index: bool = True) -> None:
    """
    שמירה של DataFrame כ-CSV.
    דואג שהתיקייה קיימת לפני השמירה.
    """
    ensure_dirs(path)
    df.to_csv(path, index=index)

def load_csv(path: str) -> pd.DataFrame:
    """
    טוען CSV כ-DataFrame.
    אם הקובץ לא קיים – מחזיר DataFrame ריק (במקום להפיל את התהליך).
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, parse_dates=True, index_col=0)

def save_parquet(df: pd.DataFrame, path: str, *, index: bool = True) -> None:
    """
    שמירה בפורמט Parquet (מהיר וקל יותר מ-CSV, מתאים לדאטה גדול).
    גם כאן יוודא שהתיקייה קיימת.
    """
    ensure_dirs(path)
    df.to_parquet(path, index=index)
