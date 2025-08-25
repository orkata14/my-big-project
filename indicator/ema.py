import pandas as pd
import numpy as np
def add_ema(df: pd.DataFrame, span: int, col_name: str | None = None) -> pd.DataFrame:
    """
    EMA קלאסי על מחירי הסגירה.
    span = תקופה (למשל 5, 12, 21)
    """
    if col_name is None:
        col_name = f'ema_{span}'
    df[col_name] = df['close'].ewm(span=span, adjust=False).mean()
    return df