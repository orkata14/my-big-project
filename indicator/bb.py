import pandas as pd
import numpy as np


def add_bollinger(df: pd.DataFrame, window: int = 20, num_std: float = 2.0,
                  mid_col='bb_mid', up_col='bb_up', low_col='bb_low', width_col='bb_width') -> pd.DataFrame:
    """
    BB קלאסי על close:
    mid = SMA(window)
    up  = mid + num_std * std(window)
    low = mid - num_std * std(window)
    width(%) = (up - low) / mid * 100
    """
    mid = df['close'].rolling(window=window, min_periods=window).mean()
    std = df['close'].rolling(window=window, min_periods=window).std(ddof=0)
    up = mid + num_std * std
    low = mid - num_std * std

    df[mid_col] = mid
    df[up_col] = up
    df[low_col] = low
    df[width_col] = ((up - low) / mid * 100).replace([np.inf, -np.inf], np.nan)
    return df
