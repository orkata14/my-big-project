import pandas as pd
import numpy as np
def add_rsi(df: pd.DataFrame, period: int = 14, col_name: str = 'rsi') -> pd.DataFrame:
    """
    RSI לפי הגדרה סטנדרטית (Wilder).
    """
    delta = df['close'].diff()

    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    # ממוצע אקספוננציאלי בסגנון Wilder (alpha=1/period)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

    rs = avg_gain / (avg_loss.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    df[col_name] = rsi.fillna(50.0)  # התחלה ניטרלית
    return df