import pandas as pd
from indicator.run_indikators import add_all_indicators
from technical_analysis.run_technical import add_all_technical
# אם תרצה – גם מה technical_live:
# from technical_live.orderbook_technical import ob_update
# from technical_live.trade_history_technical import th_update

# --- נבנה DataFrame דמה קטן (2–3 נרות) ---
data = [
    {"ts": pd.Timestamp("2024-01-01 00:00:00"), "open": 100, "high": 105, "low": 95, "close": 102, "volume": 1000},
    {"ts": pd.Timestamp("2024-01-01 00:01:00"), "open": 102, "high": 108, "low": 101, "close": 107, "volume": 1500},
    {"ts": pd.Timestamp("2024-01-01 00:02:00"), "open": 107, "high": 110, "low": 106, "close": 109, "volume": 1200},
]
df = pd.DataFrame(data)

# --- מריצים אינדיקטורים ---
df = add_all_indicators(df)
print(">>> Columns after add_all_indicators:")
print(df.columns.tolist())

# --- מריצים טכניים ---
df = add_all_technical(df)
print(">>> Columns after add_all_technical:")
print(df.columns.tolist())
