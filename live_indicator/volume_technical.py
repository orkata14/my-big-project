import pandas as pd
import numpy as np

# ------------------------------------------------------------
# Utils: מיפוי לסטטוס וציון
# ------------------------------------------------------------
def _vd_score_and_status(delta_ratio: pd.Series) -> pd.DataFrame:
    # קליפינג עדין למניעת קצוות חולים
    r = delta_ratio.clip(-0.98, 0.98)
    score = ((r + 1.0) / 2.0 * 100.0).round().astype(int)  # 0..100
    # סטטוס טקסטואלי
    status = np.where(r >= 0.40, "VD_STRONG_BUY",
             np.where(r >= 0.15, "VD_WEAK_BUY",
             np.where(r <= -0.40, "VD_STRONG_SELL",
             np.where(r <= -0.15, "VD_WEAK_SELL", "VD_NEUTRAL"))))
    return pd.DataFrame({"vd_score": score, "vd_status": status})

# ------------------------------------------------------------
# אינפרנס side למקרה שאין עמודה מהבורסה
# side_mode:
#   - "exchange"  → מצפה לעמודת side מוכנה ("buy"/"sell")
#   - "infer_mid" → קובע לפי trade_price לעומת mid=(best_bid+best_ask)/2
#   - "infer_tick"→ לפי שינוי מחיר לעומת העסקה הקודמת באותו נר
# ------------------------------------------------------------
def _infer_side(df: pd.DataFrame,
                side_mode: str = "exchange",
                price_col: str = "price",
                side_col: str = "side",
                bid_col: str = "best_bid",
                ask_col: str = "best_ask",
                candle_col: str = "time") -> pd.Series:
    if side_mode == "exchange" and side_col in df.columns:
        # ננקה לערכים "buy"/"sell" סטנדרטיים
        s = df[side_col].astype(str).str.lower().replace({"b":"buy","s":"sell"})
        s = np.where(s.isin(["buy","sell"]), s, np.nan)
        return pd.Series(s, index=df.index)

    if side_mode == "infer_mid" and bid_col in df.columns and ask_col in df.columns:
        mid = (df[bid_col].astype(float) + df[ask_col].astype(float)) / 2.0
        return pd.Series(np.where(df[price_col] >= mid, "buy", "sell"), index=df.index)

    # fallback: infer_tick  (לפי שינוי מחיר רציף בתוך הנר)
    # נסדר לפי נר ואז מחיר קודם
    df_sorted = df.sort_values([candle_col, "ts" if "ts" in df.columns else price_col]).copy()
    prev_price = df_sorted.groupby(candle_col)[price_col].shift(1)
    # אם המחיר עלה → buy, ירד → sell, שווה → נצמיד ל"sell" כדי לשמור קונסיסטנטיות
    inferred = np.where(df_sorted[price_col] > prev_price, "buy", "sell")
    # נחזיר לפי האינדקס המקורי
    s = pd.Series(inferred, index=df_sorted.index).reindex(df.index)
    return s

# ------------------------------------------------------------
# הפונקציה הראשית: מחשבת פיצ'רי VD פר-נר מטבלת עסקאות "שטוחה"
# trades_df: שורה=עסקה. עמודות חובה:
#   - time: חותמת הנר (datetime של פתיחת/שייכות הנר, לדוגמה רציף של 1m/30s)
#   - price: float
#   - size: float (ביחידות נכס; אם יש גם notional, אפשר להוסיף חישובים מקבילים בכסף)
#   - side? (buy/sell). אם חסר, side_mode יפעיל אינפרנס.
# אופציונלי: best_bid, best_ask, ts (חותמת בזמן אמיתי)
# ------------------------------------------------------------
def add_volume_delta_features(
    trades_df: pd.DataFrame,
    candle_col: str = "time",
    price_col: str = "price",
    size_col: str = "size",
    side_col: str = "side",
    side_mode: str = "exchange",      # "exchange" | "infer_mid" | "infer_tick"
    min_trade_size: float = 0.0,      # סינון עסקאות זעירות
    large_trade_mode: str = "pctl",   # "pctl" | "abs"
    large_trade_threshold: float = 90.0, # פרצנטיל (כשpctl) או סף מוחלט (כשabs)
    vol_norm_window: int = 20,        # לנרמולים אופציונליים
    epsilon: float = 1e-9
) -> pd.DataFrame:

    df = trades_df.copy()

    # שמירה על טיפוסים
    df[size_col]  = pd.to_numeric(df[size_col], errors="coerce").fillna(0.0)
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce").fillna(0.0)

    # סינון עסקאות זעירות
    if min_trade_size > 0:
        df = df[df[size_col] >= float(min_trade_size)]

    # אינפרנס side במידת הצורך
    if side_mode != "exchange" or side_col not in df.columns:
        df["_vd_side"] = _infer_side(df, side_mode, price_col, side_col, "best_bid", "best_ask", candle_col)
        side_use = "_vd_side"
    else:
        side_use = side_col

    # סימון buy/sell כמסיכה
    is_buy  = (df[side_use] == "buy")
    is_sell = (df[side_use] == "sell")

    # Large trades (קביעת סף)
    if large_trade_mode == "pctl":
        thr = np.nanpercentile(df[size_col].values, large_trade_threshold) if len(df) else np.nan
        large_mask = df[size_col] >= (thr if np.isfinite(thr) else np.inf)
    else:  # "abs"
        large_mask = df[size_col] >= float(large_trade_threshold)

    # חישובי בסיס ברמת עסקה
    df["_notional"] = df[price_col] * df[size_col]

    # אגרגציה פר-נר
    g = df.groupby(candle_col, dropna=False)

    vd = pd.DataFrame({
        "vd_buy_vol":      g.apply(lambda x: x.loc[ x.index[is_buy.reindex(x.index, fill_value=False)], size_col].sum()),
        "vd_sell_vol":     g.apply(lambda x: x.loc[ x.index[is_sell.reindex(x.index, fill_value=False)], size_col].sum()),
        "vd_buy_count":    g.apply(lambda x: int(is_buy.reindex(x.index, fill_value=False).sum())),
        "vd_sell_count":   g.apply(lambda x: int(is_sell.reindex(x.index, fill_value=False).sum())),
        "vd_total_count":  g.size(),
        "vd_notional_buy": g.apply(lambda x: (x.loc[ x.index[is_buy.reindex(x.index, fill_value=False)], "_notional"].sum())),
        "vd_notional_sell":g.apply(lambda x: (x.loc[ x.index[is_sell.reindex(x.index, fill_value=False)], "_notional"].sum())),
        "vd_avg_trade_size": g[size_col].mean(),
        "vd_vwap_trades":    g.apply(lambda x: (x[price_col]*x[size_col]).sum() / (x[size_col].sum() + 1e-12)),
        # large trades
        "vd_large_trades_count": g.apply(lambda x: int(large_mask.reindex(x.index, fill_value=False).sum())),
        "vd_large_trades_vol":   g.apply(lambda x: x.loc[x.index[large_mask.reindex(x.index, fill_value=False)], size_col].sum()),
    })

    # חישובים נגזרים
    vd["vd_total_vol"]      = vd["vd_buy_vol"] + vd["vd_sell_vol"]
    vd["vd_delta_vol"]      = vd["vd_buy_vol"] - vd["vd_sell_vol"]
    vd["vd_delta_notional"] = vd["vd_notional_buy"] - vd["vd_notional_sell"]

    vd["vd_buy_ratio"]  = vd["vd_buy_vol"]  / (vd["vd_total_vol"] + epsilon)
    vd["vd_sell_ratio"] = vd["vd_sell_vol"] / (vd["vd_total_vol"] + epsilon)
    vd["vd_delta_ratio"] = vd["vd_delta_vol"] / (vd["vd_total_vol"] + epsilon)

    vd["vd_aggressor_imbalance"] = (vd["vd_buy_count"] - vd["vd_sell_count"]) / (vd["vd_total_count"] + epsilon)

    # נרמולים אופציונליים (אפשר לכבות אם לא צריך)
    vd["vd_vol_sma"]   = vd["vd_total_vol"].rolling(vol_norm_window, min_periods=1).mean()
    vd["vd_vd_over_sma"] = vd["vd_delta_vol"] / (vd["vd_vol_sma"] + epsilon)

    # סטטוס/ציון
    tmp = _vd_score_and_status(vd["vd_delta_ratio"])
    vd = pd.concat([vd, tmp], axis=1)

    # דגלים לאיכות נתונים
    vd["vd_no_trades_flag"]      = (vd["vd_total_count"] == 0).astype(int)
    vd["vd_side_inferred_flag"]  = (1 if (side_mode != "exchange" or side_col not in trades_df.columns) else 0)

    # ערכי ברירת מחדל כשהנר ריק
    empty_mask = vd["vd_total_count"] == 0
    vd.loc[empty_mask, ["vd_score"]]  = 50
    vd.loc[empty_mask, ["vd_status"]] = "VD_NEUTRAL"
    vd.loc[empty_mask, ["vd_vwap_trades","vd_avg_trade_size"]] = 0.0

    # סידור טיפוסים
    numeric_cols = [c for c in vd.columns if c != "vd_status"]
    vd[numeric_cols] = vd[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    # אינדקס לפי חותמת הנר (אפשר לאפס ל-columns אם מעדיף)
    vd = vd.reset_index().rename(columns={candle_col: "time"})

    return vd
