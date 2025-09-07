import pandas as pd
import numpy as np
from dataset.schema_registry import append_row

def _ob_snapshot_to_features(snapshot: dict | None) -> dict:
    if not snapshot:
        return {}
    bids = snapshot.get("bids", []) or []
    asks = snapshot.get("asks", []) or []
    best_bid = max((p for p, q in bids), default=np.nan)
    best_ask = min((p for p, q in asks), default=np.nan)
    mid = (best_bid + best_ask) / 2 if (np.isfinite(best_bid) and np.isfinite(best_ask)) else np.nan
    spread = (best_ask - best_bid) if (np.isfinite(best_bid) and np.isfinite(best_ask)) else np.nan
    return {
        "best_bid_price": best_bid,
        "best_ask_price": best_ask,
        "mid_price": mid,
        "spread_abs": spread,
    }

async def on_candle_ready(*, t0, t1, df_chunk, ctx):
    """
    ctx: {
      "SYMBOL","INTERVAL","HORIZONS",
      "df_all","schema","price_lookup",
      "orderbook_buffer","filler",
      "add_all_indicators","add_all_technical",
      "build_feature_row","compute_th","compute_vd","process_ob",
      "save_df","SAVE_EVERY"
    }
    """
    SYMBOL   = ctx["SYMBOL"]
    INTERVAL = ctx["INTERVAL"]

    # 0) Ensure t0/t1 are UTC-aware
    t0 = pd.to_datetime(t0, utc=True)
    t1 = pd.to_datetime(t1, utc=True)

    # If df_chunk has a ts column, normalize it to UTC as well.
    if df_chunk is None or df_chunk.empty:
        return
    if "ts" in df_chunk.columns:
        try:
            df_chunk["ts"] = pd.to_datetime(df_chunk["ts"], utc=True)
        except Exception:
            pass

    # Debug prints for verification (will be captured in logs):
    print("VERIFY: closed.t0/1 UTC-aware ->", getattr(t0, 'tz', None), getattr(t1, 'tz', None))
    if "ts" in df_chunk.columns and not df_chunk["ts"].empty:
        # print the dtype and a sample element's tzinfo to avoid Series tz methods
        sample_tz = None
        try:
            sample_tz = getattr(df_chunk["ts"].iloc[0], 'tz', None)
        except Exception:
            sample_tz = None
        print("VERIFY: df_chunk.ts dtype ->", df_chunk["ts"].dtype, " sample tz ->", sample_tz)

    candle = {
        "open":  float(df_chunk["price"].iloc[0]),
        "high":  float(df_chunk["price"].max()),
        "low":   float(df_chunk["price"].min()),
        "close": float(df_chunk["price"].iloc[-1]),
        "volume":float(df_chunk["size"].sum()),
    }

    # 2) OB snapshot לא-הרסני (עדכון אחרון <= t1)
    ob_buf = ctx["orderbook_buffer"]
    snapshot = ob_buf.last_at_or_before(t1)
    ob_dict = _ob_snapshot_to_features(snapshot)

    # 3) Trade History + Volume Delta על אותו chunk
    th_row = ctx["compute_th"](df_chunk, t0).to_dict("records")[0]
    vd_row = ctx["compute_vd"](df_chunk, t0).to_dict("records")[0]

    # 4) בניית feature row בסיסי (בלי אינדיקטורים עדיין)
    row = ctx["build_feature_row"](
        ts=pd.to_datetime(t1, utc=True), symbol=SYMBOL, interval=INTERVAL,
        candle=candle,
        indicators={}, technicals={},
        orderbook=ob_dict,
        trade_history=th_row,
        volume_delta=vd_row,
    )

    # 5) הוספה ל־df_all עם סכימה יציבה
    df_all = ctx["df_all"]
    schema = ctx["schema"]
    df_all = append_row(df_all, row, schema)

    # 6) עכשיו – אינדיקטורים וטכני על כל df_all (או tail אם ממומש)
    df_all = ctx["add_all_indicators"](df_all)
    df_all = ctx["add_all_technical"](df_all)

    # 7) Targets – רישום נר חדש ועדכון לפי הזמן הנוכחי t1
    filler = ctx["filler"]
    idx = len(df_all) - 1
    filler.register_row(df_all, idx)
    filler.on_tick(df_all, current_ts=pd.to_datetime(t1, utc=True), price_lookup=ctx["price_lookup"])

    # 8) Persist תקופתי
    if idx % ctx["SAVE_EVERY"] == 0:
        ctx["save_df"](df_all, SYMBOL, INTERVAL)

    # 9) החזר df_all המעודכן ל־ctx (כי אנחנו ב-asyncland)
    ctx["df_all"] = df_all
