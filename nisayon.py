# nisayon.py
import asyncio
import pandas as pd
from contextlib import suppress

# 1) Ingest: Buffer + Pump + Aggregator
from live_data.trade_buffer import TradeBuffer
from live_data.pumps import pump_trades_to_buffer
from core.candle_aggregator import finalize_trades_chunk

# 2) LIVE Indicator: Volume Delta
from live_indicator.volume_technical import add_volume_delta_features

# 3) Technical “קלאסי” (על OHLCV)
from technical_analysis.run_technical import add_all_technical


async def main():
    # ---- קונפיג בדיקה ----
    CANDLE_SECONDS = 60
    symbol = "BTCUSDT"
    # t0 כ-UTC ועם טיים־זון (tz-aware) כדי לא ליפול בהשוואות
    t0 = pd.Timestamp.now(tz="UTC").floor("min")

    # ---- א. מכינים תור (כאילו WS) ובאפר ----
    out_q = asyncio.Queue()
    buffer = TradeBuffer(maxlen=10_000)

    # ---- ב. מפעילים pump: קורא מהתור → מכניס לבאפר (בלי חישובים) ----
    pump_task = asyncio.create_task(pump_trades_to_buffer(out_q, buffer))

    # ---- ג. “מזריקים” 3 עסקאות דמה לתור (פורמט Bybit טיפוסי) ----
    def ms(ts: pd.Timestamp) -> int:
        # בטוח ל-tz-aware
        return int(ts.timestamp() * 1000)

    msg1 = {"i": "t1", "T": ms(t0 + pd.Timedelta(seconds=10)), "s": symbol, "p": 59050.0, "q": 0.30, "S": "Buy"}
    msg2 = {"i": "t2", "T": ms(t0 + pd.Timedelta(seconds=25)), "s": symbol, "p": 59070.0, "q": 0.20, "S": "Sell"}
    msg3 = {"i": "t3", "T": ms(t0 + pd.Timedelta(seconds=62)), "s": symbol, "p": 59090.0, "q": 0.10, "S": "Buy"}  # מחוץ ל-[t0,t1)

    await out_q.put(msg1)
    await out_q.put(msg2)
    await out_q.put(msg3)
    await out_q.join()  # מוודאים שה-pump עיבד הכול

    # ---- ד. סוגרים את הנר: מוציאים trades_chunk לטווח [t0,t1) ----
    trades_chunk = finalize_trades_chunk(buffer, t0, CANDLE_SECONDS, symbol=symbol)
    print("=== trades_chunk ===")
    print(trades_chunk)

    # ---- ה. מחשבים Volume Delta על הצ'אנק (שורה אחת לנר עם vd_*) ----
    vd_df = add_volume_delta_features(trades_chunk, side_mode="exchange")
    if "symbol" not in vd_df.columns:
        vd_df["symbol"] = (trades_chunk["symbol"].iloc[0] if ("symbol" in trades_chunk.columns and not trades_chunk.empty)
        else symbol   
        )    
    print("\n=== vd_df (שורת VD לנר) ===")
    print(vd_df)

    # ---- ו. בונים OHLCV בסיסי מה-trades_chunk רק לבדיקה (בפרודקשן יהיה feed רציף) ----
    if not trades_chunk.empty:
        o = float(trades_chunk["price"].iloc[0])
        h = float(trades_chunk["price"].max())
        l = float(trades_chunk["price"].min())
        c = float(trades_chunk["price"].iloc[-1])
        v = float(trades_chunk["size"].sum())
    else:
        o = h = l = c = v = 0.0

    candles_df_last = pd.DataFrame([{
        "time": t0,
        "symbol": symbol,
        "open": o, "high": h, "low": l, "close": c, "volume": v,
    }])

    # ---- ז. בסיס מינימלי לאינדיקטורים כדי שלא יפול במצב stream (Smoke בלבד) ----
    # VWAP מהעסקאות של הנר
    if not trades_chunk.empty:
        vwap = float((trades_chunk["price"] * trades_chunk["size"]).sum() /
                     (trades_chunk["size"].sum() + 1e-12))
    else:
        vwap = float(candles_df_last["close"].iloc[0])
    candles_df_last["vwap"] = vwap

    # EMA placeholders (בדיקת עשן)
    candles_df_last["ema12"] = candles_df_last["close"].astype(float)
    candles_df_last["ema21"] = candles_df_last["close"].astype(float)

    # אם תראה שגיאת BB, בטל את שלושת השורות הבאות:
    # candles_df_last["bb_middle"] = candles_df_last["close"].astype(float)
    # candles_df_last["bb_upper"]  = candles_df_last["close"].astype(float)
    # candles_df_last["bb_lower"]  = candles_df_last["close"].astype(float)

    # ---- ח. מוסיפים נר קודם מינימלי כדי של-mode="stream" יהיה הקשר ----
    prev = candles_df_last.copy()
    prev["time"] = t0 - pd.Timedelta(seconds=CANDLE_SECONDS)
    candles_for_tech = pd.concat([prev, candles_df_last], ignore_index=True)

    # ---- ט. טכני קלאסי + איחוד עם VD ----
    tech_all = add_all_technical(candles_for_tech, mode="stream")  # EMA/VWAP/BB/נרות
    tech_df = tech_all.tail(1).copy()  # השורה של הנר הנוכחי בלבד

    row = tech_df.merge(vd_df, on=["time", "symbol"], how="left")
    print("\n=== merged row (נר מלא: טכני + VD) ===")
    print(row.tail(1))

    # ---- י. סגירת ה-pump הנצחי ----
    with suppress(asyncio.CancelledError):
        pump_task.cancel()
        await pump_task


if __name__ == "__main__":
    asyncio.run(main())
