from __future__ import annotations
import asyncio
from datetime import datetime, timezone

import pandas as pd

# --- Settings + IO ---
from core.settings_manager import SettingsManager, CFG
from io_utils.persist import save_csv
from io_utils.render import render_chart

# --- זרם טריידים ונרות ---
from live_data.trade_history import stream_trades
from graphs.graphs_time import build_candles_from_stream

# --- אינדיקטורים בסיסיים (EMA/RSI/BB/VWAP) ---
from indicator.run_indikators import add_all_indicators

# --- טכני (VWAP/BB/EMA TECH) – אגרגטור אחד ---
from technical_analysis.run_technical import add_all_technical


# ========= הגדרות מתוך Settings =========
SYMBOL = CFG("symbol")
INTERVAL_SEC = CFG("interval_sec")
CSV_PATH = CFG("io.csv_path")
PNG_PATH = CFG("io.png_path")
MAX_POINTS_ON_CHART = CFG("chart.max_points", 400)


# ========= עזר: הוספת נר ל-DF =========
def _append_candle(df: pd.DataFrame, candle: dict) -> pd.DataFrame:
    """
    candle צפוי לכלול: start_ms/start_iso, open, high, low, close, volume.
    מוודא אינדקס זמן בפורמט UTC pandas.Timestamp.
    """
    start_ms  = candle.get("start_ms")
    start_iso = candle.get("start_iso")
    if start_ms is None and start_iso is None:
        raise ValueError("candle חסר start_ms/start_iso")
    if start_ms is None:
        start_ms = int(pd.to_datetime(start_iso, utc=True).timestamp() * 1000)
    if start_iso is None:
        start_iso = pd.to_datetime(int(start_ms), unit="ms", utc=True).isoformat()

    ts = pd.to_datetime(int(start_ms), unit="ms", utc=True)
    df.loc[ts, ["start_ms", "start_iso", "open", "high", "low", "close", "volume"]] = [
        int(start_ms), start_iso,
        float(candle["open"]), float(candle["high"]),
        float(candle["low"]), float(candle["close"]),
        float(candle["volume"]),
    ]
    return df


# ========= לולאת צריכת נרות =========
async def consume_candles(candles_q: asyncio.Queue) -> None:
    df = pd.DataFrame(columns=[
        "start_ms","start_iso","open","high","low","close","volume",
        "vwap","bb_mid","bb_up","bb_low","bb_width",
        "bb_status","bb_score",
        "vwap_status","vwap_dist_pct","vwap_score",
        # שדות EMA TECH יתווספו דינמית לפי ה-run_technical
    ])
    df.index.name = "time"

    last_start_ms_printed = None

    # קונפיג לשכבת ה-Technical (מ-settings)
    tech_cfg = SettingsManager.get()["technical"]

    while True:
        candle = await candles_q.get()
        try:
            # 1) עדכון DF מנר חדש
            df = _append_candle(df, candle)

            # 2) אינדיקטורים בסיסיים (מחשב ema/vwap/bb עצמם)
            df = add_all_indicators(df)

            # 3) שכבת Technical (VWAP/BB/EMA) – לפי ה-Settings
            df = add_all_technical(df, **tech_cfg)

            # 4) פלט: שמירה + ציור
            save_csv(df, CSV_PATH)
            render_chart(
                df,
                PNG_PATH,
                last_n=MAX_POINTS_ON_CHART,
                title=f"{SYMBOL} • {INTERVAL_SEC}s • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            )

            # 5) הדפסה – פעם אחת לכל נר חדש
            sm = int(candle.get("start_ms"))
            if sm != last_start_ms_printed:
                ts = pd.to_datetime(sm, unit="ms", utc=True)
                last = df.loc[ts]

                close = float(last.get("close", float("nan")))
                vwap  = float(last.get("vwap", float("nan")))
                vwap_status = last.get("vwap_status","")
                vwap_dist   = float(last.get("vwap_dist_pct", float("nan")))
                bb_status   = last.get("bb_status","")
                bb_score    = float(last.get("bb_score", float("nan")))

                # EMA TECH (זוג 12/21 + יחסי ל-21) – אם קיימים
                ema_pair_status = last.get("ema_pair_status_12_21","")
                ema_pair_spread = float(last.get("ema_pair_spread_pct_12_21", float("nan")))
                ema_status_21   = last.get("ema_status_21","")
                ema_dist_21     = float(last.get("ema_dist_pct_21", float("nan")))
                ema_score_21    = float(last.get("ema_score_21", float("nan")))

                print(
                    f"[{ts.strftime('%Y-%m-%d %H:%M:%S UTC')}] "
                    f"close={close:.4f} | "
                    f"VWAP={vwap:.4f} ({vwap_status}, {vwap_dist:.2f}%) | "
                    f"BB={bb_status} (score={bb_score:.2f}) | "
                    f"EMA12/21={ema_pair_status} (spread={ema_pair_spread:.2f}%) | "
                    f"EMA21_rel={ema_status_21} (dist={ema_dist_21:.2f}%, score={ema_score_21:.2f})"
                )
                last_start_ms_printed = sm

        except Exception as e:
            print(f"[ERROR] candle processing failed: {e}")
        finally:
            candles_q.task_done()


# ========= main: חיבור הכל =========
async def main():
    trades_q: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    candles_q: asyncio.Queue = asyncio.Queue(maxsize=10_000)

    trades_task = asyncio.create_task(
        stream_trades(symbol=SYMBOL, out_q=trades_q, reconnect_delay=1.0)
    )
    candles_task = asyncio.create_task(
        build_candles_from_stream(trades_q=trades_q, candles_q=candles_q, interval_sec=INTERVAL_SEC)
    )
    consume_task = asyncio.create_task(consume_candles(candles_q))

    try:
        await asyncio.gather(trades_task, candles_task, consume_task)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        print("Stopping…")
        for t in (trades_task, candles_task, consume_task):
            t.cancel()
        await asyncio.gather(*[t for t in (trades_task, candles_task, consume_task)], return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
