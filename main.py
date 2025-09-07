import asyncio
import pandas as pd
import signal, sys, atexit, traceback

from live_data.trade_history import stream_trades
from live_data.orderbook import stream_orderbook
from live_data.trade_buffer import TradeBuffer
from live_data.orderbook_buffer import OrderBookBuffer
from core.window_aggregator import ReusableAggregator

from dataset.schema_registry import empty_df, ensure_target_cols
from dataset.pipeline import on_candle_ready
from io_utils.storage import load_df, save_df

# מודולים לוגיים (כבר קיימים אצלך)
from indicator.run_indikators import add_all_indicators
from technical_analysis.run_technical import add_all_technical
from dataset.feature_builder import build_feature_row
from technical_live.orderbook_technical import process_orderbook  # אם צריך הרחבה
from technical_live.trade_history_technical import compute_trade_history_technical as compute_th
from technical_live.volume_technical_delta import compute_vd_for_chunk as compute_vd
from dataset.target_filler import TargetFiller

# ===== קונפיג =====
SYMBOL        = "BTCUSDT"
INTERVAL      = "30s"
INTERVAL_SEC  = 30
HORIZONS      = [30, 60, 90, 120]
SAVE_EVERY    = 50

# ===== Buffers & Aggregator =====
trade_buf = TradeBuffer()
ob_buf    = OrderBookBuffer()
agg       = ReusableAggregator(trade_buf, symbol=SYMBOL, interval_sec=INTERVAL_SEC)
filler    = TargetFiller(HORIZONS, commission_bps=5.0, slippage_bps=2.0)

# ===== DataFrame =====
schema = ensure_target_cols(schema={}, horizons=HORIZONS)  # נתחיל סכימה רזה, נגדל תוך כדי
df_all = load_df(SYMBOL, INTERVAL)
if df_all is None or df_all.empty:
    df_all = empty_df(schema)
else:
    # הבטחת tz ל־ts אם קיים
    if "ts" in df_all.columns:
        df_all["ts"] = pd.to_datetime(df_all["ts"], utc=True)

# ===== persist on exit =====
_persisted = False
def _persist_df_all():
    global _persisted, df_all
    if _persisted: return
    try:
        save_df(df_all, SYMBOL, INTERVAL)
        print(f"[persist] rows={len(df_all)} saved ({SYMBOL} {INTERVAL})")
    except Exception:
        traceback.print_exc()
    _persisted = True

atexit.register(_persist_df_all)
for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
    if sig:
        try:
            signal.signal(sig, lambda s,f: (_persist_df_all(), sys.exit(0)))
        except Exception:
            pass

# ===== lookup לצורך TargetFiller =====
def price_lookup(ts_target: pd.Timestamp):
    if df_all.empty or "ts" not in df_all.columns:
        return None
    ts_norm = pd.to_datetime(ts_target, utc=True)
    m = df_all["ts"] == ts_norm
    if not m.any():
        return None
    v = df_all.loc[m, "close"].iloc[0]
    return None if pd.isna(v) else float(v)

# ===== WS Producers/Consumers =====
async def producer_trades(out_q: asyncio.Queue):
    await stream_trades(SYMBOL, out_q)

async def producer_orderbook(out_q: asyncio.Queue):
    await stream_orderbook(SYMBOL, out_q)

async def consumer_trades(in_q: asyncio.Queue):
    while True:
        tr = await in_q.get()
        ts = pd.to_datetime(tr.get("ts_ms"), unit="ms", utc=True)
        trade_buf.append({
            "ts": ts,
            "symbol": SYMBOL,
            "price": float(tr.get("price", 0.0)),
            "size":  float(tr.get("qty", tr.get("size", 0.0))),
            "side":  str(tr.get("side", "")).lower(),
        })
        for closed in agg.on_trade(ts):
            await on_candle_ready(
                t0=closed.t0, t1=closed.t1, df_chunk=closed.df_chunk,
                ctx={
                    "SYMBOL": SYMBOL, "INTERVAL": INTERVAL, "HORIZONS": HORIZONS,
                    "df_all": df_all, "schema": schema, "price_lookup": price_lookup,
                    "orderbook_buffer": ob_buf, "filler": filler,
                    "add_all_indicators": add_all_indicators,
                    "add_all_technical":  add_all_technical,
                    "build_feature_row":  build_feature_row,
                    "compute_th": compute_th, "compute_vd": compute_vd, "process_ob": process_orderbook,
                    "save_df": save_df, "SAVE_EVERY": SAVE_EVERY,
                }
            )
            # קבלת df מעודכן מתוך ctx
            globals()["df_all"] = closed.ctx["df_all"] if hasattr(closed, "ctx") and "df_all" in getattr(closed, "ctx") else df_all
        in_q.task_done()

async def consumer_orderbook(in_q: asyncio.Queue):
    while True:
        up = await in_q.get()
        ts = pd.to_datetime(up.get("ts_ms"), unit="ms", utc=True)
        ob_buf.add_update(
            bids=up.get("bids", []),
            asks=up.get("asks", []),
            ts=float(ts.timestamp()),
        )
        in_q.task_done()

# ===== BOOT =====
async def main_async():
    q_trades = asyncio.Queue(maxsize=20_000)
    q_ob     = asyncio.Queue(maxsize=5_000)
    tasks = [
        asyncio.create_task(producer_trades(q_trades)),
        asyncio.create_task(consumer_trades(q_trades)),
        asyncio.create_task(producer_orderbook(q_ob)),
        asyncio.create_task(consumer_orderbook(q_ob)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except Exception as e:
        print("FATAL Exception:", e)
        traceback.print_exc()
        _persist_df_all()
    except BaseException as e:
        # Catch BaseException (KeyboardInterrupt / SystemExit / GeneratorExit) to
        # print a full traceback for diagnosis and ensure persistence.
        print("FATAL BaseException:", repr(e))
        try:
            traceback.print_exc()
        except Exception:
            pass
        _persist_df_all()
        # Re-raise to preserve the original exit behaviour (non-zero exit code).
        raise