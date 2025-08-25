import asyncio
from contextlib import suppress
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# הפקת טריידים חי מה-WS
from live_data.trade_history import stream_trades

async def main():
    symbol = "BTCUSDT"            # ב-Bybit: publicTrade.{symbol}
    out_q = asyncio.Queue()       # התור שה-WS יכניס אליו הודעות

    # מפעילים את ה-WS כמשימת רקע
    producer = asyncio.create_task(stream_trades(symbol, out_q, reconnect_delay=1.0))

    # צרכן מינימלי: קח 5 הודעות מהתור והדפס
    for i in range(200_00):
        msg = await out_q.get()   # מחכה להודעה מה-WS
        print(f"[{i+1}] {msg}")
        out_q.task_done()

    # סוגרים את המשימה בעדינות
    producer.cancel()
    with suppress(asyncio.CancelledError):
        await producer

if __name__ == "__main__":
    asyncio.run(main())
