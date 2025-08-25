import asyncio
import websockets
import json

async def listen_orderbook(symbol="BTCUSDT"):
    url = "wss://stream.bybit.com/v5/public/linear"

    async with websockets.connect(url) as ws:
        # שליחת הודעת הרשמה ל־order book
        subscribe_msg = {
            "op": "subscribe",
            "args": [f"orderbook.50.{symbol}"]
        }

        await ws.send(json.dumps(subscribe_msg))
        print(f"🔗 Connected to Order Book for {symbol}")

        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)

                if "data" in data and isinstance(data["data"], dict):
                    asks = data["data"].get("a", [])
                    bids = data["data"].get("b", [])

                    # בדיקה שהרשימות לא ריקות
                    if asks and bids:
                        best_ask = asks[0]
                        best_bid = bids[0]

                        print(f"\n📈 ASK: {best_ask}   📉 BID: {best_bid}")
                    else:
                        print("⚠️ no ask/bid update in this message")

            except Exception as e:
                print("❌ Error:", e)
                break

if __name__ == "__main__":
    asyncio.run(listen_orderbook("BTCUSDT"))