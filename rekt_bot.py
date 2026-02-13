import os
import json
import time
import asyncio
import requests
import websockets
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import defaultdict
from dotenv import load_dotenv

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("REKT_BOT")

# ================== CONFIG ==================
load_dotenv()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

MIN_LIQ_USD = float(os.getenv("MIN_LIQ_USD", 1000))
CLUSTER_WINDOW = int(os.getenv("CLUSTER_WINDOW", 60))

BINANCE_LIQ_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"

# ================== TELEGRAM ==================
def send_alert(text: str):
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }
        r = requests.post(url, json=payload, timeout=5)
        log.info(f"Telegram response: {r.status_code}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ================== AGGREGATOR ==================
clusters = defaultdict(list)

def process_liquidation(symbol, side, usd_size, price):
    if usd_size < MIN_LIQ_USD:
        return

    now = time.time()
    clusters[symbol].append((now, usd_size, side, price))

    clusters[symbol] = [
        x for x in clusters[symbol]
        if now - x[0] <= CLUSTER_WINDOW
    ]

    total = sum(x[1] for x in clusters[symbol])

    if total >= MIN_LIQ_USD * 3:
        msg = f"""
💀 <b>LIQUIDATION ALERT</b>

<b>{symbol}</b>
Side: {side}
Price: {price}

💰 Cluster size: ${total:,.0f}
"""
        log.info(f"Cluster {symbol} ${total:,.0f}")
        send_alert(msg)
        clusters[symbol].clear()

# ================== BINANCE WS ==================
async def binance_ws():
    while True:
        try:
            log.info("Connecting to Binance WS...")
            async with websockets.connect(BINANCE_LIQ_WS) as ws:
                log.info("Binance WS connected")

                async for raw in ws:
                    data = json.loads(raw)
                    events = [data] if isinstance(data, dict) else data

                    for e in events:
                        if "o" not in e:
                            continue

                        o = e["o"]
                        symbol = o["s"]

                        usd = float(o["ap"]) * float(o["q"])
                        side = "LONG" if o["S"] == "SELL" else "SHORT"

                        process_liquidation(
                            symbol,
                            side,
                            usd,
                            float(o["ap"])
                        )

        except Exception as e:
            log.error(f"Binance WS error: {e}")
            await asyncio.sleep(5)

# ================== HTTP SERVER (for Render) ==================
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"REKT BOT RUNNING")

def run_http():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info(f"HTTP server running on port {port}")
    server.serve_forever()

# ================== MAIN ==================
def run_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(binance_ws())

if __name__ == "__main__":
    log.info("BOT THREAD STARTED (BINANCE LIQUIDATIONS ONLY)")
    threading.Thread(target=run_bot, daemon=True).start()
    run_http()
