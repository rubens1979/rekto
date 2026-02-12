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

MIN_LIQ_USD = float(os.getenv("MIN_LIQ_USD", 30))
CLUSTER_WINDOW = int(os.getenv("CLUSTER_WINDOW", 100))

BINANCE_LIQ_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (rekt-bot)"
}

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
        log.info(f"Telegram response: {r.status_code} | {r.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ================== OI + FUNDING ==================
def binance_oi_funding(symbol):
    try:
        base = "https://fapi.binance.com"

        oi = requests.get(
            f"{base}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "5m", "limit": 2},
            headers=HEADERS,
            timeout=5
        ).json()

        funding = requests.get(
            f"{base}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
            headers=HEADERS,
            timeout=5
        ).json()

        if not isinstance(oi, list) or len(oi) < 2:
            return None, None
        if not isinstance(funding, list) or len(funding) < 1:
            return None, None

        oi_change = (
            (float(oi[-1]["sumOpenInterest"]) -
             float(oi[-2]["sumOpenInterest"]))
            / float(oi[-2]["sumOpenInterest"]) * 100
        )

        return oi_change, float(funding[0]["fundingRate"])

    except Exception as e:
        log.error(f"Binance OI/Funding error {symbol}: {e}")
        return None, None

# ================== CLASSIFIER ==================
def priority_label(total, oi, funding):
    score = 0
    if total > 5_000_000:
        score += 2
    if abs(oi) > 5:
        score += 2
    if abs(funding) > 0.05:
        score += 1
    return ["⚠️ LOW", "🟢 MEDIUM", "🔴 HIGH", "💀 MAX"][min(score, 3)]

def classify_and_alert(symbol, side, total, price):
    oi, funding = binance_oi_funding(symbol)

    # ❌ если нет данных — не шлём алерт
    if oi is None or funding is None:
        log.info(f"No OI/Funding data for {symbol}, skipping alert")
        return

    if abs(oi) < 2 and abs(funding) < 0.02:
        log.info(f"Filtered {symbol}: OI={oi:.2f}% Funding={funding:.4f}")
        return

    mm = (
        "🔴 POSITION BUILD-UP" if oi > 3 else
        "🟢 POSITION CLOSE" if oi < -3 else
        "⚪ UNCLEAR"
    )

    priority = priority_label(total, oi, funding)

    msg = f"""
💀 <b>REKT ALERT</b> {priority}

<b>{symbol}</b> (Binance)
Side: {side}
Price: {price}

💰 Size: ${total:,.0f}
📊 OI: {oi:.2f}%
💸 Funding: {funding:.4f}

🧠 MM: <b>{mm}</b>
"""
    log.info(f"{symbol} | {priority} | {mm}")
    send_alert(msg)

# ================== AGGREGATOR ==================
clusters = defaultdict(list)

def process_liquidation(symbol, side, usd_size, price):
    if usd_size < MIN_LIQ_USD:
        return

    now = time.time()
    clusters[symbol].append((now, usd_size, side, price))
    clusters[symbol] = [x for x in clusters[symbol] if now - x[0] <= CLUSTER_WINDOW]

    total = sum(x[1] for x in clusters[symbol])

    if total >= MIN_LIQ_USD * 4:
        log.info(f"Cluster {symbol} ${total:,.0f}")
        classify_and_alert(symbol, side, total, price)
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

                        if symbol.startswith(("BTC", "ETH")):
                            continue

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
    log.info("BOT THREAD STARTED")
    threading.Thread(target=run_bot, daemon=True).start()
    run_http()
