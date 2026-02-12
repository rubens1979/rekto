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
        log.info(f"Telegram response: {r.status_code}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ================== LOAD OKX SYMBOLS ==================
OKX_SYMBOLS = set()

def load_okx_symbols():
    global OKX_SYMBOLS
    try:
        url = "https://www.okx.com/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        r = requests.get(url, params=params, headers=HEADERS, timeout=10).json()

        if r.get("code") != "0":
            log.error(f"Failed to load OKX instruments: {r}")
            return

        for item in r["data"]:
            if item["instId"].endswith("-USDT-SWAP"):
                sym = item["instId"].replace("-USDT-SWAP", "") + "USDT"
                OKX_SYMBOLS.add(sym)

        log.info(f"Loaded {len(OKX_SYMBOLS)} OKX symbols")

    except Exception as e:
        log.error(f"Error loading OKX symbols: {e}")

# ================== OKX OI ==================
def okx_oi(symbol):
    try:
        inst = symbol.replace("USDT", "-USDT-SWAP")

        url = "https://www.okx.com/api/v5/public/open-interest"
        params = {
            "instType": "SWAP",
            "instId": inst
        }

        r = requests.get(url, params=params, headers=HEADERS, timeout=5).json()

        if r.get("code") != "0":
            return None

        lst = r.get("data", [])
        if len(lst) < 2:
            return None

        oi_now = float(lst[0]["oi"])
        oi_prev = float(lst[1]["oi"])

        if oi_prev == 0:
            return None

        return (oi_now - oi_prev) / oi_prev * 100

    except Exception:
        return None

# ================== CLASSIFIER ==================
def priority_label(total, oi):
    score = 0
    if total > 5_000_000:
        score += 2
    if abs(oi) > 5:
        score += 2
    return ["⚠️ LOW", "🟢 MEDIUM", "🔴 HIGH", "💀 MAX"][min(score, 3)]

def classify_and_alert(symbol, side, total, price):
    if symbol not in OKX_SYMBOLS:
        return

    oi = okx_oi(symbol)

    if oi is None:
        return

    if abs(oi) < 2:
        log.info(f"Filtered {symbol}: OI={oi:.2f}%")
        return

    mm = (
        "🔴 POSITION BUILD-UP" if oi > 3 else
        "🟢 POSITION CLOSE" if oi < -3 else
        "⚪ UNCLEAR"
    )

    priority = priority_label(total, oi)

    msg = f"""
💀 <b>REKT ALERT</b> {priority}

<b>{symbol}</b>
Side: {side}
Price: {price}

💰 Size: ${total:,.0f}
📊 OI (OKX): {oi:.2f}%

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

                        process_liquidation(symbol, side, usd, float(o["ap"]))

        except Exception as e:
            log.error(f"Binance WS error: {e}")
            await asyncio.sleep(5)

# ================== HTTP SERVER ==================
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
    log.info("LOADING OKX SYMBOLS...")
    load_okx_symbols()

    log.info("BOT THREAD STARTED")
    threading.Thread(target=run_bot, daemon=True).start()
    run_http()
