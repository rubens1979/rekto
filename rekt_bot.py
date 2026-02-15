import os
import json
import time
import asyncio
import requests
import websockets
import logging
import threading
from collections import defaultdict
from dotenv import load_dotenv
from http.server import BaseHTTPRequestHandler, HTTPServer

# ================== LOGGING (FILE ONLY) ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("rekt_bot.log", encoding="utf-8")
    ]
)
log = logging.getLogger("REKT_BOT")

# ================== CONFIG ==================
load_dotenv()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

MIN_LIQ_USD = float(os.getenv("MIN_LIQ_USD", 300))
CLUSTER_WINDOW = int(os.getenv("CLUSTER_WINDOW", 30))

BINANCE_LIQ_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"

# ================== SIMPLE HTTP SERVER for Render health check ==================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - Rekt Bot is running")

    def log_message(self, format, *args):
        # Отключаем стандартные логи сервера, чтобы не засорять консоль
        return

def run_http_server():
    port = int(os.environ.get("PORT", 10000))
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, HealthHandler)
    log.info(f"HTTP health server started on 0.0.0.0:{port}")
    httpd.serve_forever()

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
        if r.status_code != 200:
            log.warning(f"Telegram response: {r.text}")
        else:
            log.info("Alert sent")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ================== OI + FUNDING ==================
def binance_oi_funding(symbol):
    try:
        base = "https://fapi.binance.com"
        oi = requests.get(
            f"{base}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "5m", "limit": 2},
            timeout=5
        ).json()
        funding = requests.get(
            f"{base}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
            timeout=5
        ).json()
        if len(oi) < 2:
            return 0, 0
        oi_change = (
            (float(oi[-1]["sumOpenInterest"]) - float(oi[-2]["sumOpenInterest"]))
            / float(oi[-2]["sumOpenInterest"]) * 100
        )
        return oi_change, float(funding[0]["fundingRate"])
    except Exception as e:
        log.error(f"Binance OI/Funding error {symbol}: {e}")
        return 0, 0

# ================== CLASSIFIER ==================
def priority_label(total, oi, funding):
    score = 0
    if total > 5_000_000: score += 2
    if abs(oi) > 5:       score += 2
    if abs(funding) > 0.05: score += 1
    return ["⚠️ LOW", "🟢 MEDIUM", "🔴 HIGH", "💀 MAX"][min(score, 3)]

def classify_and_alert(symbol, side, total, price):
    oi, funding = binance_oi_funding(symbol)
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

# ================== MAIN ==================
def main():
    log.info("REKT BOT STARTED (BINANCE ONLY + minimal HTTP)")

    # Запускаем HTTP-сервер в отдельном потоке
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    # Запускаем asyncio loop с websocket-клиентом
    asyncio.run(binance_ws())

if __name__ == "__main__":
    main()
