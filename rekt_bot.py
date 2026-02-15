import os
import json
import time
import asyncio
import requests
import websockets
import logging
import threading
import signal
import sys
from collections import defaultdict
from dotenv import load_dotenv
from http.server import BaseHTTPRequestHandler, HTTPServer

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("rekt_bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)           # ← важно для Render логов
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

# ================== SHUTDOWN FLAG ==================
running = True

def shutdown_handler(sig, frame):
    global running
    log.info(f"Shutdown signal received ({sig}) → stopping gracefully")
    running = False

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ================== MINIMAL HTTP SERVER FOR RENDER ==================
class SimpleHealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - Rekt Bot running\n")

    def log_message(self, format, *args):
        # Не засоряем логи Render'а
        pass

def run_http_server():
    port = int(os.environ.get("PORT", 10000))
    server_address = ("0.0.0.0", port)
    httpd = HTTPServer(server_address, SimpleHealthHandler)
    log.info(f"HTTP health server listening on 0.0.0.0:{port}")
    try:
        httpd.serve_forever()
    except Exception as e:
        log.error(f"HTTP server error: {e}")

# ================== TELEGRAM ALERT ==================
def send_alert(text: str):
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        log.info("Telegram alert sent")
    except Exception as e:
        log.error(f"Telegram failed: {e}")

# ================== OI + FUNDING ==================
def binance_oi_funding(symbol: str) -> tuple:
    try:
        base = "https://fapi.binance.com"
        oi = requests.get(
            f"{base}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "5m", "limit": 2},
            timeout=8,
        ).json()

        funding = requests.get(
            f"{base}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
            timeout=8,
        ).json()

        if len(oi) < 2:
            return 0.0, 0.0

        oi_change = (
            (float(oi[-1]["sumOpenInterest"]) - float(oi[-2]["sumOpenInterest"]))
            / float(oi[-2]["sumOpenInterest"])
            * 100
        )
        return oi_change, float(funding[0]["fundingRate"])
    except Exception as e:
        log.error(f"Binance OI/Funding error {symbol}: {e}")
        return 0.0, 0.0

# ================== PRIORITY & ALERT ==================
def priority_label(total: float, oi: float, funding: float) -> str:
    score = 0
    if total > 5_000_000: score += 2
    if abs(oi) > 5:       score += 2
    if abs(funding) > 0.05: score += 1
    return ["⚠️ LOW", "🟢 MEDIUM", "🔴 HIGH", "💀 MAX"][min(score, 3)]

def classify_and_alert(symbol: str, side: str, total: float, price: float):
    oi, funding = binance_oi_funding(symbol)

    if abs(oi) < 2 and abs(funding) < 0.02:
        log.info(f"Skipped {symbol} — low activity OI={oi:.2f}% Funding={funding:.4f}")
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
""".strip()

    log.info(f"Alert triggered: {symbol} | {priority} | ${total:,.0f}")
    send_alert(msg)

# ================== CLUSTER PROCESSING ==================
clusters = defaultdict(list)

def process_liquidation(symbol: str, side: str, usd_size: float, price: float):
    if usd_size < MIN_LIQ_USD:
        return

    now = time.time.now()
    clusters[symbol].append((now, usd_size, side, price))
    clusters[symbol] = [x for x in clusters[symbol] if now - x[0] <= CLUSTER_WINDOW]

    total = sum(x[1] for x in clusters[symbol])

    if total >= MIN_LIQ_USD * 4:
        log.info(f"Cluster detected {symbol} → ${total:,.0f}")
        classify_and_alert(symbol, side, total, price)
        clusters[symbol].clear()

# ================== BINANCE WS CLIENT ==================
async def binance_ws():
    log.info("Starting Binance WS monitoring loop")
    backoff = 5
    while running:
        log.info("Attempting connection to Binance WS...")
        try:
            async with websockets.connect(BINANCE_LIQ_WS, ping_interval=20, ping_timeout=20) as ws:
                log.info(">>> Binance WS connected SUCCESSFULLY <<<")
                backoff = 5  # reset backoff

                async for message in ws:
                    if not running:
                        break
                    try:
                        data = json.loads(message)
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
                    except Exception as inner:
                        log.warning(f"Message processing error: {inner}")
        except Exception as e:
            log.error(f"WS connection failed: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # exponential backoff max 60s

    log.info("Binance WS loop stopped")

# ================== MAIN ==================
def main():
    log.info("REKT BOT STARTED (Web Service mode with HTTP health + Binance WS)")
    log.info(f"PORT = {os.environ.get('PORT', 'not set')}")

    # Запускаем HTTP-сервер в фоне (daemon)
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    # Запускаем asyncio с WS-клиентом
    asyncio.run(binance_ws())

if __name__ == "__main__":
    main()
