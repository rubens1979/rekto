import os
import json
import time
import asyncio
import aiohttp
import logging
from collections import defaultdict
from dotenv import load_dotenv
from aiohttp import web

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
MIN_LIQ_USD = float(os.getenv("MIN_LIQ_USD", 100))
CLUSTER_WINDOW = int(os.getenv("CLUSTER_WINDOW", 60))

BINANCE_LIQ_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"

# Реалистичный заголовок браузера
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

# ================== TELEGRAM ==================
async def send_alert(text: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=5) as r:
                return await r.json()
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ================== BYBIT OI (ASYNC) ==================
async def get_bybit_oi(symbol):
    url = "https://api.bybit.com/v5/market/open-interest"
    params = {"category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": 2}
    
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, params=params, timeout=5) as r:
                if r.status != 200:
                    log.error(f"Bybit API error {r.status} for {symbol}")
                    return None
                
                data = await r.json()
                if data.get("retCode") != 0:
                    return None

                points = data["result"]["list"]
                if len(points) < 2: return None

                oi_now = float(points[0]["openInterest"])
                oi_prev = float(points[1]["openInterest"])
                return (oi_now - oi_prev) / oi_prev * 100
    except Exception as e:
        log.error(f"OI Fetch exception: {e}")
        return None

# ================== LOGIC ==================
def priority_label(total, oi_val):
    score = 0
    if total > 500_000: score += 1
    if total > 2_000_000: score += 1
    if abs(oi_val) > 4: score += 1
    return ["⚠️ LOW", "🟢 MEDIUM", "🔴 HIGH", "💀 MAX"][min(score, 3)]

async def classify_and_alert(symbol, side, total, price):
    oi = await get_bybit_oi(symbol)
    
    # Бот не молчит при ошибке, а пишет N/A
    oi_str = f"{oi:+.2f}%" if oi is not None else "⚠️ N/A"
    oi_val = oi if oi is not None else 0
    
    mm = "⚪ UNKNOWN"
    if oi is not None:
        mm = "🔴 POS BUILD-UP" if oi > 2 else "🟢 POS CLOSE" if oi < -2 else "⚪ SIDEWAYS"

    priority = priority_label(total, oi_val)
    side_emoji = "🟢 LONG" if side == "LONG" else "🔴 SHORT"

    msg = f"""
{priority} <b>REKT ALERT</b>

<b>{symbol}</b>
Side: {side_emoji}
Price: {price:,.4f}

💰 Size: <b>${total:,.0f}</b>
📊 OI Change: <code>{oi_str}</code>

🧠 MM: <b>{mm}</b>
"""
    log.info(f"Signal sent: {symbol} ${total:,.0f}")
    await send_alert(msg)

# ================== AGGREGATOR ==================
clusters = defaultdict(list)

async def process_liquidation(symbol, side, usd_size, price):
    if usd_size < MIN_LIQ_USD: return

    now = time.time()
    clusters[symbol].append((now, usd_size, side, price))
    clusters[symbol] = [x for x in clusters[symbol] if now - x[0] <= CLUSTER_WINDOW]

    total = sum(x[1] for x in clusters[symbol])

    if total >= MIN_LIQ_USD * 3:
        # Запускаем в фоне, чтобы не тормозить WS
        asyncio.create_task(classify_and_alert(symbol, side, total, price))
        clusters[symbol].clear()

# ================== BINANCE WS ==================
async def binance_ws():
    while True:
        try:
            log.info("Connecting to Binance WS...")
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(BINANCE_LIQ_WS) as ws:
                    log.info("Binance WS connected")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            o = data.get("o", {})
                            symbol = o.get("s")
                            if not symbol or symbol.startswith(("BTC", "ETH")): continue

                            usd = float(o["ap"]) * float(o["q"])
                            side = "LONG" if o["S"] == "SELL" else "SHORT"
                            await process_liquidation(symbol, side, usd, float(o["ap"]))
        except Exception as e:
            log.error(f"WS error: {e}")
            await asyncio.sleep(5)

# ================== WEB SERVER (Render Port Binding) ==================
async def handle_ping(request):
    return web.Response(text="BOT ACTIVE", status=200)

async def run_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    log.info(f"HTTP Server started on port {port}")
    await site.start()

# ================== MAIN ==================
async def main():
    await asyncio.gather(
        run_server(),
        binance_ws()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
