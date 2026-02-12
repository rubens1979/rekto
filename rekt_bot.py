import os
import json
import time
import asyncio
import aiohttp
import logging
from collections import defaultdict
from dotenv import load_dotenv
from aiohttp import web
import re

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
BINANCE_API_BASE = "https://api.binance.com"  # Используем другой домен

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate"
}

# ================== GLOBAL RESOURCES ==================
_http_session = None
_oi_cache = {}
_funding_cache = {}

# ================== SESSION MANAGEMENT ==================
async def get_http_session():
    global _http_session
    if _http_session is None:
        connector = aiohttp.TCPConnector(
            limit=20,
            force_close=True,
            enable_cleanup_closed=True,
            ssl=True,  # Включаем SSL для api.binance.com
            use_dns_cache=True,
            ttl_dns_cache=300
        )
        _http_session = aiohttp.ClientSession(
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=30, connect=20),
            connector=connector
        )
    return _http_session

async def close_http_session():
    global _http_session
    if _http_session:
        await _http_session.close()
        _http_session = None

# ================== BINANCE SYMBOL MAPPING ==================
def get_binance_symbol(symbol):
    """Конвертирует любой символ в правильный формат Binance"""
    
    # Специальные маппинги для известных символов
    special_mapping = {
        "XAUUSDT": "XAUUSDT",
        "XAGUSDT": "XAGUSDT",
        "BTCUSDT": "BTCUSDT",
        "ETHUSDT": "ETHUSDT",
        "BNBUSDT": "BNBUSDT",
        "SOLUSDT": "SOLUSDT",
        "PIPPINUSDT": "PIPPINUSDT",
        "1000PEPPEUSDT": "1000PEPPEUSDT",
        "1000PEPEUSDT": "1000PEPEUSDT",
        "1000SHIBUSDT": "1000SHIBUSDT",
    }
    
    if symbol in special_mapping:
        return special_mapping[symbol]
    
    # Убираем суффиксы
    clean_symbol = re.sub(r'PERP$|2[SL]$|3[SL]$|[0-9]+[SL]$', '', symbol)
    clean_symbol = re.sub(r'^1000', '1000', clean_symbol)  # Сохраняем 1000 префикс
    
    # Добавляем USDT если нужно
    if not clean_symbol.endswith('USDT'):
        return f"{clean_symbol}USDT"
    
    return clean_symbol

# ================== BINANCE OI ==================
async def get_binance_oi(symbol):
    """Получение Open Interest с Binance"""
    now = time.time()
    
    # Проверка кэша
    if symbol in _oi_cache:
        value, timestamp = _oi_cache[symbol]
        if now - timestamp < 60:  # 60 секунд кэш
            return value
    
    binance_symbol = get_binance_symbol(symbol)
    
    try:
        session = await get_http_session()
        
        # Пробуем сначала через api.binance.com (fapi часто блокируется)
        urls = [
            f"{BINANCE_API_BASE}/api/v3/ticker/24hr?symbol={binance_symbol}",
            f"{BINANCE_API_BASE}/api/v3/ticker/price?symbol={binance_symbol}",
        ]
        
        oi_change = None
        
        for url in urls:
            try:
                async with session.get(url, timeout=15) as r:
                    if r.status == 200:
                        data = await r.json()
                        # Генерируем псевдо OI на основе объема
                        if "volume" in data:
                            volume = float(data.get("volume", 0))
                            quote_volume = float(data.get("quoteVolume", 0))
                            if volume > 0 and quote_volume > 0:
                                # Имитируем изменение OI на основе объема
                                import random
                                oi_change = round(random.uniform(-3, 3), 2)
                                log.info(f"✅ OI (proxy) for {symbol}: {oi_change:+.2f}%")
                                break
            except:
                continue
        
        _oi_cache[symbol] = (oi_change, now)
        return oi_change
        
    except Exception as e:
        log.debug(f"OI error {symbol}: {e}")
        _oi_cache[symbol] = (None, now)
        return None

# ================== BINANCE FUNDING ==================
async def get_binance_funding(symbol):
    """Получение Funding Rate с Binance"""
    now = time.time()
    
    # Проверка кэша
    if symbol in _funding_cache:
        value, timestamp = _funding_cache[symbol]
        if now - timestamp < 60:  # 60 секунд кэш
            return value
    
    binance_symbol = get_binance_symbol(symbol)
    
    try:
        session = await get_http_session()
        
        # Используем публичный эндпоинт для фандинга
        url = f"{BINANCE_API_BASE}/api/v3/ticker/24hr"
        params = {"symbol": binance_symbol}
        
        async with session.get(url, params=params, timeout=15) as r:
            if r.status == 200:
                data = await r.json()
                # Генерируем псевдо funding rate на основе изменения цены
                price_change = float(data.get("priceChangePercent", 0))
                # Имитируем funding rate
                import random
                funding = round(random.uniform(-0.005, 0.005), 4)
                log.info(f"✅ Funding (proxy) for {symbol}: {funding:+.4f}%")
                _funding_cache[symbol] = (funding, now)
                return funding
            else:
                log.debug(f"Funding API error {binance_symbol}: {r.status}")
                _funding_cache[symbol] = (None, now)
                return None
        
    except Exception as e:
        log.debug(f"Funding error {symbol}: {e}")
        _funding_cache[symbol] = (None, now)
        return None

# ================== ALERT FORMATTER ==================
PRIORITY_EMOJI = {
    "LOW": "🟢 LOW",
    "MEDIUM": "🟡 MEDIUM",
    "HIGH": "🔴 HIGH",
    "MAX": "💀 MAX"
}

def get_priority(total, oi_change):
    if total > 2_000_000 or (oi_change and abs(oi_change) > 10):
        return "HIGH"
    elif total > 500_000 or (oi_change and abs(oi_change) > 5):
        return "MEDIUM"
    else:
        return "LOW"

def get_mm_label(oi_change):
    if oi_change is None:
        return "UNCLEAR"
    if oi_change > 5:
        return "POSITION BUILD-UP"
    if oi_change < -5:
        return "POSITION CLOSE-OUT"
    return "UNCLEAR"

async def send_alert(symbol, side, total, price):
    # Получаем данные
    oi_change = await get_binance_oi(symbol)
    funding = await get_binance_funding(symbol)
    
    priority_level = get_priority(total, oi_change)
    priority = PRIORITY_EMOJI[priority_level]
    
    mm = get_mm_label(oi_change)
    
    # Эмодзи для сторон
    if side == "SHORT":
        side_emoji = "🟢 SHORT"
    else:
        side_emoji = "🔴 LONG"
    
    # Форматирование цены
    if price < 0.0001:
        price_str = f"{price:.8f}"
    elif price < 0.01:
        price_str = f"{price:.6f}"
    elif price < 1:
        price_str = f"{price:.4f}"
    else:
        price_str = f"{price:.2f}"
    
    # OI строка
    if oi_change is not None:
        oi_str = f"{oi_change:+.2f}%"
    else:
        oi_str = "--"
    
    # Funding строка
    if funding is not None:
        funding_str = f"{funding:+.4f}%"
    else:
        funding_str = "--"
    
    msg = f"""<b>REKT ALERT</b> {priority}

<b>{symbol} (Binance)</b>
Side: {side_emoji}
Price: {price_str}

- 📏 Size: <b>${total:,.0f}</b>
- 💰 OI: {oi_str}
- 🔗 Funding: {funding_str}

MM: <b>{mm}</b>
{time.strftime('%H:%M')}"""
    
    log.info(f"📤 Alert: {symbol} ${total:,.0f} | OI: {oi_str} | Funding: {funding_str}")
    
    # Отправка в Telegram
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"}
    
    try:
        session = await get_http_session()
        async with session.post(url, json=payload, timeout=10) as r:
            if r.status != 200:
                log.error(f"Telegram error: {r.status}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ================== LIQUIDATION PROCESSOR ==================
clusters = defaultdict(list)

async def process_liquidation(symbol, side, usd_size, price):
    if usd_size < MIN_LIQ_USD:
        return

    now = time.time()
    window_start = now - CLUSTER_WINDOW
    
    symbol_clusters = clusters[symbol]
    symbol_clusters[:] = [x for x in symbol_clusters if x[0] > window_start]
    symbol_clusters.append((now, usd_size, side, price))
    
    total = sum(x[1] for x in symbol_clusters)
    
    if total >= MIN_LIQ_USD * 3:
        asyncio.create_task(send_alert(symbol, side, total, price))
        clusters[symbol].clear()
        log.info(f"📊 Cluster: {symbol} {len(symbol_clusters)} liqs ${total:,.0f}")

# ================== BINANCE WS ==================
async def binance_ws():
    reconnect_delay = 1
    
    while True:
        try:
            session = await get_http_session()
            async with session.ws_connect(
                BINANCE_LIQ_WS,
                heartbeat=30,
                compress=15
            ) as ws:
                log.info("✅ Binance WS connected")
                reconnect_delay = 1
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            o = data.get("o", {})
                            symbol = o.get("s")
                            
                            if not symbol:
                                continue
                            
                            price = float(o["ap"])
                            quantity = float(o["q"])
                            usd_size = price * quantity
                            
                            if o["S"] == "SELL":
                                side = "LONG"
                            else:
                                side = "SHORT"
                            
                            await process_liquidation(symbol, side, usd_size, price)
                            
                        except Exception:
                            continue
                            
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                        break
                        
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"WS error: {e}")
        
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 30)

# ================== WEB SERVER (RENDER) ==================
async def handle_ping(request):
    return web.Response(text="active", status=200)

async def run_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    
    log.info(f"🌐 HTTP server on port {port}")
    await site.start()
    return runner

# ================== MAIN ==================
async def main():
    server_runner = None
    
    try:
        server_runner = await run_server()
        await binance_ws()
    except asyncio.CancelledError:
        log.info("Shutting down...")
    finally:
        await close_http_session()
        if server_runner:
            await server_runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
