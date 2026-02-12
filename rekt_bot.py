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
OI_CACHE_TTL = int(os.getenv("OI_CACHE_TTL", 30))

BINANCE_LIQ_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate"
}

# ================== GLOBAL RESOURCES ==================
_http_session = None
_oi_cache = {}

# ================== SESSION MANAGEMENT ==================
async def get_http_session():
    """Переиспользуемая HTTP сессия"""
    global _http_session
    if _http_session is None:
        _http_session = aiohttp.ClientSession(
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=10),
            connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        )
    return _http_session

async def close_http_session():
    """Закрытие сессии при остановке"""
    global _http_session
    if _http_session:
        await _http_session.close()
        _http_session = None

# ================== BINANCE OI ==================
def format_symbol_for_binance(symbol):
    """Конвертирует символ в формат Binance (USDT или USD)"""
    if symbol.endswith('USDT'):
        return symbol
    elif symbol.endswith('USD'):
        return symbol
    else:
        return f"{symbol}USDT"

async def get_binance_oi(symbol):
    """Получение Open Interest с Binance"""
    now = time.time()
    
    # Проверка кэша
    if symbol in _oi_cache:
        value, timestamp = _oi_cache[symbol]
        if now - timestamp < OI_CACHE_TTL:
            return value
    
    binance_symbol = format_symbol_for_binance(symbol)
    
    try:
        session = await get_http_session()
        
        # Текущий OI
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        params = {"symbol": binance_symbol}
        
        async with session.get(url, params=params) as r:
            if r.status != 200:
                log.debug(f"OI no data for {binance_symbol}")
                _oi_cache[symbol] = (None, now)
                return None
            
            data = await r.json()
            oi_current = float(data["openInterest"])
        
        # Исторический OI для расчета изменения
        hist_url = "https://fapi.binance.com/futures/data/openInterestHist"
        hist_params = {
            "symbol": binance_symbol, 
            "period": "5m", 
            "limit": 2
        }
        
        oi_change = 0
        async with session.get(hist_url, params=hist_params) as hr:
            if hr.status == 200:
                hist_data = await hr.json()
                if len(hist_data) >= 2:
                    oi_prev = float(hist_data[1]["sumOpenInterest"])
                    if oi_prev > 0:
                        oi_change = (oi_current - oi_prev) / oi_prev * 100
        
        result = {
            "symbol": binance_symbol,
            "value": oi_current,
            "change": round(oi_change, 2)
        }
        
        _oi_cache[symbol] = (result, now)
        return result
        
    except Exception as e:
        log.debug(f"OI error {symbol}: {e}")
        _oi_cache[symbol] = (None, now)
        return None

# ================== PRIORITY & MM ==================
PRIORITY_LABELS = ["⚠️ LOW", "🟢 MEDIUM", "🔴 HIGH", "💀 MAX"]

def priority_label(total, oi_change=None):
    score = 0
    if total > 500_000: score += 1
    if total > 2_000_000: score += 1
    if total > 5_000_000: score += 1
    if oi_change and abs(oi_change) > 5: score += 1
    return PRIORITY_LABELS[min(score, 3)]

def get_mm_label(oi_change):
    if oi_change is None:
        return "⚪ NO OI DATA"
    if oi_change > 8:
        return "🔴 EXTREME LONG"
    if oi_change > 4:
        return "🟡 AGGRESSIVE LONG"
    if oi_change > 1.5:
        return "🟢 LONG BUILD-UP"
    if oi_change < -8:
        return "🔵 EXTREME SHORT"
    if oi_change < -4:
        return "🟣 AGGRESSIVE SHORT"
    if oi_change < -1.5:
        return "🔵 SHORT BUILD-UP"
    return "⚪ SIDEWAYS"

# ================== TELEGRAM ==================
async def send_alert(text: str):
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    
    try:
        session = await get_http_session()
        async with session.post(url, json=payload) as r:
            if r.status != 200:
                log.error(f"Telegram error: {r.status}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ================== ALERT GENERATOR ==================
async def classify_and_alert(symbol, side, total, price):
    """Классификация и отправка алерта с OI"""
    oi_data = await get_binance_oi(symbol)
    
    if oi_data:
        oi_change = oi_data["change"]
        oi_value = f"{oi_data['value']:,.0f}"
        oi_str = f"{oi_change:+.2f}%"
    else:
        oi_change = None
        oi_value = "N/A"
        oi_str = "N/A"
    
    priority = priority_label(total, oi_change)
    mm = get_mm_label(oi_change)
    side_emoji = "🟢 LONG" if side == "LONG" else "🔴 SHORT"
    
    # Форматирование цены
    if price < 0.0001:
        price_str = f"{price:.8f}"
    elif price < 0.01:
        price_str = f"{price:.6f}"
    elif price < 1:
        price_str = f"{price:.4f}"
    elif price < 100:
        price_str = f"{price:.2f}"
    else:
        price_str = f"{price:,.0f}"
    
    msg = f"""{priority} <b>💥 BINANCE LIQUIDATION</b>

<b>{symbol}</b>
Side: {side_emoji}
Price: ${price_str}

💰 Size: <b>${total:,.0f}</b>
📊 OI Change: <code>{oi_str}</code>
📈 OI Total: <code>{oi_value}</code>

🧠 Market: <b>{mm}</b>
⏱️ {time.strftime('%H:%M:%S')} UTC"""
    
    log.info(f"Alert: {symbol} ${total:,.0f} | OI: {oi_str}")
    await send_alert(msg)

# ================== LIQUIDATION PROCESSOR ==================
clusters = defaultdict(list)
alert_tasks = set()

async def process_liquidation(symbol, side, usd_size, price):
    """Обработка ликвидации с кластеризацией"""
    if usd_size < MIN_LIQ_USD:
        return

    now = time.time()
    window_start = now - CLUSTER_WINDOW
    
    symbol_clusters = clusters[symbol]
    symbol_clusters[:] = [x for x in symbol_clusters if x[0] > window_start]
    symbol_clusters.append((now, usd_size, side, price))
    
    total = sum(x[1] for x in symbol_clusters)
    
    if total >= MIN_LIQ_USD * 3:
        # Очистка завершенных задач
        alert_tasks.difference_update({t for t in alert_tasks if t.done()})
        
        task = asyncio.create_task(classify_and_alert(symbol, side, total, price))
        alert_tasks.add(task)
        clusters[symbol].clear()
        
        log.info(f"Cluster: {symbol} {len(symbol_clusters)} liqs ${total:,.0f}")

# ================== BINANCE WS ==================
async def binance_ws():
    """WebSocket для ликвидаций Binance"""
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
                            
                            # Пропускаем BTC и ETH если нужно
                            # if symbol in ["BTCUSDT", "ETHUSDT"]:
                            #     continue
                            
                            price = float(o["ap"])
                            quantity = float(o["q"])
                            usd_size = price * quantity
                            side = "LONG" if o["S"] == "SELL" else "SHORT"
                            
                            await process_liquidation(symbol, side, usd_size, price)
                            
                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            log.debug(f"WS parse error: {e}")
                            continue
                            
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                        break
                        
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"WS error: {e}")
        
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 30)

# ================== WEB SERVER (ТОЛЬКО ДЛЯ RENDER) ==================
async def handle_ping(request):
    """Минимальный эндпоинт для Render"""
    return web.Response(text="active", status=200)

async def run_server():
    """HTTP сервер только для привязки порта Render"""
    app = web.Application()
    app.router.add_get("/", handle_ping)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    
    log.info(f"HTTP server on port {port}")
    await site.start()
    return runner

# ================== MAIN ==================
async def main():
    """Основная функция"""
    server_runner = None
    
    try:
        server_runner = await run_server()
        await binance_ws()
    except asyncio.CancelledError:
        log.info("Shutting down...")
    finally:
        for task in alert_tasks:
            task.cancel()
        
        await close_http_session()
        
        if server_runner:
            await server_runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
