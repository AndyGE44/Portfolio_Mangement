import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo  # Python 3.9+ 内置的时区库
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from tornado.web import url
import config 

# ==========================================
# 全局配置 (Hardcoding 提取)
# ==========================================
# 默认抓取起始时间: 1672531200 (2023-01-01)
DEFAULT_START_TIMESTAMP = 1672531200 
SCRAPE_TIMEOUT = 10
REQUEST_DELAY = 1.5  # 每次请求间的睡眠时间(秒)，防止被封

# 伪装浏览器请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'identity',
}

# 创建一个全局 Session，自动帮你管理 Cookies
session = requests.Session()
session.headers.update(HEADERS)

# EOD 数据的时间限制: 美东时间 16:05 (4:05 PM)
EOD_CUTOFF_HOUR = 16
EOD_CUTOFF_MINUTE = 5
SOURCE_TYPE = 'eod'

# 初始化日志
logger = config.setup_logging("PriceSync", "sync_prices_yahoo_scraper.log")


def get_historical_price_scraping(symbol, start_timestamp=DEFAULT_START_TIMESTAMP, end_timestamp=None):
    if end_timestamp is None:
        end_timestamp = int(time.time())

    url = f"https://finance.yahoo.com/quote/{symbol}/history/?period1={start_timestamp}&period2={end_timestamp}"
    
    # 1. 获取当前美东时间，用于判断是否到达 4:05 PM EOD 标准
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    today_str = now_ny.strftime("%Y-%m-%d")
    is_before_eod_cutoff = now_ny.time() < dt_time(EOD_CUTOFF_HOUR, EOD_CUTOFF_MINUTE)

    try:
        # 删掉 headers 参数，因为 session 已经包含了
        response = session.get(url, timeout=SCRAPE_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        if not table or not table.find('tbody'):
            logger.warning(f"{symbol} 未找到数据表格。")
            return []

        rows = table.find('tbody').find_all('tr')
        history = []

        for row in rows:
            cols = row.find_all('td')
            if len(cols) != 7:
                continue
                
            try:
                date_str = cols[0].text.strip()
                # 转换日期格式: "Mar 2, 2026" -> "2026-03-02"
                parsed_date = datetime.strptime(date_str, "%b %d, %Y").strftime("%Y-%m-%d")

                # 2. 【核心修改】检查当天数据是否符合 4:05 PM 的 EOD 标准
                if parsed_date == today_str and is_before_eod_cutoff:
                    logger.debug(f"跳过 {symbol} 的当天数据 ({parsed_date})，美东时间未到 {EOD_CUTOFF_HOUR}:{EOD_CUTOFF_MINUTE:02d}。")
                    continue

                open_str = cols[1].text.strip().replace(',', '')
                high_str = cols[2].text.strip().replace(',', '')
                low_str = cols[3].text.strip().replace(',', '')
                close_str = cols[4].text.strip().replace(',', '')
                vol_str = cols[6].text.strip().replace(',', '')

                if '-' in [open_str, high_str, low_str, close_str]:
                    continue

                history.append({
                    'date': parsed_date,
                    'open': float(open_str),
                    'high': float(high_str),
                    'low': float(low_str),
                    'close': float(close_str),
                    'volume': int(vol_str) if vol_str else 0
                })
            except Exception as row_e:
                continue

        return history

    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求 {symbol} 失败: {e}")
        return []
    except Exception as e:
        logger.error(f"解析 {symbol} 数据失败: {e}")
        return []

def main():
    try:
        engine = create_engine(config.DB_CONNECTION)
    except Exception as e:
        logger.critical(f"数据库连接失败: {e}")
        return

    logger.info("=== 开始同步行情数据 (Source: Yahoo Scraper) ===")
    
    products = []
    vendor_id = None

    try:
        with engine.connect() as conn:
            # 假设你已经把 vendor 从 fmp 改成了 yahoo，或者仍在使用 fmp 的 id
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='yahoo_finance'")).scalar()
            if not vendor_id:
                logger.warning("未找到 name='yahoo' 的 vendor，尝试使用 'fmp'")
                vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='fmp'")).scalar()

            result = conn.execute(text("""
                SELECT p.id, vm.vendor_ticker 
                FROM products p
                JOIN vendor_mappings vm ON p.id = vm.product_id
                WHERE vm.vendor_id = :vid AND p.type = 'stock' AND p.id > 7
            """), {'vid': vendor_id}).fetchall()
            products = list(result)
            
    except SQLAlchemyError as e:
        logger.critical(f"数据库查询失败: {e}")
        return

    total = len(products)

    for i, (product_id, ticker) in enumerate(products):
        progress = f"[{i+1}/{total}]"
        logger.info(f"{progress} 处理: {ticker} ...")
        
        history = get_historical_price_scraping(ticker)
        
        if not history:
            logger.warning(f"{progress} {ticker} 无数据跳过")
            continue

        quotes_to_insert = []
        for h in history:
            quotes_to_insert.append({
                'pid': product_id,
                'date': h['date'],
                'o': h['open'],
                'h': h['high'],
                'l': h['low'],
                'c': h['close'],
                'v': h['volume'],
                'stype': SOURCE_TYPE,
                'vid': vendor_id
            })

        if quotes_to_insert:
            try:
                with engine.begin() as conn:
                    # 3. 【核心修改】扩大 ON CONFLICT 的目标范围
                    # 前提：你的数据库里必须有 UNIQUE(product_id, trade_date, source_type, vendor_id)
                    stmt = text("""
                        INSERT INTO quotes (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
                        VALUES (:pid, :date, :o, :h, :l, :c, :v, :stype, :vid)
                        ON CONFLICT (product_id, trade_date, source_type, vendor_id) 
                        DO UPDATE SET 
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close, 
                            volume = EXCLUDED.volume
                    """)
                    conn.execute(stmt, quotes_to_insert)
                logger.info(f"{progress} {ticker} 入库 {len(quotes_to_insert)} 条")
            except SQLAlchemyError as e:
                logger.error(f"{progress} {ticker} 写入失败: {e}")
        
        time.sleep(REQUEST_DELAY)

if __name__ == "__main__":
    main()