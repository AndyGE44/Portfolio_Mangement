import time
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import config 

# ==========================================
# 全局配置
# ==========================================
DEFAULT_START_TIMESTAMP = 1672531200 
SCRAPE_TIMEOUT = 10
MAX_WORKERS = 10 # 限制并发数，防止被 Yahoo 封锁 IP

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'identity',
}

# 创建一个全局 Session，自动帮你管理 Cookies
session = requests.Session()
session.headers.update(HEADERS)


EOD_CUTOFF_HOUR = 16
EOD_CUTOFF_MINUTE = 5
SOURCE_TYPE = 'eod'

logger = config.setup_logging("PriceSync", "sync_prices_yahoo_scraper.log")

def fetch_and_parse_ticker(ticker_data, start_timestamp, end_timestamp, today_str, is_before_eod_cutoff):
    """
    独立的工作函数，由线程池调用。负责下载和解析单个 ticker。
    """
    product_id, ticker = ticker_data
    url = f"https://finance.yahoo.com/quote/{ticker}/history/?period1={start_timestamp}&period2={end_timestamp}"
    
    try:
        response = session.get(url, timeout=SCRAPE_TIMEOUT)
        response.raise_for_status()

        # 使用 lxml 提速 (需要 pip install lxml)
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table')
        if not table or not table.find('tbody'):
            return ticker_data, []

        rows = table.find('tbody').find_all('tr')
        history = []

        for row in rows:
            cols = row.find_all('td')
            if len(cols) != 7:
                continue
                
            try:
                date_str = cols[0].text.strip()
                parsed_date = datetime.strptime(date_str, "%b %d, %Y").strftime("%Y-%m-%d")

                if parsed_date == today_str and is_before_eod_cutoff:
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
            except Exception:
                continue

        return ticker_data, history

    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求 {ticker} 失败: {e}")
        return ticker_data, []
    except Exception as e:
        logger.error(f"解析 {ticker} 数据失败: {e}")
        return ticker_data, []

def main():
    engine = create_engine(config.DB_CONNECTION)
    logger.info("=== 开始同步行情数据 (Source: Yahoo Scraper - Multi-threaded) ===")
    
    # 获取时间相关状态 (只计算一次)
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    today_str = now_ny.strftime("%Y-%m-%d")
    is_before_eod_cutoff = now_ny.time() < dt_time(EOD_CUTOFF_HOUR, EOD_CUTOFF_MINUTE)
    end_timestamp = int(time.time())

    try:
        with engine.connect() as conn:
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='yahoo_finance'")).scalar()
                    
            result = conn.execute(text("""
                SELECT p.id, vm.vendor_ticker 
                FROM products p
                JOIN vendor_mappings vm ON p.id = vm.product_id
                WHERE vm.vendor_id = :vid AND p.type = 'stock'
            """), {'vid': vendor_id}).fetchall()
            products = list(result)
            
    except SQLAlchemyError as e:
        logger.critical(f"数据库查询失败: {e}")
        return

    total = len(products)
    processed_count = 0

    # 使用线程池并发抓取
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_ticker = {
            executor.submit(
                fetch_and_parse_ticker, 
                prod, 
                DEFAULT_START_TIMESTAMP, 
                end_timestamp, 
                today_str, 
                is_before_eod_cutoff
            ): prod for prod in products
        }

        for future in concurrent.futures.as_completed(future_to_ticker):
            processed_count += 1
            product_data, history = future.result()
            product_id, ticker = product_data
            
            progress = f"[{processed_count}/{total}]"
            
            if not history:
                logger.warning(f"{progress} {ticker} 无数据或抓取失败跳过")
                continue

            # 准备入库数据
            quotes_to_insert = [
                {
                    'pid': product_id, 'date': h['date'], 'o': h['open'],
                    'h': h['high'], 'l': h['low'], 'c': h['close'],
                    'v': h['volume'], 'stype': SOURCE_TYPE, 'vid': vendor_id
                } for h in history
            ]

            # 写入数据库 (保持单线程写入，防止数据库连接池压力过大)
            try:
                with engine.begin() as conn:
                    stmt = text("""
                        INSERT INTO quotes (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
                        VALUES (:pid, :date, :o, :h, :l, :c, :v, :stype, :vid)
                        ON CONFLICT (product_id, trade_date, source_type, vendor_id) 
                        DO UPDATE SET 
                            open = EXCLUDED.open, high = EXCLUDED.high,
                            low = EXCLUDED.low, close = EXCLUDED.close, 
                            volume = EXCLUDED.volume
                    """)
                    conn.execute(stmt, quotes_to_insert)
                logger.info(f"{progress} {ticker} 入库 {len(quotes_to_insert)} 条")
            except SQLAlchemyError as e:
                logger.error(f"{progress} {ticker} 写入失败: {e}")

if __name__ == "__main__":
    main()