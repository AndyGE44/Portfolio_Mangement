import time
import yfinance as yf
import pandas as pd
import concurrent.futures
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import config 

# ==========================================
# 全局配置
# ==========================================
DEFAULT_START_TIMESTAMP = 1672531200 
MAX_WORKERS = 5  

EOD_CUTOFF_HOUR = 16
EOD_CUTOFF_MINUTE = 5
SOURCE_TYPE = 'eod'

logger = config.setup_logging("PriceSync", "sync_prices_yfinance.log")

def fetch_ticker_yfinance(ticker_data, start_date, end_date, today_str, is_before_eod_cutoff):
    """
    使用 yfinance 获取历史数据。自动处理 Yahoo 的 Cookie 和 Crumb 验证。
    """
    product_id, ticker = ticker_data
    
    try:
        # yf.Ticker 获取数据，auto_adjust=False 保持原始的 open/high/low/close
        tkr = yf.Ticker(ticker)
        df = tkr.history(start=start_date, end=end_date, auto_adjust=False)
        
        if df.empty:
            return ticker_data, []

        history = []
        
        # 遍历 Pandas DataFrame
        for index, row in df.iterrows():
            # index 是 datetime 格式，转换为 YYYY-MM-DD
            parsed_date = index.strftime("%Y-%m-%d")
            
            # 检查当天数据是否符合 4:05 PM 的 EOD 标准
            if parsed_date == today_str and is_before_eod_cutoff:
                continue

            # 处理可能存在的 NaN/空值
            if pd.isna(row['Open']):
                continue

            history.append({
                'date': parsed_date,
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': int(row['Volume'])
            })

        return ticker_data, history

    except Exception as e:
        logger.error(f"yfinance 获取 {ticker} 数据失败: {e}")
        return ticker_data, []

def main():
    try:
        engine = create_engine(config.DB_CONNECTION)
    except Exception as e:
        logger.critical(f"数据库连接失败: {e}")
        return

    logger.info("=== 开始同步行情数据 (Source: yfinance - Multi-threaded) ===")
    
    # 提前计算时间
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    today_str = now_ny.strftime("%Y-%m-%d")
    is_before_eod_cutoff = now_ny.time() < dt_time(EOD_CUTOFF_HOUR, EOD_CUTOFF_MINUTE)
    
    # yfinance 需要 YYYY-MM-DD 格式的日期字符串
    start_date = datetime.fromtimestamp(DEFAULT_START_TIMESTAMP).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    try:
        with engine.connect() as conn:
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='yfinance'")).scalar()
           

            result = conn.execute(text("""
                SELECT p.id, vm.vendor_ticker 
                FROM products p
                JOIN vendor_mappings vm ON p.id = vm.product_id
                WHERE vm.vendor_id = :vid AND p.type = 'stock' AND p.id > 100
            """), {'vid': vendor_id}).fetchall()
            products = list(result)
            
    except SQLAlchemyError as e:
        logger.critical(f"数据库查询失败: {e}")
        return

    total = len(products)
    processed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {
            executor.submit(
                fetch_ticker_yfinance, 
                prod, 
                start_date, 
                end_date, 
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
                logger.warning(f"{progress} {ticker} 无数据跳过")
                continue

            quotes_to_insert = [
                {
                    'pid': product_id, 'date': h['date'], 'o': h['open'],
                    'h': h['high'], 'l': h['low'], 'c': h['close'],
                    'v': h['volume'], 'stype': SOURCE_TYPE, 'vid': vendor_id
                } for h in history
            ]

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