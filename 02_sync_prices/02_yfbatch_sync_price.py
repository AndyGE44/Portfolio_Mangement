import time
import yfinance as yf
import pandas as pd
from datetime import datetime, time as dt_time
from datetime import timedelta # 补充导入 timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import config 

# ==========================================
# 全局配置
# ==========================================
#(2023/01/01 的时间戳)
#DEFAULT_START_TIMESTAMP = 1672531200 

EOD_CUTOFF_HOUR = 16
EOD_CUTOFF_MINUTE = 5
SOURCE_TYPE = 'eod'

logger = config.setup_logging("PriceSync", "sync_prices_yfinance_bulk.log")

def main():
    try:
        engine = create_engine(config.DB_CONNECTION)
    except Exception as e:
        logger.critical(f"数据库连接失败: {e}")
        return

    logger.info("=== 开始同步行情数据 (Source: yfinance Bulk Download) ===")
    
    # 提前计算时间
    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    today_str = now_ny.strftime("%Y-%m-%d")
    is_before_eod_cutoff = now_ny.time() < dt_time(EOD_CUTOFF_HOUR, EOD_CUTOFF_MINUTE)
    
    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        with engine.connect() as conn:
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='yahoo_finance'")).scalar()
            if not vendor_id:
                vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='fmp'")).scalar()

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

    if not products:
        logger.warning("未找到需要同步的产品。")
        return

    # 创建 Ticker 到 Product ID 的映射字典
    ticker_to_pid = {ticker: pid for pid, ticker in products}
    tickers_list = list(ticker_to_pid.keys())
    
    logger.info(f"准备批量下载 {len(tickers_list)} 个 Tickers 的数据...")

    try:
        # 核心提速：使用 yf.download 一次性获取所有数据
        # auto_adjust=False 保持原始价格
        # threads=True 允许 yfinance 内部使用优化的并发
        df = yf.download(
            tickers_list, 
            start=start_date, 
            end=end_date, 
            auto_adjust=False,
            threads=True,
            ignore_tz=True # 忽略时区，保持纯粹的 YYYY-MM-DD
        )
        
        if df.empty:
            logger.warning("下载的数据为空。")
            return

        # yf.download 返回的是 MultiIndex DataFrame。
        # 使用 .stack() 将 Ticker 从列移动到行，方便我们遍历
        # 这会将 DataFrame 转成包含 Date, Ticker, Open, High, Low, Close, Volume 的扁平结构
        df_stacked = df.stack(level="Ticker", future_stack=True).reset_index()
        
    except Exception as e:
        logger.error(f"批量下载数据失败: {e}")
        return

    quotes_to_insert = []
    
    # 遍历扁平化后的数据并构建入库字典
    for _, row in df_stacked.iterrows():
        ticker = row['Ticker']
        pid = ticker_to_pid.get(ticker)
        
        if not pid:
            continue
            
        # 解析日期
        parsed_date = row['Date'].strftime("%Y-%m-%d")
        
        # EOD 逻辑判断
        if parsed_date == today_str and is_before_eod_cutoff:
            continue
            
        # 过滤空值 (如果某天停牌，数据可能为 NaN)
        if pd.isna(row.get('Open')) or pd.isna(row.get('Close')):
            continue

        quotes_to_insert.append({
            'pid': pid,
            'date': parsed_date,
            'o': float(row['Open']),
            'h': float(row['High']),
            'l': float(row['Low']),
            'c': float(row['Close']),
            'v': int(row.get('Volume', 0)),
            'stype': SOURCE_TYPE,
            'vid': vendor_id
        })

    if not quotes_to_insert:
        logger.warning("处理后没有可入库的数据。")
        return

    logger.info(f"数据处理完成，准备分批入库 {len(quotes_to_insert)} 条记录...")

    # 你刚才可能改成了 1000，这里保持 1000 或者提升到 2000 都可以
    CHUNK_SIZE = 5000 
    total_inserted = 0

    for i in range(0, len(quotes_to_insert), CHUNK_SIZE):
        chunk = quotes_to_insert[i : i + CHUNK_SIZE]
        
        # 1. 动态拼接 VALUES 的占位符，例如: (:p0, :d0, ...), (:p1, :d1, ...)
        value_strings = []
        params = {}
        
        for j, q in enumerate(chunk):
            value_strings.append(f"(:pid_{j}, :date_{j}, :o_{j}, :h_{j}, :l_{j}, :c_{j}, :v_{j}, :stype_{j}, :vid_{j})")
            params[f"pid_{j}"] = q['pid']
            params[f"date_{j}"] = q['date']
            params[f"o_{j}"] = q['o']
            params[f"h_{j}"] = q['h']
            params[f"l_{j}"] = q['l']
            params[f"c_{j}"] = q['c']
            params[f"v_{j}"] = q['v']
            params[f"stype_{j}"] = q['stype']
            params[f"vid_{j}"] = q['vid']
            
        # 把所有的 (:p0... ) 用逗号连起来
        values_sql = ",\n".join(value_strings)

        # 2. 构造真正的单条 Multi-row SQL 语句
        stmt = text(f"""
            INSERT INTO quotes (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
            VALUES {values_sql}
            ON CONFLICT (product_id, trade_date, source_type, vendor_id) 
            DO UPDATE SET 
                open = EXCLUDED.open, high = EXCLUDED.high,
                low = EXCLUDED.low, close = EXCLUDED.close, 
                volume = EXCLUDED.volume
        """)

        try:
            with engine.begin() as conn:
                # 3. 这次真正只发生 1 次网络请求！
                conn.execute(stmt, params)
            
            total_inserted += len(chunk)
            logger.info(f"写入进度: {total_inserted} / {len(quotes_to_insert)} 条...")
            
        except SQLAlchemyError as e:
            logger.error(f"批次写入数据库失败 (第 {i} 到 {i + len(chunk)} 条): {e}")

    logger.info("=== 行情数据同步全部完成！ ===")

if __name__ == "__main__":
    main()