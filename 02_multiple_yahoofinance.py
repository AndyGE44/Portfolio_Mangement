import yfinance as yf
import time
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed # 引入线程池
import config 
import pandas as pd

# 初始化日志
logger = config.setup_logging("PriceSync", "sync_prices_turbo.log")

# 全局数据库引擎 (SQLAlchemy 引擎是线程安全的)
engine = create_engine(config.DB_CONNECTION, pool_size=20, max_overflow=0)

def process_one_stock(stock_data):
    """
    【修复版】增加去重逻辑，解决 'cannot convert series to float' 报错
    """
    product_id, ticker, vendor_id = stock_data
    
    try:
        # 1. 下载数据
        df = yf.download(ticker, start="2023-01-01", progress=False, auto_adjust=False)
        
        if df.empty:
            logger.warning(f"{ticker} 无数据")
            return
            
        # 2. 扁平化列名 (Handle MultiIndex)
        if isinstance(df.columns, pd.MultiIndex):
            # 取第一层级 ('Open', 'MMM') -> 'Open'
            df.columns = df.columns.get_level_values(0)

        # --- 【核心修复】强力去重 ---
        # 如果 flattening 后出现了两个 'Open'，这行代码会保留第一个，删掉后面的
        # 这是解决 "Series to float" 错误的关键
        df = df.loc[:, ~df.columns.duplicated()]
        # ------------------------
        
        # 3. 格式化日期
        # 确保 index 是 DatetimeIndex 再 strftime
        if not isinstance(df.index, pd.DatetimeIndex):
             df.index = pd.to_datetime(df.index)
             
        df.index = df.index.strftime('%Y-%m-%d')
        df = df.reset_index()
        
        # 4. 准备数据 (使用 .get 安全获取，防止某一列不存在报错)
        quotes = []
        for _, row in df.iterrows():
            try:
                # 只有当 Open/High/Low/Close 都在的时候才处理
                if 'Open' not in row or 'Close' not in row:
                    continue
                    
                quotes.append({
                    'pid': product_id,
                    'date': row['Date'], # reset_index 后 Date 变成了列
                    'o': float(row['Open']),
                    'h': float(row['High']),
                    'l': float(row['Low']),
                    'c': float(row['Close']),
                    'v': int(row.get('Volume', 0)), # Volume 有时候会缺失，给个默认值 0
                    'vid': vendor_id
                })
            except ValueError:
                continue # 如果某一行数据本身是坏的 (比如 NaN)，跳过该行

        if not quotes:
            return

        # 5. 写入数据库
        with engine.begin() as conn:
            stmt = text("""
                INSERT INTO quotes (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
                VALUES (:pid, :date, :o, :h, :l, :c, :v, 'eod', :vid)
                ON CONFLICT (product_id, trade_date) 
                DO UPDATE SET close = EXCLUDED.close, volume = EXCLUDED.volume
            """)
            conn.execute(stmt, quotes)
        
        logger.info(f"✅ {ticker} 完成 ({len(quotes)} 条)")
        
    except Exception as e:
        logger.error(f"❌ {ticker} 失败: {e}")

def main():
    logger.info("=== 🚀 启动多线程极速同步 ===")
    
    # 1. 获取任务列表
    products = []
    try:
        with engine.connect() as conn:
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='fmp'")).scalar()
            result = conn.execute(text("""
                SELECT p.id, vm.vendor_ticker 
                FROM products p
                JOIN vendor_mappings vm ON p.id = vm.product_id
                WHERE vm.vendor_id = :vid AND p.type = 'stock'
            """), {'vid': vendor_id}).fetchall()
            
            # 打包任务数据：(id, ticker, vendor_id)
            for row in result:
                products.append((row[0], row[1], vendor_id))
                
    except Exception as e:
        logger.critical(f"读取任务失败: {e}")
        return

    logger.info(f"待处理股票: {len(products)} 只")

    # 2. 开启线程池 (这里设置 max_workers=5，太高会被 Yahoo 封)
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 提交所有任务给线程池
        futures = [executor.submit(process_one_stock, p) for p in products]
        
        # 等待所有任务完成
        for future in as_completed(futures):
            pass # 这里可以处理异常，但我们在函数里已经 catch 过了

    end_time = time.time()
    logger.info(f"🎉 全部完成！耗时: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    import pandas as pd # 记得在文件头导入
    main()