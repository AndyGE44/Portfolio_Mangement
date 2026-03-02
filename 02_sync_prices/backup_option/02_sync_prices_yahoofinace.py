import yfinance as yf  # 【改动1】引入 yfinance
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import config 
import pandas as pd  # 【改动2】引入 pandas 用于数据处理

# 初始化日志
logger = config.setup_logging("PriceSync", "sync_prices_yfinance.log")

def get_historical_price_yf(symbol, start_date="2023-01-01"):
    """
    【修复版】获取 Yahoo 数据，增加去重和扁平化处理
    解决 'Series object has no attribute strftime' 错误
    """
    try:
        # 下载数据
        df = yf.download(symbol, start=start_date, progress=False, auto_adjust=False)
        
        if df.empty:
            return []

        # --- 【核心修复 1】扁平化列名 ---
        # 如果列名是多层索引 (MultiIndex)，比如 ('Open', 'AAPL')，强制取第一层 'Open'
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # --- 【核心修复 2】提前转换日期格式 ---
        # 不要等到循环里再去转，直接在 Index 上批量转，速度更快且不出错
        # 此时 df.index 是 DatetimeIndex
        df.index = df.index.strftime('%Y-%m-%d')
        
        # 重置索引，把 Date (已经是字符串了) 变成普通列
        df = df.reset_index()
        
        # 转为字典列表
        history = []
        for index, row in df.iterrows():
            # 这里的 row['Date'] 现在肯定是字符串了
            history.append({
                'date': row['Date'], 
                'open': float(row['Open']),   # 强制转 float 防止 numpy 类型报错
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': int(row['Volume'])
            })
            
        return history

    except Exception as e:
        logger.error(f"Yahoo 下载 {symbol} 失败: {e}")
        return []
def main():
    try:
        engine = create_engine(config.DB_CONNECTION)
    except Exception as e:
        logger.critical(f"数据库连接失败: {e}")
        return

    logger.info("=== 开始同步行情数据 (Source: Yahoo Finance) ===")
    
    products = []
    vendor_id = None

    try:
        with engine.connect() as conn:
            # 这里的逻辑稍微变一下：
            # 虽然我们现在用 Yahoo 下载，但之前我们在 products 表里映射的是 'fmp'
            # 为了省事，我们继续沿用 vendor_id='fmp' 的记录，
            # 只要 vendor_ticker (股票代码) 是对的就行。
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='fmp'")).scalar()
            
            result = conn.execute(text("""
                SELECT p.id, vm.vendor_ticker 
                FROM products p
                JOIN vendor_mappings vm ON p.id = vm.product_id
                WHERE vm.vendor_id = :vid AND p.type = 'stock'
            """), {'vid': vendor_id}).fetchall()
            products = list(result)
            logger.info(f"待处理股票数量: {len(products)}")
            
    except SQLAlchemyError as e:
        logger.critical(f"数据库查询失败: {e}")
        return

    total = len(products)
    for i, (product_id, ticker) in enumerate(products):
        progress = f"[{i+1}/{total}]"
        logger.info(f"{progress} 处理: {ticker} ...")
        
        # 使用 Yahoo 下载
        history = get_historical_price_yf(ticker)
        
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
                'vid': vendor_id
            })

        if quotes_to_insert:
            try:
                with engine.begin() as conn:
                    stmt = text("""
                        INSERT INTO quotes (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
                        VALUES (:pid, :date, :o, :h, :l, :c, :v, 'eod', :vid)
                        ON CONFLICT (product_id, trade_date) 
                        DO UPDATE SET close = EXCLUDED.close, volume = EXCLUDED.volume
                    """)
                    conn.execute(stmt, quotes_to_insert)
                logger.info(f"{progress} {ticker} 入库 {len(quotes_to_insert)} 条")
            except SQLAlchemyError as e:
                logger.error(f"{progress} {ticker} 写入失败: {e}")
        
        # Yahoo 对速率限制比较宽容，但还是稍微睡一下
        time.sleep(0.1)

if __name__ == "__main__":
    main()