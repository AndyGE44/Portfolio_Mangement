import requests
import time
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from requests.exceptions import RequestException
import config # 导入配置文件

# 初始化日志
logger = config.setup_logging("PriceSync", "sync_prices.log")

def safe_request(url, retries=3):
    """FMP API 请求封装 (含 429/402/403 处理)"""
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=10)
            status = resp.status_code
            
            if status == 200:
                return resp.json()
            elif status == 429:
                logger.warning(f"触发限流 (429)，休眠 5 秒后重试... ({i+1}/{retries})")
                time.sleep(5)
            elif status == 403:
                logger.error("❌ 403 Forbidden: 权限不足")
                return None
            elif status == 402:
                logger.critical("❌ 402 Payment Required: 额度用尽，程序停止！")
                sys.exit(0) # 停止整个脚本
            else:
                logger.error(f"API 错误 [{status}]: {url}")
                return None
        except RequestException as e:
            logger.error(f"网络错误: {e}")
            time.sleep(2)
    return None

def get_historical_price(symbol, from_date="2023-01-01"):
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={from_date}&apikey={config.FMP_API_KEY}"
    data = safe_request(url)
    if data and 'historical' in data:
        return data['historical']
    return []

def main():
    try:
        engine = create_engine(config.DB_CONNECTION)
    except Exception as e:
        logger.critical(f"数据库连接失败: {e}")
        return

    logger.info("=== 开始同步行情数据 ===")
    
    products = []
    vendor_id = None

    # 1. 从数据库获取任务列表
    try:
        with engine.connect() as conn:
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='fmp'")).scalar()
            if not vendor_id:
                logger.error("未找到 FMP 供应商记录，请先运行 01_sync_products.py")
                return

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

    # 2. 循环下载
    total = len(products)
    for i, (product_id, fmp_ticker) in enumerate(products):
        progress = f"[{i+1}/{total}]"
        logger.info(f"{progress} 处理: {fmp_ticker} ...")
        
        history = get_historical_price(fmp_ticker)
        
        if not history:
            logger.warning(f"{progress} {fmp_ticker} 无数据跳过")
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
                start_t = time.time()
                with engine.begin() as conn:
                    stmt = text("""
                        INSERT INTO quotes (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
                        VALUES (:pid, :date, :o, :h, :l, :c, :v, 'eod', :vid)
                        ON CONFLICT (product_id, trade_date) 
                        DO UPDATE SET close = EXCLUDED.close, volume = EXCLUDED.volume
                    """)
                    conn.execute(stmt, quotes_to_insert)
                logger.info(f"{progress} {fmp_ticker} 入库 {len(quotes_to_insert)} 条 ({(time.time()-start_t):.2f}s)")
            except SQLAlchemyError as e:
                logger.error(f"{progress} {fmp_ticker} 写入失败: {e}")
        
        # 3. 控频 (FMP 免费版限制 250次/天，速度也不宜太快)
        time.sleep(0.25)

if __name__ == "__main__":
    main()