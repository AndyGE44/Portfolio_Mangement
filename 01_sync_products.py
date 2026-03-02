import pandas as pd
import requests  # 【新增】需要引入 requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import config  # 导入配置文件

# 初始化日志
logger = config.setup_logging("ProductSync", "sync_products.log")

def get_sp500_list_from_wiki():
    """从维基百科获取 S&P 500 名单 (使用 requests 伪装浏览器)"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    logger.info(f"正在从 Wikipedia 读取表格: {url}")
    
    try:
        # --- 【修改开始】使用 requests + Header 伪装 ---
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        # 1. 用 requests 下载网页 HTML 文本
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status() # 如果是 403/404 这里会抛出异常
        
        # 2. 将 HTML 文本传给 pandas 解析
        # pandas 不仅能读 URL，也能直接读 HTML 字符串
        tables = pd.read_html(response.text)
        # --- 【修改结束】 ---

        df = tables[0]
        
        constituents = []
        for index, row in df.iterrows():
            symbol = row['Symbol']
            # 【关键】将 BRK.B 转为 BRK-B 以适配 FMP
            fmp_symbol = symbol.replace('.', '-') 
            
            constituents.append({
                'symbol': fmp_symbol,
                'name': row['Security'],
                'sector': row['GICS Sector']
            })
        return constituents

    except Exception as e:
        logger.error(f"从维基百科解析失败: {e}", exc_info=True)
        return []

def main():
    
    try:
        engine = create_engine(config.DB_CONNECTION)
        # 【新增】强制尝试连接一次，如果是坏地址，立刻报错
        with engine.connect() as connection:
            pass 
        logger.info("数据库连接成功 (已验证)")
    except Exception as e:
        logger.critical(f"数据库配置错误: {e}")
        return

    constituents = get_sp500_list_from_wiki()
    if not constituents:
        logger.error("未能获取成分股列表，程序终止")
        return

    logger.info(f"获取到 {len(constituents)} 只股票，开始写入数据库...")
    success_count = 0

    try:
        with engine.begin() as conn:
            # 1. 确保 FMP 供应商存在
            conn.execute(text("INSERT INTO vendors (name) VALUES ('fmp') ON CONFLICT (name) DO NOTHING"))
            vendor_id = conn.execute(text("SELECT id FROM vendors WHERE name='fmp'")).scalar()
            
            logger.info(f"目标 Vendor ID: {vendor_id} (fmp)")

            for item in constituents:
                symbol = item['symbol']
                name = item['name']
                
                try:
                    # 2. 插入/更新 Product
                    sql_product = text("""
                        INSERT INTO products (symbol, name, type, base_currency)
                        VALUES (:s, :n, 'stock', 'USD')
                        ON CONFLICT (symbol, type) DO UPDATE 
                        SET name = EXCLUDED.name
                        RETURNING id
                    """)
                    result = conn.execute(sql_product, {'s': symbol, 'n': name})
                    product_id = result.scalar()

                    # 3. 插入 Mapping (Product ID <-> FMP Vendor)
                    sql_mapping = text("""
                        INSERT INTO vendor_mappings (product_id, vendor_id, vendor_ticker)
                        VALUES (:pid, :vid, :vt)
                        ON CONFLICT (product_id, vendor_id) DO NOTHING
                    """)
                    conn.execute(sql_mapping, {'pid': product_id, 'vid': vendor_id, 'vt': symbol})
                    
                    success_count += 1
                except SQLAlchemyError as db_err:
                    logger.error(f"处理 {symbol} 失败: {db_err}")

        logger.info(f"✅ 产品表同步完成！成功映射: {success_count}/{len(constituents)}")

    except Exception as e:
        logger.critical(f"发生严重错误: {e}", exc_info=True)

if __name__ == "__main__":
    main()