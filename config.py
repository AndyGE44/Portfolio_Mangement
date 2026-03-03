# config.py
import logging
import sys
import os
from dotenv import load_dotenv

# 核心魔法：这一行会尝试在本地寻找 .env 文件并加载里面的变量。
# 如果是在 GitHub Actions 环境里运行，因为没有 .env 文件，它什么也不会做，不会报错。
load_dotenv() 

# 统一从系统环境变量中获取数据库连接
# 本地运行：从刚才加载的 .env 里拿
# GitHub 运行：从 yml 文件里 env: DB_CONNECTION: ${{ secrets.DB_CONNECTION }} 拿
DB_CONNECTION = os.environ.get("DB_CONNECTION")
FMP_API_KEY = os.environ.get("FMP_API_KEY")
# 增加一个安全校验，如果没读到配置，直接报错，避免程序带着空值往后跑
if not DB_CONNECTION:
    raise ValueError("严重错误：未找到 DB_CONNECTION 环境变量，请检查 .env 文件或 GitHub Secrets 配置！")

# --- 日志配置函数 (两个脚本共用) ---
def setup_logging(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 清除旧的 handlers 防止重复打印
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # 文件日志
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger