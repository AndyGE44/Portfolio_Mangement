# config.py
import logging
import sys

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