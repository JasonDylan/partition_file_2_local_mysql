import inspect
import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import List

import pytz


def setup_logging(file_path: str, argv: List[str] = sys.argv):
    # 创建日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 创建格式化器,包含文件名和行号信息，并设置香港时区
    class HKFormatter(logging.Formatter):
        def converter(self, timestamp):
            dt = datetime.fromtimestamp(timestamp)
            hong_kong_tz = pytz.timezone("Asia/Hong_Kong")
            return dt.astimezone(hong_kong_tz)

        def formatTime(self, record, datefmt=None):
            dt = self.converter(record.created)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

    formatter = HKFormatter(
        "%(asctime)s - %(levelname)s - %(processName)s - %(threadName)s - %(message)s - %(filename)s:%(lineno)d - [PID: %(process)d]"
    )

    # 使用下划线连接所有参数，创建日志文件夹名称
    current_file = os.path.splitext(os.path.basename(file_path))[0]
    log_folder_name = f"{current_file}_{'_'.join(argv[1:])}"

    # 创建日志文件夹路径
    base_dir = os.path.dirname(file_path)
    log_folder = os.path.join(base_dir, "log")

    # 创建日志文件夹
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # 创建按时间切割的文件处理器（用于详细日志）
    detailed_log_file_path = os.path.join(log_folder, log_folder_name, "log.log")
    os.makedirs(os.path.dirname(detailed_log_file_path), exist_ok=True)
    file_handler = logging.handlers.TimedRotatingFileHandler(
        detailed_log_file_path, when="midnight", interval=1, backupCount=30
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # 将处理器添加到日志记录器
    logger.addHandler(file_handler)

    print(f"Detailed log file created: {detailed_log_file_path}")
