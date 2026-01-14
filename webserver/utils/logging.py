import logging
from logging.handlers import RotatingFileHandler
import os


def setup_logger(
    name: str = "app",
    log_file: str = "app.log",
    log_dir: str = "logs",
    level=logging.INFO,
):
    """
    공통 로거 설정
    - 파일 로그 (rotate)
    - 콘솔 로그 (최소 출력)
    """

    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger(name)

    # 이미 설정된 logger면 재설정 방지
    if logger.handlers:
        return logger

    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # 🔹 파일 핸들러 (rotate)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    # 🔹 콘솔 핸들러 (경고 이상만)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
