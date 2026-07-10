"""
logging_config — AFDA 统一日志配置
====================================
提供集中式 logging 配置，替代全项目的 print() 调用。

用法：
    from afda.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("消息")
    logger.warning("警告")
    logger.error("错误")

特性：
- 统一格式：时间 [级别] 模块名: 消息
- 默认输出到 stdout，可选文件输出
- 环境变量 AFDA_LOG_LEVEL 控制级别（DEBUG/INFO/WARNING/ERROR）
- 幂等：多次调用 get_logger 不会重复添加 handler
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

_DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DEFAULT_DATEFMT = "%H:%M:%S"
_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_configured = False


def setup_logging(
    level: str | None = None,
    log_file: str | Path | None = None,
) -> None:
    """配置根日志器。

    Parameters
    ----------
    level : str, optional
        日志级别，默认从环境变量 AFDA_LOG_LEVEL 读取，fallback INFO
    log_file : str | Path, optional
        如果提供，同时写入文件
    """
    global _configured

    if level is None:
        level = os.environ.get("AFDA_LOG_LEVEL", "INFO")
    numeric_level = _LOG_LEVELS.get(level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    # 清除已有 handlers（避免重复）
    root.handlers.clear()

    # stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATEFMT))
    stdout_handler.setLevel(numeric_level)
    root.addHandler(stdout_handler)

    # 文件 handler（可选）
    if log_file is not None:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATEFMT))
        file_handler.setLevel(numeric_level)
        root.addHandler(file_handler)

    _configured = True


def get_logger(name: str = "afda") -> logging.Logger:
    """获取一个配置好的 logger。

    首次调用时自动执行 setup_logging()。
    """
    if not _configured:
        setup_logging()
    return logging.getLogger(name)
