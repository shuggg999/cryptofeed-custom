"""
统一日志配置模块

提供应用级别和 Uvicorn 的统一日志格式配置。
确保所有日志输出格式一致，支持 PyCharm 点击跳转。

日志格式：
    时间戳 | 级别 | 模块名:行号 | 消息
    2025-11-04 21:25:36.732 | INFO     | cryptofeed_api.app:258 | 🚀 正在启动...
"""

import logging
from typing import Dict, Any

# ============================================================
# 统一日志格式
# ============================================================
# 格式说明：
# %(asctime)s.%(msecs)03d - 时间戳，毫秒用点号（不是逗号）
# %(levelname)s - 日志级别
# %(name)s:%(lineno)d - 模块名:行号（支持PyCharm点击跳转！）
# %(message)s - 日志消息
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"  # 时间格式，不包含毫秒（毫秒由 LOG_FORMAT 中的 %(msecs)03d 处理）


def setup_logging(level: str = "INFO", debug: bool = False) -> None:
    """
    配置应用日志

    Args:
        level: 日志级别（INFO, DEBUG, WARNING, ERROR）
        debug: 是否为调试模式（True 则自动设置为 DEBUG 级别）
    """
    # 如果是调试模式，强制使用 DEBUG 级别
    if debug:
        level = "DEBUG"

    # 配置根日志记录器
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        force=True,  # 强制重新配置（覆盖之前的配置）
    )

    # 降低第三方库的日志级别，避免过多噪音
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)  # 隐藏websockets的DEBUG日志
    logging.getLogger("websockets.client").setLevel(logging.WARNING)
    logging.getLogger("websockets.server").setLevel(logging.WARNING)
    logging.getLogger("clickhouse_connect").setLevel(logging.WARNING)  # 隐藏ClickHouse客户端的DEBUG日志
    logging.getLogger("clickhouse_connect.driver").setLevel(logging.WARNING)


def get_uvicorn_log_config() -> Dict[str, Any]:
    """
    获取 Uvicorn 日志配置

    返回一个符合 Uvicorn logging.config.dictConfig 格式的配置字典，
    使 Uvicorn 的日志格式与应用日志保持一致。

    Returns:
        Dict: Uvicorn 日志配置字典
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            # 统一格式化器
            "default": {
                "format": LOG_FORMAT,
                "datefmt": DATE_FORMAT,
            },
            # 访问日志格式化器（包含请求信息）
            "access": {
                "format": LOG_FORMAT,
                "datefmt": DATE_FORMAT,
            },
        },
        "handlers": {
            # 默认处理器：输出到控制台
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
            # 访问日志处理器：输出到控制台
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            # Uvicorn 主日志
            "uvicorn": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False,
            },
            # Uvicorn 错误日志
            "uvicorn.error": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False,
            },
            # Uvicorn 访问日志
            "uvicorn.access": {
                "handlers": ["access"],
                "level": "INFO",
                "propagate": False,
            },
        },
    }
