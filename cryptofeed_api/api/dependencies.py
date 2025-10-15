"""
FastAPI依赖注入
"""

import logging
from typing import Optional

from fastapi import Depends, HTTPException, Query

from ..core.config import config_manager

logger = logging.getLogger(__name__)


def validate_symbol(symbol: str = Query(..., description="交易对符号，如 BTC-USDT-PERP")) -> str:
    """验证交易对符号格式"""
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol is required")

    # 简单的符号格式验证
    if not symbol.replace("-", "").replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid symbol format")

    return symbol.upper()


def validate_exchange(exchange: str = Query("binance", description="交易所名称")) -> str:
    """验证交易所名称"""
    supported_exchanges = ["binance", "bybit", "okx", "coinbase"]

    if exchange.lower() not in supported_exchanges:
        raise HTTPException(status_code=400, detail=f"Unsupported exchange. Supported: {supported_exchanges}")

    return exchange.lower()


def validate_interval(interval: str = Query("1m", description="时间间隔")) -> str:
    """验证时间间隔格式"""
    valid_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]

    if interval not in valid_intervals:
        raise HTTPException(status_code=400, detail=f"Invalid interval. Valid intervals: {valid_intervals}")

    return interval


def validate_limit(limit: int = Query(100, ge=1, le=1000, description="返回数据数量限制")) -> int:
    """验证数量限制"""
    return limit


def get_pagination(
    page: int = Query(1, ge=1, description="页码"), size: int = Query(100, ge=1, le=1000, description="每页数量")
) -> dict:
    """分页参数"""
    offset = (page - 1) * size
    return {"offset": offset, "limit": size, "page": page, "size": size}


def get_auth_header(authorization: Optional[str] = None) -> Optional[str]:
    """获取认证头（预留给未来的API认证）"""
    # 目前返回None，未来可以实现JWT或API Key认证
    return authorization


class CommonQueryParams:
    """通用查询参数类"""

    def __init__(
        self,
        symbol: str = Depends(validate_symbol),
        exchange: str = Depends(validate_exchange),
        limit: int = Depends(validate_limit),
    ):
        self.symbol = symbol
        self.exchange = exchange
        self.limit = limit


class TimeRangeParams:
    """时间范围查询参数类"""

    def __init__(
        self,
        start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
        end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
        limit: int = Depends(validate_limit),
    ):
        from datetime import datetime

        # 验证并解析时间格式
        if start_time:
            try:
                self.start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_time format. Use ISO format.")
        else:
            self.start_time = None

        if end_time:
            try:
                self.end_time = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_time format. Use ISO format.")
        else:
            self.end_time = None

        # 验证时间范围逻辑
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise HTTPException(status_code=400, detail="start_time must be before end_time")

        self.limit = limit


class CandleQueryParams(TimeRangeParams):
    """K线数据查询参数类"""

    def __init__(
        self,
        symbol: str = Depends(validate_symbol),
        exchange: str = Depends(validate_exchange),
        interval: str = Depends(validate_interval),
        start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
        end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
        limit: int = Depends(validate_limit),
    ):
        super().__init__(start_time, end_time, limit)
        self.symbol = symbol
        self.exchange = exchange
        self.interval = interval


# 日志记录依赖
def get_request_logger():
    """获取请求日志记录器"""
    return logger


# 配置管理依赖
def get_config():
    """获取配置管理器"""
    return config_manager


# API版本验证
def validate_api_version(version: str = "v1") -> str:
    """验证API版本"""
    if version not in ["v1"]:
        raise HTTPException(status_code=400, detail="Unsupported API version")
    return version
