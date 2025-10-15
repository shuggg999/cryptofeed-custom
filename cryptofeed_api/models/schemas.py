"""
API响应模型定义
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class TradeResponse(BaseModel):
    """交易数据响应模型"""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    symbol: str
    side: str = Field(..., description="买卖方向: buy/sell")
    amount: Decimal = Field(..., description="交易数量")
    price: Decimal = Field(..., description="交易价格")
    exchange: str = Field(default="binance", description="交易所")
    trade_id: Optional[str] = Field(None, description="交易ID")


class CandleResponse(BaseModel):
    """K线数据响应模型"""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    symbol: str
    interval: str = Field(..., description="时间间隔: 1m, 5m, 1h, 1d等")
    open_price: Decimal = Field(..., description="开盘价", alias="open")
    high_price: Decimal = Field(..., description="最高价", alias="high")
    low_price: Decimal = Field(..., description="最低价", alias="low")
    close_price: Decimal = Field(..., description="收盘价", alias="close")
    volume: Decimal = Field(..., description="成交量")
    exchange: str = Field(default="binance", description="交易所")


class FundingResponse(BaseModel):
    """资金费率响应模型"""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    symbol: str
    rate: Decimal = Field(..., description="资金费率")
    next_funding_time: Optional[datetime] = Field(None, description="下次资金费率时间")
    exchange: str = Field(default="binance", description="交易所")


class TickerResponse(BaseModel):
    """行情数据响应模型"""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    symbol: str
    bid: Decimal = Field(..., description="买一价")
    ask: Decimal = Field(..., description="卖一价")
    exchange: str = Field(default="binance", description="交易所")


class HealthResponse(BaseModel):
    """健康检查响应模型"""

    status: str = Field(..., description="服务状态")
    timestamp: datetime = Field(..., description="检查时间")
    version: str = Field(..., description="服务版本")
    database: bool = Field(..., description="数据库连接状态")
    data_collection: bool = Field(..., description="数据采集状态")
    uptime_seconds: int = Field(..., description="运行时间(秒)")


class DataStatsResponse(BaseModel):
    """数据统计响应模型"""

    symbol: str
    latest_trade: Optional[datetime] = Field(None, description="最新交易时间")
    latest_candle: Optional[datetime] = Field(None, description="最新K线时间")
    latest_funding: Optional[datetime] = Field(None, description="最新资金费率时间")
    trade_count_24h: int = Field(0, description="24小时交易数量")
    data_gaps: int = Field(0, description="数据缺口数量")


class DataGapResponse(BaseModel):
    """数据缺口响应模型"""

    model_config = ConfigDict(from_attributes=True)

    symbol: str
    data_type: str = Field(..., description="数据类型: trades, candles, funding")
    interval: Optional[str] = Field(None, description="时间间隔(仅K线数据)")
    gap_start: datetime = Field(..., description="缺口开始时间")
    gap_end: datetime = Field(..., description="缺口结束时间")
    status: str = Field(..., description="状态: pending, filled, failed")
    exchange: str = Field(default="binance", description="交易所")
    created_at: datetime = Field(..., description="创建时间")


class APIResponse(BaseModel):
    """通用API响应包装器"""

    success: bool = Field(True, description="请求是否成功")
    message: str = Field("OK", description="响应消息")
    data: Optional[
        Union[
            List[TradeResponse],
            List[CandleResponse],
            List[FundingResponse],
            List[TickerResponse],
            HealthResponse,
            DataStatsResponse,
            List[DataGapResponse],
        ]
    ] = Field(None, description="响应数据")
    count: Optional[int] = Field(None, description="数据数量")


class ErrorResponse(BaseModel):
    """错误响应模型"""

    success: bool = Field(False, description="请求是否成功")
    message: str = Field(..., description="错误消息")
    error_code: Optional[str] = Field(None, description="错误码")
    details: Optional[dict] = Field(None, description="错误详情")


# Query参数模型
class TimeRangeQuery(BaseModel):
    """时间范围查询参数"""

    start_time: Optional[datetime] = Field(None, description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")
    limit: int = Field(100, ge=1, le=1000, description="返回数量限制")


class CandleQuery(TimeRangeQuery):
    """K线查询参数"""

    interval: str = Field("1m", description="时间间隔: 1m, 5m, 15m, 1h, 4h, 1d")


class SymbolListQuery(BaseModel):
    """多symbol查询参数"""

    symbols: List[str] = Field(..., description="交易对列表")
    exchange: str = Field("binance", description="交易所")
