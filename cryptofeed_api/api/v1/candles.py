"""
K线数据API端点 - 使用ClickHouse
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query

from ...models.schemas import CandleResponse, APIResponse, ErrorResponse
from ...core.clickhouse import get_clickhouse, ClickHouseService
from ..dependencies import (
    CandleQueryParams, validate_symbol, validate_exchange,
    validate_interval, get_pagination
)

router = APIRouter(prefix="/candles", tags=["Candles"])
logger = logging.getLogger(__name__)


@router.get("/", response_model=APIResponse)
async def get_candles(
    query_params: CandleQueryParams = Depends(),
    pagination: Dict = Depends(get_pagination),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取K线数据

    支持时间范围查询、分页等功能
    """
    try:
        # 查询数据
        candles_data = ch.get_candles(
            symbol=query_params.symbol,
            interval=query_params.interval,
            start_time=query_params.start_time,
            end_time=query_params.end_time,
            limit=pagination["limit"],
            offset=pagination["offset"]
        )

        # 转换为响应格式
        candles = []
        for row in candles_data:
            candle = CandleResponse(
                timestamp=row['timestamp'],
                symbol=row['symbol'],
                interval=row['interval'],
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=float(row['volume']),
                exchange="BINANCE"  # ClickHouse表中没有exchange字段，默认使用BINANCE
            )
            candles.append(candle)

        # 获取总数（用于分页）
        total_count = ch.get_candles_count(
            symbol=query_params.symbol,
            interval=query_params.interval,
            start_time=query_params.start_time,
            end_time=query_params.end_time
        )

        return APIResponse(
            success=True,
            message=f"Retrieved {len(candles)} candles",
            data=candles,
            count=total_count
        )

    except Exception as e:
        logger.error(f"Error getting candles: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve candles")


@router.get("/latest", response_model=APIResponse)
async def get_latest_candles(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    interval: str = Depends(validate_interval),
    limit: int = Query(100, ge=1, le=1000, description="返回数量"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取最新K线数据
    """
    try:
        candles_data = ch.get_candles(
            symbol=symbol,
            interval=interval,
            limit=limit,
            offset=0
        )

        candles = []
        for row in candles_data:
            candle = CandleResponse(
                timestamp=row['timestamp'],
                symbol=row['symbol'],
                interval=row['interval'],
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=float(row['volume']),
                exchange="BINANCE"
            )
            candles.append(candle)

        return APIResponse(
            success=True,
            message=f"Retrieved {len(candles)} latest candles",
            data=candles,
            count=len(candles)
        )

    except Exception as e:
        logger.error(f"Error getting latest candles: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve latest candles")


@router.get("/ohlcv", response_model=APIResponse)
async def get_ohlcv_data(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    interval: str = Depends(validate_interval),
    start_time: str = Query(None, description="开始时间 ISO格式"),
    end_time: str = Query(None, description="结束时间 ISO格式"),
    limit: int = Query(1000, ge=1, le=5000, description="返回数量"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取OHLCV格式的K线数据（兼容TradingView等图表库）

    返回格式: [timestamp, open, high, low, close, volume]
    """
    try:
        # 解析时间参数
        start_dt = None
        end_dt = None

        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_time format")

        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_time format")

        # 查询数据并按时间正序排列
        sql = """
            SELECT
                toUnixTimestamp(timestamp) * 1000 as timestamp_ms,
                toFloat64(open) as open,
                toFloat64(high) as high,
                toFloat64(low) as low,
                toFloat64(close) as close,
                toFloat64(volume) as volume
            FROM candles
            WHERE symbol = {symbol:String} AND interval = {interval:String}
        """

        params = {"symbol": symbol, "interval": interval}

        if start_dt:
            sql += " AND timestamp >= {start_time:DateTime}"
            params["start_time"] = start_dt

        if end_dt:
            sql += " AND timestamp <= {end_time:DateTime}"
            params["end_time"] = end_dt

        sql += f" ORDER BY timestamp ASC LIMIT {limit}"

        result = ch.query(sql, params)

        # 转换为OHLCV数组格式
        ohlcv_data = []
        for row in result:
            ohlcv_data.append([
                int(row['timestamp_ms']),  # timestamp (milliseconds)
                row['open'],               # open
                row['high'],               # high
                row['low'],                # low
                row['close'],              # close
                row['volume']              # volume
            ])

        return APIResponse(
            success=True,
            message=f"Retrieved {len(ohlcv_data)} OHLCV records",
            data=ohlcv_data,
            count=len(ohlcv_data)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting OHLCV data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve OHLCV data")


@router.get("/stats", response_model=APIResponse)
async def get_candles_stats(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    interval: str = Depends(validate_interval),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取K线数据统计信息
    """
    try:
        stats = ch.get_candles_stats(symbol, interval)

        if not stats or stats.get('total_count', 0) == 0:
            return APIResponse(
                success=True,
                message="No candle data found",
                data={
                    "symbol": symbol,
                    "interval": interval,
                    "total_count": 0
                }
            )

        stats_data = {
            "symbol": symbol,
            "exchange": "BINANCE",
            "interval": interval,
            "total_count": stats['total_count'],
            "earliest_time": stats['earliest_time'].isoformat() if stats.get('earliest_time') else None,
            "latest_time": stats['latest_time'].isoformat() if stats.get('latest_time') else None,
            "price_range": {
                "min": float(stats['min_price']) if stats.get('min_price') else None,
                "max": float(stats['max_price']) if stats.get('max_price') else None
            },
            "total_volume": float(stats['total_volume']) if stats.get('total_volume') else 0
        }

        return APIResponse(
            success=True,
            message="Retrieved candles statistics",
            data=stats_data
        )

    except Exception as e:
        logger.error(f"Error getting candles stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve candles statistics")


@router.get("/intervals", response_model=APIResponse)
async def get_available_intervals(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取某个交易对可用的时间间隔
    """
    try:
        intervals_data = ch.get_available_intervals(symbol)

        # 按常见顺序排序
        interval_order = {'1m': 1, '5m': 2, '15m': 3, '30m': 4, '1h': 5, '4h': 6, '1d': 7}
        intervals_data.sort(key=lambda x: interval_order.get(x['interval'], 999))

        return APIResponse(
            success=True,
            message=f"Found {len(intervals_data)} available intervals",
            data=intervals_data
        )

    except Exception as e:
        logger.error(f"Error getting available intervals: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve available intervals")