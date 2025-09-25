"""
资金费率API端点 - 使用ClickHouse
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query

from ...models.schemas import FundingResponse, APIResponse, ErrorResponse
from ...core.clickhouse import get_clickhouse, ClickHouseService
from ..dependencies import (
    CommonQueryParams, TimeRangeParams, validate_symbol,
    validate_exchange, get_pagination
)

router = APIRouter(prefix="/funding", tags=["Funding"])
logger = logging.getLogger(__name__)


@router.get("/", response_model=APIResponse)
async def get_funding(
    common_params: CommonQueryParams = Depends(),
    time_params: TimeRangeParams = Depends(),
    pagination: Dict = Depends(get_pagination),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取资金费率数据

    支持时间范围查询、分页等功能
    """
    try:
        # 查询数据
        funding_data = ch.get_funding(
            symbol=common_params.symbol,
            start_time=time_params.start_time,
            end_time=time_params.end_time,
            limit=pagination["limit"],
            offset=pagination["offset"]
        )

        # 转换为响应格式
        funding_rates = []
        for row in funding_data:
            funding = FundingResponse(
                timestamp=row['timestamp'],
                symbol=row['symbol'],
                rate=float(row['rate']),
                next_funding_time=row.get('next_funding_time'),
                exchange="BINANCE"
            )
            funding_rates.append(funding)

        # 获取总数（用于分页）
        total_count = ch.get_funding_count(
            symbol=common_params.symbol,
            start_time=time_params.start_time,
            end_time=time_params.end_time
        )

        return APIResponse(
            success=True,
            message=f"Retrieved {len(funding_rates)} funding rates",
            data=funding_rates,
            count=total_count
        )

    except Exception as e:
        logger.error(f"Error getting funding: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve funding rates")


@router.get("/latest", response_model=APIResponse)
async def get_latest_funding(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    limit: int = Query(100, ge=1, le=1000, description="返回数量"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取最新资金费率数据
    """
    try:
        funding_data = ch.get_funding(
            symbol=symbol,
            limit=limit,
            offset=0
        )

        funding_rates = []
        for row in funding_data:
            funding = FundingResponse(
                timestamp=row['timestamp'],
                symbol=row['symbol'],
                rate=float(row['rate']),
                next_funding_time=row.get('next_funding_time'),
                exchange="BINANCE"
            )
            funding_rates.append(funding)

        return APIResponse(
            success=True,
            message=f"Retrieved {len(funding_rates)} latest funding rates",
            data=funding_rates,
            count=len(funding_rates)
        )

    except Exception as e:
        logger.error(f"Error getting latest funding: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve latest funding rates")


@router.get("/current", response_model=APIResponse)
async def get_current_funding(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取当前资金费率
    """
    try:
        # 查询最新的资金费率
        sql = """
            SELECT
                timestamp,
                symbol,
                rate,
                next_funding_time
            FROM funding
            WHERE symbol = {symbol:String}
            ORDER BY timestamp DESC
            LIMIT 1
        """

        result = ch.query(sql, {"symbol": symbol})

        if not result:
            return APIResponse(
                success=True,
                message="No funding data found",
                data=None
            )

        row = result[0]
        funding = FundingResponse(
            timestamp=row['timestamp'],
            symbol=row['symbol'],
            rate=float(row['rate']),
            next_funding_time=row.get('next_funding_time'),
            exchange="BINANCE"
        )

        return APIResponse(
            success=True,
            message="Retrieved current funding rate",
            data=funding
        )

    except Exception as e:
        logger.error(f"Error getting current funding: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve current funding rate")


@router.get("/stats", response_model=APIResponse)
async def get_funding_stats(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    days: int = Query(30, ge=1, le=365, description="统计天数"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取资金费率统计信息
    """
    try:
        # 计算时间范围
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)

        # 查询统计数据
        sql = """
            SELECT
                COUNT(*) as total_records,
                AVG(rate) as avg_rate,
                MIN(rate) as min_rate,
                MAX(rate) as max_rate,
                quantile(0.5)(rate) as median_rate,
                quantile(0.25)(rate) as q25_rate,
                quantile(0.75)(rate) as q75_rate,
                countIf(rate > 0) as positive_count,
                countIf(rate < 0) as negative_count,
                countIf(rate = 0) as zero_count
            FROM funding
            WHERE symbol = {symbol:String}
              AND timestamp >= {start_time:DateTime}
              AND timestamp <= {end_time:DateTime}
        """

        result = ch.query(sql, {
            "symbol": symbol,
            "start_time": start_time,
            "end_time": end_time
        })

        if not result or result[0]['total_records'] == 0:
            return APIResponse(
                success=True,
                message="No funding data found",
                data={"symbol": symbol, "total_records": 0}
            )

        stats = result[0]
        stats_data = {
            "symbol": symbol,
            "exchange": "BINANCE",
            "time_range_days": days,
            "total_records": stats['total_records'],
            "rate_statistics": {
                "avg": float(stats['avg_rate']) if stats['avg_rate'] else 0,
                "min": float(stats['min_rate']) if stats['min_rate'] else 0,
                "max": float(stats['max_rate']) if stats['max_rate'] else 0,
                "median": float(stats['median_rate']) if stats['median_rate'] else 0,
                "q25": float(stats['q25_rate']) if stats['q25_rate'] else 0,
                "q75": float(stats['q75_rate']) if stats['q75_rate'] else 0
            },
            "distribution": {
                "positive_count": stats['positive_count'],
                "negative_count": stats['negative_count'],
                "zero_count": stats['zero_count']
            }
        }

        return APIResponse(
            success=True,
            message="Retrieved funding statistics",
            data=stats_data
        )

    except Exception as e:
        logger.error(f"Error getting funding stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve funding statistics")


@router.get("/historical", response_model=APIResponse)
async def get_historical_funding(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    days: int = Query(30, ge=1, le=365, description="历史天数"),
    interval_hours: int = Query(8, ge=1, le=24, description="时间间隔(小时)"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取历史资金费率，按指定间隔聚合
    """
    try:
        # 计算时间范围
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)

        # 查询历史数据，按时间间隔聚合
        sql = f"""
            SELECT
                toStartOfInterval(timestamp, INTERVAL {interval_hours} HOUR) as interval_start,
                AVG(rate) as avg_rate,
                MIN(rate) as min_rate,
                MAX(rate) as max_rate,
                COUNT(*) as record_count
            FROM funding
            WHERE symbol = {{symbol:String}}
              AND timestamp >= {{start_time:DateTime}}
              AND timestamp <= {{end_time:DateTime}}
            GROUP BY interval_start
            ORDER BY interval_start ASC
            LIMIT 1000
        """

        result = ch.query(sql, {
            "symbol": symbol,
            "start_time": start_time,
            "end_time": end_time
        })

        # 转换结果
        historical_data = []
        for row in result:
            historical_data.append({
                "timestamp": row['interval_start'].isoformat(),
                "avg_rate": float(row['avg_rate']) if row['avg_rate'] else 0,
                "min_rate": float(row['min_rate']) if row['min_rate'] else 0,
                "max_rate": float(row['max_rate']) if row['max_rate'] else 0,
                "record_count": row['record_count']
            })

        return APIResponse(
            success=True,
            message=f"Retrieved {len(historical_data)} historical funding periods",
            data=historical_data,
            count=len(historical_data)
        )

    except Exception as e:
        logger.error(f"Error getting historical funding: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve historical funding data")