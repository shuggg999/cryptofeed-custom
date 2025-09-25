"""
交易数据API端点 - 使用ClickHouse
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query

from ...models.schemas import TradeResponse, APIResponse, ErrorResponse
from ...core.clickhouse import get_clickhouse, ClickHouseService
from ..dependencies import (
    CommonQueryParams, TimeRangeParams, validate_symbol,
    validate_exchange, get_pagination
)

router = APIRouter(prefix="/trades", tags=["Trades"])
logger = logging.getLogger(__name__)


@router.get("/", response_model=APIResponse)
async def get_trades(
    common_params: CommonQueryParams = Depends(),
    time_params: TimeRangeParams = Depends(),
    pagination: Dict = Depends(get_pagination),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取交易数据

    支持时间范围查询、分页等功能
    """
    try:
        # 查询数据
        trades_data = ch.get_trades(
            symbol=common_params.symbol,
            start_time=time_params.start_time,
            end_time=time_params.end_time,
            limit=pagination["limit"],
            offset=pagination["offset"]
        )

        # 转换为响应格式
        trades = []
        for row in trades_data:
            trade = TradeResponse(
                timestamp=row['timestamp'],
                symbol=row['symbol'],
                side=row['side'],
                amount=float(row['amount']),
                price=float(row['price']),
                trade_id=row.get('trade_id', ''),
                exchange="BINANCE"
            )
            trades.append(trade)

        # 获取总数（用于分页）
        total_count = ch.get_trades_count(
            symbol=common_params.symbol,
            start_time=time_params.start_time,
            end_time=time_params.end_time
        )

        return APIResponse(
            success=True,
            message=f"Retrieved {len(trades)} trades",
            data=trades,
            count=total_count
        )

    except Exception as e:
        logger.error(f"Error getting trades: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve trades")


@router.get("/latest", response_model=APIResponse)
async def get_latest_trades(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    limit: int = Query(100, ge=1, le=1000, description="返回数量"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取最新交易数据
    """
    try:
        trades_data = ch.get_trades(
            symbol=symbol,
            limit=limit,
            offset=0
        )

        trades = []
        for row in trades_data:
            trade = TradeResponse(
                timestamp=row['timestamp'],
                symbol=row['symbol'],
                side=row['side'],
                amount=float(row['amount']),
                price=float(row['price']),
                trade_id=row.get('trade_id', ''),
                exchange="BINANCE"
            )
            trades.append(trade)

        return APIResponse(
            success=True,
            message=f"Retrieved {len(trades)} latest trades",
            data=trades,
            count=len(trades)
        )

    except Exception as e:
        logger.error(f"Error getting latest trades: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve latest trades")


@router.get("/stats", response_model=APIResponse)
async def get_trades_stats(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    hours: int = Query(24, ge=1, le=168, description="统计时间范围(小时)"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取交易统计信息
    """
    try:
        # 计算时间范围
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        # 查询统计数据
        sql = """
            SELECT
                COUNT(*) as total_trades,
                SUM(amount) as total_volume,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                countIf(side = 'buy') as buy_count,
                countIf(side = 'sell') as sell_count,
                sumIf(amount, side = 'buy') as buy_volume,
                sumIf(amount, side = 'sell') as sell_volume
            FROM trades
            WHERE symbol = {symbol:String}
              AND timestamp >= {start_time:DateTime}
              AND timestamp <= {end_time:DateTime}
        """

        result = ch.query(sql, {
            "symbol": symbol,
            "start_time": start_time,
            "end_time": end_time
        })

        if not result:
            return APIResponse(
                success=True,
                message="No trade data found",
                data={"symbol": symbol, "total_trades": 0}
            )

        stats = result[0]
        stats_data = {
            "symbol": symbol,
            "exchange": "BINANCE",
            "time_range_hours": hours,
            "total_trades": stats['total_trades'],
            "total_volume": float(stats['total_volume']) if stats['total_volume'] else 0,
            "price_stats": {
                "avg": float(stats['avg_price']) if stats['avg_price'] else 0,
                "min": float(stats['min_price']) if stats['min_price'] else 0,
                "max": float(stats['max_price']) if stats['max_price'] else 0
            },
            "side_distribution": {
                "buy_count": stats['buy_count'],
                "sell_count": stats['sell_count'],
                "buy_volume": float(stats['buy_volume']) if stats['buy_volume'] else 0,
                "sell_volume": float(stats['sell_volume']) if stats['sell_volume'] else 0
            }
        }

        return APIResponse(
            success=True,
            message="Retrieved trades statistics",
            data=stats_data
        )

    except Exception as e:
        logger.error(f"Error getting trades stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve trades statistics")


@router.get("/volume", response_model=APIResponse)
async def get_volume_by_intervals(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    interval_minutes: int = Query(60, ge=1, le=1440, description="时间间隔(分钟)"),
    hours: int = Query(24, ge=1, le=168, description="统计时间范围(小时)"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    按时间间隔获取交易量统计
    """
    try:
        # 计算时间范围
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        # 查询按时间间隔聚合的交易量
        sql = f"""
            SELECT
                toStartOfInterval(timestamp, INTERVAL {interval_minutes} MINUTE) as interval_start,
                COUNT(*) as trade_count,
                SUM(amount) as volume,
                AVG(price) as avg_price
            FROM trades
            WHERE symbol = {{symbol:String}}
              AND timestamp >= {{start_time:DateTime}}
              AND timestamp <= {{end_time:DateTime}}
            GROUP BY interval_start
            ORDER BY interval_start DESC
            LIMIT 100
        """

        result = ch.query(sql, {
            "symbol": symbol,
            "start_time": start_time,
            "end_time": end_time
        })

        # 转换结果
        volume_data = []
        for row in result:
            volume_data.append({
                "timestamp": row['interval_start'].isoformat(),
                "trade_count": row['trade_count'],
                "volume": float(row['volume']),
                "avg_price": float(row['avg_price']) if row['avg_price'] else 0
            })

        return APIResponse(
            success=True,
            message=f"Retrieved {len(volume_data)} volume intervals",
            data=volume_data,
            count=len(volume_data)
        )

    except Exception as e:
        logger.error(f"Error getting volume data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve volume data")


@router.get("/large", response_model=APIResponse)
async def get_large_trades(
    symbol: str = Depends(validate_symbol),
    exchange: str = Depends(validate_exchange),
    min_amount: float = Query(1000.0, ge=0.01, description="最小交易量阈值"),
    hours: int = Query(24, ge=1, le=168, description="时间范围(小时)"),
    limit: int = Query(100, ge=1, le=500, description="返回数量"),
    ch: ClickHouseService = Depends(get_clickhouse)
) -> APIResponse:
    """
    获取大额交易数据
    """
    try:
        # 计算时间范围
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        # 查询大额交易
        sql = f"""
            SELECT
                timestamp,
                symbol,
                side,
                amount,
                price,
                trade_id,
                amount * price as notional_value
            FROM trades
            WHERE symbol = {{symbol:String}}
              AND timestamp >= {{start_time:DateTime}}
              AND timestamp <= {{end_time:DateTime}}
              AND (amount * price) >= {{min_amount:Float64}}
            ORDER BY notional_value DESC
            LIMIT {limit}
        """

        result = ch.query(sql, {
            "symbol": symbol,
            "start_time": start_time,
            "end_time": end_time,
            "min_amount": min_amount
        })

        # 转换为响应格式
        large_trades = []
        for row in result:
            trade = {
                "timestamp": row['timestamp'].isoformat(),
                "symbol": row['symbol'],
                "side": row['side'],
                "amount": float(row['amount']),
                "price": float(row['price']),
                "trade_id": row.get('trade_id', ''),
                "notional_value": float(row['notional_value']),
                "exchange": "BINANCE"
            }
            large_trades.append(trade)

        return APIResponse(
            success=True,
            message=f"Retrieved {len(large_trades)} large trades",
            data=large_trades,
            count=len(large_trades)
        )

    except Exception as e:
        logger.error(f"Error getting large trades: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve large trades")