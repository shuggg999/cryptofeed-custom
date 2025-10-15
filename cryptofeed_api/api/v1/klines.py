"""
FreqTrade兼容的K线数据API端点
提供符合freqtrade本地数据源格式的API接口
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ...core.clickhouse import ClickHouseService, get_clickhouse

router = APIRouter(prefix="/klines", tags=["KLines"])
logger = logging.getLogger(__name__)


def convert_symbol_format(symbol: str) -> str:
    """
    转换符号格式：BTCUSDT -> BTC-USDT-PERP
    支持的格式：
    - BTCUSDT -> BTC-USDT-PERP
    - ETHUSDT -> ETH-USDT-PERP
    - SOLUSDT -> SOL-USDT-PERP
    - DOGEUSDT -> DOGE-USDT-PERP
    - ADAUSDT -> ADA-USDT-PERP
    """
    symbol_map = {
        "BTCUSDT": "BTC-USDT-PERP",
        "ETHUSDT": "ETH-USDT-PERP",
        "SOLUSDT": "SOL-USDT-PERP",
        "DOGEUSDT": "DOGE-USDT-PERP",
        "ADAUSDT": "ADA-USDT-PERP",
    }

    # 尝试直接映射
    if symbol.upper() in symbol_map:
        return symbol_map[symbol.upper()]

    # 尝试智能转换（如果输入是 BTC/USDT 格式）
    if "/" in symbol:
        base, quote = symbol.upper().split("/")
        if quote == "USDT":
            return f"{base}-USDT-PERP"

    # 如果已经是正确格式，直接返回
    if "-PERP" in symbol.upper():
        return symbol.upper()

    # 默认尝试添加后缀
    if symbol.upper().endswith("USDT"):
        base = symbol.upper()[:-4]
        return f"{base}-USDT-PERP"

    raise ValueError(f"Unsupported symbol format: {symbol}")


@router.get("/")
async def get_klines(
    symbol: str = Query(..., description="交易对，如 BTCUSDT"),
    interval: str = Query(..., description="时间周期: 1m, 5m, 15m, 30m, 1h, 4h, 1d"),
    limit: int = Query(1000, ge=1, le=5000, description="数据条数，最大5000"),
    startTime: Optional[int] = Query(None, description="开始时间戳（毫秒）"),
    endTime: Optional[int] = Query(None, description="结束时间戳（毫秒）"),
    ch: ClickHouseService = Depends(get_clickhouse),
):
    """
    获取K线数据 - FreqTrade兼容格式

    返回格式:
    ```json
    {
        "data": [
            [timestamp, open, high, low, close, volume],
            ...
        ]
    }
    ```

    每条数据包含6个值:
    1. timestamp: 时间戳（毫秒）
    2. open: 开盘价
    3. high: 最高价
    4. low: 最低价
    5. close: 收盘价
    6. volume: 成交量
    """
    try:
        logger.info(f"Klines endpoint called with symbol={symbol}, interval={interval}, limit={limit}")
        logger.info(f"ClickHouse service type: {type(ch)}, ch is None: {ch is None}")
        if ch:
            logger.info(f"ClickHouse client type: {type(ch.client)}, client is None: {ch.client is None}")

        # 转换符号格式
        try:
            internal_symbol = convert_symbol_format(symbol)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # 转换时间戳（从毫秒到秒）
        start_timestamp = None
        end_timestamp = None
        if startTime:
            start_timestamp = datetime.fromtimestamp(startTime / 1000)
        if endTime:
            end_timestamp = datetime.fromtimestamp(endTime / 1000)

        # 如果没有指定时间范围，使用默认值
        if not end_timestamp:
            end_timestamp = datetime.now()
        if not start_timestamp:
            # 根据interval和limit计算开始时间
            interval_minutes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
            minutes = interval_minutes.get(interval, 60)
            from datetime import timedelta

            start_timestamp = end_timestamp - timedelta(minutes=minutes * limit)

        # 构建查询
        query = f"""
        SELECT
            toUnixTimestamp64Milli(timestamp) as ts,
            open,
            high,
            low,
            close,
            volume
        FROM candles
        WHERE symbol = %(symbol)s
          AND interval = %(interval)s
          AND timestamp >= %(start_time)s
          AND timestamp <= %(end_time)s
        ORDER BY timestamp ASC
        LIMIT %(limit)s
        """

        params = {
            "symbol": internal_symbol,
            "interval": interval,
            "start_time": start_timestamp,
            "end_time": end_timestamp,
            "limit": limit,
        }

        # 执行查询
        logger.info(f"Executing ClickHouse query for symbol {internal_symbol}")
        result = ch.query(query, params)
        logger.info(f"Query result type: {type(result)}, length: {len(result) if result else 'None'}")

        # 转换结果格式
        data = []
        if result:
            for row in result:
                # 确保所有值都是正确的格式
                data.append(
                    [
                        int(row["ts"]),  # timestamp (毫秒)
                        float(row["open"]),  # open
                        float(row["high"]),  # high
                        float(row["low"]),  # low
                        float(row["close"]),  # close
                        float(row["volume"]),  # volume
                    ]
                )

        logger.info(f"Fetched {len(data)} klines for {symbol} ({internal_symbol})")

        return {"data": data}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching klines: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/test")
async def test_klines():
    """测试端点，返回模拟数据"""
    import time

    current_time = int(time.time() * 1000)

    # 返回模拟的K线数据
    test_data = []
    for i in range(10):
        timestamp = current_time - (i * 60000)  # 每分钟一条
        test_data.append(
            [
                timestamp,
                50000.0 + i * 100,  # open
                50100.0 + i * 100,  # high
                49900.0 + i * 100,  # low
                50050.0 + i * 100,  # close
                1000.5 + i * 10,  # volume
            ]
        )

    return {"data": test_data}
