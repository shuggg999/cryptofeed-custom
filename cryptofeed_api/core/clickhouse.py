#!/usr/bin/env python3
"""
ClickHouse连接和查询服务
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import clickhouse_connect
from clickhouse_connect.driver import Client

from .config import settings

logger = logging.getLogger(__name__)


class ClickHouseService:
    """ClickHouse服务类"""

    def __init__(self):
        self.client: Optional[Client] = None
        self._connection_params = {
            'host': settings.clickhouse_host,
            'port': settings.clickhouse_port,
            'username': settings.clickhouse_user,
            'password': settings.clickhouse_password,
            'database': settings.clickhouse_database,
        }

    def connect(self) -> Client:
        """建立ClickHouse连接"""
        try:
            if self.client is None:
                self.client = clickhouse_connect.get_client(**self._connection_params)
                logger.info(f"Connected to ClickHouse at {settings.clickhouse_host}:{settings.clickhouse_port}")
            return self.client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def disconnect(self):
        """断开ClickHouse连接"""
        if self.client:
            try:
                self.client.close()
                self.client = None
                logger.info("Disconnected from ClickHouse")
            except Exception as e:
                logger.error(f"Error disconnecting from ClickHouse: {e}")

    def query(self, sql: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """执行查询并返回结果"""
        try:
            client = self.connect()
            result = client.query(sql, parameters or {})

            # 将结果转换为字典列表
            if result.result_rows:
                columns = result.column_names
                return [dict(zip(columns, row)) for row in result.result_rows]
            return []
        except Exception as e:
            logger.error(f"Query failed: {sql} - Error: {e}")
            raise

    def query_df(self, sql: str, parameters: Dict[str, Any] = None):
        """执行查询并返回pandas DataFrame (如果需要)"""
        try:
            client = self.connect()
            return client.query_df(sql, parameters or {})
        except Exception as e:
            logger.error(f"DataFrame query failed: {sql} - Error: {e}")
            raise

    # Candles 相关查询方法
    def get_candles(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """查询K线数据"""
        where_conditions = ["symbol = {symbol:String}", "interval = {interval:String}"]
        params = {"symbol": symbol, "interval": interval, "limit": limit, "offset": offset}

        if start_time:
            where_conditions.append("timestamp >= {start_time:DateTime}")
            params["start_time"] = start_time

        if end_time:
            where_conditions.append("timestamp <= {end_time:DateTime}")
            params["end_time"] = end_time

        where_clause = " AND ".join(where_conditions)

        sql = f"""
            SELECT
                timestamp,
                symbol,
                interval,
                open,
                high,
                low,
                close,
                volume
            FROM candles
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT {limit}
            OFFSET {offset}
        """

        return self.query(sql, params)

    def get_candles_count(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> int:
        """查询K线数据总数"""
        where_conditions = ["symbol = {symbol:String}", "interval = {interval:String}"]
        params = {"symbol": symbol, "interval": interval}

        if start_time:
            where_conditions.append("timestamp >= {start_time:DateTime}")
            params["start_time"] = start_time

        if end_time:
            where_conditions.append("timestamp <= {end_time:DateTime}")
            params["end_time"] = end_time

        where_clause = " AND ".join(where_conditions)

        sql = f"SELECT COUNT(*) as count FROM candles WHERE {where_clause}"
        result = self.query(sql, params)
        return result[0]['count'] if result else 0

    def get_candles_stats(self, symbol: str, interval: str) -> Dict[str, Any]:
        """查询K线统计信息"""
        sql = """
            SELECT
                COUNT(*) as total_count,
                MIN(timestamp) as earliest_time,
                MAX(timestamp) as latest_time,
                MIN(low) as min_price,
                MAX(high) as max_price,
                SUM(volume) as total_volume
            FROM candles
            WHERE symbol = {symbol:String} AND interval = {interval:String}
        """

        result = self.query(sql, {"symbol": symbol, "interval": interval})
        return result[0] if result else {}

    def get_available_intervals(self, symbol: str) -> List[Dict[str, Any]]:
        """查询可用的时间间隔"""
        sql = """
            SELECT interval, COUNT(*) as count
            FROM candles
            WHERE symbol = {symbol:String}
            GROUP BY interval
            ORDER BY interval
        """

        return self.query(sql, {"symbol": symbol})

    # Trades 相关查询方法
    def get_trades(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """查询交易数据"""
        where_conditions = ["symbol = {symbol:String}"]
        params = {"symbol": symbol, "limit": limit, "offset": offset}

        if start_time:
            where_conditions.append("timestamp >= {start_time:DateTime}")
            params["start_time"] = start_time

        if end_time:
            where_conditions.append("timestamp <= {end_time:DateTime}")
            params["end_time"] = end_time

        where_clause = " AND ".join(where_conditions)

        sql = f"""
            SELECT
                timestamp,
                symbol,
                side,
                amount,
                price,
                trade_id
            FROM trades
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT {limit}
            OFFSET {offset}
        """

        return self.query(sql, params)

    def get_trades_count(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> int:
        """查询交易数据总数"""
        where_conditions = ["symbol = {symbol:String}"]
        params = {"symbol": symbol}

        if start_time:
            where_conditions.append("timestamp >= {start_time:DateTime}")
            params["start_time"] = start_time

        if end_time:
            where_conditions.append("timestamp <= {end_time:DateTime}")
            params["end_time"] = end_time

        where_clause = " AND ".join(where_conditions)

        sql = f"SELECT COUNT(*) as count FROM trades WHERE {where_clause}"
        result = self.query(sql, params)
        return result[0]['count'] if result else 0

    # Funding 相关查询方法
    def get_funding(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """查询资金费率数据"""
        where_conditions = ["symbol = {symbol:String}"]
        params = {"symbol": symbol, "limit": limit, "offset": offset}

        if start_time:
            where_conditions.append("timestamp >= {start_time:DateTime}")
            params["start_time"] = start_time

        if end_time:
            where_conditions.append("timestamp <= {end_time:DateTime}")
            params["end_time"] = end_time

        where_clause = " AND ".join(where_conditions)

        sql = f"""
            SELECT
                timestamp,
                symbol,
                rate,
                next_funding_time
            FROM funding
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT {limit}
            OFFSET {offset}
        """

        return self.query(sql, params)

    def get_funding_count(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> int:
        """查询资金费率数据总数"""
        where_conditions = ["symbol = {symbol:String}"]
        params = {"symbol": symbol}

        if start_time:
            where_conditions.append("timestamp >= {start_time:DateTime}")
            params["start_time"] = start_time

        if end_time:
            where_conditions.append("timestamp <= {end_time:DateTime}")
            params["end_time"] = end_time

        where_clause = " AND ".join(where_conditions)

        sql = f"SELECT COUNT(*) as count FROM funding WHERE {where_clause}"
        result = self.query(sql, params)
        return result[0]['count'] if result else 0


# 全局ClickHouse服务实例
_clickhouse_service = None


def get_clickhouse_service() -> ClickHouseService:
    """获取ClickHouse服务实例"""
    global _clickhouse_service
    if _clickhouse_service is None:
        _clickhouse_service = ClickHouseService()
    return _clickhouse_service


async def get_clickhouse() -> ClickHouseService:
    """FastAPI依赖注入函数"""
    return get_clickhouse_service()