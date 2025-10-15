"""
数据完整性检查服务 - ClickHouse版本
检查数据缺口，识别需要补充的历史数据
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import clickhouse_connect

from cryptofeed_api.monitor.config import config

logger = logging.getLogger(__name__)


@dataclass
class DataGap:
    """数据缺口信息"""

    symbol: str
    data_type: str  # 'trades', 'candles', 'funding'
    interval: Optional[str]  # 仅用于candles
    gap_start: datetime
    gap_end: datetime
    gap_duration_hours: float

    def __post_init__(self):
        """计算缺口持续时间"""
        duration = self.gap_end - self.gap_start
        self.gap_duration_hours = duration.total_seconds() / 3600


@dataclass
class DataStats:
    """数据统计信息"""

    symbol: str
    data_type: str
    interval: Optional[str]
    earliest_time: Optional[datetime]
    latest_time: Optional[datetime]
    total_count: int
    gaps_found: int


class DataIntegrityChecker:
    """数据完整性检查器 - ClickHouse版本"""

    def __init__(self):
        self.supported_intervals = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
        self.expected_intervals = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "30m": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "4h": timedelta(hours=4),
            "1d": timedelta(days=1),
        }

        # ClickHouse连接配置
        self.ch_config = {
            "host": config.get("clickhouse.host", "localhost"),
            "port": config.get("clickhouse.port", 8123),
            "user": config.get("clickhouse.user", "default"),
            "password": config.get("clickhouse.password", "password123"),
            "database": config.get("clickhouse.database", "cryptofeed"),
        }

    def check_candle_gaps(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        max_gap_minutes: int = 30,
    ) -> List[DataGap]:
        """
        检查K线数据缺口 - ClickHouse版本

        Args:
            symbol: 交易对符号
            interval: 时间间隔
            start_time: 检查开始时间
            end_time: 检查结束时间
            max_gap_minutes: 最大允许缺口时间（分钟）

        Returns:
            发现的数据缺口列表
        """
        if interval not in self.expected_intervals:
            raise ValueError(f"Unsupported interval: {interval}")

        try:
            client = clickhouse_connect.get_client(**self.ch_config)

            # 构建查询条件
            where_conditions = ["symbol = {symbol:String}", "interval = {interval:String}"]
            params = {"symbol": symbol, "interval": interval}

            if start_time:
                where_conditions.append("timestamp >= {start_time:DateTime}")
                params["start_time"] = start_time

            if end_time:
                where_conditions.append("timestamp <= {end_time:DateTime}")
                params["end_time"] = end_time

            where_clause = " AND ".join(where_conditions)

            # 查询有序的时间戳列表
            sql = f"""
                SELECT timestamp
                FROM candles
                WHERE {where_clause}
                ORDER BY timestamp
            """

            result = client.query(sql, params)
            timestamps = [row[0] for row in result.result_rows] if result.result_rows else []

            if len(timestamps) < 2:
                logger.warning(f"Not enough data to check gaps for {symbol} {interval}")
                return []

            # 检查连续时间戳之间的间隔
            gaps = []
            expected_delta = self.expected_intervals[interval]
            max_gap_delta = timedelta(minutes=max_gap_minutes)

            for i in range(1, len(timestamps)):
                prev_time = timestamps[i - 1]
                curr_time = timestamps[i]
                gap_duration = curr_time - prev_time

                # 如果间隔超过预期时间+容错时间，认为是缺口
                if gap_duration > expected_delta + max_gap_delta:
                    gap = DataGap(
                        symbol=symbol,
                        data_type="candles",
                        interval=interval,
                        gap_start=prev_time + expected_delta,
                        gap_end=curr_time,
                        gap_duration_hours=0,  # 会在__post_init__中计算
                    )
                    gaps.append(gap)

            logger.info(f"Found {len(gaps)} candle gaps for {symbol} {interval}")
            return gaps

        finally:
            if "client" in locals():
                client.close()

    def check_trade_gaps(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        max_gap_minutes: int = 5,
    ) -> List[DataGap]:
        """
        检查交易数据缺口 - ClickHouse版本

        Args:
            symbol: 交易对符号
            start_time: 检查开始时间
            end_time: 检查结束时间
            max_gap_minutes: 最大允许缺口时间（分钟）

        Returns:
            发现的数据缺口列表
        """
        try:
            client = clickhouse_connect.get_client(**self.ch_config)

            # 查询每分钟的交易数量
            where_conditions = ["symbol = {symbol:String}"]
            params = {"symbol": symbol}

            if start_time:
                where_conditions.append("timestamp >= {start_time:DateTime}")
                params["start_time"] = start_time

            if end_time:
                where_conditions.append("timestamp <= {end_time:DateTime}")
                params["end_time"] = end_time

            where_clause = " AND ".join(where_conditions)

            # 按分钟分组统计交易数量
            sql = f"""
                SELECT
                    toStartOfMinute(timestamp) as minute_bucket,
                    COUNT(*) as trade_count
                FROM trades
                WHERE {where_clause}
                GROUP BY toStartOfMinute(timestamp)
                ORDER BY minute_bucket
            """

            result = client.query(sql, params)
            minute_data = [(row[0], row[1]) for row in result.result_rows] if result.result_rows else []

            if len(minute_data) < 2:
                logger.warning(f"Not enough trade data to check gaps for {symbol}")
                return []

            # 检查连续分钟之间的间隔
            gaps = []
            max_gap_delta = timedelta(minutes=max_gap_minutes)

            for i in range(1, len(minute_data)):
                prev_minute = minute_data[i - 1][0]
                curr_minute = minute_data[i][0]
                gap_duration = curr_minute - prev_minute

                # 如果间隔超过容错时间，认为是缺口
                if gap_duration > max_gap_delta:
                    gap = DataGap(
                        symbol=symbol,
                        data_type="trades",
                        interval=None,
                        gap_start=prev_minute + timedelta(minutes=1),
                        gap_end=curr_minute,
                        gap_duration_hours=0,
                    )
                    gaps.append(gap)

            logger.info(f"Found {len(gaps)} trade gaps for {symbol}")
            return gaps

        finally:
            if "client" in locals():
                client.close()

    def check_funding_gaps(
        self, symbol: str, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> List[DataGap]:
        """
        检查资金费率数据缺口 - ClickHouse版本

        Args:
            symbol: 交易对符号
            start_time: 检查开始时间
            end_time: 检查结束时间

        Returns:
            发现的数据缺口列表
        """
        try:
            client = clickhouse_connect.get_client(**self.ch_config)

            where_conditions = ["symbol = {symbol:String}"]
            params = {"symbol": symbol}

            if start_time:
                where_conditions.append("timestamp >= {start_time:DateTime}")
                params["start_time"] = start_time

            if end_time:
                where_conditions.append("timestamp <= {end_time:DateTime}")
                params["end_time"] = end_time

            where_clause = " AND ".join(where_conditions)

            sql = f"""
                SELECT timestamp
                FROM funding
                WHERE {where_clause}
                ORDER BY timestamp
            """

            result = client.query(sql, params)
            timestamps = [row[0] for row in result.result_rows] if result.result_rows else []

            if len(timestamps) < 2:
                logger.warning(f"Not enough funding data to check gaps for {symbol}")
                return []

            # 资金费率通常每8小时一次
            expected_interval = timedelta(hours=8)
            max_gap_delta = timedelta(hours=12)  # 允许一定延迟

            gaps = []
            for i in range(1, len(timestamps)):
                prev_time = timestamps[i - 1]
                curr_time = timestamps[i]
                gap_duration = curr_time - prev_time

                if gap_duration > expected_interval + max_gap_delta:
                    gap = DataGap(
                        symbol=symbol,
                        data_type="funding",
                        interval=None,
                        gap_start=prev_time + expected_interval,
                        gap_end=curr_time,
                        gap_duration_hours=0,
                    )
                    gaps.append(gap)

            logger.info(f"Found {len(gaps)} funding gaps for {symbol}")
            return gaps

        finally:
            if "client" in locals():
                client.close()

    def get_data_stats(self, symbol: str, data_type: str = "candles", interval: Optional[str] = None) -> DataStats:
        """
        获取数据统计信息 - ClickHouse版本

        Args:
            symbol: 交易对符号
            data_type: 数据类型
            interval: 时间间隔（仅用于candles）

        Returns:
            数据统计信息
        """
        try:
            client = clickhouse_connect.get_client(**self.ch_config)

            if data_type == "candles":
                where_conditions = ["symbol = {symbol:String}"]
                params = {"symbol": symbol}

                if interval:
                    where_conditions.append("interval = {interval:String}")
                    params["interval"] = interval

                where_clause = " AND ".join(where_conditions)

                sql = f"""
                    SELECT
                        MIN(timestamp) as earliest,
                        MAX(timestamp) as latest,
                        COUNT(*) as total_count
                    FROM candles
                    WHERE {where_clause}
                """

            elif data_type == "trades":
                sql = """
                    SELECT
                        MIN(timestamp) as earliest,
                        MAX(timestamp) as latest,
                        COUNT(*) as total_count
                    FROM trades
                    WHERE symbol = {symbol:String}
                """
                params = {"symbol": symbol}

            elif data_type == "funding":
                sql = """
                    SELECT
                        MIN(timestamp) as earliest,
                        MAX(timestamp) as latest,
                        COUNT(*) as total_count
                    FROM funding
                    WHERE symbol = {symbol:String}
                """
                params = {"symbol": symbol}

            else:
                raise ValueError(f"Unsupported data type: {data_type}")

            result = client.query(sql, params)
            row = result.result_rows[0] if result.result_rows else (None, None, 0)

            # ClickHouse版本暂不支持gap日志表，先设为0
            gap_count = 0

            return DataStats(
                symbol=symbol,
                data_type=data_type,
                interval=interval,
                earliest_time=row[0] if row[0] else None,
                latest_time=row[1] if row[1] else None,
                total_count=row[2] if row[2] else 0,
                gaps_found=gap_count,
            )

        finally:
            if "client" in locals():
                client.close()

    def log_data_gaps(self, gaps: List[DataGap]) -> int:
        """
        将发现的数据缺口记录到日志（ClickHouse版本暂时只记录日志）

        Args:
            gaps: 数据缺口列表

        Returns:
            记录的缺口数量
        """
        if not gaps:
            return 0

        # ClickHouse版本暂时只记录到日志，不存储到数据库
        for gap in gaps:
            logger.info(
                f"Data gap detected: {gap.symbol} {gap.data_type} {gap.interval} "
                f"from {gap.gap_start} to {gap.gap_end} ({gap.gap_duration_hours:.2f} hours)"
            )

        logger.info(f"Logged {len(gaps)} data gaps to log")
        return len(gaps)

    def run_integrity_check(
        self,
        symbols: List[str],
        check_candles: bool = True,
        check_trades: bool = True,
        check_funding: bool = True,
        lookback_days: int = None,
    ) -> Dict[str, Dict]:
        """
        运行完整的数据完整性检查 - ClickHouse版本

        Args:
            symbols: 要检查的交易对列表
            check_candles: 是否检查K线数据
            check_trades: 是否检查交易数据
            check_funding: 是否检查资金费率数据
            lookback_days: 回看天数

        Returns:
            检查结果摘要
        """
        # 如果没有指定lookback_days，从配置中读取
        if lookback_days is None:
            lookback_days = config.get("data_integrity.lookback_days", 7)

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=lookback_days)

        results = {}

        for symbol in symbols:
            symbol_results = {"candle_gaps": {}, "trade_gaps": [], "funding_gaps": [], "stats": {}}

            try:
                # 检查K线数据
                if check_candles:
                    for interval in self.supported_intervals:
                        gaps = self.check_candle_gaps(symbol, interval, start_time, end_time)
                        if gaps:
                            self.log_data_gaps(gaps)
                        symbol_results["candle_gaps"][interval] = len(gaps)

                        # 获取统计信息
                        stats = self.get_data_stats(symbol, "candles", interval)
                        symbol_results["stats"][f"candles_{interval}"] = stats

                # 检查交易数据
                if check_trades:
                    trade_gaps = self.check_trade_gaps(symbol, start_time, end_time)
                    if trade_gaps:
                        self.log_data_gaps(trade_gaps)
                    symbol_results["trade_gaps"] = len(trade_gaps)

                    # 获取统计信息
                    trade_stats = self.get_data_stats(symbol, "trades")
                    symbol_results["stats"]["trades"] = trade_stats

                # 检查资金费率数据
                if check_funding:
                    funding_gaps = self.check_funding_gaps(symbol, start_time, end_time)
                    if funding_gaps:
                        self.log_data_gaps(funding_gaps)
                    symbol_results["funding_gaps"] = len(funding_gaps)

                    # 获取统计信息
                    funding_stats = self.get_data_stats(symbol, "funding")
                    symbol_results["stats"]["funding"] = funding_stats

                results[symbol] = symbol_results
                logger.info(f"Completed integrity check for {symbol}")

            except Exception as e:
                logger.error(f"Failed to check integrity for {symbol}: {e}")
                results[symbol] = {"error": str(e)}

        return results
