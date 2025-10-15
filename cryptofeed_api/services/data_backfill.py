"""
历史数据补充服务 - ClickHouse版本
自动补充ClickHouse数据库中缺失的历史数据
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import clickhouse_connect
import requests

from cryptofeed_api.monitor.config import config
from cryptofeed_api.services.data_normalizer import normalize_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class BackfillTask:
    """数据补充任务"""

    gap_log_id: int
    symbol: str
    data_type: str  # 'candles', 'trades', 'funding'
    interval: Optional[str]  # 仅用于candles
    start_time: datetime
    end_time: datetime
    status: str = "pending"
    error_message: Optional[str] = None
    records_filled: int = 0


@dataclass
class BackfillResult:
    """数据补充结果"""

    task: BackfillTask
    success: bool
    records_added: int
    error_message: Optional[str] = None
    duration_seconds: float = 0


class DataBackfillService:
    """ClickHouse数据补充服务"""

    def __init__(self, max_concurrent_tasks: int = 3):
        self.max_concurrent_tasks = max_concurrent_tasks
        self._active_tasks = 0

        # ClickHouse连接配置 - 从环境变量和配置文件读取
        import os

        clickhouse_config = config.get("clickhouse", {})
        self.ch_config = {
            "host": os.getenv("CLICKHOUSE_HOST", clickhouse_config.get("host", "localhost")),
            "port": int(os.getenv("CLICKHOUSE_PORT", clickhouse_config.get("port", 8123))),
            "user": os.getenv("CLICKHOUSE_USER", clickhouse_config.get("user", "default")),
            "password": os.getenv("CLICKHOUSE_PASSWORD", clickhouse_config.get("password", "password123")),
            "database": os.getenv("CLICKHOUSE_DATABASE", clickhouse_config.get("database", "cryptofeed")),
        }

    def detect_data_gaps(self, symbols: List[str], lookback_days: int = None) -> List[BackfillTask]:
        """
        数据缺口检测 - 按配置的回填策略检测

        Args:
            symbols: 要检查的交易对列表
            lookback_days: 检查最近N天的数据（忽略，使用配置中的按时间间隔设置）

        Returns:
            检测到的缺口任务列表
        """
        ch_client = clickhouse_connect.get_client(**self.ch_config)
        tasks = []
        now = datetime.utcnow()

        intervals = ["1d", "4h", "30m", "5m", "1m"]

        # 从统一的数据保留策略配置中获取每个时间间隔的回填天数
        retention_config = config.get("data_retention", {})
        candles_retention = retention_config.get("candles", {})

        # 如果没有配置，使用默认值（与data_retention保持一致）
        lookback_by_interval = candles_retention or {
            "1d": 1095,  # 3年
            "4h": 730,  # 2年
            "30m": 365,  # 1年
            "5m": 90,  # 90天
            "1m": 30,  # 30天
        }

        logger.info(f"📋 使用统一数据保留策略进行回填: {lookback_by_interval}")

        try:
            for symbol in symbols:
                for interval in intervals:
                    # 获取该时间间隔的回填天数
                    interval_lookback_days = lookback_by_interval.get(interval, 7)
                    logger.info(f"检查 {symbol} {interval}，回填范围：{interval_lookback_days} 天")

                    # 查找该交易对和时间间隔的最新数据时间
                    sql = """
                        SELECT MAX(timestamp) as latest_time
                        FROM candles
                        WHERE symbol = {symbol:String} AND interval = {interval:String}
                    """

                    result = ch_client.query(sql, {"symbol": symbol, "interval": interval})

                    # 检查是否有数据且数据不为NULL
                    if result.result_rows and result.result_rows[0][0] is not None:
                        latest_time = result.result_rows[0][0]

                        # 确保latest_time是有效的日期（不是1970年）
                        if latest_time.year < 2000:
                            logger.warning(f"发现无效的时间戳: {latest_time}, 当作无数据处理")
                            latest_time = None

                    else:
                        latest_time = None

                    if latest_time is not None:
                        # 有数据的情况
                        # 计算应该回填到的开始时间
                        target_start_time = now - timedelta(days=interval_lookback_days)

                        # 如果最新数据时间早于目标开始时间，说明历史数据不完整
                        if latest_time < target_start_time:
                            logger.info(
                                f"数据不完整: {symbol} {interval} 最新数据 {latest_time}，应该从 {target_start_time} 开始"
                            )

                            task = BackfillTask(
                                gap_log_id=0,
                                symbol=symbol,
                                data_type="candles",
                                interval=interval,
                                start_time=target_start_time,
                                end_time=latest_time,  # 补充到已有数据开始的位置
                            )
                            tasks.append(task)
                            logger.info(f"需要回填: {symbol} {interval} 从 {target_start_time} 到 {latest_time}")

                        # 检查是否有最新的缺口（从最新数据到现在）
                        time_since_latest = now - latest_time

                        # 根据时间间隔调整最新缺口检查阈值
                        interval_minutes = self._get_interval_minutes(interval)
                        threshold_seconds = max(interval_minutes * 60 * 3, 3600)  # 至少3个间隔或1小时

                        if time_since_latest.total_seconds() > threshold_seconds:
                            # 只回填从最新数据到现在的缺口，不回填历史
                            task = BackfillTask(
                                gap_log_id=0,
                                symbol=symbol,
                                data_type="candles",
                                interval=interval,
                                start_time=latest_time,
                                end_time=now,
                            )
                            tasks.append(task)
                            logger.info(
                                f"最新缺口: {symbol} {interval} 从 {latest_time} 到 {now} (延迟{time_since_latest.total_seconds()/3600:.1f}小时)"
                            )
                        else:
                            logger.info(f"数据完整: {symbol} {interval} 最新数据 {latest_time}，无需回填")

                    else:
                        # 完全没有数据，从指定天数前开始回填
                        start_time = now - timedelta(days=interval_lookback_days)
                        task = BackfillTask(
                            gap_log_id=0,
                            symbol=symbol,
                            data_type="candles",
                            interval=interval,
                            start_time=start_time,
                            end_time=now,
                        )
                        tasks.append(task)
                        logger.info(
                            f"完全无数据: {symbol} {interval} 从 {start_time} 到 {now} ({interval_lookback_days}天)"
                        )

            logger.info(f"检测到 {len(tasks)} 个数据缺口")
            return tasks

        finally:
            ch_client.close()

    def _find_precise_gaps(
        self, client, symbol: str, interval: str, start_time: datetime, end_time: datetime, interval_min: int
    ) -> List[Tuple[datetime, datetime]]:
        """
        精确查找数据缺口
        """
        # 获取现有数据的时间点
        sql = """
            SELECT timestamp
            FROM candles
            WHERE symbol = {symbol:String}
              AND interval = {interval:String}
              AND timestamp >= {start_time:DateTime}
              AND timestamp <= {end_time:DateTime}
            ORDER BY timestamp
        """

        result = client.query(
            sql, {"symbol": symbol, "interval": interval, "start_time": start_time, "end_time": end_time}
        )

        if not result.result_rows:
            # 完全没有数据
            return [(start_time, end_time)]

        existing_timestamps = [row[0] for row in result.result_rows]
        gaps = []

        # 生成期望的时间序列
        expected_timestamps = []
        current = start_time
        while current <= end_time:
            expected_timestamps.append(current)
            current += timedelta(minutes=interval_min)

        # 找出缺失的时间段
        existing_set = set(existing_timestamps)
        missing_timestamps = [ts for ts in expected_timestamps if ts not in existing_set]

        if not missing_timestamps:
            return []  # 没有缺口

        # 将连续的缺失时间合并为缺口范围
        gaps = []
        gap_start = missing_timestamps[0]
        gap_end = missing_timestamps[0]

        for i in range(1, len(missing_timestamps)):
            current_ts = missing_timestamps[i]
            expected_next = gap_end + timedelta(minutes=interval_min)

            if current_ts == expected_next:
                # 连续缺失，扩展当前缺口
                gap_end = current_ts
            else:
                # 新的缺口开始
                gaps.append((gap_start, gap_end + timedelta(minutes=interval_min)))
                gap_start = current_ts
                gap_end = current_ts

        # 添加最后一个缺口
        gaps.append((gap_start, gap_end + timedelta(minutes=interval_min)))

        return gaps

    def backfill_candles(
        self, symbol: str, interval: str, start_time: datetime, end_time: datetime
    ) -> Tuple[int, Optional[str]]:
        """
        分批回填K线数据 - 支持大量历史数据下载

        Args:
            symbol: 交易对符号
            interval: 时间间隔
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            (添加的记录数, 错误信息)
        """
        try:
            # 转换为Binance符号格式
            binance_symbol = self.convert_symbol_for_binance(symbol)
            binance_interval = self.convert_interval_for_binance(interval)

            total_days = (end_time - start_time).days
            logger.info(f"🔄 开始回填 {symbol} {interval}，时间范围: {total_days} 天")
            logger.info(f"   从 {start_time.strftime('%Y-%m-%d %H:%M')} 到 {end_time.strftime('%Y-%m-%d %H:%M')}")

            # 分批策略：根据时间间隔决定每批的天数
            batch_days = {
                "1d": 365,  # 日线一次获取1年
                "4h": 180,  # 4小时线一次获取半年
                "30m": 30,  # 30分钟线一次获取1个月
                "5m": 7,  # 5分钟线一次获取1周
                "1m": 3,  # 1分钟线一次获取3天
            }.get(interval, 30)

            total_records = 0
            current_start = start_time
            ch_client = clickhouse_connect.get_client(**self.ch_config)

            try:
                while current_start < end_time:
                    # 计算当前批次的结束时间
                    current_end = min(current_start + timedelta(days=batch_days), end_time)

                    logger.info(f"📦 批次: {current_start.strftime('%Y-%m-%d')} 到 {current_end.strftime('%Y-%m-%d')}")

                    # 调用Binance API
                    start_ms = int(current_start.timestamp() * 1000)
                    end_ms = int(current_end.timestamp() * 1000)

                    url = "https://fapi.binance.com/fapi/v1/klines"
                    params = {
                        "symbol": binance_symbol,
                        "interval": binance_interval,
                        "startTime": start_ms,
                        "endTime": end_ms,
                        "limit": 1500,
                    }

                    response = requests.get(url, params=params, timeout=30)

                    if response.status_code != 200:
                        error_msg = f"Binance API 错误: {response.status_code} - {response.text}"
                        logger.error(error_msg)
                        return total_records, error_msg

                    klines_data = response.json()
                    logger.info(f"  📊 API 返回 {len(klines_data)} 条数据")

                    if klines_data:
                        # 准备插入数据
                        insert_data = []
                        for kline in klines_data:
                            open_time = datetime.fromtimestamp(kline[0] / 1000)

                            # 构造标准化数据
                            raw_data = {
                                "timestamp": open_time,
                                "exchange": "binance",  # REST API原始名称
                                "symbol": symbol,
                                "interval": interval,
                                "open": float(kline[1]),
                                "high": float(kline[2]),
                                "low": float(kline[3]),
                                "close": float(kline[4]),
                                "volume": float(kline[5]),
                                "trades": int(kline[8]) if len(kline) > 8 else 0,  # 交易次数，默认0
                            }

                            # 数据标准化处理
                            normalized_data = normalize_data(raw_data, "candle")

                            insert_data.append(
                                [
                                    normalized_data["timestamp"],  # timestamp
                                    normalized_data["exchange"],  # exchange (标准化后的BINANCE_FUTURES)
                                    normalized_data["symbol"],  # symbol
                                    normalized_data["interval"],  # interval
                                    normalized_data["open"],  # open
                                    normalized_data["high"],  # high
                                    normalized_data["low"],  # low
                                    normalized_data["close"],  # close
                                    normalized_data["volume"],  # volume
                                    normalized_data.get("trades", 0),  # trades 交易次数，默认0
                                ]
                            )

                        # 批量插入到ClickHouse - 添加详细调试信息
                        if insert_data:
                            # 调试：检查第一行数据的结构
                            first_row = insert_data[0]
                            logger.debug(f"🔍 第一行数据长度: {len(first_row)}")
                            logger.debug(f"🔍 第一行数据类型: {[type(x).__name__ for x in first_row]}")

                            # 检查是否有None值
                            for i, value in enumerate(first_row):
                                if value is None:
                                    logger.error(f"🚨 发现None值在第 {i} 列")

                            # 使用INSERT OR REPLACE避免重复数据
                            try:
                                # 使用ClickHouse的ReplacingMergeTree特性，避免重复插入
                                # 先检查是否有重复数据
                                first_time = insert_data[0][0]  # timestamp
                                last_time = insert_data[-1][0]  # timestamp

                                check_query = f"""
                                SELECT COUNT(*) FROM candles
                                WHERE symbol = '{symbol}' AND interval = '{interval}'
                                AND timestamp >= '{first_time}' AND timestamp <= '{last_time}'
                                """
                                result = ch_client.query(check_query)
                                existing_count = result.result_rows[0][0] if result.result_rows else 0

                                if existing_count > 0:
                                    logger.warning(f"⚠️ 发现 {existing_count} 条重复数据，跳过插入 {symbol} {interval}")
                                    skipped_records = len(insert_data)
                                else:
                                    # 直接插入9个字段，与表结构完全匹配
                                    ch_client.insert("candles", insert_data)
                                    logger.info(f"  ✅ 插入 {len(insert_data)} 条数据")
                                    skipped_records = 0

                            except Exception as e:
                                logger.error(f"🚨 ClickHouse插入失败: {e}")
                                logger.error(f"🔍 插入数据样本 (前3行):")
                                for i, row in enumerate(insert_data[:3]):
                                    logger.error(f"  行{i}: 长度={len(row)}, 数据={row}")
                                raise e
                        else:
                            logger.warning("⚠️ 没有数据需要插入")
                        total_records += len(insert_data)

                    # 移动到下一个批次
                    current_start = current_end

                    # API限制：延迟避免触发限制
                    time.sleep(0.2)

                logger.info(f"🎉 回填完成！{symbol} {interval} 总计插入 {total_records} 条数据")
                return total_records, None

            finally:
                ch_client.close()

        except Exception as e:
            error_msg = f"回填失败 {symbol} {interval}: {str(e)}"
            logger.error(error_msg)
            return 0, error_msg

    def convert_symbol_for_binance(self, symbol: str) -> str:
        """转换cryptofeed符号格式为Binance格式"""
        if symbol.endswith("-PERP"):
            return symbol.replace("-USDT-PERP", "USDT").replace("-", "")
        return symbol.replace("-", "")

    def convert_interval_for_binance(self, interval: str) -> str:
        """转换时间间隔格式"""
        mapping = {"1m": "1m", "5m": "5m", "30m": "30m", "4h": "4h", "1d": "1d"}
        return mapping.get(interval, interval)

    def run_backfill_tasks(self, symbols: List[str], lookback_days: int = None) -> Dict[str, any]:
        """
        运行历史数据回填任务

        Args:
            symbols: 要处理的交易对列表
            lookback_days: 检查最近N天的数据

        Returns:
            回填结果统计
        """
        # 如果没有指定lookback_days，从配置中读取
        if lookback_days is None:
            lookback_days = config.get("data_backfill.default_lookback_days", 7)

        logger.info(f"Starting backfill for {len(symbols)} symbols, lookback {lookback_days} days")

        # 检测数据缺口
        tasks = self.detect_data_gaps(symbols, lookback_days)

        if not tasks:
            logger.info("No data gaps detected")
            return {"total_tasks": 0, "successful": 0, "failed": 0, "records_added": 0}

        # 执行回填任务
        successful_tasks = 0
        failed_tasks = 0
        total_records = 0
        failed_symbols = []

        for task in tasks:
            try:
                logger.info(f"Processing backfill task: {task.symbol} {task.interval}")

                records_added, error_msg = self.backfill_candles(
                    task.symbol, task.interval, task.start_time, task.end_time
                )

                if error_msg:
                    logger.error(f"Backfill failed for {task.symbol} {task.interval}: {error_msg}")
                    failed_tasks += 1
                    failed_symbols.append(f"{task.symbol} {task.interval}")
                else:
                    successful_tasks += 1
                    total_records += records_added
                    logger.info(f"Backfill completed for {task.symbol} {task.interval}: {records_added} records")

                # 避免API限制
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Unexpected error during backfill {task.symbol} {task.interval}: {e}")
                failed_tasks += 1
                failed_symbols.append(f"{task.symbol} {task.interval}")

        result = {
            "total_tasks": len(tasks),
            "successful": successful_tasks,
            "failed": failed_tasks,
            "records_added": total_records,
            "failed_symbols": failed_symbols,
        }

        logger.info(f"Backfill completed: {successful_tasks}/{len(tasks)} successful, {total_records} records added")
        if failed_symbols:
            logger.warning(f"Failed symbols: {', '.join(failed_symbols)}")

        return result

    def run_continuous_backfill(self, symbols: List[str], check_interval_hours: int = 6):
        """
        持续运行数据补充服务

        Args:
            symbols: 要监控的交易对列表
            check_interval_hours: 检查间隔（小时）
        """
        logger.info(
            f"Starting continuous backfill service for {len(symbols)} symbols (check every {check_interval_hours} hours)"
        )

        while True:
            try:
                # 运行一轮回填任务
                default_lookback = config.get("data_backfill.default_lookback_days", 7)
                result = self.run_backfill_tasks(symbols, lookback_days=default_lookback)

                if result["total_tasks"] > 0:
                    logger.info(
                        f"Backfill cycle: {result['successful']}/{result['total_tasks']} tasks successful, {result['records_added']} records added"
                    )

                # 等待下次检查
                time.sleep(check_interval_hours * 3600)

            except Exception as e:
                logger.error(f"Error in continuous backfill: {e}")
                time.sleep(300)  # 错误时等待5分钟再重试

    def _get_interval_minutes(self, interval: str) -> int:
        """获取时间间隔的分钟数"""
        interval_mapping = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
        return interval_mapping.get(interval, 60)
