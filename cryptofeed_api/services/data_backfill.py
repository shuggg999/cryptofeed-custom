"""
历史数据补充服务 - ClickHouse版本
自动补充ClickHouse数据库中缺失的历史数据
"""
import asyncio
import logging
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

import clickhouse_connect

from cryptofeed_api.monitor.config import config
from cryptofeed_api.services.data_normalizer import normalize_data

logger = logging.getLogger(__name__)


@dataclass
class BackfillTask:
    """数据补充任务"""
    gap_log_id: int
    symbol: str
    data_type: str  # 'candles', 'trades', 'funding'
    interval: Optional[str]  # 仅用于candles
    start_time: datetime
    end_time: datetime
    status: str = 'pending'
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
        clickhouse_config = config.get('clickhouse', {})
        self.ch_config = {
            'host': os.getenv('CLICKHOUSE_HOST', clickhouse_config.get('host', 'localhost')),
            'port': int(os.getenv('CLICKHOUSE_PORT', clickhouse_config.get('port', 8123))),
            'user': os.getenv('CLICKHOUSE_USER', clickhouse_config.get('user', 'default')),
            'password': os.getenv('CLICKHOUSE_PASSWORD', clickhouse_config.get('password', 'password123')),
            'database': os.getenv('CLICKHOUSE_DATABASE', clickhouse_config.get('database', 'cryptofeed'))
        }

    def detect_data_gaps(self, symbols: List[str], lookback_days: int = None) -> List[BackfillTask]:
        """
        精确检测数据缺口，基于ClickHouse数据 - 改进版
        只检测实际缺失的时间段，避免重复下载

        Args:
            symbols: 要检查的交易对列表
            lookback_days: 检查最近N天的数据

        Returns:
            检测到的缺口任务列表
        """
        # 如果没有指定lookback_days，从配置中读取
        if lookback_days is None:
            lookback_days = config.get('data_backfill.default_lookback_days', 7)

        ch_client = clickhouse_connect.get_client(**self.ch_config)
        tasks = []
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=lookback_days)

        intervals = ['1m', '5m', '30m', '4h', '1d']
        interval_minutes = {'1m': 1, '5m': 5, '30m': 30, '4h': 240, '1d': 1440}

        try:
            for symbol in symbols:
                for interval in intervals:
                    gaps = self._find_precise_gaps(
                        ch_client, symbol, interval, start_time, end_time,
                        interval_minutes[interval]
                    )

                    for gap_start, gap_end in gaps:
                        task = BackfillTask(
                            gap_log_id=0,
                            symbol=symbol,
                            data_type='candles',
                            interval=interval,
                            start_time=gap_start,
                            end_time=gap_end
                        )
                        tasks.append(task)
                        logger.info(f"Detected precise gap for {symbol} {interval}: {gap_start} to {gap_end}")

            logger.info(f"Detected {len(tasks)} precise data gaps")
            return tasks

        finally:
            ch_client.close()

    def _find_precise_gaps(self, client, symbol: str, interval: str,
                          start_time: datetime, end_time: datetime,
                          interval_min: int) -> List[Tuple[datetime, datetime]]:
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

        result = client.query(sql, {
            'symbol': symbol,
            'interval': interval,
            'start_time': start_time,
            'end_time': end_time
        })

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
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[int, Optional[str]]:
        """
        补充K线数据 - 分批获取历史数据突破API限制

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

            ch_client = clickhouse_connect.get_client(**self.ch_config)

            try:
                # 新逻辑：不删除现有数据，只插入缺失的数据
                # 这样可以避免重复下载和数据丢失的风险
                logger.info(f"开始精确回填 {symbol} {interval}，时间范围: {start_time} 到 {end_time}")

                # 精确获取缺失的数据 - 只下载需要的时间段
                start_ms = int(start_time.timestamp() * 1000)
                end_ms = int(end_time.timestamp() * 1000)

                # 调用Binance API获取指定时间段的数据
                url = "https://fapi.binance.com/fapi/v1/klines"
                params = {
                    'symbol': binance_symbol,
                    'interval': binance_interval,
                    'startTime': start_ms,
                    'endTime': end_ms,
                    'limit': 1500
                }

                logger.info(f"精确获取 {symbol} {interval}: {start_time.strftime('%Y-%m-%d %H:%M')} 到 {end_time.strftime('%Y-%m-%d %H:%M')}")

                response = requests.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    klines_data = response.json()
                    all_insert_data = []

                    if klines_data:
                        # 获取现有数据的时间戳以避免重复
                        existing_sql = """
                            SELECT timestamp
                            FROM candles
                            WHERE symbol = {symbol:String}
                              AND interval = {interval:String}
                              AND timestamp >= {start_time:DateTime}
                              AND timestamp <= {end_time:DateTime}
                        """

                        existing_result = ch_client.query(existing_sql, {
                            'symbol': symbol,
                            'interval': interval,
                            'start_time': start_time,
                            'end_time': end_time
                        })

                        existing_timestamps = set(row[0] for row in existing_result.result_rows)

                        # 只处理不存在的数据
                        for kline in klines_data:
                            open_time = datetime.fromtimestamp(kline[0] / 1000)

                            # 跳过已存在的数据点
                            if open_time in existing_timestamps:
                                continue

                            # 构造原始数据字典用于标准化
                            raw_data = {
                                'timestamp': open_time,
                                'exchange': 'binance',  # REST API原始名称
                                'symbol': symbol,
                                'interval': interval,
                                'open': float(kline[1]),
                                'high': float(kline[2]),
                                'low': float(kline[3]),
                                'close': float(kline[4]),
                                'volume': float(kline[5])
                            }

                            # 数据标准化处理
                            normalized_data = normalize_data(raw_data, 'candle')

                            all_insert_data.append([
                                normalized_data['timestamp'],      # timestamp
                                normalized_data['exchange'],       # exchange (标准化后)
                                normalized_data['symbol'],         # symbol
                                normalized_data['interval'],       # interval
                                normalized_data['open'],           # open
                                normalized_data['high'],           # high
                                normalized_data['low'],            # low
                                normalized_data['close'],          # close
                                normalized_data['volume']          # volume
                            ])

                        logger.info(f"获取到 {len(klines_data)} 条数据，其中 {len(all_insert_data)} 条为新数据需要插入")

                    else:
                        logger.warning(f"API返回空数据")
                        return 0, None
                else:
                    error_msg = f"Binance API错误: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    return 0, error_msg

                # 统一插入所有数据
                if all_insert_data:
                    ch_client.insert('candles', all_insert_data)
                    logger.info(f"✅ 成功精确回填 {len(all_insert_data)} 条 {symbol} {interval} 新数据")
                    return len(all_insert_data), None
                else:
                    logger.info(f"✓ {symbol} {interval} 无需回填，所有数据已存在")
                    return 0, None

            finally:
                ch_client.close()

        except Exception as e:
            error_msg = f"回填失败 {symbol} {interval}: {str(e)}"
            logger.error(error_msg)
            return 0, error_msg

    def convert_symbol_for_binance(self, symbol: str) -> str:
        """转换cryptofeed符号格式为Binance格式"""
        if symbol.endswith('-PERP'):
            return symbol.replace('-USDT-PERP', 'USDT').replace('-', '')
        return symbol.replace('-', '')

    def convert_interval_for_binance(self, interval: str) -> str:
        """转换时间间隔格式"""
        mapping = {
            '1m': '1m',
            '5m': '5m',
            '30m': '30m',
            '4h': '4h',
            '1d': '1d'
        }
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
            lookback_days = config.get('data_backfill.default_lookback_days', 7)

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
                    task.symbol,
                    task.interval,
                    task.start_time,
                    task.end_time
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
            "failed_symbols": failed_symbols
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
        logger.info(f"Starting continuous backfill service for {len(symbols)} symbols (check every {check_interval_hours} hours)")

        while True:
            try:
                # 运行一轮回填任务
                default_lookback = config.get('data_backfill.default_lookback_days', 7)
                result = self.run_backfill_tasks(symbols, lookback_days=default_lookback)

                if result["total_tasks"] > 0:
                    logger.info(f"Backfill cycle: {result['successful']}/{result['total_tasks']} tasks successful, {result['records_added']} records added")

                # 等待下次检查
                time.sleep(check_interval_hours * 3600)

            except Exception as e:
                logger.error(f"Error in continuous backfill: {e}")
                time.sleep(300)  # 错误时等待5分钟再重试