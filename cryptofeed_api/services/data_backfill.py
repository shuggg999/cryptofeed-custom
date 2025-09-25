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

        # ClickHouse连接配置
        self.ch_config = {
            'host': 'localhost',
            'port': 8123,
            'user': 'default',
            'password': 'password123',
            'database': 'cryptofeed'
        }

    def detect_data_gaps(self, symbols: List[str], lookback_days: int = None) -> List[BackfillTask]:
        """
        检测数据缺口，基于ClickHouse数据

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

        try:
            for symbol in symbols:
                for interval in intervals:
                    # 检查该时间段是否有数据缺口
                    sql = """
                        SELECT
                            MIN(timestamp) as first_ts,
                            MAX(timestamp) as last_ts,
                            COUNT(*) as total_count
                        FROM candles
                        WHERE symbol = {symbol:String}
                          AND interval = {interval:String}
                          AND timestamp >= {start_time:DateTime}
                          AND timestamp <= {end_time:DateTime}
                    """

                    result = ch_client.query(sql, {
                        'symbol': symbol,
                        'interval': interval,
                        'start_time': start_time,
                        'end_time': end_time
                    })

                    if result and result.result_rows:
                        row = result.result_rows[0]
                        first_ts, last_ts, count = row

                        # 如果没有数据或数据不完整，创建回填任务
                        if count == 0 or (first_ts and first_ts > start_time):
                            task = BackfillTask(
                                gap_log_id=0,  # 动态检测，无数据库记录
                                symbol=symbol,
                                data_type='candles',
                                interval=interval,
                                start_time=start_time,
                                end_time=end_time
                            )
                            tasks.append(task)
                            logger.info(f"Detected gap for {symbol} {interval}: {start_time} to {end_time}")

            logger.info(f"Detected {len(tasks)} data gaps")
            return tasks

        finally:
            ch_client.close()

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
                # 先删除整个时间范围的数据（避免重复）
                ch_client.command(f"""
                    DELETE FROM candles
                    WHERE symbol = '{symbol}' AND interval = '{interval}'
                    AND timestamp >= '{start_time}' AND timestamp <= '{end_time}'
                """)

                # 计算需要的批次数（每批1500条，5分钟间隔约5.2天）
                total_minutes = int((end_time - start_time).total_seconds() / 60)
                interval_minutes = {'1m': 1, '5m': 5, '30m': 30, '4h': 240, '1d': 1440}
                interval_min = interval_minutes.get(interval, 5)

                estimated_records = total_minutes // interval_min
                batch_size = 1500
                num_batches = (estimated_records + batch_size - 1) // batch_size

                logger.info(f"准备分批获取 {symbol} {interval} 数据：预计 {estimated_records} 条记录，{num_batches} 个批次")

                all_insert_data = []
                current_start = start_time
                batch_num = 0

                while current_start < end_time and batch_num < num_batches:
                    batch_num += 1

                    # 计算当前批次的结束时间（确保不超过总结束时间）
                    batch_end = min(
                        current_start + timedelta(days=5),  # 每批约5天数据
                        end_time
                    )

                    start_ms = int(current_start.timestamp() * 1000)
                    end_ms = int(batch_end.timestamp() * 1000)

                    # 调用Binance API
                    url = "https://fapi.binance.com/fapi/v1/klines"
                    params = {
                        'symbol': binance_symbol,
                        'interval': binance_interval,
                        'startTime': start_ms,
                        'endTime': end_ms,
                        'limit': batch_size
                    }

                    logger.info(f"批次 {batch_num}/{num_batches}: 获取 {current_start.strftime('%Y-%m-%d %H:%M')} 到 {batch_end.strftime('%Y-%m-%d %H:%M')}")

                    response = requests.get(url, params=params, timeout=30)

                    if response.status_code == 200:
                        klines_data = response.json()

                        if klines_data:
                            # 准备ClickHouse插入数据
                            for kline in klines_data:
                                open_time = datetime.fromtimestamp(kline[0] / 1000)
                                all_insert_data.append([
                                    open_time,                    # timestamp
                                    symbol,                       # symbol
                                    interval,                     # interval
                                    float(kline[1]),              # open
                                    float(kline[2]),              # high
                                    float(kline[3]),              # low
                                    float(kline[4]),              # close
                                    float(kline[5]),              # volume
                                    open_time                     # receipt_timestamp
                                ])

                            logger.info(f"批次 {batch_num} 获取到 {len(klines_data)} 条数据")

                            # 更新下次开始时间为本批次最后一条数据的时间+1个间隔
                            if klines_data:
                                last_time = datetime.fromtimestamp(klines_data[-1][0] / 1000)
                                current_start = last_time + timedelta(minutes=interval_min)
                        else:
                            logger.warning(f"批次 {batch_num} 返回空数据，跳过")
                            current_start = batch_end
                    else:
                        error_msg = f"批次 {batch_num} Binance API错误: {response.status_code} - {response.text}"
                        logger.error(error_msg)
                        return len(all_insert_data), error_msg

                    # API限制：避免过快请求
                    time.sleep(0.2)

                # 统一插入所有数据
                if all_insert_data:
                    ch_client.insert('candles', all_insert_data)
                    logger.info(f"✅ 成功回填 {len(all_insert_data)} 条 {symbol} {interval} 数据（{num_batches}个批次）")
                    return len(all_insert_data), None
                else:
                    logger.warning(f"没有获取到任何数据 {symbol} {interval}")
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