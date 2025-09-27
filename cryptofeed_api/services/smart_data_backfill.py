"""
智能历史数据补充服务 - 升级版
基于现有data_backfill.py，添加精确缺口检测和优先级分级功能
核心原则：缺什么补什么，分级别处理，确保量化系统数据完整性
"""

import asyncio
import logging
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import clickhouse_connect
import yaml

from cryptofeed_api.monitor.config import config
from cryptofeed_api.services.data_normalizer import normalize_data

logger = logging.getLogger(__name__)


class GapType(Enum):
    """缺口类型枚举"""
    URGENT = "urgent"       # 紧急（最近1小时）
    RECENT = "recent"       # 近期（1-24小时）
    HISTORICAL = "historical"  # 历史（24小时以上）
    NONE = "none"           # 无缺口


@dataclass
class PreciseGap:
    """精确缺口信息"""
    symbol: str
    interval: str
    start_time: datetime
    end_time: datetime
    gap_type: GapType
    priority: int
    expected_records: int
    gap_duration_hours: float


@dataclass
class SmartBackfillTask:
    """智能回填任务（扩展原有BackfillTask）"""
    gap_log_id: int
    symbol: str
    data_type: str
    interval: Optional[str]
    start_time: datetime
    end_time: datetime
    gap_type: GapType
    priority: int
    expected_records: int
    status: str = 'pending'
    error_message: Optional[str] = None
    records_filled: int = 0


class SmartDataBackfillService:
    """智能数据回填服务 - 在现有逻辑基础上添加智能功能"""

    def __init__(self, config_path: str = '/app/config/main.yaml'):
        """初始化智能回填服务"""
        self.config = self._load_config(config_path)
        self.clickhouse_cfg = self.config['clickhouse']

        # 优先级权重配置
        self.priority_weights = {
            'time': {
                GapType.URGENT: 10,      # 最近1小时
                GapType.RECENT: 7,       # 1-24小时
                GapType.HISTORICAL: 3,   # 24小时以上
                GapType.NONE: 0
            },
            'interval': {
                '1m': 10, '5m': 8, '15m': 6, '30m': 5,
                '1h': 4, '4h': 3, '1d': 1
            }
        }

        # 时间阈值配置
        self.time_thresholds = {
            'urgent_hours': 1,    # 1小时内为紧急
            'recent_hours': 24,   # 24小时内为近期
        }

        # 确保状态表存在
        self._ensure_status_tables()

    def _load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _ensure_status_tables(self):
        """确保状态管理表存在"""
        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        try:
            # 检查表是否存在
            check_sql = "SELECT name FROM system.tables WHERE name = 'backfill_status'"
            result = client.query(check_sql)

            if not result.result_rows:
                logger.info("创建智能回填状态表...")
                with open('/Volumes/磁盘/Projects/cryptofeed/scripts/create_smart_backfill_tables.sql', 'r') as f:
                    sql_commands = f.read().split(';')
                    for sql in sql_commands:
                        if sql.strip():
                            try:
                                client.command(sql.strip())
                            except Exception as e:
                                logger.debug(f"SQL执行提示: {e}")
        finally:
            client.close()

    async def smart_detect_and_fill(self, scenario: str = 'normal') -> Dict[str, any]:
        """
        智能检测和回填主函数
        scenario: 'normal', 'startup', 'network_recovery', 'manual_check'
        返回处理统计信息
        """
        logger.info(f"🧠 启动智能数据检测和回填 (场景: {scenario})...")
        start_time = time.time()

        # 0. 场景特定的预处理
        scenario_info = await self._handle_scenario_preprocessing(scenario)

        # 1. 快速状态检查
        status_summary = await self._quick_status_check()

        # 2. 精确缺口检测（增强场景感知）
        precise_gaps = await self._detect_precise_gaps(scenario)

        # 3. 场景特定的缺口增强检测
        additional_gaps = await self._detect_scenario_specific_gaps(scenario, scenario_info)
        precise_gaps.extend(additional_gaps)

        # 4. 优先级分类
        classified_gaps = self._classify_gaps_by_priority(precise_gaps)

        # 5. 更新状态表
        await self._update_backfill_status(classified_gaps)

        # 6. 执行分级回填
        results = await self._execute_prioritized_backfill(classified_gaps)

        duration = time.time() - start_time

        # 返回统计信息
        return {
            'duration_seconds': duration,
            'scenario': scenario,
            'scenario_info': scenario_info,
            'status_summary': status_summary,
            'gaps_detected': len(precise_gaps),
            'gaps_by_priority': {
                'urgent': len([g for g in precise_gaps if g.gap_type == GapType.URGENT]),
                'recent': len([g for g in precise_gaps if g.gap_type == GapType.RECENT]),
                'historical': len([g for g in precise_gaps if g.gap_type == GapType.HISTORICAL])
            },
            'backfill_results': results
        }

    async def _quick_status_check(self) -> Dict[str, int]:
        """快速状态检查 - 只查时间戳，不下载数据"""
        logger.info("⚡ 执行快速状态检查...")

        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        try:
            symbols = self.config['symbols']['custom_list']
            intervals = list(self.config['data_retention']['candles'].keys())

            total_sources = len(symbols) * len(intervals)
            complete_sources = 0
            partial_sources = 0
            missing_sources = 0

            for symbol in symbols:
                for interval in intervals:
                    status = await self._quick_check_single_source(
                        client, symbol, interval
                    )

                    if status == 'complete':
                        complete_sources += 1
                    elif status == 'partial':
                        partial_sources += 1
                    else:
                        missing_sources += 1

            logger.info(
                f"📊 快速检查完成: "
                f"完整={complete_sources} | "
                f"部分={partial_sources} | "
                f"缺失={missing_sources}"
            )

            return {
                'total': total_sources,
                'complete': complete_sources,
                'partial': partial_sources,
                'missing': missing_sources
            }

        finally:
            client.close()

    async def _quick_check_single_source(
        self, client, symbol: str, interval: str
    ) -> str:
        """快速检查单个数据源"""
        # 获取保留天数
        retention_days = self.config['data_retention']['candles'].get(interval, 30)
        now = datetime.now()
        expected_start = now - timedelta(days=retention_days)

        # 查询最新和最老数据
        sql = """
            SELECT
                MAX(timestamp) as latest,
                MIN(timestamp) as oldest,
                COUNT(*) as count
            FROM candles
            WHERE symbol = {symbol:String}
              AND interval = {interval:String}
        """

        result = client.query(sql, {'symbol': symbol, 'interval': interval})

        if not result.result_rows or result.result_rows[0][2] == 0:
            return 'missing'

        latest, oldest, count = result.result_rows[0]

        # 简单判断数据完整性
        if latest and oldest:
            # 检查最新数据是否及时
            time_since_latest = now - latest
            interval_minutes = self._get_interval_minutes(interval)
            expected_delay = timedelta(minutes=interval_minutes * 3)

            if time_since_latest > expected_delay:
                return 'partial'

            # 检查历史数据是否完整
            if oldest > expected_start:
                return 'partial'

            return 'complete'

        return 'missing'

    async def _detect_precise_gaps(self, scenario: str = 'normal') -> List[PreciseGap]:
        """精确缺口检测 - 逐时间段检查，找出确切的缺失部分"""
        logger.info(f"🔍 执行精确缺口检测 (场景: {scenario})...")

        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        all_gaps = []

        try:
            symbols = self.config['symbols']['custom_list']
            intervals = list(self.config['data_retention']['candles'].keys())

            for symbol in symbols:
                for interval in intervals:
                    gaps = await self._find_gaps_for_source(
                        client, symbol, interval, scenario
                    )
                    all_gaps.extend(gaps)

            logger.info(f"🎯 精确检测完成，发现 {len(all_gaps)} 个缺口")

            # 按优先级排序
            all_gaps.sort(key=lambda g: g.priority, reverse=True)

            return all_gaps

        finally:
            client.close()

    async def _find_gaps_for_source(
        self, client, symbol: str, interval: str, scenario: str = 'normal'
    ) -> List[PreciseGap]:
        """为单个数据源查找精确缺口"""
        gaps = []
        now = datetime.now()

        # 获取保留策略
        retention_days = self.config['data_retention']['candles'].get(interval, 30)
        expected_start = now - timedelta(days=retention_days)
        interval_minutes = self._get_interval_minutes(interval)

        # 查询现有数据的时间点
        sql = """
            SELECT timestamp
            FROM candles
            WHERE symbol = {symbol:String}
              AND interval = {interval:String}
              AND timestamp >= {start:DateTime}
              AND timestamp <= {end:DateTime}
            ORDER BY timestamp
        """

        result = client.query(sql, {
            'symbol': symbol,
            'interval': interval,
            'start': expected_start,
            'end': now
        })

        if not result.result_rows:
            # 完全没有数据
            gap = self._create_gap(
                symbol, interval, expected_start, now, interval_minutes
            )
            gaps.append(gap)
            return gaps

        existing_times = [row[0] for row in result.result_rows]

        # 检查开头缺口
        first_time = existing_times[0]
        if first_time > expected_start:
            gap = self._create_gap(
                symbol, interval, expected_start, first_time, interval_minutes
            )
            gaps.append(gap)

        # 检查中间缺口
        for i in range(len(existing_times) - 1):
            current_time = existing_times[i]
            next_time = existing_times[i + 1]

            expected_next = current_time + timedelta(minutes=interval_minutes)

            # 如果下一个时间点距离当前时间超过2个间隔，认为有缺口
            if next_time > expected_next + timedelta(minutes=interval_minutes):
                gap = self._create_gap(
                    symbol, interval, expected_next, next_time, interval_minutes
                )
                gaps.append(gap)

        # 检查结尾缺口
        last_time = existing_times[-1]
        expected_latest = now - timedelta(minutes=interval_minutes * 2)

        if last_time < expected_latest:
            gap = self._create_gap(
                symbol, interval, last_time + timedelta(minutes=interval_minutes),
                now, interval_minutes
            )
            gaps.append(gap)

        return gaps

    def _create_gap(
        self, symbol: str, interval: str, start: datetime,
        end: datetime, interval_minutes: int
    ) -> PreciseGap:
        """创建缺口对象"""
        duration_hours = (end - start).total_seconds() / 3600

        # 判断缺口类型
        if duration_hours <= self.time_thresholds['urgent_hours']:
            gap_type = GapType.URGENT
        elif duration_hours <= self.time_thresholds['recent_hours']:
            gap_type = GapType.RECENT
        else:
            gap_type = GapType.HISTORICAL

        # 计算优先级
        priority = self._calculate_priority(gap_type, interval, duration_hours)

        # 估算期望记录数
        expected_records = int(duration_hours * 60 / interval_minutes)

        return PreciseGap(
            symbol=symbol,
            interval=interval,
            start_time=start,
            end_time=end,
            gap_type=gap_type,
            priority=priority,
            expected_records=expected_records,
            gap_duration_hours=duration_hours
        )

    def _calculate_priority(
        self, gap_type: GapType, interval: str, duration_hours: float
    ) -> int:
        """计算回填优先级"""
        # 时间权重
        time_weight = self.priority_weights['time'].get(gap_type, 1)

        # 间隔权重
        interval_weight = self.priority_weights['interval'].get(interval, 1)

        # 持续时间权重（越长优先级越高，但有上限）
        duration_weight = min(5, max(1, duration_hours / 24))

        # 综合优先级（1-10分）
        priority = min(10, max(1,
            time_weight * 0.6 + interval_weight * 0.3 + duration_weight * 0.1
        ))

        return int(priority)

    def _classify_gaps_by_priority(self, gaps: List[PreciseGap]) -> Dict[str, List[PreciseGap]]:
        """按优先级分类缺口"""
        classified = {
            'urgent': [],
            'recent': [],
            'historical': []
        }

        for gap in gaps:
            if gap.gap_type == GapType.URGENT:
                classified['urgent'].append(gap)
            elif gap.gap_type == GapType.RECENT:
                classified['recent'].append(gap)
            else:
                classified['historical'].append(gap)

        return classified

    async def _update_backfill_status(self, classified_gaps: Dict[str, List[PreciseGap]]):
        """更新回填状态表"""
        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        try:
            # 记录检测到的缺口
            for gap_type, gaps in classified_gaps.items():
                for gap in gaps:
                    # 插入缺口检测日志
                    sql = """
                        INSERT INTO gap_detection_log (
                            log_id, symbol, interval, gap_start, gap_end,
                            gap_type, priority, records_expected, detection_time
                        ) VALUES (
                            {log_id:UInt64}, {symbol:String}, {interval:String},
                            {start:DateTime}, {end:DateTime}, {gap_type:String},
                            {priority:Int8}, {expected:UInt32}, {detection:DateTime}
                        )
                    """

                    client.command(sql, {
                        'log_id': int(time.time() * 1000000),  # 微秒时间戳作为ID
                        'symbol': gap.symbol,
                        'interval': gap.interval,
                        'start': gap.start_time,
                        'end': gap.end_time,
                        'gap_type': gap.gap_type.value,
                        'priority': gap.priority,
                        'expected': gap.expected_records,
                        'detection': datetime.now()
                    })

        finally:
            client.close()

    async def _execute_prioritized_backfill(
        self, classified_gaps: Dict[str, List[PreciseGap]]
    ) -> Dict[str, any]:
        """执行分级回填 - 紧急优先，历史延后"""
        results = {
            'urgent_completed': 0,
            'recent_completed': 0,
            'historical_completed': 0,
            'total_records_filled': 0,
            'errors': []
        }

        # 1. 立即处理紧急缺口
        if classified_gaps['urgent']:
            logger.info(f"🚨 立即处理 {len(classified_gaps['urgent'])} 个紧急缺口")
            urgent_results = await self._fill_gaps_batch(classified_gaps['urgent'])
            results['urgent_completed'] = urgent_results['completed']
            results['total_records_filled'] += urgent_results['records_filled']
            results['errors'].extend(urgent_results['errors'])

        # 2. 处理近期缺口
        if classified_gaps['recent']:
            logger.info(f"⏰ 处理 {len(classified_gaps['recent'])} 个近期缺口")
            recent_results = await self._fill_gaps_batch(classified_gaps['recent'])
            results['recent_completed'] = recent_results['completed']
            results['total_records_filled'] += recent_results['records_filled']
            results['errors'].extend(recent_results['errors'])

        # 3. 延后处理历史缺口（可选，根据系统负载）
        if classified_gaps['historical']:
            logger.info(f"📚 延后处理 {len(classified_gaps['historical'])} 个历史缺口")
            # 可以选择立即处理或放入队列稍后处理
            historical_results = await self._fill_gaps_batch(classified_gaps['historical'][:5])  # 限制数量
            results['historical_completed'] = historical_results['completed']
            results['total_records_filled'] += historical_results['records_filled']
            results['errors'].extend(historical_results['errors'])

        return results

    async def _fill_gaps_batch(self, gaps: List[PreciseGap]) -> Dict[str, any]:
        """批量填充缺口 - 复用现有的API调用逻辑"""
        results = {
            'completed': 0,
            'records_filled': 0,
            'errors': []
        }

        for gap in gaps:
            try:
                # 调用现有的回填逻辑（复用原有的API调用代码）
                records_filled = await self._fill_single_gap(gap)

                results['completed'] += 1
                results['records_filled'] += records_filled

                logger.info(
                    f"✅ 填充完成: {gap.symbol} {gap.interval} "
                    f"[{gap.start_time.strftime('%m-%d %H:%M')} - "
                    f"{gap.end_time.strftime('%m-%d %H:%M')}] "
                    f"填充 {records_filled} 条记录"
                )

            except Exception as e:
                error_msg = f"填充失败: {gap.symbol} {gap.interval} - {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

        return results

    async def _fill_single_gap(self, gap: PreciseGap) -> int:
        """填充单个缺口 - 复用现有的Binance API逻辑"""
        # 这里复用原有data_backfill.py中的API调用代码
        # 保持所有现有的转换、错误处理、数据插入逻辑不变

        # 转换为Binance格式（复用原有逻辑）
        binance_symbol = self._convert_symbol_for_binance(gap.symbol)
        binance_interval = self._convert_interval_for_binance(gap.interval)

        start_ms = int(gap.start_time.timestamp() * 1000)
        end_ms = int(gap.end_time.timestamp() * 1000)

        total_inserted = 0
        current_start = start_ms

        while current_start < end_ms:
            # 计算当前批次的结束时间
            current_end = min(current_start + (7 * 24 * 60 * 60 * 1000), end_ms)  # 7天一批

            try:
                # 调用Binance API（复用原有逻辑）
                url = "https://fapi.binance.com/fapi/v1/klines"
                params = {
                    'symbol': binance_symbol,
                    'interval': binance_interval,
                    'startTime': current_start,
                    'endTime': current_end,
                    'limit': 1500
                }

                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()

                klines_data = response.json()

                if klines_data:
                    # 插入数据（复用原有的数据处理逻辑）
                    inserted = await self._insert_klines_data(
                        klines_data, gap.symbol, gap.interval
                    )
                    total_inserted += inserted

                # API限制延迟
                await asyncio.sleep(0.05)

                current_start = current_end + 1

            except Exception as e:
                logger.error(f"API调用失败: {e}")
                raise

        return total_inserted

    async def _insert_klines_data(
        self, klines_data: List, symbol: str, interval: str
    ) -> int:
        """插入K线数据 - 复用原有的数据插入逻辑"""
        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        try:
            insert_data = []
            for kline in klines_data:
                open_time = datetime.fromtimestamp(kline[0] / 1000)

                # 复用原有的数据结构
                data_dict = {
                    'timestamp': open_time,
                    'exchange': 'binance',
                    'symbol': symbol,
                    'interval': interval,
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                }

                # 数据标准化（复用原有逻辑）
                normalized_data = normalize_data(data_dict, 'candles')
                insert_data.append(list(normalized_data.values()))

            if insert_data:
                client.insert(
                    'candles',
                    insert_data,
                    column_names=list(normalized_data.keys()) if normalized_data else []
                )

            return len(insert_data)

        finally:
            client.close()

    def _convert_symbol_for_binance(self, symbol: str) -> str:
        """转换符号格式（复用原有逻辑）"""
        if symbol.endswith('-PERP'):
            return symbol.replace('-USDT-PERP', 'USDT').replace('-', '')
        return symbol.replace('-', '')

    def _convert_interval_for_binance(self, interval: str) -> str:
        """转换时间间隔格式（复用原有逻辑）"""
        mapping = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '4h', '1d': '1d'
        }
        return mapping.get(interval, interval)

    def _get_interval_minutes(self, interval: str) -> int:
        """获取时间间隔的分钟数"""
        mapping = {
            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '4h': 240, '1d': 1440
        }
        return mapping.get(interval, 60)

    async def _handle_scenario_preprocessing(self, scenario: str) -> Dict[str, any]:
        """场景特定的预处理"""
        scenario_info = {'scenario': scenario, 'timestamp': datetime.now()}

        if scenario == 'startup':
            # 重启场景: 获取最后一次检查时间
            last_check = await self._get_last_check_time()
            scenario_info['last_check_time'] = last_check
            scenario_info['downtime_duration'] = (datetime.now() - last_check).total_seconds() / 3600 if last_check else None
            logger.info(f"🔄 重启场景检测: 上次检查时间 {last_check}, 停机时长 {scenario_info['downtime_duration']:.1f}小时")

        elif scenario == 'network_recovery':
            # 网络恢复场景: 检查WebSocket连接状态
            network_status = await self._check_network_disruption_status()
            scenario_info['network_status'] = network_status
            logger.info(f"🌐 网络恢复检测: {network_status}")

        elif scenario == 'manual_check':
            # 手动检查场景: 检查人为数据变动
            data_anomalies = await self._detect_data_anomalies()
            scenario_info['data_anomalies'] = data_anomalies
            logger.info(f"🕵️ 手动检查场景: 发现 {len(data_anomalies)} 个数据异常")

        return scenario_info

    async def _detect_scenario_specific_gaps(
        self, scenario: str, scenario_info: Dict[str, any]
    ) -> List[PreciseGap]:
        """场景特定的缺口检测"""
        additional_gaps = []

        if scenario == 'startup':
            # 重启场景: 重点检查停机期间的缺口
            additional_gaps = await self._detect_startup_gaps(scenario_info)

        elif scenario == 'network_recovery':
            # 网络恢复场景: 检查网络中断期间的缺口
            additional_gaps = await self._detect_network_gaps(scenario_info)

        elif scenario == 'manual_check':
            # 手动检查场景: 检查人为操作导致的缺口
            additional_gaps = await self._detect_manual_deletion_gaps(scenario_info)

        logger.info(f"🎯 场景特定检测完成: {scenario} - 发现 {len(additional_gaps)} 个额外缺口")
        return additional_gaps

    async def _get_last_check_time(self) -> Optional[datetime]:
        """获取最后一次检查时间"""
        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        try:
            sql = "SELECT MAX(last_check_time) FROM backfill_status"
            result = client.query(sql)

            if result.result_rows and result.result_rows[0][0]:
                return result.result_rows[0][0]

            # 如果没有记录，返回24小时前
            return datetime.now() - timedelta(hours=24)

        finally:
            client.close()

    async def _check_network_disruption_status(self) -> Dict[str, any]:
        """检查网络中断状态"""
        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        try:
            # 检查实时监控表中的连续缺口数
            sql = """
                SELECT symbol, interval, consecutive_gaps, last_websocket_time, status
                FROM real_time_monitor
                WHERE consecutive_gaps > 2 OR status != 'normal'
            """

            result = client.query(sql)

            disrupted_sources = []
            for row in result.result_rows:
                disrupted_sources.append({
                    'symbol': row[0],
                    'interval': row[1],
                    'consecutive_gaps': row[2],
                    'last_websocket_time': row[3],
                    'status': row[4]
                })

            return {
                'disrupted_count': len(disrupted_sources),
                'disrupted_sources': disrupted_sources
            }

        finally:
            client.close()

    async def _detect_data_anomalies(self) -> List[Dict[str, any]]:
        """检测数据异常（可能的手动删除）"""
        client = clickhouse_connect.get_client(
            host=self.clickhouse_cfg['host'],
            port=self.clickhouse_cfg['port'],
            username=self.clickhouse_cfg['user'],
            password=self.clickhouse_cfg.get('password', ''),
            database=self.clickhouse_cfg['database']
        )

        anomalies = []

        try:
            symbols = self.config['symbols']['custom_list']
            intervals = list(self.config['data_retention']['candles'].keys())

            for symbol in symbols:
                for interval in intervals:
                    # 检查最近的数据密度是否异常
                    sql = """
                        SELECT
                            COUNT(*) as recent_count,
                            MIN(timestamp) as min_time,
                            MAX(timestamp) as max_time
                        FROM candles
                        WHERE symbol = {symbol:String}
                          AND interval = {interval:String}
                          AND timestamp >= {check_time:DateTime}
                    """

                    check_time = datetime.now() - timedelta(hours=6)
                    result = client.query(sql, {
                        'symbol': symbol,
                        'interval': interval,
                        'check_time': check_time
                    })

                    if result.result_rows:
                        count, min_time, max_time = result.result_rows[0]

                        if count > 0 and min_time and max_time:
                            # 计算期望的数据数量
                            interval_minutes = self._get_interval_minutes(interval)
                            expected_count = 6 * 60 / interval_minutes  # 6小时的期望数量

                            # 如果实际数量明显少于期望，可能是手动删除
                            if count < expected_count * 0.7:  # 低于70%认为异常
                                anomalies.append({
                                    'symbol': symbol,
                                    'interval': interval,
                                    'actual_count': count,
                                    'expected_count': int(expected_count),
                                    'missing_ratio': 1 - (count / expected_count),
                                    'time_range': {'start': min_time, 'end': max_time}
                                })

            return anomalies

        finally:
            client.close()

    async def _detect_startup_gaps(self, scenario_info: Dict[str, any]) -> List[PreciseGap]:
        """检测重启场景的缺口"""
        gaps = []
        last_check = scenario_info.get('last_check_time')

        if not last_check:
            return gaps

        # 重点检查停机期间的缺口
        symbols = self.config['symbols']['custom_list']
        intervals = list(self.config['data_retention']['candles'].keys())
        now = datetime.now()

        for symbol in symbols:
            for interval in intervals:
                # 检查停机期间是否有缺口
                gap = self._create_gap(
                    symbol, interval, last_check, now,
                    self._get_interval_minutes(interval)
                )

                # 重启缺口优先级提高
                gap.priority = min(10, gap.priority + 2)
                gaps.append(gap)

        logger.info(f"🔄 重启缺口检测: 停机期间 {last_check} - {now}, 发现 {len(gaps)} 个缺口")
        return gaps

    async def _detect_network_gaps(self, scenario_info: Dict[str, any]) -> List[PreciseGap]:
        """检测网络中断的缺口"""
        gaps = []
        network_status = scenario_info.get('network_status', {})

        for source in network_status.get('disrupted_sources', []):
            symbol = source['symbol']
            interval = source['interval']
            last_ws_time = source.get('last_websocket_time')

            if last_ws_time:
                # 从最后WebSocket时间到现在的缺口
                gap = self._create_gap(
                    symbol, interval, last_ws_time, datetime.now(),
                    self._get_interval_minutes(interval)
                )

                # 网络中断缺口优先级提高
                gap.priority = min(10, gap.priority + 1)
                gaps.append(gap)

        logger.info(f"🌐 网络中断缺口检测: 发现 {len(gaps)} 个缺口")
        return gaps

    async def _detect_manual_deletion_gaps(self, scenario_info: Dict[str, any]) -> List[PreciseGap]:
        """检测手动删除的缺口"""
        gaps = []
        anomalies = scenario_info.get('data_anomalies', [])

        for anomaly in anomalies:
            symbol = anomaly['symbol']
            interval = anomaly['interval']
            time_range = anomaly['time_range']

            # 为数据异常的时间范围创建缺口
            gap = self._create_gap(
                symbol, interval,
                time_range['start'], time_range['end'],
                self._get_interval_minutes(interval)
            )

            # 手动删除缺口优先级中等
            gap.priority = max(5, gap.priority)
            gaps.append(gap)

        logger.info(f"🕵️ 手动删除缺口检测: 发现 {len(gaps)} 个缺口")
        return gaps


# 供主程序调用的接口函数
async def smart_data_integrity_check(scenario: str = 'normal'):
    """
    智能数据完整性检查（供主程序调用）
    scenario: 'normal', 'startup', 'network_recovery', 'manual_check'
    """
    service = SmartDataBackfillService()
    return await service.smart_detect_and_fill(scenario)

# 场景特定的便捷接口
async def startup_data_check():
    """重启时的数据完整性检查"""
    return await smart_data_integrity_check('startup')

async def network_recovery_check():
    """网络恢复后的数据完整性检查"""
    return await smart_data_integrity_check('network_recovery')

async def manual_data_audit():
    """手动数据审计检查"""
    return await smart_data_integrity_check('manual_check')


if __name__ == "__main__":
    # 测试智能回填系统
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    async def test():
        result = await smart_data_integrity_check()
        print(f"智能回填完成: {result}")

    asyncio.run(test())