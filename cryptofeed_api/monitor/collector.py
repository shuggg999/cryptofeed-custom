#!/usr/bin/env python3
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import clickhouse_connect

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import asyncio
from datetime import datetime

from cryptofeed import FeedHandler
from cryptofeed.backends.clickhouse import (  # TickerClickHouse,
    CandlesClickHouse,
    FundingClickHouse,
    LiquidationsClickHouse,
    OpenInterestClickHouse,
    TradeClickHouse,
)
from cryptofeed.defines import CANDLES, FUNDING, LIQUIDATIONS, OPEN_INTEREST, TRADES  # TICKER removed
from cryptofeed.exchanges import BinanceFutures

# Import retry manager for error handling
from ..core.retry_manager import API_RETRY_CONFIG, retry_manager, safe_execute, with_retry

# Import config and symbol manager
from .config import config
from .symbol_manager import symbol_manager

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/cryptofeed_advanced.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ClickHouse config from configuration file
clickhouse_cfg = {
    "host": config.get("clickhouse.host", "localhost"),
    "port": config.get("clickhouse.port", 8123),
    "user": config.get("clickhouse.user", "default"),
    "password": config.get("clickhouse.password", "password123"),
    "database": config.get("clickhouse.database", "cryptofeed"),
    "secure": config.get("clickhouse.secure", False),
}

# Monitor config from configuration file
INTERVALS = ["1m", "5m", "30m", "4h", "1d"]


class RateLimitedFundingClickHouse:
    """Rate limited funding backend that saves at most once per minute per symbol"""

    def __init__(self, **clickhouse_cfg):
        self.clickhouse_cfg = clickhouse_cfg
        self.last_save_times = {}  # {symbol: last_save_timestamp}
        self.save_interval = 60  # 60 seconds = 1.txt minute

    async def __call__(self, funding, receipt_timestamp):
        """Called by cryptofeed when funding data arrives"""
        try:
            current_time = time.time()
            symbol = funding.symbol

            # Check if we should save this update (1 minute interval)
            if symbol not in self.last_save_times or current_time - self.last_save_times[symbol] >= self.save_interval:

                # Determine if this is a settlement time (00:00, 08:00, 16:00 UTC)
                timestamp_dt = datetime.fromtimestamp(funding.timestamp) if funding.timestamp else datetime.now()
                hour = timestamp_dt.hour
                minute = timestamp_dt.minute
                # Consider it settlement time if within 1 minute of 00:00, 08:00, or 16:00 UTC
                is_settlement = hour in [0, 8, 16] and minute <= 1

                # Save to database
                await self._save_to_database(funding, receipt_timestamp, is_settlement)

                # Update last save time
                self.last_save_times[symbol] = current_time

                # Log with settlement indicator
                settlement_flag = "🔔 SETTLEMENT" if is_settlement else ""
                logger.info(f"💰 Funding saved: {symbol} Rate: {funding.rate:.6f} {settlement_flag}")

        except Exception as e:
            logger.error(f"Rate limited funding backend error: {e}")

    async def _save_to_database(self, funding, receipt_timestamp, is_settlement):
        """Save funding data to PostgreSQL database"""
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._sync_save, funding, receipt_timestamp, is_settlement)
        except Exception as e:
            logger.error(f"Database save error: {e}")

    def _sync_save(self, funding, receipt_timestamp, is_settlement):
        """Synchronous database save"""
        try:
            client = clickhouse_connect.get_client(
                host=self.clickhouse_cfg["host"],
                port=self.clickhouse_cfg["port"],
                user=self.clickhouse_cfg["user"],
                password=self.clickhouse_cfg["password"],
                database=self.clickhouse_cfg["database"],
            )

            # Prepare data for ClickHouse insertion (match table schema order)
            # Schema: timestamp, exchange, symbol, rate, mark_price, next_funding_time, predicted_rate, receipt_timestamp, date, is_settlement

            timestamp_dt = datetime.fromtimestamp(funding.timestamp) if funding.timestamp else datetime.now()

            data = [
                timestamp_dt,
                funding.exchange,
                funding.symbol,
                float(funding.rate) if funding.rate else 0.0,
                float(funding.mark_price) if hasattr(funding, "mark_price") and funding.mark_price else 0.0,
                datetime.fromtimestamp(funding.next_funding_time) if funding.next_funding_time else datetime.now(),
                float(funding.predicted_rate) if hasattr(funding, "predicted_rate") and funding.predicted_rate else 0.0,
                datetime.fromtimestamp(receipt_timestamp) if receipt_timestamp else datetime.now(),
                timestamp_dt.date(),
                1 if is_settlement else 0,
            ]

            # Column names for funding table: timestamp, exchange, symbol, rate, mark_price, next_funding_time, predicted_rate, receipt_timestamp, date, is_settlement
            columns = [
                "timestamp",
                "exchange",
                "symbol",
                "rate",
                "mark_price",
                "next_funding_time",
                "predicted_rate",
                "receipt_timestamp",
                "date",
                "is_settlement",
            ]
            client.insert("funding", [data], column_names=columns)
            client.close()

        except Exception as e:
            logger.error(f"Sync database save error: {e}")


class SmartTradeClickHouse:
    """智能Trades后端 - 动态分层阈值 + 7天自动清理"""

    def __init__(self, **clickhouse_cfg):
        self.clickhouse_cfg = clickhouse_cfg
        self.last_save_times = {}  # {symbol: last_save_timestamp}
        self.last_prices = {}  # {symbol: last_price} 用于价格变化检测

        # 动态分层配置
        self.symbol_tiers = {}  # {symbol: tier_info}
        self.tier_update_interval = 24 * 3600  # 24小时更新一次分层
        self.last_tier_update = 0

        # 分层参数
        self.tier_percentiles = [0.02, 0.10, 0.30]  # 2%, 10%, 30%
        self.threshold_multipliers = [2.0, 1.8, 1.5, 1.2]  # 各层阈值倍数
        self.time_intervals = [300, 600, 1200, 0]  # 各层时间间隔(秒) 0=禁用
        self.price_change_thresholds = [0.01, 0.008, 0.006, 0.005]  # 各层价格变化阈值

        # 自动清理配置 - 从配置文件读取不同数据类型保留期
        self.cleanup_interval = 3600  # 清理间隔 (1小时)
        self.last_cleanup_time = 0

        # 从配置文件读取数据保留策略
        from .config import config

        retention_config = config.get("data_retention", {})
        self.trades_retention_days = retention_config.get("trades", 90)

        logger.info(f"📋 Data retention policy - Trades: {self.trades_retention_days} days")

        # 统计信息
        self.stats = {
            "total_received": 0,
            "large_trades_saved": 0,
            "price_change_saved": 0,
            "time_interval_saved": 0,
            "filtered_out": 0,
            "tier_updates": 0,
        }

    async def __call__(self, trade, receipt_timestamp):
        """主要筛选逻辑 - 基于动态分层"""
        try:
            current_time = time.time()
            symbol = trade.symbol
            trade_value = float(trade.amount) * float(trade.price)

            self.stats["total_received"] += 1

            # 检查是否需要更新分层
            await self._check_and_update_tiers()

            # 获取该合约的分层配置
            tier_info = self.symbol_tiers.get(symbol, self._get_default_tier_info())

            should_save = False
            save_reason = ""

            # 条件1: 大额交易立即保存 (使用动态阈值)
            if trade_value >= tier_info["threshold"]:
                should_save = True
                save_reason = "large_trade"
                self.stats["large_trades_saved"] += 1

            # 条件2: 价格显著变化立即保存 (使用动态阈值)
            elif self._is_price_change_significant(symbol, float(trade.price), tier_info["price_change_threshold"]):
                should_save = True
                save_reason = "price_change"
                self.stats["price_change_saved"] += 1

            # 条件3: 时间间隔保存（保证价格连续性，使用动态间隔）
            elif tier_info["time_interval"] > 0 and (
                symbol not in self.last_save_times
                or current_time - self.last_save_times[symbol] >= tier_info["time_interval"]
            ):
                should_save = True
                save_reason = "time_interval"
                self.stats["time_interval_saved"] += 1

            if should_save:
                # 保存到数据库
                await self._save_to_database(trade, receipt_timestamp)

                # 更新缓存
                self.last_save_times[symbol] = current_time
                self.last_prices[symbol] = float(trade.price)

                logger.info(f"💰 Trade saved [{save_reason}]: {symbol} ${trade_value:.2f} @ {trade.price}")
            else:
                self.stats["filtered_out"] += 1

            # 检查是否需要自动清理
            await self._auto_cleanup_check()

        except Exception as e:
            logger.error(f"Smart trade backend error: {e}")

    def _is_price_change_significant(self, symbol, current_price, threshold):
        """检测价格变化是否显著 (使用动态阈值)"""
        if symbol not in self.last_prices:
            self.last_prices[symbol] = current_price
            return False

        last_price = self.last_prices[symbol]
        price_change = abs(current_price - last_price) / last_price

        if price_change >= threshold:
            self.last_prices[symbol] = current_price
            return True

        return False

    async def _save_to_database(self, trade, receipt_timestamp):
        """保存交易数据到PostgreSQL数据库"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._sync_save, trade, receipt_timestamp)
        except Exception as e:
            logger.error(f"Database save error: {e}")

    def _sync_save(self, trade, receipt_timestamp):
        """同步数据库保存"""
        try:
            client = clickhouse_connect.get_client(
                host=self.clickhouse_cfg["host"],
                port=self.clickhouse_cfg["port"],
                user=self.clickhouse_cfg["user"],
                password=self.clickhouse_cfg["password"],
                database=self.clickhouse_cfg["database"],
            )

            # Prepare data for ClickHouse insertion (match table schema order)
            # Schema: timestamp, exchange, symbol, side, amount, price, trade_id, receipt_timestamp, date
            data = [
                datetime.fromtimestamp(trade.timestamp) if trade.timestamp else datetime.now(),
                trade.exchange,
                trade.symbol,
                trade.side,
                float(trade.amount),
                float(trade.price),
                str(trade.id) if hasattr(trade, "id") and trade.id else "",
                datetime.fromtimestamp(receipt_timestamp) if receipt_timestamp else datetime.now(),
                datetime.fromtimestamp(trade.timestamp).date() if trade.timestamp else datetime.now().date(),
            ]

            columns = [
                "timestamp",
                "exchange",
                "symbol",
                "side",
                "amount",
                "price",
                "trade_id",
                "receipt_timestamp",
                "date",
            ]
            client.insert("trades", [data], column_names=columns)
            client.close()

        except Exception as e:
            logger.error(f"Sync database save error: {e}")

    async def _auto_cleanup_check(self):
        """自动清理检查 - 每小时清理超过7天的数据"""
        current_time = time.time()

        if current_time - self.last_cleanup_time >= self.cleanup_interval:
            self.last_cleanup_time = current_time
            await self._cleanup_old_trades()

    async def _cleanup_old_trades(self):
        """清理超过7天的trades数据"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._sync_cleanup)
        except Exception as e:
            logger.error(f"Auto cleanup failed: {e}")

    def _sync_cleanup(self):
        """同步清理旧数据 - ClickHouse使用TTL自动清理，此方法改为检查TTL状态"""
        try:
            client = clickhouse_connect.get_client(
                host=self.clickhouse_cfg["host"],
                port=self.clickhouse_cfg["port"],
                user=self.clickhouse_cfg["user"],
                password=self.clickhouse_cfg["password"],
                database=self.clickhouse_cfg["database"],
            )

            # 检查TTL清理状态（ClickHouse会自动清理）
            result = client.query(
                f"SELECT count() FROM trades WHERE timestamp < now() - INTERVAL {self.trades_retention_days} DAY"
            )
            old_count = result.result_rows[0][0] if result.result_rows else 0

            if old_count == 0:
                logger.info(f"✅ TTL cleanup working: No data older than {self.trades_retention_days} days found")
            else:
                logger.info(
                    f"⏳ TTL cleanup pending: {old_count:,} records older than {self.trades_retention_days} days (will be auto-cleaned)"
                )

            client.close()

        except Exception as e:
            logger.error(f"TTL status check failed: {e}")

    def print_stats(self):
        """打印统计信息"""
        total = self.stats["total_received"]
        if total > 0:
            logger.info("📊 Smart Trade Backend Stats:")
            logger.info(f"Total received: {total:,}")
            logger.info(
                f"Large trades saved: {self.stats['large_trades_saved']:,} ({self.stats['large_trades_saved']/total*100:.1f}%)"
            )
            logger.info(
                f"Price change saved: {self.stats['price_change_saved']:,} ({self.stats['price_change_saved']/total*100:.1f}%)"
            )
            logger.info(
                f"Time interval saved: {self.stats['time_interval_saved']:,} ({self.stats['time_interval_saved']/total*100:.1f}%)"
            )
            logger.info(f"Filtered out: {self.stats['filtered_out']:,} ({self.stats['filtered_out']/total*100:.1f}%)")
            logger.info(f"Tier updates: {self.stats['tier_updates']:,}")

    def _get_default_tier_info(self):
        """获取默认分层配置（用于新合约）"""
        return {
            "tier": 3,  # 低活跃层
            "threshold": 2000.0,
            "time_interval": 0,  # 禁用时间间隔
            "price_change_threshold": 0.005,
        }

    async def _check_and_update_tiers(self):
        """检查并更新分层配置"""
        current_time = time.time()
        if current_time - self.last_tier_update >= self.tier_update_interval:
            await self._update_symbol_tiers()
            self.last_tier_update = current_time
            self.stats["tier_updates"] += 1

    async def _update_symbol_tiers(self):
        """更新合约分层配置"""
        try:
            # 收集最近7天的统计数据
            symbol_stats = await self._collect_symbol_stats(days=7)

            if not symbol_stats:
                logger.warning("No symbol stats available for tier update")
                return

            # 计算综合评分并排序
            symbol_scores = []
            for symbol, stats in symbol_stats.items():
                score = self._calculate_symbol_score(stats)
                symbol_scores.append((symbol, score, stats))

            symbol_scores.sort(key=lambda x: x[1], reverse=True)
            total_symbols = len(symbol_scores)

            # 动态分层
            tier_boundaries = []
            for p in self.tier_percentiles:
                tier_boundaries.append(int(total_symbols * p))
            tier_boundaries.append(total_symbols)

            # 更新分层配置
            new_tiers = {}
            current_idx = 0

            for tier_level in range(4):  # 4个层级
                end_idx = tier_boundaries[tier_level]
                tier_symbols = symbol_scores[current_idx:end_idx]

                if tier_symbols:
                    # 计算该层级的P90阈值
                    tier_p90_values = [stats["p90_trade_size"] for _, _, stats in tier_symbols]
                    median_p90 = sorted(tier_p90_values)[len(tier_p90_values) // 2] if tier_p90_values else 1000.0

                    threshold = median_p90 * self.threshold_multipliers[tier_level]

                    for symbol, score, stats in tier_symbols:
                        new_tiers[symbol] = {
                            "tier": tier_level,
                            "score": score,
                            "threshold": max(threshold, 500.0),  # 最小阈值$500
                            "time_interval": self.time_intervals[tier_level],
                            "price_change_threshold": self.price_change_thresholds[tier_level],
                        }

                current_idx = end_idx

            self.symbol_tiers = new_tiers

            # 记录分层更新信息
            tier_counts = {}
            for tier_info in new_tiers.values():
                tier = tier_info["tier"]
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

            logger.info(f"🔄 Updated dynamic tiers: {tier_counts}")

        except Exception as e:
            logger.error(f"Failed to update symbol tiers: {e}")

    def _calculate_symbol_score(self, stats):
        """计算合约活跃度评分"""
        import math

        # 各项指标权重
        weights = {"total_volume": 0.4, "trade_count": 0.3, "avg_trade_size": 0.2, "max_trade_size": 0.1}

        # 标准化分数 (使用对数缩放处理大数值)
        volume_score = math.log10(max(stats.get("total_volume", 1), 1))
        count_score = math.log10(max(stats.get("trade_count", 1), 1))
        avg_score = math.log10(max(stats.get("avg_trade_size", 1), 1))
        max_score = math.log10(max(stats.get("max_trade_size", 1), 1))

        # 综合评分
        composite_score = (
            volume_score * weights["total_volume"]
            + count_score * weights["trade_count"]
            + avg_score * weights["avg_trade_size"]
            + max_score * weights["max_trade_size"]
        )

        return composite_score

    async def _collect_symbol_stats(self, days=7):
        """收集合约统计数据"""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._sync_collect_stats, days)
        except Exception as e:
            logger.error(f"Failed to collect symbol stats: {e}")
            return {}

    def _sync_collect_stats(self, days):
        """同步收集统计数据"""
        try:
            client = clickhouse_connect.get_client(
                host=self.clickhouse_cfg["host"],
                port=self.clickhouse_cfg["port"],
                user=self.clickhouse_cfg["user"],
                password=self.clickhouse_cfg["password"],
                database=self.clickhouse_cfg["database"],
            )

            # 查询最近N天的统计数据 - 使用 toFloat64 避免 decimal 溢出
            query = f"""
                SELECT
                    symbol,
                    COUNT(*) as trade_count,
                    SUM(toFloat64(amount) * toFloat64(price)) as total_volume,
                    AVG(toFloat64(amount) * toFloat64(price)) as avg_trade_size,
                    MAX(toFloat64(amount) * toFloat64(price)) as max_trade_size,
                    quantile(0.9)(toFloat64(amount) * toFloat64(price)) as p90_trade_size
                FROM trades
                WHERE timestamp >= now() - INTERVAL {days} DAY
                    AND toFloat64(amount) * toFloat64(price) > 0
                GROUP BY symbol
                HAVING COUNT(*) >= 10
                ORDER BY total_volume DESC
            """

            result = client.query(query)
            client.close()

            symbol_stats = {}
            for row in result.result_rows:
                symbol, count, volume, avg_size, max_size, p90_size = row
                symbol_stats[symbol] = {
                    "trade_count": int(count),
                    "total_volume": float(volume),
                    "avg_trade_size": float(avg_size),
                    "max_trade_size": float(max_size),
                    "p90_trade_size": float(p90_size) if p90_size else 1000.0,
                }

            return symbol_stats

        except Exception as e:
            logger.error(f"Failed to sync collect stats: {e}")
            return {}


class BinanceAdvancedMonitor:
    """Advanced Binance Monitor - Full Scale"""

    def __init__(self):
        self.feed_handler = None
        self.symbols = []
        self.is_running = False
        self.start_time = None
        self.symbol_manager = symbol_manager

        # 集成健康监控和辅助服务
        self.health_monitor = None
        self.temp_data_manager = None

        # 集成重试管理器
        self.retry_manager = retry_manager

        # Statistics
        self.stats = {
            "trades_count": 0,
            "candles_count": 0,
            "funding_count": 0,
            # 'ticker_count': 0,  # TICKER removed
            "liquidations_count": 0,
            "open_interest_count": 0,
            "last_trade_time": None,
            "last_candle_time": None,
            "last_funding_time": None,
            "last_liquidation_time": None,
            "last_open_interest_time": None,
            "errors": 0,
        }

        # Auto cleanup task - 从配置读取不同数据类型的保留期
        self.last_cleanup_time = 0
        self.cleanup_interval = 3600  # Clean up every hour

        # 从配置文件读取数据保留策略
        retention_config = config.get("data_retention", {})
        self.funding_retention_days = retention_config.get("funding", 365)
        self.liquidations_retention_days = retention_config.get("liquidations", 180)
        self.open_interest_retention_days = retention_config.get("open_interest", 365)

        logger.info(
            f"📋 Data retention policy - Funding: {self.funding_retention_days} days, "
            f"Liquidations: {self.liquidations_retention_days} days, "
            f"Open Interest: {self.open_interest_retention_days} days"
        )

        # Set up symbol change callbacks
        self.symbol_manager.set_callbacks(on_added=self.on_symbols_added, on_removed=self.on_symbols_removed)

    async def initialize_auxiliary_services(self):
        """初始化辅助服务"""
        logger.info("🔧 Initializing auxiliary services...")

        try:
            # 初始化健康监控服务
            if config.get("monitoring.metrics_enabled", True):
                from .services.health_monitor import HealthMonitor

                self.health_monitor = HealthMonitor()
                logger.info(f"✅ Health monitor started on port {self.health_monitor.health_port}")

            # 初始化临时数据管理器
            from ..services.temp_data_manager import temp_data_manager

            await temp_data_manager.start()
            self.temp_data_manager = temp_data_manager
            logger.info("✅ Temporary data manager started")

        except Exception as e:
            logger.error(f"Failed to initialize auxiliary services: {e}")
            raise

    async def initialize_symbols(self) -> List[str]:
        """Initialize symbols using dynamic symbol manager"""
        logger.info("🔧 Initializing symbol management...")

        # Initialize symbol manager
        symbols = await self.symbol_manager.initialize()

        logger.info(f"🎯 Will monitor {len(symbols)} contracts")
        logger.info(f"📋 Symbol selection mode: {self.symbol_manager.mode}")

        return symbols

    async def on_symbols_added(self, added_symbols: List[str]):
        """Callback when new symbols are added"""
        logger.info(f"➕ New symbols detected: {added_symbols}")
        # In a production system, you would dynamically add feeds here
        # For now, we'll just log the change

    async def on_symbols_removed(self, removed_symbols: List[str]):
        """Callback when symbols are removed"""
        logger.info(f"➖ Symbols removed: {removed_symbols}")
        # In a production system, you would stop feeds for these symbols
        # For now, we'll just log the change

    async def trade_callback(self, trade, receipt_time):
        """Trade data callback"""
        try:
            self.stats["trades_count"] += 1
            self.stats["last_trade_time"] = datetime.now()

            if self.stats["trades_count"] % 1000 == 0:
                logger.info(f"📈 Received {self.stats['trades_count']} trade records")

        except Exception as e:
            self.stats["errors"] += 1
            # 使用重试管理器记录错误统计
            from ..core.retry_manager import error_handler

            error_handler.handle_error(
                e, {"callback_type": "trade", "symbol": getattr(trade, "symbol", "unknown"), "timestamp": receipt_time}
            )
            logger.error(f"Trade callback error: {e}")

    async def candle_callback(self, candle, receipt_time):
        """Candle data callback"""
        try:
            self.stats["candles_count"] += 1
            self.stats["last_candle_time"] = datetime.now()

            logger.info(
                f"📊 Candle[{candle.interval}]: {candle.symbol} | Close: {candle.close} | Volume: {candle.volume}"
            )

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Candle callback error: {e}")

    async def funding_callback(self, funding, receipt_time):
        """Funding rate callback - statistics only"""
        try:
            # Update statistics (this callback is called for every funding update, but saves are rate-limited in backend)
            self.stats["funding_count"] += 1
            self.stats["last_funding_time"] = datetime.now()

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Funding callback error: {e}")

        # Check if need to run cleanup
        await self.auto_cleanup_check()

    async def liquidation_callback(self, liquidation, receipt_time):
        """Liquidation callback - statistics only"""
        try:
            # Update statistics
            self.stats["liquidations_count"] += 1
            self.stats["last_liquidation_time"] = datetime.now()

            # Log important liquidations (>$10K USD)
            usd_value = float(liquidation.quantity) * float(liquidation.price)
            if usd_value >= 10000:
                logger.info(
                    f"🔥 Large liquidation: {liquidation.symbol} ${usd_value:,.2f} @ {liquidation.price} ({liquidation.side})"
                )

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Liquidation callback error: {e}")

    async def open_interest_callback(self, open_interest, receipt_time):
        """Open interest callback - statistics only"""
        try:
            # Update statistics
            self.stats["open_interest_count"] += 1
            self.stats["last_open_interest_time"] = datetime.now()

            # Log every 100th update to avoid spam
            if self.stats["open_interest_count"] % 100 == 0:
                logger.info(
                    f"📊 Open Interest update #{self.stats['open_interest_count']}: {open_interest.symbol} = {open_interest.open_interest:,}"
                )

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Open interest callback error: {e}")

    async def auto_cleanup_check(self):
        """Auto cleanup check - runs every hour"""
        current_time = time.time()

        if current_time - self.last_cleanup_time >= self.cleanup_interval:
            self.last_cleanup_time = current_time
            await self.cleanup_old_funding_data()

    async def cleanup_old_funding_data(self):
        """Clean up old funding data automatically"""
        try:
            client = clickhouse_connect.get_client(
                host=clickhouse_cfg["host"],
                port=clickhouse_cfg["port"],
                user=clickhouse_cfg["user"],
                password=clickhouse_cfg["password"],
                database=clickhouse_cfg["database"],
            )

            # 检查TTL清理状态（ClickHouse会自动清理）
            result = client.query(
                f"SELECT count() FROM funding WHERE timestamp < now() - INTERVAL {self.funding_retention_days} DAY"
            )
            old_count = result.result_rows[0][0] if result.result_rows else 0

            if old_count == 0:
                logger.info(
                    f"✅ TTL cleanup working: No funding data older than {self.funding_retention_days} days found"
                )
            else:
                logger.info(
                    f"⏳ TTL cleanup pending: {old_count:,} funding records older than {self.funding_retention_days} days (will be auto-cleaned)"
                )

            client.close()

        except Exception as e:
            logger.error(f"Auto cleanup failed: {e}")

    def cleanup_old_funding_data_sync(self):
        """Clean up old funding data synchronously"""
        try:
            client = clickhouse_connect.get_client(
                host=clickhouse_cfg["host"],
                port=clickhouse_cfg["port"],
                user=clickhouse_cfg["user"],
                password=clickhouse_cfg["password"],
                database=clickhouse_cfg["database"],
            )

            # 检查TTL清理状态（ClickHouse会自动清理）
            result = client.query(
                f"SELECT count() FROM funding WHERE timestamp < now() - INTERVAL {self.funding_retention_days} DAY"
            )
            old_count = result.result_rows[0][0] if result.result_rows else 0

            if old_count == 0:
                logger.info(
                    f"✅ Initial TTL check: No funding data older than {self.funding_retention_days} days found"
                )
            else:
                logger.info(
                    f"⏳ Initial TTL check: {old_count:,} funding records older than {self.funding_retention_days} days (will be auto-cleaned)"
                )

            client.close()

        except Exception as e:
            logger.error(f"Initial cleanup failed: {e}")

    async def setup_monitoring(self):
        """Setup monitoring configuration"""
        logger.info("🔧 Configuring advanced monitoring system...")

        # Initialize auxiliary services first
        await self.initialize_auxiliary_services()

        # Initialize symbols using dynamic symbol manager
        self.symbols = await self.initialize_symbols()

        # Create FeedHandler
        config = {
            "log": {"filename": "logs/cryptofeed_advanced.log", "level": "WARNING", "disabled": False},
            "backend_multiprocessing": True,
            "uvloop": True,
        }

        self.feed_handler = FeedHandler(config=config)

        logger.info("🎯 Using advanced connection mode")

        # Create feeds for each interval
        for interval in INTERVALS:
            table_name = "candles"  # 统一使用candles表
            logger.info(f"Adding {interval} candle monitoring: {len(self.symbols)} contracts")

            self.feed_handler.add_feed(
                BinanceFutures(
                    symbols=self.symbols,
                    channels=[CANDLES],
                    callbacks={CANDLES: [CandlesClickHouse(table=table_name, **clickhouse_cfg), self.candle_callback]},
                    candle_interval=interval,
                )
            )

        # Trade data monitoring - smart filtering (large trades + price changes + time intervals)
        logger.info(f"Adding smart trade data monitoring: {len(self.symbols)} contracts (intelligent filtering)")
        self.smart_trade_backend = SmartTradeClickHouse(**clickhouse_cfg)
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[TRADES],
                callbacks={TRADES: [self.smart_trade_backend, self.trade_callback]},
            )
        )

        # Funding rate monitoring - rate limited (1.txt minute per symbol)
        logger.info(f"Adding funding rate monitoring: {len(self.symbols)} contracts (1.txt minute intervals)")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[FUNDING],
                callbacks={FUNDING: [RateLimitedFundingClickHouse(**clickhouse_cfg), self.funding_callback]},
            )
        )

        # Liquidations monitoring - full data (critical events)
        logger.info(f"Adding liquidations monitoring: {len(self.symbols)} contracts (all liquidation events)")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[LIQUIDATIONS],
                callbacks={LIQUIDATIONS: [LiquidationsClickHouse(**clickhouse_cfg), self.liquidation_callback]},
            )
        )

        # Open Interest monitoring - 5 minute snapshots
        logger.info(f"Adding open interest monitoring: {len(self.symbols)} contracts (5 minute snapshots)")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[OPEN_INTEREST],
                callbacks={OPEN_INTEREST: [OpenInterestClickHouse(**clickhouse_cfg), self.open_interest_callback]},
            )
        )

    def print_stats(self):
        """Print statistics"""
        current_time = datetime.now()
        uptime = current_time - self.start_time if self.start_time else timedelta(0)

        logger.info("=" * 60)
        logger.info("📊 Advanced Binance Monitor Status")
        logger.info("=" * 60)
        logger.info(f"Uptime: {uptime}")
        logger.info(f"Monitored contracts: {len(self.symbols)}")
        logger.info(f"Monitored intervals: {len(INTERVALS)}")
        logger.info(f"Trade data: {self.stats['trades_count']} records")
        logger.info(f"Candle data: {self.stats['candles_count']} records")
        logger.info(f"Funding rate: {self.stats['funding_count']} records")
        logger.info(f"Liquidations: {self.stats['liquidations_count']} records")
        logger.info(f"Open Interest: {self.stats['open_interest_count']} records")
        logger.info(f"Error count: {self.stats['errors']}")

        # 打印重试管理器和错误处理器统计信息
        try:
            from ..core.retry_manager import error_handler

            retry_stats = self.retry_manager.get_stats()
            error_stats = error_handler.get_error_stats()

            if retry_stats["retry_stats"] or error_stats["total_errors"] > 0:
                logger.info("=" * 30)
                logger.info("🔄 Error & Retry Statistics")
                logger.info(f"Total errors handled: {error_stats['total_errors']}")
                logger.info(f"Unique error types: {error_stats['unique_errors']}")

                if retry_stats["retry_stats"]:
                    logger.info("📊 Retry statistics:")
                    for func_name, stats in list(retry_stats["retry_stats"].items())[:3]:  # 显示前3个
                        logger.info(
                            f"  {func_name}: {stats['success_count']}/{stats['total_calls']} success, avg attempts: {stats['avg_attempts']:.1f}"
                        )

                if error_stats["top_errors"]:
                    logger.info("🔝 Top errors:")
                    for error_key, count in error_stats["top_errors"][:3]:  # 显示前3个
                        logger.info(f"  {error_key}: {count} times")

        except Exception as e:
            logger.warning(f"Failed to get retry/error stats: {e}")

        if self.stats["last_trade_time"]:
            logger.info(f"Last trade: {self.stats['last_trade_time'].strftime('%H:%M:%S')}")
        if self.stats["last_candle_time"]:
            logger.info(f"Last candle: {self.stats['last_candle_time'].strftime('%H:%M:%S')}")
        if self.stats["last_funding_time"]:
            logger.info(f"Last funding: {self.stats['last_funding_time'].strftime('%H:%M:%S')}")
        if self.stats["last_liquidation_time"]:
            logger.info(f"Last liquidation: {self.stats['last_liquidation_time'].strftime('%H:%M:%S')}")
        if self.stats["last_open_interest_time"]:
            logger.info(f"Last open interest: {self.stats['last_open_interest_time'].strftime('%H:%M:%S')}")

        # Smart Trade Backend Statistics
        if hasattr(self, "smart_trade_backend"):
            self.smart_trade_backend.print_stats()

        logger.info("=" * 60)

    def signal_handler(self, signum, frame):
        """Signal handler"""
        logger.info(f"\nReceived signal {signum}, stopping safely...")
        self.is_running = False
        if self.feed_handler:
            self.feed_handler.stop()

    async def run_async(self):
        """Run monitoring system asynchronously"""
        logger.info("🚀 Starting Advanced Binance Full Scale Monitor")
        logger.info("=" * 60)

        try:
            # Configure monitoring
            await self.setup_monitoring()

            # Mark start time
            self.start_time = datetime.now()
            self.is_running = True

            # Perform initial cleanup (synchronous version)
            logger.info("🗑️ Performing initial cleanup of old data...")
            self.cleanup_old_funding_data_sync()

            logger.info("✅ Monitor configuration complete")
            logger.info(f"📅 Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("📡 Starting data streams...")
            logger.info("⏹  Press Ctrl+C to stop safely")
            logger.info("=" * 60)

            # Start symbol monitoring in background
            symbol_monitor_task = asyncio.create_task(self.symbol_manager.start_monitoring())

            # Start FeedHandler in background task
            feed_task = asyncio.create_task(self._run_feedhandler())

            # Keep running until stopped
            while self.is_running:
                await asyncio.sleep(1)

            # Stop tasks
            if not symbol_monitor_task.done():
                symbol_monitor_task.cancel()
            if not feed_task.done():
                feed_task.cancel()

        except KeyboardInterrupt:
            logger.info("User manual stop")
        except Exception as e:
            logger.error(f"Monitor system error: {e}")
            import traceback

            traceback.print_exc()
        finally:
            self.is_running = False
            # Cancel symbol monitoring
            if "symbol_monitor_task" in locals():
                symbol_monitor_task.cancel()
            logger.info("🔄 Performing final cleanup...")

            # Stop auxiliary services
            await self._cleanup_auxiliary_services()

            self.print_stats()
            logger.info("✅ Monitor system stopped safely")

    async def _cleanup_auxiliary_services(self):
        """清理辅助服务"""
        try:
            # Stop health monitor
            if self.health_monitor:
                self.health_monitor.stop()
                logger.info("✅ Health monitor stopped")

            # Stop temp data manager
            if self.temp_data_manager:
                await self.temp_data_manager.stop()
                logger.info("✅ Temp data manager stopped")

        except Exception as e:
            logger.error(f"Error during auxiliary services cleanup: {e}")

    def _sync_initialize(self):
        """Synchronous initialization for FeedHandler"""
        logger.info("🚀 Starting Advanced Binance Full Scale Monitor")
        logger.info("=" * 60)
        logger.info("🔧 Configuring advanced monitoring system...")

        # Initialize auxiliary services and symbols synchronously
        import asyncio

        loop = asyncio.new_event_loop()

        try:
            # Initialize auxiliary services
            loop.run_until_complete(self.initialize_auxiliary_services())

            # Initialize symbols
            logger.info("🔧 Initializing symbol management...")
            self.symbols = loop.run_until_complete(self.initialize_symbols())
        finally:
            loop.close()

        logger.info(f"🎯 Will monitor {len(self.symbols)} contracts")
        logger.info(f"📋 Symbol selection mode: {self.symbol_manager.mode}")

        # Create FeedHandler
        config = {
            "log": {"filename": "logs/cryptofeed_advanced.log", "level": "WARNING", "disabled": False},
            "backend_multiprocessing": True,
            "uvloop": True,
        }
        self.feed_handler = FeedHandler(config=config)

        # Configure monitoring feeds synchronously
        self._setup_feeds()

        # Perform initial cleanup
        logger.info("🗑️ Performing initial cleanup of old data...")
        # No cleanup method found - skipping

        # Set start time
        self.start_time = datetime.now()

        logger.info("✅ Monitor configuration complete")
        logger.info(f"📅 Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("📡 Starting data streams...")
        logger.info("⏹  Press Ctrl+C to stop safely")
        logger.info("=" * 60)

    def _setup_feeds(self):
        """Setup monitoring feeds synchronously"""
        logger.info("🎯 Using advanced connection mode")

        # Create feeds for each interval
        for interval in INTERVALS:
            table_name = "candles"  # 统一使用candles表
            logger.info(f"Adding {interval} candle monitoring: {len(self.symbols)} contracts")

            self.feed_handler.add_feed(
                BinanceFutures(
                    symbols=self.symbols,
                    channels=[CANDLES],
                    callbacks={CANDLES: [CandlesClickHouse(table=table_name, **clickhouse_cfg), self.candle_callback]},
                    candle_interval=interval,
                )
            )

        # Trade data monitoring - smart filtering
        logger.info(f"Adding smart trade data monitoring: {len(self.symbols)} contracts (intelligent filtering)")
        self.smart_trade_backend = SmartTradeClickHouse(**clickhouse_cfg)
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[TRADES],
                callbacks={TRADES: [self.smart_trade_backend, self.trade_callback]},
            )
        )

        # Funding rate monitoring - rate limited
        logger.info(f"Adding funding rate monitoring: {len(self.symbols)} contracts (1 minute intervals)")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[FUNDING],
                callbacks={FUNDING: [RateLimitedFundingClickHouse(**clickhouse_cfg), self.funding_callback]},
            )
        )

        # Liquidations monitoring - full data
        logger.info(f"Adding liquidations monitoring: {len(self.symbols)} contracts (all liquidation events)")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[LIQUIDATIONS],
                callbacks={LIQUIDATIONS: [LiquidationsClickHouse(**clickhouse_cfg), self.liquidation_callback]},
            )
        )

        # Open Interest monitoring - 5 minute snapshots
        logger.info(f"Adding open interest monitoring: {len(self.symbols)} contracts (5 minute snapshots)")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[OPEN_INTEREST],
                callbacks={OPEN_INTEREST: [OpenInterestClickHouse(**clickhouse_cfg), self.open_interest_callback]},
            )
        )

    def run(self):
        """Run monitoring system (wrapper for async)"""
        # Setup signal handling
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            self.is_running = True
            # Use synchronous initialization
            self._sync_initialize()

            # Run FeedHandler directly (it manages its own event loop)
            self.feed_handler.run()

        except KeyboardInterrupt:
            logger.info("User manual stop")
        except Exception as e:
            logger.error(f"Monitor system error: {e}")
            import traceback

            traceback.print_exc()
        finally:
            self.is_running = False
            logger.info("🔄 Performing final cleanup...")

            # Cleanup auxiliary services synchronously
            self._sync_cleanup_auxiliary_services()

            self.print_stats()
            logger.info("✅ Monitor system stopped safely")

    def _sync_cleanup_auxiliary_services(self):
        """同步清理辅助服务"""
        try:
            # Stop health monitor
            if self.health_monitor:
                self.health_monitor.stop()
                logger.info("✅ Health monitor stopped")

            # Stop temp data manager (run async in sync context)
            if self.temp_data_manager:
                import asyncio

                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(self.temp_data_manager.stop())
                    logger.info("✅ Temp data manager stopped")
                finally:
                    loop.close()

        except Exception as e:
            logger.error(f"Error during auxiliary services cleanup: {e}")

    async def _run_feedhandler(self):
        """在单独的线程中运行FeedHandler"""
        try:
            import asyncio
            import threading

            def run_feed():
                """在单独线程中运行FeedHandler的事件循环"""
                try:
                    # 在新线程中创建并设置 event loop（FeedHandler 需要）
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    # 运行 FeedHandler（禁用信号处理器，因为只能在主线程中注册）
                    self.feed_handler.run(install_signal_handlers=False)
                except Exception as e:
                    logger.error(f"FeedHandler error: {e}")

            # 在线程中运行FeedHandler
            feed_thread = threading.Thread(target=run_feed, daemon=True)
            feed_thread.start()

            logger.info("✅ FeedHandler started in background thread")

            # 等待直到停止
            while self.is_running and feed_thread.is_alive():
                await asyncio.sleep(1)

            # 停止FeedHandler
            if self.feed_handler:
                self.feed_handler.stop()

            logger.info("✅ FeedHandler stopped")

        except Exception as e:
            logger.error(f"Error running FeedHandler: {e}")


async def start_data_integrity_service():
    """启动数据完整性检查服务"""
    try:
        from ..services.data_integrity import DataIntegrityChecker
        from .symbol_manager import symbol_manager

        logger.info("🔍 Starting data integrity check service...")

        # 获取监控的交易对列表
        symbols = await symbol_manager.get_symbols()

        if not symbols:
            logger.warning("No active symbols found for integrity check")
            return

        # 创建数据完整性检查器
        integrity_checker = DataIntegrityChecker()

        # 运行完整性检查（仅检查最近3天的数据）
        results = await integrity_checker.run_integrity_check(
            symbols=symbols[:10],  # 限制检查前10个最活跃的合约
            check_candles=config.get("data_integrity.check_types.candles", True),
            check_trades=config.get("data_integrity.check_types.trades", False),
            check_funding=config.get("data_integrity.check_types.funding", True),
            lookback_days=config.get("data_integrity.lookback_days", 3),  # 从配置文件读取
        )

        # 统计检查结果
        total_gaps = 0
        for symbol, symbol_result in results.items():
            if "error" not in symbol_result:
                candle_gaps = sum(symbol_result.get("candle_gaps", {}).values())
                funding_gaps = symbol_result.get("funding_gaps", 0)
                total_gaps += candle_gaps + funding_gaps

        logger.info(f"✅ Data integrity check completed: {total_gaps} gaps found across {len(results)} symbols")

    except Exception as e:
        logger.error(f"Data integrity check failed: {e}")


async def start_backfill_service():
    """启动历史数据回填服务（如果配置启用）"""
    try:
        # 检查配置是否启用历史数据回填
        backfill_enabled = config.get("data_backfill.enabled", False)

        if not backfill_enabled:
            logger.info("📋 Historical data backfill disabled in configuration")
            return

        from ..services.data_backfill import DataBackfillService

        logger.info("🔄 Starting historical data backfill service...")

        # 从配置文件获取回填参数
        max_concurrent = config.get("data_backfill.max_concurrent_tasks", 2)
        default_lookback = config.get("data_backfill.default_lookback_days", 7)

        backfill_service = DataBackfillService(max_concurrent_tasks=max_concurrent)
        # 运行一次数据回填检查
        symbols = await symbol_manager.get_symbols()
        results = backfill_service.run_backfill_tasks(symbols[:5], lookback_days=default_lookback)

        if results and results.get("total_tasks", 0) > 0:
            successful = results.get("successful", 0)
            total_tasks = results.get("total_tasks", 0)
            total_records = results.get("records_added", 0)
            logger.info(
                f"✅ Backfill completed: {successful}/{total_tasks} tasks successful, {total_records} records added"
            )
        else:
            logger.info("📋 No data gaps detected for backfill")

    except Exception as e:
        logger.error(f"Historical data backfill failed: {e}")


def main():
    """Main function"""
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)

    monitor = BinanceAdvancedMonitor()

    try:
        # 检查是否启用数据完整性和回填功能
        # 重新启用修复后的服务
        integrity_enabled = config.get("data_integrity.enabled", True)
        backfill_enabled = config.get("data_backfill.enabled", False)

        # 改为异步并行执行：先启动实时监控，后台进行回填
        if integrity_enabled or backfill_enabled:
            logger.info("🔍 启动后台数据完整性检查和回填服务...")

            # 创建后台任务
            import asyncio
            import threading

            def run_background_services():
                """在后台线程中运行数据完整性检查和回填"""
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                try:
                    if integrity_enabled:
                        loop.run_until_complete(start_data_integrity_service())

                    if backfill_enabled:
                        loop.run_until_complete(start_backfill_service())
                except Exception as e:
                    logger.error(f"Background services error: {e}")
                finally:
                    loop.close()

            # 启动后台线程（非阻塞）
            background_thread = threading.Thread(target=run_background_services, daemon=True)
            background_thread.start()
            logger.info("✅ 后台服务已启动，同时启动实时监控...")

        # 立即启动实时监控系统（不等待回填完成）
        monitor.run()
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
