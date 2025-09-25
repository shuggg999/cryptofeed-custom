#!/usr/bin/env python3
"""
数据收集服务
使用动态连接池收集所有币种的实时数据
"""
import asyncio
import logging
from typing import Dict, Any

from cryptofeed import FeedHandler
from cryptofeed.defines import (
    TRADES, TICKER, FUNDING, L2_BOOK, LIQUIDATIONS, OPEN_INTEREST, INDEX, CANDLES
)
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.backends.clickhouse import (
    TradeClickHouse, FundingClickHouse, CandlesClickHouse, TickerClickHouse
)

from ..config import config
from .connection_pool import DynamicConnectionPool

logger = logging.getLogger(__name__)


class DataCollectionService:
    """数据收集服务"""

    def __init__(self):
        self.connection_pool = DynamicConnectionPool()
        self.feed_handlers = []
        self.running = False

    async def start(self):
        """启动数据收集"""
        logger.info("📡 启动数据收集服务")

        try:
            # 获取所有币种
            symbols = await self.connection_pool.symbol_discovery.get_all_usdt_symbols()
            logger.info(f"📊 准备收集 {len(symbols)} 个合约的数据")

            # 计算所需连接数
            required_connections = self.connection_pool.calculate_required_connections(len(symbols))
            logger.info(f"🔗 创建 {required_connections} 个动态连接")

            # 分配币种到不同连接
            symbol_distributions = self.connection_pool.distribute_symbols(symbols, required_connections)

            # 创建Feed处理器
            for i, connection_symbols in enumerate(symbol_distributions, 1):
                if not connection_symbols:
                    continue

                logger.info(f"连接{i}: 处理 {len(connection_symbols)} 个合约")

                # 创建ClickHouse配置
                clickhouse_cfg = {
                    'host': config.get('clickhouse.host', 'localhost'),
                    'port': config.get('clickhouse.port', 8123),
                    'user': config.get('clickhouse.user', 'default'),
                    'password': config.get('clickhouse.password', 'password123'),
                    'database': config.get('clickhouse.database', 'cryptofeed'),
                }

                # 创建FeedHandler
                fh = FeedHandler()

                # 添加交易数据监控
                fh.add_feed(
                    BinanceFutures(
                        symbols=connection_symbols,
                        channels=[TRADES],
                        callbacks={
                            TRADES: [TradeClickHouse(**clickhouse_cfg)]
                        }
                    )
                )

                # 添加资金费率监控
                fh.add_feed(
                    BinanceFutures(
                        symbols=connection_symbols,
                        channels=[FUNDING],
                        callbacks={
                            FUNDING: [FundingClickHouse(**clickhouse_cfg)]
                        }
                    )
                )

                # 添加K线监控 - 分别为每个时间周期创建
                intervals = ['1m', '5m', '30m', '4h', '1d']
                for interval in intervals:
                    table_name = 'candles'  # 统一使用candles表
                    fh.add_feed(
                        BinanceFutures(
                            symbols=connection_symbols,
                            channels=[CANDLES],
                            callbacks={
                                CANDLES: [CandlesClickHouse(table=table_name, **clickhouse_cfg)]
                            },
                            candle_interval=interval
                        )
                    )

                self.feed_handlers.append(fh)

            # 启动所有连接
            self.running = True
            logger.info(f"🚀 启动 {len(self.feed_handlers)} 个数据收集连接")

            # 并发启动所有FeedHandler
            tasks = []
            for i, fh in enumerate(self.feed_handlers, 1):
                logger.info(f"🔄 启动连接 {i}")
                task = asyncio.create_task(fh.run())
                tasks.append(task)

            # 等待所有任务
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"❌ 数据收集启动失败: {e}")
            await self.stop()
            raise

    async def stop(self):
        """停止数据收集"""
        logger.info("🛑 停止数据收集服务")
        self.running = False

        for fh in self.feed_handlers:
            try:
                await fh.stop()
            except Exception as e:
                logger.error(f"停止Feed处理器失败: {e}")

        self.feed_handlers.clear()
        logger.info("✅ 数据收集服务已停止")

    def get_stats(self) -> Dict[str, Any]:
        """获取收集统计"""
        return {
            'running': self.running,
            'active_connections': len(self.feed_handlers),
            'connection_pool_stats': self.connection_pool.get_stats()
        }