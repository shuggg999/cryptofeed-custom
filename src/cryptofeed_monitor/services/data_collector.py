#!/usr/bin/env python3
"""
数据收集服务
使用动态连接池收集所有币种的实时数据
"""
import asyncio
import logging
from typing import Dict, Any

from cryptofeed import FeedHandler
from cryptofeed.defines import *
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.backends.postgres import PostgreSQL

from ..config import config

logger = logging.getLogger(__name__)


class DataCollectionService:
    """数据收集服务"""

    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
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

                # 创建PostgreSQL后端
                postgres_config = {
                    'host': config.get('database.host', '127.0.0.1'),
                    'port': config.get('database.port', 5432),
                    'user': config.get('database.user', 'postgres'),
                    'password': config.get('database.password', 'password'),
                    'database': config.get('database.database', 'cryptofeed'),
                }

                # 创建FeedHandler
                fh = FeedHandler()

                # 添加币种和数据类型
                fh.add_feed(
                    BinanceFutures,
                    channels=[
                        TRADES, TICKER, FUNDING, L2_BOOK,
                        CANDLES_1m, CANDLES_5m, CANDLES_30m, CANDLES_4h, CANDLES_1d,
                        LIQUIDATIONS, OPEN_INTEREST, INDEX
                    ],
                    symbols=connection_symbols,
                    callbacks={
                        TRADES: PostgreSQL(**postgres_config, table='trades'),
                        TICKER: PostgreSQL(**postgres_config, table='ticker'),
                        FUNDING: PostgreSQL(**postgres_config, table='funding'),
                        L2_BOOK: PostgreSQL(**postgres_config, table='l2_book'),
                        CANDLES_1m: PostgreSQL(**postgres_config, table='candles_1m'),
                        CANDLES_5m: PostgreSQL(**postgres_config, table='candles_5m'),
                        CANDLES_30m: PostgreSQL(**postgres_config, table='candles_30m'),
                        CANDLES_4h: PostgreSQL(**postgres_config, table='candles_4h'),
                        CANDLES_1d: PostgreSQL(**postgres_config, table='candles_1d'),
                        LIQUIDATIONS: PostgreSQL(**postgres_config, table='liquidations'),
                        OPEN_INTEREST: PostgreSQL(**postgres_config, table='open_interest'),
                        INDEX: PostgreSQL(**postgres_config, table='index'),
                    }
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