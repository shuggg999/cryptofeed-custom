#!/usr/bin/env python3
"""
修复版本的Binance生产级监控系统
解决配置错误，确保数据正确写入
"""
import asyncio
import logging
import psycopg2
import signal
import sys
import time
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict
from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import (
    TradePostgres, FundingPostgres, CandlesPostgres, TickerPostgres
)
from cryptofeed.defines import *
from cryptofeed.exchanges import BinanceFutures

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/binance_fixed_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL连接配置
postgres_cfg = {
    'host': '127.0.0.1',
    'port': 5432,
    'user': 'postgres',
    'password': 'password',
    'database': 'cryptofeed'
}

# Cryptofeed PostgreSQL后端配置
cryptofeed_postgres_cfg = {
    'host': '127.0.0.1',
    'port': 5432,
    'user': 'postgres',
    'pw': 'password',
    'db': 'cryptofeed'
}

class BinanceFixedMonitor:
    """修复版本的Binance监控器"""

    def __init__(self):
        self.feed_handler = None
        self.symbols = []
        self.running = False
        self.start_time = datetime.now()

        # 统计计数器
        self.stats = {
            'trades': 0,
            'candles': 0,
            'funding': 0,
            'ticker': 0,
            'errors': 0,
            'last_trade_time': None,
            'last_candle_time': None,
            'last_funding_time': None,
            'last_ticker_time': None
        }

    async def get_binance_symbols(self):
        """获取币安USDT永续合约"""
        logger.info("🔍 获取币安USDT永续合约列表...")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://fapi.binance.com/fapi/v1/exchangeInfo') as response:
                    data = await response.json()

                    usdt_symbols = []
                    for symbol_info in data['symbols']:
                        if (symbol_info['status'] == 'TRADING' and
                            symbol_info['contractType'] == 'PERPETUAL' and
                            symbol_info['quoteAsset'] == 'USDT'):

                            symbol = f"{symbol_info['baseAsset']}-USDT-PERP"
                            usdt_symbols.append(symbol)

                    logger.info(f"📊 发现 {len(usdt_symbols)} 个USDT永续合约")
                    return usdt_symbols[:50]  # 限制50个合约进行测试

        except Exception as e:
            logger.error(f"❌ 获取合约列表失败: {e}")
            return []

    def init_database(self):
        """初始化数据库表"""
        logger.info("🗃️ 初始化数据库表...")

        try:
            conn = psycopg2.connect(**postgres_cfg)
            cursor = conn.cursor()

            # 创建trades表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    timestamp TIMESTAMPTZ NOT NULL,
                    receipt_timestamp TIMESTAMPTZ,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    price NUMERIC NOT NULL,
                    id TEXT,
                    PRIMARY KEY (timestamp, exchange, symbol, id)
                );
            """)

            # 创建funding表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS funding (
                    timestamp TIMESTAMPTZ NOT NULL,
                    receipt_timestamp TIMESTAMPTZ,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    rate NUMERIC,
                    next_funding_time TIMESTAMPTZ,
                    mark_price NUMERIC,
                    PRIMARY KEY (timestamp, exchange, symbol)
                );
            """)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("✅ 数据库表初始化完成")

        except Exception as e:
            logger.error(f"❌ 数据库初始化失败: {e}")

    def trade_callback(self, data, receipt_timestamp):
        """交易数据回调"""
        self.stats['trades'] += 1
        self.stats['last_trade_time'] = datetime.now()

        if self.stats['trades'] % 100 == 0:
            logger.info(f"💰 交易数据: 已处理 {self.stats['trades']} 条")

    def funding_callback(self, data, receipt_timestamp):
        """资金费率回调"""
        self.stats['funding'] += 1
        self.stats['last_funding_time'] = datetime.now()
        logger.info(f"💰 资金费率: {data.symbol} = {data.rate}")

    def candle_callback(self, data, receipt_timestamp):
        """K线数据回调"""
        self.stats['candles'] += 1
        self.stats['last_candle_time'] = datetime.now()

        if self.stats['candles'] % 50 == 0:
            logger.info(f"📊 K线数据: 已处理 {self.stats['candles']} 条")

    def ticker_callback(self, data, receipt_timestamp):
        """Ticker回调"""
        self.stats['ticker'] += 1
        self.stats['last_ticker_time'] = datetime.now()

        if self.stats['ticker'] % 100 == 0:
            logger.info(f"💹 Ticker: 已处理 {self.stats['ticker']} 条")

    def setup_monitoring(self):
        """设置监控配置"""
        logger.info("🔧 配置监控系统...")

        # 创建FeedHandler - 修复配置问题
        self.feed_handler = FeedHandler()  # 使用默认配置
        logger.info("🎯 FeedHandler创建成功")

        # 添加Binance Futures监控
        logger.info(f"📈 添加 {len(self.symbols)} 个合约监控")

        # 交易数据监控
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[TRADES],
                callbacks={
                    TRADES: [
                        TradePostgres(**postgres_cfg),
                        self.trade_callback
                    ]
                }
            )
        )

        # 资金费率监控
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[FUNDING],
                callbacks={
                    FUNDING: [
                        FundingPostgres(**postgres_cfg),
                        self.funding_callback
                    ]
                }
            )
        )

        # K线监控 - 先测试1分钟
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[CANDLES_1m],
                callbacks={
                    CANDLES_1m: [
                        CandlesPostgres(**postgres_cfg, table='candles_1m'),
                        self.candle_callback
                    ]
                }
            )
        )

        # Ticker监控
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[TICKER],
                callbacks={
                    TICKER: [
                        TickerPostgres(**postgres_cfg),
                        self.ticker_callback
                    ]
                }
            )
        )

        logger.info("✅ 监控配置完成")

    def show_stats(self):
        """显示统计信息"""
        runtime = datetime.now() - self.start_time

        logger.info("=" * 60)
        logger.info("📊 实时监控状态")
        logger.info("=" * 60)
        logger.info(f"运行时间: {runtime}")
        logger.info(f"监控合约: {len(self.symbols)} 个")
        logger.info(f"交易数据: {self.stats['trades']} 条")
        logger.info(f"K线数据: {self.stats['candles']} 条")
        logger.info(f"资金费率: {self.stats['funding']} 条")
        logger.info(f"Ticker: {self.stats['ticker']} 条")

        if self.stats['last_trade_time']:
            logger.info(f"最后交易: {self.stats['last_trade_time'].strftime('%H:%M:%S')}")
        if self.stats['last_candle_time']:
            logger.info(f"最后K线: {self.stats['last_candle_time'].strftime('%H:%M:%S')}")
        if self.stats['last_funding_time']:
            logger.info(f"最后资金费率: {self.stats['last_funding_time'].strftime('%H:%M:%S')}")

        logger.info("=" * 60)

    async def run(self):
        """运行监控系统"""
        logger.info("🚀 启动修复版Binance监控系统")
        logger.info("=" * 60)

        try:
            # 初始化数据库
            self.init_database()

            # 获取合约列表
            self.symbols = await self.get_binance_symbols()
            if not self.symbols:
                logger.error("❌ 无法获取合约列表")
                return

            # 设置监控
            self.setup_monitoring()

            # 启动监控
            self.running = True
            logger.info("📡 开始数据收集...")
            logger.info("⏹  按 Ctrl+C 停止")
            logger.info("=" * 60)

            # 启动后台统计任务
            stats_task = asyncio.create_task(self.stats_loop())

            # 运行FeedHandler
            await self.feed_handler.run()

        except Exception as e:
            logger.error(f"❌ 监控系统错误: {e}")
            raise
        finally:
            self.running = False
            logger.info("🛑 监控系统停止")

    async def stats_loop(self):
        """统计信息循环"""
        while self.running:
            await asyncio.sleep(30)  # 每30秒显示统计
            if self.running:
                self.show_stats()

    def stop(self):
        """停止监控"""
        self.running = False
        if self.feed_handler:
            asyncio.create_task(self.feed_handler.stop())

# 信号处理
monitor = None

def signal_handler(signum, frame):
    logger.info(f"收到信号 {signum}，正在停止...")
    if monitor:
        monitor.stop()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def main():
    """主函数"""
    global monitor
    monitor = BinanceFixedMonitor()

    try:
        await monitor.run()
    except KeyboardInterrupt:
        logger.info("收到中断信号")
    except Exception as e:
        logger.error(f"系统错误: {e}")
        return 1

    return 0

if __name__ == '__main__':
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)