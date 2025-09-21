#!/usr/bin/env python3
"""
动态扩展生产监控
基于测试验证的动态连接池，实际运行200个合约（约2个连接）
"""
import asyncio
import logging
import signal
import sys
import psycopg2
import aiohttp
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from cryptofeed import FeedHandler
from cryptofeed.defines import *
from cryptofeed.exchanges import BinanceFutures

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dynamic_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DynamicCryptofeedMonitor:
    """动态扩展监控器"""

    def __init__(self):
        self.db_config = {
            'host': '127.0.0.1',
            'port': 5432,
            'user': 'postgres',
            'password': 'password',
            'database': 'cryptofeed'
        }
        self.feed_handlers = []
        self.running = False
        self.data_count = 0

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
                    return usdt_symbols[:200]  # 限制200个合约（约2个连接）

        except Exception as e:
            logger.error(f"❌ 获取合约列表失败: {e}")
            return []

    def init_database(self):
        """初始化数据库表"""
        logger.info("🗃️ 初始化数据库表...")

        try:
            conn = psycopg2.connect(**self.db_config)
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

    async def trade_callback(self, data, receipt_timestamp):
        """交易数据回调"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO trades (timestamp, receipt_timestamp, exchange, symbol, side, amount, price, id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, exchange, symbol, id) DO NOTHING
            """, (
                datetime.fromtimestamp(data.timestamp, tz=timezone.utc),
                datetime.fromtimestamp(receipt_timestamp, tz=timezone.utc),
                data.exchange,
                data.symbol,
                data.side,
                Decimal(str(data.amount)),
                Decimal(str(data.price)),
                data.id
            ))

            conn.commit()
            cursor.close()
            conn.close()

            self.data_count += 1
            if self.data_count % 1000 == 0:
                logger.info(f"💰 已处理 {self.data_count} 条交易数据")

        except Exception as e:
            logger.error(f"❌ 交易数据处理失败: {e}")

    async def funding_callback(self, data, receipt_timestamp):
        """资金费率回调"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO funding (timestamp, receipt_timestamp, exchange, symbol, rate, next_funding_time, mark_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, exchange, symbol) DO NOTHING
            """, (
                datetime.fromtimestamp(data.timestamp, tz=timezone.utc),
                datetime.fromtimestamp(receipt_timestamp, tz=timezone.utc),
                data.exchange,
                data.symbol,
                Decimal(str(data.rate)) if data.rate else None,
                datetime.fromtimestamp(data.next_funding_time, tz=timezone.utc) if data.next_funding_time else None,
                Decimal(str(data.mark_price)) if data.mark_price else None
            ))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"📊 资金费率: {data.symbol} = {data.rate}")

        except Exception as e:
            logger.error(f"❌ 资金费率处理失败: {e}")

    async def start_monitoring(self):
        """启动监控"""
        logger.info("🚀 启动动态扩展Cryptofeed监控")

        # 初始化数据库
        self.init_database()

        # 获取合约列表
        symbols = await self.get_binance_symbols()
        if not symbols:
            logger.error("❌ 无法获取合约列表")
            return

        logger.info(f"📈 准备监控 {len(symbols)} 个合约")

        # 动态连接池：每连接约100个合约（验证过的安全数量）
        symbols_per_connection = 100
        connection_count = (len(symbols) + symbols_per_connection - 1) // symbols_per_connection

        logger.info(f"🔗 创建 {connection_count} 个动态连接")

        # 创建多个FeedHandler
        for i in range(connection_count):
            start_idx = i * symbols_per_connection
            end_idx = min(start_idx + symbols_per_connection, len(symbols))
            connection_symbols = symbols[start_idx:end_idx]

            if not connection_symbols:
                continue

            logger.info(f"连接{i+1}: 监控 {len(connection_symbols)} 个合约")

            # 创建配置
            config = {
                'log': {
                    'filename': f'logs/dynamic_monitor_conn_{i}.log',
                    'level': 'INFO',
                    'disabled': False
                },
                'uvloop': True
            }

            fh = FeedHandler(config=config)
            fh.add_feed(
                BinanceFutures(
                    symbols=connection_symbols,
                    channels=[TRADES, FUNDING],
                    callbacks={
                        TRADES: self.trade_callback,
                        FUNDING: self.funding_callback
                    }
                )
            )

            self.feed_handlers.append(fh)

        # 并发启动所有连接
        self.running = True
        tasks = []

        for i, fh in enumerate(self.feed_handlers, 1):
            logger.info(f"🔄 启动连接 {i}")
            task = asyncio.create_task(fh.run())
            tasks.append(task)

        logger.info(f"✅ {connection_count} 个连接已启动，开始收集数据...")

        # 等待所有任务
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"❌ 监控运行错误: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """停止监控"""
        logger.info("🛑 停止监控...")
        self.running = False

        for fh in self.feed_handlers:
            try:
                await fh.stop()
            except:
                pass

        logger.info(f"📊 总计处理 {self.data_count} 条数据")
        logger.info("✅ 监控已停止")


async def main():
    """主函数"""
    monitor = DynamicCryptofeedMonitor()

    # 信号处理
    def signal_handler(signum, frame):
        logger.info(f"收到信号 {signum}，正在停止...")
        asyncio.create_task(monitor.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        logger.info("收到中断信号")
    except Exception as e:
        logger.error(f"监控错误: {e}")
        return 1

    return 0


if __name__ == '__main__':
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)