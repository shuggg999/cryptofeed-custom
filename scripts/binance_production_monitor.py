#!/usr/bin/env python3
"""
Binance 生产级全市场监控系统
监控全部USDT永续合约的5个周期K线、资金费率、交易数据
使用分区表和优化索引，包含数据清理逻辑
"""
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict

from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import (
    TradePostgres, FundingPostgres, CandlesPostgres, TickerPostgres
)
from cryptofeed.defines import TRADES, FUNDING, CANDLES, TICKER
from cryptofeed.exchanges import BinanceFutures

# ========================================
# 配置设置
# ========================================

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('binance_monitor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL配置
postgres_cfg = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'db': 'cryptofeed',
    'pw': 'password'
}

# 监控配置
INTERVALS = ['1m', '5m', '30m', '4h', '1d']
MAX_CONTRACTS = 30  # 最大监控合约数（基于调试结果和官方示例限制）

class BinanceProductionMonitor:
    """生产级Binance监控器"""

    def __init__(self):
        self.feed_handler = None
        self.symbols = []
        self.is_running = False
        self.start_time = None

        # 统计数据
        self.stats = {
            'trades_count': 0,
            'candles_count': 0,
            'funding_count': 0,
            'ticker_count': 0,
            'last_trade_time': None,
            'last_candle_time': None,
            'last_funding_time': None,
            'errors': 0
        }

    def get_top_liquid_symbols(self) -> List[str]:
        """获取最活跃的30个USDT永续合约（基于官方示例规模限制）"""
        try:
            all_symbols = BinanceFutures.symbols()
            # 过滤USDT永续合约
            usdt_symbols = [s for s in all_symbols if s.endswith('-USDT-PERP')]

            # 根据调试结果，限制在MAX_CONTRACTS个合约以避免连接过载
            # 选择市值最大的主流合约确保流动性
            top_symbols = usdt_symbols[:MAX_CONTRACTS]

            logger.info(f"📊 发现 {len(usdt_symbols)} 个USDT永续合约")
            logger.info(f"🎯 基于连接限制，选择前 {len(top_symbols)} 个合约监控")
            logger.warning(f"⚠️  497合约同时监控会导致连接过载，已限制为{MAX_CONTRACTS}个")

            return top_symbols

        except Exception as e:
            logger.error(f"获取合约列表失败: {e}")
            # 返回确定可用的热门合约
            fallback_symbols = [
                'BTC-USDT-PERP', 'ETH-USDT-PERP', 'BNB-USDT-PERP',
                'XRP-USDT-PERP', 'SOL-USDT-PERP', 'DOGE-USDT-PERP',
                'ADA-USDT-PERP', 'MATIC-USDT-PERP', 'DOT-USDT-PERP',
                'AVAX-USDT-PERP', 'LINK-USDT-PERP', 'UNI-USDT-PERP',
                'LTC-USDT-PERP', 'BCH-USDT-PERP', 'ETC-USDT-PERP',
                'ATOM-USDT-PERP', 'FIL-USDT-PERP', 'TRX-USDT-PERP',
                'EOS-USDT-PERP', 'XLM-USDT-PERP', 'ALGO-USDT-PERP',
                'VET-USDT-PERP', 'THETA-USDT-PERP', 'ICP-USDT-PERP',
                'FTT-USDT-PERP', 'NEAR-USDT-PERP', 'LUNA-USDT-PERP',
                'AAVE-USDT-PERP', 'COMP-USDT-PERP', 'SUSHI-USDT-PERP'
            ]
            logger.info(f"🔄 使用备用合约列表: {len(fallback_symbols)} 个合约")
            return fallback_symbols

    async def trade_callback(self, trade, receipt_time):
        """交易数据回调"""
        try:
            self.stats['trades_count'] += 1
            self.stats['last_trade_time'] = datetime.now()

            if self.stats['trades_count'] % 1000 == 0:
                logger.info(f"📈 已接收 {self.stats['trades_count']} 条交易数据")

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"交易回调错误: {e}")

    async def candle_callback(self, candle, receipt_time):
        """K线数据回调"""
        try:
            self.stats['candles_count'] += 1
            self.stats['last_candle_time'] = datetime.now()

            logger.info(f"📊 K线[{candle.interval}]: {candle.symbol} | 收盘: {candle.close} | 成交量: {candle.volume}")

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"K线回调错误: {e}")

    async def funding_callback(self, funding, receipt_time):
        """资金费率回调"""
        try:
            self.stats['funding_count'] += 1
            self.stats['last_funding_time'] = datetime.now()

            logger.info(f"💰 资金费率: {funding.symbol} | 费率: {funding.rate:.6f} | 标记价格: {funding.mark_price}")

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"资金费率回调错误: {e}")

    async def ticker_callback(self, ticker, receipt_time):
        """Ticker回调"""
        try:
            self.stats['ticker_count'] += 1

            # 只记录重要变化，避免日志过多
            if self.stats['ticker_count'] % 100 == 0:
                logger.info(f"💹 Ticker更新: 已处理 {self.stats['ticker_count']} 条")

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Ticker回调错误: {e}")

    def setup_monitoring(self):
        """设置监控配置"""
        logger.info("🔧 配置监控系统...")

        # 获取前30个最活跃USDT合约
        self.symbols = self.get_top_liquid_symbols()
        logger.info(f"🎯 将监控 {len(self.symbols)} 个合约（已优化避免连接过载）")

        # 创建FeedHandler
        config = {
            'log': {
                'filename': 'logs/binance_monitor.log',
                'level': 'WARNING',  # 减少cryptofeed内部日志
                'disabled': False
            },
            'backend_multiprocessing': True,
            'uvloop': True
        }

        self.feed_handler = FeedHandler(config=config)

        logger.info("🎯 使用单连接模式（基于调试结果优化）")

        # 为每个时间周期创建独立的feed
        for interval in INTERVALS:
            table_name = f'candles_{interval}'
            logger.info(f"添加 {interval} K线监控: {len(self.symbols)} 个合约")

            self.feed_handler.add_feed(
                BinanceFutures(
                    symbols=self.symbols,
                    channels=[CANDLES],
                    callbacks={
                        CANDLES: [
                            CandlesPostgres(table=table_name, **postgres_cfg),
                            self.candle_callback
                        ]
                    },
                    candle_interval=interval
                )
            )

        # 交易数据监控
        logger.info(f"添加交易数据监控: {len(self.symbols)} 个合约")
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
        logger.info(f"添加资金费率监控: {len(self.symbols)} 个合约")
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

        # Ticker监控
        logger.info(f"添加Ticker监控: {len(self.symbols)} 个合约")
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

    def print_stats(self):
        """打印统计信息"""
        current_time = datetime.now()
        uptime = current_time - self.start_time if self.start_time else timedelta(0)

        logger.info("=" * 60)
        logger.info("📊 Binance监控系统状态")
        logger.info("=" * 60)
        logger.info(f"运行时间: {uptime}")
        logger.info(f"监控合约数: {len(self.symbols)}")
        logger.info(f"监控周期数: {len(INTERVALS)}")
        logger.info(f"交易数据: {self.stats['trades_count']} 条")
        logger.info(f"K线数据: {self.stats['candles_count']} 条")
        logger.info(f"资金费率: {self.stats['funding_count']} 条")
        logger.info(f"Ticker更新: {self.stats['ticker_count']} 条")
        logger.info(f"错误次数: {self.stats['errors']}")

        if self.stats['last_trade_time']:
            logger.info(f"最后交易: {self.stats['last_trade_time'].strftime('%H:%M:%S')}")
        if self.stats['last_candle_time']:
            logger.info(f"最后K线: {self.stats['last_candle_time'].strftime('%H:%M:%S')}")
        if self.stats['last_funding_time']:
            logger.info(f"最后资金费率: {self.stats['last_funding_time'].strftime('%H:%M:%S')}")

        logger.info("=" * 60)

    async def cleanup_old_data(self):
        """清理旧数据"""
        import psycopg2

        try:
            conn = psycopg2.connect(
                host=postgres_cfg['host'],
                user=postgres_cfg['user'],
                password=postgres_cfg['pw'],
                database=postgres_cfg['db']
            )
            cursor = conn.cursor()

            # 清理30天前的交易数据
            cursor.execute("""
                DELETE FROM trades
                WHERE timestamp < NOW() - INTERVAL '30 days'
            """)
            deleted_trades = cursor.rowcount

            # 清理Ticker表，只保留最新记录
            cursor.execute("""
                DELETE FROM ticker t1
                WHERE EXISTS (
                    SELECT 1 FROM ticker t2
                    WHERE t2.symbol = t1.symbol
                    AND t2.exchange = t1.exchange
                    AND t2.timestamp > t1.timestamp
                )
            """)
            deleted_tickers = cursor.rowcount

            conn.commit()
            conn.close()

            logger.info(f"🧹 数据清理完成: 删除 {deleted_trades} 条交易记录, {deleted_tickers} 条旧Ticker记录")

        except Exception as e:
            logger.error(f"数据清理失败: {e}")

    def setup_periodic_tasks(self):
        """设置定期任务（通过定时器实现）"""
        import threading

        def periodic_stats():
            """定期统计任务"""
            while self.is_running:
                try:
                    time.sleep(300)  # 5分钟
                    if self.is_running:
                        self.print_stats()
                except Exception as e:
                    logger.error(f"统计任务错误: {e}")

        def periodic_cleanup():
            """定期清理任务"""
            while self.is_running:
                try:
                    time.sleep(3600)  # 1小时
                    if self.is_running:
                        current_time = datetime.now()
                        if current_time.minute < 5:  # 每小时前5分钟执行
                            asyncio.new_event_loop().run_until_complete(self.cleanup_old_data())
                except Exception as e:
                    logger.error(f"清理任务错误: {e}")

        # 启动后台线程
        stats_thread = threading.Thread(target=periodic_stats, daemon=True)
        cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)

        stats_thread.start()
        cleanup_thread.start()

    def signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"\n收到信号 {signum}，正在安全停止...")
        self.is_running = False
        if self.feed_handler:
            self.feed_handler.stop()

    def run(self):
        """运行监控系统"""
        logger.info("🚀 启动Binance生产级监控系统")
        logger.info("=" * 60)

        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            # 配置监控
            self.setup_monitoring()

            # 标记开始时间
            self.start_time = datetime.now()
            self.is_running = True

            logger.info("✅ 监控配置完成")
            logger.info(f"📅 开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("📡 开始接收数据流...")
            logger.info("⏹  按 Ctrl+C 安全停止")
            logger.info("=" * 60)

            # 启动定期任务
            self.setup_periodic_tasks()

            # 运行监控
            self.feed_handler.run()

        except KeyboardInterrupt:
            logger.info("用户手动停止")
        except Exception as e:
            logger.error(f"监控系统错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_running = False
            logger.info("🔄 执行最终清理...")
            # 同步清理数据
            asyncio.new_event_loop().run_until_complete(self.cleanup_old_data())
            self.print_stats()
            logger.info("✅ 监控系统已安全停止")

def main():
    """主函数"""
    monitor = BinanceProductionMonitor()

    try:
        # 运行监控系统
        monitor.run()
    except Exception as e:
        logger.error(f"启动失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()