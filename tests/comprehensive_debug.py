#!/usr/bin/env python3
"""
全面的Cryptofeed调试工具 - 增加详细日志输出
发现官方示例只监控30个合约，我们需要验证这个假设
"""
import asyncio
import signal
import time
import logging
import sys
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, TICKER, FUNDING
from cryptofeed.exchanges import BinanceFutures

# 设置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/comprehensive_debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveDebugTest:
    def __init__(self):
        self.running = True
        self.trade_count = 0
        self.ticker_count = 0
        self.funding_count = 0
        self.start_time = time.time()
        self.connection_attempts = 0

    async def trade_callback(self, trade, receipt_time):
        """交易数据回调"""
        self.trade_count += 1
        if self.trade_count <= 5:  # 只显示前5条
            logger.info(f"✅ 交易#{self.trade_count}: {trade.symbol} | 价格: {trade.price} | 数量: {trade.amount}")
        elif self.trade_count % 100 == 0:  # 之后每100条显示一次
            logger.info(f"✅ 交易数据流正常 - 已接收 {self.trade_count} 条")

    async def ticker_callback(self, ticker, receipt_time):
        """Ticker回调"""
        self.ticker_count += 1
        if self.ticker_count <= 3:  # 只显示前3条
            logger.info(f"✅ Ticker#{self.ticker_count}: {ticker.symbol} | 买: {ticker.bid} | 卖: {ticker.ask}")
        elif self.ticker_count % 50 == 0:  # 之后每50条显示一次
            logger.info(f"✅ Ticker数据流正常 - 已接收 {self.ticker_count} 条")

    async def funding_callback(self, funding, receipt_time):
        """资金费率回调"""
        self.funding_count += 1
        logger.info(f"✅ 资金费率#{self.funding_count}: {funding.symbol} | 费率: {funding.rate}")

    def signal_handler(self, signum, frame):
        logger.info(f"\n⏹️  收到信号 {signum}，停止测试...")
        self.running = False

    def test_scenarios(self):
        """测试不同的场景"""
        scenarios = [
            {
                'name': '场景1: 单个BTC合约（最基础测试）',
                'symbols': ['BTC-USDT-PERP'],
                'channels': [TRADES],
                'duration': 15
            },
            {
                'name': '场景2: 3个热门合约',
                'symbols': ['BTC-USDT-PERP', 'ETH-USDT-PERP', 'BNB-USDT-PERP'],
                'channels': [TRADES, TICKER],
                'duration': 20
            },
            {
                'name': '场景3: 30个合约（官方示例规模）',
                'symbols': None,  # 将在运行时获取前30个
                'channels': [TRADES, TICKER, FUNDING],
                'duration': 30
            }
        ]

        return scenarios

    def run_scenario(self, scenario):
        """运行单个测试场景"""
        logger.info("=" * 60)
        logger.info(f"🚀 开始 {scenario['name']}")
        logger.info("=" * 60)

        # 重置计数器
        self.trade_count = 0
        self.ticker_count = 0
        self.funding_count = 0
        self.start_time = time.time()

        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)

        # 获取合约列表
        if scenario['symbols'] is None:
            try:
                all_symbols = BinanceFutures.symbols()
                symbols = all_symbols[:30]  # 前30个合约，模仿官方示例
                logger.info(f"📊 获取到 {len(all_symbols)} 个合约，使用前30个")
            except Exception as e:
                logger.error(f"❌ 获取合约列表失败: {e}")
                return False
        else:
            symbols = scenario['symbols']

        logger.info(f"📡 监控合约: {symbols}")
        logger.info(f"📺 监控频道: {scenario['channels']}")

        try:
            # 创建FeedHandler - 使用DEBUG级别日志
            config = {
                'log': {
                    'filename': 'logs/cryptofeed_internal.log',
                    'level': 'DEBUG',
                    'disabled': False
                }
            }

            logger.info("🔧 创建FeedHandler...")
            f = FeedHandler(config=config)

            # 构建回调字典
            callbacks = {}
            if TRADES in scenario['channels']:
                callbacks[TRADES] = self.trade_callback
            if TICKER in scenario['channels']:
                callbacks[TICKER] = self.ticker_callback
            if FUNDING in scenario['channels']:
                callbacks[FUNDING] = self.funding_callback

            logger.info("📡 添加Binance Futures feed...")
            f.add_feed(BinanceFutures(
                symbols=symbols,
                channels=scenario['channels'],
                callbacks=callbacks
            ))

            logger.info("✅ Feed配置完成，开始连接...")

            # 设置超时自动停止
            import threading
            def auto_stop():
                time.sleep(scenario['duration'])
                if self.running:
                    logger.info(f"\n⏰ {scenario['duration']}秒测试完成，自动停止")
                    self.running = False
                    try:
                        f.stop()
                    except Exception as e:
                        logger.warning(f"停止时出现警告: {e}")

            timer = threading.Thread(target=auto_stop)
            timer.daemon = True
            timer.start()

            # 开始运行
            logger.info("🚀 开始运行FeedHandler...")
            f.run()

        except KeyboardInterrupt:
            logger.info("\n👋 用户手动停止")
            return True
        except Exception as e:
            logger.error(f"\n❌ 场景运行错误: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            # 统计结果
            runtime = time.time() - self.start_time
            logger.info(f"\n📊 {scenario['name']} 结果:")
            logger.info(f"⏱️  运行时间: {runtime:.1f} 秒")
            logger.info(f"📈 交易数据: {self.trade_count} 条")
            logger.info(f"💹 Ticker数据: {self.ticker_count} 条")
            logger.info(f"💰 资金费率: {self.funding_count} 条")

            # 判断成功
            total_data = self.trade_count + self.ticker_count + self.funding_count
            success = total_data > 0

            if success:
                logger.info("🎉 场景测试成功！")
            else:
                logger.error("❌ 场景测试失败 - 未接收到任何数据")

            logger.info("=" * 60)
            return success

    def run(self):
        """运行所有测试场景"""
        logger.info("🔍 Cryptofeed 全面调试测试")
        logger.info("=" * 60)
        logger.info("目标：找出为什么497个合约无法连接的根本原因")
        logger.info("假设：可能是合约数量过多导致连接超载")
        logger.info("=" * 60)

        scenarios = self.test_scenarios()
        results = []

        for i, scenario in enumerate(scenarios, 1):
            logger.info(f"\n🎯 准备运行第 {i}/{len(scenarios)} 个场景...")
            logger.info("按 Ctrl+C 可以跳过当前场景")

            # 给用户一点准备时间
            time.sleep(2)

            success = self.run_scenario(scenario)
            results.append({
                'scenario': scenario['name'],
                'success': success,
                'data_received': self.trade_count + self.ticker_count + self.funding_count
            })

            if not success:
                logger.warning(f"⚠️  {scenario['name']} 失败，继续下一个场景...")

            # 场景间休息
            if i < len(scenarios):
                logger.info("\n⏸️  休息5秒，然后继续下一个场景...")
                time.sleep(5)

        # 最终分析
        logger.info("\n" + "=" * 60)
        logger.info("🔬 最终分析结果")
        logger.info("=" * 60)

        for result in results:
            status = "✅ 成功" if result['success'] else "❌ 失败"
            logger.info(f"{status} {result['scenario']} - 数据量: {result['data_received']}")

        # 得出结论
        success_count = sum(1 for r in results if r['success'])

        if success_count == 0:
            logger.error("🚨 所有场景都失败！Cryptofeed可能有根本性问题")
        elif success_count == len(results):
            logger.info("🎉 所有场景都成功！问题可能确实是合约数量过多")
        else:
            logger.warning(f"📊 部分成功 ({success_count}/{len(results)}) - 需要进一步分析")

        logger.info("\n📝 请检查以下日志文件获取详细信息:")
        logger.info("- logs/comprehensive_debug.log (本程序日志)")
        logger.info("- logs/cryptofeed_internal.log (Cryptofeed内部日志)")

if __name__ == '__main__':
    test = ComprehensiveDebugTest()
    test.run()