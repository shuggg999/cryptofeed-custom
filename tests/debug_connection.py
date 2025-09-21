#!/usr/bin/env python3
"""
调试Binance连接问题
"""
import asyncio
import signal
import time
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, TICKER
from cryptofeed.exchanges import BinanceFutures

class DebugTest:
    def __init__(self):
        self.running = True
        self.trade_count = 0
        self.ticker_count = 0
        self.start_time = time.time()

    async def trade_callback(self, trade, receipt_time):
        """交易数据回调"""
        self.trade_count += 1
        print(f"✅ 交易#{self.trade_count}: {trade.symbol} | 价格: {trade.price} | 数量: {trade.amount}")

    async def ticker_callback(self, ticker, receipt_time):
        """Ticker回调"""
        self.ticker_count += 1
        print(f"✅ Ticker#{self.ticker_count}: {ticker.symbol} | 买: {ticker.bid} | 卖: {ticker.ask}")

    def signal_handler(self, signum, frame):
        print("\n⏹️  停止测试...")
        self.running = False

    def run(self):
        print("🔍 Binance连接调试测试")
        print("=" * 40)

        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)

        # 只测试3个热门合约
        symbols = ['BTC-USDT-PERP', 'ETH-USDT-PERP', 'BNB-USDT-PERP']

        # 创建FeedHandler - 最简配置
        config = {
            'log': {
                'filename': 'logs/debug.log',
                'level': 'INFO',
                'disabled': False
            }
        }

        f = FeedHandler(config=config)

        # 只添加一个简单的feed
        print(f"📡 添加 {len(symbols)} 个合约监控...")
        f.add_feed(BinanceFutures(
            channels={
                TRADES: symbols,
                TICKER: symbols
            },
            callbacks={
                TRADES: self.trade_callback,
                TICKER: self.ticker_callback
            }
        ))

        try:
            print("🚀 开始连接...")

            # 20秒后自动停止
            import threading
            def auto_stop():
                time.sleep(20)
                if self.running:
                    print("\n⏰ 20秒测试完成")
                    self.running = False
                    f.stop()

            timer = threading.Thread(target=auto_stop)
            timer.daemon = True
            timer.start()

            f.run()

        except KeyboardInterrupt:
            print("\n👋 用户停止")
        except Exception as e:
            print(f"\n❌ 错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            runtime = time.time() - self.start_time
            print(f"\n📊 测试结果:")
            print(f"运行时间: {runtime:.1f} 秒")
            print(f"交易数据: {self.trade_count} 条")
            print(f"Ticker数据: {self.ticker_count} 条")

            if self.trade_count == 0 and self.ticker_count == 0:
                print("❌ 未收到任何数据 - 可能有连接问题")
            else:
                print("✅ 连接正常")

if __name__ == '__main__':
    test = DebugTest()
    test.run()