#!/usr/bin/env python3
"""
最简单的cryptofeed测试 - 不使用数据库
"""
import asyncio
import signal
import time
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import BinanceFutures

class SimpleTest:
    def __init__(self):
        self.running = True
        self.trade_count = 0
        self.start_time = time.time()

    async def trade_callback(self, trade, receipt_time):
        """交易数据回调"""
        self.trade_count += 1
        print(f"✅ 交易#{self.trade_count}: {trade.symbol} | 价格: {trade.price} | 数量: {trade.amount}")

    def signal_handler(self, signum, frame):
        print("\n⏹️  停止测试...")
        self.running = False

    def run(self):
        print("🚀 最简单的Cryptofeed测试")
        print("=" * 40)

        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)

        # 只监控BTC
        symbols = ['BTC-USDT-PERP']

        try:
            # 最简配置 - 不使用日志文件
            f = FeedHandler()

            print(f"📡 添加 BTC 交易监控...")
            f.add_feed(BinanceFutures(
                channels={TRADES: symbols},
                callbacks={TRADES: self.trade_callback}
            ))

            print("🚀 开始接收数据...")

            # 30秒后自动停止
            import threading
            def auto_stop():
                time.sleep(30)
                if self.running:
                    print("\n⏰ 30秒测试完成")
                    self.running = False
                    try:
                        f.stop()
                    except:
                        pass

            timer = threading.Thread(target=auto_stop)
            timer.daemon = True
            timer.start()

            # 运行
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
            print(f"接收到 {self.trade_count} 条交易数据")

            if self.trade_count > 0:
                print("🎉 Cryptofeed工作正常！")
            else:
                print("❌ 没有接收到数据，可能有配置问题")

if __name__ == '__main__':
    test = SimpleTest()
    test.run()