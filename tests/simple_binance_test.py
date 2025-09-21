#!/usr/bin/env python3
"""
简单的Binance数据接收测试
不依赖PostgreSQL，直接输出到控制台
"""
import asyncio
import signal
import time
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, TICKER, FUNDING
from cryptofeed.exchanges import BinanceFutures

class SimpleBinanceTest:
    def __init__(self):
        self.running = True
        self.start_time = time.time()
        self.trade_count = 0
        self.ticker_count = 0
        self.funding_count = 0

    async def trade_callback(self, trade, receipt_time):
        """交易数据回调"""
        self.trade_count += 1
        if self.trade_count % 10 == 0:
            print(f"📈 交易#{self.trade_count}: {trade.symbol} | 价格: {trade.price} | 数量: {trade.amount} | 方向: {trade.side}")

    async def ticker_callback(self, ticker, receipt_time):
        """Ticker回调"""
        self.ticker_count += 1
        if self.ticker_count % 50 == 0:
            print(f"💹 Ticker#{self.ticker_count}: {ticker.symbol} | 买: {ticker.bid} | 卖: {ticker.ask}")

    async def funding_callback(self, funding, receipt_time):
        """资金费率回调"""
        self.funding_count += 1
        print(f"💰 资金费率: {funding.symbol} | 费率: {funding.rate:.6f} | 标记价格: {funding.mark_price}")

    def signal_handler(self, signum, frame):
        print("\n⏹️  停止测试...")
        self.running = False

    def run(self):
        print("🚀 Binance 简单数据接收测试")
        print("=" * 40)
        print("测试内容：")
        print("- 交易数据 (前3个热门合约)")
        print("- Ticker数据 (前5个热门合约)")
        print("- 资金费率 (前3个热门合约)")
        print("=" * 40)

        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)

        # 热门合约
        symbols = ['BTC-USDT-PERP', 'ETH-USDT-PERP', 'BNB-USDT-PERP']

        # 创建FeedHandler
        f = FeedHandler()

        # 添加数据源
        f.add_feed(BinanceFutures(
            channels={
                TRADES: symbols[:3],  # 前3个合约的交易数据
                TICKER: symbols[:5] if len(symbols) >= 5 else symbols,  # 前5个合约的Ticker
                FUNDING: symbols[:3]  # 前3个合约的资金费率
            },
            callbacks={
                TRADES: self.trade_callback,
                TICKER: self.ticker_callback,
                FUNDING: self.funding_callback
            }
        ))

        try:
            print("📡 开始接收数据...")
            print("⏹️  按 Ctrl+C 停止\n")

            # 运行10秒后自动停止（用于测试）
            import threading
            def auto_stop():
                time.sleep(30)  # 30秒后自动停止
                if self.running:
                    print("\n⏰ 30秒测试完成，自动停止")
                    self.running = False
                    f.stop()

            timer = threading.Thread(target=auto_stop)
            timer.daemon = True
            timer.start()

            f.run()

        except KeyboardInterrupt:
            print("\n👋 用户手动停止")
        except Exception as e:
            print(f"\n❌ 错误: {e}")
        finally:
            # 统计信息
            runtime = time.time() - self.start_time
            print(f"\n📊 测试统计:")
            print(f"运行时间: {runtime:.1f} 秒")
            print(f"交易数据: {self.trade_count} 条")
            print(f"Ticker数据: {self.ticker_count} 条")
            print(f"资金费率: {self.funding_count} 条")
            print("✅ 测试完成")

if __name__ == '__main__':
    test = SimpleBinanceTest()
    test.run()