#!/usr/bin/env python3
"""
简单的 PostgreSQL 连接测试
"""
import asyncio
import time
from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import TradePostgres
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Binance

# PostgreSQL 连接配置
postgres_cfg = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'db': 'cryptofeed',
    'pw': 'postgres'
}

def test_postgres():
    print("🔄 启动简单 PostgreSQL 测试...")

    try:
        # 创建 FeedHandler
        f = FeedHandler()

        # 添加单个交易对的交易数据
        f.add_feed(Binance(
            channels=[TRADES],
            symbols=['BTC-USDT'],
            callbacks={TRADES: TradePostgres(**postgres_cfg)}
        ))

        print("✅ 配置完成，开始接收数据...")
        print("📡 将运行 10 秒后自动停止...")

        # 使用同步方式运行
        import threading
        import signal

        def stop_handler():
            time.sleep(10)
            print("⏹️  10秒已到，停止接收数据")
            f.stop()

        # 启动停止定时器
        timer = threading.Thread(target=stop_handler)
        timer.daemon = True
        timer.start()

        # 开始运行
        f.run()

    except KeyboardInterrupt:
        print("⏹️  用户手动停止")
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_postgres()