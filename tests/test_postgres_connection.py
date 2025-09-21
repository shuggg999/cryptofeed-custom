#!/usr/bin/env python3
"""
测试 cryptofeed 连接 PostgreSQL 数据库的脚本
"""
import time
from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import TradePostgres, TickerPostgres
from cryptofeed.defines import TRADES, TICKER
from cryptofeed.exchanges import Binance

# PostgreSQL 连接配置
postgres_cfg = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'db': 'cryptofeed',
    'pw': 'postgres'  # TimescaleDB 默认密码
}

def main():
    print("🔄 启动 cryptofeed PostgreSQL 测试...")
    print(f"📊 连接配置: {postgres_cfg}")

    try:
        # 创建 FeedHandler
        f = FeedHandler()

        # 添加 Binance 交易数据 - 选择活跃的交易对
        f.add_feed(Binance(
            channels=[TRADES, TICKER],
            symbols=['BTC-USDT', 'ETH-USDT'],
            callbacks={
                TRADES: TradePostgres(**postgres_cfg),
                TICKER: TickerPostgres(**postgres_cfg)
            }
        ))

        print("✅ FeedHandler 配置完成")
        print("📡 开始接收数据 (将运行 30 秒)...")
        print("💾 数据将存储到 PostgreSQL trades 和 ticker 表中")

        # 运行 30 秒后停止
        import asyncio
        import signal

        def signal_handler():
            print("\n⏹️  停止数据接收...")
            f.stop()

        # 设置定时器
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.call_later(30, signal_handler)

        # 开始运行
        f.run()

    except KeyboardInterrupt:
        print("\n⏹️  用户手动停止")
    except Exception as e:
        print(f"❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()