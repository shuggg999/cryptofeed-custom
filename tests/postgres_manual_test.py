#!/usr/bin/env python3
"""
手动 PostgreSQL 测试 - 需要手动 Ctrl+C 停止
"""
from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import TradePostgres
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Binance

# PostgreSQL 连接配置
postgres_cfg = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'db': 'cryptofeed',
    'pw': 'password'
}

def main():
    print("🔄 启动 PostgreSQL 数据存储测试...")
    print("📊 连接配置: PostgreSQL at localhost:5432/cryptofeed")

    try:
        # 创建 FeedHandler
        f = FeedHandler()

        # 添加 Binance 交易数据
        f.add_feed(Binance(
            channels=[TRADES],
            symbols=['BTC-USDT'],
            callbacks={TRADES: TradePostgres(**postgres_cfg)}
        ))

        print("✅ 配置完成，开始接收数据...")
        print("💾 交易数据将存储到 PostgreSQL trades 表中")
        print("⏹️  按 Ctrl+C 停止...")

        # 开始运行
        f.run()

    except KeyboardInterrupt:
        print("\n⏹️  用户手动停止")
        print("📊 数据接收已停止")
    except Exception as e:
        print(f"❌ 错误: {e}")

if __name__ == '__main__':
    main()