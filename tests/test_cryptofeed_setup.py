#!/usr/bin/env python
"""
Cryptofeed 设置测试脚本
测试 TimescaleDB 连接和 Cryptofeed 数据流
"""

import asyncio
import asyncpg
from datetime import datetime
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance
from cryptofeed.defines import TRADES, L2_BOOK, TICKER
from cryptofeed.backends.postgres import TradePostgres, BookPostgres, TickerPostgres

# TimescaleDB 连接配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'cryptofeed',
    'user': 'postgres',
    'password': 'password'
}

async def init_database():
    """初始化数据库表结构"""
    print("🔧 初始化数据库...")

    # 连接数据库
    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        # 创建 TimescaleDB 扩展
        await conn.execute('CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;')
        print("✅ TimescaleDB 扩展已启用")

        # 创建交易表
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL,
                timestamp TIMESTAMPTZ NOT NULL,
                receipt_timestamp TIMESTAMPTZ,
                exchange VARCHAR(32),
                symbol VARCHAR(32),
                side VARCHAR(8),
                amount NUMERIC(64, 32),
                price NUMERIC(64, 32),
                trade_id VARCHAR(64),
                order_type VARCHAR(32)
            );
        ''')

        # 转换为 TimescaleDB 超表（如果还不是）
        try:
            await conn.execute("SELECT create_hypertable('trades', 'timestamp', if_not_exists => TRUE);")
            print("✅ trades 表已转换为 TimescaleDB 超表")
        except:
            print("ℹ️ trades 表已经是超表")

        # 创建行情表
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ticker (
                id SERIAL,
                timestamp TIMESTAMPTZ NOT NULL,
                receipt_timestamp TIMESTAMPTZ,
                exchange VARCHAR(32),
                symbol VARCHAR(32),
                bid NUMERIC(64, 32),
                ask NUMERIC(64, 32)
            );
        ''')

        # 转换为超表
        try:
            await conn.execute("SELECT create_hypertable('ticker', 'timestamp', if_not_exists => TRUE);")
            print("✅ ticker 表已转换为 TimescaleDB 超表")
        except:
            print("ℹ️ ticker 表已经是超表")

        # 创建订单簿表
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS l2_book (
                id SERIAL,
                timestamp TIMESTAMPTZ NOT NULL,
                receipt_timestamp TIMESTAMPTZ,
                exchange VARCHAR(32),
                symbol VARCHAR(32),
                data JSONB
            );
        ''')

        try:
            await conn.execute("SELECT create_hypertable('l2_book', 'timestamp', if_not_exists => TRUE);")
            print("✅ l2_book 表已转换为 TimescaleDB 超表")
        except:
            print("ℹ️ l2_book 表已经是超表")

        print("✅ 数据库表结构初始化完成！")

    finally:
        await conn.close()

async def test_connection():
    """测试数据库连接"""
    print("\n🔍 测试数据库连接...")

    try:
        conn = await asyncpg.connect(**DB_CONFIG)

        # 获取 PostgreSQL 版本
        version = await conn.fetchval('SELECT version();')
        print(f"✅ 成功连接到 PostgreSQL!")
        print(f"   版本: {version.split(',')[0]}")

        # 检查 TimescaleDB
        timescale_version = await conn.fetchval("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';")
        if timescale_version:
            print(f"✅ TimescaleDB 已安装，版本: {timescale_version}")
        else:
            print("⚠️ TimescaleDB 未安装")

        await conn.close()
        return True

    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False

# 自定义回调函数（用于调试）
async def trade_callback(trade, receipt_timestamp):
    """交易数据回调"""
    print(f"📊 交易: {trade.exchange} {trade.symbol} | "
          f"方向: {trade.side} | 价格: {trade.price} | 数量: {trade.amount}")

async def ticker_callback(ticker, receipt_timestamp):
    """行情数据回调"""
    print(f"💹 行情: {ticker.exchange} {ticker.symbol} | "
          f"买价: {ticker.bid} | 卖价: {ticker.ask}")

async def book_callback(book, receipt_timestamp):
    """订单簿回调"""
    best_bid = list(book.bids.keys())[0] if book.bids else 'N/A'
    best_ask = list(book.asks.keys())[0] if book.asks else 'N/A'
    print(f"📖 订单簿: {book.exchange} {book.symbol} | "
          f"最佳买价: {best_bid} | 最佳卖价: {best_ask}")

def test_cryptofeed():
    """测试 Cryptofeed 数据流"""
    print("\n🚀 启动 Cryptofeed 数据流测试...")
    print("   将订阅 Binance 的 BTC-USDT 数据")
    print("   按 Ctrl+C 停止\n")

    # 创建 FeedHandler
    fh = FeedHandler()

    # PostgreSQL 回调配置
    postgres_cfg = {
        'host': DB_CONFIG['host'],
        'port': DB_CONFIG['port'],
        'user': DB_CONFIG['user'],
        'pw': DB_CONFIG['password'],
        'db': DB_CONFIG['database']
    }

    # 添加 Binance 数据源
    fh.add_feed(Binance(
        symbols=['BTC-USDT'],
        channels=[TRADES, TICKER, L2_BOOK],
        callbacks={
            TRADES: [trade_callback, TradePostgres(**postgres_cfg)],
            TICKER: [ticker_callback, TickerPostgres(**postgres_cfg)],
            L2_BOOK: [book_callback, BookPostgres(**postgres_cfg)]
        }
    ))

    # 运行
    fh.run()

async def check_data():
    """检查数据库中的数据"""
    print("\n📊 检查数据库中的数据...")

    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        # 检查交易数据
        trade_count = await conn.fetchval('SELECT COUNT(*) FROM trades;')
        print(f"   交易记录数: {trade_count}")

        if trade_count > 0:
            latest_trade = await conn.fetchrow('''
                SELECT * FROM trades
                ORDER BY timestamp DESC
                LIMIT 1;
            ''')
            print(f"   最新交易: {latest_trade['symbol']} @ {latest_trade['price']}")

        # 检查行情数据
        ticker_count = await conn.fetchval('SELECT COUNT(*) FROM ticker;')
        print(f"   行情记录数: {ticker_count}")

        # 检查订单簿数据
        book_count = await conn.fetchval('SELECT COUNT(*) FROM l2_book;')
        print(f"   订单簿记录数: {book_count}")

    finally:
        await conn.close()

def main():
    """主函数"""
    print("=" * 60)
    print("🎯 Cryptofeed + TimescaleDB 设置测试")
    print("=" * 60)

    # 1. 测试连接
    loop = asyncio.get_event_loop()
    connected = loop.run_until_complete(test_connection())

    if not connected:
        print("\n❌ 请确保 TimescaleDB 容器正在运行:")
        print("   docker ps | grep timescale-crypto")
        return

    # 2. 初始化数据库
    loop.run_until_complete(init_database())

    # 3. 运行 Cryptofeed（可选）
    choice = input("\n是否运行实时数据测试? (y/n): ")
    if choice.lower() == 'y':
        try:
            test_cryptofeed()
        except KeyboardInterrupt:
            print("\n\n⏹️ 数据流已停止")

            # 检查收集的数据
            loop.run_until_complete(check_data())

    print("\n✅ 测试完成！")
    print("\n📝 后续步骤:")
    print("1. 在 DataGrip 中连接 TimescaleDB:")
    print("   - Host: localhost")
    print("   - Port: 5432")
    print("   - Database: cryptofeed")
    print("   - User: postgres")
    print("   - Password: password")
    print("\n2. 查看数据:")
    print("   SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10;")
    print("   SELECT * FROM ticker ORDER BY timestamp DESC LIMIT 10;")
    print("\n3. 运行更多示例:")
    print("   source cryptofeed-env/bin/activate")
    print("   python examples/demo_postgres.py")

if __name__ == '__main__':
    main()