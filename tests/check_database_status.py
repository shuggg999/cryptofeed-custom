#!/usr/bin/env python3
"""
检查数据库数据状态
"""
import psycopg2
from datetime import datetime

# PostgreSQL配置
postgres_cfg = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'password': 'password',
    'database': 'cryptofeed'
}

def check_database_status():
    """检查数据库数据状态"""
    try:
        conn = psycopg2.connect(**postgres_cfg)
        cursor = conn.cursor()

        print("🔍 检查数据库数据状态...")
        print("=" * 60)

        # 检查交易数据
        cursor.execute("""
            SELECT symbol, COUNT(*) as count, MAX(timestamp) as latest
            FROM trades
            GROUP BY symbol
            ORDER BY count DESC
            LIMIT 10
        """)
        trades_data = cursor.fetchall()

        print("📈 交易数据 (TOP 10):")
        if trades_data:
            for symbol, count, latest in trades_data:
                # Handle both datetime objects and timestamps
                if latest:
                    if isinstance(latest, datetime):
                        latest_time = latest
                    else:
                        latest_time = datetime.fromtimestamp(latest)
                else:
                    latest_time = "N/A"
                print(f"  {symbol}: {count} 条 | 最新: {latest_time}")
        else:
            print("  ❌ 无交易数据")

        print()

        # 检查资金费率数据
        cursor.execute("""
            SELECT symbol, COUNT(*) as count, MAX(timestamp) as latest
            FROM funding
            GROUP BY symbol
            ORDER BY count DESC
            LIMIT 10
        """)
        funding_data = cursor.fetchall()

        print("💰 资金费率数据 (TOP 10):")
        if funding_data:
            for symbol, count, latest in funding_data:
                # Handle both datetime objects and timestamps
                if latest:
                    if isinstance(latest, datetime):
                        latest_time = latest
                    else:
                        latest_time = datetime.fromtimestamp(latest)
                else:
                    latest_time = "N/A"
                print(f"  {symbol}: {count} 条 | 最新: {latest_time}")
        else:
            print("  ❌ 无资金费率数据")

        print()

        # 检查Ticker数据
        cursor.execute("""
            SELECT symbol, COUNT(*) as count, MAX(timestamp) as latest
            FROM ticker
            GROUP BY symbol
            ORDER BY count DESC
            LIMIT 10
        """)
        ticker_data = cursor.fetchall()

        print("💹 Ticker数据 (TOP 10):")
        if ticker_data:
            for symbol, count, latest in ticker_data:
                # Handle both datetime objects and timestamps
                if latest:
                    if isinstance(latest, datetime):
                        latest_time = latest
                    else:
                        latest_time = datetime.fromtimestamp(latest)
                else:
                    latest_time = "N/A"
                print(f"  {symbol}: {count} 条 | 最新: {latest_time}")
        else:
            print("  ❌ 无Ticker数据")

        print()

        # 检查K线数据
        tables = ['candles_1m', 'candles_5m', 'candles_30m', 'candles_4h', 'candles_1d']
        for table in tables:
            try:
                cursor.execute(f"""
                    SELECT symbol, COUNT(*) as count, MAX(timestamp) as latest
                    FROM {table}
                    GROUP BY symbol
                    ORDER BY count DESC
                    LIMIT 5
                """)
                candle_data = cursor.fetchall()

                print(f"📊 {table.upper()} 数据 (TOP 5):")
                if candle_data:
                    for symbol, count, latest in candle_data:
                        # Handle both datetime objects and timestamps
                        if latest:
                            if isinstance(latest, datetime):
                                latest_time = latest
                            else:
                                latest_time = datetime.fromtimestamp(latest)
                        else:
                            latest_time = "N/A"
                        print(f"  {symbol}: {count} 条 | 最新: {latest_time}")
                else:
                    print(f"  ❌ 无{table}数据")
                print()
            except Exception as e:
                print(f"  ❌ 表 {table} 不存在或错误: {e}")
                print()

        # 总统计
        cursor.execute("SELECT COUNT(*) FROM trades")
        total_trades = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM funding")
        total_funding = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM ticker")
        total_ticker = cursor.fetchone()[0]

        print("📊 总体统计:")
        print(f"  交易数据: {total_trades:,} 条")
        print(f"  资金费率: {total_funding:,} 条")
        print(f"  Ticker: {total_ticker:,} 条")

        conn.close()

        # 判断成功状态
        total_data = total_trades + total_funding + total_ticker
        if total_data > 0:
            print()
            print("🎉 数据库写入成功！监控系统正常工作")
            return True
        else:
            print()
            print("❌ 数据库中无数据，可能有写入问题")
            return False

    except Exception as e:
        print(f"❌ 数据库检查失败: {e}")
        return False

if __name__ == '__main__':
    check_database_status()