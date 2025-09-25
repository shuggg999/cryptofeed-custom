#!/usr/bin/env python3
"""
诊断Candles ClickHouse Backend问题
"""
import asyncio
import sys
sys.path.insert(0, '/Volumes/磁盘/Projects/cryptofeed')

from cryptofeed.backends.clickhouse import CandlesClickHouse
from decimal import Decimal
from datetime import datetime
import clickhouse_connect

# 测试数据
test_candle_data = {
    'timestamp': datetime.now().timestamp(),
    'symbol': 'BTC-USDT-PERP',
    'interval': '1m',
    'open': Decimal('50000.0'),
    'high': Decimal('50100.0'),
    'low': Decimal('49900.0'),
    'close': Decimal('50050.0'),
    'volume': Decimal('100.5'),
    'receipt_timestamp': datetime.now().timestamp()
}

async def test_candles_backend():
    print("=== 测试Candles ClickHouse Backend ===")

    # 创建Backend实例
    backend = CandlesClickHouse(
        host='localhost',
        port=8123,
        user='default',
        password='password123',
        database='cryptofeed'
    )

    print("1. 测试数据格式化:")
    prepared_data = backend._prepare_data(test_candle_data)
    print(f"   prepared_data类型: {type(prepared_data)}")
    print(f"   prepared_data长度: {len(prepared_data)}")
    print("   prepared_data内容:")
    for i, item in enumerate(prepared_data):
        print(f"     {i+1}. {type(item).__name__}: {item}")

    print("\n2. 检查表结构:")
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='password123',
        database='cryptofeed'
    )

    result = client.query('DESCRIBE candles')
    print("   表列定义:")
    for i, row in enumerate(result.result_rows):
        print(f"     {i+1}. {row[0]}: {row[1]}")

    print(f"\n   数据字段数: {len(prepared_data)}")
    print(f"   表列数: {len(result.result_rows)}")
    print(f"   匹配状态: {'✅ 匹配' if len(prepared_data) == len(result.result_rows) else '❌ 不匹配'}")

    print("\n3. 尝试手动插入:")
    try:
        client.insert('candles', [prepared_data])
        print("   ✅ 手动插入成功")

        # 检查插入的数据
        result = client.query("SELECT * FROM candles WHERE symbol = 'BTC-USDT-PERP' ORDER BY timestamp DESC LIMIT 1")
        if result.result_rows:
            print("   插入的数据:", result.result_rows[0])

    except Exception as e:
        print(f"   ❌ 手动插入失败: {e}")

    print("\n4. 测试Backend异步调用:")
    try:
        # 模拟异步调用
        await backend(test_candle_data, datetime.now().timestamp())
        print("   ✅ Backend异步调用成功")
    except Exception as e:
        print(f"   ❌ Backend异步调用失败: {e}")
        import traceback
        traceback.print_exc()

    client.close()

if __name__ == "__main__":
    asyncio.run(test_candles_backend())