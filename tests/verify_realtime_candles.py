#!/usr/bin/env python3
"""验证实时K线数据的10字段结构"""
import asyncio
import sys
import os
sys.path.insert(0, '/Volumes/磁盘/Projects/cryptofeed')

from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.backends.clickhouse import CandlesClickHouse

# ClickHouse配置
clickhouse_config = {
    'host': 'localhost',
    'port': 8123,
    'user': 'default',
    'password': '',
    'database': 'cryptofeed'
}

# 创建ClickHouse backend
candles_backend = CandlesClickHouse(**clickhouse_config)

async def candle_callback(candle, receipt_timestamp):
    """记录实时K线数据"""
    print(f"📊 实时K线数据:")
    print(f"   symbol: {candle.symbol}")
    print(f"   interval: {candle.interval}")
    print(f"   timestamp: {candle.timestamp}")
    print(f"   open: {candle.open}")
    print(f"   high: {candle.high}")
    print(f"   low: {candle.low}")
    print(f"   close: {candle.close}")
    print(f"   volume: {candle.volume}")
    print(f"   trades: {candle.trades}")
    print(f"   closed: {candle.closed}")

    # 检查数据格式
    candle_dict = candle.to_dict()
    print(f"📋 to_dict() 字段数: {len(candle_dict)}")
    print(f"📋 字段列表: {list(candle_dict.keys())}")

    # 测试backend数据准备
    prepared_data = candles_backend._prepare_data(candle_dict)
    print(f"🔧 Backend准备的数据长度: {len(prepared_data)}")
    print(f"🔧 数据类型: {[type(x).__name__ for x in prepared_data]}")
    print(f"🔧 Trades字段值: {prepared_data[-1]}")
    print("-" * 50)

async def main():
    print("🚀 开始测试实时K线数据...")
    f = FeedHandler()

    # 添加实时K线监听
    f.add_feed(BinanceFutures(
        symbols=['BTC-USDT-PERP'],
        channels=[CANDLES],
        callbacks={CANDLES: candle_callback},
        candle_interval='1m'
    ))

    # 运行5分钟然后停止
    await asyncio.sleep(300)
    print("✅ 测试完成")

if __name__ == '__main__':
    asyncio.run(main())