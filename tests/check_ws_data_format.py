#!/usr/bin/env python3
"""测试WebSocket K线数据格式"""
import asyncio
import json
from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES
from cryptofeed.exchanges import BinanceFutures

async def candle_callback(candle, receipt_timestamp):
    """记录WebSocket K线数据的原始格式"""
    print("\n======= WebSocket K线数据格式 =======")
    print(f"Candle对象字段:")
    print(f"  exchange: {candle.exchange}")
    print(f"  symbol: {candle.symbol}")
    print(f"  start: {candle.start}")
    print(f"  stop: {candle.stop}")
    print(f"  interval: {candle.interval}")
    print(f"  trades: {candle.trades}")
    print(f"  open: {candle.open}")
    print(f"  high: {candle.high}")
    print(f"  low: {candle.low}")
    print(f"  close: {candle.close}")
    print(f"  volume: {candle.volume}")
    print(f"  closed: {candle.closed}")
    print(f"  timestamp: {candle.timestamp}")
    print(f"  raw: {candle.raw}")
    print(f"  receipt_timestamp: {receipt_timestamp}")

    # 转换为字典查看
    candle_dict = candle.to_dict() if hasattr(candle, 'to_dict') else None
    if candle_dict:
        print(f"\nCandle.to_dict()返回的字段数: {len(candle_dict)}")
        print(f"字段列表: {list(candle_dict.keys())}")

    # 停止运行
    asyncio.get_event_loop().stop()

async def main():
    f = FeedHandler()
    # 订阅BTC-USDT 1分钟K线
    f.add_feed(BinanceFutures(
        symbols=['BTC-USDT-PERP'],
        channels=[CANDLES],
        callbacks={CANDLES: candle_callback},
        candle_interval='1m'
    ))

    f.run()

if __name__ == '__main__':
    asyncio.run(main())