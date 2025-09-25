#!/usr/bin/env python3
"""
简单的历史K线数据回填脚本 - 直接使用requests + ClickHouse
"""
import asyncio
import logging
import sys
import requests
import time
from datetime import datetime, timedelta
from pathlib import Path
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ClickHouse配置
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'user': 'default',
    'password': 'password123',
    'database': 'cryptofeed'
}

# 回填配置
SYMBOLS = ['BTC-USDT-PERP', 'ETH-USDT-PERP', 'SOL-USDT-PERP', 'ADA-USDT-PERP', 'DOGE-USDT-PERP']
INTERVALS = {
    '1m': 3,      # 3天
    '5m': 7,      # 7天
    '30m': 30,    # 30天
    '4h': 90,     # 90天
    '1d': 365     # 1年
}

def convert_symbol_for_binance(symbol):
    """转换cryptofeed符号格式为Binance格式"""
    if symbol.endswith('-PERP'):
        return symbol.replace('-USDT-PERP', 'USDT').replace('-', '')
    return symbol.replace('-', '')

def convert_interval_for_binance(interval):
    """转换时间间隔格式"""
    mapping = {
        '1m': '1m',
        '5m': '5m',
        '30m': '30m',
        '4h': '4h',
        '1d': '1d'
    }
    return mapping.get(interval, interval)

def backfill_candles_for_symbol(symbol, interval, days):
    """为单个交易对回填K线数据"""
    binance_symbol = convert_symbol_for_binance(symbol)
    binance_interval = convert_interval_for_binance(interval)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)

    logger.info(f"开始回填 {symbol} {interval} 数据，从 {start_time} 到 {end_time}")

    try:
        # 连接ClickHouse
        ch_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

        # 计算时间戳
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        # 调用Binance API
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': binance_symbol,
            'interval': binance_interval,
            'startTime': start_ms,
            'endTime': end_ms,
            'limit': 1500
        }

        logger.info(f"请求Binance API: {binance_symbol} {binance_interval}")
        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 200:
            klines_data = response.json()
            logger.info(f"获取到 {len(klines_data)} 条K线数据")

            if klines_data:
                # 准备ClickHouse插入数据
                insert_data = []
                for kline in klines_data:
                    open_time = datetime.fromtimestamp(kline[0] / 1000)

                    # 检查是否已存在 - 简化版本，减少查询次数
                    insert_data.append([
                        open_time,                    # timestamp
                        symbol,                       # symbol
                        interval,                     # interval
                        float(kline[1]),              # open
                        float(kline[2]),              # high
                        float(kline[3]),              # low
                        float(kline[4]),              # close
                        float(kline[5]),              # volume
                        open_time                     # receipt_timestamp
                    ])

                # 使用INSERT OR IGNORE来避免重复
                if insert_data:
                    try:
                        # 先删除可能存在的重复数据
                        ch_client.command(f"""
                            DELETE FROM candles
                            WHERE symbol = '{symbol}' AND interval = '{interval}'
                            AND timestamp >= '{start_time}' AND timestamp <= '{end_time}'
                        """)

                        # 插入新数据
                        ch_client.insert('candles', insert_data)
                        logger.info(f"成功插入 {len(insert_data)} 条 {symbol} {interval} 数据")

                        return len(insert_data)
                    except Exception as e:
                        logger.error(f"插入数据失败: {e}")
                        return 0
            else:
                logger.info(f"没有获取到 {symbol} {interval} 数据")
                return 0
        else:
            logger.error(f"Binance API错误: {response.status_code} - {response.text}")
            return 0

    except Exception as e:
        logger.error(f"回填 {symbol} {interval} 失败: {e}")
        return 0
    finally:
        if 'ch_client' in locals():
            ch_client.close()

def main():
    """主函数"""
    logger.info("开始历史K线数据回填...")

    total_inserted = 0

    # 为每个交易对和时间间隔回填数据
    for symbol in SYMBOLS:
        for interval, days in INTERVALS.items():
            try:
                inserted = backfill_candles_for_symbol(symbol, interval, days)
                total_inserted += inserted
                logger.info(f"完成 {symbol} {interval}: 插入 {inserted} 条数据")

                # 避免API限制
                time.sleep(1)

            except Exception as e:
                logger.error(f"回填 {symbol} {interval} 失败: {e}")

    # 最终统计
    logger.info(f"总共插入 {total_inserted} 条历史数据")

    try:
        ch_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        final_stats = ch_client.query("""
            SELECT interval, COUNT(*), MIN(timestamp), MAX(timestamp)
            FROM candles
            GROUP BY interval
            ORDER BY interval
        """)

        logger.info("=== 回填完成统计 ===")
        for row in final_stats.result_rows:
            interval, count, min_time, max_time = row
            logger.info(f"{interval}: {count:,} 条数据, {min_time} 到 {max_time}")

        ch_client.close()
    except Exception as e:
        logger.error(f"获取最终统计失败: {e}")

    logger.info("历史数据回填完成！")

if __name__ == '__main__':
    main()