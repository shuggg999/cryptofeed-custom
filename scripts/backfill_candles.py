#!/usr/bin/env python3
"""
历史K线数据回填脚本 - 直接使用Binance REST API回填ClickHouse
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
import clickhouse_connect

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from cryptofeed_api.clients.binance import BinanceRestClient

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
    '1m': 7,      # 7天
    '5m': 30,     # 30天
    '30m': 90,    # 90天
    '4h': 365,    # 365天
    '1d': 730     # 2年
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

async def backfill_candles_for_symbol(client, symbol, interval, days):
    """为单个交易对回填K线数据"""
    binance_symbol = convert_symbol_for_binance(symbol)
    binance_interval = convert_interval_for_binance(interval)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)

    logger.info(f"开始回填 {symbol} {interval} 数据，从 {start_time} 到 {end_time}")

    try:
        # 获取现有数据的时间范围
        ch_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        existing_data = ch_client.query(f"""
            SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
            FROM candles
            WHERE symbol = '{symbol}' AND interval = '{interval}'
        """)

        if existing_data.result_rows and existing_data.result_rows[0][2] > 0:
            existing_min, existing_max, existing_count = existing_data.result_rows[0]
            logger.info(f"{symbol} {interval} 现有数据: {existing_count}条, {existing_min} 到 {existing_max}")
        else:
            logger.info(f"{symbol} {interval} 没有现有数据")

        # 分批获取历史数据
        current_time = start_time
        total_inserted = 0
        batch_size = timedelta(days=30)  # 每批30天

        binance_client = BinanceRestClient()

        while current_time < end_time:
            batch_end = min(current_time + batch_size, end_time)

            logger.info(f"获取 {symbol} {interval} 数据: {current_time} 到 {batch_end}")

            # 调用Binance API
            start_ms = int(current_time.timestamp() * 1000)
            end_ms = int(batch_end.timestamp() * 1000)

            try:
                url = f"https://fapi.binance.com/fapi/v1/klines"
                params = {
                    'symbol': binance_symbol,
                    'interval': binance_interval,
                    'startTime': start_ms,
                    'endTime': end_ms,
                    'limit': 1500
                }

                async with binance_client.session.get(url, params=params) as response:
                    if response.status == 200:
                        klines_data = await response.json()

                        if klines_data:
                            # 准备ClickHouse插入数据
                            insert_data = []
                            for kline in klines_data:
                                open_time = datetime.fromtimestamp(kline[0] / 1000)
                                close_time = datetime.fromtimestamp(kline[6] / 1000)

                                # 检查是否已存在
                                existing_check = ch_client.query(f"""
                                    SELECT COUNT(*) FROM candles
                                    WHERE symbol = '{symbol}' AND interval = '{interval}' AND timestamp = '{open_time}'
                                """)

                                if existing_check.result_rows[0][0] == 0:  # 不存在才插入
                                    insert_data.append([
                                        open_time,                    # timestamp
                                        symbol,                       # symbol
                                        interval,                     # interval
                                        float(kline[1]),              # open
                                        float(kline[2]),              # high
                                        float(kline[3]),              # low
                                        float(kline[4]),              # close
                                        float(kline[5]),              # volume
                                        close_time                    # receipt_timestamp
                                    ])

                            # 批量插入ClickHouse
                            if insert_data:
                                ch_client.insert('candles', insert_data)
                                total_inserted += len(insert_data)
                                logger.info(f"插入 {len(insert_data)} 条 {symbol} {interval} 数据")
                            else:
                                logger.info(f"{symbol} {interval} 数据已存在，跳过")

                    else:
                        logger.error(f"Binance API错误: {response.status}")

            except Exception as e:
                logger.error(f"获取 {symbol} {interval} 数据失败: {e}")

            current_time = batch_end
            await asyncio.sleep(0.1)  # 避免API限制

        ch_client.close()
        logger.info(f"完成回填 {symbol} {interval}: 总共插入 {total_inserted} 条数据")

    except Exception as e:
        logger.error(f"回填 {symbol} {interval} 失败: {e}")

async def main():
    """主函数"""
    logger.info("开始历史K线数据回填...")

    # 创建Binance客户端
    binance_client = BinanceRestClient()

    # 为每个交易对和时间间隔回填数据
    for symbol in SYMBOLS:
        for interval, days in INTERVALS.items():
            try:
                await backfill_candles_for_symbol(binance_client, symbol, interval, days)
                await asyncio.sleep(1)  # 避免过快请求
            except Exception as e:
                logger.error(f"回填 {symbol} {interval} 失败: {e}")

    # 最终统计
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
    logger.info("历史数据回填完成！")

if __name__ == '__main__':
    asyncio.run(main())