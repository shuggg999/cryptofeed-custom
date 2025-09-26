'''
ClickHouse Backend for Cryptofeed
Copyright (C) 2017-2025 Bryant Moscon - bmoscon@gmail.com

ClickHouse backend implementation for high-performance time-series data storage
with native TTL support and optimal compression for cryptocurrency market data.
'''
import asyncio
from collections import defaultdict
from datetime import datetime as dt, datetime
from decimal import Decimal
from typing import Tuple
import logging

import clickhouse_connect
from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue
from cryptofeed.defines import CANDLES, FUNDING, OPEN_INTEREST, TICKER, TRADES, LIQUIDATIONS


LOG = logging.getLogger('feedhandler')


class ClickHouseCallback(BackendQueue):
    def __init__(self, host='localhost', user='default', password=None, database='cryptofeed',
                 port=8123, table=None, secure=False, custom_columns: dict = None,
                 none_to=None, numeric_type=float, **kwargs):
        """
        ClickHouse Backend for Cryptofeed

        Args:
            host: str - ClickHouse server host (default: localhost)
            user: str - Username (default: default)
            password: str - Password
            database: str - Database name (default: cryptofeed)
            port: int - HTTP port (default: 8123)
            table: str - Table name (defaults to data type specific table)
            secure: bool - Use HTTPS connection
            custom_columns: dict - Column mapping (optional)
            none_to: any - Value for None fields
            numeric_type: type - Numeric data type (default: float)
        """
        self.client = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.secure = secure
        self.table = table if table else self.default_table
        self.custom_columns = custom_columns
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.multiprocess = False  # ClickHouse不支持multiprocess
        self.running = True

        # 调用父类初始化
        super().__init__(**kwargs)

    async def _connect(self):
        """建立ClickHouse连接"""
        if self.client is None:
            try:
                self.client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                    database=self.database,
                    secure=self.secure
                )
                LOG.info(f"Connected to ClickHouse at {self.host}:{self.port}/{self.database}")
            except Exception as e:
                LOG.error(f"Failed to connect to ClickHouse: {e}")
                raise

    def _format_timestamp(self, timestamp):
        """格式化时间戳为ClickHouse DateTime64格式"""
        if timestamp is None:
            return None
        if isinstance(timestamp, (int, float)):
            return dt.utcfromtimestamp(timestamp)
        return timestamp

    def _format_decimal(self, value):
        """格式化数值为ClickHouse兼容的数值类型"""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return float(value)  # 使用float而不是Decimal，避免ClickHouse序列化问题
            except (ValueError, TypeError):
                return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    async def writer(self):
        """异步批量写入数据到ClickHouse"""
        await self._connect()

        while self.running:
            try:
                async with self.read_queue() as updates:
                    if len(updates) > 0:
                        batch_data = []
                        for data in updates:
                            formatted_data = self._prepare_data(data)
                            if formatted_data:
                                batch_data.append(formatted_data)

                        if batch_data:
                            await self.write_batch(batch_data)

            except Exception as e:
                LOG.error(f"Error in ClickHouse writer: {e}")
                await asyncio.sleep(1)  # 错误后短暂等待

    def _prepare_data(self, data):
        """准备数据格式以匹配ClickHouse表结构"""
        # 子类实现具体的数据格式化逻辑
        raise NotImplementedError("Subclasses must implement _prepare_data")

    async def write_batch(self, batch_data: list):
        """批量写入数据到ClickHouse"""
        if not batch_data:
            return

        try:
            await self._connect()
            # ClickHouse客户端的insert是同步的
            self.client.insert(self.table, batch_data)
            LOG.debug(f"Inserted {len(batch_data)} records into {self.table}")

        except Exception as e:
            LOG.error(f"Failed to insert batch into {self.table}: {e}")
            raise e  # 重新抛出异常以便调试


class TradeClickHouse(ClickHouseCallback, BackendCallback):
    """交易数据ClickHouse Backend"""
    default_table = 'trades'

    def _prepare_data(self, data):
        """准备交易数据 - 返回列表格式以匹配ClickHouse表结构"""
        return [
            self._format_timestamp(data.get('timestamp')),      # timestamp
            data.get('symbol'),                                 # symbol
            data.get('side'),                                   # side
            self._format_decimal(data.get('amount')),          # amount
            self._format_decimal(data.get('price')),           # price
            data.get('id', ''),                                # trade_id
            self._format_timestamp(data.get('receipt_timestamp')) # receipt_timestamp
        ]


class CandlesClickHouse(ClickHouseCallback, BackendCallback):
    """K线数据ClickHouse Backend"""
    default_table = 'candles'

    def _prepare_data(self, data):
        """准备K线数据 - 返回列表格式 (匹配实际9列表结构)"""
        # 注意：data是Candle.to_dict()的结果，包含更多字段
        # 我们需要按照表结构顺序提取正确的字段
        return [
            self._format_timestamp(data.get('timestamp')),      # timestamp
            data.get('exchange', ''),                           # exchange
            data.get('symbol'),                                 # symbol
            data.get('interval'),                               # interval
            self._format_decimal(data.get('open')),            # open
            self._format_decimal(data.get('high')),            # high
            self._format_decimal(data.get('low')),             # low
            self._format_decimal(data.get('close')),           # close
            self._format_decimal(data.get('volume')),          # volume
        ]


class FundingClickHouse(ClickHouseCallback, BackendCallback):
    """资金费率ClickHouse Backend"""
    default_table = 'funding'

    def _prepare_data(self, data):
        """准备资金费率数据 - 返回列表格式"""
        return [
            self._format_timestamp(data.get('timestamp')),      # timestamp
            data.get('symbol'),                                 # symbol
            self._format_decimal(data.get('rate')),            # rate
            self._format_timestamp(data.get('next_funding_time')), # next_funding_time
            self._format_timestamp(data.get('receipt_timestamp')) # receipt_timestamp
        ]


class TickerClickHouse(ClickHouseCallback, BackendCallback):
    """报价数据ClickHouse Backend"""
    default_table = 'ticker'

    def _prepare_data(self, data):
        """准备报价数据 - 返回列表格式"""
        return [
            self._format_timestamp(data.get('timestamp')),      # timestamp
            data.get('symbol'),                                 # symbol
            self._format_decimal(data.get('bid')),             # bid
            self._format_decimal(data.get('ask')),             # ask
            self._format_decimal(data.get('bid_size')),        # bid_size
            self._format_decimal(data.get('ask_size')),        # ask_size
            self._format_timestamp(data.get('receipt_timestamp')) # receipt_timestamp
        ]


class OpenInterestClickHouse(ClickHouseCallback, BackendCallback):
    """持仓量ClickHouse Backend"""
    default_table = 'open_interest'

    def _prepare_data(self, data):
        """准备持仓量数据 - 返回列表格式"""
        return [
            self._format_timestamp(data.get('timestamp')),      # timestamp
            data.get('exchange'),                               # exchange
            data.get('symbol'),                                 # symbol
            self._format_decimal(data.get('open_interest')),   # open_interest
            self._format_timestamp(data.get('receipt_timestamp')), # receipt_timestamp
            self._format_timestamp(data.get('timestamp')).date() if data.get('timestamp') else datetime.now().date()  # date
        ]


class LiquidationsClickHouse(ClickHouseCallback, BackendCallback):
    """清算数据ClickHouse Backend"""
    default_table = 'liquidations'

    def _prepare_data(self, data):
        """准备清算数据 - 返回列表格式 (匹配实际6列表结构)"""
        return [
            self._format_timestamp(data.get('timestamp')),      # timestamp
            data.get('exchange', ''),                           # exchange
            data.get('symbol'),                                 # symbol
            data.get('side'),                                   # side
            self._format_decimal(data.get('quantity')),        # amount (注意字段名映射)
            self._format_decimal(data.get('price')),           # price
        ]


class BookClickHouse(ClickHouseCallback, BackendBookCallback):
    """订单簿ClickHouse Backend"""
    default_table = 'orderbook'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)

    def _prepare_data(self, data):
        """准备订单簿数据"""
        # 订单簿数据需要特殊处理，将bids/asks展开为多行记录
        records = []

        timestamp = self._format_timestamp(data.get('timestamp'))
        receipt_timestamp = self._format_timestamp(data.get('receipt_timestamp'))
        symbol = data.get('symbol')

        # 处理完整快照
        if 'book' in data:
            book_data = data['book']
            # 处理买盘
            for price, size in book_data.get('bid', {}).items():
                records.append([
                    timestamp,
                    symbol,
                    'bid',
                    self._format_decimal(price),
                    self._format_decimal(size),
                    receipt_timestamp
                ])
            # 处理卖盘
            for price, size in book_data.get('ask', {}).items():
                records.append([
                    timestamp,
                    symbol,
                    'ask',
                    self._format_decimal(price),
                    self._format_decimal(size),
                    receipt_timestamp
                ])

        # 处理增量更新
        elif 'delta' in data:
            delta_data = data['delta']
            # 处理买盘变化
            for price, size in delta_data.get('bid', {}).items():
                records.append([
                    timestamp,
                    symbol,
                    'bid',
                    self._format_decimal(price),
                    self._format_decimal(size),
                    receipt_timestamp
                ])
            # 处理卖盘变化
            for price, size in delta_data.get('ask', {}).items():
                records.append([
                    timestamp,
                    symbol,
                    'ask',
                    self._format_decimal(price),
                    self._format_decimal(size),
                    receipt_timestamp
                ])

        return records

    async def write_batch(self, batch_data: list):
        """订单簿数据的特殊批量写入处理"""
        if not batch_data:
            return

        # 展平订单簿记录列表
        flattened_records = []
        for item in batch_data:
            if isinstance(item, list):
                flattened_records.extend(item)
            else:
                flattened_records.append(item)

        if flattened_records:
            try:
                await self._connect()
                self.client.insert(self.table, flattened_records)
                LOG.debug(f"Inserted {len(flattened_records)} orderbook records into {self.table}")
            except Exception as e:
                LOG.error(f"Failed to insert orderbook batch: {e}")