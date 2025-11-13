"""
ClickHouse Backend for Cryptofeed
高性能、高压缩率的时序数据存储
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

logger = logging.getLogger(__name__)


class ClickHouseBackend:
    """
    ClickHouse异步批量写入Backend
    特点：
    1. 批量写入，提高性能
    2. 异步处理，不阻塞数据采集
    3. 自动重试机制
    4. 高压缩率存储
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "cryptofeed",
        user: str = "default",
        password: str = "password",
        batch_size: int = 1000,
        batch_timeout: float = 1.0,
        max_retries: int = 3,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.max_retries = max_retries

        # 兼容 cryptofeed 的 Backend 基类（需要 queue 属性）
        self.queue = None

        # 数据缓冲区
        self.buffer: Dict[str, List[Dict]] = {
            "trades": [],
            "candles": [],
            "funding": [],
            "liquidations": [],
            "open_interest": [],
        }

        # 批量写入任务
        self.flush_task = None
        self.is_running = False

        # HTTP客户端（用于异步写入）
        self.http_session = None

        # Native客户端（用于同步操作）
        self.client = None

    async def start(self):
        """启动Backend"""
        try:
            # 创建HTTP会话
            self.http_session = aiohttp.ClientSession()

            # 测试连接
            self.client = Client(
                host=self.host.replace("http://", "").replace("https://", ""),
                port=9000,  # Native端口
                database=self.database,
                user=self.user,
                password=self.password,
                settings={"use_numpy": False},
            )

            # 验证数据库存在
            self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"✅ Connected to ClickHouse at {self.host}:{self.port}")

            # 启动批量写入任务
            self.is_running = True
            self.flush_task = asyncio.create_task(self._periodic_flush())

        except Exception as e:
            logger.error(f"Failed to start ClickHouse backend: {e}")
            raise

    async def stop(self):
        """停止Backend"""
        self.is_running = False

        # 刷新剩余数据
        await self._flush_all()

        # 取消定期刷新任务
        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass

        # 关闭连接
        if self.http_session:
            await self.http_session.close()

        if self.client:
            self.client.disconnect()

        logger.info("ClickHouse backend stopped")

    async def write_trades(self, trades: List[Dict]):
        """写入交易数据"""
        self.buffer["trades"].extend(trades)

        # 如果缓冲区满了，立即刷新
        if len(self.buffer["trades"]) >= self.batch_size:
            await self._flush_trades()

    async def write_candles(self, candles: List[Dict]):
        """写入K线数据"""
        self.buffer["candles"].extend(candles)

        if len(self.buffer["candles"]) >= self.batch_size:
            await self._flush_candles()

    async def write_funding(self, funding: List[Dict]):
        """写入资金费率数据"""
        self.buffer["funding"].extend(funding)

        if len(self.buffer["funding"]) >= self.batch_size:
            await self._flush_funding()

    async def write_liquidations(self, liquidations: List[Dict]):
        """写入清算数据"""
        self.buffer["liquidations"].extend(liquidations)

        if len(self.buffer["liquidations"]) >= self.batch_size:
            await self._flush_liquidations()

    async def write_open_interest(self, open_interest: List[Dict]):
        """写入持仓量数据"""
        self.buffer["open_interest"].extend(open_interest)

        if len(self.buffer["open_interest"]) >= self.batch_size:
            await self._flush_open_interest()

    async def _periodic_flush(self):
        """定期刷新缓冲区"""
        while self.is_running:
            await asyncio.sleep(self.batch_timeout)
            await self._flush_all()

    async def _flush_all(self):
        """刷新所有缓冲区"""
        tasks = []

        if self.buffer["trades"]:
            tasks.append(self._flush_trades())
        if self.buffer["candles"]:
            tasks.append(self._flush_candles())
        if self.buffer["funding"]:
            tasks.append(self._flush_funding())
        if self.buffer["liquidations"]:
            tasks.append(self._flush_liquidations())
        if self.buffer["open_interest"]:
            tasks.append(self._flush_open_interest())

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _flush_trades(self):
        """刷新交易数据到ClickHouse"""
        if not self.buffer["trades"]:
            return

        data = self.buffer["trades"]
        self.buffer["trades"] = []

        try:
            await self._insert_data("trades", data)
            logger.debug(f"Flushed {len(data)} trades to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to flush trades: {e}")
            # 失败的数据重新加入缓冲区
            self.buffer["trades"].extend(data)

    async def _flush_candles(self):
        """刷新K线数据到ClickHouse"""
        if not self.buffer["candles"]:
            return

        data = self.buffer["candles"]
        self.buffer["candles"] = []

        try:
            await self._insert_data("candles", data)
            logger.debug(f"Flushed {len(data)} candles to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to flush candles: {e}")
            self.buffer["candles"].extend(data)

    async def _flush_funding(self):
        """刷新资金费率数据到ClickHouse"""
        if not self.buffer["funding"]:
            return

        data = self.buffer["funding"]
        self.buffer["funding"] = []

        try:
            await self._insert_data("funding", data)
            logger.debug(f"Flushed {len(data)} funding records to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to flush funding: {e}")
            self.buffer["funding"].extend(data)

    async def _flush_liquidations(self):
        """刷新清算数据到ClickHouse"""
        if not self.buffer["liquidations"]:
            return

        data = self.buffer["liquidations"]
        self.buffer["liquidations"] = []

        try:
            await self._insert_data("liquidations", data)
            logger.debug(f"Flushed {len(data)} liquidations to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to flush liquidations: {e}")
            self.buffer["liquidations"].extend(data)

    async def _flush_open_interest(self):
        """刷新持仓量数据到ClickHouse"""
        if not self.buffer["open_interest"]:
            return

        data = self.buffer["open_interest"]
        self.buffer["open_interest"] = []

        try:
            await self._insert_data("open_interest", data)
            logger.debug(f"Flushed {len(data)} open_interest records to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to flush open_interest: {e}")
            self.buffer["open_interest"].extend(data)

    async def _insert_data(self, table: str, data: List[Dict]):
        """异步插入数据到ClickHouse"""
        if not data:
            return

        # 构建INSERT查询
        columns = list(data[0].keys())
        values = []

        for row in data:
            row_values = []
            for col in columns:
                val = row.get(col)
                if isinstance(val, datetime):
                    row_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'")
                elif isinstance(val, (int, float, Decimal)):
                    row_values.append(str(val))
                elif isinstance(val, str):
                    row_values.append(f"'{val}'")
                elif val is None:
                    row_values.append("NULL")
                else:
                    row_values.append(f"'{str(val)}'")
            values.append(f"({','.join(row_values)})")

        query = f"""
            INSERT INTO {self.database}.{table} ({','.join(columns)})
            VALUES {','.join(values)}
        """

        # 使用HTTP接口异步执行
        url = f"http://{self.host}:{self.port}/"
        params = {"database": self.database, "query": query, "user": self.user, "password": self.password}

        # 重试机制
        for attempt in range(self.max_retries):
            try:
                async with self.http_session.post(url, params=params) as resp:
                    if resp.status == 200:
                        return
                    else:
                        error_text = await resp.text()
                        raise ClickHouseError(f"Insert failed: {error_text}")
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2**attempt)  # 指数退避


class ClickHouseTrade(ClickHouseBackend):
    """交易数据Backend"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = "trades"

    async def __call__(self, trade, receipt_timestamp):
        """Cryptofeed回调接口"""
        data = {
            "timestamp": datetime.fromtimestamp(trade.timestamp),
            "exchange": trade.exchange,
            "symbol": trade.symbol,
            "side": trade.side,
            "amount": float(trade.amount),
            "price": float(trade.price),
            "trade_id": trade.id if hasattr(trade, "id") else None,
            "receipt_timestamp": datetime.fromtimestamp(receipt_timestamp),
        }

        await self.write_trades([data])


class ClickHouseCandles(ClickHouseBackend):
    """K线数据Backend"""

    def __init__(self, interval: str = "1m", **kwargs):
        super().__init__(**kwargs)
        self.table = "candles"
        self.interval = interval

    async def __call__(self, candle, receipt_timestamp):
        """Cryptofeed回调接口"""
        data = {
            "timestamp": datetime.fromtimestamp(candle.timestamp),
            "exchange": candle.exchange,
            "symbol": candle.symbol,
            "interval": self.interval,
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "trades": candle.trades if hasattr(candle, "trades") else 0,
        }

        await self.write_candles([data])


class ClickHouseFunding(ClickHouseBackend):
    """资金费率Backend"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = "funding"

    async def __call__(self, funding, receipt_timestamp):
        """Cryptofeed回调接口"""
        data = {
            "timestamp": datetime.fromtimestamp(funding.timestamp),
            "exchange": funding.exchange,
            "symbol": funding.symbol,
            "rate": float(funding.rate),
            "mark_price": float(funding.mark_price) if funding.mark_price else None,
            "next_funding_time": (
                datetime.fromtimestamp(funding.next_funding_time) if funding.next_funding_time else None
            ),
            "predicted_rate": float(funding.predicted_rate) if funding.predicted_rate else None,
            "receipt_timestamp": datetime.fromtimestamp(receipt_timestamp),
        }

        await self.write_funding([data])


class ClickHouseLiquidations(ClickHouseBackend):
    """清算数据Backend"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = "liquidations"

    async def __call__(self, liquidation, receipt_timestamp):
        """Cryptofeed回调接口"""
        data = {
            "timestamp": datetime.fromtimestamp(liquidation.timestamp),
            "exchange": liquidation.exchange,
            "symbol": liquidation.symbol,
            "side": liquidation.side,
            "quantity": float(liquidation.quantity),
            "price": float(liquidation.price),
            "liquidation_id": liquidation.id if hasattr(liquidation, "id") else None,
            "receipt_timestamp": datetime.fromtimestamp(receipt_timestamp),
        }

        await self.write_liquidations([data])


class ClickHouseOpenInterest(ClickHouseBackend):
    """持仓量Backend"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = "open_interest"

    async def __call__(self, open_interest, receipt_timestamp):
        """Cryptofeed回调接口"""
        data = {
            "timestamp": datetime.fromtimestamp(open_interest.timestamp),
            "exchange": open_interest.exchange,
            "symbol": open_interest.symbol,
            "open_interest": float(open_interest.open_interest),
            "receipt_timestamp": datetime.fromtimestamp(receipt_timestamp),
        }

        await self.write_open_interest([data])
