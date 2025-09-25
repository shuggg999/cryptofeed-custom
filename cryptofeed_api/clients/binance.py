"""
Binance REST API 客户端封装
用于历史数据补充和数据完整性检查
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
from decimal import Decimal

import aiohttp
from aiohttp import ClientTimeout, ClientResponseError

logger = logging.getLogger(__name__)


class BinanceAPIError(Exception):
    """Binance API 错误"""
    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


class BinanceRateLimitError(BinanceAPIError):
    """Binance 速率限制错误"""
    pass


class BinanceRestClient:
    """Binance REST API 客户端"""

    def __init__(self, base_url: str = "https://fapi.binance.com"):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
        self._rate_limit_lock = asyncio.Semaphore(10)  # 限制并发请求数
        self._last_request_time = 0
        self._min_request_interval = 0.1  # 最小请求间隔（秒）

    async def __aenter__(self):
        await self.init_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def init_session(self):
        """初始化HTTP会话"""
        if not self.session:
            timeout = ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "User-Agent": "Cryptofeed-API/1.0.0",
                    "Content-Type": "application/json"
                }
            )

    async def close(self):
        """关闭HTTP会话"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """发送HTTP请求"""
        if not self.session:
            await self.init_session()

        url = f"{self.base_url}{endpoint}"

        async with self._rate_limit_lock:
            # 简单的速率限制
            current_time = asyncio.get_event_loop().time()
            time_since_last = current_time - self._last_request_time
            if time_since_last < self._min_request_interval:
                await asyncio.sleep(self._min_request_interval - time_since_last)

            try:
                async with self.session.get(url, params=params) as response:
                    self._last_request_time = asyncio.get_event_loop().time()

                    if response.status == 429:
                        # 处理速率限制
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limited, waiting {retry_after} seconds")
                        raise BinanceRateLimitError(f"Rate limited, retry after {retry_after}s")

                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Binance API error {response.status}: {error_text}")
                        raise BinanceAPIError(f"API error {response.status}: {error_text}", response.status)

                    data = await response.json()
                    return data

            except aiohttp.ClientError as e:
                logger.error(f"HTTP client error: {e}")
                raise BinanceAPIError(f"HTTP client error: {e}")

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """
        获取K线数据

        Args:
            symbol: 交易对符号 (如 BTCUSDT)
            interval: 时间间隔 (1m, 5m, 1h, 1d等)
            start_time: 开始时间
            end_time: 结束时间
            limit: 返回数量限制 (最大1500)

        Returns:
            K线数据列表
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": min(limit, 1500)  # Binance限制
        }

        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        try:
            data = await self._make_request("/fapi/v1/klines", params)

            # 转换数据格式
            candles = []
            for item in data:
                candles.append({
                    "timestamp": datetime.fromtimestamp(int(item[0]) / 1000),
                    "open_price": Decimal(str(item[1])),
                    "high_price": Decimal(str(item[2])),
                    "low_price": Decimal(str(item[3])),
                    "close_price": Decimal(str(item[4])),
                    "volume": Decimal(str(item[5])),
                    "close_time": datetime.fromtimestamp(int(item[6]) / 1000),
                    "quote_volume": Decimal(str(item[7])),
                    "trades_count": int(item[8])
                })

            return candles

        except BinanceRateLimitError:
            raise
        except Exception as e:
            logger.error(f"Failed to get klines for {symbol}: {e}")
            raise BinanceAPIError(f"Failed to get klines: {e}")

    async def get_trades(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """
        获取交易数据

        Args:
            symbol: 交易对符号
            start_time: 开始时间
            end_time: 结束时间
            limit: 返回数量限制 (最大1000)

        Returns:
            交易数据列表
        """
        params = {
            "symbol": symbol,
            "limit": min(limit, 1000)
        }

        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        try:
            data = await self._make_request("/fapi/v1/aggTrades", params)

            # 转换数据格式
            trades = []
            for item in data:
                trades.append({
                    "timestamp": datetime.fromtimestamp(int(item["T"]) / 1000),
                    "price": Decimal(str(item["p"])),
                    "amount": Decimal(str(item["q"])),
                    "side": "buy" if item["m"] is False else "sell",  # m=true表示买方是maker
                    "trade_id": str(item["a"])
                })

            return trades

        except BinanceRateLimitError:
            raise
        except Exception as e:
            logger.error(f"Failed to get trades for {symbol}: {e}")
            raise BinanceAPIError(f"Failed to get trades: {e}")

    async def get_funding_rate(self, symbol: str, limit: int = 100) -> List[Dict]:
        """
        获取资金费率历史

        Args:
            symbol: 交易对符号
            limit: 返回数量限制 (最大1000)

        Returns:
            资金费率数据列表
        """
        params = {
            "symbol": symbol,
            "limit": min(limit, 1000)
        }

        try:
            data = await self._make_request("/fapi/v1/fundingRate", params)

            # 转换数据格式
            funding_rates = []
            for item in data:
                funding_rates.append({
                    "timestamp": datetime.fromtimestamp(int(item["fundingTime"]) / 1000),
                    "rate": Decimal(str(item["fundingRate"])),
                    "symbol": item["symbol"]
                })

            return funding_rates

        except Exception as e:
            logger.error(f"Failed to get funding rate for {symbol}: {e}")
            raise BinanceAPIError(f"Failed to get funding rate: {e}")

    async def get_exchange_info(self) -> Dict:
        """获取交易所信息"""
        try:
            return await self._make_request("/fapi/v1/exchangeInfo")
        except Exception as e:
            logger.error(f"Failed to get exchange info: {e}")
            raise BinanceAPIError(f"Failed to get exchange info: {e}")

    async def get_symbols(self) -> List[str]:
        """获取所有USDT永续合约符号"""
        try:
            exchange_info = await self.get_exchange_info()
            symbols = []

            for symbol_info in exchange_info.get("symbols", []):
                if (symbol_info.get("status") == "TRADING" and
                    symbol_info.get("contractType") == "PERPETUAL" and
                    symbol_info.get("quoteAsset") == "USDT"):
                    symbols.append(symbol_info["symbol"])

            return sorted(symbols)

        except Exception as e:
            logger.error(f"Failed to get symbols: {e}")
            raise BinanceAPIError(f"Failed to get symbols: {e}")

    async def batch_get_klines(
        self,
        symbols: List[str],
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
        max_concurrent: int = 5
    ) -> Dict[str, List[Dict]]:
        """
        批量获取多个符号的K线数据

        Args:
            symbols: 交易对符号列表
            interval: 时间间隔
            start_time: 开始时间
            end_time: 结束时间
            limit: 每个符号的返回数量限制
            max_concurrent: 最大并发数

        Returns:
            符号到K线数据的映射
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}

        async def fetch_symbol_klines(symbol: str):
            async with semaphore:
                try:
                    data = await self.get_klines(symbol, interval, start_time, end_time, limit)
                    results[symbol] = data
                    logger.debug(f"Fetched {len(data)} klines for {symbol}")
                except Exception as e:
                    logger.error(f"Failed to fetch klines for {symbol}: {e}")
                    results[symbol] = []

        # 执行并发请求
        tasks = [fetch_symbol_klines(symbol) for symbol in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)

        return results

    def normalize_symbol(self, symbol: str) -> str:
        """
        规范化交易对符号

        将内部格式 'BTC-USDT-PERP' 转换为Binance格式 'BTCUSDT'
        """
        if "-" in symbol:
            # 移除 -USDT-PERP 后缀，重新组合
            parts = symbol.split("-")
            if len(parts) >= 2:
                return f"{parts[0]}USDT"

        # 如果已经是Binance格式，直接返回
        if symbol.endswith("USDT") and "-" not in symbol:
            return symbol

        return symbol

    def denormalize_symbol(self, binance_symbol: str) -> str:
        """
        反规范化交易对符号

        将Binance格式 'BTCUSDT' 转换为内部格式 'BTC-USDT-PERP'
        """
        if binance_symbol.endswith("USDT") and "-" not in binance_symbol:
            base = binance_symbol[:-4]  # 移除USDT
            return f"{base}-USDT-PERP"

        return binance_symbol