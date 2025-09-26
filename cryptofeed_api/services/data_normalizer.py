"""
数据标准化服务 - 统一不同数据源的格式
"""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ExchangeNormalizer:
    """交易所名称标准化器"""

    # 交易所名称映射表
    EXCHANGE_MAPPING = {
        # Binance 相关
        'binance': 'BINANCE_FUTURES',
        'BINANCE': 'BINANCE_FUTURES',
        'BINANCE_FUTURES': 'BINANCE_FUTURES',
        'binance_futures': 'BINANCE_FUTURES',

        # 其他交易所可以在这里添加
        'okx': 'OKX',
        'okex': 'OKX',
        'bybit': 'BYBIT',
        'huobi': 'HUOBI',
    }

    @classmethod
    def normalize_exchange_name(cls, exchange: str) -> str:
        """
        统一交易所名称

        Args:
            exchange: 原始交易所名称

        Returns:
            标准化后的交易所名称
        """
        if not exchange:
            return 'UNKNOWN'

        normalized = cls.EXCHANGE_MAPPING.get(exchange.strip(), exchange.upper())
        return normalized


class DataNormalizer:
    """数据标准化器"""

    def __init__(self):
        self.exchange_normalizer = ExchangeNormalizer()

    def normalize_candle_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化K线数据

        Args:
            data: 原始K线数据字典

        Returns:
            标准化后的数据字典
        """
        normalized_data = data.copy()

        # 1. 统一交易所名称
        if 'exchange' in normalized_data:
            normalized_data['exchange'] = self.exchange_normalizer.normalize_exchange_name(
                normalized_data['exchange']
            )

        # 2. 统一符号格式 (如果需要)
        if 'symbol' in normalized_data:
            normalized_data['symbol'] = self._normalize_symbol(normalized_data['symbol'])

        # 3. 统一时间间隔格式 (如果需要)
        if 'interval' in normalized_data:
            normalized_data['interval'] = self._normalize_interval(normalized_data['interval'])

        # 4. 统一数值格式
        for field in ['open', 'high', 'low', 'close', 'volume']:
            if field in normalized_data and normalized_data[field] is not None:
                try:
                    normalized_data[field] = float(normalized_data[field])
                except (ValueError, TypeError):
                    logger.warning(f"Failed to convert {field} to float: {normalized_data[field]}")
                    normalized_data[field] = 0.0

        return normalized_data

    def _normalize_symbol(self, symbol: str) -> str:
        """
        标准化交易对符号

        Args:
            symbol: 原始符号

        Returns:
            标准化后的符号
        """
        if not symbol:
            return symbol

        # 确保符号格式一致（例如都使用 BTC-USDT-PERP 格式）
        symbol = symbol.strip().upper()

        # 这里可以添加更多符号标准化逻辑
        return symbol

    def _normalize_interval(self, interval: str) -> str:
        """
        标准化时间间隔

        Args:
            interval: 原始时间间隔

        Returns:
            标准化后的时间间隔
        """
        if not interval:
            return interval

        interval = interval.strip().lower()

        # 统一间隔格式
        interval_mapping = {
            '1min': '1m',
            '5min': '5m',
            '30min': '30m',
            '1hour': '1h',
            '4hour': '4h',
            '1day': '1d',
        }

        return interval_mapping.get(interval, interval)


# 全局数据标准化实例
data_normalizer = DataNormalizer()


def normalize_data(data: Dict[str, Any], data_type: str = 'candle') -> Dict[str, Any]:
    """
    标准化数据的便捷函数

    Args:
        data: 原始数据
        data_type: 数据类型 ('candle', 'trade', 'funding' 等)

    Returns:
        标准化后的数据
    """
    if data_type == 'candle':
        return data_normalizer.normalize_candle_data(data)

    # 未来可以添加其他数据类型的标准化
    return data