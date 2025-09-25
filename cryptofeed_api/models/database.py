"""
数据库模型定义
"""
from sqlalchemy import Column, String, DECIMAL, TIMESTAMP, BigInteger, Index, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from decimal import Decimal
from datetime import datetime

Base = declarative_base()


class TradeData(Base):
    """交易数据模型"""
    __tablename__ = 'trades'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP, nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    side = Column(String(10), nullable=False)  # 'buy' or 'sell'
    amount = Column(DECIMAL(20, 8), nullable=False)
    price = Column(DECIMAL(20, 8), nullable=False)
    exchange = Column(String(50), nullable=False, default='binance')
    trade_id = Column(String(100))

    # 复合索引用于高效查询
    __table_args__ = (
        Index('idx_trades_symbol_timestamp', 'symbol', 'timestamp'),
        Index('idx_trades_exchange_symbol', 'exchange', 'symbol'),
    )


class CandleData(Base):
    """K线数据模型"""
    __tablename__ = 'candles'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP, nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    interval = Column(String(10), nullable=False)  # '1m', '5m', '1h', '1d' etc
    open_price = Column(DECIMAL(20, 8), nullable=False)
    high_price = Column(DECIMAL(20, 8), nullable=False)
    low_price = Column(DECIMAL(20, 8), nullable=False)
    close_price = Column(DECIMAL(20, 8), nullable=False)
    volume = Column(DECIMAL(30, 8), nullable=False)
    exchange = Column(String(50), nullable=False, default='binance')

    # 复合索引用于高效查询
    __table_args__ = (
        Index('idx_candles_symbol_interval_timestamp', 'symbol', 'interval', 'timestamp'),
        Index('idx_candles_exchange_symbol', 'exchange', 'symbol'),
    )


class FundingData(Base):
    """资金费率数据模型"""
    __tablename__ = 'funding'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP, nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    rate = Column(DECIMAL(10, 8), nullable=False)
    next_funding_time = Column(TIMESTAMP)
    exchange = Column(String(50), nullable=False, default='binance')

    # 复合索引用于高效查询
    __table_args__ = (
        Index('idx_funding_symbol_timestamp', 'symbol', 'timestamp'),
        Index('idx_funding_exchange_symbol', 'exchange', 'symbol'),
    )


class TickerData(Base):
    """行情数据模型"""
    __tablename__ = 'tickers'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP, nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    bid = Column(DECIMAL(20, 8), nullable=False)
    ask = Column(DECIMAL(20, 8), nullable=False)
    exchange = Column(String(50), nullable=False, default='binance')

    # 复合索引用于高效查询
    __table_args__ = (
        Index('idx_tickers_symbol_timestamp', 'symbol', 'timestamp'),
        Index('idx_tickers_exchange_symbol', 'exchange', 'symbol'),
    )


class DataGapLog(Base):
    """数据缺口日志模型 - 用于跟踪需要补充的历史数据"""
    __tablename__ = 'data_gaps'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(50), nullable=False, index=True)
    data_type = Column(String(20), nullable=False)  # 'trades', 'candles', 'funding'
    interval = Column(String(10))  # for candles only
    gap_start = Column(TIMESTAMP, nullable=False)
    gap_end = Column(TIMESTAMP, nullable=False)
    status = Column(String(20), nullable=False, default='pending')  # 'pending', 'filled', 'failed'
    exchange = Column(String(50), nullable=False, default='binance')
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    filled_at = Column(TIMESTAMP)
    error_message = Column(Text)

    # 复合索引用于高效查询
    __table_args__ = (
        Index('idx_gaps_symbol_type_status', 'symbol', 'data_type', 'status'),
        Index('idx_gaps_exchange_symbol', 'exchange', 'symbol'),
    )