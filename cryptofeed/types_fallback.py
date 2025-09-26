'''
Pure Python fallback for cryptofeed.types when Cython compilation fails
Copyright (C) 2017-2025 Bryant Moscon - bmoscon@gmail.com
'''
from decimal import Decimal
from cryptofeed.defines import BID, ASK
from order_book import OrderBook as _OrderBook


class Trade:
    def __init__(self, exchange=None, symbol=None, side=None, amount=None, price=None,
                 timestamp=None, id=None, type=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.side = side
        self.amount = amount
        self.price = price
        self.timestamp = timestamp
        self.id = id
        self.type = type
        self.raw = raw


class Ticker:
    def __init__(self, exchange=None, symbol=None, bid=None, ask=None, timestamp=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.bid = bid
        self.ask = ask
        self.timestamp = timestamp
        self.raw = raw


class Liquidation:
    def __init__(self, exchange=None, symbol=None, side=None, quantity=None, price=None,
                 timestamp=None, id=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.side = side
        self.quantity = quantity
        self.price = price
        self.timestamp = timestamp
        self.id = id
        self.raw = raw


class Funding:
    def __init__(self, exchange=None, symbol=None, timestamp=None, rate=None,
                 next_funding_time=None, predicted_rate=None, mark_price=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.timestamp = timestamp
        self.rate = rate
        self.next_funding_time = next_funding_time
        self.predicted_rate = predicted_rate
        self.mark_price = mark_price
        self.raw = raw


class Candle:
    def __init__(self, exchange=None, symbol=None, start=None, stop=None,
                 interval=None, trades=None, open=None, high=None, low=None,
                 close=None, volume=None, closed=None, timestamp=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.start = start
        self.stop = stop
        self.interval = interval
        self.trades = trades
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.closed = closed
        self.timestamp = timestamp
        self.raw = raw


class Index:
    def __init__(self, exchange=None, symbol=None, price=None, timestamp=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.price = price
        self.timestamp = timestamp
        self.raw = raw


class OpenInterest:
    def __init__(self, exchange=None, symbol=None, open_interest=None, timestamp=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.open_interest = open_interest
        self.timestamp = timestamp
        self.raw = raw


class OrderBook:
    def __init__(self, exchange=None, symbol=None, book=None, timestamp=None,
                 delta=None, sequence_number=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.book = book or _OrderBook()
        self.timestamp = timestamp
        self.delta = delta
        self.sequence_number = sequence_number
        self.raw = raw


class Order:
    def __init__(self, exchange=None, symbol=None, side=None, amount=None, price=None,
                 order_id=None, timestamp=None, order_type=None, account=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.side = side
        self.amount = amount
        self.price = price
        self.order_id = order_id
        self.timestamp = timestamp
        self.order_type = order_type
        self.account = account
        self.raw = raw


class OrderInfo:
    def __init__(self, exchange=None, symbol=None, order_id=None, side=None,
                 order_type=None, amount=None, executed=None, pending=None,
                 price=None, account=None, timestamp=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.order_id = order_id
        self.side = side
        self.order_type = order_type
        self.amount = amount
        self.executed = executed
        self.pending = pending
        self.price = price
        self.account = account
        self.timestamp = timestamp
        self.raw = raw


class Balance:
    def __init__(self, exchange=None, currency=None, balance=None, reserved=None, raw=None):
        self.exchange = exchange
        self.currency = currency
        self.balance = balance
        self.reserved = reserved
        self.raw = raw


class L1Book:
    def __init__(self, exchange=None, symbol=None, bid=None, ask=None,
                 bid_amount=None, ask_amount=None, timestamp=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.bid = bid
        self.ask = ask
        self.bid_amount = bid_amount
        self.ask_amount = ask_amount
        self.timestamp = timestamp
        self.raw = raw


class Transaction:
    def __init__(self, exchange=None, currency=None, type=None, status=None,
                 amount=None, timestamp=None, tx_id=None, raw=None):
        self.exchange = exchange
        self.currency = currency
        self.type = type
        self.status = status
        self.amount = amount
        self.timestamp = timestamp
        self.tx_id = tx_id
        self.raw = raw


class Fill:
    def __init__(self, exchange=None, symbol=None, price=None, amount=None,
                 side=None, fee=None, id=None, order_id=None, timestamp=None,
                 account=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.price = price
        self.amount = amount
        self.side = side
        self.fee = fee
        self.id = id
        self.order_id = order_id
        self.timestamp = timestamp
        self.account = account
        self.raw = raw


class Position:
    def __init__(self, exchange=None, symbol=None, side=None, position=None,
                 entry_price=None, unrealised_pnl=None, timestamp=None,
                 account=None, raw=None):
        self.exchange = exchange
        self.symbol = symbol
        self.side = side
        self.position = position
        self.entry_price = entry_price
        self.unrealised_pnl = unrealised_pnl
        self.timestamp = timestamp
        self.account = account
        self.raw = raw