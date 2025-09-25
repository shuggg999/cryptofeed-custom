#!/usr/bin/env python3
"""
Debug script to test PostgreSQL backends and identify why some write data while others don't.
"""
import asyncio
import logging
import sys
from datetime import datetime
import traceback

from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import TradePostgres, FundingPostgres, CandlesPostgres
from cryptofeed.defines import TRADES, FUNDING, CANDLES
from cryptofeed.exchanges import BinanceFutures

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database config
postgres_cfg = {
    'host': '127.0.0.1.txt',
    'user': 'postgres',
    'db': 'cryptofeed',
    'pw': 'password'
}

class DebugPostgresCallback:
    """Debug wrapper for PostgreSQL callbacks to add detailed logging"""

    def __init__(self, original_callback, callback_name):
        self.original_callback = original_callback
        self.callback_name = callback_name
        self.call_count = 0
        self.error_count = 0

    async def __call__(self, *args, **kwargs):
        self.call_count += 1
        logger.info(f"🔍 {self.callback_name} - Call #{self.call_count}")

        try:
            # Call the original callback
            result = await self.original_callback(*args, **kwargs)
            logger.info(f"✅ {self.callback_name} - Successfully processed call #{self.call_count}")
            return result
        except Exception as e:
            self.error_count += 1
            logger.error(f"❌ {self.callback_name} - Error in call #{self.call_count}: {e}")
            logger.error(f"❌ {self.callback_name} - Traceback: {traceback.format_exc()}")
            raise

# Enhanced PostgreSQL callbacks with better error handling
class DebugTradePostgres(TradePostgres):
    async def write_batch(self, updates: list):
        logger.info(f"🔍 DebugTradePostgres - Writing batch of {len(updates)} records")
        try:
            await super().write_batch(updates)
            logger.info(f"✅ DebugTradePostgres - Successfully wrote {len(updates)} records")
        except Exception as e:
            logger.error(f"❌ DebugTradePostgres - Batch write failed: {e}")
            logger.error(f"❌ DebugTradePostgres - Traceback: {traceback.format_exc()}")
            # Re-raise to see what happens
            raise

class DebugCandlesPostgres(CandlesPostgres):
    async def write_batch(self, updates: list):
        logger.info(f"🔍 DebugCandlesPostgres - Writing batch of {len(updates)} records")
        try:
            await super().write_batch(updates)
            logger.info(f"✅ DebugCandlesPostgres - Successfully wrote {len(updates)} records")
        except Exception as e:
            logger.error(f"❌ DebugCandlesPostgres - Batch write failed: {e}")
            logger.error(f"❌ DebugCandlesPostgres - Traceback: {traceback.format_exc()}")
            # Re-raise to see what happens
            raise

class DebugFundingPostgres(FundingPostgres):
    async def write_batch(self, updates: list):
        logger.info(f"🔍 DebugFundingPostgres - Writing batch of {len(updates)} records")
        try:
            await super().write_batch(updates)
            logger.info(f"✅ DebugFundingPostgres - Successfully wrote {len(updates)} records")
        except Exception as e:
            logger.error(f"❌ DebugFundingPostgres - Batch write failed: {e}")
            logger.error(f"❌ DebugFundingPostgres - Traceback: {traceback.format_exc()}")
            # Re-raise to see what happens
            raise

# Callback counters
trade_callback_count = 0
candle_callback_count = 0
funding_callback_count = 0

async def debug_trade_callback(trade, receipt_time):
    global trade_callback_count
    trade_callback_count += 1
    logger.info(f"📈 Trade callback #{trade_callback_count}: {trade.symbol} | {trade.side} | {trade.amount} @ {trade.price}")

async def debug_candle_callback(candle, receipt_time):
    global candle_callback_count
    candle_callback_count += 1
    logger.info(f"📊 Candle callback #{candle_callback_count}: {candle.symbol} | {candle.interval} | Close: {candle.close}")

async def debug_funding_callback(funding, receipt_time):
    global funding_callback_count
    funding_callback_count += 1
    logger.info(f"💰 Funding callback #{funding_callback_count}: {funding.symbol} | Rate: {funding.rate}")

def main():
    logger.info("🚀 Starting PostgreSQL backend debug test")
    logger.info("=" * 60)

    # Create debug backends
    trade_backend = DebugTradePostgres(**postgres_cfg)
    candle_backend = DebugCandlesPostgres(**postgres_cfg)
    funding_backend = DebugFundingPostgres(**postgres_cfg)

    # Wrap them with debug callbacks
    debug_trade_backend = DebugPostgresCallback(trade_backend, "TradePostgres")
    debug_candle_backend = DebugPostgresCallback(candle_backend, "CandlesPostgres")
    debug_funding_backend = DebugPostgresCallback(funding_backend, "FundingPostgres")

    config = {
        'log': {
            'level': 'DEBUG',
            'disabled': False
        },
        'backend_multiprocessing': False,  # Disable to see errors directly
        'uvloop': False
    }

    feed_handler = FeedHandler(config=config)

    # Test with a single symbol to isolate issues
    test_symbols = ['BTC-USDT-PERP']

    logger.info(f"📡 Testing with symbols: {test_symbols}")

    # Add feeds with debug backends
    logger.info("➕ Adding trades feed...")
    feed_handler.add_feed(
        BinanceFutures(
            symbols=test_symbols,
            channels=[TRADES],
            callbacks={
                TRADES: [debug_trade_backend, debug_trade_callback]
            }
        )
    )

    logger.info("➕ Adding candles feed...")
    feed_handler.add_feed(
        BinanceFutures(
            symbols=test_symbols,
            channels=[CANDLES],
            callbacks={
                CANDLES: [debug_candle_backend, debug_candle_callback]
            },
            candle_interval='1m'
        )
    )

    logger.info("➕ Adding funding feed...")
    feed_handler.add_feed(
        BinanceFutures(
            symbols=test_symbols,
            channels=[FUNDING],
            callbacks={
                FUNDING: [debug_funding_backend, debug_funding_callback]
            }
        )
    )

    logger.info("✅ All feeds configured")
    logger.info("🎯 Starting data collection (will run for 60 seconds)...")
    logger.info("=" * 60)

    try:
        # Run for 60 seconds to collect some data
        async def run_with_timeout():
            await asyncio.sleep(60)
            feed_handler.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Start the timeout task
        loop.create_task(run_with_timeout())

        # Run the feed handler
        feed_handler.run()

    except Exception as e:
        logger.error(f"❌ Feed handler error: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
    finally:
        logger.info("=" * 60)
        logger.info("📊 FINAL STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Trade callbacks: {trade_callback_count}")
        logger.info(f"Candle callbacks: {candle_callback_count}")
        logger.info(f"Funding callbacks: {funding_callback_count}")
        logger.info(f"Trade backend calls: {debug_trade_backend.call_count}, errors: {debug_trade_backend.error_count}")
        logger.info(f"Candle backend calls: {debug_candle_backend.call_count}, errors: {debug_candle_backend.error_count}")
        logger.info(f"Funding backend calls: {debug_funding_backend.call_count}, errors: {debug_funding_backend.error_count}")
        logger.info("=" * 60)

if __name__ == '__main__':
    main()