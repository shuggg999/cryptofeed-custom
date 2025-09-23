#!/usr/bin/env python3
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import (
    TradePostgres, FundingPostgres, CandlesPostgres, TickerPostgres
)
import asyncio
import psycopg2
from datetime import datetime
from cryptofeed.defines import TRADES, FUNDING, CANDLES, TICKER
from cryptofeed.exchanges import BinanceFutures

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/cryptofeed_advanced.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL config
postgres_cfg = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'db': 'cryptofeed',
    'pw': 'password'
}

# Monitor config
INTERVALS = ['1m', '5m', '30m', '4h', '1d']
MAX_CONTRACTS = 500  # Support all 497 USDT perpetual contracts

class RateLimitedFundingPostgres:
    """Rate limited funding backend that saves at most once per minute per symbol"""

    def __init__(self, **postgres_cfg):
        self.postgres_cfg = postgres_cfg
        self.last_save_times = {}  # {symbol: last_save_timestamp}
        self.save_interval = 60  # 60 seconds = 1 minute

    async def __call__(self, funding, receipt_timestamp):
        """Called by cryptofeed when funding data arrives"""
        try:
            current_time = time.time()
            symbol = funding.symbol

            # Check if we should save this update (1 minute interval)
            if symbol not in self.last_save_times or \
               current_time - self.last_save_times[symbol] >= self.save_interval:

                # Save to database
                await self._save_to_database(funding, receipt_timestamp)

                # Update last save time
                self.last_save_times[symbol] = current_time

                logger.info(f"💰 Funding saved: {symbol} Rate: {funding.rate:.6f}")

        except Exception as e:
            logger.error(f"Rate limited funding backend error: {e}")

    async def _save_to_database(self, funding, receipt_timestamp):
        """Save funding data to PostgreSQL database"""
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._sync_save, funding, receipt_timestamp)
        except Exception as e:
            logger.error(f"Database save error: {e}")

    def _sync_save(self, funding, receipt_timestamp):
        """Synchronous database save"""
        try:
            conn = psycopg2.connect(
                host=self.postgres_cfg['host'],
                user=self.postgres_cfg['user'],
                password=self.postgres_cfg['pw'],
                database=self.postgres_cfg['db']
            )

            cursor = conn.cursor()

            # Insert funding data (convert timestamps to datetime objects)
            cursor.execute("""
                INSERT INTO funding (timestamp, receipt_timestamp, exchange, symbol, mark_price, rate, next_funding_time, predicted_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.fromtimestamp(funding.timestamp) if funding.timestamp else None,
                datetime.fromtimestamp(receipt_timestamp) if receipt_timestamp else None,
                funding.exchange,
                funding.symbol,
                funding.mark_price,
                funding.rate,
                datetime.fromtimestamp(funding.next_funding_time) if funding.next_funding_time else None,
                funding.predicted_rate
            ))

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Sync database save error: {e}")

class BinanceAdvancedMonitor:
    """Advanced Binance Monitor - Full Scale"""

    def __init__(self):
        self.feed_handler = None
        self.symbols = []
        self.is_running = False
        self.start_time = None

        # Statistics
        self.stats = {
            'trades_count': 0,
            'candles_count': 0,
            'funding_count': 0,
            'ticker_count': 0,
            'last_trade_time': None,
            'last_candle_time': None,
            'last_funding_time': None,
            'errors': 0
        }

        # Funding rate is now handled by RateLimitedFundingPostgres backend

        # Auto cleanup task
        self.last_cleanup_time = 0
        self.cleanup_interval = 3600  # Clean up every hour
        self.cleanup_days = 30  # Keep 30 days of data

    def get_all_usdt_symbols(self) -> List[str]:
        """Get all USDT perpetual contracts"""
        try:
            all_symbols = BinanceFutures.symbols()
            # Filter USDT perpetual contracts
            usdt_symbols = [s for s in all_symbols if s.endswith('-USDT-PERP')]

            # Full scale mode: monitor ALL contracts
            selected_symbols = usdt_symbols[:MAX_CONTRACTS]

            logger.info(f"📊 Found {len(usdt_symbols)} USDT perpetual contracts")
            logger.info(f"🎯 Monitoring ALL {len(selected_symbols)} contracts")
            logger.info(f"🚀 Full scale collection mode enabled")

            return selected_symbols

        except Exception as e:
            logger.error(f"Failed to get contract list: {e}")
            # Return fallback symbols
            fallback_symbols = [
                'BTC-USDT-PERP', 'ETH-USDT-PERP', 'BNB-USDT-PERP',
                'XRP-USDT-PERP', 'SOL-USDT-PERP', 'DOGE-USDT-PERP',
                'ADA-USDT-PERP', 'MATIC-USDT-PERP', 'DOT-USDT-PERP',
                'AVAX-USDT-PERP', 'LINK-USDT-PERP', 'UNI-USDT-PERP'
            ]
            logger.info(f"🔄 Using fallback contract list: {len(fallback_symbols)} contracts")
            return fallback_symbols

    async def trade_callback(self, trade, receipt_time):
        """Trade data callback"""
        try:
            self.stats['trades_count'] += 1
            self.stats['last_trade_time'] = datetime.now()

            if self.stats['trades_count'] % 1000 == 0:
                logger.info(f"📈 Received {self.stats['trades_count']} trade records")

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Trade callback error: {e}")

    async def candle_callback(self, candle, receipt_time):
        """Candle data callback"""
        try:
            self.stats['candles_count'] += 1
            self.stats['last_candle_time'] = datetime.now()

            logger.info(f"📊 Candle[{candle.interval}]: {candle.symbol} | Close: {candle.close} | Volume: {candle.volume}")

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Candle callback error: {e}")

    async def funding_callback(self, funding, receipt_time):
        """Funding rate callback - statistics only"""
        try:
            # Update statistics (this callback is called for every funding update, but saves are rate-limited in backend)
            self.stats['funding_count'] += 1
            self.stats['last_funding_time'] = datetime.now()

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Funding callback error: {e}")

        # Check if need to run cleanup
        await self.auto_cleanup_check()

    async def auto_cleanup_check(self):
        """Auto cleanup check - runs every hour"""
        current_time = time.time()

        if current_time - self.last_cleanup_time >= self.cleanup_interval:
            self.last_cleanup_time = current_time
            await self.cleanup_old_funding_data()

    async def cleanup_old_funding_data(self):
        """Clean up old funding data automatically"""
        try:
            import asyncio
            import psycopg2

            # Database connection
            conn = psycopg2.connect(
                host=postgres_cfg['host'],
                user=postgres_cfg['user'],
                password=postgres_cfg['pw'],
                database=postgres_cfg['db']
            )

            cursor = conn.cursor()

            # Delete data older than cleanup_days
            cursor.execute(f"""
                DELETE FROM funding
                WHERE timestamp < NOW() - INTERVAL '{self.cleanup_days} days'
            """)

            deleted_count = cursor.rowcount
            conn.commit()

            if deleted_count > 0:
                logger.info(f"🗑️ Auto cleanup: Removed {deleted_count:,} old funding records (>{self.cleanup_days} days)")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Auto cleanup failed: {e}")

    def cleanup_old_funding_data_sync(self):
        """Clean up old funding data synchronously"""
        try:
            import psycopg2

            # Database connection
            conn = psycopg2.connect(
                host=postgres_cfg['host'],
                user=postgres_cfg['user'],
                password=postgres_cfg['pw'],
                database=postgres_cfg['db']
            )

            cursor = conn.cursor()

            # Delete data older than cleanup_days
            cursor.execute(f"""
                DELETE FROM funding
                WHERE timestamp < NOW() - INTERVAL '{self.cleanup_days} days'
            """)

            deleted_count = cursor.rowcount
            conn.commit()

            if deleted_count > 0:
                logger.info(f"🗑️ Initial cleanup: Removed {deleted_count:,} old funding records (>{self.cleanup_days} days)")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Initial cleanup failed: {e}")

    def setup_monitoring(self):
        """Setup monitoring configuration"""
        logger.info("🔧 Configuring advanced monitoring system...")

        # Get all USDT contracts
        self.symbols = self.get_all_usdt_symbols()
        logger.info(f"🎯 Will monitor {len(self.symbols)} contracts (full scale mode)")

        # Create FeedHandler
        config = {
            'log': {
                'filename': 'logs/cryptofeed_advanced.log',
                'level': 'WARNING',
                'disabled': False
            },
            'backend_multiprocessing': True,
            'uvloop': True
        }

        self.feed_handler = FeedHandler(config=config)

        logger.info("🎯 Using advanced connection mode")

        # Create feeds for each interval
        for interval in INTERVALS:
            table_name = f'candles_{interval}'
            logger.info(f"Adding {interval} candle monitoring: {len(self.symbols)} contracts")

            self.feed_handler.add_feed(
                BinanceFutures(
                    symbols=self.symbols,
                    channels=[CANDLES],
                    callbacks={
                        CANDLES: [
                            CandlesPostgres(table=table_name, **postgres_cfg),
                            self.candle_callback
                        ]
                    },
                    candle_interval=interval
                )
            )

        # Trade data monitoring
        logger.info(f"Adding trade data monitoring: {len(self.symbols)} contracts")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[TRADES],
                callbacks={
                    TRADES: [
                        TradePostgres(**postgres_cfg),
                        self.trade_callback
                    ]
                }
            )
        )

        # Funding rate monitoring - rate limited (1 minute per symbol)
        logger.info(f"Adding funding rate monitoring: {len(self.symbols)} contracts (1 minute intervals)")
        self.feed_handler.add_feed(
            BinanceFutures(
                symbols=self.symbols,
                channels=[FUNDING],
                callbacks={
                    FUNDING: [
                        RateLimitedFundingPostgres(**postgres_cfg),
                        self.funding_callback
                    ]
                }
            )
        )

    def print_stats(self):
        """Print statistics"""
        current_time = datetime.now()
        uptime = current_time - self.start_time if self.start_time else timedelta(0)

        logger.info("=" * 60)
        logger.info("📊 Advanced Binance Monitor Status")
        logger.info("=" * 60)
        logger.info(f"Uptime: {uptime}")
        logger.info(f"Monitored contracts: {len(self.symbols)}")
        logger.info(f"Monitored intervals: {len(INTERVALS)}")
        logger.info(f"Trade data: {self.stats['trades_count']} records")
        logger.info(f"Candle data: {self.stats['candles_count']} records")
        logger.info(f"Funding rate: {self.stats['funding_count']} records")
        logger.info(f"Error count: {self.stats['errors']}")

        if self.stats['last_trade_time']:
            logger.info(f"Last trade: {self.stats['last_trade_time'].strftime('%H:%M:%S')}")
        if self.stats['last_candle_time']:
            logger.info(f"Last candle: {self.stats['last_candle_time'].strftime('%H:%M:%S')}")
        if self.stats['last_funding_time']:
            logger.info(f"Last funding: {self.stats['last_funding_time'].strftime('%H:%M:%S')}")

        logger.info("=" * 60)

    def signal_handler(self, signum, frame):
        """Signal handler"""
        logger.info(f"\nReceived signal {signum}, stopping safely...")
        self.is_running = False
        if self.feed_handler:
            self.feed_handler.stop()

    def run(self):
        """Run monitoring system"""
        logger.info("🚀 Starting Advanced Binance Full Scale Monitor")
        logger.info("=" * 60)

        # Setup signal handling
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            # Configure monitoring
            self.setup_monitoring()

            # Mark start time
            self.start_time = datetime.now()
            self.is_running = True

            # Perform initial cleanup (synchronous version)
            logger.info("🗑️ Performing initial cleanup of old data...")
            self.cleanup_old_funding_data_sync()

            logger.info("✅ Monitor configuration complete")
            logger.info(f"📅 Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("📡 Starting data streams...")
            logger.info("⏹  Press Ctrl+C to stop safely")
            logger.info("=" * 60)

            # Run monitoring
            self.feed_handler.run()

        except KeyboardInterrupt:
            logger.info("User manual stop")
        except Exception as e:
            logger.error(f"Monitor system error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_running = False
            logger.info("🔄 Performing final cleanup...")
            self.print_stats()
            logger.info("✅ Monitor system stopped safely")

def main():
    """Main function"""
    # Ensure logs directory exists
    Path('logs').mkdir(exist_ok=True)

    monitor = BinanceAdvancedMonitor()

    try:
        # Run monitoring system
        monitor.run()
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()