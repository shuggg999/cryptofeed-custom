#!/usr/bin/env python3
"""
Dynamic Symbol Manager
Handle symbol discovery, filtering, and real-time updates
"""
import asyncio
import fnmatch
import logging
import time
from typing import List, Dict, Set, Optional
from datetime import datetime
from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from cryptofeed.exchanges import BinanceFutures
from .config import config

logger = logging.getLogger(__name__)

class DynamicSymbolManager:
    """动态符号管理器"""

    def __init__(self):
        self.current_symbols: Set[str] = set()
        self.last_update_time: float = 0
        self.update_interval = config.get('symbols.update_interval', 300)  # 5分钟
        self.max_contracts = config.get('symbols.max_contracts', 500)
        self.mode = config.get('symbols.mode', 'all')

        # Callbacks for symbol changes
        self.on_symbols_added: Optional[callable] = None
        self.on_symbols_removed: Optional[callable] = None

    async def get_symbols(self) -> List[str]:
        """获取符号列表（根据配置模式）"""
        try:
            mode = self.mode.lower()

            if mode == 'all':
                symbols = await self._get_all_usdt_symbols()
            elif mode == 'custom':
                symbols = self._get_custom_symbols()
            elif mode == 'filter':
                symbols = await self._get_filtered_symbols()
            else:
                logger.warning(f"Unknown mode '{mode}', falling back to 'all'")
                symbols = await self._get_all_usdt_symbols()

            # Apply max contracts limit
            if len(symbols) > self.max_contracts:
                symbols = symbols[:self.max_contracts]
                logger.info(f"🔒 Limited to {self.max_contracts} contracts (from {len(symbols)} total)")

            return symbols

        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            return self._get_fallback_symbols()

    async def _get_all_usdt_symbols(self) -> List[str]:
        """获取所有USDT永续合约"""
        try:
            all_symbols = BinanceFutures.symbols()
            usdt_symbols = [s for s in all_symbols if s.endswith('-USDT-PERP')]

            logger.info(f"📊 Found {len(usdt_symbols)} USDT perpetual contracts")
            return usdt_symbols

        except Exception as e:
            logger.error(f"Failed to fetch symbols from Binance: {e}")
            raise

    def _get_custom_symbols(self) -> List[str]:
        """获取自定义符号列表"""
        custom_list = config.get('symbols.custom_list', [])

        if not custom_list:
            logger.warning("Custom mode selected but no custom_list provided, using fallback")
            return self._get_fallback_symbols()

        logger.info(f"📋 Using custom symbol list: {len(custom_list)} contracts")
        return custom_list

    async def _get_filtered_symbols(self) -> List[str]:
        """根据筛选条件获取符号"""
        # First get all USDT symbols
        all_usdt_symbols = await self._get_all_usdt_symbols()

        filters = config.get('symbols.filters', {})

        # Apply pattern filters
        symbols = self._apply_pattern_filters(all_usdt_symbols, filters)

        # Apply volume-based filters (if needed)
        # Note: This would require real-time market data which might be complex
        # For now, we'll use the pattern filtering and top_n selection

        top_n = filters.get('top_n_by_volume')
        if top_n and len(symbols) > top_n:
            # For now, just take the first N symbols
            # In a production system, you'd sort by actual volume
            symbols = symbols[:top_n]
            logger.info(f"📊 Filtered to top {top_n} symbols by configuration")

        logger.info(f"🔍 Filtered symbols: {len(symbols)} contracts")
        return symbols

    def _apply_pattern_filters(self, symbols: List[str], filters: Dict) -> List[str]:
        """应用模式筛选"""
        result = symbols.copy()

        # Apply include patterns (if specified)
        include_patterns = filters.get('include_patterns', [])
        if include_patterns:
            included = []
            for symbol in result:
                if any(fnmatch.fnmatch(symbol, pattern) for pattern in include_patterns):
                    included.append(symbol)
            result = included
            logger.info(f"📥 Include patterns matched: {len(result)} symbols")

        # Apply exclude patterns
        exclude_patterns = filters.get('exclude_patterns', [])
        if exclude_patterns:
            excluded = []
            for symbol in result:
                if not any(fnmatch.fnmatch(symbol, pattern) for pattern in exclude_patterns):
                    excluded.append(symbol)
            result = excluded
            logger.info(f"📤 Exclude patterns applied: {len(result)} symbols")

        return result

    def _get_fallback_symbols(self) -> List[str]:
        """获取备用符号列表"""
        fallback = [
            'BTC-USDT-PERP', 'ETH-USDT-PERP', 'SOL-USDT-PERP', 'DOGE-USDT-PERP',
            'ADA-USDT-PERP', 'AVAX-USDT-PERP', 'LINK-USDT-PERP', 'DOT-USDT-PERP',
            'UNI-USDT-PERP', 'LTC-USDT-PERP'
        ]
        logger.warning(f"🔄 Using fallback symbol list: {len(fallback)} contracts")
        return fallback

    async def check_for_updates(self) -> Dict[str, List[str]]:
        """检查符号更新（新增/删除）"""
        current_time = time.time()

        if current_time - self.last_update_time < self.update_interval:
            return {"added": [], "removed": []}

        try:
            new_symbols = set(await self.get_symbols())

            added = list(new_symbols - self.current_symbols)
            removed = list(self.current_symbols - new_symbols)

            if added or removed:
                logger.info(f"🔄 Symbol changes detected:")
                if added:
                    logger.info(f"  ➕ Added: {added}")
                if removed:
                    logger.info(f"  ➖ Removed: {removed}")

                # Update current symbols
                self.current_symbols = new_symbols

                # Call callbacks if set
                if added and self.on_symbols_added:
                    await self.on_symbols_added(added)
                if removed and self.on_symbols_removed:
                    await self.on_symbols_removed(removed)

            self.last_update_time = current_time
            return {"added": added, "removed": removed}

        except Exception as e:
            logger.error(f"Error checking for symbol updates: {e}")
            return {"added": [], "removed": []}

    async def initialize(self) -> List[str]:
        """初始化符号管理器"""
        logger.info("🔄 Initializing Dynamic Symbol Manager...")
        logger.info(f"📋 Mode: {self.mode}")
        logger.info(f"⏰ Update interval: {self.update_interval}s")
        logger.info(f"🔒 Max contracts: {self.max_contracts}")

        symbols = await self.get_symbols()
        self.current_symbols = set(symbols)
        self.last_update_time = time.time()

        logger.info(f"✅ Initialized with {len(symbols)} symbols")
        return symbols

    async def start_monitoring(self):
        """开始监控符号变化"""
        logger.info("🔍 Starting symbol change monitoring...")

        while True:
            try:
                await self.check_for_updates()
                await asyncio.sleep(60)  # Check every minute for efficiency
            except asyncio.CancelledError:
                logger.info("Symbol monitoring stopped")
                break
            except Exception as e:
                logger.error(f"Error in symbol monitoring: {e}")
                await asyncio.sleep(60)

    def get_current_symbols(self) -> List[str]:
        """获取当前监控的符号列表"""
        return list(self.current_symbols)

    def set_callbacks(self, on_added: callable = None, on_removed: callable = None):
        """设置符号变化回调"""
        self.on_symbols_added = on_added
        self.on_symbols_removed = on_removed


# Global instance
symbol_manager = DynamicSymbolManager()