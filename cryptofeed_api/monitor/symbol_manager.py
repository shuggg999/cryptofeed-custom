#!/usr/bin/env python3
"""
动态符号管理器
处理符号发现、过滤和实时更新
"""
import asyncio
import fnmatch
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from cryptofeed.exchanges import BinanceFutures

from .config import config

logger = logging.getLogger(__name__)


class DynamicSymbolManager:
    """动态符号管理器"""

    def __init__(self):
        """初始化动态符号管理器"""
        self.current_symbols: Set[str] = set()
        self.last_update_time: float = 0
        self.update_interval = config.get("symbols.update_interval", 300)  # 5分钟
        self.max_contracts = config.get("symbols.max_contracts", 500)
        self.mode = config.get("symbols.mode", "all")

        # 符号变更回调函数
        self.on_symbols_added: Optional[Callable] = None
        self.on_symbols_removed: Optional[Callable] = None

    async def get_symbols(self) -> List[str]:
        """获取符号列表(根据配置模式)

        Returns:
            符号列表
        """
        try:
            mode = self.mode.lower()

            if mode == "all":
                symbols = await self._get_all_usdt_symbols()
            elif mode == "custom":
                symbols = self._get_custom_symbols()
            elif mode == "filter":
                symbols = await self._get_filtered_symbols()
            else:
                logger.warning(f"未知模式 '{mode}', 回退到 'all' 模式")
                symbols = await self._get_all_usdt_symbols()

            # 应用最大合约数限制
            if len(symbols) > self.max_contracts:
                symbols = symbols[: self.max_contracts]
                logger.info(f"🔒 限制为 {self.max_contracts} 个合约 (总共 {len(symbols)} 个)")

            return symbols

        except Exception as e:
            logger.error(f"获取符号时出错: {e}")
            return self._get_fallback_symbols()

    async def _get_all_usdt_symbols(self) -> List[str]:
        """获取所有USDT永续合约

        Returns:
            USDT永续合约符号列表
        """
        try:
            all_symbols = BinanceFutures.symbols()
            usdt_symbols = [s for s in all_symbols if s.endswith("-USDT-PERP")]

            logger.info(f"📊 找到 {len(usdt_symbols)} 个USDT永续合约")
            return usdt_symbols

        except Exception as e:
            logger.error(f"从Binance获取符号失败: {e}")
            raise

    def _get_custom_symbols(self) -> List[str]:
        """获取自定义符号列表

        Returns:
            自定义符号列表
        """
        custom_list = config.get("symbols.custom_list", [])

        if not custom_list:
            logger.warning("选择了自定义模式但未提供custom_list,使用备用列表")
            return self._get_fallback_symbols()

        logger.info(f"📋 使用自定义符号列表: {len(custom_list)} 个合约")
        return custom_list

    async def _get_filtered_symbols(self) -> List[str]:
        """根据筛选条件获取符号

        Returns:
            筛选后的符号列表
        """
        # 首先获取所有USDT符号
        all_usdt_symbols = await self._get_all_usdt_symbols()

        filters = config.get("symbols.filters", {})

        # 应用模式筛选
        symbols = self._apply_pattern_filters(all_usdt_symbols, filters)

        # 应用基于交易量的筛选(如果需要)
        # 注意: 这需要实时市场数据,可能比较复杂
        # 目前我们使用模式筛选和top_n选择

        top_n = filters.get("top_n_by_volume")
        if top_n and len(symbols) > top_n:
            # 目前只是简单地取前N个符号
            # 在生产系统中,你会按实际交易量排序
            symbols = symbols[:top_n]
            logger.info(f"📊 根据配置筛选出前 {top_n} 个符号")

        logger.info(f"🔍 筛选后的符号: {len(symbols)} 个合约")
        return symbols

    def _apply_pattern_filters(self, symbols: List[str], filters: Dict) -> List[str]:
        """应用模式筛选

        Args:
            symbols: 待筛选的符号列表
            filters: 筛选配置

        Returns:
            筛选后的符号列表
        """
        result = symbols.copy()

        # 应用包含模式(如果指定)
        include_patterns = filters.get("include_patterns", [])
        if include_patterns:
            included = []
            for symbol in result:
                if any(fnmatch.fnmatch(symbol, pattern) for pattern in include_patterns):
                    included.append(symbol)
            result = included
            logger.info(f"📥 包含模式匹配: {len(result)} 个符号")

        # 应用排除模式
        exclude_patterns = filters.get("exclude_patterns", [])
        if exclude_patterns:
            excluded = []
            for symbol in result:
                if not any(fnmatch.fnmatch(symbol, pattern) for pattern in exclude_patterns):
                    excluded.append(symbol)
            result = excluded
            logger.info(f"📤 排除模式应用: {len(result)} 个符号")

        return result

    def _get_fallback_symbols(self) -> List[str]:
        """获取备用符号列表

        Returns:
            备用符号列表
        """
        fallback = [
            "BTC-USDT-PERP",
            "ETH-USDT-PERP",
            "SOL-USDT-PERP",
            "DOGE-USDT-PERP",
            "ADA-USDT-PERP",
            "AVAX-USDT-PERP",
            "LINK-USDT-PERP",
            "DOT-USDT-PERP",
            "UNI-USDT-PERP",
            "LTC-USDT-PERP",
        ]
        logger.warning(f"🔄 使用备用符号列表: {len(fallback)} 个合约")
        return fallback

    async def check_for_updates(self) -> Dict[str, List[str]]:
        """检查符号更新(新增/删除)

        Returns:
            包含'added'和'removed'键的字典
        """
        current_time = time.time()

        if current_time - self.last_update_time < self.update_interval:
            return {"added": [], "removed": []}

        try:
            new_symbols = set(await self.get_symbols())

            added = list(new_symbols - self.current_symbols)
            removed = list(self.current_symbols - new_symbols)

            if added or removed:
                logger.info(f"🔄 检测到符号变化:")
                if added:
                    logger.info(f"  ➕ 新增: {added}")
                if removed:
                    logger.info(f"  ➖ 移除: {removed}")

                # 更新当前符号
                self.current_symbols = new_symbols

                # 调用回调函数(如果已设置)
                if added and self.on_symbols_added:
                    await self.on_symbols_added(added)
                if removed and self.on_symbols_removed:
                    await self.on_symbols_removed(removed)

            self.last_update_time = current_time
            return {"added": added, "removed": removed}

        except Exception as e:
            logger.error(f"检查符号更新时出错: {e}")
            return {"added": [], "removed": []}

    async def initialize(self) -> List[str]:
        """初始化符号管理器

        Returns:
            初始符号列表
        """
        logger.info("🔄 正在初始化动态符号管理器...")
        logger.info(f"📋 模式: {self.mode}")
        logger.info(f"⏰ 更新间隔: {self.update_interval}秒")
        logger.info(f"🔒 最大合约数: {self.max_contracts}")

        symbols = await self.get_symbols()
        self.current_symbols = set(symbols)
        self.last_update_time = time.time()

        logger.info(f"✅ 已初始化 {len(symbols)} 个符号")
        return symbols

    async def start_monitoring(self) -> None:
        """开始监控符号变化"""
        logger.info("🔍 开始符号变化监控...")

        while True:
            try:
                await self.check_for_updates()
                await asyncio.sleep(60)  # 每分钟检查一次以提高效率
            except asyncio.CancelledError:
                logger.info("符号监控已停止")
                break
            except Exception as e:
                logger.error(f"符号监控出错: {e}")
                await asyncio.sleep(60)

    def get_current_symbols(self) -> List[str]:
        """获取当前监控的符号列表

        Returns:
            当前符号列表
        """
        return list(self.current_symbols)

    def set_callbacks(self, on_added: Optional[Callable] = None, on_removed: Optional[Callable] = None) -> None:
        """设置符号变化回调函数

        Args:
            on_added: 符号新增时的回调函数
            on_removed: 符号移除时的回调函数
        """
        self.on_symbols_added = on_added
        self.on_symbols_removed = on_removed


# 全局实例
symbol_manager = DynamicSymbolManager()
