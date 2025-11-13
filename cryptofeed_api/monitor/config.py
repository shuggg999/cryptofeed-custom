#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理模块 - 重定向到统一配置管理器

为了向后兼容，这个模块现在直接使用 core.config 的配置管理器。
所有配置都通过环境变量 ENV 来选择 dev.yaml 或 prod.yaml。
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

# 导入统一的配置管理器
from ..core.config import config_manager as core_config_manager


class Config:
    """配置管理器（兼容层，重定向到 core.config_manager）"""

    def __init__(self, config_file: Optional[str] = None):
        """初始化配置管理器

        Args:
            config_file: 配置文件路径（已废弃，现在通过 ENV 环境变量控制）
        """
        # 使用统一的配置管理器
        self._core_config = core_config_manager
        self.data = self._core_config._config_data

        # 默认配置（作为后备）
        self.default_config = {
            "database": {
                "host": "127.0.0.1",
                "port": 5432,
                "user": "postgres",
                "password": "password",
                "database": "cryptofeed",
            },
            "connection_pool": {
                "streams_per_connection": 1000,
                "auto_scaling": {"enabled": True, "symbol_check_interval": 300},
            },
            "collection": {
                "data_types": [
                    "trades",
                    "ticker",
                    "funding",
                    "l2_book",
                    "candles",
                    "candles",
                    "candles",
                    "candles",
                    "candles",  # 统一使用candles表
                    "liquidations",
                    "open_interest",
                    "index",
                ]
            },
            "monitoring": {"metrics_enabled": True, "health_check_port": 8080, "stats_interval": 300},
            "logging": {"level": "INFO", "filename": "logs/cryptofeed_monitor.log"},
        }

        # 如果配置文件为空，则已经通过 core_config_manager 加载了

    def load_config(self, config_file: str) -> None:
        """加载配置文件

        Args:
            config_file: 配置文件路径
        """
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                loaded_config = yaml.safe_load(f) or {}

            # 合并默认配置和加载的配置
            self.data = self._merge_config(self.default_config, loaded_config)
        except Exception as e:
            print(f"警告: 加载配置文件失败 {config_file}: {e}")
            print("使用默认配置")
            self.data = self.default_config

    def _merge_config(self, default: Dict, custom: Dict) -> Dict:
        """递归合并配置字典

        Args:
            default: 默认配置
            custom: 自定义配置

        Returns:
            合并后的配置字典
        """
        result = default.copy()
        for key, value in custom.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(result[key], value)
            else:
                result[key] = value
        return result

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值,支持点号分隔的键名和环境变量

        Args:
            key: 配置键名,支持点号分隔(如 'database.host')
            default: 默认值

        Returns:
            配置值,如果不存在则返回默认值
        """
        # 直接使用统一的配置管理器
        return self._core_config.get(key, default)


# 全局配置实例
config = Config()
