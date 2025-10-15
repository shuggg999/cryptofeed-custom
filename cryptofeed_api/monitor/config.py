#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理模块
读取YAML配置文件并提供配置访问接口
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class Config:
    """配置管理器"""

    def __init__(self, config_file: Optional[str] = None):
        """初始化配置管理器

        Args:
            config_file: 配置文件路径,如果为None则使用默认路径
        """
        self.data = {}

        # 默认配置
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

        # 加载配置文件
        if config_file:
            self.load_config(config_file)
        else:
            # 尝试从默认位置加载
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "main.yaml"
            if config_path.exists():
                self.load_config(str(config_path))
            else:
                self.data = self.default_config

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

        # 首先检查环境变量
        env_key = key.replace(".", "_").upper()
        if env_key in os.environ:
            env_value = os.environ[env_key]
            # 尝试转换为适当的类型
            if env_value.lower() in ("true", "false"):
                return env_value.lower() == "true"
            elif env_value.isdigit():
                return int(env_value)
            else:
                return env_value

        # 回退到配置文件值
        keys = key.split(".")
        value = self.data

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default


# 全局配置实例
config = Config()
