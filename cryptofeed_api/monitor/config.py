#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration management module
Read YAML config files and provide configuration access interface
"""
import os
import yaml
from pathlib import Path
from typing import Any, Dict

class Config:
    """Configuration manager"""

    def __init__(self, config_file: str = None):
        self.data = {}

        # Default configuration
        self.default_config = {
            'database': {
                'host': '127.0.0.1',
                'port': 5432,
                'user': 'postgres',
                'password': 'password',
                'database': 'cryptofeed'
            },
            'connection_pool': {
                'streams_per_connection': 1000,
                'auto_scaling': {
                    'enabled': True,
                    'symbol_check_interval': 300
                }
            },
            'collection': {
                'data_types': [
                    'trades', 'ticker', 'funding', 'l2_book',
                    'candles', 'candles', 'candles', 'candles', 'candles',  # 统一使用candles表
                    'liquidations', 'open_interest', 'index'
                ]
            },
            'monitoring': {
                'metrics_enabled': True,
                'health_check_port': 8080,
                'stats_interval': 300
            },
            'logging': {
                'level': 'INFO',
                'filename': 'logs/cryptofeed_monitor.log'
            }
        }

        # Load config file
        if config_file:
            self.load_config(config_file)
        else:
            # Try to load from default location
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / 'config' / 'main.yaml'
            if config_path.exists():
                self.load_config(str(config_path))
            else:
                self.data = self.default_config

    def load_config(self, config_file: str):
        """Load configuration file"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                loaded_config = yaml.safe_load(f) or {}

            # Merge default and loaded config
            self.data = self._merge_config(self.default_config, loaded_config)
        except Exception as e:
            print(f"Warning: Failed to load config file {config_file}: {e}")
            print("Using default configuration")
            self.data = self.default_config

    def _merge_config(self, default: Dict, custom: Dict) -> Dict:
        """Recursively merge configurations"""
        result = default.copy()
        for key, value in custom.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(result[key], value)
            else:
                result[key] = value
        return result

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value, supports dot-separated key names"""
        keys = key.split('.')
        value = self.data

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

# Global config instance
config = Config()