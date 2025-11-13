"""
核心配置管理

支持多环境配置：
- 开发环境（dev）：使用 config/dev.yaml
- 生产环境（prod）：使用 config/prod.yaml
- 默认：开发环境

环境切换方式：
1. 环境变量：export ENV=prod
2. 启动时指定：ENV=prod python main.py
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic_settings import BaseSettings

# 获取 logger（在模块级别初始化）
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """应用设置（从配置文件动态加载）"""

    # 应用信息
    app_name: str = "Cryptofeed API Service"
    app_version: str = "1.0.0"
    debug: bool = False

    # 环境标识
    environment: str = "development"  # development 或 production

    # API配置
    api_host: str = "0.0.0.0"
    api_port: int = 8888
    api_prefix: str = "/api"

    # ClickHouse配置
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "password123")
    clickhouse_database: str = os.getenv("CLICKHOUSE_DATABASE", "cryptofeed")

    # 监控配置
    monitor_enabled: bool = True
    monitor_symbols: list = ["BTC-USDT-PERP", "ETH-USDT-PERP"]

    # 健康检查端口
    health_check_port: int = 8080

    # 临时缓存配置
    temp_cache_max_memory_mb: int = 100
    temp_cache_max_entries: int = 10000
    temp_cache_default_ttl_minutes: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = False


class ConfigManager:
    """配置管理器（支持多环境）"""

    def __init__(self, config_path: Optional[str] = None):
        # 从环境变量获取环境标识（默认为 dev）
        self.env = os.getenv("ENV", "dev").lower()

        # 根据环境选择配置文件
        if config_path:
            self.config_path = config_path
        else:
            if self.env == "prod" or self.env == "production":
                self.config_path = "config/prod.yaml"
            elif self.env == "dev" or self.env == "development":
                self.config_path = "config/dev.yaml"
            else:
                # 未知环境，默认使用 dev
                self.config_path = "config/dev.yaml"

        # 加载配置
        self._config_data = self._load_config()

        # 创建 Settings 实例并从配置文件更新
        self.settings = Settings()
        self._update_settings_from_config()

        # 使用日志记录环境信息（而不是 print）
        logger.info(f"🔧 Environment: {self.env}")
        logger.info(f"📄 Config file: {self.config_path}")
        logger.info(f"🌐 API Port: {self.settings.api_port}")
        logger.info(f"🗄️  Database: {self.settings.clickhouse_database}")
        logger.info(f"🏥 Health Check Port: {self.settings.health_check_port}")

    def _load_config(self) -> Dict[str, Any]:
        """加载YAML配置文件"""
        config_file = Path(self.config_path)
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f) or {}
                return config_data
        else:
            logger.warning(f"⚠️  Warning: Config file not found: {self.config_path}")
            logger.warning(f"⚠️  Using default settings")
            return {}

    def _update_settings_from_config(self):
        """从配置文件更新 Settings"""
        # 更新环境标识
        if "environment" in self._config_data:
            self.settings.environment = self._config_data["environment"]

        # 更新 API 配置
        if "api" in self._config_data:
            api_config = self._config_data["api"]
            if "host" in api_config:
                self.settings.api_host = api_config["host"]
            if "port" in api_config:
                self.settings.api_port = api_config["port"]
            if "debug" in api_config:
                self.settings.debug = api_config["debug"]

        # 更新 ClickHouse 配置
        if "clickhouse" in self._config_data:
            ch_config = self._config_data["clickhouse"]
            if "host" in ch_config:
                self.settings.clickhouse_host = ch_config["host"]
            if "port" in ch_config:
                self.settings.clickhouse_port = ch_config["port"]
            if "user" in ch_config:
                self.settings.clickhouse_user = ch_config["user"]
            if "password" in ch_config:
                self.settings.clickhouse_password = ch_config["password"]
            if "database" in ch_config:
                self.settings.clickhouse_database = ch_config["database"]

        # 更新监控配置
        if "monitoring" in self._config_data:
            monitoring_config = self._config_data["monitoring"]
            if "health_check_port" in monitoring_config:
                self.settings.health_check_port = monitoring_config["health_check_port"]

        # 更新交易对列表
        if "symbols" in self._config_data:
            symbols_config = self._config_data["symbols"]
            if "custom_list" in symbols_config:
                self.settings.monitor_symbols = symbols_config["custom_list"]

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值，支持点号分隔的嵌套key"""
        keys = key.split(".")
        value = self._config_data

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value


# 全局配置实例
config_manager = ConfigManager()
settings = config_manager.settings
