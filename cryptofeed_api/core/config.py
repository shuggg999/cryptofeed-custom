"""
核心配置管理
"""
import os
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """应用设置"""
    # 应用信息
    app_name: str = "Cryptofeed API Service"
    app_version: str = "1.0.0"
    debug: bool = False

    # API配置
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_prefix: str = "/api/v1"

    # 数据库配置 (PostgreSQL for API metadata)
    database_host: str = "127.0.0.1"
    database_port: int = 5432
    database_user: str = "postgres"
    database_password: str = "password"
    database_name: str = "cryptofeed"

    # ClickHouse配置 (主要数据存储)
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = "password123"
    clickhouse_database: str = "cryptofeed"

    # 监控配置
    monitor_enabled: bool = True
    monitor_symbols: list = ["BTC-USDT-PERP", "ETH-USDT-PERP"]

    # 临时缓存配置
    temp_cache_max_memory_mb: int = 100
    temp_cache_max_entries: int = 10000
    temp_cache_default_ttl_minutes: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = False


class ConfigManager:
    """配置管理器"""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config/main.yaml"
        self.settings = Settings()
        self._config_data = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """加载YAML配置文件"""
        config_file = Path(self.config_path)
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值，支持点号分隔的嵌套key"""
        keys = key.split('.')
        value = self._config_data

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    @property
    def database_url(self) -> str:
        """生成数据库连接URL"""
        db_config = self.get('database', {})
        host = db_config.get('host', self.settings.database_host)
        port = db_config.get('port', self.settings.database_port)
        user = db_config.get('user', self.settings.database_user)
        password = db_config.get('password', self.settings.database_password)
        database = db_config.get('database', self.settings.database_name)

        return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"

    @property
    def postgres_config(self) -> Dict[str, Any]:
        """PostgreSQL后端配置"""
        db_config = self.get('database', {})
        return {
            'host': db_config.get('host', self.settings.database_host),
            'port': db_config.get('port', self.settings.database_port),
            'user': db_config.get('user', self.settings.database_user),
            'pw': db_config.get('password', self.settings.database_password),
            'db': db_config.get('database', self.settings.database_name),
        }


# 全局配置实例
config_manager = ConfigManager()
settings = config_manager.settings