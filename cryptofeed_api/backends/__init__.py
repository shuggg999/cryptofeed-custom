"""
Cryptofeed API 后端存储模块

包含各种数据库后端实现
"""

from .clickhouse import *

__all__ = ["clickhouse"]
