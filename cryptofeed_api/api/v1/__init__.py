"""
API v1 路由模块
"""

from fastapi import APIRouter

from .candles import router as candles_router
from .funding import router as funding_router
from .health import router as health_router
from .klines import router as klines_router
from .monitoring import router as monitoring_router
from .trades import router as trades_router

# 创建v1 API路由器
api_router = APIRouter(prefix="/v1")

# 注册子路由
api_router.include_router(health_router)
api_router.include_router(candles_router)
api_router.include_router(trades_router)
api_router.include_router(funding_router)
api_router.include_router(monitoring_router)
api_router.include_router(klines_router)  # FreqTrade兼容的K线API
