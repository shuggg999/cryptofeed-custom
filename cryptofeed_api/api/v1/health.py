"""
健康检查API端点 - 仅使用ClickHouse
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict

import clickhouse_connect
from fastapi import APIRouter, Depends, HTTPException

from ...core.clickhouse import get_clickhouse
from ...core.config import config_manager
from ...models.schemas import APIResponse, HealthResponse
from ..dependencies import get_request_logger

router = APIRouter(prefix="/health", tags=["Health"])
logger = logging.getLogger(__name__)

# 服务启动时间
SERVICE_START_TIME = time.time()


async def check_clickhouse_health():
    """检查ClickHouse数据库健康状态"""
    try:
        client = clickhouse_connect.get_client(
            host=config_manager.settings.clickhouse_host,
            port=config_manager.settings.clickhouse_port,
            username=config_manager.settings.clickhouse_user,
            password=config_manager.settings.clickhouse_password,
            database=config_manager.settings.clickhouse_database,
        )
        result = client.query("SELECT 1")
        client.close()
        return True
    except Exception as e:
        logger.error(f"ClickHouse health check failed: {e}")
        return False


@router.get("/", response_model=APIResponse)
async def health_check(request_logger: logging.Logger = Depends(get_request_logger)) -> APIResponse:
    """
    系统健康检查端点

    返回系统各个组件的健康状态
    """
    request_logger.info("Health check requested")

    # 计算运行时间
    uptime_seconds = int(time.time() - SERVICE_START_TIME)
    hours = uptime_seconds // 3600
    minutes = (uptime_seconds % 3600) // 60
    seconds = uptime_seconds % 60
    uptime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    # 检查ClickHouse连接
    clickhouse_healthy = await check_clickhouse_health()

    health_data = HealthResponse(
        status="healthy" if clickhouse_healthy else "degraded",
        timestamp=datetime.utcnow(),
        uptime=uptime,
        database=clickhouse_healthy,
        version=config_manager.settings.app_version,
        checks={"clickhouse": "healthy" if clickhouse_healthy else "unhealthy", "api": "healthy"},
    )

    return APIResponse(
        success=True,
        data=health_data.dict(),
        message="System is operational" if clickhouse_healthy else "Database connection issues",
    )


@router.get("/liveness")
async def liveness_check() -> Dict[str, str]:
    """
    Kubernetes存活性检查

    简单返回200状态，表示服务存活
    """
    return {"status": "alive"}


@router.get("/readiness")
async def readiness_check() -> Dict[str, Any]:
    """
    Kubernetes就绪性检查

    检查服务是否准备好接收流量
    """
    # 检查ClickHouse连接
    clickhouse_ready = await check_clickhouse_health()

    if not clickhouse_ready:
        raise HTTPException(status_code=503, detail="Service not ready")

    return {"status": "ready", "clickhouse": clickhouse_ready}


@router.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    """
    获取服务指标

    返回Prometheus格式的指标数据
    """
    uptime_seconds = time.time() - SERVICE_START_TIME

    metrics = {
        "service_uptime_seconds": uptime_seconds,
        "service_start_time": SERVICE_START_TIME,
        "current_time": time.time(),
        "version": config_manager.settings.app_version,
    }

    return metrics
