"""
健康检查API端点
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ...models.schemas import HealthResponse, APIResponse
from ...core.database import check_database_health, get_db_session
from ...core.config import config_manager
from ..dependencies import get_request_logger

router = APIRouter(prefix="/health", tags=["Health"])
logger = logging.getLogger(__name__)

# 服务启动时间
SERVICE_START_TIME = time.time()


@router.get("/", response_model=APIResponse)
async def health_check(
    db: AsyncSession = Depends(get_db_session),
    request_logger: logging.Logger = Depends(get_request_logger)
) -> APIResponse:
    """
    基础健康检查

    返回服务的基本健康状态信息
    """
    try:
        # 计算运行时间
        uptime_seconds = int(time.time() - SERVICE_START_TIME)

        # 检查数据库连接
        db_health = await check_database_health()
        db_connected = db_health.get("connected", False)

        # 检查数据采集状态（暂时返回True，后续集成monitor模块时更新）
        data_collection_status = True

        health_data = HealthResponse(
            status="healthy" if db_connected else "unhealthy",
            timestamp=datetime.now(),
            version=config_manager.settings.app_version,
            database=db_connected,
            data_collection=data_collection_status,
            uptime_seconds=uptime_seconds
        )

        return APIResponse(
            success=True,
            message="Health check completed",
            data=health_data
        )

    except Exception as e:
        request_logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")


@router.get("/detailed", response_model=Dict[str, Any])
async def detailed_health_check(
    db: AsyncSession = Depends(get_db_session),
    request_logger: logging.Logger = Depends(get_request_logger)
) -> Dict[str, Any]:
    """
    详细健康检查

    返回详细的系统状态信息，包括数据库统计、内存使用等
    """
    try:
        # 基础健康信息
        uptime_seconds = int(time.time() - SERVICE_START_TIME)

        # 数据库详细信息
        db_health = await check_database_health()

        # 系统信息
        import psutil
        memory_info = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)

        # 数据采集状态（预留接口）
        collection_status = {
            "status": "active",
            "last_update": datetime.now().isoformat(),
            "symbols_monitored": config_manager.settings.monitor_symbols,
            "errors": []  # 后续添加错误跟踪
        }

        detailed_info = {
            "service": {
                "name": config_manager.settings.app_name,
                "version": config_manager.settings.app_version,
                "status": "healthy" if db_health.get("connected") else "degraded",
                "uptime_seconds": uptime_seconds,
                "timestamp": datetime.now().isoformat()
            },
            "database": {
                "connected": db_health.get("connected", False),
                "tables": db_health.get("tables", []),
                "stats": db_health.get("stats", {}),
                "error": db_health.get("error")
            },
            "system": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory_info.percent,
                "memory_available_mb": round(memory_info.available / 1024 / 1024, 2),
                "disk_usage": _get_disk_usage()
            },
            "data_collection": collection_status,
            "configuration": {
                "debug_mode": config_manager.settings.debug,
                "monitor_enabled": config_manager.settings.monitor_enabled,
                "api_host": config_manager.settings.api_host,
                "api_port": config_manager.settings.api_port
            }
        }

        return detailed_info

    except Exception as e:
        request_logger.error(f"Detailed health check failed: {e}")
        raise HTTPException(status_code=500, detail="Detailed health check failed")


@router.get("/readiness")
async def readiness_check(
    db: AsyncSession = Depends(get_db_session)
) -> Dict[str, Any]:
    """
    就绪检查

    检查服务是否准备好接收请求
    """
    try:
        # 检查数据库连接
        db_health = await check_database_health()

        if not db_health.get("connected"):
            raise HTTPException(status_code=503, detail="Database not ready")

        # 检查必要的表是否存在
        required_tables = ["trades", "candles", "funding", "tickers"]
        existing_tables = db_health.get("tables", [])
        missing_tables = [table for table in required_tables if table not in existing_tables]

        if missing_tables:
            return {
                "ready": False,
                "message": f"Missing required tables: {missing_tables}",
                "timestamp": datetime.now().isoformat()
            }

        return {
            "ready": True,
            "message": "Service is ready",
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=500, detail="Readiness check failed")


@router.get("/liveness")
async def liveness_check() -> Dict[str, str]:
    """
    存活检查

    简单的存活检查，用于Kubernetes等容器编排系统
    """
    return {
        "status": "alive",
        "timestamp": datetime.now().isoformat()
    }


def _get_disk_usage() -> Dict[str, float]:
    """获取磁盘使用情况"""
    try:
        import psutil
        disk = psutil.disk_usage('/')
        return {
            "total_gb": round(disk.total / 1024**3, 2),
            "used_gb": round(disk.used / 1024**3, 2),
            "free_gb": round(disk.free / 1024**3, 2),
            "percent": round((disk.used / disk.total) * 100, 2)
        }
    except Exception:
        return {"error": "Unable to get disk usage"}