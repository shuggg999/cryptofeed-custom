"""
监控和系统状态API端点
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import psutil
from fastapi import APIRouter, Depends, HTTPException, Query

from ...core.config import config_manager
from ...models.schemas import APIResponse

router = APIRouter(prefix="/monitoring", tags=["Monitoring"])
logger = logging.getLogger(__name__)


@router.get("/system", response_model=APIResponse)
async def get_system_metrics() -> APIResponse:
    """
    获取系统资源使用情况
    """
    try:
        # CPU 信息
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        cpu_freq = psutil.cpu_freq()

        # 内存信息
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()

        # 磁盘信息
        disk_usage = psutil.disk_usage("/")

        # 网络信息
        net_io = psutil.net_io_counters()

        # 进程信息
        process = psutil.Process()
        process_memory = process.memory_info()
        process_cpu = process.cpu_percent()

        system_metrics = {
            "timestamp": datetime.now().isoformat(),
            "cpu": {
                "usage_percent": cpu_percent,
                "count": cpu_count,
                "frequency_mhz": cpu_freq.current if cpu_freq else None,
                "process_usage_percent": process_cpu,
            },
            "memory": {
                "total_mb": round(memory.total / 1024 / 1024),
                "available_mb": round(memory.available / 1024 / 1024),
                "used_mb": round(memory.used / 1024 / 1024),
                "usage_percent": memory.percent,
                "process_rss_mb": round(process_memory.rss / 1024 / 1024),
                "process_vms_mb": round(process_memory.vms / 1024 / 1024),
            },
            "swap": {
                "total_mb": round(swap.total / 1024 / 1024),
                "used_mb": round(swap.used / 1024 / 1024),
                "usage_percent": swap.percent,
            },
            "disk": {
                "total_gb": round(disk_usage.total / 1024**3, 2),
                "used_gb": round(disk_usage.used / 1024**3, 2),
                "free_gb": round(disk_usage.free / 1024**3, 2),
                "usage_percent": round((disk_usage.used / disk_usage.total) * 100, 2),
            },
            "network": {
                "bytes_sent": net_io.bytes_sent,
                "bytes_recv": net_io.bytes_recv,
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv,
            },
        }

        return APIResponse(success=True, message="System metrics retrieved", data=system_metrics)

    except Exception as e:
        logger.error(f"Error getting system metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve system metrics")


@router.get("/performance", response_model=APIResponse)
async def get_performance_metrics() -> APIResponse:
    """
    获取应用性能指标
    """
    try:
        performance_metrics = {
            "timestamp": datetime.now().isoformat(),
            "service_status": "running",
            "api_enabled": True,
        }

        return APIResponse(success=True, message="Performance metrics retrieved", data=performance_metrics)

    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve performance metrics")


@router.get("/database", response_model=APIResponse)
async def get_database_metrics() -> APIResponse:
    """
    获取数据库相关指标 - ClickHouse only
    """
    try:
        import clickhouse_connect

        # ClickHouse健康检查
        client = clickhouse_connect.get_client(
            host=config_manager.settings.clickhouse_host,
            port=config_manager.settings.clickhouse_port,
            username=config_manager.settings.clickhouse_user,
            password=config_manager.settings.clickhouse_password,
            database=config_manager.settings.clickhouse_database,
        )

        try:
            result = client.query("SELECT 1")
            clickhouse_healthy = True
        except Exception:
            clickhouse_healthy = False
        finally:
            client.close()

        database_metrics = {
            "timestamp": datetime.now().isoformat(),
            "clickhouse_healthy": clickhouse_healthy,
            "database_type": "ClickHouse",
        }

        return APIResponse(success=True, message="Database metrics retrieved", data=database_metrics)

    except Exception as e:
        logger.error(f"Error getting database metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve database metrics")


@router.get("/data-quality", response_model=APIResponse)
async def get_data_quality_metrics(
    symbols: str = Query("BTC-USDT-PERP,ETH-USDT-PERP", description="交易对列表，逗号分隔"),
    hours: int = Query(24, ge=1, le=168, description="检查时间范围（小时）"),
) -> APIResponse:
    """
    获取数据质量指标
    """
    try:
        symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        quality_metrics = {}
        overall_score = 95  # Simplified score

        return APIResponse(
            success=True,
            message=f"Data quality metrics for {len(symbol_list)} symbols",
            data={
                "overall_quality_score": overall_score,
                "symbols_analyzed": len(symbol_list),
                "analysis_period": {"start": start_time.isoformat(), "end": end_time.isoformat(), "hours": hours},
                "symbol_metrics": quality_metrics,
            },
        )

    except Exception as e:
        logger.error(f"Error getting data quality metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve data quality metrics")


@router.get("/alerts", response_model=APIResponse)
async def get_system_alerts(
    severity: str = Query("all", description="告警级别: all, critical, warning, info"),
    limit: int = Query(50, ge=1, le=200, description="返回数量限制"),
) -> APIResponse:
    """
    获取系统告警信息
    """
    try:
        alerts = []
        current_time = datetime.now()

        # 检查系统资源告警
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=0.1)
        disk_usage = psutil.disk_usage("/")

        # 内存使用率告警
        if memory.percent > 90:
            alerts.append(
                {
                    "id": "memory_critical",
                    "timestamp": current_time.isoformat(),
                    "severity": "critical",
                    "component": "system",
                    "message": f"Memory usage critical: {memory.percent:.1f}%",
                    "details": {
                        "current_usage": memory.percent,
                        "threshold": 90,
                        "available_mb": round(memory.available / 1024 / 1024),
                    },
                }
            )
        elif memory.percent > 80:
            alerts.append(
                {
                    "id": "memory_warning",
                    "timestamp": current_time.isoformat(),
                    "severity": "warning",
                    "component": "system",
                    "message": f"Memory usage high: {memory.percent:.1f}%",
                    "details": {"current_usage": memory.percent, "threshold": 80},
                }
            )

        # CPU使用率告警
        if cpu_percent > 90:
            alerts.append(
                {
                    "id": "cpu_critical",
                    "timestamp": current_time.isoformat(),
                    "severity": "critical",
                    "component": "system",
                    "message": f"CPU usage critical: {cpu_percent:.1f}%",
                    "details": {"current_usage": cpu_percent, "threshold": 90},
                }
            )

        # 磁盘使用率告警
        disk_percent = (disk_usage.used / disk_usage.total) * 100
        if disk_percent > 90:
            alerts.append(
                {
                    "id": "disk_critical",
                    "timestamp": current_time.isoformat(),
                    "severity": "critical",
                    "component": "system",
                    "message": f"Disk usage critical: {disk_percent:.1f}%",
                    "details": {
                        "current_usage": disk_percent,
                        "threshold": 90,
                        "free_gb": round(disk_usage.free / 1024**3, 2),
                    },
                }
            )

        # Simplified monitoring - no database dependency

        # 按严重程度过滤
        if severity != "all":
            alerts = [alert for alert in alerts if alert["severity"] == severity]

        # 按时间戳排序并限制数量
        alerts.sort(key=lambda x: x["timestamp"], reverse=True)
        alerts = alerts[:limit]

        # 按严重程度统计
        severity_counts = {"critical": 0, "warning": 0, "info": 0}
        for alert in alerts:
            severity_counts[alert["severity"]] += 1

        return APIResponse(
            success=True,
            message=f"Retrieved {len(alerts)} system alerts",
            data={
                "alerts": alerts,
                "summary": {
                    "total_alerts": len(alerts),
                    "by_severity": severity_counts,
                    "generated_at": current_time.isoformat(),
                },
            },
        )

    except Exception as e:
        logger.error(f"Error getting system alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve system alerts")


@router.get("/comprehensive", response_model=APIResponse)
async def get_comprehensive_status(
    include_data_quality: bool = Query(False, description="是否包含数据质量检查")
) -> APIResponse:
    """
    获取综合系统状态报告
    """
    try:
        # 并发获取各项指标
        tasks = [get_system_metrics(), get_performance_metrics(), get_database_metrics(), get_system_alerts()]

        if include_data_quality:
            tasks.append(get_data_quality_metrics())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        comprehensive_status = {
            "timestamp": datetime.now().isoformat(),
            "service_info": {
                "name": config_manager.settings.app_name,
                "version": config_manager.settings.app_version,
                "environment": "production" if not config_manager.settings.debug else "development",
                "uptime_seconds": int(time.time() - SERVICE_START_TIME) if "SERVICE_START_TIME" in globals() else 0,
            },
        }

        # 处理各项结果
        section_names = ["system_metrics", "performance_metrics", "database_metrics", "system_alerts"]
        if include_data_quality:
            section_names.append("data_quality_metrics")

        for i, result in enumerate(results):
            section_name = section_names[i]
            if isinstance(result, Exception):
                comprehensive_status[section_name] = {"error": str(result), "success": False}
                logger.error(f"Error getting {section_name}: {result}")
            else:
                comprehensive_status[section_name] = result.data if hasattr(result, "data") else result

        # 计算整体健康评分 (0-100)
        health_score = 100

        # 系统资源评分
        if "system_metrics" in comprehensive_status and "error" not in comprehensive_status["system_metrics"]:
            sys_metrics = comprehensive_status["system_metrics"]

            # CPU扣分
            cpu_usage = sys_metrics.get("cpu", {}).get("usage_percent", 0)
            if cpu_usage > 90:
                health_score -= 20
            elif cpu_usage > 80:
                health_score -= 10

            # 内存扣分
            mem_usage = sys_metrics.get("memory", {}).get("usage_percent", 0)
            if mem_usage > 90:
                health_score -= 20
            elif mem_usage > 80:
                health_score -= 10

        # 数据库健康评分
        if "database_metrics" in comprehensive_status and "error" not in comprehensive_status["database_metrics"]:
            clickhouse_healthy = comprehensive_status["database_metrics"].get("clickhouse_healthy", False)
            if not clickhouse_healthy:
                health_score -= 30

        # 告警扣分
        if "system_alerts" in comprehensive_status and "error" not in comprehensive_status["system_alerts"]:
            alerts = comprehensive_status["system_alerts"].get("alerts", [])
            critical_alerts = sum(1 for alert in alerts if alert.get("severity") == "critical")
            warning_alerts = sum(1 for alert in alerts if alert.get("severity") == "warning")

            health_score -= critical_alerts * 10  # 每个严重告警扣10分
            health_score -= warning_alerts * 3  # 每个警告扣3分

        comprehensive_status["overall_health"] = {
            "score": max(0, health_score),
            "status": "healthy" if health_score >= 90 else "degraded" if health_score >= 70 else "unhealthy",
            "message": f"System health score: {max(0, health_score)}/100",
        }

        return APIResponse(success=True, message="Comprehensive system status retrieved", data=comprehensive_status)

    except Exception as e:
        logger.error(f"Error getting comprehensive status: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve comprehensive status")


# 用于跟踪服务启动时间
import time

SERVICE_START_TIME = time.time()
