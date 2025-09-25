"""
监控和系统状态API端点
"""
import logging
import psutil
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ...models.schemas import APIResponse
from ...core.database import get_db_session, check_database_health
from ...core.config import config_manager
from ...core.retry_manager import retry_manager, error_handler
from ...core.rate_limiter import rate_limiter
from ...services.temp_data_manager import temp_data_manager
from ...services.data_integrity import DataIntegrityChecker

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
        disk_usage = psutil.disk_usage('/')

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
                "process_usage_percent": process_cpu
            },
            "memory": {
                "total_mb": round(memory.total / 1024 / 1024),
                "available_mb": round(memory.available / 1024 / 1024),
                "used_mb": round(memory.used / 1024 / 1024),
                "usage_percent": memory.percent,
                "process_rss_mb": round(process_memory.rss / 1024 / 1024),
                "process_vms_mb": round(process_memory.vms / 1024 / 1024)
            },
            "swap": {
                "total_mb": round(swap.total / 1024 / 1024),
                "used_mb": round(swap.used / 1024 / 1024),
                "usage_percent": swap.percent
            },
            "disk": {
                "total_gb": round(disk_usage.total / 1024**3, 2),
                "used_gb": round(disk_usage.used / 1024**3, 2),
                "free_gb": round(disk_usage.free / 1024**3, 2),
                "usage_percent": round((disk_usage.used / disk_usage.total) * 100, 2)
            },
            "network": {
                "bytes_sent": net_io.bytes_sent,
                "bytes_recv": net_io.bytes_recv,
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv
            }
        }

        return APIResponse(
            success=True,
            message="System metrics retrieved",
            data=system_metrics
        )

    except Exception as e:
        logger.error(f"Error getting system metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve system metrics")


@router.get("/performance", response_model=APIResponse)
async def get_performance_metrics() -> APIResponse:
    """
    获取应用性能指标
    """
    try:
        # 重试管理器统计
        retry_stats = retry_manager.get_stats()

        # 错误处理统计
        error_stats = error_handler.get_error_stats()

        # 限流器统计
        rate_limit_stats = await rate_limiter.get_stats()

        # 缓存统计
        cache_stats = await temp_data_manager.get_stats()
        cache_detailed = await temp_data_manager.get_detailed_info()

        performance_metrics = {
            "timestamp": datetime.now().isoformat(),
            "retry_manager": retry_stats,
            "error_handler": error_stats,
            "rate_limiter": rate_limit_stats,
            "cache": {
                "basic": {
                    "total_entries": cache_stats.total_entries,
                    "total_size_bytes": cache_stats.total_size_bytes,
                    "hit_count": cache_stats.hit_count,
                    "miss_count": cache_stats.miss_count,
                    "hit_rate": round(
                        cache_stats.hit_count / (cache_stats.hit_count + cache_stats.miss_count) * 100, 2
                    ) if (cache_stats.hit_count + cache_stats.miss_count) > 0 else 0
                },
                "detailed": cache_detailed
            }
        }

        return APIResponse(
            success=True,
            message="Performance metrics retrieved",
            data=performance_metrics
        )

    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve performance metrics")


@router.get("/database", response_model=APIResponse)
async def get_database_metrics(
    db: AsyncSession = Depends(get_db_session)
) -> APIResponse:
    """
    获取数据库相关指标
    """
    try:
        # 基础健康检查
        db_health = await check_database_health()

        # 详细统计查询
        tables_info = {}
        table_names = ["trades", "candles", "funding", "tickers", "data_gaps"]

        for table in table_names:
            try:
                # 获取表基本统计
                stats_query = text(f"""
                    SELECT
                        COUNT(*) as row_count,
                        pg_size_pretty(pg_total_relation_size('{table}')) as table_size,
                        MIN(timestamp) as earliest_timestamp,
                        MAX(timestamp) as latest_timestamp
                    FROM {table}
                    WHERE timestamp IS NOT NULL
                """)

                result = await db.execute(stats_query)
                row = result.first()

                if row:
                    tables_info[table] = {
                        "row_count": row.row_count,
                        "table_size": row.table_size,
                        "earliest_timestamp": row.earliest_timestamp.isoformat() if row.earliest_timestamp else None,
                        "latest_timestamp": row.latest_timestamp.isoformat() if row.latest_timestamp else None
                    }

                    # 获取最近24小时的数据量
                    recent_query = text(f"""
                        SELECT COUNT(*) as recent_count
                        FROM {table}
                        WHERE timestamp >= NOW() - INTERVAL '24 hours'
                    """)
                    recent_result = await db.execute(recent_query)
                    recent_row = recent_result.first()

                    if recent_row:
                        tables_info[table]["recent_24h_count"] = recent_row.recent_count

            except Exception as table_error:
                logger.warning(f"Failed to get stats for table {table}: {table_error}")
                tables_info[table] = {"error": str(table_error)}

        # 数据库连接信息
        connection_query = text("""
            SELECT
                count(*) as total_connections,
                count(*) FILTER (WHERE state = 'active') as active_connections,
                count(*) FILTER (WHERE state = 'idle') as idle_connections
            FROM pg_stat_activity
            WHERE datname = current_database()
        """)

        conn_result = await db.execute(connection_query)
        conn_row = conn_result.first()

        database_metrics = {
            "timestamp": datetime.now().isoformat(),
            "health": db_health,
            "connections": {
                "total": conn_row.total_connections if conn_row else 0,
                "active": conn_row.active_connections if conn_row else 0,
                "idle": conn_row.idle_connections if conn_row else 0
            },
            "tables": tables_info
        }

        return APIResponse(
            success=True,
            message="Database metrics retrieved",
            data=database_metrics
        )

    except Exception as e:
        logger.error(f"Error getting database metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve database metrics")


@router.get("/data-quality", response_model=APIResponse)
async def get_data_quality_metrics(
    symbols: str = Query("BTC-USDT-PERP,ETH-USDT-PERP", description="交易对列表，逗号分隔"),
    hours: int = Query(24, ge=1, le=168, description="检查时间范围（小时）"),
    db: AsyncSession = Depends(get_db_session)
) -> APIResponse:
    """
    获取数据质量指标
    """
    try:
        symbol_list = [s.strip().upper() for s in symbols.split(',') if s.strip()]
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        integrity_checker = DataIntegrityChecker()
        quality_metrics = {}

        for symbol in symbol_list[:5]:  # 限制最多5个符号
            try:
                symbol_metrics = {
                    "symbol": symbol,
                    "time_range": {
                        "start": start_time.isoformat(),
                        "end": end_time.isoformat(),
                        "hours": hours
                    }
                }

                # 检查各数据类型的统计
                for data_type in ["trades", "candles", "funding"]:
                    try:
                        stats = await integrity_checker.get_data_stats(symbol, data_type, "1m" if data_type == "candles" else None)

                        symbol_metrics[data_type] = {
                            "total_count": stats.total_count,
                            "earliest_time": stats.earliest_time.isoformat() if stats.earliest_time else None,
                            "latest_time": stats.latest_time.isoformat() if stats.latest_time else None,
                            "gaps_found": stats.gaps_found
                        }

                        # 计算数据新鲜度（最新数据距离现在的时间）
                        if stats.latest_time:
                            freshness_minutes = (datetime.now() - stats.latest_time).total_seconds() / 60
                            symbol_metrics[data_type]["freshness_minutes"] = round(freshness_minutes, 2)

                    except Exception as dt_error:
                        symbol_metrics[data_type] = {"error": str(dt_error)}

                # 数据完整性评分（0-100）
                completeness_score = 100
                freshness_penalty = 0

                # 根据数据缺口和新鲜度计算评分
                for data_type in ["trades", "candles", "funding"]:
                    if data_type in symbol_metrics and "gaps_found" in symbol_metrics[data_type]:
                        gaps = symbol_metrics[data_type]["gaps_found"]
                        completeness_score -= min(gaps * 5, 30)  # 每个缺口扣5分，最多扣30分

                    if data_type in symbol_metrics and "freshness_minutes" in symbol_metrics[data_type]:
                        freshness = symbol_metrics[data_type]["freshness_minutes"]
                        if freshness > 10:  # 超过10分钟算不新鲜
                            freshness_penalty += min((freshness - 10) * 2, 20)  # 每分钟扣2分，最多扣20分

                symbol_metrics["quality_score"] = max(0, round(completeness_score - freshness_penalty))

                quality_metrics[symbol] = symbol_metrics

            except Exception as symbol_error:
                logger.error(f"Error processing symbol {symbol}: {symbol_error}")
                quality_metrics[symbol] = {"error": str(symbol_error)}

        # 计算整体数据质量评分
        valid_scores = [
            metrics.get("quality_score", 0)
            for metrics in quality_metrics.values()
            if isinstance(metrics.get("quality_score"), (int, float))
        ]

        overall_score = round(sum(valid_scores) / len(valid_scores)) if valid_scores else 0

        return APIResponse(
            success=True,
            message=f"Data quality metrics for {len(symbol_list)} symbols",
            data={
                "overall_quality_score": overall_score,
                "symbols_analyzed": len(symbol_list),
                "analysis_period": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                    "hours": hours
                },
                "symbol_metrics": quality_metrics
            }
        )

    except Exception as e:
        logger.error(f"Error getting data quality metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve data quality metrics")


@router.get("/alerts", response_model=APIResponse)
async def get_system_alerts(
    severity: str = Query("all", description="告警级别: all, critical, warning, info"),
    limit: int = Query(50, ge=1, le=200, description="返回数量限制")
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
        disk_usage = psutil.disk_usage('/')

        # 内存使用率告警
        if memory.percent > 90:
            alerts.append({
                "id": "memory_critical",
                "timestamp": current_time.isoformat(),
                "severity": "critical",
                "component": "system",
                "message": f"Memory usage critical: {memory.percent:.1f}%",
                "details": {
                    "current_usage": memory.percent,
                    "threshold": 90,
                    "available_mb": round(memory.available / 1024 / 1024)
                }
            })
        elif memory.percent > 80:
            alerts.append({
                "id": "memory_warning",
                "timestamp": current_time.isoformat(),
                "severity": "warning",
                "component": "system",
                "message": f"Memory usage high: {memory.percent:.1f}%",
                "details": {
                    "current_usage": memory.percent,
                    "threshold": 80
                }
            })

        # CPU使用率告警
        if cpu_percent > 90:
            alerts.append({
                "id": "cpu_critical",
                "timestamp": current_time.isoformat(),
                "severity": "critical",
                "component": "system",
                "message": f"CPU usage critical: {cpu_percent:.1f}%",
                "details": {
                    "current_usage": cpu_percent,
                    "threshold": 90
                }
            })

        # 磁盘使用率告警
        disk_percent = (disk_usage.used / disk_usage.total) * 100
        if disk_percent > 90:
            alerts.append({
                "id": "disk_critical",
                "timestamp": current_time.isoformat(),
                "severity": "critical",
                "component": "system",
                "message": f"Disk usage critical: {disk_percent:.1f}%",
                "details": {
                    "current_usage": disk_percent,
                    "threshold": 90,
                    "free_gb": round(disk_usage.free / 1024**3, 2)
                }
            })

        # 检查数据库连接告警
        try:
            db_health = await check_database_health()
            if not db_health.get("connected", False):
                alerts.append({
                    "id": "database_connection",
                    "timestamp": current_time.isoformat(),
                    "severity": "critical",
                    "component": "database",
                    "message": "Database connection failed",
                    "details": db_health
                })
        except Exception as db_error:
            alerts.append({
                "id": "database_check_error",
                "timestamp": current_time.isoformat(),
                "severity": "critical",
                "component": "database",
                "message": f"Database health check failed: {str(db_error)}",
                "details": {"error": str(db_error)}
            })

        # 检查错误率告警
        error_stats = error_handler.get_error_stats()
        if error_stats["total_errors"] > 100:  # 总错误数超过100
            alerts.append({
                "id": "high_error_rate",
                "timestamp": current_time.isoformat(),
                "severity": "warning",
                "component": "application",
                "message": f"High error count: {error_stats['total_errors']} total errors",
                "details": error_stats
            })

        # 检查限流告警
        rate_limit_stats = rate_limiter.get_stats()
        blocked_ips = rate_limit_stats.get("blocked_ips", 0)
        if blocked_ips > 0:
            alerts.append({
                "id": "blocked_ips",
                "timestamp": current_time.isoformat(),
                "severity": "info",
                "component": "rate_limiter",
                "message": f"{blocked_ips} IP addresses are currently blocked",
                "details": {"blocked_count": blocked_ips}
            })

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
                    "generated_at": current_time.isoformat()
                }
            }
        )

    except Exception as e:
        logger.error(f"Error getting system alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve system alerts")


@router.get("/comprehensive", response_model=APIResponse)
async def get_comprehensive_status(
    include_data_quality: bool = Query(False, description="是否包含数据质量检查"),
    db: AsyncSession = Depends(get_db_session)
) -> APIResponse:
    """
    获取综合系统状态报告
    """
    try:
        # 并发获取各项指标
        tasks = [
            get_system_metrics(),
            get_performance_metrics(),
            get_database_metrics(),
            get_system_alerts()
        ]

        if include_data_quality:
            tasks.append(get_data_quality_metrics())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        comprehensive_status = {
            "timestamp": datetime.now().isoformat(),
            "service_info": {
                "name": config_manager.settings.app_name,
                "version": config_manager.settings.app_version,
                "environment": "production" if not config_manager.settings.debug else "development",
                "uptime_seconds": int(time.time() - SERVICE_START_TIME) if 'SERVICE_START_TIME' in globals() else 0
            }
        }

        # 处理各项结果
        section_names = ["system_metrics", "performance_metrics", "database_metrics", "system_alerts"]
        if include_data_quality:
            section_names.append("data_quality_metrics")

        for i, result in enumerate(results):
            section_name = section_names[i]
            if isinstance(result, Exception):
                comprehensive_status[section_name] = {
                    "error": str(result),
                    "success": False
                }
                logger.error(f"Error getting {section_name}: {result}")
            else:
                comprehensive_status[section_name] = result.data if hasattr(result, 'data') else result

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
            db_health = comprehensive_status["database_metrics"].get("health", {})
            if not db_health.get("connected", False):
                health_score -= 30

        # 告警扣分
        if "system_alerts" in comprehensive_status and "error" not in comprehensive_status["system_alerts"]:
            alerts = comprehensive_status["system_alerts"].get("alerts", [])
            critical_alerts = sum(1 for alert in alerts if alert.get("severity") == "critical")
            warning_alerts = sum(1 for alert in alerts if alert.get("severity") == "warning")

            health_score -= critical_alerts * 10  # 每个严重告警扣10分
            health_score -= warning_alerts * 3    # 每个警告扣3分

        comprehensive_status["overall_health"] = {
            "score": max(0, health_score),
            "status": "healthy" if health_score >= 90 else "degraded" if health_score >= 70 else "unhealthy",
            "message": f"System health score: {max(0, health_score)}/100"
        }

        return APIResponse(
            success=True,
            message="Comprehensive system status retrieved",
            data=comprehensive_status
        )

    except Exception as e:
        logger.error(f"Error getting comprehensive status: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve comprehensive status")


# 用于跟踪服务启动时间
import time
SERVICE_START_TIME = time.time()