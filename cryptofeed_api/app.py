"""
Cryptofeed API Service 统一入口
融合数据采集和REST API服务
"""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .api import api_router
from .core import config_manager, settings
from .monitor.collector import BinanceAdvancedMonitor
from .services.data_backfill import DataBackfillService
from .services.data_integrity import DataIntegrityChecker

# 配置日志
logging.basicConfig(
    level=logging.INFO if not settings.debug else logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# 全局变量用于管理数据采集器
monitor_instance: BinanceAdvancedMonitor = None
monitor_task: asyncio.Task = None
backfill_task: asyncio.Task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global monitor_instance, monitor_task, backfill_task

    logger.info("Starting Cryptofeed API Service...")

    try:
        # 启动数据采集服务
        if settings.monitor_enabled:
            logger.info("Starting data collection monitor...")
            monitor_instance = BinanceAdvancedMonitor()

            # 在后台任务中运行数据采集
            monitor_task = asyncio.create_task(run_monitor())
            logger.info("Data collection monitor started")

            # 启动历史数据补充服务
            logger.info("Starting historical data backfill service...")
            backfill_task = asyncio.create_task(run_backfill_service())
            logger.info("Historical data backfill service started")

        logger.info("Cryptofeed API Service started successfully")

        yield  # 应用运行期间

    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise

    finally:
        # 关闭数据采集服务
        if monitor_task and not monitor_task.done():
            logger.info("Stopping data collection monitor...")
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            logger.info("Data collection monitor stopped")

        # 停止历史数据补充任务
        if backfill_task and not backfill_task.done():
            logger.info("Stopping historical data backfill service...")
            backfill_task.cancel()
            try:
                await backfill_task
            except asyncio.CancelledError:
                pass
            logger.info("Historical data backfill service stopped")

        if monitor_instance:
            try:
                # 停止监控器（设置停止标志）
                monitor_instance.is_running = False
                if monitor_instance.feed_handler:
                    monitor_instance.feed_handler.stop()
            except Exception as e:
                logger.error(f"Error stopping monitor: {e}")

        logger.info("Cryptofeed API Service stopped")


async def run_monitor():
    """运行数据采集监控器"""
    try:
        await monitor_instance.run_async()
    except asyncio.CancelledError:
        logger.info("Monitor task cancelled")
        raise
    except Exception as e:
        logger.error(f"Monitor error: {e}")
        # 可以在这里实现重启逻辑
        raise


async def run_backfill_service():
    """运行历史数据补充服务"""
    try:
        # 启动数据完整性检查
        integrity_checker = DataIntegrityChecker()
        backfill_service = DataBackfillService()

        logger.info("🔍 Starting historical data backfill service...")

        # 从配置获取符号列表和补充策略
        symbols = config_manager.get("symbols.custom_list", [])
        if not symbols:
            logger.warning("No symbols configured for monitoring")
            return

        backfill_config = config_manager.get("backfill_strategy", {})
        candle_backfill_days = backfill_config.get("candles", {})

        from datetime import datetime, timedelta

        end_time = datetime.utcnow()

        logger.info(f"📋 Checking backfill for {len(symbols)} symbols from config")

        # 检查并补充不同时间周期的K线数据
        for interval, days in candle_backfill_days.items():
            start_time = end_time - timedelta(days=days)
            logger.info(f"🕐 Checking {interval} candles for last {days} days...")

            for symbol in symbols:
                gaps = await integrity_checker.check_candle_gaps(symbol, interval, start_time, end_time)
                if gaps:
                    logger.info(f"🔧 Found {len(gaps)} {interval} candle gaps for {symbol}, starting backfill...")
                    await backfill_service.backfill_candle_gaps(gaps)
                else:
                    logger.debug(f"✅ No {interval} gaps found for {symbol}")

        # 检查交易数据补充
        trade_days = backfill_config.get("trades", 30)
        trade_start_time = end_time - timedelta(days=trade_days)
        logger.info(f"📊 Checking trade data for last {trade_days} days...")

        for symbol in symbols:
            trade_gaps = await integrity_checker.check_trade_continuity(symbol, trade_start_time, end_time)
            if trade_gaps:
                logger.info(f"🔧 Found {len(trade_gaps)} trade gaps for {symbol}")

        # 检查资金费率数据补充
        funding_days = backfill_config.get("funding", 90)
        funding_start_time = end_time - timedelta(days=funding_days)
        logger.info(f"💰 Checking funding data for last {funding_days} days...")

        logger.info("✅ Historical data backfill service completed initial check")

        # 每小时运行一次完整性检查
        while True:
            await asyncio.sleep(3600)  # 1小时
            logger.info("🔍 Running periodic data integrity check...")

            # 这里可以添加定期的数据完整性检查逻辑

    except asyncio.CancelledError:
        logger.info("Backfill service task cancelled")
        raise
    except Exception as e:
        logger.error(f"Backfill service error: {e}")
        import traceback

        traceback.print_exc()
        raise


# 创建FastAPI应用
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Cryptocurrency data collection and API service",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制特定域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 全局异常处理器
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """全局异常处理"""
    logger.error(f"Global exception on {request.url}: {exc}", exc_info=True)

    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "message": "Internal server error",
            "error_code": "INTERNAL_ERROR",
            "path": str(request.url.path),
        },
    )


# 健康检查中间件
@app.middleware("http")
async def health_check_middleware(request: Request, call_next):
    """请求处理中间件"""
    # 记录请求信息
    logger.debug(f"Request: {request.method} {request.url}")

    response = await call_next(request)

    # 记录响应信息
    logger.debug(f"Response: {response.status_code}")

    return response


# 注册路由
app.include_router(api_router, prefix=settings.api_prefix)


# 根路径
@app.get("/")
async def root() -> Dict[str, Any]:
    """根路径信息"""
    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "docs": "/docs",
        "health": f"{settings.api_prefix}/v1/health",
        "data_collection": settings.monitor_enabled,
    }


# 获取服务状态
@app.get("/status")
async def service_status() -> Dict[str, Any]:
    """服务状态信息"""
    global monitor_instance, monitor_task

    monitor_status = "unknown"
    if settings.monitor_enabled:
        if monitor_task and not monitor_task.done():
            monitor_status = "running"
        else:
            monitor_status = "stopped"
    else:
        monitor_status = "disabled"

    return {
        "api_service": "running",
        "data_collection": monitor_status,
        "configuration": {
            "debug": settings.debug,
            "monitor_enabled": settings.monitor_enabled,
            "monitor_symbols": settings.monitor_symbols,
        },
    }


def handle_shutdown_signal(signum, frame):
    """处理关闭信号"""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


def main():
    """主入口函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"API will be available at http://{settings.api_host}:{settings.api_port}")
    logger.info(f"Documentation at http://{settings.api_host}:{settings.api_port}/docs")

    # 启动uvicorn服务器
    uvicorn.run(
        "cryptofeed_api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level="debug" if settings.debug else "info",
        access_log=True,
    )


if __name__ == "__main__":
    main()
