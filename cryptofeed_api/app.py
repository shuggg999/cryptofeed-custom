"""
Cryptofeed API Service 统一入口
融合数据采集和REST API服务

主要功能：
1. 启动 FastAPI REST API 服务（提供数据查询接口）
2. 启动实时数据采集器（从 Binance WebSocket 采集交易数据）
3. 启动历史数据回填服务（补充缺失的历史数据）

架构说明：
- 使用 FastAPI 框架提供 REST API
- 使用 asyncio 异步编程实现高并发
- 使用 lifespan 管理应用生命周期
- 后台任务与 API 服务并行运行
"""

# ============================================================
# 标准库导入
# ============================================================
import asyncio  # 异步编程核心库
import logging  # 日志记录
import signal   # 信号处理（用于优雅关闭程序）
import sys      # 系统相关功能
from contextlib import asynccontextmanager  # 异步上下文管理器装饰器
from typing import Any, Dict  # 类型提示

# ============================================================
# 第三方库导入
# ============================================================
import uvicorn  # ASGI 服务器（用于运行 FastAPI 应用）
from fastapi import FastAPI, Request  # FastAPI 框架核心
from fastapi.middleware.cors import CORSMiddleware  # CORS 跨域中间件
from fastapi.responses import JSONResponse  # JSON 响应类

# ============================================================
# 本地模块导入
# ============================================================
from .api import api_router  # API 路由聚合器（包含所有 /api/v1/* 路由）
from .core import config_manager, settings  # 配置管理器和全局设置
from .core.logging_config import setup_logging, get_uvicorn_log_config  # 统一日志配置
from .monitor.collector import BinanceAdvancedMonitor  # Binance 数据采集器
from .services.data_backfill import DataBackfillService  # 历史数据回填服务
from .services.data_integrity import DataIntegrityChecker  # 数据完整性检查器

# ============================================================
# 日志配置
# ============================================================
# 使用统一的日志配置
# 格式：时间戳 | 级别 | 模块名:行号 | 消息
# 支持 PyCharm 点击跳转到源码
setup_logging(level="INFO", debug=settings.debug)
logger = logging.getLogger(__name__)  # 获取当前模块的日志记录器

# ============================================================
# 全局变量 - 用于管理后台任务
# ============================================================
# 这些全局变量在 lifespan 函数中被初始化和管理
monitor_instance: BinanceAdvancedMonitor = None  # 数据采集器实例
monitor_task: asyncio.Task = None  # 数据采集后台任务
backfill_task: asyncio.Task = None  # 数据回填后台任务


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用生命周期管理器

    这是 FastAPI 的生命周期钩子，用于管理应用启动和关闭时的操作。
    yield 之前的代码在应用启动时执行，yield 之后的代码在应用关闭时执行。

    启动时：
    1. 创建 Binance 数据采集器实例
    2. 启动数据采集后台任务（WebSocket 连接）
    3. 启动数据回填后台任务（检查并补充历史数据）

    关闭时：
    1. 取消所有后台任务
    2. 停止数据采集器
    3. 清理资源
    """
    global monitor_instance, monitor_task, backfill_task

    logger.info("🚀 正在启动 Cryptofeed API 服务...")

    try:
        # ============================================================
        # 启动阶段 - 初始化所有服务
        # ============================================================

        # 检查是否启用了数据监控（通过配置文件控制）
        if settings.monitor_enabled:
            logger.info("📡 正在启动数据采集监控器...")

            # 创建 Binance 数据采集器实例
            monitor_instance = BinanceAdvancedMonitor()

            # 创建后台任务运行数据采集器
            # asyncio.create_task() 会立即返回，任务在后台异步运行
            monitor_task = asyncio.create_task(run_monitor())
            logger.info("✅ 数据采集监控器已启动")

            # 启动历史数据补充服务
            logger.info("📚 正在启动历史数据回填服务...")
            backfill_task = asyncio.create_task(run_backfill_service())
            logger.info("✅ 历史数据回填服务已启动")

        logger.info("🎉 Cryptofeed API 服务启动成功！")

        # ============================================================
        # 运行阶段 - yield 暂停在这里，等待应用关闭信号
        # ============================================================
        yield  # 应用运行期间会停留在这里

        # ============================================================
        # 关闭阶段 - 清理资源
        # ============================================================

    except Exception as e:
        logger.error(f"❌ 服务启动失败: {e}")
        raise

    finally:
        # 无论是否发生异常，都会执行清理操作

        # 停止数据采集任务
        if monitor_task and not monitor_task.done():
            logger.info("⏸️  正在停止数据采集监控器...")
            monitor_task.cancel()  # 发送取消信号
            try:
                await monitor_task  # 等待任务真正结束
            except asyncio.CancelledError:
                pass  # 忽略取消异常
            logger.info("✅ 数据采集监控器已停止")

        # 停止历史数据补充任务
        if backfill_task and not backfill_task.done():
            logger.info("⏸️  正在停止历史数据回填服务...")
            backfill_task.cancel()
            try:
                await backfill_task
            except asyncio.CancelledError:
                pass
            logger.info("✅ 历史数据回填服务已停止")

        # 停止监控器实例（关闭 WebSocket 连接等）
        if monitor_instance:
            try:
                monitor_instance.is_running = False  # 设置停止标志
                if monitor_instance.feed_handler:
                    monitor_instance.feed_handler.stop()  # 停止 Cryptofeed 的 FeedHandler
            except Exception as e:
                logger.error(f"❌ 停止监控器时出错: {e}")

        logger.info("👋 Cryptofeed API 服务已停止")


async def run_monitor():
    """
    运行数据采集监控器

    这个函数会在后台持续运行，通过 WebSocket 连接到 Binance
    实时采集交易数据、K线数据、资金费率等信息。

    执行流程：
    1. 调用 monitor_instance.run_async() 启动 WebSocket 连接
    2. 持续接收和处理数据流
    3. 直到收到取消信号或发生错误
    """
    try:
        # 启动数据采集器的异步运行方法
        # 这个方法会一直运行，除非被取消或发生异常
        await monitor_instance.run_async()

    except asyncio.CancelledError:
        # 收到取消信号（正常关闭）
        logger.info("📡 监控任务已被取消")
        raise  # 重新抛出，让调用者知道任务被取消

    except Exception as e:
        # 发生意外错误
        logger.error(f"❌ 监控器运行错误: {e}")
        # TODO: 可以在这里实现自动重启逻辑
        raise


async def run_backfill_service():
    """
    运行历史数据补充服务

    功能说明：
    1. 检查数据库中的数据完整性（是否有缺口）
    2. 补充缺失的历史数据（K线、交易、资金费率）
    3. 定期运行完整性检查（每小时一次）

    工作流程：
    - 启动时：执行一次完整的数据检查和回填
    - 运行时：每小时检查一次数据完整性
    - 关闭时：收到取消信号后停止
    """
    try:
        # ============================================================
        # 初始化服务
        # ============================================================
        integrity_checker = DataIntegrityChecker()  # 数据完整性检查器
        backfill_service = DataBackfillService()    # 数据回填服务

        logger.info("🔍 正在启动历史数据回填服务...")

        # ============================================================
        # 读取配置
        # ============================================================
        # 从配置文件获取需要监控的交易对列表（如 BTC-USDT, ETH-USDT）
        symbols = config_manager.get("symbols.custom_list", [])
        if not symbols:
            logger.warning("⚠️  配置中没有设置交易对，无法进行数据回填")
            return

        # 获取回填策略配置（定义了需要回填多少天的数据）
        backfill_config = config_manager.get("backfill_strategy", {})
        candle_backfill_days = backfill_config.get("candles", {})

        from datetime import datetime, timedelta

        end_time = datetime.utcnow()  # 当前时间（UTC）

        logger.info(f"📋 正在检查 {len(symbols)} 个交易对的历史数据...")

        # ============================================================
        # 检查并补充 K线数据
        # ============================================================
        # 遍历不同的时间周期（如 1m, 5m, 1h, 1d）
        for interval, days in candle_backfill_days.items():
            start_time = end_time - timedelta(days=days)  # 计算起始时间
            logger.info(f"🕐 正在检查 {interval} K线数据（最近 {days} 天）...")

            # 遍历每个交易对
            for symbol in symbols:
                # 检查数据缺口
                gaps = await integrity_checker.check_candle_gaps(
                    symbol, interval, start_time, end_time
                )

                if gaps:
                    # 发现缺口，开始回填
                    logger.info(f"🔧 发现 {symbol} 的 {len(gaps)} 个 {interval} K线缺口，开始回填...")
                    await backfill_service.backfill_candle_gaps(gaps)
                else:
                    # 数据完整
                    logger.debug(f"✅ {symbol} 的 {interval} K线数据完整")

        # ============================================================
        # 检查并补充交易数据
        # ============================================================
        trade_days = backfill_config.get("trades", 30)  # 默认检查 30 天
        trade_start_time = end_time - timedelta(days=trade_days)
        logger.info(f"📊 正在检查交易数据（最近 {trade_days} 天）...")

        for symbol in symbols:
            # check_trade_gaps 不是异步方法，不需要 await
            trade_gaps = integrity_checker.check_trade_gaps(
                symbol, trade_start_time, end_time
            )
            if trade_gaps:
                logger.info(f"🔧 发现 {symbol} 的 {len(trade_gaps)} 个交易数据缺口")
                # TODO: 实现交易数据回填逻辑

        # ============================================================
        # 检查并补充资金费率数据
        # ============================================================
        funding_days = backfill_config.get("funding", 90)  # 默认检查 90 天
        funding_start_time = end_time - timedelta(days=funding_days)
        logger.info(f"💰 正在检查资金费率数据（最近 {funding_days} 天）...")
        # TODO: 实现资金费率完整性检查和回填

        logger.info("✅ 历史数据回填服务初始检查完成")

        # ============================================================
        # 定期检查循环
        # ============================================================
        # 每小时运行一次完整性检查
        while True:
            await asyncio.sleep(3600)  # 休眠 3600 秒（1小时）
            logger.info("🔍 正在执行定期数据完整性检查...")

            # TODO: 可以在这里添加定期的数据完整性检查逻辑
            # 例如：重新检查最近几小时的数据

    except asyncio.CancelledError:
        # 收到取消信号（正常关闭）
        logger.info("📚 数据回填服务任务已被取消")
        raise

    except Exception as e:
        # 发生意外错误
        logger.error(f"❌ 数据回填服务错误: {e}")
        import traceback
        traceback.print_exc()  # 打印完整的错误堆栈
        raise


# ============================================================
# 创建 FastAPI 应用实例
# ============================================================
app = FastAPI(
    title=settings.app_name,  # 应用名称（显示在 API 文档中）
    version=settings.app_version,  # 版本号
    description="加密货币数据采集与 API 服务",  # 应用描述
    docs_url="/docs",  # Swagger UI 文档地址
    redoc_url="/redoc",  # ReDoc 文档地址
    lifespan=lifespan,  # 应用生命周期管理器
)

# ============================================================
# 添加 CORS 中间件 - 允许跨域请求
# ============================================================
# CORS（跨域资源共享）允许前端从不同域名访问 API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源（生产环境应该限制为特定域名）
    allow_credentials=True,  # 允许携带认证信息（cookies）
    allow_methods=["*"],  # 允许所有 HTTP 方法（GET, POST, PUT, DELETE 等）
    allow_headers=["*"],  # 允许所有请求头
)


# ============================================================
# 全局异常处理器 - 捕获所有未处理的异常
# ============================================================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    全局异常处理器

    当发生未捕获的异常时，这个处理器会被调用，
    返回统一格式的错误响应，避免暴露敏感的错误信息。
    """
    logger.error(f"❌ 全局异常 on {request.url}: {exc}", exc_info=True)

    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "message": "服务器内部错误",
            "error_code": "INTERNAL_ERROR",
            "path": str(request.url.path),
        },
    )


# ============================================================
# HTTP 请求处理中间件 - 记录所有请求
# ============================================================
@app.middleware("http")
async def health_check_middleware(request: Request, call_next):
    """
    HTTP 请求日志中间件

    这个中间件会拦截所有 HTTP 请求，记录请求和响应信息。
    在调试模式下很有用，可以看到每个请求的详细信息。
    """
    # 记录请求信息（仅在 DEBUG 模式下）
    logger.debug(f"📨 请求: {request.method} {request.url}")

    # 调用下一个处理器（实际的路由处理函数）
    response = await call_next(request)

    # 记录响应信息（仅在 DEBUG 模式下）
    logger.debug(f"📤 响应: {response.status_code}")

    return response


# ============================================================
# 注册所有 API 路由
# ============================================================
# api_router 包含所有的 API 端点（来自 cryptofeed_api/api/__init__.py）
# 所有路由都会添加 /api 前缀（通过 settings.api_prefix 配置）
app.include_router(api_router, prefix=settings.api_prefix)


# ============================================================
# 根路径 - 服务基本信息
# ============================================================
@app.get("/")
async def root() -> Dict[str, Any]:
    """
    根路径 GET /

    返回服务的基本信息，包括版本、状态、文档链接等。
    """
    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "docs": "/docs",  # Swagger UI 文档地址
        "health": f"{settings.api_prefix}/v1/health",  # 健康检查接口
        "data_collection": settings.monitor_enabled,  # 数据采集是否启用
    }


# ============================================================
# 服务状态接口 - 查看服务运行状态
# ============================================================
@app.get("/status")
async def service_status() -> Dict[str, Any]:
    """
    服务状态 GET /status

    返回详细的服务运行状态，包括：
    - API 服务状态
    - 数据采集状态（running/stopped/disabled）
    - 配置信息
    """
    global monitor_instance, monitor_task

    # 判断数据采集器的运行状态
    monitor_status = "unknown"
    if settings.monitor_enabled:
        # 检查监控任务是否在运行
        if monitor_task and not monitor_task.done():
            monitor_status = "running"  # 正在运行
        else:
            monitor_status = "stopped"  # 已停止
    else:
        monitor_status = "disabled"  # 未启用

    return {
        "api_service": "running",  # API 服务状态
        "data_collection": monitor_status,  # 数据采集状态
        "configuration": {
            "debug": settings.debug,  # 是否为调试模式
            "monitor_enabled": settings.monitor_enabled,  # 是否启用监控
            "monitor_symbols": settings.monitor_symbols,  # 监控的交易对列表
        },
    }


# ============================================================
# 信号处理器 - 优雅关闭
# ============================================================
def handle_shutdown_signal(signum, frame):
    """
    处理系统关闭信号

    当收到 SIGINT（Ctrl+C）或 SIGTERM 信号时，
    会调用这个函数优雅地关闭服务。

    Args:
        signum: 信号编号
        frame: 当前堆栈帧
    """
    logger.info(f"🛑 收到关闭信号 {signum}，正在关闭服务...")
    sys.exit(0)


# ============================================================
# 主入口函数
# ============================================================
def main():
    """
    主入口函数

    这是程序的启动入口，执行以下操作：
    1. 注册系统信号处理器（用于优雅关闭）
    2. 打印启动信息
    3. 启动 uvicorn ASGI 服务器

    uvicorn 是一个高性能的 ASGI 服务器，用于运行 FastAPI 应用。
    """
    # ============================================================
    # 注册信号处理器
    # ============================================================
    # SIGINT: Ctrl+C 信号
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    # SIGTERM: kill 命令发送的终止信号
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    # ============================================================
    # 打印启动信息
    # ============================================================  
    logger.info(f"🚀 正在启动 {settings.app_name} v{settings.app_version}")
    logger.info(f"🌐 API 服务地址: http://{settings.api_host}:{settings.api_port}")
    logger.info(f"📚 API 文档地址: http://{settings.api_host}:{settings.api_port}/docs")

    # ============================================================
    # 启动 uvicorn 服务器
    # ============================================================
    uvicorn.run(
        "cryptofeed_api.app:app",  # 应用路径（模块:变量）
        host=settings.api_host,  # 监听地址（0.0.0.0 表示所有网卡）
        port=settings.api_port,  # 监听端口
        reload=settings.debug,  # 调试模式下自动重载代码
        log_level="debug" if settings.debug else "info",  # 日志级别
        access_log=True,  # 启用访问日志
        log_config=get_uvicorn_log_config(),  # 使用统一的日志配置
    )


# ============================================================
# 脚本直接运行时的入口点
# ============================================================
# 当直接运行这个文件时（python app.py），会执行 main()
# 当作为模块导入时，不会执行 main()
if __name__ == "__main__":
    main()
