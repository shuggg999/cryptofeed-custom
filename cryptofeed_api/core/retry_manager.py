"""
错误处理和重试机制
"""
import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import (
    Callable, Dict, Any, Optional, Union, List, Tuple,
    Type, Awaitable, TypeVar
)
from dataclasses import dataclass
from functools import wraps
import traceback

T = TypeVar('T')

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """重试配置"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    jitter: bool = True
    exponential_backoff: bool = True


@dataclass
class RetryResult:
    """重试结果"""
    success: bool
    result: Any = None
    attempts: int = 0
    total_duration: float = 0.0
    last_error: Optional[Exception] = None
    error_history: List[Exception] = None

    def __post_init__(self):
        if self.error_history is None:
            self.error_history = []


class RetryableError(Exception):
    """可重试的错误基类"""
    pass


class NonRetryableError(Exception):
    """不可重试的错误基类"""
    pass


class CircuitBreakerError(Exception):
    """熔断器错误"""
    pass


class CircuitBreaker:
    """熔断器实现"""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "closed"  # closed, open, half_open

    def _reset(self):
        """重置熔断器"""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"

    def _record_success(self):
        """记录成功"""
        self._reset()

    def _record_failure(self):
        """记录失败"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

    def _can_attempt(self) -> bool:
        """检查是否可以尝试"""
        if self.state == "closed":
            return True

        if self.state == "open":
            if self.last_failure_time:
                time_since_failure = datetime.now() - self.last_failure_time
                if time_since_failure.total_seconds() > self.recovery_timeout:
                    self.state = "half_open"
                    logger.info("Circuit breaker half-opened for recovery test")
                    return True
            return False

        # half_open state
        return True

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """通过熔断器调用函数"""
        if not self._can_attempt():
            raise CircuitBreakerError("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            self._record_success()
            return result

        except self.expected_exception as e:
            self._record_failure()
            raise

        except Exception as e:
            # 非预期异常不计入失败
            raise


class RetryManager:
    """重试管理器"""

    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.retry_stats: Dict[str, Dict[str, Any]] = {}

    def get_circuit_breaker(self, name: str, **kwargs) -> CircuitBreaker:
        """获取或创建熔断器"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker(**kwargs)
        return self.circuit_breakers[name]

    def _calculate_delay(self, attempt: int, config: RetryConfig) -> float:
        """计算延迟时间"""
        if config.exponential_backoff:
            delay = config.base_delay * (config.backoff_factor ** (attempt - 1))
        else:
            delay = config.base_delay

        # 限制最大延迟
        delay = min(delay, config.max_delay)

        # 添加抖动
        if config.jitter:
            jitter_factor = random.uniform(0.5, 1.5)
            delay *= jitter_factor

        return delay

    def _should_retry(self, exception: Exception, attempt: int, max_attempts: int) -> bool:
        """判断是否应该重试"""
        if attempt >= max_attempts:
            return False

        # 不可重试的错误
        if isinstance(exception, NonRetryableError):
            return False

        # 熔断器错误不重试
        if isinstance(exception, CircuitBreakerError):
            return False

        # 明确标记的可重试错误
        if isinstance(exception, RetryableError):
            return True

        # 网络相关错误通常可重试
        network_errors = (
            ConnectionError,
            TimeoutError,
            OSError
        )
        if isinstance(exception, network_errors):
            return True

        # HTTP相关错误（需要导入后才能检查）
        try:
            import aiohttp
            if isinstance(exception, (aiohttp.ClientError, aiohttp.ServerTimeoutError)):
                return True
        except ImportError:
            pass

        # 数据库连接错误通常可重试
        try:
            from sqlalchemy.exc import DisconnectionError, TimeoutError as SQLTimeoutError
            if isinstance(exception, (DisconnectionError, SQLTimeoutError)):
                return True
        except ImportError:
            pass

        return False

    async def retry_async(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        config: Optional[RetryConfig] = None,
        circuit_breaker_name: Optional[str] = None,
        **kwargs
    ) -> RetryResult:
        """异步函数重试"""
        config = config or RetryConfig()
        start_time = asyncio.get_event_loop().time()

        result = RetryResult(success=False)
        circuit_breaker = None

        # 设置熔断器
        if circuit_breaker_name:
            circuit_breaker = self.get_circuit_breaker(circuit_breaker_name)

        for attempt in range(1, config.max_attempts + 1):
            result.attempts = attempt

            try:
                # 通过熔断器调用或直接调用
                if circuit_breaker:
                    func_result = await circuit_breaker.call(func, *args, **kwargs)
                else:
                    func_result = await func(*args, **kwargs)

                result.success = True
                result.result = func_result
                break

            except Exception as e:
                result.last_error = e
                result.error_history.append(e)

                logger.warning(
                    f"Attempt {attempt}/{config.max_attempts} failed for {func.__name__}: {e}"
                )

                # 检查是否应该重试
                if not self._should_retry(e, attempt, config.max_attempts):
                    logger.error(f"Non-retryable error or max attempts reached: {e}")
                    break

                # 如果不是最后一次尝试，等待后重试
                if attempt < config.max_attempts:
                    delay = self._calculate_delay(attempt, config)
                    logger.info(f"Waiting {delay:.2f}s before retry {attempt + 1}")
                    await asyncio.sleep(delay)

        result.total_duration = asyncio.get_event_loop().time() - start_time

        # 记录统计信息
        func_name = f"{func.__module__}.{func.__name__}" if hasattr(func, '__module__') else str(func)
        if func_name not in self.retry_stats:
            self.retry_stats[func_name] = {
                "total_calls": 0,
                "success_count": 0,
                "failure_count": 0,
                "total_attempts": 0,
                "avg_attempts": 0.0
            }

        stats = self.retry_stats[func_name]
        stats["total_calls"] += 1
        stats["total_attempts"] += result.attempts

        if result.success:
            stats["success_count"] += 1
        else:
            stats["failure_count"] += 1

        stats["avg_attempts"] = stats["total_attempts"] / stats["total_calls"]

        return result

    def retry_sync(
        self,
        func: Callable[..., T],
        *args,
        config: Optional[RetryConfig] = None,
        **kwargs
    ) -> RetryResult:
        """同步函数重试（简化版）"""
        config = config or RetryConfig()
        start_time = datetime.now()

        result = RetryResult(success=False)

        for attempt in range(1, config.max_attempts + 1):
            result.attempts = attempt

            try:
                func_result = func(*args, **kwargs)
                result.success = True
                result.result = func_result
                break

            except Exception as e:
                result.last_error = e
                result.error_history.append(e)

                logger.warning(
                    f"Attempt {attempt}/{config.max_attempts} failed for {func.__name__}: {e}"
                )

                if not self._should_retry(e, attempt, config.max_attempts):
                    break

                if attempt < config.max_attempts:
                    delay = self._calculate_delay(attempt, config)
                    import time
                    time.sleep(delay)

        result.total_duration = (datetime.now() - start_time).total_seconds()
        return result

    def get_stats(self) -> Dict[str, Any]:
        """获取重试统计信息"""
        return {
            "retry_stats": self.retry_stats.copy(),
            "circuit_breakers": {
                name: {
                    "state": cb.state,
                    "failure_count": cb.failure_count,
                    "last_failure_time": cb.last_failure_time.isoformat() if cb.last_failure_time else None
                }
                for name, cb in self.circuit_breakers.items()
            }
        }


# 全局重试管理器实例
retry_manager = RetryManager()


def with_retry(
    config: Optional[RetryConfig] = None,
    circuit_breaker_name: Optional[str] = None
):
    """重试装饰器"""

    def decorator(func: Callable):
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                result = await retry_manager.retry_async(
                    func, *args,
                    config=config,
                    circuit_breaker_name=circuit_breaker_name,
                    **kwargs
                )
                if not result.success:
                    raise result.last_error
                return result.result

            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                result = retry_manager.retry_sync(func, *args, config=config, **kwargs)
                if not result.success:
                    raise result.last_error
                return result.result

            return sync_wrapper

    return decorator


# 预定义的重试配置
DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=5.0,
    backoff_factor=2.0,
    jitter=True
)

API_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    backoff_factor=2.0,
    jitter=True
)

EXTERNAL_SERVICE_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=2.0,
    max_delay=60.0,
    backoff_factor=1.5,
    jitter=True
)


# 常用的重试装饰器
database_retry = with_retry(DATABASE_RETRY_CONFIG, "database")
api_retry = with_retry(API_RETRY_CONFIG, "api")
external_service_retry = with_retry(EXTERNAL_SERVICE_RETRY_CONFIG, "external_service")


class ErrorHandler:
    """统一错误处理器"""

    def __init__(self):
        self.error_counts: Dict[str, int] = {}
        self.error_history: List[Dict[str, Any]] = []
        self.max_history = 1000

    def handle_error(
        self,
        error: Exception,
        context: Dict[str, Any] = None,
        notify: bool = False
    ) -> Dict[str, Any]:
        """处理错误"""
        error_info = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback.format_exc(),
            "context": context or {}
        }

        # 记录错误历史
        self.error_history.append(error_info)
        if len(self.error_history) > self.max_history:
            self.error_history.pop(0)

        # 统计错误次数
        error_key = f"{type(error).__name__}:{str(error)}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1

        # 记录日志
        logger.error(
            f"Error handled: {type(error).__name__}: {error}",
            extra={"context": context},
            exc_info=True
        )

        # 如果需要通知（例如发送告警）
        if notify:
            self._send_notification(error_info)

        return error_info

    def _send_notification(self, error_info: Dict[str, Any]):
        """发送错误通知（预留接口）"""
        # 这里可以集成邮件、短信、Slack等通知方式
        pass

    def get_error_stats(self) -> Dict[str, Any]:
        """获取错误统计"""
        total_errors = sum(self.error_counts.values())
        top_errors = sorted(
            self.error_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        return {
            "total_errors": total_errors,
            "unique_errors": len(self.error_counts),
            "top_errors": top_errors,
            "recent_errors": self.error_history[-10:] if self.error_history else []
        }

    def clear_stats(self):
        """清空错误统计"""
        self.error_counts.clear()
        self.error_history.clear()


# 全局错误处理器
error_handler = ErrorHandler()


# 便利函数
async def safe_execute(
    func: Callable,
    *args,
    config: Optional[RetryConfig] = None,
    circuit_breaker_name: Optional[str] = None,
    handle_errors: bool = True,
    **kwargs
) -> Tuple[bool, Any, Optional[Exception]]:
    """
    安全执行函数，集成重试和错误处理

    Returns:
        (成功标志, 结果, 错误信息)
    """
    try:
        if asyncio.iscoroutinefunction(func):
            result = await retry_manager.retry_async(
                func, *args,
                config=config,
                circuit_breaker_name=circuit_breaker_name,
                **kwargs
            )
        else:
            result = retry_manager.retry_sync(func, *args, config=config, **kwargs)

        if result.success:
            return True, result.result, None
        else:
            if handle_errors:
                error_handler.handle_error(result.last_error, {
                    "function": func.__name__,
                    "attempts": result.attempts,
                    "duration": result.total_duration
                })
            return False, None, result.last_error

    except Exception as e:
        if handle_errors:
            error_handler.handle_error(e, {
                "function": func.__name__,
                "args": str(args)[:200],
                "kwargs": str(kwargs)[:200]
            })
        return False, None, e