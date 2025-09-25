"""
API限流和速率控制
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass
from collections import deque, defaultdict
import logging

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

logger = logging.getLogger(__name__)


@dataclass
class RateLimit:
    """限流配置"""
    requests: int  # 请求数量
    window: int    # 时间窗口（秒）
    burst: Optional[int] = None  # 突发请求数量


@dataclass
class RateLimitStatus:
    """限流状态"""
    allowed: bool
    remaining: int
    reset_time: float
    retry_after: Optional[int] = None


class TokenBucket:
    """令牌桶算法实现"""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.tokens = float(capacity)
        self.refill_rate = refill_rate
        self.last_refill = time.time()

    def consume(self, tokens: int = 1) -> bool:
        """消费令牌"""
        now = time.time()
        self._refill(now)

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def _refill(self, now: float):
        """补充令牌"""
        time_passed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + time_passed * self.refill_rate)
        self.last_refill = now

    def get_status(self) -> Dict[str, float]:
        """获取桶状态"""
        self._refill(time.time())
        return {
            "tokens": self.tokens,
            "capacity": self.capacity,
            "refill_rate": self.refill_rate
        }


class SlidingWindowCounter:
    """滑动窗口计数器"""

    def __init__(self, window_size: int, max_requests: int):
        self.window_size = window_size
        self.max_requests = max_requests
        self.requests = deque()

    def is_allowed(self) -> Tuple[bool, int]:
        """检查是否允许请求"""
        now = time.time()
        self._cleanup_old_requests(now)

        if len(self.requests) < self.max_requests:
            self.requests.append(now)
            return True, self.max_requests - len(self.requests)

        return False, 0

    def _cleanup_old_requests(self, now: float):
        """清理过期请求记录"""
        cutoff = now - self.window_size
        while self.requests and self.requests[0] <= cutoff:
            self.requests.popleft()

    def get_reset_time(self) -> float:
        """获取重置时间"""
        if not self.requests:
            return time.time()
        return self.requests[0] + self.window_size

    def get_status(self) -> Dict[str, Any]:
        """获取窗口状态"""
        now = time.time()
        self._cleanup_old_requests(now)

        return {
            "current_requests": len(self.requests),
            "max_requests": self.max_requests,
            "window_size": self.window_size,
            "reset_time": self.get_reset_time()
        }


class RateLimiter:
    """速率限制器"""

    def __init__(self):
        self.limiters: Dict[str, SlidingWindowCounter] = {}
        self.token_buckets: Dict[str, TokenBucket] = {}
        self.rate_limits: Dict[str, RateLimit] = {}
        self.blocked_ips: Dict[str, float] = {}  # IP -> 解封时间
        self.request_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def set_rate_limit(self, key: str, rate_limit: RateLimit):
        """设置速率限制"""
        self.rate_limits[key] = rate_limit

        # 创建滑动窗口计数器
        self.limiters[key] = SlidingWindowCounter(
            window_size=rate_limit.window,
            max_requests=rate_limit.requests
        )

        # 如果有突发限制，创建令牌桶
        if rate_limit.burst:
            self.token_buckets[key] = TokenBucket(
                capacity=rate_limit.burst,
                refill_rate=rate_limit.requests / rate_limit.window
            )

    def check_rate_limit(self, key: str, client_ip: str = None) -> RateLimitStatus:
        """检查速率限制"""
        # 检查IP是否被阻塞
        if client_ip and client_ip in self.blocked_ips:
            if time.time() < self.blocked_ips[client_ip]:
                return RateLimitStatus(
                    allowed=False,
                    remaining=0,
                    reset_time=self.blocked_ips[client_ip],
                    retry_after=int(self.blocked_ips[client_ip] - time.time())
                )
            else:
                # 解封IP
                del self.blocked_ips[client_ip]

        # 检查基本速率限制
        if key not in self.limiters:
            return RateLimitStatus(allowed=True, remaining=999, reset_time=time.time())

        limiter = self.limiters[key]
        allowed, remaining = limiter.is_allowed()
        reset_time = limiter.get_reset_time()

        # 如果基本限制不通过
        if not allowed:
            # 记录统计
            self.request_stats[key]["rejected"] += 1

            return RateLimitStatus(
                allowed=False,
                remaining=0,
                reset_time=reset_time,
                retry_after=int(reset_time - time.time())
            )

        # 检查令牌桶（突发限制）
        if key in self.token_buckets:
            bucket = self.token_buckets[key]
            if not bucket.consume(1):
                self.request_stats[key]["burst_rejected"] += 1
                return RateLimitStatus(
                    allowed=False,
                    remaining=0,
                    reset_time=reset_time,
                    retry_after=1  # 令牌桶通常很快恢复
                )

        # 记录允许的请求
        self.request_stats[key]["allowed"] += 1

        return RateLimitStatus(
            allowed=True,
            remaining=remaining,
            reset_time=reset_time
        )

    def block_ip(self, ip: str, duration: int = 3600):
        """阻塞IP地址"""
        self.blocked_ips[ip] = time.time() + duration
        logger.warning(f"Blocked IP {ip} for {duration} seconds")

    def unblock_ip(self, ip: str):
        """解封IP地址"""
        if ip in self.blocked_ips:
            del self.blocked_ips[ip]
            logger.info(f"Unblocked IP {ip}")

    def get_stats(self) -> Dict[str, Any]:
        """获取限流统计信息"""
        stats = {
            "rate_limits": {},
            "blocked_ips": len(self.blocked_ips),
            "request_stats": dict(self.request_stats)
        }

        for key in self.limiters:
            limiter_stats = self.limiters[key].get_status()
            bucket_stats = None

            if key in self.token_buckets:
                bucket_stats = self.token_buckets[key].get_status()

            stats["rate_limits"][key] = {
                "limiter": limiter_stats,
                "bucket": bucket_stats,
                "config": {
                    "requests": self.rate_limits[key].requests,
                    "window": self.rate_limits[key].window,
                    "burst": self.rate_limits[key].burst
                }
            }

        return stats

    def cleanup_expired(self):
        """清理过期数据"""
        now = time.time()

        # 清理过期的IP阻塞
        expired_ips = [ip for ip, unblock_time in self.blocked_ips.items() if now >= unblock_time]
        for ip in expired_ips:
            del self.blocked_ips[ip]


# 全局限流器实例
rate_limiter = RateLimiter()

# 预定义的限流配置
DEFAULT_RATE_LIMITS = {
    "default": RateLimit(requests=100, window=60, burst=20),  # 100请求/分钟，突发20
    "api_key": RateLimit(requests=1000, window=60, burst=100),  # 有API key的用户
    "premium": RateLimit(requests=5000, window=60, burst=500),  # 高级用户
    "candles": RateLimit(requests=200, window=60, burst=50),  # K线数据端点
    "trades": RateLimit(requests=100, window=60, burst=20),   # 交易数据端点
    "funding": RateLimit(requests=50, window=60, burst=10),   # 资金费率端点
}

# 初始化默认限流规则
for key, limit in DEFAULT_RATE_LIMITS.items():
    rate_limiter.set_rate_limit(key, limit)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """FastAPI限流中间件"""

    def __init__(self, app, limiter: RateLimiter = None):
        super().__init__(app)
        self.limiter = limiter or rate_limiter

    def get_client_ip(self, request: Request) -> str:
        """获取客户端IP"""
        # 检查代理头
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        # 回退到直接连接IP
        return request.client.host if request.client else "unknown"

    def get_rate_limit_key(self, request: Request) -> str:
        """获取限流键"""
        # 检查是否有API密钥
        api_key = request.headers.get("x-api-key") or request.query_params.get("api_key")
        if api_key:
            # 这里可以根据API密钥查询用户等级
            return "api_key"  # 简化处理

        # 根据路径确定限流类型
        path = request.url.path
        if "/candles" in path:
            return "candles"
        elif "/trades" in path:
            return "trades"
        elif "/funding" in path:
            return "funding"

        return "default"

    async def dispatch(self, request: Request, call_next) -> Response:
        """处理请求"""
        client_ip = self.get_client_ip(request)
        rate_limit_key = self.get_rate_limit_key(request)

        # 检查速率限制
        status = self.limiter.check_rate_limit(rate_limit_key, client_ip)

        if not status.allowed:
            # 返回429状态码
            headers = {
                "X-RateLimit-Limit": str(self.limiter.rate_limits.get(rate_limit_key, RateLimit(0, 0)).requests),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(int(status.reset_time))
            }

            if status.retry_after:
                headers["Retry-After"] = str(status.retry_after)

            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers=headers
            )

        # 处理请求
        response = await call_next(request)

        # 添加限流信息到响应头
        rate_limit = self.limiter.rate_limits.get(rate_limit_key, RateLimit(999, 60))
        response.headers["X-RateLimit-Limit"] = str(rate_limit.requests)
        response.headers["X-RateLimit-Remaining"] = str(status.remaining)
        response.headers["X-RateLimit-Reset"] = str(int(status.reset_time))

        return response


# 装饰器形式的限流器
def rate_limit(key: str = "default"):
    """限流装饰器"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # 这里可以从请求上下文获取IP等信息
            status = rate_limiter.check_rate_limit(key)

            if not status.allowed:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded",
                    headers={"Retry-After": str(status.retry_after or 60)}
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator


# 管理功能
async def get_rate_limit_stats() -> Dict[str, Any]:
    """获取限流统计信息"""
    return rate_limiter.get_stats()


async def block_client_ip(ip: str, duration: int = 3600) -> bool:
    """阻塞客户端IP"""
    try:
        rate_limiter.block_ip(ip, duration)
        return True
    except Exception as e:
        logger.error(f"Failed to block IP {ip}: {e}")
        return False


async def unblock_client_ip(ip: str) -> bool:
    """解封客户端IP"""
    try:
        rate_limiter.unblock_ip(ip)
        return True
    except Exception as e:
        logger.error(f"Failed to unblock IP {ip}: {e}")
        return False


# 清理任务
async def cleanup_rate_limiter():
    """清理限流器过期数据"""
    while True:
        try:
            rate_limiter.cleanup_expired()
            await asyncio.sleep(300)  # 每5分钟清理一次
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error cleaning up rate limiter: {e}")
            await asyncio.sleep(60)