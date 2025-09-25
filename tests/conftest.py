"""
Pytest 配置和共享 fixtures
"""
import asyncio
import os
import pytest
import pytest_asyncio
from typing import AsyncGenerator
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from cryptofeed_api.main import app
from cryptofeed_api.core.database import get_db_session
from cryptofeed_api.models.database import Base


# 测试数据库URL
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:password@127.0.0.1:5432/cryptofeed_test"
)


@pytest.fixture(scope="session")
def event_loop():
    """创建一个会话级别的事件循环"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def test_engine():
    """创建测试数据库引擎"""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def test_db_setup(test_engine):
    """设置测试数据库"""
    # 创建所有表
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    # 清理：删除所有表
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def test_db_session(test_engine, test_db_setup) -> AsyncGenerator[AsyncSession, None]:
    """创建测试数据库会话"""
    async_session = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    async with async_session() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def client(test_db_session) -> AsyncGenerator[AsyncClient, None]:
    """创建测试HTTP客户端"""

    # 覆盖数据库依赖
    async def override_get_db():
        yield test_db_session

    app.dependency_overrides[get_db_session] = override_get_db

    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

    # 清理依赖覆盖
    app.dependency_overrides.clear()


@pytest.fixture
def sample_trade_data():
    """示例交易数据"""
    from datetime import datetime
    from decimal import Decimal

    return {
        "timestamp": datetime.now(),
        "symbol": "BTC-USDT-PERP",
        "side": "buy",
        "amount": Decimal("0.1"),
        "price": Decimal("50000.00"),
        "exchange": "binance",
        "trade_id": "test_trade_1"
    }


@pytest.fixture
def sample_candle_data():
    """示例K线数据"""
    from datetime import datetime
    from decimal import Decimal

    return {
        "timestamp": datetime.now(),
        "symbol": "BTC-USDT-PERP",
        "interval": "1m",
        "open_price": Decimal("49900.00"),
        "high_price": Decimal("50100.00"),
        "low_price": Decimal("49800.00"),
        "close_price": Decimal("50000.00"),
        "volume": Decimal("10.5"),
        "exchange": "binance"
    }


@pytest.fixture
def sample_funding_data():
    """示例资金费率数据"""
    from datetime import datetime
    from decimal import Decimal

    return {
        "timestamp": datetime.now(),
        "symbol": "BTC-USDT-PERP",
        "rate": Decimal("0.0001"),
        "exchange": "binance"
    }


# 跳过集成测试的标记
slow = pytest.mark.slow
integration = pytest.mark.integration
unit = pytest.mark.unit