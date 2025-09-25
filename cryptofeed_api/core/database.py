"""
数据库连接和会话管理
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

from ..models.database import Base
from .config import config_manager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """数据库管理器"""

    def __init__(self):
        self.engine = None
        self.session_factory = None
        self._initialized = False

    async def initialize(self) -> None:
        """初始化数据库连接"""
        if self._initialized:
            return

        try:
            # 创建异步引擎
            database_url = config_manager.database_url
            self.engine = create_async_engine(
                database_url,
                echo=config_manager.settings.debug,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=300
            )

            # 创建会话工厂
            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # 测试连接
            await self.test_connection()

            self._initialized = True
            logger.info("Database initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    async def create_tables(self) -> None:
        """创建表结构"""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """获取数据库会话上下文管理器"""
        if not self._initialized:
            await self.initialize()

        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(f"Database session error: {e}")
                raise
            except Exception as e:
                await session.rollback()
                logger.error(f"Unexpected error in database session: {e}")
                raise

    async def close(self) -> None:
        """关闭数据库连接"""
        if self.engine:
            await self.engine.dispose()
            logger.info("Database connections closed")


# 全局数据库管理器实例
db_manager = DatabaseManager()


# FastAPI依赖注入函数
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI依赖注入：获取数据库会话"""
    async with db_manager.get_session() as session:
        yield session


# 数据库健康检查
async def check_database_health() -> dict:
    """检查数据库健康状态"""
    try:
        is_connected = await db_manager.test_connection()

        if is_connected:
            # 获取基本统计信息
            async with db_manager.get_session() as session:
                # 检查表是否存在
                result = await session.execute(text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name IN ('trades', 'candles', 'funding', 'tickers')
                """))
                tables = [row[0] for row in result]

                # 获取数据统计
                stats = {}
                for table in tables:
                    count_result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    stats[f"{table}_count"] = count_result.scalar()

        return {
            "connected": is_connected,
            "tables": tables if is_connected else [],
            "stats": stats if is_connected else {}
        }

    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return {
            "connected": False,
            "error": str(e),
            "tables": [],
            "stats": {}
        }


# 数据库初始化函数
async def init_database():
    """初始化数据库（创建表等）"""
    try:
        await db_manager.initialize()
        await db_manager.create_tables()
        logger.info("Database initialization completed")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


# 数据库清理函数
async def cleanup_database():
    """清理数据库连接"""
    await db_manager.close()