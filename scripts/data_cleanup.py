#!/usr/bin/env python3
"""
数据清理工具
按不同数据类型的保留策略清理历史数据
"""
import psycopg2
import logging
from datetime import datetime, timedelta
import argparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL配置
postgres_cfg = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'db': 'cryptofeed',
    'pw': 'password'
}

class DataCleaner:
    """数据清理器"""

    def __init__(self):
        self.conn = None

    def connect(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(
                host=postgres_cfg['host'],
                user=postgres_cfg['user'],
                password=postgres_cfg['pw'],
                database=postgres_cfg['db']
            )
            logger.info("✅ 数据库连接成功")
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            raise

    def cleanup_trades(self, days=30):
        """清理交易数据 - 保留指定天数"""
        try:
            cursor = self.conn.cursor()

            # 计算删除时间点
            cutoff_date = datetime.now() - timedelta(days=days)

            cursor.execute("""
                DELETE FROM trades
                WHERE timestamp < %s
            """, (cutoff_date,))

            deleted_count = cursor.rowcount
            self.conn.commit()

            logger.info(f"🧹 交易数据清理完成: 删除了 {deleted_count} 条 {days} 天前的记录")

        except Exception as e:
            logger.error(f"❌ 交易数据清理失败: {e}")
            self.conn.rollback()

    def cleanup_candles_by_interval(self, interval, days):
        """按时间周期清理K线数据"""
        try:
            cursor = self.conn.cursor()
            table_name = f'candles_{interval}'

            # 计算删除时间点
            cutoff_date = datetime.now() - timedelta(days=days)

            cursor.execute(f"""
                DELETE FROM {table_name}
                WHERE timestamp < %s
            """, (cutoff_date,))

            deleted_count = cursor.rowcount
            self.conn.commit()

            logger.info(f"🧹 {interval} K线清理完成: 删除了 {deleted_count} 条 {days} 天前的记录")

        except Exception as e:
            logger.error(f"❌ {interval} K线清理失败: {e}")
            self.conn.rollback()

    def cleanup_all_candles(self):
        """按策略清理所有K线数据"""
        # K线保留策略
        candle_policies = {
            '1m': 90,    # 1分钟线保留3个月
            '5m': 180,   # 5分钟线保留6个月
            '30m': 365,  # 30分钟线保留1年
            '4h': 730,   # 4小时线保留2年
            '1d': None   # 日线永久保留
        }

        for interval, days in candle_policies.items():
            if days is not None:
                self.cleanup_candles_by_interval(interval, days)
            else:
                logger.info(f"📊 {interval} K线数据永久保留，跳过清理")

    def cleanup_ticker(self):
        """清理Ticker表 - 只保留每个合约的最新记录"""
        try:
            cursor = self.conn.cursor()

            # 删除旧的ticker记录，只保留最新的
            cursor.execute("""
                DELETE FROM ticker t1
                WHERE EXISTS (
                    SELECT 1 FROM ticker t2
                    WHERE t2.symbol = t1.symbol
                    AND t2.exchange = t1.exchange
                    AND t2.timestamp > t1.timestamp
                )
            """)

            deleted_count = cursor.rowcount
            self.conn.commit()

            logger.info(f"🧹 Ticker清理完成: 删除了 {deleted_count} 条旧记录")

        except Exception as e:
            logger.error(f"❌ Ticker清理失败: {e}")
            self.conn.rollback()

    def get_table_stats(self):
        """获取表统计信息"""
        try:
            cursor = self.conn.cursor()

            tables = ['trades', 'candles_1m', 'candles_5m', 'candles_30m', 'candles_4h', 'candles_1d', 'funding', 'ticker']

            logger.info("📊 数据表统计信息:")
            logger.info("-" * 80)

            for table in tables:
                try:
                    # 获取记录数
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]

                    # 获取表大小
                    cursor.execute("""
                        SELECT pg_size_pretty(pg_total_relation_size(%s))
                    """, (table,))
                    size = cursor.fetchone()[0]

                    # 获取最早和最新时间
                    cursor.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}")
                    min_time, max_time = cursor.fetchone()

                    logger.info(f"{table:15} | {count:>10,} 条 | {size:>10} | {min_time} ~ {max_time}")

                except Exception as e:
                    logger.warning(f"{table:15} | 查询失败: {e}")

            logger.info("-" * 80)

        except Exception as e:
            logger.error(f"❌ 获取统计信息失败: {e}")

    def vacuum_analyze(self):
        """执行表维护操作"""
        try:
            # 需要新连接来执行VACUUM（不能在事务中）
            self.conn.close()
            self.connect()
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            cursor = self.conn.cursor()
            tables = ['trades', 'candles_1m', 'candles_5m', 'candles_30m', 'candles_4h', 'candles_1d', 'funding', 'ticker']

            for table in tables:
                try:
                    logger.info(f"🔧 维护表: {table}")
                    cursor.execute(f"VACUUM ANALYZE {table}")
                except Exception as e:
                    logger.warning(f"⚠️  表 {table} 维护失败: {e}")

            logger.info("✅ 表维护完成")

        except Exception as e:
            logger.error(f"❌ 表维护失败: {e}")

    def full_cleanup(self):
        """执行完整清理"""
        logger.info("🧹 开始数据清理...")

        # 清理前统计
        logger.info("📊 清理前状态:")
        self.get_table_stats()

        # 执行清理
        self.cleanup_trades(days=30)  # 交易数据保留30天
        self.cleanup_all_candles()   # K线按策略清理
        self.cleanup_ticker()        # Ticker只保留最新

        # 表维护
        self.vacuum_analyze()

        # 清理后统计
        logger.info("📊 清理后状态:")
        self.get_table_stats()

        logger.info("✅ 数据清理完成")

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()

def main():
    parser = argparse.ArgumentParser(description='Cryptofeed数据清理工具')
    parser.add_argument('--action', choices=['full', 'trades', 'candles', 'ticker', 'stats'],
                       default='stats', help='清理操作类型')
    parser.add_argument('--days', type=int, default=30, help='交易数据保留天数')

    args = parser.parse_args()

    cleaner = DataCleaner()

    try:
        cleaner.connect()

        if args.action == 'full':
            cleaner.full_cleanup()
        elif args.action == 'trades':
            cleaner.cleanup_trades(args.days)
        elif args.action == 'candles':
            cleaner.cleanup_all_candles()
        elif args.action == 'ticker':
            cleaner.cleanup_ticker()
        elif args.action == 'stats':
            cleaner.get_table_stats()

    finally:
        cleaner.close()

if __name__ == '__main__':
    main()