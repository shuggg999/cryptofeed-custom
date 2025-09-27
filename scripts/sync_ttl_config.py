#!/usr/bin/env python3
"""
ClickHouse TTL 配置自动同步脚本
启动时自动检查并同步YAML配置到ClickHouse TTL设置
确保数据保留策略、回填天数、TTL设置三者保持一致

用法:
1. Docker启动时自动运行: python scripts/sync_ttl_config.py
2. 手动运行: python scripts/sync_ttl_config.py --config config/main.yaml
"""

import sys
import os
import yaml
import clickhouse_connect
import logging
import argparse
from pathlib import Path

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [TTL同步] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(config_path):
    """加载YAML配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"✅ 配置文件加载成功: {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"❌ 配置文件不存在: {config_path}")
        return None
    except yaml.YAMLError as e:
        logger.error(f"❌ YAML格式错误: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ 配置文件加载失败: {e}")
        return None

def get_clickhouse_client(config):
    """创建ClickHouse客户端连接"""
    try:
        clickhouse_cfg = config['clickhouse']
        client = clickhouse_connect.get_client(
            host=clickhouse_cfg['host'],
            port=clickhouse_cfg['port'],
            username=clickhouse_cfg['user'],
            password=clickhouse_cfg.get('password', ''),
            database=clickhouse_cfg['database'],
            secure=clickhouse_cfg.get('secure', False)
        )
        logger.info(f"✅ ClickHouse连接成功: {clickhouse_cfg['host']}:{clickhouse_cfg['port']}")
        return client
    except Exception as e:
        logger.error(f"❌ ClickHouse连接失败: {e}")
        return None

def get_current_ttl(client):
    """获取当前candles表的TTL设置"""
    try:
        result = client.query("SHOW CREATE TABLE candles")
        create_sql = result.result_rows[0][0] if result.result_rows else ""
        return create_sql
    except Exception as e:
        logger.error(f"❌ 获取当前TTL失败: {e}")
        return ""

def build_ttl_sql(candles_retention):
    """根据配置构建TTL SQL语句"""
    if not candles_retention:
        logger.warning("⚠️  数据保留配置为空，使用默认值")
        candles_retention = {
            '1m': 30, '5m': 90, '30m': 365,
            '4h': 730, '1d': 1095
        }

    # 构建CASE语句的各个分支
    case_parts = []
    for interval, days in sorted(candles_retention.items()):
        case_parts.append(f"        WHEN interval = '{interval}' THEN toIntervalDay({days})")

    # 添加默认值
    case_parts.append("        ELSE toIntervalDay(365)")

    # 完整的TTL SQL
    ttl_sql = f"""ALTER TABLE candles MODIFY TTL
    toDateTime(timestamp) + CASE
{chr(10).join(case_parts)}
    END"""

    return ttl_sql

def sync_ttl_settings(client, config):
    """同步TTL设置到ClickHouse"""
    try:
        # 获取数据保留配置
        retention_config = config.get('data_retention', {})
        candles_retention = retention_config.get('candles', {})

        if not candles_retention:
            logger.warning("⚠️  未找到candles保留配置，跳过TTL同步")
            return False

        logger.info("🔧 开始同步TTL设置...")
        logger.info(f"📋 当前配置: {candles_retention}")

        # 构建TTL SQL
        ttl_sql = build_ttl_sql(candles_retention)
        logger.info(f"🔨 执行SQL: {ttl_sql}")

        # 执行TTL修改
        client.command(ttl_sql)
        logger.info("✅ TTL设置更新成功")

        # 强制触发TTL清理（可选）
        logger.info("🧹 触发TTL清理优化...")
        client.command("OPTIMIZE TABLE candles FINAL")
        logger.info("✅ TTL清理触发完成")

        return True

    except Exception as e:
        logger.error(f"❌ TTL同步失败: {e}")
        return False

def verify_config_consistency(config):
    """验证配置一致性"""
    logger.info("🔍 检查配置一致性...")

    retention_config = config.get('data_retention', {})
    candles_retention = retention_config.get('candles', {})

    if not candles_retention:
        logger.warning("⚠️  数据保留配置缺失")
        return False

    # 验证配置完整性
    required_intervals = ['1m', '5m', '30m', '4h', '1d']
    missing_intervals = [interval for interval in required_intervals
                        if interval not in candles_retention]

    if missing_intervals:
        logger.warning(f"⚠️  缺少时间间隔配置: {missing_intervals}")

    # 显示当前配置
    logger.info("📋 统一数据保留策略:")
    for interval, days in sorted(candles_retention.items()):
        logger.info(f"   {interval}: {days}天")

    return True

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='ClickHouse TTL配置同步')
    parser.add_argument(
        '--config',
        default='/app/config/main.yaml',
        help='配置文件路径 (默认: /app/config/main.yaml)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='只检查配置，不执行同步'
    )

    args = parser.parse_args()

    logger.info("🚀 开始ClickHouse TTL配置同步...")

    # 加载配置
    config = load_config(args.config)
    if not config:
        sys.exit(1)

    # 验证配置
    if not verify_config_consistency(config):
        logger.error("❌ 配置验证失败")
        sys.exit(1)

    if args.dry_run:
        logger.info("✅ 配置检查完成 (--dry-run模式，未执行同步)")
        return

    # 连接ClickHouse
    client = get_clickhouse_client(config)
    if not client:
        sys.exit(1)

    try:
        # 获取当前TTL（用于对比）
        current_ttl = get_current_ttl(client)
        if current_ttl:
            logger.info("📋 当前表结构已获取")

        # 同步TTL设置
        if sync_ttl_settings(client, config):
            logger.info("🎉 TTL配置同步成功！")
        else:
            logger.error("💥 TTL配置同步失败！")
            sys.exit(1)

    finally:
        client.close()

if __name__ == "__main__":
    main()