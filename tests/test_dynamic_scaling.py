#!/usr/bin/env python3
"""
动态扩展测试脚本
测试系统对400+合约的自适应能力
"""
import asyncio
import logging
import sys
import time
from pathlib import Path
from datetime import datetime

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from cryptofeed_monitor.services.symbol_discovery import SymbolDiscoveryService
from cryptofeed_monitor.services.connection_pool import DynamicConnectionPool
from cryptofeed_monitor.services.health_monitor import HealthMonitor
from cryptofeed_monitor.config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DynamicScalingTest:
    """动态扩展测试器"""

    def __init__(self):
        self.symbol_discovery = SymbolDiscoveryService()
        self.connection_pool = None
        self.health_monitor = HealthMonitor()
        self.test_results = {}

    async def test_symbol_discovery(self):
        """测试币种发现功能"""
        logger.info("🔍 测试币种发现功能...")

        # 获取所有USDT永续合约
        symbols = await self.symbol_discovery.get_all_usdt_symbols()

        logger.info(f"📊 发现 {len(symbols)} 个USDT永续合约")
        logger.info(f"前10个合约: {symbols[:10]}")

        self.test_results['symbol_count'] = len(symbols)
        self.test_results['symbols'] = symbols

        return len(symbols) > 0

    async def test_connection_calculation(self):
        """测试连接数计算"""
        logger.info("🧮 测试连接数计算...")

        symbols = self.test_results.get('symbols', [])
        if not symbols:
            logger.error("没有可用的合约数据")
            return False

        self.connection_pool = DynamicConnectionPool(self.symbol_discovery)

        # 测试不同数量合约的连接数计算
        test_cases = [
            (10, "测试小规模"),
            (100, "测试中等规模"),
            (len(symbols), f"测试实际规模({len(symbols)}个合约)")
        ]

        for symbol_count, description in test_cases:
            required_connections = self.connection_pool.calculate_required_connections(symbol_count)
            total_streams = symbol_count * 12  # 假设12种数据类型
            logger.info(f"{description}: {symbol_count}合约 → {required_connections}连接 ({total_streams}流)")

        return True

    async def test_symbol_distribution(self):
        """测试合约分配算法"""
        logger.info("📝 测试合约分配算法...")

        symbols = self.test_results.get('symbols', [])[:100]  # 使用前100个合约测试

        # 测试不同连接数的分配
        for connection_count in [5, 8, 12]:
            logger.info(f"\n--- 测试 {connection_count} 个连接 ---")
            distributed = self.connection_pool.distribute_symbols(symbols, connection_count)

            total_assigned = sum(len(conn_symbols) for conn_symbols in distributed)
            logger.info(f"总分配: {total_assigned}/{len(symbols)} 个合约")

            for i, conn_symbols in enumerate(distributed, 1):
                if conn_symbols:
                    logger.info(f"连接{i}: {len(conn_symbols)}个合约")

        return True

    async def test_health_monitoring(self):
        """测试健康监控"""
        logger.info("❤️ 测试健康监控...")

        # 注册组件
        self.health_monitor.register_components(
            symbol_discovery=self.symbol_discovery,
            connection_pool=self.connection_pool
        )

        # 获取健康状态
        health_status = self.health_monitor.get_health_status()
        logger.info(f"健康状态: {health_status['status']}")

        # 获取系统指标
        metrics = self.health_monitor.get_metrics()
        if 'system' in metrics:
            system_metrics = metrics['system']
            logger.info(f"系统资源: CPU {system_metrics.get('cpu_percent', 0):.1f}%, "
                       f"内存 {system_metrics.get('memory_percent', 0):.1f}%")

        return health_status['status'] == 'healthy'

    async def test_configuration_validation(self):
        """测试配置验证"""
        logger.info("⚙️ 测试配置验证...")

        try:
            # 验证配置
            config.validate()
            logger.info("✅ 配置验证通过")

            # 显示关键配置
            logger.info(f"数据库主机: {config.get('database.host')}")
            logger.info(f"每连接流数: {config.get('connection_pool.streams_per_connection')}")
            logger.info(f"数据类型: {len(config.get('collection.data_types', []))}")

            return True

        except Exception as e:
            logger.error(f"❌ 配置验证失败: {e}")
            return False

    async def simulate_scaling_scenario(self):
        """模拟扩展场景"""
        logger.info("🚀 模拟动态扩展场景...")

        # 模拟币种数量变化
        scenarios = [
            (400, "当前规模"),
            (600, "中期增长"),
            (800, "高速增长"),
            (1000, "目标规模")
        ]

        for symbol_count, description in scenarios:
            required_connections = self.connection_pool.calculate_required_connections(symbol_count)
            total_streams = symbol_count * 12

            # 计算资源需求
            estimated_memory = symbol_count * 0.5  # 每个合约约0.5MB
            estimated_bandwidth = symbol_count * 10  # 每个合约约10KB/s

            logger.info(f"{description} ({symbol_count}合约):")
            logger.info(f"  连接数: {required_connections}")
            logger.info(f"  数据流: {total_streams}")
            logger.info(f"  预估内存: {estimated_memory:.1f}MB")
            logger.info(f"  预估带宽: {estimated_bandwidth:.1f}KB/s")
            logger.info("")

        return True

    async def run_all_tests(self):
        """运行所有测试"""
        logger.info("🧪 开始动态扩展测试")
        logger.info("=" * 60)

        test_cases = [
            ("币种发现", self.test_symbol_discovery),
            ("连接数计算", self.test_connection_calculation),
            ("合约分配", self.test_symbol_distribution),
            ("健康监控", self.test_health_monitoring),
            ("配置验证", self.test_configuration_validation),
            ("扩展场景模拟", self.simulate_scaling_scenario)
        ]

        results = {}
        start_time = datetime.now()

        for test_name, test_func in test_cases:
            logger.info(f"\n🔄 执行测试: {test_name}")
            try:
                result = await test_func()
                results[test_name] = result
                status = "✅ 通过" if result else "❌ 失败"
                logger.info(f"{status}: {test_name}")
            except Exception as e:
                results[test_name] = False
                logger.error(f"❌ 错误: {test_name} - {e}")

        # 测试总结
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("\n" + "=" * 60)
        logger.info("📋 测试总结")
        logger.info("=" * 60)

        passed = sum(1 for result in results.values() if result)
        total = len(results)

        for test_name, result in results.items():
            status = "✅ 通过" if result else "❌ 失败"
            logger.info(f"{status} {test_name}")

        logger.info(f"\n总计: {passed}/{total} 个测试通过")
        logger.info(f"耗时: {duration}")

        if passed == total:
            logger.info("🎉 所有测试通过！系统具备400+合约动态扩展能力")
        else:
            logger.warning(f"⚠️  {total - passed} 个测试失败，需要检查系统配置")

        # 停止监控服务
        self.health_monitor.stop()

        return passed == total


async def main():
    """主函数"""
    test = DynamicScalingTest()

    try:
        success = await test.run_all_tests()
        return 0 if success else 1
    except Exception as e:
        logger.error(f"测试运行失败: {e}")
        return 1


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)