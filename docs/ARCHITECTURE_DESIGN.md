# Cryptofeed 全量数据采集架构设计

## 📋 项目概述

个人量化交易数据采集系统，动态监控Binance全部USDT永续合约（当前400+个，自动适应增长），存储14种数据类型，为freqtrade策略提供数据支撑。

## 🎯 核心需求

- **覆盖范围**: 全部USDT永续合约（动态适应新币上市/下架）
- **数据类型**: 14种数据全量采集，无分级
- **可靠性**: 100%数据完整性，支持断线重连
- **性能要求**: 全速率运行，低延迟（1-3秒）
- **扩展性**: 自动适应币种数量变化（400→500→1000+）
- **集成需求**: freqtrade直接查询PostgreSQL

## 🏗️ 技术架构

### 简洁自适应架构图

```
    币种发现服务（5分钟检查一次）
                │
                ▼
    动态连接池（自动计算N个连接）
                │
    ┌───────────┼───────────┐
    │           │           │
  连接1        连接2   ...   连接N
 (均匀分配)   (均匀分配)    (均匀分配)
    │           │           │
    └───────────┼───────────┘
                │
         批量写入14个表
                │
          PostgreSQL/TimescaleDB
```

### 动态连接池设计

```yaml
自动伸缩策略:
  基础配置:
    streams_per_connection: 1000      # 每连接最大流数
    symbol_check_interval: 300       # 5分钟检查新币

  自动计算:
    # connections = ceil(币种数 × 14流 / 1000)
    # 示例：400币 × 14流 = 5600流 → 6个连接
    #      800币 × 14流 = 11200流 → 12个连接
    #      1000币 × 14流 = 14000流 → 14个连接

  自动触发:
    - 新币上市自动添加连接
    - 币种下架自动减少连接
    - 连接失败自动补充
```

## 📊 数据流设计

### 全量数据采集

所有USDT永续合约均采集全部14种数据类型，无差别对待：

| 数据类型 | 数据表 | 采集状态 | 更新频率 |
|---------|--------|---------|----------|
| 实时成交 | trades | ✅ | 实时 |
| K线-1分钟 | candles_1m | ✅ | 每分钟 |
| K线-5分钟 | candles_5m | ✅ | 每5分钟 |
| K线-30分钟 | candles_30m | ✅ | 每30分钟 |
| K线-4小时 | candles_4h | ✅ | 每4小时 |
| K线-1天 | candles_1d | ✅ | 每天 |
| 24h行情 | ticker | ✅ | 实时 |
| 资金费率 | funding | ✅ | 每8小时 |
| 订单簿 | l2_book | ✅ | 实时 |
| 爆仓数据 | liquidations | ✅ | 事件驱动 |
| 持仓量 | open_interest | ✅ | 每5秒 |
| 指数价格 | index | ✅ | 实时 |
| 自定义K线 | custom_candles | ✅ | 按需 |

### 批量写入策略

```yaml
写入批次配置:
  trades:
    batch_size: 1000
    batch_timeout: 1秒

  l2_book:
    batch_size: 500
    batch_timeout: 2秒

  candles_*:
    batch_size: 100
    batch_timeout: 5秒

  其他:
    batch_size: 50
    batch_timeout: 10秒
```

## 🚀 核心架构特点

### 配置驱动设计

```yaml
# config/main.yaml
collection:
  # 简单配置，全量采集
  data_types:
    - trades
    - candles_1m
    - candles_5m
    - candles_30m
    - candles_4h
    - candles_1d
    - ticker
    - funding
    - l2_book
    - liquidations
    - open_interest
    - index

connection_pool:
  streams_per_connection: 1000
  auto_scaling:
    enabled: true
    symbol_check_interval: 300  # 5分钟检查新币

database:
  host: 127.0.0.1
  user: postgres
  password: password
  database: cryptofeed
```

### 动态服务发现

```python
class SymbolDiscoveryService:
    """币种发现服务 - 简化版"""

    async def discover_new_symbols(self):
        """每5分钟检查新币"""
        current_symbols = await self.get_all_usdt_symbols()
        new_symbols = set(current_symbols) - set(self.known_symbols)

        if new_symbols:
            logger.info(f"发现新币种: {new_symbols}")
            await self.trigger_rebalance()

    async def get_all_usdt_symbols(self):
        """获取所有USDT永续合约"""
        all_symbols = BinanceFutures.symbols()
        return [s for s in all_symbols if s.endswith('-USDT-PERP')]
```

### 动态连接池管理

```python
class DynamicConnectionPool:
    """自适应连接池 - 简化版"""

    async def calculate_required_connections(self):
        """计算需要的连接数"""
        symbol_count = len(await self.get_all_symbols())
        total_streams = symbol_count * 14  # 14种数据类型
        return math.ceil(total_streams / self.streams_per_connection)

    async def auto_scale(self):
        """根据币种数量自动伸缩"""
        required = await self.calculate_required_connections()
        current = len(self.active_connections)

        if required > current:
            await self.add_connections(required - current)
        elif required < current:
            await self.remove_connections(current - required)
```

## 🔒 数据完整性保证

### 简化保护机制

1. **WAL日志**: 进程崩溃前的数据持久化
2. **事务写入**: 批量数据原子性保证
3. **自动重连**: WebSocket断线自动恢复
4. **监控告警**: 异常实时通知

### 故障恢复流程

```yaml
故障场景处理:
  网络中断:
    - 自动重连机制
    - 从WAL恢复丢失数据

  进程崩溃:
    - systemd自动重启
    - WAL恢复未写入数据

  数据库异常:
    - 本地队列缓存
    - 重试写入机制

  连接不足:
    - 自动增加连接数
    - 确保全量数据采集
```

## 🗑️ 数据生命周期管理

### 保留策略

| 数据类型 | 保留期限 | 清理原因 |
|---------|---------|---------|
| trades | 7天 | 数据量大，分析价值递减 |
| l2_book | 3天 | 极高频，历史价值低 |
| ticker | 1天 | 快照数据，无累积价值 |
| candles_1m | 30天 | 可聚合为大周期 |
| candles_5m | 90天 | 中期策略需要 |
| candles_30m | 180天 | 长期趋势分析 |
| candles_4h | 1年 | 重要支撑阻力 |
| candles_1d | 永久 | 历史回测必需 |
| funding | 90天 | 套利分析周期 |
| liquidations | 30天 | 事件分析 |
| open_interest | 90天 | 持仓趋势 |
| index | 90天 | 价格偏差分析 |

### 自动清理任务

```yaml
清理调度:
- 执行时间: 每天凌晨3点
- 清理方式: DELETE + VACUUM
- 性能优化: 分批删除，避免锁表
```

## 💻 技术栈

- **语言**: Python 3.11
- **异步框架**: AsyncIO + aiohttp
- **WebSocket**: cryptofeed库
- **数据库**: PostgreSQL 15 + TimescaleDB
- **进程管理**: systemd/Docker
- **监控**: Prometheus + Grafana（可选）

## 📈 性能指标

### 资源占用预估

```yaml
内存使用: 200-250MB
- Python进程: 80MB
- 6个WebSocket: 60MB
- 队列缓冲: 50MB
- 其他开销: 30MB

CPU使用: 15-25%（单核）
网络带宽: 5-10Mbps
磁盘I/O: 批量写入优化

数据延迟:
- 写入延迟: 1-3秒
- 查询延迟: <100ms
```

### 扩展能力

- **水平扩展**: 可拆分为多实例
- **垂直扩展**: 支持更多合约
- **多交易所**: 架构支持扩展

## 🚀 部署方案

### Docker Compose配置

```yaml
version: '3.8'

services:
  collector:
    build: ./collector
    environment:
      - DB_HOST=timescaledb
      - CONNECTION_COUNT=6
      - SYMBOLS_PER_CONNECTION=85
    depends_on:
      - timescaledb
    restart: always

  timescaledb:
    image: timescale/timescaledb:latest-pg15
    volumes:
      - ./data:/var/lib/postgresql/data
    environment:
      - POSTGRES_PASSWORD=password
```

### 监控接入

- 健康检查端点: `http://localhost:8080/health`
- Metrics端点: `http://localhost:8080/metrics`
- 日志输出: JSON格式，便于ELK收集

## 🔄 与freqtrade集成

freqtrade直接查询PostgreSQL，零改动：

```sql
-- freqtrade查询示例
SELECT * FROM candles_5m
WHERE symbol = 'BTC-USDT-PERP'
  AND timestamp > NOW() - INTERVAL '30 days'
ORDER BY timestamp DESC;
```

## ⚠️ 风险与对策

| 风险场景 | 影响程度 | 检测机制 | 应对策略 |
|---------|---------|---------|---------|
| API限制变更 | 高 | 连接失败监控 | 动态调整连接数 |
| 币种数量激增(400→1000+) | 高 | 自动发现服务 | 自动伸缩连接池 |
| 数据量突增 | 中 | 内存监控 | 增加连接数+批量优化 |
| 网络不稳定 | 中 | 心跳检测 | WAL+自动重连 |
| 数据库故障 | 高 | 写入监控 | 本地缓存+重试机制 |

## 🔄 设计原则

### 1. 简洁配置
- 最小化配置项
- 全量采集，无需复杂规则
- 自动化优于配置

### 2. 自动适应
- 自动检测币种变化
- 自动调整连接数量
- 无需手动干预

### 3. 全量保证
- 所有币种同等对待
- 所有数据类型全采集
- 最高性能运行

### 4. 未来扩展
- 支持多交易所扩展
- 数据格式标准化
- 模块化设计

## 📝 维护指南

### 日常运维

1. **监控检查**: 每日查看数据完整性
2. **日志审计**: 检查错误日志
3. **性能优化**: 定期VACUUM数据库
4. **备份策略**: 每周全量+每日增量

### 故障排查

```bash
# 检查进程状态
systemctl status cryptofeed

# 查看实时日志
tail -f /var/log/cryptofeed/collector.log

# 数据库连接数
SELECT count(*) FROM pg_stat_activity;

# 数据完整性检查
python scripts/check_data_integrity.py
```

---

**文档版本**: v1.0
**更新时间**: 2024-01
**作者**: Claude Assistant
**状态**: 设计阶段