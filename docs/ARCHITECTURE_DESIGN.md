# Cryptofeed 智能量化数据采集架构

## 📋 项目概述

个人量化交易数据采集系统，基于Cryptofeed框架对Binance全部USDT永续合约（496个）进行智能数据采集，实现高性能、低存储占用的量化数据管道。

## 🎯 系统特性

- **全覆盖监控**: 496个USDT永续合约实时监控
- **智能筛选**: 动态分层算法，大幅减少数据冗余
- **实时数据**: 支持trades、funding、liquidations、open_interest、candles
- **自动清理**: 滚动窗口数据管理，无需人工维护
- **高可靠性**: 断线重连、错误恢复、数据完整性保证

## 🏗️ 核心架构

### 系统架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Binance Futures WebSocket API                 │
│                     (496 USDT-PERP contracts)                   │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Cryptofeed Monitor System                      │
├─────────────────────┬─────────────────────┬─────────────────────┤
│    Smart Trade     │   Rate Limited      │   Full Event        │
│    Filtering       │   Funding           │   Monitoring        │
│                    │                     │                     │
│  • Large trades    │  • 1-minute rate    │  • Liquidations     │
│  • Price changes   │  • 30-day window    │  • Open Interest    │
│  • Dynamic tiers   │  • Auto cleanup     │  • Real-time        │
│  • 7-day cleanup   │                     │                     │
└─────────────────────┼─────────────────────┼─────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                PostgreSQL + TimescaleDB                        │
├──────────────┬──────────────┬──────────────┬──────────────┬──────
│   trades     │   funding    │ liquidations │open_interest │ candles
│              │              │              │              │
│ Smart Filter │ 1min/symbol  │ All Events   │ 5min Snapshot│ OHLCV
│ ~600/min     │ 496/min      │ ~3-5/min     │ ~6/min       │ Multi-TF
│ 7-day TTL    │ 30-day TTL   │ 30-day TTL   │ 30-day TTL   │ Long-term
└──────────────┴──────────────┴──────────────┴──────────────┴──────┘
```

### 智能数据筛选策略

#### 1. 动态分层交易筛选

```python
# 4-tier 动态分层系统
Tier 0 (Top 2%): 主流币种 - 最严格阈值 (2.0x P90)
Tier 1 (2-10%): 活跃币种 - 高阈值 (1.8x P90)
Tier 2 (10-30%): 中等币种 - 中阈值 (1.5x P90)
Tier 3 (30%+): 小众币种 - 低阈值 (1.2x P90)

# 多维度评分算法
score = (trading_volume × 0.4) + (frequency × 0.3) +
        (avg_size × 0.2) + (max_size × 0.1)

# 筛选条件（三选一即保存）
1. 大额交易: USD value > dynamic_threshold
2. 价格变动: price_change > 0.5%
3. 时间间隔: 按层级设定间隔时间保存
```

#### 2. 数据保留策略

| 数据类型 | 采集频率 | 保留期限 | 存储优化 |
|---------|---------|---------|---------|
| **trades** | 智能筛选 | 7天 | 动态阈值，减少99%+ |
| **funding** | 1分钟/合约 | 30天 | 减少95%数据量 |
| **liquidations** | 实时事件 | 30天 | 全量保存（关键风控） |
| **open_interest** | 5分钟快照 | 30天 | 周期性快照 |
| **candles_1m** | 标准K线 | 90天 | 完整OHLCV |
| **candles_5m** | 标准K线 | 180天 | 完整OHLCV |
| **candles_30m** | 标准K线 | 1年 | 完整OHLCV |
| **candles_4h** | 标准K线 | 2年 | 完整OHLCV |
| **candles_1d** | 标准K线 | 永久 | 完整OHLCV |

## 💾 数据库架构

### 表结构设计

```sql
-- Smart Trade Data (高度优化)
CREATE TABLE trades (
    timestamp TIMESTAMPTZ,
    symbol VARCHAR(32),
    side VARCHAR(4),
    amount DECIMAL(32,16),
    price DECIMAL(32,16),
    trade_id VARCHAR(64),
    save_reason VARCHAR(20)  -- large_trade, price_change, time_interval
);

-- Rate Limited Funding (减少95%数据量)
CREATE TABLE funding (
    timestamp TIMESTAMPTZ,
    symbol VARCHAR(32),
    rate DECIMAL(16,8),
    next_funding_time TIMESTAMPTZ
);

-- Critical Event Data (全量保存)
CREATE TABLE liquidations (
    timestamp TIMESTAMPTZ,
    symbol VARCHAR(32),
    side VARCHAR(8),
    quantity DECIMAL(64,32),
    price DECIMAL(64,32),
    status VARCHAR(16)
);

CREATE TABLE open_interest (
    timestamp TIMESTAMPTZ,
    symbol VARCHAR(32),
    open_interest INTEGER
);

-- REMOVED: ticker table (冗余数据，已删除)
```

### TimescaleDB优化

```sql
-- 时序数据分区 (按时间自动分区)
SELECT create_hypertable('trades', 'timestamp', chunk_time_interval => interval '1 day');
SELECT create_hypertable('funding', 'timestamp', chunk_time_interval => interval '7 days');
SELECT create_hypertable('liquidations', 'timestamp', chunk_time_interval => interval '7 days');

-- 自动数据清理策略
SELECT add_retention_policy('trades', INTERVAL '7 days');
SELECT add_retention_policy('funding', INTERVAL '30 days');
SELECT add_retention_policy('liquidations', INTERVAL '30 days');
```

## 🔧 核心组件

### 1. 智能交易过滤器

```python
class SmartTradePostgres:
    """动态分层交易筛选系统"""

    def __init__(self):
        self.symbol_tiers = {}           # 合约分层缓存
        self.tier_percentiles = [0.02, 0.10, 0.30]  # 2%, 10%, 30%
        self.threshold_multipliers = [2.0, 1.8, 1.5, 1.2]
        self.tier_update_interval = 24 * 3600  # 24小时更新分层

    def _calculate_symbol_score(self, stats):
        """多维度评分算法"""
        weights = {
            'total_volume': 0.4,    # 交易量权重40%
            'trade_count': 0.3,     # 频率权重30%
            'avg_trade_size': 0.2,  # 平均大小权重20%
            'max_trade_size': 0.1   # 最大交易权重10%
        }

    def should_save_trade(self, symbol, trade):
        """三级筛选逻辑"""
        # 1. 大额交易 (动态阈值)
        if usd_value > dynamic_threshold:
            return True, "large_trade"

        # 2. 价格突变 (>0.5%)
        if price_change > 0.005:
            return True, "price_change"

        # 3. 时间间隔 (分层控制)
        if time_since_last > tier_interval:
            return True, "time_interval"

        return False, None
```

### 2. 频率限制资金费率

```python
class RateLimitedFundingPostgres:
    """1分钟频率限制的funding数据采集"""

    def __init__(self):
        self.last_save_time = {}  # 每个合约的上次保存时间
        self.save_interval = 60   # 1分钟保存间隔

    async def __call__(self, funding, receipt_timestamp):
        symbol = funding.symbol
        current_time = time.time()

        # 检查是否超过1分钟间隔
        if (current_time - self.last_save_time.get(symbol, 0)) >= self.save_interval:
            await self.postgres.write(funding, receipt_timestamp)
            self.last_save_time[symbol] = current_time
```

### 3. 自动清理系统

```python
async def auto_cleanup_check(self):
    """自动数据清理 - 每小时检查"""
    if time.time() - self.last_cleanup_time >= 3600:
        await asyncio.gather(
            self.cleanup_old_trades(7),      # 7天trades
            self.cleanup_old_funding(30),    # 30天funding
            self.cleanup_old_liquidations(30), # 30天liquidations
            self.cleanup_old_open_interest(30) # 30天open_interest
        )
```

## 📊 性能优化效果

### 数据量对比

| 数据类型 | 优化前 | 优化后 | 减少幅度 |
|---------|--------|--------|----------|
| **trades** | ~75,600条/分钟 | ~600条/分钟 | **99.2%** ↓ |
| **funding** | ~9,920条/分钟 | 496条/分钟 | **95.0%** ↓ |
| **liquidations** | N/A | ~5条/分钟 | **新增** |
| **open_interest** | N/A | ~6条/分钟 | **新增** |

### 系统资源占用

```yaml
内存使用: ~200MB
- Python进程: ~120MB
- WebSocket连接: ~50MB
- 数据缓冲: ~30MB

CPU使用: ~15% (单核)
网络带宽: ~2-5Mbps
磁盘I/O: 批量写入优化

数据延迟: 1-3秒
查询响应: <100ms
```

## 🚀 部署配置

### Docker环境

```yaml
# TimescaleDB容器
docker run -d \
  --name timescale-crypto \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=cryptofeed \
  timescale/timescaledb:latest-pg15

# Conda环境配置
conda activate cryptofeed
pip install cryptofeed psycopg2-binary
```

### 系统配置

```yaml
# config/main.yaml
database:
  host: 127.0.0.1
  port: 5432
  user: postgres
  password: password
  database: cryptofeed

collection:
  data_types:
    - trades      # 智能筛选
    - funding     # 1分钟频率
    - candles_1m  # 全量
    - candles_5m  # 全量
    - candles_30m # 全量
    - candles_4h  # 全量
    - candles_1d  # 全量
    - liquidations # 全量事件
    - open_interest # 5分钟快照

monitoring:
  metrics_enabled: true
  health_check_port: 8080
  stats_interval: 300

logging:
  level: INFO
  filename: logs/cryptofeed_monitor.log
```

## 🔍 监控指标

### 关键指标监控

```python
# 实时统计信息
{
    "trades_count": 152847,        # 智能筛选后的交易数
    "funding_count": 87234,        # 1分钟频率的funding数
    "liquidations_count": 342,     # 强平事件数
    "open_interest_count": 1829,   # 持仓量快照数
    "candles_count": 234987,       # K线数据数
    "errors": 3,                   # 错误计数

    "last_trade_time": "13:45:23",
    "last_funding_time": "13:45:15",
    "last_liquidation_time": "13:44:12",
    "last_open_interest_time": "13:45:20"
}
```

### 数据质量检查

```sql
-- 检查数据完整性
SELECT
    COUNT(*) as total_symbols,
    COUNT(DISTINCT symbol) as unique_symbols
FROM funding
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- 检查智能筛选效果
SELECT
    save_reason,
    COUNT(*) as count,
    AVG(amount::float * price::float) as avg_usd_value
FROM trades
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY save_reason;
```

## 🔄 系统维护

### 自动化维护

1. **滚动数据清理**: TimescaleDB自动分区清理
2. **性能监控**: 实时统计和日志记录
3. **错误恢复**: WebSocket自动重连机制
4. **分层更新**: 24小时自动重新分层

### 手动维护

```bash
# 启动系统
python /path/to/cryptofeed/run.py

# 检查数据库状态
docker exec timescale-crypto psql -U postgres -d cryptofeed -c "\dt+"

# 查看实时日志
tail -f logs/cryptofeed_monitor.log

# 数据完整性检查
python tests/check_data_integrity.py
```

## 📈 扩展规划

### 近期优化

1. **更精细的交易筛选**: 基于市场微结构的噪音过滤
2. **多时间框架融合**: K线数据的智能聚合
3. **实时异常检测**: 基于统计学的异常交易识别

### 长期扩展

1. **多交易所支持**: OKX、Bybit等主流交易所
2. **现货数据集成**: USDT现货对的数据采集
3. **机器学习集成**: 基于历史数据的预测模型
4. **API服务**: RESTful API对外提供数据服务

---

**文档版本**: v2.0
**更新时间**: 2025-09-24
**系统状态**: 生产运行
**数据覆盖**: 496个USDT永续合约
**优化效果**: 数据量减少95%+，保持完整信息价值