# Cryptofeed 加密货币数据监控系统

## 🚀 项目概述

基于开源项目 [Cryptofeed](https://github.com/bmoscon/cryptofeed) 的Docker化加密货币数据收集系统，集成ClickHouse时序数据库，提供实时数据流和历史数据回填功能。

### ✨ 核心特性

- **实时数据收集**：WebSocket连接Binance获取实时K线、交易、资金费率等数据
- **🧠 智能回填系统V2**：精确检测缺失数据，按优先级自动补充，确保数据完整性
- **高性能存储**：ClickHouse时序数据库，支持数据压缩和TTL自动清理
- **Docker化部署**：完整的Docker Compose栈，一键启动
- **日志管理**：自动日志轮转，防止磁盘空间占满
- **健康监控**：内置健康检查和故障恢复机制

### 🧠 智能回填系统特性

**核心理念："缺什么补什么"** - 精确检测并只填充真正缺失的数据，避免不必要的下载

- **精确缺口检测**：逐时间段检查，找出确切的缺失时间范围
- **优先级分级**：
  - 🚨 **紧急缺口** (最近1小时)：立即处理，最高优先级
  - ⏰ **近期缺口** (1-24小时)：快速处理，中等优先级
  - 📚 **历史缺口** (24小时以上)：延后处理，较低优先级
- **场景感知处理**：
  - 🔄 **重启场景**：检测服务停机期间的数据缺口
  - 🌐 **网络恢复**：处理网络中断导致的WebSocket断连缺口
  - 🕵️ **手动检查**：检测人为操作导致的数据异常
- **智能策略**：
  - ✅ 复用现有API逻辑，避免重复开发
  - ✅ API限制友好，50ms间隔控制请求频率
  - ✅ 批量处理优化，单次最多1500条记录
  - ✅ 自动重试机制，指数退避策略

## 📊 数据覆盖

### 支持的交易对
- BTC-USDT-PERP （比特币永续合约）
- ETH-USDT-PERP （以太坊永续合约）
- SOL-USDT-PERP （Solana永续合约）
- DOGE-USDT-PERP （狗狗币永续合约）
- ADA-USDT-PERP （Cardano永续合约）

### 支持的数据类型
- **K线数据**：1m, 5m, 30m, 4h, 1d 多时间周期
- **交易数据**：实时成交记录
- **资金费率**：永续合约资金费率
- **持仓量**：未平仓合约数量
- **清算数据**：强制平仓记录

## 🛠️ 快速开始

### 环境要求
- Docker Desktop 或 Docker + Docker Compose
- 至少 4GB 可用内存
- 至少 10GB 可用磁盘空间

### 🚀 生产模式启动

```bash
# 启动整个系统（自动构建镜像）
docker-compose up -d

# 查看运行状态
docker-compose ps

# 查看日志
docker-compose logs -f cryptofeed-monitor
```

### 🔧 开发模式启动（推荐用于代码开发）

```bash
# 启动开发环境 - 支持代码热更新，无需重构镜像
./scripts/dev-mode.sh start

# 查看状态
./scripts/dev-mode.sh status

# 查看日志
./scripts/dev-mode.sh logs

# 停止环境
./scripts/dev-mode.sh stop
```

**开发模式特性**：
- ✅ **代码热更新**: 修改代码立即生效，无需重构Docker镜像
- ✅ **配置热更新**: 修改YAML配置立即生效
- ✅ **日志持久化**: 容器重启后日志不丢失
- ✅ **开发友好**: 大幅提升开发效率

### 验证部署

```bash
# 检查ClickHouse连接
docker-compose exec clickhouse clickhouse-client --query "SELECT version()"

# 检查数据量
docker-compose exec clickhouse clickhouse-client --database cryptofeed --query "SELECT COUNT(*) FROM candles"

# 查看数据分布
docker-compose exec clickhouse clickhouse-client --database cryptofeed --query "
SELECT symbol, interval, COUNT(*) as records
FROM candles
GROUP BY symbol, interval
ORDER BY symbol, records DESC"
```

## 🏗️ 系统架构

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Binance API   │────│  Cryptofeed App  │────│   ClickHouse    │
│   WebSocket     │    │   (Container)    │    │   (Container)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              │
                       ┌─────────────────┐
                       │ 历史数据回填服务  │
                       │ (每6小时运行)    │
                       └─────────────────┘
```

## 📁 项目结构

```
cryptofeed/
├── cryptofeed/                 # 核心代码库
├── cryptofeed_api/             # API和服务层
│   ├── monitor/                # 监控服务
│   └── services/               # 数据回填服务
├── docker/                     # Docker配置文件
│   └── clickhouse/
│       └── init/               # ClickHouse初始化脚本
├── config/                     # 配置文件
│   └── main.yaml              # 主配置文件
├── docker-compose.yml          # Docker编排文件
├── Dockerfile                  # 应用镜像构建文件
├── setup.py                   # Python包配置（Cython编译）⭐
└── requirements.txt           # Python依赖
```

## 🔧 重要文件说明

### `setup.py` ⭐ 重要文件
这个文件解决了Cython编译问题，是项目正常运行的关键：
- 处理 `cryptofeed.types` 模块的Cython编译
- 提供纯Python备用方案（`types_fallback.py`）
- Docker构建时自动处理编译失败情况
- **请勿删除此文件**

### 配置文件 `config/main.yaml`

```yaml
# 交易对配置
symbols:
  - BTC-USDT-PERP
  - ETH-USDT-PERP
  # ... 更多交易对

# 历史数据回填配置
data_backfill:
  enabled: true
  default_lookback_days: 7
  check_interval_hours: 6

# ClickHouse配置
clickhouse:
  host: clickhouse
  port: 8123
  database: cryptofeed
```

### Docker环境变量

```bash
# ClickHouse连接配置
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=password123
CLICKHOUSE_DATABASE=cryptofeed
```

## 📈 数据查询示例

### 获取最新K线数据
```sql
SELECT * FROM candles
WHERE symbol = 'BTC-USDT-PERP' AND interval = '1m'
ORDER BY timestamp DESC
LIMIT 10;
```

### 统计各交易对数据量
```sql
SELECT
    symbol,
    interval,
    COUNT(*) as records,
    MIN(timestamp) as earliest,
    MAX(timestamp) as latest
FROM candles
GROUP BY symbol, interval
ORDER BY symbol, records DESC;
```

### 查看实时交易数据
```sql
SELECT * FROM trades
WHERE symbol = 'BTC-USDT-PERP'
ORDER BY timestamp DESC
LIMIT 10;
```

## 🔍 监控与维护

### 查看系统状态
```bash
# 容器状态
docker-compose ps

# 查看系统资源使用
docker stats
```

### 📊 Docker日志查看指南

#### 基础日志命令
```bash
# 查看实时日志（推荐）
docker-compose logs -f cryptofeed-monitor

# 查看最近50行日志
docker-compose logs --tail=50 cryptofeed-monitor

# 查看ClickHouse日志
docker-compose logs -f clickhouse

# 查看所有服务日志
docker-compose logs -f
```

#### 高级日志查看
```bash
# 查看特定时间段日志
docker-compose logs --since="2h" cryptofeed-monitor

# 查看带时间戳的日志
docker-compose logs -f -t cryptofeed-monitor

# 搜索特定关键词
docker-compose logs cryptofeed-monitor | grep "ERROR"
docker-compose logs cryptofeed-monitor | grep "智能回填"

# 将日志保存到文件
docker-compose logs cryptofeed-monitor > monitor.log
```

#### 智能回填系统日志
```bash
# 查看智能回填执行日志
docker-compose logs cryptofeed-monitor | grep -E "(智能|回填|缺口|填充)"

# 查看API调用统计
docker-compose logs cryptofeed-monitor | grep -E "(填充完成|API调用)"

# 查看数据完整性检查
docker-compose logs cryptofeed-monitor | grep -E "(快速检查|精确检测)"
```

### 🧠 智能回填系统操作

#### 手动触发智能回填
```bash
# 进入容器手动执行回填
docker-compose exec cryptofeed-monitor python -c "
import asyncio
from cryptofeed_api.services.smart_data_backfill import smart_data_integrity_check

async def run_check():
    result = await smart_data_integrity_check('manual_check')
    print(f'回填完成: {result}')

asyncio.run(run_check())
"
```

#### 查看回填状态
```bash
# 检查ClickHouse中的缺口记录
docker-compose exec clickhouse clickhouse-client --database cryptofeed --query "
SELECT symbol, interval, gap_type, priority, status, detection_time
FROM gap_detection_log
ORDER BY detection_time DESC
LIMIT 10"

# 查看回填统计
docker-compose exec clickhouse clickhouse-client --database cryptofeed --query "
SELECT
    gap_type,
    status,
    COUNT(*) as count,
    SUM(records_filled) as total_filled
FROM gap_detection_log
GROUP BY gap_type, status
ORDER BY gap_type"
```

### 数据维护
```bash
# 重启服务
docker-compose restart cryptofeed-monitor

# 停止服务
docker-compose down

# 重新构建并启动
docker-compose build && docker-compose up -d

# 查看数据存储情况
docker-compose exec clickhouse clickhouse-client --database cryptofeed --query "
SELECT
    table,
    sum(rows) as total_rows,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size
FROM system.parts
WHERE database = 'cryptofeed'
GROUP BY table
ORDER BY total_rows DESC"
```

## 🚨 故障排除

### 常见问题

1. **Cython编译失败** ✅ 已解决
   - 项目包含 `setup.py` 和 `types_fallback.py` 作为备用方案
   - Docker构建时会自动处理编译失败情况

2. **历史数据回填错误** ✅ 已解决
   - ClickHouse连接配置已修复
   - 数据结构匹配问题已解决
   - 序列化错误已修复

3. **ClickHouse连接问题**
   - 确保容器间网络正常
   - 检查环境变量配置
   - 验证数据库初始化脚本

4. **内存不足**
   - 推荐至少4GB内存
   - 可以减少监控的交易对数量
   - 调整ClickHouse内存设置

### 日志位置
- 应用日志：Docker容器内 `/app/logs/`
- ClickHouse日志：Docker容器内 `/var/log/clickhouse-server/`
- Docker日志：使用 `docker-compose logs` 查看

## 📊 当前系统状态

✅ **部署状态**：已完成Docker化部署
✅ **实时数据**：WebSocket连接正常运行
✅ **历史回填**：智能回填系统，自动补齐缺失数据
✅ **数据标准化**：统一交易所名称格式（BINANCE_FUTURES）
✅ **数据库**：ClickHouse运行正常，支持数据压缩和TTL
✅ **监控服务**：健康检查和日志管理正常
✅ **开发工作流**：支持代码热更新，提升开发效率

### 最新更新 (2025-09-28)
- ✅ **智能回填系统V2**：完全重写数据回填逻辑，实现"缺什么补什么"
- ✅ **精确缺口检测**：逐时间段检查，找出确切的缺失部分
- ✅ **优先级分级处理**：紧急(1小时) → 近期(24小时) → 历史自动分级
- ✅ **场景感知回填**：支持重启、网络恢复、手动检查等特殊场景
- ✅ **复用现有代码**：基于existing data_backfill.py，避免重新造轮子
- ✅ **数据完整性保障**：确保量化交易系统数据100%完整

### 数据统计（截至最后更新）
- **总K线数据**：66,000+ 条（新增118条）
- **覆盖交易对**：5个主流加密货币
- **时间周期**：1m, 5m, 30m, 4h, 1d
- **历史数据范围**：7天完整数据
- **实时数据**：持续更新中
- **数据完整性**：✅ 已修复所有已知缺口

## 🤝 基于开源项目

本项目基于 [Cryptofeed](https://github.com/bmoscon/cryptofeed) 开源项目构建，感谢原作者的贡献。

## 📄 许可证

基于原 Cryptofeed 项目许可证。

---

**最后更新**：2025年9月25日
**当前版本**：Docker化ClickHouse版本
**部署状态**：✅ 生产就绪，数据收集正常