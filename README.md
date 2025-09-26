# Cryptofeed 加密货币数据监控系统

## 🚀 项目概述

基于开源项目 [Cryptofeed](https://github.com/bmoscon/cryptofeed) 的Docker化加密货币数据收集系统，集成ClickHouse时序数据库，提供实时数据流和历史数据回填功能。

### ✨ 核心特性

- **实时数据收集**：WebSocket连接Binance获取实时K线、交易、资金费率等数据
- **历史数据回填**：自动检测并补充缺失的历史K线数据（支持7天回填）
- **高性能存储**：ClickHouse时序数据库，支持数据压缩和TTL自动清理
- **Docker化部署**：完整的Docker Compose栈，一键启动
- **日志管理**：自动日志轮转，防止磁盘空间占满
- **健康监控**：内置健康检查和故障恢复机制

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

### 一键启动

```bash
# 启动整个系统（自动构建镜像）
docker-compose up -d

# 查看运行状态
docker-compose ps

# 查看日志
docker-compose logs -f cryptofeed-monitor
```

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

# 查看日志
docker-compose logs -f cryptofeed-monitor
docker-compose logs -f clickhouse

# 查看系统资源使用
docker stats
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
✅ **历史回填**：已成功回填60,000+条K线数据
✅ **数据库**：ClickHouse运行正常，支持数据压缩和TTL
✅ **监控服务**：健康检查和日志管理正常

### 数据统计（截至最后更新）
- **总K线数据**：60,000+ 条
- **覆盖交易对**：5个主流加密货币
- **时间周期**：1m, 5m, 30m, 4h, 1d
- **历史数据范围**：7天完整数据
- **实时数据**：持续更新中

## 🤝 基于开源项目

本项目基于 [Cryptofeed](https://github.com/bmoscon/cryptofeed) 开源项目构建，感谢原作者的贡献。

## 📄 许可证

基于原 Cryptofeed 项目许可证。

---

**最后更新**：2025年9月25日
**当前版本**：Docker化ClickHouse版本
**部署状态**：✅ 生产就绪，数据收集正常