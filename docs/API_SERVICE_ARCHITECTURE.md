# Cryptofeed API Service Architecture Design

## 📋 项目概述

本文档描述了为Cryptofeed项目设计的API服务层架构，该架构旨在解决FreqTrade量化交易系统的数据获取和管理需求。

## 🎯 核心问题与目标

### 当前架构痛点
1. **冷启动问题** - WebSocket只能获取实时数据，历史数据需要慢慢积累
2. **数据连续性** - 服务重启导致数据断档
3. **响应延迟** - FreqTrade启动时需要等待数据积累
4. **缺乏灵活性** - 无法按需获取特定时间段的历史数据
5. **存储成本** - 长期存储所有历史数据成本较高

### 设计目标
- ✅ 提供快速的历史数据补充机制
- ✅ 保证数据连续性和完整性
- ✅ 支持按需获取任意时间段数据
- ✅ 控制存储成本，动态管理数据
- ✅ 简化FreqTrade的数据获取逻辑

## 🏗️ 整体架构设计

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   FreqTrade     │────▶│   API Service    │────▶│   PostgreSQL    │
│  (量化交易)      │     │   (Web层)        │     │   (TimescaleDB) │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │                           ▲
                               ▼                           │
                        ┌──────────────────┐              │
                        │  Binance REST    │──────────────┘
                        │  (历史数据补充)   │
                        └──────────────────┘
                               ▲
                               │
                        ┌──────────────────┐
                        │  WebSocket       │
                        │  (实时数据采集)   │
                        └──────────────────┘
```

### 架构层次

#### 1. **数据采集层**（现有）
- **WebSocket实时采集**：持续获取最新的市场数据
- **REST API补充**：按需获取历史数据，填补断档

#### 2. **数据服务层**（新增 - 核心组件）
- **Web API服务**：提供统一的数据接口
- **数据完整性检查**：自动检测和修复数据断档
- **智能路由**：有数据直接返回，缺失数据自动补充
- **临时数据管理**：动态创建和清理临时数据表

#### 3. **存储层**（现有）
- **PostgreSQL + TimescaleDB**：时序数据专用存储
- **分区管理**：按时间分区，便于数据管理
- **临时表机制**：支持大数据回测时的动态扩展

#### 4. **消费层**
- **FreqTrade**：通过标准API获取所需数据
- **其他量化系统**：可复用同一套API接口

## 🔄 核心工作流程

### 1. 服务启动流程
```mermaid
graph TD
    A[API服务启动] --> B[检查数据完整性]
    B --> C{发现数据断档?}
    C -->|是| D[启动REST API补充]
    C -->|否| E[服务就绪]
    D --> F[批量获取历史数据]
    F --> G[存储到数据库]
    G --> H[继续检查下一段]
    H --> I{所有断档修复?}
    I -->|是| E
    I -->|否| F
    E --> J[开启WebSocket采集]
```

### 2. 数据请求流程
```mermaid
graph TD
    A[FreqTrade请求数据] --> B[API接收请求]
    B --> C[查询数据库]
    C --> D{数据完整?}
    D -->|是| E[直接返回数据]
    D -->|否| F[计算缺失范围]
    F --> G[REST API补充数据]
    G --> H[存储补充数据]
    H --> I[返回完整数据]
```

### 3. 临时数据扩展流程
```mermaid
graph TD
    A[回测需求超出存储范围] --> B[创建临时表/分区]
    B --> C[REST API获取额外数据]
    C --> D[存储到临时表]
    D --> E[返回完整数据集]
    E --> F[回测完成]
    F --> G[自动清理临时数据]
```

## 🚀 性能与效率优化

### REST API vs WebSocket 效率对比
| 方式 | 1年5分钟数据获取时间 | 优缺点 |
|------|---------------------|--------|
| **WebSocket逐条** | ~28小时 | ❌ 极慢，需要实时等待 |
| **REST API批量** | ~2-3小时 | ✅ 快速，受限于API限流 |

### API限流管理策略
- **Binance限制**：1200请求/分钟，10请求/秒
- **批次控制**：每次请求获取最大允许数据量
- **智能间隔**：动态调整请求频率，避免触碰限制
- **断点续传**：支持中断后从断点继续补充

### 存储成本优化
```yaml
数据保留策略:
  常规存储: 1年数据 (~100MB/symbol)
  临时扩展: 按需创建，自动清理
  分区管理: 按月分区，便于管理
  压缩优化: TimescaleDB自动压缩历史数据
```

## 🛠️ API接口设计

### 核心端点规划
```http
# 获取K线数据
GET /api/v1/candles/{symbol}/{interval}
Query: start_time, end_time, limit

# 获取交易数据
GET /api/v1/trades/{symbol}
Query: start_time, end_time, limit

# 获取资金费率
GET /api/v1/funding/{symbol}
Query: start_time, end_time

# 数据完整性检查
GET /api/v1/health/data-integrity/{symbol}

# 临时数据管理
POST /api/v1/temp-data/create
DELETE /api/v1/temp-data/{temp_id}
```

### 响应格式统一
```json
{
  "status": "success",
  "data": [...],
  "metadata": {
    "symbol": "BTC-USDT-PERP",
    "interval": "5m",
    "count": 10000,
    "start_time": "2023-01-01T00:00:00Z",
    "end_time": "2024-01-01T00:00:00Z",
    "data_source": "database|rest_api",
    "completeness": 100
  }
}
```

## 🔒 数据完整性保障

### 断档检测机制
```python
def detect_data_gaps(symbol: str, interval: str, start: datetime, end: datetime):
    """
    检测指定时间范围内的数据断档
    返回需要补充的时间段列表
    """
    expected_count = calculate_expected_records(interval, start, end)
    actual_count = query_actual_records(symbol, interval, start, end)

    if actual_count < expected_count:
        return find_missing_ranges(symbol, interval, start, end)
    return []
```

### 数据质量验证
- **时间戳连续性**：检查K线数据时间间隔是否正确
- **数据合理性**：价格、交易量等数值范围验证
- **重复检测**：避免补充数据时产生重复记录
- **完整性评分**：计算数据完整度百分比

## 📊 监控与运维

### 关键指标监控
```yaml
性能指标:
  - API响应时间
  - 数据库查询延迟
  - REST API调用频率
  - 数据补充速度

数据指标:
  - 数据完整性百分比
  - 实时数据延迟
  - 存储空间使用率
  - 临时数据清理状态

系统指标:
  - CPU和内存使用率
  - 数据库连接池状态
  - WebSocket连接状态
  - 错误率和重试次数
```

### 告警策略
- **数据断档超过1小时** → 立即告警
- **API响应时间超过5秒** → 性能告警
- **存储空间使用超过80%** → 容量告警
- **REST API限流触发** → 限流告警

## 🛡️ 容错与恢复机制

### 故障场景处理
1. **REST API请求失败**
   - 指数退避重试机制
   - 降级策略：返回部分可用数据
   - 失败记录：待后续补充

2. **数据库连接中断**
   - 连接池自动重连
   - 请求队列缓存
   - 健康检查端点

3. **WebSocket连接断开**
   - 自动重连机制
   - 断线期间数据补充
   - 状态同步验证

### 数据一致性保证
```python
@transaction
def supplement_historical_data(symbol: str, start: datetime, end: datetime):
    """
    事务保证：要么全部成功，要么全部回滚
    """
    try:
        gaps = detect_data_gaps(symbol, start, end)
        for gap in gaps:
            data = fetch_from_binance_rest(symbol, gap.start, gap.end)
            validate_data_quality(data)
            store_to_database(data)
        commit_transaction()
    except Exception as e:
        rollback_transaction()
        raise DataSupplementError(f"Failed to supplement data: {e}")
```

## 🚀 部署与扩展

### Docker化部署
```yaml
services:
  api-service:
    build: ./api-service
    environment:
      - DATABASE_URL=postgresql://user:pass@timescale:5432/cryptofeed
    depends_on:
      - timescale

  data-collector:
    build: ./data-collector
    depends_on:
      - timescale
      - api-service

  timescale:
    image: timescale/timescaledb:latest
    volumes:
      - postgres_data:/var/lib/postgresql/data
```

### 水平扩展策略
- **API服务**：多实例负载均衡
- **数据采集**：按交易对分片
- **数据库**：TimescaleDB分布式部署
- **缓存层**：可选Redis集群（按需）

## 💡 最佳实践建议

### 开发实践
1. **API版本管理** - 使用语义化版本，保证向后兼容
2. **错误处理** - 统一错误码和错误信息格式
3. **日志规范** - 结构化日志，便于监控和调试
4. **文档维护** - API文档实时更新，包含示例代码

### 运维实践
1. **渐进式发布** - 灰度发布，监控指标
2. **数据备份** - 定期备份关键数据
3. **容量规划** - 监控增长趋势，提前扩容
4. **安全加固** - API访问控制，数据传输加密

## 📈 未来扩展方向

### 短期优化（1-3个月）
- [ ] 实现基础API服务和数据补充功能
- [ ] 完成与FreqTrade的集成测试
- [ ] 建立监控和告警体系
- [ ] 优化数据补充算法效率

### 中期扩展（3-6个月）
- [ ] 支持更多交易所数据源
- [ ] 实现智能数据预加载
- [ ] 增加数据分析和统计功能
- [ ] 开发Web管理界面

### 长期规划（6-12个月）
- [ ] 机器学习驱动的数据质量优化
- [ ] 分布式架构支持
- [ ] 实时数据流处理优化
- [ ] 多租户支持

## 📝 总结

本架构设计通过引入API服务层，有效解决了当前Cryptofeed项目的数据管理痛点：

**✅ 核心优势**
- **快速冷启动**：REST API批量补充，比WebSocket快10倍以上
- **数据完整性**：自动检测和修复数据断档
- **成本可控**：动态数据管理，按需扩展
- **架构简洁**：避免过度复杂化，专注核心需求
- **高度兼容**：无缝集成现有FreqTrade工作流程

**🎯 设计理念**
- **实用主义**：解决实际问题，不过度工程化
- **成本意识**：平衡功能需求与资源消耗
- **渐进式发展**：先满足核心需求，后续迭代优化
- **可维护性**：简单清晰的架构，便于后续维护

该架构为Cryptofeed项目的生产化部署奠定了坚实基础，能够满足专业量化交易的数据需求。

---

**文档版本**: v1.0
**创建日期**: 2024-09-25
**更新日期**: 2024-09-25
**作者**: Cryptofeed Team

> 💡 **提示**: 本文档将随着项目发展持续更新，建议定期查看最新版本。