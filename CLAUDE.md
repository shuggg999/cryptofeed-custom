# Cryptofeed Data Collection System - AI Assistant Guide

> **最后更新**：2025-11-09  
> **项目版本**：v1.2  
> **重构状态**：阶段 1 (70% 完成) → 准备补齐阶段 0

---

## 🎯 项目概述

**Cryptofeed** 是基于开源 cryptofeed 库的加密货币数据采集系统，实时采集、存储 Binance 交易所市场数据。

**核心功能：**
- 实时采集：Trades、K线、资金费率、清算、持仓量
- 自动数据回填
- REST API 查询接口
- 多环境隔离（dev/prod）

**技术栈：**
- 数据采集：cryptofeed（WebSocket）
- 数据存储：ClickHouse（时序数据库）
- Web框架：FastAPI + Uvicorn
- 运行环境：Conda（cryptofeed环境）
- 容器化：Docker（ClickHouse）

---

## 🔴 CRITICAL: 环境配置

### 1. Conda 环境（必须）

```bash
# 激活环境
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate cryptofeed

# 验证
echo $CONDA_DEFAULT_ENV  # 应显示: cryptofeed
which python             # 应显示: /opt/anaconda3/envs/cryptofeed/bin/python
```

**关键依赖**：cryptofeed, clickhouse-connect, fastapi, uvicorn, pydantic

### 2. ClickHouse 数据库（Docker）

```bash
# 容器信息
容器名：cryptofeed-clickhouse
端口：8123 (HTTP) / 9000 (Native)
数据库：cryptofeed (生产) / cryptofeed_dev (开发)
用户：default / 密码：password123

# 常用命令
docker start cryptofeed-clickhouse
docker exec cryptofeed-clickhouse clickhouse-client --query "SELECT 1"
```

### 3. 环境隔离 ⭐ 重要

| 环境 | 配置 | 数据库 | 端口 | 交易对 | 保留期 |
|------|------|--------|------|--------|--------|
| 开发 | `config/dev.yaml` | `cryptofeed_dev` | 8889 | 2个 | 7天 |
| 生产 | `config/prod.yaml` | `cryptofeed` | 8888 | 5个 | 30-365天 |

**启动方式**：
```bash
ENV=dev python -m cryptofeed_api.app   # 开发
ENV=prod python -m cryptofeed_api.app  # 生产
```

---

## 📂 项目结构

```
cryptofeed/
├── config/                  # 配置文件（环境隔离）
│   ├── dev.yaml
│   └── prod.yaml
├── cryptofeed_api/          # 核心代码
│   ├── app.py               # FastAPI 入口
│   ├── api/v1/              # REST API
│   ├── backends/            # 数据存储层
│   ├── core/                # 核心配置
│   │   ├── config.py
│   │   ├── clickhouse.py
│   │   └── logging_config.py  # 统一日志
│   ├── monitor/             # 数据采集
│   │   ├── collector.py     # 核心采集器
│   │   └── symbol_manager.py
│   ├── services/            # 业务服务
│   │   ├── data_backfill.py
│   │   └── data_integrity.py
│   ├── models/
│   └── utils/
├── docs/project/            # 项目文档
│   ├── 渐进式重构计划.md
│   └── 环境隔离使用指南.md
├── scripts/                 # 生产脚本
├── tests/                   # 测试代码
└── logs/                    # 日志文件
```

**🚫 严禁在根目录创建临时文件！** test_*.py → tests/, config → config/, docs → docs/

---

## 📋 数据库表（8张）

**核心数据表（5张）**：
1. trades - 交易数据
2. candles - K线数据（1分钟）
3. funding - 资金费率（⭐ 分层TTL）
4. liquidations - 清算数据
5. open_interest - 持仓量

**管理表（3张）**：backfill_status, gap_detection_log, real_time_monitor（预留）

**⭐ 资金费率分层存储（2025-11-09实现）**：
- 预测费率（is_settlement=0）：7天
- 结算费率（is_settlement=1）：90天（dev）/ 365天（prod）
- 结算时刻：UTC 00:00/08:00/16:00

---

## 🔧 代码规范

### 命名

- **文件/目录**：小写+下划线 `data_collector.py` ❌驼峰
- **变量/函数**：小写+下划线 `get_data()` `user_name`
- **类**：驼峰 `DataCollector` `SymbolManager`
- **常量**：全大写 `MAX_RETRY_COUNT`
- **私有**：单下划线开头 `_internal_method()`

### 导入顺序（PEP 8）

```python
# 1. 标准库
import os
from datetime import datetime

# 2. 第三方库
from fastapi import FastAPI

# 3. 本地应用
from cryptofeed_api.core import config_manager
```

### 复杂度控制

- 每行 ≤ 120 字符
- 函数 ≤ 50 行，参数 ≤ 5 个
- 类 ≤ 300 行
- 文件 ≤ 500 行

### 格式

- 4空格缩进（禁止Tab）
- 类前空2行，方法间空1行
- 优先双引号
- 文件末尾保留1空行

---

## 📝 注释规范

### 统一使用中文注释

**代码用英文，注释用中文**

```python
def get_user_data(user_id: int) -> Dict[str, Any]:
    """获取用户数据
    
    Args:
        user_id: 用户ID
    
    Returns:
        包含用户信息的字典
    """
    # 从数据库查询用户
    return db.query(user_id)
```

### Docstring（Google风格）

**函数**：
- 简短描述（一行）
- 详细说明（可选）
- Args: 参数说明
- Returns: 返回值说明
- Raises: 异常说明（可选）
- Example: 使用示例（可选）

**类**：
- 类的作用描述
- Attributes: 属性说明
- Example: 使用示例（可选）

### 类型提示（强制）

```python
# ✅ 正确
async def get_candles(
    symbol: str,
    start_time: datetime,
    end_time: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    pass

# ❌ 错误：缺少类型提示
async def get_candles(symbol, start_time, end_time=None):
    pass
```

---

## 🚀 日志配置

**统一日志模块**：`cryptofeed_api/core/logging_config.py`

**格式**：
```
2025-11-09 13:58:37.637 - INFO - cryptofeed_api.app:481 - 🚀 正在启动...
```

- 时间戳（毫秒）- 级别 - 模块:行号 - 消息
- 支持 PyCharm 点击跳转
- 第三方库（websockets, clickhouse_connect）设为 WARNING 级别

---

## ⚠️ 重要提醒

### 1. 资金费率采集

- 每分钟采集预测费率
- 结算时刻（UTC 00:00/08:00/16:00）自动标记 `is_settlement=1`
- 日志显示 `🔔 SETTLEMENT`

### 2. 回填逻辑现状

- ✅ K线：已实现
- ⚠️ 交易：仅检测，未回填
- ⚠️ 资金费率：未实现

### 3. 异常处理

- 捕获具体异常，避免裸 `except Exception`
- 使用 `raise ... from e` 保持异常链
- 使用 `with` 管理资源
- 避免魔法数字

### 4. Git 提交

格式：`<type>: <description>`
- feat: 新功能
- fix: Bug修复
- refactor: 重构
- docs: 文档
- chore: 构建/工具

---

## 📚 相关文档

- `docs/project/渐进式重构计划.md` - 重构 roadmap（必读）
- `docs/project/环境隔离使用指南.md` - 环境切换
- `config/dev.yaml` / `config/prod.yaml` - 环境配置

**注意**：config/main.yaml 已废弃，不再使用

---

**最后更新**：2025-11-09  
**文档版本**：v2.0（简洁版，移除代码示例，添加环境隔离）

记住：渐进式重构，不追求完美，慢慢来比较快！🚀
