# Cryptofeed - AI Assistant Guide

## 🔴 CRITICAL: ALWAYS USE CONDA ENVIRONMENT
**重要提醒：项目使用conda环境，不是普通Python环境！**

### 环境激活方式：
```bash
# 正确的conda环境激活方式
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate cryptofeed

# 检查环境
echo $CONDA_DEFAULT_ENV  # 应该显示 cryptofeed

# 验证Python路径
which python  # 应该显示 /opt/anaconda3/envs/cryptofeed/bin/python
```

### 依赖管理：
- 使用 `pip install` 在conda环境内安装包
- 数据库连接使用 `psycopg2-binary`（已安装）
- 所有Python脚本都必须在conda环境中运行

## 🔴 CRITICAL: PostgreSQL 在 Docker 容器中
**重要：PostgreSQL 数据库运行在 Docker 容器 `timescale-crypto` 中，不是本地安装！**

### Docker PostgreSQL 信息：
```bash
# 容器名称：timescale-crypto
# 端口映射：localhost:5432 -> container:5432
# 数据库名：cryptofeed
# 用户名：postgres
# 密码：password

# 启动数据库
docker start timescale-crypto

# 连接测试
docker exec timescale-crypto psql -U postgres -d cryptofeed -c "SELECT 1;"

# Python连接配置
postgres_cfg = {
    'host': '127.0.0.1.txt',  # 通过Docker端口映射连接
    'port': 5432,
    'user': 'postgres',
    'database': 'cryptofeed',
    'password': 'password'
}
```

### psycopg2 说明：
- **psycopg2** 是 Python 的 PostgreSQL 数据库驱动
- 只需要在 Python 环境中安装，不需要本地 PostgreSQL
- 通过网络连接到 Docker 中的 PostgreSQL

## 🔴 MANDATORY: 项目文件组织规范
**严格按照以下目录结构组织文件，绝不允许在根目录创建临时文件！**

### 强制目录结构：
```
cryptofeed/                     # 项目根目录
├── cryptofeed/                 # 核心代码库（不要修改）
├── examples/                   # 官方示例（不要修改）
├── scripts/                    # 🟢 生产脚本
│   ├── binance_production_monitor.py  # 主监控程序
│   ├── data_cleanup.py               # 数据清理工具
│   └── start_monitor.sh              # 启动脚本
├── tests/                      # 🟡 测试脚本
│   ├── test_*.py               # 所有测试文件
│   └── integration/            # 集成测试
├── config/                     # 🔵 配置文件
│   ├── *.yaml                  # YAML配置
│   ├── *.sql                   # SQL脚本
│   └── *.json                  # JSON配置
├── docs/                       # 📚 文档
│   ├── *.md                    # Markdown文档
│   └── guides/                 # 使用指南
├── tools/                      # 🔧 开发工具
├── logs/                       # 📝 日志文件
├── CLAUDE.md                   # 项目指南（根目录）
└── README.md                   # 项目说明（根目录）
```

### 🚫 严格禁止：
- **根目录创建临时文件**：如 test_*.py, demo_*.py, temp_*.py
- **根目录创建配置文件**：如 *.yaml, *.json, *.sql
- **根目录创建脚本文件**：如 *.sh, 监控脚本
- **根目录创建文档文件**：如 *.md（除CLAUDE.md外）

### 🟢 文件创建规则：
1. **生产脚本** → `scripts/` 目录
2. **测试代码** → `tests/` 目录
3. **配置文件** → `config/` 目录
4. **文档说明** → `docs/` 目录
5. **开发工具** → `tools/` 目录
6. **日志文件** → `logs/` 目录

### 🔧 强制执行：
- 每次创建文件前检查目录位置
- 如发现根目录有临时文件，立即移动到正确位置
- 定期清理不需要的临时文件

---

## 📝 代码规范

### 命名规范

**文件和目录命名：**
- ✅ 使用小写字母+下划线：`data_collector.py`、`config_manager.py`
- ✅ 包目录必须有`__init__.py`
- ❌ 禁止使用驼峰命名：`DataCollector.py`
- ❌ 禁止项目内出现多个`main.py`（只允许根目录有一个）

**变量和函数命名：**
- ✅ 变量：小写+下划线：`user_name`、`total_count`
- ✅ 函数：小写+下划线：`get_data()`、`process_trades()`
- ✅ 私有方法：单下划线开头：`_internal_method()`
- ✅ 常量：全大写+下划线：`MAX_RETRY_COUNT`、`DEFAULT_TIMEOUT`

**类命名：**
- ✅ 类名：驼峰命名：`DataCollector`、`SymbolManager`
- ✅ 私有类：单下划线开头：`_InternalHelper`

### 目录结构规范

**cryptofeed_api/ 项目结构：**
```
cryptofeed_api/
├── __init__.py                  # 包初始化
├── app.py                       # FastAPI应用入口（不叫main.py）
├── api/                         # REST API模块
│   ├── __init__.py
│   ├── dependencies.py
│   └── v1/
│       ├── __init__.py
│       ├── health.py
│       └── monitoring.py
├── monitor/                     # 数据监控模块
│   ├── __init__.py
│   ├── collector.py             # 数据采集器（不叫main.py）
│   ├── config.py
│   └── backends/
│       ├── __init__.py          # 必须有！
│       └── clickhouse.py
├── core/                        # 核心配置
│   ├── __init__.py
│   ├── config.py
│   └── clickhouse.py
├── services/                    # 业务服务
│   ├── __init__.py
│   ├── data_backfill.py
│   └── data_integrity.py
├── models/                      # 数据模型
│   ├── __init__.py
│   └── schemas.py
└── utils/                       # 工具函数
    ├── __init__.py
    └── helpers.py
```

### 导入顺序规范

**按照以下顺序组织导入（PEP 8标准）：**
```python
# 1. 标准库导入
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

# 2. 第三方库导入
import asyncio
from fastapi import FastAPI, Request
from pydantic import BaseModel

# 3. 本地应用导入
from cryptofeed_api.core import config_manager
from cryptofeed_api.services.data_backfill import DataBackfillService
```

### 模块入口规范

**只允许根目录有一个main.py作为统一入口：**
```
cryptofeed/                      # 项目根目录
├── main.py                      # ✅ 唯一的main.py（统一启动入口）
├── cryptofeed_api/
│   ├── app.py                   # ✅ FastAPI应用
│   └── monitor/
│       └── collector.py         # ✅ 数据采集器
```

### 代码复杂度规范

- 每行最多120字符
- 单个函数≤50行，参数≤5个，嵌套≤4层
- 单个类≤300行，方法≤20个，公共方法≤10个
- 单个文件≤500行
- 超过限制必须拆分

### 格式规范

- 使用4空格缩进（禁止Tab）
- 类定义前空2行，方法间空1行
- 优先使用双引号
- 长行使用括号自然折行
- 文件末尾保留1个空行

### 异常和资源规范

- 捕获具体异常，避免裸`except Exception`
- 异常重抛时使用`raise ... from e`保持异常链
- 必须使用上下文管理器（`with`）管理资源
- 避免魔法数字，使用有意义的常量

### TODO注释格式

- `# TODO(用户名): 具体内容 - 优先级 - 日期`
- `# FIXME:` 需要修复的问题
- `# NOTE:` 重要说明
- `# HACK:` 临时方案

---

## 📚 注释规范

### 注释语言规范

**统一使用中文注释：**
```python
# ✅ 正确示例
def get_user_data(user_id: int) -> Dict[str, Any]:
    """获取用户数据

    Args:
        user_id: 用户ID

    Returns:
        包含用户信息的字典
    """
    # 从数据库查询用户
    user = db.query(user_id)
    return user

# ❌ 错误示例
def get_user_data(user_id: int) -> Dict[str, Any]:
    """Get user data"""  # 不要用英文注释
    # Query user from database
    user = db.query(user_id)
    return user
```

**注意：代码本身保持英文命名，只有注释用中文。**

### Docstring规范（Google风格）

**函数文档字符串：**
```python
def calculate_statistics(data: List[float], interval: str) -> Dict[str, float]:
    """计算统计数据

    对给定的数据列表计算各种统计指标，包括均值、中位数和标准差。

    Args:
        data: 数值列表，用于计算统计指标
        interval: 时间间隔，支持 '1m', '5m', '1h' 等

    Returns:
        包含以下键的字典：
        - mean: 平均值
        - median: 中位数
        - std: 标准差

    Raises:
        ValueError: 当data为空列表时

    Example:
        >>> data = [1.0, 2.0, 3.0, 4.0, 5.0]
        >>> stats = calculate_statistics(data, '1m')
        >>> print(stats['mean'])
        3.0
    """
    if not data:
        raise ValueError("数据列表不能为空")

    return {
        'mean': sum(data) / len(data),
        'median': sorted(data)[len(data) // 2],
        'std': calculate_std(data)
    }
```

**类文档字符串：**
```python
class DataCollector:
    """数据采集器

    负责从交易所WebSocket连接采集实时数据，并存储到数据库。
    支持多交易对并发采集，具有自动重连和错误恢复机制。

    Attributes:
        symbols: 监控的交易对列表
        feed_handler: Cryptofeed的FeedHandler实例
        is_running: 采集器运行状态标志

    Example:
        >>> collector = DataCollector()
        >>> await collector.start()
        >>> print(collector.is_running)
        True
    """

    def __init__(self):
        """初始化数据采集器"""
        self.symbols = []
        self.feed_handler = None
        self.is_running = False
```

### 类型提示规范

**强制使用类型提示：**
```python
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

# ✅ 正确：有完整的类型提示
async def get_candles(
    symbol: str,
    interval: str,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """获取K线数据"""
    pass

# ❌ 错误：缺少类型提示
async def get_candles(symbol, interval, start_time, end_time=None, limit=1000):
    """获取K线数据"""
    pass
```

**常用类型提示：**
```python
from typing import Dict, List, Optional, Any, Union, Tuple

# 基础类型
name: str = "BTC-USDT"
count: int = 100
price: float = 50000.0
is_active: bool = True

# 容器类型
symbols: List[str] = ["BTC-USDT", "ETH-USDT"]
config: Dict[str, Any] = {"host": "localhost", "port": 8123}
result: Optional[str] = None  # 可能是None

# 函数类型提示
def process(data: Dict[str, Any]) -> Tuple[int, str]:
    return (1, "success")

# 类型别名（复杂类型）
from typing import TypeAlias
SymbolConfig: TypeAlias = Dict[str, Union[str, int, bool]]
```

### 行内注释规范

**简洁清晰，不要废话：**
```python
# ✅ 好的注释：解释为什么
delay = 0.05  # 50ms间隔，避免触发Binance API限流（1200次/分钟）

# ❌ 差的注释：重复代码
delay = 0.05  # 设置延迟为0.05秒
```

---

**最后更新**：2025年1月
**适用范围**：cryptofeed_api/ 项目