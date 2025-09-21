# PyCharm配置指南 - Cryptofeed项目

## 项目架构说明

**重要**：Cryptofeed不是Web服务！它是一个**实时数据流处理系统**，用于：
- 连接加密货币交易所的WebSocket
- 实时接收市场数据（交易、K线、资金费率等）
- 将数据存储到PostgreSQL数据库

## PyCharm配置步骤

### 1. 设置Python解释器

1. 打开PyCharm → Preferences → Project → Python Interpreter
2. 点击齿轮图标 → Add
3. 选择 "Conda Environment" → "Existing environment"
4. 解释器路径：`/opt/anaconda3/envs/cryptofeed/bin/python`
5. 点击OK

### 2. 配置运行配置

#### 主监控程序配置
1. 点击右上角 "Add Configuration" → "+"  → "Python"
2. 配置如下：
   - **Name**: Binance Production Monitor
   - **Script path**: `/Volumes/磁盘/Projects/cryptofeed/scripts/binance_production_monitor.py`
   - **Environment variables**:
     ```
     PYTHONPATH=/Volumes/磁盘/Projects/cryptofeed
     ```
   - **Working directory**: `/Volumes/磁盘/Projects/cryptofeed`
   - **Python interpreter**: 选择上面配置的conda环境

### 3. 数据库环境检查

运行前确保PostgreSQL已启动：
```bash
docker ps | grep timescale-crypto
```

如果未运行，启动它：
```bash
docker start timescale-crypto
```

## 项目主要脚本说明

### 生产环境脚本
- **`scripts/binance_production_monitor.py`** - 主监控程序（监控30个USDT永续合约）
  - 实时接收交易、K线、资金费率数据
  - 自动存储到PostgreSQL
  - 包含数据清理逻辑

### 测试和调试脚本
- **`tests/simple_working_test.py`** - 最简单的测试（只监控BTC）
- **`tests/comprehensive_debug.py`** - 综合调试工具
- **`tests/check_database_status.py`** - 检查数据库状态
- **`tests/network_debug.py`** - 网络连接诊断

## 在PyCharm中运行

### 方式1：直接运行主监控程序
1. 打开 `scripts/binance_production_monitor.py`
2. 右键 → "Run 'binance_production_monitor'"
3. 查看控制台输出，你会看到：
   ```
   🚀 启动Binance生产级监控系统
   📊 发现 497 个USDT永续合约
   🎯 基于连接限制，选择前 30 个合约监控
   📡 开始接收数据流...
   ```

### 方式2：运行简单测试
如果想先测试，运行：
1. 打开 `tests/simple_working_test.py`
2. 右键运行
3. 这只会监控BTC一个合约，用于验证环境

### 方式3：检查数据状态
运行数据库检查脚本：
1. 打开 `tests/check_database_status.py`
2. 右键运行
3. 查看数据库中的数据统计

## 重要提示

1. **不是Web服务**：这是数据采集系统，运行后会持续接收数据流
2. **30合约限制**：由于API限制，只能同时监控30个合约
3. **实时数据流**：程序会持续运行，按Ctrl+C停止
4. **数据存储**：所有数据自动存储到PostgreSQL

## 监控的数据类型

- **交易数据** (trades) - 实时成交
- **K线数据** (candles) - 1分、5分、30分、4小时、1天
- **资金费率** (funding) - 永续合约资金费率
- **Ticker数据** (ticker) - 最新价格快照

## 常见问题

### Q: 程序一直在运行，这正常吗？
A: 是的，这是实时数据流系统，会持续运行直到手动停止

### Q: 为什么只监控30个合约？
A: 调试发现497个合约会导致连接过载，30个是稳定上限

### Q: 如何查看收集的数据？
A: 运行 `tests/check_database_status.py` 或直接查询PostgreSQL

### Q: 可以修改监控的合约吗？
A: 可以，修改 `scripts/binance_production_monitor.py` 中的 `get_top_liquid_symbols()` 方法

## PyCharm调试技巧

1. 设置断点：点击行号左侧
2. Debug模式运行：右键 → "Debug"
3. 查看变量：在Debug窗口查看实时数据
4. 条件断点：右键断点设置条件

## 项目结构

```
cryptofeed/
├── scripts/                  # 生产脚本
│   └── binance_production_monitor.py  # 主监控程序
├── tests/                    # 测试脚本
│   ├── simple_working_test.py
│   ├── comprehensive_debug.py
│   └── check_database_status.py
├── logs/                     # 日志文件
├── cryptofeed/              # 核心库代码
│   ├── exchanges/           # 交易所接口
│   └── backends/            # 数据存储后端
└── CLAUDE.md               # 项目说明文档
```

---

**提示**：在PyCharm中运行更方便调试，可以设置断点、查看变量、分析数据流。