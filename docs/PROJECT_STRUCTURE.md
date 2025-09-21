# Cryptofeed 项目结构说明

## 📁 目录结构

```
cryptofeed/                     # 项目根目录
├── 🔧 核心组件
│   ├── cryptofeed/             # 核心代码库
│   ├── examples/               # 官方示例
│   └── sample_data/            # 示例数据
│
├── 🚀 生产环境
│   ├── scripts/                # 生产脚本
│   │   ├── binance_production_monitor.py  # 主监控程序
│   │   ├── data_cleanup.py               # 数据清理工具
│   │   └── start_monitor.sh              # 启动脚本
│   ├── config/                 # 配置文件
│   │   ├── config_optimize.yaml         # 性能优化配置
│   │   ├── setup_partitioned_tables.sql # 数据库初始化
│   │   └── create_partitions_only.sql   # 分区表创建
│   └── logs/                   # 日志文件
│
├── 🧪 开发测试
│   ├── tests/                  # 测试脚本
│   │   ├── test_*.py           # 单元测试
│   │   ├── integration/        # 集成测试
│   │   └── postgres_manual_test.py  # 手动测试
│   └── tools/                  # 开发工具
│
├── 📚 文档
│   ├── docs/                   # 项目文档
│   │   ├── MONITOR_GUIDE.md    # 监控系统指南
│   │   └── PROJECT_STRUCTURE.md # 项目结构说明
│   ├── CLAUDE.md               # AI助手指南
│   └── README.md               # 项目说明
│
├── ⚙️ 环境配置
│   ├── cryptofeed-env/         # conda虚拟环境
│   ├── config.yaml             # 主配置文件
│   ├── requirements.txt        # 依赖列表
│   └── setup.py                # 安装脚本
│
└── 🎯 项目管理
    └── run.sh                  # 统一启动脚本
```

## 🎯 主要入口

### 生产环境使用
```bash
# 统一管理脚本
./run.sh

# 直接启动监控
./run.sh monitor

# 数据清理
./run.sh cleanup

# 查看统计
./run.sh stats
```

### 开发测试
```bash
# 运行测试
./run.sh test

# 检查项目状态
./run.sh status

# 清理根目录
./run.sh clean
```

## 📋 文件说明

### 🚀 生产脚本 (`scripts/`)
- `binance_production_monitor.py` - 主监控程序，支持495个USDT合约
- `data_cleanup.py` - 数据清理工具，按策略清理历史数据
- `start_monitor.sh` - 简化启动脚本

### 🔧 配置文件 (`config/`)
- `config_optimize.yaml` - 性能优化配置
- `setup_partitioned_tables.sql` - 完整数据库初始化
- `create_partitions_only.sql` - 仅创建分区表

### 🧪 测试文件 (`tests/`)
- `test_postgres_connection.py` - PostgreSQL连接测试
- `postgres_manual_test.py` - 手动监控测试
- `simple_postgres_test.py` - 简单连接测试
- `test_claude_api.py` - API连接测试

### 📚 文档文件 (`docs/`)
- `MONITOR_GUIDE.md` - 监控系统完整使用指南
- `PROJECT_STRUCTURE.md` - 项目结构说明（本文件）

## 🚫 严格禁止

### 不允许在根目录创建的文件类型：
- ❌ `test_*.py` → 移动到 `tests/`
- ❌ `demo_*.py` → 移动到 `tests/`
- ❌ `*.yaml`, `*.json` → 移动到 `config/`
- ❌ `*.sql` → 移动到 `config/`
- ❌ `*.sh` (除了run.sh) → 移动到 `scripts/`
- ❌ `*.md` (除了CLAUDE.md, README.md) → 移动到 `docs/`
- ❌ `*.log` → 移动到 `logs/`

### 自动清理
```bash
# run.sh 提供自动清理功能
./run.sh clean
```

## 🔄 迁移指南

如果发现根目录有错位文件，使用以下命令清理：

```bash
# 自动移动到正确位置
./run.sh clean

# 手动移动示例
mv test_*.py tests/
mv *.yaml config/
mv *.sql config/
mv *.md docs/
mv *.log logs/
```

## 🎯 最佳实践

1. **新建文件前**：确认应该放在哪个目录
2. **定期清理**：使用 `./run.sh clean` 整理项目
3. **遵循规范**：严格按照目录结构组织文件
4. **使用统一入口**：通过 `run.sh` 执行所有操作

---

**保持项目整洁，提高开发效率！**