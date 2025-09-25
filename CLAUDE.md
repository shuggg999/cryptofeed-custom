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

## Project Overview
Cryptofeed is a cryptocurrency exchange data feed handler that normalizes and standardizes data from multiple exchanges. It handles websockets, REST endpoints, and provides real-time data for trades, order books, tickers, funding, and other market data.

**Key Stats:**
- 40+ supported exchanges (Binance, Coinbase, Kraken, BitMEX, Bybit, OKX, etc.)
- Python 3.9+ required (supports 3.9, 3.10, 3.11, 3.12)
- AsyncIO-based architecture with uvloop optimization
- Cython-optimized core types for performance-critical paths
- Extensive backend support (Redis, MongoDB, PostgreSQL, InfluxDB, Kafka, RabbitMQ, etc.)
- Current version: 2.4.1 (Feb 2025)

## Architecture & Core Concepts

### Main Components
- **FeedHandler**: Central orchestrator that manages multiple exchange feeds
- **Exchange Classes**: Individual exchange implementations in `/cryptofeed/exchanges/`
- **Channels**: Data types (L1_BOOK, L2_BOOK, L3_BOOK, TRADES, TICKER, FUNDING, etc.)
- **Callbacks**: User-defined functions for handling data events
- **Backends**: Storage/output destinations (Redis, MongoDB, etc.)
- **Symbols**: Normalized instrument representation across exchanges

### Key Architecture Patterns
1. **AsyncIO Event Loop**: All exchanges run asynchronously
2. **Websocket-First**: Prefers websockets over REST polling when available
3. **Normalization**: All data is normalized to standard formats across exchanges
4. **Type Safety**: Uses Cython for performance-critical types with runtime assertions
5. **Modular Backends**: Pluggable storage/output systems

### Core Data Flow
```
Exchange WebSocket → Exchange Class → Normalization → Callbacks/Backends
```

## Development Commands

### Setup & Installation
```bash
# Development install with Cython compilation
python setup.py develop

# Install with all optional dependencies
pip install cryptofeed[all]

# Install specific backend dependencies
pip install cryptofeed[redis,mongo,postgres]
```

### Testing
```bash
# Run all tests
pytest tests/

# Run specific test categories
pytest tests/unit/
pytest tests/integration/

# Test with coverage
python setup.py test
```

### Code Quality
```bash
# Linting (configured in .flake8, ignores E501,F405,F403)
flake8

# Import sorting (configured in pyproject.toml, line length 130)
isort --jobs 8 ./cryptofeed

# Coverage testing (configured in .coveragerc)
python setup.py test

# Manual Cython compilation for types
python setup.py build_ext --inplace

# Check Cython compilation with assertions
# By default CYTHON_WITHOUT_ASSERTIONS is defined in setup.py
# Comment out line 40 in setup.py to enable runtime type assertions
```

### Build & Distribution
```bash
# Build wheels for multiple Python versions (uses build-wheels.sh)
./build-wheels.sh

# Simple wheel building
./wheels.sh

# Build Cython extensions in-place
python setup.py build_ext --inplace
```

## Cryptofeed-Specific Patterns & Conventions

### Exchange Implementation
- All exchanges inherit from `Exchange` base class in `/cryptofeed/exchange.py`
- Must implement: `_connect()`, `_book_snapshot()`, `_reset()`
- WebSocket message handlers use async pattern: `async def _handler(self, msg, timestamp)`
- Symbol normalization via `symbol_mapping` dictionaries

### Data Type Standards
- **Decimal**: All prices/amounts use `decimal.Decimal` for precision
- **Timestamps**: Float timestamps in Unix epoch format
- **Symbols**: Internal format like 'BTC-USD', normalized across exchanges
- **Sides**: Standardized to 'buy'/'sell' and 'bid'/'ask'

### Configuration Patterns
- Config via YAML files (see `config.yaml` example)
- Per-exchange API credentials support (key_id, key_secret, key_passphrase)
- Global settings: logging, uvloop, multiprocessing, ignore_invalid_instruments
- Backend-specific configuration available for each storage system
- Authentication supports master API keys for some exchanges (e.g., Gemini account_name)

### Callback Signatures
```python
# Standard callback patterns
async def trade_callback(trade, receipt_timestamp):
    # trade object contains: symbol, side, amount, price, timestamp, etc.

async def book_callback(book, receipt_timestamp):
    # book.book contains SortedDict of bids/asks
    # book.delta contains incremental updates
```

## Critical Dependencies & Setup

### Core Dependencies
- **websockets**: WebSocket client library (v14.1+)
- **aiohttp**: Async HTTP client for REST endpoints (v3.11.6+)
- **pyyaml**: Configuration file parsing
- **yapic.json**: High-performance JSON parsing (v1.6.3+)
- **order_book**: Specialized order book data structures (v0.6.0+)
- **Cython**: Performance-critical type system
- **requests**: HTTP client for synchronous operations (v2.18.4+)
- **aiofile**: Async file operations (v2.0.0+)

### Platform-Specific
- **uvloop**: Unix-only event loop (auto-detected)
- **aiodns**: Faster DNS resolution

### Exchange-Specific Requirements
- Some exchanges require API credentials even for public data
- Rate limiting considerations vary by exchange
- Checksum validation available for some exchanges (OKX, Kraken, etc.)

## Common Development Patterns

### Adding New Exchange
1. Create new file in `/cryptofeed/exchanges/`
2. Inherit from `Exchange` base class
3. Implement required methods and message handlers
4. Add exchange to `/cryptofeed/exchanges/__init__.py`
5. Add constants to `/cryptofeed/defines.py`
6. Create example in `/examples/`

### Testing Exchange Integration
- Use sample data in `/sample_data/` directory
- Integration tests in `/tests/integration/`
- Unit tests focus on normalization logic

### Performance Considerations
- Order book updates can be high-frequency (1000s/second)
- Use Cython types for performance-critical paths
- Callbacks should be lightweight (avoid blocking operations)
- Consider multiprocessing for backends under high load

## File Structure Deep Dive

### `/cryptofeed/` (Main Package)
- `feedhandler.py`: Central FeedHandler orchestrator
- `exchange.py`: Base exchange implementation
- `defines.py`: All string constants and data structure docs
- `types.pyx`: Cython-optimized data types
- `connection.py`: WebSocket connection management
- `symbols.py`: Symbol normalization utilities

### `/cryptofeed/exchanges/` (Exchange Implementations)
- Individual exchange files (e.g., `binance.py`, `coinbase.py`)
- `/mixins/`: Shared functionality across exchanges

### `/cryptofeed/backends/` (Output Destinations)
- Redis, MongoDB, PostgreSQL, InfluxDB, etc.
- Each backend handles different data types appropriately

### `/examples/` (Usage Examples)
- `demo.py`: Comprehensive multi-exchange example
- Backend-specific examples (Redis, Arctic, etc.)
- Authentication examples for private channels

## Debugging & Troubleshooting

### Common Issues
- **Symbol Mismatches**: Check exchange-specific symbol formats
- **Rate Limits**: Some exchanges have strict rate limits
- **SSL/TLS**: Some exchanges require specific SSL configurations
- **Timezone Handling**: All timestamps should be UTC

### Logging Configuration
```python
config = {
    'log': {
        'filename': 'cryptofeed.log',
        'level': 'DEBUG',  # DEBUG, INFO, WARNING, ERROR
        'disabled': False
    }
}
```

### Debug Tools
- `/tools/` directory contains debugging utilities
- `websockets_test.py`: Direct WebSocket testing
- `book_test.py`: Order book validation
- Raw data collection available for debugging

## Testing Philosophy
- **Unit Tests**: Focus on data normalization and utility functions
- **Integration Tests**: Test live exchange connections (when possible)
- **Mock Data**: Use `/sample_data/` for consistent testing
- **Continuous Integration**: GitHub Actions test on Python 3.10, 3.11, 3.12

## Performance Notes
- Cryptofeed can handle thousands of updates per second
- Memory usage scales with number of active order book subscriptions
- Consider using `backend_multiprocessing: True` for high-throughput scenarios
- UV loop provides significant performance benefits on Unix systems

## Security Considerations
- API credentials stored in config files (never commit these!)
- Some authenticated channels require specific permissions
- WebSocket connections may need proxy support in corporate environments
- Rate limiting is exchange-specific and should be respected

---

**Last Updated**: Sep 2025 - Based on cryptofeed v2.4.1 codebase analysis

**Recent Notable Changes (v2.4.1):**
- Coinbase transitioned from Pro to Advanced Trade API
- Bybit spot support added
- Bybit migrated to API V5 for public streams
- WebSocket library updated to v14.1+ compatibility
- Support for JSON payloads in HTTPSync connections

This guide focuses on the non-obvious, cryptofeed-specific knowledge that will help you be productive quickly when working with this codebase.