-- ClickHouse Schema for Cryptofeed
-- 优化存储和查询性能，支持自动TTL

-- 创建数据库
CREATE DATABASE IF NOT EXISTS cryptofeed;
USE cryptofeed;

-- 1. 交易数据表（高压缩率，自动过期）
CREATE TABLE IF NOT EXISTS trades (
    timestamp DateTime64(3),
    exchange String,
    symbol String,
    side Enum8('buy' = 1, 'sell' = 2),
    amount Decimal64(8),
    price Decimal64(8),
    trade_id String,
    receipt_timestamp DateTime64(3) DEFAULT now64(3),
    date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY  -- 自动删除90天前的数据
SETTINGS index_granularity = 8192;

-- 2. K线数据表（分级存储，不同周期不同TTL，自动去重）
CREATE TABLE IF NOT EXISTS candles (
    timestamp DateTime64(3),
    exchange String,
    symbol String,
    interval String,
    open Float64,      -- 统一使用Float64，简单高效
    high Float64,      -- 支持任意大小的价格和交易量
    low Float64,       -- 无需精度验证，行业标准做法
    close Float64,
    volume Float64,    -- 完美兼容DOGE等meme币的大交易量
    trades UInt32      -- 交易次数字段，用于市场微观结构分析
) ENGINE = ReplacingMergeTree()
PARTITION BY (interval, toYYYYMM(timestamp))
ORDER BY (symbol, exchange, interval, timestamp)
TTL toDateTime(timestamp) + CASE
    WHEN interval = '1m' THEN toIntervalDay(30)    -- 1分钟K线保留30天
    WHEN interval = '5m' THEN toIntervalDay(90)    -- 5分钟K线保留90天
    WHEN interval = '15m' THEN toIntervalDay(180)  -- 15分钟K线保留180天
    WHEN interval = '30m' THEN toIntervalDay(365)  -- 30分钟K线保留1年
    WHEN interval = '1h' THEN toIntervalDay(365)   -- 1小时K线保留1年
    WHEN interval = '4h' THEN toIntervalDay(730)   -- 4小时K线保留2年
    WHEN interval = '1d' THEN toIntervalDay(3650)  -- 日K线保留10年
    ELSE toIntervalDay(365)
END
SETTINGS index_granularity = 8192;

-- 3. 资金费率表
CREATE TABLE IF NOT EXISTS funding (
    timestamp DateTime64(3),
    exchange String,
    symbol String,
    rate Decimal64(8),
    mark_price Decimal64(8),
    next_funding_time DateTime,
    predicted_rate Decimal64(8),
    receipt_timestamp DateTime64(3) DEFAULT now64(3),
    date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 365 DAY  -- 资金费率保留1年
SETTINGS index_granularity = 8192;

-- 4. 清算数据表
CREATE TABLE IF NOT EXISTS liquidations (
    timestamp DateTime64(3),
    exchange String,
    symbol String,
    side Enum8('buy' = 1, 'sell' = 2),
    quantity Decimal64(8),
    price Decimal64(8),
    liquidation_id String,
    receipt_timestamp DateTime64(3) DEFAULT now64(3),
    date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 180 DAY  -- 清算数据保留180天
SETTINGS index_granularity = 8192;

-- 5. 持仓量数据表
CREATE TABLE IF NOT EXISTS open_interest (
    timestamp DateTime64(3),
    exchange String,
    symbol String,
    open_interest Decimal64(8),
    receipt_timestamp DateTime64(3) DEFAULT now64(3),
    date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 365 DAY  -- 持仓量保留1年
SETTINGS index_granularity = 8192;

-- 创建物化视图：自动聚合生成更高级别K线（节省存储）
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1h_mv
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 730 DAY
AS SELECT
    toStartOfHour(timestamp) as timestamp,
    symbol,
    exchange,
    '1h' as interval,
    argMin(open, timestamp) as open,
    max(high) as high,
    min(low) as low,
    argMax(close, timestamp) as close,
    sum(volume) as volume,
    count() as trades,
    now64(3) as receipt_timestamp,
    toDate(timestamp) as date
FROM candles
WHERE interval = '5m'
GROUP BY toStartOfHour(timestamp), symbol, exchange;

-- 创建统计表（用于快速查询）
CREATE TABLE IF NOT EXISTS symbol_stats (
    date Date,
    symbol String,
    total_volume Decimal64(8),
    total_trades UInt64,
    avg_price Decimal64(8),
    price_volatility Decimal64(8)
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, date);

-- 查询示例
-- 1. 获取最新价格
-- SELECT symbol, max(timestamp) as latest_time, argMax(close, timestamp) as latest_price
-- FROM candles
-- WHERE interval = '1m' AND timestamp > now() - INTERVAL 1 HOUR
-- GROUP BY symbol;

-- 2. 计算24小时交易量
-- SELECT symbol, sum(volume) as volume_24h
-- FROM trades
-- WHERE timestamp > now() - INTERVAL 24 HOUR
-- GROUP BY symbol
-- ORDER BY volume_24h DESC;

-- 3. 获取K线数据（极速查询）
-- SELECT * FROM candles
-- WHERE symbol = 'BTC-USDT-PERP'
--   AND interval = '1h'
--   AND timestamp >= '2025-01-01 00:00:00'
-- ORDER BY timestamp;