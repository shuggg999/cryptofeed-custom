-- DataGrip 查询模板（无时区标签显示）
-- 永久解决方案：使用已创建的清洁视图，时间显示为字符串格式

-- === 永久解决方案：使用视图查询（推荐） ===

-- 1. K线数据（无时区标签）
SELECT * FROM candles_clean
WHERE symbol = 'BTC-USDT-PERP'
ORDER BY raw_timestamp DESC
LIMIT 100;

-- 2. 交易数据（无时区标签）
SELECT * FROM trades_clean
WHERE symbol = 'BTC-USDT-PERP'
ORDER BY raw_timestamp DESC
LIMIT 100;

-- === 临时解决方案：手动格式化时间 ===

-- 1. 查看K线数据（格式化时间为字符串，避免时区标签）
SELECT
    toString(timestamp) as time,
    exchange,
    symbol,
    interval,
    open,
    high,
    low,
    close,
    volume,
    trades
FROM cryptofeed.candles
WHERE symbol = 'BTC-USDT-PERP'
ORDER BY timestamp DESC
LIMIT 100;

-- 2. 查看交易数据（格式化时间）
SELECT
    toString(timestamp) as time,
    exchange,
    symbol,
    side,
    amount,
    price,
    trade_id
FROM cryptofeed.trades
WHERE symbol = 'BTC-USDT-PERP'
ORDER BY timestamp DESC
LIMIT 100;

-- === 视图创建脚本（已执行） ===
-- CREATE VIEW IF NOT EXISTS candles_clean AS
-- SELECT toString(timestamp) as time, exchange, symbol, interval, open, high, low, close, volume, trades, timestamp as raw_timestamp FROM candles;
--
-- CREATE VIEW IF NOT EXISTS trades_clean AS
-- SELECT toString(timestamp) as time, exchange, symbol, side, amount, price, trade_id, toString(receipt_timestamp) as receipt_time, timestamp as raw_timestamp FROM trades;