-- ClickHouse TTL 修改脚本
-- 用于调整不同K线周期的数据保留时间

-- 查看当前TTL设置
SHOW CREATE TABLE candles;

-- 修改TTL示例（根据需要调整天数）
ALTER TABLE candles MODIFY TTL
    toDateTime(timestamp) + CASE
        WHEN interval = '1m' THEN toIntervalDay(60)    -- 改为60天（原30天）
        WHEN interval = '5m' THEN toIntervalDay(180)   -- 改为180天（原90天）
        WHEN interval = '15m' THEN toIntervalDay(365)  -- 改为365天（原180天）
        WHEN interval = '30m' THEN toIntervalDay(730)  -- 改为730天（原365天）
        WHEN interval = '1h' THEN toIntervalDay(730)   -- 改为730天（原365天）
        WHEN interval = '4h' THEN toIntervalDay(1460)  -- 改为1460天（原730天）
        WHEN interval = '1d' THEN toIntervalDay(7300)  -- 改为7300天（原3650天）
        ELSE toIntervalDay(730)                        -- 默认改为730天
    END;

-- 验证TTL修改是否成功
SHOW CREATE TABLE candles;

-- 强制触发TTL清理（立即生效）
OPTIMIZE TABLE candles FINAL;