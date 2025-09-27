-- ClickHouse 数据量统计查询

-- 1. 按标的和K线周期统计数据量
SELECT
    symbol,
    interval,
    count() as record_count,
    min(time) as earliest_time,
    max(time) as latest_time
FROM candles_clean
GROUP BY symbol, interval
ORDER BY symbol, interval;

-- 2. 总体数据量概览
SELECT
    count() as total_candles,
    uniq(symbol) as unique_symbols,
    uniq(interval) as unique_intervals,
    min(time) as earliest_record,
    max(time) as latest_record
FROM candles_clean;

-- 3. 各标的总数据量排序
SELECT
    symbol,
    count() as total_records,
    uniq(interval) as interval_count
FROM candles_clean
GROUP BY symbol
ORDER BY total_records DESC;

-- 4. 各K线周期数据量统计
SELECT
    interval,
    count() as record_count,
    uniq(symbol) as symbol_count,
    min(time) as earliest,
    max(time) as latest
FROM candles_clean
GROUP BY interval
ORDER BY
    CASE interval
        WHEN '1m' THEN 1
        WHEN '5m' THEN 2
        WHEN '15m' THEN 3
        WHEN '30m' THEN 4
        WHEN '1h' THEN 5
        WHEN '4h' THEN 6
        WHEN '1d' THEN 7
        ELSE 99
    END;

-- 5. 按日期统计数据量（最近7天）
SELECT
    toDate(raw_timestamp) as date,
    count() as daily_records,
    uniq(symbol) as symbols_count,
    uniq(interval) as intervals_count
FROM candles_clean
WHERE raw_timestamp >= now() - INTERVAL 7 DAY
GROUP BY date
ORDER BY date DESC;

-- 6. 数据完整性检查（查看是否有缺失的时间段）
SELECT
    symbol,
    interval,
    count() as actual_records,
    dateDiff('minute', min(raw_timestamp), max(raw_timestamp)) as total_minutes,
    CASE interval
        WHEN '1m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp))
        WHEN '5m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp)) / 5
        WHEN '15m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp)) / 15
        WHEN '30m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp)) / 30
        WHEN '1h' THEN dateDiff('hour', min(raw_timestamp), max(raw_timestamp))
        WHEN '4h' THEN dateDiff('hour', min(raw_timestamp), max(raw_timestamp)) / 4
        WHEN '1d' THEN dateDiff('day', min(raw_timestamp), max(raw_timestamp))
        ELSE 0
    END as expected_records,
    round(actual_records * 100.0 / CASE interval
        WHEN '1m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp))
        WHEN '5m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp)) / 5
        WHEN '15m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp)) / 15
        WHEN '30m' THEN dateDiff('minute', min(raw_timestamp), max(raw_timestamp)) / 30
        WHEN '1h' THEN dateDiff('hour', min(raw_timestamp), max(raw_timestamp))
        WHEN '4h' THEN dateDiff('hour', min(raw_timestamp), max(raw_timestamp)) / 4
        WHEN '1d' THEN dateDiff('day', min(raw_timestamp), max(raw_timestamp))
        ELSE 1
    END, 2) as completeness_percent
FROM candles_clean
GROUP BY symbol, interval
ORDER BY symbol, interval;