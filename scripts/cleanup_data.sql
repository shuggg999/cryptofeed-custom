-- 自动数据清理脚本
-- 基于配置的保留策略删除过期数据

-- 交易数据清理（保留90天）
DELETE FROM trades
WHERE timestamp < NOW() - INTERVAL '90 days';

-- 资金费率清理（保留365天）
DELETE FROM funding
WHERE timestamp < NOW() - INTERVAL '365 days';

-- 清算数据清理（保留180天）
DELETE FROM liquidations
WHERE timestamp < NOW() - INTERVAL '180 days';

-- 持仓量数据清理（保留365天）
DELETE FROM open_interest
WHERE timestamp < NOW() - INTERVAL '365 days';

-- K线数据分级清理
-- 1分钟K线（保留30天）
DELETE FROM candles_1m
WHERE timestamp < NOW() - INTERVAL '30 days';

-- 5分钟K线（保留90天）
DELETE FROM candles_5m
WHERE timestamp < NOW() - INTERVAL '90 days';

-- 30分钟K线（保留365天）
DELETE FROM candles_30m
WHERE timestamp < NOW() - INTERVAL '365 days';

-- 4小时K线（保留730天）
DELETE FROM candles_4h
WHERE timestamp < NOW() - INTERVAL '730 days';

-- 日K线（保留3650天，即10年）
DELETE FROM candles_1d
WHERE timestamp < NOW() - INTERVAL '3650 days';

-- 通用candles表清理（如果存在）
DELETE FROM candles
WHERE timestamp < NOW() - INTERVAL '90 days';

-- 数据gap日志清理（保留30天）
DELETE FROM data_gaps
WHERE created_at < NOW() - INTERVAL '30 days';

-- 输出清理结果
SELECT
    'Data cleanup completed at: ' || NOW() as cleanup_status,
    (SELECT COUNT(*) FROM trades) as trades_count,
    (SELECT COUNT(*) FROM funding) as funding_count,
    (SELECT COUNT(*) FROM candles_1m) as candles_1m_count,
    (SELECT COUNT(*) FROM candles_5m) as candles_5m_count,
    (SELECT COUNT(*) FROM liquidations) as liquidations_count,
    (SELECT COUNT(*) FROM open_interest) as open_interest_count;