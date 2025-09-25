-- TimescaleDB 数据保留策略设置脚本
-- 连接到 cryptofeed 数据库后执行此脚本

\c cryptofeed;

-- 检查当前的保留策略
SELECT * FROM timescaledb_information.data_retention_policies;

-- 设置交易数据保留策略（90天）
SELECT add_retention_policy('trades', INTERVAL '90 days');

-- 设置K线数据保留策略（按时间周期差异化）
SELECT add_retention_policy('candles_1m', INTERVAL '30 days');   -- 1分钟K线保留30天
SELECT add_retention_policy('candles_5m', INTERVAL '90 days');   -- 5分钟K线保留90天
SELECT add_retention_policy('candles_15m', INTERVAL '180 days'); -- 15分钟K线保留180天
SELECT add_retention_policy('candles_30m', INTERVAL '365 days'); -- 30分钟K线保留1年
SELECT add_retention_policy('candles_4h', INTERVAL '730 days');  -- 4小时K线保留2年
SELECT add_retention_policy('candles_1d', INTERVAL '1095 days'); -- 日K线保留3年

-- 设置资金费率保留策略（1年）
SELECT add_retention_policy('funding', INTERVAL '365 days');

-- 设置清算数据保留策略（180天）
SELECT add_retention_policy('liquidations', INTERVAL '180 days');

-- 设置持仓量保留策略（1年）
SELECT add_retention_policy('open_interest', INTERVAL '365 days');

-- 查看设置结果
SELECT * FROM timescaledb_information.data_retention_policies;

-- 设置保留策略执行时间（每天凌晨2点）
-- TimescaleDB会自动管理，无需额外配置

PRINT '数据保留策略设置完成！';
PRINT '系统将自动按策略清理过期数据，无需程序干预。';