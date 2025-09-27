-- 智能回填状态管理表
-- 用于记录每个数据源的状态，实现精确的缺口补充

-- 删除旧表（如果存在）
DROP TABLE IF EXISTS backfill_status;
DROP TABLE IF EXISTS gap_detection_log;

-- 创建回填状态表
CREATE TABLE backfill_status (
    symbol String COMMENT '交易对',
    interval String COMMENT '时间间隔',
    last_check_time DateTime DEFAULT now() COMMENT '最后检查时间',
    last_data_time Nullable(DateTime) COMMENT '最新数据时间',
    oldest_data_time Nullable(DateTime) COMMENT '最老数据时间',
    expected_start_time DateTime COMMENT '期望开始时间（根据保留策略）',
    data_count UInt64 DEFAULT 0 COMMENT '数据记录数',
    gap_type String DEFAULT 'none' COMMENT '缺口类型: urgent/recent/historical/none',
    priority Int8 DEFAULT 0 COMMENT '回填优先级 1-10',
    status String DEFAULT 'complete' COMMENT '状态: complete/partial/missing',
    last_backfill_time Nullable(DateTime) COMMENT '最后回填时间',
    continuous_check_count UInt32 DEFAULT 0 COMMENT '连续检查次数',
    last_gap_count UInt32 DEFAULT 0 COMMENT '最后检查的缺口数量',
    created_at DateTime DEFAULT now() COMMENT '创建时间',
    updated_at DateTime DEFAULT now() COMMENT '更新时间'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (symbol, interval)
SETTINGS index_granularity = 8192
COMMENT '回填状态表，记录每个数据源的检查状态和缺口信息';

-- 创建缺口检测日志表
CREATE TABLE gap_detection_log (
    log_id UInt64 COMMENT '日志ID',
    symbol String COMMENT '交易对',
    interval String COMMENT '时间间隔',
    gap_start DateTime COMMENT '缺口开始时间',
    gap_end DateTime COMMENT '缺口结束时间',
    gap_type String COMMENT '缺口类型',
    priority Int8 COMMENT '优先级',
    status String DEFAULT 'detected' COMMENT '状态: detected/filling/completed/failed',
    records_expected UInt32 DEFAULT 0 COMMENT '预期记录数',
    records_filled UInt32 DEFAULT 0 COMMENT '已填充记录数',
    detection_time DateTime DEFAULT now() COMMENT '检测时间',
    fill_start_time Nullable(DateTime) COMMENT '开始填充时间',
    fill_complete_time Nullable(DateTime) COMMENT '填充完成时间',
    error_message String DEFAULT '' COMMENT '错误信息',
    retry_count UInt8 DEFAULT 0 COMMENT '重试次数'
) ENGINE = MergeTree()
ORDER BY (detection_time, priority DESC)
PARTITION BY toYYYYMM(detection_time)
TTL detection_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192
COMMENT '缺口检测日志表，记录每个发现的数据缺口和处理状态';

-- 创建实时监控表（用于监控最近的数据完整性）
CREATE TABLE real_time_monitor (
    symbol String COMMENT '交易对',
    interval String COMMENT '时间间隔',
    last_websocket_time Nullable(DateTime) COMMENT '最后WebSocket数据时间',
    last_rest_check_time Nullable(DateTime) COMMENT '最后REST检查时间',
    gap_detected_time Nullable(DateTime) COMMENT '检测到缺口的时间',
    gap_filled_time Nullable(DateTime) COMMENT '缺口填充时间',
    consecutive_gaps UInt32 DEFAULT 0 COMMENT '连续缺口次数',
    status String DEFAULT 'normal' COMMENT '状态: normal/gap_detected/filling/network_issue',
    updated_at DateTime DEFAULT now() COMMENT '更新时间'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (symbol, interval)
SETTINGS index_granularity = 8192
COMMENT '实时监控表，追踪WebSocket和REST数据的连接状态';

-- 创建索引以优化查询
ALTER TABLE backfill_status ADD INDEX idx_priority (priority) TYPE minmax GRANULARITY 1;
ALTER TABLE backfill_status ADD INDEX idx_gap_type (gap_type) TYPE set(0) GRANULARITY 1;
ALTER TABLE gap_detection_log ADD INDEX idx_status (status) TYPE set(0) GRANULARITY 1;

-- 创建视图：当前需要填充的缺口（按优先级排序）
CREATE OR REPLACE VIEW pending_gaps AS
SELECT
    log_id,
    symbol,
    interval,
    gap_start,
    gap_end,
    gap_type,
    priority,
    records_expected,
    detection_time,
    retry_count
FROM gap_detection_log
WHERE status = 'detected'
   OR (status = 'failed' AND retry_count < 3)
ORDER BY priority DESC, detection_time;

-- 创建视图：数据源健康状态
CREATE OR REPLACE VIEW data_health_status AS
SELECT
    bs.symbol,
    bs.interval,
    bs.status,
    bs.gap_type,
    bs.priority,
    bs.last_data_time,
    bs.last_check_time,
    rtm.last_websocket_time,
    rtm.consecutive_gaps,
    rtm.status as monitor_status,
    CASE
        WHEN bs.status = 'complete' AND rtm.status = 'normal' THEN 'healthy'
        WHEN bs.gap_type = 'urgent' OR rtm.consecutive_gaps > 2 THEN 'critical'
        WHEN bs.gap_type = 'recent' OR rtm.status = 'gap_detected' THEN 'warning'
        ELSE 'normal'
    END as health_level
FROM backfill_status bs
LEFT JOIN real_time_monitor rtm ON bs.symbol = rtm.symbol AND bs.interval = rtm.interval
ORDER BY health_level DESC, bs.priority DESC;

-- 创建视图：回填统计
CREATE OR REPLACE VIEW backfill_statistics AS
SELECT
    gap_type,
    status,
    COUNT(*) as gap_count,
    SUM(records_expected) as total_expected,
    SUM(records_filled) as total_filled,
    AVG(priority) as avg_priority,
    MIN(detection_time) as earliest_detection,
    MAX(detection_time) as latest_detection
FROM gap_detection_log
WHERE detection_time >= now() - INTERVAL 24 HOUR
GROUP BY gap_type, status
ORDER BY gap_type, status;