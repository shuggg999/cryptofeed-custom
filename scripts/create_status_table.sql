-- ClickHouse数据状态跟踪表
-- 用于记录每个数据源的检查状态和回填优先级

-- 删除旧表（如果存在）
DROP TABLE IF EXISTS data_status;

-- 创建数据状态表
CREATE TABLE data_status (
    symbol String COMMENT '交易对',
    interval String COMMENT '时间间隔',
    last_check_time DateTime DEFAULT now() COMMENT '最后检查时间',
    last_data_time Nullable(DateTime) COMMENT '最新数据时间',
    oldest_data_time Nullable(DateTime) COMMENT '最老数据时间',
    expected_start_time DateTime COMMENT '期望开始时间（根据保留策略）',
    data_count UInt64 DEFAULT 0 COMMENT '数据记录数',
    data_completeness Float32 DEFAULT 0 COMMENT '数据完整度 0-100%',
    status String DEFAULT 'unknown' COMMENT '状态: complete/partial/missing/checking',
    gap_type String DEFAULT '' COMMENT '缺口类型: urgent/recent/historical/none',
    priority Int8 DEFAULT 0 COMMENT '回填优先级 1-10',
    last_backfill_time Nullable(DateTime) COMMENT '最后回填时间',
    backfill_status String DEFAULT '' COMMENT '回填状态: pending/running/completed/failed',
    error_count UInt32 DEFAULT 0 COMMENT '错误次数',
    notes String DEFAULT '' COMMENT '备注信息',
    created_at DateTime DEFAULT now() COMMENT '创建时间',
    updated_at DateTime DEFAULT now() COMMENT '更新时间'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (symbol, interval)
SETTINGS index_granularity = 8192
COMMENT '数据状态跟踪表，用于智能回填系统';

-- 创建回填任务队列表
DROP TABLE IF EXISTS backfill_queue;

CREATE TABLE backfill_queue (
    task_id UUID DEFAULT generateUUIDv4() COMMENT '任务ID',
    symbol String COMMENT '交易对',
    interval String COMMENT '时间间隔',
    start_time DateTime COMMENT '回填开始时间',
    end_time DateTime COMMENT '回填结束时间',
    gap_type String COMMENT '缺口类型: urgent/recent/historical',
    priority Int8 COMMENT '优先级 1-10',
    status String DEFAULT 'pending' COMMENT '状态: pending/running/completed/failed/cancelled',
    retry_count UInt8 DEFAULT 0 COMMENT '重试次数',
    max_retries UInt8 DEFAULT 3 COMMENT '最大重试次数',
    records_expected UInt32 DEFAULT 0 COMMENT '预期记录数',
    records_inserted UInt32 DEFAULT 0 COMMENT '实际插入数',
    error_message String DEFAULT '' COMMENT '错误信息',
    created_at DateTime DEFAULT now() COMMENT '创建时间',
    started_at Nullable(DateTime) COMMENT '开始执行时间',
    completed_at Nullable(DateTime) COMMENT '完成时间',
    next_retry_at Nullable(DateTime) COMMENT '下次重试时间'
) ENGINE = MergeTree()
ORDER BY (priority DESC, created_at)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 30 DAY
SETTINGS index_granularity = 8192
COMMENT '回填任务队列表，按优先级排序执行';

-- 创建回填历史记录表（用于分析和审计）
DROP TABLE IF EXISTS backfill_history;

CREATE TABLE backfill_history (
    task_id UUID COMMENT '任务ID',
    symbol String COMMENT '交易对',
    interval String COMMENT '时间间隔',
    start_time DateTime COMMENT '回填开始时间',
    end_time DateTime COMMENT '回填结束时间',
    gap_type String COMMENT '缺口类型',
    priority Int8 COMMENT '优先级',
    status String COMMENT '最终状态',
    records_inserted UInt32 COMMENT '插入记录数',
    duration_seconds UInt32 COMMENT '执行时长（秒）',
    api_calls UInt32 DEFAULT 0 COMMENT 'API调用次数',
    bytes_downloaded UInt64 DEFAULT 0 COMMENT '下载字节数',
    completed_at DateTime COMMENT '完成时间'
) ENGINE = MergeTree()
ORDER BY completed_at
PARTITION BY toYYYYMM(completed_at)
TTL completed_at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192
COMMENT '回填历史记录表，用于性能分析和审计';

-- 创建索引以优化查询
ALTER TABLE data_status ADD INDEX idx_status (status) TYPE set(0) GRANULARITY 1;
ALTER TABLE data_status ADD INDEX idx_priority (priority) TYPE minmax GRANULARITY 1;
ALTER TABLE backfill_queue ADD INDEX idx_status (status) TYPE set(0) GRANULARITY 1;

-- 创建视图：当前需要回填的数据源
CREATE OR REPLACE VIEW pending_backfills AS
SELECT
    symbol,
    interval,
    gap_type,
    priority,
    data_completeness,
    last_check_time
FROM data_status
WHERE status IN ('partial', 'missing')
  AND priority > 0
ORDER BY priority DESC, last_check_time;

-- 创建视图：回填任务执行状态
CREATE OR REPLACE VIEW backfill_status AS
SELECT
    status,
    gap_type,
    COUNT(*) as task_count,
    AVG(priority) as avg_priority
FROM backfill_queue
WHERE created_at >= now() - INTERVAL 24 HOUR
GROUP BY status, gap_type
ORDER BY status, gap_type;