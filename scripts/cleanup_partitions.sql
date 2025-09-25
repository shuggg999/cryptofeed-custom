-- 基于分区的高效数据清理脚本
-- 直接删除整个分区，比DELETE快1000倍

-- 计算需要删除的分区
-- 删除90天前的trades分区
DO $$
DECLARE
    partition_name TEXT;
    cutoff_date DATE;
BEGIN
    -- 计算90天前的日期
    cutoff_date := CURRENT_DATE - INTERVAL '90 days';

    -- 查找并删除过期的trades分区
    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename ~ '^trades_\d{4}_\d{2}$'
        AND TO_DATE(SUBSTRING(tablename FROM 8), 'YYYY_MM') < cutoff_date
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', partition_name);
        RAISE NOTICE 'Dropped partition: %', partition_name;
    END LOOP;
END $$;

-- 删除30天前的1分钟K线分区
DO $$
DECLARE
    partition_name TEXT;
    cutoff_date DATE;
BEGIN
    cutoff_date := CURRENT_DATE - INTERVAL '30 days';

    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename ~ '^candles_1m_\d{4}_\d{2}$'
        AND TO_DATE(SUBSTRING(tablename FROM 12), 'YYYY_MM') < cutoff_date
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', partition_name);
        RAISE NOTICE 'Dropped 1m candle partition: %', partition_name;
    END LOOP;
END $$;

-- 删除90天前的5分钟K线分区
DO $$
DECLARE
    partition_name TEXT;
    cutoff_date DATE;
BEGIN
    cutoff_date := CURRENT_DATE - INTERVAL '90 days';

    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename ~ '^candles_5m_\d{4}_\d{2}$'
        AND TO_DATE(SUBSTRING(tablename FROM 12), 'YYYY_MM') < cutoff_date
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', partition_name);
        RAISE NOTICE 'Dropped 5m candle partition: %', partition_name;
    END LOOP;
END $$;

-- 删除365天前的30分钟K线分区
DO $$
DECLARE
    partition_name TEXT;
    cutoff_date DATE;
BEGIN
    cutoff_date := CURRENT_DATE - INTERVAL '365 days';

    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename ~ '^candles_30m_\d{4}_\d{2}$'
        AND TO_DATE(SUBSTRING(tablename FROM 13), 'YYYY_MM') < cutoff_date
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', partition_name);
        RAISE NOTICE 'Dropped 30m candle partition: %', partition_name;
    END LOOP;
END $$;

-- 删除730天前的4小时K线分区
DO $$
DECLARE
    partition_name TEXT;
    cutoff_date DATE;
BEGIN
    cutoff_date := CURRENT_DATE - INTERVAL '730 days';

    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename ~ '^candles_4h_\d{4}_\d{2}$'
        AND TO_DATE(SUBSTRING(tablename FROM 12), 'YYYY_MM') < cutoff_date
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', partition_name);
        RAISE NOTICE 'Dropped 4h candle partition: %', partition_name;
    END LOOP;
END $$;

-- 处理funding表（按年分区）
DO $$
DECLARE
    partition_name TEXT;
    cutoff_year INT;
BEGIN
    cutoff_year := EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '365 days');

    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename ~ '^funding_\d{4}$'
        AND SUBSTRING(tablename FROM 9)::INT < cutoff_year
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', partition_name);
        RAISE NOTICE 'Dropped funding partition: %', partition_name;
    END LOOP;
END $$;

-- 清理空分区（可选）
-- 删除没有数据的分区表
DO $$
DECLARE
    partition_name TEXT;
    row_count INT;
BEGIN
    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND (tablename ~ '^trades_\d{4}_\d{2}$'
             OR tablename ~ '^candles_\w+_\d{4}_\d{2}$')
    LOOP
        -- 检查分区是否为空
        EXECUTE format('SELECT COUNT(*) FROM %I', partition_name) INTO row_count;

        IF row_count = 0 THEN
            EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', partition_name);
            RAISE NOTICE 'Dropped empty partition: %', partition_name;
        END IF;
    END LOOP;
END $$;

-- 显示清理后的统计信息
SELECT
    COUNT(CASE WHEN tablename ~ '^trades_' THEN 1 END) as trades_partitions,
    COUNT(CASE WHEN tablename ~ '^candles_1m_' THEN 1 END) as candles_1m_partitions,
    COUNT(CASE WHEN tablename ~ '^candles_5m_' THEN 1 END) as candles_5m_partitions,
    COUNT(CASE WHEN tablename ~ '^candles_30m_' THEN 1 END) as candles_30m_partitions,
    COUNT(CASE WHEN tablename ~ '^candles_4h_' THEN 1 END) as candles_4h_partitions,
    COUNT(CASE WHEN tablename ~ '^funding_' THEN 1 END) as funding_partitions
FROM pg_tables
WHERE schemaname = 'public';

-- 显示剩余数据统计
SELECT 'Cleanup completed at: ' || NOW() as status;