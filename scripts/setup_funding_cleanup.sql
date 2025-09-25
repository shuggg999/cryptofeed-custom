-- Funding表30天滚动窗口配置脚本
-- 保持funding表数据量在可控范围内

-- 1.txt. 创建自动清理函数
CREATE OR REPLACE FUNCTION cleanup_old_funding()
RETURNS void AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- 删除超过30天的数据
    DELETE FROM funding
    WHERE timestamp < NOW() - INTERVAL '30 days';

    GET DIAGNOSTICS deleted_count = ROW_COUNT;

    -- 记录清理日志
    IF deleted_count > 0 THEN
        RAISE NOTICE 'Cleaned % old funding records at %', deleted_count, NOW();
    END IF;

    -- 更新表统计信息以优化查询
    ANALYZE funding;
END;
$$ LANGUAGE plpgsql;

-- 2. 手动执行一次清理（清理现有的旧数据）
SELECT cleanup_old_funding();

-- 3. 查看清理后的数据情况
SELECT
    'After cleanup' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as symbols,
    MIN(timestamp) as oldest_data,
    MAX(timestamp) as newest_data,
    pg_size_pretty(pg_total_relation_size('funding')) as table_size
FROM funding;

-- 4. 创建定期清理任务（如果有pg_cron扩展）
-- 如果没有pg_cron，需要使用外部cron或手动定期执行
/*
-- 每天凌晨3点执行清理
SELECT cron.schedule(
    'cleanup_funding_daily',
    '0 3 * * *',
    'SELECT cleanup_old_funding()'
);
*/

-- 5. 创建索引优化查询性能
CREATE INDEX IF NOT EXISTS idx_funding_timestamp ON funding(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_funding_symbol_timestamp ON funding(symbol, timestamp DESC);

-- 6. 提示信息
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Funding表30天滚动窗口配置完成 ===';
    RAISE NOTICE '1.txt. 已创建自动清理函数 cleanup_old_funding()';
    RAISE NOTICE '2. 已清理超过30天的旧数据';
    RAISE NOTICE '3. 已创建优化索引';
    RAISE NOTICE '';
    RAISE NOTICE '建议：';
    RAISE NOTICE '- 每天执行一次 SELECT cleanup_old_funding();';
    RAISE NOTICE '- 或配置cron定时任务自动执行';
    RAISE NOTICE '';
END$$;