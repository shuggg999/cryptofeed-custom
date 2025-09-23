#!/bin/bash
# 每日维护脚本 - funding表清理

echo "开始执行funding表30天滚动维护..."
echo "时间: $(date)"

# 连接数据库并执行清理
docker exec timescale-crypto psql -U postgres -d cryptofeed -c "SELECT cleanup_old_funding();"

# 检查当前数据量
echo "当前funding表状态:"
docker exec timescale-crypto psql -U postgres -d cryptofeed -c "
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as symbols,
    MIN(timestamp) as oldest_data,
    MAX(timestamp) as newest_data,
    pg_size_pretty(pg_total_relation_size('funding')) as table_size
FROM funding;
"

echo "维护完成: $(date)"