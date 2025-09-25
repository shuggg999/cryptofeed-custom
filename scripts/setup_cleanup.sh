#!/bin/bash
# 设置自动数据清理
# 每天凌晨2点执行数据清理

# 脚本目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 日志文件
LOG_FILE="$PROJECT_ROOT/logs/data_cleanup.log"

# 确保日志目录存在
mkdir -p "$PROJECT_ROOT/logs"

# 创建清理脚本
cat > "$SCRIPT_DIR/run_cleanup.sh" << 'EOF'
#!/bin/bash
# 执行数据库清理

# 配置
DB_HOST="127.0.0.1"
DB_PORT="5432"
DB_NAME="cryptofeed"
DB_USER="postgres"
DB_PASSWORD="password"

# 脚本目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_ROOT/logs/data_cleanup.log"

# 记录开始时间
echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting data cleanup..." >> "$LOG_FILE"

# 执行清理SQL
if docker exec timescale-crypto psql -U postgres -d cryptofeed -f /scripts/cleanup_data.sql >> "$LOG_FILE" 2>&1; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Data cleanup completed successfully" >> "$LOG_FILE"

    # 记录存储空间使用情况
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Database size after cleanup:" >> "$LOG_FILE"
    docker exec timescale-crypto psql -U postgres -d cryptofeed -c "
        SELECT
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT 10;
    " >> "$LOG_FILE" 2>&1
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Data cleanup failed" >> "$LOG_FILE"
    exit 1
fi

# 清理旧日志文件（保留30天）
find "$PROJECT_ROOT/logs" -name "*.log" -type f -mtime +30 -delete 2>/dev/null

echo "$(date '+%Y-%m-%d %H:%M:%S') - Cleanup script finished" >> "$LOG_FILE"
EOF

# 使清理脚本可执行
chmod +x "$SCRIPT_DIR/run_cleanup.sh"

# 设置cron作业（每天凌晨2点执行）
CRON_JOB="0 2 * * * $SCRIPT_DIR/run_cleanup.sh"

# 检查是否已经添加了这个cron作业
if ! crontab -l 2>/dev/null | grep -q "$SCRIPT_DIR/run_cleanup.sh"; then
    # 添加到crontab
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "✅ Cron job added: Daily cleanup at 2:00 AM"
    echo "📝 Log file: $LOG_FILE"
else
    echo "⚠️  Cron job already exists"
fi

# 显示当前cron作业
echo "Current cron jobs:"
crontab -l

# 创建手动执行脚本
cat > "$SCRIPT_DIR/manual_cleanup.sh" << 'EOF'
#!/bin/bash
# 手动执行数据清理

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "🗑️  Starting manual data cleanup..."
"$SCRIPT_DIR/run_cleanup.sh"

if [ $? -eq 0 ]; then
    echo "✅ Manual cleanup completed successfully"

    # 显示清理后的统计信息
    echo "📊 Current data statistics:"
    docker exec timescale-crypto psql -U postgres -d cryptofeed -c "
        SELECT
            'trades' as table_name,
            COUNT(*) as record_count,
            pg_size_pretty(pg_total_relation_size('trades')) as table_size,
            MIN(timestamp) as oldest_record,
            MAX(timestamp) as newest_record
        FROM trades
        UNION ALL
        SELECT
            'funding' as table_name,
            COUNT(*) as record_count,
            pg_size_pretty(pg_total_relation_size('funding')) as table_size,
            MIN(timestamp) as oldest_record,
            MAX(timestamp) as newest_record
        FROM funding
        UNION ALL
        SELECT
            'candles_1m' as table_name,
            COUNT(*) as record_count,
            pg_size_pretty(pg_total_relation_size('candles_1m')) as table_size,
            MIN(timestamp) as oldest_record,
            MAX(timestamp) as newest_record
        FROM candles_1m
        ORDER BY table_name;
    "
else
    echo "❌ Manual cleanup failed"
    exit 1
fi
EOF

chmod +x "$SCRIPT_DIR/manual_cleanup.sh"

echo "🎯 Setup completed!"
echo "✅ Automatic cleanup: Every day at 2:00 AM"
echo "✅ Manual cleanup: $SCRIPT_DIR/manual_cleanup.sh"
echo "📝 Logs: $LOG_FILE"