#!/bin/bash
# Cryptofeed Docker 快速启动脚本

set -e  # 遇到错误立即退出

echo "🐳 Starting Cryptofeed Docker Stack..."

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# 启动ClickHouse数据库
echo "📊 Starting ClickHouse database..."
docker-compose up -d clickhouse

# 等待数据库启动并检查健康状态
echo "⏳ Waiting for ClickHouse to be ready..."
for i in {1..30}; do
    if docker-compose exec clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
        echo "✅ ClickHouse is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ ClickHouse failed to start within 30 seconds"
        docker-compose logs clickhouse
        exit 1
    fi
    sleep 1
    echo -n "."
done

# 启动主监控应用
echo "🚀 Starting Cryptofeed monitor..."
docker-compose up -d cryptofeed-monitor

# 等待应用启动
echo "⏳ Waiting for monitor to be ready..."
sleep 5

# 显示服务状态
echo ""
echo "📋 Service Status:"
docker-compose ps

echo ""
echo "✅ Cryptofeed Stack Started Successfully!"
echo ""
echo "🔍 Available services:"
echo "  - ClickHouse Database: http://localhost:8123"
echo "  - Cryptofeed Monitor: http://localhost:8080"
echo ""
echo "📝 Useful commands:"
echo "  docker-compose logs -f cryptofeed-monitor  # View monitor logs"
echo "  docker-compose logs -f clickhouse          # View database logs"
echo "  docker-compose exec clickhouse clickhouse-client --database cryptofeed  # Connect to database"
echo ""
echo "⏹  Stop all services:"
echo "  docker-compose down"