#!/usr/bin/env python3
"""
健康检查和监控系统
提供HTTP健康检查端点和系统监控
"""
import asyncio
import json
import logging
import psutil
import psycopg2
import threading
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, parse_qs

from ..config import config

logger = logging.getLogger(__name__)


class HealthMonitor:
    """健康监控器"""

    def __init__(self):
        self.enabled = config.get('monitoring.metrics_enabled', True)
        self.health_port = config.get('monitoring.health_check_port', 8080)
        self.stats_interval = config.get('monitoring.stats_interval', 300)

        # 数据库配置
        self.db_config = {
            'host': config.get('database.host'),
            'port': config.get('database.port', 5432),
            'user': config.get('database.user'),
            'password': config.get('database.password'),
            'database': config.get('database.database')
        }

        # 系统状态
        self.system_stats = {}
        self.database_stats = {}
        self.application_stats = {}
        self.alerts = []

        # HTTP服务器
        self.http_server = None
        self.server_thread = None

        # 监控组件引用
        self.symbol_discovery = None
        self.connection_pool = None
        self.data_cleanup = None
        self.wal_manager = None

        if self.enabled:
            self._start_http_server()
            logger.info(f"健康监控服务已启用: http://localhost:{self.health_port}")
        else:
            logger.info("健康监控服务已禁用")

    def register_components(self, **components):
        """注册监控组件"""
        self.symbol_discovery = components.get('symbol_discovery')
        self.connection_pool = components.get('connection_pool')
        self.data_cleanup = components.get('data_cleanup')
        self.wal_manager = components.get('wal_manager')

    def _start_http_server(self):
        """启动HTTP服务器"""
        try:
            handler = self._create_http_handler()
            self.http_server = HTTPServer(('0.0.0.0', self.health_port), handler)
            self.server_thread = threading.Thread(target=self.http_server.serve_forever, daemon=True)
            self.server_thread.start()
            logger.info(f"HTTP健康检查服务启动: 端口 {self.health_port}")
        except Exception as e:
            logger.error(f"启动HTTP服务器失败: {e}")

    def _create_http_handler(self):
        """创建HTTP处理器"""
        health_monitor = self

        class HealthHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                # 禁用默认日志
                pass

            def do_GET(self):
                try:
                    parsed_url = urlparse(self.path)
                    path = parsed_url.path
                    query_params = parse_qs(parsed_url.query)

                    if path == '/health':
                        self._handle_health_check()
                    elif path == '/metrics':
                        self._handle_metrics()
                    elif path == '/stats':
                        self._handle_stats()
                    elif path == '/status':
                        self._handle_status()
                    else:
                        self._handle_404()

                except Exception as e:
                    logger.error(f"HTTP请求处理错误: {e}")
                    self._send_error(500, str(e))

            def _handle_health_check(self):
                """健康检查端点"""
                health_status = health_monitor.get_health_status()
                status_code = 200 if health_status['status'] == 'healthy' else 503

                self._send_json_response(health_status, status_code)

            def _handle_metrics(self):
                """指标端点"""
                metrics = health_monitor.get_metrics()
                self._send_json_response(metrics)

            def _handle_stats(self):
                """统计信息端点"""
                stats = health_monitor.get_comprehensive_stats()
                self._send_json_response(stats)

            def _handle_status(self):
                """状态概览端点"""
                status = health_monitor.get_status_overview()
                self._send_json_response(status)

            def _handle_404(self):
                """404处理"""
                self._send_error(404, "Not Found")

            def _send_json_response(self, data, status_code=200):
                """发送JSON响应"""
                response_data = json.dumps(data, indent=2, ensure_ascii=False)
                self.send_response(status_code)
                self.send_header('Content-type', 'application/json')
                self.send_header('Content-Length', len(response_data.encode('utf-8')))
                self.end_headers()
                self.wfile.write(response_data.encode('utf-8'))

            def _send_error(self, code, message):
                """发送错误响应"""
                error_data = {'error': message, 'code': code}
                self._send_json_response(error_data, code)

        return HealthHandler

    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        try:
            # 检查数据库连接
            db_healthy = self._check_database_health()

            # 检查系统资源
            system_healthy = self._check_system_health()

            # 检查应用组件
            app_healthy = self._check_application_health()

            # 综合判断
            overall_healthy = db_healthy and system_healthy and app_healthy

            return {
                'status': 'healthy' if overall_healthy else 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'checks': {
                    'database': 'healthy' if db_healthy else 'unhealthy',
                    'system': 'healthy' if system_healthy else 'unhealthy',
                    'application': 'healthy' if app_healthy else 'unhealthy'
                },
                'uptime': self._get_uptime()
            }

        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }

    def _check_database_health(self) -> bool:
        """检查数据库健康状态"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1.txt")
            cursor.fetchone()
            conn.close()
            return True
        except Exception as e:
            logger.warning(f"数据库健康检查失败: {e}")
            return False

    def _check_system_health(self) -> bool:
        """检查系统资源健康状态"""
        try:
            # 检查CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > 90:
                return False

            # 检查内存使用率
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                return False

            # 检查磁盘使用率
            disk = psutil.disk_usage('/')
            if disk.percent > 90:
                return False

            return True

        except Exception as e:
            logger.warning(f"系统健康检查失败: {e}")
            return False

    def _check_application_health(self) -> bool:
        """检查应用组件健康状态"""
        try:
            # 检查连接池状态
            if self.connection_pool:
                pool_stats = self.connection_pool.get_stats()
                if pool_stats['active_connections'] == 0:
                    return False

            # 检查数据流
            recent_data = self._check_recent_data()
            if not recent_data:
                return False

            return True

        except Exception as e:
            logger.warning(f"应用健康检查失败: {e}")
            return False

    def _check_recent_data(self) -> bool:
        """检查是否有最近的数据"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # 检查最近5分钟是否有交易数据
            cursor.execute("""
                SELECT COUNT(*) FROM trades
                WHERE timestamp > NOW() - INTERVAL '5 minutes'
            """)
            recent_trades = cursor.fetchone()[0]

            conn.close()
            return recent_trades > 0

        except Exception as e:
            logger.warning(f"检查最近数据失败: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """获取系统指标"""
        try:
            # 系统指标
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # 网络指标
            network = psutil.net_io_counters()

            # 进程指标
            process = psutil.Process()
            process_memory = process.memory_info()

            metrics = {
                'timestamp': datetime.now().isoformat(),
                'system': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_gb': memory.used / (1024**3),
                    'memory_total_gb': memory.total / (1024**3),
                    'disk_percent': disk.percent,
                    'disk_used_gb': disk.used / (1024**3),
                    'disk_total_gb': disk.total / (1024**3)
                },
                'network': {
                    'bytes_sent': network.bytes_sent,
                    'bytes_recv': network.bytes_recv,
                    'packets_sent': network.packets_sent,
                    'packets_recv': network.packets_recv
                },
                'process': {
                    'memory_rss_mb': process_memory.rss / (1024**2),
                    'memory_vms_mb': process_memory.vms / (1024**2),
                    'cpu_percent': process.cpu_percent(),
                    'num_threads': process.num_threads()
                }
            }

            # 数据库指标
            db_metrics = self._get_database_metrics()
            if db_metrics:
                metrics['database'] = db_metrics

            return metrics

        except Exception as e:
            logger.error(f"获取指标失败: {e}")
            return {'error': str(e)}

    def _get_database_metrics(self) -> Optional[Dict[str, Any]]:
        """获取数据库指标"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # 连接数
            cursor.execute("SELECT count(*) FROM pg_stat_activity")
            connection_count = cursor.fetchone()[0]

            # 数据库大小
            cursor.execute(f"""
                SELECT pg_size_pretty(pg_database_size('{self.db_config["database"]}'))
            """)
            db_size = cursor.fetchone()[0]

            # 各表记录数
            table_counts = {}
            tables = ['trades', 'funding', 'ticker', 'candles_1m', 'candles_5m', 'candles_30m', 'candles_4h', 'candles_1d']
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    table_counts[table] = cursor.fetchone()[0]
                except:
                    table_counts[table] = 0

            conn.close()

            return {
                'connection_count': connection_count,
                'database_size': db_size,
                'table_counts': table_counts
            }

        except Exception as e:
            logger.warning(f"获取数据库指标失败: {e}")
            return None

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """获取综合统计信息"""
        stats = {
            'timestamp': datetime.now().isoformat(),
            'health': self.get_health_status(),
            'metrics': self.get_metrics()
        }

        # 添加组件统计
        if self.symbol_discovery:
            stats['symbol_discovery'] = {
                'symbol_count': self.symbol_discovery.get_symbol_count()
            }

        if self.connection_pool:
            stats['connection_pool'] = self.connection_pool.get_stats()

        if self.data_cleanup:
            stats['data_cleanup'] = self.data_cleanup.get_stats()

        if self.wal_manager:
            stats['wal'] = self.wal_manager.get_stats()

        return stats

    def get_status_overview(self) -> Dict[str, Any]:
        """获取状态概览"""
        return {
            'service': 'Cryptofeed Monitor',
            'version': '1.txt.0.0',
            'status': self.get_health_status()['status'],
            'timestamp': datetime.now().isoformat(),
            'uptime': self._get_uptime(),
            'endpoints': {
                'health': f'http://localhost:{self.health_port}/health',
                'metrics': f'http://localhost:{self.health_port}/metrics',
                'stats': f'http://localhost:{self.health_port}/stats'
            }
        }

    def _get_uptime(self) -> str:
        """获取运行时间"""
        try:
            process = psutil.Process()
            create_time = datetime.fromtimestamp(process.create_time())
            uptime = datetime.now() - create_time
            return str(uptime).split('.')[0]  # 去掉微秒
        except:
            return "unknown"

    def stop(self):
        """停止监控服务"""
        if self.http_server:
            self.http_server.shutdown()
            self.http_server.server_close()
            logger.info("健康监控服务已停止")

    def add_alert(self, level: str, message: str):
        """添加告警"""
        alert = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        }
        self.alerts.append(alert)

        # 只保留最近100条告警
        self.alerts = self.alerts[-100:]

        logger.warning(f"告警 [{level}]: {message}")

    def get_alerts(self) -> List[Dict[str, Any]]:
        """获取告警列表"""
        return self.alerts