#!/usr/bin/env python
"""
Cryptofeed 项目统一启动入口

使用方式:
    python main.py

这个脚本会启动完整的 Cryptofeed API 服务，包括:
- FastAPI REST API 服务
- 实时数据采集监控
- 历史数据补充服务
"""
import sys
import os

# 确保项目根目录在 Python 路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cryptofeed_api.app import main

if __name__ == "__main__":
    main()
