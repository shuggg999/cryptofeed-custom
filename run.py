#!/usr/bin/env python3
"""
Cryptofeed 项目启动器
统一入口点，调用高级动态连接池监控系统
"""
import subprocess
import sys
from pathlib import Path

def main():
    """启动高级监控系统"""
    project_root = Path(__file__).parent
    main_script = project_root / "src" / "cryptofeed_monitor" / "main.py"

    print("🚀 启动 Cryptofeed 高级动态连接池数据采集系统...")
    print(f"📍 主脚本位置: {main_script}")
    print("🎯 支持全部497个USDT永续合约")

    try:
        # 直接执行高级监控系统
        subprocess.run([sys.executable, str(main_script)], check=True)
    except KeyboardInterrupt:
        print("\n⏹️  用户停止程序")
    except subprocess.CalledProcessError as e:
        print(f"❌ 启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()