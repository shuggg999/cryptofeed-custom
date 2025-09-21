#!/usr/bin/env python3
"""
网络连接诊断工具
"""
import asyncio
import websockets
import ssl
import json
import time

async def test_binance_websocket():
    """测试Binance WebSocket连接"""
    print("🔍 测试Binance WebSocket连接...")

    # Binance期货WebSocket端点
    uri = "wss://fstream.binance.com/ws/btcusdt@trade"

    try:
        # 创建SSL上下文
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        print(f"📡 连接到: {uri}")

        async with websockets.connect(uri, ssl=ssl_context, ping_interval=20) as websocket:
            print("✅ WebSocket连接成功！")

            # 等待数据
            start_time = time.time()
            count = 0

            async for message in websocket:
                count += 1
                data = json.loads(message)
                print(f"📈 交易#{count}: 价格={data['p']}, 数量={data['q']}")

                # 5条数据后停止
                if count >= 5:
                    break

                # 超时检查
                if time.time() - start_time > 30:
                    print("⏰ 30秒超时")
                    break

            print(f"✅ 成功接收 {count} 条交易数据")

    except Exception as e:
        print(f"❌ WebSocket连接失败: {e}")
        return False

    return True

async def test_network():
    """测试网络连接"""
    print("🌐 测试网络连接...")

    import aiohttp

    try:
        async with aiohttp.ClientSession() as session:
            # 测试REST API
            async with session.get("https://fapi.binance.com/fapi/v1/ping") as resp:
                print(f"✅ REST API: {resp.status}")

            # 测试获取合约信息
            async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
                data = await resp.json()
                symbols = [s for s in data['symbols'] if s['status'] == 'TRADING' and s['contractType'] == 'PERPETUAL']
                print(f"✅ 获取到 {len(symbols)} 个永续合约")

    except Exception as e:
        print(f"❌ 网络测试失败: {e}")
        return False

    return True

async def main():
    """主测试函数"""
    print("🔧 Binance连接诊断工具")
    print("=" * 50)

    # 1. 测试基础网络
    network_ok = await test_network()
    if not network_ok:
        print("❌ 基础网络连接失败，请检查网络设置")
        return

    print()

    # 2. 测试WebSocket
    websocket_ok = await test_binance_websocket()
    if not websocket_ok:
        print("❌ WebSocket连接失败，可能是防火墙或代理问题")
        return

    print()
    print("🎉 所有连接测试通过！网络连接正常")

if __name__ == '__main__':
    asyncio.run(main())