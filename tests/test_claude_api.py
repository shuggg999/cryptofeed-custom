#!/usr/bin/env python3
"""
测试 Claude API 连接的简单脚本
在 VPS 上运行此脚本来验证是否能连接到 Claude
"""

import requests
import json
import os

# 测试 API 端点连接
def test_api_connection():
    api_key = os.getenv('ANTHROPIC_API_KEY')

    if not api_key:
        print("❌ 错误: 未设置 ANTHROPIC_API_KEY 环境变量")
        print("请运行: export ANTHROPIC_API_KEY='your-api-key-here'")
        return False

    headers = {
        'x-api-key': api_key,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
    }

    data = {
        'model': 'claude-3-haiku-20240307',
        'max_tokens': 100,
        'messages': [
            {'role': 'user', 'content': 'Say "VPS connection test successful" if you receive this'}
        ]
    }

    try:
        print("🔄 正在测试 Claude API 连接...")
        response = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers=headers,
            json=data,
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            print("✅ 成功连接到 Claude API!")
            print(f"📝 响应: {result['content'][0]['text']}")
            return True
        else:
            print(f"❌ API 返回错误: {response.status_code}")
            print(f"详情: {response.text}")
            return False

    except requests.exceptions.ConnectionError:
        print("❌ 无法连接到 api.anthropic.com")
        return False
    except requests.exceptions.Timeout:
        print("❌ 连接超时")
        return False
    except Exception as e:
        print(f"❌ 发生错误: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("Claude API VPS 连接测试")
    print("=" * 50)

    if test_api_connection():
        print("\n🎉 VPS 可以正常使用 Claude API!")
    else:
        print("\n⚠️  请检查网络连接和 API 配置")