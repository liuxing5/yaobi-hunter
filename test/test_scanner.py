import asyncio
import sys
import os

# 添加根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanner import YaoBiScanner
from config import Config
from client import MockClient

class _NullNotifier:
    async def send_message(self, *a, **k): pass
    async def send_signal(self, *a, **k): pass

async def test_scanner():
    config = Config()
    config.EXPERIMENTAL_FILTERS_ENABLED = False
    config.MIN_SCORE = 50
    
    mock_client = MockClient()
    scanner = YaoBiScanner(config, _NullNotifier())
    scanner.client = mock_client
    
    # 设置模拟时间到2026-04-01
    import time
    test_ts = int(time.mktime(time.strptime('2026-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000)
    scanner._bt_now_ms = test_ts
    mock_client.set_time(test_ts)
    
    print(f"测试时间: {test_ts}")
    
    # 测试获取币种列表
    symbols = await scanner._get_symbols()
    print(f"币种数: {len(symbols)}")
    
    # 测试前5个币种的分析
    for symbol in symbols[:5]:
        print(f"\n分析 {symbol}...")
        try:
            # 直接调用分析方法
            result = await scanner._analyze(symbol)
            if result:
                print(f"  信号: {result}")
            else:
                print(f"  无信号")
        except Exception as e:
            print(f"  错误: {e}")

if __name__ == "__main__":
    asyncio.run(test_scanner())
