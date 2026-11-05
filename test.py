import time
import hmac
import hashlib
import requests
import json

# ================= 配置区 =================
BINANCE_API_KEY = 'Hk2xFwJJH6LkrKVlNxS2tbl4d2JxQF8IxGIwuHql4tReJpou3FAoezoGJzwES5kV'
BINANCE_API_SECRET = '1TypHdP4PkbZCLsAMaBFm73HC3DbxjQK1hsRH3w4f13s7Awaq6HR36hCiveVqQmm'

# 显式代理配置（解决你之前的 404 报错）
PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}
# ==========================================

def test_binance_connection():
    endpoint = 'https://fapi.binance.com'
    path = '/fapi/v1/account'
    
    # 1. 获取服务器时间（用于检查本地时间是否同步）
    try:
        time_res = requests.get(f"{endpoint}/fapi/v1/time", proxies=PROXIES, timeout=10)
        server_time = time_res.json()['serverTime']
        local_time = int(time.time() * 1000)
        diff = server_time - local_time
        print(f"--- 时间检查 ---")
        print(f"本地时间: {local_time}")
        print(f"服务器时间: {server_time}")
        print(f"时间偏移: {diff}ms (偏移超过 5000ms 会导致 401)")
    except Exception as e:
        print(f"无法连接币安服务器: {e}")
        return

    # 2. 构造签名请求
    timestamp = server_time # 直接使用服务器时间，防止本地时间不对导致的 401
    query_string = f'timestamp={timestamp}&recvWindow=10000'
    signature = hmac.new(
        BINANCE_API_SECRET.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    url = f'{endpoint}{path}?{query_string}&signature={signature}'

    # 3. 发送请求
    print(f"\n--- 鉴权请求 ---")
    headers = {
        'X-MBX-APIKEY': BINANCE_API_KEY,
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        # 强制禁用系统环境变量，只使用我们定义的代理
        session = requests.Session()
        session.trust_env = False 
        
        response = session.get(url, headers=headers, proxies=PROXIES, timeout=10)
        
        print(f"状态码: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 鉴权成功！")
            print(f"账户总权益: {data.get('totalMarginBalance', '0')} USDT")
            print(f"是否可交易: {data.get('canTrade')}")
        elif response.status_code == 401:
            print("❌ 401 错误：签名校验失败。")
            print("检查：1. Key/Secret 是否填反；2. Key 是否勾选了'允许合约'；3. Secret 结尾是否有空格。")
            print(f"返回详情: {response.text}")
        elif response.status_code == 404:
            print("❌ 404 错误：请求路径不对或被代理拦截。")
            print(f"返回详情: {response.text}")
        else:
            print(f"❌ 其他错误: {response.status_code}")
            print(f"返回详情: {response.text}")

    except Exception as e:
        print(f"请求发生异常: {e}")

if __name__ == "__main__":
    test_binance_connection()