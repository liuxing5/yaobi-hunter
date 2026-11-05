import json

# 读取信号文件
try:
    lines = open('data/raw_signals.jsonl', 'r', encoding='utf-8').readlines()
    print(f"总信号数: {len(lines)}")
    
    if lines:
        # 分析前5个信号
        for i, line in enumerate(lines[:5]):
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                print(f"\n信号 {i+1}:")
                print(f"  交易对: {d.get('symbol', '无')}")
                print(f"  方向: {d.get('direction', '无')}")
                print(f"  分数: {d.get('score', 0)}")
                print(f"  结构触发: {d.get('factors', {}).get('structure_trigger', '无')}")
                print(f"  价格: {d.get('price', 0)}")
            except Exception as e:
                print(f"解析失败: {e}")
    else:
        print("无信号数据")
except Exception as e:
    print(f"错误: {e}")
