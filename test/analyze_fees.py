import json

# 读取回测结果文件
try:
    results = []
    with open('data/result_B_fees.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                results.append(json.loads(line))
            except Exception as e:
                print(f"解析失败: {e}")
                print(f"行内容: {line[:100]}...")

    if results:
        total_pnl = sum(r.get('pnl_usdt', 0) for r in results)
        total_trades = len(results)
        win_count = sum(1 for r in results if r.get('pnl_usdt', 0) > 0)
        win_rate = win_count / total_trades * 100 if total_trades else 0
        avg_pnl = total_pnl / total_trades if total_trades else 0
        
        print(f"总盈亏: {total_pnl:.2f} U")
        print(f"总交易: {total_trades}")
        print(f"胜率: {win_rate:.1f}%")
        print(f"平均盈亏: {avg_pnl:.2f} U")
        
        # 按方向统计
        long_trades = [r for r in results if r.get('direction') == 'LONG']
        short_trades = [r for r in results if r.get('direction') == 'SHORT']
        
        if long_trades:
            long_pnl = sum(r.get('pnl_usdt', 0) for r in long_trades)
            long_win = sum(1 for r in long_trades if r.get('pnl_usdt', 0) > 0)
            print(f"LONG: {len(long_trades)}笔, 盈亏: {long_pnl:.2f} U, 胜率: {long_win/len(long_trades)*100:.1f}%")
        
        if short_trades:
            short_pnl = sum(r.get('pnl_usdt', 0) for r in short_trades)
            short_win = sum(1 for r in short_trades if r.get('pnl_usdt', 0) > 0)
            print(f"SHORT: {len(short_trades)}笔, 盈亏: {short_pnl:.2f} U, 胜率: {short_win/len(short_trades)*100:.1f}%")
    else:
        print("无交易数据")
except Exception as e:
    print(f"错误: {e}")
