import json
from collections import defaultdict
from datetime import datetime, timezone

def load_results(path):
    results = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            r = json.loads(line.strip())
            results.append(r)
    return results

A = load_results('data/result_A.jsonl')
B = load_results('data/result_B.jsonl')

print('=' * 80)
print('A/B 组对比总览')
print('=' * 80)

for name, data in [('A组(实验过滤器开)', A), ('B组(实验过滤器关)', B)]:
    total_pnl = sum(r['pnl_usdt'] for r in data)
    total_usdt = sum(r['usdt'] for r in data)
    wins = [r for r in data if r['pnl_usdt'] > 0]
    losses = [r for r in data if r['pnl_usdt'] <= 0]
    roi = total_pnl / total_usdt * 100 if total_usdt > 0 else 0
    avg_win = sum(r['pnl_usdt'] for r in wins) / len(wins) if wins else 0
    avg_loss = sum(r['pnl_usdt'] for r in losses) / len(losses) if losses else 0
    print(f'\n{name}:')
    print(f'  笔数: {len(data)}, 胜率: {len(wins)/len(data)*100:.1f}%')
    print(f'  总盈亏: {total_pnl:+.2f}U, ROI: {roi:+.2f}%')
    print(f'  平均盈利: {avg_win:+.2f}U, 平均亏损: {avg_loss:+.2f}U')
    if avg_loss != 0 and avg_win != 0:
        print(f'  盈亏比: 1:{abs(avg_loss/avg_win):.2f}')

# Q1
print('\n' + '=' * 80)
print('Q1: MIN_SCORE 应该设多少？（按评分分档的胜率曲线）')
print('=' * 80)
for name, data in [('A组', A), ('B组', B)]:
    print(f'\n{name}:')
    buckets = defaultdict(list)
    for r in data:
        s = r['score']
        if s >= 85:
            b = '85+'
        elif s >= 75:
            b = '75-84'
        elif s >= 65:
            b = '65-74'
        elif s >= 55:
            b = '55-64'
        elif s >= 45:
            b = '45-54'
        elif s >= 35:
            b = '35-44'
        else:
            b = '<35'
        buckets[b].append(r)
    header = f'  {"档":<10} {"笔数":>6} {"胜率":>8} {"总盈亏":>12} {"平均盈亏":>10} {"ROI%":>8}'
    print(header)
    print('  ' + '-' * 60)
    for b in ['85+', '75-84', '65-74', '55-64', '45-54', '35-44', '<35']:
        if b not in buckets:
            continue
        rs = buckets[b]
        ws = [r for r in rs if r['pnl_usdt'] > 0]
        total = sum(r['pnl_usdt'] for r in rs)
        avg = total / len(rs)
        usdt = sum(r['usdt'] for r in rs)
        roi = total / usdt * 100 if usdt > 0 else 0
        print(f'  {b:<10} {len(rs):>6} {len(ws)/len(rs)*100:>7.1f}% {total:>+11.2f}U {avg:>+9.2f}U {roi:>+7.2f}%')

# Q2
print('\n' + '=' * 80)
print('Q2: 实验过滤器有用吗？（A/B 对比）')
print('=' * 80)
a_pnl = sum(r['pnl_usdt'] for r in A)
b_pnl = sum(r['pnl_usdt'] for r in B)
a_wr = len([r for r in A if r['pnl_usdt'] > 0]) / len(A) * 100
b_wr = len([r for r in B if r['pnl_usdt'] > 0]) / len(B) * 100
a_avg = a_pnl / len(A)
b_avg = b_pnl / len(B)
print(f'  A组(开): {len(A)}笔, 胜率{a_wr:.1f}%, 总盈亏{a_pnl:+.2f}U, 平均{a_avg:+.2f}U')
print(f'  B组(关): {len(B)}笔, 胜率{b_wr:.1f}%, 总盈亏{b_pnl:+.2f}U, 平均{b_avg:+.2f}U')
diff = a_pnl - b_pnl
if a_pnl > b_pnl:
    print(f'  => 实验过滤器有帮助，A组比B组多赚 {diff:+.2f}U')
elif a_pnl < b_pnl:
    print(f'  => 实验过滤器有负面影响，A组比B组少赚 {abs(diff):.2f}U')
else:
    print(f'  => 实验过滤器无明显影响')
if a_wr > b_wr:
    print(f'  => A组胜率更高 (+{a_wr-b_wr:.1f}%)')
else:
    print(f'  => B组胜率更高 (+{b_wr-a_wr:.1f}%)')

# Q3
print('\n' + '=' * 80)
print('Q3: LONG vs SHORT 哪个更好？')
print('=' * 80)
for name, data in [('A组', A), ('B组', B)]:
    print(f'\n{name}:')
    for d in ['LONG', 'SHORT']:
        rs = [r for r in data if r['direction'] == d]
        if not rs:
            continue
        ws = [r for r in rs if r['pnl_usdt'] > 0]
        total = sum(r['pnl_usdt'] for r in rs)
        avg = total / len(rs)
        print(f'  {d:<6} 笔数={len(rs):>4} 胜率={len(ws)/len(rs)*100:5.1f}% 总盈亏={total:+.2f}U 平均={avg:+.2f}U')

# Q4
print('\n' + '=' * 80)
print('Q4: 5m restart 是不是最强信号？（按 structure_trigger 对比）')
print('=' * 80)
for name, data in [('A组', A), ('B组', B)]:
    print(f'\n{name}:')
    types = defaultdict(list)
    for r in data:
        types[r['structure_trigger']].append(r)
    for t, rs in sorted(types.items(), key=lambda x: -sum(r['pnl_usdt'] for r in x[1])):
        ws = [r for r in rs if r['pnl_usdt'] > 0]
        total = sum(r['pnl_usdt'] for r in rs)
        avg = total / len(rs)
        has_5m = '5m restart' in t
        marker = ' <<<' if has_5m else ''
        print(f'  {t:<50} 笔={len(rs):>3} 胜率={len(ws)/len(rs)*100:5.1f}% 总={total:+.2f}U 均={avg:+.2f}U{marker}')

# Q5
print('\n' + '=' * 80)
print('Q5: FAKE_BK 减半 vs 全退 vs 不开')
print('=' * 80)
for name, data in [('A组', A), ('B组', B)]:
    fake_half = [r for r in data if r['status'] == 'FAKE_BK_HALF']
    fake_full = [r for r in data if r['status'] == 'FAKE_BK_FULL']
    print(f'\n{name}:')
    if fake_half:
        total = sum(r['pnl_usdt'] for r in fake_half)
        ws = [r for r in fake_half if r['pnl_usdt'] > 0]
        print(f'  FAKE_BK_HALF(减半): {len(fake_half)}笔, 胜率{len(ws)/len(fake_half)*100:.1f}%, 总盈亏{total:+.2f}U')
    else:
        print(f'  FAKE_BK_HALF(减半): 0笔')
    if fake_full:
        total = sum(r['pnl_usdt'] for r in fake_full)
        ws = [r for r in fake_full if r['pnl_usdt'] > 0]
        print(f'  FAKE_BK_FULL(全退): {len(fake_full)}笔, 胜率{len(ws)/len(fake_full)*100:.1f}%, 总盈亏{total:+.2f}U')
    else:
        print(f'  FAKE_BK_FULL(全退): 0笔')

# Q6
print('\n' + '=' * 80)
print('Q6: 什么时段交易最好？（按 entry_time 的 hour 分组，UTC）')
print('=' * 80)
for name, data in [('A组', A), ('B组', B)]:
    print(f'\n{name}:')
    hours = defaultdict(list)
    for r in data:
        dt = datetime.strptime(r['entry_time'], '%Y-%m-%d %H:%M:%S')
        hours[dt.hour].append(r)
    print(f'  {"时段(UTC)":<14} {"笔数":>6} {"胜率":>8} {"总盈亏":>12} {"平均盈亏":>10}')
    print('  ' + '-' * 55)
    sorted_hours = sorted(hours.items(), key=lambda x: -sum(r['pnl_usdt'] for r in x[1]))
    for h, rs in sorted_hours:
        ws = [r for r in rs if r['pnl_usdt'] > 0]
        total = sum(r['pnl_usdt'] for r in rs)
        avg = total / len(rs)
        print(f'  {h:02d}:00-{h:02d}:59     {len(rs):>6} {len(ws)/len(rs)*100:>7.1f}% {total:>+11.2f}U {avg:>+9.2f}U')

# 额外: 按平仓原因分析
print('\n' + '=' * 80)
print('额外: 按平仓原因分析')
print('=' * 80)
for name, data in [('A组', A), ('B组', B)]:
    print(f'\n{name}:')
    reasons = defaultdict(list)
    for r in data:
        reasons[r['status']].append(r)
    for reason, rs in sorted(reasons.items(), key=lambda x: -len(x[1])):
        ws = [r for r in rs if r['pnl_usdt'] > 0]
        total = sum(r['pnl_usdt'] for r in rs)
        avg = total / len(rs)
        print(f'  {reason:<20} {len(rs):>4}笔 胜率={len(ws)/len(rs)*100:5.1f}% 总={total:+.2f}U 均={avg:+.2f}U')

# 额外: 持仓时长分析
print('\n' + '=' * 80)
print('额外: 按持仓时长分析')
print('=' * 80)
for name, data in [('A组', A), ('B组', B)]:
    print(f'\n{name}:')
    hold_buckets = defaultdict(list)
    for r in data:
        h = r['hold_min']
        if h < 30:
            b = '<30min'
        elif h < 60:
            b = '30-60min'
        elif h < 120:
            b = '1-2h'
        elif h < 240:
            b = '2-4h'
        else:
            b = '4h+'
        hold_buckets[b].append(r)
    for b in ['<30min', '30-60min', '1-2h', '2-4h', '4h+']:
        if b not in hold_buckets:
            continue
        rs = hold_buckets[b]
        ws = [r for r in rs if r['pnl_usdt'] > 0]
        total = sum(r['pnl_usdt'] for r in rs)
        avg = total / len(rs)
        print(f'  {b:<12} {len(rs):>4}笔 胜率={len(ws)/len(rs)*100:5.1f}% 总={total:+.2f}U 均={avg:+.2f}U')
