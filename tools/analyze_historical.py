#!/usr/bin/env python3
import json
from collections import defaultdict

sigs = [json.loads(l) for l in open('data/historical_signals.jsonl','r',encoding='utf-8') if l.strip()]
print(f'Total signals: {len(sigs)}')
print(f'Score range: {min(s["score"] for s in sigs)}-{max(s["score"] for s in sigs)}')

score_buckets = defaultdict(list)
for s in sigs:
    sc = s['score']
    if sc >= 35: score_buckets['35+'].append(s)
    elif sc >= 30: score_buckets['30-34'].append(s)
    elif sc >= 25: score_buckets['25-29'].append(s)
    elif sc >= 20: score_buckets['20-24'].append(s)
    else: score_buckets['<20'].append(s)

print('\n=== Score Distribution ===')
for k in ['35+','30-34','25-29','20-24','<20']:
    items = score_buckets[k]
    n = len(items)
    longs = sum(1 for s in items if s['direction']=='LONG')
    shorts = n - longs
    print(f'  {k}: {n} signals (LONG:{longs} SHORT:{shorts})')

long_sigs = [s for s in sigs if s['direction']=='LONG']
short_sigs = [s for s in sigs if s['direction']=='SHORT']
print(f'\n=== Direction ===')
print(f'  LONG: {len(long_sigs)}')
print(f'  SHORT: {len(short_sigs)}')

triggers = defaultdict(int)
for s in sigs:
    t = s.get('factors',{}).get('structure_trigger','NONE')
    triggers[t] += 1
print(f'\n=== Structure Triggers ===')
for k,v in sorted(triggers.items(), key=lambda x:-x[1]):
    print(f'  {k}: {v}')

daily = defaultdict(int)
for s in sigs:
    daily[s['timestamp'][:10]] += 1
print(f'\n=== Daily Distribution ===')
for k in sorted(daily.keys()):
    print(f'  {k}: {daily[k]}')

symbols = defaultdict(int)
for s in sigs:
    symbols[s['symbol']] += 1
print(f'\n=== Top Symbols ===')
for k,v in sorted(symbols.items(), key=lambda x:-x[1])[:15]:
    print(f'  {k}: {v}')
