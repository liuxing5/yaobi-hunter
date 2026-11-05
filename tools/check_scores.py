import json
from collections import Counter

sigs = [json.loads(l) for l in open('data/signals.jsonl', 'r', encoding='utf-8') if l.strip()]
scores = [s.get('score', 0) for s in sigs]
print(f"Total: {len(sigs)}")
print(f"Min: {min(scores)}  Max: {max(scores)}")

buckets = Counter()
for s in scores:
    lo = s // 10 * 10
    buckets[lo] += 1

for k in sorted(buckets):
    print(f"  {k}-{k+9}: {buckets[k]}")

print()
above75 = [s for s in scores if s >= 75]
above70 = [s for s in scores if s >= 70]
below70 = [s for s in scores if s < 70]
print(f">=75: {len(above75)}  >=70: {len(above70)}  <70: {len(below70)}")
