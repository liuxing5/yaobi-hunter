# 回测快速开始

## 📦 完整流程

```bash
# 1. 安装依赖
pip install pandas pyarrow aiohttp

# 2. 拉取历史数据（~30分钟）
python fetch_historical.py

# 3. 跑完整回测（A组，全开过滤器）
python backtest_full.py --output data/result_A.jsonl

# 4. 跑 B组（关闭实验性过滤器，对照）
python backtest_full.py --no-experimental --output data/result_B.jsonl

# 5. 对比两组结果
```

## 🔧 backtest_full.py 参数

```bash
# 基础用法
python backtest_full.py

# 指定时间范围（YYYY-MM-DD，UTC）
python backtest_full.py --start 2025-03-20 --end 2025-04-15

# 扫描间隔（默认300秒=5分钟，和实盘一致）
python backtest_full.py --interval 300

# 只回测前50个币（调试用，快）
python backtest_full.py --symbol-limit 50

# 订单簿固定分值（无历史数据，1.0=中性，>1.0=假设偏买盘）
python backtest_full.py --ob-score 1.0

# 临时覆盖评分门槛（不改config）
python backtest_full.py --min-score 70 --tradable-min-score 75

# 关闭所有实验性过滤器
python backtest_full.py --no-experimental
```

## 📊 理解输出

跑完后会打印类似这样的报告：

```
================================================================================
回测结果汇总（共 156 笔）
================================================================================

总盈亏: +245.30 U
总投入: 7800.00 U
ROI: +3.14%
胜/负: 52 / 104
胜率: 33.3%
平均盈利: 12.50 U
平均亏损: -4.10 U
盈亏比: 1 : 0.33    ← 这个越小说明盈大亏小

── 按评分分档 ──
档           笔数      胜率       总盈亏        平均
-------------------------------------------------------
85+           12     58.3%    +85.20U    +7.10U   ← 85+档胜率高
75-84         89     35.0%   +180.50U    +2.03U   ← 主力档
65-74         55     22.0%   -20.40U    -0.37U   ← 低分档亏钱

── 按信号类型 ──
类型                                      笔数      胜率       总盈亏
--------------------------------------------------------------
15m breakout+retest+5m restart            78     42.3%   +220.00U
15m breakout+strong hold                  45     28.8%    +50.30U
15m breakdown+retest+5m restart           33     30.3%   -25.00U

── 按平仓原因 ──
原因                   笔数      胜率       总盈亏
-------------------------------------------------
HALF_CLOSE + (后续)     52     100.0%   +625.00U   ← 每笔赚了才进到这
SL                      60      0.0%    -240.00U
FAKE_BK_HALF            25     20.0%    -50.00U
TIMEOUT                 19     52.6%    -89.70U
```

## 🎯 A/B 对照分析

跑完两组后，关注这几个核心指标的差异：

| 指标 | A组（过滤全开） | B组（只硬风控） | 结论 |
|------|----------------|---------------|------|
| 总笔数 | 例如 156 | 例如 287 | B组多100+（被过滤器砍的信号）|
| 总盈亏 | +245U | +312U | B 反而更好？→ 过滤过度 |
| 胜率 | 33% | 28% | A稍高但B总盈亏高 → 过滤砍了高盈利样本 |
| 平均盈利 | 12.5U | 8.9U | A 保留了大赢利信号 ✓ |
| 最大单笔亏损 | -45U | -98U | 硬风控不够 → B 缺少实验性保护 |

**判断标准：**

- 如果 A 组 ROI 比 B 组高 >15% → 实验过滤器有效，保留
- 如果两者差距 <10% → 过拟合嫌疑大，B组更简洁，推荐关掉
- 如果 B 组 >A 组 → 明显过拟合，立刻全关

## ⚠️ 关键注意事项

### 1. 订单簿数据缺失的影响

回测里 `ob_imbalance` 分数固定为 0（因为 ob_ratio=1.0 在 scanner 逻辑里约等于 0 分）。

**这意味着：**
- 回测分数比实盘系统性低 20 分左右
- 推荐**临时降低** `MIN_SCORE` 到实盘值 - 20（比如实盘 70，回测 50）保持信号数一致

```bash
# 让回测产生和实盘相似的信号数量
python backtest_full.py --min-score 50 --tradable-min-score 55
```

或者用 `--ob-score 2.5`（假设所有币 ob 都满足阈值）让分数贴近实盘：

```bash
python backtest_full.py --ob-score 2.5
```

### 2. K线精度 = 分钟级

回测按 1m K 线 evolve 持仓，所以止损/止盈检测精度是 1 分钟。
实盘是秒级监控，所以：
- 回测里在 1 分钟内同时触发止损和止盈时，**保守按止损成交**
- 这让回测略微悲观，实盘表现通常稍好

### 3. 未来数据不泄漏

`historical_data.snapshot(sym, ts_ms)` 严格只返回 `time < ts_ms` 的数据。

### 4. 滑点和手续费未模拟

当前回测**不计滑点和手续费**。实际会扣除：
- 开仓+平仓 taker 手续费：约 0.08%（币安0.04%×2）
- 分批止盈再全平 ≈ 0.12%
- 滑点（小币种挂市价）：0.05-0.3%

**粗估实际 ROI 会比回测低 0.3-0.5%**。如果回测 ROI 接近 0%，实际就是亏钱。

## 🐛 常见问题

### Q: "scanner.scan() at ts=...: xxx no kline data"

某些币在时间范围内还没上线或被下架。正常现象，会被跳过。

### Q: 内存占用太高

改 `lazy=True` 模式（已是默认）。200币 × 30天 × 8类数据总量约 3GB，一次加载到内存可能吃紧。

### Q: 回测结果和 simulate_signals.py 不一致

simulate_signals.py 是"读 signals.jsonl 重放"，依赖实盘产生的信号。
backtest_full.py 是"用历史数据重跑 scanner"，信号由回测自己产生。

**两者不应该一致**，backtest_full 是更 rigorous 的版本。

### Q: 能不能加速？

- 减少 `--symbol-limit 50`：只用前50币
- 增大 `--interval 600`：每10分钟扫一次而不是5分钟
- 缩短回测范围 `--start 2025-04-01`

### Q: 怎么看具体的交易明细？

输出的 `data/backtest_results.jsonl` 每行一个交易：

```bash
# 按盈亏排序
python -c "
import json
results = [json.loads(l) for l in open('data/backtest_results.jsonl')]
results.sort(key=lambda r: r['pnl_usdt'])
print('最亏5笔:')
for r in results[:5]:
    print(f\"  {r['symbol']} {r['direction']} {r['entry_time']} → {r['exit_time']} : {r['pnl_usdt']:+.2f}U [{r['status']}]\")
print('\\n最赚5笔:')
for r in results[-5:]:
    print(f\"  {r['symbol']} {r['direction']} {r['entry_time']} → {r['exit_time']} : {r['pnl_usdt']:+.2f}U [{r['status']}]\")
"
```
