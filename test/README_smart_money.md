# 大户多空比升级 - 使用指南

## 🎯 这次改了什么

三个文件全部更新，让回测用**真实的币安大户多空比数据**而不是 `ls_ratio^1.3` 的数学假设。

| 文件 | 改动 |
|------|------|
| `fetch_historical.py` | 新增拉取 `topLongShortAccountRatio`（大户账户）和 `topLongShortPositionRatio`（大户持仓） |
| `historical_data.py` | 加载新 parquet + `SymbolSnapshot` 新增 `top_acc_hist` / `top_pos_hist` 字段 |
| `backtest_full.py` | MockClient 优先用真实数据；无数据时退化为近似并warn |

## 📥 使用步骤

### 1. 清理旧数据，重新拉取（推荐）

```bash
cd yaobi_v3/
python fetch_historical.py --cleanup
```

**为什么要 cleanup？** 老的 `manifest.json` 没记录 `top_acc_5m` / `top_pos_5m`，不cleanup 的话会漏拉这两类数据。

### 2. 或者增量拉取（保留老数据）

如果老数据已经拉完不想重拉，只补新的两类：

```bash
# 先备份 manifest
cp data/backtest/manifest.json data/backtest/manifest.json.bak

# 手动删除 manifest 里的 top_acc_5m / top_pos_5m 标记
# 实际上 manifest 里一开始就没有这两项，所以 --resume 会自动拉新数据
python fetch_historical.py --resume
```

增量拉取只会补拉新增的两种数据（每个币 2 个请求），200 币 × 2 ≈ 400 请求，预计 **3-5 分钟**。

### 3. 验证数据

```bash
python historical_data.py
```

应该看到：
```
sample数据文件:
  klines_1m: 43200 行
  klines_5m: 8640 行
  ...
  top_acc_5m: 8640 行    ← 新增
  top_pos_5m: 8640 行    ← 新增
  funding: 90 行

  ls_hist: 6
  taker_hist: 6
  top_acc_hist: 6        ← 新增
  top_pos_hist: 6        ← 新增
  funding: 3
```

### 4. 重跑回测

```bash
# A 组（所有过滤器开，带真实大户数据）
python backtest_full.py --output data/result_A_real.jsonl

# B 组对照（硬风控）
python backtest_full.py --no-experimental --output data/result_B_real.jsonl
```

如果没有真实数据，回测日志会出现：
```
WARNING  XXXUSDT 无真实 top_acc_hist，退化为 ls_ratio^1.3 近似。请重新运行 fetch_historical.py 拉真实数据
```
这是**兼容提示**，不影响运行。

## 📊 两种大户接口的区别

币安提供两个"大户"接口：

| 接口 | 含义 | 说明 |
|------|------|------|
| `topLongShortAccountRatio` | 大户**账户**多空比 | 按**账户数**，每个账户占 1 票 |
| `topLongShortPositionRatio` | 大户**持仓**多空比 | 按**仓位金额**加权 |

**SOP 里说的"大户多空比"通常指持仓比**（因为 1 个大户可能仓位 > 1000 个小户加起来）。

**现在 scanner 用的是 `topLongShortAccountRatio`（账户比）。可能需要改。**

两种数据都拉下来了，你可以：

- 保持现状（用账户比）
- 在 scanner 里改成持仓比
- 两者组合（都作为因子，权重各半）

我建议**先用账户比验证一遍**，和之前回测对比。再根据需要试试持仓比。

## 🔍 新数据量估算

每个币多 2 种数据：
- `top_acc_5m`: 8640 行 × 200 字节 ≈ 200 KB
- `top_pos_5m`: 8640 行 × 200 字节 ≈ 200 KB

每个币总数据量从 3.3 MB → **3.7 MB**

200 币种 ≈ **740 MB**（原来 660 MB，增加 80 MB）

请求量增加：
- 每个币 × 30 天 / 500 条 ≈ 18 次 × 2 种数据 = **36 次请求/币**
- 200 币 × 36 = **7200 次请求**
- 15 req/s 限流 ≈ **8-10 分钟额外时间**

## ⚠️ 重要提醒：scanner.py 也要改

这次只改了**回测侧**能用真实数据了。但**实盘运行的 scanner.py**没动。

scanner.py 里的 `_get_smart_vs_dumb_money` 方法已经调 `topLongShortAccountRatio`，所以**实盘是拿真实数据的**。

真正不一致的是：
- 实盘：只调 `topLongShortAccountRatio`，没用 `topLongShortPositionRatio`
- 回测：现在两个都有数据可用

**推荐：等回测数据验证后，再考虑 scanner 里加 position ratio 因子。**

## 🎬 下一步

根据 fresh 回测数据回答这些问题：

1. **真实大户数据 vs 假设数据，对胜率有影响吗？**
   对比 `ls_ratio^1.3` 版本的 result_A.jsonl 和真实版本的 result_A_real.jsonl

2. **SMART_VS_DUMB 因子（真实数据下）是否有效？**
   如果有显著正贡献，保留；如果和假设数据一样没用，考虑降权重或关闭

3. **大户账户比 vs 大户持仓比，哪个更强？**
   单独跑两次，看哪个效果更好

---

## 附：数据字段说明

### top_acc_5m / top_pos_5m 字段

```json
{
  "symbol": "BTCUSDT",
  "longShortRatio": "2.34",      // 多空比（多/空）
  "longAccount": "0.7",           // 多头占比 70%
  "shortAccount": "0.3",          // 空头占比 30%
  "timestamp": 1700000000000
}
```

**读法：**
- `longShortRatio > 1`：大户偏多
- `longShortRatio < 1`：大户偏空
- `longShortRatio = 1`：平衡
- 结合 `longAccount + shortAccount = 1` 可验证数据完整性
