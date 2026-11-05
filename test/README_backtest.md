# 历史数据拉取与回测框架

## 📦 文件结构

```
yaobi_v3/
├── fetch_historical.py      # 数据拉取脚本
├── historical_data.py       # 数据加载工具（回测脚本会用）
├── data/
│   └── backtest/            # 拉下来的 parquet 文件
│       ├── manifest.json
│       ├── RAVEUSDT_klines_1m.parquet
│       ├── RAVEUSDT_klines_5m.parquet
│       ├── RAVEUSDT_klines_15m.parquet
│       ├── RAVEUSDT_klines_1h.parquet
│       ├── RAVEUSDT_oi_5m.parquet
│       ├── RAVEUSDT_ls_ratio_5m.parquet
│       ├── RAVEUSDT_taker_5m.parquet
│       └── RAVEUSDT_funding.parquet
│       └── ... (其他币种)
```

## 🛠 安装依赖

```bash
pip install pandas pyarrow aiohttp
```

## 🚀 快速开始

### 1. 拉取全部 TOP 200 币种近 30 天数据

```bash
python fetch_historical.py
```

**预计时间：** 25-40 分钟（取决于网络和代理）

**预计数据量：**
- 每个币种约 6-10 MB（8 个 parquet 文件）
- 200 币种 ≈ 1.5-2 GB

### 2. 先小规模测试（推荐先跑）

拉 5 个币种试水，确认流程跑通：

```bash
python fetch_historical.py --symbol RAVEUSDT SPKUSDT LABUSDT SAGAUSDT BIOUSDT
```

预计 1-2 分钟跑完。

### 3. 其他常用命令

```bash
# 只拉 100 个币（更快）
python fetch_historical.py --top-n 100

# 拉 14 天（更快）
python fetch_historical.py --days 14

# 断点续传（之前中断过的，跳过已拉完的）
python fetch_historical.py --resume

# 重新开始拉（清空旧数据）
python fetch_historical.py --cleanup

# 降低并发（如果遇到 429 限频）
python fetch_historical.py --concurrent 10

# 指定特定币种
python fetch_historical.py --symbol BTCUSDT ETHUSDT
```

### 4. 验证数据加载

```bash
python historical_data.py
```

会输出：
```
可用币种数: 200

RAVEUSDT 数据文件:
  klines_1m: 43200 行
  klines_5m: 8640 行
  ...

snapshot @ 2024-11-15 12:00:00 UTC
  klines_1h: 48
  klines_5m: 20
  ...
```

## ⚠️ 关键限制

### 有历史的数据（30 天内）

| 数据 | 接口 | 粒度 | 说明 |
|------|------|------|------|
| K线 | `/fapi/v1/klines` | 1m/5m/15m/1h | 可拉多年，但 OI 绑定 30 天 |
| 资金费率 | `/fapi/v1/fundingRate` | 8h | 完整历史 |
| OI 历史 | `/futures/data/openInterestHist` | 5m | **最多 30 天** |
| 多空账户比 | `/futures/data/globalLongShortAccountRatio` | 5m | **最多 30 天** |
| 主动买卖比 | `/futures/data/takerlongshortRatio` | 5m | **最多 30 天** |

### ❌ 没有历史的数据

- **订单簿** (`/fapi/v1/depth`) - 完全无历史
- 所以 `ob_imbalance` 因子无法完全回测

**解决方案：** 用主动买卖比 (`taker_5m`) 作为近似替代，或回测时给 `ob_imbalance` 一个中性固定值（如 12 分）。

## 📊 在回测脚本里使用

```python
from historical_data import HistoricalDataset

# 加载（lazy=True 按需加载，不占内存）
ds = HistoricalDataset(lazy=True)

# 所有可用币种
print(ds.symbols)

# 获取某币在某时刻的快照
import time
snapshot = ds.snapshot("RAVEUSDT", int(time.time() * 1000) - 86400 * 7 * 1000)

if snapshot.is_complete():
    # 传给 scanner 的 _analyze 重放
    price = snapshot.current_price()
    oi_usd = snapshot.latest_oi_usd()
    klines_1h = snapshot.klines_1h      # pandas DataFrame
    klines_5m = snapshot.klines_5m
    klines_15m = snapshot.klines_15m
    oi_hist = snapshot.oi_hist          # OI 历史
    ls_hist = snapshot.ls_hist          # 多空比历史
    taker_hist = snapshot.taker_hist    # 主动买卖比（OB 替代）
    funding = snapshot.funding          # 资金费率
```

## 🎯 下一步：写回测主脚本

拉完数据后，写 `backtest_full.py`：

```python
# 伪代码
from historical_data import HistoricalDataset

ds = HistoricalDataset(lazy=True)
t_start, t_end = ds.time_range()

# 每 5 分钟扫一次（模拟 scan_interval）
ts = t_start
while ts < t_end:
    for sym in ds.symbols:
        snap = ds.snapshot(sym, ts)
        if not snap.is_complete():
            continue
        # 重放 scanner._analyze(snap) 得到 signal
        # 重放 trader.try_open(signal) 模拟开仓
        # 检查止损/止盈/超时
    ts += 300_000  # +5 分钟
```

## 🐛 常见问题

### Q: 拉取中途被限频（429）

降低并发：
```bash
python fetch_historical.py --concurrent 10 --resume
```

### Q: 某些币的 oi_5m 为空

正常现象。部分新上线的币种 30 天前还没有 OI 数据，或者币种被下架。Manifest 会记录"已尝试但无数据"，避免重拉。

### Q: --days 超过 30 会怎样

OI 历史接口硬限制 30 天。脚本检测到后会强制改为 30 天并警告。K线本身可以拉更久，但 OI/多空比数据残缺，回测会不完整。

### Q: 代理设置

脚本会读取 `SCANNER_PROXY` 环境变量。如果没设置，默认 `http://127.0.0.1:10808`。

```bash
export SCANNER_PROXY=http://127.0.0.1:10808
python fetch_historical.py

# 或者不用代理（国外服务器）
SCANNER_PROXY="" python fetch_historical.py
```

## 📈 数据容量估算

每个币种 30 天数据：

| 数据类型 | 行数 | 大小 |
|---------|------|------|
| klines_1m | 43200 | ~2 MB |
| klines_5m | 8640 | ~400 KB |
| klines_15m | 2880 | ~150 KB |
| klines_1h | 720 | ~40 KB |
| oi_5m | 8640 | ~300 KB |
| ls_ratio_5m | 8640 | ~200 KB |
| taker_5m | 8640 | ~200 KB |
| funding | 90 | ~5 KB |
| **合计** | | **~3.3 MB** |

200 币种 ≈ **660 MB**
