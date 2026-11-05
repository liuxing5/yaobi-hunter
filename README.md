# 妖币猎手 v3

币安合约中小币种异动信号扫描 + 自动交易系统。

---

## 目录

- [操作说明](#操作说明)
- [部署指南](#部署指南)
- [评分体系](#评分体系)
- [结构信号评分](#结构信号评分)
- [止盈止损逻辑](#止盈止损逻辑)
- [完整参数表](#完整参数表)
- [文件说明](#文件说明)

---

## 操作说明

### 推荐操作流程

1. **第1周**：`TRADE_ENABLED=False` 纯观察信号质量
2. **第2周**：开启下单，`USDT_PER_TRADE=10` 小金额测试
3. **后续**：根据胜率逐步调整参数和投入

### Dashboard 使用

浏览器访问 `http://<服务器IP>:8765`

| 标签页 | 功能 |
|---|---|
| 信号 | 实时信号列表，点击查看因子详情和K线链接 |
| 观察信号 | 未达理想入场标准的信号，可按方向/高分筛选 |
| 持仓 | 当前持仓状态，支持手动平仓 |
| 参数 | 在线修改所有参数，立即生效，支持搜索和单个恢复默认 |

### 参数修改

三种方式：

1. **Dashboard 参数页**：搜索 → 修改 → 点击"应用"
2. **API 调用**：`POST /api/params/{key}` body: `{"value": 新值}`
3. **运行时文件**：自动保存到 `data/runtime_params.json`，重启后保留

### 回测观察信号

```bash
python -m tools.backtest_observe --days 1 --hours 4
```

按 score 分桶统计胜率和平均收益，验证信号质量。

### 关键调参建议

| 场景 | 调整 |
|---|---|
| 信号太少 | 降低 `MIN_SCORE`（如45），或提高 `W_OI_SURGE` 权重 |
| 信号太多/质量差 | 提高 `MIN_SCORE`（如65），开启 `STRICT_MARKET_BIAS` |
| 妖币行情 | `HALF_CLOSE_RATIO`→0.3，`POST_HALF_HOLD_HOURS`→48 |
| 稳健市场 | `ATR_SL_MULTIPLIER`→2.0，`HALF_CLOSE_RATIO`→0.5 |
| 止损太频繁 | 增大 `ATR_SL_MULTIPLIER`（如2.0） |
| 止盈太早 | 增大 `ATR_TP1_MULTIPLIER`（如3.0），减小 `HALF_CLOSE_RATIO` |

---

## 部署指南

### Render.com 部署（推荐）

项目已包含 [`render.yaml`](render.yaml)，支持 Render Blueprint 一键部署。

**前置条件**

- Render 账号：<https://dashboard.render.com/>
- GitHub 仓库：`liuxing5/yaobi-hunter`
- 币安合约 API Key / Secret（建议只开合约交易权限，**关闭提币权限**）
- Telegram Bot Token / Chat ID

**Blueprint 部署步骤**

1. 打开 Render Dashboard → **New** → **Blueprint**。
2. 连接 GitHub，选择仓库 `liuxing5/yaobi-hunter`。
3. 按提示填写 `BINANCE_API_KEY`、`BINANCE_SECRET`、`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`。
4. 点击 **Apply**，等待构建和健康检查（`/health`）通过。
5. 在服务的 **Environment** 页面查看自动生成的 `DASHBOARD_PASSWORD`；用户名是 `admin`。
6. 打开 `https://<服务名>.onrender.com`，浏览器会弹出登录框。

也可以直接使用部署链接：

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/liuxing5/yaobi-hunter)

**Render 注意事项（重要）**

| 事项 | 说明 |
|---|---|
| 免费实例会休眠 | Render 免费 Web Service 在约 15 分钟无入站请求后休眠，休眠期间**扫描和持仓监控会停止**。需要 7×24 自动交易，请把 `render.yaml` 里的 `plan: free` 改为 `starter`（约 $7/月），或在控制台升级实例 |
| 币安区域限制 | Render 美国区域访问 Binance 可能返回 HTTP 451。`render.yaml` 默认选择 `frankfurt`；如仍受限可尝试 `singapore` |
| 共享 IP 限频 | Binance 可能对 Render 共享出口 IP 返回 HTTP 418/429。代码已内置请求节流（`BINANCE_MIN_INTERVAL`）和封禁退避；如持续 418，建议换 region 或配置 `SCANNER_PROXY` |
| 磁盘不持久 | 免费实例的 `data/` 是临时目录，重新部署/重启后参数、持仓状态和信号日志会清空。重要参数请记录，或用数据库/持久磁盘（付费） |
| 密钥安全 | `.env` 已被 `.gitignore` 排除；Render 上只通过 Environment 注入密钥，不要提交到 Git |
| API 白名单 | 如果币安 API Key 绑定了 IP 白名单，Render 出口 IP 不固定，需要关闭白名单或改用固定出口 IP 的部署方式 |
| Dashboard 认证 | 公网部署务必保留 `DASHBOARD_USER` / `DASHBOARD_PASSWORD`；未设置时 Dashboard 和 API 完全公开，任何人可改参数或平仓 |

**手动创建 Web Service（不使用 Blueprint）**

| 配置项 | 值 |
|---|---|
| Runtime | Python 3 |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `python main.py` |
| Health Check Path | `/health` |
| Region | Frankfurt（或 Singapore） |

然后在 **Environment** 中添加：

```text
BINANCE_API_KEY=...
BINANCE_SECRET=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
PUSHPLUS_TOKEN=...          # 可选
DASHBOARD_USER=admin
DASHBOARD_PASSWORD=设置一个强密码
```

`SCANNER_PROXY` / `TG_PROXY` 在 Render 上**留空**（不要设置成 `http://127.0.0.1:...`）。程序会自动读取 Render 注入的 `PORT` 并监听 `0.0.0.0`。

> 默认 `TRADE_ENABLED=False`，只报警不下单。确认运行正常后，在 Dashboard 的「参数」页开启自动交易。

### 本地运行

复制环境变量模板并填写：

```bash
cp .env.example .env
```

本地如需代理，在 `.env` 中设置：

```text
SCANNER_PROXY=http://127.0.0.1:10808
TG_PROXY=http://127.0.0.1:10808
```

Render 上不要设置这两项。

### 安装依赖

```bash
pip install aiohttp --break-system-packages
```

### 配置密钥

```bash
cat >> ~/.bashrc << 'EOF'
export BINANCE_API_KEY="你的币安API_KEY"
export BINANCE_SECRET="你的币安SECRET"
export TELEGRAM_BOT_TOKEN="你的Bot_Token"
export TELEGRAM_CHAT_ID="你的Chat_ID"
EOF
source ~/.bashrc
```

获取 Telegram Chat ID：向 @userinfobot 发任意消息

### 启动

```bash
python3 main.py
```

### 后台常驻

```bash
sudo tee /etc/systemd/system/yaobi.service > /dev/null << EOF
[Unit]
Description=妖币猎手
After=network.target
[Service]
Type=simple
User=$USER
WorkingDirectory=$HOME/yaobi_hunter
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=15
Environment=BINANCE_API_KEY=你的KEY
Environment=BINANCE_SECRET=你的SECRET
Environment=TELEGRAM_BOT_TOKEN=你的Token
Environment=TELEGRAM_CHAT_ID=你的ChatID
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload && sudo systemctl enable yaobi && sudo systemctl start yaobi
```

### Dashboard

浏览器打开：`http://你的服务器IP:8765`

防火墙：`sudo ufw allow 8765`

---

## 评分体系

总分 = 8 个因子分数之和，满分 100。

### 因子权重（默认合计 100）

| 因子 | 权重 | 参数名 | 说明 |
|---|---|---|---|
| OI异动 | 25 | `W_OI_SURGE` | OI持仓量加速度，主力建仓核心信号 |
| 订单簿 | 20 | `W_ORDERBOOK` | 买卖盘失衡比，最实时的领先指标 |
| 成交量 | 15 | `W_VOLUME_SPIKE` | 近3根5m K线量能 vs 前3根的比值 |
| 资金费率 | 10 | `W_FUNDING_EXTREME` | 费率极端或斜率急变 |
| 价格压缩 | 10 | `W_PRICE_SQUEEZE` | 趋势结构分 + 触发结构分映射 |
| 多空比 | 10 | `W_LS_RATIO` | 散户多空持仓比极端 |
| 基差背离 | 5 | `W_BASIS_DIVERGE` | 现货-合约价差异常 |
| 5m动量 | 5 | `W_MOMENTUM_BREAKOUT` | 趋势动量 + 触发动量映射 |

### 评分计算流程

```
信号扫描 → 三组评分 → 映射到8因子 → 总分 → 大盘过滤 → 最终分数
```

**三组内部评分**（各组满分 100）：

| 组别 | 硬性门槛 | 包含子项 | 说明 |
|---|---|---|---|
| 趋势组 | ≥40 才通过 | structure(0-55) + momentum(0-45) | MA8/MA20趋势、价格结构、斜率 |
| 触发组 | ≥30 才通过 | oi(0-45) + ob(0-25) + momentum(0-35) + structure(0-25) + volume(0-15) | OI爆发、订单簿、5m动量、15m结构突破 |
| 情绪组 | 无硬性门槛 | funding(0-50) + ls_ratio(0-50) | 资金费率、多空比 |

**映射到 8 因子**：

| 因子 | 原始来源 | 映射公式 |
|---|---|---|
| oi_surge | trigger组 oi (0-45) | `min(W, int(oi_raw/45 × W))` |
| ob_imbalance | trigger组 ob (0-25) | `min(W, int(ob_raw/25 × W))` |
| volume_spike | trigger组 volume (0-15) | 直接取（已按权重缩放） |
| funding_extreme | emotion组 funding (0-50) | `min(W, int(funding_raw/50 × W))` |
| price_squeeze | trend结构 + trigger结构 (0-80) | `min(W, int(squeeze_raw/80 × W))` |
| ls_ratio | emotion组 ls_ratio (0-50) | `min(W, int(ls_raw/50 × W))` |
| basis_diverge | factors.basis 值 | 按阈值比例线性映射 (0-5) |
| momentum_breakout | trend动量 + trigger动量 (0-80) | `min(W, int(momentum_raw/80 × W))` |

### 大盘过滤

BTC 1H K线计算 MA8 vs MA24，每5分钟缓存一次：

| BTC状态 | 做多 | 做空 |
|---|---|---|
| BULL | 正常 | `STRICT_MARKET_BIAS=True`→禁止；`False`→扣30% |
| BEAR | `STRICT_MARKET_BIAS=True`→禁止；`False`→扣30% | 正常 |
| NEUTRAL | 正常 | 正常 |

### 信号分级

| 条件 | 级别 | 行为 |
|---|---|---|
| score ≥ 75 且 structure_trigger 含 "5m restart" 或 "strong hold" | 理想信号 | 推送TG + 自动下单(若开启) |
| score ≥ MIN_SCORE 但非理想 | 观察信号 | 仅推送TG和写日志，不下单 |

---

## 结构信号评分

15m K线突破+回踩判断，是触发组的核心子项：

| 结构触发 | 分数 | 含义 |
|---|---|---|
| `15m breakout+retest+5m restart` | 25 | 理想入场：突破→深回踩→5m重启确认 |
| `15m breakdown+retest+5m restart` | 25 | 理想入场（做空） |
| `15m breakout+strong hold` | 20 | 强势：突破后浅回踩即继续 |
| `15m breakout+retest(wait restart)` | 15 | 等待中：回踩了但5m未重启 |
| `15m breakout(pending)` | 10 | 刚突破，尚无回踩 |
| `15m fake breakout` | 0 | 假突破：回踩跌破关键位 |

---

## 止盈止损逻辑

### 止损

| 参数 | 默认 | 说明 |
|---|---|---|
| `ATR_SL_MULTIPLIER` | 1.5 | 初始止损距离 = ATR × 此倍数 |
| `STOP_LOSS_PCT` | 3% | ATR=0时的兜底止损比例 |

止损价 = 入场价 ∓ ATR × multiplier（LONG减/SHORT加），挂交易所 STOP_MARKET 单。

### 分批止盈

| 参数 | 默认 | 说明 |
|---|---|---|
| `ATR_TP1_MULTIPLIER` | 2.5 | 第一批止盈距离 = ATR × 此倍数 |
| `HALF_CLOSE_RATIO` | 0.4 | 第一批平仓比例（0.4=平四成） |

当标记价格 ≥ 入场价 + ATR × TP1倍数（LONG），触发分批止盈：
1. 市价平仓 `qty × HALF_CLOSE_RATIO`
2. 撤旧止损单，挂新移动止损单

### 移动止损

| 参数 | 默认 | 说明 |
|---|---|---|
| `TRAIL_STOP_ENABLED` | True | 开启移动止损 |
| `ATR_TRAIL_MULTIPLIER` | 0.8 | 移动止损距离 = ATR × 此倍数（比初始止损更紧，锁利润） |

分批止盈后，止损线只朝有利方向移动（LONG上移/SHORT下移），使用更紧的 `ATR_TRAIL_MULTIPLIER` 跟踪。

### 超时平仓

| 参数 | 默认 | 说明 |
|---|---|---|
| `MAX_HOLD_HOURS` | 12 | 普通持仓最长持有时间 |
| `POST_HALF_HOLD_HOURS` | 24 | 分批止盈后剩余仓位最长持有时间 |

---

## 完整参数表

### 信号扫描

| 参数 | 默认 | 类型 | 范围 | 说明 |
|---|---|---|---|---|
| `MIN_SCORE` | 70 | int | 20-100 | 触发信号最低综合评分 |
| `TOP_N_SYMBOLS` | 200 | int | 50-500 | 每轮扫描币种数（按24H成交额排序，已排除市值前30大币） |
| `SCAN_INTERVAL` | 60 | int | 30-300 | 扫描间隔（秒）。建议60s，太短易被限频 |
| `COOLDOWN_SECONDS` | 1800 | int | 600-14400 | 同一币种信号冷却时间（秒） |

### 市场状态

| 参数 | 默认 | 类型 | 范围 | 说明 |
|---|---|---|---|---|
| `ALLOW_VOLATILE_ENTRY` | False | bool | - | 是否允许在VOLATILE高波动状态下开仓 |

### 进场结构

| 参数 | 默认 | 类型 | 范围 | 说明 |
|---|---|---|---|---|
| `RETEST_TOLERANCE` | 0.3 | float | 0.1-2.0 | 回踩容忍度(%)。突破后回落在此百分比范围内视为有效回踩 |
| `RETEST_TIMEOUT_MIN` | 45 | int | 15-120 | 回踩等待最长时间(分钟) |
| `M5_RESTART_STRENGTH` | 0.5 | float | 0.1-2.0 | 5m启动强度(%)。5m K线从回踩低点反弹超过此比例才入场 |

### 趋势过滤

| 参数 | 默认 | 类型 | 范围 | 说明 |
|---|---|---|---|---|
| `MAX_24H_GAIN_LONG` | 40.0 | float | 5-100 | 做多时24H涨幅上限(%)。超过此值视为追高，禁止做多 |
| `MAX_24H_LOSS_SHORT` | 40.0 | float | 5-100 | 做空时24H跌幅上限(%)。超过此值视为追空，禁止做空 |
| `TREND_CONFIRM_PERIODS` | 1 | int | 1-10 | 趋势确认K线数 |
| `MIN_OI_USD_M` | 2.0 | float | 0.5-100 | 最小OI规模(百万USDT)。过小的币流动性差，跳过 |
| `STRICT_MARKET_BIAS` | True | bool | - | True=逆BTC方向直接跳过；False=逆BTC方向扣30%分 |
| `OBSERVE_TG_MIN_SCORE` | 70 | int | 50-100 | 观察信号推送TG的最低评分。低于此分数只写日志不推送 |

### 交易设置

| 参数 | 默认 | 类型 | 范围 | 说明 |
|---|---|---|---|---|
| `TRADE_ENABLED` | False | bool | - | **主开关**：是否开启自动下单。强烈建议先False观察一周 |
| `USDT_PER_TRADE` | 50.0 | float | 10-5000 | 每笔投入USDT（不含杠杆）。建议≤账户总资金5% |
| `LEVERAGE` | 10 | int | 1-20 | 合约杠杆倍数。妖币10x配合紧止损，建议≤10 |
| `MAX_OPEN_POSITIONS` | 3 | int | 1-20 | 最大同时持仓数 |
| `LONG_ENABLED` | True | bool | - | 允许做多 |
| `SHORT_ENABLED` | True | bool | - | 允许做空 |
| `MAX_TOTAL_RISK_PCT` | 15.0 | float | 5-30 | 账户最大总风险敞口(%) |

### 止盈止损

| 参数 | 默认 | 类型 | 范围 | 说明 |
|---|---|---|---|---|
| `STOP_LOSS_PCT` | 3.0 | float | 0.5-10 | 止损比例(%)。ATR=0时的兜底止损，高杠杆下需收紧 |
| `TAKE_PROFIT_PCT` | 20.0 | float | 1-50 | 止盈比例(%)。ATR=0时的兜底止盈，高杠杆下需收紧 |
| `TRAIL_STOP_ENABLED` | True | bool | - | 启用移动止损 |
| `MAX_HOLD_HOURS` | 12 | int | 1-168 | 最长持仓小时数，超时强制平仓 |
| `POST_HALF_HOLD_HOURS` | 24 | int | 1-168 | 分批止盈后剩余仓位最长持有时间 |
| `ATR_SL_MULTIPLIER` | 1.5 | float | 0.5-3.0 | 初始止损距离 = ATR × 此倍数 |
| `ATR_TRAIL_MULTIPLIER` | 0.8 | float | 0.3-2.0 | 移动止损距离 = ATR × 此倍数（比初始止损更紧，锁利润） |
| `ATR_TP1_MULTIPLIER` | 2.5 | float | 1.0-5.0 | 第一批止盈距离 = ATR × 此倍数 |
| `HALF_CLOSE_RATIO` | 0.4 | float | 0.2-0.8 | 第一批止盈平仓比例。0.4=平四成留六成 |

### 风控熔断

| 参数 | 默认 | 类型 | 范围 | 说明 |
|---|---|---|---|---|
| `DAILY_LOSS_LIMIT_PCT` | 5.0 | float | 1-20 | 每日最大亏损%(相对初始资金)。触发熔断后停止开新仓 |

### 因子权重（总和建议=100）

| 参数 | 默认 | 范围 | 说明 |
|---|---|---|---|
| `W_OI_SURGE` | 25 | 0-40 | OI持仓量加速度权重。主力建仓核心信号 |
| `W_ORDERBOOK` | 20 | 0-40 | 订单簿买卖比权重。最实时的领先指标 |
| `W_VOLUME_SPIKE` | 15 | 0-40 | 成交量爆量权重 |
| `W_FUNDING_EXTREME` | 10 | 0-30 | 资金费率斜率权重 |
| `W_PRICE_SQUEEZE` | 10 | 0-30 | 价格波动压缩权重 |
| `W_LS_RATIO` | 10 | 0-25 | 多空持仓比权重 |
| `W_BASIS_DIVERGE` | 5 | 0-25 | 基差背离权重 |
| `W_MOMENTUM_BREAKOUT` | 5 | 0-25 | 5分钟动量权重 |

### 因子阈值

| 参数 | 默认 | 范围 | 说明 |
|---|---|---|---|
| `TH_OI_SURGE_PCT` | 8.0 | 2-30 | OI 5分钟变化触发阈值(%) |
| `TH_VOLUME_SPIKE` | 3.0 | 1.5-10 | 成交量倍数阈值 |
| `TH_FUNDING_LONG` | 0.10 | 0.03-0.5 | 多头过热费率阈值(%) |
| `TH_FUNDING_SHORT` | 0.10 | 0.03-0.5 | 空头过热费率阈值(%) |
| `TH_LS_RATIO_HIGH` | 2.0 | 1.2-5.0 | 多空比上限 |
| `TH_LS_RATIO_LOW` | 0.5 | 0.1-0.9 | 多空比下限 |
| `TH_PRICE_SQUEEZE` | 0.30 | 0.05-1.0 | ATR/价格上限(%) |
| `TH_MOMENTUM` | 3.0 | 1-10 | 1H动量阈值(%) |
| `TH_BASIS_DIVERGE` | 0.50 | 0.1-2.0 | 基差背离阈值(%) |
| `TH_OB_RATIO_HIGH` | 2.5 | 1.5-5.0 | 买盘主导比值阈值 |
| `TH_OB_RATIO_LOW` | 0.4 | 0.1-0.8 | 卖盘主导比值阈值 |
| `TH_M5_MOMENTUM` | 0.8 | 0.3-3.0 | 5分钟动量阈值(%) |

---

## 文件说明

| 文件 | 说明 |
|---|---|
| `main.py` | 主程序：扫描循环、信号分发、持仓监控 |
| `scanner.py` | 8大因子扫描（自动过滤前30大币）、三组评分、结构信号判断 |
| `trader.py` | 开仓/平仓/止盈/止损/熔断/移动止损 |
| `config.py` | 所有参数定义（含类型、范围、说明） |
| `api_server.py` | HTTP API（端口8765），供Dashboard读写参数和持仓 |
| `notifier.py` | Telegram 通知推送 |
| `dashboard.html` | 浏览器监控台，信号/持仓/参数实时查看 |
| `tools/backtest_observe.py` | 观察信号回测脚本 |
