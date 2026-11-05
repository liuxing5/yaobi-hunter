"""
妖币猎手 v3 · 参数配置
密钥通过 .env 文件或环境变量注入，严禁硬编码
"""
import os, json
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

PARAM_SCHEMA = {
    # ─ 信号 ─
    "MIN_SCORE":              (50,    "int",   20,  100, "触发信号最低综合评分(0-100)。因子权重之和，满分100"),
    "TRADABLE_MIN_SCORE":     (50,    "int",   50,  100, "可交易信号的最低分。低于此分只观察不下单"),
    "TOP_N_SYMBOLS":          (200,   "int",   50,  500, "每轮扫描币种数（按24H成交额排序，已排除市值前30大币）"),
    "SCAN_INTERVAL":          (60,    "int",   30,  300, "扫描间隔（秒）。建议60s，太短易被限频"),
    "COOLDOWN_SECONDS":       (1800,  "int",   600, 14400,"同一币种信号冷却时间（秒）。复盘显示1小时冷却可避免反复误入"),

    # ─ 市场状态 ─
    "ALLOW_VOLATILE_ENTRY":   (False, "bool", None, None, "是否允许在VOLATILE高波动状态下开仓。False=只做趋势市，True=高波动也参与"),

    # ─ 进场结构优化 ─
    "RETEST_TOLERANCE":       (0.3,   "float", 0.1,  2.0,  "回踩容忍度(%)。突破后回落在此百分比范围内视为有效回踩"),
    "RETEST_TIMEOUT_MIN":     (45,    "int",   15,   120,  "回踩等待最长时间(分钟)。超过此时间未重新启动则失效"),
    "M5_RESTART_STRENGTH":    (0.5,   "float", 0.1,  2.0,  "5m启动强度(%)。5m K线从回踩低点反弹超过此比例才入场"),

    # ─ 趋势过滤（核心新增）─
    "MAX_24H_GAIN_LONG":      (40.0,  "float", 5.0, 100, "做多时24H涨幅上限(%)。超过此值视为追高，禁止做多（MOVR教训）"),
    "MAX_24H_LOSS_SHORT":     (40.0,  "float", 5.0, 100, "做空时24H跌幅上限(%)。超过此值视为追空，禁止做空"),
    "TREND_CONFIRM_PERIODS":  (1,     "int",   1,   10,  "趋势确认K线数。做多需最近N根1H K线均价递增，做空反之"),
    "MIN_OI_USD_M":           (2.0,   "float", 0.5, 100, "最小OI规模(百万USDT)。过小的币流动性差，滑点大，跳过"),
    "STRICT_MARKET_BIAS":     (True,  "bool",  None,None, "True=逆BTC方向直接跳过；False=逆BTC方向扣30%分"),
    "OBSERVE_TG_MIN_SCORE":   (70,    "int",   50, 100, "观察信号推送TG的最低评分。低于此分数只写日志不推送"),

    # ─ 交易 ─
    "TRADE_ENABLED":          (False, "bool",  None,None,"【主开关】是否开启自动下单。强烈建议先False观察一周"),
    "USDT_PER_TRADE":         (50.0,  "float", 10,  5000,"每笔投入USDT（不含杠杆）。建议≤账户总资金5%"),
    "LEVERAGE":               (5,     "int",   1,   20,  "合约杠杆倍数。降杠杆是保护资金最有效的手段，5x配合宽止损"),
    "MAX_OPEN_POSITIONS":     (3,     "int",   1,   20,  "最大同时持仓数"),
    "LONG_ENABLED":           (True,  "bool",  None,None,"允许做多"),
    "SHORT_ENABLED":          (True,  "bool",  None,None,"允许做空"),
    "MAX_TOTAL_RISK_PCT":     (15.0,  "float", 5, 30,    "账户最大总风险敞口(%)。所有持仓的潜在最大亏损不超过账户的此比例"),

    # ─ 止盈止损 ─
    "STOP_LOSS_PCT":          (3.0,   "float", 0.5, 10,  "止损比例(%)。ATR=0时的兜底止损，高杠杆下需收紧"),
    "TAKE_PROFIT_PCT":        (20.0,  "float", 1.0, 50,  "止盈比例(%)。ATR=0时的兜底止盈，高杠杆下需收紧"),
    "TRAIL_STOP_ENABLED":     (True,  "bool",  None,    None,   "启用移动止损。盈利超过止盈50%后，回撤超过止损触发"),
    "MAX_HOLD_HOURS":         (12,     "int",   1,   168, "最长持仓小时数，超时强制平仓"),
    "POST_HALF_HOLD_HOURS":   (24,     "int",   1,   168, "分批止盈后剩余仓位最长持有时间。妖币行情建议48"),
    "ATR_SL_MULTIPLIER":      (2.0,   "float", 0.5, 3.0,    "初始止损距离 = ATR × 此倍数。2.0倍ATR给足空间避免插针"),
    "ATR_TRAIL_MULTIPLIER":   (0.8,   "float", 0.3, 2.0,   "移动止损距离 = ATR × 此倍数。分批止盈后用此倍数跟踪，比初始止损更紧以锁利润"),
    "ATR_TP1_MULTIPLIER":     (2.5,   "float", 1.0, 5.0,    "第一批止盈距离 = ATR × 此倍数。2.5倍ATR约等于1.7R，盈亏比更优"),
    "HALF_CLOSE_RATIO":       (0.4,   "float", 0.2, 0.8,  "第一批止盈平仓比例。0.4=平四成留六成跟趋势"),
    "MAX_SL_RISK_PCT":        (0.6,   "float", 0.3, 0.9,  "单笔最大保证金风险比例。ATR×ATR_SL_MULTIPLIER×LEVERAGE > 此值跳过"),
    "MAX_ATR_PCT":            (5.0,   "float", 1.0, 15.0, "最大允许ATR%。高波动币高杠杆下止损距离过大，直接跳过"),

    # ─ 信号过滤增强 ─
    "OB_PERSIST_COUNT":       (3,     "int",   2,   10,  "订单簿持续性快照数。过去N次OB ratio均>阈值才通过，减少单次快照假信号"),
    "OB_PERSIST_THRESHOLD":   (1.8,   "float", 1.2, 3.0, "订单簿持续性比值阈值。OB ratio持续超过此值才算有效"),
    "FUNDING_DIR_FILTER":     (False, "bool",  None,None, "资金费率方向一致性过滤。做多需费率>0(多头付费)，做空需费率<0"),
    "BTC_ATR_VOL_THRESHOLD":  (1.5,   "float", 0.3, 3.0, "BTC 1H ATR/价格阈值(%)。超过此值说明大盘波动剧烈，小币种降仓位"),
    "BTC_VOL_POSITION_SCALE": (0.5,   "float", 0.1, 1.0, "大盘剧烈波动时仓位缩放系数。0.5=半仓"),

    # ─ 动态仓位 ─
    "CERTAINTY_MIN_FOR_BOOST":(80,    "int",   60,  100, "确定性分数阈值。≥此值且机会分数≥60时用1.5x仓位"),
    "OPPORTUNITY_MIN_FOR_BOOST":(60,  "int",   40,  100, "机会分数阈值。确定性≥80且机会≥此值时用1.5x仓位"),
    "POSITION_BOOST_SCALE":   (1.5,   "float", 1.0, 3.0, "高确定性信号仓位放大系数"),
    "POSITION_REDUCE_SCALE":  (0.7,   "float", 0.1, 1.0, "普通信号仓位缩小系数"),

    # ─ 分时段过滤 ─
    "PREFERRED_HOURS":        ("7,8,9,10,11,12", "str", None, None, "优选交易时段(UTC小时，逗号分隔)。这些时段信号不打折"),
    "OFF_HOUR_SCORE_DISCOUNT":(0.8,   "float", 0.5, 1.0, "非优选时段信号评分折扣。0.8=打八折"),

    # ─ 防重复信号 ─
    "DAILY_LOSS_COOLDOWN_H":  (4,     "int",   1,   48,  "当日亏损币冷却时间(小时)。同一币亏损后N小时内不再进场"),
    "SIGNAL_REPEAT_ZONE_PCT": (3.0,   "float", 1.0, 10.0, "信号重复区域(%)。新信号价格在历史失败信号价±此百分比内则跳过"),

    # ─ 假突破识别 ─
    "FAKE_BREAKOUT_MIN":      (99999,    "int",   5,   99999, "关闭假突破检测"),
    "FAKE_BREAKOUT_ATR_RATIO":(0.5,   "float", 0.2, 1.0, "假突破ATR比例。N分钟内价格需朝预期方向走ATR×此比例，否则判定假突破"),
    "FAKE_BK_HALF_MODE":      (False,  "bool",  None,None, "假突破检测后的处理方式。True=减半 False=全退"),

    # ─ 实验开关 ─
    "EXPERIMENTAL_FILTERS_ENABLED": (False, "bool", None,None,
        "是否启用实验性过滤器(假突破/时段折扣/重复区域/BTC波动/确定性分档)。False=只保留硬性风控"),

    # ─ 熔断 ─
    "DAILY_LOSS_LIMIT_PCT":   (5.0,   "float", 1.0, 20,  "每日最大亏损%(相对初始资金)。触发熔断后停止开新仓"),

    # ─ 因子权重（总和建议=100） ─
    "W_OI_SURGE":             (25,    "int",   0,   40,  "OI持仓量加速度权重。主力建仓核心信号"),
    "W_ORDERBOOK":            (20,    "int",   0,   40,  "订单簿买卖比权重。最实时的领先指标"),
    "W_VOLUME_SPIKE":         (15,    "int",   0,   40,  "成交量爆量权重"),
    "W_FUNDING_EXTREME":      (10,    "int",   0,   30,  "资金费率斜率权重"),
    "W_PRICE_SQUEEZE":        (10,    "int",   0,   30,  "价格波动压缩权重"),
    "W_LS_RATIO":             (10,    "int",   0,   25,  "多空持仓比权重"),
    "W_BASIS_DIVERGE":        (5,     "int",   0,   25,  "基差背离权重"),
    "W_MOMENTUM_BREAKOUT":    (5,     "int",   0,   25,  "5分钟动量权重"),
    "W_CAPITAL_FLOW":         (10,    "int",   0,   30,  "资金净流入/市值权重。异常资金流入是强确认信号"),
    "W_SMART_DUMB":           (10,    "int",   0,   30,  "大户vs散户多空对比权重。大户方向正确率更高"),

    # ─ 因子阈值 ─
    "TH_OI_SURGE_PCT":        (8.0,   "float", 2.0, 30,  "OI 5分钟变化触发阈值(%)"),
    "TH_VOLUME_SPIKE":        (3.0,   "float", 1.5, 10,  "成交量倍数阈值"),
    "TH_FUNDING_LONG":        (0.10,  "float", 0.03,0.5, "多头过热费率阈值(%)"),
    "TH_FUNDING_SHORT":       (0.10,  "float", 0.03,0.5, "空头过热费率阈值(%)"),
    "TH_LS_RATIO_HIGH":       (2.0,   "float", 1.2, 5.0, "多空比上限"),
    "TH_LS_RATIO_LOW":        (0.5,   "float", 0.1, 0.9, "多空比下限"),
    "TH_PRICE_SQUEEZE":       (0.30,  "float", 0.05,1.0, "ATR/价格上限(%)"),
    "TH_MOMENTUM":            (3.0,   "float", 1.0, 10,  "1H动量阈值(%)"),
    "TH_BASIS_DIVERGE":       (0.50,  "float", 0.1, 2.0, "基差背离阈值(%)"),
    "TH_OB_RATIO_HIGH":       (2.5,   "float", 1.5, 5.0, "买盘主导比值阈值"),
    "TH_OB_RATIO_LOW":        (0.4,   "float", 0.1, 0.8, "卖盘主导比值阈值"),
    "TH_M5_MOMENTUM":         (0.8,   "float", 0.3, 3.0, "5分钟动量阈值(%)"),
    "TH_CAPITAL_FLOW_HIGH":   (2.0,   "float", 0.5, 5.0, "资金净流入/市值异常高阈值(%)"),
    "TH_CAPITAL_FLOW_MID":    (0.5,   "float", 0.1, 2.0, "资金净流入/市值正常强势阈值(%)"),
    "TH_SMART_DUMB_RATIO":    (1.3,   "float", 1.0, 2.0, "大户vs散户多空偏离阈值。大户偏多>此值且散户偏空时触发"),
    "TH_FUNDING_SWEET_LOW":   (-0.05, "float", -0.1, 0.0, "资金费率甜蜜区下限(%)"),
    "TH_FUNDING_SWEET_HIGH":  (0.05,  "float", 0.0, 0.1, "资金费率甜蜜区上限(%)"),
}

TOP30_BLACKLIST = {
    "BTC","ETH","BNB","SOL","XRP","DOGE","ADA","AVAX","SHIB","DOT",
    "LINK","MATIC","LTC","BCH","UNI","ATOM","XLM","ETC","ALGO","VET",
    "ICP","FIL","APT","ARB","OP","INJ","SUI","TRX","TON","NEAR",
}

STOCK_RELATED_BLACKLIST = {
    "COIN","TSLA","AAPL","GOOG","NVDA","AMZN","MSTR","MARA","HOOD",
    "SQ","RIOT","CLSK","HUT","BTBT","BFCH","CBSE",
}

_RUNTIME_FILE = Path(__file__).parent / "data" / "runtime_params.json"


class Config:
    def __init__(self):
        self.BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
        self.BINANCE_SECRET     = os.getenv("BINANCE_SECRET", "")
        self.TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
        self.PUSHPLUS_TOKEN     = os.getenv("PUSHPLUS_TOKEN", "")
        self.SIGNAL_LOG_PATH    = "data/signals.jsonl"
        self.STATE_PATH         = "data/state.json"

        for key, (default, *_) in PARAM_SCHEMA.items():
            setattr(self, key, default)
        self._load_runtime()

    def validate(self):
        """启动时校验必填配置"""
        missing = [k for k in ["BINANCE_API_KEY","BINANCE_SECRET","TELEGRAM_BOT_TOKEN","TELEGRAM_CHAT_ID"]
                   if not getattr(self, k)]
        if missing:
            raise ValueError(f"缺少必填环境变量: {missing}\n"
                             f"请执行: export BINANCE_API_KEY=xxx 等")

    def _load_runtime(self):
        if _RUNTIME_FILE.exists():
            try:
                for k, v in json.loads(_RUNTIME_FILE.read_text()).items():
                    if k in PARAM_SCHEMA:
                        setattr(self, k, v)
            except Exception:
                pass

    def save_runtime(self):
        _RUNTIME_FILE.parent.mkdir(exist_ok=True)
        _RUNTIME_FILE.write_text(
            json.dumps({k: getattr(self, k) for k in PARAM_SCHEMA}, indent=2, ensure_ascii=False)
        )

    def update(self, key: str, value) -> tuple[bool, str]:
        if key not in PARAM_SCHEMA:
            return False, "未知参数"
        _, typ, lo, hi, _ = PARAM_SCHEMA[key]
        try:
            if typ == "int":     value = int(value)
            elif typ == "float": value = float(value)
            elif typ == "bool":
                value = value.lower() in ("true","1","yes") if isinstance(value, str) else bool(value)
        except Exception:
            return False, "类型转换失败"
        if lo is not None and value < lo:
            return False, f"不能小于 {lo}"
        if hi is not None and value > hi:
            return False, f"不能大于 {hi}"
        setattr(self, key, value)
        self.save_runtime()
        return True, "OK"

    def as_dict(self):
        return {k: getattr(self, k) for k in PARAM_SCHEMA}

    @property
    def FACTOR_WEIGHTS(self):
        return {
            "oi_surge":          self.W_OI_SURGE,
            "ob_imbalance":      self.W_ORDERBOOK,
            "volume_spike":      self.W_VOLUME_SPIKE,
            "funding_extreme":   self.W_FUNDING_EXTREME,
            "price_squeeze":     self.W_PRICE_SQUEEZE,
            "ls_ratio":          self.W_LS_RATIO,
            "basis_diverge":     self.W_BASIS_DIVERGE,
            "momentum_breakout": self.W_MOMENTUM_BREAKOUT,
            "capital_flow":      self.W_CAPITAL_FLOW,
            "smart_dumb":        self.W_SMART_DUMB,
        }

    @property
    def THRESHOLDS(self):
        return {
            "oi_surge_pct":      self.TH_OI_SURGE_PCT / 100,
            "volume_spike_ratio":self.TH_VOLUME_SPIKE,
            "funding_long":      self.TH_FUNDING_LONG / 100,
            "funding_short":    -self.TH_FUNDING_SHORT / 100,
            "ls_ratio_high":     self.TH_LS_RATIO_HIGH,
            "ls_ratio_low":      self.TH_LS_RATIO_LOW,
            "price_squeeze_atr": self.TH_PRICE_SQUEEZE,
            "momentum_breakout": self.TH_MOMENTUM / 100,
            "basis_diverge":     self.TH_BASIS_DIVERGE / 100,
            "ob_ratio_high":     self.TH_OB_RATIO_HIGH,
            "ob_ratio_low":      self.TH_OB_RATIO_LOW,
            "m5_momentum":       self.TH_M5_MOMENTUM / 100,
            "retest_tolerance":  self.RETEST_TOLERANCE / 100,
            "retest_timeout":    self.RETEST_TIMEOUT_MIN,
            "m5_restart":        self.M5_RESTART_STRENGTH / 100,
            "ob_persist_threshold": self.OB_PERSIST_THRESHOLD,
            "capital_flow_high": self.TH_CAPITAL_FLOW_HIGH / 100,
            "capital_flow_mid":  self.TH_CAPITAL_FLOW_MID / 100,
            "smart_dumb_ratio":  self.TH_SMART_DUMB_RATIO,
            "funding_sweet_low": self.TH_FUNDING_SWEET_LOW / 100,
            "funding_sweet_high": self.TH_FUNDING_SWEET_HIGH / 100,
        }

    @property
    def PREFERRED_HOURS_LIST(self):
        try:
            return [int(h.strip()) for h in self.PREFERRED_HOURS.split(",") if h.strip()]
        except Exception:
            return [7, 8, 9, 10, 11, 12]
