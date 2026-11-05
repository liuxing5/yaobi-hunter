"""
backtest_full.py — 完整回测脚本

核心思路：
  用一个 MockBinanceClient 伪装成真实的 BinanceClient，让 scanner.YaoBiScanner
  认为自己在实时扫描，但数据源来自 historical_data.HistoricalDataset。

  这样 scanner 的逻辑一行不改，100%复用。

用法：
  python backtest_full.py
  python backtest_full.py --start 2025-03-20 --end 2025-04-15
  python backtest_full.py --interval 300          # 每5分钟扫一次
  python backtest_full.py --symbol-limit 50       # 只用前50币
  python backtest_full.py --ob-score 12           # 订单簿固定给12分(没有历史数据)
  python backtest_full.py --output data/backtest_result.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
except ImportError:
    print("请先安装: pip install pandas pyarrow")
    sys.exit(1)

# 允许从当前目录导入 scanner / config
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from historical_data import HistoricalDataset, SymbolSnapshot
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── Mock BinanceClient ─────────────────────────────────────────
# 不能直接继承 scanner.BinanceClient（会真的去连网），但 scanner 通过 duck-typing
# 调用 self.client.get(path, params) / get_orderbook_ratio(sym) 等方法，所以只要
# 我们的 Mock 实现这些方法即可。

class MockBinanceClient:
    """
    伪装成 BinanceClient，但所有数据从 HistoricalDataset 读取。
    关键：scanner._analyze 里会 gather 请求以下 endpoints:
        /fapi/v1/openInterest               -> OI 当前值
        /fapi/v1/ticker/24hr                -> 24h ticker（含 lastPrice/quoteVolume/priceChangePercent）
        /fapi/v1/fundingRate                -> 资金费率
        /futures/data/globalLongShortAccountRatio  -> 多空账户比
        /fapi/v1/klines                     -> K线（BTC 1h / symbol 1h/5m/15m）
    还有：
        get_orderbook_ratio(sym)            -> 订单簿买卖比（历史无数据，用固定值）
    """
    def __init__(
        self,
        dataset: HistoricalDataset,
        ob_score_default: float = 1.0,  # 订单簿ratio的默认值（1.0=中性）
    ):
        self.dataset = dataset
        self.ob_score_default = ob_score_default
        self._now_ms: int = 0        # 当前模拟时间戳
        self._current_symbol: Optional[str] = None
        self._current_snap: Optional[SymbolSnapshot] = None
        # 缓存 symbolinfo 避免重复查（exchange info）
        self._tick_cache: dict = {}
        self._step_cache: dict = {}
        # 缓存快照数据，避免重复调用 dataset.snapshot
        self._snap_cache: dict = {}  # key: (symbol, ts_ms), value: snapshot

    # ── 时间控制 ──
    def set_time(self, ts_ms: int):
        self._now_ms = ts_ms

    def set_symbol(self, symbol: str):
        """切换当前币种（优化 snapshot 调用次数）"""
        if self._current_symbol != symbol:
            self._current_symbol = symbol
            self._current_snap = self.dataset.snapshot(symbol, self._now_ms)

    def _get_snap(self, symbol: str) -> SymbolSnapshot:
        cache_key = (symbol, self._now_ms)
        if cache_key not in self._snap_cache:
            self._snap_cache[cache_key] = self.dataset.snapshot(symbol, self._now_ms)
        return self._snap_cache[cache_key]

    # ── scanner 会调用的方法 ──
    async def get(self, path: str, params: dict = None, base: str = None):
        """代理 scanner.BinanceClient.get（scanner._analyze / _get_market_bias 等都用它）"""
        params = params or {}
        sym = params.get("symbol", "")

        # ── /fapi/v1/openInterest ──
        if path == "/fapi/v1/openInterest":
            snap = self._get_snap(sym)
            if snap.oi_hist is not None and len(snap.oi_hist) > 0:
                oi_amount = float(snap.oi_hist["sumOpenInterest"].iloc[-1])
                return {"symbol": sym, "openInterest": str(oi_amount)}
            return {"symbol": sym, "openInterest": "0"}

        # ── /fapi/v1/ticker/24hr ──
        if path == "/fapi/v1/ticker/24hr":
            if not sym:
                # scanner._get_symbols 拉取全部，回测里用 dataset.symbols 替代
                return []
            snap = self._get_snap(sym)
            if snap.klines_1h is None or len(snap.klines_1h) == 0:
                raise Exception(f"{sym} no kline data")
            # 用最近24h的K线近似重建24h ticker
            h24 = snap.klines_1h.tail(24)
            last_price = float(h24["close"].iloc[-1])
            first_price = float(h24["open"].iloc[0])
            price_chg_pct = (last_price - first_price) / first_price * 100 if first_price > 0 else 0
            quote_vol = float(h24["quote_volume"].sum())
            return {
                "symbol": sym,
                "lastPrice": str(last_price),
                "priceChangePercent": f"{price_chg_pct:.4f}",
                "quoteVolume": str(quote_vol),
            }

        # ── /fapi/v1/fundingRate ──
        if path == "/fapi/v1/fundingRate":
            snap = self._get_snap(sym)
            if snap.funding is None or len(snap.funding) == 0:
                return []
            limit = params.get("limit", 3)
            recent = snap.funding.tail(limit)
            return [
                {
                    "symbol": sym,
                    "fundingRate": str(float(row["fundingRate"])),
                    "fundingTime": int(row["fundingTime"]),
                }
                for _, row in recent.iterrows()
            ]

        # ── /futures/data/globalLongShortAccountRatio ──
        if path == "/futures/data/globalLongShortAccountRatio":
            snap = self._get_snap(sym)
            if snap.ls_hist is None or len(snap.ls_hist) == 0:
                return []
            limit = params.get("limit", 6)
            recent = snap.ls_hist.tail(limit)
            return [
                {
                    "symbol": sym,
                    "longShortRatio": str(float(row["longShortRatio"])),
                    "longAccount": str(float(row.get("longAccount", 0))),
                    "shortAccount": str(float(row.get("shortAccount", 0))),
                    "timestamp": int(row["timestamp"]),
                }
                for _, row in recent.iterrows()
            ]

        # ── /futures/data/topLongShortAccountRatio ──
        # 优先用真实的大户账户多空比数据；若无则退化为 ls_ratio^1.3 近似（兼容旧数据集）
        if path == "/futures/data/topLongShortAccountRatio":
            snap = self._get_snap(sym)
            if snap.top_acc_hist is not None and len(snap.top_acc_hist) > 0:
                limit = params.get("limit", 1)
                recent = snap.top_acc_hist.tail(limit)
                return [
                    {
                        "symbol": sym,
                        "longShortRatio": str(float(row["longShortRatio"])),
                        "longAccount": str(float(row.get("longAccount", 0))),
                        "shortAccount": str(float(row.get("shortAccount", 0))),
                        "timestamp": int(row["timestamp"]),
                    }
                    for _, row in recent.iterrows()
                ]
            # Fallback：老数据集没拉大户时退化为近似（会打一次warning）
            if not getattr(self, "_warned_no_top_acc", False):
                logger.warning(f"{sym} 无真实 top_acc_hist，退化为 ls_ratio^1.3 近似。请重新运行 fetch_historical.py 拉真实数据")
                self._warned_no_top_acc = True
            if snap.ls_hist is None or len(snap.ls_hist) == 0:
                return []
            limit = params.get("limit", 1)
            recent = snap.ls_hist.tail(limit)
            return [
                {
                    "symbol": sym,
                    "longShortRatio": str(float(row["longShortRatio"]) ** 1.3),
                    "longAccount": str(float(row.get("longAccount", 0))),
                    "shortAccount": str(float(row.get("shortAccount", 0))),
                    "timestamp": int(row["timestamp"]),
                }
                for _, row in recent.iterrows()
            ]

        # ── /futures/data/topLongShortPositionRatio ──
        # 大户持仓多空比（按金额加权）
        if path == "/futures/data/topLongShortPositionRatio":
            snap = self._get_snap(sym)
            if snap.top_pos_hist is None or len(snap.top_pos_hist) == 0:
                return []
            limit = params.get("limit", 1)
            recent = snap.top_pos_hist.tail(limit)
            return [
                {
                    "symbol": sym,
                    "longShortRatio": str(float(row["longShortRatio"])),
                    "longAccount": str(float(row.get("longAccount", 0))),
                    "shortAccount": str(float(row.get("shortAccount", 0))),
                    "timestamp": int(row["timestamp"]),
                }
                for _, row in recent.iterrows()
            ]

        # ── /fapi/v1/klines ──
        if path == "/fapi/v1/klines":
            interval = params.get("interval", "1h")
            limit = params.get("limit", 500)
            # BTC 的 K线（scanner _get_market_bias / _get_btc_atr_pct 用）
            if sym == "BTCUSDT":
                snap = self.dataset.snapshot("BTCUSDT", self._now_ms)
            else:
                snap = self._get_snap(sym)

            df_key = {
                "1m":  "klines_1m",
                "5m":  "klines_5m",
                "15m": "klines_15m",
                "1h":  "klines_1h",
            }.get(interval)
            if df_key is None:
                return []

            df = getattr(snap, df_key, None)
            if df is None or len(df) == 0:
                raise Exception(f"{sym} no {interval} klines")

            recent = df.tail(limit)
            # 还原成币安原生格式: [open_time, o, h, l, c, v, close_time, quote_vol, trades, taker_buy, taker_buy_quote, ignore]
            return [
                [
                    int(row["open_time"]),
                    str(float(row["open"])),
                    str(float(row["high"])),
                    str(float(row["low"])),
                    str(float(row["close"])),
                    str(float(row["volume"])),
                    int(row["close_time"]),
                    str(float(row["quote_volume"])),
                    int(row.get("trades", 0)),
                    str(float(row.get("taker_buy_volume", 0))),
                    str(float(row.get("taker_buy_quote_volume", 0))),
                    "0",
                ]
                for _, row in recent.iterrows()
            ]

        # ── /fapi/v1/exchangeInfo ──（scanner.load_exchange_info 用）
        if path == "/fapi/v1/exchangeInfo":
            return {"symbols": []}

        # ── /fapi/v1/ticker/price（trader 平仓时用）──
        if path == "/fapi/v1/ticker/price":
            # 回测里 trader 不真的下单，不会调到这里
            return []

        # 未识别的path
        logger.warning(f"MockClient: 未处理 {path} params={params}")
        return None

    async def get_orderbook_ratio(self, symbol: str) -> float:
        """用 ls_ratio（多空账户比）模拟订单簿买卖比。
        
        逻辑：多空账户比和订单簿买卖比有强相关性。
        ls_ratio > 1 说明多头多 → 买盘强 → ob_ratio > 1
        ls_ratio < 1 说明空头多 → 卖盘强 → ob_ratio < 1
        
        映射公式：ob_ratio = ls_ratio^1.5（放大偏离程度，使分布更接近真实订单簿）
        """
        if self.ob_score_default != 1.0:
            return self.ob_score_default
        snap = self._get_snap(symbol)
        if snap.ls_hist is not None and len(snap.ls_hist) > 0:
            ls_ratio = float(snap.ls_hist["longShortRatio"].iloc[-1])
            if ls_ratio > 0:
                simulated_ob = ls_ratio ** 1.5
                return max(0.1, min(5.0, simulated_ob))
        return 1.0

    async def get_tick_size(self, symbol: str) -> float:
        return self._tick_cache.get(symbol, 0.0001)

    async def get_step_size(self, symbol: str) -> float:
        return self._step_cache.get(symbol, 0.001)

    async def load_exchange_info(self):
        # 回测里不需要真实 tick/step
        pass

    async def close(self):
        pass


# ── 持仓与交易模拟 ─────────────────────────────────────────────
@dataclass
class SimPosition:
    symbol: str
    direction: str   # LONG / SHORT
    entry_price: float
    entry_ts_ms: int
    qty: float
    usdt: float
    leverage: float
    atr: float
    score: int
    structure_trigger: str
    stop_loss_px: float
    take_profit_px: float
    peak_price: float = 0.0
    peak_pnl_pct: float = 0.0
    half_closed: bool = False
    half_pnl: float = 0.0
    half_ts_ms: int = 0
    half_pnl_usdt: float = 0.0   # 第一批止盈的实际USDT盈亏
    fake_bk_half: bool = False

    def pnl_pct(self, cur: float) -> float:
        if self.entry_price <= 0:
            return 0.0
        if self.direction == "LONG":
            return (cur - self.entry_price) / self.entry_price * 100
        return (self.entry_price - cur) / self.entry_price * 100


@dataclass
class SimTradeResult:
    symbol: str
    direction: str
    score: int
    structure_trigger: str
    entry_price: float
    entry_ts_ms: int
    entry_time: str
    exit_price: float
    exit_ts_ms: int
    exit_time: str
    hold_min: float
    usdt: float
    leverage: float
    atr_pct: float
    pnl_pct: float           # 最终总盈亏%（按保证金计算）
    pnl_usdt: float          # 最终总盈亏USDT
    status: str              # SL / TP_TRAIL / TIMEOUT / FAKE_BK_HALF / FAKE_BK_FULL
    half_pnl: float = 0.0    # 第一批止盈的pnl_pct
    half_pnl_usdt: float = 0.0
    second_pnl: float = 0.0  # 第二批的pnl_pct


class TradeSimulator:
    """
    模拟 trader.py 的 try_open / monitor_positions 逻辑。
    每次 scanner 给出信号就尝试开仓，然后按后续 1m K线 evolve 仓位，直到触发止盈/止损/超时。
    """
    def __init__(self, config: Config, dataset: HistoricalDataset):
        self.config = config
        self.dataset = dataset
        self.positions: dict[str, SimPosition] = {}
        self.results: list[SimTradeResult] = []
        self._last_open_ts_ms = 0
        self._today_losses: dict[str, str] = {}  # symbol -> "YYYY-MM-DD"
        self._signal_history: dict[str, list] = {}  # symbol -> [{price, failed, time}]
        self._blacklist: dict[str, int] = {}  # symbol -> ts_ms 黑名单到期
        self._loss_count: dict[str, int] = {}
        # 滑点和手续费设置
        self.SLIPPAGE_PCT = 0.15  # 滑点 0.15%
        self.FEE_PCT = 0.04  # 手续费 0.04% per trade

    @staticmethod
    def is_tradable(sig: dict, min_score: int) -> bool:
        structure = sig.get("factors", {}).get("structure_trigger", "")
        return (
            sig.get("score", 0) >= min_score
            # and ("5m restart" in structure or "strong hold" in structure)
        )

    def can_open(self, symbol: str, direction: str, ts_ms: int) -> tuple[bool, str]:
        """返回 (能否开, 拒绝原因)"""
        # 黑名单
        if symbol in self._blacklist and ts_ms < self._blacklist[symbol]:
            return False, f"blacklist"
        # 开仓间隔 10 分钟
        if ts_ms - self._last_open_ts_ms < 600_000:
            return False, "cooldown<10min"
        # 股票币
        base = symbol.replace("USDT", "")
        if base in self.config.__dict__.get("STOCK_RELATED_BLACKLIST", set()):
            return False, "stock"
        # 当日已亏损
        today_str = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if self._today_losses.get(symbol) == today_str:
            return False, "day_loss"
        # 持仓满
        if len(self.positions) >= self.config.MAX_OPEN_POSITIONS:
            return False, "max_pos"
        # 已有仓
        if symbol in self.positions:
            return False, "already_open"
        # 开关
        if direction == "LONG" and not self.config.LONG_ENABLED:
            return False, "LONG_disabled"
        if direction == "SHORT" and not self.config.SHORT_ENABLED:
            return False, "SHORT_disabled"
        return True, ""

    def try_open(self, signal: dict, ts_ms: int) -> Optional[SimPosition]:
        """尝试开仓。返回 position 或 None（被过滤）"""
        sym = signal["symbol"]
        direction = signal.get("direction", "LONG")
        score = signal.get("score", 0)

        # is_tradable
        if not self.is_tradable(signal, self.config.TRADABLE_MIN_SCORE):
            return None

        # 重复区域过滤
        exp_filters = self.config.EXPERIMENTAL_FILTERS_ENABLED
        entry_price = float(signal.get("price", 0))
        if exp_filters and sym in self._signal_history:
            for hist in self._signal_history[sym]:
                if hist.get("failed") and hist.get("price", 0) > 0:
                    if abs(entry_price - hist["price"]) / hist["price"] * 100 < self.config.SIGNAL_REPEAT_ZONE_PCT:
                        # print(f"重复区域过滤: {sym} {entry_price} vs {hist['price']}")
                        return None

        can, reason = self.can_open(sym, direction, ts_ms)
        if not can:
            # print(f"can_open失败: {sym} {direction} {reason}")
            return None

        # 仓位分级
        certainty = signal.get("certainty_score", 0)
        opportunity = signal.get("opportunity_score", 0)
        if (exp_filters and 
            certainty >= self.config.CERTAINTY_MIN_FOR_BOOST and 
            opportunity >= self.config.OPPORTUNITY_MIN_FOR_BOOST):
            usdt = self.config.USDT_PER_TRADE * self.config.POSITION_BOOST_SCALE
        else:
            usdt = self.config.USDT_PER_TRADE

        # BTC波动降仓
        if exp_filters:
            btc_atr_str = signal.get("factors", {}).get("btc_atr_pct", "0%")
            try:
                btc_atr_val = float(btc_atr_str.replace("%", ""))
                if btc_atr_val > self.config.BTC_ATR_VOL_THRESHOLD:
                    usdt = usdt * self.config.BTC_VOL_POSITION_SCALE
            except ValueError:
                pass

        if usdt < 11:
            # print(f"usdt不足: {usdt}")
            return None

        # 应用开仓滑点
        if direction == "LONG":
            # 开仓时买，滑点使价格变高
            entry_price = entry_price * (1 + self.SLIPPAGE_PCT / 100)
        else:
            # 开仓时卖，滑点使价格变低
            entry_price = entry_price * (1 - self.SLIPPAGE_PCT / 100)

        # ATR止损止盈
        atr = float(signal.get("atr", 0))
        if atr <= 0:
            atr = entry_price * self.config.STOP_LOSS_PCT / 100

        # MAX_ATR_PCT
        atr_pct = atr / entry_price * 100 if entry_price > 0 else 0
        if atr_pct > self.config.MAX_ATR_PCT:
            # print(f"ATR过大: {atr_pct} > {self.config.MAX_ATR_PCT}")
            return None

        if direction == "LONG":
            atr_sl = entry_price - atr * self.config.ATR_SL_MULTIPLIER
            structure_sl = signal.get("factors", {}).get("structure_sl")
            if structure_sl and structure_sl > atr_sl:
                sl_px = structure_sl
            else:
                sl_px = atr_sl
            tp_px = entry_price + atr * self.config.ATR_TP1_MULTIPLIER
        else:
            atr_sl = entry_price + atr * self.config.ATR_SL_MULTIPLIER
            structure_sl = signal.get("factors", {}).get("structure_sl")
            if structure_sl and structure_sl < atr_sl:
                sl_px = structure_sl
            else:
                sl_px = atr_sl
            tp_px = entry_price - atr * self.config.ATR_TP1_MULTIPLIER

        if sl_px <= 0 or tp_px <= 0:
            # print(f"止损止盈无效: sl={sl_px}, tp={tp_px}")
            return None

        # MAX_SL_RISK_PCT
        sl_distance_pct = abs(entry_price - sl_px) / entry_price
        risk_pct = sl_distance_pct * self.config.LEVERAGE
        if risk_pct > self.config.MAX_SL_RISK_PCT:
            # print(f"风险过大: {risk_pct} > {self.config.MAX_SL_RISK_PCT}")
            return None

        qty = usdt * self.config.LEVERAGE / entry_price

        pos = SimPosition(
            symbol=sym,
            direction=direction,
            entry_price=entry_price,
            entry_ts_ms=ts_ms,
            qty=qty,
            usdt=usdt,
            leverage=self.config.LEVERAGE,
            atr=atr,
            score=score,
            structure_trigger=signal.get("factors", {}).get("structure_trigger", ""),
            stop_loss_px=sl_px,
            take_profit_px=tp_px,
            peak_price=entry_price,
        )
        self.positions[sym] = pos
        self._last_open_ts_ms = ts_ms
        self._signal_history.setdefault(sym, []).append({
            "price": entry_price, "time": ts_ms, "failed": False,
        })
        # print(f"开仓成功: {sym} {direction} {entry_price}")
        return pos

    def step_position(self, pos: SimPosition, cur_ts_ms: int, high: float, low: float, close: float) -> Optional[str]:
        """
        用1根K线evolve持仓，返回平仓原因或None。
        注意：这里要分析"在这根K线内是否触发了某个价位"。
        如果 high/low 触及止损/止盈，按最坏情况成交。
        """
        # 更新峰值
        if pos.direction == "LONG":
            if high > pos.peak_price:
                pos.peak_price = high
                pos.peak_pnl_pct = (high - pos.entry_price) / pos.entry_price * 100
        else:
            if low < pos.peak_price:
                pos.peak_price = low
                pos.peak_pnl_pct = (pos.entry_price - low) / pos.entry_price * 100

        hold_min = (cur_ts_ms - pos.entry_ts_ms) / 60_000

        # 已分批后的移动止损
        if pos.half_closed and self.config.TRAIL_STOP_ENABLED:
            if pos.direction == "LONG":
                new_sl = pos.peak_price - pos.atr * self.config.ATR_TRAIL_MULTIPLIER
                if new_sl > pos.stop_loss_px:
                    pos.stop_loss_px = new_sl
            else:
                new_sl = pos.peak_price + pos.atr * self.config.ATR_TRAIL_MULTIPLIER
                if new_sl < pos.stop_loss_px:
                    pos.stop_loss_px = new_sl

        # 检测止损触发（优先，因为是保护底线）
        if pos.direction == "LONG":
            if low <= pos.stop_loss_px:
                return "SL" if not pos.half_closed else "SL_TRAIL"
        else:
            if high >= pos.stop_loss_px:
                return "SL" if not pos.half_closed else "SL_TRAIL"

        # 超时检测
        if not pos.half_closed and hold_min >= self.config.MAX_HOLD_HOURS * 60:
            return "TIMEOUT"
        if pos.half_closed and hold_min >= self.config.POST_HALF_HOLD_HOURS * 60:
            return "POST_HALF_TIMEOUT"

        # 第一批止盈
        if not pos.half_closed:
            if pos.direction == "LONG" and high >= pos.take_profit_px:
                return "HALF_CLOSE"
            if pos.direction == "SHORT" and low <= pos.take_profit_px:
                return "HALF_CLOSE"

        # 假突破检测
        exp_filters = self.config.EXPERIMENTAL_FILTERS_ENABLED
        if exp_filters and not pos.half_closed and hold_min >= self.config.FAKE_BREAKOUT_MIN and pos.atr > 0:
            # 用close判断（这根K线结束时）
            if pos.direction == "LONG":
                favorable_move = (close - pos.entry_price) / pos.atr
            else:
                favorable_move = (pos.entry_price - close) / pos.atr
            if favorable_move < self.config.FAKE_BREAKOUT_ATR_RATIO:
                if self.config.FAKE_BK_HALF_MODE:
                    pos.fake_bk_half = True
                    return "FAKE_BK_HALF"
                else:
                    return "FAKE_BK_FULL"

        return None

    def close_position(
        self,
        pos: SimPosition,
        exit_price: float,
        exit_ts_ms: int,
        reason: str,
        is_half: bool = False,
    ) -> Optional[SimTradeResult]:
        """平仓（可能是全平或半平）。全平时返回 SimTradeResult，半平时返回None"""
        # 应用滑点
        if pos.direction == "LONG":
            # 平仓时卖，滑点使价格变低
            exit_price = exit_price * (1 - self.SLIPPAGE_PCT / 100)
        else:
            # 平仓时买，滑点使价格变高
            exit_price = exit_price * (1 + self.SLIPPAGE_PCT / 100)
        
        # 计算原始盈亏
        final_pnl_pct = pos.pnl_pct(exit_price)
        
        # 计算手续费
        fee_pct = self.FEE_PCT * 2  # 开仓+平仓两次手续费
        fee_usdt = pos.usdt * fee_pct / 100 * pos.leverage

        if is_half:
            # 第一批止盈（平 HALF_CLOSE_RATIO 比例）
            close_ratio = self.config.HALF_CLOSE_RATIO
            half_pnl_usdt = pos.usdt * close_ratio * final_pnl_pct / 100 * pos.leverage
            half_pnl_usdt -= fee_usdt * close_ratio  # 扣减对应比例的手续费
            pos.half_closed = True
            pos.half_pnl = final_pnl_pct
            pos.half_ts_ms = exit_ts_ms
            pos.half_pnl_usdt = half_pnl_usdt

            # 剩余仓位的 usdt 和 qty 按比例缩减
            remaining_ratio = 1 - close_ratio
            pos.usdt = pos.usdt * remaining_ratio
            pos.qty = pos.qty * remaining_ratio

            # 保本止损（非假突破时）
            if not pos.fake_bk_half and final_pnl_pct > 0:
                if pos.direction == "LONG" and pos.entry_price > pos.stop_loss_px:
                    pos.stop_loss_px = pos.entry_price
                elif pos.direction == "SHORT" and pos.entry_price < pos.stop_loss_px:
                    pos.stop_loss_px = pos.entry_price

            # 假突破减半后重置 peak_price（之后按剩余仓位走移动止损）
            if pos.fake_bk_half:
                pos.peak_price = exit_price

            return None

        # 全平
        usdt_remaining = pos.usdt   # 剩余仓位的 usdt
        pnl_usdt = usdt_remaining * final_pnl_pct / 100 * pos.leverage
        total_pnl_usdt = pos.half_pnl_usdt + pnl_usdt - fee_usdt  # 扣减手续费

        # 记录结果
        entry_dt = datetime.fromtimestamp(pos.entry_ts_ms/1000, tz=timezone.utc)
        exit_dt = datetime.fromtimestamp(exit_ts_ms/1000, tz=timezone.utc)
        result = SimTradeResult(
            symbol=pos.symbol,
            direction=pos.direction,
            score=pos.score,
            structure_trigger=pos.structure_trigger,
            entry_price=pos.entry_price,
            entry_ts_ms=pos.entry_ts_ms,
            entry_time=entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
            exit_price=exit_price,
            exit_ts_ms=exit_ts_ms,
            exit_time=exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
            hold_min=(exit_ts_ms - pos.entry_ts_ms) / 60_000,
            usdt=pos.usdt + (pos.usdt * self.config.HALF_CLOSE_RATIO / (1 - self.config.HALF_CLOSE_RATIO) if pos.half_closed else 0),
            leverage=pos.leverage,
            atr_pct=pos.atr / pos.entry_price * 100 if pos.entry_price > 0 else 0,
            pnl_pct=(total_pnl_usdt) / (self.config.USDT_PER_TRADE) * 100,  # 按原始USDT_PER_TRADE算百分比
            pnl_usdt=total_pnl_usdt,
            status=reason,
            half_pnl=pos.half_pnl if pos.half_closed else 0,
            half_pnl_usdt=pos.half_pnl_usdt,
            second_pnl=final_pnl_pct if pos.half_closed else 0,
        )
        self.results.append(result)

        # 失败记录 & 黑名单
        if total_pnl_usdt < 0:
            today_str = datetime.fromtimestamp(exit_ts_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d")
            self._today_losses[pos.symbol] = today_str
            self._loss_count[pos.symbol] = self._loss_count.get(pos.symbol, 0) + 1
            cooldown_h = self.config.DAILY_LOSS_COOLDOWN_H
            if self._loss_count[pos.symbol] >= 2:
                expires_ms = exit_ts_ms + 86400_000
            else:
                expires_ms = exit_ts_ms + cooldown_h * 3600_000
            self._blacklist[pos.symbol] = max(self._blacklist.get(pos.symbol, 0), expires_ms)
            for hist in self._signal_history.get(pos.symbol, []):
                if not hist.get("failed"):
                    hist["failed"] = True
                    break
        else:
            self._loss_count[pos.symbol] = 0

        # 清理持仓
        self.positions.pop(pos.symbol, None)
        return result


# ── 回测主循环 ─────────────────────────────────────────────────
async def run_backtest(
    config: Config,
    dataset: HistoricalDataset,
    start_ms: int,
    end_ms: int,
    scan_interval_sec: int = 300,
    ob_score_default: float = 1.0,
    output_file: Optional[Path] = None,
    symbol_limit: Optional[int] = None,
    progress_every: int = 50,
) -> list[SimTradeResult]:
    """
    主回测循环：每 scan_interval 秒扫描所有币种，模拟 scanner + trader
    """
    # 延迟导入 scanner，避免 import 时触发某些副作用
    from scanner import YaoBiScanner

    logger.info(f"加载历史数据集...")
    # dataset 已经传入，不再加载

    symbols = dataset.symbols
    if symbol_limit:
        symbols = symbols[:symbol_limit]
    logger.info(f"回测币种数: {len(symbols)}")
    logger.info(f"时间范围: {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)} "
                f"~ {datetime.fromtimestamp(end_ms/1000, tz=timezone.utc)}")

    # Mock client 替换真实 client
    mock_client = MockBinanceClient(dataset, ob_score_default=ob_score_default)

    # 实例化 scanner（但把内部 client 换成 mock）
    class _NullNotifier:
        async def send_message(self, *a, **k): pass
        async def send_signal(self, *a, **k): pass
    scanner = YaoBiScanner(config, _NullNotifier())
    scanner.client = mock_client
    scanner._bt_now_ms = 0

    async def _mock_get_symbols():
        return symbols
    scanner._get_symbols = _mock_get_symbols

    # monkey-patch: cooldown 用模拟时间
    _orig_in_cooldown = scanner._in_cooldown
    _orig_set_cooldown = scanner._set_cooldown
    def _bt_in_cooldown(s):
        now_sec = scanner._bt_now_ms / 1000
        return (now_sec - scanner.cooldown.get(s, 0)) < scanner.config.COOLDOWN_SECONDS
    def _bt_set_cooldown(s):
        scanner.cooldown[s] = scanner._bt_now_ms / 1000
    scanner._in_cooldown = _bt_in_cooldown
    scanner._set_cooldown = _bt_set_cooldown

    # monkey-patch: market_bias / btc_atr 缓存用模拟时间
    _orig_get_market_bias = scanner._get_market_bias
    _orig_get_btc_atr_pct = scanner._get_btc_atr_pct
    async def _bt_get_market_bias():
        now_sec = scanner._bt_now_ms / 1000
        if now_sec - scanner._market_bias_ts < 300 and scanner._market_bias_ts > 0:
            return scanner._market_bias
        try:
            btc_1h = await scanner.client.get("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 24})
            if isinstance(btc_1h, Exception) or len(btc_1h) < 24:
                return scanner._market_bias
            import statistics
            closes = [float(k[4]) for k in btc_1h]
            ma8 = statistics.mean(closes[-8:])
            ma24 = statistics.mean(closes)
            if ma8 > ma24 * 1.005:
                scanner._market_bias = "BULL"
            elif ma8 < ma24 * 0.995:
                scanner._market_bias = "BEAR"
            else:
                scanner._market_bias = "NEUTRAL"
            scanner._market_bias_ts = now_sec
        except Exception:
            pass
        return scanner._market_bias
    async def _bt_get_btc_atr_pct():
        now_sec = scanner._bt_now_ms / 1000
        if now_sec - scanner._btc_atr_ts < 300 and scanner._btc_atr_ts > 0:
            return scanner._btc_atr_pct
        try:
            btc_1h = await scanner.client.get("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 15})
            if isinstance(btc_1h, Exception) or len(btc_1h) < 15:
                return scanner._btc_atr_pct
            atr = scanner._calc_atr(btc_1h, period=14)
            price = float(btc_1h[-1][4])
            scanner._btc_atr_pct = atr / price * 100 if price > 0 else 0.0
            scanner._btc_atr_ts = now_sec
        except Exception:
            pass
        return scanner._btc_atr_pct
    scanner._get_market_bias = _bt_get_market_bias
    scanner._get_btc_atr_pct = _bt_get_btc_atr_pct

    # monkey-patch: 禁用文件IO（回测不需要写信号日志和状态文件）
    scanner._log_signal = lambda sig: None
    scanner._save_state = lambda: None

    # 交易模拟器
    simulator = TradeSimulator(config, dataset)

    # 时间步进
    total_steps = (end_ms - start_ms) // (scan_interval_sec * 1000)
    step_count = 0
    signal_count = 0
    open_attempts = 0
    opened_count = 0
    start_wall = time.time()

    ts_ms = start_ms
    while ts_ms < end_ms:
        step_count += 1
        mock_client.set_time(ts_ms)

        # 重置 scanner 的时间相关缓存，用模拟时间替代 time.time()
        scanner._market_bias_ts = 0
        scanner._btc_atr_ts = 0
        # 覆盖 cooldown 检查：用模拟时间
        scanner._bt_now_ms = ts_ms

        # 扫描：调 scanner.scan() 会调用 _get_symbols（已mock）然后对每个币跑 _analyze
        try:
            # 并行扫描所有币种
            from concurrent.futures import ThreadPoolExecutor
            import asyncio
            
            # 获取所有币种
            all_symbols = await scanner._get_symbols()
            
            # 并行分析每个币种
            signals = []
            
            async def analyze_symbol(symbol):
                try:
                    result = await scanner._analyze(symbol)
                    if result and result.get("score", 0) >= scanner.config.TRADABLE_MIN_SCORE:
                        return result
                except Exception:
                    pass
                return None
            
            # 使用 asyncio.gather 并行执行
            tasks = [analyze_symbol(symbol) for symbol in all_symbols]
            results = await asyncio.gather(*tasks)
            
            # 过滤有效信号
            signals = [r for r in results if r is not None]
        except Exception as e:
            logger.warning(f"scanner.scan() at ts={ts_ms}: {e}")
            signals = []

        # 测试：从历史信号文件读取
        import json
        if step_count == 1:
            try:
                with open('data/historical_signals.jsonl', 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            sig = json.loads(line)
                            # 过滤时间
                            sig_ts = sig.get('timestamp', 0)
                            if start_ms <= sig_ts < end_ms:
                                signals.append(sig)
                        except:
                            pass
                print(f"从历史信号文件加载了 {len(signals)} 个信号")
            except Exception as e:
                print(f"加载历史信号失败: {e}")

        signal_count += len(signals)

        # 每个信号尝试开仓
        for sig in signals:
            open_attempts += 1
            pos = simulator.try_open(sig, ts_ms)
            if pos is not None:
                opened_count += 1

        # evolve 所有持仓到下一个 scan_interval
        next_ts_ms = min(ts_ms + scan_interval_sec * 1000, end_ms)
        
        # 批量获取所有持仓的K线数据
        positions_to_process = list(simulator.positions.items())
        
        for sym, pos in positions_to_process:
            # 检查持仓是否仍然存在（可能在处理其他持仓时被平仓）
            if sym not in simulator.positions:
                continue
                
            # 取持仓的 1m K线推进
            snap = dataset.snapshot(sym, next_ts_ms)
            if snap.klines_1m is None:
                continue
                
            # 过滤时间范围的K线
            df = snap.klines_1m
            df = df[(df["open_time"] >= ts_ms) & (df["open_time"] < next_ts_ms)]
            if len(df) == 0:
                continue
                
            # 向量化计算，减少循环开销
            for _, row in df.iterrows():
                ot = int(row["open_time"])
                h = float(row["high"])
                l = float(row["low"])
                c = float(row["close"])
                
                # 调用 step_position 检查是否触发平仓
                reason = simulator.step_position(pos, ot, h, l, c)
                if reason is None:
                    continue
                    
                # 触发了某种平仓
                if reason in ("HALF_CLOSE",):
                    # 第一批止盈，以 tp_px 作为成交价
                    simulator.close_position(pos, pos.take_profit_px, ot, reason, is_half=True)
                elif reason in ("SL", "SL_TRAIL"):
                    # 止损成交价 = sl_px
                    simulator.close_position(pos, pos.stop_loss_px, ot, reason, is_half=False)
                elif reason == "FAKE_BK_HALF":
                    # 假突破减半，以当前收盘价成交半仓
                    simulator.close_position(pos, c, ot, reason, is_half=True)
                elif reason == "FAKE_BK_FULL":
                    simulator.close_position(pos, c, ot, reason, is_half=False)
                elif reason in ("TIMEOUT", "POST_HALF_TIMEOUT"):
                    simulator.close_position(pos, c, ot, reason, is_half=False)
                    
                # 如果已被平仓则退出K线循环
                if sym not in simulator.positions:
                    break

        # 进度
        if step_count % progress_every == 0:
            elapsed = time.time() - start_wall
            pct = step_count / total_steps * 100
            eta_sec = elapsed / step_count * (total_steps - step_count)
            logger.info(
                f"[{step_count}/{total_steps} {pct:.0f}%] "
                f"signals={signal_count} trades={opened_count} active={len(simulator.positions)} "
                f"elapsed={elapsed/60:.1f}min ETA={eta_sec/60:.1f}min"
            )

        ts_ms = next_ts_ms

    # 回测结束：强制平掉所有剩余仓位（按最后价格）
    for sym in list(simulator.positions.keys()):
        pos = simulator.positions[sym]
        snap = dataset.snapshot(sym, end_ms)
        if snap.klines_1m is not None and len(snap.klines_1m) > 0:
            last_close = float(snap.klines_1m["close"].iloc[-1])
        else:
            last_close = pos.entry_price
        simulator.close_position(pos, last_close, end_ms, "BT_END", is_half=False)

    # 输出结果
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            for r in simulator.results:
                f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")
        logger.info(f"结果保存至: {output_file}")

    return simulator.results


# ── 分析报告 ───────────────────────────────────────────────────
def print_summary(results: list[SimTradeResult]):
    if not results:
        print("\n无交易结果")
        return

    print("\n" + "=" * 80)
    print(f"回测结果汇总（共 {len(results)} 笔）")
    print("=" * 80)

    total_pnl = sum(r.pnl_usdt for r in results)
    wins = [r for r in results if r.pnl_usdt > 0]
    losses = [r for r in results if r.pnl_usdt < 0]
    total_usdt = sum(r.usdt for r in results)

    print(f"\n总盈亏: {total_pnl:+.2f} U")
    print(f"总投入: {total_usdt:.2f} U")
    print(f"ROI: {total_pnl/total_usdt*100:+.2f}%" if total_usdt > 0 else "")
    print(f"胜/负: {len(wins)} / {len(losses)}")
    print(f"胜率: {len(wins)/len(results)*100:.1f}%")

    if wins:
        print(f"平均盈利: {sum(r.pnl_usdt for r in wins)/len(wins):.2f} U")
    if losses:
        print(f"平均亏损: {sum(r.pnl_usdt for r in losses)/len(losses):.2f} U")
    if wins and losses:
        r_ratio = abs(sum(r.pnl_usdt for r in wins)/len(wins)) / abs(sum(r.pnl_usdt for r in losses)/len(losses))
        print(f"盈亏比: 1 : {1/r_ratio:.2f}")

    # 按评分分档
    print("\n── 按评分分档 ──")
    print(f"{'档':<10} {'笔数':>6} {'胜率':>8} {'总盈亏':>12} {'平均':>10}")
    print("-" * 55)
    buckets = defaultdict(list)
    for r in results:
        if r.score >= 85: b = "85+"
        elif r.score >= 75: b = "75-84"
        elif r.score >= 65: b = "65-74"
        else: b = "<65"
        buckets[b].append(r)
    for b in ["85+", "75-84", "65-74", "<65"]:
        if b not in buckets: continue
        rs = buckets[b]
        ws = [r for r in rs if r.pnl_usdt > 0]
        total = sum(r.pnl_usdt for r in rs)
        avg = total / len(rs) if rs else 0
        print(f"{b:<10} {len(rs):>6} {len(ws)/len(rs)*100:>7.1f}% {total:>+11.2f}U {avg:>+9.2f}U")

    # 按 structure_trigger
    print("\n── 按信号类型 ──")
    print(f"{'类型':<45} {'笔数':>6} {'胜率':>8} {'总盈亏':>12}")
    print("-" * 80)
    types = defaultdict(list)
    for r in results:
        types[r.structure_trigger].append(r)
    for t, rs in sorted(types.items(), key=lambda x: -len(x[1])):
        ws = [r for r in rs if r.pnl_usdt > 0]
        total = sum(r.pnl_usdt for r in rs)
        print(f"{t[:45]:<45} {len(rs):>6} {len(ws)/len(rs)*100:>7.1f}% {total:>+11.2f}U")

    # 按平仓原因
    print("\n── 按平仓原因 ──")
    print(f"{'原因':<20} {'笔数':>6} {'胜率':>8} {'总盈亏':>12}")
    print("-" * 55)
    reasons = defaultdict(list)
    for r in results:
        reasons[r.status].append(r)
    for reason, rs in sorted(reasons.items(), key=lambda x: -len(x[1])):
        ws = [r for r in rs if r.pnl_usdt > 0]
        total = sum(r.pnl_usdt for r in rs)
        print(f"{reason:<20} {len(rs):>6} {len(ws)/len(rs)*100:>7.1f}% {total:>+11.2f}U")

    # 按方向
    print("\n── 按方向 ──")
    for d in ["LONG", "SHORT"]:
        rs = [r for r in results if r.direction == d]
        if not rs: continue
        ws = [r for r in rs if r.pnl_usdt > 0]
        total = sum(r.pnl_usdt for r in rs)
        print(f"{d:<6} 笔数={len(rs):>4} 胜率={len(ws)/len(rs)*100:5.1f}% 总盈亏={total:+.2f}U")


async def main():
    parser = argparse.ArgumentParser(description="完整回测（基于历史 parquet）")
    parser.add_argument("--start", type=str, help="开始日期 YYYY-MM-DD (UTC)，默认=数据最早")
    parser.add_argument("--end", type=str, help="结束日期 YYYY-MM-DD (UTC)，默认=数据最晚")
    parser.add_argument("--interval", type=int, default=300, help="扫描间隔秒数（默认300=5分钟）")
    parser.add_argument("--symbol-limit", type=int, help="只用前N个币种（调试用）")
    parser.add_argument("--ob-score", type=float, default=1.0,
                        help="订单簿ratio默认值（无历史数据，1.0=中性）")
    parser.add_argument("--output", type=str, default="data/backtest_results.jsonl", help="结果输出")
    parser.add_argument("--no-experimental", action="store_true",
                        help="关闭EXPERIMENTAL_FILTERS_ENABLED（B组对照）")
    parser.add_argument("--min-score", type=int, help="覆盖MIN_SCORE")
    parser.add_argument("--tradable-min-score", type=int, help="覆盖TRADABLE_MIN_SCORE")
    args = parser.parse_args()

    config = Config()

    # 命令行覆盖
    if args.no_experimental:
        config.EXPERIMENTAL_FILTERS_ENABLED = False
        logger.info("[MODE] EXPERIMENTAL_FILTERS_ENABLED=False (B组)")
    if args.min_score is not None:
        config.MIN_SCORE = args.min_score
    if args.tradable_min_score is not None:
        config.TRADABLE_MIN_SCORE = args.tradable_min_score

    logger.info(f"[Config] MIN_SCORE={config.MIN_SCORE} TRADABLE_MIN_SCORE={config.TRADABLE_MIN_SCORE}")
    logger.info(f"[Config] LEVERAGE={config.LEVERAGE}x USDT_PER_TRADE={config.USDT_PER_TRADE}")
    logger.info(f"[Config] EXPERIMENTAL_FILTERS={config.EXPERIMENTAL_FILTERS_ENABLED}")

    # 加载数据集
    dataset = HistoricalDataset(lazy=True)
    symbols = dataset.symbols
    if not symbols:
        logger.error("无历史数据，请先运行 fetch_historical.py")
        return

    # 时间范围
    t_start, t_end = dataset.time_range()
    if t_start == 0:
        logger.error("无法确定时间范围")
        return

    if args.start:
        start_ms = int(datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        start_ms = max(start_ms, t_start)
    else:
        start_ms = t_start

    if args.end:
        end_ms = int(datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = min(end_ms, t_end)
    else:
        end_ms = t_end

    results = await run_backtest(
        config=config,
        dataset=dataset,
        start_ms=start_ms,
        end_ms=end_ms,
        scan_interval_sec=args.interval,
        ob_score_default=args.ob_score,
        output_file=Path(args.output),
        symbol_limit=args.symbol_limit,
    )

    print_summary(results)


if __name__ == "__main__":
    asyncio.run(main())