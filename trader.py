"""
妖币猎手 v3 · 交易执行模块
最终修复版：
  - Bug 1修复：try_open 整段 try/except 包裹，下单异常不崩主循环
  - Bug 2修复：_update_exchange_stoploss 改为"先撤旧再挂新"顺序
  - Bug 3修复：_tick_cache 独立缓存价格精度，不再与step混淆
  - Bug 4修复：初始止损单去掉 closePosition，统一用 quantity+reduceOnly
  - Bug 5修复：所有止损单挂单方式统一
  - Bug 6修复：reduceOnly 全部用字符串 "true"
  - Bug 7修复：to_dict 清理运行时临时字段
  - Bug 8修复：audit_positions 中文详细报警
"""
import asyncio
import json
import logging
import math
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from config import TOP30_BLACKLIST, STOCK_RELATED_BLACKLIST
from scanner import BinanceClient

logger = logging.getLogger(__name__)
POSITIONS_FILE = Path("data/positions.json")


def _floor_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    precision = max(0, round(-math.log10(step)))
    factor = 10 ** precision
    return math.floor(value * factor + 1e-9) / factor


def is_tradable_signal(sig: dict, min_score: int = 50) -> bool:
    structure = sig.get("factors", {}).get("structure_trigger", "")
    return (
        sig.get("score", 0) >= min_score
        and ("5m restart" in structure or "strong hold" in structure)
    )


def _fmt(value: float, step: float) -> str:
    if step <= 0:
        step = 0.01
    precision = max(0, round(-math.log10(step)))
    formatted = f"{value:.{precision}f}"
    # 防止格式化结果为0（value比step还小的情况）
    if float(formatted) <= 0:
        return f"{step:.{precision}f}"  # 至少返回1个step的值
    return formatted


class Position:
    def __init__(self, symbol, direction, entry_price, qty, usdt,
                 leverage, open_time, signal_score, atr=0.0):
        self.symbol         = symbol
        self.direction      = direction
        self.entry_price    = entry_price
        self.qty            = qty              # 当前剩余数量
        self.qty_original   = qty              # 原始数量
        self.usdt           = usdt
        self.leverage       = leverage
        self.open_time      = open_time
        self.signal_score   = signal_score
        self.order_id       = None
        self.peak_pnl_pct   = 0.0
        self.peak_price     = entry_price
        self.half_closed    = False
        self.half_pnl       = 0.0
        self._updating_sl   = False
        self.atr            = atr
        self.stop_loss_px   = 0.0
        self.take_profit_px = 0.0

    def pnl_pct(self, cur: float) -> float:
        if self.entry_price <= 0:
            return 0.0
        if self.direction == "LONG":
            return (cur - self.entry_price) / self.entry_price * 100
        return (self.entry_price - cur) / self.entry_price * 100

    def to_dict(self):
        d = self.__dict__.copy()
        for k in ("_warned", "_updating_sl", "_fake_bk_half"):
            d.pop(k, None)
        return d

    @classmethod
    def from_dict(cls, d: dict):
        p = cls.__new__(cls)
        p.__dict__.update(d)
        p._updating_sl = False
        defaults = {
            "peak_pnl_pct":   0.0,
            "peak_price":     d.get("entry_price", 0),
            "half_closed":    False,
            "half_pnl":       0.0,
            "qty_original":   d.get("qty", 0),
            "atr":            0.0,
            "stop_loss_px":   0.0,
            "take_profit_px": 0.0,
        }
        for name, default in defaults.items():
            if not hasattr(p, name):
                setattr(p, name, default)
        return p


class DailyGuard:
    """每日亏损熔断"""
    def __init__(self):
        self._day: Optional[date] = None
        self._start_equity = 0.0
        self._blown = False

    def reset_if_new_day(self, equity: float):
        today = date.today()
        if self._day != today:
            self._day = today
            self._start_equity = equity
            self._blown = False
            logger.info(f"[熔断] 新的一天，初始权益={equity:.2f}U")

    def is_safe(self, equity: float, limit_pct: float) -> bool:
        if self._blown:
            return False
        if self._start_equity <= 0:
            return True
        loss_pct = (self._start_equity - equity) / self._start_equity * 100
        if loss_pct >= limit_pct:
            self._blown = True
            logger.warning(f"[熔断] 当日亏损{loss_pct:.2f}% ≥ 限额{limit_pct}%，停止开仓")
            return False
        return True


class Trader:
    def __init__(self, config, notifier):
        self.config    = config
        self.notifier  = notifier
        self.client    = BinanceClient(
            config.BINANCE_API_KEY,
            config.BINANCE_SECRET,
            proxy=os.getenv("SCANNER_PROXY", ""),
        )
        self.positions: dict[str, Position] = {}
        self.guard     = DailyGuard()
        self._load_positions()
        self._last_open_time = 0
        self._loss_count: dict[str, int] = {}
        self._blacklist: dict[str, float] = {}
        self._today_losses: dict[str, date] = {}
        self._signal_history: dict[str, list] = {}

    async def _get_step(self, sym: str) -> float:
        return await self.client.get_step_size(sym)

    async def _get_tick(self, sym: str) -> float:
        return await self.client.get_tick_size(sym)

    @staticmethod
    def _risk_usdt(usdt: float, leverage: float, entry_price: float, stop_price: float) -> float:
        if entry_price <= 0:
            return 0.0
        return usdt * leverage * abs(entry_price - stop_price) / entry_price

    # ── 开仓 ───────────────────────────────────────────────────────
    async def try_open(self, signal: dict):
        sym = signal["symbol"]
        direction = signal.get("direction", "LONG")

        base = sym.replace("USDT", "")
        if base in STOCK_RELATED_BLACKLIST:
            logger.info(f"[skip] {sym} stock-related token, not crypto-native")
            return

        if self._today_losses.get(sym) == date.today():
            logger.info(f"[skip] {sym} already lost today, no re-entry")
            return

        signal_price = float(signal.get("price", 0))
        repeat_zone = getattr(self.config, 'SIGNAL_REPEAT_ZONE_PCT', 3.0)
        exp_filters = getattr(self.config, 'EXPERIMENTAL_FILTERS_ENABLED', True)
        if exp_filters and sym in self._signal_history:
            for hist in self._signal_history[sym]:
                if hist.get("failed") and hist.get("price", 0) > 0:
                    if abs(signal_price - hist["price"]) / hist["price"] * 100 < repeat_zone:
                        logger.info(f"[skip] {sym} price within {repeat_zone}% of failed signal @ {hist['price']}")
                        return

        # 黑名单
        if sym in self._blacklist:
            if time.time() < self._blacklist[sym]:
                logger.info(f"[跳过] {sym} 在黑名单中，剩余{(self._blacklist[sym] - time.time())/3600:.1f}小时")
                return
            else:
                del self._blacklist[sym]

        # 开仓间隔
        if time.time() - self._last_open_time < 600:
            logger.info(f"[跳过] 距上次开仓不足10分钟")
            return

        # 开关
        if not self.config.TRADE_ENABLED:
            logger.info(f"[跳过] TRADE_ENABLED=False  {sym}")
            return
        if direction == "LONG" and not self.config.LONG_ENABLED:
            return
        if direction == "SHORT" and not self.config.SHORT_ENABLED:
            return
        if sym in self.positions:
            logger.info(f"[跳过] {sym} 已有持仓")
            return
        if len(self.positions) >= self.config.MAX_OPEN_POSITIONS:
            logger.info(f"[跳过] 持仓已满 {len(self.positions)}/{self.config.MAX_OPEN_POSITIONS}")
            return

        # 同向持仓数限制≤2：应对相关性风险（妖币往往同涨同跌）
        same_dir_count = sum(1 for p in self.positions.values() if p.direction == direction)
        if same_dir_count >= 2:
            logger.info(f"[跳过] {sym} 同向持仓已达上限 ({same_dir_count}/2) {direction}")
            return

        try:
            ex_pos = await self.client.get_signed("/fapi/v2/positionRisk", {"symbol": sym})
            for p in ex_pos:
                amt = float(p.get("positionAmt", 0))
                if amt != 0:
                    ex_dir = "LONG" if amt > 0 else "SHORT"
                    if direction == ex_dir:
                        logger.info(f"[跳过] {sym} 交易所已有同向仓位 {ex_dir} amt={amt}")
                        return
                    else:
                        logger.warning(f"[跳过] {sym} 交易所已有反向仓位 {ex_dir} amt={amt}，不可对冲")
                        return
        except Exception as e:
            logger.warning(f"[仓位检查] {sym} 查询交易所仓位失败: {e}，继续开仓流程")

        # 账户 & 熔断
        try:
            acc = await self.client.get_signed("/fapi/v2/account")
            equity    = float(acc.get("totalMarginBalance", 0))
            available = float(acc.get("availableBalance", 0))
        except Exception as e:
            logger.error(f"账户查询失败: {e}")
            return

        self.guard.reset_if_new_day(equity)
        if not self.guard.is_safe(equity, self.config.DAILY_LOSS_LIMIT_PCT):
            await self.notifier.send_message(
                f"🚨 *每日熔断触发*\n当日亏损超过 {self.config.DAILY_LOSS_LIMIT_PCT}%\n今日停止开新仓"
            )
            return

        # 评分分级
        score = signal.get("score", 0)
        tradable_min = getattr(self.config, 'TRADABLE_MIN_SCORE', 75)
        if not is_tradable_signal(signal, tradable_min):
            structure_trigger = signal.get("factors", {}).get("structure_trigger", "")
            logger.info(f"[跳过] {sym} 非理想信号 structure={structure_trigger}，仅观察不下单")
            return

        certainty = signal.get("certainty_score", 0)
        opportunity = signal.get("opportunity_score", 0)
        certainty_min = getattr(self.config, 'CERTAINTY_MIN_FOR_BOOST', 80)
        opp_min = getattr(self.config, 'OPPORTUNITY_MIN_FOR_BOOST', 60)
        boost = getattr(self.config, 'POSITION_BOOST_SCALE', 1.5)
        exp_filters = getattr(self.config, 'EXPERIMENTAL_FILTERS_ENABLED', True)
        if exp_filters and certainty >= certainty_min and opportunity >= opp_min:
            usdt = self.config.USDT_PER_TRADE * boost
            position_mode = f"{boost}x(高确定性+高机会)"
        else:
            usdt = self.config.USDT_PER_TRADE
            position_mode = "1.0x(默认)"

        btc_atr_pct_str = signal.get("factors", {}).get("btc_atr_pct", "")
        try:
            btc_atr_pct_val = float(btc_atr_pct_str.replace("%", ""))
        except (ValueError, AttributeError):
            btc_atr_pct_val = 0.0
        btc_vol_threshold = getattr(self.config, 'BTC_ATR_VOL_THRESHOLD', 1.0)
        btc_vol_scale = getattr(self.config, 'BTC_VOL_POSITION_SCALE', 0.5)
        if exp_filters and btc_atr_pct_val > btc_vol_threshold:
            usdt = usdt * btc_vol_scale
            position_mode += f" → {btc_vol_scale}x(BTC波动{btc_atr_pct_val:.1f}%>{btc_vol_threshold}%)"

        if usdt < 11:
            logger.warning(f"[跳过] USDT={usdt:.2f} 低于最小名义价值")
            return
        if available < usdt:
            logger.warning(f"[跳过] 可用余额{available:.2f}U < 需要{usdt:.2f}U")
            return

        # 设置杠杆
        try:
            await self.client.post_signed(
                "/fapi/v1/leverage",
                {"symbol": sym, "leverage": self.config.LEVERAGE},
            )
        except Exception as e:
            logger.error(f"[跳过] {sym} 设置杠杆失败: {e}")
            await self.notifier.send_message(f"⚠️ {sym} 设置杠杆失败，跳过开仓\n{str(e)[:200]}")
            return

        # 精度
        try:
            step = await self._get_step(sym)
            tick = await self._get_tick(sym)
        except Exception as e:
            logger.error(f"[跳过] {sym} 获取交易精度失败: {e}")
            return

        price = float(signal["price"])
        qty = _floor_step(usdt * self.config.LEVERAGE / price, step)
        if qty <= 0:
            logger.warning(f"[跳过] {sym} 计算数量={qty}")
            return
        if qty * price < 5:
            logger.warning(f"[跳过] {sym} 名义价值{qty*price:.2f}U 低于最低限制")
            return

        # ATR止损止盈
        atr = float(signal.get("atr", 0))
        if atr <= 0:
            atr = price * self.config.STOP_LOSS_PCT / 100

        max_atr_pct = getattr(self.config, 'MAX_ATR_PCT', 5.0)
        if atr / price * 100 > max_atr_pct:
            logger.warning(f"[skip] {sym} ATR={atr/price*100:.2f}% > MAX_ATR_PCT={max_atr_pct}%")
            await self.notifier.send_message(
                f"⚠️ `{sym}` ATR={atr/price*100:.2f}% 过大，跳过"
            )
            return

        if direction == "LONG":
            atr_sl = price - atr * self.config.ATR_SL_MULTIPLIER
            structure_sl = signal.get("factors", {}).get("structure_sl")
            if structure_sl and structure_sl > atr_sl:
                stop_loss_px = structure_sl
                logger.info(f"[结构位止损] {sym} LONG 结构位={structure_sl:.6g} > ATR止损={atr_sl:.6g}, 取结构位")
            else:
                stop_loss_px = atr_sl
            take_profit_px = price + atr * self.config.ATR_TP1_MULTIPLIER
        else:
            atr_sl = price + atr * self.config.ATR_SL_MULTIPLIER
            structure_sl = signal.get("factors", {}).get("structure_sl")
            if structure_sl and structure_sl < atr_sl:
                stop_loss_px = structure_sl
                logger.info(f"[结构位止损] {sym} SHORT 结构位={structure_sl:.6g} < ATR止损={atr_sl:.6g}, 取结构位")
            else:
                stop_loss_px = atr_sl
            take_profit_px = price - atr * self.config.ATR_TP1_MULTIPLIER

        stop_loss_px   = _floor_step(stop_loss_px, tick)
        take_profit_px = _floor_step(take_profit_px, tick)

        if stop_loss_px <= 0 or take_profit_px <= 0:
            logger.warning(f"[跳过] {sym} 止损/止盈价格异常")
            return

        max_risk_pct = getattr(self.config, 'MAX_SL_RISK_PCT', 0.6)
        sl_distance_pct = abs(price - stop_loss_px) / price
        risk_pct = sl_distance_pct * self.config.LEVERAGE
        if risk_pct > max_risk_pct:
            logger.warning(f"[跳过] {sym} 止损触发保证金损失{risk_pct*100:.0f}% > {max_risk_pct*100:.0f}%")
            await self.notifier.send_message(
                f"⚠️ `{sym}` ATR={atr/price*100:.2f}% 过大，{self.config.LEVERAGE}x杠杆下止损风险{risk_pct*100:.0f}%，跳过"
            )
            return

        # 风险预算
        current_risk = sum(
            self._risk_usdt(p.usdt, p.leverage, p.entry_price, p.stop_loss_px or p.entry_price)
            for p in self.positions.values()
        )
        new_risk = self._risk_usdt(usdt, self.config.LEVERAGE, price, stop_loss_px)
        if equity > 0 and (current_risk + new_risk) / equity * 100 > self.config.MAX_TOTAL_RISK_PCT:
            logger.info(f"[跳过] 总风险敞口超限")
            return

        # ── 下单（整段try包裹）──────────────────────────────────
        side = "BUY" if direction == "LONG" else "SELL"
        try:
            order = await self.client.post_signed(
                "/fapi/v1/order",
                {"symbol": sym, "side": side, "type": "MARKET", "quantity": _fmt(qty, step)},
            )

            entry_price = float(order.get("avgPrice") or 0)

            # avgPrice=0 时追溯查询
            if entry_price <= 0:
                logger.warning(f"{sym} avgPrice=0，等待2秒后追溯...")
                await asyncio.sleep(2)
                try:
                    order_info = await self.client.get_signed(
                        "/fapi/v1/order",
                        {"symbol": sym, "orderId": order.get("orderId")},
                    )
                    if order_info.get("status") == "FILLED":
                        entry_price = float(order_info.get("avgPrice") or 0)
                        logger.info(f"{sym} 追溯均价: {entry_price}")
                except Exception as query_err:
                    logger.error(f"追溯订单失败: {query_err}")

            # 最终兜底
            if entry_price <= 0:
                entry_price = float(signal["price"])

            if entry_price <= 0:
                logger.error(f"[中止] {sym} 均价仍为0")
                await self.notifier.send_message(
                    f"🚨 *开仓异常* `{sym}`\n"
                    f"订单orderId={order.get('orderId')}但均价=0\n"
                    f"**请手动登录币安确认仓位并设置止损！**"
                )
                return

            # 基于实际入场价重算止损止盈（原基于信号价，滑点会导致偏差）
            if direction == "LONG":
                atr_sl = entry_price - atr * self.config.ATR_SL_MULTIPLIER
                structure_sl = signal.get("factors", {}).get("structure_sl")
                if structure_sl and structure_sl > atr_sl:
                    stop_loss_px = structure_sl
                else:
                    stop_loss_px = atr_sl
                take_profit_px = entry_price + atr * self.config.ATR_TP1_MULTIPLIER
            else:
                atr_sl = entry_price + atr * self.config.ATR_SL_MULTIPLIER
                structure_sl = signal.get("factors", {}).get("structure_sl")
                if structure_sl and structure_sl < atr_sl:
                    stop_loss_px = structure_sl
                else:
                    stop_loss_px = atr_sl
                take_profit_px = entry_price - atr * self.config.ATR_TP1_MULTIPLIER

            stop_loss_px   = _floor_step(stop_loss_px, tick)
            take_profit_px = _floor_step(take_profit_px, tick)

            if stop_loss_px <= 0 or take_profit_px <= 0:
                logger.warning(f"[异常] {sym} 基于入场价重算后止损/止盈价格异常")
                await self.notifier.send_message(
                    f"⚠️ `{sym}` 入场价={entry_price:.6g} 重算后止损/止盈异常，请手动设置！"
                )
                return

            sl_distance_pct2 = abs(entry_price - stop_loss_px) / entry_price
            risk_pct2 = sl_distance_pct2 * self.config.LEVERAGE
            if risk_pct2 > max_risk_pct:
                logger.warning(f"[跳过] {sym} 基于入场价重算后止损风险{risk_pct2*100:.0f}% > {max_risk_pct*100:.0f}%")
                await self.notifier.send_message(
                    f"⚠️ `{sym}` 滑点后止损风险{risk_pct2*100:.0f}%超限，跳过"
                )
                return

            # 创建持仓记录
            pos = Position(
                symbol=sym,
                direction=direction,
                entry_price=entry_price,
                qty=qty,
                usdt=usdt,
                leverage=self.config.LEVERAGE,
                open_time=time.time(),
                signal_score=score,
                atr=atr,
            )
            pos.order_id       = order.get("orderId")
            pos.stop_loss_px   = stop_loss_px
            pos.take_profit_px = take_profit_px
            self.positions[sym] = pos
            self._save_positions()
            self._last_open_time = time.time()

            self._signal_history.setdefault(sym, []).append({
                "price": entry_price,
                "time": time.time(),
                "order_id": pos.order_id,
                "failed": False,
            })
            if len(self._signal_history[sym]) > 20:
                self._signal_history[sym] = self._signal_history[sym][-20:]

            # 挂止损单（统一 quantity + reduceOnly）
            try:
                await self.client.post_signed(
                    "/fapi/v1/order",
                    {
                        "symbol":      sym,
                        "side":        "SELL" if direction == "LONG" else "BUY",
                        "type":        "STOP_MARKET",
                        "stopPrice":   _fmt(pos.stop_loss_px, tick),
                        "quantity":    _fmt(qty, step),
                        "reduceOnly":  "true",
                        "workingType": "MARK_PRICE",
                    },
                )
                logger.info(f"[止损单] {sym} 止损价={pos.stop_loss_px}")
            except Exception as e:
                logger.warning(f"[止损单失败] {sym}: {e}")
                await self.notifier.send_message(
                    f"⚠️ `{sym}` 交易所止损单挂单失败，依赖程序轮询\n{str(e)[:150]}"
                )

            factors = signal.get("factors", {})
            structure_info = factors.get("structure_trigger", "")
            trend_info = factors.get("trend_ma", "")
            slope_info = factors.get("ma_slope", "")
            bias_info = factors.get("market_bias", "")

            await self.notifier.send_message(
                f"{'📈' if direction=='LONG' else '📉'} *开仓成功* `{sym}`\n"
                f"━━━━━━━━━━━━\n"
                f"方向: `{direction}`  杠杆: `{self.config.LEVERAGE}x`\n"
                f"入场价: `{entry_price:.6g}`  数量: `{qty}`\n"
                f"投入: `{usdt:.2f}U`  名义: `{qty*entry_price:.2f}U`\n"
                f"仓位模式: `{position_mode}`\n"
                f"ATR: `{atr:.6g}` ({atr/entry_price*100:.2f}%)\n"
                f"止损价: `{pos.stop_loss_px:.6g}`\n"
                f"第一批止盈: `{pos.take_profit_px:.6g}`\n"
                f"信号评分: `{score}` (确定性`{signal.get('certainty_score',0)}` 机会`{signal.get('opportunity_score',0)}`)\n"
                f"━━━━━━━━━━━━\n"
                f"入场逻辑: `{structure_info or '无'}`\n"
                f"趋势: `{trend_info or '?'}`  MA斜率: `{slope_info or '?'}`\n"
                f"大盘: `{bias_info or '?'}`"
            )
            logger.info(f"[开仓] {sym} {direction} @ {entry_price} qty={qty}")

        except Exception as e:
            err = str(e)[:300]
            logger.error(f"[开仓失败] {sym}: {err}", exc_info=True)
            await self.notifier.send_message(f"⚠️ *开仓失败* `{sym}`\n{err}")
            return

    # ── 同步交易所持仓 ─────────────────────────────────────────
    async def sync_positions_from_exchange(self):
        try:
            account = await self.client.get_signed("/fapi/v2/account")
            real_positions = {
                p["symbol"]: float(p["positionAmt"])
                for p in account.get("positions", [])
                if float(p["positionAmt"]) != 0
            }
        except Exception as e:
            logger.warning(f"[对账失败] {e}")
            return

        ghost = []
        for sym in list(self.positions.keys()):
            if abs(real_positions.get(sym, 0)) < 1e-8:
                ghost.append(sym)

        for sym in ghost:
            logger.info(f"[对账] {sym} 交易所已无持仓，清理程序记录")
            self.positions.pop(sym, None)
            try:
                await self.client.delete_signed("/fapi/v1/allOpenOrders", {"symbol": sym})
            except Exception:
                pass
            await self.notifier.send_message(
                f"🔄 *持仓已同步清理* `{sym}`\n"
                f"交易所实际持仓=0，可能已被原生止损/止盈触发"
            )

        if ghost:
            self._save_positions()

    # ── 更新止损单（先撤旧再挂新）──────────────────────────────
    async def _update_exchange_stoploss(self, pos: Position):
        sym = pos.symbol
        try:
            try:
                orders = await self.client.get_signed("/fapi/v1/openOrders", {"symbol": sym})
                for o in orders:
                    if o["type"] == "STOP_MARKET":
                        try:
                            await self.client.delete_signed(
                                "/fapi/v1/order",
                                {"symbol": sym, "orderId": o["orderId"]},
                            )
                        except Exception as e:
                            logger.debug(f"撤销旧止损单 {o['orderId']}: {e}")
            except Exception as e:
                logger.debug(f"查询旧挂单失败: {e}")

            tick = await self._get_tick(sym)
            step = await self._get_step(sym)
            await self.client.post_signed(
                "/fapi/v1/order",
                {
                    "symbol":      sym,
                    "side":        "SELL" if pos.direction == "LONG" else "BUY",
                    "type":        "STOP_MARKET",
                    "stopPrice":   _fmt(pos.stop_loss_px, tick),
                    "quantity":    _fmt(pos.qty, step),
                    "reduceOnly":  "true",
                    "workingType": "MARK_PRICE",
                },
            )
            logger.info(f"[止损更新] {sym} 新止损价={pos.stop_loss_px}")
        except Exception as e:
            logger.warning(f"[止损更新失败] {sym}: {e}")

    # ── 持仓监控 ───────────────────────────────────────────────
    async def monitor_positions(self):
        if not self.positions:
            return

        await self.sync_positions_from_exchange()
        if not self.positions:
            return

        try:
            results = await asyncio.gather(
                self.client.get("/fapi/v1/ticker/price"),
                self.client.get("/fapi/v1/premiumIndex"),
                self.client.get_signed("/fapi/v1/openOrders"),
                return_exceptions=True,
            )
            tickers = results[0] if not isinstance(results[0], Exception) else []
            mark_tickers = results[1] if not isinstance(results[1], Exception) else []
            all_orders = results[2] if not isinstance(results[2], Exception) else []
        except Exception as e:
            logger.warning(f"获取行情/挂单失败: {e}")
            return

        try:
            prices = {t["symbol"]: float(t["price"]) for t in tickers}
        except Exception:
            prices = {}

        try:
            mark_prices = {t["symbol"]: float(t["markPrice"]) for t in mark_tickers}
        except Exception:
            mark_prices = {}

        try:
            orders_map: dict[str, list] = {}
            for o in all_orders:
                orders_map.setdefault(o["symbol"], []).append(o)
        except Exception:
            orders_map = {}

        to_half_close = []
        to_full_close = []

        for sym, pos in list(self.positions.items()):
            if pos.entry_price <= 0:
                if not hasattr(pos, '_warned') or pos._warned % 100 == 0:
                    await self.notifier.send_message(
                        f"🚨 *异常持仓需手动处理*\n`{sym}` entry_price=0\n请登录币安App手动平仓！"
                    )
                pos._warned = getattr(pos, '_warned', 0) + 1
                continue

            cur = prices.get(sym)
            if not cur:
                continue

            mark = mark_prices.get(sym, cur)

            pnl = pos.pnl_pct(mark)
            if pnl > pos.peak_pnl_pct:
                pos.peak_pnl_pct = pnl
                pos.peak_price = mark

            hold_hours = (time.time() - pos.open_time) / 3600

            # ATR止损（使用标记价格，与交易所止损单 workingType=MARK_PRICE 一致）
            sl_hit = (
                (pos.direction == "LONG"  and mark <= pos.stop_loss_px) or
                (pos.direction == "SHORT" and mark >= pos.stop_loss_px)
            )
            if sl_hit and pos.stop_loss_px > 0 and not pos._updating_sl:
                to_full_close.append((pos, mark, pnl, f"ATR止损 标记价={mark:.6g} 止损线={pos.stop_loss_px:.6g}"))
                continue

            # 本地硬止损：last_price直接插破止损线，不等待交易所触发
            hard_sl_hit = (
                (pos.direction == "LONG"  and cur <= pos.stop_loss_px) or
                (pos.direction == "SHORT" and cur >= pos.stop_loss_px)
            )
            if hard_sl_hit and pos.stop_loss_px > 0 and not pos._updating_sl:
                to_full_close.append((pos, cur, pnl, f"硬止损触发 最新价={cur:.6g} 止损线={pos.stop_loss_px:.6g}"))
                logger.warning(f"[硬止损] {sym} last_price={cur:.6g} 已插破止损={pos.stop_loss_px:.6g}，紧急平仓")
                continue

            # 超时（未分批前）
            if not pos.half_closed and hold_hours >= self.config.MAX_HOLD_HOURS:
                to_full_close.append((pos, mark, pnl, f"超时 {hold_hours:.1f}h"))
                continue

            # 假突破检测
            exp_filters = getattr(self.config, 'EXPERIMENTAL_FILTERS_ENABLED', True)
            fake_min = getattr(self.config, 'FAKE_BREAKOUT_MIN', 15)
            fake_ratio = getattr(self.config, 'FAKE_BREAKOUT_ATR_RATIO', 0.5)
            if exp_filters and not pos.half_closed and pos.atr > 0:
                hold_min = (time.time() - pos.open_time) / 60
                if hold_min >= fake_min:
                    if pos.direction == "LONG":
                        favorable_move = (mark - pos.entry_price) / pos.atr
                    else:
                        favorable_move = (pos.entry_price - mark) / pos.atr
                    if favorable_move < fake_ratio:
                        fake_half_mode = getattr(self.config, 'FAKE_BK_HALF_MODE', True)
                        if fake_half_mode and not pos.half_closed:
                            pos._fake_bk_half = True
                            to_half_close.append((pos, mark, pnl))
                            logger.info(f"[假突破减半] {sym} {hold_min:.0f}min内仅移动{favorable_move:.2f}x ATR")
                        else:
                            to_full_close.append((pos, mark, pnl, f"假突破 {hold_min:.0f}min内仅移动{favorable_move:.2f}x ATR < {fake_ratio}x"))
                        continue

            # 第一批止盈（使用标记价格）
            tp_hit = (
                (pos.direction == "LONG"  and mark >= pos.take_profit_px) or
                (pos.direction == "SHORT" and mark <= pos.take_profit_px)
            )
            if not pos.half_closed and tp_hit:
                to_half_close.append((pos, mark, pnl))
                continue

            # 移动止损（仅已分批后，使用标记价格）
            if pos.half_closed and self.config.TRAIL_STOP_ENABLED:
                try:
                    tick = await self._get_tick(sym)
                except Exception as e:
                    logger.warning(f"[移动止损] {sym} 获取tick失败: {e}，跳过本次移动止损")
                    continue

                trail_mult = getattr(self.config, 'ATR_TRAIL_MULTIPLIER', self.config.ATR_SL_MULTIPLIER)
                if pos.direction == "LONG":
                    new_sl = pos.peak_price - pos.atr * trail_mult
                else:
                    new_sl = pos.peak_price + pos.atr * trail_mult
                new_sl = _floor_step(new_sl, tick)

                updated = False
                if pos.direction == "LONG" and new_sl > pos.stop_loss_px:
                    pos.stop_loss_px = new_sl
                    updated = True
                elif pos.direction == "SHORT" and new_sl < pos.stop_loss_px:
                    pos.stop_loss_px = new_sl
                    updated = True

                if updated:
                    pos._updating_sl = True
                    try:
                        await self._update_exchange_stoploss(pos)
                    finally:
                        pos._updating_sl = False
                    logger.info(f"[移动止损] {sym} 新止损={new_sl:.6g} 当前盈利={pnl:.2f}%")

                if hold_hours >= getattr(self.config, 'POST_HALF_HOLD_HOURS', self.config.MAX_HOLD_HOURS * 2):
                    to_full_close.append((pos, mark, pnl, f"分批后超时 {hold_hours:.1f}h"))
                    continue

            # 补挂止损单（如果交易所侧丢失）
            has_sl = any(o["type"] == "STOP_MARKET" for o in orders_map.get(sym, []))
            if not has_sl and pos.stop_loss_px > 0:
                try:
                    tick = await self._get_tick(sym)
                    step = await self._get_step(sym)
                    await self.client.post_signed(
                        "/fapi/v1/order",
                        {
                            "symbol":      sym,
                            "side":        "SELL" if pos.direction == "LONG" else "BUY",
                            "type":        "STOP_MARKET",
                            "stopPrice":   _fmt(pos.stop_loss_px, tick),
                            "quantity":    _fmt(pos.qty, step),
                            "reduceOnly":  "true",
                            "workingType": "MARK_PRICE",
                        },
                    )
                    logger.info(f"[补挂止损] {sym} 止损价={pos.stop_loss_px}")
                except Exception as e:
                    logger.warning(f"[补挂止损失败] {sym}: {e}")
                    await self.notifier.send_message(
                        f"⚠️ *补挂止损失败* `{sym}`\n"
                        f"仓位暂时无交易所止损保护，依赖程序轮询\n"
                        f"错误: `{str(e)[:150]}`"
                    )

        for pos, cur, pnl in to_half_close:
            await self._half_close(pos, cur, pnl)

        for pos, cur, pnl, reason in to_full_close:
            await self._close_position(pos, cur, pnl, reason)

        self._save_positions()

    # ── 第一批止盈（平一半）──────────────────────────────────
    async def _half_close(self, pos: Position, cur: float, pnl: float):
        sym = pos.symbol
        try:
            step = await self._get_step(sym)
        except Exception as e:
            logger.error(f"[分批失败] {sym} 获取精度失败: {e}")
            return

        close_ratio = getattr(self.config, 'HALF_CLOSE_RATIO', 0.5)
        half_qty = _floor_step(pos.qty * close_ratio, step)
        if half_qty <= 0:
            logger.warning(f"[分批跳过] {sym} 数量过小，改为全平")
            await self._close_position(pos, cur, pnl, f"止盈(不可分批) {pnl:.2f}%")
            return

        remaining_notional = (pos.qty - half_qty) * cur
        if remaining_notional < 20:
            logger.info(f"[分批跳过] {sym} 剩余名义价值仅{remaining_notional:.2f}U，直接全平")
            await self._close_position(pos, cur, pnl, f"止盈(剩余过小) {pnl:.2f}%")
            return

        side = "SELL" if pos.direction == "LONG" else "BUY"

        try:
            await self.client.post_signed(
                "/fapi/v1/order",
                {
                    "symbol":     sym,
                    "side":       side,
                    "type":       "MARKET",
                    "quantity":   _fmt(half_qty, step),
                    "reduceOnly": "true",
                },
            )

            pnl_usdt_half = pos.usdt * close_ratio * pnl / 100 * pos.leverage

            # 从交易所获取实际剩余持仓数量（避免部分成交导致数量不同步）
            try:
                positions = await self.client.get_signed("/fapi/v2/positionRisk", {"symbol": sym})
                for p in positions:
                    amt = float(p.get("positionAmt", 0))
                    if pos.direction == "LONG" and amt > 0:
                        actual_remaining = amt
                        break
                    if pos.direction == "SHORT" and amt < 0:
                        actual_remaining = abs(amt)
                        break
                else:
                    actual_remaining = 0
            except Exception:
                actual_remaining = pos.qty - half_qty

            if actual_remaining > 0:
                pos.qty = actual_remaining
                denominator = actual_remaining + half_qty
                if denominator > 0:
                    pos.usdt = pos.usdt * (actual_remaining / denominator)
                else:
                    pos.usdt = pos.usdt * (1 - close_ratio)
            else:
                logger.warning(f"[半仓止盈] {sym} 交易所仓位已为0，可能已被止损触发")
                self.positions.pop(sym, None)
                self._save_positions()
                await self.notifier.send_message(
                    f"ℹ️ *仓位已由交易所平仓* `{sym}`\n"
                    f"方向: `{pos.direction}`\n"
                    f"半仓止盈后发现仓位为0（可能已被止损触发）"
                )
                return
            pos.half_closed = True
            pos.half_pnl    = pnl
            pos.peak_pnl_pct = pnl
            pos.peak_price = cur
            pos._updating_sl = True

            fake_bk = getattr(pos, '_fake_bk_half', False)
            if not fake_bk and pnl > 0:
                try:
                    tick = await self._get_tick(sym)
                    breakeven = _floor_step(pos.entry_price, tick)
                    if pos.direction == "LONG" and breakeven > pos.stop_loss_px:
                        pos.stop_loss_px = breakeven
                        logger.info(f"[保本止损] {sym} 止损移至入场价={breakeven}")
                    elif pos.direction == "SHORT" and breakeven < pos.stop_loss_px:
                        pos.stop_loss_px = breakeven
                        logger.info(f"[保本止损] {sym} 止损移至入场价={breakeven}")
                except Exception as e:
                    logger.warning(f"[保本止损失败] {sym}: {e}")
            elif fake_bk:
                logger.info(f"[假突破减半] {sym} 保留原ATR止损={pos.stop_loss_px:.6g}，不设保本止损")

            # 直接调用_update_exchange_stoploss（内部已包含撤旧挂新逻辑）
            await self._update_exchange_stoploss(pos)
            pos._updating_sl = False
            self._save_positions()

            await self.notifier.send_message(
                f"🔔 *第一批止盈* `{sym}`\n"
                f"━━━━━━━━━━━━\n"
                f"平仓数量: `{half_qty}` (原{pos.qty_original}的{half_qty/pos.qty_original*100:.0f}%)\n"
                f"入场价: `{pos.entry_price:.6g}` → 当前: `{cur:.6g}`\n"
                f"本批盈亏: `{pnl:+.2f}%` ≈ `{pnl_usdt_half:+.2f}U`\n"
                f"剩余仓位继续持有，ATR移动止损跟踪\n"
                f"当前止损线: `{pos.stop_loss_px:.6g}`"
            )
            logger.info(f"[第一批止盈] {sym} 平{half_qty}，盈利{pnl:.2f}%")

        except Exception as e:
            err = str(e)[:300]
            logger.error(f"[第一批止盈失败] {sym}: {err}")
            pos._updating_sl = False
            await self.notifier.send_message(f"⚠️ *第一批止盈失败* `{sym}`\n{err}")

    # ── 全仓平仓 ───────────────────────────────────────────────
    async def _close_position(self, pos: Position, cur: float, pnl: float, reason: str):
        sym = pos.symbol
        side = "SELL" if pos.direction == "LONG" else "BUY"

        if pnl < 0:
            self._loss_count[sym] = self._loss_count.get(sym, 0) + 1
            cooldown_h = getattr(self.config, 'DAILY_LOSS_COOLDOWN_H', 4)
            self._today_losses[sym] = date.today()

            if self._loss_count[sym] >= 2:
                expires = time.time() + 86400
                await self.notifier.send_message(f"⛔ `{sym}` 连续亏损2次，加入24小时黑名单")
            else:
                expires = time.time() + cooldown_h * 3600

            self._blacklist[sym] = max(self._blacklist.get(sym, 0), expires)

            for hist in self._signal_history.get(sym, []):
                if hist.get("order_id") == pos.order_id:
                    hist["failed"] = True
                    break
        else:
            self._loss_count[sym] = 0

        # 先撤销交易所止损单，避免竞态条件
        try:
            await self.client.delete_signed("/fapi/v1/allOpenOrders", {"symbol": sym})
            logger.info(f"[平仓前撤单] {sym} 所有挂单已撤销")
        except Exception as e:
            logger.debug(f"[平仓前撤单失败] {sym}: {e}")

        # 检查交易所仓位是否仍存在（可能已被止损单平仓），并获取实际持仓数量
        actual_qty = None
        try:
            positions = await self.client.get_signed("/fapi/v2/positionRisk", {"symbol": sym})
            for p in positions:
                amt = float(p.get("positionAmt", 0))
                if pos.direction == "LONG" and amt > 0:
                    actual_qty = amt
                    break
                if pos.direction == "SHORT" and amt < 0:
                    actual_qty = abs(amt)
                    break
            else:
                logger.info(f"[平仓跳过] {sym} 交易所仓位已不存在（可能已被止损触发）")
                self.positions.pop(sym, None)
                self._save_positions()
                await self.notifier.send_message(
                    f"ℹ️ *仓位已由交易所平仓* `{sym}`\n"
                    f"方向: `{pos.direction}`\n"
                    f"原因: {reason}（交易所止损单已触发）"
                )
                return
        except Exception as e:
            logger.debug(f"[仓位检查失败] {sym}: {e}")

        # 使用交易所实际持仓数量平仓（避免程序记录与交易所不同步）
        close_qty = actual_qty if actual_qty is not None else pos.qty
        try:
            step = await self._get_step(sym)
            close_qty = _floor_step(close_qty, step)
            await self.client.post_signed(
                "/fapi/v1/order",
                {
                    "symbol":     sym,
                    "side":       side,
                    "type":       "MARKET",
                    "quantity":   _fmt(close_qty, step),
                    "reduceOnly": "true",
                },
            )

            pnl_usdt = pos.usdt * pnl / 100 * pos.leverage
            emoji = "✅" if pnl >= 0 else "❌"

            if pos.half_closed:
                # 第一批和第二批的pos.usdt相同（都是分批后剩余的一半）
                half_usdt_base = pos.usdt
                half_pnl_usdt  = half_usdt_base * pos.half_pnl / 100 * pos.leverage
                total_usdt     = half_pnl_usdt + pnl_usdt
                await self.notifier.send_message(
                    f"{emoji} *第二批平仓完成* `{sym}`\n"
                    f"━━━━━━━━━━━━\n"
                    f"方向: `{pos.direction}`\n"
                    f"入场: `{pos.entry_price:.6g}` → 出场: `{cur:.6g}`\n"
                    f"第二批盈亏: `{pnl:+.2f}%` ≈ `{pnl_usdt:+.2f}U`\n"
                    f"原因: {reason}\n"
                    f"━━━━━━━━━━━━\n"
                    f"📊 *两批合计: `{total_usdt:+.2f}U`*\n"
                    f"  第一批({pos.half_pnl:+.1f}%): `{half_pnl_usdt:+.2f}U`\n"
                    f"  第二批({pnl:+.1f}%): `{pnl_usdt:+.2f}U`"
                )
            else:
                await self.notifier.send_message(
                    f"{emoji} *平仓完成* `{sym}`\n"
                    f"━━━━━━━━━━━━\n"
                    f"方向: `{pos.direction}`\n"
                    f"入场: `{pos.entry_price:.6g}` → 出场: `{cur:.6g}`\n"
                    f"盈亏: `{pnl:+.2f}%` ≈ `{pnl_usdt:+.2f}U`\n"
                    f"原因: {reason}"
                )

            logger.info(f"[平仓] {sym} {pnl:+.2f}% | {reason}")

        except Exception as e:
            err = str(e)[:300]
            logger.error(f"[平仓失败] {sym}: {err}")

            # 平仓失败时立即补挂止损单作为保底
            backup_msg = ""
            try:
                tick = await self._get_tick(sym)
                step = await self._get_step(sym)
                await self.client.post_signed(
                    "/fapi/v1/order",
                    {
                        "symbol": sym,
                        "side": "SELL" if pos.direction == "LONG" else "BUY",
                        "type": "STOP_MARKET",
                        "stopPrice": _fmt(pos.stop_loss_px, tick),
                        "quantity": _fmt(pos.qty, step),
                        "reduceOnly": "true",
                        "workingType": "MARK_PRICE",
                    },
                )
                backup_msg = "✅ 已自动补挂止损单作为保底"
                logger.info(f"[平仓失败保底] {sym} 已重新挂止损单")
            except Exception as backup_err:
                backup_msg = "❌ 保底止损单也挂单失败，请立即手动处理！"
                logger.error(f"[保底止损也失败] {sym}: {backup_err}")

            await self.notifier.send_message(
                f"🚨 *平仓失败！请立即手动处理！*\n"
                f"币种: `{sym}`\n"
                f"方向: `{pos.direction}` 当前盈亏: `{pnl:+.2f}%`\n"
                f"原因: {reason}\n"
                f"错误: {err}\n"
                f"{backup_msg}\n"
                f"⚠️ 请登录币安App确认仓位！"
            )
            return

        self.positions.pop(sym, None)
        self._save_positions()
        try:
            await self.client.delete_signed("/fapi/v1/allOpenOrders", {"symbol": sym})
            logger.info(f"[撤单] {sym} 所有挂单已撤销")
        except Exception as e:
            logger.debug(f"[撤单失败] {sym}: {e}")

    # ── 辅助 ───────────────────────────────────────────────────
    async def get_account_summary(self) -> dict:
        try:
            data = await self.client.get_signed("/fapi/v2/account")
            result = {
                "wallet":     float(data.get("totalWalletBalance", 0)),
                "unrealized": float(data.get("totalUnrealizedProfit", 0)),
                "margin":     float(data.get("totalInitialMargin", 0)),
            }
            try:
                today_start = int(datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ).timestamp() * 1000)
                incomes = await self.client.get_signed("/fapi/v1/income", {
                    "incomeType": "REALIZED_PNL",
                    "startTime":  today_start,
                    "limit":      1000,
                })
                result["daily_realized"] = sum(
                    float(i.get("income", 0)) for i in incomes
                )
            except Exception:
                result["daily_realized"] = 0.0
            return result
        except Exception:
            return {"wallet": 0, "unrealized": 0, "margin": 0, "daily_realized": 0}

    def get_positions_summary(self) -> list[dict]:
        return [
            {
                "symbol":      s,
                "direction":   p.direction,
                "entry_price": p.entry_price,
                "qty":         p.qty,
                "usdt":        p.usdt,
                "leverage":    p.leverage,
                "open_time":   datetime.fromtimestamp(p.open_time).strftime("%H:%M:%S"),
                "score":       p.signal_score,
                "peak_pnl":    p.peak_pnl_pct,
                "stop_loss":   p.stop_loss_px,
                "take_profit": p.take_profit_px,
                "half_closed": p.half_closed,
            }
            for s, p in self.positions.items()
        ]

    def _save_positions(self):
        POSITIONS_FILE.parent.mkdir(exist_ok=True)
        POSITIONS_FILE.write_text(
            json.dumps(
                [p.to_dict() for p in self.positions.values()],
                ensure_ascii=False,
                indent=2,
            )
        )

    def _load_positions(self):
        if POSITIONS_FILE.exists():
            try:
                for d in json.loads(POSITIONS_FILE.read_text()):
                    p = Position.from_dict(d)
                    self.positions[p.symbol] = p
                logger.info(f"恢复 {len(self.positions)} 个持仓")
            except Exception as e:
                logger.warning(f"持仓恢复失败: {e}")

    async def audit_positions(self):
        bad = [sym for sym, pos in self.positions.items() if pos.entry_price <= 0]
        if not bad:
            return
        logger.error(f"发现 {len(bad)} 个异常持仓(entry_price=0): {bad}")
        await self.notifier.send_message(
            f"🚨 *启动审计发现异常持仓*\n"
            f"以下持仓 entry_price=0，已隔离，止盈止损无效：\n"
            + "\n".join(f"  • `{s}`" for s in bad) +
            f"\n\n请手动登录币安确认实际仓位并处理！"
        )
        for sym in bad:
            self.positions.pop(sym, None)
        self._save_positions()