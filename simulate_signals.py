import asyncio
import aiohttp
import json
import sys
import io
import os
import argparse
from datetime import datetime, timezone, date

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

FAPI = "https://fapi.binance.com"
PROXY = "http://127.0.0.1:10808"
SIGNAL_FILE = os.path.join(os.path.dirname(__file__), "data", "historical_signals.jsonl")

sys.path.insert(0, os.path.dirname(__file__))
from config import Config
_cfg = Config()

LEVERAGE = _cfg.LEVERAGE
USDT_PER_TRADE = _cfg.USDT_PER_TRADE
ATR_SL_MULTIPLIER = _cfg.ATR_SL_MULTIPLIER
ATR_TRAIL_MULTIPLIER = _cfg.ATR_TRAIL_MULTIPLIER
ATR_TP1_MULTIPLIER = _cfg.ATR_TP1_MULTIPLIER
HALF_CLOSE_RATIO = _cfg.HALF_CLOSE_RATIO
STOP_LOSS_PCT = _cfg.STOP_LOSS_PCT
MAX_HOLD_HOURS = _cfg.MAX_HOLD_HOURS
POST_HALF_HOLD_HOURS = _cfg.POST_HALF_HOLD_HOURS
MIN_SCORE = _cfg.MIN_SCORE
TRADABLE_MIN_SCORE = getattr(_cfg, 'TRADABLE_MIN_SCORE', 75)
MAX_SL_RISK_PCT = _cfg.MAX_SL_RISK_PCT
MIN_REMAINING_NOTIONAL = 20.0
MAX_ATR_PCT = _cfg.MAX_ATR_PCT

FUNDING_DIR_FILTER = _cfg.FUNDING_DIR_FILTER
PREFERRED_HOURS = [int(h) for h in _cfg.PREFERRED_HOURS.split(",")]
OFF_HOUR_SCORE_DISCOUNT = _cfg.OFF_HOUR_SCORE_DISCOUNT
BTC_ATR_VOL_THRESHOLD = _cfg.BTC_ATR_VOL_THRESHOLD
BTC_VOL_POSITION_SCALE = _cfg.BTC_VOL_POSITION_SCALE
CERTAINTY_MIN_FOR_BOOST = _cfg.CERTAINTY_MIN_FOR_BOOST
OPPORTUNITY_MIN_FOR_BOOST = _cfg.OPPORTUNITY_MIN_FOR_BOOST
POSITION_BOOST_SCALE = _cfg.POSITION_BOOST_SCALE
POSITION_REDUCE_SCALE = _cfg.POSITION_REDUCE_SCALE

DAILY_LOSS_COOLDOWN_H = _cfg.DAILY_LOSS_COOLDOWN_H
SIGNAL_REPEAT_ZONE_PCT = _cfg.SIGNAL_REPEAT_ZONE_PCT
FAKE_BREAKOUT_MIN = _cfg.FAKE_BREAKOUT_MIN
FAKE_BREAKOUT_ATR_RATIO = _cfg.FAKE_BREAKOUT_ATR_RATIO
FAKE_BK_HALF_MODE = _cfg.FAKE_BK_HALF_MODE

# 滑点模型：每笔交易成本（开仓+平仓双向合计）
SLIPPAGE_PCT = 0.15  # 0.15% 单边，双向共 0.3%

STOCK_BLACKLIST = getattr(_cfg, 'STOCK_RELATED_BLACKLIST', {
    "COIN","TSLA","AAPL","GOOG","NVDA","AMZN","MSTR","MARA","HOOD",
    "SQ","RIOT","CLSK","HUT","BTBT","BFCH","CBSE",
})


def is_tradable_signal(sig: dict, min_score: int = None) -> bool:
    if min_score is None:
        min_score = TRADABLE_MIN_SCORE
    structure = sig.get("factors", {}).get("structure_trigger", "")
    return (
        sig.get("score", 0) >= min_score
        and ("5m restart" in structure or "strong hold" in structure)
    )


def load_signals(path):
    signals = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            sig = json.loads(line)
            factors = sig.get("factors", {})
            score_parts = sig.get("score_parts", {})

            fr_str = factors.get("funding_rate", "0%")
            try:
                fr_val = float(fr_str.replace("%", "")) / 100
            except (ValueError, AttributeError):
                fr_val = 0.0

            atr = sig.get("atr")
            if atr is None:
                atr_pct_str = factors.get("atr_pct", "0%")
                try:
                    atr_pct_val = float(atr_pct_str.replace("%", "")) / 100
                except (ValueError, AttributeError):
                    atr_pct_val = 0.0
                atr = sig.get("price", 0) * atr_pct_val

            certainty_score = (
                score_parts.get("oi_surge", 0)
                + score_parts.get("ob_imbalance", 0)
                + score_parts.get("ls_ratio", 0)
                + score_parts.get("basis_diverge", 0)
            )
            opportunity_score = (
                score_parts.get("momentum_breakout", 0)
                + score_parts.get("volume_spike", 0)
                + score_parts.get("price_squeeze", 0)
                + score_parts.get("funding_extreme", 0)
            )

            ts = sig.get("timestamp", "")
            signals.append({
                "symbol": sig.get("symbol", ""),
                "direction": sig.get("direction", ""),
                "score": sig.get("score", 0),
                "price": float(sig.get("price", 0)),
                "atr": float(atr) if atr else 0,
                "time": ts,
                "funding_rate": fr_val,
                "certainty_score": certainty_score,
                "opportunity_score": opportunity_score,
                "trend_score": sig.get("trend_score", 0),
                "trigger_score": sig.get("trigger_score", 0),
                "emotion_score": sig.get("emotion_score", 0),
                "market_state": sig.get("market_state", ""),
                "atr_pct_str": factors.get("atr_pct", ""),
                "structure_trigger": factors.get("structure_trigger", ""),
                "factors": factors,
            })
    return signals


async def fetch_klines(session, symbol, interval, start_ms, end_ms, limit=500):
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    }
    try:
        async with session.get(f"{FAPI}/fapi/v1/klines", params=params, proxy=PROXY) as r:
            if r.status == 200:
                return await r.json()
            else:
                text = await r.text()
                print(f"  [{symbol}] klines error {r.status}: {text[:100]}")
                return []
    except Exception as e:
        print(f"  [{symbol}] klines exception: {e}")
        return []


async def fetch_btc_atr_pct(session, sig_ts_ms):
    start_ms = sig_ts_ms - 15 * 3600 * 1000
    try:
        async with session.get(
            f"{FAPI}/fapi/v1/klines",
            params={"symbol": "BTCUSDT", "interval": "1h", "startTime": start_ms, "endTime": sig_ts_ms, "limit": 15},
            proxy=PROXY,
        ) as r:
            if r.status != 200:
                return 0.0
            klines = await r.json()
            if len(klines) < 15:
                return 0.0
            trs = []
            for i in range(1, len(klines)):
                h = float(klines[i][2])
                l = float(klines[i][3])
                pc = float(klines[i - 1][4])
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            atr = sum(trs[-14:]) / 14
            price = float(klines[-1][4])
            return atr / price * 100 if price > 0 else 0.0
    except Exception:
        return 0.0


def calc_atr(klines_1h, period=14):
    if len(klines_1h) < period + 1:
        return None
    trs = []
    for i in range(1, len(klines_1h)):
        h = float(klines_1h[i][2])
        l = float(klines_1h[i][3])
        pc = float(klines_1h[i - 1][4])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def get_position_usdt(score, certainty_score, opportunity_score, btc_atr_pct):
    if score >= TRADABLE_MIN_SCORE:
        if certainty_score >= CERTAINTY_MIN_FOR_BOOST and opportunity_score >= OPPORTUNITY_MIN_FOR_BOOST:
            usdt = USDT_PER_TRADE * POSITION_BOOST_SCALE
            mode = f"{POSITION_BOOST_SCALE}x(C{certainty_score}>={CERTAINTY_MIN_FOR_BOOST}+O{opportunity_score}>={OPPORTUNITY_MIN_FOR_BOOST})"
        else:
            usdt = USDT_PER_TRADE
            mode = "1.0x(default)"
    else:
        usdt = USDT_PER_TRADE * POSITION_REDUCE_SCALE
        mode = f"{POSITION_REDUCE_SCALE}x(score<{TRADABLE_MIN_SCORE})"

    if btc_atr_pct > BTC_ATR_VOL_THRESHOLD:
        usdt = usdt * BTC_VOL_POSITION_SCALE
        mode += f"->{BTC_VOL_POSITION_SCALE}x(BTC{btc_atr_pct:.1f}%)"

    return usdt, mode


def simulate_trade(entry_price, direction, atr, klines_5m, signal_ts_ms, now_ms, usdt):
    if atr is None or atr <= 0:
        atr = entry_price * STOP_LOSS_PCT / 100

    atr_pct = atr / entry_price * 100
    if atr_pct > MAX_ATR_PCT:
        return {
            "status": f"ATR-HIGH{atr_pct:.0f}%",
            "close_time": "-",
            "close_reason": f"ATR%={atr_pct:.1f}%>{MAX_ATR_PCT}%",
            "pnl_usdt": 0.0,
            "pnl_pct": 0.0,
            "half_pnl_usdt": 0.0,
            "half_pnl_pct": 0.0,
            "remaining_pnl_usdt": 0.0,
            "remaining_pnl_pct": 0.0,
            "entry_price": entry_price,
            "sl_px": 0,
            "tp1_px": 0,
            "atr": atr,
            "atr_pct": f"{atr_pct:.2f}%",
            "risk_skipped": True,
            "atr_skipped": True,
        }

    if direction == "LONG":
        sl_px = entry_price - atr * ATR_SL_MULTIPLIER
        tp1_px = entry_price + atr * ATR_TP1_MULTIPLIER
    else:
        sl_px = entry_price + atr * ATR_SL_MULTIPLIER
        tp1_px = entry_price - atr * ATR_TP1_MULTIPLIER

    sl_distance_pct = abs(entry_price - sl_px) / entry_price
    risk_pct = sl_distance_pct * LEVERAGE
    if risk_pct > MAX_SL_RISK_PCT:
        return {
            "status": f"LIQ-RISK{risk_pct*100:.0f}%",
            "close_time": "-",
            "close_reason": f"SL-risk{risk_pct*100:.0f}%>{MAX_SL_RISK_PCT*100:.0f}%",
            "pnl_usdt": 0.0,
            "pnl_pct": 0.0,
            "half_pnl_usdt": 0.0,
            "half_pnl_pct": 0.0,
            "remaining_pnl_usdt": 0.0,
            "remaining_pnl_pct": 0.0,
            "entry_price": entry_price,
            "sl_px": sl_px,
            "tp1_px": tp1_px,
            "atr": atr,
            "atr_pct": f"{atr/entry_price*100:.2f}%",
            "risk_skipped": True,
            "atr_skipped": False,
        }

    half_closed = False
    peak_price = entry_price
    trail_sl_px = sl_px
    result = {
        "status": "RUNNING",
        "close_time": "-",
        "close_reason": "-",
        "pnl_usdt": 0.0,
        "pnl_pct": 0.0,
        "half_pnl_usdt": 0.0,
        "half_pnl_pct": 0.0,
        "remaining_pnl_usdt": 0.0,
        "remaining_pnl_pct": 0.0,
        "entry_price": entry_price,
        "sl_px": sl_px,
        "tp1_px": tp1_px,
        "atr": atr,
        "atr_pct": f"{atr/entry_price*100:.2f}%",
        "risk_skipped": False,
        "atr_skipped": False,
    }

    for k in klines_5m:
        k_open_ts = int(k[0])
        if k_open_ts < signal_ts_ms:
            continue

        k_high = float(k[2])
        k_low = float(k[3])
        k_close = float(k[4])
        hold_hours = (k_open_ts - signal_ts_ms) / 3600 / 1000
        hold_min = (k_open_ts - signal_ts_ms) / 60 / 1000

        if not half_closed:
            if direction == "LONG":
                if k_low <= sl_px:
                    loss_pct = (sl_px - entry_price) / entry_price * 100 * LEVERAGE
                    result["status"] = "SL-HIT"
                    result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                    result["close_reason"] = f"ATR-SL @{sl_px:.6g}"
                    result["pnl_usdt"] = usdt * loss_pct / 100
                    result["pnl_pct"] = loss_pct
                    return result
                if k_high >= tp1_px:
                    half_pnl_pct = (tp1_px - entry_price) / entry_price * 100 * LEVERAGE
                    result["half_pnl_pct"] = half_pnl_pct
                    result["half_pnl_usdt"] = usdt * HALF_CLOSE_RATIO * half_pnl_pct / 100
                    half_closed = True
                    peak_price = k_high
                    trail_sl_px = entry_price
                    result["status"] = "TP1-DONE"
                    result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S") + "(TP1)"
            else:
                if k_high >= sl_px:
                    loss_pct = (entry_price - sl_px) / entry_price * 100 * LEVERAGE
                    result["status"] = "SL-HIT"
                    result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                    result["close_reason"] = f"ATR-SL @{sl_px:.6g}"
                    result["pnl_usdt"] = usdt * loss_pct / 100
                    result["pnl_pct"] = loss_pct
                    return result
                if k_low <= tp1_px:
                    half_pnl_pct = (entry_price - tp1_px) / entry_price * 100 * LEVERAGE
                    result["half_pnl_pct"] = half_pnl_pct
                    result["half_pnl_usdt"] = usdt * HALF_CLOSE_RATIO * half_pnl_pct / 100
                    half_closed = True
                    peak_price = k_low
                    trail_sl_px = entry_price
                    result["status"] = "TP1-DONE"
                    result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S") + "(TP1)"

            if not half_closed and atr > 0 and hold_min >= FAKE_BREAKOUT_MIN:
                if direction == "LONG":
                    favorable = (k_close - entry_price) / atr
                else:
                    favorable = (entry_price - k_close) / atr
                if favorable < FAKE_BREAKOUT_ATR_RATIO:
                    if direction == "LONG":
                        rem_pnl_pct = (k_close - entry_price) / entry_price * 100 * LEVERAGE
                    else:
                        rem_pnl_pct = (entry_price - k_close) / entry_price * 100 * LEVERAGE
                    if FAKE_BK_HALF_MODE:
                        result["status"] = "FAKE-BK-HALF"
                        result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                        result["close_reason"] = f"fake-breakout-half {hold_min:.0f}min move={favorable:.2f}xATR<{FAKE_BREAKOUT_ATR_RATIO}x"
                        result["half_pnl_pct"] = rem_pnl_pct
                        result["half_pnl_usdt"] = usdt * HALF_CLOSE_RATIO * rem_pnl_pct / 100
                        half_closed = True
                        trail_sl_px = sl_px
                        result["status"] = "FAKE-BK-HALF"
                    else:
                        result["status"] = "FAKE-BK"
                        result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                        result["close_reason"] = f"fake-breakout {hold_min:.0f}min move={favorable:.2f}xATR<{FAKE_BREAKOUT_ATR_RATIO}x"
                        result["pnl_usdt"] = usdt * rem_pnl_pct / 100
                        result["pnl_pct"] = rem_pnl_pct
                        return result

            if hold_hours >= MAX_HOLD_HOURS and not half_closed:
                if direction == "LONG":
                    rem_pnl_pct = (k_close - entry_price) / entry_price * 100 * LEVERAGE
                else:
                    rem_pnl_pct = (entry_price - k_close) / entry_price * 100 * LEVERAGE
                result["status"] = "TIMEOUT"
                result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                result["close_reason"] = f"timeout {hold_hours:.1f}h"
                result["pnl_usdt"] = usdt * rem_pnl_pct / 100
                result["pnl_pct"] = rem_pnl_pct
                return result
        else:
            remaining_qty_ratio = 1 - HALF_CLOSE_RATIO
            remaining_notional = usdt * remaining_qty_ratio * LEVERAGE
            if remaining_notional < MIN_REMAINING_NOTIONAL:
                if direction == "LONG":
                    rem_pnl_pct = (k_close - entry_price) / entry_price * 100 * LEVERAGE
                else:
                    rem_pnl_pct = (entry_price - k_close) / entry_price * 100 * LEVERAGE
                total_pnl = result["half_pnl_usdt"] + usdt * remaining_qty_ratio * rem_pnl_pct / 100
                result["status"] = "TP-SMALL"
                result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                result["close_reason"] = f"remain${remaining_notional:.0f}<${MIN_REMAINING_NOTIONAL:.0f}"
                result["remaining_pnl_pct"] = rem_pnl_pct
                result["remaining_pnl_usdt"] = usdt * remaining_qty_ratio * rem_pnl_pct / 100
                result["pnl_usdt"] = total_pnl
                result["pnl_pct"] = total_pnl / usdt * 100
                return result

            if direction == "LONG":
                if k_high > peak_price:
                    peak_price = k_high
                new_sl = peak_price - atr * ATR_TRAIL_MULTIPLIER
                if new_sl > trail_sl_px:
                    trail_sl_px = new_sl
                if k_low <= trail_sl_px:
                    rem_pnl_pct = (trail_sl_px - entry_price) / entry_price * 100 * LEVERAGE
                    total_pnl = result["half_pnl_usdt"] + usdt * (1 - HALF_CLOSE_RATIO) * rem_pnl_pct / 100
                    result["status"] = "TRAIL-TP"
                    result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                    result["close_reason"] = f"trail-SL @{trail_sl_px:.6g}"
                    result["remaining_pnl_pct"] = rem_pnl_pct
                    result["remaining_pnl_usdt"] = usdt * (1 - HALF_CLOSE_RATIO) * rem_pnl_pct / 100
                    result["pnl_usdt"] = total_pnl
                    result["pnl_pct"] = total_pnl / usdt * 100
                    return result
            else:
                if k_low < peak_price:
                    peak_price = k_low
                new_sl = peak_price + atr * ATR_TRAIL_MULTIPLIER
                if new_sl < trail_sl_px:
                    trail_sl_px = new_sl
                if k_high >= trail_sl_px:
                    rem_pnl_pct = (entry_price - trail_sl_px) / entry_price * 100 * LEVERAGE
                    total_pnl = result["half_pnl_usdt"] + usdt * (1 - HALF_CLOSE_RATIO) * rem_pnl_pct / 100
                    result["status"] = "TRAIL-TP"
                    result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                    result["close_reason"] = f"trail-SL @{trail_sl_px:.6g}"
                    result["remaining_pnl_pct"] = rem_pnl_pct
                    result["remaining_pnl_usdt"] = usdt * (1 - HALF_CLOSE_RATIO) * rem_pnl_pct / 100
                    result["pnl_usdt"] = total_pnl
                    result["pnl_pct"] = total_pnl / usdt * 100
                    return result

            if hold_hours >= POST_HALF_HOLD_HOURS:
                if direction == "LONG":
                    rem_pnl_pct = (k_close - entry_price) / entry_price * 100 * LEVERAGE
                else:
                    rem_pnl_pct = (entry_price - k_close) / entry_price * 100 * LEVERAGE
                total_pnl = result["half_pnl_usdt"] + usdt * (1 - HALF_CLOSE_RATIO) * rem_pnl_pct / 100
                result["status"] = "POST-TP-TIMEOUT"
                result["close_time"] = datetime.fromtimestamp(k_open_ts / 1000).strftime("%H:%M:%S")
                result["close_reason"] = f"timeout {hold_hours:.1f}h"
                result["remaining_pnl_pct"] = rem_pnl_pct
                result["remaining_pnl_usdt"] = usdt * (1 - HALF_CLOSE_RATIO) * rem_pnl_pct / 100
                result["pnl_usdt"] = total_pnl
                result["pnl_pct"] = total_pnl / usdt * 100
                return result

    if half_closed:
        if klines_5m:
            last_close = float(klines_5m[-1][4])
            if direction == "LONG":
                rem_pnl_pct = (last_close - entry_price) / entry_price * 100 * LEVERAGE
            else:
                rem_pnl_pct = (entry_price - last_close) / entry_price * 100 * LEVERAGE
            result["remaining_pnl_pct"] = rem_pnl_pct
            result["remaining_pnl_usdt"] = usdt * (1 - HALF_CLOSE_RATIO) * rem_pnl_pct / 100
            result["pnl_usdt"] = result["half_pnl_usdt"] + result["remaining_pnl_usdt"]
            result["pnl_pct"] = result["pnl_usdt"] / usdt * 100
    else:
        if klines_5m:
            last_close = float(klines_5m[-1][4])
            if direction == "LONG":
                result["pnl_pct"] = (last_close - entry_price) / entry_price * 100 * LEVERAGE
            else:
                result["pnl_pct"] = (entry_price - last_close) / entry_price * 100 * LEVERAGE
            result["pnl_usdt"] = usdt * result["pnl_pct"] / 100

    # 扣除滑点成本（双向 2 * SLIPPAGE_PCT * LEVERAGE）
    slippage_cost = usdt * LEVERAGE * (2 * SLIPPAGE_PCT / 100)
    result["pnl_usdt"] -= slippage_cost
    if usdt > 0:
        result["pnl_pct"] = result["pnl_usdt"] / usdt * 100
    result["slippage_cost"] = slippage_cost

    return result


async def main():
    global FAKE_BREAKOUT_MIN, FAKE_BREAKOUT_ATR_RATIO, OFF_HOUR_SCORE_DISCOUNT, FUNDING_DIR_FILTER, FAKE_BK_HALF_MODE
    global BTC_ATR_VOL_THRESHOLD, BTC_VOL_POSITION_SCALE, CERTAINTY_MIN_FOR_BOOST, OPPORTUNITY_MIN_FOR_BOOST
    global MIN_SCORE, TRADABLE_MIN_SCORE
    global POSITION_BOOST_SCALE, POSITION_REDUCE_SCALE, SIGNAL_REPEAT_ZONE_PCT

    parser = argparse.ArgumentParser()
    parser.add_argument("--no-fakebk", action="store_true", help="Disable fake breakout filter")
    parser.add_argument("--no-hour-discount", action="store_true", help="Disable off-hour score discount")
    parser.add_argument("--no-fr-filter", action="store_true", help="Disable funding rate direction filter")
    parser.add_argument("--fakebk-half", action="store_true", help="Fake breakout: close half instead of full")
    parser.add_argument("--no-experimental", action="store_true", help="Disable ALL experimental filters (A/B control group)")
    parser.add_argument("--min-score", type=int, default=None, help="Override MIN_SCORE for backtest")
    parser.add_argument("--tradable-min-score", type=int, default=None, help="Override TRADABLE_MIN_SCORE for backtest")
    args = parser.parse_args()

    if args.min_score is not None:
        MIN_SCORE = args.min_score
        print(f"[OVERRIDE] MIN_SCORE={MIN_SCORE}")
    if args.tradable_min_score is not None:
        TRADABLE_MIN_SCORE = args.tradable_min_score
        print(f"[OVERRIDE] TRADABLE_MIN_SCORE={TRADABLE_MIN_SCORE}")

    if args.no_experimental:
        FAKE_BREAKOUT_MIN = 99999
        OFF_HOUR_SCORE_DISCOUNT = 1.0
        FUNDING_DIR_FILTER = False
        FAKE_BK_HALF_MODE = True
        BTC_ATR_VOL_THRESHOLD = 999.0
        BTC_VOL_POSITION_SCALE = 1.0
        CERTAINTY_MIN_FOR_BOOST = 0
        OPPORTUNITY_MIN_FOR_BOOST = 0
        POSITION_BOOST_SCALE = 1.0
        POSITION_REDUCE_SCALE = 1.0
        SIGNAL_REPEAT_ZONE_PCT = 0.0
        print("[MODE] === ALL EXPERIMENTAL FILTERS DISABLED (Control Group B) ===")
        print("[MODE] Only hard risk controls: MAX_ATR_PCT, MAX_SL_RISK_PCT, STOCK_BLACKLIST, DAILY_LOSS_COOLDOWN")
    else:
        print("[MODE] === EXPERIMENTAL FILTERS ENABLED (Group A) ===")

    if args.no_fakebk:
        FAKE_BREAKOUT_MIN = 99999
        print("[MODE] FAKE-BK filter DISABLED")
    if args.no_hour_discount:
        OFF_HOUR_SCORE_DISCOUNT = 1.0
        print("[MODE] Off-hour score discount DISABLED")
    if args.no_fr_filter:
        FUNDING_DIR_FILTER = False
        print("[MODE] FR direction filter DISABLED")
    if args.fakebk_half:
        FAKE_BK_HALF_MODE = True
        print("[MODE] FAKE-BK will close HALF position instead of full")

    if not os.path.exists(SIGNAL_FILE):
        print(f"Signal file not found: {SIGNAL_FILE}")
        return

    signals = load_signals(SIGNAL_FILE)
    print(f"Loaded {len(signals)} signals from {SIGNAL_FILE}")

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15), connector=connector) as session:
        today_losses = {}
        failed_signals = {}

        results = []
        for i, sig in enumerate(signals):
            sym = sig["symbol"]
            direction = sig["direction"]
            score = sig["score"]
            entry_price = sig["price"]
            sig_time = sig["time"]
            funding_rate = sig.get("funding_rate", 0)
            certainty_score = sig.get("certainty_score", 0)
            opportunity_score = sig.get("opportunity_score", 0)
            atr_from_signal = sig.get("atr", 0)

            filter_reasons = []

            base = sym.replace("USDT", "")
            if base in STOCK_BLACKLIST:
                filter_reasons.append(f"STOCK-TOKEN({base})")

            if sym in today_losses and today_losses[sym] == date.today():
                filter_reasons.append(f"DAY-LOSS({base})")

            if not is_tradable_signal(sig, TRADABLE_MIN_SCORE):
                filter_reasons.append(f"not-tradable(score{score}<{TRADABLE_MIN_SCORE} or no 5m-restart/strong-hold)")

            if FUNDING_DIR_FILTER:
                if direction == "LONG" and funding_rate < 0:
                    filter_reasons.append(f"LONG-but-FR{funding_rate*100:.2f}%<0")
                elif direction == "SHORT" and funding_rate > 0:
                    filter_reasons.append(f"SHORT-but-FR{funding_rate*100:.2f}%>0")

            try:
                sig_dt = datetime.strptime(sig_time, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                sig_dt = datetime.now()
            utc_hour = sig_dt.hour
            if utc_hour not in PREFERRED_HOURS:
                adjusted_score = int(score * OFF_HOUR_SCORE_DISCOUNT)
                filter_reasons.append(f"UTC{utc_hour}h-off-hour->{adjusted_score}(x{OFF_HOUR_SCORE_DISCOUNT})")
                score = adjusted_score
                if score < MIN_SCORE:
                    filter_reasons.append(f"discounted-score{score}<{MIN_SCORE}")

            if sym in failed_signals and entry_price > 0:
                for fs in failed_signals[sym]:
                    fp = fs.get("price", 0)
                    if fp > 0 and abs(entry_price - fp) / fp * 100 < SIGNAL_REPEAT_ZONE_PCT:
                        filter_reasons.append(f"REPEAT-ZONE({entry_price:.4g}~{fp:.4g})")
                        break

            filtered = "TRADE" if not filter_reasons else "SKIP"
            filter_str = "; ".join(filter_reasons) if filter_reasons else "-"

            sig_ts_ms = int(sig_dt.timestamp() * 1000)

            print(f"[{i+1}/{len(signals)}] {sym} {direction} score={sig['score']}->{score} entry={entry_price} ...")

            if atr_from_signal and atr_from_signal > 0:
                atr = atr_from_signal
            else:
                atr_1h_start = sig_ts_ms - 20 * 3600 * 1000
                klines_1h = await fetch_klines(session, sym, "1h", atr_1h_start, sig_ts_ms, limit=20)
                atr = calc_atr(klines_1h) if klines_1h else None

            btc_atr_pct = await fetch_btc_atr_pct(session, sig_ts_ms)

            usdt, pos_mode = get_position_usdt(score, certainty_score, opportunity_score, btc_atr_pct)

            sim_end_ms = sig_ts_ms + 24 * 3600 * 1000
            klines_5m = await fetch_klines(session, sym, "5m", sig_ts_ms, sim_end_ms, limit=288)

            if not klines_5m:
                print(f"  no 5m data, skip")
                results.append({
                    "symbol": sym, "direction": direction, "score": sig["score"],
                    "adjusted_score": score, "entry": entry_price, "time": sig_time,
                    "filtered": filtered, "filter_reason": filter_str,
                    "status": "NO-DATA", "pnl_usdt": 0, "pnl_pct": 0,
                    "close_time": "-", "close_reason": "-",
                    "atr": "-", "atr_pct": "-", "risk_skipped": False, "atr_skipped": False,
                    "usdt": usdt, "pos_mode": pos_mode,
                    "funding_rate": funding_rate, "btc_atr_pct": btc_atr_pct,
                    "certainty_score": certainty_score, "opportunity_score": opportunity_score,
                })
                await asyncio.sleep(0.2)
                continue

            res = simulate_trade(entry_price, direction, atr, klines_5m, sig_ts_ms, 0, usdt)
            res["symbol"] = sym
            res["direction"] = direction
            res["score"] = sig["score"]
            res["adjusted_score"] = score
            res["entry"] = entry_price
            res["time"] = sig_time
            res["usdt"] = usdt
            res["pos_mode"] = pos_mode
            res["funding_rate"] = funding_rate
            res["btc_atr_pct"] = btc_atr_pct
            res["certainty_score"] = certainty_score
            res["opportunity_score"] = opportunity_score
            res["filter_reason"] = filter_str

            if res.get("risk_skipped"):
                if res.get("atr_skipped"):
                    filtered = "ATR-HIGH"
                else:
                    filtered = "LIQ-RISK"

            res["filtered"] = filtered
            results.append(res)

            if filtered == "TRADE" and res["pnl_usdt"] < 0:
                today_losses[sym] = date.today()
                failed_signals.setdefault(sym, []).append({"price": entry_price, "time": sig_time})

            status = res["status"]
            pnl = res["pnl_usdt"]
            risk_flag = " !!LIQ-RISK" if res.get("risk_skipped") and not res.get("atr_skipped") else ""
            atr_flag = " !!ATR-HIGH" if res.get("atr_skipped") else ""
            fund_flag = ""
            if FUNDING_DIR_FILTER:
                if direction == "LONG" and funding_rate < 0:
                    fund_flag = " !!FR-REVERSE"
            print(f"  -> {status} PnL={pnl:+.2f}U ({res['pnl_pct']:+.1f}%) ATR={res.get('atr_pct','?')} pos={pos_mode}{risk_flag}{atr_flag}{fund_flag}")

            await asyncio.sleep(0.3)

        print("\n" + "=" * 190)
        print(f"{'#':>3} {'Symbol':<14} {'Dir':<6} {'Sc':>3} {'Adj':>3} {'Entry':>12} {'ATR%':>7} {'FR%':>8} {'BTC%':>5} {'Position':>24} {'Status':<14} {'CloseTime':<14} {'Reason':<40} {'PnL_U':>8} {'Filter':>10}")
        print("-" * 190)

        total_pnl = 0
        traded_pnl = 0
        traded_count = 0
        skipped_risk = 0
        skipped_atr = 0
        skipped_funding = 0
        skipped_hour = 0
        skipped_stock = 0
        skipped_dayloss = 0
        skipped_repeat = 0
        skipped_not_tradable = 0
        total_invested = 0
        for i, r in enumerate(results):
            total_pnl += r["pnl_usdt"]
            if r["filtered"] == "TRADE":
                traded_pnl += r["pnl_usdt"]
                traded_count += 1
                total_invested += r["usdt"]
            if r.get("risk_skipped") and not r.get("atr_skipped"):
                skipped_risk += 1
            if r.get("atr_skipped"):
                skipped_atr += 1
            fr = r.get("funding_rate", 0)
            if FUNDING_DIR_FILTER:
                d = r.get("direction", "")
                if d == "LONG" and fr < 0:
                    skipped_funding += 1
            fstr = r.get("filter_reason", "")
            if "off-hour" in fstr:
                skipped_hour += 1
            if "STOCK-TOKEN" in fstr:
                skipped_stock += 1
            if "DAY-LOSS" in fstr:
                skipped_dayloss += 1
            if "REPEAT-ZONE" in fstr:
                skipped_repeat += 1
            if "not-tradable" in fstr:
                skipped_not_tradable += 1

            sym_display = r["symbol"].replace("USDT", "/USDT")
            atr_pct = r.get('atr_pct', '-')
            fr_str = f"{fr*100:.2f}" if fr else "-"
            btc_str = f"{r.get('btc_atr_pct', 0):.2f}" if r.get('btc_atr_pct') else "-"
            pos_mode = r.get('pos_mode', '-')
            print(f"{i+1:>3} {sym_display:<14} {r['direction']:<6} {r['score']:>3} {r.get('adjusted_score',r['score']):>3} {r['entry']:>12.6f} {atr_pct:>7} {fr_str:>8} {btc_str:>5} {pos_mode:>24} {r['status']:<14} {r.get('close_time','-'):<14} {r.get('close_reason','-'):<40} {r['pnl_usdt']:>+8.2f} {r['filtered']:>10}")

        print("-" * 190)
        print(f"{'TOTAL(all signals)':<100} {'':<14} {'':<40} {total_pnl:>+8.2f}U")
        print(f"{'TOTAL(tradable by new code)':<100} {'':<14} {'':<40} {traded_pnl:>+8.2f}U")
        print("=" * 190)

        win = sum(1 for r in results if r["pnl_usdt"] > 0 and r["filtered"] == "TRADE")
        lose = sum(1 for r in results if r["pnl_usdt"] < 0 and r["filtered"] == "TRADE")
        total_traded = win + lose
        print(f"\nTotal signals: {len(results)}")
        print(f"  Tradable: {traded_count}")
        print(f"  Stock-token skipped: {skipped_stock}")
        print(f"  Day-loss skipped: {skipped_dayloss}")
        print(f"  ATR-high skipped: {skipped_atr}")
        print(f"  Liq-risk skipped: {skipped_risk}")
        print(f"  FR-direction skipped: {skipped_funding}")
        print(f"  Off-hour skipped: {skipped_hour}")
        print(f"  Not-tradable skipped: {skipped_not_tradable}")
        print(f"  Repeat-zone skipped: {skipped_repeat}")
        if total_traded > 0:
            print(f"\n  Actual trades: {total_traded}  W: {win}  L: {lose}  WR: {win/total_traded*100:.0f}%")
            print(f"  Total invested: {total_invested:.0f}U  Total PnL: {traded_pnl:+.2f}U  ROI: {traded_pnl/total_invested*100:+.1f}%")
            avg_win = sum(r["pnl_usdt"] for r in results if r["pnl_usdt"] > 0 and r["filtered"] == "TRADE") / max(win, 1)
            avg_loss = sum(r["pnl_usdt"] for r in results if r["pnl_usdt"] < 0 and r["filtered"] == "TRADE") / max(lose, 1)
            print(f"  Avg win: {avg_win:+.2f}U  Avg loss: {avg_loss:+.2f}U  R:R = 1:{abs(avg_loss/avg_win):.2f}" if avg_win > 0 else "")

            # 计算 MaxDD 和 Sharpe
            trade_pnls = [r["pnl_usdt"] for r in results if r["filtered"] == "TRADE"]
            if trade_pnls:
                # MaxDD: 最大回撤
                cumulative = []
                running_sum = 0
                for pnl in trade_pnls:
                    running_sum += pnl
                    cumulative.append(running_sum)
                peak = 0
                max_dd = 0
                for val in cumulative:
                    if val > peak:
                        peak = val
                    dd = peak - val
                    if dd > max_dd:
                        max_dd = dd
                # Sharpe: 简化版（无风险利率=0）
                import statistics
                if len(trade_pnls) > 1:
                    mean_pnl = statistics.mean(trade_pnls)
                    std_pnl = statistics.stdev(trade_pnls)
                    sharpe = (mean_pnl / std_pnl * (252 ** 0.5)) if std_pnl > 0 else 0  # 年化Sharpe，假设每日交易
                else:
                    sharpe = 0
                total_slippage = sum(r.get("slippage_cost", 0) for r in results if r["filtered"] == "TRADE")
                print(f"\n  Max Drawdown: {max_dd:.2f}U  Sharpe: {sharpe:.2f}")
                print(f"  Total slippage cost: {total_slippage:.2f}U")
        print(f"\nParams: LEVERAGE={LEVERAGE}x  ATR_SL={ATR_SL_MULTIPLIER}x  MAX_ATR_PCT={MAX_ATR_PCT}%")
        print(f"Score: MIN_SCORE={MIN_SCORE}  TRADABLE_MIN_SCORE={TRADABLE_MIN_SCORE}")
        print(f"Filters: FR-dir={FUNDING_DIR_FILTER}, Hours={PREFERRED_HOURS}, BTC-vol>{BTC_ATR_VOL_THRESHOLD}%")
        print(f"Dynamic pos: C>={CERTAINTY_MIN_FOR_BOOST}+O>={OPPORTUNITY_MIN_FOR_BOOST}->{POSITION_BOOST_SCALE}x, else default=1.0x")
        print(f"Anti-repeat: DAY-LOSS={DAILY_LOSS_COOLDOWN_H}h, ZONE={SIGNAL_REPEAT_ZONE_PCT}%, FAKE-BK={FAKE_BREAKOUT_MIN}min/{FAKE_BREAKOUT_ATR_RATIO}xATR")


if __name__ == "__main__":
    asyncio.run(main())
