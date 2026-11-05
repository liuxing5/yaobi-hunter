#!/usr/bin/env python3
"""
Historical Signal Generator v3 - With OI/Funding/LS historical data

Strategy:
  - Sequential processing (1 symbol at a time)
  - Klines (1h/5m/15m) + OI history + Funding history + LS ratio
  - OB (order book) still N/A (no historical data available)
  - 0.5s delay between API calls, 2s between symbols
  - 418 ban auto-recovery
  - Progress saved to resume if interrupted

Usage:
  python tools/historical_signal_generator.py --days 7
  python tools/historical_signal_generator.py --days 3 --top 30
  python tools/historical_signal_generator.py --resume
"""

import os
import sys
import io
import asyncio
import aiohttp
import json
import time
import statistics
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

FAPI = "https://fapi.binance.com"
PROXY = "http://127.0.0.1:10808"
DELAY_BETWEEN_CALLS = 0.5
DELAY_BETWEEN_SYMBOLS = 3.0

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config, TOP30_BLACKLIST, STOCK_RELATED_BLACKLIST

cfg = Config()
THRESHOLDS = cfg.THRESHOLDS
WEIGHTS = cfg.FACTOR_WEIGHTS
HIST_MIN_SCORE = 50
HIST_MIN_TRIGGER = 30


class RateLimiter:
    def __init__(self):
        self.last_call = 0
        self.ban_until = 0

    async def wait(self):
        now = time.time()
        if now < self.ban_until:
            wait_s = self.ban_until - now + 1
            print(f"    [BAN] Waiting {wait_s:.0f}s until ban expires...", flush=True)
            await asyncio.sleep(wait_s)
        elapsed = now - self.last_call
        if elapsed < DELAY_BETWEEN_CALLS:
            await asyncio.sleep(DELAY_BETWEEN_CALLS - elapsed)
        self.last_call = time.time()

    def set_ban(self, until_ts_ms):
        self.ban_until = until_ts_ms / 1000 + 1


limiter = RateLimiter()


async def api_get(session, path, params=None, retries=3):
    for attempt in range(retries):
        await limiter.wait()
        try:
            async with session.get(FAPI + path, params=params, proxy=PROXY) as r:
                if r.status == 200:
                    return await r.json()
                if r.status == 429:
                    wait = 5 * (attempt + 1)
                    print(f"    [429] rate limited, waiting {wait}s...", flush=True)
                    await asyncio.sleep(wait)
                    continue
                if r.status == 418:
                    text = await r.text()
                    ban_ts = 0
                    try:
                        data = json.loads(text)
                        ban_ts = int(data.get("banUntil", 0))
                    except Exception:
                        pass
                    if ban_ts > 0:
                        limiter.set_ban(ban_ts)
                        print(f"    [418] IP banned until {datetime.fromtimestamp(ban_ts/1000)}", flush=True)
                        continue
                    await asyncio.sleep(60)
                    continue
                text = await r.text()
                if attempt == 0:
                    print(f"    [HTTP {r.status}] {path}: {text[:60]}", flush=True)
                return None
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2)
            else:
                print(f"    [ERR] {path}: {e}", flush=True)
                return None
    return None


def ts_to_hour(ts_ms):
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)


def hour_to_ts(dt):
    return int(dt.timestamp() * 1000)


async def fetch_symbols(session, top_n=50):
    data = await api_get(session, "/fapi/v1/ticker/24hr")
    if not data:
        print("[FATAL] Cannot fetch symbol list", flush=True)
        sys.exit(1)
    filtered = []
    for d in data:
        sym = d["symbol"]
        if not sym.endswith("USDT"):
            continue
        base = sym.replace("USDT", "")
        if base in TOP30_BLACKLIST or base in STOCK_RELATED_BLACKLIST:
            continue
        vol = float(d.get("quoteVolume", 0))
        if vol < 2_000_000:
            continue
        filtered.append((sym, vol))
    filtered.sort(key=lambda x: x[1], reverse=True)
    symbols = [s for s, _ in filtered[:top_n]]
    print(f"[INFO] {len(symbols)} symbols selected (top {top_n} by volume)", flush=True)
    return symbols


async def fetch_klines(session, symbol, interval, start_ms, end_ms, limit=1500):
    all_klines = []
    cur_start = start_ms
    while cur_start < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": limit,
        }
        data = await api_get(session, "/fapi/v1/klines", params)
        if not data:
            break
        all_klines.extend(data)
        if len(data) < limit:
            break
        last_ts = int(data[-1][0])
        cur_start = last_ts + 1
    return all_klines


async def fetch_oi_history(session, symbol, start_ms, end_ms):
    all_data = []
    cur_start = start_ms
    while cur_start < end_ms:
        params = {
            "symbol": symbol,
            "period": "1h",
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": 500,
        }
        data = await api_get(session, "/futures/data/openInterestHist", params)
        if not data:
            break
        all_data.extend(data)
        if len(data) < 500:
            break
        last_ts = int(data[-1]["timestamp"])
        cur_start = last_ts + 1
    return all_data


async def fetch_funding_history(session, symbol, start_ms, end_ms):
    all_data = []
    cur_start = start_ms
    while cur_start < end_ms:
        params = {
            "symbol": symbol,
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": 1000,
        }
        data = await api_get(session, "/fapi/v1/fundingRate", params)
        if not data:
            break
        all_data.extend(data)
        if len(data) < 1000:
            break
        last_ts = int(data[-1]["fundingTime"])
        cur_start = last_ts + 1
    return all_data


async def fetch_ls_history(session, symbol, start_ms, end_ms):
    all_data = []
    cur_start = start_ms
    while cur_start < end_ms:
        params = {
            "symbol": symbol,
            "period": "1h",
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": 500,
        }
        data = await api_get(session, "/futures/data/globalLongShortAccountRatio", params)
        if not data:
            break
        all_data.extend(data)
        if len(data) < 500:
            break
        last_ts = int(data[-1]["timestamp"])
        cur_start = last_ts + 1
    return all_data


def klines_at(klines, ts_ms, count):
    result = [k for k in klines if int(k[0]) <= ts_ms]
    return result[-count:] if len(result) >= count else result


def calc_atr(klines_1h, period=14):
    if len(klines_1h) < period + 1:
        return 0.0
    trs = []
    for i in range(-period, 0):
        h = float(klines_1h[i][2])
        l = float(klines_1h[i][3])
        cp = float(klines_1h[i - 1][4])
        trs.append(max(h - l, abs(h - cp), abs(l - cp)))
    return statistics.mean(trs) if trs else 0.0


def detect_market_state(klines_1h, price):
    if len(klines_1h) < 24:
        return "CHOP"
    closes = [float(k[4]) for k in klines_1h[-24:]]
    highs = [float(k[2]) for k in klines_1h[-24:]]
    lows = [float(k[3]) for k in klines_1h[-24:]]
    ma8 = statistics.mean(closes[-8:])
    ma24 = statistics.mean(closes[-24:])
    recent_closes = closes[-4:]
    up_count = sum(1 for i in range(1, len(recent_closes)) if recent_closes[i] > recent_closes[i - 1])
    down_count = sum(1 for i in range(1, len(recent_closes)) if recent_closes[i] < recent_closes[i - 1])
    recent_range = (max(highs[-8:]) - min(lows[-8:])) / ma8 if ma8 > 0 else 0
    if recent_range > 0.20:
        return "VOLATILE"
    if ma8 > ma24 * 1.01 and up_count >= 3:
        return "TREND_UP"
    if ma8 < ma24 * 0.99 and down_count >= 3:
        return "TREND_DOWN"
    return "CHOP"


def calc_trend_group(klines_1h, price, market_state):
    if len(klines_1h) < 20:
        return 0, "LONG", {}, {"structure": 0, "momentum": 0}
    closes = [float(k[4]) for k in klines_1h]
    highs = [float(k[2]) for k in klines_1h]
    lows = [float(k[3]) for k in klines_1h]
    ma8 = statistics.mean(closes[-8:])
    ma20 = statistics.mean(closes[-20:])
    ma8_slope = (statistics.mean(closes[-4:]) - statistics.mean(closes[-8:-4])) / statistics.mean(closes[-8:-4]) * 100
    recent_high = max(highs[-8:])
    recent_low = min(lows[-8:])
    prev_high = max(highs[-16:-8])
    prev_low = min(lows[-16:-8])

    score = 0
    sub = {"structure": 0, "momentum": 0}
    factors = {}
    direction = "LONG"

    if market_state == "TREND_UP" or (market_state == "VOLATILE" and ma8 >= ma20):
        direction = "LONG"
        if ma8 > ma20:
            score += 30; sub["structure"] += 30; factors["trend_ma"] = "MA8>MA20"
        if ma8_slope > 0.3:
            score += 25; sub["momentum"] += 25; factors["ma_slope"] = f"{ma8_slope:.2f}%"
        if recent_high > prev_high * 1.01:
            score += 25; sub["structure"] += 25; factors["structure"] = "higher highs"
        if price > ma8:
            score += 20; sub["momentum"] += 20; factors["price_ma"] = "price above MA8"
    elif market_state == "TREND_DOWN" or (market_state == "VOLATILE" and ma8 < ma20):
        direction = "SHORT"
        if ma8 < ma20:
            score += 30; sub["structure"] += 30; factors["trend_ma"] = "MA8<MA20"
        if ma8_slope < -0.3:
            score += 25; sub["momentum"] += 25; factors["ma_slope"] = f"{ma8_slope:.2f}%"
        if recent_low < prev_low * 0.99:
            score += 25; sub["structure"] += 25; factors["structure"] = "lower lows"
        if price < ma8:
            score += 20; sub["momentum"] += 20; factors["price_ma"] = "price below MA8"
    else:
        return 0, "LONG", {}, {"structure": 0, "momentum": 0}

    factors["ma8"] = round(ma8, 6)
    factors["ma20"] = round(ma20, 6)
    return score, direction, factors, sub


def calc_oi_score_at_hour(oi_history, hour_ts_ms, price, thresholds):
    relevant = [d for d in oi_history if int(d["timestamp"]) <= hour_ts_ms]
    if len(relevant) < 2:
        return 0, {}, 0

    oi_values = []
    for d in relevant[-12:]:
        oi_coin = float(d.get("sumOpenInterest", 0))
        oi_usd = oi_coin * price
        oi_values.append(oi_usd)

    if len(oi_values) < 2:
        return 0, {}, 0

    oi_now = oi_values[-1]
    oi_prev_5 = oi_values[-6] if len(oi_values) >= 6 else oi_values[0]
    oi_prev_1 = oi_values[-2] if len(oi_values) >= 2 else oi_values[0]

    chg_5 = (oi_now - oi_prev_5) / oi_prev_5 if oi_prev_5 else 0
    chg_1 = (oi_now - oi_prev_1) / oi_prev_1 if oi_prev_1 else 0
    chg_12 = (oi_now - oi_values[0]) / oi_values[0] if oi_values[0] else 0
    oi_accel = chg_1 - (chg_12 / 12 if chg_12 else 0)

    factors = {}
    score = 0

    if oi_accel > thresholds["oi_surge_pct"] * 0.3 and chg_5 > thresholds["oi_surge_pct"] * 0.5:
        score = 40
        factors["oi_surge"] = "OI accelerating"
    elif abs(chg_5) >= thresholds["oi_surge_pct"]:
        score = 30
        factors["oi_surge"] = "OI spike"
    else:
        score = int(40 * min(abs(chg_5) / max(thresholds["oi_surge_pct"], 1e-9), 1.0))

    factors["oi_change_5m"] = f"{chg_5 * 100:.2f}%"
    factors["oi_accel"] = f"{oi_accel * 100:.3f}%"
    factors["oi_usd"] = f"${oi_now / 1e6:.1f}M"

    return score, factors, oi_now


def calc_funding_score_at_hour(funding_history, hour_ts_ms, thresholds):
    relevant = [d for d in funding_history if int(d["fundingTime"]) <= hour_ts_ms]
    if not relevant:
        return 0, {}, 0.0

    recent = relevant[-6:]
    fr = float(recent[-1]["fundingRate"])
    current_fr = fr

    fr_slope = 0.0
    if len(recent) >= 2:
        fr_slope = float(recent[-1]["fundingRate"]) - float(recent[0]["fundingRate"])

    factors = {}
    score = 0
    sub = {"funding": 0}

    factors["funding_rate"] = f"{fr * 100:.4f}%"
    factors["funding_slope"] = f"{fr_slope * 100:.4f}%"

    if fr > thresholds["funding_long"] or fr < thresholds["funding_short"]:
        score = 50
        sub["funding"] = 50
        factors["funding_extreme"] = "funding extreme"
    elif abs(fr_slope) > thresholds["funding_long"] * 0.5:
        score = 30
        sub["funding"] = 30
        factors["funding_extreme"] = "funding slope change"

    return score, factors, current_fr


def calc_ls_score_at_hour(ls_history, hour_ts_ms, thresholds):
    relevant = [d for d in ls_history if int(d["timestamp"]) <= hour_ts_ms]
    if not relevant:
        return 0, {}

    lsr = float(relevant[-1]["longShortRatio"])
    factors = {}
    score = 0
    sub = {"ls_ratio": 0}

    factors["ls_ratio"] = f"{lsr:.2f}"
    if lsr >= thresholds["ls_ratio_high"] or lsr <= thresholds["ls_ratio_low"]:
        score = 50
        sub["ls_ratio"] = 50
        factors["ls_ratio_signal"] = f"{lsr:.2f}"

    return score, factors


def calc_trigger_group(klines_5m, klines_15m, price_chg_24, thresholds,
                       oi_score, oi_factors, price):
    factors = {}
    scores = {}

    scores["oi"] = oi_score
    factors.update(oi_factors)

    m5_chg = 0.0
    if len(klines_5m) >= 3:
        open_3 = float(klines_5m[-3][1])
        close_0 = float(klines_5m[-1][4])
        m5_chg = (close_0 - open_3) / open_3 if open_3 else 0
        bar_chgs = [(float(k[4]) - float(k[1])) / float(k[1]) if float(k[1]) else 0 for k in klines_5m[-3:]]
        max_bar = max(abs(c) for c in bar_chgs)
        th_m5 = thresholds["momentum_breakout"] * 0.27
        if abs(m5_chg) >= th_m5 or max_bar >= th_m5 * 0.7:
            scores["momentum"] = 35
            factors["momentum_signal"] = f"{abs(m5_chg) * 100:.2f}%"
            factors["trigger_direction"] = "LONG" if m5_chg > 0 else "SHORT"
        else:
            scores["momentum"] = int(35 * min(abs(m5_chg) / max(th_m5, 1e-9), 1.0))
            factors["trigger_direction"] = "LONG" if m5_chg >= 0 else "SHORT"
        factors["m5_change"] = f"{m5_chg * 100:.2f}%"

        if len(klines_5m) >= 6:
            recent_vols = [float(k[5]) for k in klines_5m[-3:]]
            prev_vols = [float(k[5]) for k in klines_5m[-6:-3]]
            avg_recent = sum(recent_vols) / len(recent_vols) if recent_vols else 1
            avg_prev = sum(prev_vols) / len(prev_vols) if prev_vols else 1
            vol_ratio_val = avg_recent / avg_prev if avg_prev > 0 else 1.0
            factors["vol_ratio"] = f"{vol_ratio_val:.1f}x"
            th_vol = thresholds["volume_spike_ratio"]
            if vol_ratio_val >= th_vol:
                scores["volume"] = 15
                factors["volume_spike"] = f"volume spike {vol_ratio_val:.1f}x"
            else:
                scores["volume"] = int(15 * min(vol_ratio_val / max(th_vol, 1e-9), 1.0))

    if len(klines_15m) >= 8:
        tol = thresholds["retest_tolerance"]
        timeout_bars = max(1, thresholds["retest_timeout"] // 15)
        restart_th = thresholds["m5_restart"]

        swing_highs = [float(k[2]) for k in klines_15m[:-1]]
        swing_lows = [float(k[3]) for k in klines_15m[:-1]]
        prev_swing_high = max(swing_highs[-5:-1]) if len(swing_highs) >= 5 else max(swing_highs)
        prev_swing_low = min(swing_lows[-5:-1]) if len(swing_lows) >= 5 else min(swing_lows)

        breakout_long = False
        breakout_short = False
        breakout_bar_idx = -1

        for i in range(max(1, len(klines_15m) - timeout_bars - 1), len(klines_15m)):
            c_prev = float(klines_15m[i-1][4])  # 用前一K线close判断，避免未来函数
            if c_prev > prev_swing_high and not breakout_long:
                breakout_long = True
                breakout_bar_idx = i
            elif c_prev < prev_swing_low and not breakout_short:
                breakout_short = True
                breakout_bar_idx = i

        def _eval_breakout(dir_str, is_long):
            nonlocal breakout_bar_idx
            deep_retest = shallow_hold = fake_break = False
            has_post = (breakout_bar_idx + 1) < len(klines_15m)

            if has_post:
                if is_long:
                    post_lows = [float(klines_15m[j][3]) for j in range(breakout_bar_idx + 1, len(klines_15m))]
                    mpl = min(post_lows)
                    if mpl < prev_swing_high * (1 - tol):
                        fake_break = True
                    elif mpl < prev_swing_high:
                        deep_retest = True
                    else:
                        shallow_hold = True
                else:
                    post_highs = [float(klines_15m[j][2]) for j in range(breakout_bar_idx + 1, len(klines_15m))]
                    mph = max(post_highs)
                    if mph > prev_swing_low * (1 + tol):
                        fake_break = True
                    elif mph > prev_swing_low:
                        deep_retest = True
                    else:
                        shallow_hold = True

            restart_ok = False
            if (deep_retest or shallow_hold) and len(klines_5m) >= 6:
                if is_long:
                    pbl = min(float(k[3]) for k in klines_5m[-6:])
                    lc = float(klines_5m[-1][4])
                    rp = (lc - pbl) / pbl if pbl else 0
                else:
                    pbh = max(float(k[2]) for k in klines_5m[-6:])
                    lc = float(klines_5m[-1][4])
                    rp = (pbh - lc) / pbh if pbh else 0
                if rp >= restart_th:
                    restart_ok = True

            prefix = "breakout" if is_long else "breakdown"
            if fake_break:
                scores["structure"] = 0
                factors["structure_trigger"] = f"15m fake {prefix}"
                factors["trigger_direction"] = dir_str
            elif deep_retest and restart_ok:
                scores["structure"] = 25
                factors["structure_trigger"] = f"15m {prefix}+retest+5m restart"
                factors["trigger_direction"] = dir_str
            elif shallow_hold:
                scores["structure"] = 22
                factors["structure_trigger"] = f"15m {prefix}+strong hold"
                factors["trigger_direction"] = dir_str
            elif deep_retest:
                scores["structure"] = 15
                factors["structure_trigger"] = f"15m {prefix}+retest(wait restart)"
                factors["trigger_direction"] = dir_str
            else:
                scores["structure"] = 10
                factors["structure_trigger"] = f"15m {prefix}(pending)"
                factors["trigger_direction"] = dir_str

        if breakout_long:
            _eval_breakout("LONG", True)
        elif breakout_short:
            _eval_breakout("SHORT", False)

    scores["ob"] = 0
    factors["ob_ratio"] = "N/A"
    factors["ob_imbalance"] = "N/A (historical)"

    total = min(100, scores.get("oi", 0) + scores.get("momentum", 0) + scores.get("structure", 0) + scores.get("ob", 0))
    return total, factors, scores


def get_market_bias(btc_klines):
    if not btc_klines or len(btc_klines) < 24:
        return "NEUTRAL"
    closes = [float(k[4]) for k in btc_klines]
    ma8 = statistics.mean(closes[-8:])
    ma24 = statistics.mean(closes)
    if ma8 > ma24 * 1.005:
        return "BULL"
    elif ma8 < ma24 * 0.995:
        return "BEAR"
    return "NEUTRAL"


def get_btc_atr_pct(btc_klines):
    if not btc_klines or len(btc_klines) < 15:
        return 0.0
    atr = calc_atr(btc_klines, period=14)
    price = float(btc_klines[-1][4])
    return atr / price * 100 if price > 0 else 0.0


def generate_signal_at_hour(symbol, klines_1h_all, klines_5m_all, klines_15m_all,
                            oi_history, funding_history, ls_history,
                            hour_ts, btc_klines, btc_atr_pct, market_bias):
    klines_1h = klines_at(klines_1h_all, hour_ts, 48)
    klines_5m = klines_at(klines_5m_all, hour_ts, 20)
    klines_15m = klines_at(klines_15m_all, hour_ts, 12)

    if len(klines_1h) < 24:
        return None

    price = float(klines_1h[-1][4])

    closes_24 = [float(k[4]) for k in klines_1h[-24:]]
    price_chg_24 = (closes_24[-1] - closes_24[0]) / closes_24[0] * 100 if closes_24[0] else 0
    vol_24h = sum(float(k[5]) * float(k[4]) for k in klines_1h[-24:]) if len(klines_1h) >= 24 else 0

    market_state = detect_market_state(klines_1h, price)
    if market_state == "CHOP":
        return None
    if market_state == "VOLATILE" and not cfg.ALLOW_VOLATILE_ENTRY:
        return None

    trend_score, direction, trend_factors, trend_sub = calc_trend_group(klines_1h, price, market_state)
    if trend_score < 40:
        return None

    oi_score, oi_factors, oi_usd = calc_oi_score_at_hour(oi_history, hour_ts, price, THRESHOLDS)
    if oi_usd > 0 and oi_usd < cfg.MIN_OI_USD_M * 1_000_000:
        return None

    trigger_score, trigger_factors, trigger_sub = calc_trigger_group(
        klines_5m, klines_15m, price_chg_24, THRESHOLDS,
        oi_score, oi_factors, price
    )
    if trigger_score < HIST_MIN_TRIGGER:
        return None

    trigger_dir = trigger_factors.get("trigger_direction", direction)
    if trigger_dir != direction:
        return None

    funding_score, funding_factors, current_fr = calc_funding_score_at_hour(
        funding_history, hour_ts, THRESHOLDS
    )

    ls_score, ls_factors = calc_ls_score_at_hour(ls_history, hour_ts, THRESHOLDS)

    if cfg.FUNDING_DIR_FILTER:
        if direction == "LONG" and current_fr < 0:
            return None
        if direction == "SHORT" and current_fr > 0:
            return None

    w = WEIGHTS
    oi_raw = trigger_sub.get("oi", 0)
    ob_raw = trigger_sub.get("ob", 0)
    trigger_momentum = trigger_sub.get("momentum", 0)
    trigger_structure = trigger_sub.get("structure", 0)
    vol_raw = trigger_sub.get("volume", 0)
    funding_raw = funding_score
    ls_raw = ls_score

    trend_structure = trend_sub.get("structure", 0)
    trend_momentum = trend_sub.get("momentum", 0)

    squeeze_raw = trend_structure + trigger_structure
    momentum_raw = trend_momentum + trigger_momentum

    score_parts = {
        "oi_surge":          min(w["oi_surge"],          int(oi_raw / 45 * w["oi_surge"])),
        "ob_imbalance":      min(w["ob_imbalance"],      int(ob_raw / 25 * w["ob_imbalance"])),
        "volume_spike":      min(w["volume_spike"],      vol_raw),
        "funding_extreme":   min(w["funding_extreme"],   int(funding_raw / 50 * w["funding_extreme"])),
        "price_squeeze":     min(w["price_squeeze"],     int(squeeze_raw / 80 * w["price_squeeze"])),
        "ls_ratio":          min(w["ls_ratio"],          int(ls_raw / 50 * w["ls_ratio"])),
        "basis_diverge":     0,
        "momentum_breakout": min(w["momentum_breakout"], int(momentum_raw / 80 * w["momentum_breakout"])),
    }

    total_score = sum(score_parts.values())

    certainty_parts = {
        "oi_surge": score_parts["oi_surge"],
        "ob_imbalance": score_parts["ob_imbalance"],
        "funding_extreme": score_parts["funding_extreme"],
        "ls_ratio": score_parts["ls_ratio"],
    }
    opportunity_parts = {
        "volume_spike": score_parts["volume_spike"],
        "price_squeeze": score_parts["price_squeeze"],
        "momentum_breakout": score_parts["momentum_breakout"],
        "basis_diverge": 0,
    }
    certainty_score = sum(certainty_parts.values())
    opportunity_score = sum(opportunity_parts.values())

    if market_bias == "BEAR" and direction == "LONG":
        if cfg.STRICT_MARKET_BIAS:
            return None
        total_score = int(total_score * 0.7)
    elif market_bias == "BULL" and direction == "SHORT":
        if cfg.STRICT_MARKET_BIAS:
            return None
        total_score = int(total_score * 0.7)

    exp_filters = getattr(cfg, 'EXPERIMENTAL_FILTERS_ENABLED', True)
    if exp_filters and btc_atr_pct > cfg.BTC_ATR_VOL_THRESHOLD:
        total_score = int(total_score * 0.8)

    dt_utc = datetime.fromtimestamp(hour_ts / 1000, tz=timezone.utc)
    now_utc_hour = dt_utc.hour
    preferred = cfg.PREFERRED_HOURS_LIST
    if exp_filters and now_utc_hour not in preferred:
        discount = cfg.OFF_HOUR_SCORE_DISCOUNT
        total_score = int(total_score * discount)

    if total_score < HIST_MIN_SCORE:
        return None

    if direction == "LONG" and price_chg_24 > cfg.MAX_24H_GAIN_LONG:
        return None
    if direction == "SHORT" and price_chg_24 < -cfg.MAX_24H_LOSS_SHORT:
        return None

    atr = calc_atr(klines_1h, period=14)

    all_factors = {}
    all_factors.update(trend_factors)
    all_factors.update(trigger_factors)
    all_factors.update(funding_factors)
    all_factors.update(ls_factors)
    all_factors["market_state"] = market_state
    all_factors["market_bias"] = market_bias
    all_factors["btc_atr_pct"] = f"{btc_atr_pct:.2f}%"
    all_factors["atr"] = atr
    all_factors["atr_pct"] = f"{atr / price * 100:.3f}%" if price > 0 else "0%"

    return {
        "symbol": symbol,
        "score": total_score,
        "certainty_score": certainty_score,
        "opportunity_score": opportunity_score,
        "trend_score": trend_score,
        "trigger_score": trigger_score,
        "emotion_score": funding_score + ls_score,
        "market_state": market_state,
        "direction": direction,
        "price": price,
        "atr": atr,
        "price_change_24h": f"{price_chg_24:.2f}%",
        "volume_24h_usd": f"${vol_24h / 1e6:.1f}M",
        "factors": all_factors,
        "score_parts": score_parts,
        "timestamp": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "observed_at": int(hour_ts / 1000),
        "source": "historical_generator_v3",
    }


PROGRESS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                              "data", "hist_gen_progress.json")


def save_progress(done_symbols, all_signals, output_path):
    tmp = output_path + ".tmp"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        for sig in all_signals:
            f.write(json.dumps(sig, ensure_ascii=False) + "\n")
    if os.path.exists(output_path):
        os.remove(output_path)
    os.rename(tmp, output_path)

    prog = {"done_symbols": list(done_symbols), "output": output_path}
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(prog, f)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return None


async def main():
    parser = argparse.ArgumentParser(description="Historical Signal Generator v3")
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    parser.add_argument("--top", type=int, default=50, help="Top N symbols by volume")
    parser.add_argument("--output", type=str, default="data/historical_signals.jsonl")
    parser.add_argument("--resume", action="store_true", help="Resume from last progress")
    parser.add_argument("--min-score", type=int, default=50, help="Min score threshold")
    parser.add_argument("--no-oi", action="store_true", help="Skip OI history (faster but less accurate)")
    parser.add_argument("--no-funding", action="store_true", help="Skip funding history")
    parser.add_argument("--no-ls", action="store_true", help="Skip LS ratio history")
    parser.add_argument("--symbols", type=str, default=None,
                        help="Comma-separated symbol list (e.g. BOMEUSDT,ORDIUSDT). "
                             "If set, --top is ignored")
    args = parser.parse_args()

    global HIST_MIN_SCORE, HIST_MIN_TRIGGER
    HIST_MIN_SCORE = args.min_score
    HIST_MIN_TRIGGER = max(10, args.min_score // 2)

    now = datetime.now(timezone.utc)
    end_hour = ts_to_hour(int(now.timestamp() * 1000))
    start_hour = end_hour - timedelta(days=args.days)

    hours = []
    cur = start_hour
    while cur < end_hour:
        hours.append(cur)
        cur += timedelta(hours=1)

    data_sources = []
    if not args.no_oi:
        data_sources.append("OI")
    if not args.no_funding:
        data_sources.append("Funding")
    if not args.no_ls:
        data_sources.append("LS-ratio")

    print(f"{'='*60}", flush=True)
    print(f"[Historical Signal Generator v3]", flush=True)
    print(f"  Period: {start_hour.strftime('%Y-%m-%d %H:%M')} -> {end_hour.strftime('%Y-%m-%d %H:%M')}", flush=True)
    print(f"  Hours: {len(hours)}", flush=True)
    print(f"  Top symbols: {args.top if not args.symbols else 'custom list'}", flush=True)
    print(f"  Output: {args.output}", flush=True)
    print(f"  Rate limit: {DELAY_BETWEEN_CALLS}s/call, {DELAY_BETWEEN_SYMBOLS}s/symbol", flush=True)
    print(f"  Data sources: klines(1h/5m/15m) + BTC ref + {', '.join(data_sources)}", flush=True)
    print(f"  Missing: OB (order book) - no historical data available", flush=True)
    print(f"  Min score: {HIST_MIN_SCORE}", flush=True)
    print(f"{'='*60}", flush=True)

    output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.output)

    done_symbols = set()
    all_signals = []

    if args.resume:
        prog = load_progress()
        if prog:
            done_symbols = set(prog.get("done_symbols", []))
            print(f"[RESUME] {len(done_symbols)} symbols already done", flush=True)
            if os.path.exists(output_path):
                with open(output_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            all_signals.append(json.loads(line))
                print(f"[RESUME] Loaded {len(all_signals)} existing signals", flush=True)

    connector = aiohttp.TCPConnector(ssl=False, limit=3)
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=connector,
    ) as session:

        if args.symbols:
            symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
            symbols = [s for s in symbols if s.endswith("USDT")]
            print(f"[INFO] {len(symbols)} custom symbols provided", flush=True)
        else:
            symbols = await fetch_symbols(session, top_n=args.top)
        remaining = [s for s in symbols if s not in done_symbols]
        print(f"[INFO] {len(remaining)} symbols remaining to process", flush=True)

        print(f"\n[STEP 1] Fetching BTC reference data...", flush=True)
        btc_start = hour_to_ts(start_hour) - 48 * 3600 * 1000
        btc_end = hour_to_ts(end_hour) + 3600 * 1000
        btc_klines_raw = await fetch_klines(session, "BTCUSDT", "1h", btc_start, btc_end)

        btc_klines_map = {}
        btc_atr_map = {}
        market_bias_map = {}
        for h in hours:
            h_ts = hour_to_ts(h)
            btc_k = klines_at(btc_klines_raw, h_ts, 48)
            btc_klines_map[h_ts] = btc_k
            btc_atr_map[h_ts] = get_btc_atr_pct(btc_k)
            market_bias_map[h_ts] = get_market_bias(btc_k)
        print(f"[OK] BTC reference data loaded for {len(hours)} hours", flush=True)

        print(f"\n[STEP 2] Processing {len(remaining)} symbols sequentially...", flush=True)
        for idx, sym in enumerate(remaining):
            print(f"\n  [{idx+1}/{len(remaining)}] {sym}...", end=" ", flush=True)

            try:
                klines_1h = await fetch_klines(session, sym, "1h",
                                               hour_to_ts(start_hour) - 48 * 3600 * 1000,
                                               hour_to_ts(end_hour) + 3600 * 1000)
                if not klines_1h or len(klines_1h) < 24:
                    print("SKIP (no 1h data)", flush=True)
                    done_symbols.add(sym)
                    continue

                klines_5m = await fetch_klines(session, sym, "5m",
                                               hour_to_ts(start_hour) - 2 * 3600 * 1000,
                                               hour_to_ts(end_hour) + 3600 * 1000)
                klines_15m = await fetch_klines(session, sym, "15m",
                                                hour_to_ts(start_hour) - 6 * 3600 * 1000,
                                                hour_to_ts(end_hour) + 3600 * 1000)

                oi_history = []
                if not args.no_oi:
                    oi_history = await fetch_oi_history(
                        session, sym,
                        hour_to_ts(start_hour) - 12 * 3600 * 1000,
                        hour_to_ts(end_hour) + 3600 * 1000
                    )

                funding_history = []
                if not args.no_funding:
                    funding_history = await fetch_funding_history(
                        session, sym,
                        hour_to_ts(start_hour) - 48 * 3600 * 1000,
                        hour_to_ts(end_hour) + 3600 * 1000
                    )

                ls_history = []
                if not args.no_ls:
                    ls_history = await fetch_ls_history(
                        session, sym,
                        hour_to_ts(start_hour) - 6 * 3600 * 1000,
                        hour_to_ts(end_hour) + 3600 * 1000
                    )

                sym_signals = []
                cooldown_until = 0

                for h in hours:
                    h_ts = hour_to_ts(h)
                    if h_ts < cooldown_until:
                        continue

                    sig = generate_signal_at_hour(
                        sym, klines_1h, klines_5m, klines_15m,
                        oi_history, funding_history, ls_history,
                        h_ts, btc_klines_map[h_ts], btc_atr_map[h_ts], market_bias_map[h_ts]
                    )
                    if sig is not None:
                        cooldown_until = h_ts + cfg.COOLDOWN_SECONDS * 1000
                        sym_signals.append(sig)

                if sym_signals:
                    all_signals.extend(sym_signals)
                    print(f"{len(sym_signals)} signals", flush=True)
                else:
                    print("0 signals", flush=True)

                done_symbols.add(sym)

                if (idx + 1) % 5 == 0:
                    save_progress(done_symbols, all_signals, output_path)
                    print(f"  [SAVE] Progress saved ({len(all_signals)} total signals)", flush=True)

            except Exception as e:
                print(f"ERROR: {e}", flush=True)
                continue

            await asyncio.sleep(DELAY_BETWEEN_SYMBOLS)

    all_signals.sort(key=lambda x: x["timestamp"])

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for sig in all_signals:
            f.write(json.dumps(sig, ensure_ascii=False) + "\n")

    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    print(f"\n{'='*60}", flush=True)
    print(f"[DONE] Generated {len(all_signals)} signals over {args.days} days", flush=True)
    print(f"[DONE] Saved to {output_path}", flush=True)

    if all_signals:
        score_dist = defaultdict(int)
        dir_dist = defaultdict(int)
        for sig in all_signals:
            bucket = sig["score"] // 10 * 10
            score_dist[bucket] += 1
            dir_dist[sig["direction"]] += 1
        print(f"\n[Score Distribution]", flush=True)
        for k in sorted(score_dist.keys()):
            bar = "#" * (score_dist[k] // 2)
            print(f"  {k:3d}-{k+9:3d}: {score_dist[k]:4d} {bar}", flush=True)
        print(f"\n[Direction Distribution]", flush=True)
        for k, v in sorted(dir_dist.items()):
            print(f"  {k}: {v}", flush=True)

        dates = defaultdict(int)
        for sig in all_signals:
            dates[sig["timestamp"][:10]] += 1
        print(f"\n[Daily Signal Count]", flush=True)
        for k in sorted(dates.keys()):
            bar = "#" * min(dates[k], 50)
            print(f"  {k}: {dates[k]:3d} {bar}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
