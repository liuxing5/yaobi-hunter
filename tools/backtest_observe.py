"""
观察信号回测脚本
用法: python -m tools.backtest_observe [--days 1] [--hours 4] [--source observe|signals]

读取 data/observe_signals_*.jsonl 或 data/signals.jsonl，
对每个信号查询信号后N小时内的最高/最低价，
按 score/ATR/时段/structure_trigger 等多维度分桶统计胜率和平均收益，
验证各过滤器的实际价值。
"""
import json
import time
import asyncio
import argparse
import sys
import os
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scanner import BinanceClient
from config import Config


async def fetch_klines_after(client: BinanceClient, symbol: str, start_ts: int, hours: int = 4):
    start_ms = start_ts * 1000
    end_ms = start_ms + hours * 3600 * 1000
    try:
        data = await client.get("/fapi/v1/klines", {
            "symbol": symbol,
            "interval": "5m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": hours * 12,
        })
        if isinstance(data, Exception) or not data:
            return None
        return [(float(k[2]), float(k[3]), float(k[4])) for k in data]
    except Exception as e:
        err_str = str(e)
        if "429" in err_str or "1003" in err_str:
            print(f"  rate-limited, waiting 10s...")
            await asyncio.sleep(10)
        return None


def calc_max_pnl(direction: str, entry: float, highs_lows_closes):
    if not highs_lows_closes:
        return None, None
    max_favorable = 0.0
    max_adverse = 0.0
    for high, low, close in highs_lows_closes:
        if direction == "LONG":
            favorable = (high - entry) / entry * 100
            adverse = (low - entry) / entry * 100
        else:
            favorable = (entry - low) / entry * 100
            adverse = (entry - high) / entry * 100
        max_favorable = max(max_favorable, favorable)
        max_adverse = min(max_adverse, adverse)
    return max_favorable, max_adverse


def calc_sl_tp_hit(direction: str, entry: float, atr: float, highs_lows_closes, sl_mult=2.0, tp_mult=2.5):
    if not highs_lows_closes or atr <= 0:
        return False, False, None, None
    if direction == "LONG":
        sl_px = entry - atr * sl_mult
        tp_px = entry + atr * tp_mult
    else:
        sl_px = entry + atr * sl_mult
        tp_px = entry - atr * tp_mult

    hit_sl = False
    hit_tp = False
    time_to_sl = None
    time_to_tp = None

    for i, (high, low, close) in enumerate(highs_lows_closes):
        elapsed_min = (i + 1) * 5
        if not hit_sl:
            if direction == "LONG" and low <= sl_px:
                hit_sl = True
                time_to_sl = elapsed_min
            elif direction == "SHORT" and high >= sl_px:
                hit_sl = True
                time_to_sl = elapsed_min
        if not hit_tp:
            if direction == "LONG" and high >= tp_px:
                hit_tp = True
                time_to_tp = elapsed_min
            elif direction == "SHORT" and low <= tp_px:
                hit_tp = True
                time_to_tp = elapsed_min
        if hit_sl and hit_tp:
            break

    return hit_sl, hit_tp, time_to_sl, time_to_tp


def score_bucket(score: int) -> str:
    if score >= 85:
        return "85+"
    if score >= 80:
        return "80-84"
    if score >= 75:
        return "75-79"
    if score >= 70:
        return "70-74"
    if score >= 65:
        return "65-69"
    if score >= 60:
        return "60-64"
    if score >= 40:
        return "40-59"
    if score >= 20:
        return "20-39"
    return "<20"


def atr_bucket(atr_pct: float) -> str:
    if atr_pct <= 0:
        return "N/A"
    if atr_pct < 1:
        return "<1%"
    if atr_pct < 2:
        return "1-2%"
    if atr_pct < 3:
        return "2-3%"
    if atr_pct < 4:
        return "3-4%"
    if atr_pct < 5:
        return "4-5%"
    return "5%+"


def trigger_bucket(trigger: str) -> str:
    t = (trigger or "").lower()
    if "breakout" in t:
        return "breakout"
    if "breakdown" in t:
        return "breakdown"
    if "strong" in t:
        return "strong_hold"
    if "pending" in t:
        return "pending"
    return "NONE"


def fa_bucket(fa: float) -> str:
    if fa >= 2.0:
        return "F/A>2.0(great)"
    if fa >= 1.0:
        return "F/A 1.0-2.0(ok)"
    if fa >= 0.5:
        return "F/A 0.5-1.0(bad)"
    return "F/A<0.5(terrible)"


def extract_signal_fields(s: dict) -> dict:
    factors = s.get("factors", {})
    score_parts = s.get("score_parts", {})

    fr_str = factors.get("funding_rate", "0%")
    try:
        fr_val = float(fr_str.replace("%", "")) / 100
    except (ValueError, AttributeError):
        fr_val = 0.0

    atr = s.get("atr")
    price = float(s.get("price", 0))
    if atr is None:
        atr_pct_str = factors.get("atr_pct", "0%")
        try:
            atr_pct_val = float(atr_pct_str.replace("%", "")) / 100
        except (ValueError, AttributeError):
            atr_pct_val = 0.0
        atr = price * atr_pct_val
    atr = float(atr) if atr else 0
    atr_pct = atr / price * 100 if atr and price > 0 else 0

    structure_trigger = factors.get("structure_trigger", "")

    ts = s.get("timestamp", "")
    observed_at = s.get("observed_at", 0)
    if observed_at > 0:
        utc_hour = datetime.fromtimestamp(observed_at, tz=timezone.utc).hour
    elif ts:
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            utc_hour = dt.hour
        except (ValueError, TypeError):
            utc_hour = -1
    else:
        utc_hour = -1

    return {
        "symbol": s.get("symbol", ""),
        "direction": s.get("direction", "LONG"),
        "score": s.get("score", 0),
        "price": price,
        "atr": atr,
        "atr_pct": atr_pct,
        "observed_at": observed_at,
        "timestamp": ts,
        "utc_hour": utc_hour,
        "funding_rate": fr_val,
        "structure_trigger": structure_trigger,
        "oi_surge": score_parts.get("oi_surge", 0),
        "ob_imbalance": score_parts.get("ob_imbalance", 0),
        "momentum_breakout": score_parts.get("momentum_breakout", 0),
        "volume_spike": score_parts.get("volume_spike", 0),
        "funding_extreme": score_parts.get("funding_extreme", 0),
        "market_state": s.get("market_state", factors.get("market_state", "")),
    }


def analyze_group(name: str, key_fn, records):
    groups = defaultdict(list)
    for r in records:
        groups[key_fn(r)].append(r)

    print(f"\n{'='*100}")
    print(f"  {name}")
    print(f"{'='*100}")
    print(f"  {'Group':<25} {'Count':>5} {'WR(>2%)':>8} {'AvgFav':>8} {'AvgAdv':>8} {'AvgF/A':>8} {'SL%':>6} {'TP%':>6} {'NetEdge':>8}")
    print(f"  {'-'*25} {'-'*5} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*6} {'-'*6} {'-'*8}")

    for gk in sorted(groups.keys(), key=lambda x: str(x)):
        items = groups[gk]
        n = len(items)
        tp_count = sum(1 for r in items if r.get("hit_tp"))
        sl_count = sum(1 for r in items if r.get("hit_sl"))
        win_count = sum(1 for r in items if r["max_fav"] > 2.0)
        avg_fav = sum(r["max_fav"] for r in items) / n
        avg_adv = sum(r["max_adv"] for r in items) / n
        avg_fa = sum(r["fav_adv_ratio"] for r in items) / n
        wr = win_count / n * 100 if n > 0 else 0
        sl_pct = sl_count / n * 100 if n > 0 else 0
        tp_pct = tp_count / n * 100 if n > 0 else 0
        net_edge = avg_fav - avg_adv
        print(f"  {str(gk):<25} {n:>5} {wr:>7.0f}% {avg_fav:>7.2f}% {avg_adv:>7.2f}% {avg_fa:>7.2f} {sl_pct:>5.0f}% {tp_pct:>5.0f}% {net_edge:>+7.2f}%")


async def evaluate(days: int = 1, hours: int = 4, source: str = "observe"):
    config = Config()
    proxy = os.getenv("SCANNER_PROXY", "")
    client = BinanceClient(config.BINANCE_API_KEY, config.BINANCE_SECRET, proxy)

    try:
        data_dir = Path(__file__).resolve().parent.parent / "data"
        raw_signals = []

        if source == "signals":
            signals_file = data_dir / "signals.jsonl"
            if signals_file.exists():
                with open(signals_file, encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            raw_signals.append(json.loads(line.strip()))
                        except Exception:
                            pass
                print(f"Loaded {len(raw_signals)} signals from signals.jsonl")
            else:
                print(f"signals.jsonl not found")
                return
        elif source == "historical":
            signals_file = data_dir / "historical_signals.jsonl"
            if signals_file.exists():
                with open(signals_file, encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            raw_signals.append(json.loads(line.strip()))
                        except Exception:
                            pass
                print(f"Loaded {len(raw_signals)} signals from historical_signals.jsonl")
            else:
                print(f"historical_signals.jsonl not found")
                return
        else:
            for f in sorted(data_dir.glob("observe_signals_*.jsonl")):
                with open(f, encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            raw_signals.append(json.loads(line.strip()))
                        except Exception:
                            pass
            print(f"Loaded {len(raw_signals)} observe signals")

        if source == "observe":
            cutoff = time.time() - days * 86400
            raw_signals = [s for s in raw_signals if cutoff <= s.get("observed_at", 0) < time.time() - hours * 3600]
            print(f"After time filter: {len(raw_signals)} signals (last {days}d, >= {hours}h ago)")

        if not raw_signals:
            print("No data, exiting")
            return

        records = []
        for i, s in enumerate(raw_signals):
            fields = extract_signal_fields(s)
            sym = fields["symbol"]
            direction = fields["direction"]
            entry = fields["price"]
            atr = fields["atr"]
            observed_at = fields["observed_at"]

            if not sym or entry <= 0:
                continue

            if observed_at > 0:
                start_ts = int(observed_at)
            else:
                ts = fields.get("timestamp", "")
                try:
                    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                    start_ts = int(dt.timestamp())
                except (ValueError, TypeError):
                    continue

            print(f"  [{i+1}/{len(raw_signals)}] {sym} {direction} score={fields['score']} atr%={fields['atr_pct']:.1f} ...", end="", flush=True)

            klines = await fetch_klines_after(client, sym, start_ts, hours)
            if klines is None:
                print(" no-data")
                await asyncio.sleep(0.15)
                continue

            max_fav, max_adv = calc_max_pnl(direction, entry, klines)
            if max_fav is None:
                print(" calc-fail")
                continue

            hit_sl, hit_tp, t_sl, t_tp = calc_sl_tp_hit(direction, entry, atr, klines)

            fa = max_fav / abs(max_adv) if max_adv != 0 else 999.0

            record = {
                **fields,
                "max_fav": max_fav,
                "max_adv": max_adv,
                "fav_adv_ratio": fa,
                "hit_sl": hit_sl,
                "hit_tp": hit_tp,
                "time_to_sl": t_sl,
                "time_to_tp": t_tp,
            }
            records.append(record)

            sl_s = f"SL@{t_sl}min" if hit_sl else "no-SL"
            tp_s = f"TP@{t_tp}min" if hit_tp else "no-TP"
            print(f" FAV={max_fav:.2f}% ADV={max_adv:.2f}% F/A={fa:.2f} {sl_s} {tp_s}")

            await asyncio.sleep(0.15)

        if not records:
            print("No valid records")
            return

        print(f"\n{'='*120}")
        print("BACKTEST OBSERVE - CATEGORY ANALYSIS")
        print(f"{'='*120}")

        analyze_group("BY SCORE TIER", lambda r: score_bucket(r["score"]), records)

        analyze_group("BY ATR% TIER", lambda r: atr_bucket(r["atr_pct"]), records)

        analyze_group("BY UTC HOUR", lambda r: f"UTC{r['utc_hour']:02d}h" if r["utc_hour"] >= 0 else "N/A", records)

        analyze_group("BY DIRECTION", lambda r: r["direction"], records)

        analyze_group("BY STRUCTURE TRIGGER", lambda r: trigger_bucket(r.get("structure_trigger", "")), records)

        analyze_group("BY OI_SURGE SCORE", lambda r: (
            "25" if r.get("oi_surge", 0) >= 25 else
            "17-24" if r.get("oi_surge", 0) >= 17 else
            "13-16" if r.get("oi_surge", 0) >= 13 else
            "<13"
        ), records)

        analyze_group("BY OB_IMBALANCE SCORE", lambda r: (
            "20" if r.get("ob_imbalance", 0) >= 20 else
            "14-19" if r.get("ob_imbalance", 0) >= 14 else
            "10-13" if r.get("ob_imbalance", 0) >= 10 else
            "<10"
        ), records)

        analyze_group("BY FAV/ADV RATIO", lambda r: fa_bucket(r["fav_adv_ratio"]), records)

        analyze_group("BY SCORE+ATR COMBO", lambda r: (
            "HighSc+HighATR" if r["score"] >= 75 and r["atr_pct"] >= 3 else
            "HighSc+LowATR" if r["score"] >= 75 and r["atr_pct"] < 3 else
            "LowSc+HighATR" if r["score"] < 75 and r["atr_pct"] >= 3 else
            "LowSc+LowATR"
        ), records)

        print(f"\n{'='*120}")
        print("INDIVIDUAL SIGNALS RANKED BY FAV/ADV RATIO (top 20 best, bottom 20 worst)")
        print(f"{'='*120}")

        sorted_recs = sorted(records, key=lambda r: r["fav_adv_ratio"], reverse=True)

        print(f"\n  TOP 20 (best F/A ratio):")
        print(f"  {'#':>3} {'Symbol':<14} {'Dir':<6} {'Sc':>3} {'ATR%':>6} {'Hour':>5} {'MAX_FAV':>8} {'MAX_ADV':>8} {'F/A':>6} {'SL':>5} {'TP':>5} {'Trigger':<20}")
        print(f"  {'-'*3} {'-'*14} {'-'*6} {'-'*3} {'-'*6} {'-'*5} {'-'*8} {'-'*8} {'-'*6} {'-'*5} {'-'*5} {'-'*20}")
        for j, r in enumerate(sorted_recs[:20]):
            sl_s = "Y" if r["hit_sl"] else "-"
            tp_s = "Y" if r["hit_tp"] else "-"
            print(f"  {j+1:>3} {r['symbol'].replace('USDT','/USDT'):<14} {r['direction']:<6} {r['score']:>3} {r['atr_pct']:>5.1f}% {r['utc_hour']:>5} {r['max_fav']:>7.2f}% {r['max_adv']:>7.2f}% {r['fav_adv_ratio']:>5.2f} {sl_s:>5} {tp_s:>5} {trigger_bucket(r.get('structure_trigger','')):<20}")

        print(f"\n  BOTTOM 20 (worst F/A ratio):")
        print(f"  {'#':>3} {'Symbol':<14} {'Dir':<6} {'Sc':>3} {'ATR%':>6} {'Hour':>5} {'MAX_FAV':>8} {'MAX_ADV':>8} {'F/A':>6} {'SL':>5} {'TP':>5} {'Trigger':<20}")
        print(f"  {'-'*3} {'-'*14} {'-'*6} {'-'*3} {'-'*6} {'-'*5} {'-'*8} {'-'*8} {'-'*6} {'-'*5} {'-'*5} {'-'*20}")
        for j, r in enumerate(sorted_recs[-20:]):
            sl_s = "Y" if r["hit_sl"] else "-"
            tp_s = "Y" if r["hit_tp"] else "-"
            print(f"  {j+1:>3} {r['symbol'].replace('USDT','/USDT'):<14} {r['direction']:<6} {r['score']:>3} {r['atr_pct']:>5.1f}% {r['utc_hour']:>5} {r['max_fav']:>7.2f}% {r['max_adv']:>7.2f}% {r['fav_adv_ratio']:>5.2f} {sl_s:>5} {tp_s:>5} {trigger_bucket(r.get('structure_trigger','')):<20}")

        print(f"\n{'='*120}")
        print("SUMMARY STATS")
        print(f"{'='*120}")
        n = len(records)
        avg_fav = sum(r["max_fav"] for r in records) / n
        avg_adv = sum(r["max_adv"] for r in records) / n
        tp_pct = sum(1 for r in records if r["hit_tp"]) / n * 100
        sl_pct = sum(1 for r in records if r["hit_sl"]) / n * 100
        print(f"  Total signals: {n}")
        print(f"  Avg MAX_FAV: {avg_fav:.2f}%  Avg MAX_ADV: {avg_adv:.2f}%  Net: {avg_fav-avg_adv:+.2f}%")
        print(f"  TP hit rate: {tp_pct:.0f}%  SL hit rate: {sl_pct:.0f}%")
        print(f"  Signals with F/A>1: {sum(1 for r in records if r['fav_adv_ratio']>1)}/{n} ({sum(1 for r in records if r['fav_adv_ratio']>1)/n*100:.0f}%)")
        print(f"  Signals with F/A>2: {sum(1 for r in records if r['fav_adv_ratio']>2)}/{n} ({sum(1 for r in records if r['fav_adv_ratio']>2)/n*100:.0f}%)")

    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser(description="观察信号回测")
    parser.add_argument("--days", type=int, default=1, help="回看天数（默认1天）")
    parser.add_argument("--hours", type=int, default=4, help="评估信号后N小时表现")
    parser.add_argument("--source", choices=["observe", "signals", "historical"], default="observe",
                        help="信号来源: observe=observe_signals_*.jsonl, signals=signals.jsonl, historical=historical_signals.jsonl")
    args = parser.parse_args()
    asyncio.run(evaluate(args.days, args.hours, args.source))


if __name__ == "__main__":
    main()
