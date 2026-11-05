import os
import asyncio
import logging
import time
import json
import statistics
from datetime import datetime, timezone
from typing import Optional

from config import TOP30_BLACKLIST, STOCK_RELATED_BLACKLIST
from scanner import BinanceClient

logger = logging.getLogger(__name__)


class MeanReversionScanner:
    def __init__(self, config, notifier):
        self.config = config
        self.notifier = notifier
        proxy = os.getenv("SCANNER_PROXY", "")
        self.client = BinanceClient(config.BINANCE_API_KEY, config.BINANCE_SECRET, proxy)
        self.cooldown: dict[str, float] = {}
        self.total_signals = 0
        self._funding_history: dict[str, list] = {}
        self._price_history: dict[str, list] = {}
        self._failed_signals: dict[str, list] = {}
        self._btc_atr_pct: float = 0.0
        self._btc_atr_ts: float = 0.0
        self._market_bias = "NEUTRAL"
        self._market_bias_ts = 0.0
        self.last_signal_time: dict[str, float] = {}  # 每个币的最后信号时间
        self.COOLDOWN_HOURS = 24  # 24小时冷却期
        self._load_state()

    async def scan(self) -> list[dict]:
        try:
            symbols = await self._get_symbols()
        except Exception as e:
            logger.error(f"failed to fetch symbols: {e}", exc_info=True)
            return []

        results = await asyncio.gather(
            *[self._analyze(sym) for sym in symbols],
            return_exceptions=True,
        )

        signals = []
        for r in results:
            if not isinstance(r, dict):
                continue
            if r.get("score", 0) < self.config.MIN_SCORE:
                continue
            sym = r["symbol"]
            if self._in_cooldown(sym):
                continue
            repeat_zone = getattr(self.config, 'SIGNAL_REPEAT_ZONE_PCT', 3.0)
            sig_price = r.get("price", 0)
            in_repeat_zone = False
            if sym in self._failed_signals and sig_price > 0:
                for fs in self._failed_signals[sym]:
                    fp = fs.get("price", 0)
                    if fp > 0 and abs(sig_price - fp) / fp * 100 < repeat_zone:
                        in_repeat_zone = True
                        break
            if in_repeat_zone:
                continue
            self._set_cooldown(sym)
            self._log_signal(r)
            self.total_signals += 1
            signals.append(r)

        signals.sort(key=lambda x: x["score"], reverse=True)
        self._save_state()
        return signals

    async def _get_symbols(self) -> list[str]:
        data = await self.client.get("/fapi/v1/ticker/24hr")
        filtered = []
        for d in data:
            sym = d["symbol"]
            if not sym.endswith("USDT"):
                continue
            base = sym.replace("USDT", "")
            if base in TOP30_BLACKLIST:
                continue
            if base in STOCK_RELATED_BLACKLIST:
                continue
            vol = float(d.get("quoteVolume", 0))
            if vol < 2_000_000:
                continue
            filtered.append((sym, vol))
        filtered.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in filtered[: self.config.TOP_N_SYMBOLS]]

    async def _analyze(self, symbol: str) -> Optional[dict]:
        # 检查24小时冷却期
        now_ts = time.time()
        if symbol in self.last_signal_time:
            elapsed = (now_ts - self.last_signal_time[symbol]) / 3600
            if elapsed < self.COOLDOWN_HOURS:
                return None  # 跳过，等冷却结束
        
        try:
            ticker, funding_list, klines_1h, klines_4h = await asyncio.gather(
                self.client.get("/fapi/v1/ticker/24hr", {"symbol": symbol}),
                self.client.get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 6}),
                self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": "1h", "limit": 48}),
                self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": "4h", "limit": 24}),
                return_exceptions=True,
            )
            if any(isinstance(d, Exception) for d in [ticker, klines_1h, klines_4h]):
                return None
        except Exception:
            return None

        factors = {}

        price = float(ticker["lastPrice"])
        volume_24h = float(ticker["quoteVolume"])
        price_chg_24 = float(ticker["priceChangePercent"])

        # 计算移动平均线
        closes_1h = [float(k[4]) for k in klines_1h]
        closes_4h = [float(k[4]) for k in klines_4h]
        
        ma20_1h = statistics.mean(closes_1h[-20:]) if len(closes_1h) >= 20 else price
        ma50_1h = statistics.mean(closes_1h[-50:]) if len(closes_1h) >= 50 else price
        ma20_4h = statistics.mean(closes_4h[-20:]) if len(closes_4h) >= 20 else price

        # 计算价格偏离度
        price_deviation_1h = (price - ma20_1h) / ma20_1h * 100
        price_deviation_4h = (price - ma20_4h) / ma20_4h * 100

        # 计算波动率
        atr_1h = self._calc_atr(klines_1h, period=14)
        volatility_pct = atr_1h / price * 100 if price > 0 else 0

        # 资金费率分析
        current_fr = 0.0
        funding_trend = 0.0
        if funding_list and not isinstance(funding_list, Exception) and len(funding_list) > 0:
            fr_values = [float(f["fundingRate"]) for f in funding_list]
            current_fr = fr_values[-1]
            funding_trend = fr_values[-1] - fr_values[0] if len(fr_values) >= 2 else 0
            
            # 保存资金费率历史
            fhist = self._funding_history.setdefault(symbol, [])
            fhist.append(current_fr)
            if len(fhist) > 12:
                fhist.pop(0)

        # 价格历史
        phist = self._price_history.setdefault(symbol, [])
        phist.append(price)
        if len(phist) > 24:
            phist.pop(0)

        # 均值回归信号生成
        score = 0
        direction = "LONG"
        
        # 极端资金费率策略
        if abs(current_fr) > 0.0015:  # 资金费率绝对值大于0.15%
            if current_fr > 0:  # 正费率，倾向做空
                direction = "SHORT"
                score += 30
                factors["funding_extreme"] = f"high positive funding {current_fr*100:.4f}%"
            else:  # 负费率，倾向做多
                direction = "LONG"
                score += 30
                factors["funding_extreme"] = f"high negative funding {current_fr*100:.4f}%"
        
        # 价格偏离策略
        if abs(price_deviation_1h) > 3:  # 价格偏离MA20超过3%
            if price_deviation_1h > 0:  # 价格过高，倾向做空
                direction = "SHORT"
                score += 25
                factors["price_deviation"] = f"overbought {price_deviation_1h:.2f}% above MA20"
            else:  # 价格过低，倾向做多
                direction = "LONG"
                score += 25
                factors["price_deviation"] = f"oversold {abs(price_deviation_1h):.2f}% below MA20"
        
        # 4小时级别确认
        if direction == "LONG" and price_deviation_4h < -2:
            score += 15
            factors["4h_confirmation"] = "4h oversold confirmation"
        elif direction == "SHORT" and price_deviation_4h > 2:
            score += 15
            factors["4h_confirmation"] = "4h overbought confirmation"
        
        # 波动率过滤
        if volatility_pct > 5:
            score -= 10
            factors["high_volatility"] = f"high volatility {volatility_pct:.2f}%"
        
        # 24小时涨跌幅过滤
        if direction == "LONG" and price_chg_24 < -5:
            score += 10
            factors["extreme_move"] = f"extreme 24h decline {price_chg_24:.2f}%"
        elif direction == "SHORT" and price_chg_24 > 5:
            score += 10
            factors["extreme_move"] = f"extreme 24h rally {price_chg_24:.2f}%"
        
        # 市场环境
        market_bias = await self._get_market_bias()
        factors["market_bias"] = market_bias
        
        # 调整得分
        if market_bias == "BEAR" and direction == "LONG":
            score = int(score * 1.2)  # 熊市中做多信号更强
            factors["market_boost"] = "+20% in BEAR market"
        elif market_bias == "BULL" and direction == "SHORT":
            score = int(score * 1.2)  # 牛市中做空信号更强
            factors["market_boost"] = "+20% in BULL market"
        
        # 计算ATR用于止损止盈
        atr = self._calc_atr(klines_1h, period=14)
        
        # 结构止损
        structure_sl = None
        try:
            klines_5m_sl = await self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": 10})
            if klines_5m_sl and not isinstance(klines_5m_sl, Exception) and len(klines_5m_sl) >= 5:
                if direction == "LONG":
                    structure_sl = min(float(k[3]) for k in klines_5m_sl[-5:])
                else:
                    structure_sl = max(float(k[2]) for k in klines_5m_sl[-5:])
                factors["structure_sl"] = structure_sl
        except Exception:
            pass

        # 确保得分不为负
        score = max(0, score)
        
        if score < self.config.MIN_SCORE:
            return None

        # 记录信号时间
        self.last_signal_time[symbol] = time.time()

        return {
            "symbol": symbol,
            "score": score,
            "direction": direction,
            "price": price,
            "atr": atr,
            "price_change_24h": f"{price_chg_24:.2f}%",
            "volume_24h_usd": f"${volume_24h / 1e6:.1f}M",
            "factors": factors,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    async def _get_market_bias(self) -> str:
        now = time.time()
        if now - self._market_bias_ts < 300 and self._market_bias_ts > 0:
            return self._market_bias
        try:
            btc_1h = await self.client.get("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 24})
            if isinstance(btc_1h, Exception) or len(btc_1h) < 24:
                return self._market_bias
            closes = [float(k[4]) for k in btc_1h]
            ma8 = statistics.mean(closes[-8:])
            ma24 = statistics.mean(closes)
            if ma8 > ma24 * 1.005:
                self._market_bias = "BULL"
            elif ma8 < ma24 * 0.995:
                self._market_bias = "BEAR"
            else:
                self._market_bias = "NEUTRAL"
            self._market_bias_ts = now
        except Exception:
            pass
        return self._market_bias

    def _calc_atr(self, klines, period: int = 14) -> float:
        if isinstance(klines, Exception) or len(klines) < period + 1:
            return 0.0
        trs = []
        for i in range(-period, 0):
            h = float(klines[i][2])
            l = float(klines[i][3])
            cp = float(klines[i - 1][4])
            trs.append(max(h - l, abs(h - cp), abs(l - cp)))
        return statistics.mean(trs) if trs else 0.0

    def _in_cooldown(self, s: str) -> bool:
        return (time.time() - self.cooldown.get(s, 0)) < self.config.COOLDOWN_SECONDS

    def _set_cooldown(self, s: str):
        self.cooldown[s] = time.time()

    def _log_signal(self, sig: dict):
        path = self.config.SIGNAL_LOG_PATH.replace("signal", "mr_signal")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(sig, ensure_ascii=False) + "\n")

    def _load_state(self):
        state_path = self.config.STATE_PATH.replace("state", "mr_state")
        if os.path.exists(state_path):
            try:
                with open(state_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.cooldown = data.get("cooldown", {})
                    self._funding_history = data.get("funding_history", {})
                    self._price_history = data.get("price_history", {})
                    self._failed_signals = data.get("failed_signals", {})
                    self.last_signal_time = data.get("last_signal_time", {})
            except Exception as e:
                logger.warning(f"加载状态失败: {e}")

    def _save_state(self):
        state_path = self.config.STATE_PATH.replace("state", "mr_state")
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "cooldown": self.cooldown,
                    "funding_history": self._funding_history,
                    "price_history": self._price_history,
                    "failed_signals": self._failed_signals,
                    "last_signal_time": self.last_signal_time,
                },
                f,
                ensure_ascii=False,
            )
