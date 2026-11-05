import os
import asyncio
import logging
import time
import json
import statistics
import hmac
import hashlib
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

import aiohttp

from config import TOP30_BLACKLIST, STOCK_RELATED_BLACKLIST

logger = logging.getLogger(__name__)


class BinanceClient:
    FAPI = "https://fapi.binance.com"
    API = "https://api.binance.com"

    def __init__(self, api_key: str, secret: str, proxy: str = ""):
        self.api_key = api_key
        self.secret = secret
        self.proxy = proxy or None
        self._session: Optional[aiohttp.ClientSession] = None
        self._tick_cache: dict[str, float] = {}
        self._step_cache: dict[str, float] = {}
        # 全局请求节流：默认每 80ms 最多发出一个请求，避免 Render 共享 IP
        # 在扫描 200 个币种时瞬间打满 Binance 权重而被 418 封禁。
        self._min_interval = max(0.0, float(os.getenv("BINANCE_MIN_INTERVAL", "0.08")))
        self._rate_lock = asyncio.Lock()
        self._last_request = 0.0
        self._banned_until = 0.0

    async def _throttle(self):
        if time.time() < self._banned_until:
            left = int(self._banned_until - time.time())
            raise RuntimeError(f"Binance IP 封禁中，暂停请求，剩余约 {left}s")
        async with self._rate_lock:
            wait = self._min_interval - (time.monotonic() - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = time.monotonic()

    def _check_ban_response(self, r):
        if r.status in (418, 429):
            retry = r.headers.get("Retry-After", "60")
            try:
                retry_s = max(60, int(float(retry)))
            except (TypeError, ValueError):
                retry_s = 60
            self._banned_until = time.time() + retry_s
            logger.error(f"Binance 限频/封禁: HTTP {r.status}, Retry-After={retry_s}s，暂停请求")
            raise RuntimeError(f"Binance HTTP {r.status}, retry after {retry_s}s")

    async def load_exchange_info(self):
        try:
            info = await self.get("/fapi/v1/exchangeInfo")
            for s in info.get("symbols", []):
                sym = s["symbol"]
                for f in s.get("filters", []):
                    if f["filterType"] == "PRICE_FILTER":
                        self._tick_cache[sym] = float(f["tickSize"])
                    elif f["filterType"] == "LOT_SIZE":
                        self._step_cache[sym] = float(f["stepSize"])
            logger.info(f"exchangeInfo缓存完成: {len(self._tick_cache)} tick, {len(self._step_cache)} step")
        except Exception as e:
            logger.warning(f"exchangeInfo缓存失败: {e}")

    async def get_tick_size(self, symbol: str) -> float:
        if symbol in self._tick_cache:
            return self._tick_cache[symbol]
        try:
            info = await self.get("/fapi/v1/exchangeInfo")
            for s in info.get("symbols", []):
                if s["symbol"] == symbol:
                    for f in s.get("filters", []):
                        if f["filterType"] == "PRICE_FILTER":
                            val = float(f["tickSize"])
                            self._tick_cache[symbol] = val
                            return val
        except Exception:
            pass
        return 0.0001

    async def _sess(self):
        if not self._session or self._session.closed:
            connector = aiohttp.TCPConnector(ssl=False)
            # 公共行情接口不需要 API Key；带上 Key 既无必要，也会在 Key 被限频时
            # 连带导致公开行情请求返回 418，因此只在签名接口按需附加。
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                connector=connector,
            )
        return self._session

    def _signed_headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    async def get(self, path: str, params: dict = None, base: str = None):
        await self._throttle()
        s = await self._sess()
        url = (base or self.FAPI) + path
        async with s.get(url, params=params, proxy=self.proxy) as r:
            self._check_ban_response(r)
            r.raise_for_status()
            return await r.json()

    async def post_signed(self, path: str, params: dict):
        await self._throttle()
        params["timestamp"] = int(time.time() * 1000)
        qs = urlencode(params)
        sig = hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        qs += f"&signature={sig}"
        s = await self._sess()
        async with s.post(self.FAPI + path + "?" + qs, headers=self._signed_headers(), proxy=self.proxy) as r:
            self._check_ban_response(r)
            r.raise_for_status()
            return await r.json()

    async def delete_signed(self, path: str, params: dict):
        await self._throttle()
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        qs = urlencode(params)
        sig = hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        qs += f"&signature={sig}"
        s = await self._sess()
        async with s.delete(self.FAPI + path + "?" + qs, headers=self._signed_headers(), proxy=self.proxy) as r:
            self._check_ban_response(r)
            r.raise_for_status()
            return await r.json()

    async def get_signed(self, path: str, params: dict = None):
        await self._throttle()
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        qs = urlencode(params)
        sig = hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        qs += f"&signature={sig}"
        s = await self._sess()
        async with s.get(self.FAPI + path + "?" + qs, headers=self._signed_headers(), proxy=self.proxy) as r:
            self._check_ban_response(r)
            r.raise_for_status()
            return await r.json()

    async def get_orderbook_ratio(self, symbol: str) -> float:
        try:
            data = await self.get("/fapi/v1/depth", {"symbol": symbol, "limit": 10})
            bid = sum(float(b[1]) for b in data["bids"])
            ask = sum(float(a[1]) for a in data["asks"])
            return bid / ask if ask > 0 else 1.0
        except Exception:
            return 1.0

    async def get_step_size(self, symbol: str) -> float:
        if symbol in self._step_cache:
            return self._step_cache[symbol]
        try:
            info = await self.get("/fapi/v1/exchangeInfo")
            for s in info.get("symbols", []):
                if s["symbol"] == symbol:
                    for f in s.get("filters", []):
                        if f["filterType"] == "LOT_SIZE":
                            val = float(f["stepSize"])
                            self._step_cache[symbol] = val
                            return val
        except Exception:
            pass
        return 0.001

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


class YaoBiScanner:
    def __init__(self, config, notifier):
        self.config = config
        self.notifier = notifier
        proxy = os.getenv("SCANNER_PROXY", "")
        self.client = BinanceClient(config.BINANCE_API_KEY, config.BINANCE_SECRET, proxy)
        self.cooldown: dict[str, float] = {}
        self.total_signals = 0
        self._oi_history: dict[str, list] = {}
        self._funding_history: dict[str, list] = {}
        self._ob_history: dict[str, list] = {}
        self._failed_signals: dict[str, list] = {}
        self._btc_atr_pct: float = 0.0
        self._btc_atr_ts: float = 0.0
        self._market_bias = "NEUTRAL"
        self._market_bias_ts = 0.0
        # 限制同时分析的币种数，避免 200 个币种 * 多个接口瞬间并发
        # 触发 Binance 限频（Render 共享出口 IP 尤其敏感）。
        self._scan_sem = asyncio.Semaphore(max(1, int(os.getenv("SCAN_CONCURRENCY", "3"))))
        self._load_state()

    async def _analyze_limited(self, symbol: str):
        async with self._scan_sem:
            return await self._analyze(symbol)

    async def scan(self) -> list[dict]:
        try:
            symbols = await self._get_symbols()
        except Exception as e:
            # 不要用 repr(e)：aiohttp 的 RequestInfo 会带出 X-MBX-APIKEY 请求头
            logger.error(f"failed to fetch symbols: {e}", exc_info=True)
            return []

        results = await asyncio.gather(
            *[self._analyze_limited(sym) for sym in symbols],
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
        try:
            oi_data, ticker, funding_list, ls_data, klines_1h = await asyncio.gather(
                self.client.get("/fapi/v1/openInterest", {"symbol": symbol}),
                self.client.get("/fapi/v1/ticker/24hr", {"symbol": symbol}),
                self.client.get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 3}),
                self.client.get("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": "1h", "limit": 6}),
                self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": "1h", "limit": 48}),
                return_exceptions=True,
            )
            if any(isinstance(d, Exception) for d in [oi_data, ticker, klines_1h]):
                return None
        except Exception:
            return None

        thresholds = self.config.THRESHOLDS
        weights = self.config.FACTOR_WEIGHTS
        factors = {}

        price = float(ticker["lastPrice"])
        volume_24h = float(ticker["quoteVolume"])
        price_chg_24 = float(ticker["priceChangePercent"])
        oi_now = float(oi_data["openInterest"]) * price
        if oi_now < self.config.MIN_OI_USD_M * 1_000_000:
            return None

        market_state = self._detect_market_state(klines_1h, price)
        factors["market_state"] = market_state
        if market_state == "CHOP":
            return None
        if market_state == "VOLATILE" and not self.config.ALLOW_VOLATILE_ENTRY:
            return None

        market_bias = await self._get_market_bias()
        factors["market_bias"] = market_bias

        trend_score, trend_direction, trend_factors, trend_sub = self._calc_trend_group(klines_1h, price, market_state)
        factors.update(trend_factors)
        if trend_score < 40:
            return None

        trigger_score, trigger_factors, trigger_sub = await self._calc_trigger_group(symbol, oi_now, thresholds, price_chg_24)
        factors.update(trigger_factors)
        if trigger_score < 30:
            return None

        direction = trend_direction
        trigger_dir = factors.get("trigger_direction", direction)
        if trigger_dir != direction:
            return None

        emotion_score, emotion_factors, emotion_sub, current_fr = self._calc_emotion_group(symbol, funding_list, ls_data, thresholds, direction)
        factors.update(emotion_factors)

        if self.config.FUNDING_DIR_FILTER:
            if direction == "LONG" and current_fr < 0:
                factors["funding_dir_filter"] = f"做多但费率{current_fr*100:.4f}%<0(多头撤退)"
                return None
            if direction == "SHORT" and current_fr > 0:
                factors["funding_dir_filter"] = f"做空但费率{current_fr*100:.4f}%>0(空头还未发力)"
                return None

        w = self.config.FACTOR_WEIGHTS
        oi_raw = trigger_sub.get("oi", 0)
        ob_raw = trigger_sub.get("ob", 0)
        trigger_momentum = trigger_sub.get("momentum", 0)
        trigger_structure = trigger_sub.get("structure", 0)
        vol_raw = trigger_sub.get("volume", 0)
        funding_raw = emotion_sub.get("funding", 0)
        ls_raw = emotion_sub.get("ls_ratio", 0)

        trend_structure = trend_sub.get("structure", 0)
        trend_momentum = trend_sub.get("momentum", 0)

        squeeze_raw = trend_structure + trigger_structure
        momentum_raw = trend_momentum + trigger_momentum

        basis_raw = 0
        basis_val = factors.get("basis")
        if basis_val:
            try:
                bv = abs(float(basis_val.replace("%", "")))
                th_basis = thresholds["basis_diverge"] * 100
                if bv >= th_basis:
                    basis_raw = 5
                else:
                    basis_raw = int(5 * min(bv / max(th_basis, 1e-9), 1.0))
            except (ValueError, AttributeError):
                pass

        capital_flow_raw = 0
        try:
            klines_5m_cf = await self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": 20})
            cf_pct = await self._calc_capital_flow_ratio(symbol, klines_5m_cf)
            factors["capital_flow_pct"] = f"{cf_pct:.2f}%"
            if abs(cf_pct) >= thresholds["capital_flow_high"]:
                capital_flow_raw = 25
                factors["capital_flow"] = f"extreme flow {cf_pct:.2f}%"
            elif abs(cf_pct) >= thresholds["capital_flow_mid"]:
                capital_flow_raw = 15
                factors["capital_flow"] = f"strong flow {cf_pct:.2f}%"
            else:
                capital_flow_raw = int(25 * min(abs(cf_pct) / max(thresholds["capital_flow_high"], 1e-9), 1.0))
        except Exception:
            pass

        smart_dumb_raw = 0
        try:
            top_ls, global_ls = await self._get_smart_vs_dumb_money(symbol)
            if top_ls is not None and global_ls is not None:
                factors["top_ls"] = f"{top_ls:.2f}"
                factors["global_ls"] = f"{global_ls:.2f}"
                sd_ratio = thresholds["smart_dumb_ratio"]
                if direction == "LONG":
                    if top_ls > sd_ratio and global_ls < 1.0:
                        smart_dumb_raw = 25
                        factors["smart_vs_dumb"] = "大户多+散户空 轧空"
                    elif top_ls > 1.0 and global_ls < 1.0:
                        smart_dumb_raw = 15
                        factors["smart_vs_dumb"] = "大户偏多+散户偏空"
                elif direction == "SHORT":
                    if top_ls < (1 / sd_ratio) and global_ls > 1.0:
                        smart_dumb_raw = 25
                        factors["smart_vs_dumb"] = "大户空+散户多 反转"
                    elif top_ls < 1.0 and global_ls > 1.0:
                        smart_dumb_raw = 15
                        factors["smart_vs_dumb"] = "大户偏空+散户偏多"
        except Exception:
            pass

        score_parts = {
            "oi_surge":          min(w["oi_surge"],          int(oi_raw / 45 * w["oi_surge"])),
            "ob_imbalance":      min(w["ob_imbalance"],      int(ob_raw / 25 * w["ob_imbalance"])),
            "volume_spike":      min(w["volume_spike"],      vol_raw),
            "funding_extreme":   min(w["funding_extreme"],   int(max(funding_raw, 0) / 30 * w["funding_extreme"])),
            "price_squeeze":     min(w["price_squeeze"],     int(squeeze_raw / 80 * w["price_squeeze"])),
            "ls_ratio":          min(w["ls_ratio"],          int(ls_raw / 50 * w["ls_ratio"])),
            "basis_diverge":     min(w["basis_diverge"],     basis_raw),
            "momentum_breakout": min(w["momentum_breakout"], int(momentum_raw / 80 * w["momentum_breakout"])),
            "capital_flow":      min(w["capital_flow"],      int(capital_flow_raw / 25 * w["capital_flow"])),
            "smart_dumb":        min(w["smart_dumb"],        int(smart_dumb_raw / 25 * w["smart_dumb"])),
        }

        total_score = sum(score_parts.values())

        certainty_parts = {
            "oi_surge":     score_parts["oi_surge"],
            "ob_imbalance": score_parts["ob_imbalance"],
            "funding_extreme": score_parts["funding_extreme"],
            "ls_ratio":     score_parts["ls_ratio"],
            "capital_flow": score_parts["capital_flow"],
            "smart_dumb":   score_parts["smart_dumb"],
        }
        opportunity_parts = {
            "volume_spike":     score_parts["volume_spike"],
            "price_squeeze":   score_parts["price_squeeze"],
            "momentum_breakout": score_parts["momentum_breakout"],
            "basis_diverge":   score_parts["basis_diverge"],
        }
        certainty_score = sum(certainty_parts.values())
        opportunity_score = sum(opportunity_parts.values())

        if market_bias == "BEAR" and trend_direction == "LONG":
            if self.config.STRICT_MARKET_BIAS:
                factors["filter"] = "BTC熊市禁止做多"
                return None
            total_score = int(total_score * 0.7)
            factors["market_penalty"] = "-30% (BTC BEAR)"
        elif market_bias == "BULL" and trend_direction == "SHORT":
            if self.config.STRICT_MARKET_BIAS:
                factors["filter"] = "BTC牛市禁止做空"
                return None
            total_score = int(total_score * 0.7)
            factors["market_penalty"] = "-30% (BTC BULL)"

        btc_atr_pct = await self._get_btc_atr_pct()
        factors["btc_atr_pct"] = f"{btc_atr_pct:.2f}%"
        exp_filters = getattr(self.config, 'EXPERIMENTAL_FILTERS_ENABLED', True)
        if exp_filters and btc_atr_pct > self.config.BTC_ATR_VOL_THRESHOLD:
            total_score = int(total_score * 0.8)
            factors["btc_vol_penalty"] = f"-20% (BTC ATR {btc_atr_pct:.1f}%>{self.config.BTC_ATR_VOL_THRESHOLD}%)"

        now_utc_hour = datetime.now(timezone.utc).hour
        preferred = self.config.PREFERRED_HOURS_LIST
        if exp_filters and now_utc_hour not in preferred:
            discount = self.config.OFF_HOUR_SCORE_DISCOUNT
            total_score = int(total_score * discount)
            factors["off_hour_discount"] = f"-{(1-discount)*100:.0f}% (UTC {now_utc_hour}h不在优选时段{preferred})"

        if total_score < self.config.MIN_SCORE:
            return None

        if direction == "LONG" and price_chg_24 > self.config.MAX_24H_GAIN_LONG:
            return None
        if direction == "SHORT" and price_chg_24 < -self.config.MAX_24H_LOSS_SHORT:
            return None

        atr = self._calc_atr(klines_1h, period=14)
        factors["atr"] = atr
        factors["atr_pct"] = f"{atr / price * 100:.3f}%"

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

        return {
            "symbol": symbol,
            "score": total_score,
            "certainty_score": certainty_score,
            "opportunity_score": opportunity_score,
            "trend_score": trend_score,
            "trigger_score": trigger_score,
            "emotion_score": emotion_score,
            "market_state": market_state,
            "direction": direction,
            "price": price,
            "atr": atr,
            "price_change_24h": f"{price_chg_24:.2f}%",
            "volume_24h_usd": f"${volume_24h / 1e6:.1f}M",
            "factors": factors,
            "score_parts": score_parts,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _detect_market_state(self, klines_1h, price: float) -> str:
        if isinstance(klines_1h, Exception) or len(klines_1h) < 24:
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

    async def _get_btc_atr_pct(self) -> float:
        now = time.time()
        if now - self._btc_atr_ts < 300 and self._btc_atr_ts > 0:
            return self._btc_atr_pct
        try:
            btc_1h = await self.client.get("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 15})
            if isinstance(btc_1h, Exception) or len(btc_1h) < 15:
                return self._btc_atr_pct
            atr = self._calc_atr(btc_1h, period=14)
            price = float(btc_1h[-1][4])
            self._btc_atr_pct = atr / price * 100 if price > 0 else 0.0
            self._btc_atr_ts = now
        except Exception:
            pass
        return self._btc_atr_pct

    def _calc_trend_group(self, klines_1h, price: float, market_state: str) -> tuple:
        if isinstance(klines_1h, Exception) or len(klines_1h) < 20:
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
                score += 30
                sub["structure"] += 30
                factors["trend_ma"] = "MA8>MA20"
            if ma8_slope > 0.3:
                score += 25
                sub["momentum"] += 25
                factors["ma_slope"] = f"{ma8_slope:.2f}%"
            if recent_high > prev_high * 1.01:
                score += 25
                sub["structure"] += 25
                factors["structure"] = "higher highs"
            if price > ma8:
                score += 20
                sub["momentum"] += 20
                factors["price_ma"] = "price above MA8"
        elif market_state == "TREND_DOWN" or (market_state == "VOLATILE" and ma8 < ma20):
            direction = "SHORT"
            if ma8 < ma20:
                score += 30
                sub["structure"] += 30
                factors["trend_ma"] = "MA8<MA20"
            if ma8_slope < -0.3:
                score += 25
                sub["momentum"] += 25
                factors["ma_slope"] = f"{ma8_slope:.2f}%"
            if recent_low < prev_low * 0.99:
                score += 25
                sub["structure"] += 25
                factors["structure"] = "lower lows"
            if price < ma8:
                score += 20
                sub["momentum"] += 20
                factors["price_ma"] = "price below MA8"
        else:
            return 0, "LONG", {}, {"structure": 0, "momentum": 0}

        factors["ma8"] = round(ma8, 6)
        factors["ma20"] = round(ma20, 6)
        factors["ma8_slope"] = f"{ma8_slope:.3f}%"
        return score, direction, factors, sub

    async def _calc_trigger_group(self, symbol, oi_now, thresholds, price_chg_24=0.0) -> tuple:
        factors = {}
        scores = {}
        m5_chg = 0.0

        hist = self._oi_history.setdefault(symbol, [])
        hist.append(oi_now)
        if len(hist) > 12:
            hist.pop(0)

        oi_prev_5m = hist[-6] if len(hist) >= 6 else oi_now
        oi_prev_1m = hist[-2] if len(hist) >= 2 else oi_now
        chg_5m = (oi_now - oi_prev_5m) / oi_prev_5m if oi_prev_5m else 0
        chg_1m = (oi_now - oi_prev_1m) / oi_prev_1m if oi_prev_1m else 0
        chg_12m = (oi_now - hist[0]) / hist[0] if hist[0] else 0
        oi_accel = chg_1m - (chg_12m / 12 if chg_12m else 0)
        oi_expanding = chg_5m > 0

        if oi_accel > thresholds["oi_surge_pct"] * 0.3 and chg_5m > thresholds["oi_surge_pct"] * 0.5:
            scores["oi"] = 40
            factors["oi_surge"] = "OI accelerating"
        elif abs(chg_5m) >= thresholds["oi_surge_pct"]:
            scores["oi"] = 30
            factors["oi_surge"] = "OI spike"
        else:
            scores["oi"] = int(40 * min(abs(chg_5m) / max(thresholds["oi_surge_pct"], 1e-9), 1.0))

        factors["oi_change_5m"] = f"{chg_5m * 100:.2f}%"
        factors["oi_accel"] = f"{oi_accel * 100:.3f}%"
        factors["oi_usd"] = f"${oi_now / 1e6:.1f}M"

        try:
            klines_5m, klines_15m = await asyncio.gather(
                self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": 20}),
                self.client.get("/fapi/v1/klines", {"symbol": symbol, "interval": "15m", "limit": 12}),
            )

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
                swing_lows  = [float(k[3]) for k in klines_15m[:-1]]
                prev_swing_high = max(swing_highs[-5:-1]) if len(swing_highs) >= 5 else max(swing_highs)
                prev_swing_low  = min(swing_lows[-5:-1])  if len(swing_lows)  >= 5 else min(swing_lows)

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

                if breakout_long:
                    deep_retest = False
                    shallow_hold = False
                    fake_break = False
                    has_post_bars = (breakout_bar_idx + 1) < len(klines_15m)

                    if has_post_bars:
                        post_lows = [float(klines_15m[j][3]) for j in range(breakout_bar_idx + 1, len(klines_15m))]
                        min_post_low = min(post_lows)
                        if min_post_low < prev_swing_high * (1 - tol):
                            fake_break = True
                        elif min_post_low < prev_swing_high:
                            deep_retest = True
                        else:
                            shallow_hold = True

                    restart_ok = False
                    if (deep_retest or shallow_hold) and len(klines_5m) >= 6:
                        pullback_low = min(float(k[3]) for k in klines_5m[-6:])
                        last_5m_close = float(klines_5m[-1][4])
                        restart_pct = (last_5m_close - pullback_low) / pullback_low if pullback_low else 0
                        if restart_pct >= restart_th:
                            restart_ok = True

                    if fake_break:
                        scores["structure"] = 0
                        factors["structure_trigger"] = "15m fake breakout"
                        factors["trigger_direction"] = "LONG"
                    elif deep_retest and restart_ok:
                        scores["structure"] = 25
                        factors["structure_trigger"] = "15m breakout+retest+5m restart"
                        factors["trigger_direction"] = "LONG"
                    elif shallow_hold:
                        scores["structure"] = 22
                        factors["structure_trigger"] = "15m breakout+strong hold"
                        factors["trigger_direction"] = "LONG"
                    elif deep_retest:
                        scores["structure"] = 15
                        factors["structure_trigger"] = "15m breakout+retest(wait restart)"
                        factors["trigger_direction"] = "LONG"
                    else:
                        scores["structure"] = 10
                        factors["structure_trigger"] = "15m breakout(pending)"
                        factors["trigger_direction"] = "LONG"

                elif breakout_short:
                    deep_retest = False
                    shallow_hold = False
                    fake_break = False
                    has_post_bars = (breakout_bar_idx + 1) < len(klines_15m)

                    if has_post_bars:
                        post_highs = [float(klines_15m[j][2]) for j in range(breakout_bar_idx + 1, len(klines_15m))]
                        max_post_high = max(post_highs)
                        if max_post_high > prev_swing_low * (1 + tol):
                            fake_break = True
                        elif max_post_high > prev_swing_low:
                            deep_retest = True
                        else:
                            shallow_hold = True

                    restart_ok = False
                    if (deep_retest or shallow_hold) and len(klines_5m) >= 6:
                        pullback_high = max(float(k[2]) for k in klines_5m[-6:])
                        last_5m_close = float(klines_5m[-1][4])
                        restart_pct = (pullback_high - last_5m_close) / pullback_high if pullback_high else 0
                        if restart_pct >= restart_th:
                            restart_ok = True

                    if fake_break:
                        scores["structure"] = 0
                        factors["structure_trigger"] = "15m fake breakdown"
                        factors["trigger_direction"] = "SHORT"
                    elif deep_retest and restart_ok:
                        scores["structure"] = 25
                        factors["structure_trigger"] = "15m breakdown+retest+5m restart"
                        factors["trigger_direction"] = "SHORT"
                    elif shallow_hold:
                        scores["structure"] = 22
                        factors["structure_trigger"] = "15m breakdown+strong hold"
                        factors["trigger_direction"] = "SHORT"
                    elif deep_retest:
                        scores["structure"] = 15
                        factors["structure_trigger"] = "15m breakdown+retest(wait restart)"
                        factors["trigger_direction"] = "SHORT"
                    else:
                        scores["structure"] = 10
                        factors["structure_trigger"] = "15m breakdown(pending)"
                        factors["trigger_direction"] = "SHORT"
        except Exception:
            scores["momentum"] = 0

        if price_chg_24 != 0:
            price_dir_up = price_chg_24 > 0
            factors["oi_price_sync"] = "aligned" if oi_expanding == price_dir_up else "diverged"
            if oi_expanding == price_dir_up:
                scores["oi"] = min(45, scores.get("oi", 0) + 5)
            else:
                scores["oi"] = max(0, scores.get("oi", 0) - 10)
        else:
            factors["oi_price_sync"] = "unknown"

        ob_ratio = await self.client.get_orderbook_ratio(symbol)
        factors["ob_ratio"] = f"{ob_ratio:.2f}"

        ob_hist = self._ob_history.setdefault(symbol, [])
        ob_hist.append(ob_ratio)
        if len(ob_hist) > self.config.OB_PERSIST_COUNT:
            ob_hist.pop(0)

        ob_persistent = False
        if len(ob_hist) >= self.config.OB_PERSIST_COUNT:
            persist_th = self.config.OB_PERSIST_THRESHOLD
            if ob_ratio >= thresholds["ob_ratio_high"]:
                ob_persistent = all(r >= persist_th for r in ob_hist)
            elif ob_ratio <= thresholds["ob_ratio_low"]:
                ob_persistent = all(r <= (1 / persist_th) for r in ob_hist)

        if ob_ratio >= thresholds["ob_ratio_high"]:
            if ob_persistent:
                scores["ob"] = 25
                factors["ob_imbalance"] = f"bid heavy {ob_ratio:.1f}x (persistent)"
            else:
                scores["ob"] = 12
                factors["ob_imbalance"] = f"bid heavy {ob_ratio:.1f}x (not persistent)"
        elif ob_ratio <= thresholds["ob_ratio_low"]:
            if ob_persistent:
                scores["ob"] = 25
                factors["ob_imbalance"] = f"ask heavy {ob_ratio:.1f}x (persistent)"
            else:
                scores["ob"] = 12
                factors["ob_imbalance"] = f"ask heavy {ob_ratio:.1f}x (not persistent)"
        else:
            scores["ob"] = int(25 * max(0, (abs(ob_ratio - 1) - 0.2) / 1.3))

        total = min(100, scores.get("oi", 0) + scores.get("momentum", 0) + scores.get("structure", 0) + scores.get("ob", 0))
        return total, factors, scores

    def _calc_emotion_group(self, symbol, funding_list, ls_data, thresholds, direction="LONG") -> tuple:
        factors = {}
        sub_scores = {"funding": 0, "ls_ratio": 0}
        score = 0
        current_fr = 0.0

        if funding_list and not isinstance(funding_list, Exception):
            fr = float(funding_list[-1]["fundingRate"])
            current_fr = fr
            fhist = self._funding_history.setdefault(symbol, [])
            fhist.append(fr)
            if len(fhist) > 6:
                fhist.pop(0)
            fr_slope = fhist[-1] - fhist[0] if len(fhist) >= 2 else 0
            factors["funding_rate"] = f"{fr * 100:.4f}%"
            factors["funding_slope"] = f"{fr_slope * 100:.4f}%"

            funding_sweet_low = thresholds.get("funding_sweet_low", -0.0005)
            funding_sweet_high = thresholds.get("funding_sweet_high", 0.0005)

            if funding_sweet_low <= fr <= funding_sweet_high:
                score += 30
                sub_scores["funding"] = 30
                factors["funding_sweet"] = f"sweet zone {fr*100:.3f}%"
            elif (direction == "LONG" and fr < funding_sweet_low) or \
                 (direction == "SHORT" and fr > funding_sweet_high):
                score += 20
                sub_scores["funding"] = 20
                factors["funding_reverse_chance"] = f"reverse opportunity {fr*100:.3f}%"
            elif (direction == "LONG" and fr > 0.001) or \
                 (direction == "SHORT" and fr < -0.001):
                score -= 20
                sub_scores["funding"] = -20
                factors["funding_chase_risk"] = f"chase risk {fr*100:.3f}%"
            elif abs(fr_slope) > thresholds["funding_long"] * 0.5:
                score += 15
                sub_scores["funding"] = 15
                factors["funding_slope_signal"] = "funding slope change"

        if ls_data and not isinstance(ls_data, Exception) and len(ls_data) > 0:
            lsr = float(ls_data[-1]["longShortRatio"])
            factors["ls_ratio"] = f"{lsr:.2f}"
            if lsr >= thresholds["ls_ratio_high"] or lsr <= thresholds["ls_ratio_low"]:
                score += 50
                sub_scores["ls_ratio"] = 50
                factors["ls_ratio_signal"] = f"{lsr:.2f}"

        return min(100, score), factors, sub_scores, current_fr

    def _calc_atr(self, klines_1h, period: int = 14) -> float:
        if isinstance(klines_1h, Exception) or len(klines_1h) < period + 1:
            return 0.0
        trs = []
        for i in range(-period, 0):
            h = float(klines_1h[i][2])
            l = float(klines_1h[i][3])
            cp = float(klines_1h[i - 1][4])
            trs.append(max(h - l, abs(h - cp), abs(l - cp)))
        return statistics.mean(trs) if trs else 0.0

    async def _calc_capital_flow_ratio(self, symbol, klines_5m) -> float:
        if klines_5m is None or len(klines_5m) < 12:
            return 0.0

        net_flow = 0
        for k in klines_5m[-12:]:
            taker_buy_quote = float(k[10])
            total_quote = float(k[7])
            taker_sell_quote = total_quote - taker_buy_quote
            net_flow += (taker_buy_quote - taker_sell_quote)

        price = float(klines_5m[-1][4])
        oi_now = await self.client.get("/fapi/v1/openInterest", {"symbol": symbol})
        notional = float(oi_now["openInterest"]) * price

        if notional <= 0:
            return 0.0
        return net_flow / notional * 100

    async def _get_smart_vs_dumb_money(self, symbol):
        top_ratio = await self.client.get(
            "/futures/data/topLongShortAccountRatio",
            {"symbol": symbol, "period": "1h", "limit": 1},
        )
        global_ratio = await self.client.get(
            "/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": "1h", "limit": 1},
        )
        if not top_ratio or not global_ratio:
            return None, None
        if isinstance(top_ratio, Exception) or isinstance(global_ratio, Exception):
            return None, None
        if len(top_ratio) == 0 or len(global_ratio) == 0:
            return None, None
        top_ls = float(top_ratio[0]["longShortRatio"])
        global_ls = float(global_ratio[0]["longShortRatio"])
        return top_ls, global_ls

    def _in_cooldown(self, s: str) -> bool:
        return (time.time() - self.cooldown.get(s, 0)) < self.config.COOLDOWN_SECONDS

    def _set_cooldown(self, s: str):
        self.cooldown[s] = time.time()

    def _log_signal(self, sig: dict):
        path = self.config.SIGNAL_LOG_PATH
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(sig, ensure_ascii=False) + "\n")

    def _save_state(self):
        os.makedirs(os.path.dirname(self.config.STATE_PATH), exist_ok=True)
        with open(self.config.STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "cooldown": self.cooldown,
                    "oi_history": self._oi_history,
                    "funding_history": self._funding_history,
                    "ob_history": self._ob_history,
                    "total_signals": self.total_signals,
                },
                f,
            )

    def _load_state(self):
        try:
            if os.path.exists(self.config.STATE_PATH):
                s = json.load(open(self.config.STATE_PATH, encoding="utf-8"))
                self.cooldown = s.get("cooldown", {})
                self._oi_history = s.get("oi_history", {})
                self._funding_history = s.get("funding_history", {})
                self._ob_history = s.get("ob_history", {})
                self.total_signals = s.get("total_signals", 0)
                logger.info(f"restored {self.total_signals} signals and {len(self.cooldown)} cooldown symbols")
        except Exception as e:
            logger.warning(f"failed to load scanner state: {e}")
