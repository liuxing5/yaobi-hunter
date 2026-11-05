"""
fetch_historical.py - 拉取币安合约历史数据用于回测

用法：
    python fetch_historical.py                  # 拉近30天默认200币
    python fetch_historical.py --days 14        # 拉近14天
    python fetch_historical.py --top-n 100      # 拉前100币
    python fetch_historical.py --symbol RAVEUSDT SPKUSDT  # 只拉指定币种
    python fetch_historical.py --resume         # 断点续传
    python fetch_historical.py --cleanup        # 清理旧数据重拉

输出：
    data/backtest/
      ├── manifest.json              # 元数据（已拉完的symbol+interval清单）
      ├── BTCUSDT_klines_1m.parquet
      ├── BTCUSDT_klines_5m.parquet
      ├── BTCUSDT_klines_15m.parquet
      ├── BTCUSDT_klines_1h.parquet
      ├── BTCUSDT_oi_5m.parquet
      ├── BTCUSDT_ls_ratio_5m.parquet
      ├── BTCUSDT_taker_5m.parquet
      └── BTCUSDT_funding.parquet

关键限制：
    - 订单簿无历史：回测时 ob_imbalance 需给固定分值
    - OI/多空比历史仅30天：超过自动截断
    - /futures/data/* 权重限制较严格，已做限流
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import aiohttp

try:
    import pandas as pd
except ImportError:
    print("请先安装: pip install pandas pyarrow aiohttp")
    sys.exit(1)


# ── 配置 ─────────────────────────────────────────────────────────
FAPI = "https://fapi.binance.com"
PROXY = os.getenv("SCANNER_PROXY", "http://127.0.0.1:10808") or None

OUTPUT_DIR = Path(__file__).parent / "data" / "backtest"
MANIFEST_FILE = OUTPUT_DIR / "manifest.json"

# 并发限制（每秒请求数；实际权重受限于2400/min即40/s，留buffer）
MAX_CONCURRENT = 15

# 单次重试参数
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

# 黑名单（scanner里一样的）
TOP30_BLACKLIST = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "SHIB", "DOT",
    "LINK", "MATIC", "LTC", "BCH", "UNI", "ATOM", "XLM", "ETC", "ALGO", "VET",
    "ICP", "FIL", "APT", "ARB", "OP", "INJ", "SUI", "TRX", "TON", "NEAR",
}
STOCK_RELATED_BLACKLIST = {
    "COIN", "TSLA", "AAPL", "GOOG", "NVDA", "AMZN", "MSTR", "MARA", "HOOD",
    "SQ", "RIOT", "CLSK", "HUT", "BTBT", "BFCH", "CBSE",
}

# K线每次 limit 上限（币安规定）
KLINE_LIMIT = 1500
# OI历史每次 limit 上限
OI_LIMIT = 500
# 资金费率每次 limit 上限
FUNDING_LIMIT = 1000

# 每种数据对应的 interval
KLINE_INTERVALS = ["1m", "5m", "15m", "1h"]
OI_INTERVAL = "5m"      # 可选 5m/15m/30m/1h/2h/4h/6h/12h/1d；5m对齐scanner的OI加速度
LS_INTERVAL = "5m"      # 全账户多空比
TAKER_INTERVAL = "5m"   # 主动买卖比
TOP_ACC_INTERVAL = "5m" # 大户账户多空比（topLongShortAccountRatio）
TOP_POS_INTERVAL = "5m" # 大户持仓多空比（topLongShortPositionRatio）


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── 工具函数 ────────────────────────────────────────────────────
def interval_to_ms(interval: str) -> int:
    """K线间隔字符串转毫秒"""
    unit = interval[-1]
    n = int(interval[:-1])
    return {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit] * n


def date_utc_str(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


# ── 限流 ────────────────────────────────────────────────────────
class RateLimiter:
    """令牌桶限流：每秒最多 N 个请求"""
    def __init__(self, rate_per_sec: int):
        self.rate = rate_per_sec
        self.tokens = rate_per_sec
        self.last_refill = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_refill = now
            if self.tokens < 1:
                wait = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait)
                self.tokens = 0
            else:
                self.tokens -= 1


# ── HTTP ────────────────────────────────────────────────────────
async def fetch_json(
    session: aiohttp.ClientSession,
    path: str,
    params: dict,
    limiter: RateLimiter,
) -> Optional[list | dict]:
    """带限流和重试的请求"""
    url = FAPI + path
    for attempt in range(MAX_RETRIES):
        await limiter.acquire()
        try:
            async with session.get(url, params=params, proxy=PROXY) as r:
                if r.status == 429:
                    retry_after = int(r.headers.get("Retry-After", "10"))
                    logger.warning(f"429限频，等待{retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                if r.status == 418:
                    logger.error("418 IP ban，已被封禁。停止拉取")
                    raise SystemExit(1)
                if r.status >= 500:
                    logger.warning(f"{path} {params} HTTP {r.status}，重试{attempt+1}/{MAX_RETRIES}")
                    await asyncio.sleep(RETRY_BACKOFF ** attempt)
                    continue
                if r.status != 200:
                    text = await r.text()
                    logger.error(f"{path} {params} HTTP {r.status}: {text[:200]}")
                    return None
                return await r.json()
        except asyncio.TimeoutError:
            logger.warning(f"{path} 超时，重试{attempt+1}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_BACKOFF ** attempt)
        except aiohttp.ClientError as e:
            logger.warning(f"{path} 网络错误: {e}，重试{attempt+1}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_BACKOFF ** attempt)
    logger.error(f"{path} {params} 重试{MAX_RETRIES}次均失败")
    return None


# ── Manifest 管理 ───────────────────────────────────────────────
@dataclass
class Manifest:
    days: int = 30
    top_n: int = 200
    fetched: dict[str, list[str]] = field(default_factory=dict)  # symbol -> [data_type]
    last_updated: str = ""

    def mark(self, symbol: str, data_type: str):
        self.fetched.setdefault(symbol, [])
        if data_type not in self.fetched[symbol]:
            self.fetched[symbol].append(data_type)

    def is_done(self, symbol: str, data_type: str) -> bool:
        return data_type in self.fetched.get(symbol, [])

    def save(self):
        self.last_updated = datetime.now(timezone.utc).isoformat()
        MANIFEST_FILE.write_text(json.dumps({
            "days": self.days,
            "top_n": self.top_n,
            "fetched": self.fetched,
            "last_updated": self.last_updated,
        }, indent=2))

    @classmethod
    def load(cls) -> "Manifest":
        if not MANIFEST_FILE.exists():
            return cls()
        try:
            d = json.loads(MANIFEST_FILE.read_text())
            return cls(
                days=d.get("days", 30),
                top_n=d.get("top_n", 200),
                fetched=d.get("fetched", {}),
                last_updated=d.get("last_updated", ""),
            )
        except Exception:
            return cls()


# ── 目标币种列表 ────────────────────────────────────────────────
async def get_target_symbols(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    top_n: int,
    specified: Optional[list[str]] = None,
) -> list[str]:
    """获取目标币种（按24H成交额排序，跳过黑名单）"""
    if specified:
        # 用户指定了具体币种
        return [s.upper() for s in specified]

    data = await fetch_json(session, "/fapi/v1/ticker/24hr", {}, limiter)
    if not data:
        logger.error("获取ticker失败")
        return []

    filtered = []
    for d in data:
        sym = d["symbol"]
        if not sym.endswith("USDT"):
            continue
        base = sym.replace("USDT", "")
        if base in TOP30_BLACKLIST or base in STOCK_RELATED_BLACKLIST:
            continue
        try:
            vol = float(d.get("quoteVolume", 0))
        except (ValueError, TypeError):
            continue
        if vol < 2_000_000:
            continue
        filtered.append((sym, vol))

    filtered.sort(key=lambda x: x[1], reverse=True)
    syms = [s for s, _ in filtered[:top_n]]
    logger.info(f"目标币种: {len(syms)} 个（top {top_n}）")
    return syms


# ── K线拉取 ─────────────────────────────────────────────────────
async def fetch_klines_range(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> Optional[pd.DataFrame]:
    """分页拉取时间范围内的K线"""
    all_rows = []
    cur = start_ms
    ms_per_bar = interval_to_ms(interval)
    while cur < end_ms:
        chunk_end = min(cur + KLINE_LIMIT * ms_per_bar, end_ms)
        data = await fetch_json(
            session,
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": cur,
                "endTime": chunk_end,
                "limit": KLINE_LIMIT,
            },
            limiter,
        )
        if data is None:
            return None
        if not data:
            # 没数据了，说明这段时间币还没上
            cur = chunk_end
            continue
        all_rows.extend(data)
        # 下一段从最后一根K线的下一个周期开始
        last_open_time = data[-1][0]
        cur = last_open_time + ms_per_bar

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_volume", "taker_buy_quote_volume", "ignore",
    ])
    # 转数值类型
    for c in ["open", "high", "low", "close", "volume",
              "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = df["open_time"].astype("int64")
    df["close_time"] = df["close_time"].astype("int64")
    df["trades"] = df["trades"].astype("int64")
    df = df.drop(columns=["ignore"])
    # 去重（防止分页重叠）
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    return df


# ── OI历史拉取 ──────────────────────────────────────────────────
async def fetch_oi_hist(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
    period: str = OI_INTERVAL,
) -> Optional[pd.DataFrame]:
    all_rows = []
    cur = start_ms
    ms_per_bar = interval_to_ms(period)
    while cur < end_ms:
        chunk_end = min(cur + OI_LIMIT * ms_per_bar, end_ms)
        data = await fetch_json(
            session,
            "/futures/data/openInterestHist",
            {
                "symbol": symbol,
                "period": period,
                "startTime": cur,
                "endTime": chunk_end,
                "limit": OI_LIMIT,
            },
            limiter,
        )
        if data is None:
            return None
        if not data:
            cur = chunk_end
            continue
        all_rows.extend(data)
        last_ts = int(data[-1]["timestamp"])
        cur = last_ts + ms_per_bar

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows)
    df["timestamp"] = df["timestamp"].astype("int64")
    df["sumOpenInterest"] = pd.to_numeric(df["sumOpenInterest"], errors="coerce")
    df["sumOpenInterestValue"] = pd.to_numeric(df["sumOpenInterestValue"], errors="coerce")
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df


# ── 通用: /futures/data/* 类接口的分页拉取 ──────────────────────
async def _fetch_futures_data_period(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    path: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    period: str,
    numeric_cols: list[str],
    ts_field: str = "timestamp",
) -> Optional[pd.DataFrame]:
    """
    通用分页拉取 /futures/data/* 类接口
    这类接口共同特点：返回 [{timestamp, symbol, ...数值字段}]，最多500条/次，最多30天历史
    """
    all_rows = []
    cur = start_ms
    ms_per_bar = interval_to_ms(period)
    while cur < end_ms:
        chunk_end = min(cur + OI_LIMIT * ms_per_bar, end_ms)
        data = await fetch_json(
            session,
            path,
            {
                "symbol": symbol,
                "period": period,
                "startTime": cur,
                "endTime": chunk_end,
                "limit": OI_LIMIT,
            },
            limiter,
        )
        if data is None:
            return None
        if not data:
            cur = chunk_end
            continue
        all_rows.extend(data)
        last_ts = int(data[-1][ts_field])
        cur = last_ts + ms_per_bar

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows)
    df[ts_field] = df[ts_field].astype("int64")
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.drop_duplicates(subset=[ts_field]).sort_values(ts_field).reset_index(drop=True)
    return df


# ── 多空账户比历史（全账户） ────────────────────────────────────
async def fetch_ls_ratio_hist(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
    period: str = LS_INTERVAL,
) -> Optional[pd.DataFrame]:
    return await _fetch_futures_data_period(
        session, limiter,
        "/futures/data/globalLongShortAccountRatio",
        symbol, start_ms, end_ms, period,
        numeric_cols=["longShortRatio", "longAccount", "shortAccount"],
    )


# ── 大户多空账户比历史 ─────────────────────────────────────────
# 币安提供两个"大户"接口：
#   topLongShortAccountRatio  = 大户账户多空比（按账户数）
#   topLongShortPositionRatio = 大户持仓多空比（按金额加权）
# SOP里"大户vs散户"的策略用账户数对比更合适，但持仓数更反映实际资金

async def fetch_top_acc_ratio_hist(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
    period: str = TOP_ACC_INTERVAL,
) -> Optional[pd.DataFrame]:
    """大户账户多空比（topLongShortAccountRatio）"""
    return await _fetch_futures_data_period(
        session, limiter,
        "/futures/data/topLongShortAccountRatio",
        symbol, start_ms, end_ms, period,
        numeric_cols=["longShortRatio", "longAccount", "shortAccount"],
    )


async def fetch_top_pos_ratio_hist(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
    period: str = TOP_POS_INTERVAL,
) -> Optional[pd.DataFrame]:
    """大户持仓多空比（topLongShortPositionRatio）"""
    return await _fetch_futures_data_period(
        session, limiter,
        "/futures/data/topLongShortPositionRatio",
        symbol, start_ms, end_ms, period,
        numeric_cols=["longShortRatio", "longAccount", "shortAccount"],
    )


# ── 主动买卖比历史（近似订单簿ob_imbalance的替代） ──────────────
async def fetch_taker_ratio_hist(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
    period: str = TAKER_INTERVAL,
) -> Optional[pd.DataFrame]:
    return await _fetch_futures_data_period(
        session, limiter,
        "/futures/data/takerlongshortRatio",
        symbol, start_ms, end_ms, period,
        numeric_cols=["buySellRatio", "buyVol", "sellVol"],
    )


# ── 资金费率历史 ────────────────────────────────────────────────
async def fetch_funding_hist(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> Optional[pd.DataFrame]:
    """资金费率每8小时结算一次，限制每次1000条"""
    all_rows = []
    cur = start_ms
    # 资金费率8小时一次，1000条就是333天，一般一次拉完
    data = await fetch_json(
        session,
        "/fapi/v1/fundingRate",
        {
            "symbol": symbol,
            "startTime": cur,
            "endTime": end_ms,
            "limit": FUNDING_LIMIT,
        },
        limiter,
    )
    if data is None:
        return None
    all_rows.extend(data)

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows)
    df["fundingTime"] = df["fundingTime"].astype("int64")
    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df = df.drop_duplicates(subset=["fundingTime"]).sort_values("fundingTime").reset_index(drop=True)
    return df


# ── 单币全量拉取 ────────────────────────────────────────────────
async def fetch_symbol_all(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
    manifest: Manifest,
):
    """为单个币种拉取所有数据。若某数据已在 manifest 中则跳过"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. 4种K线
    for interval in KLINE_INTERVALS:
        dtype = f"klines_{interval}"
        if manifest.is_done(symbol, dtype):
            continue
        out = OUTPUT_DIR / f"{symbol}_{dtype}.parquet"
        try:
            df = await fetch_klines_range(session, limiter, symbol, interval, start_ms, end_ms)
            if df is not None and len(df) > 0:
                df.to_parquet(out, index=False)
                manifest.mark(symbol, dtype)
                logger.info(f"  ✓ {symbol} {dtype} ({len(df)}行)")
            else:
                logger.warning(f"  ⚠ {symbol} {dtype} 无数据")
                manifest.mark(symbol, dtype)  # 没数据也标记，下次不重试
        except Exception as e:
            logger.error(f"  ✗ {symbol} {dtype} 失败: {e}")

    # 2. OI历史
    if not manifest.is_done(symbol, "oi_5m"):
        try:
            df = await fetch_oi_hist(session, limiter, symbol, start_ms, end_ms)
            if df is not None and len(df) > 0:
                df.to_parquet(OUTPUT_DIR / f"{symbol}_oi_5m.parquet", index=False)
                logger.info(f"  ✓ {symbol} oi_5m ({len(df)}行)")
            else:
                logger.warning(f"  ⚠ {symbol} oi_5m 无数据（可能超出30天限制）")
            manifest.mark(symbol, "oi_5m")
        except Exception as e:
            logger.error(f"  ✗ {symbol} oi_5m 失败: {e}")

    # 3. 多空账户比历史（全账户）
    if not manifest.is_done(symbol, "ls_ratio_5m"):
        try:
            df = await fetch_ls_ratio_hist(session, limiter, symbol, start_ms, end_ms)
            if df is not None and len(df) > 0:
                df.to_parquet(OUTPUT_DIR / f"{symbol}_ls_ratio_5m.parquet", index=False)
                logger.info(f"  ✓ {symbol} ls_ratio_5m ({len(df)}行)")
            else:
                logger.warning(f"  ⚠ {symbol} ls_ratio_5m 无数据")
            manifest.mark(symbol, "ls_ratio_5m")
        except Exception as e:
            logger.error(f"  ✗ {symbol} ls_ratio_5m 失败: {e}")

    # 3b. 大户账户多空比
    if not manifest.is_done(symbol, "top_acc_5m"):
        try:
            df = await fetch_top_acc_ratio_hist(session, limiter, symbol, start_ms, end_ms)
            if df is not None and len(df) > 0:
                df.to_parquet(OUTPUT_DIR / f"{symbol}_top_acc_5m.parquet", index=False)
                logger.info(f"  ✓ {symbol} top_acc_5m ({len(df)}行)")
            else:
                logger.warning(f"  ⚠ {symbol} top_acc_5m 无数据")
            manifest.mark(symbol, "top_acc_5m")
        except Exception as e:
            logger.error(f"  ✗ {symbol} top_acc_5m 失败: {e}")

    # 3c. 大户持仓多空比
    if not manifest.is_done(symbol, "top_pos_5m"):
        try:
            df = await fetch_top_pos_ratio_hist(session, limiter, symbol, start_ms, end_ms)
            if df is not None and len(df) > 0:
                df.to_parquet(OUTPUT_DIR / f"{symbol}_top_pos_5m.parquet", index=False)
                logger.info(f"  ✓ {symbol} top_pos_5m ({len(df)}行)")
            else:
                logger.warning(f"  ⚠ {symbol} top_pos_5m 无数据")
            manifest.mark(symbol, "top_pos_5m")
        except Exception as e:
            logger.error(f"  ✗ {symbol} top_pos_5m 失败: {e}")

    # 4. 主动买卖量历史（OB替代）
    if not manifest.is_done(symbol, "taker_5m"):
        try:
            df = await fetch_taker_ratio_hist(session, limiter, symbol, start_ms, end_ms)
            if df is not None and len(df) > 0:
                df.to_parquet(OUTPUT_DIR / f"{symbol}_taker_5m.parquet", index=False)
                logger.info(f"  ✓ {symbol} taker_5m ({len(df)}行)")
            else:
                logger.warning(f"  ⚠ {symbol} taker_5m 无数据")
            manifest.mark(symbol, "taker_5m")
        except Exception as e:
            logger.error(f"  ✗ {symbol} taker_5m 失败: {e}")

    # 5. 资金费率
    if not manifest.is_done(symbol, "funding"):
        try:
            df = await fetch_funding_hist(session, limiter, symbol, start_ms, end_ms)
            if df is not None and len(df) > 0:
                df.to_parquet(OUTPUT_DIR / f"{symbol}_funding.parquet", index=False)
                logger.info(f"  ✓ {symbol} funding ({len(df)}行)")
            else:
                logger.warning(f"  ⚠ {symbol} funding 无数据")
            manifest.mark(symbol, "funding")
        except Exception as e:
            logger.error(f"  ✗ {symbol} funding 失败: {e}")


# ── 主入口 ──────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="拉取币安合约历史数据用于回测")
    parser.add_argument("--days", type=int, default=30, help="回看天数（默认30，OI/多空比硬上限30天）")
    parser.add_argument("--top-n", type=int, default=200, help="币种数量（默认200）")
    parser.add_argument("--symbol", nargs="+", help="指定特定币种（覆盖 top-n）")
    parser.add_argument("--resume", action="store_true", help="断点续传：跳过已拉的")
    parser.add_argument("--cleanup", action="store_true", help="清空之前的数据重新拉")
    parser.add_argument("--concurrent", type=int, default=MAX_CONCURRENT, help=f"并发请求数（默认{MAX_CONCURRENT}）")
    args = parser.parse_args()

    if args.cleanup and OUTPUT_DIR.exists():
        logger.warning(f"清空 {OUTPUT_DIR}")
        import shutil
        shutil.rmtree(OUTPUT_DIR)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # OI/多空比硬上限30天
    if args.days > 30:
        logger.warning(f"OI/多空比硬上限30天，--days={args.days}超过会无数据。已改为30天")
        args.days = 30

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - args.days * 86_400_000

    logger.info(f"拉取范围: {date_utc_str(start_ms)} ~ {date_utc_str(end_ms)} UTC (共{args.days}天)")

    # Manifest
    manifest = Manifest.load() if args.resume else Manifest()
    manifest.days = args.days
    manifest.top_n = args.top_n

    limiter = RateLimiter(args.concurrent)

    connector = aiohttp.TCPConnector(ssl=False, limit=args.concurrent * 2)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        symbols = await get_target_symbols(session, limiter, args.top_n, args.symbol)
        if not symbols:
            logger.error("无目标币种，退出")
            return

        start_time = time.time()

        # 串行跑每个币（每个币内部的5种数据已经共享limiter）
        # 如果要并发多币，改成 asyncio.gather(tasks)，但更容易触发限频
        for i, sym in enumerate(symbols):
            logger.info(f"[{i+1}/{len(symbols)}] {sym}")
            try:
                await fetch_symbol_all(session, limiter, sym, start_ms, end_ms, manifest)
            except KeyboardInterrupt:
                logger.warning("用户中断，保存 manifest")
                manifest.save()
                raise
            except Exception as e:
                logger.error(f"{sym} 整体失败: {e}")

            # 每完成10个币存一次 manifest（断点续传用）
            if (i + 1) % 10 == 0:
                manifest.save()
                elapsed = time.time() - start_time
                remaining = len(symbols) - i - 1
                eta_min = (elapsed / (i + 1)) * remaining / 60
                logger.info(f"  进度 {i+1}/{len(symbols)}  预计剩余 {eta_min:.1f} 分钟")

        manifest.save()
        elapsed = time.time() - start_time
        logger.info(f"\n全部完成，耗时 {elapsed/60:.1f} 分钟")

    # 汇总统计
    pq_files = list(OUTPUT_DIR.glob("*.parquet"))
    total_size_mb = sum(f.stat().st_size for f in pq_files) / 1024 / 1024
    logger.info(f"共生成 {len(pq_files)} 个 parquet 文件，总大小 {total_size_mb:.1f} MB")
    logger.info(f"数据目录: {OUTPUT_DIR}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断")