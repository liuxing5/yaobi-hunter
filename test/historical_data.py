"""
historical_data.py - 读取 fetch_historical.py 拉取的 parquet 数据

提供两类接口：
1. HistoricalDataset - 一次性加载所有币种数据到内存
2. SymbolSnapshot - 获取指定币种在某时间点的"历史快照"，模拟 scanner._analyze 的输入

用法：
    dataset = HistoricalDataset()
    snapshot = dataset.snapshot("RAVEUSDT", ts_ms)
    # snapshot.klines_1h / klines_5m / klines_15m / oi_hist / ls_hist / taker_hist / funding
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
except ImportError:
    import sys
    print("请先安装: pip install pandas pyarrow")
    sys.exit(1)

logger = logging.getLogger(__name__)

BACKTEST_DIR = Path(__file__).parent / "data" / "backtest"


@dataclass
class SymbolSnapshot:
    """指定币种在某时间点的历史快照，模拟 scanner._analyze 所需的输入"""
    symbol: str
    ts_ms: int                                  # 当前时间戳
    klines_1m: Optional[pd.DataFrame] = None
    klines_5m: Optional[pd.DataFrame] = None
    klines_15m: Optional[pd.DataFrame] = None
    klines_1h: Optional[pd.DataFrame] = None
    oi_hist: Optional[pd.DataFrame] = None      # 每5分钟
    ls_hist: Optional[pd.DataFrame] = None      # 全账户多空比
    taker_hist: Optional[pd.DataFrame] = None   # 主动买卖比（OB替代）
    top_acc_hist: Optional[pd.DataFrame] = None # 大户账户多空比（SOP "大户vs散户"）
    top_pos_hist: Optional[pd.DataFrame] = None # 大户持仓多空比
    funding: Optional[pd.DataFrame] = None

    def is_complete(self) -> bool:
        """判断数据是否齐全足以回测（订单簿无历史是预期的）"""
        return all([
            self.klines_1h is not None and len(self.klines_1h) >= 24,
            self.klines_5m is not None and len(self.klines_5m) >= 20,
            self.klines_15m is not None and len(self.klines_15m) >= 12,
            self.oi_hist is not None and len(self.oi_hist) >= 12,
        ])

    def current_price(self) -> float:
        """用1m或5m K线的最新close作为当前价"""
        for df in (self.klines_1m, self.klines_5m):
            if df is not None and len(df) > 0:
                return float(df["close"].iloc[-1])
        return 0.0

    def latest_oi_usd(self) -> float:
        """最新 OI 规模（美元）"""
        if self.oi_hist is None or len(self.oi_hist) == 0:
            return 0.0
        return float(self.oi_hist["sumOpenInterestValue"].iloc[-1])


class HistoricalDataset:
    """
    一次性加载所有币种的历史数据到内存。
    对每个币种预加载 parquet 到 DataFrame，snapshot 时按时间戳切片。

    注意：200 币 × 5 种数据 × 30天 大约 2-5GB 内存。
    如果内存紧张，改用 lazy 模式：按需加载。
    """
    def __init__(self, data_dir: Path = BACKTEST_DIR, lazy: bool = False):
        self.data_dir = Path(data_dir)
        self.lazy = lazy
        self._cache: dict[str, dict[str, pd.DataFrame]] = {}  # symbol -> {dtype: df}
        self._snapshot_cache: dict[tuple[str, int], SymbolSnapshot] = {}  # (symbol, ts_ms) -> snapshot
        if not lazy:
            self._preload_all()

    @property
    def symbols(self) -> list[str]:
        """从 manifest 读取已拉取的币种"""
        import json
        manifest_file = self.data_dir / "manifest.json"
        if not manifest_file.exists():
            return []
        try:
            m = json.loads(manifest_file.read_text())
            return list(m.get("fetched", {}).keys())
        except Exception:
            return []

    def _preload_all(self):
        symbols = self.symbols
        logger.info(f"预加载 {len(symbols)} 个币种的历史数据...")
        for i, sym in enumerate(symbols):
            self._cache[sym] = self._load_symbol(sym)
            if (i + 1) % 20 == 0:
                logger.info(f"  已加载 {i+1}/{len(symbols)}")
        mem_mb = sum(
            df.memory_usage(deep=True).sum() 
            for sym_data in self._cache.values() 
            for df in sym_data.values()
        ) / 1024 / 1024
        logger.info(f"预加载完成，占用内存约 {mem_mb:.1f} MB")

    def _load_symbol(self, symbol: str) -> dict[str, pd.DataFrame]:
        """加载单个币种的所有 parquet"""
        data = {}
        patterns = {
            "klines_1m":   f"{symbol}_klines_1m.parquet",
            "klines_5m":   f"{symbol}_klines_5m.parquet",
            "klines_15m":  f"{symbol}_klines_15m.parquet",
            "klines_1h":   f"{symbol}_klines_1h.parquet",
            "oi_5m":       f"{symbol}_oi_5m.parquet",
            "ls_ratio_5m": f"{symbol}_ls_ratio_5m.parquet",
            "taker_5m":    f"{symbol}_taker_5m.parquet",
            "top_acc_5m":  f"{symbol}_top_acc_5m.parquet",
            "top_pos_5m":  f"{symbol}_top_pos_5m.parquet",
            "funding":     f"{symbol}_funding.parquet",
        }
        for key, fname in patterns.items():
            fpath = self.data_dir / fname
            if fpath.exists():
                try:
                    data[key] = pd.read_parquet(fpath)
                except Exception as e:
                    logger.warning(f"{symbol} {key} 读取失败: {e}")
        return data

    def get_symbol_data(self, symbol: str) -> dict[str, pd.DataFrame]:
        """lazy 模式按需加载"""
        if symbol not in self._cache:
            self._cache[symbol] = self._load_symbol(symbol)
        return self._cache[symbol]

    def snapshot(self, symbol: str, ts_ms: int) -> SymbolSnapshot:
        """
        返回指定币种在 ts_ms 时刻的历史快照。
        严格保证不泄漏未来：所有 df 只返回 open_time < ts_ms 的数据。

        返回的 K线数量模拟 scanner 的 limit 参数：
            klines_1h: 最近48根
            klines_5m: 最近20根
            klines_15m: 最近12根
            oi_hist: 最近12根
            ls_hist: 最近6根
            taker_hist: 最近6根
        """
        cache_key = (symbol, ts_ms)
        if cache_key in self._snapshot_cache:
            return self._snapshot_cache[cache_key]

        snap = SymbolSnapshot(symbol=symbol, ts_ms=ts_ms)
        raw = self.get_symbol_data(symbol)

        if "klines_1h" in raw:
            df = raw["klines_1h"]
            df = df[df["open_time"] < ts_ms].tail(48)
            if len(df) > 0:
                snap.klines_1h = df

        if "klines_5m" in raw:
            df = raw["klines_5m"]
            df = df[df["open_time"] < ts_ms].tail(20)
            if len(df) > 0:
                snap.klines_5m = df

        if "klines_15m" in raw:
            df = raw["klines_15m"]
            df = df[df["open_time"] < ts_ms].tail(12)
            if len(df) > 0:
                snap.klines_15m = df

        if "klines_1m" in raw:
            df = raw["klines_1m"]
            df = df[df["open_time"] < ts_ms].tail(60)
            if len(df) > 0:
                snap.klines_1m = df

        if "oi_5m" in raw:
            df = raw["oi_5m"]
            df = df[df["timestamp"] < ts_ms].tail(12)
            if len(df) > 0:
                snap.oi_hist = df

        if "ls_ratio_5m" in raw:
            df = raw["ls_ratio_5m"]
            df = df[df["timestamp"] < ts_ms].tail(6)
            if len(df) > 0:
                snap.ls_hist = df

        if "taker_5m" in raw:
            df = raw["taker_5m"]
            df = df[df["timestamp"] < ts_ms].tail(6)
            if len(df) > 0:
                snap.taker_hist = df

        if "top_acc_5m" in raw:
            df = raw["top_acc_5m"]
            df = df[df["timestamp"] < ts_ms].tail(6)
            if len(df) > 0:
                snap.top_acc_hist = df

        if "top_pos_5m" in raw:
            df = raw["top_pos_5m"]
            df = df[df["timestamp"] < ts_ms].tail(6)
            if len(df) > 0:
                snap.top_pos_hist = df

        if "funding" in raw:
            df = raw["funding"]
            df = df[df["fundingTime"] < ts_ms].tail(3)
            if len(df) > 0:
                snap.funding = df

        # 缓存结果
        self._snapshot_cache[cache_key] = snap
        return snap

    def time_range(self) -> tuple[int, int]:
        """返回所有币种数据共同的时间范围（交集的最大start、最小end）"""
        if not self._cache:
            # 至少加载一个币探测范围
            symbols = self.symbols
            if not symbols:
                return (0, 0)
            self.get_symbol_data(symbols[0])

        starts, ends = [], []
        for sym_data in self._cache.values():
            if "klines_5m" in sym_data and len(sym_data["klines_5m"]) > 0:
                starts.append(int(sym_data["klines_5m"]["open_time"].min()))
                ends.append(int(sym_data["klines_5m"]["open_time"].max()))
        if not starts:
            return (0, 0)
        return (max(starts), min(ends))


if __name__ == "__main__":
    # 简单 smoke test
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not BACKTEST_DIR.exists():
        print(f"数据目录不存在: {BACKTEST_DIR}")
        print("请先运行: python fetch_historical.py")
        import sys
        sys.exit(1)

    ds = HistoricalDataset(lazy=True)
    symbols = ds.symbols
    print(f"可用币种数: {len(symbols)}")
    if symbols:
        sample = symbols[0]
        data = ds.get_symbol_data(sample)
        print(f"\n{sample} 数据文件:")
        for key, df in data.items():
            print(f"  {key}: {len(df)} 行")

        # 取数据中间某个时刻做 snapshot
        if "klines_5m" in data:
            mid_ts = int(data["klines_5m"]["open_time"].iloc[len(data["klines_5m"]) // 2])
            import datetime
            from datetime import timezone
            print(f"\nsnapshot @ {datetime.datetime.fromtimestamp(mid_ts/1000, tz=timezone.utc)} UTC")
            snap = ds.snapshot(sample, mid_ts)
            print(f"  klines_1h: {len(snap.klines_1h) if snap.klines_1h is not None else 0}")
            print(f"  klines_5m: {len(snap.klines_5m) if snap.klines_5m is not None else 0}")
            print(f"  klines_15m: {len(snap.klines_15m) if snap.klines_15m is not None else 0}")
            print(f"  oi_hist: {len(snap.oi_hist) if snap.oi_hist is not None else 0}")
            print(f"  ls_hist: {len(snap.ls_hist) if snap.ls_hist is not None else 0}")
            print(f"  taker_hist: {len(snap.taker_hist) if snap.taker_hist is not None else 0}")
            print(f"  top_acc_hist: {len(snap.top_acc_hist) if snap.top_acc_hist is not None else 0}")
            print(f"  top_pos_hist: {len(snap.top_pos_hist) if snap.top_pos_hist is not None else 0}")
            print(f"  funding: {len(snap.funding) if snap.funding is not None else 0}")
            print(f"  current_price: {snap.current_price()}")
            print(f"  latest_oi_usd: {snap.latest_oi_usd():.0f}")
            print(f"  complete: {snap.is_complete()}")

    t_start, t_end = ds.time_range()
    if t_start > 0:
        from datetime import datetime, timezone
        print(f"\n时间范围: {datetime.fromtimestamp(t_start/1000, tz=timezone.utc)} ~ {datetime.fromtimestamp(t_end/1000, tz=timezone.utc)}")