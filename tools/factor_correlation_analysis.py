#!/usr/bin/env python3
"""
因子相关性分析工具
- 从历史信号数据计算因子间相关系数
- 识别高相关性(>0.7)的冗余因子
- 给出权重优化建议
"""
import json
import sys
import os
from pathlib import Path
import math

# 尝试导入pandas，如果没有则使用纯Python实现
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Warning: pandas not installed, using pure Python implementation")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import Config

SIGNAL_FILE = Path(__file__).parent.parent / "data" / "historical_signals.jsonl"
CORR_THRESHOLD = 0.7  # 相关性阈值，超过则认为冗余


def load_signals_with_scores(path: Path, max_samples: int = 5000):
    """加载历史信号数据，提取score_parts作为因子分数"""
    samples = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                sig = json.loads(line)
                score_parts = sig.get("score_parts", {})
                if score_parts:
                    samples.append(score_parts)
                if len(samples) >= max_samples:
                    break
            except json.JSONDecodeError:
                continue
    return samples


def calculate_correlation_matrix(samples: list):
    """计算因子间相关系数矩阵"""
    if not samples:
        return {}, []

    # 获取所有因子名称
    factors = list(samples[0].keys())

    if HAS_PANDAS:
        # 使用pandas计算
        df = pd.DataFrame(samples)
        corr_matrix = df.corr()
        return corr_matrix.to_dict(), factors
    else:
        # 纯Python实现：计算Pearson相关系数
        # 1. 计算均值
        means = {}
        for factor in factors:
            values = [s.get(factor, 0) for s in samples]
            means[factor] = sum(values) / len(values) if values else 0

        # 2. 计算标准差
        stds = {}
        for factor in factors:
            mean = means[factor]
            values = [s.get(factor, 0) for s in samples]
            variance = sum((v - mean) ** 2 for v in values) / len(values) if values else 0
            stds[factor] = math.sqrt(variance)

        # 3. 计算相关系数
        corr_dict = {}
        for f1 in factors:
            corr_dict[f1] = {}
            for f2 in factors:
                if f1 == f2:
                    corr_dict[f1][f2] = 1.0
                    continue
                # Pearson r = cov(X,Y) / (stdX * stdY)
                mean1, mean2 = means[f1], means[f2]
                std1, std2 = stds[f1], stds[f2]
                if std1 == 0 or std2 == 0:
                    corr_dict[f1][f2] = 0.0
                    continue
                values1 = [s.get(f1, 0) for s in samples]
                values2 = [s.get(f2, 0) for s in samples]
                cov = sum((v1 - mean1) * (v2 - mean2) for v1, v2 in zip(values1, values2)) / len(values1)
                corr = cov / (std1 * std2)
                corr_dict[f1][f2] = corr

        return corr_dict, factors


def find_high_correlations(corr_dict: dict, factors: list, threshold: float = 0.7):
    """找出高相关性的因子对"""
    high_corr_pairs = []
    for i, f1 in enumerate(factors):
        for f2 in factors[i+1:]:  # 只检查上三角，避免重复
            corr = corr_dict.get(f1, {}).get(f2, 0)
            if abs(corr) >= threshold:
                high_corr_pairs.append((f1, f2, corr))
    # 按相关性绝对值降序
    high_corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)
    return high_corr_pairs


def get_factor_group(factor: str) -> str:
    """判断因子所属分组"""
    certainty_factors = ["oi_surge", "ob_imbalance", "funding_extreme", "ls_ratio", "capital_flow", "smart_dumb"]
    opportunity_factors = ["volume_spike", "price_squeeze", "momentum_breakout", "basis_diverge"]
    if factor in certainty_factors:
        return "确定性"
    elif factor in opportunity_factors:
        return "机会性"
    else:
        return "其他"


def recommend_removal(high_corr_pairs: list, cfg: Config) -> list:
    """
    根据相关性和配置给出砍因子建议
    策略：
    1. 如果两个因子同组（都是确定性或都是机会性），保留权重高的
    2. 如果跨组，优先保留确定性因子（更稳定）
    3. 考虑因子稳定性：OI、订单簿通常比动量更稳定
    """
    factor_weights = cfg.FACTOR_WEIGHTS
    to_remove = set()

    print("\n" + "="*70)
    print("相关性分析结果")
    print("="*70)

    for f1, f2, corr in high_corr_pairs:
        if f1 in to_remove or f2 in to_remove:
            continue

        w1 = factor_weights.get(f1, 0)
        w2 = factor_weights.get(f2, 0)
        g1 = get_factor_group(f1)
        g2 = get_factor_group(f2)

        print(f"\n高相关性: {f1} ↔ {f2} (r={corr:.3f})")
        print(f"  {f1}: {g1}组, 权重={w1}")
        print(f"  {f2}: {g2}组, 权重={w2}")

        # 决策逻辑
        if g1 == g2:
            # 同组：砍权重低的
            if w1 <= w2:
                to_remove.add(f1)
                print(f"  ➡️ 建议砍掉: {f1}（同组内权重较低）")
            else:
                to_remove.add(f2)
                print(f"  ➡️ 建议砍掉: {f2}（同组内权重较低）")
        else:
            # 跨组：保留确定性组
            if g1 == "确定性":
                to_remove.add(f2)
                print(f"  ➡️ 建议砍掉: {f2}（机会性因子，保留确定性）")
            elif g2 == "确定性":
                to_remove.add(f1)
                print(f"  ➡️ 建议砍掉: {f1}（机会性因子，保留确定性）")
            else:
                # 都是其他，砍权重低的
                if w1 <= w2:
                    to_remove.add(f1)
                    print(f"  ➡️ 建议砍掉: {f1}（权重较低）")
                else:
                    to_remove.add(f2)
                    print(f"  ➡️ 建议砍掉: {f2}（权重较低）")

    return list(to_remove)


def print_correlation_matrix(corr_dict: dict, factors: list):
    """打印相关系数矩阵"""
    print("\n" + "="*70)
    print("因子相关系数矩阵")
    print("="*70)

    # 列标题
    header = "Factor".ljust(20) + "".join([f[:8].center(10) for f in factors])
    print(header)
    print("-" * len(header))

    for f1 in factors:
        row = f1[:18].ljust(20)
        for f2 in factors:
            corr = corr_dict.get(f1, {}).get(f2, 0)
            if f1 == f2:
                row += "  1.00   "
            else:
                marker = "*" if abs(corr) >= CORR_THRESHOLD else " "
                row += f"{corr:+.2f}{marker}  "
        print(row)

    print("\n* = 相关性绝对值 >=", CORR_THRESHOLD)


def main():
    print("因子相关性分析工具")
    print(f"数据来源: {SIGNAL_FILE}")

    if not SIGNAL_FILE.exists():
        print(f"错误: 信号文件不存在: {SIGNAL_FILE}")
        print("请先运行历史信号生成脚本")
        return

    # 加载配置
    cfg = Config()

    # 加载样本
    print("\n加载历史信号数据...")
    samples = load_signals_with_scores(SIGNAL_FILE, max_samples=3000)
    if not samples:
        print("错误: 未能加载任何信号数据")
        return

    print(f"成功加载 {len(samples)} 条信号记录")

    # 计算相关性
    print("\n计算因子相关系数...")
    corr_dict, factors = calculate_correlation_matrix(samples)

    if not factors:
        print("错误: 未能提取因子数据")
        return

    print(f"分析因子: {factors}")

    # 打印矩阵
    print_correlation_matrix(corr_dict, factors)

    # 找出高相关性
    high_corr = find_high_correlations(corr_dict, factors, CORR_THRESHOLD)

    if not high_corr:
        print("\n✅ 未发现相关性超过阈值(>0.7)的因子对，当前因子组合合理")
        return

    print(f"\n发现 {len(high_corr)} 对高相关性因子:")
    for f1, f2, corr in high_corr:
        print(f"  {f1} ↔ {f2}: r={corr:+.3f}")

    # 给出建议
    to_remove = recommend_removal(high_corr, cfg)

    print("\n" + "="*70)
    print("优化建议总结")
    print("="*70)
    if to_remove:
        print(f"\n建议从模型中移除以下因子（共 {len(to_remove)} 个）:")
        for f in to_remove:
            w = cfg.FACTOR_WEIGHTS.get(f, 0)
            g = get_factor_group(f)
            print(f"  - {f} ({g}组, 当前权重={w})")
        print("\n移除后，建议将剩余同组因子的权重重新归一化")
    else:
        print("\n无需移除任何因子")

    # 计算剩余因子数量
    remaining = set(factors) - set(to_remove)
    print(f"\n因子数量: {len(factors)} → {len(remaining)} (减少 {len(to_remove)} 个)")


if __name__ == "__main__":
    main()
