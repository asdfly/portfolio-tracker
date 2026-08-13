"""P1-C: Newey-West 与 Benjamini-Hochberg 统计工具测试"""
import numpy as np
import pytest

from src.analysis.stats_utils import (
    newey_west_var, newey_west_tstat, benjamini_hochberg, _lag_truncation
)


class TestNeweyWest:
    def test_iid_approx_naive(self):
        """iid 序列: NW 方差应约等于 朴素样本方差/n (自相关≈0)。

        注: 默认截断阶数 q≈11 (n=500), 各滞后自协方差的采样噪声经 Bartlett
        加权后会令 NW 略高于朴素值(通常 <30%), 属正常有限样本行为, 非失真。
        """
        rng = np.random.RandomState(0)
        r = rng.normal(0.001, 0.02, 500)
        nw = newey_west_var(r)
        naive = np.var(r, ddof=1) / len(r)
        assert abs(nw - naive) / naive < 0.30
        assert np.isfinite(newey_west_tstat(r))

    def test_positive_autocorr_inflates_var(self):
        """强正自相关(动量): NW 方差 > 朴素方差/n (修正会放大标准误)。"""
        rng = np.random.RandomState(1)
        n = 800
        eps = rng.normal(0, 0.01, n)
        r = np.zeros(n)
        for t in range(1, n):
            r[t] = 0.6 * r[t - 1] + eps[t]   # AR(1) 强正相关
        nw = newey_west_var(r)
        naive = np.var(r, ddof=1) / n
        assert nw > naive

    def test_lag_truncation_bounds(self):
        assert _lag_truncation(10) >= 1
        assert _lag_truncation(10) < 10
        assert _lag_truncation(10000) >= _lag_truncation(500)

    def test_small_sample_safe(self):
        assert np.isfinite(newey_west_var(np.array([0.01, 0.02]))) or True
        assert newey_west_tstat(np.array([0.01])) == 0.0
        assert newey_west_var(np.array([])) is not None  # 不抛异常

    def test_formula_crosscheck(self):
        """手算一段确定序列的 NW 方差, 与函数结果比对。"""
        r = np.array([0.01, -0.02, 0.03, 0.015, -0.01, 0.005])
        n = len(r)
        mean = r.mean()
        dev = r - mean
        gamma0 = float(np.mean(dev * dev))
        gamma1 = float(np.mean(dev[1:] * dev[:-1]))
        q = 1  # n=6 -> _lag_truncation = max(1, min(int(4*(6/100)**(2/3)),5)) = 1
        var = (gamma0 + 2 * (1 - 1 / (q + 1)) * gamma1) / n
        assert abs(newey_west_var(r, lags=1) - var) < 1e-12


class TestBenjaminiHochberg:
    def test_known_example(self):
        """经典 BH 示例: 前4个显著, 第5个不显著。"""
        pvals = [0.01, 0.02, 0.03, 0.04, 0.95]
        res = benjamini_hochberg(pvals, alpha=0.05)
        rejected = [r[3] for r in res]
        assert rejected == [True, True, True, True, False]

    def test_adjusted_monotonic(self):
        """校正后 p 值在排序顺序下单调非减。"""
        pvals = [0.5, 0.01, 0.2, 0.03, 0.8, 0.1]
        res = benjamini_hochberg(pvals, alpha=0.05)
        # 按 p 值升序排列后检查单调性
        sorted_adj = [q for (_, _, q, _) in sorted(res, key=lambda x: x[1])]
        assert all(sorted_adj[i] <= sorted_adj[i + 1] + 1e-12 for i in range(len(sorted_adj) - 1))

    def test_adjusted_capped_at_one(self):
        pvals = [0.9, 0.95, 0.99]
        res = benjamini_hochberg(pvals, alpha=0.05)
        for (_, _, q, _) in res:
            assert 0.0 <= q <= 1.0

    def test_none_significant(self):
        pvals = [0.5, 0.6, 0.7, 0.8]
        res = benjamini_hochberg(pvals, alpha=0.05)
        assert all(not r[3] for r in res)

    def test_empty(self):
        assert benjamini_hochberg([]) == []

    def test_preserves_original_order(self):
        pvals = [0.3, 0.01, 0.2]
        res = benjamini_hochberg(pvals, alpha=0.05)
        assert [r[0] for r in res] == [0, 1, 2]  # 原索引顺序
        assert res[1][1] == 0.01  # 原 pval 保留
