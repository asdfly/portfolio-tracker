"""
统计效用模块 (P1-C 回测/信号现实化)

提供量化回测中缺失的两类统计严谨性工具:
  1. Newey-West (1987) HAC 自相关-异方差稳健 t 统计量
     —— 日收益常存在序列相关(动量/反转), 普通 t 检验会高估显著性, NW 修正后更可靠。
  2. Benjamini-Hochberg (1995) FDR 多重检验校正
     —— 多信号/多指标同时回测时, 大量检验会放大伪显著, BH 控制错误发现率。
"""
import numpy as np
from typing import List, Tuple, Optional


def _lag_truncation(n: int) -> int:
    """Newey-West 滞后截断阶数 (常用金融日频经验式)。

    q = int(4 * (n/100)^(2/3)), 下限1, 上限 n-1。
    """
    q = int(4 * (n / 100.0) ** (2.0 / 3.0))
    return max(1, min(q, n - 1))


def newey_west_var(returns: np.ndarray, lags: Optional[int] = None) -> float:
    """Newey-West 异方差-自相关稳健方差估计（样本均值的方差）。

    Var_NW(ȳ) = (1/n)[γ0 + 2·Σ_{l=1}^{q}(1 - l/(q+1))·γ_l]
    其中 γ_l 为有偏自协方差估计, q 为滞后截断阶数。

    Args:
        returns: 一维收益序列 (小数, 如 0.01 表示 1%)
        lags: 手动指定滞后阶数; 为 None 时用 NW 经验截断式
    Returns:
        均值的标准误的平方 (方差); 样本不足时退回普通方差/n
    """
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    n = len(r)
    if n < 3:
        return float(np.var(r, ddof=1) / n) if n > 0 else np.nan

    mean = r.mean()
    q = lags if lags is not None else _lag_truncation(n)
    q = min(q, n - 1)

    # 有偏自协方差 γ_l = (1/n) Σ (r_t-ȳ)(r_{t-l}-ȳ)
    gamma = [0.0] * (q + 1)
    dev = r - mean
    for l in range(q + 1):
        if l == 0:
            gamma[0] = float(np.mean(dev * dev))
        else:
            gamma[l] = float(np.mean(dev[l:] * dev[:-l]))

    var = gamma[0]
    for l in range(1, q + 1):
        var += 2.0 * (1.0 - l / (q + 1.0)) * gamma[l]
    var = var / n
    return float(var)


def newey_west_tstat(returns: np.ndarray, lags: Optional[int] = None) -> float:
    """Newey-West 稳健 t 统计量: 检验收益均值是否显著非零。

    t = ȳ / sqrt(Var_NW(ȳ))
    序列相关为正(动量)时分母被放大 -> t 变小; 这正是 NW 修正的目的。
    """
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    n = len(r)
    if n < 3:
        return 0.0
    var = newey_west_var(r, lags=lags)
    if var <= 0:
        return 0.0
    return float(r.mean() / np.sqrt(var))


def benjamini_hochberg(pvals: List[float], alpha: float = 0.05
                       ) -> List[Tuple[int, float, float, bool]]:
    """Benjamini-Hochberg FDR 多重检验校正。

    Args:
        pvals: 原始 p 值列表 (0~1)
        alpha: _family-wise_ 错误发现率阈值 (默认 0.05)
    Returns:
        [(orig_idx, pval, qval_adjusted, rejected), ...] 按原索引顺序,
        qval_adjusted 为 BH 校正后 p 值, rejected 表示该检验在 alpha 下显著。
    """
    m = len(pvals)
    if m == 0:
        return []
    # 记录原始索引并升序排列
    order = sorted(range(m), key=lambda i: pvals[i])
    sorted_p = [pvals[i] for i in order]

    # 步升式拒绝阈值: 找到最大的 k 使 p_(k) <= k/m * alpha
    rej_step = [False] * m
    largest_k = -1
    for k in range(1, m + 1):
        if sorted_p[k - 1] <= (k / m) * alpha:
            largest_k = k - 1  # 0-based
    if largest_k >= 0:
        for k in range(largest_k + 1):
            rej_step[k] = True

    # 校正后 p 值 (BH adjusted): q_(i) = min_{j>=i} (m/j * p_(j)), 上限 1
    adj = [0.0] * m
    running_min = 1.0
    for k in range(m - 1, -1, -1):
        val = min(1.0, (m / (k + 1)) * sorted_p[k])
        running_min = min(running_min, val)
        adj[k] = running_min

    # 还原原顺序
    out = [None] * m
    for rank, orig in enumerate(order):
        out[orig] = (orig, pvals[orig], adj[rank], rej_step[rank])
    return out
