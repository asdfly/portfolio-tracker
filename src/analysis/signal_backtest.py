"""
ETF 趋势信号回测引擎 (v3)

P0 优化 (v2):
  1. 评分公式优化 — 对数样本标度+效果量门槛+Sharpe收益+一致性分量
  2. 收益加权命中率 — 连续评分替代二值命中率
  3. 市场状态分层 — 牛/熊/震荡分别回测

P1 优化 (v3):
  4. Per-ETF 独立回测 — 数据充足的ETF单独统计，消除品种混合偏差
  5. 多信号组合确认 — 组合信号回测，过滤假信号提升置信度

P2 优化 (v4):
  6. 滚动窗口回测 — 3年训练+步长6月, 计算置信度稳定性指标
  7. 信号强度分级 — RSI/布林带按强度(mild/moderate/extreme)分档回测

对 etf_technical 表中 6 类技术指标信号进行历史回测，
量化各信号在不同前瞻时间窗口（5/10/20/30/60 交易日）下
对收益方向的预测准确率，并计算置信度评分。
"""
import logging
import math
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

# ============================================================
# 常量定义
# ============================================================

FORWARD_WINDOWS = [5, 10, 20, 30, 60]

SIGNAL_DIRECTIONS: Dict[str, Dict[str, int]] = {
    "ma_signal": {
        "多头排列": 1, "金叉": 1,
        "空头排列": -1, "死叉": -1,
    },
    "macd_signal": {
        "多头": 1, "金叉": 1, "看多": 1,
        "空头": -1, "死叉": -1,
        "中性": 0,
    },
    "rsi_status": {
        "超卖": 1, "严重超卖": 1,
        "超买": -1, "严重超买": -1,
        "正常": 0,
    },
    "kdj_signal": {
        "金叉": 1, "死叉": -1, "中性": 0,
    },
    "trend": {
        "强势上涨": 1, "温和上涨": 1,
        "下跌": -1, "温和下跌": -1, "强势下跌": -1,
        "震荡整理": 0,
    },
}

# 布林带: 数值型，特殊处理
BOLLINGER_BUY_THRESHOLD = 20
BOLLINGER_SELL_THRESHOLD = 80

# 信号强度分级
STRENGTH_ALL = "all"
STRENGTH_MILD = "mild"          # 轻度
STRENGTH_MODERATE = "moderate"  # 中度
STRENGTH_EXTREME = "extreme"    # 极端
ALL_STRENGTHS = [STRENGTH_ALL, STRENGTH_MILD, STRENGTH_MODERATE, STRENGTH_EXTREME]

# RSI 强度分档 (买入方向: 超卖区间)
RSI_STRENGTH_BUY = [
    (STRENGTH_MILD, 25, 30.01),      # 轻度超卖: 25-30
    (STRENGTH_MODERATE, 20, 25.01),   # 中度超卖: 20-25
    (STRENGTH_EXTREME, 0, 20.01),     # 极端超卖: <20
]
# RSI 强度分档 (卖出方向: 超买区间)
RSI_STRENGTH_SELL = [
    (STRENGTH_MILD, 70, 75.01),       # 轻度超买: 70-75
    (STRENGTH_MODERATE, 75, 80.01),   # 中度超买: 75-80
    (STRENGTH_EXTREME, 80, 101),      # 极端超买: >80
]

# 布林带强度分档 (买入方向: 低位)
BOLL_STRENGTH_BUY = [
    (STRENGTH_MILD, 15, 20.01),
    (STRENGTH_MODERATE, 10, 15.01),
    (STRENGTH_EXTREME, 0, 10.01),
]
# 布林带强度分档 (卖出方向: 高位)
BOLL_STRENGTH_SELL = [
    (STRENGTH_MILD, 80, 85.01),
    (STRENGTH_MODERATE, 85, 90.01),
    (STRENGTH_EXTREME, 90, 101),
]

# 滚动窗口回测参数
ROLLING_WINDOW_DAYS = 730   # 每个窗口约3年交易日
ROLLING_STEP_DAYS = 125     # 步长约6个月
ROLLING_MIN_WINDOWS = 2     # 最少需要2个窗口才计算稳定性

MIN_SAMPLE_SIZE = 10  # 最小样本量
PER_ETF_MIN_ROWS = 1000  # Per-ETF 独立回测的最低数据量

# 市场状态
REGIME_BULL = "bull"
REGIME_BEAR = "bear"
REGIME_SIDEWAYS = "sideways"
REGIME_ALL = "all"
ALL_REGIMES = [REGIME_ALL, REGIME_BULL, REGIME_BEAR, REGIME_SIDEWAYS]

# 市场状态分类参数
REGIME_MA_PERIOD = 20
REGIME_SLOPE_PERIOD = 5
MARKET_PROXY_CODE = "510300"

# 窗口权重
WINDOW_WEIGHTS = {5: 0.10, 10: 0.15, 20: 0.25, 30: 0.25, 60: 0.25}

# 效果量门槛
EFFECT_SIZE_THRESHOLD = 0.05

# Per-ETF 回测标记
SCOPE_ALL = "all"      # 全市场统计
SCOPE_ETF = "etf"      # Per-ETF 统计

# ============================================================
# 多信号组合定义
# ============================================================

# 组合信号: (名称, 条件列表, 方向)
# 每个条件: (indicator, signal_values, direction)
# 同一行数据中所有条件同时满足时触发组合信号
COMBO_SIGNALS: List[Dict] = [
    {
        "name": "RSI超卖+MACD金叉",
        "conditions": [
            ("rsi_status", ["超卖", "严重超卖"], 1),
            ("macd_signal", ["金叉", "看多", "多头"], 1),
        ],
        "direction": 1,
    },
    {
        "name": "RSI超卖+布林带低位",
        "conditions": [
            ("rsi_status", ["超卖", "严重超卖"], 1),
            ("bollinger", ["low"], 1),
        ],
        "direction": 1,
    },
    {
        "name": "RSI超买+布林带高位",
        "conditions": [
            ("rsi_status", ["超买", "严重超买"], -1),
            ("bollinger", ["high"], -1),
        ],
        "direction": -1,
    },
    {
        "name": "多头排列+趋势上涨",
        "conditions": [
            ("ma_signal", ["多头排列"], 1),
            ("trend", ["强势上涨", "温和上涨"], 1),
        ],
        "direction": 1,
    },
    {
        "name": "空头排列+趋势下跌",
        "conditions": [
            ("ma_signal", ["空头排列"], -1),
            ("trend", ["下跌", "温和下跌", "强势下跌"], -1),
        ],
        "direction": -1,
    },
    {
        "name": "KDJ金叉+MACD看多",
        "conditions": [
            ("kdj_signal", ["金叉"], 1),
            ("macd_signal", ["金叉", "看多", "多头"], 1),
        ],
        "direction": 1,
    },
    {
        "name": "KDJ死叉+MACD空头",
        "conditions": [
            ("kdj_signal", ["死叉"], -1),
            ("macd_signal", ["死叉", "空头"], -1),
        ],
        "direction": -1,
    },
    {
        "name": "RSI严重超买+趋势强势",
        "conditions": [
            ("rsi_status", ["严重超买"], -1),
            ("trend", ["强势上涨"], 1),
        ],
        "direction": -1,  # 反转信号
    },
    {
        "name": "RSI严重超卖+趋势强势跌",
        "conditions": [
            ("rsi_status", ["严重超卖"], 1),
            ("trend", ["强势下跌"], -1),
        ],
        "direction": 1,  # 反转信号
    },
]


# ============================================================
# 置信度评分 v2
# ============================================================

def compute_confidence(n: int, hit_rate: float, p_value: float,
                       avg_return: float, std_return: float = 0.0,
                       weighted_hit_rate: Optional[float] = None) -> Tuple[float, str]:
    """计算置信度评分 v2 (0-100) 和等级 (A/B/C/D)。"""
    # 1. 预测力 (40分)
    if weighted_hit_rate is not None and not math.isnan(weighted_hit_rate):
        predictive = min(abs(weighted_hit_rate) * 200, 40)
    else:
        predictive = min(abs(hit_rate - 0.5) * 80, 40)

    # 2. 样本量 (15分) — 对数标度
    sample_component = min(math.log10(max(n, 1)) / math.log10(500), 1.0) * 15

    # 3. 统计显著性 (25分) — p值 + 效果量双重门槛
    effect_size = abs(hit_rate - 0.5)
    has_effect = effect_size > EFFECT_SIZE_THRESHOLD

    if p_value < 0.01:
        sig_component = 25 if has_effect else 15
    elif p_value < 0.05:
        sig_component = 20 if has_effect else 10
    elif p_value < 0.10:
        sig_component = 8
    else:
        sig_component = 0

    # 4. 风险调整收益 (20分) — Sharpe 比率形式
    if std_return > 1e-8:
        annualized_std = std_return * math.sqrt(252)
        sharpe = abs(avg_return) / annualized_std
        return_component = min(sharpe / 2.0, 1.0) * 20
    else:
        return_component = min(abs(avg_return) / 0.05, 1.0) * 20

    score = round(predictive + sample_component + sig_component + return_component, 1)
    grade = "A" if score >= 70 else "B" if score >= 50 else "C" if score >= 30 else "D"
    return score, grade


def _compute_weighted_hit_rate(returns: pd.Series, direction: int) -> float:
    """计算收益加权命中率 [-1, 1]。"""
    if len(returns) == 0:
        return 0.0
    signed_returns = returns * (1 if direction > 0 else -1)
    total_abs = returns.abs().sum()
    if total_abs < 1e-10:
        return 0.0
    return float(signed_returns.sum() / total_abs)


def _compute_consistency_bonus(hit_rates: List[float]) -> float:
    """计算窗口间一致性加成 (-5 到 +5)。"""
    if len(hit_rates) < 2:
        return 0.0
    above = sum(1 for hr in hit_rates if hr > 0.5)
    below = sum(1 for hr in hit_rates if hr < 0.5)
    if above == 0 or below == 0:
        return 5.0
    return -5.0



# ============================================================
# 信号强度分级
# ============================================================

def _classify_rsi_strength(rsi_value, direction: int) -> str:
    """对RSI信号进行强度分级。"""
    if pd.isna(rsi_value):
        return STRENGTH_ALL
    v = float(rsi_value)
    tiers = RSI_STRENGTH_BUY if direction > 0 else RSI_STRENGTH_SELL
    for strength, lo, hi in tiers:
        if lo <= v < hi:
            return strength
    return STRENGTH_ALL

def _classify_bollinger_strength(boll_pos, direction: int) -> str:
    """对布林带信号进行强度分级。"""
    if pd.isna(boll_pos):
        return STRENGTH_ALL
    v = float(boll_pos)
    tiers = BOLL_STRENGTH_BUY if direction > 0 else BOLL_STRENGTH_SELL
    for strength, lo, hi in tiers:
        if lo <= v < hi:
            return strength
    return STRENGTH_ALL

# ============================================================
# 滚动窗口稳定性
# ============================================================

def _compute_rolling_window_stability(df: pd.DataFrame, indicator: str,
                                       signal_val: str, direction: int,
                                       n: int) -> Optional[float]:
    """计算滚动窗口置信度稳定性指标 (0-1, 越高越稳定)。

    将历史数据按 ROLLING_WINDOW_DAYS 分为滚动时间窗口，
    分别计算各窗口置信度，返回变异系数的逆变换。
    """
    ret_col = f"fwd_ret_{n}"
    if ret_col not in df.columns:
        return None

    # 过滤信号匹配的数据
    if indicator == "bollinger":
        if "低位" in signal_val:
            mask = df["bollinger_position"] <= BOLLINGER_BUY_THRESHOLD
        elif "高位" in signal_val:
            mask = df["bollinger_position"] >= BOLLINGER_SELL_THRESHOLD
        else:
            return None
    elif indicator == "combo":
        return None  # 组合信号不参与滚动窗口稳定性
    else:
        mask = df[indicator] == signal_val

    subset = df[mask & df[ret_col].notna()].sort_values("date")
    if len(subset) < MIN_SAMPLE_SIZE * ROLLING_MIN_WINDOWS:
        return None

    returns = subset[ret_col].values
    total_len = len(subset)

    window_confs = []
    start = 0
    while start + MIN_SAMPLE_SIZE <= total_len:
        end = min(start + ROLLING_WINDOW_DAYS, total_len)
        window_returns = returns[start:end]
        if len(window_returns) < MIN_SAMPLE_SIZE:
            break

        hits = int(((direction > 0) & (window_returns > 0)).sum() +
                   ((direction < 0) & (window_returns < 0)).sum())
        hr = hits / len(window_returns)
        whr = _compute_weighted_hit_rate(pd.Series(window_returns), direction)
        avg_ret = float(np.mean(window_returns))
        std_ret = float(np.std(window_returns)) if len(window_returns) > 1 else 0.0

        se = 0.5 / (len(window_returns) ** 0.5)
        t_stat = (hr - 0.5) / se if se > 0 else 0.0
        p_value = 2 * (1 - stats.norm.cdf(abs(t_stat)))

        conf, _ = compute_confidence(
            len(window_returns), hr, p_value, avg_ret, std_ret, whr)
        window_confs.append(conf)

        start += ROLLING_STEP_DAYS

    if len(window_confs) < ROLLING_MIN_WINDOWS:
        return None

    # 稳定性: 1 / (1 + cv), cv = std/mean
    mean_conf = float(np.mean(window_confs))
    std_conf = float(np.std(window_confs))
    cv = std_conf / mean_conf if mean_conf > 1e-6 else 1.0
    stability = 1.0 / (1.0 + cv)
    return round(stability, 4)

# ============================================================
# 市场状态分类
# ============================================================

def _classify_market_regime(df: pd.DataFrame) -> pd.Series:
    """为每行数据打上市场状态标签。"""
    proxy = df[df["code"] == MARKET_PROXY_CODE].copy()
    if proxy.empty:
        first_code = df["code"].iloc[0]
        proxy = df[df["code"] == first_code].copy()

    proxy = proxy.sort_values("date").reset_index(drop=True)
    proxy["ma"] = proxy["close"].rolling(REGIME_MA_PERIOD, min_periods=1).mean()
    proxy["ma_slope"] = proxy["ma"].diff(REGIME_SLOPE_PERIOD)
    proxy["regime"] = REGIME_SIDEWAYS
    above_ma = proxy["close"] > proxy["ma"]
    ma_rising = proxy["ma_slope"] > 0
    proxy.loc[above_ma & ma_rising, "regime"] = REGIME_BULL
    proxy.loc[~above_ma & ~ma_rising, "regime"] = REGIME_BEAR

    date_regime = dict(zip(proxy["date"], proxy["regime"]))
    return df["date"].map(date_regime).fillna(REGIME_SIDEWAYS)


# ============================================================
# 回测核心
# ============================================================

def _load_tech_with_price(conn) -> pd.DataFrame:
    """从数据库加载技术指标 + 价格数据"""
    tech = pd.read_sql_query(
        "SELECT date, code, ma_signal, macd_signal, rsi_value, rsi_status, "
        "kdj_signal, bollinger_position, trend, atr_pct "
        "FROM etf_technical ORDER BY code, date", conn)

    prices = pd.read_sql_query(
        "SELECT date, code, current_price AS close "
        "FROM portfolio_snapshots ORDER BY code, date", conn)

    df = tech.merge(prices, on=["date", "code"], how="inner")
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    logger.info(f"信号回测数据: {len(df)} 行, {df['code'].nunique()} 只ETF, "
                f"{df['date'].min()} ~ {df['date'].max()}")
    return df


def _compute_forward_returns(df: pd.DataFrame,
                             windows: List[int] = None) -> pd.DataFrame:
    """计算前瞻收益"""
    if windows is None:
        windows = FORWARD_WINDOWS

    for n in windows:
        df[f"fwd_ret_{n}"] = df.groupby("code")["close"].transform(
            lambda x: x.shift(-n) / x - 1
        )
    return df


def _backtest_single(subset: pd.DataFrame, indicator: str,
                     signal_val: str, direction: int,
                     n: int, regime: str,
                     code: str = None, scope: str = SCOPE_ALL,
                     signal_strength: str = STRENGTH_ALL,
                     stability_score: Optional[float] = None) -> Optional[dict]:
    """对单个 信号×窗口×市场状态 组合进行回测统计。"""
    ret_col = f"fwd_ret_{n}"
    valid = subset[ret_col].dropna()
    n_samples = len(valid)
    if n_samples < MIN_SAMPLE_SIZE:
        return None

    hits = int(((direction > 0) & (valid > 0)).sum() +
               ((direction < 0) & (valid < 0)).sum())
    hit_rate = hits / n_samples

    whr = _compute_weighted_hit_rate(valid, direction)

    avg_ret = float(valid.mean())
    std_ret = float(valid.std()) if n_samples > 1 else 0.0

    se = 0.5 / (n_samples ** 0.5)
    t_stat = (hit_rate - 0.5) / se if se > 0 else 0.0
    p_value = 2 * (1 - stats.norm.cdf(abs(t_stat)))

    conf, grade = compute_confidence(
        n_samples, hit_rate, p_value, avg_ret, std_ret, whr)

    # 稳定性调整: 高稳定性维持置信度, 低稳定性降权
    if stability_score is not None and not math.isnan(stability_score):
        adjusted_conf = round(conf * (0.7 + 0.3 * stability_score), 1)
        adjusted_grade = ("A" if adjusted_conf >= 70 else
                          "B" if adjusted_conf >= 50 else
                          "C" if adjusted_conf >= 30 else "D")
    else:
        adjusted_conf = conf
        adjusted_grade = grade

    return {
        "indicator": indicator,
        "signal_value": signal_val,
        "signal_direction": direction,
        "forward_window": n,
        "market_regime": regime,
        "scope": scope,
        "code": code,
        "signal_strength": signal_strength,
        "stability_score": stability_score,
        "sample_count": n_samples,
        "hit_count": hits,
        "hit_rate": round(hit_rate, 4),
        "weighted_hit_rate": round(whr, 4),
        "avg_return": round(avg_ret, 6),
        "std_return": round(std_ret, 6),
        "t_statistic": round(t_stat, 4),
        "p_value": round(p_value, 6),
        "confidence_score": adjusted_conf,
        "confidence_grade": adjusted_grade,
    }


def _backtest_categorical(df: pd.DataFrame, indicator: str,
                          direction_map: Dict[str, int],
                          windows: List[int],
                          code: str = None,
                          scope: str = SCOPE_ALL) -> List[dict]:
    """对分类信号进行回测 (含市场状态分层 + RSI强度分级)。"""
    results = []
    for signal_val, direction in direction_map.items():
        if direction == 0:
            continue
        mask = df[indicator] == signal_val
        subset_all = df[mask]

        # 计算滚动窗口稳定性 (仅全市场+all状态)
        stability_map = {}
        if scope == SCOPE_ALL:
            for n in windows:
                stab = _compute_rolling_window_stability(
                    df, indicator, signal_val, direction, n)
                stability_map[n] = stab

        for regime in ALL_REGIMES:
            if regime == REGIME_ALL:
                subset = subset_all
            else:
                subset = subset_all[subset_all["market_regime"] == regime]

            for n in windows:
                r = _backtest_single(subset, indicator, signal_val,
                                     direction, n, regime, code, scope,
                                     STRENGTH_ALL, stability_map.get(n))
                if r:
                    results.append(r)

        # RSI 强度分级回测 (仅在 rsi_value 列可用时)
        if indicator == "rsi_status" and "rsi_value" in df.columns:
            for strength_tier in [STRENGTH_MILD, STRENGTH_MODERATE, STRENGTH_EXTREME]:
                # 根据方向选择分档区间
                tiers = RSI_STRENGTH_BUY if direction > 0 else RSI_STRENGTH_SELL
                tier_def = next((t for t in tiers if t[0] == strength_tier), None)
                if tier_def is None:
                    continue
                _, lo, hi = tier_def

                strength_mask = mask & (df["rsi_value"] >= lo) & (df["rsi_value"] < hi)
                strength_subset_all = df[strength_mask]

                # 强度分级的稳定性 (仅全市场)
                strength_stab_map = {}
                if scope == SCOPE_ALL:
                    for n in windows:
                        # 用强度过滤后的数据计算稳定性
                        strength_df = df[strength_mask].copy()
                        stab = _compute_rolling_window_stability(
                            strength_df, indicator, signal_val, direction, n)
                        strength_stab_map[n] = stab

                for regime in ALL_REGIMES:
                    if regime == REGIME_ALL:
                        subset = strength_subset_all
                    else:
                        subset = strength_subset_all[strength_subset_all["market_regime"] == regime]

                    for n in windows:
                        r = _backtest_single(subset, indicator, signal_val,
                                             direction, n, regime, code, scope,
                                             strength_tier, strength_stab_map.get(n))
                        if r:
                            results.append(r)

    return results


def _backtest_bollinger(df: pd.DataFrame, windows: List[int],
                        code: str = None,
                        scope: str = SCOPE_ALL) -> List[dict]:
    """对布林带数值型信号进行回测 (含市场状态分层 + 强度分级)。"""
    results = []
    boll = df["bollinger_position"].dropna()
    if boll.empty:
        return results

    for direction, lo, hi, label in [
        (1, 0, BOLLINGER_BUY_THRESHOLD, "低位(≤20)"),
        (-1, BOLLINGER_SELL_THRESHOLD, 100, "高位(≥80)"),
    ]:
        mask = (df["bollinger_position"] >= lo) & (df["bollinger_position"] < hi + 1)
        subset_all = df[mask]

        # 滚动窗口稳定性 (仅全市场)
        stability_map = {}
        if scope == SCOPE_ALL:
            for n in windows:
                stab = _compute_rolling_window_stability(
                    df, "bollinger", label, direction, n)
                stability_map[n] = stab

        # 全强度回测 (strength=all)
        for regime in ALL_REGIMES:
            if regime == REGIME_ALL:
                subset = subset_all
            else:
                subset = subset_all[subset_all["market_regime"] == regime]

            for n in windows:
                r = _backtest_single(subset, "bollinger", label,
                                     direction, n, regime, code, scope,
                                     STRENGTH_ALL, stability_map.get(n))
                if r:
                    results.append(r)

        # 强度分档回测
        strength_tiers = BOLL_STRENGTH_BUY if direction > 0 else BOLL_STRENGTH_SELL
        for strength_tier, s_lo, s_hi in strength_tiers:
            s_mask = mask & (df["bollinger_position"] >= s_lo) & (df["bollinger_position"] < s_hi)
            s_subset_all = df[s_mask]

            # 强度分档稳定性
            s_stab_map = {}
            if scope == SCOPE_ALL:
                for n in windows:
                    s_df = df[s_mask].copy()
                    stab = _compute_rolling_window_stability(
                        s_df, "bollinger", label, direction, n)
                    s_stab_map[n] = stab

            for regime in ALL_REGIMES:
                if regime == REGIME_ALL:
                    subset = s_subset_all
                else:
                    subset = s_subset_all[s_subset_all["market_regime"] == regime]

                for n in windows:
                    r = _backtest_single(subset, "bollinger", label,
                                         direction, n, regime, code, scope,
                                         strength_tier, s_stab_map.get(n))
                    if r:
                        results.append(r)

    return results


# ============================================================
# 多信号组合回测
# ============================================================

def _evaluate_combo_condition(row: pd.Series, indicator: str,
                              signal_values: List[str],
                              direction: int) -> bool:
    """检查单行数据是否满足组合信号中的一个条件。"""
    if indicator == "bollinger":
        val = row.get("bollinger_position")
        if pd.isna(val):
            return False
        v = float(val)
        if "low" in signal_values and v <= BOLLINGER_BUY_THRESHOLD:
            return True
        if "high" in signal_values and v >= BOLLINGER_SELL_THRESHOLD:
            return True
        return False

    cell_val = str(row.get(indicator, ""))
    if cell_val in signal_values:
        return True
    return False


def _backtest_combos(df: pd.DataFrame, windows: List[int],
                     code: str = None,
                     scope: str = SCOPE_ALL) -> List[dict]:
    """对多信号组合进行回测。"""
    results = []
    for combo in COMBO_SIGNALS:
        combo_name = combo["name"]
        combo_dir = combo["direction"]
        conditions = combo["conditions"]

        # 检查每行是否满足所有条件
        mask = pd.Series(True, index=df.index)
        for ind, sig_vals, cond_dir in conditions:
            cond_mask = df.apply(
                lambda row: _evaluate_combo_condition(row, ind, sig_vals, cond_dir),
                axis=1
            )
            mask = mask & cond_mask

        subset_all = df[mask]
        if len(subset_all) < MIN_SAMPLE_SIZE:
            continue

        for regime in ALL_REGIMES:
            if regime == REGIME_ALL:
                subset = subset_all
            else:
                subset = subset_all[subset_all["market_regime"] == regime]

            for n in windows:
                r = _backtest_single(
                    subset, "combo", combo_name,
                    combo_dir, n, regime, code, scope,
                    STRENGTH_ALL, None
                )
                if r:
                    results.append(r)

    return results


# ============================================================
# 回测主函数
# ============================================================

def run_backtest(conn=None) -> pd.DataFrame:
    """执行完整回测，返回统计结果 DataFrame。

    v4 改进:
      - 信号强度分级: RSI/布林带按 mild/moderate/extreme 分档统计
      - 滚动窗口稳定性: 3年窗口+6月步长, 计算置信度稳定性指标
      - 保留 v3: Per-ETF 独立回测 + 多信号组合 + 市场状态分层
    """
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        df = _load_tech_with_price(conn)
        df = _compute_forward_returns(df)

        # 市场状态分类
        df["market_regime"] = _classify_market_regime(df)
        regime_counts = df["market_regime"].value_counts()
        logger.info(f"市场状态分布: {dict(regime_counts)}")

        all_results = []

        # ---- 1. 全市场回测 (scope=all) ----
        for ind_col, dir_map in SIGNAL_DIRECTIONS.items():
            all_results.extend(
                _backtest_categorical(df, ind_col, dir_map, FORWARD_WINDOWS))
        all_results.extend(_backtest_bollinger(df, FORWARD_WINDOWS))

        # ---- 2. Per-ETF 回测 (scope=etf) ----
        etf_counts = df.groupby("code").size()
        per_etf_codes = etf_counts[etf_counts >= PER_ETF_MIN_ROWS].index.tolist()
        logger.info(f"Per-ETF回测: {len(per_etf_codes)} 只ETF (>= {PER_ETF_MIN_ROWS} 行)")

        for code in per_etf_codes:
            etf_df = df[df["code"] == code]
            for ind_col, dir_map in SIGNAL_DIRECTIONS.items():
                all_results.extend(
                    _backtest_categorical(etf_df, ind_col, dir_map,
                                          FORWARD_WINDOWS, code, SCOPE_ETF))
            all_results.extend(
                _backtest_bollinger(etf_df, FORWARD_WINDOWS, code, SCOPE_ETF))

        # ---- 3. 多信号组合回测 (全市场 + Per-ETF) ----
        all_results.extend(_backtest_combos(df, FORWARD_WINDOWS))
        for code in per_etf_codes:
            etf_df = df[df["code"] == code]
            all_results.extend(
                _backtest_combos(etf_df, FORWARD_WINDOWS, code, SCOPE_ETF))

        result_df = pd.DataFrame(all_results)
        n_all = len(result_df[result_df["scope"] == SCOPE_ALL])
        n_etf = len(result_df[result_df["scope"] == SCOPE_ETF])
        n_combo = len(result_df[result_df["indicator"] == "combo"])
        n_strength = len(result_df[result_df["signal_strength"] != STRENGTH_ALL]) if "signal_strength" in result_df.columns else 0
        n_stable = result_df["stability_score"].notna().sum() if "stability_score" in result_df.columns else 0
        logger.info(f"回测完成 (v4): {len(result_df)} 组统计 "
                    f"(全市场={n_all}, Per-ETF={n_etf}, 组合={n_combo}, "
                    f"强度分级={n_strength}, 稳定性={n_stable})")
        return result_df
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 数据库写入
# ============================================================

def save_backtest_results(result_df: pd.DataFrame, conn=None) -> int:
    """保存回测结果到 signal_backtest_stats 表 (v3 schema)"""
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        conn.execute("DROP TABLE IF EXISTS signal_backtest_stats")
        conn.execute("""
            CREATE TABLE signal_backtest_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator TEXT NOT NULL,
                signal_value TEXT NOT NULL,
                signal_direction INTEGER NOT NULL,
                forward_window INTEGER NOT NULL,
                market_regime TEXT NOT NULL DEFAULT 'all',
                scope TEXT NOT NULL DEFAULT 'all',
                code TEXT,
                signal_strength TEXT NOT NULL DEFAULT 'all',
                stability_score REAL,
                sample_count INTEGER NOT NULL,
                hit_count INTEGER NOT NULL,
                hit_rate REAL NOT NULL,
                weighted_hit_rate REAL,
                avg_return REAL NOT NULL,
                std_return REAL,
                t_statistic REAL,
                p_value REAL,
                confidence_score REAL NOT NULL,
                confidence_grade TEXT,
                backtest_date TEXT NOT NULL,
                UNIQUE(indicator, signal_value, forward_window, market_regime, scope, code, signal_strength)
            )
        """)
        today = datetime.now().strftime("%Y-%m-%d")
        rows = result_df.copy()
        rows["backtest_date"] = today

        # 分批写入避免 SQLite 变量数限制
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows.iloc[i:i + batch_size]
            batch.to_sql("signal_backtest_stats", conn, if_exists="append",
                        index=False, method="multi")
        conn.commit()
        logger.info(f"回测结果已保存: {len(rows)} 行 ({(len(rows)-1)//batch_size + 1} 批)")
        return len(rows)
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 当前信号置信度
# ============================================================

def _get_bollinger_signal(val) -> Tuple[str, int]:
    """布林带数值 → (信号标签, 方向)"""
    if pd.isna(val):
        return ("中性", 0)
    v = float(val)
    if v <= BOLLINGER_BUY_THRESHOLD:
        return (f"低位({v:.0f}%)", 1)
    if v >= BOLLINGER_SELL_THRESHOLD:
        return (f"高位({v:.0f}%)", -1)
    return (f"中位({v:.0f}%)", 0)


def _get_current_market_regime(conn) -> str:
    """获取当前市场状态。"""
    proxy = pd.read_sql_query(
        f"SELECT date, current_price AS close FROM portfolio_snapshots "
        f"WHERE code = '{MARKET_PROXY_CODE}' ORDER BY date", conn)
    if len(proxy) < REGIME_MA_PERIOD + REGIME_SLOPE_PERIOD:
        return REGIME_SIDEWAYS

    proxy = proxy.tail(REGIME_MA_PERIOD + REGIME_SLOPE_PERIOD).reset_index(drop=True)
    ma = proxy["close"].rolling(REGIME_MA_PERIOD, min_periods=1).mean().iloc[-1]
    ma_prev = proxy["close"].rolling(REGIME_MA_PERIOD, min_periods=1).mean().iloc[-1 - REGIME_SLOPE_PERIOD]
    price = proxy["close"].iloc[-1]
    slope = ma - ma_prev

    if price > ma and slope > 0:
        return REGIME_BULL
    if price < ma and slope < 0:
        return REGIME_BEAR
    return REGIME_SIDEWAYS


def _get_current_combo_signals(row: pd.Series) -> List[Tuple[str, int]]:
    """检查当前ETF数据行是否触发任何组合信号。

    Returns:
        [(combo_name, direction), ...]
    """
    triggered = []
    for combo in COMBO_SIGNALS:
        conditions = combo["conditions"]
        all_met = True
        for ind, sig_vals, cond_dir in conditions:
            if not _evaluate_combo_condition(row, ind, sig_vals, cond_dir):
                all_met = False
                break
        if all_met:
            triggered.append((combo["name"], combo["direction"]))
    return triggered


# ============================================================
# 方向净值评分 + 矛盾检测 (方案一 + 方案三)
# ============================================================

# 信号等级权重
GRADE_WEIGHTS = {"A": 1.0, "B": 0.7, "C": 0.4, "D": 0.1}

# 矛盾类型检测规则
# 每条规则: (看多指标集合, 看空指标集合, 矛盾描述模板)
_CONFLICT_RULES = [
    # 趋势vs超买
    ({"ma_signal", "trend"}, {"rsi_status"},
     "趋势上行但短期超涨"),
    ({"ma_signal", "trend"}, {"bollinger"},
     "趋势上行但估值偏高"),
    ({"ma_signal"}, {"rsi_status", "bollinger"},
     "中期趋势偏多但短期指标超买"),
    # 短期vs长期
    ({"kdj_signal", "macd_signal"}, {"ma_signal", "trend"},
     "短期反弹但中期趋势偏空"),
    ({"kdj_signal"}, {"ma_signal"},
     "短期金叉但均线空头排列"),
    # 位置vs趋势
    ({"bollinger"}, {"ma_signal", "trend"},
     "布林带低位但趋势偏空"),
    ({"bollinger"}, {"trend"},
     "布林带低位但趋势下行"),
    # 通用
    (set(), set(),
     "多空信号并存"),
]


def _compute_direction_net_score(etf_signals):
    """计算单只ETF的方向净值评分

    Args:
        etf_signals: 该ETF的所有信号行(list of dict)

    Returns:
        {"direction_net_score": float, "direction_label": str,
         "bull_score": float, "bear_score": float}
    """
    bull_score = 0.0
    bear_score = 0.0

    for sig in etf_signals:
        conf = sig.get("composite_confidence")
        grade = sig.get("composite_grade")
        direction = sig.get("signal_direction", 0)

        if conf is None or direction == 0:
            continue

        weight = GRADE_WEIGHTS.get(grade, 0.5)
        weighted = conf * weight

        if direction > 0:
            bull_score += weighted
        else:
            bear_score += weighted

    net = bull_score - bear_score

    if abs(net) <= 50:
        label = "MIXED"
    elif net > 0:
        label = "BULL"
    else:
        label = "BEAR"

    return {
        "direction_net_score": round(net, 1),
        "direction_label": label,
        "bull_score": round(bull_score, 1),
        "bear_score": round(bear_score, 1),
    }


def _detect_signal_conflict(etf_signals):
    """检测单只ETF的信号矛盾类型

    当 direction_label == MIXED 时调用。

    Args:
        etf_signals: 该ETF的所有信号行

    Returns:
        矛盾类型描述字符串，无矛盾时返回空字符串
    """
    bull_indicators = set()
    bear_indicators = set()

    for sig in etf_signals:
        conf = sig.get("composite_confidence")
        grade = sig.get("composite_grade")
        direction = sig.get("signal_direction", 0)
        indicator = sig.get("indicator", "")

        if conf is None or direction == 0:
            continue
        if grade not in ("A", "B"):
            continue

        if direction > 0:
            bull_indicators.add(indicator)
        else:
            bear_indicators.add(indicator)

    if not bull_indicators or not bear_indicators:
        return ""

    for bull_set, bear_set, desc in _CONFLICT_RULES:
        if bull_set and bear_set:
            bull_match = bool(bull_indicators & bull_set)
            bear_match = bool(bear_indicators & bear_set)
            if bull_match and bear_match:
                return desc
        elif not bull_set and not bear_set:
            return desc

    return "多空信号并存"


def get_current_confidence(conn=None) -> pd.DataFrame:
    """获取当前各ETF最新信号及其置信度 (v3)。

    v4 改进:
      - 信号强度匹配: 根据当前RSI/布林带数值匹配强度档位统计
      - 稳定性加权: 置信度已含稳定性调整 (v4 回测时计算)

    v3 改进:
      - Per-ETF 优先: 优先使用该ETF的独立回测结果 (scope=etf),
        回退到全市场 (scope=all)
      - 组合信号: 检查当前是否触发组合信号，附加组合置信度

    Returns:
        DataFrame with confidence per ETF per indicator + combo signals
    """
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM signal_backtest_stats").fetchone()[0]
        if cnt == 0:
            logger.warning("signal_backtest_stats 表为空")
            return pd.DataFrame()

        latest = pd.read_sql_query("""
            SELECT t.date, t.code, t.ma_signal, t.macd_signal,
                   t.rsi_value, t.rsi_status, t.kdj_signal,
                   t.bollinger_position, t.trend, t.atr_pct,
                   s.name
            FROM etf_technical t
            LEFT JOIN portfolio_snapshots s
                ON t.code = s.code AND t.date = s.date
            WHERE t.date = (SELECT MAX(date) FROM etf_technical)
        """, conn)
        if latest.empty:
            return pd.DataFrame()

        current_regime = _get_current_market_regime(conn)
        logger.info(f"当前市场状态: {current_regime}")

        stats_df = pd.read_sql_query(
            "SELECT * FROM signal_backtest_stats", conn)

        # 构建查找索引
        # (indicator, signal_value, forward_window, market_regime, scope, code, signal_strength) → row
        stats_lookup = {}
        for _, r in stats_df.iterrows():
            strength = r.get("signal_strength", STRENGTH_ALL) if pd.notna(r.get("signal_strength")) else STRENGTH_ALL
            key = (r["indicator"], r["signal_value"], int(r["forward_window"]),
                   r["market_regime"], r["scope"],
                   r["code"] if pd.notna(r["code"]) else None,
                   strength)
            stats_lookup[key] = r

        def _lookup(ind, sig_val, n, regime, code=None, strength=STRENGTH_ALL):
            """查找回测统计: 强度匹配 → all强度, Per-ETF → 全市场, 指定状态 → all"""
            # 1. 尝试指定强度: Per-ETF + 指定状态
            if code:
                key = (ind, sig_val, n, regime, SCOPE_ETF, code, strength)
                if key in stats_lookup:
                    return stats_lookup[key]
            # 2. 指定强度: 全市场 + 指定状态
            key = (ind, sig_val, n, regime, SCOPE_ALL, None, strength)
            if key in stats_lookup:
                return stats_lookup[key]
            # 3. 回退到 all 强度: Per-ETF + 指定状态
            if code:
                key = (ind, sig_val, n, regime, SCOPE_ETF, code, STRENGTH_ALL)
                if key in stats_lookup:
                    return stats_lookup[key]
            # 4. all 强度: 全市场 + 指定状态
            key = (ind, sig_val, n, regime, SCOPE_ALL, None, STRENGTH_ALL)
            if key in stats_lookup:
                return stats_lookup[key]
            # 5. all 强度: Per-ETF + all 状态
            if code:
                key = (ind, sig_val, n, REGIME_ALL, SCOPE_ETF, code, STRENGTH_ALL)
                if key in stats_lookup:
                    return stats_lookup[key]
            # 6. 回退: 全市场 + all 状态 + all 强度
            key = (ind, sig_val, n, REGIME_ALL, SCOPE_ALL, None, STRENGTH_ALL)
            return stats_lookup.get(key)

        results = []
        for _, etf in latest.iterrows():
            code = etf["code"]
            name = etf["name"] if pd.notna(etf["name"]) else code
            date = etf["date"]

            current_signals = {
                "ma_signal": (str(etf["ma_signal"]),
                              SIGNAL_DIRECTIONS["ma_signal"].get(str(etf["ma_signal"]), 0)),
                "macd_signal": (str(etf["macd_signal"]),
                                SIGNAL_DIRECTIONS["macd_signal"].get(str(etf["macd_signal"]), 0)),
                "rsi_status": (str(etf["rsi_status"]),
                               SIGNAL_DIRECTIONS["rsi_status"].get(str(etf["rsi_status"]), 0),
                               float(etf["rsi_value"]) if pd.notna(etf["rsi_value"]) else None),
                "kdj_signal": (str(etf["kdj_signal"]),
                               SIGNAL_DIRECTIONS["kdj_signal"].get(str(etf["kdj_signal"]), 0)),
                "trend": (str(etf["trend"]),
                          SIGNAL_DIRECTIONS["trend"].get(str(etf["trend"]), 0)),
            }

            boll_label, boll_dir = _get_bollinger_signal(etf["bollinger_position"])
            if boll_dir == 1:
                boll_lookup_val = "低位(≤20)"
            elif boll_dir == -1:
                boll_lookup_val = "高位(≥80)"
            else:
                boll_lookup_val = None
            current_signals["bollinger"] = (boll_label, boll_dir, boll_lookup_val)

            for ind, sig_info in current_signals.items():
                rsi_val = None
                if ind == "bollinger":
                    sig_val_display = sig_info[0]
                    direction = sig_info[1]
                    lookup_val = sig_info[2]
                elif ind == "rsi_status":
                    sig_val_display = sig_info[0]
                    direction = sig_info[1]
                    rsi_val = sig_info[2] if len(sig_info) > 2 else None
                    lookup_val = sig_val_display if direction != 0 else None
                else:
                    sig_val_display = sig_info[0]
                    direction = sig_info[1]
                    lookup_val = sig_val_display if direction != 0 else None

                # 计算当前信号强度
                current_strength = STRENGTH_ALL
                if ind == "rsi_status" and rsi_val is not None and direction != 0:
                    current_strength = _classify_rsi_strength(rsi_val, direction)
                elif ind == "bollinger" and direction != 0:
                    current_strength = _classify_bollinger_strength(
                        etf["bollinger_position"], direction)

                row = {
                    "code": code, "name": name, "date": date,
                    "indicator": ind,
                    "signal_value": sig_val_display,
                    "signal_direction": direction,
                    "market_regime": current_regime if direction != 0 else None,
                    "scope": SCOPE_ETF if direction != 0 else None,
                    "signal_strength": current_strength if direction != 0 else None,
                }

                confs = []
                hit_rates_for_consistency = []
                for n in FORWARD_WINDOWS:
                    col_conf = f"conf_{n}d"
                    col_hr = f"hit_rate_{n}d"
                    if lookup_val is not None:
                        s = _lookup(ind, lookup_val, n, current_regime, code, current_strength)
                        if s is not None:
                            row[col_conf] = float(s["confidence_score"])
                            row[col_hr] = float(s["hit_rate"])
                            row["scope"] = s.get("scope", SCOPE_ALL)
                            confs.append(float(s["confidence_score"]))
                            hit_rates_for_consistency.append(float(s["hit_rate"]))
                        else:
                            row[col_conf] = None
                            row[col_hr] = None
                    else:
                        row[col_conf] = None
                        row[col_hr] = None

                if confs:
                    weights = [WINDOW_WEIGHTS[n] for n in FORWARD_WINDOWS]
                    valid_pairs = [(c, w) for c, w in zip(confs, weights) if c is not None]
                    if valid_pairs:
                        total_w = sum(w for _, w in valid_pairs)
                        base_composite = sum(c * w for c, w in valid_pairs) / total_w
                        consistency = _compute_consistency_bonus(hit_rates_for_consistency)
                        composite = max(0.0, min(100.0, base_composite + consistency))
                        row["composite_confidence"] = round(composite, 1)
                        row["composite_grade"] = (
                            "A" if composite >= 70 else
                            "B" if composite >= 50 else
                            "C" if composite >= 30 else "D"
                        )
                    else:
                        row["composite_confidence"] = None
                        row["composite_grade"] = None
                else:
                    row["composite_confidence"] = None
                    row["composite_grade"] = None

                results.append(row)

            # ---- 组合信号 ----
            triggered_combos = _get_current_combo_signals(etf)
            for combo_name, combo_dir in triggered_combos:
                row = {
                    "code": code, "name": name, "date": date,
                    "indicator": "combo",
                    "signal_value": combo_name,
                    "signal_direction": combo_dir,
                    "market_regime": current_regime,
                    "scope": SCOPE_ETF,
                    "signal_strength": STRENGTH_ALL,
                }

                confs = []
                hit_rates_for_consistency = []
                for n in FORWARD_WINDOWS:
                    col_conf = f"conf_{n}d"
                    col_hr = f"hit_rate_{n}d"
                    s = _lookup("combo", combo_name, n, current_regime, code, STRENGTH_ALL)
                    if s is not None:
                        row[col_conf] = float(s["confidence_score"])
                        row[col_hr] = float(s["hit_rate"])
                        row["scope"] = s.get("scope", SCOPE_ALL)
                        confs.append(float(s["confidence_score"]))
                        hit_rates_for_consistency.append(float(s["hit_rate"]))
                    else:
                        row[col_conf] = None
                        row[col_hr] = None

                if confs:
                    weights = [WINDOW_WEIGHTS[n] for n in FORWARD_WINDOWS]
                    valid_pairs = [(c, w) for c, w in zip(confs, weights) if c is not None]
                    if valid_pairs:
                        total_w = sum(w for _, w in valid_pairs)
                        base_composite = sum(c * w for c, w in valid_pairs) / total_w
                        consistency = _compute_consistency_bonus(hit_rates_for_consistency)
                        composite = max(0.0, min(100.0, base_composite + consistency))
                        row["composite_confidence"] = round(composite, 1)
                        row["composite_grade"] = (
                            "A" if composite >= 70 else
                            "B" if composite >= 50 else
                            "C" if composite >= 30 else "D"
                        )
                    else:
                        row["composite_confidence"] = None
                        row["composite_grade"] = None
                else:
                    row["composite_confidence"] = None
                    row["composite_grade"] = None

                results.append(row)

        # ── 方向净值评分 + 矛盾标注 (方案一 + 方案三) ──
        results_df = pd.DataFrame(results)
        if not results_df.empty:
            direction_rows = []
            for code, group in results_df.groupby("code"):
                signals = group.to_dict("records")
                net_info = _compute_direction_net_score(signals)
                if net_info["direction_label"] == "MIXED":
                    conflict_desc = _detect_signal_conflict(signals)
                else:
                    conflict_desc = ""
                for _ in range(len(group)):
                    direction_rows.append({
                        "direction_net_score": net_info["direction_net_score"],
                        "direction_label": net_info["direction_label"],
                        "conflict_type": conflict_desc,
                    })
            results_df["direction_net_score"] = [r["direction_net_score"] for r in direction_rows]
            results_df["direction_label"] = [r["direction_label"] for r in direction_rows]
            results_df["conflict_type"] = [r["conflict_type"] for r in direction_rows]
        return results_df
    finally:
        if close_conn:
            conn.close()


def save_current_confidence(conf_df: pd.DataFrame, conn=None) -> int:
    """保存当前信号置信度到 signal_confidence_current 表 (v3 schema)"""
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        conn.execute("DROP TABLE IF EXISTS signal_confidence_current")
        conn.execute("""
            CREATE TABLE signal_confidence_current (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                date TEXT NOT NULL,
                indicator TEXT NOT NULL,
                signal_value TEXT NOT NULL,
                signal_direction INTEGER NOT NULL,
                market_regime TEXT,
                scope TEXT,
                signal_strength TEXT,
                conf_5d REAL,
                conf_10d REAL,
                conf_20d REAL,
                conf_30d REAL,
                conf_60d REAL,
                composite_confidence REAL,
                composite_grade TEXT,
                hit_rate_5d REAL,
                hit_rate_10d REAL,
                hit_rate_20d REAL,
                hit_rate_30d REAL,
                hit_rate_60d REAL,
                stability_score REAL,
                direction_net_score REAL,
                direction_label TEXT,
                conflict_type TEXT,
                updated_at TEXT NOT NULL,
                UNIQUE(code, indicator, signal_value, date)
            )
        """)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = conf_df.copy()
        rows["updated_at"] = now

        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows.iloc[i:i + batch_size]
            batch.to_sql("signal_confidence_current", conn, if_exists="append",
                        index=False, method="multi")
        conn.commit()
        logger.info(f"当前置信度已保存: {len(rows)} 行")
        return len(rows)
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 一键运行
# ============================================================

def run_full_backtest_pipeline() -> dict:
    """完整回测流程: 回测 → 保存 → 当前置信度 → 保存

    v3: Per-ETF独立回测 + 多信号组合 + 市场状态分层 + 收益加权命中率

    Returns:
        {"backtest_rows": int, "confidence_rows": int}
    """
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        result_df = run_backtest(conn)
        bt_n = save_backtest_results(result_df, conn)
        conf_df = get_current_confidence(conn)
        if not conf_df.empty:
            conf_n = save_current_confidence(conf_df, conn)
        else:
            conf_n = 0
        logger.info(f"回测流程完成 (v4): {bt_n} 统计行, {conf_n} 置信度行")
        return {"backtest_rows": bt_n, "confidence_rows": conf_n}
    finally:
        conn.close()
