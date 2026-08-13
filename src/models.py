"""结构化数据模型 — 替代 data_loader 中复杂 dict 返回值

将 compute_extended_risk_metrics / run_monte_carlo /
compute_return_attribution / compute_rebalance_suggestion
四个函数的返回值从裸 dict 收敛为 dataclass，提升可扩展性和 IDE 支持。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# 1. RiskMetrics — compute_extended_risk_metrics 返回值
# ---------------------------------------------------------------------------

@dataclass
class RiskMetrics:
    """扩展风险指标集（基于持仓稳定后的日收益率）"""

    sortino: float = np.nan
    calmar: float = np.nan
    win_rate: float = np.nan          # 百分比 (0-100)
    pl_ratio: float = np.nan
    max_consec_win: int = 0
    max_consec_loss: int = 0
    max_dd_duration: int = 0         # 最大回撤持续天数
    skewness: float = np.nan
    kurtosis: float = np.nan
    annual_return: float = np.nan     # 年化收益率（小数）
    annual_std: float = np.nan       # 年化波动率（小数）

    # --- 兼容 dict 风格访问（支持渐进迁移） ---
    def __getitem__(self, key: str):
        """向后兼容：支持 result['sortino'] 等旧式 dict 访问"""
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: str, default=None):
        """向后兼容：支持 result.get('sortino', np.nan)"""
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        """向后兼容：支持 dict(result) 或 for k in result"""
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def __contains__(self, key: str) -> bool:
        return key in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __bool__(self) -> bool:
        """空结果（所有指标为 nan/0）应视为 falsy，与原 {} 语义一致"""
        return bool(not pd.isna(self.sortino) or not pd.isna(self.calmar))

    @classmethod
    def empty(cls) -> "RiskMetrics":
        """数据不足时返回的空对象（等效于原 return {}）"""
        return cls()


# ---------------------------------------------------------------------------
# 2. MonteCarloResult — run_monte_carlo 返回值
# ---------------------------------------------------------------------------

@dataclass
class MonteCarloResult:
    """蒙特卡洛模拟结果"""

    paths: Optional[np.ndarray] = None          # (n_simulations, days+1)
    percentiles: Optional[pd.DataFrame] = None  # day, p5, p25, p50, p75, p95
    last_value: float = 0.0
    mean_return: float = 0.0
    daily_std: float = 0.0
    sample_count: int = 0
    filtered_count: int = 0
    sample_start: str = ""

    # --- 兼容 dict 风格访问 ---
    def __getitem__(self, key: str):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def __contains__(self, key: str) -> bool:
        return key in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __bool__(self) -> bool:
        """数据不足时 data_loader 返回 None，此处做防御"""
        return self.paths is not None


# ---------------------------------------------------------------------------
# 3. ReturnAttribution — compute_return_attribution 返回值
# ---------------------------------------------------------------------------

@dataclass
class ReturnAttribution:
    """Brinson 收益归因结果"""

    total_return: float = 0.0
    benchmark_return: float = 0.0
    allocation_effect: Dict[str, float] = field(default_factory=dict)
    selection_effect: Dict[str, float] = field(default_factory=dict)
    interaction_effect: Dict[str, float] = field(default_factory=dict)
    sector_returns: Dict[str, float] = field(default_factory=dict)
    sector_weights: Dict[str, float] = field(default_factory=dict)
    bench_weights: Dict[str, float] = field(default_factory=dict)

    # --- 兼容 dict 风格访问 ---
    def __getitem__(self, key: str):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def __contains__(self, key: str) -> bool:
        return key in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __bool__(self) -> bool:
        """空结果（无行业数据）应视为 falsy，与原 return None 语义一致"""
        return bool(self.sector_returns)


# ---------------------------------------------------------------------------
# 4. RebalanceTrade — compute_rebalance_suggestion 中单笔交易
# ---------------------------------------------------------------------------

@dataclass
class RebalanceTrade:
    """单笔调仓建议"""

    sector: str = ""
    code: str = ""
    name: str = ""
    current_weight: float = 0.0
    target_weight: float = 0.0
    diff: float = 0.0             # current - target (正=超配)
    trade_value: float = 0.0       # 交易金额 (正=买入)
    shares: int = 0
    direction: str = ""            # "买入" or "卖出"
    price: float = 0.0

    # --- 兼容 dict 风格访问 ---
    def __getitem__(self, key: str):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def __contains__(self, key: str) -> bool:
        return key in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)


# ---------------------------------------------------------------------------
# 5. RebalanceSuggestion — compute_rebalance_suggestion 返回值
# ---------------------------------------------------------------------------

@dataclass
class RebalanceSuggestion:
    """再平衡建议结果"""

    current_weights: Dict[str, float] = field(default_factory=dict)
    target_weights: Dict[str, float] = field(default_factory=dict)
    suggestions: List[RebalanceTrade] = field(default_factory=list)
    total_value: float = 0.0
    threshold: float = 0.05

    # --- 兼容 dict 风格访问 ---
    def __getitem__(self, key: str):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def __contains__(self, key: str) -> bool:
        return key in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __bool__(self) -> bool:
        """空结果应视为 falsy，与原 return None 语义一致"""
        return bool(self.current_weights)
