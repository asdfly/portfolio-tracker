"""仓位管理建议模块 — 基于综合评分+持仓占比+行业暴露度输出建议仓位。"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import numpy as np
import pandas as pd

SECTOR_EXPOSURE_LIMIT = 30.0
SECTOR_WARNING_LIMIT = 25.0

SCORE_POSITION_MAP = {
    (75, 101): ("加仓", 0.05, 0.15),
    (62, 75):  ("加仓", 0.02, 0.08),
    (42, 62):  ("维持", 0.00, 0.00),
    (28, 42):  ("减仓", 0.02, 0.08),
    (0, 28):   ("减仓", 0.05, 0.15),
}

@dataclass
class PositionAdvice:
    """单只ETF的仓位建议。"""
    code: str = ""
    name: str = ""
    sector: str = ""
    current_weight: float = 0.0
    current_mv: float = 0.0
    mf_total: float = 50.0
    sector_weight: float = 0.0
    sector_exposure_status: str = ""
    adjust_action: str = "维持"
    adjust_min_pct: float = 0.0
    adjust_max_pct: float = 0.0
    target_weight_min: float = 0.0
    target_weight_max: float = 0.0
    risk_constrained: bool = False
    constraint_reason: str = ""
    advice_text: str = ""

    def __getitem__(self, key): return getattr(self, key)
    def get(self, key, default=None):
        try: return self[key]
        except AttributeError: return default
    def keys(self): return self.__dict__.keys()

@dataclass
class SectorExposure:
    """行业暴露度汇总。"""
    sector: str = ""
    total_weight: float = 0.0
    etf_count: int = 0
    status: str = "正常"
    advice: str = ""

def _get_score_range(score):
    for (lo, hi), (action, pct_min, pct_max) in SCORE_POSITION_MAP.items():
        if lo <= score < hi:
            return action, pct_min, pct_max
    return "维持", 0.0, 0.0

def _check_sector_exposure(sector_weight):
    if sector_weight >= SECTOR_EXPOSURE_LIMIT:
        return "超限", f"行业占比{sector_weight:.1f}%超过{SECTOR_EXPOSURE_LIMIT:.0f}%上限"
    elif sector_weight >= SECTOR_WARNING_LIMIT:
        return "偏高", f"行业占比{sector_weight:.1f}%接近{SECTOR_EXPOSURE_LIMIT:.0f}%上限"
    return "正常", ""

def compute_position_advice(code, name, sector, current_weight, current_mv,
                            mf_total, sector_weight, risk_constrained=False):
    """计算单只ETF的仓位建议。

    Parameters
    ----------
    code, name, sector : str
    current_weight : float - 当前持仓占比(0-100)
    current_mv : float - 当前市值
    mf_total : float - 多因子综合评分(0-100)
    sector_weight : float - 该行业总占比(0-100)
    risk_constrained : bool - 是否被风险约束

    Returns
    -------
    PositionAdvice
    """
    # 评分区间映射
    adjust_action, pct_min, pct_max = _get_score_range(mf_total)

    # 行业暴露度检查
    exp_status, exp_reason = _check_sector_exposure(sector_weight)

    # 行业超限约束: 超限时"加仓"降级为"维持"
    if adjust_action == "加仓" and exp_status == "超限":
        adjust_action = "维持"
        pct_min, pct_max = 0.0, 0.0

    # 风险约束
    if risk_constrained and adjust_action == "加仓":
        adjust_action = "维持"
        pct_min, pct_max = 0.0, 0.0

    # 计算目标仓位
    if adjust_action == "加仓":
        target_min = current_weight + pct_min
        target_max = current_weight + pct_max
    elif adjust_action == "减仓":
        target_min = max(0, current_weight - pct_max)
        target_max = max(0, current_weight - pct_min)
    else:
        target_min = current_weight
        target_max = current_weight

    # 生成建议文本
    parts = []
    if adjust_action == "加仓":
        parts.append(f"建议加仓{pct_min:.0%}-{pct_max:.0%}，目标占比{target_min:.1f}-{target_max:.1f}%")
    elif adjust_action == "减仓":
        parts.append(f"建议减仓{pct_min:.0%}-{pct_max:.0%}，目标占比{target_min:.1f}-{target_max:.1f}%")
    else:
        parts.append("建议维持当前仓位")

    if exp_status == "超限":
        parts.append(f"行业暴露超限({sector_weight:.1f}%)，不宜加仓")
    elif exp_status == "偏高":
        parts.append(f"行业暴露偏高({sector_weight:.1f}%)，谨慎加仓")

    advice = PositionAdvice(
        code=code, name=name, sector=sector,
        current_weight=current_weight, current_mv=current_mv,
        mf_total=mf_total,
        sector_weight=sector_weight, sector_exposure_status=exp_status,
        adjust_action=adjust_action,
        adjust_min_pct=pct_min, adjust_max_pct=pct_max,
        target_weight_min=target_min, target_weight_max=target_max,
        risk_constrained=risk_constrained,
        advice_text="；".join(parts),
    )
    if risk_constrained:
        advice.constraint_reason = "风险评分过高，加仓受限"
    if exp_status == "超限":
        advice.constraint_reason = advice.constraint_reason + "；" + exp_reason if advice.constraint_reason else exp_reason
    return advice


def compute_all_position_advice(positions, mf_scores, etf_categories=None):
    """批量计算所有持仓ETF的仓位建议。

    Parameters
    ----------
    positions : pd.DataFrame - 持仓数据(code, name, market_value)
    mf_scores : list - MultiFactorScore列表
    etf_categories : dict, optional - ETF分类配置

    Returns
    -------
    List[PositionAdvice]
    """
    if positions is None or positions.empty:
        return []
    if etf_categories is None:
        from config.settings import ETF_CATEGORIES
        etf_categories = ETF_CATEGORIES

    total_mv = positions["market_value"].sum()

    # 计算行业权重
    sector_weights = {}
    for _, pos in positions.iterrows():
        code = str(pos["code"])
        sec = etf_categories.get(code, {}).get("sector", "其他")
        mv = pos.get("market_value", 0)
        sector_weights[sec] = sector_weights.get(sec, 0) + mv

    sector_pcts = {s: w / total_mv * 100 if total_mv > 0 else 0 for s, w in sector_weights.items()}

    # 构建MF score lookup
    mf_map = {}
    if mf_scores:
        for mf in mf_scores:
            mf_map[mf.code] = mf

    results = []
    for _, pos in positions.iterrows():
        code = str(pos["code"])
        name = pos.get("name", code)
        sec = etf_categories.get(code, {}).get("sector", "其他")
        mv = pos.get("market_value", 0)
        weight = mv / total_mv * 100 if total_mv > 0 else 0

        mf = mf_map.get(code)
        mf_total = mf.total_score if mf else 50.0
        risk_constrained = mf.risk_constrained if mf else False

        advice = compute_position_advice(
            code, name, sec, weight, mv,
            mf_total, sector_pcts.get(sec, 0), risk_constrained
        )
        results.append(advice)

    # 按调整紧迫度排序(减仓优先)
    order = {"减仓": 0, "维持": 1, "加仓": 2}
    results.sort(key=lambda x: (order.get(x.adjust_action, 1), -x.mf_total))
    return results


def compute_sector_exposures(positions, etf_categories=None):
    """计算行业暴露度汇总。

    Returns
    -------
    List[SectorExposure]
    """
    if positions is None or positions.empty:
        return []
    if etf_categories is None:
        from config.settings import ETF_CATEGORIES
        etf_categories = ETF_CATEGORIES

    total_mv = positions["market_value"].sum()
    sector_data = {}
    for _, pos in positions.iterrows():
        code = str(pos["code"])
        sec = etf_categories.get(code, {}).get("sector", "其他")
        mv = pos.get("market_value", 0)
        if sec not in sector_data:
            sector_data[sec] = {"total_mv": 0, "count": 0}
        sector_data[sec]["total_mv"] += mv
        sector_data[sec]["count"] += 1

    results = []
    for sec, data in sorted(sector_data.items(), key=lambda x: -x[1]["total_mv"]):
        weight = data["total_mv"] / total_mv * 100 if total_mv > 0 else 0
        status, reason = _check_sector_exposure(weight)
        advice = ""
        if status == "超限":
            advice = f"建议减仓至{SECTOR_EXPOSURE_LIMIT*100:.0f}%以下"
        elif status == "偏高":
            advice = "注意控制仓位"
        results.append(SectorExposure(
            sector=sec, total_weight=round(weight, 1),
            etf_count=data["count"], status=status, advice=advice
        ))
    return results
