"""多因子综合评分模块 — 将技术面/风险面/资金面/基本面四维信息融合为统一决策评分。

设计原则:
- 技术面(40%): 信号评分(进攻信号，何时买入/卖出)
- 风险面(25%): 风险评分(防守约束，当前风险水平)
- 资金面(20%): 资金流向(市场共识，聪明钱方向)
- 基本面(15%): 估值水平(长期价值锚)
- 四维独立评分 0-100，加权输出综合评分 0-100
- 风险面作为约束条件：高风险ETF建议上限为"持有"，不触发"买入/加仓"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import logging
import numpy as np
logger = logging.getLogger(__name__)
import pandas as pd


# ---------------------------------------------------------------------------
# Dataclass 定义
# ---------------------------------------------------------------------------

@dataclass
class FactorDetail:
    """单因子评分详情。"""
    score: float = 50.0       # 0-100
    weight: float = 0.0       # 权重
    weighted: float = 0.0      # 加权得分
    level: str = "中性"        # 等级标签
    detail: str = ""           # 人可读描述


@dataclass
class MultiFactorScore:
    """单只 ETF 的多因子综合评分结果。

    四维评分 + 综合评分 + 操作建议 + 风险约束。
    """
    code: str = ""
    name: str = ""

    # 四维因子
    technical: FactorDetail = field(default_factory=FactorDetail)
    risk: FactorDetail = field(default_factory=FactorDetail)
    fund_flow: FactorDetail = field(default_factory=FactorDetail)
    fundamental: FactorDetail = field(default_factory=FactorDetail)

    # 综合
    total_score: float = 50.0
    grade: str = "持有"
    action: str = "持有"
    urgency: str = "维持"
    reasons: List[str] = field(default_factory=list)

    # 风险约束
    risk_constrained: bool = False
    risk_constraint_reason: str = ""

    # 仓位建议(预留)
    position_advice: str = ""

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key, default=None):
        try:
            return self[key]
        except AttributeError:
            return default

    def keys(self):
        return self.__dict__.keys()

    def items(self):
        return self.__dict__.items()


# ---------------------------------------------------------------------------
# 因子评分函数
# ---------------------------------------------------------------------------

# --- 默认权重配置 ---
DEFAULT_WEIGHTS: Dict[str, float] = {
    "technical": 0.40,
    "risk": 0.25,
    "fund_flow": 0.20,
    "fundamental": 0.15,
}

# --- 风险约束阈值 ---
RISK_HOLD_THRESHOLD = 65   # 风险评分 >= 此值，建议上限为"持有"
RISK_WATCH_THRESHOLD = 50  # 风险评分 >= 此值，"加仓"降级为"持有"


def _score_technical(signal_score: float) -> FactorDetail:
    """将信号评分映射为技术因子详情。

    signal_score 已是 0-100 分，直接使用。
    level 划分: >=75 强烈买入, >=60 买入, >=40 持有, >=25 卖出, <25 强烈卖出
    """
    if signal_score >= 75:
        level = "强烈看多"
    elif signal_score >= 60:
        level = "偏多"
    elif signal_score >= 40:
        level = "中性"
    elif signal_score >= 25:
        level = "偏空"
    else:
        level = "强烈看空"

    return FactorDetail(
        score=signal_score,
        weight=DEFAULT_WEIGHTS["technical"],
        weighted=signal_score * DEFAULT_WEIGHTS["technical"],
        level=level,
        detail=f"技术信号评分 {signal_score:.0f}（{level}）",
    )

def _score_risk(risk_score: float) -> FactorDetail:
    """将风险评分映射为风险因子详情。

    注意: 风险评分越高=风险越大，与综合评分方向相反。
    综合评分中风险因子使用 (100 - risk_score) 做反向映射：
    - risk_score 低(低风险) -> 综合贡献高
    - risk_score 高(高风险) -> 综合贡献低
    """
    inv = 100.0 - risk_score  # 反向映射

    if risk_score >= 70:
        level = "高风险"
    elif risk_score >= 50:
        level = "中风险"
    elif risk_score >= 30:
        level = "低风险"
    else:
        level = "极低风险"

    return FactorDetail(
        score=inv,
        weight=DEFAULT_WEIGHTS["risk"],
        weighted=inv * DEFAULT_WEIGHTS["risk"],
        level=level,
        detail=f"风险评分 {risk_score:.0f}/100（{level}）",
        # 额外字段供决策使用
    )


def _score_fund_flow(fund_flow_df: pd.DataFrame) -> FactorDetail:
    """基于资金流向数据计算资金因子评分。

    逻辑:
    - 近5日主力净流入占比 > 0: 加分(最高80)
    - 近20日趋势: 持续净流入加分, 持续净流出减分
    - 近期异动: 有大额净流入日加分
    - 无数据: 中性50
    """
    if fund_flow_df is None or fund_flow_df.empty:
        return FactorDetail(
            score=50.0, weight=DEFAULT_WEIGHTS["fund_flow"],
            weighted=25.0 * DEFAULT_WEIGHTS["fund_flow"],
            level="数据不足", detail="资金流向数据不足，中性评分"
        )

    score = 50.0
    details = []

    # 近5日主力净流入占比
    recent5 = fund_flow_df.head(5)
    if "net_amount" in recent5.columns:
        net5 = recent5["net_amount"].astype(float).sum()
        if net5 > 0:
            score += min(15, net5 / 1e8 * 5)  # 每1亿加5分，上限15
            details.append(f"近5日净流入{net5/1e8:.1f}亿")
        elif net5 < 0:
            score -= min(15, abs(net5) / 1e8 * 5)
            details.append(f"近5日净流出{abs(net5)/1e8:.1f}亿")

    # 近20日趋势
    recent20 = fund_flow_df.head(20)
    if len(recent20) >= 10 and "net_amount" in recent20.columns:
        net20 = recent20["net_amount"].astype(float).sum()
        pos_days = (recent20["net_amount"].astype(float) > 0).sum()
        total_days = len(recent20)
        if net20 > 0 and pos_days / total_days > 0.5:
            score += 10
            details.append(f"近{total_days}日资金偏多({pos_days}/{total_days}日净流入)")
        elif net20 < 0 and pos_days / total_days < 0.4:
            score -= 10
            details.append(f"近{total_days}日资金偏空(仅{pos_days}/{total_days}日净流入)")

    # 异动检测: 单日净流入超过近期日均3倍
    if "net_amount" in recent5.columns and len(recent5) >= 3:
        daily_net = recent5["net_amount"].astype(float)
        avg_net = daily_net.abs().mean()
        if avg_net > 0:
            max_single = daily_net.max()
            if max_single > avg_net * 3:
                score += 5
                details.append("有大额资金异动")

    score = float(np.clip(score, 0, 100))

    if score >= 70:
        level = "资金偏多"
    elif score >= 55:
        level = "资金温和偏多"
    elif score >= 45:
        level = "资金中性"
    elif score >= 30:
        level = "资金温和偏空"
    else:
        level = "资金偏空"

    return FactorDetail(
        score=score,
        weight=DEFAULT_WEIGHTS["fund_flow"],
        weighted=score * DEFAULT_WEIGHTS["fund_flow"],
        level=level,
        detail="；".join(details) if details else "资金流向中性",
    )


def _score_fundamental(fund_data: dict) -> FactorDetail:
    """基于基本面数据计算估值因子评分。

    输入 fund_data 包含:
    - pe_ratio: 追踪指数PE (可选)
    - pb_ratio: 追踪指数PB (可选)
    - dividend_yield: 股息率 (可选)
    - discount_rate: 折价率 (可选)
    - shares: 份额 (可选)

    逻辑:
    - 有PE时: PE < 12 加分(低估), 12-18 中性, >18 减分(高估)
    - 有折价率时: 折价(负值)加分(便宜), 溢价(正值)减分
    - 有份额趋势时: 规模增长加分
    - 无任何数据: 中性50
    """
    if not fund_data:
        return FactorDetail(
            score=50.0, weight=DEFAULT_WEIGHTS["fundamental"],
            weighted=25.0 * DEFAULT_WEIGHTS["fundamental"],
            level="数据不足", detail="基本面数据不足，中性评分"
        )

    score = 50.0
    details = []

    # PE估值
    pe = fund_data.get("pe_ratio")
    if pe is not None and not (isinstance(pe, float) and (pd.isna(pe) or pe <= 0)):
        try:
            pe_val = float(pe)
            if pe_val < 10:
                score += 20
                details.append(f"PE {pe_val:.1f}（极度低估）")
            elif pe_val < 14:
                score += 10
                details.append(f"PE {pe_val:.1f}（低估）")
            elif pe_val < 20:
                details.append(f"PE {pe_val:.1f}（合理）")
            elif pe_val < 30:
                score -= 10
                details.append(f"PE {pe_val:.1f}（偏高）")
            else:
                score -= 20
                details.append(f"PE {pe_val:.1f}（高估）")
        except (ValueError, TypeError):
            pass

    # 股息率
    div = fund_data.get("dividend_yield")
    if div is not None and not (isinstance(div, float) and pd.isna(div)):
        try:
            div_val = float(div)
            if div_val >= 4.0:
                score += 10
                details.append(f"股息率 {div_val:.1f}%（高）")
            elif div_val >= 2.5:
                score += 5
                details.append(f"股息率 {div_val:.1f}%（中）")
        except (ValueError, TypeError):
            pass

    # 折价率
    discount = fund_data.get("discount_rate")
    if discount is not None and not (isinstance(discount, float) and pd.isna(discount)):
        try:
            disc_val = float(discount)
            if disc_val < -0.5:
                score += 10
                details.append(f"折价 {disc_val:.1f}%（较大折价，买入机会）")
            elif disc_val < -0.1:
                score += 5
                details.append(f"折价 {disc_val:.1f}%")
            elif disc_val > 1.0:
                score -= 10
                details.append(f"溢价 {disc_val:.1f}%（溢价较高）")
            elif disc_val > 0.3:
                score -= 5
                details.append(f"溢价 {disc_val:.1f}%")
        except (ValueError, TypeError):
            pass

    score = float(np.clip(score, 0, 100))

    if score >= 70:
        level = "低估"
    elif score >= 55:
        level = "偏低估"
    elif score >= 45:
        level = "合理"
    elif score >= 30:
        level = "偏高估"
    else:
        level = "高估"

    return FactorDetail(
        score=score,
        weight=DEFAULT_WEIGHTS["fundamental"],
        weighted=score * DEFAULT_WEIGHTS["fundamental"],
        level=level,
        detail="；".join(details) if details else "基本面数据中性",
    )

def _score_risk(risk_score: float) -> FactorDetail:
    """将风险评分映射为风险因子详情。风险评分越高=风险越大，反向映射到综合评分。"""
    inv = 100.0 - risk_score
    if risk_score >= 70:
        level = "高风险"
    elif risk_score >= 50:
        level = "中风险"
    elif risk_score >= 30:
        level = "低风险"
    else:
        level = "极低风险"
    return FactorDetail(
        score=inv, weight=DEFAULT_WEIGHTS["risk"],
        weighted=inv * DEFAULT_WEIGHTS["risk"],
        level=level, detail=f"风险评分 {risk_score:.0f}/100（{level}）",
    )


def _score_fund_flow(fund_flow_df: pd.DataFrame) -> FactorDetail:
    """基于资金流向数据计算资金因子评分。"""
    if fund_flow_df is None or fund_flow_df.empty:
        return FactorDetail(
            score=50.0, weight=DEFAULT_WEIGHTS["fund_flow"],
            weighted=50.0 * DEFAULT_WEIGHTS["fund_flow"], level="数据不足", detail="资金流向数据不足"
        )
    score = 50.0
    details = []
    recent5 = fund_flow_df.head(5)
    if "net_amount" in recent5.columns:
        net5 = recent5["net_amount"].astype(float).sum()
        if net5 > 0:
            score += min(15, net5 / 1e8 * 5)
            details.append(f"近5日净流入{net5/1e8:.1f}亿")
        elif net5 < 0:
            score -= min(15, abs(net5) / 1e8 * 5)
            details.append(f"近5日净流出{abs(net5)/1e8:.1f}亿")
    recent20 = fund_flow_df.head(20)
    if len(recent20) >= 10 and "net_amount" in recent20.columns:
        net20 = recent20["net_amount"].astype(float).sum()
        pos_days = (recent20["net_amount"].astype(float) > 0).sum()
        total_days = len(recent20)
        if net20 > 0 and pos_days / total_days > 0.5:
            score += 10
            details.append(f"近{total_days}日资金偏多({pos_days}/{total_days}日)")
        elif net20 < 0 and pos_days / total_days < 0.4:
            score -= 10
            details.append(f"近{total_days}日资金偏空(仅{pos_days}/{total_days}日)")
    if "net_amount" in recent5.columns and len(recent5) >= 3:
        daily_net = recent5["net_amount"].astype(float)
        avg_net = daily_net.abs().mean()
        if avg_net > 0 and daily_net.max() > avg_net * 3:
            score += 5
            details.append("有大额资金异动")
    score = float(np.clip(score, 0, 100))
    if score >= 70: level = "资金偏多"
    elif score >= 55: level = "资金温和偏多"
    elif score >= 45: level = "资金中性"
    elif score >= 30: level = "资金温和偏空"
    else: level = "资金偏空"
    return FactorDetail(
        score=score, weight=DEFAULT_WEIGHTS["fund_flow"],
        weighted=score * DEFAULT_WEIGHTS["fund_flow"],
        level=level, detail="；".join(details) if details else "资金流向中性",
    )


def _score_fundamental(fund_data: dict) -> FactorDetail:
    """基于基本面数据计算估值因子评分。"""
    if not fund_data:
        return FactorDetail(
            score=50.0, weight=DEFAULT_WEIGHTS["fundamental"],
            weighted=50.0 * DEFAULT_WEIGHTS["fundamental"], level="数据不足", detail="基本面数据不足"
        )
    score = 50.0
    details = []
    pe = fund_data.get("pe_ratio")
    if pe is not None and not (isinstance(pe, float) and (pd.isna(pe) or pe <= 0)):
        try:
            pe_val = float(pe)
            if pe_val < 10:
                score += 20; details.append(f"PE {pe_val:.1f}（极度低估）")
            elif pe_val < 14:
                score += 10; details.append(f"PE {pe_val:.1f}（低估）")
            elif pe_val < 20:
                details.append(f"PE {pe_val:.1f}（合理）")
            elif pe_val < 30:
                score -= 10; details.append(f"PE {pe_val:.1f}（偏高）")
            else:
                score -= 20; details.append(f"PE {pe_val:.1f}（高估）")
        except (ValueError, TypeError):
            pass
    div = fund_data.get("dividend_yield")
    if div is not None and not (isinstance(div, float) and pd.isna(div)):
        try:
            div_val = float(div)
            if div_val >= 4.0:
                score += 10; details.append(f"股息率 {div_val:.1f}%（高）")
            elif div_val >= 2.5:
                score += 5; details.append(f"股息率 {div_val:.1f}%（中）")
        except (ValueError, TypeError):
            pass
    discount = fund_data.get("discount_rate")
    if discount is not None and not (isinstance(discount, float) and pd.isna(discount)):
        try:
            disc_val = float(discount)
            if disc_val < -0.5:
                score += 10; details.append(f"折价 {disc_val:.1f}%（较大折价）")
            elif disc_val < -0.1:
                score += 5; details.append(f"折价 {disc_val:.1f}%")
            elif disc_val > 1.0:
                score -= 10; details.append(f"溢价 {disc_val:.1f}%（较高）")
            elif disc_val > 0.3:
                score -= 5; details.append(f"溢价 {disc_val:.1f}%")
        except (ValueError, TypeError):
            pass
    score = float(np.clip(score, 0, 100))
    if score >= 70: level = "低估"
    elif score >= 55: level = "偏低估"
    elif score >= 45: level = "合理"
    elif score >= 30: level = "偏高估"
    else: level = "高估"
    return FactorDetail(
        score=score, weight=DEFAULT_WEIGHTS["fundamental"],
        weighted=score * DEFAULT_WEIGHTS["fundamental"],
        level=level, detail="；".join(details) if details else "基本面中性",
    )

def _apply_risk_constraint(score, raw_action):
    """应用风险约束：高风险ETF建议上限为持有。"""
    risk_val = 100.0 - score.risk.score
    if risk_val >= RISK_HOLD_THRESHOLD:
        score.risk_constrained = True
        score.risk_constraint_reason = f"风险评分{risk_val:.0f}>={RISK_HOLD_THRESHOLD}，建议上限为持有"
        if raw_action in ("买入", "加仓"):
            return "持有"
        return raw_action
    if risk_val >= RISK_WATCH_THRESHOLD:
        if raw_action == "加仓":
            score.risk_constrained = True
            score.risk_constraint_reason = f"风险评分{risk_val:.0f}>={RISK_WATCH_THRESHOLD}，加仓降级为持有"
            return "持有"
    return raw_action

def _generate_action(total):
    """基于综合评分生成操作建议和紧急度。"""
    if total >= 75: return "买入", "强烈建议"
    elif total >= 62: return "加仓", "建议"
    elif total >= 42: return "持有", "维持"
    elif total >= 28: return "观望", "建议"
    else: return "卖出", "建议"

def _compose_reasons(score):
    """生成决策理由列表。"""
    reasons = []
    if score.technical.score >= 65:
        reasons.append(f"技术面偏多({score.technical.score:.0f}分)")
    elif score.technical.score <= 35:
        reasons.append(f"技术面偏空({score.technical.score:.0f}分)")
    risk_val = 100.0 - score.risk.score
    if risk_val >= 65:
        reasons.append(f"高风险({risk_val:.0f}分)，约束建议")
    elif risk_val <= 30:
        reasons.append(f"低风险({risk_val:.0f}分)")
    if score.fund_flow.score >= 65:
        reasons.append(f"资金偏多({score.fund_flow.score:.0f}分)")
    elif score.fund_flow.score <= 35:
        reasons.append(f"资金偏空({score.fund_flow.score:.0f}分)")
    if score.fundamental.score >= 65:
        reasons.append(f"估值偏低({score.fundamental.score:.0f}分)")
    elif score.fundamental.score <= 35:
        reasons.append(f"估值偏高({score.fundamental.score:.0f}分)")
    if not reasons:
        reasons.append("多因子综合评分中性")
    return reasons

def compute_multi_factor_score(code, name, signal_score_val, risk_score_val,
                                fund_flow_df=None, fund_data=None):
    """计算单只ETF的多因子综合评分。

    Parameters
    ----------
    code : str - ETF代码
    name : str - ETF名称
    signal_score_val : float - 技术信号评分(0-100)
    risk_score_val : float - 风险评分(0-100)
    fund_flow_df : pd.DataFrame, optional - 资金流向数据
    fund_data : dict, optional - 基本面数据

    Returns
    -------
    MultiFactorScore
    """
    tech = _score_technical(signal_score_val)
    risk = _score_risk(risk_score_val)
    flow = _score_fund_flow(fund_flow_df)
    fund = _score_fundamental(fund_data or {})
    total = tech.weighted + risk.weighted + flow.weighted + fund.weighted
    total = round(float(np.clip(total, 0, 100)), 1)
    raw_action, urgency = _generate_action(total)
    result = MultiFactorScore(
        code=code, name=name,
        technical=tech, risk=risk, fund_flow=flow, fundamental=fund,
        total_score=total, grade=raw_action,
    )
    final_action = _apply_risk_constraint(result, raw_action)
    result.action = final_action
    result.urgency = urgency
    result.reasons = _compose_reasons(result)
    if result.risk_constrained:
        result.reasons.append(result.risk_constraint_reason)
    return result

def compute_multi_factor_scores(positions):
    """批量计算所有持仓ETF的多因子综合评分。

    Parameters
    ----------
    positions : pd.DataFrame - 持仓数据，需含code, name列

    Returns
    -------
    List[MultiFactorScore] - 按综合评分降序排列
    """
    from data_loader import (load_signal_score, load_etf_risk_scan,
                              load_etf_fund_flow, load_etf_fundamental)
    if positions is None or positions.empty:
        return []
    results = []
    for _, pos in positions.iterrows():
        code = str(pos["code"])
        name = pos.get("name", code)
        try:
            sig_df = load_signal_score(code)
            sig_val = float(sig_df["total_score"].iloc[0]) if sig_df is not None and not sig_df.empty else 50.0
        except (KeyError, IndexError, TypeError, ValueError, AttributeError) as e:
            logger.warning(f"Signal score error for {code}: {e}")
            sig_val = 50.0
        try:
            risk_df = load_etf_risk_scan(code)
            risk_val = float(risk_df["total_score"].iloc[0]) if risk_df is not None and not risk_df.empty else 50.0
        except (KeyError, IndexError, TypeError, ValueError, AttributeError) as e:
            logger.warning(f"Risk scan error for {code}: {e}")
            risk_val = 50.0
        try:
            flow_df = load_etf_fund_flow(code, days=60)
        except (ConnectionError, OSError, ValueError, KeyError) as e:
            logger.warning(f"Fund flow error for {code}: {e}")
            flow_df = None
        try:
            fund_df = load_etf_fundamental()
            if fund_df is not None and not fund_df.empty:
                fund_row = fund_df[fund_df["code"].astype(str) == code]
                fund_data = fund_row.iloc[0].to_dict() if not fund_row.empty else {}
            else:
                fund_data = {}
        except (KeyError, IndexError, TypeError, ValueError, AttributeError) as e:
            logger.warning(f"Fundamental data error for {code}: {e}")
            fund_data = {}
        result = compute_multi_factor_score(code, name, sig_val, risk_val, flow_df, fund_data)
        results.append(result)
    results.sort(key=lambda x: x.total_score, reverse=True)
    return results
