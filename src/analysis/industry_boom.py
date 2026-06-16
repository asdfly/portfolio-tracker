# -*- coding: utf-8 -*-
"""
行业景气度指标模块 - P3 进阶能力

综合多维度数据评估行业景气度:
  - 资金面: 北向资金/主力资金行业净流入
  - 基本面: 行业指数PE/PB分位数
  - 技术面: 行业指数趋势(均线排列、成交量)
  - 政策面: 近期行业政策/事件信号

输出: IndustryBoomResult (0-100景气度评分 + 信号)
"""

import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class IndustryBoomResult:
    """行业景气度分析结果"""
    industry: str
    boom_score: float            # 0-100综合景气度
    fund_score: float            # 资金面得分(0-100)
    valuation_score: float       # 估值面得分(0-100)
    trend_score: float           # 技术面得分(0-100)
    policy_score: float          # 政策面得分(0-100)
    signal: str                  # 强烈推荐/推荐/中性/谨慎/回避
    top_reasons: List[str] = field(default_factory=list)
    risk_reasons: List[str] = field(default_factory=list)


# 行业中文名称映射
INDUSTRY_NAMES = {
    "bank": "银行", "insurance": "保险", "securities": "证券",
    "real_estate": "房地产", "consumer": "大消费", "healthcare": "医药生物",
    "technology": "科技", "new_energy": "新能源", "manufacturing": "先进制造",
    "resources": "资源周期", "infrastructure": "基建", "agriculture": "农林牧渔",
    "communication": "通信", "media": "传媒", "automotive": "汽车",
    "semiconductor": "半导体", "defense": "国防军工", "utilities": "公用事业",
    "transport": "交通运输", "steel": "钢铁", "chemical": "化工",
}

# 常见ETF代码到行业的映射
ETF_INDUSTRY_MAP = {
    "512800": "bank", "512010": "securities", "512080": "technology",
    "159928": "consumer", "512010": "securities", "159915": "new_energy",
    "512170": "healthcare", "512660": "semiconductor", "512690": "automotive",
    "159934": "resources", "512980": "communication",
}


def classify_boom_signal(score: float) -> Tuple[str, str]:
    """根据景气度评分输出信号。

    Parameters
    ----------
    score : float - 0-100

    Returns (signal, label)
    """
    if score >= 80:
        return ("强烈推荐", "行业景气度极高，多项指标共振向好")
    elif score >= 65:
        return ("推荐", "行业景气度较好，多数指标偏正面")
    elif score >= 45:
        return ("中性", "行业景气度一般，多空因素交织")
    elif score >= 30:
        return ("谨慎", "行业景气度偏低，需关注风险")
    else:
        return ("回避", "行业景气度低迷，多项指标恶化")


def calc_fund_score(net_inflow_5d: float, net_inflow_20d: float) -> float:
    """计算资金面得分。

    基于近5日和20日行业资金净流入金额。

    Parameters
    ----------
    net_inflow_5d : float - 近5日净流入(亿元)
    net_inflow_20d : float - 近20日净流入(亿元)

    Returns float 0-100
    """
    # 短期权重60%, 中期权重40%
    score_5d = min(100, max(0, 50 + net_inflow_5d * 2))  # 每1亿+2分
    score_20d = min(100, max(0, 50 + net_inflow_20d * 0.5))  # 每1亿+0.5分
    return round(score_5d * 0.6 + score_20d * 0.4, 1)


def calc_valuation_score(pe_percentile: float, pb_percentile: float) -> float:
    """计算估值面得分。

    低分位 = 便宜 = 得分高。

    Parameters
    ----------
    pe_percentile : float - PE历史分位数(0-100)
    pb_percentile : float - PB历史分位数(0-100)

    Returns float 0-100
    """
    # 分位数越低越便宜，得分越高
    pe_score = max(0, 100 - pe_percentile)
    pb_score = max(0, 100 - pb_percentile)
    return round(pe_score * 0.6 + pb_score * 0.4, 1)


def calc_trend_score(ma5_above_ma20: bool, ma20_above_ma60: bool,
                     vol_ratio: float, price_change_20d: float) -> float:
    """计算技术面得分。

    Parameters
    ----------
    ma5_above_ma20 : bool - 短期均线是否在中期上方
    ma20_above_ma60 : bool - 中期均线是否在长期上方
    vol_ratio : float - 近5日成交量/近20日均值
    price_change_20d : float - 近20日涨跌幅(%)

    Returns float 0-100
    """
    score = 50  # 基准

    # 均线排列 (最多+20)
    if ma5_above_ma20 and ma20_above_ma60:
        score += 20  # 多头排列
    elif ma5_above_ma20:
        score += 10  # 部分多头
    elif not ma5_above_ma20 and not ma20_above_ma60:
        score -= 20  # 空头排列

    # 量能 (最多+15)
    if vol_ratio > 1.5:
        score += 15  # 放量
    elif vol_ratio > 1.0:
        score += 5

    # 价格趋势 (最多+15)
    if price_change_20d > 10:
        score += 15
    elif price_change_20d > 0:
        score += 5
    elif price_change_20d < -10:
        score -= 15
    elif price_change_20d < 0:
        score -= 5

    return round(min(100, max(0, score)), 1)


def calc_policy_score(has_positive_policy: bool, has_negative_policy: bool,
                      recent_events: int = 0) -> float:
    """计算政策面得分。

    Parameters
    ----------
    has_positive_policy : bool - 近期是否有利好政策
    has_negative_policy : bool - 近期是否有利空政策
    recent_events : int - 近30天行业事件数量

    Returns float 0-100
    """
    score = 50

    if has_positive_policy:
        score += 25
    if has_negative_policy:
        score -= 25

    # 事件频率 (适中为佳，过多表示不确定性高)
    if 1 <= recent_events <= 3:
        score += 5
    elif recent_events > 5:
        score -= 10

    return round(min(100, max(0, score)), 1)


def compute_industry_boom(
    industry: str,
    net_inflow_5d: float = 0,
    net_inflow_20d: float = 0,
    pe_percentile: float = 50,
    pb_percentile: float = 50,
    ma5_above_ma20: bool = False,
    ma20_above_ma60: bool = False,
    vol_ratio: float = 1.0,
    price_change_20d: float = 0,
    has_positive_policy: bool = False,
    has_negative_policy: bool = False,
    recent_events: int = 0,
) -> IndustryBoomResult:
    """计算单个行业综合景气度。

    Parameters
    ----------
    industry : str - 行业代码或名称

    Returns IndustryBoomResult
    """
    fund_s = calc_fund_score(net_inflow_5d, net_inflow_20d)
    val_s = calc_valuation_score(pe_percentile, pb_percentile)
    trend_s = calc_trend_score(ma5_above_ma20, ma20_above_ma60,
                                vol_ratio, price_change_20d)
    policy_s = calc_policy_score(has_positive_policy, has_negative_policy,
                                 recent_events)

    # 综合评分: 资金30% + 估值25% + 技术25% + 政策20%
    total = round(fund_s * 0.30 + val_s * 0.25 + trend_s * 0.25 + policy_s * 0.20, 1)

    signal, desc = classify_boom_signal(total)

    # 收集看多/看空理由
    top_reasons = []
    risk_reasons = []
    if fund_s >= 65:
        top_reasons.append(f"资金持续净流入(5d:{net_inflow_5d:.1f}亿)")
    elif fund_s <= 35:
        risk_reasons.append(f"资金持续净流出(5d:{net_inflow_5d:.1f}亿)")
    if val_s >= 65:
        top_reasons.append(f"估值处于低位(PE分位{pe_percentile:.0f}%)")
    elif val_s <= 35:
        risk_reasons.append(f"估值偏高(PE分位{pe_percentile:.0f}%)")
    if trend_s >= 65:
        top_reasons.append("技术面多头排列，趋势向好")
    elif trend_s <= 35:
        risk_reasons.append("技术面走弱，均线空头排列")
    if policy_s >= 70:
        top_reasons.append("近期有利好政策支撑")
    elif policy_s <= 30:
        risk_reasons.append("近期面临政策风险")

    return IndustryBoomResult(
        industry=industry,
        boom_score=total,
        fund_score=fund_s,
        valuation_score=val_s,
        trend_score=trend_s,
        policy_score=policy_s,
        signal=signal,
        top_reasons=top_reasons,
        risk_reasons=risk_reasons,
    )


def load_industry_fund_flow(industry: str, days: int = 20) -> Tuple[float, float]:
    """从数据库加载行业资金流向。

    Returns (net_inflow_5d, net_inflow_20d) in 亿元
    """
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, net_amount FROM sector_fund_flow "
            "WHERE sector_name=? AND date >= date('now', ?) ORDER BY date",
            conn, params=[INDUSTRY_NAMES.get(industry, industry), f"-{days} days"]
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError, KeyError) as e:
        logger.warning(f"DB error loading industry fund flow: {e}")
        return (0.0, 0.0)
    finally:
        conn.close()

    if df.empty or "net_amount" not in df.columns:
        return (0.0, 0.0)

    inflow_5d = df.tail(5)["net_amount"].sum() / 1e8 if len(df) >= 5 else 0
    inflow_20d = df.tail(20)["net_amount"].sum() / 1e8 if len(df) >= 20 else 0
    return (round(inflow_5d, 2), round(inflow_20d, 2))


def compute_boom_for_position(etf_code: str) -> Optional[IndustryBoomResult]:
    """为持仓ETF计算所属行业景气度。

    Parameters
    ----------
    etf_code : str - ETF代码

    Returns IndustryBoomResult or None
    """
    industry = ETF_INDUSTRY_MAP.get(etf_code)
    if industry is None:
        return None

    fund_5d, fund_20d = load_industry_fund_flow(industry)

    # 估值默认50(中性), 技术面需从指数数据获取
    # 这里提供基础计算, 实际数据由 data_loader 预处理
    return compute_industry_boom(
        industry=industry,
        net_inflow_5d=fund_5d,
        net_inflow_20d=fund_20d,
    )


def compute_boom_multi(etf_codes: List[str] = None) -> List[IndustryBoomResult]:
    """批量计算持仓ETF的行业景气度。

    Parameters
    ----------
    etf_codes : list[str], optional - ETF代码列表

    Returns list[IndustryBoomResult]
    """
    if etf_codes is None:
        etf_codes = list(ETF_INDUSTRY_MAP.keys())

    results = []
    for code in etf_codes:
        r = compute_boom_for_position(code)
        if r is not None:
            results.append(r)
    return results
