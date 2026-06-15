# -*- coding: utf-8 -*-
"""
交易信号交叉回验模块 — P2 信号交叉回验

将多维度信号（技术评分、资金流向、新闻情绪、风险评分）交叉对比，
检验信号方向一致性，识别分歧/共振信号。
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class SignalItem:
    dimension: str
    direction: int
    score: float
    detail: str
    weight: float

@dataclass
class CrossValidationResult:
    code: str
    signals: list
    agreement_count: int
    disagreement_count: int
    neutral_count: int
    consensus_direction: int
    consensus_score: float
    action: str
    summary: str


def _classify_direction(score, thresholds=(60, 40)):
    if score >= thresholds[0]: return 1
    elif score <= thresholds[1]: return -1
    return 0


def cross_validate_signals(code, tech_score=None, risk_score=None,
    fund_flow_signal=None, sentiment_signal=None, news_direction=None,
    tech_detail="", risk_detail="", flow_detail="", sentiment_detail=""):
    """交叉验证多维度信号方向。"""
    signals = []
    if tech_score is not None:
        d = _classify_direction(tech_score)
        signals.append(SignalItem("技术面", d, tech_score,
            tech_detail or f"技术评分 {tech_score:.1f}", 0.30))
    if risk_score is not None:
        safe_score = 100 - risk_score
        d = _classify_direction(safe_score)
        label = "低风险" if risk_score < 40 else ("中风险" if risk_score < 65 else "高风险")
        signals.append(SignalItem("风险面", d, safe_score,
            risk_detail or f"风险评分 {risk_score:.1f}（{label}）", 0.20))
    if fund_flow_signal is not None:
        d = _classify_direction(fund_flow_signal)
        signals.append(SignalItem("资金面", d, fund_flow_signal,
            flow_detail or f"资金流信号 {fund_flow_signal:.1f}", 0.25))
    if news_direction is not None:
        signals.append(SignalItem("消息面", news_direction,
            50 + news_direction * 30,
            sentiment_detail or f"情绪{'看多' if news_direction>0 else '看空' if news_direction<0 else '中性'}",
            0.25))
    elif sentiment_signal is not None:
        s100 = sentiment_signal * 100
        d = _classify_direction(s100)
        signals.append(SignalItem("消息面", d, s100,
            sentiment_detail or f"情绪评分 {s100:.1f}", 0.25))
    if not signals:
        return CrossValidationResult(code, [], 0, 0, 0, 0, 0.0, "观望", "无可用信号")
    dirs = [s.direction for s in signals]
    pos, neg, neu = dirs.count(1), dirs.count(-1), dirs.count(0)
    wd = sum(s.direction * s.weight for s in signals)
    total = len(signals)
    if pos > neg and pos >= total * 0.6:
        cons = 1; cs = round(min(wd, 1.0), 2)
    elif neg > pos and neg >= total * 0.6:
        cons = -1; cs = round(max(wd, -1.0), 2)
    else:
        cons = 0; cs = round(abs(wd), 2)
    if cons == 1 and cs >= 0.6: action = "共振看多，建议加仓"
    elif cons == 1: action = "偏多信号为主，可适度增持"
    elif cons == -1 and cs <= -0.6: action = "共振看空，建议减仓"
    elif cons == -1: action = "偏空信号为主，注意风险"
    else: action = "信号分歧，建议观望"
    dn = [f"{s.dimension}({'看多' if s.direction>0 else '看空' if s.direction<0 else '中性'})" for s in signals]
    summary = f"{'、'.join(dn)}。共{total}维，{pos}多/{neg}空/{neu}中性。"
    return CrossValidationResult(code, signals, max(pos,neg), min(pos,neg), neu,
        cons, cs, action, summary)
