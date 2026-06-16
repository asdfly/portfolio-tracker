# -*- coding: utf-8 -*-
"""
智能预警推送模块 - P3 进阶能力

监控持仓ETF的多维信号, 自动触发预警:
  - 价格预警: 跌破均线/支撑位, 突破压力位
  - 资金预警: 大额资金异动(净流入/流出)
  - 波动预警: 波动率突增(超过历史2倍标准差)
  - 估值预警: PE突破历史极端分位(<10%或>90%)
  - 风险预警: ERP信号、回撤幅度

预警级别: 紧急(红)/重要(橙)/关注(黄)/信息(蓝)
"""

import logging
import pandas as pd
import numpy

logger = logging.getLogger(__name__)

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime, timedelta


@dataclass
class AlertEvent:
    """单条预警事件"""
    etf_code: str
    etf_name: str
    alert_type: str          # price/fund/volatility/valuation/risk
    level: str               # 紧急/重要/关注/信息
    title: str               # 预警标题
    detail: str              # 预警详情
    value: float = 0.0       # 触发值
    threshold: float = 0.0   # 阈值
    timestamp: str = ""
    action_hint: str = ""    # 建议操作

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class AlertSummary:
    """预警汇总"""
    total: int = 0
    urgent: int = 0          # 紧急
    important: int = 0       # 重要
    watch: int = 0            # 关注
    info: int = 0             # 信息
    events: List[AlertEvent] = field(default_factory=list)


def get_level_order(level: str) -> int:
    """获取预警级别排序值。"""
    order = {"紧急": 0, "重要": 1, "关注": 2, "信息": 3}
    return order.get(level, 9)


def check_price_alert(etf_code: str, etf_name: str, current_price: float,
                      ma20: float, ma60: float, support: float, resist: float,
                      drop_pct: float = 0) -> Optional[AlertEvent]:
    """价格预警检测。

    Parameters
    ----------
    current_price : float - 当前价格
    ma20, ma60 : float - 20日/60日均线
    support, resist : float - 支撑位/压力位
    drop_pct : float - 当日跌幅(%)

    Returns AlertEvent or None
    """
    alerts = []

    # 跌破20日均线
    if ma20 > 0 and current_price < ma20 and drop_pct < -1:
        alerts.append(("重要", "跌破20日均线",
                       f"价格{current_price:.3f}跌破MA20({ma20:.3f})，跌幅{drop_pct:.1f}%",
                       "关注是否有效跌破，若缩量企稳可观察"))

    # 跌破支撑位
    if support > 0 and current_price < support:
        alerts.append(("紧急", "跌破关键支撑位",
                       f"价格{current_price:.3f}跌破支撑位{support:.3f}",
                       "建议减仓或设止损，等待企稳信号"))

    # 突破压力位
    if resist > 0 and current_price > resist:
        alerts.append(("关注", "突破压力位",
                       f"价格{current_price:.3f}突破压力位{resist:.3f}",
                       "关注量能配合，有效突破可加仓"))

    # 单日大跌
    if drop_pct < -3:
        alerts.append(("紧急", "单日大幅下跌",
                       f"当日跌幅{drop_pct:.1f}%，需警惕趋势逆转",
                       "检查是否为技术性回调，确认止损位"))

    # 单日大涨
    if drop_pct > 3:
        alerts.append(("信息", "单日大幅上涨",
                       f"当日涨幅{drop_pct:.1f}%",
                       "勿追高，等待回调确认支撑"))

    if alerts:
        # 返回最高级别的预警
        alerts.sort(key=lambda x: get_level_order(x[0]))
        level, title, detail, hint = alerts[0]
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="price", level=level,
            title=title, detail=detail,
            action_hint=hint, value=current_price,
            threshold=ma20 if "均线" in title else support,
        )
    return None


def check_fund_flow_alert(etf_code: str, etf_name: str,
                          net_inflow_today: float,
                          net_inflow_5d: float) -> Optional[AlertEvent]:
    """资金流向预警检测。

    Parameters
    ----------
    net_inflow_today : float - 当日净流入(亿元)
    net_inflow_5d : float - 近5日净流入(亿元)

    Returns AlertEvent or None
    """
    if net_inflow_today > 5:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="fund", level="关注",
            title="大额资金流入",
            detail=f"当日净流入{net_inflow_today:.1f}亿，近5日累计{net_inflow_5d:.1f}亿",
            action_hint="关注是否有持续资金推动",
            value=net_inflow_today, threshold=5,
        )
    elif net_inflow_today < -5:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="fund", level="重要",
            title="大额资金流出",
            detail=f"当日净流出{abs(net_inflow_today):.1f}亿，近5日累计{net_inflow_5d:.1f}亿",
            action_hint="警惕主力撤离，评估持仓风险",
            value=net_inflow_today, threshold=-5,
        )

    # 连续流出趋势
    if net_inflow_5d < -10:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="fund", level="关注",
            title="持续资金流出",
            detail=f"近5日累计流出{abs(net_inflow_5d):.1f}亿",
            action_hint="关注资金流向变化，警惕趋势性下跌",
            value=net_inflow_5d, threshold=-10,
        )
    return None


def check_volatility_alert(etf_code: str, etf_name: str,
                            current_vol: float, avg_vol: float,
                            vol_std: float) -> Optional[AlertEvent]:
    """波动率预警检测。

    Parameters
    ----------
    current_vol : float - 当前波动率(如20日)
    avg_vol : float - 历史平均波动率
    vol_std : float - 波动率历史标准差

    Returns AlertEvent or None
    """
    if avg_vol <= 0 or vol_std <= 0:
        return None

    z_score = (current_vol - avg_vol) / vol_std

    if z_score > 2:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="volatility", level="重要",
            title="波动率异常飙升",
            detail=f"当前波动率{current_vol:.1f}%，为历史均值{avg_vol:.1f}%的{z_score:.1f}倍标准差",
            action_hint="波动加剧，考虑降低仓位或收紧止损",
            value=current_vol, threshold=avg_vol + 2 * vol_std,
        )
    elif z_score >= 1.5:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="volatility", level="关注",
            title="波动率上升",
            detail=f"当前波动率{current_vol:.1f}%，高于均值{avg_vol:.1f}%",
            action_hint="注意风险管理",
            value=current_vol, threshold=avg_vol + 1.5 * vol_std,
        )
    return None


def check_valuation_alert(etf_code: str, etf_name: str,
                          pe_percentile: float) -> Optional[AlertEvent]:
    """估值预警检测。

    Parameters
    ----------
    pe_percentile : float - PE历史分位数(0-100)

    Returns AlertEvent or None
    """
    if pe_percentile < 10:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="valuation", level="关注",
            title="估值处于历史极低位",
            detail=f"PE分位{pe_percentile:.0f}%，历史极端低估区间",
            action_hint="可考虑逐步建仓，但需确认基本面无恶化",
            value=pe_percentile, threshold=10,
        )
    elif pe_percentile > 90:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="valuation", level="重要",
            title="估值处于历史极高位",
            detail=f"PE分位{pe_percentile:.0f}%，历史极端高估区间",
            action_hint="警惕估值回归风险，考虑止盈或减仓",
            value=pe_percentile, threshold=90,
        )
    return None


def check_risk_alert(etf_code: str, etf_name: str,
                     max_drawdown: float, erp_signal: str = "") -> Optional[AlertEvent]:
    """风险预警检测。

    Parameters
    ----------
    max_drawdown : float - 当前最大回撤(%)
    erp_signal : str - ERP信号(偏多/中性/偏空)

    Returns AlertEvent or None
    """
    if max_drawdown < -15:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="risk", level="紧急",
            title="深度回撤预警",
            detail=f"当前最大回撤{max_drawdown:.1f}%，已触发深度回撤阈值(-15%)",
            action_hint="评估是否需要止损离场或降低仓位",
            value=max_drawdown, threshold=-15,
        )
    elif max_drawdown < -10:
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="risk", level="重要",
            title="显著回撤预警",
            detail=f"当前最大回撤{max_drawdown:.1f}%",
            action_hint="关注支撑位，评估继续持有风险",
            value=max_drawdown, threshold=-10,
        )

    if erp_signal == "偏空":
        return AlertEvent(
            etf_code=etf_code, etf_name=etf_name,
            alert_type="risk", level="关注",
            title="ERP显示股权吸引力弱",
            detail=f"当前股债性价比信号为偏空，大盘系统性风险偏高",
            action_hint="建议降低整体仓位，增加防御性配置",
            value=0, threshold=0,
        )
    return None


def scan_all_alerts(
    etf_code: str, etf_name: str,
    current_price: float = 0, ma20: float = 0, ma60: float = 0,
    support: float = 0, resist: float = 0, drop_pct: float = 0,
    net_inflow_today: float = 0, net_inflow_5d: float = 0,
    current_vol: float = 0, avg_vol: float = 0, vol_std: float = 0,
    pe_percentile: float = 50, max_drawdown: float = 0,
    erp_signal: str = "",
) -> List[AlertEvent]:
    """对单只ETF执行全维度预警扫描。

    Returns list[AlertEvent] - 按级别排序
    """
    events = []

    checkers = [
        lambda: check_price_alert(etf_code, etf_name, current_price,
                                  ma20, ma60, support, resist, drop_pct),
        lambda: check_fund_flow_alert(etf_code, etf_name,
                                       net_inflow_today, net_inflow_5d),
        lambda: check_volatility_alert(etf_code, etf_name,
                                        current_vol, avg_vol, vol_std),
        lambda: check_valuation_alert(etf_code, etf_name, pe_percentile),
        lambda: check_risk_alert(etf_code, etf_name,
                                  max_drawdown, erp_signal),
    ]

    for checker in checkers:
        try:
            result = checker()
            if result is not None:
                events.append(result)
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            logger.debug(f"Alert check error: {e}")
            continue

    # 按级别排序
    events.sort(key=lambda e: get_level_order(e.level))
    return events


def summarize_alerts(alerts: List[AlertEvent]) -> AlertSummary:
    """汇总多条预警。

    Returns AlertSummary
    """
    summary = AlertSummary(total=len(alerts))
    for event in alerts:
        if event.level == "紧急":
            summary.urgent += 1
        elif event.level == "重要":
            summary.important += 1
        elif event.level == "关注":
            summary.watch += 1
        else:
            summary.info += 1
    summary.events = alerts
    return summary


def format_alert_text(alert: AlertEvent) -> str:
    """格式化单条预警为可读文本。

    Returns str
    """
    level_emoji_map = {"紧急": "[RED]", "重要": "[ORG]", "关注": "[YLW]", "信息": "[BLU]"}
    marker = level_emoji_map.get(alert.level, "[???]")
    return (
        f"{marker} [{alert.level}] {alert.etf_name}({alert.etf_code}) "
        f"- {alert.title}: {alert.detail}"
    )
