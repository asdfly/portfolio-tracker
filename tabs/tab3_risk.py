"""Tab3: 风险分析（编排函数 + 数据委托）"""

from components.ui import render_chart, render_empty_state
import streamlit as st
import pandas as pd
from config.settings import CHART_DAYS
from data_loader import load_positions, load_summary

# 数据层委托
def compute_extended_risk_metrics(end_date=None, min_date="2025-08-01"):
    """compute_extended_risk_metrics（委托到 data_loader）"""
    import data_loader as _dl
    return _dl.compute_extended_risk_metrics(end_date=end_date, min_date=min_date)

def compute_return_attribution(days=CHART_DAYS["default"], end_date=None):
    """compute_return_attribution（委托到 data_loader）"""
    import data_loader as _dl
    return _dl.compute_return_attribution(days=days, end_date=end_date)

def load_alerts(limit=10):
    """load_alerts（委托到 data_loader）"""
    import data_loader as _dl
    return _dl.load_alerts(limit=limit)

# 子模块导入
from tabs.tab3_risk_dashboard import (
    _render_risk_gauge_and_metrics,
    _render_drawdown_chart,
)
from tabs.tab3_risk_attribution import (
    _render_brinson_attribution,
    _render_multi_factor_attribution,
)
from tabs.tab3_risk_warnings import (
    _render_risk_warnings,
    _render_style_exposure,
    _render_sector_rotation,
)
from tabs.tab3_risk_alerts import (
    _render_alert_center,
    _render_alert_gauge_dashboard,
    _render_alert_trend_analysis,
)


def render_tab3():
    """渲染Tab3: 风险分析"""
    selected_date = st.session_state.get("selected_date", "")
    selected_benchmark = st.session_state.get("selected_benchmark", "sh000300")
    positions = load_positions(selected_date)
    show_days = st.session_state.get("show_days", 250)
    summary = load_summary(show_days, selected_date)

    profit_count = int((positions["pnl"] > 0).sum()) if not positions.empty else 0
    loss_count = int((positions["pnl"] < 0).sum()) if not positions.empty else 0
    if not summary.empty:
        latest = summary.iloc[-1]
        volatility = latest.get("volatility", None)
        max_dd = latest.get("max_drawdown", None)
    else:
        volatility = None
        max_dd = None
    sharpe = None

    _render_risk_gauge_and_metrics(sharpe, volatility, max_dd, selected_date, summary, positions, profit_count, loss_count, show_days=show_days)
    _render_drawdown_chart(summary)
    _render_brinson_attribution(show_days, selected_date)
    _render_multi_factor_attribution(positions)
    _render_risk_warnings(positions, volatility, max_dd, profit_count, loss_count, selected_date)
    _render_style_exposure(positions)
    _render_sector_rotation()
    _render_alert_center(positions, summary, selected_date)
