"""Tab3 风险仪表盘子模块 — 仪表盘展示、回撤曲线"""

from components.ui import render_chart
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.chart_utils import downsample, _add_min_max_annotations
from data_loader import compute_extended_risk_metrics
from config.settings import CHART_DAYS, DOWNSAMPLE_MAX_POINTS
def _render_risk_gauge_and_metrics(sharpe, volatility, max_dd, selected_date, summary, positions, profit_count, loss_count, show_days=CHART_DAYS["default"]):
    """渲染风险仪表盘和风险指标详情。"""

    """渲染Tab3: 风险分析"""

    st.caption("⚠️ 展示风险评分仪表盘、风险指标详情、回撤曲线及Brinson收益归因分析")
    col_risk_gauge, col_risk_detail = st.columns([1, 1])

    with col_risk_gauge:
        st.markdown(
            '<div class="tip-title" style="">风险指标仪表盘<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">综合年化波动率和最大回撤计算风险评分（0-100分）。满分100表示低风险，低于60分表示高风险。颜色越绿越安全，越红风险越高。</span></div>',
            unsafe_allow_html=True,
        )

        # 风险评分
        risk_score = 100
        if volatility and not np.isnan(volatility):
            if volatility > 30:
                risk_score -= 30
            elif volatility > 20:
                risk_score -= 15
            elif volatility > 15:
                risk_score -= 5
        if max_dd and not np.isnan(max_dd):
            if abs(max_dd) > 15:
                risk_score -= 30
            elif abs(max_dd) > 10:
                risk_score -= 20
            elif abs(max_dd) > 5:
                risk_score -= 10
        if sharpe and not np.isnan(sharpe):
            if sharpe < 0:
                risk_score -= 20
            elif sharpe < 0.5:
                risk_score -= 10

        risk_score = max(0, min(100, risk_score))
        risk_color = "#22c55e" if risk_score >= 70 else "#f59e0b" if risk_score >= 40 else "#ef4444"
        risk_label = "低风险" if risk_score >= 70 else "中等风险" if risk_score >= 40 else "高风险"

        fig_gauge = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=risk_score,
                number={"suffix": "分", "font": {"size": 40, "color": risk_color}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": "#8b949e", "tickfont": {"size": 10}},
                    "bar": {"color": risk_color},
                    "bgcolor": "#161b22",
                    "steps": [
                        {"range": [0, 40], "color": "rgba(239,68,68,0.15)"},
                        {"range": [40, 70], "color": "rgba(245,158,11,0.15)"},
                        {"range": [70, 100], "color": "rgba(34,197,94,0.15)"},
                    ],
                    "threshold": {"line": {"color": risk_color, "width": 3}, "thickness": 0.8, "value": risk_score},
                },
            )
        )
        fig_gauge.update_layout(
            height=250,
            plot_bgcolor="#0d1117",
            paper_bgcolor="#0d1117",
            font=dict(color="#c9d1d9"),
            margin=dict(l=30, r=30, t=10, b=10),
        )
        render_chart(fig_gauge)

        st.markdown(
            f'<div style="text-align:center;color:{risk_color};font-size:16px;font-weight:bold;">'
            f"{risk_label}</div>",
            unsafe_allow_html=True,
        )

    with col_risk_detail:
        st.markdown(
            '<div class="tip-title" style="">风险指标详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示夏普比率、Sortino比率、Calmar比率、最大回撤、年化波动率、胜率和盈亏比等核心风险指标，悬停可查看指标含义。</span></div>',
            unsafe_allow_html=True,
        )

        # 计算扩展风险指标
        ext_risk = compute_extended_risk_metrics(end_date=selected_date)

        risk_metrics = [
            ("夏普比率", sharpe, "衡量风险调整后收益，>1为优秀"),
            ("Sortino比率", ext_risk.get("sortino", np.nan), "仅考虑下行波动的风险调整收益"),
            ("Calmar比率", ext_risk.get("calmar", np.nan), "年化收益 / 最大回撤，越高越好"),
            ("最大回撤", ext_risk.get("max_drawdown", max_dd), "历史最大亏损幅度"),
            ("年化波动率", ext_risk.get("annual_std", volatility), "收益率的标准差，越高越不稳定"),
            ("胜率", ext_risk.get("win_rate", np.nan), "盈利天数 / 有盈亏交易天数"),
            ("盈亏比", ext_risk.get("pl_ratio", np.nan), "平均盈利 / 平均亏损，>1为优"),
            ("最大连续盈利", ext_risk.get("max_consec_win", 0), "历史最长连续盈利天数"),
            ("最大连续亏损", ext_risk.get("max_consec_loss", 0), "历史最长连续亏损天数"),
            ("最大回撤持续", ext_risk.get("max_dd_duration", 0), "历史最长回撤恢复天数（净值低于峰值）"),
            ("偏度", ext_risk.get("skewness", np.nan), "收益率分布偏斜，正值为右偏"),
            ("峰度", ext_risk.get("kurtosis", np.nan), "收益率分布尾部厚度，>0为尖峰"),
            (
                "持仓盈亏比",
                f"{profit_count}:{loss_count}" if profit_count or loss_count else "N/A",
                f"盈利{profit_count}只 vs 亏损{loss_count}只",
            ),
            ("数据周期", f"{len(summary)}天" if not summary.empty else "N/A", "历史数据积累天数"),
        ]

        for name, value, desc in risk_metrics:
            if isinstance(value, float) and not np.isnan(value):
                val_str = f"{value:.3f}" if abs(value) < 1 else f"{value:.2f}"
            elif value is None or (isinstance(value, float) and np.isnan(value)):
                val_str = '<span style="color:#888;">N/A</span>'
            else:
                val_str = str(value)

            st.markdown(
                f'<div style="padding:8px 12px;border-bottom:1px solid #21262d;">'
                f'<div style="display:flex;justify-content:space-between;">'
                f'<span style="color:#8b949e;font-size:13px;">{name}</span>'
                f'<span style="color:#c9d1d9;font-size:13px;font-weight:bold;">{val_str}</span>'
                f"</div>"
                f'<div style="font-size:11px;color:#484f58;">{desc}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )




def _render_drawdown_chart(summary):
    """Extracted from render_tab3."""
    # 回撤曲线（降采样）
    if not summary.empty and len(summary) > 5:
        st.markdown(
            '<div class="tip-title" style="">回撤曲线<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示组合从历史最高点到当前市值的回撤幅度(%)。可识别最大回撤区间及其恢复时间，评估组合的抗风险能力。</span></div>',
            unsafe_allow_html=True,
        )
        # 使用 corrected daily_return 构建累积净值来计算回撤，避免 total_value 跳变影响
        dd_data = summary[["date", "daily_return"]].copy()
        if "daily_return" in dd_data.columns:
            dd_data["cumret"] = (1 + dd_data["daily_return"] / 100).cumprod()
            dd_data["drawdown"] = (dd_data["cumret"] / dd_data["cumret"].cummax() - 1) * 100
        else:
            dd_data["drawdown"] = (
                (summary["total_value"] - summary["total_value"].cummax()) / summary["total_value"].cummax() * 100
            )
        dd_chart = downsample(dd_data, max_points=DOWNSAMPLE_MAX_POINTS)

        fig_dd = go.Figure()
        fig_dd.add_trace(
            go.Scatter(
                x=dd_chart["date"],
                y=dd_chart["drawdown"],
                mode="lines",
                name="回撤",
                fill="tozeroy",
                line=dict(color="#ef4444", width=1.5),
                fillcolor="rgba(239,68,68,0.15)",
            )
        )
        # 标记最大回撤
        _add_min_max_annotations(fig_dd, dd_chart["date"], dd_chart["drawdown"], y_label="回撤")

        fig_dd.update_layout(
            height=200,
            plot_bgcolor="#0d1117",
            paper_bgcolor="#0d1117",
            font=dict(color="#c9d1d9", size=11),
            margin=dict(l=50, r=20, t=10, b=40),
            xaxis=dict(showgrid=False),
            yaxis=dict(title="回撤 (%)", showgrid=True, gridcolor="#21262d"),
        )
        render_chart(fig_dd)


