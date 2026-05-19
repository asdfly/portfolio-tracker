"""
黄金季节性规律分析模块
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

from .gold_utils import fetch_sge_hist, calc_monthly_returns, base_layout

MONTH_LABELS = ["1月","2月","3月","4月","5月","6月","7月","8月","9月","10月","11月","12月"]


def render_seasonality():
    st.markdown("**黄金季节性规律分析**")

    col_ctrl = st.columns([2, 2])
    with col_ctrl[0]:
        symbol = st.selectbox("品种", ["Au99.99", "Au99.95", "Au(T+D)"], key="seas_symbol")
    with col_ctrl[1]:
        min_years = st.selectbox("最少年数", ["5年", "10年", "15年", "全部"], key="seas_years")

    sge_df = fetch_sge_hist(symbol=symbol)
    if sge_df is None or sge_df.empty:
        st.warning("SGE历史数据获取失败")
        return

    if "close" not in sge_df.columns:
        for c in sge_df.columns:
            if "收盘" in c or "close" in c.lower():
                sge_df["close"] = pd.to_numeric(sge_df[c], errors="coerce")
                break
    sge_df["close"] = pd.to_numeric(sge_df["close"], errors="coerce")

    monthly = calc_monthly_returns(sge_df)
    if monthly.empty:
        st.info("数据不足，无法计算月度收益率")
        return

    year_map = {"5年": 5, "10年": 10, "15年": 15, "全部": 0}
    min_y = year_map.get(min_years, 0)
    if min_y > 0:
        latest_year = monthly["year"].max()
        monthly = monthly[monthly["year"] >= latest_year - min_y + 1]

    pivot = monthly.pivot_table(index="year", columns="month", values="monthly_return")
    pivot = pivot.reindex(columns=range(1, 13))
    years = [str(y) for y in pivot.index]

    fig_heat = go.Figure(data=go.Heatmap(
        z=pivot.values * 100,
        x=MONTH_LABELS, y=years,
        colorscale=[[0, "#26a69a"], [0.5, "#1a1a2e"], [1, "#ef5350"]],
        zmid=0,
        text=pivot.values * 100,
        texttemplate="%{text:.1f}%",
        textfont=dict(size=9),
        hovertemplate="%{y}年%{x}: %{text:.2f}%<extra></extra>",
    ))
    fig_heat.update_layout(
        title=dict(text=f"{symbol} 月度收益率热力图 ({len(years)}年)", font=dict(size=14)),
        xaxis=dict(gridcolor="#333", side="top"),
        yaxis=dict(gridcolor="#333", autorange="reversed"),
        height=max(300, len(years) * 28 + 80),
        **base_layout(margin=dict(l=50, r=30, t=50, b=30)),
    )
    st.plotly_chart(fig_heat, width='stretch')

    # 月度统计柱状图
    stats = monthly.groupby("month")["monthly_return"].agg(["mean", "std", "count", lambda x: (x > 0).sum()])
    stats.columns = ["avg_return", "std_return", "count", "up_count"]
    stats["up_prob"] = stats["up_count"] / stats["count"] * 100
    stats = stats.reindex(range(1, 13))

    fig_bar = make_subplots(specs=[[{"secondary_y": True}]])
    bar_colors = ["#ef5350" if v >= 0 else "#26a69a" for v in stats["avg_return"]]
    fig_bar.add_trace(go.Bar(
        x=MONTH_LABELS, y=stats["avg_return"] * 100,
        name="平均收益率(%)", marker_color=bar_colors, opacity=0.8,
    ), secondary_y=False)
    fig_bar.add_trace(go.Scatter(
        x=MONTH_LABELS, y=stats["up_prob"],
        name="上涨概率(%)", mode="lines+markers+text",
        line=dict(color="#FFD700", width=2), marker=dict(size=6),
        text=[f"{v:.0f}%" for v in stats["up_prob"]],
        textposition="top center", textfont=dict(size=9, color="#FFD700"),
    ), secondary_y=True)

    current_month = pd.Timestamp.now().month
    curr_idx = current_month - 1
    fig_bar.add_vline(x=curr_idx, line_dash="dash", line_color="#FFD700", opacity=0.5)
    curr_avg = stats["avg_return"].iloc[curr_idx] * 100
    fig_bar.add_annotation(
        x=curr_idx, y=curr_avg * 1.5 if curr_avg > 0 else curr_avg * 0.5,
        text="当前月份", showarrow=True, font=dict(color="#FFD700", size=11), arrowhead=2,
    )
    fig_bar.update_layout(
        title=dict(text=f"月度统计汇总 ({len(years)}年数据)", font=dict(size=14)),
        xaxis=dict(gridcolor="#333"), height=400, **base_layout(),
    )
    fig_bar.update_yaxes(title_text="平均收益率(%)", gridcolor="#333", secondary_y=False)
    fig_bar.update_yaxes(title_text="上涨概率(%)", range=[0, 100], gridcolor="#333", secondary_y=True)
    st.plotly_chart(fig_bar, width='stretch')

    st.markdown("---")
    st.markdown("**季节性规律摘要**")
    best_month = stats["avg_return"].idxmax()
    worst_month = stats["avg_return"].idxmin()
    best_prob_month = stats["up_prob"].idxmax()
    s1, s2, s3 = st.columns(3)
    s1.metric("最强月份", MONTH_LABELS[best_month - 1],
              f"平均 {stats['avg_return'].iloc[best_month - 1] * 100:.2f}%")
    s2.metric("最弱月份", MONTH_LABELS[worst_month - 1],
              f"平均 {stats['avg_return'].iloc[worst_month - 1] * 100:.2f}%")
    s3.metric("上涨概率最高", MONTH_LABELS[best_prob_month - 1],
              f"{stats['up_prob'].iloc[best_prob_month - 1]:.0f}%")
