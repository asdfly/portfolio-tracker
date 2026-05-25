"""
黄金储备与外汇储备占比分析模块
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from .gold_utils import fetch_china_reserve, base_layout


def render_reserve_analysis():

    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">储备分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">跟踪中国央行黄金储备变化趋势，分析官方购金行为对金价的中长期影响。</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown("**黄金储备与外汇储备占比分析**")

    reserve_df = fetch_china_reserve()
    if reserve_df is None or reserve_df.empty:
        st.warning("中国储备数据获取失败")
        return

    has_gold = "gold_reserve" in reserve_df.columns
    has_fx = "fx_reserve" in reserve_df.columns

    if has_gold and has_fx:
        reserve_df["gold_pct"] = reserve_df["gold_reserve"] / reserve_df["fx_reserve"] * 100
        _render_ratio_chart(reserve_df)
        _render_increase_chart(reserve_df)
    elif has_gold:
        st.info("仅获取到黄金储备数据，外汇储备数据不可用")
        _render_gold_only_chart(reserve_df)
    else:
        st.warning("无可用储备数据")


def _render_ratio_chart(df):
    st.markdown("**黄金储备占外汇储备比例趋势**")
    c1, c2, c3 = st.columns(3)
    latest = df.iloc[-1]
    c1.metric("黄金储备", f"{latest['gold_reserve']:.0f} 万盎司")
    if "fx_reserve" in df.columns:
        c2.metric("外汇储备", f"{latest['fx_reserve']:.0f} 亿美元")
    c3.metric("黄金占比", f"{latest['gold_pct']:.2f}%", "全球平均约15%")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["gold_pct"],
        mode="lines+markers", name="黄金占比(%)",
        line=dict(color="#FFD700", width=2), marker=dict(size=3),
    ))
    fig.add_hline(y=15, line_dash="dash", line_color="#666",
                  annotation_text="全球平均 ~15%", annotation_position="top left")
    fig.update_layout(
        title=dict(text="中国黄金储备占外汇储备比例", font=dict(size=13)),
        xaxis=dict(gridcolor="#333", tickformat="%Y-%m"),
        yaxis=dict(title="占比(%)", gridcolor="#333"),
        height=350, **base_layout(margin=dict(l=50, r=30, t=40, b=30)),
    )
    st.plotly_chart(fig, width='stretch')


def _render_increase_chart(df):
    st.markdown("**黄金储备增持趋势**")
    df_sorted = df.sort_values("date").copy()
    df_sorted["gold_change"] = df_sorted["gold_reserve"].diff()
    df_sorted["is_increase"] = df_sorted["gold_change"] > 0
    df_recent = df_sorted.tail(60)

    fig = go.Figure()
    colors = ["#ef5350" if v > 0 else "#26a69a" for v in df_recent["gold_change"]]
    fig.add_trace(go.Bar(
        x=df_recent["date"], y=df_recent["gold_change"],
        name="月度增持(万盎司)", marker_color=colors, opacity=0.8,
    ))
    fig.add_trace(go.Scatter(
        x=df_recent["date"], y=df_recent["gold_reserve"],
        name="黄金储备(万盎司)", mode="lines",
        line=dict(color="#FFD700", width=2), yaxis="y2",
    ))
    fig.update_layout(
        title=dict(text="月度增持量与储备总量", font=dict(size=13)),
        xaxis=dict(gridcolor="#333", tickformat="%Y-%m"),
        yaxis=dict(title="增持量(万盎司)", gridcolor="#333"),
        yaxis2=dict(title="储备总量(万盎司)", overlaying="y", side="right",
                     gridcolor="#333", tickfont=dict(color="#ddd")),
        height=350,
        **base_layout(margin=dict(l=50, r=60, t=40, b=30)),
    )
    st.plotly_chart(fig, width='stretch')

    total_increase = df_recent["gold_change"].sum()
    increase_months = df_recent["is_increase"].sum()
    total_months = len(df_recent)
    st.markdown(f"**近{total_months}个月**：累计增持 {total_increase:.0f} 万盎司，"
                f"其中 {int(increase_months)} 个月为增持，占比 {increase_months/total_months*100:.0f}%")


def _render_gold_only_chart(df):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["date"], y=df["gold_reserve"],
        name="黄金储备(万盎司)", marker_color="#FFA726", opacity=0.8,
    ))
    fig.update_layout(
        title=dict(text="中国黄金储备", font=dict(size=14)),
        xaxis=dict(gridcolor="#333", tickformat="%Y-%m"),
        yaxis=dict(title="万盎司", gridcolor="#333"),
        height=350, **base_layout(),
    )
    st.plotly_chart(fig, width='stretch')
