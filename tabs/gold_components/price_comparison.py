"""
上海金基准价 vs SGE现货价格对比模块
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from .gold_utils import fetch_sge_benchmark, fetch_sge_hist, base_layout


def render_price_comparison():

    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">基准价对比<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">对比上海黄金交易所各品种（Au99.99/Au99.95/Au100g等）的价差和走势差异，发现套利机会。</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown("**上海金基准价 vs SGE现货对比**")

    col_ctrl = st.columns([2, 2, 2])
    with col_ctrl[0]:
        period = st.selectbox("对比周期", ["近30天", "近90天", "近180天", "近1年", "全部"], key="bench_period")
    with col_ctrl[1]:
        symbol = st.selectbox("SGE品种", ["Au99.99", "Au99.95", "Au(T+D)"], key="bench_symbol")
    with col_ctrl[2]:
        show_spread = st.checkbox("显示价差图", value=True, key="bench_show_spread")

    bench_df = fetch_sge_benchmark()
    sge_df = fetch_sge_hist(symbol=symbol)

    if bench_df is None or sge_df is None:
        st.warning("上海金基准价或SGE历史数据获取失败")
        return

    # 确保close列是数值型
    bench_df["close"] = pd.to_numeric(bench_df["close"], errors="coerce")
    # 基准价数据有晚盘价和早盘价，close取晚盘价即可
    if "close" not in sge_df.columns:
        for c in sge_df.columns:
            if "收盘" in c or "close" in c.lower():
                sge_df["close"] = pd.to_numeric(sge_df[c], errors="coerce")
                break
    sge_df["close"] = pd.to_numeric(sge_df["close"], errors="coerce")

    # 如果基准价数据有晚盘价和早盘价，计算均价
    extra_price_cols = [c for c in bench_df.columns if c not in ["date", "close"]]
    if extra_price_cols:
        bench_df["close"] = (bench_df["close"] + pd.to_numeric(bench_df[extra_price_cols[0]], errors="coerce")) / 2

    # 按周期过滤
    period_map = {"近30天": 30, "近90天": 90, "近180天": 180, "近1年": 365, "全部": 99999}
    n_days = period_map.get(period, 90)
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=n_days)
    bench_df = bench_df[bench_df["date"] >= cutoff].copy()
    sge_df = sge_df[sge_df["date"] >= cutoff].copy()

    if bench_df.empty or sge_df.empty:
        st.info("所选周期内数据不足")
        return

    # 合并
    merged = pd.merge(
        bench_df[["date", "close"]].rename(columns={"close": "benchmark"}),
        sge_df[["date", "close"]].rename(columns={"close": "sge_spot"}),
        on="date", how="inner",
    ).dropna()

    if merged.empty:
        st.info("两个数据源无重叠日期，无法对比")
        return

    merged["spread"] = merged["benchmark"] - merged["sge_spot"]
    spread_mean = merged["spread"].mean()
    spread_std = merged["spread"].std()

    # 统计指标卡片
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("平均价差", f"{spread_mean:.2f}", "基准价 - 现货价")
    c2.metric("价差标准差", f"{spread_std:.2f}")
    latest_spread = merged["spread"].iloc[-1]
    c3.metric("当前价差", f"{latest_spread:.2f}")
    pct_rank = (merged["spread"] < latest_spread).sum() / len(merged) * 100
    c4.metric("价差分位数", f"{pct_rank:.0f}%", "历史百分位")

    # 双轴对比图
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=merged["date"], y=merged["benchmark"],
        mode="lines", name="上海金基准价",
        line=dict(color="#FFD700", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=merged["date"], y=merged["sge_spot"],
        mode="lines", name=f"SGE {symbol}",
        line=dict(color="#00BCD4", width=1.5),
    ))
    fig.update_layout(
        title=dict(text="价格走势对比", font=dict(size=14)),
        xaxis=dict(gridcolor="#333"),
        yaxis=dict(title="价格(元/克)", gridcolor="#333"),
        height=400, **base_layout(),
    )
    st.plotly_chart(fig, width='stretch')

    # 价差图
    if show_spread:
        fig_sp = go.Figure()
        colors = ["#26a69a" if v < 0 else "#ef5350" for v in merged["spread"]]
        fig_sp.add_trace(go.Bar(
            x=merged["date"], y=merged["spread"],
            name="价差", marker_color=colors, opacity=0.7,
        ))
        fig_sp.add_hline(y=spread_mean, line_dash="dash", line_color="#FFD700",
                         annotation_text=f"均值 {spread_mean:.2f}")
        fig_sp.add_hline(y=spread_mean + spread_std, line_dash="dot", line_color="#666",
                         annotation_text=f"+1σ")
        fig_sp.add_hline(y=spread_mean - spread_std, line_dash="dot", line_color="#666",
                         annotation_text=f"-1σ")
        fig_sp.update_layout(
            title=dict(text="价差分布 (基准价 - 现货价)", font=dict(size=14)),
            xaxis=dict(gridcolor="#333"),
            yaxis=dict(title="价差(元/克)", gridcolor="#333"),
            height=300, **base_layout(),
        )
        st.plotly_chart(fig_sp, width='stretch')
