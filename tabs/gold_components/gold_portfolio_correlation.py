"""
黄金与投资组合相关性分析模块（Phase 7B新增）

展示黄金价格走势与投资组合净值的收益率相关性，
帮助评估黄金作为组合对冲资产的有效性。
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

from tabs.gold_components.gold_utils import fetch_sge_hist, DARK_BG, DARK_FONT_COLOR, GRID_COLOR
from src.utils.database import get_db_connection


@st.cache_data(ttl=3600)
def _load_portfolio_returns(n_days=365):
    """加载投资组合日收益率"""
    conn = get_db_connection()
    try:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=n_days)
        df = pd.read_sql_query(
            "SELECT date, daily_return, total_value FROM portfolio_summary "
            "WHERE date >= ? ORDER BY date",
            conn, params=(cutoff.isoformat(),),
        )
    finally:
        conn.close()
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    df["portfolio_return"] = df["total_value"].pct_change()
    return df[["date", "portfolio_return"]].dropna()


@st.cache_data(ttl=3600)
def _load_gold_returns(n_days=365):
    """加载Au99.99日收益率"""
    gold_df = fetch_sge_hist("Au99.99")
    if gold_df is None or gold_df.empty:
        return pd.DataFrame()
    gold_df["date"] = pd.to_datetime(gold_df["date"])
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=n_days)
    gold_df = gold_df[gold_df["date"] >= cutoff].copy()
    if gold_df.empty:
        return pd.DataFrame()
    gold_df["gold_return"] = gold_df["close"].pct_change()
    return gold_df[["date", "gold_return"]].dropna()


@st.cache_data(ttl=3600)
def _compute_rolling_corr(port_df, gold_df, window=60):
    """计算滚动相关性"""
    merged = port_df.merge(gold_df, on="date", how="inner").sort_values("date")
    if len(merged) < window:
        return pd.DataFrame()
    merged["rolling_corr"] = merged["portfolio_return"].rolling(window).corr(merged["gold_return"])
    return merged


def render_gold_portfolio_correlation():
    """渲染黄金与组合相关性分析"""
    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">'
        '黄金与组合相关性<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '分析黄金价格与投资组合净值的收益率相关性，评估黄金作为组合对冲/分散资产的有效性。</span></div>',
        unsafe_allow_html=True,
    )
    
    period = st.selectbox("分析周期", ["近6个月", "近1年", "近2年"], key="gp_corr_period")
    period_map = {"近6个月": 180, "近1年": 365, "近2年": 730}
    n_days = period_map.get(period, 365)
    
    port_df = _load_portfolio_returns(n_days)
    gold_df = _load_gold_returns(n_days)
    
    if port_df.empty:
        st.info("暂无投资组合数据")
        return
    if gold_df.empty:
        st.info("暂无黄金价格数据")
        return
    
    # 合并数据
    merged = port_df.merge(gold_df, on="date", how="inner").sort_values("date")
    if len(merged) < 30:
        st.warning("有效数据不足30天，无法计算相关性")
        return
    
    # 总体相关性
    overall_corr = merged["portfolio_return"].corr(merged["gold_return"])
    
    # 摘要指标
    c1, c2, c3 = st.columns(3)
    with c1:
        corr_color = "#22c55e" if abs(overall_corr) < 0.3 else "#f59e0b" if abs(overall_corr) < 0.6 else "#ef4444"
        st.metric("总体相关系数", f"{overall_corr:.3f}")
        if abs(overall_corr) < 0.3:
            hedge_desc = "低相关，分散效果好"
        elif abs(overall_corr) < 0.6:
            hedge_desc = "中等相关"
        else:
            hedge_desc = "高相关，对冲效果弱"
        st.caption(f"*{hedge_desc}*")
    with c2:
        gold_cum = (1 + merged["gold_return"]).prod() - 1
        port_cum = (1 + merged["portfolio_return"]).prod() - 1
        st.metric("区间金价涨幅", f"{gold_cum*100:+.2f}%")
    with c3:
        st.metric("区间组合涨幅", f"{port_cum*100:+.2f}%")
    
    # 滚动相关性图
    rolling_window = st.selectbox("滚动窗口", [30, 60, 120], index=1, key="gp_rolling_window")
    rolling_df = _compute_rolling_corr(port_df, gold_df, window=rolling_window)
    
    if not rolling_df.empty:
        fig_rolling = go.Figure()
        fig_rolling.add_trace(go.Scatter(
            x=rolling_df["date"], y=rolling_df["rolling_corr"],
            mode="lines", name=f"{rolling_window}日滚动相关性",
            line=dict(color="#FFD700", width=2),
            fill="tozeroy", fillcolor="rgba(255,215,0,0.08)",
        ))
        fig_rolling.add_hline(y=0, line_dash="dash", line_color="#8b949e", opacity=0.5)
        fig_rolling.add_hrect(y0=0.3, y1=1, fillcolor="rgba(239,68,68,0.05)", line_width=0)
        fig_rolling.add_hrect(y0=-1, y1=-0.3, fillcolor="rgba(239,68,68,0.05)", line_width=0)
        fig_rolling.add_hrect(y0=-0.3, y1=0.3, fillcolor="rgba(34,197,94,0.05)", line_width=0)
        fig_rolling.update_layout(
            height=250,
            plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
            font=dict(color=DARK_FONT_COLOR, size=11),
            margin=dict(l=50, r=20, t=10, b=30),
            xaxis=dict(title="", showgrid=True, gridcolor=GRID_COLOR),
            yaxis=dict(title="相关系数", range=[-1, 1], showgrid=True, gridcolor=GRID_COLOR),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        font=dict(size=10, color="#8b949e")),
        )
        st.plotly_chart(fig_rolling, width="stretch")
    
    # 收益率散点图
    fig_scatter = go.Figure()
    fig_scatter.add_trace(go.Scatter(
        x=merged["gold_return"] * 100,
        y=merged["portfolio_return"] * 100,
        mode="markers",
        name="日收益率",
        marker=dict(color="#58a6ff", size=4, opacity=0.6),
    ))
    # 拟合线
    if len(merged) >= 20:
        x_vals = merged["gold_return"].values
        y_vals = merged["portfolio_return"].values
        z = np.polyfit(x_vals, y_vals, 1)
        p = np.poly1d(z)
        x_fit = np.linspace(x_vals.min(), x_vals.max(), 100)
        y_fit = p(x_fit)
        fig_scatter.add_trace(go.Scatter(
            x=x_fit * 100, y=y_fit * 100,
            mode="lines",
            name=f"拟合线 (r={overall_corr:.3f})",
            line=dict(color="#FF7043", width=2, dash="dash"),
        ))
    fig_scatter.update_layout(
        height=300,
        plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
        font=dict(color=DARK_FONT_COLOR, size=11),
        margin=dict(l=50, r=20, t=10, b=30),
        xaxis=dict(title="Au99.99日收益率 (%)", showgrid=True, gridcolor=GRID_COLOR),
        yaxis=dict(title="组合日收益率 (%)", showgrid=True, gridcolor=GRID_COLOR),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    font=dict(size=10, color="#8b949e")),
    )
    st.plotly_chart(fig_scatter, width="stretch")
