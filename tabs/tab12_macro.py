"""
Tab12: 宏观市场数据面板
展示汇率、国债收益率、黄金基准、LPR、Shibor、两融余额等宏观数据
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.utils.database import get_db_connection


def _load_macro_data(indicator_codes: list, days: int = 365) -> pd.DataFrame:
    """从macro_daily表加载指定指标的数据"""
    conn = get_db_connection()
    try:
        placeholders = ",".join(["?"] * len(indicator_codes))
        df = pd.read_sql_query(f"""
            SELECT date, indicator_code, name, value, change_pct
            FROM macro_daily
            WHERE indicator_code IN ({placeholders})
            ORDER BY date
        """, conn, params=indicator_codes)
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    df = df[df["date"] >= cutoff]
    return df


def _load_sentiment_data(indicator_codes: list, days: int = 365) -> pd.DataFrame:
    """从market_sentiment表加载指定指标的数据"""
    conn = get_db_connection()
    try:
        placeholders = ",".join(["?"] * len(indicator_codes))
        df = pd.read_sql_query(f"""
            SELECT date, indicator_code, name, value, change_pct, change_value
            FROM market_sentiment
            WHERE indicator_code IN ({placeholders})
            ORDER BY date
        """, conn, params=indicator_codes)
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    df = df[df["date"] >= cutoff]
    return df


def _style_fig(fig, title=""):
    """统一深色主题图表样式"""
    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color="#c9d1d9")),
        plot_bgcolor="#161b22",
        paper_bgcolor="#161b22",
        font=dict(color="#8b949e", size=11),
        xaxis=dict(gridcolor="#21262d", rangeslider=dict(visible=False)),
        yaxis=dict(gridcolor="#21262d"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10)),
        margin=dict(l=50, r=20, t=40, b=30),
        height=380,
    )
    return fig


def render_tab12(**kwargs):
    """渲染Tab12: 宏观市场"""
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '宏观市场数据'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '汇率、国债收益率、黄金基准价、LPR/Shibor利率、两融余额等核心宏观指标。</span></div>',
        unsafe_allow_html=True,
    )

    # 时间范围选择
    col_period, col_refresh = st.columns([3, 1])
    with col_period:
        period = st.selectbox(
            "时间范围", ["近30天", "近90天", "近半年", "近1年", "近2年", "全部"],
            key="macro_period", index=3
        )
    period_map = {"近30天": 30, "近90天": 90, "近半年": 180, "近1年": 365, "近2年": 730, "全部": 9999}
    days = period_map.get(period, 365)

    sub_tabs = st.tabs([
        "🌐 汇率",
        "📈 国债收益率",
        "🗺 黄金基准",
        "💰 利率",
        "📊 两融余额",
    ])

    with sub_tabs[0]:
        _render_exchange_rate(days)
    with sub_tabs[1]:
        _render_bond_yields(days)
    with sub_tabs[2]:
        _render_gold_benchmark(days)
    with sub_tabs[3]:
        _render_interest_rates(days)
    with sub_tabs[4]:
        _render_margin_data(days)


def _render_exchange_rate(days: int):
    """USD/CNY汇率"""
    df = _load_macro_data(["USD_CNY"], days)
    if df.empty:
        st.info("暂无USD/CNY汇率数据")
        return

    df = df.sort_values("date")
    latest = df.iloc[-1]
    change = latest.get("change_pct", 0)

    c1, c2, c3 = st.columns(3)
    c1.metric("最新汇率", f"{(latest['value'] or 0):.4f}", f"{change:+.2f}%")
    c2.metric("数据跨度", f"{df['date'].min().strftime('%Y-%m-%d')} ~ {df['date'].max().strftime('%Y-%m-%d')}")
    c3.metric("数据点", f"{len(df)}条")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["value"].fillna(method="ffill"), mode="lines",
                             name="USD/CNY", line=dict(color="#58a6ff", width=2),
                             hovertemplate="%{x|%Y-%m-%d}<br>汇率: %{y:.4f}<extra></extra>"))
    fig = _style_fig(fig, "USD/CNY 美元兑人民币汇率")
    st.plotly_chart(fig, width='stretch')


def _render_bond_yields(days: int):
    """中美国债收益率对比"""
    codes = ["CN_10Y_BOND", "US_10Y_BOND", "CN_2Y_BOND", "US_2Y_BOND", "CN_US_SPREAD"]
    df = _load_macro_data(codes, days)
    if df.empty:
        st.info("暂无国债收益率数据")
        return

    pivot = df.pivot_table(index="date", columns="indicator_code", values="value", aggfunc="first").reset_index()
    pivot["date"] = pd.to_datetime(pivot["date"])
    pivot = pivot.sort_values("date")

    # 最新数据
    if not pivot.empty:
        latest = pivot.iloc[-1]
        c1, c2, c3, c4 = st.columns(4)
        if "CN_10Y_BOND" in pivot.columns:
            c1.metric("中国10Y", f"{(latest['CN_10Y_BOND'] or 0):.2f}%")
        if "US_10Y_BOND" in pivot.columns:
            c2.metric("美国10Y", f"{(latest['US_10Y_BOND'] or 0):.2f}%")
        if "CN_2Y_BOND" in pivot.columns:
            c3.metric("中国2Y", f"{(latest['CN_2Y_BOND'] or 0):.2f}%")
        if "US_2Y_BOND" in pivot.columns:
            c4.metric("美国2Y", f"{(latest['US_2Y_BOND'] or 0):.2f}%")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 10Y收益率
    for code, color, name in [("CN_10Y_BOND", "#ef4444", "中国10Y"),
                                ("US_10Y_BOND", "#58a6ff", "美国10Y")]:
        if code in pivot.columns:
            fig.add_trace(go.Scatter(x=pivot["date"], y=pivot[code].ffill(), mode="lines",
                                     name=name, line=dict(color=color, width=2)), secondary_y=False)

    # 中美利差
    if "CN_US_SPREAD" in pivot.columns:
        spread = pivot[["date", "CN_US_SPREAD"]].dropna()
        fig.add_trace(go.Bar(x=spread["date"], y=spread["CN_US_SPREAD"],
                             name="中美利差(10Y)", marker_color="#f59e0b",
                             opacity=0.6), secondary_y=True)

    fig.update_yaxes(title_text="收益率 (%)", secondary_y=False, gridcolor="#21262d")
    fig.update_yaxes(title_text="利差 (bp)", secondary_y=True, gridcolor="#21262d")
    fig = _style_fig(fig, "中美国债收益率对比 (10Y)")
    fig.update_layout(xaxis=dict(gridcolor="#21262d", rangeslider=dict(visible=False)))
    st.plotly_chart(fig, width='stretch')


def _render_gold_benchmark(days: int):
    """黄金基准价格（COMEX + SGE）"""
    df = _load_macro_data(["COMEX_GOLD", "SGE_GOLD"], days)
    if df.empty:
        st.info("暂无黄金基准数据")
        return

    pivot = df.pivot_table(index="date", columns="indicator_code", values="value", aggfunc="first").reset_index()
    pivot["date"] = pd.to_datetime(pivot["date"])
    pivot = pivot.sort_values("date")

    if not pivot.empty:
        latest = pivot.iloc[-1]
        c1, c2 = st.columns(2)
        if "COMEX_GOLD" in pivot.columns and pd.notna(latest.get("COMEX_GOLD")):
            c1.metric("COMEX黄金", f"${latest['COMEX_GOLD']:.1f}/oz")
        if "SGE_GOLD" in pivot.columns and pd.notna(latest.get("SGE_GOLD")):
            c2.metric("上海金基准", f"¥{latest['SGE_GOLD']:.2f}/g")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    if "COMEX_GOLD" in pivot.columns:
        comex = pivot[["date", "COMEX_GOLD"]].dropna()
        fig.add_trace(go.Scatter(x=comex["date"], y=comex["COMEX_GOLD"], mode="lines",
                                 name="COMEX黄金 ($/oz)", line=dict(color="#FFD700", width=2)),
                      secondary_y=False)

    if "SGE_GOLD" in pivot.columns:
        sge = pivot[["date", "SGE_GOLD"]].dropna()
        fig.add_trace(go.Scatter(x=sge["date"], y=sge["SGE_GOLD"], mode="lines",
                                 name="上海金 (¥/g)", line=dict(color="#ef4444", width=2)),
                      secondary_y=True)

    fig.update_yaxes(title_text="$/oz", secondary_y=False, gridcolor="#21262d")
    fig.update_yaxes(title_text="¥/g", secondary_y=True, gridcolor="#21262d")
    fig = _style_fig(fig, "COMEX黄金 vs 上海金基准价")
    fig.update_layout(xaxis=dict(gridcolor="#21262d", rangeslider=dict(visible=False)))
    st.plotly_chart(fig, width='stretch')


def _render_interest_rates(days: int):
    """LPR + Shibor利率"""
    # LPR
    lpr_df = _load_macro_data(["LPR_1Y", "LPR_5Y"], days)
    # Shibor
    shibor_df = _load_macro_data(["SHIBOR_ON"], days)

    col_lpr, col_shibor = st.columns(2)

    with col_lpr:
        st.markdown("**LPR贷款市场报价利率**")
        if lpr_df.empty:
            st.info("暂无LPR数据")
        else:
            pivot = lpr_df.pivot_table(index="date", columns="indicator_code", values="value", aggfunc="first").reset_index()
            pivot["date"] = pd.to_datetime(pivot["date"])
            pivot = pivot.sort_values("date")

            if not pivot.empty:
                latest = pivot.iloc[-1]
                c1, c2 = st.columns(2)
                if "LPR_1Y" in pivot.columns:
                    c1.metric("LPR 1Y", f"{latest['LPR_1Y']:.2f}%")
                if "LPR_5Y" in pivot.columns:
                    c2.metric("LPR 5Y", f"{latest['LPR_5Y']:.2f}%")

            fig = go.Figure()
            for code, color, name in [("LPR_1Y", "#58a6ff", "LPR 1Y"),
                                       ("LPR_5Y", "#f59e0b", "LPR 5Y")]:
                if code in pivot.columns:
                    fig.add_trace(go.Scatter(x=pivot["date"], y=pivot[code], mode="lines+markers",
                                             name=name, line=dict(color=color, width=2),
                                             marker=dict(size=4)))
            fig = _style_fig(fig, "")
            fig.update_layout(height=300)
            st.plotly_chart(fig, width='stretch')

    with col_shibor:
        st.markdown("**Shibor隔夜利率**")
        if shibor_df.empty:
            st.info("暂无Shibor数据")
        else:
            shibor_df = shibor_df.sort_values("date")
            latest = shibor_df.iloc[-1]
            st.metric("最新Shibor ON", f"{(latest['value'] or 0):.3f}%", f"{latest.get('change_pct', 0):+.3f}%")

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=shibor_df["date"], y=shibor_df["value"].fillna(method="ffill"), mode="lines",
                                     name="Shibor ON", line=dict(color="#22c55e", width=1.5),
                                     fill="tozeroy", fillcolor="rgba(34,197,94,0.1)"))
            fig = _style_fig(fig, "")
            fig.update_layout(height=300)
            st.plotly_chart(fig, width='stretch')


def _render_margin_data(days: int):
    """两融余额（沪深合计）"""
    df = _load_sentiment_data(["MARGIN_TOTAL", "MARGIN_上", "MARGIN_深"], days)
    if df.empty:
        st.info("暂无两融余额数据")
        return

    # 合计余额
    total = df[df["indicator_code"] == "MARGIN_TOTAL"].sort_values("date")
    sh = df[df["indicator_code"] == "MARGIN_上"].sort_values("date")
    sz = df[df["indicator_code"] == "MARGIN_深"].sort_values("date")

    if not total.empty:
        latest = total.iloc[-1]
        change = latest.get("change_value", 0)
        c1, c2, c3 = st.columns(3)
        val = latest['value'] or 0
        chg = change if change is not None else 0
        c1.metric("两融余额合计", f"{val/1e4:.0f}亿元", f"{chg/1e4:+.0f}亿元")
        if not sh.empty:
            sh_val = sh.iloc[-1]['value'] or 0
            c2.metric("沪市", f"{sh_val/1e4:.0f}亿元")
        if not sz.empty:
            sz_val = sz.iloc[-1]['value'] or 0
            c3.metric("深市", f"{sz_val/1e4:.0f}亿元")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 合计余额（左轴）
    if not total.empty:
        fig.add_trace(go.Scatter(x=total["date"], y=total["value"].fillna(0)/1e4, mode="lines",
                                 name="两融合计", line=dict(color="#58a6ff", width=2)),
                      secondary_y=False)

    # 变化额（右轴，柱状图）
    if not total.empty and "change_value" in total.columns:
        fig.add_trace(go.Bar(x=total["date"], y=total["change_value"].fillna(0)/1e4,
                             name="日变化(亿元)", marker_color="#f59e0b", opacity=0.5,
                             yaxis="y2"),
                      secondary_y=True)

    fig.update_yaxes(title_text="余额(亿元)", secondary_y=False, gridcolor="#21262d")
    fig.update_yaxes(title_text="日变化(亿元)", secondary_y=True, gridcolor="#21262d")
    fig = _style_fig(fig, "沪深两市融资融券余额")
    fig.update_layout(xaxis=dict(gridcolor="#21262d", rangeslider=dict(visible=False)))
    st.plotly_chart(fig, width='stretch')
