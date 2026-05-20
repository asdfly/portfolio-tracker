"""
Tab13: 数据质量监控面板
展示数据新鲜度、覆盖率、回测完整度、综合质量评分
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import date
from src.utils.data_quality import DataQualityChecker
from src.utils.database import get_db_connection
from config.settings import DATABASE_PATH


def _score_ring(score: float, grade: str) -> go.Figure:
    """生成质量评分环形图"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": f"综合评分 <b>{grade}</b>", "font": {"size": 16, "color": "#c9d1d9"}},
        number={"font": {"size": 48, "color": "#58a6ff"}, "suffix": "/100"},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#484f58",
                     "tickfont": {"size": 10}},
            "bar": {"color": "#1f6feb", "thickness": 0.3},
            "bgcolor": "#161b22",
            "borderwidth": 0,
            "steps": [
                {"range": [0, 60], "color": "#2d1215"},
                {"range": [60, 80], "color": "#2d2006"},
                {"range": [80, 90], "color": "#0d2818"},
                {"range": [90, 100], "color": "#0d2818"},
            ],
            "threshold": {
                "line": {"color": "#22c55e", "width": 4},
                "thickness": 0.8,
                "value": score,
            },
        },
    ))
    fig.update_layout(
        paper_bgcolor="#161b22",
        height=280,
        margin=dict(l=20, r=20, t=40, b=0),
    )
    return fig


def _freshness_heatmap(freshness_data: list) -> go.Figure:
    """数据新鲜度状态可视化"""
    labels = [f["label"] for f in freshness_data]
    status_map = {"OK": 3, "WARN": 2, "STALE": 1, "EMPTY": 0, "ERROR": 0}
    values = [status_map.get(f["status"], 0) for f in freshness_data]
    lags = [f["days_lag"] for f in freshness_data]

    colors = []
    for v in values:
        if v == 3:
            colors.append("#22c55e")
        elif v == 2:
            colors.append("#f59e0b")
        elif v == 1:
            colors.append("#ef4444")
        else:
            colors.append("#484f58")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=labels, y=values,
        marker_color=colors,
        text=[f"{l}天" for l in lags],
        textposition="auto",
        textfont=dict(color="#c9d1d9", size=12),
        hovertemplate="%{x}<br>延迟: %{text}<extra></extra>",
    ))

    # 添加OK/WARN/STALE区域
    fig.add_hrect(y0=2.5, y1=3.5, fillcolor="rgba(34,197,94,0.08)", line_width=0)
    fig.add_hrect(y0=1.5, y1=2.5, fillcolor="rgba(245,158,11,0.08)", line_width=0)
    fig.add_hrect(y0=0.5, y1=1.5, fillcolor="rgba(239,68,68,0.08)", line_width=0)

    fig.update_layout(
        title=dict(text="数据新鲜度 (延迟天数)", font=dict(size=13, color="#c9d1d9")),
        plot_bgcolor="#161b22",
        paper_bgcolor="#161b22",
        font=dict(color="#8b949e"),
        xaxis=dict(gridcolor="#21262d", tickangle=0),
        yaxis=dict(gridcolor="#21262d", range=[0, 4],
                   tickvals=[0, 1, 2, 3],
                   ticktext=["", "STALE", "WARN", "OK"]),
        height=300,
        margin=dict(l=50, r=20, t=40, b=30),
        showlegend=False,
    )
    return fig


def _coverage_table(coverage_data: dict) -> pd.DataFrame:
    """生成覆盖率摘要表格"""
    rows = []
    for table, info in coverage_data.items():
        rows.append({
            "数据表": table,
            "记录数": f"{info['total_rows']:,}",
            "覆盖标的": f"{info['distinct_codes']}只" if info["distinct_codes"] > 0 else "-",
            "时间跨度": info["date_range"],
        })
    return pd.DataFrame(rows)


def _backtest_summary(backtest_data: dict) -> pd.DataFrame:
    """生成回测覆盖摘要"""
    rows = []
    periods = backtest_data.get("periods_per_indicator", {})
    for name, period_count in periods.items():
        rows.append({"指标名称": name, "覆盖周期数": period_count})
    return pd.DataFrame(rows)


def render_tab13(**kwargs):
    """渲染Tab13: 数据质量监控"""
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '数据质量监控'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '全库数据完整性、新鲜度、覆盖率和回测质量综合评估。</span></div>',
        unsafe_allow_html=True,
    )

    if st.button("\U0001f504 刷新检查", key="dq_refresh"):
        st.cache_data.clear()

    # 执行检查
    with st.spinner("正在检查数据质量..."):
        checker = DataQualityChecker(str(DATABASE_PATH))
        report = checker.run_full_check()

    # === 综合评分 + 三维度 ===
    col_score, col_dims = st.columns([1, 2])

    with col_score:
        fig = _score_ring(report["total_score"], report["grade"])
        st.plotly_chart(fig, width='stretch')

    with col_dims:
        dims = [
            ("新鲜度", report["freshness_score"], 40, "#22c55e"),
            ("覆盖度", report["coverage_score"], 30, "#58a6ff"),
            ("回测度", report["backtest_score"], 30, "#f59e0b"),
        ]
        for name, score, max_score, color in dims:
            pct = (score if score is not None else 0) / max_score if max_score > 0 else 0
            bar_bg = "#21262d"
            st.markdown(
                f'''
                <div style="margin-bottom:8px;">
                <span style="font-size:12px;color:#8b949e;">{name}</span>
                <span style="font-size:12px;color:{color};float:right;">{score}/{max_score}</span>
                <div style="height:8px;background:{bar_bg};border-radius:4px;overflow:hidden;">
                <div style="height:100%;width:{pct*100:.0f}%;background:{color};border-radius:4px;transition:width 0.3s;"></div>
                </div></div>
                ''', unsafe_allow_html=True
            )

    st.markdown("---")

    # === 新鲜度热力图 ===
    st.subheader("数据新鲜度")
    freshness = report["details"]["freshness"]
    fig_fresh = _freshness_heatmap(freshness)
    st.plotly_chart(fig_fresh, width='stretch')

    # 新鲜度详情表
    fresh_rows = []
    for f in freshness:
        icon = {"OK": "\u2705", "WARN": "\u26a0\ufe0f", "STALE": "\u274c", "EMPTY": "\u2796", "ERROR": "\u2753"}.get(f["status"], "?")
        fresh_rows.append({
            "状态": icon,
            "数据模块": f["label"],
            "最新日期": f["latest_date"],
            "延迟天数": f["days_lag"],
        })
    st.markdown(pd.DataFrame(fresh_rows).to_html(index=False, escape=False), unsafe_allow_html=True)

    st.markdown("---")

    # === 数据覆盖率 ===
    col_cov, col_bt = st.columns(2)

    with col_cov:
        st.subheader("数据覆盖率")
        cov_df = _coverage_table(report["details"]["coverage"])
        st.markdown(cov_df.to_html(index=False, escape=False), unsafe_allow_html=True)

    with col_bt:
        st.subheader("回测完整度")
        bt = report["details"]["backtest"]
        st.metric("指标模板", f"{bt['template_count']}个")
        st.metric("回测结果", f"{bt['result_count']}条")
        bt_df = _backtest_summary(bt)
        if not bt_df.empty:
            st.markdown(bt_df.to_html(index=False, escape=False), unsafe_allow_html=True)

    # === 全库概览 ===
    st.markdown("---")
    st.subheader("全库数据概览")
    conn = get_db_connection()
    try:
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            conn
        )
        overview_rows = []
        for _, row in tables.iterrows():
            tname = row["name"]
            try:
                count_df = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM [{tname}]", conn)
                cnt = count_df.iloc[0]["cnt"]
            except Exception:
                cnt = 0
            overview_rows.append({"数据表": tname, "记录数": f"{cnt:,}"})
        st.markdown(pd.DataFrame(overview_rows).to_html(index=False, escape=False), unsafe_allow_html=True)
    finally:
        conn.close()
