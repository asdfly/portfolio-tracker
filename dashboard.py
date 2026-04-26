#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资组合跟踪分析系统 - Streamlit 可视化 Dashboard
启动方式: streamlit run dashboard.py

性能优化:
  - @st.cache_data 缓存所有数据库查询，相同参数命中缓存零延迟
  - 图表数据自动降采样，4000天数据压缩到<=500个点
  - SQLite 索引加速查询
"""

import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

import sqlite3
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

from config.settings import DATABASE_PATH, INDEX_CODES

# ==================== 数据库索引 ====================
def _ensure_indexes():
    """确保数据库索引存在（只执行一次）"""
    conn = sqlite3.connect(str(DATABASE_PATH))
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_snap_date ON portfolio_snapshots(date)",
        "CREATE INDEX IF NOT EXISTS idx_snap_code_date ON portfolio_snapshots(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_summary_date ON portfolio_summary(date)",
        "CREATE INDEX IF NOT EXISTS idx_idx_quote_code_date ON index_quotes(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_tech_date ON etf_technical(date)",
        "CREATE INDEX IF NOT EXISTS idx_tech_code_date ON etf_technical(code, date)",
    ]
    for sql in indexes:
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.commit()
    conn.close()

_ensure_indexes()

# ==================== 页面配置 ====================
st.set_page_config(
    page_title="投资组合跟踪分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== 降采样工具 ====================
def downsample(df, date_col='date', max_points=500):
    """将时间序列降采样到max_points个点，保留边界值"""
    n = len(df)
    if n <= max_points:
        return df

    # 确保首尾在结果中
    step = max(1, (n - 2) // (max_points - 2))
    indices = list(range(0, n, step))
    if indices[-1] != n - 1:
        indices.append(n - 1)
    if indices[0] != 0:
        indices.insert(0, 0)

    # 去重排序
    indices = sorted(set(indices))
    return df.iloc[indices].reset_index(drop=True)


# ==================== 数据读取工具（带缓存） ====================
def get_db_connection():
    """获取数据库连接"""
    return sqlite3.connect(str(DATABASE_PATH), check_same_thread=False)


@st.cache_data(ttl=300, show_spinner=False)
def load_positions(date_str=None):
    """加载持仓数据"""
    conn = get_db_connection()
    if date_str:
        query = "SELECT * FROM portfolio_snapshots WHERE date = ? ORDER BY market_value DESC"
        df = pd.read_sql_query(query, conn, params=(date_str,))
    else:
        query = """
            SELECT * FROM portfolio_snapshots 
            WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
            ORDER BY market_value DESC
        """
        df = pd.read_sql_query(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_summary(days=60, end_date=None):
    """加载组合汇总历史"""
    conn = get_db_connection()
    if end_date:
        query = "SELECT * FROM portfolio_summary WHERE date <= ? ORDER BY date DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(end_date, days))
    else:
        query = "SELECT * FROM portfolio_summary ORDER BY date DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(days,))
    df = df.sort_values('date').reset_index(drop=True)
    conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_index_quotes(code='sh000300', days=60, end_date=None):
    """加载指数行情"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, close, volume 
            FROM index_quotes 
            WHERE code = ? AND date <= ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, end_date, days))
    else:
        query = """
            SELECT date, close, volume 
            FROM index_quotes 
            WHERE code = ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, days))
    df = df.sort_values('date').reset_index(drop=True)
    conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_technical():
    """加载技术指标，关联ETF名称"""
    conn = get_db_connection()
    query = """
        SELECT t.*, p.name 
        FROM etf_technical t 
        LEFT JOIN portfolio_snapshots p ON t.code = p.code AND t.date = p.date
        WHERE t.date = (SELECT MAX(date) FROM etf_technical)
    """
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df['name'] = df['name'].fillna(df['code'])
    conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_alerts(limit=10):
    """加载告警"""
    conn = get_db_connection()
    query = "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_execution_logs(limit=10):
    """加载执行日志"""
    conn = get_db_connection()
    query = "SELECT * FROM execution_logs ORDER BY created_at DESC LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


@st.cache_data(ttl=600, show_spinner=False)
def get_available_dates():
    """获取所有交易日日期"""
    conn = get_db_connection()
    query = "SELECT DISTINCT date FROM portfolio_snapshots ORDER BY date DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['date'].tolist()


# ==================== 样式工具 ====================
def format_value(val, prefix="", suffix="", decimals=2):
    """格式化数值"""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    if isinstance(val, (int, float)):
        return f"{prefix}{val:,.{decimals}f}{suffix}"
    return str(val)


# ==================== 主页面 ====================
def main():
    # 自定义CSS
    st.markdown(
        """
        <style>
        .stApp { background-color: #0d1117; }
        .main-header {
            font-size: 28px; font-weight: bold; color: #58a6ff;
            text-align: center; padding: 20px 0 10px 0;
        }
        .sub-header {
            font-size: 14px; color: #8b949e; text-align: center; padding-bottom: 15px;
        }
        .section-title {
            font-size: 18px; font-weight: bold; color: #c9d1d9;
            padding: 10px 0 5px 0; border-bottom: 1px solid #30363d;
        }
        </style>
        """, unsafe_allow_html=True
    )

    # 标题
    st.markdown('<div class="main-header">📊 投资组合跟踪分析系统</div>', unsafe_allow_html=True)

    # 获取数据
    available_dates = get_available_dates()
    if not available_dates:
        st.warning("暂无数据，请先运行 run_analysis.py")
        return

    # 侧边栏
    with st.sidebar:
        st.markdown("### 🔧 控制面板")

        selected_date = st.selectbox(
            "选择日期", available_dates,
            index=0,
            format_func=lambda x: f"{x} {'(最新)' if x == available_dates[0] else ''}"
        )

        # 快捷预设
        preset = st.radio("时间范围", ["3个月", "6个月", "1年", "2年", "5年", "全部", "自定义"],
                          horizontal=True, index=2)
        preset_days = {"3个月": 60, "6个月": 120, "1年": 250, "2年": 500, "5年": 1250, "全部": 4000}
        if preset == "自定义":
            show_days = st.slider("自定义天数", min_value=10, max_value=4000, value=250, step=10)
        else:
            show_days = preset_days[preset]

        st.markdown("---")
        st.markdown("### 📋 系统信息")

        logs = load_execution_logs(5)
        if not logs.empty:
            for _, log in logs.iterrows():
                status_icon = "✅" if log['status'] == 'success' else "❌" if log['status'] == 'failed' else "⏳"
                st.markdown(f"{status_icon} `{log['task_name']}` - {log['status']}")
                if pd.notna(log.get('duration_seconds')):
                    st.caption(f"  耗时: {log['duration_seconds']:.1f}s")

        st.markdown("---")
        st.markdown(f"*数据更新: {available_dates[0]}*")

    # 加载数据（带缓存，相同参数不重复查询）
    positions = load_positions(selected_date)
    summary = load_summary(show_days, selected_date)
    technical = load_technical()

    # 预生成缓存：最近10个交易日 x 各时间预设，后台静默触发一次
    _preset_days_list = [60, 120, 250, 500, 1250, 4000]
    _recent = available_dates[:10]  # 最近10个交易日
    with st.spinner(""):
        for _d in _recent:
            load_positions(_d)
            load_summary(show_days, _d)
            load_index_quotes("sh000300", show_days, _d)
        for _days in _preset_days_list:
            load_summary(_days, available_dates[0])
            load_index_quotes("sh000300", _days, available_dates[0])

    if positions.empty:
        st.warning(f"{selected_date} 无持仓数据")
        return

    # ========== 概览指标 ==========
    latest_summary = summary.iloc[-1] if not summary.empty else {}
    total_value = latest_summary.get('total_value', 0)
    total_cost = latest_summary.get('total_cost', 0)
    total_pnl = latest_summary.get('total_pnl', 0)
    total_return = (total_pnl / total_cost * 100) if total_cost > 0 else 0
    daily_return = latest_summary.get('daily_return', 0)
    daily_pnl = latest_summary.get('daily_pnl', 0)
    sharpe = latest_summary.get('sharpe_ratio')
    max_dd = latest_summary.get('max_drawdown')
    volatility = latest_summary.get('volatility')
    profit_count = latest_summary.get('profit_count', 0)
    loss_count = latest_summary.get('loss_count', 0)

    # 概览卡片行
    cols = st.columns(6)
    with cols[0]:
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #58a6ff;">'
            f'<div style="font-size:11px;color:#8b949e;">总市值</div>'
            f'<div style="font-size:20px;font-weight:bold;color:#58a6ff;">¥{format_value(total_value)}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[1]:
        pnl_color = "#22c55e" if total_pnl >= 0 else "#ef4444"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {pnl_color};">'
            f'<div style="font-size:11px;color:#8b949e;">总盈亏</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{pnl_color};">{format_value(total_pnl, prefix="¥")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(total_return, suffix="%")}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[2]:
        dr_color = "#22c55e" if daily_return >= 0 else "#ef4444"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dr_color};">'
            f'<div style="font-size:11px;color:#8b949e;">日收益</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dr_color};">{format_value(daily_return, suffix="%")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(daily_pnl, prefix="¥")}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[3]:
        sharpe_color = "#22c55e" if (sharpe and sharpe > 0.5) else "#f59e0b" if sharpe else "#888"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {sharpe_color};">'
            f'<div style="font-size:11px;color:#8b949e;">夏普比率</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{sharpe_color};">{format_value(sharpe, decimals=3)}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[4]:
        dd_color = "#ef4444" if (max_dd and abs(max_dd) > 10) else "#f59e0b" if (max_dd and abs(max_dd) > 5) else "#22c55e"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dd_color};">'
            f'<div style="font-size:11px;color:#8b949e;">最大回撤</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dd_color};">{format_value(max_dd, suffix="%")}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[5]:
        vol_color = "#ef4444" if (volatility and volatility > 25) else "#f59e0b" if (volatility and volatility > 15) else "#22c55e"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {vol_color};">'
            f'<div style="font-size:11px;color:#8b949e;">年化波动率</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{vol_color};">{format_value(volatility, suffix="%")}</div>'
            f'</div>', unsafe_allow_html=True)

    # ========== 图表行1: 净值曲线 + 收益分布 ==========
    tab1, tab2, tab3 = st.tabs(["📈 净值走势", "📊 持仓分布", "⚠️ 风险分析"])

    with tab1:
        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.markdown('<div class="section-title">组合净值走势</div>', unsafe_allow_html=True)
            if not summary.empty and len(summary) > 1:
                # 计算累计净值（基准100）
                base_value = summary.iloc[0]['total_value']
                summary_plot = summary.copy()
                summary_plot['nav'] = summary_plot['total_value'] / base_value * 100

                # 降采样用于图表渲染
                chart_data = downsample(summary_plot, max_points=500)

                # 沪深300对比
                hs300 = load_index_quotes('sh000300', show_days + 10, selected_date)
                if not hs300.empty:
                    hs300_base = hs300.iloc[0]['close']
                    hs300_plot = hs300.copy()
                    hs300_plot['nav'] = hs300_plot['close'] / hs300_base * 100
                    hs300_chart = downsample(hs300_plot, max_points=500)

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=hs300_chart['date'], y=hs300_chart['nav'],
                        mode='lines', name='沪深300',
                        line=dict(color='#8b949e', width=1.5, dash='dash')
                    ))
                    fig.add_trace(go.Scatter(
                        x=chart_data['date'], y=chart_data['nav'],
                        mode='lines', name='投资组合',
                        line=dict(color='#58a6ff', width=2)
                    ))
                else:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=chart_data['date'], y=chart_data['nav'],
                        mode='lines', name='投资组合',
                        line=dict(color='#58a6ff', width=2)
                    ))

                fig.update_layout(
                    height=350,
                    plot_bgcolor='#0d1117',
                    paper_bgcolor='#0d1117',
                    font=dict(color='#c9d1d9', size=11),
                    margin=dict(l=50, r=20, t=10, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                                font=dict(size=11)),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(title='净值 (基准100)', showgrid=True, gridcolor='#21262d')
                )
                st.plotly_chart(fig, width='stretch')

        with col_right:
            st.markdown('<div class="section-title">日收益率分布</div>', unsafe_allow_html=True)
            if not summary.empty and 'daily_return' in summary.columns and len(summary) > 5:
                # 用原始数据计算分布（不降采样，数据量不大）
                daily_rets = summary['daily_return'].dropna().values
                if len(daily_rets) > 0:
                    fig_hist = go.Figure()
                    fig_hist.add_trace(go.Histogram(
                        x=daily_rets,
                        nbinsx=40,
                        marker_color='#58a6ff',
                        marker_line_color='#0d1117',
                        marker_line_width=0.5,
                        opacity=0.85
                    ))
                    mean_ret = np.mean(daily_rets)
                    fig_hist.add_vline(x=mean_ret, line_dash="dash", line_color="#f59e0b",
                                       annotation_text=f"均值 {mean_ret:.3f}%")
                    fig_hist.update_layout(
                        height=200,
                        plot_bgcolor='#0d1117',
                        paper_bgcolor='#0d1117',
                        font=dict(color='#c9d1d9', size=11),
                        margin=dict(l=50, r=20, t=10, b=40),
                        xaxis=dict(title='日收益率 (%)', showgrid=True, gridcolor='#21262d'),
                        yaxis=dict(title='天数', showgrid=True, gridcolor='#21262d')
                    )
                    st.plotly_chart(fig_hist, width='stretch')

        # 日收益柱状图（降采样）
        st.markdown('<div class="section-title">每日盈亏</div>', unsafe_allow_html=True)
        if not summary.empty and 'daily_pnl' in summary.columns and len(summary) > 1:
            bar_data = downsample(summary[['date', 'daily_pnl']].copy(), max_points=500)
            colors = ['#22c55e' if dp >= 0 else '#ef4444' for dp in bar_data['daily_pnl']]
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                x=bar_data['date'], y=bar_data['daily_pnl'],
                marker_color=colors,
                name='日盈亏'
            ))
            fig_bar.update_layout(
                height=200,
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=50, r=20, t=10, b=40),
                xaxis=dict(showgrid=False, tickfont=dict(size=9)),
                yaxis=dict(title='盈亏 (¥)', showgrid=True, gridcolor='#21262d')
            )
            st.plotly_chart(fig_bar, width='stretch')

    with tab2:
        col_dist, col_table = st.columns([1, 1])

        with col_dist:
            st.markdown('<div class="section-title">持仓分布</div>', unsafe_allow_html=True)
            if not positions.empty:
                fig_pie = go.Figure(go.Pie(
                    labels=positions['name'],
                    values=positions['market_value'],
                    hole=0.45,
                    textinfo='label+percent',
                    textfont=dict(size=10),
                    marker=dict(colors=[
                        '#58a6ff', '#22c55e', '#f59e0b', '#ef4444', '#a855f7',
                        '#06b6d4', '#f97316', '#ec4899', '#84cc16', '#6366f1',
                        '#14b8a6', '#e11d48', '#8b5cf6', '#0ea5e9', '#d946ef',
                        '#10b981', '#f43f5e', '#6d28d9', '#0891b2', '#c026d3',
                        '#65a30d', '#be123c', '#7c3aed'
                    ])
                ))
                fig_pie.update_layout(
                    height=400,
                    plot_bgcolor='#0d1117',
                    paper_bgcolor='#0d1117',
                    font=dict(color='#c9d1d9'),
                    margin=dict(l=10, r=10, t=10, b=10),
                    showlegend=False
                )
                st.plotly_chart(fig_pie, width='stretch')

        with col_table:
            st.markdown('<div class="section-title">持仓明细</div>', unsafe_allow_html=True)
            if not positions.empty:
                display_df = positions[['name', 'code', 'quantity', 'cost_price', 'current_price',
                                       'market_value', 'pnl', 'pnl_rate']].copy()
                display_df.columns = ['名称', '代码', '持仓量', '成本价', '现价', '市值', '盈亏', '收益率%']
                display_df['持仓量'] = display_df['持仓量'].apply(lambda x: f"{x:,.0f}")
                display_df['成本价'] = display_df['成本价'].apply(lambda x: f"{x:.3f}")
                display_df['现价'] = display_df['现价'].apply(lambda x: f"{x:.3f}")
                display_df['市值'] = display_df['市值'].apply(lambda x: f"¥{x:,.0f}")
                display_df['盈亏'] = display_df['盈亏'].apply(lambda x: f"¥{x:,.0f}")
                display_df['收益率%'] = display_df['收益率%'].apply(
                    lambda x: f'<span style="color:{"#22c55e" if x >= 0 else "#ef4444"}">{x:.2f}%</span>'
                )

                st.markdown(display_df.to_html(index=False, escape=False), unsafe_allow_html=True)

    with tab3:
        col_risk_gauge, col_risk_detail = st.columns([1, 1])

        with col_risk_gauge:
            st.markdown('<div class="section-title">风险指标仪表盘</div>', unsafe_allow_html=True)

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

            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=risk_score,
                number={'suffix': '分', 'font': {'size': 40, 'color': risk_color}},
                gauge={
                    'axis': {'range': [0, 100], 'tickcolor': '#8b949e', 'tickfont': {'size': 10}},
                    'bar': {'color': risk_color},
                    'bgcolor': '#161b22',
                    'steps': [
                        {'range': [0, 40], 'color': 'rgba(239,68,68,0.15)'},
                        {'range': [40, 70], 'color': 'rgba(245,158,11,0.15)'},
                        {'range': [70, 100], 'color': 'rgba(34,197,94,0.15)'}
                    ],
                    'threshold': {
                        'line': {'color': risk_color, 'width': 3},
                        'thickness': 0.8,
                        'value': risk_score
                    }
                }
            ))
            fig_gauge.update_layout(
                height=250,
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9'),
                margin=dict(l=30, r=30, t=10, b=10)
            )
            st.plotly_chart(fig_gauge, width='stretch')

            st.markdown(f'<div style="text-align:center;color:{risk_color};font-size:16px;font-weight:bold;">'
                        f'{risk_label}</div>', unsafe_allow_html=True)

        with col_risk_detail:
            st.markdown('<div class="section-title">风险指标详情</div>', unsafe_allow_html=True)

            risk_metrics = [
                ("夏普比率", sharpe, "衡量风险调整后收益，>1为优秀"),
                ("最大回撤", max_dd, "历史最大亏损幅度"),
                ("年化波动率", volatility, "收益率的标准差，越高越不稳定"),
                ("盈亏比", f"{profit_count}:{loss_count}" if profit_count or loss_count else "N/A",
                 f"盈利{profit_count}只 vs 亏损{loss_count}只"),
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
                    f'</div>'
                    f'<div style="font-size:11px;color:#484f58;">{desc}</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

        # 回撤曲线（降采样）
        if not summary.empty and len(summary) > 5:
            st.markdown('<div class="section-title">回撤曲线</div>', unsafe_allow_html=True)
            dd_data = summary[['date', 'total_value']].copy()
            dd_data['drawdown'] = (dd_data['total_value'] - dd_data['total_value'].cummax()) / dd_data['total_value'].cummax() * 100
            dd_chart = downsample(dd_data, max_points=500)

            fig_dd = go.Figure()
            fig_dd.add_trace(go.Scatter(
                x=dd_chart['date'], y=dd_chart['drawdown'],
                mode='lines', name='回撤',
                fill='tozeroy',
                line=dict(color='#ef4444', width=1.5),
                fillcolor='rgba(239,68,68,0.15)'
            ))
            fig_dd.update_layout(
                height=200,
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=50, r=20, t=10, b=40),
                xaxis=dict(showgrid=False),
                yaxis=dict(title='回撤 (%)', showgrid=True, gridcolor='#21262d')
            )
            st.plotly_chart(fig_dd, width='stretch')

    # ========== 技术指标 ==========
    st.markdown('<div class="section-title" style="margin-top:20px;">🔍 技术指标信号</div>', unsafe_allow_html=True)

    if not technical.empty:
        trend_map = {'bullish': ('看多', '#22c55e'), 'bearish': ('看空', '#ef4444'),
                     'neutral': ('中性', '#f59e0b'), None: ('--', '#888')}

        tech_cols = st.columns(5)
        for idx, (_, row) in enumerate(technical.iterrows()):
            if idx >= 10:
                break
            with tech_cols[idx % 5]:
                trend_label, trend_color = trend_map.get(row.get('trend'), ('--', '#888'))
                rsi_val = row.get('rsi_value', 0)
                rsi_status = row.get('rsi_status', '--')
                ma_signal = row.get('ma_signal', '--')
                macd_signal = row.get('macd_signal', '--')

                st.markdown(
                    f'<div style="padding:10px;border-radius:8px;background:#161b22;'
                    f'border-top:2px solid {trend_color};margin-bottom:6px;">'
                    f'<div style="font-size:12px;color:#c9d1d9;font-weight:bold;white-space:nowrap;'
                    f'overflow:hidden;text-overflow:ellipsis;">{row.get("name", row.get("code", "未知"))}</div>'
                    f'<div style="font-size:11px;color:{trend_color};">{trend_label}</div>'
                    f'<div style="font-size:10px;color:#8b949e;">RSI: {rsi_val:.1f} ({rsi_status})</div>'
                    f'<div style="font-size:10px;color:#8b949e;">MA: {ma_signal}</div>'
                    f'<div style="font-size:10px;color:#8b949e;">MACD: {macd_signal}</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

    # ========== 智能建议 ==========
    report_dir = PROJECT_ROOT / "data" / "reports"
    if report_dir.exists():
        report_files = sorted(report_dir.glob("smart_report_*.md"), reverse=True)
        if report_files:
            with st.expander("💡 智能分析建议（最新报告）", expanded=False):
                with open(report_files[0], "r", encoding="utf-8") as f:
                    report_text = f.read()
                st.markdown(report_text[:3000] + ("..." if len(report_text) > 3000 else ""))

    # ========== 页脚 ==========
    st.markdown("---")
    st.markdown(
        f'<div style="text-align:center;color:#484f58;font-size:11px;">'
        f'投资组合跟踪分析系统 v1.3 | 数据截至 {selected_date} | '
        f'共 {len(positions)} 只持仓</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()