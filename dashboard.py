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
import calendar

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


@st.cache_data(ttl=600, show_spinner=False)
def load_calendar_data():
    """加载全部日历收益数据（年/月/日汇总）"""
    conn = get_db_connection()
    query = "SELECT date, daily_pnl, daily_return FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return df
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    return df


@st.cache_data(ttl=600, show_spinner=False)
def compute_extended_risk_metrics(end_date=None):
    """计算扩展风险指标（基于全部历史日收益率）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return, daily_pnl FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty or len(df) < 10:
        return {}
    df['date'] = pd.to_datetime(df['date'])
    if end_date:
        df = df[df['date'] <= pd.Timestamp(end_date)]
    if len(df) < 10:
        return {}

    returns = df['daily_return'].dropna()
    pnls = df['daily_pnl']

    # Sortino Ratio (downside deviation)
    neg_returns = returns[returns < 0]
    downside_std = neg_returns.std() * np.sqrt(252) if len(neg_returns) > 1 else np.nan
    annual_return = returns.mean() * 252
    annual_std = returns.std() * np.sqrt(252)
    sortino = annual_return / downside_std if downside_std and downside_std > 0 else np.nan

    # Max Drawdown Duration (最大回撤持续时间)
    max_dd_duration = 0
    current_dd_duration = 0
    if 'total_value' in df.columns:
        cummax = df['total_value'].cummax()
        in_drawdown = df['total_value'] < cummax
        for is_dd in in_drawdown:
            if is_dd:
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)
            else:
                current_dd_duration = 0

    # Calmar Ratio (annual return / max drawdown)
    cummax = df['total_value'].cummax() if 'total_value' in df.columns else None
    if cummax is not None:
        dd = (df['total_value'] - cummax) / cummax * 100
        max_dd_abs = abs(dd.min())
        calmar = annual_return / max_dd_abs if max_dd_abs > 0 else np.nan
    else:
        calmar = np.nan

    # Win rate
    win_days = len(pnls[pnls > 0])
    total_days = len(pnls[pnls != 0])
    win_rate = win_days / total_days * 100 if total_days > 0 else np.nan

    # Profit/Loss ratio
    avg_win = pnls[pnls > 0].mean() if win_days > 0 else 0
    avg_loss = abs(pnls[pnls < 0].mean()) if len(pnls[pnls < 0]) > 0 else 1
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else np.nan

    # Max consecutive win/loss days
    max_consec_win, max_consec_loss = 0, 0
    consec_win, consec_loss = 0, 0
    for p in pnls:
        if p > 0:
            consec_win += 1
            consec_loss = 0
            max_consec_win = max(max_consec_win, consec_win)
        elif p < 0:
            consec_loss += 1
            consec_win = 0
            max_consec_loss = max(max_consec_loss, consec_loss)
        else:
            consec_win, consec_loss = 0, 0

    # Skewness & Kurtosis
    skewness = returns.skew()
    kurtosis = returns.kurtosis()

    return {
        'sortino': sortino,
        'calmar': calmar,
        'win_rate': win_rate,
        'pl_ratio': pl_ratio,
        'max_consec_win': max_consec_win,
        'max_consec_loss': max_consec_loss,
        'max_dd_duration': max_dd_duration,
        'skewness': skewness,
        'kurtosis': kurtosis,
        'annual_return': annual_return,
        'annual_std': annual_std,
    }


@st.cache_data(ttl=600, show_spinner=False)
def compute_monthly_returns():
    """计算月度收益率矩阵（年份 x 月份，含年度合计列和汇总行）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame()
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    monthly = df.groupby(['year', 'month'])['daily_return'].sum().reset_index()
    pivot = monthly.pivot(index='year', columns='month', values='daily_return')
    pivot.columns = [f'{m}月' for m in pivot.columns]
    # 年度合计列（各月收益率简单求和作为年度累计收益率）
    pivot['年累计'] = pivot.sum(axis=1)
    # 汇总行（各年份同月收益率均值，作为月均收益率参考）
    summary_row = pivot.mean(axis=0)
    summary_row.name = '月均'
    pivot = pd.concat([pivot, summary_row.to_frame().T])
    return pivot


@st.cache_data(ttl=600, show_spinner=False)
def compute_rolling_metrics(window=60, end_date=None):
    """计算滚动夏普比率和滚动波动率（支持end_date过滤）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return FROM portfolio_summary ORDER BY date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty or len(df) < window:
        return pd.DataFrame()
    df['date'] = pd.to_datetime(df['date'])
    if end_date:
        df = df[df['date'] <= pd.Timestamp(end_date)]
    if len(df) < window:
        return pd.DataFrame()
    ret = df['daily_return']
    rolling_sharpe = (ret.rolling(window).mean() / ret.rolling(window).std() * np.sqrt(252))
    rolling_vol = ret.rolling(window).std() * np.sqrt(252)
    result = pd.DataFrame({
        'date': df['date'],
        'rolling_sharpe': rolling_sharpe,
        'rolling_vol': rolling_vol
    }).dropna()
    return result


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

        .cal-table { border-collapse: collapse; margin: 0 auto; }
        .cal-table th { padding: 6px 8px; font-size: 12px; color: #8b949e; font-weight: normal; }
        .cal-table td {
            width: 50px; height: 44px; text-align: center; vertical-align: middle;
            border: 1px solid #21262d; border-radius: 4px; cursor: default;
            position: relative; padding: 2px;
        }
        .cal-table td.cal-today { border: 2px solid #58a6ff; }
        .cal-table td.cal-non-trading {
            background: #0d1117; color: #30363d;
        }
        .cal-table td.cal-trading {
            background: #161b22;
        }
        .cal-table td.cal-profit { background: #0d2818; }
        .cal-table td.cal-loss { background: #2d1215; }
        .cal-day { font-size: 12px; color: #c9d1d9; }
        .cal-pnl { font-size: 10px; display: block; line-height: 1.2; }
        .cal-pnl-profit { color: #22c55e; }
        .cal-pnl-loss { color: #ef4444; }
        .cal-pnl-zero { color: #484f58; }
        .yr-pill {
            display: inline-block; padding: 4px 14px; margin: 2px;
            border-radius: 14px; font-size: 13px; cursor: pointer;
            background: #21262d; color: #c9d1d9; border: 1px solid #30363d;
        }
        .yr-pill.active { background: #1f6feb; color: #ffffff; border-color: #1f6feb; }
        .mo-pill {
            display: inline-block; padding: 3px 12px; margin: 2px;
            border-radius: 12px; font-size: 12px; cursor: pointer;
            background: #161b22; color: #8b949e; border: 1px solid #21262d;
        }
        .mo-pill.active { background: #238636; color: #ffffff; border-color: #238636; }
        .cal-summary {
            display: inline-block; padding: 4px 12px; margin: 2px 6px;
            border-radius: 6px; font-size: 12px; background: #161b22;
        }
        .cal-summary-profit { color: #22c55e; }
        .cal-summary-loss { color: #ef4444; }

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
    tab1, tab2, tab3, tab4 = st.tabs(["📈 净值走势", "📊 持仓分布", "⚠️ 风险分析", "📅 收益日历"])

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

        # ---------- 滚动指标图表 ----------
        r1, r2 = st.columns([1, 3])
        with r1:
            rolling_window = st.selectbox(
                "滚动窗口", options=[60, 120, 250],
                format_func=lambda x: f"{x}日", index=0, key="rolling_window"
            )
        rolling_data = compute_rolling_metrics(window=rolling_window, end_date=selected_date)
        if not rolling_data.empty and len(rolling_data) > 5:
            st.markdown(f'<div class="section-title">滚动风险指标（{rolling_window}日窗口）</div>', unsafe_allow_html=True)
            rolling_chart = downsample(rolling_data, max_points=500)

            fig_roll = make_subplots(
                rows=2, cols=1, shared_xaxes=True,
                vertical_spacing=0.08,
                subplot_titles=('滚动夏普比率', '滚动年化波动率')
            )
            fig_roll.add_trace(go.Scatter(
                x=rolling_chart['date'], y=rolling_chart['rolling_sharpe'],
                mode='lines', name='滚动夏普',
                line=dict(color='#58a6ff', width=1.5),
                fill='tozeroy', fillcolor='rgba(88,166,255,0.08)'
            ), row=1, col=1)
            fig_roll.add_hline(y=0, line_dash='dash', line_color='#484f58', row=1, col=1)
            fig_roll.add_hline(y=1, line_dash='dot', line_color='#22c55e',
                               annotation_text='优秀线(1.0)', row=1, col=1)

            fig_roll.add_trace(go.Scatter(
                x=rolling_chart['date'], y=rolling_chart['rolling_vol'],
                mode='lines', name='滚动波动率',
                line=dict(color='#f59e0b', width=1.5),
                fill='tozeroy', fillcolor='rgba(245,158,11,0.08)'
            ), row=2, col=1)

            fig_roll.update_layout(
                height=350,
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=50, r=20, t=35, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                            font=dict(size=10)),
                showlegend=False,
            )
            fig_roll.update_xaxes(showgrid=False, row=1, col=1)
            fig_roll.update_xaxes(showgrid=False, row=2, col=1)
            fig_roll.update_yaxes(title_text='夏普比率', showgrid=True,
                                 gridcolor='#21262d', row=1, col=1)
            fig_roll.update_yaxes(title_text='波动率 (%)', showgrid=True,
                                 gridcolor='#21262d', row=2, col=1)
            st.plotly_chart(fig_roll, width='stretch')

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

            # 计算扩展风险指标
            ext_risk = compute_extended_risk_metrics(end_date=selected_date)

            risk_metrics = [
                ("夏普比率", sharpe, "衡量风险调整后收益，>1为优秀"),
                ("Sortino比率", ext_risk.get('sortino', np.nan),
                 "仅考虑下行波动的风险调整收益"),
                ("Calmar比率", ext_risk.get('calmar', np.nan),
                 "年化收益 / 最大回撤，越高越好"),
                ("最大回撤", max_dd, "历史最大亏损幅度"),
                ("年化波动率", volatility, "收益率的标准差，越高越不稳定"),
                ("胜率", ext_risk.get('win_rate', np.nan), "盈利天数 / 有盈亏交易天数"),
                ("盈亏比", ext_risk.get('pl_ratio', np.nan),
                 "平均盈利 / 平均亏损，>1为优"),
                ("最大连续盈利", ext_risk.get('max_consec_win', 0), "历史最长连续盈利天数"),
                ("最大连续亏损", ext_risk.get('max_consec_loss', 0), "历史最长连续亏损天数"),
                ("最大回撤持续", ext_risk.get('max_dd_duration', 0), "历史最长回撤恢复天数（净值低于峰值）"),
                ("偏度", ext_risk.get('skewness', np.nan), "收益率分布偏斜，正值为右偏"),
                ("峰度", ext_risk.get('kurtosis', np.nan), "收益率分布尾部厚度，>0为尖峰"),
                ("持仓盈亏比", f"{profit_count}:{loss_count}" if profit_count or loss_count else "N/A",
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

    # ========== 收益日历 ==========
    with tab4:
        cal_data = load_calendar_data()

        if cal_data.empty:
            st.info("暂无日历数据")
        else:
            years = sorted(cal_data['year'].unique(), reverse=True)
            latest_year = years[0]
            today_str = datetime.now().strftime('%Y-%m-%d')

            # --- 年份选择 ---
            yr_cols = st.columns([6, 1])
            with yr_cols[0]:
                st.markdown("**选择年份**")
                yr_html = '<div style="margin-bottom:8px;">'
                for yr in years:
                    is_active = (st.session_state.get('cal_year', latest_year) == yr)
                    cls = 'active' if is_active else ''
                    yr_html += f'<span class="yr-pill {cls}">{yr}</span>'
                yr_html += '</div>'
                st.markdown(yr_html, unsafe_allow_html=True)

                # 年份按钮用 columns 实现
                yr_sel = st.columns(len(years))
                for i, yr in enumerate(years):
                    with yr_sel[i]:
                        label = f"**{yr}**" if st.session_state.get('cal_year', latest_year) == yr else str(yr)
                        if st.button(label, key=f"yr_{yr}", use_container_width=True):
                            st.session_state['cal_year'] = yr
                            st.rerun()

            st.markdown("---")

            sel_year = st.session_state.get('cal_year', latest_year)
            year_df = cal_data[cal_data['year'] == sel_year]

            # --- 月份选择 ---
            months_in_year = sorted(year_df['month'].unique())
            month_names = [f"{m}月" for m in months_in_year]

            st.markdown("**选择月份**")
            mo_sel = st.columns(len(months_in_year))
            for i, m in enumerate(months_in_year):
                with mo_sel[i]:
                    is_active = (st.session_state.get('cal_month', months_in_year[-1]) == m)
                    label = f"**{m}月**" if is_active else f"{m}月"
                    if st.button(label, key=f"mo_{sel_year}_{m}", use_container_width=True):
                        st.session_state['cal_month'] = m
                        st.rerun()

            sel_month = st.session_state.get('cal_month', months_in_year[-1])
            month_df = year_df[year_df['month'] == sel_month]

            # --- 月度汇总 ---
            m_pnl = month_df['daily_pnl'].sum()
            m_return = month_df['daily_return'].sum()
            m_trading = len(month_df)
            m_profit = len(month_df[month_df['daily_pnl'] > 0])
            m_loss = len(month_df[month_df['daily_pnl'] < 0])

            st.markdown("---")
            sum_col1, sum_col2, sum_col3, sum_col4, sum_col5 = st.columns(5)
            with sum_col1:
                st.metric("月收益", f"¥{m_pnl:,.0f}")
            with sum_col2:
                st.metric("月收益率", f"{m_return:.2f}%")
            with sum_col3:
                st.metric("交易日", f"{m_trading}天")
            with sum_col4:
                st.metric("盈利天数", f"{m_profit}天")
            with sum_col5:
                st.metric("亏损天数", f"{m_loss}天")

            st.markdown("---")

            # --- 年度月汇总条 ---
            st.markdown("**年度月度概览**")
            yr_monthly = year_df.groupby('month').agg(
                pnl_sum=('daily_pnl', 'sum'),
                ret_sum=('daily_return', 'sum'),
                days=('day', 'count')
            ).reset_index()

            sm_html = '<div style="margin-bottom:12px;">'
            for _, row in yr_monthly.iterrows():
                m = int(row['month'])
                pnl = row['pnl_sum']
                is_active = (m == sel_month)
                bold = 'font-weight:bold;' if is_active else ''
                border = 'border:2px solid #58a6ff;' if is_active else 'border:1px solid #21262d;'
                if pnl >= 0:
                    color = '#22c55e'
                    bg = '#0d2818'
                else:
                    color = '#ef4444'
                    bg = '#2d1215'
                sm_html += (
                    f'<span class="cal-summary" style="{bold}{border}background:{bg};color:{color};">'
                    f'{m}月 &nbsp; ¥{pnl:,.0f}</span>'
                )
            yr_total_pnl = year_df['daily_pnl'].sum()
            yr_total_ret = year_df['daily_return'].sum()
            yr_color = '#22c55e' if yr_total_pnl >= 0 else '#ef4444'
            sm_html += (
                f'<span class="cal-summary" style="border:1px solid #30363d;font-weight:bold;'
                f'color:{yr_color};">全年 &nbsp; ¥{yr_total_pnl:,.0f} ({yr_total_ret:.2f}%)</span>'
            )
            sm_html += '</div>'
            st.markdown(sm_html, unsafe_allow_html=True)

            # --- 月度日历 ---
            st.markdown(f"**{sel_year}年{sel_month}月 日历**")

            # 获取交易日数据字典
            trading_days = {}
            for _, row in month_df.iterrows():
                d = int(row['day'])
                pnl = row['daily_pnl']
                ret = row['daily_return']
                dt_str = row['date'].strftime('%Y-%m-%d')
                trading_days[d] = {'pnl': pnl, 'ret': ret, 'date_str': dt_str}

            # 构建日历HTML
            cal = calendar.Calendar(firstweekday=0)  # 周一开始
            month_days = list(cal.itermonthdays(sel_year, sel_month))

            # 周标题
            week_headers = ['一', '二', '三', '四', '五', '六', '日']

            cal_html = '<table class="cal-table"><tr>'
            for h in week_headers:
                cal_html += f'<th>{h}</th>'
            cal_html += '</tr><tr>'

            for i, day in enumerate(month_days):
                if day == 0:
                    cal_html += '<td class="cal-non-trading"></td>'
                elif day in trading_days:
                    info = trading_days[day]
                    pnl = info['pnl']
                    ret = info['ret']
                    dt_str = info['date_str']

                    if pnl > 0:
                        td_cls = 'cal-trading cal-profit'
                        pnl_cls = 'cal-pnl cal-pnl-profit'
                    elif pnl < 0:
                        td_cls = 'cal-trading cal-loss'
                        pnl_cls = 'cal-pnl cal-pnl-loss'
                    else:
                        td_cls = 'cal-trading'
                        pnl_cls = 'cal-pnl cal-pnl-zero'

                    today_cls = ' cal-today' if dt_str == today_str else ''

                    # 格式化收益金额
                    if abs(pnl) >= 10000:
                        pnl_text = f"{pnl/10000:.1f}万"
                    elif abs(pnl) >= 1000:
                        pnl_text = f"{pnl/1000:.1f}k"
                    else:
                        pnl_text = f"{pnl:.0f}"

                    cal_html += (
                        f'<td class="{td_cls}{today_cls}" title="{dt_str}  收益: ¥{pnl:,.0f}  ({ret:+.2f}%)">'
                        f'<span class="cal-day">{day}</span>'
                        f'<span class="{pnl_cls}">{pnl_text}</span>'
                        f'</td>'
                    )
                else:
                    # 非交易日
                    cal_html += f'<td class="cal-non-trading"><span class="cal-day">{day}</span></td>'

                if (i + 1) % 7 == 0:
                    cal_html += '</tr><tr>'

            # 清理最后可能的多余tr
            cal_html = cal_html.rstrip('<tr>')
            cal_html += '</table>'

            st.markdown(cal_html, unsafe_allow_html=True)

            # --- 每日收益明细表 ---
            with st.expander("查看每日收益明细", expanded=False):
                detail_df = month_df[['date', 'daily_pnl', 'daily_return']].copy()
                detail_df.columns = ['日期', '日收益 (¥)', '日收益率 (%)']
                detail_df['日期'] = detail_df['日期'].dt.strftime('%Y-%m-%d')
                detail_df['日收益 (¥)'] = detail_df['日收益 (¥)'].apply(
                    lambda x: f'<span style="color:{"#22c55e" if x >= 0 else "#ef4444"}">{x:,.2f}</span>'
                )
                detail_df['日收益率 (%)'] = detail_df['日收益率 (%)'].apply(
                    lambda x: f'<span style="color:{"#22c55e" if x >= 0 else "#ef4444"}">{x:+.2f}%</span>'
                )
                st.markdown(detail_df.to_html(index=False, escape=False), unsafe_allow_html=True)

            # --- 月度收益热力图 ---
            st.markdown("---")
            st.markdown("**月度收益热力图**")
            monthly_pivot = compute_monthly_returns()
            if not monthly_pivot.empty:
                fig_heat = go.Figure(go.Heatmap(
                    z=monthly_pivot.values,
                    x=monthly_pivot.columns.tolist(),
                    y=monthly_pivot.index.astype(str).tolist(),
                    text=monthly_pivot.values,
                    texttemplate='%{text:.2f}%' if monthly_pivot.abs().max().max() < 10 else '%{text:.1f}%',
                    textfont=dict(size=10),
                    colorscale=[[0, '#ef4444'], [0.5, '#0d1117'], [1, '#22c55e']],
                    zmin=-monthly_pivot.abs().max().max(),
                    zmax=monthly_pivot.abs().max().max(),
                    xgap=2, ygap=2,
                    hovertemplate='%{y}年%{x}<br>收益率: %{z:.2f}%<extra></extra>'
                ))
                fig_heat.update_layout(
                    height=max(250, 40 * len(monthly_pivot)),
                    plot_bgcolor='#0d1117',
                    paper_bgcolor='#0d1117',
                    font=dict(color='#c9d1d9', size=11),
                    margin=dict(l=50, r=20, t=10, b=40),
                    xaxis=dict(title='', showgrid=False, side='top'),
                    yaxis=dict(title='', showgrid=False, autorange='reversed'),
                )
                st.plotly_chart(fig_heat, width='stretch')

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