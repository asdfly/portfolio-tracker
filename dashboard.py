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
from datetime import datetime
import calendar
import base64

from config.settings import DATABASE_PATH, INDEX_CODES, ETF_CATEGORIES, SECTOR_COLORS

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

# ==================== 图表辅助函数 ====================
def _add_min_max_annotations(fig, x_data, y_data, row=None, col=None,
                              y_label=None, date_format='%m-%d'):
    """在时间轴图表中标记最大值和最小值的位置及数值。
    
    Args:
        fig: plotly 图表对象 (go.Figure 或 make_subplots 子图)
        x_data: x 轴数据序列 (日期)
        y_data: y 轴数据序列 (数值)
        row, col: 子图位置 (make_subplots 时使用)，默认 None 表示单图
        y_label: y 轴标签，用于标注文字前缀
        date_format: 日期格式化字符串
    """
    import numpy as np
    
    x_arr = np.array(x_data)
    y_arr = np.array(y_data, dtype=float)
    
    # 过滤 NaN
    valid = ~np.isnan(y_arr)
    x_arr, y_arr = x_arr[valid], y_arr[valid]
    
    if len(x_arr) < 2:
        return
    
    max_idx = np.argmax(y_arr)
    min_idx = np.argmin(y_arr)
    
    max_x, max_y = x_arr[max_idx], y_arr[max_idx]
    min_x, min_y = x_arr[min_idx], y_arr[min_idx]
    
    # 格式化日期
    if hasattr(max_x, 'strftime'):
        max_date_str = max_x.strftime(date_format)
        min_date_str = min_x.strftime(date_format)
    else:
        max_date_str = str(max_x)
        min_date_str = str(min_x)
    
    # 格式化数值
    def fmt_val(v):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        elif abs(v) >= 1:
            return f"{v:.2f}"
        else:
            return f"{v:.4f}"
    
    max_text = f"Max {fmt_val(max_y)}"
    min_text = f"Min {fmt_val(min_y)}"
    
    # 添加散点标记
    scatter_kwargs = dict(
        mode='markers+text',
        hoverinfo='skip',
        showlegend=False,
    )
    
    if row is not None and col is not None:
        # make_subplots 子图
        fig.add_trace(go.Scatter(
            x=[max_x], y=[max_y],
            marker=dict(color='#22c55e', size=8, symbol='triangle-down'),
            text=[max_text], textposition='top center',
            textfont=dict(size=9, color='#22c55e'),
            **scatter_kwargs
        ), row=row, col=col)
        fig.add_trace(go.Scatter(
            x=[min_x], y=[min_y],
            marker=dict(color='#ef4444', size=8, symbol='triangle-up'),
            text=[min_text], textposition='bottom center',
            textfont=dict(size=9, color='#ef4444'),
            **scatter_kwargs
        ), row=row, col=col)
    else:
        # 单图
        fig.add_trace(go.Scatter(
            x=[max_x], y=[max_y],
            marker=dict(color='#22c55e', size=8, symbol='triangle-down'),
            text=[max_text], textposition='top center',
            textfont=dict(size=9, color='#22c55e'),
            **scatter_kwargs
        ))
        fig.add_trace(go.Scatter(
            x=[min_x], y=[min_y],
            marker=dict(color='#ef4444', size=8, symbol='triangle-up'),
            text=[min_text], textposition='bottom center',
            textfont=dict(size=9, color='#ef4444'),
            **scatter_kwargs
        ))


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

def _cleanse_daily_returns(df, return_col='daily_return', threshold=5.0, max_tail=500):
    """清洗日收益率数据：过滤异常值 + 截断早期高波动区间

    Args:
        df: 包含 daily_return 列的 DataFrame
        return_col: 收益率列名
        threshold: 异常值阈值（%），默认5%（ETF单日正常波动上限）
        max_tail: 最大采样条数，默认500（约2个交易年），避免早期高波动区间污染

    Returns:
        (cleaned_df, stats) 元组
        stats = {'original': n, 'after_filter': n, 'after_tail': n, 'filtered': n, 'tailed': n}
    """
    original_count = len(df)

    # 步骤1: 过滤 |return| > threshold 的异常值
    mask = df[return_col].abs() <= threshold
    filtered_df = df[mask].copy()
    filtered_count = original_count - len(filtered_df)

    # 步骤2: 截断到最近 max_tail 条，排除早期高波动区间
    if len(filtered_df) > max_tail:
        tailed_df = filtered_df.tail(max_tail).copy()
        tailed_count = len(filtered_df) - len(tailed_df)
    else:
        tailed_df = filtered_df
        tailed_count = 0

    stats = {
        'original': original_count,
        'after_filter': len(filtered_df),
        'after_tail': len(tailed_df),
        'filtered': filtered_count,
        'tailed': tailed_count,
    }

    if filtered_count > 0 or tailed_count > 0:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(
            f"日收益率清洗: {original_count}条 -> 过滤|ret|>{threshold}%: {filtered_count}条, "
            f"截断早期: {tailed_count}条, 剩余{len(tailed_df)}条"
        )

    return tailed_df, stats


def compute_extended_risk_metrics(end_date=None):
    """计算扩展风险指标（基于全部历史日收益率）"""
    conn = get_db_connection()
    query = "SELECT date, daily_return, daily_pnl, total_value FROM portfolio_summary ORDER BY date"
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



# ==================== P1: 持仓相关性矩阵 ====================
@st.cache_data(ttl=600, show_spinner=False)
def load_correlation_matrix(days=250, end_date=None):
    """计算持仓ETF之间的皮尔逊相关系数矩阵（基于各ETF市值变动）"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, code, market_value 
            FROM portfolio_snapshots 
            WHERE date <= ? 
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn, params=(end_date,))
    else:
        query = """
            SELECT date, code, market_value 
            FROM portfolio_snapshots 
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame(), []

    # 取最近N个交易日
    dates = df['date'].unique()[:days]
    df = df[df['date'].isin(dates)]

    # 构建透视表：行=日期, 列=code, 值=market_value
    pivot = df.pivot_table(index='date', columns='code', values='market_value', aggfunc='first')

    # 只保留有足够数据的ETF（至少80%的交易日有数据）
    min_count = int(len(pivot) * 0.8)
    valid_cols = pivot.columns[pivot.notna().sum() >= min_count]
    pivot = pivot[valid_cols]

    if pivot.shape[1] < 2:
        return pd.DataFrame(), []

    # 计算日收益率
    returns = pivot.pct_change().dropna()

    # 计算相关系数矩阵
    corr = returns.corr()

    # 获取ETF名称
    conn = get_db_connection()
    names = {}
    for code in corr.columns:
        row = conn.execute(
            "SELECT name FROM portfolio_snapshots WHERE code = ? ORDER BY date DESC LIMIT 1",
            (code,)
        ).fetchone()
        names[code] = row[0] if row else code
    conn.close()

    # 简化名称（取前4个字 + "..."）
    short_names = {}
    for code, name in names.items():
        if len(name) > 6:
            short_names[code] = name[:6] + ".."
        else:
            short_names[code] = name

    return corr, short_names


# ==================== P1: 单只ETF详情数据 ====================
@st.cache_data(ttl=300, show_spinner=False)
def load_etf_detail(code, days=120, end_date=None):
    """加载单只ETF的快照数据和技术指标（含成本价、持仓量）"""
    conn = get_db_connection()

    # 从snapshots获取市值变化
    if end_date:
        query_snap = """
            SELECT date, current_price, market_value, quantity, cost_price,
                   pnl, pnl_rate, ytd_return, beta
            FROM portfolio_snapshots
            WHERE code = ? AND date <= ?
            ORDER BY date DESC LIMIT ?
        """
        df_snap = pd.read_sql_query(query_snap, conn, params=(code, end_date, days))
    else:
        query_snap = """
            SELECT date, current_price, market_value, quantity, cost_price,
                   pnl, pnl_rate, ytd_return, beta
            FROM portfolio_snapshots
            WHERE code = ?
            ORDER BY date DESC LIMIT ?
        """
        df_snap = pd.read_sql_query(query_snap, conn, params=(code, days))

    df_snap = df_snap.sort_values('date').reset_index(drop=True)

    # 从etf_technical获取技术指标
    if end_date:
        query_tech = """
            SELECT date, rsi_value, rsi_status, ma_signal, macd_signal, trend,
                   kdj_signal, bollinger_position, atr_pct
            FROM etf_technical
            WHERE code = ? AND date <= ?
            ORDER BY date DESC LIMIT ?
        """
        df_tech = pd.read_sql_query(query_tech, conn, params=(code, end_date, days))
    else:
        query_tech = """
            SELECT date, rsi_value, rsi_status, ma_signal, macd_signal, trend,
                   kdj_signal, bollinger_position, atr_pct
            FROM etf_technical
            WHERE code = ?
            ORDER BY date DESC LIMIT ?
        """
        df_tech = pd.read_sql_query(query_tech, conn, params=(code, days))

    df_tech = df_tech.sort_values('date').reset_index(drop=True)

    # 获取ETF名称
    name_row = conn.execute(
        "SELECT name FROM portfolio_snapshots WHERE code = ? ORDER BY date DESC LIMIT 1",
        (code,)
    ).fetchone()
    etf_name = name_row[0] if name_row else code

    conn.close()

    # 合并数据
    if not df_snap.empty and not df_tech.empty:
        df = pd.merge(df_snap, df_tech, on='date', how='outer')
        df = df.sort_values('date').reset_index(drop=True)
    elif not df_snap.empty:
        df = df_snap
    else:
        df = pd.DataFrame()

    return df, etf_name



def _render_etf_detail_panel(row, selected_date, total_value=0):
    """渲染ETF增强版详情面板：核心指标 + 价格走势 + 技术分析"""
    code = row['code']
    name = row['name']

    # 加载详细数据（命中缓存时零延迟）
    detail_df, etf_name = load_etf_detail(code, days=120, end_date=selected_date)
    price_df = load_etf_price_history(code, days=250, end_date=selected_date)

    # ===== 第一行：核心指标卡片（6列） =====
    mv = row.get('market_value', 0)
    pnl = row.get('pnl', 0)
    pnl_rate = row.get('pnl_rate', 0)
    cost = row.get('cost_price', 0)
    current = row.get('current_price', 0)
    qty = row.get('quantity', 0)

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    with c1:
        st.metric("市值", f"¥{mv:,.0f}")
    with c2:
        st.metric("累计盈亏", f"¥{pnl:,.0f}",
                  delta=f"{pnl_rate:+.2f}%")
    with c3:
        if pd.notna(row.get('ytd_return')):
            yt = row['ytd_return']
            st.metric("年内收益", f"{yt:+.2f}%")
        else:
            st.metric("年内收益", "--")
    with c4:
        if pd.notna(row.get('beta')):
            st.metric("Beta", f"{row['beta']:.2f}")
        else:
            st.metric("Beta", "--")
    with c5:
        cost_val = f"{cost:.3f}" if pd.notna(cost) else "--"
        st.metric("成本价", cost_val)
    with c6:
        price_diff = current - cost if pd.notna(cost) and pd.notna(current) else None
        delta_str = f"{price_diff:+.3f}" if price_diff is not None else None
        st.metric("现价", f"{current:.3f}" if pd.notna(current) else "--",
                  delta=delta_str)

    # ===== 第二行：价格走势图 + 技术指标详情 =====
    if not price_df.empty:
        col_chart, col_tech = st.columns([3, 1])

        with col_chart:
            st.markdown('<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">价格走势（近250日）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF近250个交易日的收盘价走势，叠加MA5/MA10/MA20均线，并标注买入成本参考线。</span></div>', unsafe_allow_html=True)
            df = price_df.sort_values('date').copy()

            # 降采样
            if len(df) > 500:
                step = max(1, len(df) // 500)
                df_plot = df.iloc[::step].copy()
            else:
                df_plot = df.copy()

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_plot['date'], y=df_plot['close'],
                mode='lines', name='收盘价',
                line=dict(color='#58a6ff', width=1.5),
                fill='tozeroy',
                fillcolor='rgba(88,166,255,0.05)',
                hovertemplate='%{x|%m-%d}<br>价格: %{y:.3f}<extra></extra>'
            ))

            # 添加成本线
            if pd.notna(cost) and cost > 0:
                fig.add_hline(
                    y=cost, line_dash="dash", line_color="#f59e0b",
                    annotation_text=f"成本 {cost:.3f}",
                    annotation_position="top left",
                    annotation_font=dict(size=10, color="#f59e0b")
                )


            # 标记最高价和最低价
            _add_min_max_annotations(fig, df_plot['date'], df_plot['close'], y_label="价格")

            fig.update_layout(
                height=220,
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=40, r=15, t=10, b=30),
                xaxis=dict(showgrid=False, tickformat='%m-%d', dtick="M1"),
                yaxis=dict(showgrid=True, gridcolor='#21262d', tickformat='.3f'),
                hovermode='x unified',
            )
            st.plotly_chart(fig, width='stretch')

        with col_tech:
            st.markdown('<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">技术指标<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF的RSI、MACD、KDJ、布林带等常用技术指标信号。</span></div>', unsafe_allow_html=True)
            if not detail_df.empty:
                latest = detail_df.iloc[-1]

                trend_map = {
                    'bullish': ('看多', '#22c55e'),
                    'bearish': ('看空', '#ef4444'),
                    'neutral': ('中性', '#f59e0b'),
                    None: ('--', '#888')
                }
                trend_label, trend_color = trend_map.get(latest.get('trend'), ('--', '#888'))

                # 技术指标卡片
                indicators = [
                    ("趋势", trend_label, trend_color),
                    ("RSI", f"{latest.get('rsi_value', '--'):.1f}" if pd.notna(latest.get('rsi_value')) else "--",
                     '#22c55e' if latest.get('rsi_status') in ('oversold',) else
                     '#ef4444' if latest.get('rsi_status') in ('overbought',) else '#c9d1d9'),
                    ("MA信号", str(latest.get('ma_signal', '--')), '#c9d1d9'),
                    ("MACD", str(latest.get('macd_signal', '--')), '#c9d1d9'),
                    ("KDJ", str(latest.get('kdj_signal', '--')), '#c9d1d9'),
                    ("布林位置", str(latest.get('bollinger_position', '--')), '#c9d1d9'),
                    ("ATR%", f"{latest.get('atr_pct', '--'):.1f}%" if pd.notna(latest.get('atr_pct')) else "--", '#c9d1d9'),
                ]

                for label, value, color in indicators:
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:4px 8px;'
                        f'border-bottom:1px solid #21262d;font-size:12px;">'
                        f'<span style="color:#8b949e;">{label}</span>'
                        f'<span style="color:{color};font-weight:bold;">{value}</span>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

                # RSI 仪表条
                rsi_val = latest.get('rsi_value', None)
                if pd.notna(rsi_val):
                    rsi_clamped = max(0, min(100, float(rsi_val)))
                    bar_color = '#ef4444' if rsi_clamped > 70 else '#22c55e' if rsi_clamped < 30 else '#f59e0b'
                    st.markdown(
                        f'<div style="margin-top:8px;font-size:11px;color:#8b949e;">RSI 位置</div>'
                        f'<div style="background:#21262d;border-radius:4px;height:8px;position:relative;">'
                        f'<div style="background:{bar_color};border-radius:4px;height:8px;width:{rsi_clamped}%;"></div>'
                        f'<div style="position:absolute;top:-2px;left:70%;width:1px;height:12px;background:#ef4444;opacity:0.5;"></div>'
                        f'<div style="position:absolute;top:-2px;left:30%;width:1px;height:12px;background:#22c55e;opacity:0.5;"></div>'
                        f'</div>'
                        f'<div style="display:flex;justify-content:space-between;font-size:9px;color:#484f58;">'
                        f'<span>超卖 30</span><span>中性</span><span>超买 70</span></div>',
                        unsafe_allow_html=True
                    )
            else:
                st.info("暂无技术指标数据")

    # ===== 第三行：收益率分布 + 关键统计 =====
    if not detail_df.empty:
        col_stats, col_dist = st.columns([1, 2])

        with col_stats:
            st.markdown('<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">关键统计<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF的日均收益、标准差、最大涨跌幅等关键统计指标。</span></div>', unsafe_allow_html=True)
            df_detail = detail_df.sort_values('date')
            daily_returns = df_detail['current_price'].pct_change().dropna() if len(df_detail) > 1 else pd.Series()

            stats_items = []
            if len(daily_returns) > 0:
                stats_items.append(("日均收益", f"{daily_returns.mean()*100:+.3f}%"))
                stats_items.append(("日收益标准差", f"{daily_returns.std()*100:.3f}%"))
                stats_items.append(("最大单日涨幅", f"{daily_returns.max()*100:+.2f}%"))
                stats_items.append(("最大单日跌幅", f"{daily_returns.min()*100:+.2f}%"))
            stats_items.append(("数据天数", f"{len(df_detail)} 天"))
            stats_items.append(("持仓市值占比", f"{mv/total_value*100:.1f}%" if total_value > 0 else "--"))

            for label, value in stats_items:
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;padding:4px 8px;'
                    f'border-bottom:1px solid #21262d;font-size:12px;">'
                    f'<span style="color:#8b949e;">{label}</span>'
                    f'<span style="color:#c9d1d9;font-weight:bold;">{value}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )

        with col_dist:
            if len(daily_returns) > 5:
                st.markdown('<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">日收益率分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">统计该ETF日收益率的频率分布，可判断收益的正态性和肥尾特征。</span></div>', unsafe_allow_html=True)
                fig_hist = go.Figure()
                colors = ['#22c55e' if v >= 0 else '#ef4444' for v in daily_returns]
                fig_hist.add_trace(go.Histogram(
                    x=daily_returns * 100,
                    marker_color='#58a6ff',
                    nbinsx=30,
                    opacity=0.7,
                    hovertemplate='区间: %{x:.2f}%<br>次数: %{y}<extra></extra>'
                ))
                # 标记零线
                fig_hist.add_vline(x=0, line_dash="dash", line_color="#f59e0b", line_width=1)
                fig_hist.update_layout(
                    height=180,
                    plot_bgcolor='#0d1117',
                    paper_bgcolor='#0d1117',
                    font=dict(color='#c9d1d9', size=11),
                    margin=dict(l=40, r=15, t=10, b=30),
                    xaxis=dict(title='日收益率 %', showgrid=False),
                    yaxis=dict(title='频次', showgrid=True, gridcolor='#21262d'),
                    bargap=0.05,
                )
                st.plotly_chart(fig_hist, width='stretch')

@st.cache_data(ttl=300, show_spinner=False)
def load_etf_price_history(code, days=250, end_date=None):
    """加载单只ETF的价格历史，用于绘制K线/走势图"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, current_price as close, market_value, quantity
            FROM portfolio_snapshots
            WHERE code = ? AND date <= ?
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, end_date, days))
    else:
        query = """
            SELECT date, current_price as close, market_value, quantity
            FROM portfolio_snapshots
            WHERE code = ?
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, days))
    df = df.sort_values('date').reset_index(drop=True)
    conn.close()

    # 计算简单统计
    if not df.empty:
        df['returns'] = df['close'].pct_change()
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['ma60'] = df['close'].rolling(60).mean()

    return df



# ==================== P1: 多基准指数对比数据 ====================
@st.cache_data(ttl=600, show_spinner=False)
def load_benchmark_comparison(code, days=250, end_date=None):
    """加载指定基准指数行情，用于净值曲线对比"""
    conn = get_db_connection()
    if end_date:
        query = """
            SELECT date, close 
            FROM index_quotes 
            WHERE code = ? AND date <= ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, end_date, days))
    else:
        query = """
            SELECT date, close 
            FROM index_quotes 
            WHERE code = ? 
            ORDER BY date DESC LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(code, days))
    df = df.sort_values('date').reset_index(drop=True)
    conn.close()
    return df


# ==================== 样式工具 ====================

@st.cache_data(ttl=300, show_spinner=False)
def load_sector_weights(days=250, end_date=None):
    """加载按行业聚合的持仓权重历史（堆叠面积图数据源）"""
    query = """
        SELECT ps.date, ps.code, ps.market_value, ps.quantity, ps.current_price
        FROM portfolio_snapshots ps
        WHERE ps.date >= (
            SELECT DISTINCT date FROM portfolio_snapshots
            ORDER BY date DESC
            LIMIT 1 OFFSET ?
        )
    """
    if end_date:
        query += " AND ps.date <= ?"
    query += " ORDER BY ps.date, ps.code"

    try:
        conn = sqlite3.connect(str(DATABASE_PATH))
        params = [days]
        if end_date:
            params.append(end_date)
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"load_sector_weights 查询失败: {e}")
        return pd.DataFrame(), {}

    if df.empty:
        return pd.DataFrame(), {}

    # 按行业分类
    df["sector"] = df["code"].map(lambda c: ETF_CATEGORIES.get(c, {}).get("sector", "其他"))

    # 每日各行业总市值
    pivot = df.pivot_table(index="date", columns="sector", values="market_value",
                           aggfunc="sum", fill_value=0)
    # 计算每日权重百分比
    daily_total = pivot.sum(axis=1)
    weight_df = pivot.div(daily_total, axis=0) * 100

    # 确定显示顺序（按最新日期的权重降序）
    if not weight_df.empty:
        latest = weight_df.iloc[-1].sort_values(ascending=False)
        weight_df = weight_df[latest.index]

    # 扇区颜色映射
    sector_color_map = {}
    for sector in weight_df.columns:
        sector_color_map[sector] = SECTOR_COLORS.get(sector, "#6b7280")

    return weight_df, sector_color_map




@st.cache_data(ttl=600, show_spinner=False)
def run_monte_carlo(days=252, n_simulations=500, end_date=None):
    """蒙特卡洛模拟：基于历史日收益率分布，生成未来N日组合净值路径

    数据清洗：
    1. 移除 |daily_return| > 15% 的异常值（历史脏数据/数据迁移错误）
    2. 默认仅使用近2年数据采样，避免早期高波动数据污染预测
    3. 近期数据指数加权，更贴近当前市场状态

    Args:
        days: 模拟未来交易日天数
        n_simulations: 模拟路径数量
        end_date: 截止日期

    Returns:
        dict: {
            'paths': np.ndarray (n_simulations, days+1),
            'percentiles': DataFrame (date, p5, p25, p50, p75, p95),
            'last_value': float,
            'mean_return': float,
            'daily_std': float,
            'sample_count': int,      # 采样池大小
            'filtered_count': int,    # 过滤掉的异常值数
            'sample_start': str,      # 采样起始日期
        }
    """
    conn = get_db_connection()
    query = "SELECT date, daily_return FROM portfolio_summary ORDER BY date"
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty or len(df) < 30:
        return None

    if end_date:
        df = df[df['date'] <= end_date]

    # 获取最新市值
    conn2 = get_db_connection()
    query2 = "SELECT total_value FROM portfolio_summary WHERE date <= ? ORDER BY date DESC LIMIT 1"
    last_row = pd.read_sql(query2, conn2, params=(str(df['date'].max()),))
    conn2.close()

    if last_row.empty:
        return None

    last_value = float(last_row['total_value'].iloc[0])
    returns = df['daily_return'].dropna()

    # ===== 数据清洗（统一使用 _cleanse_daily_returns）=====
    df_clean, clean_stats = _cleanse_daily_returns(
        df[['date', 'daily_return']], return_col='daily_return', threshold=5.0, max_tail=500
    )
    returns = df_clean['daily_return']
    filtered_count = clean_stats['filtered']

    sample_start = str(df_clean['date'].iloc[0])

    mean_ret = float(returns.mean())
    std_ret = float(returns.std())

    if std_ret <= 0:
        std_ret = 1e-8

    # ===== Bootstrap 采样（指数加权，近期数据权重更高） =====
    np.random.seed(42)
    hist_returns = returns.values

    # 指数加权：最近的数据权重最大，半年前的权重约为0.5
    n_hist = len(hist_returns)
    half_life = 126  # 半衰期约6个月(126个交易日)
    weights = np.array([2 ** (-i / half_life) for i in range(n_hist)])
    weights = weights[::-1]  # 最近的在末尾，权重最大
    weights /= weights.sum()  # 归一化

    paths = np.zeros((n_simulations, days + 1))
    paths[:, 0] = last_value

    for t in range(1, days + 1):
        # 加权 Bootstrap 采样
        indices = np.random.choice(n_hist, size=n_simulations, replace=True, p=weights)
        samples = hist_returns[indices]
        paths[:, t] = paths[:, t - 1] * (1 + samples / 100)

    # 计算百分位
    percentiles_data = {'day': list(range(days + 1))}
    for p in [5, 25, 50, 75, 95]:
        percentiles_data[f'p{p}'] = np.percentile(paths, p, axis=0)
    percentiles_df = pd.DataFrame(percentiles_data)

    return {
        'paths': paths,
        'percentiles': percentiles_df,
        'last_value': last_value,
        'mean_return': mean_ret,
        'daily_std': std_ret,
        'sample_count': len(returns),
        'filtered_count': filtered_count,
        'sample_start': sample_start,
    }


@st.cache_data(ttl=600, show_spinner=False)
def compute_return_attribution(days=250, end_date=None):
    """Brinson 收益归因：将组合收益分解为行业配置效应和选股效应

    使用基准指数（沪深300）的行业权重作为参考基准。

    Returns:
        dict: {
            'total_return': float,       # 组合总收益率
            'benchmark_return': float,   # 基准总收益率
            'allocation_effect': dict,   # 行业配置效应 {sector: value}
            'selection_effect': dict,    # 选股效应 {sector: value}
            'sector_returns': dict,      # 各行业实际收益率
            'sector_weights': dict,      # 组合各行业权重
            'bench_weights': dict,       # 基准各行业权重（近似）
        }
    """
    conn = get_db_connection()

    # 获取组合持仓快照
    query_snap = """
        SELECT ps.date, ps.code, ps.market_value, ps.pnl_rate
        FROM portfolio_snapshots ps
        WHERE ps.date = (SELECT MAX(date) FROM portfolio_snapshots WHERE date <= :end)
        AND ps.market_value > 0
    """
    if end_date:
        df_snap = pd.read_sql(query_snap, conn, params={"end": end_date})
    else:
        df_snap = pd.read_sql(query_snap, conn, params={"end": "9999-12-31"})

    if df_snap.empty:
        conn.close()
        return None

    # 获取N天前快照
    query_prev = """
        SELECT ps.code, ps.market_value as prev_mv
        FROM portfolio_snapshots ps
        WHERE ps.date = (
            SELECT DISTINCT date FROM portfolio_snapshots 
            WHERE date <= :end 
            ORDER BY date DESC 
            LIMIT 1 OFFSET :skip
        )
        AND ps.market_value > 0
    """
    skip = days
    if end_date:
        df_prev = pd.read_sql(query_prev, conn, params={"end": end_date, "skip": skip})
    else:
        df_prev = pd.read_sql(query_prev, conn, params={"end": "9999-12-31", "skip": skip})

    conn.close()

    if df_prev.empty:
        return None

    # 行业分类
    def get_sector(code):
        clean = code.replace("sh", "").replace("sz", "")
        cat = ETF_CATEGORIES.get(clean, {})
        return cat.get("sector", "其他")

    # 当前快照按行业聚合
    df_snap['sector'] = df_snap['code'].apply(get_sector)
    total_mv = df_snap['market_value'].sum()
    sector_weights = {}
    for sector, grp in df_snap.groupby('sector'):
        sector_weights[sector] = float(grp['market_value'].sum() / total_mv)

    # 计算各行业收益率
    df_prev['sector'] = df_prev['code'].apply(get_sector)

    # 计算每只ETF的N日收益率
    current_mv = df_snap.set_index('code')['market_value']
    prev_mv = df_prev.set_index('code')['prev_mv']

    # 匹配代码
    common_codes = current_mv.index.intersection(prev_mv.index)
    if len(common_codes) == 0:
        return None

    etf_returns = (current_mv[common_codes] / prev_mv[common_codes] - 1)
    etf_returns_df = etf_returns.reset_index()
    etf_returns_df.columns = ['code', 'return']
    etf_returns_df['sector'] = etf_returns_df['code'].apply(get_sector)

    # 各行业加权收益率
    sector_returns = {}
    for sector, grp in etf_returns_df.groupby('sector'):
        sector_returns[sector] = float(grp['return'].mean())

    # 基准行业权重（近似：均匀分布，实际应用中应从指数成分获取）
    n_sectors = len(sector_weights)
    bench_weights = {s: 1.0 / max(n_sectors, 1) for s in sector_weights}

    # 组合总收益率
    total_return = float(df_snap['market_value'].sum() / df_prev['prev_mv'].sum() - 1)

    # 基准收益率
    conn3 = get_db_connection()
    query_bench = "SELECT close FROM index_quotes WHERE code='sh000300' ORDER BY date DESC LIMIT 1"
    query_bench_prev = "SELECT close FROM index_quotes WHERE code='sh000300' ORDER BY date DESC LIMIT 1 OFFSET ?"
    bench_now = pd.read_sql(query_bench, conn3)
    bench_prev = pd.read_sql(query_bench_prev, conn3, params=(days,))
    conn3.close()

    benchmark_return = 0.0
    if not bench_now.empty and not bench_prev.empty:
        benchmark_return = float(bench_now['close'].iloc[0] / bench_prev['close'].iloc[0] - 1)

    # Brinson 分解
    all_sectors = set(list(sector_weights.keys()) + list(bench_weights.keys()))
    allocation_effect = {}
    selection_effect = {}

    for s in all_sectors:
        w_p = sector_weights.get(s, 0)    # 组合权重
        w_b = bench_weights.get(s, 0)     # 基准权重
        r_p = sector_returns.get(s, 0)    # 行业组合收益
        r_b = sector_returns.get(s, 0)    # 行业基准收益（简化：使用同值）

        allocation_effect[s] = (w_p - w_b) * r_b
        selection_effect[s] = w_p * (r_p - r_b)

    return {
        'total_return': total_return,
        'benchmark_return': benchmark_return,
        'allocation_effect': allocation_effect,
        'selection_effect': selection_effect,
        'sector_returns': sector_returns,
        'sector_weights': sector_weights,
        'bench_weights': bench_weights,
    }


@st.cache_data(ttl=600, show_spinner=False)
def compute_rebalance_suggestion(target_weights=None, threshold=0.05):
    """计算再平衡建议：基于目标权重与实际权重的偏离，生成调仓方案

    Args:
        target_weights: dict {sector: target_pct}，None则使用等权重
        threshold: 最小偏离阈值（百分比），低于此值不触发调仓

    Returns:
        dict or None
    """
    if target_weights is None:
        target_weights = {
            "医药": 0.15, "金融": 0.10, "军工": 0.10, "新能源": 0.15,
            "科技": 0.15, "宽基": 0.20, "红利": 0.10, "债券": 0.05
        }

    conn = get_db_connection()
    query = """
        SELECT code, name, market_value, current_price, quantity, cost_price
        FROM portfolio_snapshots 
        WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
        AND market_value > 0
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        return None

    total_mv = df['market_value'].sum()

    def get_sector(code):
        clean = code.replace("sh", "").replace("sz", "")
        cat = ETF_CATEGORIES.get(clean, {})
        return cat.get("sector", "其他")

    df['sector'] = df['code'].apply(get_sector)

    # 当前行业权重
    current_weights = {}
    sector_etfs = {}
    for sector, grp in df.groupby('sector'):
        current_weights[sector] = float(grp['market_value'].sum() / total_mv)
        sector_etfs[sector] = grp

    # 计算偏离
    suggestions = []
    all_sectors = set(list(target_weights.keys()) + list(current_weights.keys()))

    for sector in all_sectors:
        target = target_weights.get(sector, 0)
        current = current_weights.get(sector, 0)
        diff = current - target  # 正值=超配，负值=低配

        if abs(diff) < threshold:
            continue

        # 调仓金额
        trade_value = -diff * total_mv  # 负diff(低配) => 正trade(买入)

        etfs = sector_etfs.get(sector)
        if etfs is None or etfs.empty:
            continue

        # 等权分配到该行业的各ETF
        n_etfs = len(etfs)
        per_etf_value = trade_value / n_etfs

        for _, etf in etfs.iterrows():
            if abs(per_etf_value) < 100:  # 忽略小额
                continue
            shares = int(per_etf_value / etf['current_price']) if etf['current_price'] > 0 else 0
            if shares == 0:
                continue
            suggestions.append({
                'sector': sector,
                'code': etf['code'],
                'name': etf['name'],
                'current_weight': current,
                'target_weight': target,
                'diff': diff,
                'trade_value': per_etf_value,
                'shares': shares,
                'direction': '买入' if per_etf_value > 0 else '卖出',
                'price': etf['current_price'],
            })

    return {
        'current_weights': current_weights,
        'target_weights': target_weights,
        'suggestions': suggestions,
        'total_value': total_mv,
        'threshold': threshold,
    }

def export_positions_csv(positions_df, filename="持仓数据"):
    """导出持仓数据为CSV"""
    import tempfile
    import streamlit.components.v1 as components

    csv = positions_df.to_csv(index=False, encoding="utf-8-sig")
    b64 = base64.b64encode(csv.encode("utf-8-sig")).decode()
    href = f'data:text/csv;charset=utf-8-sig;base64,{b64}'
    return href, f"{filename}.csv"


def export_summary_csv(summary_df, filename="收益数据"):
    """导出收益数据为CSV"""
    csv = summary_df.to_csv(index=False, encoding="utf-8-sig")
    b64 = base64.b64encode(csv.encode("utf-8-sig")).decode()
    href = f'data:text/csv;charset=utf-8-sig;base64,{b64}'
    return href, f"{filename}.csv"




@st.cache_data(ttl=0, show_spinner=False)
def capture_dashboard_screenshot(port=8501):
    """截取 Dashboard 页面截图（PNG）

    通过 Selenium headless Chrome + webdriver_manager 自动管理 ChromeDriver。
    智能等待 Plotly 图表渲染完成后全页截图。

    Args:
        port: Streamlit 端口号

    Returns:
        str: PNG 文件路径，失败返回 None
    """
    try:
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("截图失败: 缺少 selenium 或 webdriver-manager，请执行 pip install selenium webdriver-manager")
        return None

    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = str(output_dir / f"dashboard_{timestamp}.png")

    try:
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--window-size=1920,3000')

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"http://localhost:{port}")

        # Step 1: 等待 Streamlit App 容器就绪
        for i in range(30):
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-testid='stApp']")
                if el.is_displayed():
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 2: 等待 Plotly 图表渲染（至少2个SVG出现）
        for i in range(45):
            try:
                charts = driver.find_elements(By.CSS_SELECTOR, ".js-plotly-plot .main-svg")
                if len(charts) >= 2:
                    time.sleep(2)  # 等待剩余图表
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 3: 滚动到底部触发懒加载，再滚回顶部
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # Step 4: 截取完整页面
        driver.save_screenshot(png_path)
        driver.quit()
        return png_path
    except Exception as e:
        print(f"截图失败: {e}")
        return None


def export_dashboard_pdf(port=8501):
    """导出 Dashboard 为 PDF

    通过 Selenium headless Chrome + CDP Page.printToPDF 实现，A3 宽幅输出。
    智能等待 Plotly 图表渲染完成后导出。

    Args:
        port: Streamlit 端口号

    Returns:
        str: PDF 文件路径，失败返回 None
    """
    try:
        import time
        import base64
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("PDF导出失败: 缺少 selenium 或 webdriver-manager，请执行 pip install selenium webdriver-manager")
        return None

    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = str(output_dir / f"dashboard_{timestamp}.pdf")

    try:
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--window-size=1920,3000')

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"http://localhost:{port}")

        # Step 1: 等待 Streamlit App 容器就绪
        for i in range(30):
            try:
                el = driver.find_element(By.CSS_SELECTOR, "[data-testid='stApp']")
                if el.is_displayed():
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 2: 等待 Plotly 图表渲染
        for i in range(45):
            try:
                charts = driver.find_elements(By.CSS_SELECTOR, ".js-plotly-plot .main-svg")
                if len(charts) >= 2:
                    time.sleep(2)
                    break
            except Exception:
                pass
            time.sleep(1)

        # Step 3: 滚动触发懒加载
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # Step 4: CDP printToPDF (A3 宽幅)
        pdf_result = driver.execute_cdp_cmd("Page.printToPDF", {
            "landscape": False,
            "displayHeaderFooter": False,
            "printBackground": True,
            "paperWidth": 13.0,
            "paperHeight": 19.0,
            "marginTop": 0.4,
            "marginBottom": 0.4,
            "marginLeft": 0.4,
            "marginRight": 0.4,
        })

        pdf_bytes = base64.b64decode(pdf_result['data'])
        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes)
        driver.quit()
        return pdf_path
    except Exception as e:
        print(f"PDF导出失败: {e}")
        return None

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
        .tip-title {
            font-size: 18px; font-weight: bold; color: #c9d1d9;
            padding: 10px 0 5px 0; border-bottom: 1px solid #30363d;
            display: inline-block; cursor: help;
        }
        .tip-title::after {
            content: ' ℹ';
            font-size: 11px; color: #58a6ff; font-weight: normal;
        }
        .tip-title .tip-text {
            visibility: hidden; opacity: 0;
            position: absolute; z-index: 999;
            background: #1c2333; color: #c9d1d9;
            border: 1px solid #30363d; border-radius: 6px;
            padding: 8px 12px; font-size: 12px; font-weight: normal;
            line-height: 1.5; width: max-content; max-width: 360px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.4);
            transition: opacity 0.2s, visibility 0.2s;
            margin-top: 6px; margin-left: 0;
        }
        .tip-title:hover .tip-text {
            visibility: visible; opacity: 1;
        }
        .tip-title .tip-arrow {
            visibility: hidden; opacity: 0;
            position: absolute; z-index: 999;
            border-left: 6px solid transparent;
            border-right: 6px solid transparent;
            border-bottom: 6px solid #30363d;
            transition: opacity 0.2s, visibility 0.2s;
        }
        .tip-title:hover .tip-arrow {
            visibility: visible; opacity: 1;
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

        # 基准指数选择（P1改进）
        st.markdown("### 📌 基准指数")
        benchmark_options = {k: v for k, v in INDEX_CODES.items()}
        # 默认选中沪深300
        default_bench = "sh000300"
        benchmark_keys = list(benchmark_options.keys())
        default_idx = benchmark_keys.index(default_bench) if default_bench in benchmark_keys else 0
        selected_benchmark = st.selectbox(
            "对比基准",
            options=benchmark_keys,
            index=default_idx,
            format_func=lambda x: benchmark_options[x],
            key="benchmark_select"
        )

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
            load_benchmark_comparison(selected_benchmark, show_days, _d)
        for _days in _preset_days_list:
            load_summary(_days, available_dates[0])
            load_benchmark_comparison(selected_benchmark, _days, available_dates[0])

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
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="当前所有持仓证券的市值总和">总市值 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:#58a6ff;">¥{format_value(total_value)}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[1]:
        pnl_color = "#22c55e" if total_pnl >= 0 else "#ef4444"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {pnl_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="所有持仓的累计盈亏金额和收益率，基于买入成本计算">总盈亏 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{pnl_color};">{format_value(total_pnl, prefix="¥")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(total_return, suffix="%")}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[2]:
        dr_color = "#22c55e" if daily_return >= 0 else "#ef4444"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dr_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="选定日期相对于前一交易日的收益率(%)和盈亏金额(元)">日收益 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dr_color};">{format_value(daily_return, suffix="%")}</div>'
            f'<div style="font-size:11px;color:#8b949e;">{format_value(daily_pnl, prefix="¥")}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[3]:
        sharpe_color = "#22c55e" if (sharpe and sharpe > 0.5) else "#f59e0b" if sharpe else "#888"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {sharpe_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="风险调整后收益指标 = (年化收益率 - 无风险利率) / 年化波动率。>1为优秀，>0.5为良好">夏普比率 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{sharpe_color};">{format_value(sharpe, decimals=3)}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[4]:
        dd_color = "#ef4444" if (max_dd and abs(max_dd) > 10) else "#f59e0b" if (max_dd and abs(max_dd) > 5) else "#22c55e"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {dd_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="选定时间段内，组合从历史最高点到最低点的最大跌幅(%)">最大回撤 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{dd_color};">{format_value(max_dd, suffix="%")}</div>'
            f'</div>', unsafe_allow_html=True)
    with cols[5]:
        vol_color = "#ef4444" if (volatility and volatility > 25) else "#f59e0b" if (volatility and volatility > 15) else "#22c55e"
        st.markdown(
            f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid {vol_color};">'
            f'<div style="font-size:11px;color:#8b949e;cursor:help;border-bottom:1px dotted #8b949e;display:inline;" title="日收益率标准差的年化值，反映组合收益的波动幅度。值越高表示风险越大">年化波动率 ℹ</div>'
            f'<div style="font-size:20px;font-weight:bold;color:{vol_color};">{format_value(volatility, suffix="%")}</div>'
            f'</div>', unsafe_allow_html=True)

    # ========== 图表行1: 净值曲线 + 收益分布 ==========
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 净值走势", "📊 持仓分布", "⚠️ 风险分析", "📅 收益日历", "💠 高级分析"])

    with tab1:
        st.caption("📈 展示组合净值走势与基准对比、日收益率分布、每日盈亏及滚动风险指标")
        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.markdown('<div class="tip-title" style="">组合净值走势<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以组合总市值为基准归一化到100，展示组合净值随时间的变化趋势，同时叠加基准指数走势进行对比。</span></div>', unsafe_allow_html=True)
            if not summary.empty and len(summary) > 1:
                # 计算累计净值（基准100）
                base_value = summary.iloc[0]['total_value']
                summary_plot = summary.copy()
                summary_plot['nav'] = summary_plot['total_value'] / base_value * 100

                # 降采样用于图表渲染
                chart_data = downsample(summary_plot, max_points=500)

                # 基准指数对比（使用侧边栏选择的基准）
                bench_name = INDEX_CODES.get(selected_benchmark, selected_benchmark)
                bench_df = load_benchmark_comparison(selected_benchmark, show_days + 10, selected_date)
                if not bench_df.empty:
                    bench_base = bench_df.iloc[0]['close']
                    bench_plot = bench_df.copy()
                    bench_plot['nav'] = bench_plot['close'] / bench_base * 100
                    bench_chart = downsample(bench_plot, max_points=500)

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=bench_chart['date'], y=bench_chart['nav'],
                        mode='lines', name=bench_name,
                        line=dict(color='#8b949e', width=1.5, dash='dash')
                    ))
                    fig.add_trace(go.Scatter(
                        x=chart_data['date'], y=chart_data['nav'],
                        mode='lines', name='投资组合',
                        line=dict(color='#58a6ff', width=2)
                    ))

                    # 标记净值最高和最低
                    _add_min_max_annotations(fig, chart_data['date'], chart_data['nav'], y_label="净值")

                else:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=chart_data['date'], y=chart_data['nav'],
                        mode='lines', name='投资组合',
                        line=dict(color='#58a6ff', width=2)
                    ))

                    # 标记净值最高和最低
                    _add_min_max_annotations(fig, chart_data['date'], chart_data['nav'], y_label="净值")

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
            st.markdown('<div class="tip-title" style="">日收益率分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">统计选定时间范围内每日收益率(%)的频率分布。橙色虚线为均值，黄色区间为±1个标准差范围，绿色虚线为±2个标准差。</span></div>', unsafe_allow_html=True)
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
        st.markdown('<div class="tip-title" style="">每日盈亏<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示每个交易日的盈亏金额(元)。绿色柱体表示盈利日，红色柱体表示亏损日，可直观观察收益的连续性和波动幅度。</span></div>', unsafe_allow_html=True)
        if not summary.empty and 'daily_pnl' in summary.columns and len(summary) > 1:
            bar_data = downsample(summary[['date', 'daily_pnl']].copy(), max_points=500)
            colors = ['#22c55e' if dp >= 0 else '#ef4444' for dp in bar_data['daily_pnl']]
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                x=bar_data['date'], y=bar_data['daily_pnl'],
                marker_color=colors,
                name='日盈亏'
            ))
            # 标记最大盈亏
            _add_min_max_annotations(fig_bar, bar_data['date'], bar_data['daily_pnl'], y_label="盈亏")

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
            st.markdown(f'<div class="tip-title">'f'滚动风险指标（{rolling_window}日窗口）'f'<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'f'<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'f'使用{rolling_window}日滚动窗口计算的夏普比率和年化波动率。滚动夏普比率反映近期风险调整收益的稳定程度；滚动波动率反映近期市场波动水平的演变。</span>'f'</div>', unsafe_allow_html=True)
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

            # 标记滚动夏普最高最低
            _add_min_max_annotations(fig_roll, rolling_chart['date'], rolling_chart['rolling_sharpe'], row=1, col=1)

            fig_roll.add_trace(go.Scatter(
                x=rolling_chart['date'], y=rolling_chart['rolling_vol'],
                mode='lines', name='滚动波动率',
                line=dict(color='#f59e0b', width=1.5),
                fill='tozeroy', fillcolor='rgba(245,158,11,0.08)'
            ), row=2, col=1)

            # 标记滚动波动率最高最低
            _add_min_max_annotations(fig_roll, rolling_chart['date'], rolling_chart['rolling_vol'], row=2, col=1)

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
        st.caption("📊 展示持仓分布饼图、持仓明细表格、行业权重变化趋势及持仓相关性矩阵")
        col_dist, col_table = st.columns([1, 1])

        with col_dist:
            st.markdown('<div class="tip-title" style="">持仓分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以环形饼图展示各只ETF的市值占比，中心空白区域显示总持仓数量。鼠标悬停可查看具体金额和百分比。</span></div>', unsafe_allow_html=True)
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
            st.markdown('<div class="tip-title" style="">持仓明细<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示所有持仓ETF的详细信息，包括名称、代码、持仓量、成本价、现价、市值、盈亏和收益率。点击下拉框可查看单只ETF的技术分析详情。</span></div>', unsafe_allow_html=True)
            if not positions.empty:
                # 格式化显示列
                display_df = positions[['name', 'code', 'quantity', 'cost_price', 'current_price',
                                       'market_value', 'pnl', 'pnl_rate']].copy()
                display_df.columns = ['名称', '代码', '持仓量', '成本价', '现价', '市值', '盈亏', '收益率%']
                display_df['持仓量'] = display_df['持仓量'].apply(lambda x: f"{x:,.0f}")
                display_df['成本价'] = display_df['成本价'].apply(lambda x: f"{x:.3f}")
                display_df['现价'] = display_df['现价'].apply(lambda x: f"{x:.3f}")
                display_df['市值'] = display_df['市值'].apply(lambda x: f"¥{x:,.0f}")
                display_df['盈亏'] = display_df['盈亏'].apply(lambda x: f"¥{x:,.0f}")
                display_df['收益率%'] = display_df['收益率%'].apply(lambda x: f"{x:+.2f}%")

                # 交互式表格 + ETF 选择器（点击行查看详情）
                selected_etf = st.selectbox(
                    "选择ETF查看详情",
                    options=[f"{row['name']}（{row['code']}）" for _, row in positions.iterrows()],
                    key="etf_detail_select",
                    label_visibility="collapsed",
                )
                st.dataframe(
                    display_df,
                    height=420,
                    
                    hide_index=True,
                    column_config={
                        "收益率%": st.column_config.TextColumn(disabled=True),
                    }
                )

                if selected_etf and not positions.empty:
                    match = positions[positions.apply(lambda r: f"{r['name']}（{r['code']}）" == selected_etf, axis=1)]
                    if not match.empty:
                        row = match.iloc[0]
                        with st.expander(f"**{row['name']}（{row['code']}）** 详细分析", expanded=True):
                            _render_etf_detail_panel(row, selected_date, total_value)

        # ===== 行业权重堆叠面积图 =====
        st.markdown('<div class="tip-title" style="">行业权重变化趋势<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以堆叠面积图展示各行业ETF在组合中的权重占比随时间的变化，可观察仓位配置的调整趋势。</span></div>', unsafe_allow_html=True)
        sector_weight_df, sector_colors = load_sector_weights(days=show_days, end_date=selected_date)
        if not sector_weight_df.empty:
            fig_sector = go.Figure()
            for col in sector_weight_df.columns:
                fig_sector.add_trace(go.Scatter(
                    x=sector_weight_df.index,
                    y=sector_weight_df[col],
                    name=col,
                    mode="lines",
                    stackgroup="one",
                    line=dict(width=0.5),
                    fillcolor=sector_colors.get(col, "#6b7280"),
                    hovertemplate=f"<b>{col}</b><br>权重: %{{y:.1f}}%<extra></extra>"
                ))
            fig_sector.update_layout(
                height=280,
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=50, r=20, t=10, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                            font=dict(size=10)),
                xaxis=dict(showgrid=False, tickformat='%m-%d'),
                yaxis=dict(title='权重 %', showgrid=True, gridcolor='#21262d'),
                hovermode='x unified',
            )
            st.plotly_chart(fig_sector, width='stretch')

            # 行业权重摘要卡片
            latest_weights = sector_weight_df.iloc[-1]
            n_sectors = len(latest_weights[latest_weights > 1])
            max_sector = latest_weights.idxmax()
            min_sector = latest_weights[latest_weights > 0].idxmin()
            st.caption(
                f"覆盖 {n_sectors} 个行业 | 最大: **{max_sector}** {latest_weights[max_sector]:.1f}% | "
                f"最小: **{min_sector}** {latest_weights[min_sector]:.1f}% | 数据截至 {selected_date}"
            )
        else:
            st.info("持仓历史数据不足，暂无法展示行业权重变化")

        st.markdown("---")
                # ===== 相关性矩阵热力图 =====
        st.markdown("---")
        st.markdown('<div class="tip-title" style="">持仓相关性矩阵（日收益率 Pearson）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于最近250个交易日的日收益率计算各ETF之间的Pearson相关系数。数值接近1表示同向变动，接近-1表示反向变动，接近0表示无相关性。</span></div>', unsafe_allow_html=True)
        corr_df, short_names = load_correlation_matrix(days=250, end_date=selected_date)
        if not corr_df.empty and len(short_names) >= 2:
            fig_corr = go.Figure(go.Heatmap(
                z=corr_df.values,
                x=[short_names.get(c, c) for c in corr_df.columns],
                y=[short_names.get(c, c) for c in corr_df.index],
                colorscale=[
                    [0, '#0d419d'],
                    [0.25, '#1a6bb5'],
                    [0.5, '#21262d'],
                    [0.75, '#b5411a'],
                    [1, '#9d0d0d']
                ],
                zmin=-1, zmax=1,
                text=corr_df.values.round(2),
                texttemplate="%{text}",
                textfont=dict(size=9),
                hovertemplate="<b>%{x} vs %{y}</b><br>相关系数: %{z:.3f}<extra></extra>",
                colorbar=dict(
                    thickness=15,
                    len=0.9,
                    outlinewidth=0,
                    tickfont=dict(size=10, color='#8b949e')
                )
            ))
            fig_corr.update_layout(
                height=max(500, len(corr_df) * 28),
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=5, r=40, t=10, b=5),
                xaxis=dict(tickangle=45, side='bottom', tickfont=dict(size=9)),
                yaxis=dict(tickfont=dict(size=9), autorange='reversed'),
            )
            fig_corr.update_xaxes(showgrid=False)
            fig_corr.update_yaxes(showgrid=False)
            st.plotly_chart(fig_corr, width='stretch')
            st.caption(f"基于最近250个交易日的市值日收益率计算 | 数据截至 {selected_date}")
        else:
            st.info("持仓数据不足，暂无法计算相关性矩阵")

    with tab3:
        st.caption("⚠️ 展示风险评分仪表盘、风险指标详情、回撤曲线及Brinson收益归因分析")
        col_risk_gauge, col_risk_detail = st.columns([1, 1])

        with col_risk_gauge:
            st.markdown('<div class="tip-title" style="">风险指标仪表盘<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">综合年化波动率和最大回撤计算风险评分（0-100分）。满分100表示低风险，低于60分表示高风险。颜色越绿越安全，越红风险越高。</span></div>', unsafe_allow_html=True)

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
            st.markdown('<div class="tip-title" style="">风险指标详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示夏普比率、Sortino比率、Calmar比率、最大回撤、年化波动率、胜率和盈亏比等核心风险指标，悬停可查看指标含义。</span></div>', unsafe_allow_html=True)

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
            st.markdown('<div class="tip-title" style="">回撤曲线<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示组合从历史最高点到当前市值的回撤幅度(%)。可识别最大回撤区间及其恢复时间，评估组合的抗风险能力。</span></div>', unsafe_allow_html=True)
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
            # 标记最大回撤
            _add_min_max_annotations(fig_dd, dd_chart['date'], dd_chart['drawdown'], y_label="回撤")

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


        # ===== P2: 收益归因分析（Brinson模型） =====
        st.markdown("---")
        st.markdown('<div class="tip-title" style="">收益归因分析（Brinson 模型）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">使用Brinson归因模型将组合超额收益分解为「配置效应」（超配/低配行业的贡献）和「选股效应」（行业内个股选择的贡献），帮助判断收益来源。</span></div>', unsafe_allow_html=True)
        st.caption("将组合收益分解为行业配置效应（超配/低配行业的贡献）和选股效应（行业内个股选择的贡献）")

        attr_result = compute_return_attribution(days=show_days, end_date=selected_date)
        if attr_result and attr_result.get('sector_returns'):
            ar = attr_result

            # 瀑布图数据
            waterfall_labels = ["基准收益"]
            waterfall_values = [ar['benchmark_return'] * 100]
            waterfall_colors = ["#8b949e"]

            # 配置效应
            alloc_total = 0
            for sector, val in sorted(ar['allocation_effect'].items(), key=lambda x: abs(x[1]), reverse=True):
                if abs(val) > 0.001:  # > 0.1% 才显示
                    waterfall_labels.append(f"{sector}\n配置")
                    waterfall_values.append(val * 100)
                    waterfall_colors.append("#22c55e" if val > 0 else "#ef4444")
                    alloc_total += val

            # 选股效应
            sel_total = 0
            for sector, val in sorted(ar['selection_effect'].items(), key=lambda x: abs(x[1]), reverse=True):
                if abs(val) > 0.001:
                    waterfall_labels.append(f"{sector}\n选股")
                    waterfall_values.append(val * 100)
                    waterfall_colors.append("#58a6ff" if val > 0 else "#f59e0b")
                    sel_total += val

            waterfall_labels.append("组合收益")
            waterfall_values.append(ar['total_return'] * 100)
            waterfall_colors.append("#a855f7")

            # 计算瀑布图中间值
            running = 0
            y_data = []
            for i, v in enumerate(waterfall_values):
                if i == 0 or i == len(waterfall_values) - 1:
                    y_data.append(v)
                    running = v
                else:
                    y_data.append(running + v)
                    running += v

            # 底部坐标（从上一个running开始）
            base_data = [0]  # 基准从0开始
            run = waterfall_values[0]
            for i in range(1, len(waterfall_values) - 1):
                base_data.append(run)
                run += waterfall_values[i]
            base_data.append(0)  # 组合收益从0开始

            fig_wf = go.Figure()
            fig_wf.add_trace(go.Bar(
                x=waterfall_labels,
                y=[v if i == 0 or i == len(waterfall_values)-1 else abs(v)
                   for i, v in enumerate(waterfall_values)],
                base=base_data,
                marker_color=waterfall_colors,
                text=[f"{v:+.2f}%" for v in waterfall_values],
                textposition="outside",
                textfont=dict(size=9, color="#c9d1d9"),
                hovertemplate="<b>%{x}</b><br>贡献: %{text}<extra></extra>",
            ))
            fig_wf.update_layout(
                height=max(350, len(waterfall_labels) * 22),
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=50, r=20, t=10, b=80),
                xaxis=dict(tickangle=45, tickfont=dict(size=8)),
                yaxis=dict(title='收益率 (%)', showgrid=True, gridcolor='#21262d'),
                showlegend=False,
                barmode='relative',
            )
            st.plotly_chart(fig_wf, width='stretch')

            # 归因摘要卡片
            col_attr1, col_attr2, col_attr3 = st.columns(3)
            with col_attr1:
                st.metric("组合收益", f"{ar['total_return']*100:+.2f}%")
            with col_attr2:
                st.metric("基准收益", f"{ar['benchmark_return']*100:+.2f}%")
            with col_attr3:
                alpha = (ar['total_return'] - ar['benchmark_return']) * 100
                st.metric("超额收益 (Alpha)", f"{alpha:+.2f}%")

            # 行业明细表
            with st.expander("查看行业归因明细", expanded=False):
                attr_rows = []
                for sector in sorted(set(list(ar['sector_weights'].keys()) + list(ar.get('allocation_effect', {}).keys()))):
                    attr_rows.append({
                        '行业': sector,
                        '组合权重': f"{ar['sector_weights'].get(sector, 0)*100:.1f}%",
                        '行业收益': f"{ar['sector_returns'].get(sector, 0)*100:+.2f}%",
                        '配置效应': f"{ar['allocation_effect'].get(sector, 0)*100:+.3f}%",
                        '选股效应': f"{ar['selection_effect'].get(sector, 0)*100:+.3f}%",
                    })
                if attr_rows:
                    st.markdown(
                        pd.DataFrame(attr_rows).to_html(index=False, escape=False),
                        unsafe_allow_html=True
                    )
        else:
            st.info("历史数据不足（需要至少250个交易日），暂无法进行收益归因分析")

    # ========== 收益日历 ==========
    with tab4:
        st.caption("📅 以日历热力图形式展示每月每个交易日的收益情况，支持按年/月切换查看")
        cal_data = load_calendar_data()

        if cal_data.empty:
            st.info("暂无日历数据")
        else:
            years = sorted(cal_data['year'].unique(), reverse=True)
            latest_year = years[0]
            today_str = datetime.now().strftime('%Y-%m-%d')

            # --- 年份选择 ---
            cur_year = st.session_state.get('cal_year', latest_year)
            yr_cols = st.columns(len(years))
            for i, yr in enumerate(years):
                with yr_cols[i]:
                    if st.button(str(yr), key=f"yr_{yr}",
                                 type="primary" if cur_year == yr else "secondary"):
                        st.session_state['cal_year'] = yr
                        st.session_state.pop('cal_month', None)
                        st.rerun()

            sel_year = cur_year
            year_df = cal_data[cal_data['year'] == sel_year]

            # --- 年度月度概览（月份可点击切换） ---
            months_in_year = sorted(year_df['month'].unique())
            sel_month = st.session_state.get('cal_month', months_in_year[-1] if months_in_year else 1)
            if sel_month not in months_in_year:
                sel_month = months_in_year[-1] if months_in_year else 1

            yr_monthly = year_df.groupby('month').agg(
                pnl_sum=('daily_pnl', 'sum'),
                ret_sum=('daily_return', 'sum'),
                days=('day', 'count')
            ).reset_index()

            yr_monthly['profit_days'] = year_df[year_df['daily_pnl'] > 0].groupby('month').size().reindex(yr_monthly['month'], fill_value=0).values
            yr_monthly['loss_days'] = year_df[year_df['daily_pnl'] < 0].groupby('month').size().reindex(yr_monthly['month'], fill_value=0).values

            # --- 年度月度概览（月份按钮在表格内） ---
            yr_total_pnl = year_df['daily_pnl'].sum()
            yr_total_ret = year_df['daily_return'].sum()
            yr_total_days = len(year_df)
            yr_profit_days = len(year_df[year_df['daily_pnl'] > 0])
            yr_loss_days = len(year_df[year_df['daily_pnl'] < 0])
            yr_pnl_color = '#22c55e' if yr_total_pnl >= 0 else '#ef4444'
            yr_ret_color = '#22c55e' if yr_total_ret >= 0 else '#ef4444'

            # Header row: label + data headers
            hdr_col1, hdr_col2 = st.columns([1, 5])
            with hdr_col1:
                st.markdown('<div style="color:#8b949e;font-size:13px;padding:6px 0;border-bottom:1px solid #30363d;text-align:center;">月份</div>', unsafe_allow_html=True)
            with hdr_col2:
                st.markdown(
                    '<div style="display:flex;color:#8b949e;font-size:13px;border-bottom:1px solid #30363d;">'
                    '<div style="flex:1;text-align:right;padding:6px 10px;">月收益</div>'
                    '<div style="flex:1;text-align:right;padding:6px 10px;">月收益率</div>'
                    '<div style="flex:1;text-align:center;padding:6px 10px;">交易日</div>'
                    '<div style="flex:1;text-align:center;padding:6px 10px;">盈利天数</div>'
                    '<div style="flex:1;text-align:center;padding:6px 10px;">亏损天数</div>'
                    '</div>', unsafe_allow_html=True)

            # Data rows: month button + data
            for _, row in yr_monthly.iterrows():
                m = int(row['month'])
                pnl = row['pnl_sum']
                ret = row['ret_sum']
                days = int(row['days'])
                profit_d = int(row['profit_days'])
                loss_d = int(row['loss_days'])
                pnl_color = '#22c55e' if pnl >= 0 else '#ef4444'
                ret_color = '#22c55e' if ret >= 0 else '#ef4444'
                is_active = (m == sel_month)

                row_col1, row_col2 = st.columns([1, 5])
                with row_col1:
                    _b1, _b2, _b3 = st.columns([1, 1, 1])
                    with _b2:
                        st.button(f"{m}月", key=f"mo_{sel_year}_{m}",
                                  type="primary" if is_active else "secondary")
                with row_col2:
                    bg = 'background:#161b22;' if is_active else ''
                    st.markdown(
                        f'<div style="display:flex;{bg}border-bottom:1px solid #21262d;">'
                        f'<div style="flex:1;text-align:right;padding:6px 10px;color:{pnl_color};">¥{pnl:,.0f}</div>'
                        f'<div style="flex:1;text-align:right;padding:6px 10px;color:{ret_color};">{ret:+.2f}%</div>'
                        f'<div style="flex:1;text-align:center;padding:6px 10px;">{days}天</div>'
                        f'<div style="flex:1;text-align:center;padding:6px 10px;color:#22c55e;">{profit_d}天</div>'
                        f'<div style="flex:1;text-align:center;padding:6px 10px;color:#ef4444;">{loss_d}天</div>'
                        f'</div>', unsafe_allow_html=True)

            # Yearly total row
            tot_col1, tot_col2 = st.columns([1, 5])
            with tot_col1:
                st.markdown('<div style="font-weight:bold;text-align:center;padding:8px 0;color:#58a6ff;'
                            'border-top:2px solid #30363d;">全年合计</div>', unsafe_allow_html=True)
            with tot_col2:
                st.markdown(
                    f'<div style="display:flex;font-weight:bold;background:#161b22;border-top:2px solid #30363d;">'
                    f'<div style="flex:1;text-align:right;padding:8px 10px;color:{yr_pnl_color};">¥{yr_total_pnl:,.0f}</div>'
                    f'<div style="flex:1;text-align:right;padding:8px 10px;color:{yr_ret_color};">{yr_total_ret:+.2f}%</div>'
                    f'<div style="flex:1;text-align:center;padding:8px 10px;">{yr_total_days}天</div>'
                    f'<div style="flex:1;text-align:center;padding:8px 10px;color:#22c55e;">{yr_profit_days}天</div>'
                    f'<div style="flex:1;text-align:center;padding:8px 10px;color:#ef4444;">{yr_loss_days}天</div>'
                    f'</div>', unsafe_allow_html=True)


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
            st.markdown('<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">月度收益热力图<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">以热力图形式展示12个月的月度收益，颜色深浅反映收益高低。</span></div>', unsafe_allow_html=True)
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

    # ========== Tab5: 高级分析（Monte Carlo / 再平衡建议） ==========
    with tab5:
        st.markdown('<div class="tip-title" style="">高级分析工具<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">包含Monte Carlo模拟（基于历史收益率随机采样预测未来收益区间）和再平衡建议（基于目标权重偏离度生成调仓方案）两种高级分析工具。</span></div>', unsafe_allow_html=True)

        # ----- Monte Carlo 模拟 -----
        st.markdown('<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">Monte Carlo 模拟（未来收益预测）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于历史日收益率分布进行Bootstrap随机采样，生成大量模拟路径，统计未来市值的概率分布区间（P5/P50/P95）。</span></div>', unsafe_allow_html=True)
        st.caption("基于历史日收益率分布进行 Bootstrap 采样，生成未来收益区间预测")

        mc_col1, mc_col2 = st.columns([2, 1])
        with mc_col1:
            mc_days = st.slider("模拟天数", 30, 500, 252, step=30, key="mc_days")
        with mc_col2:
            mc_sims = st.selectbox("模拟路径数", [200, 500, 1000], index=1, key="mc_sims")

        mc_result = run_monte_carlo(days=mc_days, n_simulations=mc_sims, end_date=selected_date)

        if mc_result is not None:
            perc_df = mc_result['percentiles']

            # 扇形区域图
            fig_mc = go.Figure()

            # 扇形填充区域（从外到内）
            fig_mc.add_trace(go.Scatter(
                x=perc_df['day'], y=perc_df['p95'],
                mode='lines', name='P95', line=dict(width=0),
                showlegend=False, hoverinfo='skip'
            ))
            fig_mc.add_trace(go.Scatter(
                x=perc_df['day'], y=perc_df['p75'],
                mode='lines', name='P75', fill='tonexty',
                fillcolor='rgba(88,166,255,0.08)', line=dict(width=0),
                showlegend=False, hoverinfo='skip'
            ))
            fig_mc.add_trace(go.Scatter(
                x=perc_df['day'], y=perc_df['p25'],
                mode='lines', name='P25', fill='tonexty',
                fillcolor='rgba(88,166,255,0.12)', line=dict(width=0),
                showlegend=False, hoverinfo='skip'
            ))
            fig_mc.add_trace(go.Scatter(
                x=perc_df['day'], y=perc_df['p5'],
                mode='lines', name='P5', fill='tonexty',
                fillcolor='rgba(88,166,255,0.08)', line=dict(width=0),
                showlegend=False, hoverinfo='skip'
            ))

            # 中位数线
            fig_mc.add_trace(go.Scatter(
                x=perc_df['day'], y=perc_df['p50'],
                mode='lines', name='中位数 (P50)',
                line=dict(color='#58a6ff', width=2),
                hovertemplate='第 %{x} 天<br>中位数: ¥%{y:,.0f}<extra></extra>'
            ))

            # 起始值水平线
            fig_mc.add_hline(
                y=mc_result['last_value'], line_dash="dash", line_color="#f59e0b",
                annotation_text=f"当前 ¥{mc_result['last_value']:,.0f}",
                annotation_position="top right",
                annotation_font=dict(size=10, color="#f59e0b")
            )

            fig_mc.update_layout(
                height=350,
                plot_bgcolor='#0d1117',
                paper_bgcolor='#0d1117',
                font=dict(color='#c9d1d9', size=11),
                margin=dict(l=60, r=20, t=10, b=40),
                xaxis=dict(title='交易日', showgrid=False),
                yaxis=dict(title='组合市值 (¥)', showgrid=True, gridcolor='#21262d'),
                hovermode='x unified',
                legend=dict(
                    orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                    font=dict(size=10, color='#8b949e')
                ),
            )
            st.plotly_chart(fig_mc, width='stretch')

            # 模拟摘要
            mc_sum1, mc_sum2, mc_sum3, mc_sum4 = st.columns(4)
            with mc_sum1:
                st.metric("当前市值", f"¥{mc_result['last_value']:,.0f}")
            with mc_sum2:
                final_p50 = perc_df['p50'].iloc[-1]
                chg = (final_p50 / mc_result['last_value'] - 1) * 100 if mc_result['last_value'] > 0 else 0
                st.metric(f"P50 ({mc_days}日后)", f"¥{final_p50:,.0f}", delta=f"{chg:+.1f}%")
            with mc_sum3:
                final_p5 = perc_df['p5'].iloc[-1]
                loss = (final_p5 / mc_result['last_value'] - 1) * 100 if mc_result['last_value'] > 0 else 0
                st.metric("P5 (悲观)", f"¥{final_p5:,.0f}", delta=f"{loss:+.1f}%")
            with mc_sum4:
                final_p95 = perc_df['p95'].iloc[-1]
                gain = (final_p95 / mc_result['last_value'] - 1) * 100 if mc_result['last_value'] > 0 else 0
                st.metric("P95 (乐观)", f"¥{final_p95:,.0f}", delta=f"{gain:+.1f}%")

            # VaR 估计
            with st.expander("查看风险价值 (VaR) 估计", expanded=False):
                var_95 = (mc_result['last_value'] - perc_df['p5'].iloc[-1])
                cvar_95 = mc_result['last_value'] - np.percentile(mc_result['paths'][:, -1], 5)
                st.markdown(
                    f"**95% VaR（{mc_days}日）:** ¥{var_95:,.0f}\n\n"
                    f"**95% CVaR（条件VaR）:** ¥{cvar_95:,.0f}\n\n"
                    f"*VaR 表示在 95% 置信度下，{mc_days} 个交易日内的最大可能损失。"
                    f"CVaR 是超出 VaR 时的平均损失（尾部风险）。*"
                )
        else:
            st.info("历史数据不足（需要至少30个交易日），暂无法进行 Monte Carlo 模拟")

        st.markdown("---")

        # ----- 再平衡建议 -----
        st.markdown('<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">再平衡建议<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于各行业目标权重与实际权重的偏离度，自动生成调仓方案。超过偏离阈值的行业将给出买入/卖出建议和估算股数。</span></div>', unsafe_allow_html=True)
        st.caption("基于目标行业权重与实际权重的偏离，生成调仓方案")

        rb_col1, rb_col2 = st.columns([2, 1])
        with rb_col1:
            st.markdown("*默认目标权重*")
        with rb_col2:
            show_rb = st.toggle("显示再平衡方案", value=True, key="rb_toggle")

        # 目标权重展示
        default_targets = {
            "医药": 0.15, "金融": 0.10, "军工": 0.10, "新能源": 0.15,
            "科技": 0.15, "宽基": 0.20, "红利": 0.10, "债券": 0.05
        }

        if show_rb:
            rb_result = compute_rebalance_suggestion(threshold=0.03)

            if rb_result is not None:
                rw = rb_result['current_weights']
                tw = rb_result['target_weights']
                all_sectors = sorted(set(list(rw.keys()) + list(tw.keys())))

                # 权重对比柱状图
                fig_rb = go.Figure()
                x_labels = all_sectors
                fig_rb.add_trace(go.Bar(
                    name='当前权重', x=x_labels,
                    y=[rw.get(s, 0) * 100 for s in all_sectors],
                    marker_color='#58a6ff', opacity=0.85,
                    hovertemplate='%{x}<br>当前: %{y:.1f}%<extra></extra>'
                ))
                fig_rb.add_trace(go.Bar(
                    name='目标权重', x=x_labels,
                    y=[tw.get(s, 0) * 100 for s in all_sectors],
                    marker_color='#f59e0b', opacity=0.6,
                    hovertemplate='%{x}<br>目标: %{y:.1f}%<extra></extra>'
                ))

                # 偏离线
                deviations = [(rw.get(s, 0) - tw.get(s, 0)) * 100 for s in all_sectors]
                fig_rb.add_trace(go.Scatter(
                    name='偏离', x=x_labels, y=deviations,
                    mode='lines+markers', marker_color='#ef4444', marker_size=6,
                    line=dict(color='#ef4444', width=1.5, dash='dot'),
                    yaxis='y2',
                    hovertemplate='%{x}<br>偏离: %{y:+.1f}%<extra></extra>'
                ))

                fig_rb.update_layout(
                    height=300,
                    barmode='group',
                    plot_bgcolor='#0d1117',
                    paper_bgcolor='#0d1117',
                    font=dict(color='#c9d1d9', size=11),
                    margin=dict(l=40, r=40, t=10, b=40),
                    xaxis=dict(showgrid=False, tickfont=dict(size=10)),
                    yaxis=dict(title='权重 (%)', showgrid=True, gridcolor='#21262d'),
                    yaxis2=dict(title='偏离 (%)', overlaying='y', side='right',
                                showgrid=False, range=[-20, 20]),
                    legend=dict(
                        orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                        font=dict(size=10, color='#8b949e')
                    ),
                )
                st.plotly_chart(fig_rb, width='stretch')

                # 摘要指标
                rb_s1, rb_s2, rb_s3 = st.columns(3)
                with rb_s1:
                    n_suggestions = len(rb_result['suggestions'])
                    st.metric("需调仓行业", f"{n_suggestions} 个")
                with rb_s2:
                    max_dev = max(abs(rw.get(s, 0) - tw.get(s, 0)) * 100 for s in all_sectors)
                    max_sector = max(all_sectors, key=lambda s: abs(rw.get(s, 0) - tw.get(s, 0)))
                    st.metric("最大偏离", f"{max_dev:.1f}%", delta=max_sector)
                with rb_s3:
                    st.metric("组合总市值", f"¥{rb_result['total_value']:,.0f}")

                # 调仓明细
                if rb_result['suggestions']:
                    with st.expander("查看调仓明细", expanded=False):
                        rb_rows = []
                        for s in rb_result['suggestions']:
                            rb_rows.append({
                                '行业': s['sector'],
                                'ETF': f"{s['name']}（{s['code']}）",
                                '方向': s['direction'],
                                '当前权重': f"{s['current_weight']*100:.1f}%",
                                '目标权重': f"{s['target_weight']*100:.1f}%",
                                '偏离': f"{s['diff']*100:+.1f}%",
                                '调仓金额': f"¥{s['trade_value']:+,.0f}",
                                '预估股数': f"{s['shares']:+,}",
                                '现价': f"¥{s['price']:.3f}",
                            })
                        st.markdown(
                            pd.DataFrame(rb_rows).to_html(index=False, escape=False),
                            unsafe_allow_html=True
                        )
                        st.caption(f"*调仓阈值为 {rb_result['threshold']*100:.0f}%，低于此偏离的行业不触发调仓。股数按整数估算，实际以交易为准。*")
                else:
                    st.success("当前行业权重分布合理，无需调仓")
            else:
                st.info("暂无持仓数据，无法生成再平衡建议")
        else:
            # 显示目标权重表格
            target_df = pd.DataFrame([
                {'行业': k, '目标权重': f"{v*100:.0f}%"}
                for k, v in default_targets.items()
            ])
            st.markdown(target_df.to_html(index=False, escape=False), unsafe_allow_html=True)


    # ========== 技术指标（增强版：点击持仓行查看详情） ==========
    st.markdown('<div class="tip-title" style="margin-top:20px;">🔍 技术指标信号<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示各ETF的技术指标信号概览，包括RSI超买超卖、MA均线信号和综合趋势判断(看多/看空/中性)。点击持仓表格中的ETF行可查看完整技术分析面板。</span></div>', unsafe_allow_html=True)
    if not technical.empty:
        st.info("💡 点击上方持仓表格中的任意ETF行，即可查看完整的技术分析详情面板（价格走势、RSI/MACD/KDJ指标、收益率分布等）。")
        # 全览信号卡片（精简版）
        trend_map = {'bullish': ('看多', '#22c55e'), 'bearish': ('看空', '#ef4444'),
                     'neutral': ('中性', '#f59e0b'), None: ('--', '#888')}
        tech_cols = st.columns(min(len(technical), 6))
        for idx, (_, row) in enumerate(technical.iterrows()):
            if idx >= 12:
                break
            with tech_cols[idx % len(tech_cols)]:
                trend_label, trend_color = trend_map.get(row.get('trend'), ('--', '#888'))
                st.markdown(
                    f'<div style="padding:8px;border-radius:6px;background:#161b22;'
                    f'border-left:3px solid {trend_color};margin-bottom:4px;">'
                    f'<div style="font-size:11px;color:#c9d1d9;font-weight:bold;white-space:nowrap;'
                    f'overflow:hidden;text-overflow:ellipsis;">{row.get("name", row.get("code", "未知"))}</div>'
                    f'<div style="font-size:11px;color:{trend_color};">{trend_label}</div>'
                    f'<div style="font-size:10px;color:#8b949e;">RSI: {row.get("rsi_value", 0):.1f} | MA: {row.get("ma_signal", "--")}</div>'
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

    # ========== 数据导出 ==========
    st.markdown("---")
    with st.expander("📥 数据导出", expanded=False):
        col_exp1, col_exp2, col_exp3 = st.columns(3)
        with col_exp1:
            if not positions.empty:
                href_pos, fname_pos = export_positions_csv(positions, f"持仓数据_{selected_date}")
                st.markdown(
                    f'<a href="{href_pos}" download="{fname_pos}" '
                    f'style="display:inline-block;padding:8px 16px;background:#21262d;color:#c9d1d9;'
                    f'border-radius:6px;text-decoration:none;font-size:13px;border:1px solid #30363d;">'
                    f'📋 导出持仓数据 (CSV)</a>',
                    unsafe_allow_html=True
                )
        with col_exp2:
            if not summary.empty:
                href_sum, fname_sum = export_summary_csv(summary, f"收益数据_{selected_date}")
                st.markdown(
                    f'<a href="{href_sum}" download="{fname_sum}" '
                    f'style="display:inline-block;padding:8px 16px;background:#21262d;color:#c9d1d9;'
                    f'border-radius:6px;text-decoration:none;font-size:13px;border:1px solid #30363d;">'
                    f'📈 导出收益数据 (CSV)</a>',
                    unsafe_allow_html=True
                )
        with col_exp3:
            if st.button("📸 导出 Dashboard 截图 (PNG)", key="screenshot_btn"):
                with st.spinner("正在截图，请稍候..."):
                    screenshot_path = capture_dashboard_screenshot(port=8501)
                if screenshot_path:
                    st.success(f"截图已保存: {screenshot_path}")
                    # 提供下载链接
                    with open(screenshot_path, "rb") as f:
                        img_bytes = f.read()
                    st.download_button(
                        label="📥 下载截图",
                        data=img_bytes,
                        file_name=f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                        mime="image/png",
                        key="download_screenshot"
                    )
                else:
                    st.error("截图失败，请确认 Dashboard 正在运行")

        # PDF 导出按钮（第4列，新行）
        st.markdown("<br>", unsafe_allow_html=True)
        col_exp4 = st.columns([1, 1, 1])[1]
        with col_exp4:
            if st.button("📄 导出 Dashboard 报告 (PDF)", key="pdf_btn"):
                with st.spinner("正在生成 PDF，请稍候..."):
                    pdf_path = export_dashboard_pdf(port=8501)
                if pdf_path:
                    st.success(f"PDF 已生成: {pdf_path}")
                    with open(pdf_path, "rb") as f:
                        pdf_bytes = f.read()
                    st.download_button(
                        label="📥 下载 PDF 报告",
                        data=pdf_bytes,
                        file_name=f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                        mime="application/pdf",
                        key="download_pdf"
                    )
                else:
                    st.error("PDF 导出失败，请确认 Dashboard 正在运行")

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