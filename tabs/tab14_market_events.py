"""
Tab14: 市场事件监控面板
展示龙虎榜、融资融券、股东增减持、机构调研、大宗交易数据。
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from config.settings import DATABASE_PATH


# ============================================================
#  数据查询
# ============================================================

@st.cache_data(ttl=600)
def _load_market_events(table: str, days: int = 30) -> pd.DataFrame:
    """从数据库加载指定表最近N天的数据"""
    import sqlite3
    conn = sqlite3.connect(str(DATABASE_PATH))
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        df = pd.read_sql_query(
            f"SELECT * FROM {table} WHERE date >= ? ORDER BY date DESC, id DESC",
            conn, params=(cutoff,)
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@st.cache_data(ttl=600)
def _load_date_list(table: str) -> list:
    """获取指定表的可用日期列表"""
    import sqlite3
    conn = sqlite3.connect(str(DATABASE_PATH))
    try:
        df = pd.read_sql_query(
            f"SELECT DISTINCT date FROM {table} ORDER BY date DESC", conn
        )
        dates = df['date'].tolist()
    except Exception:
        dates = []
    conn.close()
    return dates


# ============================================================
#  子面板: 龙虎榜
# ============================================================

def _render_lhb_panel():
    st.markdown("#### 龙虎榜明细")
    
    dates = _load_date_list("stock_lhb")
    if not dates:
        st.info("暂无龙虎榜数据")
        return
    
    col1, col2 = st.columns([1, 3])
    with col1:
        sel_date = st.selectbox("选择日期", dates, key="lhb_date")
    with col2:
        st.caption(f"共 {len(dates)} 个交易日有数据")
    
    df = _load_market_events("stock_lhb", 90)
    if df.empty:
        st.info("所选日期无数据")
        return
    
    day_df = df[df['date'] == sel_date].copy()
    
    # 统计卡片
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("上榜家数", len(day_df))
    with c2:
        net_buy = day_df['lhb_net_buy'].sum() / 1e8 if day_df['lhb_net_buy'].notna().any() else 0
        st.metric("净买入(亿)", f"{net_buy:.2f}")
    with c3:
        top_net = day_df.nlargest(1, 'lhb_net_buy')['lhb_net_buy'].iloc[0] / 1e4 if len(day_df) > 0 else 0
        st.metric("最大净买(万)", f"{top_net:.0f}")
    with c4:
        reasons = day_df['reason'].value_counts()
        st.metric("上榜原因数", len(reasons))
    
    # 数据表
    show_cols = ['code', 'name', 'close', 'change_pct', 'lhb_net_buy',
                 'lhb_buy_amount', 'lhb_sell_amount', 'net_buy_ratio', 'reason']
    show_cols = [c for c in show_cols if c in day_df.columns]
    display_df = day_df[show_cols].copy()
    display_df['lhb_net_buy'] = display_df['lhb_net_buy'].apply(lambda x: f"{x/1e4:.0f}万" if pd.notna(x) else "")
    display_df['lhb_buy_amount'] = display_df['lhb_buy_amount'].apply(lambda x: f"{x/1e4:.0f}万" if pd.notna(x) else "")
    display_df['lhb_sell_amount'] = display_df['lhb_sell_amount'].apply(lambda x: f"{x/1e4:.0f}万" if pd.notna(x) else "")
    if 'change_pct' in display_df.columns:
        display_df['change_pct'] = display_df['change_pct'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
    
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)


# ============================================================
#  子面板: 融资融券
# ============================================================

def _render_margin_panel():
    st.markdown("#### 融资融券（上交所）")
    
    dates = _load_date_list("stock_margin")
    if not dates:
        st.info("暂无融资融券数据")
        return
    
    col1, col2 = st.columns([1, 3])
    with col1:
        sel_date = st.selectbox("选择日期", dates, key="margin_date")
    with col2:
        search = st.text_input("搜索代码/名称", key="margin_search", placeholder="输入代码或名称过滤")
    
    df = _load_market_events("stock_margin", 30)
    if df.empty:
        st.info("所选日期无数据")
        return
    
    day_df = df[df['date'] == sel_date].copy()
    
    # 搜索过滤
    if search:
        mask = day_df['code'].str.contains(search, case=False, na=False) | \
               day_df['name'].str.contains(search, case=False, na=False)
        day_df = day_df[mask]
    
    # 统计卡片
    c1, c2, c3 = st.columns(3)
    with c1:
        total_balance = day_df['margin_balance'].sum() / 1e8 if day_df['margin_balance'].notna().any() else 0
        st.metric("融资余额(亿)", f"{total_balance:.2f}")
    with c2:
        total_buy = day_df['margin_buy'].sum() / 1e8 if day_df['margin_buy'].notna().any() else 0
        st.metric("融资买入(亿)", f"{total_buy:.2f}")
    with c3:
        total_short = day_df['short_volume'].sum() / 1e8 if day_df['short_volume'].notna().any() else 0
        st.metric("融券余量(万)", f"{total_short:.0f}")
    
    # 数据表
    show_cols = ['code', 'name', 'margin_balance', 'margin_buy', 'margin_repay',
                 'short_volume', 'short_sell', 'short_repay']
    show_cols = [c for c in show_cols if c in day_df.columns]
    display_df = day_df[show_cols].copy()
    for c in ['margin_balance', 'margin_buy', 'margin_repay']:
        if c in display_df.columns:
            display_df[c] = display_df[c].apply(lambda x: f"{x/1e8:.2f}亿" if pd.notna(x) else "")
    for c in ['short_volume', 'short_sell', 'short_repay']:
        if c in display_df.columns:
            display_df[c] = display_df[c].apply(lambda x: f"{x/1e4:.0f}万" if pd.notna(x) else "")
    
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)


# ============================================================
#  子面板: 股东增减持
# ============================================================

def _render_holder_change_panel():
    st.markdown("#### 股东增减持")
    
    df = _load_market_events("stock_holder_change", 30)
    if df.empty:
        st.info("暂无股东增减持数据")
        return
    
    # 日期筛选
    dates = sorted(df['date'].unique(), reverse=True)
    col1, col2 = st.columns([1, 3])
    with col1:
        sel_date = st.selectbox("选择日期", dates, key="holder_date")
    
    day_df = df[df['date'] == sel_date].copy()
    
    # 统计
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("变动记录", len(day_df))
    with c2:
        increase = len(day_df[day_df['qty_change'].fillna(0) > 0])
        st.metric("增持", increase)
    with c3:
        decrease = len(day_df[day_df['qty_change'].fillna(0) < 0])
        st.metric("减持", decrease)
    
    # 数据表
    show_cols = ['code', 'name', 'holder_name', 'holder_type', 'qty_change',
                 'qty_change_pct', 'change_type', 'float_mv']
    show_cols = [c for c in show_cols if c in day_df.columns]
    display_df = day_df[show_cols].copy()
    if 'qty_change' in display_df.columns:
        display_df['qty_change'] = display_df['qty_change'].apply(
            lambda x: f"{x/1e4:.0f}万" if pd.notna(x) else "")
    if 'qty_change_pct' in display_df.columns:
        display_df['qty_change_pct'] = display_df['qty_change_pct'].apply(
            lambda x: f"{x:.2f}%" if pd.notna(x) else "")
    if 'float_mv' in display_df.columns:
        display_df['float_mv'] = display_df['float_mv'].apply(
            lambda x: f"{x/1e8:.1f}亿" if pd.notna(x) else "")
    
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)


# ============================================================
#  子面板: 机构调研
# ============================================================

def _render_institution_panel():
    st.markdown("#### 机构调研")
    
    df = _load_market_events("stock_institution_research", 30)
    if df.empty:
        st.info("暂无机构调研数据")
        return
    
    dates = sorted(df['date'].unique(), reverse=True)
    col1, col2 = st.columns([1, 3])
    with col1:
        sel_date = st.selectbox("选择日期", dates, key="jgdy_date")
    
    day_df = df[df['date'] == sel_date].copy()
    
    # 统计卡片
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("调研记录", len(day_df))
    with c2:
        unique_codes = day_df['code'].nunique() if 'code' in day_df.columns else 0
        st.metric("被调研公司", unique_codes)
    with c3:
        unique_inst = day_df['institution'].nunique() if 'institution' in day_df.columns else 0
        st.metric("调研机构", unique_inst)
    
    # 被调研公司统计
    if 'code' in day_df.columns and 'name' in day_df.columns:
        code_stats = day_df.groupby(['code', 'name']).size().reset_index(name='调研次数')
        code_stats = code_stats.sort_values('调研次数', ascending=False)
        
        fig = go.Figure(go.Bar(
            x=code_stats['调研次数'].values,
            y=code_stats['name'].values,
            orientation='h',
            marker_color='#1f6feb',
            text=code_stats['调研次数'].values,
            textposition='auto',
        ))
        fig.update_layout(
            height=min(300, max(150, len(code_stats) * 30)),
            margin=dict(l=120, r=20, t=10, b=10),
            xaxis_title="调研次数",
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # 数据表
    show_cols = ['code', 'name', 'price', 'change_pct', 'institution',
                 'inst_type', 'receive_method', 'research_date']
    show_cols = [c for c in show_cols if c in day_df.columns]
    display_df = day_df[show_cols].copy()
    if 'change_pct' in display_df.columns:
        display_df['change_pct'] = display_df['change_pct'].apply(
            lambda x: f"{x:.2f}%" if pd.notna(x) else "")
    
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=350)


# ============================================================
#  子面板: 大宗交易
# ============================================================

def _render_block_trade_panel():
    st.markdown("#### 大宗交易")
    
    dates = _load_date_list("stock_block_trade")
    if not dates:
        st.info("暂无大宗交易数据")
        return
    
    col1, col2 = st.columns([1, 3])
    with col1:
        sel_date = st.selectbox("选择日期", dates, key="block_date")
    
    df = _load_market_events("stock_block_trade", 30)
    if df.empty:
        st.info("所选日期无数据")
        return
    
    day_df = df[df['date'] == sel_date].copy()
    
    # 统计卡片
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("成交笔数", len(day_df))
    with c2:
        total_amount = day_df['amount'].sum() / 1e8 if day_df['amount'].notna().any() else 0
        st.metric("成交总额(亿)", f"{total_amount:.2f}")
    with c3:
        total_vol = day_df['volume'].sum() / 1e4 if day_df['volume'].notna().any() else 0
        st.metric("成交量(万)", f"{total_vol:.0f}")
    with c4:
        avg_premium = day_df['premium_rate'].mean()
        st.metric("平均折溢率", f"{avg_premium:.2f}%" if pd.notna(avg_premium) else "N/A")
    
    # 折溢价分布图
    if 'premium_rate' in day_df.columns and len(day_df) > 0:
        prem = day_df['premium_rate'].dropna()
        if len(prem) > 0:
            fig = go.Figure(go.Histogram(
                x=prem, nbinsx=20,
                marker_color='#1f6feb',
                opacity=0.8,
            ))
            fig.add_vline(x=0, line_dash="dash", line_color="#f85149", line_width=1.5)
            fig.update_layout(
                height=200,
                margin=dict(l=40, r=20, t=10, b=30),
                xaxis_title="折溢价率(%)",
                yaxis_title="笔数",
                bargap=0.05,
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # 数据表
    show_cols = ['code', 'name', 'close', 'trade_price', 'premium_rate',
                 'volume', 'amount', 'buyer_broker', 'seller_broker']
    show_cols = [c for c in show_cols if c in day_df.columns]
    display_df = day_df[show_cols].copy()
    if 'premium_rate' in display_df.columns:
        display_df['premium_rate'] = display_df['premium_rate'].apply(
            lambda x: f"{x:.2f}%" if pd.notna(x) else "")
    if 'amount' in display_df.columns:
        display_df['amount'] = display_df['amount'].apply(
            lambda x: f"{x/1e4:.0f}万" if pd.notna(x) else "")
    if 'volume' in display_df.columns:
        display_df['volume'] = display_df['volume'].apply(
            lambda x: f"{x/1e4:.0f}万" if pd.notna(x) else "")
    
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)


# ============================================================
#  主渲染函数
# ============================================================

def render_tab14(**kwargs):
    """渲染Tab14: 市场事件监控"""
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '市场事件监控'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '龙虎榜、融资融券、股东增减持、机构调研、大宗交易等市场事件数据。</span></div>',
        unsafe_allow_html=True,
    )

    if st.button("\U0001f504 刷新数据", key="me_refresh"):
        st.cache_data.clear()

    sub_tabs = st.tabs([
        "🐉 龙虎榜",
        "💹 融资融券",
        "👥 股东增减持",
        "🏢 机构调研",
        "📦 大宗交易",
    ])

    with sub_tabs[0]:
        _render_lhb_panel()
    with sub_tabs[1]:
        _render_margin_panel()
    with sub_tabs[2]:
        _render_holder_change_panel()
    with sub_tabs[3]:
        _render_institution_panel()
    with sub_tabs[4]:
        _render_block_trade_panel()
