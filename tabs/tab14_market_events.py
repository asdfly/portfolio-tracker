"""
Tab14: 市场事件监控面板
展示龙虎榜、融资融券、股东增减持、机构调研、大宗交易数据。
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
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
    
    # 深度分析: 近30日上榜频率TOP10
    st.markdown("**近30日上榜频率 TOP10**")
    freq = df.groupby(['code', 'name']).size().reset_index(name='上榜次数')
    freq = freq.sort_values('上榜次数', ascending=False).head(10)
    if not freq.empty:
        fig_freq = go.Figure(go.Bar(
            x=freq['上榜次数'].values, y=freq['name'].values,
            orientation='h', marker_color='#1f6feb',
            text=freq['上榜次数'].values, textposition='auto',
        ))
        fig_freq.update_layout(height=max(200, len(freq)*30), margin=dict(l=120, r=20, t=5, b=10),
                               xaxis_title="上榜次数", yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_freq, width='stretch')

    # 深度分析: 净买入金额趋势(近30日合计)
    if 'lhb_net_buy' in df.columns and not df.empty:
        st.markdown("**净买入金额趋势**")
        daily_net = df.groupby('date')['lhb_net_buy'].sum().reset_index()
        daily_net = daily_net.sort_values('date')
        colors = ['#22c55e' if v >= 0 else '#ef4444' for v in daily_net['lhb_net_buy']]
        fig_trend = go.Figure(go.Bar(x=daily_net['date'], y=daily_net['lhb_net_buy']/1e8,
                                     marker_color=colors, opacity=0.8))
        fig_trend.update_layout(height=220, margin=dict(l=40, r=20, t=5, b=30),
                                xaxis_title="", yaxis_title="净买入(亿)", bargap=0.3)
        st.plotly_chart(fig_trend, width='stretch')

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
    
    # 深度分析: 融资余额TOP10
    st.markdown("**融资余额 TOP10**")
    if 'margin_balance' in df.columns and not df.empty:
        latest_date = df['date'].max()
        latest_df = df[df['date'] == latest_date]
        top10 = latest_df.nlargest(10, 'margin_balance')
        if not top10.empty:
            fig_top = go.Figure(go.Bar(
                x=top10['margin_balance']/1e8, y=top10['name'].values,
                orientation='h', marker_color='#f59e0b',
                text=[f"{v/1e8:.1f}亿" for v in top10['margin_balance'].values],
                textposition='auto',
            ))
            fig_top.update_layout(height=max(200, len(top10)*30), margin=dict(l=120, r=20, t=5, b=10),
                                  xaxis_title="融资余额(亿)", yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_top, width='stretch')

    # 深度分析: 融资余额日变化趋势
    if 'margin_balance' in df.columns and not df.empty:
        st.markdown("**融资余额变化趋势**")
        daily_total = df.groupby('date').agg(
            total_balance=('margin_balance', 'sum'),
            total_buy=('margin_buy', 'sum'),
            stock_count=('code', 'nunique')
        ).reset_index().sort_values('date')
        fig_mt = go.Figure()
        fig_mt.add_trace(go.Scatter(x=daily_total['date'], y=daily_total['total_balance']/1e8,
                                     mode='lines+markers', name='融资余额合计(亿)',
                                     line=dict(color='#58a6ff', width=2), marker=dict(size=4)))
        fig_mt.update_layout(height=220, margin=dict(l=40, r=20, t=5, b=30),
                             xaxis_title="", yaxis_title="融资余额(亿)")
        st.plotly_chart(fig_mt, width='stretch')

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
    
    # 深度分析: 增减持规模趋势
    st.markdown("**增减持规模趋势**")
    if not df.empty and 'qty_change' in df.columns:
        daily_chg = df.groupby('date').agg(
            increase_vol=('qty_change', lambda x: x[x > 0].sum()),
            decrease_vol=('qty_change', lambda x: x[x < 0].sum()),
            record_count=('qty_change', 'count')
        ).reset_index().sort_values('date')
        fig_hc = go.Figure()
        fig_hc.add_trace(go.Bar(x=daily_chg['date'], y=daily_chg['increase_vol']/1e4,
                                name='增持(万股)', marker_color='#22c55e', opacity=0.8))
        fig_hc.add_trace(go.Bar(x=daily_chg['date'], y=daily_chg['decrease_vol'].abs()/1e4,
                                name='减持(万股)', marker_color='#ef4444', opacity=0.8))
        fig_hc.update_layout(height=220, margin=dict(l=40, r=20, t=5, b=30),
                             xaxis_title="", yaxis_title="变动量(万股)", barmode='relative')
        st.plotly_chart(fig_hc, width='stretch')

    # 深度分析: 减持规模TOP10
    st.markdown("**减持规模 TOP10**")
    if not df.empty and 'qty_change' in df.columns:
        dec = df[df['qty_change'].fillna(0) < 0].copy()
        if not dec.empty:
            dec['abs_qty'] = dec['qty_change'].abs()
            top_dec = dec.groupby(['code', 'name'])['abs_qty'].sum().nlargest(10).reset_index()
            fig_dec = go.Figure(go.Bar(
                x=top_dec['abs_qty']/1e4, y=top_dec['name'].values,
                orientation='h', marker_color='#ef4444',
                text=[f"{v/1e4:.0f}万" for v in top_dec['abs_qty'].values],
                textposition='auto',
            ))
            fig_dec.update_layout(height=max(200, len(top_dec)*30), margin=dict(l=120, r=20, t=5, b=10),
                                  xaxis_title="减持量(万股)", yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_dec, width='stretch')

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
    
    # 深度分析: 调研热度趋势(被调研公司数)
    st.markdown("**调研热度趋势**")
    if not df.empty:
        daily_stats = df.groupby('date').agg(
            companies=('code', 'nunique'),
            institutions=('institution', 'nunique'),
            records=('id', 'count') if 'id' in df.columns else ('code', 'count')
        ).reset_index().sort_values('date')
        fig_jt = make_subplots(specs=[[{"secondary_y": True}]])
        fig_jt.add_trace(go.Bar(x=daily_stats['date'], y=daily_stats['companies'],
                                name='被调研公司', marker_color='#1f6feb', opacity=0.7),
                         secondary_y=False)
        fig_jt.add_trace(go.Scatter(x=daily_stats['date'], y=daily_stats['institutions'],
                                    mode='lines+markers', name='调研机构数',
                                    line=dict(color='#f59e0b', width=2), marker=dict(size=4)),
                         secondary_y=True)
        fig_jt.update_layout(height=220, margin=dict(l=40, r=40, t=5, b=30),
                             legend=dict(orientation="h", yanchor="bottom", y=1.02))
        fig_jt.update_yaxes(title_text="公司数", secondary_y=False)
        fig_jt.update_yaxes(title_text="机构数", secondary_y=True)
        st.plotly_chart(fig_jt, width='stretch')

    # 深度分析: 机构类型分布
    if not df.empty and 'inst_type' in df.columns:
        type_dist = df['inst_type'].dropna().value_counts().head(8)
        if not type_dist.empty:
            fig_pie = go.Figure(go.Pie(labels=type_dist.index, values=type_dist.values,
                                        hole=0.4, textinfo='label+percent',
                                        marker_colors=px.colors.qualitative.Set2[:len(type_dist)]))
            fig_pie.update_layout(height=250, margin=dict(l=20, r=20, t=5, b=5),
                                  legend=dict(font=dict(size=10), orientation="h", yanchor="bottom", y=-0.1))
            st.plotly_chart(fig_pie, width='stretch')

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
    
    # 深度分析: 买方营业部TOP10（按成交额）
    st.markdown("**买方营业部 TOP10（按成交额）**")
    if not df.empty and 'buyer_broker' in df.columns and 'amount' in df.columns:
        broker_buy = df.groupby('buyer_broker')['amount'].sum().nlargest(10).reset_index()
        fig_broker = go.Figure(go.Bar(
            x=broker_buy['amount']/1e8, y=broker_buy['buyer_broker'].apply(lambda x: x[:20] if len(x)>20 else x).values,
            orientation='h', marker_color='#8b5cf6',
            text=[f"{v/1e8:.1f}亿" for v in broker_buy['amount'].values],
            textposition='auto',
        ))
        fig_broker.update_layout(height=max(200, len(broker_buy)*30),
                                 margin=dict(l=200, r=20, t=5, b=10),
                                 xaxis_title="成交额(亿)", yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_broker, width='stretch')

    # 深度分析: 日成交额趋势
    if not df.empty and 'amount' in df.columns:
        st.markdown("**成交额趋势**")
        daily_amt = df.groupby('date').agg(
            total_amount=('amount', 'sum'),
            trade_count=('code', 'count'),
            avg_premium=('premium_rate', 'mean')
        ).reset_index().sort_values('date')
        fig_bt = make_subplots(specs=[[{"secondary_y": True}]])
        fig_bt.add_trace(go.Bar(x=daily_amt['date'], y=daily_amt['total_amount']/1e8,
                                name='成交额(亿)', marker_color='#58a6ff', opacity=0.7),
                         secondary_y=False)
        fig_bt.add_trace(go.Scatter(x=daily_amt['date'], y=daily_amt['avg_premium'],
                                    mode='lines+markers', name='平均折溢率(%)',
                                    line=dict(color='#f85149', width=2), marker=dict(size=4)),
                         secondary_y=True)
        fig_bt.update_layout(height=220, margin=dict(l=40, r=40, t=5, b=30),
                             legend=dict(orientation="h", yanchor="bottom", y=1.02))
        fig_bt.update_yaxes(title_text="成交额(亿)", secondary_y=False)
        fig_bt.update_yaxes(title_text="折溢率(%)", secondary_y=True)
        st.plotly_chart(fig_bt, width='stretch')

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
