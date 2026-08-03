from components.ui import render_chart, render_empty_state
"""
Tab15: 交易复盘 — 买卖胜率 / 定投追踪 / 交易成本
"""

import streamlit as st
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection


# ==================== 数据查询（纯函数）====================

def load_trade_records():
    """加载全部交易流水"""
    conn = get_db_connection()
    df = pd.read_sql_query("""
        SELECT date, market, code, name, action, quantity, price,
               amount, commission, stamp_tax, change_amount
        FROM trade_records ORDER BY date
    """, conn)
    conn.close()
    return df


def load_portfolio_snapshots():
    """加载全部持仓快照"""
    conn = get_db_connection()
    df = pd.read_sql_query("""
        SELECT date, code, name, quantity, cost_price, current_price, market_value
        FROM portfolio_snapshots ORDER BY date, code
    """, conn)
    conn.close()
    return df


def calc_etf_trade_pnl(df_trades):
    """场内 ETF 买卖配对盈亏（FIFO）"""
    buys = df_trades[df_trades['action'] == '证券买入'].copy()
    sells = df_trades[df_trades['action'] == '证券卖出'].copy()
    if buys.empty or sells.empty:
        return pd.DataFrame()

    results = []
    for code in sells['code'].unique():
        code_buys = buys[buys['code'] == code].sort_values('date')
        code_sells = sells[sells['code'] == code].sort_values('date')
        buy_queue = []
        for _, buy in code_buys.iterrows():
            buy_queue.append((buy['price'], abs(buy['quantity'])))
        for _, sell in code_sells.iterrows():
            remaining = abs(sell['quantity'])
            while remaining > 0.01 and buy_queue:
                bp, bq = buy_queue[0]
                match_qty = min(bq, remaining)
                buy_cost = bp * match_qty
                sell_revenue = sell['price'] * match_qty
                fee = (sell['commission'] + sell['stamp_tax']) * (match_qty / abs(sell['quantity']))
                net_pnl = sell_revenue - buy_cost - fee
                pnl_rate = (net_pnl / buy_cost * 100) if buy_cost > 0 else 0
                results.append({
                    'code': code, 'name': sell['name'],
                    'buy_price': round(bp, 4), 'sell_price': round(sell['price'], 4),
                    'quantity': match_qty,
                    'gross_pnl': round(sell_revenue - buy_cost, 2),
                    'fee': round(fee, 2),
                    'net_pnl': round(net_pnl, 2),
                    'pnl_rate': round(pnl_rate, 2),
                    'sell_date': sell['date'],
                })
                buy_queue[0] = (bp, bq - match_qty)
                if buy_queue[0][1] <= 0.01:
                    buy_queue.pop(0)
                remaining -= match_qty

    return pd.DataFrame(results) if results else pd.DataFrame()


def calc_dca_tracking(df_trades, df_snapshots):
    """定投基金追踪：累计投入、当前市值、收益率

    投入成本使用隐含总成本(持仓量 × 成本价)而非仅定投金额，
    因为当前市值包含定投+手动申购两部分资金。"""
    dca = df_trades[df_trades['action'] == '产品定时定额投资确认'].copy()
    if dca.empty:
        return pd.DataFrame()
    # 手动申购记录（产品申购确认）
    manual = df_trades[df_trades['action'] == '产品申购确认'].copy()
    # 红利记录
    dividends = df_trades[df_trades['action'] == '产品红利发放'].copy()

    latest_date = df_snapshots['date'].max()
    latest_snap = df_snapshots[df_snapshots['date'] == latest_date]
    results = []
    for code in dca['code'].unique():
        cd = dca[dca['code'] == code].sort_values('date')
        dca_amount = abs(cd['amount'].sum())
        # 手动申购金额
        m = manual[manual['code'] == code]
        manual_amount = abs(m['amount'].sum()) if not m.empty else 0
        # 红利收入
        div = dividends[dividends['code'] == code]
        div_income = div['change_amount'].sum() if not div.empty else 0
        # 交易记录净投入（所有流出金额之和）
        code_trades = df_trades[df_trades['code'] == code]
        trade_net_invest = abs(code_trades[code_trades['change_amount'] < 0]['change_amount'].sum())

        snap = latest_snap[latest_snap['code'] == code]
        # 如果最新快照不含该基金（如8/03仅有场内ETF），回退到包含该基金的最近快照
        if snap.empty:
            code_snaps = df_snapshots[df_snapshots['code'] == code].sort_values('date', ascending=False)
            if not code_snaps.empty:
                snap = code_snaps.iloc[[0]]
        mv = snap['market_value'].values[0] if not snap.empty else 0
        qty = snap['quantity'].values[0] if not snap.empty else 0
        cost_price = snap['cost_price'].values[0] if not snap.empty else 0
        # 隐含总成本 = 持仓量 × 成本价
        implied_cost = qty * cost_price if qty > 0 else 0
        # 使用隐含总成本作为投入基准（比交易记录更准确，尤其对记录缺失的基金）
        # 直接使用隐含总成本作为投入基准（当前持仓的真实成本，不受赎回/记录缺失影响）
        invested = implied_cost if implied_cost > 0 else trade_net_invest

        profit = mv - invested
        rate = (profit / invested * 100) if invested > 0 else 0
        # 赎回记录
        redeems = df_trades[df_trades['action'] == '产品赎回确认']
        r = redeems[redeems['code'] == code]
        redeem_net = (r['change_amount'].sum() - r['commission'].sum()) if not r.empty else 0

        # 全生命周期收益 = 赎回到账 + 红利 + 当前市值 - 全部流出(定投+申购)
        total_flow_out = dca_amount + manual_amount
        lifecycle_pnl = redeem_net + div_income + mv - total_flow_out
        lifecycle_rate = (lifecycle_pnl / total_flow_out * 100) if total_flow_out > 0 else 0

        results.append({
            'code': code, 'name': cd['name'].iloc[0],
            'dca_count': len(cd), 'first_date': cd['date'].iloc[0],
            'last_date': cd['date'].iloc[-1],
            'dca_amount': round(dca_amount, 2),
            'manual_amount': round(manual_amount, 2),
            'div_income': round(div_income, 2),
            'redeem_net': round(redeem_net, 2),
            'total_invested': round(invested, 2),
            'implied_cost': round(implied_cost, 2),
            'current_mv': round(mv, 2),
            'profit': round(profit, 2),
            'profit_rate': round(rate, 2),
            'lifecycle_pnl': round(lifecycle_pnl, 2),
            'lifecycle_rate': round(lifecycle_rate, 2),
            'current_qty': round(qty, 2),
        })
    return pd.DataFrame(results).sort_values('total_invested', ascending=False).reset_index(drop=True)


def calc_trade_cost_summary(df_trades):
    """交易成本汇总：按月、按证券"""
    df = df_trades[(df_trades['commission'] > 0) | (df_trades['stamp_tax'] > 0)].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), {}
    df['month'] = df['date'].str[:7]
    df['total_fee'] = df['commission'] + df['stamp_tax']
    monthly = df.groupby('month').agg(
        trade_count=('date', 'count'),
        total_commission=('commission', 'sum'),
        total_stamp_tax=('stamp_tax', 'sum'),
        total_fee=('total_fee', 'sum'),
        total_amount=('amount', 'sum'),
    ).reset_index()
    monthly['fee_rate'] = np.where(
        monthly['total_amount'].abs() > 0,
        monthly['total_fee'] / monthly['total_amount'].abs() * 100, 0
    ).round(4)
    by_code = df.groupby(['code', 'name']).agg(
        trade_count=('date', 'count'),
        total_commission=('commission', 'sum'),
        total_stamp_tax=('stamp_tax', 'sum'),
        total_fee=('total_fee', 'sum'),
    ).reset_index().sort_values('total_fee', ascending=False)
    total_row = {
        'trade_count': len(df),
        'total_commission': df['commission'].sum(),
        'total_stamp_tax': df['stamp_tax'].sum(),
        'total_fee': df['total_fee'].sum(),
    }
    return monthly, by_code, total_row


def calc_monthly_cashflow(df_trades):
    """月度资金流向 pivot

    将天添利(880013, 货币基金)和银行转存从投资流中分离:
    - 投资流: 买入/卖出/定投/基金申购/基金赎回/股息/红利/质押
    - 现金管理: 银行转存/天添利净流
    - 汇总: 净投资流(不含现金管理), 净现金流(全部)
    """
    df = df_trades.copy()
    df['month'] = df['date'].str[:7]

    # 天添利(880013) 单独归类, 其余按 action 映射
    TTL_CODE = '880013'
    action_map = {
        '银行转存': '银行转存',
        '产品申购确认': '基金申购', '产品定时定额投资确认': '定投',
        '产品赎回确认': '基金赎回', '证券买入': '买入',
        '证券卖出': '卖出', '股息入账': '股息',
        '产品红利发放': '红利', '拆出质押购回': '质押',
    }
    df['flow_type'] = df['action'].map(action_map).fillna(df['action'])
    # 天添利的申购/赎回/红利 → 统一归入 "天添利净流"
    ttl_mask = df['code'] == TTL_CODE
    df.loc[ttl_mask, 'flow_type'] = '天添利净流'

    pivot = df.pivot_table(index='month', columns='flow_type',
                           values='change_amount', aggfunc='sum', fill_value=0)

    # 列排序: 投资流在前, 现金管理在后
    invest_cols = ['买入', '卖出', '定投', '基金申购', '基金赎回',
                   '股息', '红利', '质押']
    cash_cols = ['银行转存', '天添利净流']
    # 报价回购拆出 / 质押回购拆出 等罕见类型归入投资流尾部
    other_cols = [c for c in pivot.columns
                  if c not in invest_cols and c not in cash_cols]
    ordered_cols = [c for c in invest_cols if c in pivot.columns]         + other_cols         + [c for c in cash_cols if c in pivot.columns]
    pivot = pivot.reindex(columns=ordered_cols, fill_value=0)

    # 汇总列
    invest_present = [c for c in invest_cols + other_cols if c in pivot.columns]
    cash_present = [c for c in cash_cols if c in pivot.columns]
    pivot['净投资流'] = pivot[invest_present].sum(axis=1) if invest_present else 0
    pivot['净现金流'] = pivot[invest_present + cash_present].sum(axis=1)

    return pivot.reset_index()


# ==================== 渲染子函数 ====================

def _render_etf_pnl_section(df_trades):
    """场内 ETF 买卖胜率"""
    import plotly.graph_objects as go
    st.subheader("场内 ETF 买卖配对盈亏")
    pnl_df = calc_etf_trade_pnl(df_trades)
    if pnl_df.empty:
        render_empty_state("暂无场内交易记录")
        return
    total = len(pnl_df)
    win = len(pnl_df[pnl_df['net_pnl'] > 0])
    win_rate = win / total * 100
    sum_pnl = pnl_df['net_pnl'].sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("配对交易", f"{total} 笔")
    c2.metric("胜率", f"{win_rate:.1f}%", delta=f"盈{win}/亏{total - win}")
    c3.metric("总盈亏", f"¥{sum_pnl:,.0f}")
    c4.metric("平均盈亏", f"¥{pnl_df['net_pnl'].mean():,.0f}/笔")

    by_code = pnl_df.groupby(['code', 'name']).agg(
        n=('net_pnl', 'count'),
        total_pnl=('net_pnl', 'sum'),
        avg_rate=('pnl_rate', 'mean'),
    ).reset_index().sort_values('total_pnl', ascending=False)
    by_code.columns = ['代码', '名称', '配对数', '总盈亏(¥)', '平均收益率(%)']
    by_code['总盈亏(¥)'] = by_code['总盈亏(¥)'].round(0)
    by_code['平均收益率(%)'] = by_code['平均收益率(%)'].round(1)
    st.dataframe(by_code.reset_index(drop=True), use_container_width=True, hide_index=True)

    colors = ['#28a745' if x > 0 else '#dc3545' for x in by_code['总盈亏(¥)']]
    fig = go.Figure(go.Bar(x=by_code['名称'], y=by_code['总盈亏(¥)'],
                           marker_color=colors,
                           text=[f"¥{v:,.0f}" for v in by_code['总盈亏(¥)']],
                           textposition='auto'))
    fig.update_layout(title="各 ETF 配对盈亏", height=400,
                      xaxis_title="", yaxis_title="盈亏(¥)",
                      margin=dict(l=40, r=20, t=40, b=120))
    render_chart(fig)


def _render_dca_section(df_trades, df_snapshots):
    """定投收益追踪"""
    import plotly.graph_objects as go
    st.subheader("定投基金收益追踪")
    dca_df = calc_dca_tracking(df_trades, df_snapshots)
    if dca_df.empty:
        render_empty_state("暂无定投记录")
        return
    total_inv = dca_df['total_invested'].sum()
    total_mv = dca_df['current_mv'].sum()
    total_p = dca_df['profit'].sum()
    total_dca = dca_df['dca_amount'].sum()
    total_manual = dca_df['manual_amount'].sum()
    ovr = (total_p / total_inv * 100) if total_inv > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("定投基金", f"{len(dca_df)} 只")
    c2.metric("总投入", f"¥{total_inv:,.0f}", delta=f"定投{total_dca:,.0f}+手动{total_manual:,.0f}")
    c3.metric("当前市值", f"¥{total_mv:,.0f}")
    c4.metric("总收益", f"¥{total_p:,.0f}", delta=f"{ovr:.1f}%")

    total_lc = dca_df['lifecycle_pnl'].sum()
    total_lc_inv = (dca_df['dca_amount'] + dca_df['manual_amount']).sum()
    total_lc_rate = (total_lc / total_lc_inv * 100) if total_lc_inv > 0 else 0
    total_redeem = dca_df['redeem_net'].sum()
    total_div = dca_df['div_income'].sum()

    st.caption("投入成本 = 持仓量 × 成本价（隐含总成本）；全生命周期 = 含赎回+红利，基于交易记录净流入")

    display = dca_df[['code', 'name', 'dca_count', 'dca_amount', 'manual_amount',
                       'total_invested', 'current_mv', 'profit', 'profit_rate',
                       'redeem_net', 'div_income', 'lifecycle_pnl', 'lifecycle_rate']].copy()
    display.columns = ['代码', '名称', '定投次数', '定投金额', '手动申购',
                        '持仓投入(隐含)', '当前市值', '持仓收益', '持仓收益率%',
                        '赎回到账', '红利', '全周期收益', '全周期收益率%']
    for col in ['定投金额', '手动申购', '持仓投入(隐含)', '当前市值', '持仓收益', '赎回到账', '红利', '全周期收益']:
        display[col] = display[col].round(0)
    for col in ['持仓收益率%', '全周期收益率%']:
        display[col] = display[col].round(1)
    st.dataframe(display.reset_index(drop=True), use_container_width=True, hide_index=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(name='持仓投入(隐含成本)', x=dca_df['name'], y=dca_df['total_invested'],
                         marker_color='#4a90d9'))
    fig.add_trace(go.Bar(name='当前市值', x=dca_df['name'], y=dca_df['current_mv'],
                         marker_color='#28a745'))
    fig.add_trace(go.Bar(name='全周期收益', x=dca_df['name'], y=dca_df['lifecycle_pnl'],
                         marker_color='#ffc107'))
    fig.update_layout(barmode='group', title="定投基金：持仓投入 vs 市值 vs 全周期收益",
                      height=400, xaxis_title="", yaxis_title="金额(¥)",
                      margin=dict(l=40, r=20, t=40, b=100))
    render_chart(fig)

    # 全周期汇总卡片
    if total_redeem > 0 or total_div > 0:
        st.markdown("---")
        c1, c2, c3 = st.columns(3)
        c1.metric("全周期总投入(交易流出)", f"¥{total_lc_inv:,.0f}")
        c2.metric("已收回(赎回+红利)", f"¥{total_redeem + total_div:,.0f}",
                  delta=f"赎回{total_redeem:,.0f}+红利{total_div:,.0f}")
        c3.metric("全周期净收益", f"¥{total_lc:,.0f}", delta=f"{total_lc_rate:.1f}%")


def _render_cost_section(df_trades):
    """交易成本统计"""
    import plotly.graph_objects as go
    st.subheader("交易成本统计")
    monthly, by_code, total_row = calc_trade_cost_summary(df_trades)
    if monthly.empty:
        render_empty_state("暂无交易费用记录")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("总费用", f"¥{total_row['total_fee']:,.2f}")
    c2.metric("总佣金", f"¥{total_row['total_commission']:,.2f}")
    c3.metric("总印花税", f"¥{total_row['total_stamp_tax']:,.2f}")

    st.markdown("**按月汇总**")
    m_disp = monthly.round(2).rename(columns={
        'month': '月份', 'trade_count': '笔数',
        'total_commission': '佣金', 'total_stamp_tax': '印花税',
        'total_fee': '总费用', 'total_amount': '交易额', 'fee_rate': '费率%'
    })
    st.dataframe(m_disp.reset_index(drop=True), use_container_width=True, hide_index=True)

    st.markdown("**按证券 TOP 10**")
    bc = by_code.head(10).round(2).rename(columns={
        'code': '代码', 'name': '名称', 'trade_count': '笔数',
        'total_commission': '佣金', 'total_stamp_tax': '印花税', 'total_fee': '总费用'
    })
    st.dataframe(bc.reset_index(drop=True), use_container_width=True, hide_index=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=monthly['month'], y=monthly['total_commission'], name='佣金'))
    fig.add_trace(go.Bar(x=monthly['month'], y=monthly['total_stamp_tax'], name='印花税'))
    fig.update_layout(barmode='stack', title="月度交易费用",
                      height=350, xaxis_title='月份', yaxis_title='费用(¥)')
    render_chart(fig)


def _render_cashflow_section(df_trades):
    """月度资金流向"""
    import plotly.graph_objects as go
    st.subheader("月度资金流向")
    pivot = calc_monthly_cashflow(df_trades)
    if pivot.empty or len(pivot.columns) < 3:
        render_empty_state("暂无资金流数据")
        return

    # 汇总指标卡
    total_invest = pivot['净投资流'].sum() if '净投资流' in pivot.columns else 0
    total_cash = pivot['净现金流'].sum() if '净现金流' in pivot.columns else 0
    c1, c2 = st.columns(2)
    c1.metric("累计净投资流", f"¥{total_invest:,.0f}",
              help="不含银行转存和天添利(货币基金)的净投资金额")
    c2.metric("累计净现金流", f"¥{total_cash:,.0f}",
              help="含银行转存和天添利的全部净现金流")

    st.dataframe(pivot.round(0), use_container_width=True, hide_index=True)

    # 图表: 只展示投资流 + 净投资流, 排除现金管理列避免量纲差异
    colors_map = {
        '买入': '#dc3545', '卖出': '#20c997', '定投': '#ffc107',
        '基金申购': '#fd7e14', '基金赎回': '#28a745',
        '股息': '#17a2b8', '红利': '#6f42c1', '质押': '#343a40',
        '净投资流': '#1f77b4',
    }
    cash_cols = {'银行转存', '天添利净流', '净现金流'}
    chart_cols = [c for c in pivot.columns
                  if c not in cash_cols and c != 'month' and c != 'index']
    fig = go.Figure()
    for col in chart_cols:
        fig.add_trace(go.Bar(name=col, x=pivot['month'], y=pivot[col],
                             marker_color=colors_map.get(col, '#888')))
    fig.update_layout(barmode='group', title="月度投资资金流向",
                      height=400, xaxis_title='', yaxis_title='金额(¥)')
    render_chart(fig)


# ==================== 主入口 ====================

def render_tab15():
    """Tab15 交易复盘"""
    df_trades = load_trade_records()
    df_snapshots = load_portfolio_snapshots()
    if df_trades.empty:
        render_empty_state("数据库中暂无交易流水数据，请先导入对账单。")
        return
    total = len(df_trades)
    etf_b = len(df_trades[df_trades['action'] == '证券买入'])
    etf_s = len(df_trades[df_trades['action'] == '证券卖出'])
    dca_n = len(df_trades[df_trades['action'] == '产品定时定额投资确认'])
    fee = (df_trades['commission'] + df_trades['stamp_tax']).sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("总交易", f"{total} 笔")
    c2.metric("场内买卖", f"{etf_b} 买/{etf_s} 卖")
    c3.metric("定投", f"{dca_n} 次")
    c4.metric("交易费用", f"¥{fee:,.2f}")
    _render_etf_pnl_section(df_trades)
    _render_dca_section(df_trades, df_snapshots)
    _render_cost_section(df_trades)
    _render_cashflow_section(df_trades)
