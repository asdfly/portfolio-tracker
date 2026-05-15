"""
Dashboard 辅助函数（依赖 streamlit / database / 外部数据源）
从 dashboard.py 提取，供各 Tab 模块调用。
"""

import streamlit as st
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection
from src.utils.chart_utils import downsample, _add_min_max_annotations
from config.settings import INDEX_CODES
from datetime import datetime




# ===== Stub 函数（替代 dashboard.py 中的外部数据加载函数）=====
# 这些函数在数据库无数据时返回空结果，避免 NameError

@st.cache_data(ttl=300, show_spinner=False)
def load_etf_detail(code, days=120, end_date=None):
    """加载ETF详情数据（stub）"""
    conn = get_db_connection()
    try:
        query = "SELECT * FROM etf_details WHERE code = ? ORDER BY date DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(code, days))
        return df, code
    except Exception:
        return pd.DataFrame(), ""
@st.cache_data(ttl=300, show_spinner=False)
def load_etf_price_history(code, days=250, end_date=None):
    """加载ETF价格历史（stub）"""
    conn = get_db_connection()
    try:
        query = "SELECT date, close, volume FROM etf_price_history WHERE code = ? ORDER BY date DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(code, days))
        return df.sort_values("date")
    except Exception:
        return pd.DataFrame()


def _render_etf_detail_panel(row, selected_date, total_value=0):

    """渲染ETF增强版详情面板：核心指标 + 价格走势 + 技术分析"""

    code = row["code"]

    name = row["name"]



    # 加载详细数据（命中缓存时零延迟）

    detail_df, etf_name = load_etf_detail(code, days=120, end_date=selected_date)

    price_df = load_etf_price_history(code, days=250, end_date=selected_date)



    # ===== 第一行：核心指标卡片（6列） =====

    mv = row.get("market_value", 0)

    pnl = row.get("pnl", 0)

    pnl_rate = row.get("pnl_rate", 0)

    cost = row.get("cost_price", 0)

    current = row.get("current_price", 0)

    _qty = row.get("quantity", 0)



    c1, c2, c3, c4, c5, c6 = st.columns(6)



    with c1:

        st.metric("市值", f"¥{mv:,.0f}")

    with c2:

        st.metric("累计盈亏", f"¥{pnl:,.0f}", delta=f"{pnl_rate:+.2f}%")

    with c3:

        if pd.notna(row.get("ytd_return")):

            yt = row["ytd_return"]

            st.metric("年内收益", f"{yt:+.2f}%")

        else:

            st.metric("年内收益", "--")

    with c4:

        if pd.notna(row.get("beta")):

            st.metric("Beta", f"{row['beta']:.2f}")

        else:

            st.metric("Beta", "--")

    with c5:

        cost_val = f"{cost:.3f}" if pd.notna(cost) else "--"

        st.metric("成本价", cost_val)

    with c6:

        price_diff = current - cost if pd.notna(cost) and pd.notna(current) else None

        delta_str = f"{price_diff:+.3f}" if price_diff is not None else None

        st.metric("现价", f"{current:.3f}" if pd.notna(current) else "--", delta=delta_str)



    # ===== 第二行：价格走势图 + 技术指标详情 =====

    if not price_df.empty:

        col_chart, col_tech = st.columns([3, 1])



        with col_chart:

            st.markdown(

                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">价格走势（近250日）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF近250个交易日的收盘价走势，叠加MA5/MA10/MA20均线，并标注买入成本参考线。</span></div>',

                unsafe_allow_html=True,

            )

            df = price_df.sort_values("date").copy()



            # 降采样

            if len(df) > 500:

                step = max(1, len(df) // 500)

                df_plot = df.iloc[::step].copy()

            else:

                df_plot = df.copy()



            fig = go.Figure()

            fig.add_trace(

                go.Scatter(

                    x=df_plot["date"],

                    y=df_plot["close"],

                    mode="lines",

                    name="收盘价",

                    line=dict(color="#58a6ff", width=1.5),

                    fill="tozeroy",

                    fillcolor="rgba(88,166,255,0.05)",

                    hovertemplate="%{x|%m-%d}<br>价格: %{y:.3f}<extra></extra>",

                )

            )



            # 添加成本线

            if pd.notna(cost) and cost > 0:

                fig.add_hline(

                    y=cost,

                    line_dash="dash",

                    line_color="#f59e0b",

                    annotation_text=f"成本 {cost:.3f}",

                    annotation_position="top left",

                    annotation_font=dict(size=10, color="#f59e0b"),

                )



            # 标记最高价和最低价

            _add_min_max_annotations(fig, df_plot["date"], df_plot["close"], y_label="价格")



            fig.update_layout(

                height=220,

                plot_bgcolor="#0d1117",

                paper_bgcolor="#0d1117",

                font=dict(color="#c9d1d9", size=11),

                margin=dict(l=40, r=15, t=10, b=30),

                xaxis=dict(showgrid=False, tickformat="%m-%d", dtick="M1"),

                yaxis=dict(showgrid=True, gridcolor="#21262d", tickformat=".3f"),

                hovermode="x unified",

            )

            st.plotly_chart(fig, width="stretch")



        with col_tech:

            st.markdown(

                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">技术指标<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF的RSI、MACD、KDJ、布林带等常用技术指标信号。</span></div>',

                unsafe_allow_html=True,

            )

            if not detail_df.empty:

                latest = detail_df.iloc[-1]



                trend_map = {

                    "强势上涨": ("看多", "#22c55e"),

                    "强势下跌": ("看空", "#ef4444"),

                    "震荡中性": ("中性", "#f59e0b"),

                    None: ("--", "#888"),

                }

                _trend = str(latest.get("trend", ""))
                if "上涨" in _trend:
                    trend_label, trend_color = ("看多", "#22c55e")
                elif "下跌" in _trend:
                    trend_label, trend_color = ("看空", "#ef4444")
                else:
                    trend_label, trend_color = trend_map.get(_trend if _trend else None, ("--", "#888"))



                # 技术指标卡片

                indicators = [

                    ("趋势", trend_label, trend_color),

                    (

                        "RSI",

                        f"{latest.get('rsi_value', '--'):.1f}" if pd.notna(latest.get("rsi_value")) else "--",

                        (

                            "#22c55e"

                            if latest.get("rsi_status") in ("超卖", "偏低")

                            else "#ef4444" if latest.get("rsi_status") in ("超买", "偏高") else "#c9d1d9"

                        ),

                    ),

                    ("MA信号", str(latest.get("ma_signal", "--")), "#c9d1d9"),

                    ("MACD", str(latest.get("macd_signal", "--")), "#c9d1d9"),

                    ("KDJ", str(latest.get("kdj_signal", "--")), "#c9d1d9"),

                    ("布林位置", str(latest.get("bollinger_position", "--")), "#c9d1d9"),

                    (

                        "ATR%",

                        f"{latest.get('atr_pct', '--'):.1f}%" if pd.notna(latest.get("atr_pct")) else "--",

                        "#c9d1d9",

                    ),

                ]



                for label, value, color in indicators:

                    st.markdown(

                        f'<div style="display:flex;justify-content:space-between;padding:4px 8px;'

                        f'border-bottom:1px solid #21262d;font-size:12px;">'

                        f'<span style="color:#8b949e;">{label}</span>'

                        f'<span style="color:{color};font-weight:bold;">{value}</span>'

                        f"</div>",

                        unsafe_allow_html=True,

                    )



                # RSI 仪表条

                rsi_val = latest.get("rsi_value", None)

                if pd.notna(rsi_val):

                    rsi_clamped = max(0, min(100, float(rsi_val)))

                    bar_color = "#ef4444" if rsi_clamped > 70 else "#22c55e" if rsi_clamped < 30 else "#f59e0b"

                    st.markdown(

                        f'<div style="margin-top:8px;font-size:11px;color:#8b949e;">RSI 位置</div>'

                        f'<div style="background:#21262d;border-radius:4px;height:8px;position:relative;">'

                        f'<div style="background:{bar_color};border-radius:4px;height:8px;width:{rsi_clamped}%;"></div>'

                        f'<div style="position:absolute;top:-2px;left:70%;width:1px;height:12px;background:#ef4444;opacity:0.5;"></div>'

                        f'<div style="position:absolute;top:-2px;left:30%;width:1px;height:12px;background:#22c55e;opacity:0.5;"></div>'

                        f"</div>"

                        f'<div style="display:flex;justify-content:space-between;font-size:9px;color:#484f58;">'

                        f"<span>超卖 30</span><span>中性</span><span>超买 70</span></div>",

                        unsafe_allow_html=True,

                    )

            else:

                st.info("暂无技术指标数据")



    # ===== 第三行：收益率分布 + 关键统计 =====

    if not detail_df.empty:

        col_stats, col_dist = st.columns([1, 2])



        with col_stats:

            st.markdown(

                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">关键统计<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF的日均收益、标准差、最大涨跌幅等关键统计指标。</span></div>',

                unsafe_allow_html=True,

            )

            df_detail = detail_df.sort_values("date")

            daily_returns = df_detail["current_price"].pct_change().dropna() if len(df_detail) > 1 else pd.Series()



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

                    f"</div>",

                    unsafe_allow_html=True,

                )



        with col_dist:

            if len(daily_returns) > 5:

                st.markdown(

                    '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">日收益率分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">统计该ETF日收益率的频率分布，可判断收益的正态性和肥尾特征。</span></div>',

                    unsafe_allow_html=True,

                )

                fig_hist = go.Figure()

                colors = ["#22c55e" if v >= 0 else "#ef4444" for v in daily_returns]

                fig_hist.add_trace(

                    go.Histogram(

                        x=daily_returns * 100,

                        marker_color="#58a6ff",

                        nbinsx=30,

                        opacity=0.7,

                        hovertemplate="区间: %{x:.2f}%<br>次数: %{y}<extra></extra>",

                    )

                )

                # 标记零线

                fig_hist.add_vline(x=0, line_dash="dash", line_color="#f59e0b", line_width=1)

                fig_hist.update_layout(

                    height=180,

                    plot_bgcolor="#0d1117",

                    paper_bgcolor="#0d1117",

                    font=dict(color="#c9d1d9", size=11),

                    margin=dict(l=40, r=15, t=10, b=30),

                    xaxis=dict(title="日收益率 %", showgrid=False),

                    yaxis=dict(title="频次", showgrid=True, gridcolor="#21262d"),

                    bargap=0.05,

                )

                st.plotly_chart(fig_hist, width="stretch")





@st.cache_data(ttl=300, show_spinner=False)



def _generate_oneclick_report(positions, summary, technical, selected_date, selected_benchmark):

    """生成综合分析报告 HTML"""

    import math



    if positions.empty or summary.empty:

        return None



    total_value = positions["market_value"].sum()

    total_cost = summary.iloc[-1].get("total_cost", 0)

    total_pnl = positions["pnl"].sum()

    total_return = (total_pnl / total_cost * 100) if total_cost > 0 else 0



    port_daily = summary["total_value"].pct_change().dropna()

    ann_ret = port_daily.mean() * 252 * 100 if len(port_daily) > 0 else 0

    ann_vol = port_daily.std() * math.sqrt(252) * 100 if len(port_daily) > 1 else 0

    sharpe = (port_daily.mean() / port_daily.std() * math.sqrt(252)) if port_daily.std() > 0 else 0

    cummax = summary["total_value"].cummax()

    max_dd = ((summary["total_value"] - cummax) / cummax * 100).min()



    pc = len(positions[positions["pnl"] > 0])

    lc = len(positions[positions["pnl"] < 0])

    wr = (pc / (pc + lc) * 100) if (pc + lc) > 0 else 0



    pnl_color = "#22c55e" if total_pnl >= 0 else "#ef4444"

    ret_color = "#22c55e" if total_return >= 0 else "#ef4444"



    # 持仓明细表

    pos_rows = ""

    for _, pos in positions.iterrows():

        p_color = "#22c55e" if pos["pnl"] >= 0 else "#ef4444"

        pos_rows += (

            f'<tr style="border-bottom:1px solid #eee;">'

            f'<td style="padding:6px 8px;">{pos["name"]}</td>'

            f'<td style="padding:6px 8px;">{pos["code"]}</td>'

            f'<td style="padding:6px 8px;text-align:right;">{pos["quantity"]:,.0f}</td>'

            f'<td style="padding:6px 8px;text-align:right;">{pos["cost_price"]:.3f}</td>'

            f'<td style="padding:6px 8px;text-align:right;">{pos["current_price"]:.3f}</td>'

            f'<td style="padding:6px 8px;text-align:right;">¥{pos["market_value"]:,.0f}</td>'

            f'<td style="padding:6px 8px;text-align:right;color:{p_color};">¥{pos["pnl"]:,.0f}</td>'

            f'<td style="padding:6px 8px;text-align:right;color:{p_color};">{pos["pnl_rate"]:+.2f}%</td>'

            f"</tr>"

        )



    # 技术信号摘要

    tech_rows = ""

    if technical is not None and not technical.empty:

        tech_latest = technical.drop_duplicates("code", keep="first")

        for _, tr in tech_latest.iterrows():

            name = tr.get("name", tr["code"])

            trend = tr.get("trend", "--")

            ma = tr.get("ma_signal", "--")

            macd = tr.get("macd_signal", "--")

            rsi_st = tr.get("rsi_status", "--")

            tech_rows += (

                f'<tr style="border-bottom:1px solid #eee;">'

                f'<td style="padding:5px 8px;">{name}</td>'

                f'<td style="padding:5px 8px;">{trend}</td>'

                f'<td style="padding:5px 8px;">{ma}</td>'

                f'<td style="padding:5px 8px;">{macd}</td>'

                f'<td style="padding:5px 8px;">{rsi_st}</td>'

                f'<td style="padding:5px 8px;">{tr.get("rsi_value", "--"):.1f}</td>'

                f"</tr>"

            )



    bench_name = INDEX_CODES.get(selected_benchmark, selected_benchmark)



    html = f"""<!DOCTYPE html>

<html><head><meta charset="utf-8"><title>投资组合分析报告 {selected_date}</title>

<style>

body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 960px; margin: 0 auto; padding: 20px; color: #333; }}

h1 {{ font-size: 22px; border-bottom: 2px solid #4a90d9; padding-bottom: 8px; }}

h2 {{ font-size: 16px; color: #4a90d9; margin-top: 24px; }}

.meta {{ font-size: 12px; color: #888; margin-bottom: 20px; }}

.metrics {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 12px 0; }}

.metric-card {{ background: #f8f9fa; border-radius: 8px; padding: 12px 16px; min-width: 140px; }}

.metric-label {{ font-size: 11px; color: #888; }}

.metric-value {{ font-size: 20px; font-weight: bold; }}

table {{ width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0; }}

th {{ background: #f0f2f5; padding: 6px 8px; text-align: left; font-size: 11px; color: #666; }}

td {{ padding: 5px 8px; }}

.section {{ margin: 16px 0; padding: 12px; background: #fafbfc; border-radius: 6px; border-left: 3px solid #4a90d9; }}

.footer {{ font-size: 11px; color: #aaa; text-align: center; margin-top: 30px; border-top: 1px solid #eee; padding-top: 12px; }}

</style></head><body>

<h1>📊 投资组合分析报告</h1>

<div class="meta">生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | 数据截至: {selected_date} | 基准: {bench_name}</div>



<h2>一、组合概览</h2>

<div class="metrics">

  <div class="metric-card"><div class="metric-label">总市值</div><div class="metric-value">¥{total_value:,.0f}</div></div>

  <div class="metric-card"><div class="metric-label">总盈亏</div><div class="metric-value" style="color:{pnl_color};">¥{total_pnl:,.0f}</div></div>

  <div class="metric-card"><div class="metric-label">总收益率</div><div class="metric-value" style="color:{ret_color};">{total_return:+.2f}%</div></div>

  <div class="metric-card"><div class="metric-label">年化收益率</div><div class="metric-value">{ann_ret:+.2f}%</div></div>

  <div class="metric-card"><div class="metric-label">夏普比率</div><div class="metric-value">{sharpe:.3f}</div></div>

  <div class="metric-card"><div class="metric-label">最大回撤</div><div class="metric-value" style="color:#ef4444;">{max_dd:.2f}%</div></div>

  <div class="metric-card"><div class="metric-label">年化波动率</div><div class="metric-value">{ann_vol:.2f}%</div></div>

  <div class="metric-card"><div class="metric-label">胜率</div><div class="metric-value">{wr:.1f}% ({pc}盈/{lc}亏)</div></div>

</div>



<h2>二、持仓明细</h2>

<table><thead><tr>

<th>名称</th><th>代码</th><th style="text-align:right;">持仓量</th>

<th style="text-align:right;">成本价</th><th style="text-align:right;">现价</th>

<th style="text-align:right;">市值</th><th style="text-align:right;">盈亏</th>

<th style="text-align:right;">收益率</th>

</tr></thead><tbody>{pos_rows}</tbody></table>



<h2>三、技术信号</h2>

{"<table><thead><tr><th>ETF</th><th>趋势</th><th>均线</th><th>MACD</th><th>RSI状态</th><th>RSI值</th></tr></thead><tbody>" + tech_rows + "</tbody></table>" if tech_rows else "<p style='color:#888;'>暂无技术信号数据</p>"}



<h2>四、风险提示</h2>

<div class="section">

<ul style="font-size:13px;line-height:1.8;">

<li>最大回撤 <b>{max_dd:.2f}%</b>，{'超过15%警戒线，需注意控制下行风险' if abs(max_dd) > 15 else '处于正常波动范围'}</li>

<li>年化波动率 <b>{ann_vol:.2f}%</b>，{'波动较大，注意风险管理' if ann_vol > 25 else '处于合理水平'}</li>

<li>胜率 <b>{wr:.1f}%</b>，{'持仓中大部分标的处于盈利状态' if wr > 60 else '盈利标的占比较低，需关注'}</li>

</ul></div>



<div class="footer">投资组合跟踪分析系统 v2.0 | 本报告仅供参考，不构成投资建议</div>

</body></html>"""

    return html





@st.cache_data(ttl=600, show_spinner=False)



def _load_latest_news(_categories):

    """加载最新新闻（带缓存）"""

    conn = get_db_connection()

    try:

        placeholders = ",".join(["?" for _ in _categories])

        return pd.read_sql_query(

            f"SELECT date, category, title, source, url, summary, publish_time "

            f"FROM daily_news WHERE category IN ({placeholders}) "

            f"ORDER BY date DESC, publish_time DESC LIMIT 30",

            conn,

            params=list(_categories),

        )

    except Exception:

        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)



def _load_tech_signals(_codes, _full=False):

    """加载技术指标信号（带缓存）"""

    if not _codes:

        return pd.DataFrame()

    conn = get_db_connection()

    try:

        ph = ",".join(["?" for _ in _codes])

        if _full:

            cols = "*"

        else:

            cols = "code, ma_signal, macd_signal, rsi_status, kdj_signal, bollinger_position, trend"

        return pd.read_sql_query(

            f"SELECT {cols} FROM etf_technical WHERE code IN ({ph}) ORDER BY date DESC", conn, params=list(_codes)

        )

    except Exception:

        return pd.DataFrame()

