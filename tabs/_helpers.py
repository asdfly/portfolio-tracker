"""
Dashboard 辅助函数（依赖 streamlit / database / 外部数据源）
从 dashboard.py 提取，供各 Tab 模块调用。
"""

from components.ui import render_chart, render_empty_state
import logging

import streamlit as st
import pandas as pd

logger = logging.getLogger(__name__)
import numpy as np
from src.utils.database import get_db_connection
from src.utils.chart_utils import downsample, _add_min_max_annotations
from config.settings import CACHE_TTL, CHART_DAYS, DOWNSAMPLE_MAX_POINTS, INDEX_CODES
import plotly.graph_objects as go
from datetime import datetime
import sqlite3




# ===== Stub 函数（替代 dashboard.py 中的外部数据加载函数）=====
# 这些函数在数据库无数据时返回空结果，避免 NameError





def _render_etf_metrics(row, total_value):
    mv = row.get("market_value", 0)
    pnl = row.get("pnl", 0)
    pnl_rate = row.get("pnl_rate", 0)
    cost = row.get("cost_price", 0)
    current = row.get("current_price", 0)

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

def _render_etf_price_chart(price_df, detail_df, cost, current, code, selected_date):
    if not price_df.empty:

        col_chart, col_tech = st.columns([3, 1])



        with col_chart:

            st.markdown(

                '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">价格走势（近250日）<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示该ETF近250个交易日的收盘价走势，叠加MA5/MA10/MA20均线，并标注买入成本参考线。</span></div>',

                unsafe_allow_html=True,

            )

            df = price_df.sort_values("date").copy()



            # 降采样

            if len(df) > DOWNSAMPLE_MAX_POINTS:

                step = max(1, len(df) // DOWNSAMPLE_MAX_POINTS)

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

            render_chart(fig)



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

def _render_etf_stats(detail_df, mv, total_value):
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

                render_chart(fig_hist)

def _render_etf_detail_panel(row, selected_date, total_value=0):
    """渲染ETF增强版详情面板：核心指标 + 价格走势 + 同类对比 + 技术评分"""
    from data_loader import load_etf_detail, load_etf_price_history
    code = row["code"]
    name = row["name"]
    detail_df, etf_name = load_etf_detail(code, days=120, end_date=selected_date)
    price_df = load_etf_price_history(code, days=250, end_date=selected_date)

    _render_etf_metrics(row, total_value)
    _render_etf_price_chart(price_df, detail_df, row.get("cost_price", 0),
                            row.get("current_price", 0), code, selected_date)
    _render_etf_stats(detail_df, row.get("market_value", 0), total_value)

    # 新增 tab：同类对比 + 技术评分
    from data_loader import load_etf_fundamental
    from src.analysis.signal_score import compute_signal_score
    from config.settings import ETF_CATEGORIES, CACHE_TTL
    from src.utils.database import get_db_connection

    fund_df = load_etf_fundamental()
    sector = ETF_CATEGORIES.get(str(code), {}).get("sector", "")

    # 加载最新技术指标
    conn = get_db_connection()
    try:
        tech_df = pd.read_sql_query(
            "SELECT * FROM etf_technical ORDER BY date DESC LIMIT 500",
            conn
        )
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading technical data: {e}")
        tech_df = pd.DataFrame()
    finally:
        conn.close()

    tab_peer, tab_signal, tab_risk, tab_flow, tab_trades, tab_news = st.tabs(["同类ETF对比", "技术信号评分", "风险全景", "资金流向", "交易复盘", "行业观点"])
    with tab_peer:
        if sector:
            sub_tab_basic, sub_tab_pen = st.tabs(["行情对比", "穿透分析"])
            with sub_tab_basic:
                _render_peer_comparison(code, sector, fund_df)
            with sub_tab_pen:
                _render_peer_penetration_panel(code, sector)
        else:
            st.info("该ETF未配置行业分类，无法进行同类对比")
    with tab_signal:
        _render_signal_score_panel(code, tech_df)
    with tab_risk:
        _render_risk_scan_panel(code, fund_df)
    with tab_flow:
        _render_fund_flow_panel(code)
    with tab_trades:
        _render_trade_review_panel(code)
    with tab_news:
        _render_industry_news_panel(code)






def _render_peer_comparison(code, sector, fund_df):
    """渲染同类ETF横向对比：排序表格 + 雷达图。"""
    if fund_df is None or fund_df.empty:
        st.info("暂无同类ETF数据")
        return

    from config.settings import ETF_CATEGORIES

    peer_codes = [c for c, info in ETF_CATEGORIES.items() if info.get("sector") == sector]
    peer_df = fund_df[fund_df["code"].astype(str).isin(peer_codes)].copy()
    if peer_df.empty:
        st.info("该行业暂无同类ETF数据")
        return

    sort_col = st.selectbox(
        "排序指标",
        ["折价率", "资金净流入(万)", "换手率", "量比", "规模(亿)"],
        key="peer_sort_" + code,
    )
    sort_map = {
        "折价率": ("discount_rate", False),
        "资金净流入(万)": ("main_net_inflow", True),
        "换手率": ("turnover_rate", False),
        "量比": ("volume_ratio", False),
        "规模(亿)": ("total_mv", True),
    }
    col_name, ascending = sort_map[sort_col]

    display_df = peer_df[["code", "name", "price", "iopv", "discount_rate",
                           "change_pct", "turnover_rate", "volume_ratio",
                           "main_net_inflow", "main_net_inflow_pct",
                           "total_mv"]].copy()
    display_df["total_mv"] = display_df["total_mv"] / 1e8
    display_df["main_net_inflow"] = display_df["main_net_inflow"] / 1e4
    display_df = display_df.sort_values(by=col_name, ascending=ascending).reset_index(drop=True)
    display_df.columns = ["代码", "名称", "价格", "IOPV", "折价率%",
                          "涨跌幅%", "换手率%", "量比",
                          "资金净流入(万)", "资金净流入%", "规模(亿)"]

    st.dataframe(display_df, use_container_width=True, height=min(200 + len(display_df) * 28, 400),
                 hide_index=True)

    if len(peer_df) >= 2:
        top5 = peer_df.nlargest(min(5, len(peer_df)), "total_mv")
        categories_radar = ["折价率(归一化)", "换手率", "量比", "资金流入", "规模"]
        fig = go.Figure()
        colors = ["#1a5276", "#e74c3c", "#2980b9", "#27ae60", "#e67e22"]
        for i, (_, r) in enumerate(top5.iterrows()):
            vals = [
                max(0, (r.get("discount_rate", 0) + 2) / 4),
                min(r.get("turnover_rate", 0) / 15, 1),
                min(r.get("volume_ratio", 1) / 5, 1),
                min(max((r.get("main_net_inflow", 0) / 1e4 + 5000) / 10000, 0), 1),
                min(r.get("total_mv", 0) / peer_df["total_mv"].max(), 1) if peer_df["total_mv"].max() > 0 else 0.5,
            ]
            is_current = str(r["code"]) == str(code)
            fig.add_trace(go.Scatterpolar(
                r=vals + [vals[0]],
                theta=categories_radar + [categories_radar[0]],
                fill="toself" if is_current else None,
                name=r.get("name", r["code"]),
                line_color=colors[i % len(colors)],
                opacity=0.9 if is_current else 0.5,
                line_width=2.5 if is_current else 1.5,
            ))
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            showlegend=True,
            legend=dict(font_size=10, x=0.02, y=0.98),
            margin=dict(l=40, r=40, t=30, b=30),
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_signal_score_panel(code, technical_df):
    """渲染技术信号综合评分面板。

    Parameters
    ----------
    code : str - ETF 代码
    technical_df : pd.DataFrame - etf_technical 表数据（全部或最新日期）
    """
    from src.analysis.signal_score import compute_signal_score

    if technical_df is None or technical_df.empty:
        st.info("暂无技术指标数据")
        return

    # 取最新一行
    latest = technical_df[technical_df["code"].astype(str) == str(code)]
    if latest.empty:
        st.info(f"{code} 暂无技术指标数据")
        return
    row = latest.iloc[-1]

    result = compute_signal_score(row)
    score = result["total_score"]
    grade = result["grade"]

    # 评分颜色映射
    if score >= 70:
        score_color = "#27ae60"
        bg_color = "#eafaf1"
    elif score >= 55:
        score_color = "#2980b9"
        bg_color = "#eaf2f8"
    elif score >= 40:
        score_color = "#f39c12"
        bg_color = "#fef9e7"
    else:
        score_color = "#e74c3c"
        bg_color = "#fdedec"

    # 顶部评分展示
    c1, c2 = st.columns([1, 3])
    with c1:
        st.markdown(
            '<div style="text-align:center;padding:15px;border-radius:8px;'
            'background:%s;border:2px solid %s;">'
            '<div style="font-size:36px;font-weight:bold;color:%s;">%s</div>'
            '<div style="font-size:14px;color:%s;margin-top:5px;">%s</div>'
            '</div>' % (bg_color, score_color, score_color, score, score_color, grade),
            unsafe_allow_html=True,
        )
    with c2:
        # 各维度条形图
        dim_names = {
            "trend": ("趋势", 0.30),
            "momentum": ("动量", 0.25),
            "volatility": ("波动", 0.20),
            "oversold_overbought": ("超买超卖", 0.15),
            "volume": ("成交量", 0.10),
        }
        for key, (label, weight) in dim_names.items():
            sig = result["signals"][key]
            s = sig["score"]
            detail = sig["detail"]
            if s >= 70:
                bar_color = "#27ae60"
            elif s >= 40:
                bar_color = "#f39c12"
            else:
                bar_color = "#e74c3c"
            st.markdown(
                '<div style="margin:2px 0;">'
                '<span style="font-size:11px;color:#555;display:inline-block;width:65px;">%s(%d%%)</span>'
                '<div style="display:inline-block;width:65%%;height:14px;background:#ecf0f1;'
                'border-radius:3px;vertical-align:middle;position:relative;">'
                '<div style="width:%s%%;height:14px;background:%s;border-radius:3px;"></div>'
                '</div>'
                '<span style="font-size:11px;color:#333;margin-left:5px;">%s</span>'
                '</div>' % (label, int(weight * 100), s, bar_color, "%.0f" % s),
                unsafe_allow_html=True,
            )
            st.caption(detail)



def _render_risk_scan_panel(code, fund_df):
    """渲染单品风险全景扫描面板。

    Parameters
    ----------
    code : str - ETF 代码
    fund_df : pd.DataFrame - etf_fundamental 全量数据
    """
    from data_loader import load_etf_risk_scan

    result = load_etf_risk_scan(code)
    if result is None:
        st.info("暂无风险扫描数据")
        return

    score = result["total_score"]
    risk_level = result["risk_level"]
    grade = result["grade"]

    # 颜色映射（风险越高颜色越红）
    if score >= 70:
        score_color, bg_color = "#e74c3c", "#fdedec"
    elif score >= 55:
        score_color, bg_color = "#e67e22", "#fef5e7"
    elif score >= 40:
        score_color, bg_color = "#f39c12", "#fef9e7"
    else:
        score_color, bg_color = "#27ae60", "#eafaf1"

    # 顶部风险评分卡
    st.markdown(
        '<div style="text-align:center;padding:15px;border-radius:8px;'
        'background:%s;border:2px solid %s;margin-bottom:10px;">'
        '<div style="font-size:12px;color:#666;">综合风险评分</div>'
        '<div style="font-size:36px;font-weight:bold;color:%s;">%s</div>'
        '<div style="font-size:14px;color:%s;">%s - %s</div>'
        '<div style="font-size:12px;color:#888;margin-top:5px;">%s</div>'
        '</div>' % (bg_color, score_color, score_color, score,
                    score_color, risk_level, grade,
                    result["summary"].replace("<", "&lt;").replace(">", "&gt;")),
        unsafe_allow_html=True,
    )

    # 5 维度风险详情
    dim_config = {
        "volatility": ("波动率", "25%"),
        "discount": ("折价风险", "20%"),
        "liquidity": ("流动性", "20%"),
        "downside": ("下行压力", "20%"),
        "deviation": ("偏离度", "15%"),
    }

    cols = st.columns(5)
    for idx, (key, (label, weight)) in enumerate(dim_config.items()):
        dim = result["dimensions"][key]
        d_score = dim["score"]
        d_level = dim["level"]
        detail = dim["detail"]

        if d_score >= 70:
            d_color = "#e74c3c"
        elif d_score >= 45:
            d_color = "#f39c12"
        else:
            d_color = "#27ae60"

        with cols[idx]:
            st.markdown(
                '<div style="text-align:center;padding:10px;border-radius:6px;'
                'background:#f8f9fa;border-left:3px solid %s;margin-bottom:5px;">'
                '<div style="font-size:11px;color:#666;">%s(%s)</div>'
                '<div style="font-size:24px;font-weight:bold;color:%s;">%s</div>'
                '<div style="font-size:10px;color:%s;">%s</div>'
                '</div>' % (d_color, label, weight, d_color, "%.0f" % d_score, d_color, d_level),
                unsafe_allow_html=True,
            )
            st.caption(detail)

    # 风险雷达图
    import plotly.graph_objects as go
    categories = list(dim_config.keys())
    labels = [v[0] for v in dim_config.values()]
    values = [result["dimensions"][k]["score"] for k in categories]

    fig = go.Figure(data=go.Scatterpolar(
        r=values + [values[0]],
        theta=labels + [labels[0]],
        fill="toself",
        fillcolor="rgba(231,76,60,0.15)",
        line_color=score_color,
        line_width=2,
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        margin=dict(l=50, r=50, t=30, b=30),
        height=300,
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)



def _render_fund_flow_panel(code):
    """渲染 ETF 资金流向面板。

    Parameters
    ----------
    code : str - ETF 代码
    """
    from data_loader import load_etf_fund_flow
    import plotly.graph_objects as go

    df = load_etf_fund_flow(code, days=60)
    if df.empty:
        st.info("暂无资金流向数据")
        return

    # 资金流向柱状图（正负）
    fig = go.Figure()
    colors = ["#27ae60" if v >= 0 else "#e74c3c" for v in df["net_inflow"]]
    fig.add_trace(go.Bar(
        x=df["date"],
        y=df["net_inflow"] / 1e4,
        marker_color=colors,
        name="净流入",
    ))
    # 大单 vs 小单对比
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df.get("super_large_inflow", pd.Series(dtype=float)) / 1e4,
        mode="lines+markers",
        name="超大单",
        line=dict(color="#e67e22", width=1.5),
        marker=dict(size=3),
    ))
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df.get("small_inflow", pd.Series(dtype=float)) / 1e4,
        mode="lines+markers",
        name="小单",
        line=dict(color="#3498db", width=1.5),
        marker=dict(size=3),
    ))
    fig.update_layout(
        title="资金流向（万元）",
        xaxis_title="",
        yaxis_title="万元",
        legend=dict(font_size=10, orientation="h", y=1.1),
        margin=dict(l=40, r=20, t=40, b=30),
        height=320,
        xaxis=dict(tickangle=-45, nticks=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    # 资金流统计摘要
    c1, c2, c3 = st.columns(3)
    total_net = df["net_inflow"].sum() / 1e8
    avg_daily = df["net_inflow"].mean() / 1e4
    positive_days = (df["net_inflow"] > 0).sum()
    negative_days = (df["net_inflow"] < 0).sum()

    with c1:
        color = "#e74c3c" if total_net < 0 else "#27ae60"
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;'
            'background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">累计净流入</div>'
            '<div style="font-size:20px;font-weight:bold;color:%s;">%+.2f亿</div>'
            '</div>' % (color, total_net),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;'
            'background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">日均净流入</div>'
            '<div style="font-size:20px;font-weight:bold;color:#333;">%+.0f万</div>'
            '</div>' % avg_daily,
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;'
            'background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">净流入/流出天数</div>'
            '<div style="font-size:20px;font-weight:bold;color:#27ae60;">%d / '
            '<span style="color:#e74c3c;">%d</span></div>'
            '</div>' % (positive_days, negative_days),
            unsafe_allow_html=True,
        )

    # 最近资金流向明细
    recent = df.tail(10).copy()
    recent["net_inflow"] = recent["net_inflow"] / 1e4
    recent["super_large_inflow"] = recent.get("super_large_inflow", 0) / 1e4
    recent["large_inflow"] = recent.get("large_inflow", 0) / 1e4
    recent["small_inflow"] = recent.get("small_inflow", 0) / 1e4
    display = recent[["date", "net_inflow", "net_inflow_pct",
                       "super_large_inflow", "large_inflow", "small_inflow"]].copy()
    display.columns = ["日期", "净流入(万)", "净流入%", "超大单(万)", "大单(万)", "小单(万)"]
    st.dataframe(display, use_container_width=True, hide_index=True)


def _render_trade_review_panel(code):
    """渲染交易复盘面板。

    Parameters
    ----------
    code : str - ETF 代码
    """
    from data_loader import load_trade_analysis

    analysis = load_trade_analysis(code)
    if not analysis:
        st.info("暂无交易记录。可通过 CSV 导入券商交易流水后在持仓详情查看复盘。")
        return

    trades_df = analysis["trades"]

    # 顶部统计卡片
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        pnl = analysis["realized_pnl"]
        color = "#27ae60" if pnl >= 0 else "#e74c3c"
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">已实现盈亏</div>'
            '<div style="font-size:18px;font-weight:bold;color:%s;">%+.2f</div>'
            '</div>' % (color, pnl),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">胜率</div>'
            '<div style="font-size:18px;font-weight:bold;color:#333;">%.1f%%</div>'
            '</div>' % analysis["win_rate"],
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">总交易次数</div>'
            '<div style="font-size:18px;font-weight:bold;color:#333;">%d</div>'
            '</div>' % analysis["total_trades"],
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">累计手续费</div>'
            '<div style="font-size:18px;font-weight:bold;color:#e74c3c;">%.2f</div>'
            '</div>' % analysis["total_fee"],
            unsafe_allow_html=True,
        )

    # 交易明细表
    if not trades_df.empty:
        available = ["date", "direction", "price", "quantity", "fee"]
        extra = [c for c in ["change_amount", "note"] if c in trades_df.columns][:1]
        display = trades_df[available + extra].copy()
        col_names = ["日期", "方向", "价格", "数量", "手续费"] + (["发生额"] if "change_amount" in trades_df.columns else ["备注"])
        display.columns = col_names
        display["金额"] = display["价格"] * display["数量"]
        display.columns = list(display.columns[:-1]) + ["金额"]
        st.dataframe(display, use_container_width=True, hide_index=True)

    # 买卖点标注价格图
    if not trades_df.empty:
        try:
            from data_loader import load_etf_price_history
            import plotly.graph_objects as go

            price_df = load_etf_price_history(code, days=250)
            if price_df is not None and not price_df.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=price_df["date"],
                    y=price_df["close"],
                    mode="lines",
                    name="收盘价",
                    line=dict(color="#3498db", width=1.5),
                ))

                buys = trades_df[trades_df["direction"] == "BUY"]
                sells = trades_df[trades_df["direction"] == "SELL"]

                if not buys.empty:
                    fig.add_trace(go.Scatter(
                        x=buys["date"],
                        y=buys["price"],
                        mode="markers",
                        name="买入",
                        marker=dict(symbol="triangle-up", size=12, color="#27ae60"),
                    ))
                if not sells.empty:
                    fig.add_trace(go.Scatter(
                        x=sells["date"],
                        y=sells["price"],
                        mode="markers",
                        name="卖出",
                        marker=dict(symbol="triangle-down", size=12, color="#e74c3c"),
                    ))

                fig.update_layout(
                    title="买卖点标注",
                    xaxis_title="",
                    yaxis_title="价格",
                    legend=dict(font_size=10, orientation="h", y=1.1),
                    margin=dict(l=40, r=20, t=40, b=30),
                    height=300,
                    xaxis=dict(tickangle=-45, nticks=10),
                )
                st.plotly_chart(fig, use_container_width=True)
        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"Chart render skipped: {e}")


def _render_industry_news_panel(code):
    """渲染行业观点与研报聚合面板。

    Parameters
    ----------
    code : str - ETF 代码
    """
    from data_loader import load_etf_industry_news, load_sector_sentiment

    # 行业情绪概览
    sentiment = load_sector_sentiment(code, days=30)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        avg = sentiment["avg_sentiment"]
        color = "#27ae60" if avg > 0.1 else ("#e74c3c" if avg < -0.1 else "#f39c12")
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">行业情绪</div>'
            '<div style="font-size:18px;font-weight:bold;color:%s;">%s</div>'
            '</div>' % (color, "偏多" if avg > 0.1 else ("偏空" if avg < -0.1 else "中性")),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">相关资讯</div>'
            '<div style="font-size:18px;font-weight:bold;color:#333;">%d 条</div>'
            '</div>' % sentiment["news_count"],
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">正面</div>'
            '<div style="font-size:18px;font-weight:bold;color:#27ae60;">%d</div>'
            '</div>' % sentiment["positive_count"],
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            '<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
            '<div style="font-size:11px;color:#666;">负面</div>'
            '<div style="font-size:18px;font-weight:bold;color:#e74c3c;">%d</div>'
            '</div>' % sentiment["negative_count"],
            unsafe_allow_html=True,
        )

    # Top headlines
    headlines = sentiment.get("top_headlines", [])
    if headlines:
        st.markdown("**近期行业要闻**")
        for h in headlines:
            sent_val = h.get("sentiment_score", 0)
            if pd.isna(sent_val):
                sent_val = 0
            icon = "🟢" if sent_val > 0.1 else ("🔴" if sent_val < -0.1 else "⚪")
            st.markdown(
                '- %s **[%s]** %s — *%s*' % (
                    icon,
                    h.get("date", "")[:10],
                    h.get("title", ""),
                    h.get("source", ""),
                )
            )
    else:
        st.info("暂无该 ETF 相关的行业资讯")

    # 新闻明细表
    news_df = load_etf_industry_news(code, days=30)
    if not news_df.empty:
        display = news_df[["date", "title", "source", "sentiment_score"]].copy()
        display.columns = ["日期", "标题", "来源", "情绪"]
        display["情绪"] = display["情绪"].apply(
            lambda x: "偏多" if (pd.notna(x) and x > 0.1) else ("偏空" if (pd.notna(x) and x < -0.1) else "中性")
        )
        st.dataframe(display, use_container_width=True, hide_index=True, height=min(200 + len(display) * 28, 400))
        # P2: Sentiment trend chart
        try:
            from data_loader import load_news_sentiment_for_positions
            from config.settings import ETF_CATEGORIES
            held_sectors = list(set(v.get("sector", "") for v in ETF_CATEGORIES.values() if v.get("sector")))
            if held_sectors:
                sent = load_news_sentiment_for_positions(held_sectors, days=14)
                if sent and sent.trend_df is not None and not sent.trend_df.empty:
                    import plotly.express as px
                    from components.ui import render_chart
                    fig = px.line(sent.trend_df, x="date", y="avg_score", color="sector",
                                  title="板块情绪趋势(14日)", markers=True)
                    fig.update_yaxes(range=[0, 1])
                    render_chart(fig, use_container_width=True)
        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"Chart render skipped: {e}")

def _generate_oneclick_report(positions, summary, technical, selected_date, selected_benchmark):

    """生成综合分析报告 HTML"""

    import math



    if positions.empty or summary.empty:

        return None



    total_value = positions["market_value"].sum()

    total_cost = summary.iloc[-1].get("total_cost", 0)

    total_pnl = positions["pnl"].sum()

    total_return = (total_pnl / total_cost * 100) if total_cost > 0 else 0



    # 使用 portfolio_summary 预存的 corrected daily_return（百分比格式/100=小数），避免 total_value 跳变影响
    port_daily = (summary["daily_return"] / 100).dropna() if "daily_return" in summary.columns else summary["total_value"].pct_change().dropna()

    ann_ret = port_daily.mean() * 252 * 100 if len(port_daily) > 0 else 0

    ann_vol = port_daily.std() * math.sqrt(252) * 100 if len(port_daily) > 1 else 0

    sharpe = (port_daily.mean() / port_daily.std() * math.sqrt(252)) if port_daily.std() > 0 else 0

    # 使用预存的 max_drawdown（百分比格式，直接使用）
    max_dd = summary["max_drawdown"].min() if "max_drawdown" in summary.columns else ((summary["total_value"] - summary["total_value"].cummax()) / summary["total_value"].cummax() * 100).min()



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





@st.cache_data(ttl=CACHE_TTL['medium'], show_spinner=False)



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

    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError):

        return pd.DataFrame()

@st.cache_data(ttl=CACHE_TTL['short'], show_spinner=False)



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

    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError):

        return pd.DataFrame()


def _render_peer_penetration_panel(code, sector):
    """Render peer ETF penetration: overlap matrix + ranking."""
    from data_loader import load_peer_penetration
    from config.settings import ETF_CATEGORIES
    cat_info = ETF_CATEGORIES.get(code, {})
    name = cat_info.get('name', code)
    pen = load_peer_penetration(code, name, sector)
    if pen is None:
        st.info('No peer data available')
        return

    st.markdown('**\u91cd\u4ed3\u80a1\u91cd\u53e0\u5ea6\u77e9\u9635**')
    if pen.overlap_results:
        overlap_rows = []
        for o in pen.overlap_results:
            peer_name = ETF_CATEGORIES.get(o.code_b, {}).get('name', o.code_b)
            overlap_rows.append({
                "code": o.code_b, "name": peer_name,
                "jaccard": f"{o.jaccard_index:.1%}",
                "common": len(o.common_stocks),
                "detail": o.overlap_detail,
            })
        odf = pd.DataFrame(overlap_rows)
        st.dataframe(odf, use_container_width=True, hide_index=True)
        # Show top overlap detail
        if pen.overlap_results:
            top_o = pen.overlap_results[0]
            if top_o.common_stocks:
                st.markdown(f'*Top \u91cd\u53e0: {ETF_CATEGORIES.get(top_o.code_b, {}).get("name", top_o.code_b)}*')
                for cs in top_o.common_stocks[:5]:
                    st.markdown(f"  - {cs["stock_name"]}: \u6301\u4ed3A {cs["weight_a"]:.1%} / \u6301\u4ed3B {cs["weight_b"]:.1%}")
    else:
        st.info('\u6682\u65e0\u91cd\u4ed3\u80a1\u6570\u636e\u7528\u4e8e\u91cd\u53e0\u5ea6\u8ba1\u7b97')

    st.markdown('---')
    st.markdown('**\u591a\u7ef4\u6392\u540d**')
    if pen.ranking_results and pen.target_rank:
        rank_rows = []
        for r in pen.ranking_results:
            mark = ' <<<' if r.code == code else ''
            rank_rows.append({
                "\u4ee3\u7801": r.code, "\u540d\u79f0": r.name,
                "\u89c4\u6a21(\u4ebf)": r.total_mv,
                "\u6298\u4ef7\u7387%": r.discount_rate,
                "\u6362\u624b\u7387%": r.turnover_rate,
                "\u8d44\u91d1\u6d41\u5165(\u4e07)": r.main_net_inflow,
                "\u91cf\u6bd4": r.volume_ratio,
                "\u7efc\u5408\u5f97\u5206": r.composite_rank,
            })
        rdf = pd.DataFrame(rank_rows)
        st.dataframe(rdf, use_container_width=True, hide_index=True)

def _render_signal_cross_validate(code):
    """Render signal cross-validation panel."""
    from data_loader import load_technical_signals
    from src.utils.database import get_db_connection
    import sqlite3, pandas as pd
    # Load tech score
    tech_df = load_technical_signals([code])
    tech_score = None
    if tech_df is not None and not tech_df.empty:
        row = tech_df.iloc[0]
        cols = ['ma_signal', 'macd_signal', 'rsi_status', 'kdj_signal', 'bollinger_position', 'trend']
        s = 50
        for col in cols:
            if col in row.index:
                v = str(row[col])
                if v in ['golden_cross', 'bullish', 'oversold', 'up']: s += 8
                elif v in ['death_cross', 'bearish', 'overbought', 'down']: s -= 8
        tech_score = max(0, min(100, s))
    # Load risk score
    risk_score = None
    conn = get_db_connection()
    try:
        rdf = pd.read_sql_query(
            "SELECT risk_score FROM etf_risk_scan WHERE code=? ORDER BY date DESC LIMIT 1",
            conn, params=[code]
        )
        if not rdf.empty:
            risk_score = float(rdf.iloc[0]['risk_score'])
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading risk score: {e}")
        pass
    except (KeyError, IndexError, TypeError) as e:
        logger.debug(f"Risk score parse error: {e}")
        pass
    finally:
        conn.close()
    # Load sentiment direction
    news_dir = None
    try:
        conn = get_db_connection()
        ndf = pd.read_sql_query(
            "SELECT AVG(sentiment_score) as avg_s FROM daily_news WHERE sentiment_score IS NOT NULL LIMIT 10",
            conn
        )
        if not ndf.empty and pd.notna(ndf.iloc[0]['avg_s']):
            avg_s = float(ndf.iloc[0]['avg_s'])
            news_dir = 1 if avg_s >= 0.6 else (-1 if avg_s <= 0.4 else 0)
        conn.close()
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logger.warning(f"DB error loading news sentiment: {e}")
        pass
    except (KeyError, IndexError, TypeError) as e:
        logger.debug(f"News sentiment parse error: {e}")
        pass
    # Cross validate
    from src.analysis.signal_cross_validate import cross_validate_signals
    result = cross_validate_signals(
        code, tech_score=tech_score, risk_score=risk_score, news_direction=news_dir
    )
    # Render
    if not result.signals:
        st.info('No signals available for cross-validation')
        return
    st.markdown(f'**{result.action}**')
    st.markdown(result.summary)
    # Signal details table
    sig_rows = []
    for s in result.signals:
        arrow = "\u2191" if s.direction > 0 else ("\u2193" if s.direction < 0 else "\u2192")
        dir_label = "\u770b\u591a" if s.direction > 0 else ("\u770b\u7a7a" if s.direction < 0 else "\u4e2d\u6027")
        sig_rows.append({
            "\u7ef4\u5ea6": s.dimension,
            "\u65b9\u5411": f"{arrow} {dir_label}",
            "\u5f97\u5206": f"{s.score:.1f}",
            "\u6743\u91cd": f"{s.weight:.0%}",
            "\u8bf4\u660e": s.detail,
        })
    st.dataframe(pd.DataFrame(sig_rows), use_container_width=True, hide_index=True)

# ============================================================
# P3: ERP股债性价比面板
# ============================================================

def _render_erp_panel():
    """渲染股债性价比(ERP)分析面板。"""
    import streamlit as st
    import data_loader as dl

    st.markdown("#### 股债性价比 (ERP)")
    st.caption("基于指数PE分位数 + 10Y国债收益率，判断股/债相对吸引力")

    results = dl.load_erp_analysis()
    if not results:
        st.info("暂无ERP数据，需要指数PE历史数据")
        return

    rows = []
    for r in results:
        signal_color = "🟢" if "偏多" in r.signal else ("🔴" if "偏空" in r.signal else "🟡")
        rows.append({
            "指数": r.index_name,
            "PE": r.current_pe,
            "盈利收益率%": r.earnings_yield,
            "无风险利率%": r.risk_free_rate,
            "ERP%": r.erp,
            "分位数%": r.erp_percentile,
            "信号": f"{signal_color} {r.signal}",
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # 详情展开
    for r in results:
        with st.expander(f"{r.index_name} - {r.signal}"):
            st.markdown(r.detail)


# ============================================================
# P3: 定投回测面板
# ============================================================

def _render_dca_panel(etf_code, etf_name=""):
    """渲染单只ETF定投回测面板。"""
    import streamlit as st
    import data_loader as dl

    st.markdown("#### 定投回测对比")
    if etf_name:
        st.caption(f"ETF: {etf_name}({etf_code})")

    col1, col2 = st.columns(2)
    with col1:
        period_amount = st.number_input("每期投入(元)", 100, 100000, 1000, 100, key="dca_amount")
    with col2:
        freq = st.selectbox("定投频率", ["W", "2W", "ME"], format_func={
            "W": "每周", "2W": "每两周", "ME": "每月"
        }.get, key="dca_freq")

    result = dl.load_dca_backtest(etf_code, period_amount, freq)
    if result is None:
        st.info("暂无定投回测数据，需要该ETF的价格历史")
        return

    # 核心指标
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("总投入", f"{result.total_invest:,.0f}")
    c2.metric("最终市值", f"{result.final_value:,.0f}")
    c3.metric("总收益率", f"{result.total_return_pct:+.2f}%")
    c4.metric("最大回撤", f"{result.max_drawdown_pct:.2f}%")

    if result.annual_return_pct != 0:
        c5, c6 = st.columns(2)
        c5.metric("年化收益", f"{result.annual_return_pct:+.2f}%")
        if result.sharpe_ratio != 0:
            c6.metric("夏普比率", f"{result.sharpe_ratio:.2f}")


# ============================================================
# P3: 行业景气度面板
# ============================================================

def _render_industry_boom_panel(etf_code):
    """渲染行业景气度面板。"""
    import streamlit as st
    import data_loader as dl

    result = dl.load_industry_boom(etf_code)
    if result is None:
        st.info("该ETF暂无行业景气度数据")
        return

    st.markdown(f"#### 行业景气度: {result.industry}")
    signal_color = "🟢" if result.boom_score >= 65 else ("🔴" if result.boom_score < 35 else "🟡")

    # 总分
    c1, c2 = st.columns([1, 3])
    c1.metric("综合景气度", f"{result.boom_score:.0f}/100")
    c2.markdown(f"**信号**: {signal_color} {result.signal}")

    # 四维评分
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("资金面", f"{result.fund_score:.0f}")
    c2.metric("估值面", f"{result.valuation_score:.0f}")
    c3.metric("技术面", f"{result.trend_score:.0f}")
    c4.metric("政策面", f"{result.policy_score:.0f}")

    # 看多/看空理由
    if result.top_reasons:
        st.markdown("**利好因素:**")
        for r in result.top_reasons:
            st.markdown(f"- {r}")
    if result.risk_reasons:
        st.markdown("**风险因素:**")
        for r in result.risk_reasons:
            st.markdown(f"- {r}")


# ============================================================
# P3: 智能预警面板
# ============================================================

def _render_smart_alert_panel(etf_code, etf_name=""):
    """渲染单只ETF智能预警面板。"""
    import streamlit as st
    import data_loader as dl

    events = dl.load_smart_alerts(etf_code, etf_name)
    if not events:
        st.info(f"当前无活跃预警 ({etf_name or etf_code})")
        return

    st.markdown(f"#### 智能预警: {etf_name or etf_code}")
    st.markdown(f"共 **{len(events)}** 条预警")

    level_colors = {"紧急": "red", "重要": "orange", "关注": "blue", "信息": "gray"}
    for event in events:
        color = level_colors.get(event.level, "gray")
        with st.container():
            cols = st.columns([1, 2, 4, 3])
            cols[0].markdown(f"**[{event.level}]**")
            cols[1].markdown(f"**{event.alert_type}**")
            cols[2].markdown(event.detail)
            cols[3].markdown(event.action_hint)
        st.divider()


def _render_alert_summary_panel():
    """渲染所有持仓预警汇总面板。"""
    import streamlit as st
    import data_loader as dl

    st.markdown("#### 全持仓预警汇总")
    summary = dl.load_all_smart_alerts()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("总预警", summary.total)
    c2.metric("🔴 紧急", summary.urgent)
    c3.metric("🟠 重要", summary.important)
    c4.metric("🔵 关注", summary.watch)
    c5.metric("⚪ 信息", summary.info)

    if summary.events:
        from src.analysis.smart_alert import format_alert_text
        for event in summary.events[:20]:  # 最多显示20条
            st.markdown(format_alert_text(event))
