"""
Tab11: 黄金市场
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from tabs.gold_components.price_comparison import render_price_comparison
from tabs.gold_components.seasonality import render_seasonality
from tabs.gold_components.reserve_analysis import render_reserve_analysis

def render_tab11(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    """渲染Tab11: 黄金市场"""
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">黄金市场分析'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '上海金交所金价走势、基准价对比、季节性规律、储备分析、实时行情及SPDR持仓。</span></div>',
        unsafe_allow_html=True,
    )

    gt1, gt2, gt3, gt4, gt5 = st.tabs([
        "📈 金价走势", "⚖️ 基准价对比", "📊 季节性规律", "🏦 储备分析", "📡 实时行情",
    ])

    with gt1:
        _render_gold_price_trend()
    with gt2:
        render_price_comparison()
    with gt3:
        render_seasonality()
    with gt4:
        render_reserve_analysis()
    with gt5:
        _render_realtime_and_holdings()

def _render_gold_price_trend():
    """子Tab1: SGE金价走势"""
    col_gt1_ctrl = st.columns([2, 2, 2])
    with col_gt1_ctrl[0]:
        gold_symbol = st.selectbox("品种选择", ["Au99.99", "Au99.95", "Au(T+D)", "mAu(T+D)"], key="gold_symbol")
    with col_gt1_ctrl[1]:
        gold_period = st.selectbox("周期选择", ["近30天", "近90天", "近180天", "近1年", "全部"], key="gold_period")
    with col_gt1_ctrl[2]:
        show_ma = st.checkbox("显示均线", value=True, key="gold_show_ma")

    try:
        import akshare as ak
        gold_df = ak.spot_hist_sge(symbol=gold_symbol)
        if gold_df is not None and not gold_df.empty:
            gold_df["date"] = pd.to_datetime(gold_df["date"])
            period_map = {"近30天": 30, "近90天": 90, "近180天": 180, "近1年": 365, "全部": 99999}
            n_days = period_map.get(gold_period, 90)
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=n_days)
            plot_df = gold_df[gold_df["date"] >= cutoff].copy()
            if not plot_df.empty:
                if show_ma:
                    plot_df["MA5"] = plot_df["close"].rolling(5).mean()
                    plot_df["MA20"] = plot_df["close"].rolling(20).mean()
                    plot_df["MA60"] = plot_df["close"].rolling(60).mean()
                fig_kline = go.Figure()
                fig_kline.add_trace(go.Candlestick(
                    x=plot_df["date"], open=plot_df["open"], high=plot_df["high"],
                    low=plot_df["low"], close=plot_df["close"], name="K线",
                    increasing_line_color="#ef5350", decreasing_line_color="#26a69a",
                ))
                if show_ma:
                    for ma_name, ma_col, ma_color in [("MA5", "MA5", "#FFD700"), ("MA20", "MA20", "#FF69B4"), ("MA60", "MA60", "#00CED1")]:
                        if plot_df[ma_col].notna().sum() > 0:
                            fig_kline.add_trace(go.Scatter(
                                x=plot_df["date"], y=plot_df[ma_col],
                                mode="lines", name=ma_name, line=dict(color=ma_color, width=1),
                            ))
                fig_kline.update_layout(
                    title=dict(text=f"{gold_symbol} 日K线走势", font=dict(size=14)),
                    xaxis_rangeslider_visible=False, height=450,
                    xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
                    plot_bgcolor="#1a1a2e", paper_bgcolor="#1a1a2e", font=dict(color="#ddd"),
                    margin=dict(l=50, r=30, t=40, b=30),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_kline, use_container_width=True)

                latest = gold_df.iloc[-1]
                prev = gold_df.iloc[-2] if len(gold_df) > 1 else None
                latest_close = float(latest["close"])
                latest_date = latest["date"]
                if prev is not None:
                    prev_close = float(prev["close"])
                    chg = latest_close - prev_close
                    chg_pct = chg / prev_close * 100
                    chg_sign = "+" if chg >= 0 else ""
                else:
                    chg, chg_pct, chg_sign = 0, 0, ""
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("最新价", f"¥{latest_close:.2f}", f"{chg_sign}{chg:.2f} ({chg_sign}{chg_pct:.2f}%)")
                if prev is not None:
                    c2.metric("最高", f"¥{float(latest['high']):.2f}")
                    c3.metric("最低", f"¥{float(latest['low']):.2f}")
                c4.metric("日期", str(latest_date))
            else:
                st.info("所选周期内无数据")
        else:
            st.warning("暂无黄金历史数据")
    except Exception as e:
        st.info(f"金价走势模块暂不可用: {str(e)[:80]}")



def _render_realtime_and_holdings():
    """子Tab5: 实时行情 + SPDR持仓 + 中国黄金储备"""
    try:
        import requests
        sge_url = "https://www.sge.com.cn/api/market/realPrice"
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.sge.com.cn/"}
        resp = requests.get(sge_url, headers=headers, timeout=10)
        if resp.status_code == 200 and resp.text.strip():
            rt_data = resp.json()
            if rt_data and isinstance(rt_data, list) and len(rt_data) > 0:
                rt_df = pd.DataFrame(rt_data)
                st.dataframe(rt_df.tail(50), use_container_width=True, height=400, hide_index=True)
            else:
                st.info("当前非交易时段，暂无实时行情数据。上海金交所交易时间：周一至周五 09:50-15:30（夜盘 19:50-02:30）")
        else:
            st.info("当前非交易时段，暂无实时行情数据。上海金交所交易时间：周一至周五 09:50-15:30（夜盘 19:50-02:30）")
    except Exception:
        st.info("实时行情暂不可用（当前非交易时段或接口维护中）")

    st.markdown("---")
    st.subheader("🏦 黄金储备与持仓")
    bottom_left, bottom_right = st.columns(2)

    with bottom_left:
        st.markdown("**SPDR Gold Trust 持仓趋势**")
        try:
            import akshare as ak
            spdr_df = ak.macro_cons_gold()
            if spdr_df is not None and not spdr_df.empty:
                spdr_df.columns = [c.strip() for c in spdr_df.columns]
                date_col = stock_col = change_col = None
                for c in spdr_df.columns:
                    if "日期" in c or "date" in c.lower():
                        date_col = c
                    if "总库存" in c or "库存" in c:
                        stock_col = c
                    if "增持" in c or "减持" in c or "变化" in c:
                        change_col = c
                if date_col and stock_col:
                    spdr_df[date_col] = pd.to_datetime(spdr_df[date_col], errors="coerce")
                    spdr_df = spdr_df.dropna(subset=[date_col]).sort_values(date_col).tail(180)
                    fig_spdr = go.Figure()
                    fig_spdr.add_trace(go.Bar(
                        x=spdr_df[date_col], y=spdr_df[stock_col],
                        name="总库存(吨)", marker_color="#FFD700", opacity=0.7,
                    ))
                    if change_col:
                        fig_spdr.add_trace(go.Scatter(
                            x=spdr_df[date_col], y=spdr_df[change_col],
                            name="增减持(吨)", mode="lines",
                            line=dict(color="#00BCD4", width=1.5), yaxis="y2",
                        ))
                    spdr_layout = dict(
                        height=350,
                        xaxis=dict(gridcolor="#333", tickformat="%Y-%m"),
                        yaxis=dict(title="总库存(吨)", title_font_color="#FFD700", gridcolor="#333", tickfont=dict(color="#ddd")),
                        plot_bgcolor="#1a1a2e", paper_bgcolor="#1a1a2e", font=dict(color="#ddd"),
                        margin=dict(l=50, r=60, t=20, b=30),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    if change_col:
                        spdr_layout["yaxis2"] = dict(
                            title="增减持(吨)", title_font_color="#00BCD4",
                            overlaying="y", side="right", gridcolor="#333", tickfont=dict(color="#ddd"),
                        )
                    fig_spdr.update_layout(**spdr_layout)
                    st.plotly_chart(fig_spdr, use_container_width=True)
                else:
                    st.info("SPDR数据列名不匹配")
                    st.dataframe(spdr_df.tail(5))
            else:
                st.info("暂无SPDR持仓数据")
        except Exception as e:
            st.info(f"SPDR持仓模块暂不可用: {str(e)[:80]}")

    with bottom_right:
        st.markdown("**中国黄金储备**")
        try:
            import akshare as ak
            cn_gold = ak.macro_china_fx_gold()
            if cn_gold is not None and not cn_gold.empty:
                cn_gold.columns = [c.strip() for c in cn_gold.columns]
                date_col = gold_col = yoy_col = None
                for c in cn_gold.columns:
                    if "月份" in c or "日期" in c:
                        date_col = c
                    if "黄金储备" in c and "数值" in c:
                        gold_col = c
                    if "黄金储备" in c and "同比" in c:
                        yoy_col = c
                if date_col and gold_col:
                    _match = cn_gold[date_col].str.extract(r"(\d{4})年(\d{2})月份")
                    cn_gold["_ym_str"] = _match[0] + "-" + _match[1]
                    cn_gold[date_col] = pd.to_datetime(cn_gold["_ym_str"], format="%Y-%m")
                    cn_gold = cn_gold.drop(columns=["_ym_str"])
                    cn_gold = cn_gold.dropna(subset=[date_col]).sort_values(date_col).tail(60)
                    fig_cng = go.Figure()
                    fig_cng.add_trace(go.Bar(
                        x=cn_gold[date_col], y=cn_gold[gold_col],
                        name="黄金储备(万盎司)", marker_color="#FFA726", opacity=0.8,
                    ))
                    if yoy_col and yoy_col in cn_gold.columns:
                        fig_cng.add_trace(go.Scatter(
                            x=cn_gold[date_col], y=cn_gold[yoy_col],
                            name="同比(%)", mode="lines",
                            line=dict(color="#66BB6A", width=2), yaxis="y2",
                        ))
                    fig_cng.update_layout(
                        height=350,
                        xaxis=dict(gridcolor="#333", tickformat="%Y-%m"),
                        yaxis=dict(title="黄金储备(万盎司)", title_font_color="#FFA726", gridcolor="#333", tickfont=dict(color="#ddd")),
                        plot_bgcolor="#1a1a2e", paper_bgcolor="#1a1a2e", font=dict(color="#ddd"),
                        margin=dict(l=50, r=60, t=20, b=30),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(fig_cng, use_container_width=True)
                else:
                    st.info("中国黄金储备数据列名不匹配")
                    st.dataframe(cn_gold.tail(5))
            else:
                st.info("暂无中国黄金储备数据")
        except Exception as e:
            st.info(f"中国黄金储备模块暂不可用: {str(e)[:80]}")
