"""Tab11: 黄金市场"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from tabs.gold_components.price_comparison import render_price_comparison
from tabs.gold_components.seasonality import render_seasonality
from tabs.gold_components.reserve_analysis import render_reserve_analysis
from tabs.gold_components.technical_signals import render_technical_signals
from tabs.gold_components.correlation import render_correlation
from tabs.gold_components.realtime_quotes import render_realtime_quotes
from tabs.gold_components.central_bank_trends import render_central_bank_trends
from tabs.gold_components.supply_demand import render_supply_demand
from tabs.gold_components.international_comparison import render_international_comparison

def render_tab11(positions, summary, index_quotes=None, selected_date=None, selected_benchmark=None, **kwargs):
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">黄金市场分析'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '金价走势、技术信号、基准价对比、季节性、储备分析、定价因子、实时行情、央行购金、供需平衡、国际金价。</span></div>',
        unsafe_allow_html=True,
    )
    # 预加载共享数据源（并发 I/O，首次 ~22s，后续命中缓存 ~0s）
    from tabs.gold_components.gold_preloader import preload_gold_data
    preload_gold_data()

    tabs = st.tabs([
        "\U0001f4c8 金价走势",
        "\U0001f4ca 技术信号",
        "\u2696\ufe0f 基准价对比",
        "\U0001f4c8 季节性规律",
        "\U0001f3e6 储备分析",
        "\U0001f517 定价因子",
        "\U0001f4e1 实时行情",
        "\U0001f3e6 央行购金",
        "\u2696\ufe0f 供需平衡",
        "\U0001f310 国际金价",
    ])
    with tabs[0]: _render_gold_price_trend()
    with tabs[1]: render_technical_signals()
    with tabs[2]: render_price_comparison()
    with tabs[3]: render_seasonality()
    with tabs[4]: render_reserve_analysis()
    with tabs[5]: render_correlation()
    with tabs[6]: render_realtime_quotes()
    with tabs[7]: render_central_bank_trends()
    with tabs[8]: render_supply_demand()
    with tabs[9]: render_international_comparison()

def _render_gold_price_trend():
    from tabs.gold_components.realtime_quotes import SYMBOL_GROUPS
    all_options = []
    for gname, gsyms in SYMBOL_GROUPS.items():
        au_syms = [s for s in gsyms if s.startswith("Au")]
        if au_syms:
            all_options.append(f"--- {gname} ---")
            all_options.extend(au_syms)
    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        sel = st.selectbox("品种选择", all_options, index=1, key="gold_symbol")
        if sel.startswith("---"): sel = "Au99.99"
    with c2:
        gold_period = st.selectbox("周期选择", ["近30天", "近90天", "近180天", "近1年", "全部"], key="gold_period")
    with c3:
        show_ma = st.checkbox("显示均线", value=True, key="gold_show_ma")
    try:
        from tabs.gold_components.gold_utils import fetch_sge_hist
        gold_df = fetch_sge_hist(symbol=sel)
        if gold_df is not None and not gold_df.empty:
            gold_df["date"] = pd.to_datetime(gold_df["date"])
            pmap = {"近30天": 30, "近90天": 90, "近180天": 180, "近1年": 365, "全部": 99999}
            nd = pmap.get(gold_period, 90)
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=nd)
            pdf = gold_df[gold_df["date"] >= cutoff].copy()
            if not pdf.empty:
                if show_ma:
                    pdf["MA5"] = pdf["close"].rolling(5).mean()
                    pdf["MA20"] = pdf["close"].rolling(20).mean()
                    pdf["MA60"] = pdf["close"].rolling(60).mean()
                fig = go.Figure()
                fig.add_trace(go.Candlestick(x=pdf["date"], open=pdf["open"], high=pdf["high"], low=pdf["low"], close=pdf["close"], name="K线", increasing_line_color="#ef5350", decreasing_line_color="#26a69a"))
                if show_ma:
                    for mn, mc, ml in [("MA5","MA5","#FFD700"),("MA20","MA20","#FF69B4"),("MA60","MA60","#00CED1")]:
                        if pdf[mc].notna().sum() > 0:
                            fig.add_trace(go.Scatter(x=pdf["date"], y=pdf[mc], mode="lines", name=mn, line=dict(color=ml, width=1)))
                fig.update_layout(title=dict(text=f"{sel} 日K线走势", font=dict(size=14)), xaxis_rangeslider_visible=False, height=450, xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"), plot_bgcolor="#1a1a2e", paper_bgcolor="#1a1a2e", font=dict(color="#ddd"), margin=dict(l=50, r=30, t=40, b=30), legend=dict(orientation="h", yanchor="bottom", y=1.02))
                st.plotly_chart(fig, width='stretch')
                latest = gold_df.iloc[-1]
                prev = gold_df.iloc[-2] if len(gold_df) > 1 else None
                lc = float(latest["close"])
                ld = latest["date"]
                if prev is not None:
                    pc = float(prev["close"])
                    chg = lc - pc
                    cpct = chg / pc * 100
                    cs = "+" if chg >= 0 else ""
                else:
                    chg, cpct, cs = 0, 0, ""
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("最新价", f"\u00a5{lc:.2f}", f"{cs}{chg:.2f} ({cs}{cpct:.2f}%)")
                if prev is not None:
                    m2.metric("最高", f"\u00a5{float(latest['high']):.2f}")
                    m3.metric("最低", f"\u00a5{float(latest['low']):.2f}")
                m4.metric("日期", str(ld))
            else:
                st.info("所选周期内无数据")
        else:
            st.warning("暂无历史数据")
    except Exception as e:
        st.info(f"金价走势模块暂不可用: {str(e)[:80]}")
