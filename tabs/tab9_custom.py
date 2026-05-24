"""
Tab9: 自定义指标
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection


def render_tab9(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)
    """渲染Tab9: 自定义指标"""
    
    st.caption("🔬 自定义技术指标组合回测，K线形态识别，量化验证交易策略")

    tab9_sub1, tab9_sub2, tab9_sub3 = st.tabs(["📊 指标回测", "🕯️ K线形态", "📈 回测历史"])

    # ----- 指标回测子Tab -----
    with tab9_sub1:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">指标信号回测<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">选择预置模板或自定义指标条件，对持仓ETF进行历史信号回测。</span></div>',
            unsafe_allow_html=True,
        )

        try:
            from src.analysis.indicator_backtest import (
                INDICATOR_TEMPLATES,
                backtest_technical_composite,
            )

            # 模板选择
            template_names = [t["name"] for t in INDICATOR_TEMPLATES]
            selected_tmpl = st.selectbox("选择指标模板", template_names, key="ind_tmpl_sel")
            tmpl = next(t for t in INDICATOR_TEMPLATES if t["name"] == selected_tmpl)
            st.caption(tmpl["description"])

            # 选择ETF
            if not positions.empty:
                etf_options = {f"{row['name']}({row['code']})": str(row["code"]) for _, row in positions.iterrows()}
                selected_etf = st.selectbox("选择ETF", list(etf_options.keys()), key="ind_etf_sel")
                etf_code = etf_options[selected_etf]
            else:
                st.info("暂无持仓数据")
                etf_code = None

            # 回测参数
            col_bt1, col_bt2 = st.columns(2)
            with col_bt1:
                hold_days = st.slider("持有天数", 1, 30, 5, key="ind_hold")
            with col_bt2:
                lookback = st.selectbox("回溯天数", [90, 180, 250, 500], index=2, key="ind_lookback")

            if etf_code and st.button("🚀 开始回测", key="ind_run_bt", type="primary"):
                with st.spinner("正在回测..."):
                    conn_bt = get_db_connection()
                    try:
                        result = backtest_technical_composite(conn_bt, etf_code, tmpl["formula"], lookback=lookback)
                    finally:
                        conn_bt.close()

                if "error" in result:
                    st.warning(result["error"])
                else:
                    # 结果展示
                    col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                    with col_r1:
                        st.metric("总信号数", result["total_signals"])
                    with col_r2:
                        wr = result["win_rate"]
                        wr_color = "#22c55e" if wr >= 50 else "#ef4444"
                        st.metric("胜率", f"{wr}%", delta_color="normal" if wr >= 50 else "inverse")
                    with col_r3:
                        st.metric("平均收益", f"{result['avg_return_pct']:+.2f}%")
                    with col_r4:
                        pf = result["profit_factor"]
                        st.metric("盈亏比", f"{pf:.2f}" if pf != float("inf") else "∞")

                    st.metric(
                        "最大单次收益",
                        f"{result['max_return_pct']:+.2f}%",
                        delta=f"最大亏损: {result['max_loss_pct']:+.2f}%",
                    )

                    # 信号明细
                    details = result.get("signals_detail", [])
                    if details:
                        st.markdown(
                            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">最近信号记录<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示所选指标最近触发的买卖信号及其后续收益表现。</span></div>',
                            unsafe_allow_html=True,
                        )
                        detail_df = pd.DataFrame(details)
                        detail_df["return_pct"] = detail_df["return_pct"].apply(
                            lambda x: f'<span style="color:{"#22c55e" if x>0 else "#ef4444"}">{x:+.2f}%</span>'
                        )
                        col_map = {"date": "日期", "entry": "买入价", "exit": "卖出价", "return_pct": "收益率"}
                        detail_df = detail_df.rename(columns={k: v for k, v in col_map.items() if k in detail_df.columns})
                        st.markdown(detail_df.to_html(index=False, escape=False), unsafe_allow_html=True)

                    # 收益分布图
                    if len(details) >= 3:
                        fig_bt = go.Figure(
                            go.Bar(
                                x=[d["date"][-5:] for d in details],
                                y=[d["return_pct"] for d in details],
                                marker_color=["#22c55e" if d["return_pct"] > 0 else "#ef4444" for d in details],
                                text=[f"{d['return_pct']:+.1f}%" for d in details],
                                textposition="auto",
                                textfont=dict(size=9, color="#c9d1d9"),
                            )
                        )
                        fig_bt.add_hline(y=0, line_dash="dash", line_color="#484f58")
                        fig_bt.update_layout(
                            xaxis=dict(
                                title="信号日期", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                            ),
                            yaxis=dict(title="收益%", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=280,
                            margin=dict(l=40, r=20, t=10, b=30),
                            bargap=0.3,
                        )
                        st.plotly_chart(fig_bt, width="stretch")

        except Exception as e:
            st.info(f"指标回测模块暂不可用: {str(e)[:80]}")

    # ----- K线形态识别子Tab -----
    with tab9_sub2:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">K线形态识别<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">自动识别常见K线反转和持续形态，辅助判断市场转折点。</span></div>',
            unsafe_allow_html=True,
        )

        try:
            from src.analysis.candle_patterns import (
                PATTERN_NAME_MAP,
                PATTERN_SIGNAL,
                detect_candle_patterns,
            )

            if not positions.empty:
                etf_opt2 = {f"{row['name']}({row['code']})": str(row["code"]) for _, row in positions.iterrows()}
                sel_etf2 = st.selectbox("选择ETF", list(etf_opt2.keys()), key="candle_etf_sel")
                candle_code = etf_opt2[sel_etf2]
            else:
                st.info("暂无持仓数据")
                candle_code = None

            if candle_code:
                n_candle = st.slider("显示天数", 20, 120, 60, key="candle_days")

                conn_ck = get_db_connection()
                try:
                    snaps = pd.read_sql_query(
                        """
                        SELECT date, current_price AS close
                        FROM portfolio_snapshots
                        WHERE code = ?
                        ORDER BY date DESC
                        LIMIT ?
                    """,
                        conn_ck,
                        params=[candle_code, n_candle],
                    )
                finally:
                    conn_ck.close()

                if not snaps.empty:
                    snaps = snaps.sort_values("date").reset_index(drop=True)

                    # 基于收盘价合成 OHLC 数据
                    # open = 前一日close（首日 open=close）
                    snaps["open"] = snaps["close"].shift(1).fillna(snaps["close"])
                    # high = max(open, close) * (1 + 微小随机波动)
                    snaps["high"] = snaps[["open", "close"]].max(axis=1) * 1.003
                    # low = min(open, close) * (1 - 微小随机波动)
                    snaps["low"] = snaps[["open", "close"]].min(axis=1) * 0.997
                    # 将 high/low 限制在合理范围
                    snaps["high"] = snaps[["high", "close"]].max(axis=1)
                    snaps["low"] = snaps[["low", "close"]].min(axis=1)

                    ohlc = detect_candle_patterns(snaps)

                    # 筛选有形态的行
                    pattern_rows = ohlc[ohlc["pattern"] != ""]
                    if not pattern_rows.empty:
                        # 统计
                        pat_count = {}
                        for p in pattern_rows["pattern"]:
                            for name in p.split(","):
                                cn = PATTERN_NAME_MAP.get(name.strip(), name)
                                pat_count[cn] = pat_count.get(cn, 0) + 1

                        col_pc1, col_pc2 = st.columns([1, 2])
                        with col_pc1:
                            st.markdown(
                                '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">形态统计<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">统计各K线形态在所选周期内的出现次数及其信号方向。</span></div>',
                                unsafe_allow_html=True,
                            )
                            for pname, cnt in sorted(pat_count.items(), key=lambda x: -x[1]):
                                sig = PATTERN_SIGNAL.get(pname, "neutral")
                                icon = "🟢" if sig == "bullish" else ("🔴" if sig == "bearish" else "⚪")
                                st.markdown(f"{icon} {pname}: **{cnt}** 次")

                        with col_pc2:
                            # K线图
                            fig_k = go.Figure(
                                data=[
                                    go.Candlestick(
                                        x=ohlc["date"],
                                        open=ohlc["open"],
                                        high=ohlc["high"],
                                        low=ohlc["low"],
                                        close=ohlc["close"],
                                        increasing_line_color="#22c55e",
                                        decreasing_line_color="#ef4444",
                                    )
                                ]
                            )
                            # 标记形态位置
                            for _, row in pattern_rows.iterrows():
                                sig = "bullish"
                                for p in row["pattern"].split(","):
                                    ps = PATTERN_SIGNAL.get(p.strip(), "neutral")
                                    if ps == "bearish":
                                        sig = "bearish"
                                color = "#22c55e" if sig == "bullish" else "#ef4444"
                                name_str = PATTERN_NAME_MAP.get(row["pattern"].split(",")[0].strip(), "")
                                fig_k.add_annotation(
                                    x=row["date"],
                                    y=row["high"] * 1.005,
                                    text=f"▼ {name_str}" if sig == "bearish" else f"▲ {name_str}",
                                    showarrow=False,
                                    font=dict(size=9, color=color),
                                )

                            fig_k.update_layout(
                                xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                yaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#0d1117",
                                height=max(350, n_candle * 4),
                                margin=dict(l=40, r=20, t=10, b=30),
                                xaxis_rangeslider_visible=False,
                            )
                            st.plotly_chart(fig_k, width="stretch")
                    else:
                        st.info(f"近 {n_candle} 日未检测到经典K线形态")
        except Exception as e:
            st.info(f"K线形态识别暂不可用: {str(e)[:80]}")
    # ----- 回测历史子Tab -----
    with tab9_sub3:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">回测历史记录</div>',
            unsafe_allow_html=True,
        )
        try:
            conn_h = get_db_connection()
            try:
                hdf = pd.read_sql_query("""
                    SELECT br.id, ci.name as indicator_name, ci.description,
                           br.test_period, br.total_signals, br.win_count, br.loss_count,
                           br.win_rate, br.avg_pnl, br.sharpe, br.created_at
                    FROM indicator_backtest_results br
                    JOIN custom_indicators ci ON br.indicator_id = ci.id
                    ORDER BY br.created_at DESC
                """, conn_h)
            finally:
                conn_h.close()
            if hdf.empty:
                st.info("暂无回测历史记录")
            else:
                c1, c2, c3 = st.columns(3)
                c1.metric("回测记录", len(hdf))
                c2.metric("平均胜率", f"{hdf['win_rate'].mean():.1f}%")
                c3.metric("最佳胜率", f"{hdf['win_rate'].max():.1f}%")
                # 各指标胜率对比
                ist = hdf.groupby('indicator_name').agg(
                    avg_wr=('win_rate', 'mean'), cnt=('id', 'count'),
                    avg_pnl=('avg_pnl', 'mean')
                ).sort_values('avg_wr', ascending=True).reset_index()
                colors = ['#22c55e' if v >= 50 else '#f59e0b' if v >= 45 else '#ef4444' for v in ist['avg_wr']]
                fig_wr = go.Figure(go.Bar(
                    x=ist['avg_wr'], y=ist['indicator_name'], orientation='h',
                    marker_color=colors,
                    text=[f"{v:.1f}%" for v in ist['avg_wr']], textposition='auto',
                ))
                fig_wr.update_layout(height=max(200, len(ist)*35), margin=dict(l=130, r=20, t=5, b=10),
                                     xaxis_title="平均胜率(%)", xaxis_range=[0, 70],
                                     yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_wr, width='stretch')
                # 胜率箱线图
                fig_box = go.Figure()
                for _, row in ist.iterrows():
                    yd = hdf[hdf['indicator_name'] == row['indicator_name']]['win_rate']
                    fig_box.add_trace(go.Box(y=yd, name=row['indicator_name']))
                fig_box.update_layout(height=280, margin=dict(l=20, r=20, t=5, b=60),
                                      boxmode='group', yaxis_title="胜率(%)",
                                      legend=dict(orientation="h", yanchor="bottom", y=-0.3, font=dict(size=9)))
                st.plotly_chart(fig_box, width='stretch')
                # 明细表
                sel = st.selectbox("筛选指标", ["全部"] + sorted(hdf['indicator_name'].unique()), key="hist_sel")
                sdf = hdf if sel == "全部" else hdf[hdf['indicator_name'] == sel]
                disp = sdf[['indicator_name', 'test_period', 'total_signals', 'win_count', 'loss_count', 'win_rate', 'avg_pnl', 'created_at']].copy()
                disp['win_rate'] = disp['win_rate'].apply(lambda x: f"{x:.1f}%")
                disp['avg_pnl'] = disp['avg_pnl'].apply(lambda x: f"{x:+.3f}%")
                disp['created_at'] = disp['created_at'].str[:16]
                st.dataframe(disp, use_container_width=True, hide_index=True, height=350)
        except Exception as e:
            st.info(f"回测历史加载失败: {str(e)[:80]}")


    


