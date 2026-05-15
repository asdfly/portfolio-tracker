"""
Tab10: 资金动向
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection


def render_tab10(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)
    """渲染Tab10: 资金动向"""
    
    st.caption("💰 行业/ETF资金流向分析，追踪主力资金动态，辅助判断市场热点切换")

    tab10_sub1, tab10_sub2, tab10_sub3 = st.tabs(["📊 行业资金流", "📈 ETF资金流", "💰 主力资金"])

    # ----- 行业资金流 -----
    with tab10_sub1:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">行业资金流向<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">各行业板块主力资金净流入/流出排名与趋势。</span></div>',
            unsafe_allow_html=True,
        )

        try:
            conn_ff = get_db_connection()
            try:
                sector_df = pd.read_sql_query(
                    """
                    SELECT date, name, code, net_inflow
                    FROM fund_flows
                    WHERE category = 'sector'
                    ORDER BY date DESC, net_inflow DESC
                """,
                    conn_ff,
                )
            finally:
                conn_ff.close()

            if not sector_df.empty:
                # 去除重复行业（同板块不同层级代码数据相同，只保留Ⅱ）
                sector_df = sector_df[
                    ~sector_df["code"].isin(
                        {"BK1366", "BK1471"}  # 证券Ⅲ→保留Ⅱ(BK0473), 工程咨询服务Ⅲ→保留Ⅱ(BK0726)
                    )
                ].copy()

                # 最新日期的行业排名
                latest_date = sector_df["date"].iloc[0]
                latest = sector_df[sector_df["date"] == latest_date].head(20)

                if not latest.empty:
                    fig_sf = go.Figure(
                        go.Bar(
                            orientation="h",
                            y=latest["name"],
                            x=latest["net_inflow"] / 1e8,
                            marker_color=["#22c55e" if v > 0 else "#ef4444" for v in latest["net_inflow"] / 1e8],
                            text=[f"{v/1e8:.1f}亿" for v in latest["net_inflow"]],
                            textposition="auto",
                            textfont=dict(size=9, color="#c9d1d9"),
                        )
                    )
                    fig_sf.update_layout(
                        title=f"<span style='font-size:12px;color:#8b949e'>{latest_date} 行业资金净流入TOP20</span>",
                        xaxis=dict(
                            title="净流入(亿元)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                        ),
                        yaxis=dict(title="", tickfont=dict(size=10, color="#c9d1d9")),
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=max(400, 22 * len(latest)),
                        margin=dict(l=80, r=30, t=35, b=30),
                        bargap=0.2,
                    )
                    st.plotly_chart(fig_sf, width="stretch")
                    # TOP10行业资金净流入时间趋势
                    if sector_df["date"].nunique() >= 3:
                        # 只选至少有30天数据的行业，避免单日行业被选中导致趋势线无意义
                        days_per_name = sector_df.groupby("name")["date"].nunique()
                        qualified = days_per_name[days_per_name >= 10].index
                        if len(qualified) > 0:
                            trend_df = sector_df[sector_df["name"].isin(qualified)].copy()
                            # 按最近30天累计净流入排序选TOP10
                            recent_cutoff = sorted(trend_df["date"].unique())[
                                -min(30, trend_df["date"].nunique()) :
                            ]
                            recent_sum = (
                                trend_df[trend_df["date"].isin(recent_cutoff)].groupby("name")["net_inflow"].sum()
                            )
                            top10_names = recent_sum.nlargest(10).index.tolist()
                            trend_df = trend_df[trend_df["name"].isin(top10_names)].copy()
                            trend_df["net_inflow_yi"] = trend_df["net_inflow"] / 1e8

                            fig_trend = go.Figure()
                            for name in top10_names:
                                sub = trend_df[trend_df["name"] == name].sort_values("date")
                                fig_trend.add_trace(
                                    go.Scatter(
                                        x=sub["date"],
                                        y=sub["net_inflow_yi"],
                                        name=name,
                                        mode="lines",
                                        line=dict(width=1.5),
                                    )
                                )
                            fig_trend.add_hline(y=0, line_dash="dash", line_color="#484f58")
                            fig_trend.update_layout(
                                title="<span style='font-size:12px;color:#8b949e'>TOP10行业资金净流入趋势(亿元)</span>",
                                yaxis=dict(
                                    title="净流入(亿元)",
                                    gridcolor="#21262d",
                                    tickfont=dict(size=9, color="#8b949e"),
                                ),
                                xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#0d1117",
                                height=400,
                                margin=dict(l=50, r=30, t=50, b=30),
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.02,
                                    font=dict(size=9, color="#8b949e"),
                                    groupclick="toggleitem",
                                ),
                            )
                            st.plotly_chart(fig_trend, width="stretch")
                # 多日趋势热力图
                if sector_df["date"].nunique() >= 3:
                    # 取最近30个交易日
                    recent_dates = sorted(sector_df["date"].unique(), reverse=True)[:30]
                    heat_df = sector_df[sector_df["date"].isin(recent_dates)].copy()

                    pivot = heat_df.pivot_table(index="name", columns="date", values="net_inflow", aggfunc="sum")
                    # 过滤数据稀疏行业: 至少覆盖一半日期，避免NaN过多导致热力图失真
                    min_coverage = max(5, len(pivot.columns) // 2)
                    valid_mask = pivot.notna().sum(axis=1) >= min_coverage
                    pivot = pivot.loc[valid_mask]
                    if not pivot.empty:
                        # 按最近5日日均净流入排序，正负各半选取更有对比度
                        daily_avg = pivot.apply(lambda row: row.tail(5).mean(), axis=1).sort_values(ascending=False)
                        top_pos = daily_avg.nlargest(8).index.tolist()
                        top_neg = daily_avg.nsmallest(7).index.tolist()
                        top_names = [n for n in top_pos + top_neg if n in pivot.index]
                        pivot = pivot.loc[top_names]

                        pivot_yi = pivot / 1e8  # 转亿元，保留NaN

                        fig_heat = go.Figure(
                            go.Heatmap(
                                z=pivot_yi.values,
                                x=[str(d)[-5:] for d in pivot_yi.columns],
                                y=pivot_yi.index,
                                colorscale=[[0, "#ef4444"], [0.5, "#0d1117"], [1, "#22c55e"]],
                                zmid=0,
                                text=[[f"{v:.1f}" if pd.notna(v) else "" for v in row] for row in pivot_yi.values],
                                texttemplate="%{text}",
                                textfont=dict(size=8),
                                hovertemplate="%{y}: %{x}<br>净流入: %{z:.1f}亿<extra></extra>",
                            )
                        )
                        fig_heat.update_layout(
                            title="<span style='font-size:12px;color:#8b949e'>近30日行业资金流热力图(亿元)</span>",
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=max(350, 30 * len(pivot_yi)),
                            margin=dict(l=100, r=20, t=35, b=30),
                            xaxis=dict(side="bottom", tickangle=45),
                            yaxis=dict(tickfont=dict(size=10)),
                        )
                        st.plotly_chart(fig_heat, width="stretch")
            else:
                st.info("暂无行业资金流数据，请先运行数据采集任务")
                if st.button("采集行业资金流", key="fetch_sector_flow"):
                    with st.spinner("正在采集..."):
                        try:
                            from src.data_sources.fund_flow import (
                                fetch_sector_fund_flow,
                                save_fund_flows,
                            )

                            conn_f = get_db_connection()
                            try:
                                sdf = fetch_sector_fund_flow()
                                if not sdf.empty:
                                    cnt = save_fund_flows(conn_f, sdf)
                                    st.success(f"采集成功，写入 {cnt} 条记录")
                                else:
                                    st.warning("采集返回空数据")
                            finally:
                                conn_f.close()
                        except Exception as e:
                            st.error(f"采集失败: {str(e)[:100]}")
                        st.rerun()

        except Exception as e:
            st.info(f"行业资金流模块暂不可用: {str(e)[:80]}")

    # ----- ETF资金流 -----
    with tab10_sub2:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">ETF资金流向<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">持仓ETF的主力资金流入流出趋势。</span></div>',
            unsafe_allow_html=True,
        )

        try:
            conn_ef = get_db_connection()
            try:
                etf_flow = pd.read_sql_query(
                    """
                    SELECT f.date, f.code, f.name, f.net_inflow,
                           ps.current_price AS close
                    FROM fund_flows f
                    LEFT JOIN portfolio_snapshots ps
                        ON f.code = ps.code AND f.date = ps.date
                    WHERE f.category = 'etf'
                    ORDER BY f.date DESC, f.code
                """,
                    conn_ef,
                )
            finally:
                conn_ef.close()

            if not etf_flow.empty:
                etf_flow = etf_flow.sort_values("date").reset_index(drop=True)
                etf_list = etf_flow["code"].unique()

                selected_etf_flow = st.selectbox(
                    "选择ETF",
                    etf_list,
                    format_func=lambda x: etf_flow[etf_flow["code"] == x]["name"].iloc[0],
                    key="etf_flow_sel",
                )
                etf_single = etf_flow[etf_flow["code"] == selected_etf_flow]

                if not etf_single.empty and "close" in etf_single.columns:
                    col_p1, col_p2 = st.columns([3, 1])
                    with col_p1:
                        fig_ef = go.Figure()
                        fig_ef.add_trace(
                            go.Bar(
                                x=etf_single["date"],
                                y=etf_single["net_inflow"] / 1e8,
                                name="主力净流入",
                                marker_color=[
                                    "#22c55e" if v > 0 else "#ef4444" for v in etf_single["net_inflow"] / 1e8
                                ],
                                yaxis="y",
                            )
                        )
                        fig_ef.add_trace(
                            go.Scatter(
                                x=etf_single["date"],
                                y=etf_single["close"],
                                name="收盘价",
                                mode="lines",
                                line=dict(color="#58a6ff", width=1.5),
                                yaxis="y2",
                            )
                        )
                        fig_ef.update_layout(
                            yaxis=dict(
                                title="净流入(亿元)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                            ),
                            yaxis2=dict(
                                title="收盘价",
                                overlaying="y",
                                side="right",
                                gridcolor="#21262d",
                                tickfont=dict(size=9, color="#58a6ff"),
                            ),
                            xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=350,
                            margin=dict(l=50, r=50, t=10, b=30),
                            legend=dict(
                                orientation="h", yanchor="bottom", y=1.02, font=dict(size=10, color="#8b949e")
                            ),
                            showlegend=True,
                        )
                        st.plotly_chart(fig_ef, width="stretch")
                    with col_p2:
                        total_net = etf_single["net_inflow"].sum()
                        st.metric(
                            "累计净流入",
                            f"{total_net/1e8:.1f}亿" if abs(total_net) > 1e8 else f"{total_net/1e4:.0f}万",
                        )
                        flow_up = len(etf_single[etf_single["net_inflow"] > 0])
                        st.metric(
                            "流入天数",
                            f"{flow_up} / {len(etf_single)}",
                            delta=f"{flow_up/len(etf_single)*100:.0f}%",
                        )
            else:
                st.info("暂无ETF资金流数据")
                if not positions.empty:
                    if st.button("采集持仓ETF资金流", key="fetch_etf_flow"):
                        with st.spinner("正在采集..."):
                            try:
                                from src.data_sources.fund_flow import (
                                    fetch_etf_fund_flow,
                                    save_fund_flows,
                                )

                                conn_f2 = get_db_connection()
                                try:
                                    for _, pos in positions.head(5).iterrows():
                                        code = str(pos["code"])
                                        name = pos["name"]
                                        st.caption(f"正在采集 {name}...")
                                        edf = fetch_etf_fund_flow(code, name)
                                        if not edf.empty:
                                            save_fund_flows(conn_f2, edf)
                                    st.success("采集完成")
                                finally:
                                    conn_f2.close()
                            except Exception as e:
                                st.error(f"采集失败: {str(e)[:100]}")
                            st.rerun()

        except Exception as e:
            st.info(f"ETF资金流模块暂不可用: {str(e)[:80]}")

    # ----- 主力资金净流入（替代已停更的北向资金） -----
    with tab10_sub3:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">主力资金<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">A股主力资金净流入趋势（主力=超大单+大单），数据自2025-11-07起，替代已停更的北向资金。</span></div>',
            unsafe_allow_html=True,
        )

        try:
            conn_nf = get_db_connection()
            try:
                mf_df = pd.read_sql_query(
                    """
                    SELECT date, net_inflow, super_large_inflow, large_inflow,
                           medium_inflow, small_inflow, net_inflow_pct
                    FROM fund_flows
                    WHERE category = 'main_fund'
                    ORDER BY date
                """,
                    conn_nf,
                )
            finally:
                conn_nf.close()

            if not mf_df.empty:
                col_n1, col_n2, col_n3 = st.columns(3)
                latest_mf = mf_df.iloc[-1]
                with col_n1:
                    val = latest_mf["net_inflow"] / 1e8
                    st.metric(
                        "最新主力净流入", f"{val:.1f}亿", delta=f"{val/1e4:.2f}万亿" if abs(val) > 10000 else None
                    )
                with col_n2:
                    val5 = mf_df.tail(5)["net_inflow"].sum() / 1e8
                    st.metric("近5日累计", f"{val5:.1f}亿")
                with col_n3:
                    val20 = mf_df.tail(20)["net_inflow"].sum() / 1e8
                    st.metric("近20日累计", f"{val20:.1f}亿")

                fig_mf = go.Figure()
                fig_mf.add_trace(
                    go.Bar(
                        x=mf_df["date"],
                        y=mf_df["net_inflow"] / 1e8,
                        name="主力净流入(亿)",
                        marker_color=["#22c55e" if v > 0 else "#ef4444" for v in mf_df["net_inflow"] / 1e8],
                    )
                )
                fig_mf.add_trace(
                    go.Scatter(
                        x=mf_df["date"],
                        y=(mf_df["net_inflow"] / 1e8).cumsum(),
                        name="累计净流入(亿)",
                        mode="lines",
                        line=dict(color="#f59e0b", width=2),
                        yaxis="y2",
                    )
                )
                fig_mf.add_hline(y=0, line_dash="dash", line_color="#484f58")
                fig_mf.update_layout(
                    yaxis=dict(title="日净流入(亿)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                    yaxis2=dict(
                        title="累计(亿)",
                        overlaying="y",
                        side="right",
                        gridcolor="#21262d",
                        tickfont=dict(size=9, color="#f59e0b"),
                    ),
                    xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                    paper_bgcolor="#0d1117",
                    plot_bgcolor="#0d1117",
                    height=350,
                    margin=dict(l=50, r=50, t=10, b=30),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10, color="#8b949e")),
                )
                st.plotly_chart(fig_mf, width="stretch")

                # ----- 持仓ETF合计主力资金净流入 -----
                st.markdown(
                    '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:3px 0;">持仓ETF合计资金流<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">持仓中所有ETF的主力资金净流入合计趋势，含90日累计统计与单日明细。</span></div>',
                    unsafe_allow_html=True,
                )

                try:
                    conn_ef2 = get_db_connection()
                    try:
                        etf_total = pd.read_sql_query(
                            """
                            SELECT f.date, SUM(f.net_inflow) as total_net_inflow,
                                   COUNT(DISTINCT f.code) as etf_count
                            FROM fund_flows f
                            WHERE f.category = 'etf'
                              AND f.date >= date('now', '-90 days')
                            GROUP BY f.date
                            ORDER BY f.date
                        """,
                            conn_ef2,
                        )
                    finally:
                        conn_ef2.close()

                    if not etf_total.empty:
                        col_e1, col_e2, col_e3 = st.columns(3)
                        latest_et = etf_total.iloc[-1]
                        with col_e1:
                            st.metric("最新ETF合计净流入", f"{latest_et['total_net_inflow']/1e8:.1f}亿")
                        with col_e2:
                            st.metric("覆盖ETF数", f"{latest_et['etf_count']}只")
                        with col_e3:
                            ev5 = etf_total.tail(5)["total_net_inflow"].sum() / 1e8
                            st.metric("近5日ETF累计", f"{ev5:.1f}亿")

                        fig_etf_total = go.Figure()
                        fig_etf_total.add_trace(
                            go.Bar(
                                x=etf_total["date"],
                                y=etf_total["total_net_inflow"] / 1e8,
                                name="ETF合计净流入(亿)",
                                marker_color=[
                                    "#22c55e" if v > 0 else "#ef4444" for v in etf_total["total_net_inflow"] / 1e8
                                ],
                            )
                        )
                        fig_etf_total.add_trace(
                            go.Scatter(
                                x=etf_total["date"],
                                y=(etf_total["total_net_inflow"] / 1e8).cumsum(),
                                name="累计净流入(亿)",
                                mode="lines",
                                line=dict(color="#58a6ff", width=2),
                                yaxis="y2",
                            )
                        )
                        fig_etf_total.add_hline(y=0, line_dash="dash", line_color="#484f58")
                        fig_etf_total.update_layout(
                            yaxis=dict(
                                title="日净流入(亿)", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")
                            ),
                            yaxis2=dict(
                                title="累计(亿)",
                                overlaying="y",
                                side="right",
                                gridcolor="#21262d",
                                tickfont=dict(size=9, color="#58a6ff"),
                            ),
                            xaxis=dict(gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                            paper_bgcolor="#0d1117",
                            plot_bgcolor="#0d1117",
                            height=350,
                            margin=dict(l=50, r=50, t=10, b=30),
                            legend=dict(
                                orientation="h", yanchor="bottom", y=1.02, font=dict(size=10, color="#8b949e")
                            ),
                        )
                        st.plotly_chart(fig_etf_total, width="stretch")
                    else:
                        st.info("暂无ETF资金流数据")
                except Exception as e2:
                    st.caption(f"ETF合计资金流: {str(e2)[:60]}")
            else:
                st.info("暂无主力资金数据")
                if st.button("采集主力资金数据", key="fetch_main_fund"):
                    with st.spinner("正在采集..."):
                        try:
                            from src.data_sources.fund_flow import (
                                fetch_main_fund_flow,
                                save_fund_flows,
                            )

                            conn_f3 = get_db_connection()
                            try:
                                mdf = fetch_main_fund_flow(days=120)
                                if not mdf.empty:
                                    cnt = save_fund_flows(conn_f3, mdf)
                                    st.success(f"采集成功，写入 {cnt} 条记录")
                            finally:
                                conn_f3.close()
                        except Exception as e:
                            st.error(f"采集失败: {str(e)[:100]}")
                        st.rerun()

        except Exception as e:
            st.info(f"主力资金模块暂不可用: {str(e)[:80]}")

            


