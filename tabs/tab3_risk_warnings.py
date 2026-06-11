"""Tab3 风险预警子模块 — 风险预警规则、风格暴露分析、行业轮动"""

from components.ui import render_chart, render_empty_state
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.chart_utils import downsample
from config.settings import CHART_DAYS, DOWNSAMPLE_MAX_POINTS, ETF_CATEGORIES
from src.utils.database import get_db_connection
from data_loader import load_alerts
def _render_risk_warnings(positions, volatility, max_dd, profit_count, loss_count, selected_date):
    """Extracted from render_tab3."""
    # ---------- 风险提示面板 ----------
    if not positions.empty:
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">风险提示<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于当前持仓结构和风险指标，自动识别并提示需要关注的风险因素。</span></div>',
            unsafe_allow_html=True,
        )

        warnings = []
        import math

        # 1. 集中度风险 - 单一持仓占比过高
        if not positions.empty:
            total_mv = positions["market_value"].sum()
            max_pos = positions.loc[positions["market_value"].idxmax()]
            max_weight = max_pos["market_value"] / total_mv * 100 if total_mv > 0 else 0
            if max_weight > 30:
                warnings.append(
                    (
                        "🔴",
                        "集中度风险",
                        f'「{max_pos["name"]}」占比 {max_weight:.1f}%，超过30%阈值，建议适当分散降低单一持仓集中度',
                    )
                )
            elif max_weight > 20:
                warnings.append(
                    ("🟡", "集中度风险", f'「{max_pos["name"]}」占比 {max_weight:.1f}%，接近30%警戒线，需关注')
                )

            # 前3大持仓集中度
            top3_weight = positions.nlargest(3, "market_value")["market_value"].sum() / total_mv * 100
            if top3_weight > 60:
                warnings.append(("🟡", "集中度风险", f"前3大持仓合计占比 {top3_weight:.1f}%，集中度偏高"))

        # 2. Beta 系统性风险
        beta_available = positions[positions["beta"].notna() & (positions["beta"] > 0)]
        if not beta_available.empty:
            port_beta = (
                (beta_available["beta"] * beta_available["market_value"]).sum()
                / beta_available["market_value"].sum()
                if beta_available["market_value"].sum() > 0
                else 1.0
            )
            if port_beta > 1.2:
                warnings.append(("🟡", "Beta风险", f"组合加权Beta为 {port_beta:.2f}，高于市场1.0，系统性风险偏高"))
            elif port_beta < 0.8:
                warnings.append(
                    ("🔵", "Beta风险", f"组合加权Beta为 {port_beta:.2f}，低于市场1.0，防御性较强但可能错失上涨行情")
                )

        # 3. 回撤风险
        if max_dd and not np.isnan(max_dd):
            dd_pct = abs(max_dd)
            if dd_pct > 15:
                warnings.append(("🔴", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，超过15%警戒线，注意控制下行风险"))
            elif dd_pct > 10:
                warnings.append(("🟡", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，处于较高水平"))
            elif dd_pct > 5:
                warnings.append(("🔵", "回撤风险", f"历史最大回撤 {dd_pct:.2f}%，处于正常波动范围"))

        # 4. 波动率风险
        if volatility and not np.isnan(volatility):
            if volatility > 25:
                warnings.append(("🟡", "波动率风险", f"年化波动率 {volatility:.2f}%，组合波动较大，注意风险管理"))
            elif volatility < 8:
                warnings.append(("🔵", "波动率风险", f"年化波动率 {volatility:.2f}%，组合波动较低"))

        # 5. 胜率风险
        if profit_count is not None and loss_count is not None and (profit_count + loss_count) > 0:
            wr = profit_count / (profit_count + loss_count) * 100
            if wr < 40:
                warnings.append(("🟡", "胜率偏低", f"当前胜率 {wr:.1f}%，持仓中盈利标的占比较低"))
            elif wr > 70:
                warnings.append(("🟢", "胜率优异", f"当前胜率 {wr:.1f}%，持仓中大部分标的处于盈利状态"))

        # 6. 亏损标的预警
        loss_positions = positions[positions["pnl"] < 0]
        if not loss_positions.empty:
            max_loss = loss_positions.loc[loss_positions["pnl_rate"].idxmin()]
            if max_loss["pnl_rate"] < -15:
                warnings.append(
                    ("🔴", "个股预警", f'「{max_loss["name"]}」亏损 {max_loss["pnl_rate"]:.2f}%，建议关注止损')
                )
            elif len(loss_positions) > len(positions) * 0.5:
                warnings.append(
                    (
                        "🟡",
                        "持仓预警",
                        f"亏损标的有 {len(loss_positions)} 只，占比 {len(loss_positions)/len(positions)*100:.0f}%",
                    )
                )

        # 7. 总盈亏趋势
        total_pnl = positions["pnl"].sum()
        if total_pnl < 0:
            warnings.append(("🟡", "组合亏损", f"当前总盈亏 ¥{total_pnl:,.0f}，整体处于浮亏状态"))

        # 渲染风险提示
        if warnings:
            for icon, title, desc in warnings:
                # 根据等级设置背景色
                if "🔴" in icon:
                    bg_color = "rgba(239,68,68,0.08)"
                    border_color = "rgba(239,68,68,0.3)"
                elif "🟡" in icon:
                    bg_color = "rgba(245,158,11,0.08)"
                    border_color = "rgba(245,158,11,0.3)"
                else:
                    bg_color = "rgba(34,197,94,0.06)"
                    border_color = "rgba(34,197,94,0.2)"
                st.markdown(
                    f'<div style="background:{bg_color};border:1px solid {border_color};border-radius:6px;padding:10px 14px;margin-bottom:6px;">'
                    f'<div style="font-size:13px;font-weight:bold;color:#c9d1d9;">{icon} {title}</div>'
                    f'<div style="font-size:12px;color:#8b949e;margin-top:3px;">{desc}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                '<div style="background:rgba(34,197,94,0.06);border:1px solid rgba(34,197,94,0.2);border-radius:6px;padding:12px 14px;">'
                '<div style="font-size:13px;color:#22c55e;font-weight:bold;">🟢 风险状况良好</div>'
                '<div style="font-size:12px;color:#8b949e;margin-top:3px;">当前未检测到显著风险因素，继续保持关注。</div>'
                "</div>",
                unsafe_allow_html=True,
            )




def _render_style_exposure(positions):
    """Extracted from render_tab3."""
    # ===== P2c: 风格暴露分析 =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="">风格暴露分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓 ETF 的分类标签，加权计算组合在规模、风格、行业三个维度的暴露度。</span></div>',
        unsafe_allow_html=True,
    )

    try:
        from src.analysis.factor_attribution import compute_style_exposure

        style_exp = compute_style_exposure(positions, ETF_CATEGORIES)
        if style_exp:
            col_size, col_style, col_sect = st.columns([1, 1, 1])

            # 规模暴露
            with col_size:
                st.markdown(
                    '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">规模维度<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓ETF的市值规模分类，计算大盘/中盘/小盘风格的暴露占比。</span></div>',
                    unsafe_allow_html=True,
                )
                size_exp = style_exp.get("size_exposure", {})
                if size_exp:
                    fig_size = go.Figure(
                        go.Pie(
                            labels=list(size_exp.keys()),
                            values=list(size_exp.values()),
                            marker_colors=["#58a6ff", "#f59e0b", "#a855f7"],
                            textinfo="label+percent",
                            textfont=dict(size=11, color="#c9d1d9"),
                            hole=0.5,
                        )
                    )
                    fig_size.update_layout(
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=220,
                        margin=dict(t=5, b=5, l=5, r=5),
                        showlegend=False,
                    )
                    render_chart(fig_size)

            # 风格暴露
            with col_style:
                st.markdown(
                    '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">风格维度<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓ETF的风格标签，计算成长/价值/均衡风格的暴露占比。</span></div>',
                    unsafe_allow_html=True,
                )
                style_exp_d = style_exp.get("style_exposure", {})
                if style_exp_d:
                    fig_sty = go.Figure(
                        go.Pie(
                            labels=list(style_exp_d.keys()),
                            values=list(style_exp_d.values()),
                            marker_colors=["#22c55e", "#ef4444", "#8b949e"],
                            textinfo="label+percent",
                            textfont=dict(size=11, color="#c9d1d9"),
                            hole=0.5,
                        )
                    )
                    fig_sty.update_layout(
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=220,
                        margin=dict(t=5, b=5, l=5, r=5),
                        showlegend=False,
                    )
                    render_chart(fig_sty)

            # 行业暴露
            with col_sect:
                st.markdown(
                    '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">行业维度<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于持仓ETF的行业分类，计算各行业的暴露权重，展示组合的行业集中度。</span></div>',
                    unsafe_allow_html=True,
                )
                sector_exp = style_exp.get("sector_exposure", {})
                if sector_exp:
                    sec_labels = list(sector_exp.keys())[:8]
                    sec_values = list(sector_exp.values())[:8]
                    fig_sec = go.Figure(
                        go.Bar(
                            orientation="h",
                            y=sec_labels,
                            x=sec_values,
                            marker_color="#58a6ff",
                            text=[f"{v:.1f}%" for v in sec_values],
                            textposition="auto",
                            textfont=dict(size=10, color="#c9d1d9"),
                        )
                    )
                    fig_sec.update_layout(
                        xaxis=dict(title="权重%", gridcolor="#21262d", tickfont=dict(size=9, color="#8b949e")),
                        yaxis=dict(title="", tickfont=dict(size=9, color="#c9d1d9")),
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        height=220,
                        margin=dict(l=60, r=20, t=5, b=25),
                        bargap=0.3,
                    )
                    render_chart(fig_sec)

            # 风格雷达图
            size_e = style_exp.get("size_exposure", {})
            style_e = style_exp.get("style_exposure", {})
            if size_e or style_e:
                radar_cats = []
                radar_vals = []
                for k, v in size_e.items():
                    radar_cats.append(f"规模-{k}")
                    radar_vals.append(v)
                for k, v in style_e.items():
                    radar_cats.append(f"风格-{k}")
                    radar_vals.append(v)

                fig_radar_style = go.Figure(
                    go.Scatterpolar(
                        r=radar_vals,
                        theta=radar_cats,
                        fill="toself",
                        fillcolor="rgba(88,166,255,0.15)",
                        line=dict(color="#58a6ff", width=2),
                        marker=dict(size=6, color="#58a6ff"),
                    )
                )
                fig_radar_style.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            tickfont=dict(size=9, color="#6e7681"),
                            gridcolor="#21262d",
                            range=[0, max(radar_vals) * 1.3] if radar_vals else [0, 100],
                        ),
                        angularaxis=dict(tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
                        bgcolor="#0d1117",
                    ),
                    paper_bgcolor="#0d1117",
                    plot_bgcolor="#0d1117",
                    height=300,
                    margin=dict(t=10, b=10, l=10, r=10),
                    showlegend=False,
                )
                render_chart(fig_radar_style)
    except (ImportError, ValueError, KeyError, TypeError) as e:
        st.info(f"风格暴露分析暂不可用: {str(e)[:80]}")




def _render_sector_rotation():
    """Extracted from render_tab3."""
    # ===== P2d: 行业轮动分析 =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="">行业轮动分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">追踪各行业指数在不同时间窗口的收益排名变化，衡量市场轮动速度。</span></div>',
        unsafe_allow_html=True,
    )

    try:
        from src.analysis.factor_attribution import compute_sector_rotation

        conn_rot = get_db_connection()
        try:
            rotation = compute_sector_rotation(conn_rot)
        finally:
            conn_rot.close()

        if rotation and "error" not in rotation:
            # 轮动速度指标
            rot_speed = rotation.get("rotation_speed", {})
            if rot_speed:
                col_rs = st.columns(len(rot_speed))
                for ci, (period, speed) in enumerate(rot_speed.items()):
                    with col_rs[ci]:
                        st.metric(
                            f"轮动速度 ({period})", f"{speed:.1f}", help=f"行业收益标准差，值越大说明行业分化越明显"
                        )
                st.caption("轮动速度 = 行业收益率标准差，反映行业分化程度。高轮动速度意味着行业间表现差异大。")

            # 行业排名变化表
            period_returns = rotation.get("sector_period_returns", {})
            if period_returns:
                periods = list(period_returns.keys())
                # 取最近两个时段做对比
                if len(periods) >= 2:
                    p1, p2 = periods[0], periods[1]
                    r1 = period_returns.get(p1, {})
                    r2 = period_returns.get(p2, {})
                    all_sectors = sorted(set(list(r1.keys()) + list(r2.keys())))
                    table_rows = []
                    for sec in all_sectors:
                        ret1 = r1.get(sec, 0)
                        ret2 = r2.get(sec, 0)
                        rank1 = sorted(r1.items(), key=lambda x: -x[1])
                        rank2 = sorted(r2.items(), key=lambda x: -x[1])
                        rk1 = next((i + 1 for i, (k, _) in enumerate(rank1) if k == sec), "-")
                        rk2 = next((i + 1 for i, (k, _) in enumerate(rank2) if k == sec), "-")
                        rank_change = ""
                        if isinstance(rk1, int) and isinstance(rk2, int):
                            diff = rk1 - rk2
                            if diff > 0:
                                rank_change = f'<span style="color:#22c55e">↑{diff}</span>'
                            elif diff < 0:
                                rank_change = f'<span style="color:#ef4444">↓{abs(diff)}</span>'
                            else:
                                rank_change = "-"
                        table_rows.append(
                            {
                                "行业/指数": sec,
                                f"{p1}收益": f"{ret1:+.2f}%",
                                f"{p1}排名": rk1,
                                f"{p2}收益": f"{ret2:+.2f}%",
                                f"{p2}排名": rk2,
                                "排名变化": rank_change,
                            }
                        )
                    if table_rows:
                        st.markdown(
                            pd.DataFrame(table_rows).to_html(index=False, escape=False), unsafe_allow_html=True
                        )
    except (sqlite3.OperationalError, ImportError, ValueError, KeyError) as e:
        st.info(f"行业轮动分析暂不可用: {str(e)[:80]}")


