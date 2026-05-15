"""
Tab6: 技术信号
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.utils.chart_utils import _atr_c, _boll_c, _rsi_c, _sig
from src.utils.database import get_db_connection


def load_technical():
    """加载技术指标，关联ETF名称"""
    conn = get_db_connection()
    query = """
        SELECT t.*, p.name 
        FROM etf_technical t 
        LEFT JOIN portfolio_snapshots p ON t.code = p.code AND t.date = p.date
        WHERE t.date = (SELECT MAX(date) FROM etf_technical)
    """
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df["name"] = df["name"].fillna(df["code"])
    return df



def render_tab6(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)
    cal_data = kwargs.get('cal_data', pd.DataFrame())
    tech_signals = kwargs.get('tech_signals', pd.DataFrame())

    """渲染Tab6: 技术信号"""
    
    st.markdown(
        '<div class="tip-title" style="">技术信号总览<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于MA均线排列、MACD、KDJ、RSI、布林带位置等技术指标，对持仓品种进行全面信号检测，辅助判断短期走势。</span></div>',
        unsafe_allow_html=True,
    )

    tech_df = load_technical()

    if tech_df is None or tech_df.empty:
        st.info("暂无技术信号数据")
    else:
        # ---------- 信号概览卡片 ----------
        n_total = len(tech_df)
        n_bullish_ma = len(tech_df[tech_df["ma_signal"].isin(["多头排列", "金叉"])])
        n_bearish_ma = len(tech_df[tech_df["ma_signal"].isin(["空头排列", "死叉"])])
        n_overbought = len(tech_df[tech_df["rsi_status"].isin(["超买", "严重超买"])])
        n_oversold = len(tech_df[tech_df["rsi_status"].isin(["超卖", "严重超卖"])])
        n_bull_macd = len(tech_df[tech_df["macd_signal"].isin(["多头", "金叉", "看多"])])
        n_bear_macd = len(tech_df[tech_df["macd_signal"].isin(["空头", "死叉"])])
        n_bull_kdj = len(tech_df[tech_df["kdj_signal"] == "金叉"])
        n_bear_kdj = len(tech_df[tech_df["kdj_signal"] == "死叉"])
        n_strong_up = len(tech_df[tech_df["trend"] == "强势上涨"])
        n_weak_down = len(tech_df[tech_df["trend"] == "下跌"])

        overview_cols = st.columns(6)
        with overview_cols[0]:
            st.markdown(
                f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #22c55e;">'
                f'<div style="font-size:11px;color:#8b949e;">MA 多头</div>'
                f'<div style="font-size:20px;font-weight:bold;color:#22c55e;">{n_bullish_ma}<span style="font-size:12px;color:#484f58;">/{n_total}</span></div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with overview_cols[1]:
            st.markdown(
                f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #ef4444;">'
                f'<div style="font-size:11px;color:#8b949e;">MA 空头</div>'
                f'<div style="font-size:20px;font-weight:bold;color:#ef4444;">{n_bearish_ma}<span style="font-size:12px;color:#484f58;">/{n_total}</span></div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with overview_cols[2]:
            st.markdown(
                f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #f59e0b;">'
                f'<div style="font-size:11px;color:#8b949e;">RSI 超买</div>'
                f'<div style="font-size:20px;font-weight:bold;color:#f59e0b;">{n_overbought}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with overview_cols[3]:
            st.markdown(
                f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #3b82f6;">'
                f'<div style="font-size:11px;color:#8b949e;">RSI 超卖</div>'
                f'<div style="font-size:20px;font-weight:bold;color:#3b82f6;">{n_oversold}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with overview_cols[4]:
            st.markdown(
                f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #22c55e;">'
                f'<div style="font-size:11px;color:#8b949e;">强势上涨</div>'
                f'<div style="font-size:20px;font-weight:bold;color:#22c55e;">{n_strong_up}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with overview_cols[5]:
            st.markdown(
                f'<div style="padding:10px;border-radius:8px;background:#161b22;border-left:3px solid #ef4444;">'
                f'<div style="font-size:11px;color:#8b949e;">下跌趋势</div>'
                f'<div style="font-size:20px;font-weight:bold;color:#ef4444;">{n_weak_down}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

        # ---------- 信号强度雷达图 ----------
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">多空信号分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">雷达图展示各维度的多空力量对比，越靠近外圈表示该维度多头信号越强。</span></div>',
            unsafe_allow_html=True,
        )

        radar_cols = st.columns([1, 1])
        with radar_cols[0]:
            # 雷达图：多头 vs 空头
            categories = ["MA均线", "MACD", "KDJ", "RSI<br>(超买)", "RSI<br>(超卖)", "趋势<br>(强势)"]
            bull_values = [n_bullish_ma, n_bull_macd, n_bull_kdj, n_overbought, n_oversold, n_strong_up]
            bear_values = [n_bearish_ma, n_bear_macd, n_bear_kdj, n_overbought, n_oversold, n_weak_down]

            fig_radar = go.Figure()
            fig_radar.add_trace(
                go.Scatterpolar(
                    r=bull_values,
                    theta=categories,
                    fill="toself",
                    fillcolor="rgba(34,197,94,0.15)",
                    line_color="#22c55e",
                    name="多头信号",
                    marker_size=5,
                )
            )
            fig_radar.add_trace(
                go.Scatterpolar(
                    r=bear_values,
                    theta=categories,
                    fill="toself",
                    fillcolor="rgba(239,68,68,0.15)",
                    line_color="#ef4444",
                    name="空头信号",
                    marker_size=5,
                )
            )
            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, max(max(bull_values), max(bear_values), 1)],
                        gridcolor="#30363d",
                        tickfont=dict(size=9, color="#8b949e"),
                        angle=45,
                    ),
                    bgcolor="#0d1117",
                    angularaxis=dict(gridcolor="#30363d", tickfont=dict(size=11, color="#c9d1d9")),
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.1,
                    xanchor="center",
                    x=0.5,
                    font=dict(size=11, color="#c9d1d9"),
                ),
                paper_bgcolor="#0d1117",
                plot_bgcolor="#0d1117",
                height=320,
                margin=dict(l=40, r=40, t=40, b=40),
            )
            st.plotly_chart(fig_radar, width="stretch")

        with radar_cols[1]:
            # 技术指标信号汇总堆叠柱状图
            indicator_labels = ["MA多头", "MA空头", "MACD多头", "MACD空头", "KDJ金叉", "KDJ死叉", "超买", "超卖"]
            indicator_values = [
                n_bullish_ma,
                n_bearish_ma,
                n_bull_macd,
                n_bear_macd,
                n_bull_kdj,
                n_bear_kdj,
                n_overbought,
                n_oversold,
            ]
            bar_colors = ["#22c55e", "#ef4444", "#22c55e", "#ef4444", "#22c55e", "#ef4444", "#f59e0b", "#3b82f6"]

            fig_bar = go.Figure(
                go.Bar(
                    x=indicator_labels,
                    y=indicator_values,
                    marker_color=bar_colors,
                    text=indicator_values,
                    textposition="auto",
                    textfont=dict(size=12, color="#c9d1d9"),
                    hovertemplate="%{x}: %{y}只<extra></extra>",
                )
            )
            fig_bar.update_layout(
                xaxis=dict(tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
                yaxis=dict(title="持仓数量", tickfont=dict(size=10, color="#8b949e"), gridcolor="#21262d", dtick=1),
                paper_bgcolor="#0d1117",
                plot_bgcolor="#0d1117",
                height=320,
                margin=dict(l=50, r=20, t=20, b=40),
                bargap=0.3,
            )
            st.plotly_chart(fig_bar, width="stretch")

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

        # ---------- 技术信号详情表 ----------
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">持仓技术信号详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示每只持仓品种的完整技术指标信号。颜色标记：绿色=多头/看多，红色=空头/看空，黄色=超买/超卖预警。</span></div>',
            unsafe_allow_html=True,
        )

        # 构建 HTML 表格（st.dataframe 不渲染 HTML 标签）
        def _sig(val, bull, bear, warn=None):
            if warn and val in warn:
                return f'<span style="color:#f59e0b;font-weight:bold">{val}</span>'
            if val in bull:
                return f'<span style="color:#22c55e;font-weight:bold">{val}</span>'
            if val in bear:
                return f'<span style="color:#ef4444;font-weight:bold">{val}</span>'
            return f'<span style="color:#8b949e">{val}</span>'

        def _rsi_c(v):
            if v >= 80:
                return f'<span style="color:#ef4444;font-weight:bold">{v:.1f}</span>'
            if v >= 70:
                return f'<span style="color:#f59e0b;font-weight:bold">{v:.1f}</span>'
            if v <= 20:
                return f'<span style="color:#3b82f6;font-weight:bold">{v:.1f}</span>'
            if v <= 30:
                return f'<span style="color:#f59e0b">{v:.1f}</span>'
            return f'<span style="color:#c9d1d9">{v:.1f}</span>'

        def _boll_c(v):
            if v >= 80:
                return f'<span style="color:#ef4444;font-weight:bold">{v:.1f}%</span>'
            if v >= 60:
                return f'<span style="color:#22c55e">{v:.1f}%</span>'
            if v <= 20:
                return f'<span style="color:#3b82f6;font-weight:bold">{v:.1f}%</span>'
            if v <= 40:
                return f'<span style="color:#f59e0b">{v:.1f}%</span>'
            return f'<span style="color:#c9d1d9">{v:.1f}%</span>'

        def _atr_c(v):
            if v >= 3.0:
                return f'<span style="color:#f59e0b;font-weight:bold">{v:.2f}%</span>'
            if v >= 2.0:
                return f'<span style="color:#c9d1d9">{v:.2f}%</span>'
            return f'<span style="color:#22c55e">{v:.2f}%</span>'

        tbl = (
            '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:13px;">'
            '<thead><tr style="background:#161b22;">'
            '<th style="padding:8px 10px;color:#8b949e;text-align:left;font-size:12px;">代码</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:left;font-size:12px;">名称</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">趋势</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">MA信号</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">MACD信号</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">KDJ信号</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">RSI值</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">RSI状态</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">布林位置</th>'
            '<th style="padding:8px 10px;color:#8b949e;text-align:center;font-size:12px;">ATR</th>'
            "</tr></thead><tbody>"
        )
        for _, r in tech_df.iterrows():
            bg = "#161b22" if _ % 2 == 0 else "#0d1117"
            tbl += (
                f'<tr style="background:{bg};border-bottom:1px solid #21262d;">'
                f'<td style="padding:7px 10px;color:#c9d1d9;font-family:monospace;">{r["code"]}</td>'
                f'<td style="padding:7px 10px;color:#c9d1d9;">{r["name"]}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_sig(r["trend"], ["强势上涨","温和上涨"], ["下跌"])}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_sig(r["ma_signal"], ["多头排列","金叉"], ["空头排列","死叉"])}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_sig(r["macd_signal"], ["多头","金叉","看多"], ["空头","死叉"])}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_sig(r["kdj_signal"], ["金叉"], ["死叉"])}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_rsi_c(r["rsi_value"])}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_sig(r["rsi_status"], [], [], ["超买","严重超买"])}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_boll_c(r["bollinger_position"])}</td>'
                f'<td style="padding:7px 10px;text-align:center;">{_atr_c(r["atr_pct"])}</td>'
                "</tr>"
            )
        tbl += "</tbody></table></div>"
        st.markdown(tbl, unsafe_allow_html=True)

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

        # ---------- 布林带位置分布图 ----------
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">布林带位置分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示所有持仓品种在布林带中的相对位置(0%=下轨, 50%=中轨, 100%=上轨)。超过100%或低于0%表示突破布林带轨道。</span></div>',
            unsafe_allow_html=True,
        )

        # 按布林位置排序
        boll_df = tech_df[["name", "code", "bollinger_position"]].sort_values("bollinger_position", ascending=True)

        boll_colors = []
        for _, row in boll_df.iterrows():
            bp = row["bollinger_position"]
            if bp >= 80:
                boll_colors.append("#ef4444")
            elif bp >= 60:
                boll_colors.append("#22c55e")
            elif bp <= 20:
                boll_colors.append("#3b82f6")
            elif bp <= 40:
                boll_colors.append("#f59e0b")
            else:
                boll_colors.append("#8b949e")

        fig_boll = go.Figure(
            go.Bar(
                orientation="h",
                y=boll_df["name"],
                x=boll_df["bollinger_position"],
                marker_color=boll_colors,
                text=[f"{v:.1f}%" for v in boll_df["bollinger_position"]],
                textposition="auto",
                textfont=dict(size=10, color="#c9d1d9"),
                hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
            )
        )
        # 添加参考线
        fig_boll.add_vline(x=0, line_dash="dash", line_color="#3b82f6", opacity=0.5, annotation_text="下轨")
        fig_boll.add_vline(x=50, line_dash="dash", line_color="#8b949e", opacity=0.5, annotation_text="中轨")
        fig_boll.add_vline(x=100, line_dash="dash", line_color="#ef4444", opacity=0.5, annotation_text="上轨")

        fig_boll.update_layout(
            xaxis=dict(
                title="布林带位置 (%)",
                range=[
                    min(-10, boll_df["bollinger_position"].min() - 5),
                    max(110, boll_df["bollinger_position"].max() + 5),
                ],
                tickfont=dict(size=10, color="#8b949e"),
                gridcolor="#21262d",
            ),
            yaxis=dict(title="", tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
            paper_bgcolor="#0d1117",
            plot_bgcolor="#0d1117",
            height=max(300, 30 * n_total),
            margin=dict(l=80, r=30, t=30, b=40),
            bargap=0.2,
        )
        st.plotly_chart(fig_boll, width="stretch")

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

        # ---------- RSI 分布图 ----------
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">RSI 相对强弱分布<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示所有持仓品种的RSI值分布。RSI>70为超买区间(红色)，RSI<30为超卖区间(蓝色)。</span></div>',
            unsafe_allow_html=True,
        )

        rsi_df = tech_df[["name", "code", "rsi_value", "rsi_status"]].sort_values("rsi_value", ascending=True)

        rsi_bar_colors = []
        for _, row in rsi_df.iterrows():
            r = row["rsi_value"]
            if r >= 80:
                rsi_bar_colors.append("#ef4444")
            elif r >= 70:
                rsi_bar_colors.append("#f59e0b")
            elif r <= 20:
                rsi_bar_colors.append("#3b82f6")
            elif r <= 30:
                rsi_bar_colors.append("#f59e0b")
            else:
                rsi_bar_colors.append("#22c55e")

        fig_rsi = go.Figure(
            go.Bar(
                orientation="h",
                y=rsi_df["name"],
                x=rsi_df["rsi_value"],
                marker_color=rsi_bar_colors,
                text=[f"{v:.1f}" for v in rsi_df["rsi_value"]],
                textposition="auto",
                textfont=dict(size=10, color="#c9d1d9"),
                hovertemplate="%{y}: RSI=%{x:.1f}<extra></extra>",
            )
        )
        # RSI 参考区域
        fig_rsi.add_vrect(x0=0, x1=30, fillcolor="rgba(59,130,246,0.08)", line_width=0)
        fig_rsi.add_vrect(x0=70, x1=100, fillcolor="rgba(239,68,68,0.08)", line_width=0)
        fig_rsi.add_vline(x=30, line_dash="dash", line_color="#3b82f6", opacity=0.4)
        fig_rsi.add_vline(x=70, line_dash="dash", line_color="#ef4444", opacity=0.4)
        fig_rsi.add_vline(x=50, line_dash="dot", line_color="#8b949e", opacity=0.3)

        fig_rsi.update_layout(
            xaxis=dict(
                title="RSI 值", range=[0, 100], tickfont=dict(size=10, color="#8b949e"), gridcolor="#21262d"
            ),
            yaxis=dict(title="", tickfont=dict(size=10, color="#c9d1d9"), gridcolor="#21262d"),
            paper_bgcolor="#0d1117",
            plot_bgcolor="#0d1117",
            height=max(300, 30 * n_total),
            margin=dict(l=80, r=30, t=30, b=40),
            bargap=0.2,
        )
        st.plotly_chart(fig_rsi, width="stretch")

# ========== Tab7: 资讯与评估 ==========
