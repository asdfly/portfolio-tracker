"""
Tab8: 操作建议
"""

import os
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from config.settings import ETF_CATEGORIES, SECTOR_COLORS, DATABASE_PATH
from tabs._helpers import _load_tech_signals
from src.utils.database import get_db_connection


def render_tab8(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)
    """渲染Tab8: 操作建议"""
    
    st.caption("💡 基于技术信号和持仓状态，生成具体操作建议")

    if not positions.empty:
        conn = get_db_connection()
        try:
            held_codes = positions["code"].tolist()
            if held_codes:
                tech_df = _load_tech_signals(tuple(held_codes), _full=True)
            else:
                tech_df = pd.DataFrame()

        except Exception:
            tech_df = pd.DataFrame()
        suggestions = []
        action_colors = {
            "买入": "#22c55e",
            "持有": "#f59e0b",
            "观望": "#8b949e",
            "卖出": "#ef4444",
            "加仓": "#22c55e",
            "减仓": "#ef4444",
        }

        if not tech_df.empty:
            latest_tech = tech_df.drop_duplicates("code", keep="first")

            for _, pos in positions.iterrows():
                code = str(pos["code"])
                name = pos["name"]
                pnl_rate = pos.get("pnl_rate", 0)
                mv = pos["market_value"]
                cat_info = ETF_CATEGORIES.get(code, {})
                sector = cat_info.get("sector", "未知")

                tech_row = latest_tech[latest_tech["code"] == code]
                if tech_row.empty:
                    continue
                tr = tech_row.iloc[0]

                # 技术面综合判断
                buy_signals = 0
                sell_signals = 0
                reasons = []

                # 均线信号
                if tr.get("ma_signal") == "多头排列":
                    buy_signals += 2
                    reasons.append("均线多头排列")
                elif tr.get("ma_signal") == "空头排列":
                    sell_signals += 2
                    reasons.append("均线空头排列")
                elif tr.get("ma_signal") == "金叉":
                    buy_signals += 1
                    reasons.append("均线金叉")
                elif tr.get("ma_signal") == "死叉":
                    sell_signals += 1
                    reasons.append("均线死叉")

                # MACD信号
                macd = str(tr.get("macd_signal", ""))
                if macd in ("金叉", "多头", "看多"):
                    buy_signals += 1.5
                    reasons.append(f"MACD{macd}")
                elif macd in ("死叉", "空头"):
                    sell_signals += 1.5
                    reasons.append(f"MACD{macd}")
                elif macd == "中性":
                    pass

                # RSI信号
                rsi_val = tr.get("rsi_value", 50)
                rsi_status = tr.get("rsi_status", "正常")
                if rsi_status in ("超卖", "严重超卖"):
                    buy_signals += 1
                    reasons.append(f"RSI{rsi_status}({rsi_val:.0f})")
                elif rsi_status in ("超买", "严重超买"):
                    sell_signals += 1
                    reasons.append(f"RSI{rsi_status}({rsi_val:.0f})")

                # KDJ信号
                kdj = tr.get("kdj_signal", "")
                if "金叉" in str(kdj):
                    buy_signals += 1
                    reasons.append("KDJ金叉")
                elif "死叉" in str(kdj):
                    sell_signals += 1
                    reasons.append("KDJ死叉")

                # 布林带
                boll_pos = tr.get("bollinger_position", "")
                if "下轨" in str(boll_pos):
                    buy_signals += 0.5
                    reasons.append("触及布林下轨")
                elif "上轨" in str(boll_pos):
                    sell_signals += 0.5
                    reasons.append("触及布林上轨")

                # 趋势
                trend = str(tr.get("trend", ""))
                if "上涨" in trend:
                    buy_signals += 1
                    reasons.append(f"趋势{trend}")
                elif trend in ("下跌", "温和下跌"):
                    sell_signals += 1
                    reasons.append(f"趋势{trend}")

                # 盈亏状态调整
                if pnl_rate < -10:
                    sell_signals += 0.5
                    reasons.append(f"亏损较深({pnl_rate:.1f}%)")
                elif pnl_rate > 20:
                    sell_signals += 0.5
                    reasons.append(f"盈利较多({pnl_rate:+.1f}%)，注意止盈")

                # 生成建议
                net_signal = buy_signals - sell_signals
                if net_signal >= 3:
                    action = "买入"
                    urgency = "强烈建议"
                elif net_signal >= 1.5:
                    action = "加仓"
                    urgency = "建议"
                elif net_signal >= -0.5:
                    action = "持有"
                    urgency = "维持"
                elif net_signal >= -2:
                    action = "观望"
                    urgency = "建议"
                else:
                    action = "卖出"
                    urgency = "建议"

                suggestions.append(
                    {
                        "name": name,
                        "code": code,
                        "sector": sector,
                        "action": action,
                        "urgency": urgency,
                        "reasons": reasons,
                        "buy_score": buy_signals,
                        "sell_score": sell_signals,
                        "net_signal": net_signal,
                        "pnl_rate": pnl_rate,
                        "market_value": mv,
                        "trend": trend,
                        "rsi": rsi_val,
                    }
                )

        # 按净信号排序
        suggestions.sort(key=lambda x: x["net_signal"], reverse=True)

        # ===== 操作建议汇总卡片 =====
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">建议汇总<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于技术指标综合评分，为每只持仓ETF生成操作建议。</span></div>',
            unsafe_allow_html=True,
        )

        action_counts = {}
        for s in suggestions:
            action_counts[s["action"]] = action_counts.get(s["action"], 0) + 1

        summary_html_parts = []
        for action in ["买入", "加仓", "持有", "观望", "卖出"]:
            cnt = action_counts.get(action, 0)
            if cnt > 0:
                color = action_colors[action]
                summary_html_parts.append(
                    f'<span style="display:inline-flex;align-items:center;gap:4px;background:{color}15;color:{color};padding:6px 14px;border-radius:6px;margin:0 4px 4px 0;font-size:13px;font-weight:bold;">'
                    f'{action} <span style="font-size:16px;">{cnt}</span>只</span>'
                )
        st.markdown(
            f'<div style="display:flex;flex-wrap:wrap;gap:4px;padding:8px 0;">{"".join(summary_html_parts)}</div>',
            unsafe_allow_html=True,
        )

        # ===== 建议分布饼图 + 信号强度矩阵 =====
        if suggestions:
            viz_col1, viz_col2 = st.columns([1, 2])

            with viz_col1:
                # 建议分布饼图
                fig_pie = go.Figure(go.Pie(
                    labels=[s["action"] for s in suggestions],
                    hole=0.5,
                    marker_colors=[action_colors.get(s["action"], "#8b949e") for s in suggestions],
                    textinfo="label",
                    textfont=dict(size=11, color="#c9d1d9"),
                    hovertemplate="%{label}<br>净信号: %{customdata}<extra></extra>",
                    customdata=[f'{s["net_signal"]:+.1f}' for s in suggestions],
                ))
                fig_pie.update_layout(
                    paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                    height=280, margin=dict(l=10, r=10, t=10, b=10),
                    showlegend=False,
                    annotations=[dict(text=f"{len(suggestions)}只", x=0.5, y=0.5, font_size=20, font_color="#c9d1d9", showarrow=False)],
                )
                st.plotly_chart(fig_pie, width="stretch")

            with viz_col2:
                # 信号强度热力图 (ETF x 指标维度)
                indicators = ["MA", "MACD", "RSI", "KDJ", "布林带", "趋势", "盈亏"]
                fig_heat = go.Figure(go.Heatmap(
                    z=[[1 if r else -1 for r in [
                        "均线" in str(s.get("reasons",[])),
                        "MACD" in str(s.get("reasons",[])),
                        "RSI" in str(s.get("reasons",[])),
                        "KDJ" in str(s.get("reasons",[])),
                        "布林" in str(s.get("reasons",[])),
                        "趋势" in str(s.get("reasons",[])),
                        "亏损" in str(s.get("reasons",[])) or "盈利" in str(s.get("reasons",[])),
                    ]] for s in suggestions],
                    x=indicators,
                    y=[s["name"] for s in suggestions],
                    colorscale=[[0, "#ef4444"], [0.5, "#0d1117"], [1, "#22c55e"]],
                    zmid=0,
                    text=[["+" if v > 0 else "-" for v in row] for row in [
                        [1 if r else -1 for r in [
                            "均线" in str(s.get("reasons",[])),
                            "MACD" in str(s.get("reasons",[])),
                            "RSI" in str(s.get("reasons",[])),
                            "KDJ" in str(s.get("reasons",[])),
                            "布林" in str(s.get("reasons",[])),
                            "趋势" in str(s.get("reasons",[])),
                            "亏损" in str(s.get("reasons",[])) or "盈利" in str(s.get("reasons",[])),
                        ]] for s in suggestions
                    ]],
                    texttemplate="%{text}",
                    textfont=dict(size=12, color="#c9d1d9"),
                    hovertemplate="%{y} - %{x}<extra></extra>",
                ))
                fig_heat.update_layout(
                    paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                    height=max(200, 30 * len(suggestions)),
                    margin=dict(l=80, r=20, t=5, b=30),
                    xaxis=dict(tickfont=dict(size=10, color="#8b949e")),
                    yaxis=dict(tickfont=dict(size=10, color="#c9d1d9")),
                )
                st.plotly_chart(fig_heat, width="stretch")

        # ===== 建议详情 =====
        st.markdown(
            '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">建议详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于技术信号与持仓收益的智能调仓建议，包含多空评分、操作方向及信号来源。</span></div>',
            unsafe_allow_html=True,
        )

        for s in suggestions:
            action_color = action_colors.get(s["action"], "#8b949e")
            sector_color = SECTOR_COLORS.get(s["sector"], "#8b949e")
            _t = s.get("trend", "")
            if "上涨" in _t:
                trend_icon = "🟢"
            elif _t in ("下跌", "温和下跌"):
                trend_icon = "🔴"
            elif "震荡" in _t:
                trend_icon = "🟡"
            else:
                trend_icon = "⚪"
            reasons_str = " | ".join(s["reasons"][:5]) if s["reasons"] else "暂无明显信号"

            st.markdown(
                f'<div style="background:#161b22;border-radius:6px;padding:12px 14px;margin-bottom:6px;border-left:3px solid {action_color};">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f"<div>"
                f'<span style="font-size:14px;font-weight:bold;color:#e6edf3;">{s["name"]}</span>'
                f'<span style="font-size:11px;color:#484f58;margin-left:8px;">{s["code"]}</span>'
                f'<span style="font-size:11px;color:{sector_color};background:{sector_color}15;padding:1px 6px;border-radius:3px;margin-left:6px;">{s["sector"]}</span>'
                f"</div>"
                f'<div style="display:flex;align-items:center;gap:6px;">'
                f"{trend_icon}"
                f'<span style="color:{action_color};font-size:13px;font-weight:bold;background:{action_color}15;padding:3px 10px;border-radius:4px;">{s["urgency"]}{s["action"]}</span>'
                f"</div></div>"
                f'<div style="font-size:12px;color:#6e7681;margin-top:6px;">信号: {reasons_str}</div>'
                f'<div style="display:flex;gap:16px;margin-top:4px;font-size:11px;color:#484f58;">'
                f'<span>多空信号: <b style="color:#22c55e;">{s["buy_score"]:.1f}</b> / <b style="color:#ef4444;">{s["sell_score"]:.1f}</b></span>'
                f'<span>净信号: <b style="color:{action_color};">{s["net_signal"]:+.1f}</b></span>'
                f'<span>收益率: <b style="color:{"#22c55e" if s["pnl_rate"] >= 0 else "#ef4444"};">{s["pnl_rate"]:+.2f}%</b></span>'
                f'<span>RSI: {s["rsi"]:.0f}</span>'
                f"</div></div>",
                unsafe_allow_html=True,
            )

        if not suggestions:
            st.info("暂无足够技术数据生成操作建议")
    else:
        st.info("暂无持仓数据")

    # ========== 市场事件驱动信号 ==========
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;">'
        '<span>📡 市场事件信号（近5个交易日）</span></div>',
        unsafe_allow_html=True,
    )
    try:
        import sqlite3
        from src.analysis.market_event_signals import MarketEventSignalEngine
        ev_conn = sqlite3.connect(str(DATABASE_PATH))
        ev_engine = MarketEventSignalEngine(ev_conn)
        ev_signals = ev_engine.generate_all_signals(lookback_days=5)
        ev_summary = ev_engine.get_signal_summary(ev_signals)
        ev_conn.close()

        # Summary cards
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("总信号", ev_summary["total"])
        sc2.metric("风险信号", ev_summary["by_type"]["risk"], delta=None)
        sc3.metric("机会信号", ev_summary["by_type"]["opp"], delta=None)
        sc4.metric("高优先级", ev_summary["by_level"]["high"], delta=None)

        # Source breakdown
        if ev_summary["by_source"]:
            src_df = pd.DataFrame([
                {"来源": {"lhb":"龙虎榜","margin":"融资融券","holder_change":"股东增减持",
                          "block_trade":"大宗交易","institution":"机构调研"}.get(k,k),
                 "信号数": v}
                for k, v in ev_summary["by_source"].items()
            ])
            st.bar_chart(src_df, x="来源", y="信号数", height=200)

        # Top signals
        if ev_summary["top_risk"]:
            with st.expander("🔴 Top 风险信号", expanded=False):
                for s in ev_summary["top_risk"][:10]:
                    st.markdown(f"**{s.title}** ({s.date}) — {s.description}")
        if ev_summary["top_opportunity"]:
            with st.expander("🟢 Top 机会信号", expanded=False):
                for s in ev_summary["top_opportunity"][:10]:
                    st.markdown(f"**{s.title}** ({s.date}) — {s.description}")

        # Portfolio impact
        if not positions.empty:
            held = positions["code"].tolist()
            rpt = ev_engine.get_portfolio_signal_report(ev_signals, held)
            if rpt["related_count"] > 0:
                level_map = {"high": "🔴 高", "medium": "🟡 中", "low": "🟢 低"}
                st.markdown(f"**持仓关联信号: {rpt['related_count']}条** "
                            f"（组合风险等级: {level_map.get(rpt['portfolio_risk_level'], '?')}）")
                for pos in rpt["affected_positions"][:5]:
                    lv_tag = level_map.get(pos["highest_level"], "")
                    st.markdown(f"- {lv_tag} **{pos['name']}** ({pos['code']}): "
                                f"{pos['signal_count']}条信号")
            else:
                st.success("近5日无持仓关联的市场事件信号")
        else:
            st.info("无持仓数据，跳过关联分析")
    except Exception as e:
        st.warning(f"市场事件信号加载失败: {e}")

    # ========== 数据导出 ==========
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">数据导出<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">将当前投资组合数据导出为 Excel 专业报告，包含持仓明细、收益汇总、风险分析、技术指标和告警记录。</span></div>',
        unsafe_allow_html=True,
    )

    ec1, ec2 = st.columns(2)
    with ec1:
        if st.button("📊 导出 Excel 报告", width='stretch', type="primary"):
            try:
                from src.report.excel_report import ExcelReportGenerator

                gen = ExcelReportGenerator(str(DATABASE_PATH))
                output = gen.generate()
                st.success(f"报告已生成: {output}")
                with open(output, "rb") as f:
                    st.download_button(
                        label="⬇ 下载 Excel 报告",
                        data=f.read(),
                        file_name=os.path.basename(output),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width='stretch',
                    )
            except Exception as e:
                st.error(f"导出失败: {e}")
    with ec2:
        if st.button("📄 导出 HTML 日报", width='stretch'):
            try:
                from src.utils.email_report import EmailReportBuilder

                builder = EmailReportBuilder(str(DATABASE_PATH))
                html = builder.build_daily_report()
                report_path = builder.save_report(html)
                st.success(f"报告已生成: {report_path}")
                with open(report_path, "r", encoding="utf-8") as f:
                    st.download_button(
                        label="⬇ 下载 HTML 日报",
                        data=f.read(),
                        file_name=os.path.basename(report_path),
                        mime="text/html",
                        width='stretch',
                    )
            except Exception as e:
                st.error(f"导出失败: {e}")

    # === 闭环反馈: 市场环境快览面板 ===
    with st.expander("📊 市场环境快览", expanded=False):
        try:
            env_conn = get_db_connection()
            # 资金流快览
            ff_sql = ("SELECT code, SUM(net_inflow) as total_net, COUNT(*) as days "
                      "FROM fund_flows WHERE source='etf' AND trade_date >= date('now','-7 days') "
                      "GROUP BY code ORDER BY total_net DESC LIMIT 10")
            ff_df = pd.read_sql(ff_sql, env_conn)
            if not ff_df.empty:
                st.subheader("ETF资金流向 (近7日)")
                st.dataframe(ff_df, use_container_width=True, hide_index=True)

            # 市场情绪快览
            ms_sql = ("SELECT name, value, date "
                      "FROM market_sentiment WHERE date >= date('now','-3 days') "
                      "ORDER BY trade_date DESC")
            ms_df = pd.read_sql(ms_sql, env_conn)
            if not ms_df.empty:
                st.subheader("市场情绪指标")
                ms_latest = ms_df.drop_duplicates('name', keep='first')
                st.dataframe(ms_latest, use_container_width=True, hide_index=True)

            # 宏观指标快览
            mc_sql = ("SELECT name, value, 'N/A' as unit, date "
                      "FROM macro_daily WHERE date >= date('now','-3 days') "
                      "ORDER BY trade_date DESC")
            mc_df = pd.read_sql(mc_sql, env_conn)
            if not mc_df.empty:
                st.subheader("宏观指标")
                mc_latest = mc_df.drop_duplicates('name', keep='first')
                st.dataframe(mc_latest, use_container_width=True, hide_index=True)

            env_conn.close()
        except Exception as e:
            st.warning(f"市场环境数据加载失败: {e}")
