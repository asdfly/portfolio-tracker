"""
Tab8: 操作建议
"""

from components.ui import render_chart, render_empty_state
import logging
import os
from datetime import datetime
import streamlit as st
logger = logging.getLogger(__name__)
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from config.settings import ETF_CATEGORIES, SECTOR_COLORS, DATABASE_PATH
from tabs._helpers import _load_tech_signals
from src.utils.database import get_db_connection
from data_loader import load_positions, load_summary
import sqlite3



def _render_suggestions_compute(positions, summary):
    conn = get_db_connection()
    try:
        held_codes = positions["code"].tolist()
        if held_codes:
            tech_df = _load_tech_signals(tuple(held_codes), _full=True)
        else:
            tech_df = pd.DataFrame()

    except sqlite3.OperationalError:
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
            boll_pos = tr.get("bollinger_position", 50)
            try:
                boll_pct = float(boll_pos) if not pd.isna(boll_pos) else 50.0
            except (ValueError, TypeError):
                boll_pct = 50.0
            if boll_pct <= 20:
                buy_signals += 0.5
                reasons.append(f"布林带低位({boll_pct:.0f}%)")
            elif boll_pct >= 80:
                sell_signals += 0.5
                reasons.append(f"布林带高位({boll_pct:.0f}%)")

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
    return suggestions, action_colors

def _render_suggestions_compute_v2(positions, summary):
    """基于多因子综合评分生成操作建议（v2: 替代纯技术面逻辑）。
    四维因子: 技术(40%) + 风险(25%) + 资金(20%) + 基本面(15%)
    风险约束: 高风险ETF建议上限为"持有"
    """
    from data_loader import load_multi_factor_scores
    action_colors = {
        "买入": "#22c55e", "持有": "#f59e0b", "观望": "#8b949e",
        "卖出": "#ef4444", "加仓": "#22c55e", "减仓": "#ef4444",
    }
    try:
        mf_scores = load_multi_factor_scores(positions)
    except (ImportError, ValueError, TypeError, KeyError) as e:
        logger.warning(f"Multi-factor load error: {e}")
        return [], action_colors

    # 从 positions 构建 pnl_rate 查找表
    pnl_map = {}
    if positions is not None and not positions.empty:
        for _, pos in positions.iterrows():
            code = str(pos["code"])
            pnl_rate = pos.get("pnl_rate", 0)
            try:
                pnl_rate = float(pnl_rate) if not pd.isna(pnl_rate) else 0.0
            except (ValueError, TypeError):
                pnl_rate = 0.0
            pnl_map[code] = pnl_rate

    # 从 etf_technical 读取 RSI 值
    rsi_map = {}
    tech_codes = [str(mf.code) for mf in mf_scores]
    if tech_codes:
        try:
            conn_tech = get_db_connection()
            ph = ",".join(["?" for _ in tech_codes])
            tech_df = pd.read_sql_query(
                f"SELECT code, rsi_value, trend FROM etf_technical "
                f"WHERE code IN ({ph}) ORDER BY date DESC",
                conn_tech, params=tech_codes)
            conn_tech.close()
            latest_tech = tech_df.drop_duplicates("code", keep="first")
            for _, tr in latest_tech.iterrows():
                rsi_map[str(tr["code"])] = float(tr["rsi_value"]) if not pd.isna(tr["rsi_value"]) else 50.0
        except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError):
            pass

    # ETF_CATEGORIES sector 查找
    from config.settings import ETF_CATEGORIES

    suggestions = []
    for mf in mf_scores:
        code = str(mf.code)
        cat_info = ETF_CATEGORIES.get(code, {})
        suggestions.append({
            "name": mf.name, "code": code, "sector": cat_info.get("sector", "未知"),
            "action": mf.action, "urgency": mf.urgency,
            "reasons": mf.reasons,
            "buy_score": mf.technical.score,
            "sell_score": 100.0 - mf.technical.score,
            "net_signal": mf.total_score,
            "pnl_rate": pnl_map.get(code, 0.0),
            "market_value": 0,
            "trend": mf.technical.level,
            "rsi": rsi_map.get(code, 50.0),
            "mf_total": mf.total_score,
            "mf_tech": mf.technical.score,
            "mf_risk": 100.0 - mf.risk.score,
            "mf_flow": mf.fund_flow.score,
            "mf_fund": mf.fundamental.score,
            "mf_risk_constrained": mf.risk_constrained,
        })
    return suggestions, action_colors


def _render_multi_factor_radar(suggestions):
    """渲染多因子评分雷达图（仅当suggestions含mf_字段时显示）。"""
    if not suggestions or "mf_total" not in suggestions[0]:
        return
    categories = ["技术面", "风险面", "资金面", "基本面"]
    fig = go.Figure()
    for s in suggestions[:6]:
        fig.add_trace(go.Scatterpolar(
            r=[s["mf_tech"], s["mf_risk"], s["mf_flow"], s["mf_fund"]],
            theta=categories, fill="toself", name=s["name"], opacity=0.5,
        ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10, color="#c9d1d9")),
        paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
        height=350, margin=dict(l=40, r=40, t=40, b=40),
        title=dict(text="多因子评分对比", font=dict(size=13, color="#c9d1d9")),
    )
    render_chart(fig)


def _render_suggestion_cards(suggestions, action_colors):
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


def _render_suggestion_pie(suggestions, action_colors):
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
            render_chart(fig_pie)

        with viz_col2:
            # 信号强度热力图 (ETF x 指标维度)
            # 直接从etf_technical表读取原始指标信号，不依赖reasons关键词
            _heat_codes = [s.get("code", "") for s in suggestions]
            _heat_data = {}
            if _heat_codes:
                try:
                    _conn = get_db_connection()
                    _ph = ",".join(["?" for _ in _heat_codes])
                    _tech = pd.read_sql_query(
                        f"SELECT code, ma_signal, macd_signal, rsi_value, rsi_status, "
                        f"kdj_signal, bollinger_position, trend "
                        f"FROM etf_technical WHERE code IN ({_ph}) "
                        f"ORDER BY date DESC", _conn, params=_heat_codes)
                    _conn.close()
                    _latest = _tech.drop_duplicates("code", keep="first")
                    for _, _r in _latest.iterrows():
                        _heat_data[_r["code"]] = _r
                except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError):
                    pass

            def _signal_val(code, indicator):
                """返回1(多), -1(空), 0(中性)"""
                tr = _heat_data.get(code)
                if tr is None:
                    return 0
                if indicator == "MA":
                    ma = str(tr.get("ma_signal", ""))
                    if ma == "多头排列" or ma == "金叉": return 1
                    if ma == "空头排列" or ma == "死叉": return -1
                elif indicator == "MACD":
                    m = str(tr.get("macd_signal", ""))
                    if m in ("金叉", "多头", "看多"): return 1
                    if m in ("死叉", "空头"): return -1
                elif indicator == "RSI":
                    s = str(tr.get("rsi_status", ""))
                    if s in ("超卖", "严重超卖"): return 1
                    if s in ("超买", "严重超买"): return -1
                elif indicator == "KDJ":
                    k = str(tr.get("kdj_signal", ""))
                    if "金叉" in k: return 1
                    if "死叉" in k: return -1
                elif indicator == "布林带":
                    b = tr.get("bollinger_position", 50)
                    try:
                        b = float(b) if not pd.isna(b) else 50.0
                    except (ValueError, TypeError):
                        b = 50.0
                    if b <= 20: return 1
                    if b >= 80: return -1
                elif indicator == "趋势":
                    t = str(tr.get("trend", ""))
                    if "上涨" in t: return 1
                    if "下跌" in t: return -1
                return 0

            indicators = ["MA", "MACD", "RSI", "KDJ", "布林带", "趋势", "盈亏"]
            _z = []
            _text = []
            for s in suggestions:
                row_z = []
                row_text = []
                for ind in indicators:
                    if ind == "盈亏":
                        pnl = s.get("pnl_rate", 0)
                        try:
                            pnl = float(pnl)
                        except (ValueError, TypeError):
                            pnl = 0
                        v = 1 if pnl > 0 else (-1 if pnl < 0 else 0)
                    else:
                        v = _signal_val(s.get("code", ""), ind)
                    row_z.append(v)
                    row_text.append("+" if v > 0 else ("-" if v < 0 else "·"))
                _z.append(row_z)
                _text.append(row_text)

            fig_heat = go.Figure(go.Heatmap(
                z=_z,
                x=indicators,
                y=[s["name"] for s in suggestions],
                colorscale=[[0, "#ef4444"], [0.5, "#1c2128"], [1, "#22c55e"]],
                zmid=0,
                text=_text,
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
            render_chart(fig_heat)


def _render_suggestion_details(suggestions, action_colors):
    # ===== 建议详情 =====
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">建议详情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于技术信号与持仓收益的智能调仓建议，包含多空评分、操作方向及信号来源。</span></div>',
        unsafe_allow_html=True,
    )

    # 加载信号置信度数据用于在卡片中展示
    conf_lookup = {}
    try:
        _conf_conn = get_db_connection()
        _conf_rows = pd.read_sql_query(
            "SELECT code, composite_confidence, composite_grade, signal_value, indicator "
            "FROM signal_confidence_current WHERE composite_confidence IS NOT NULL",
            _conf_conn)
        _conf_conn.close()
        for _, row in _conf_rows.iterrows():
            key = row["code"]
            if key not in conf_lookup or row["composite_confidence"] > conf_lookup[key]["score"]:
                conf_lookup[key] = {
                    "score": row["composite_confidence"],
                    "grade": row["composite_grade"],
                    "signal": row["signal_value"],
                    "indicator": row["indicator"],
                }
    except Exception:
        pass

    grade_colors = {"A": "#22c55e", "B": "#84cc16", "C": "#f59e0b", "D": "#6b7280"}

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

        # 信号置信度徽章
        conf_info = conf_lookup.get(s["code"])
        if conf_info:
            gc = grade_colors.get(conf_info["grade"], "#6b7280")
            conf_badge = (f'<span style="font-size:10px;color:{gc};background:{gc}15;'
                          f'padding:2px 6px;border-radius:3px;margin-left:6px;">'
                          f'回测置信 {conf_info["score"]:.0f} [{conf_info["grade"]}]</span>')
        else:
            conf_badge = ""

        st.markdown(
            f'<div style="background:#161b22;border-radius:6px;padding:12px 14px;margin-bottom:6px;border-left:3px solid {action_color};">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;">'
            f"<div>"
            f'<span style="font-size:14px;font-weight:bold;color:#e6edf3;">{s["name"]}</span>'
            f'<span style="font-size:11px;color:#484f58;margin-left:8px;">{s["code"]}</span>'
            f'<span style="font-size:11px;color:{sector_color};background:{sector_color}15;padding:1px 6px;border-radius:3px;margin-left:6px;">{s["sector"]}</span>'
            f"{conf_badge}"
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
            f'<span>RSI: <b style="color:{"#ef4444" if s["rsi"] >= 70 else "#22c55e" if s["rsi"] <= 30 else "#8b949e"};">{s["rsi"]:.0f}</b></span>'
            f"</div></div>",
            unsafe_allow_html=True,
        )

    if not suggestions:
        st.info("暂无足够技术数据生成操作建议")

def _render_signal_confidence(positions):
    """渲染当前持仓ETF的信号置信度面板。

    展示每只ETF当前技术信号的回测置信度：
    - 指标 × 前瞻窗口的热力表格
    - 综合置信度评分与等级
    - 命中率趋势图
    """
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '信号置信度分析<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '基于历史回测，量化各技术信号对未来5/10/20/30/60日收益方向的预测准确率与置信度。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    try:
        conn = get_db_connection()

        conf_df = pd.read_sql_query("""
            SELECT code, name, indicator, signal_value, signal_direction,
                   market_regime, scope,
                   conf_5d, conf_10d, conf_20d, conf_30d, conf_60d,
                   composite_confidence, composite_grade,
                   hit_rate_5d, hit_rate_10d, hit_rate_20d, hit_rate_30d, hit_rate_60d
            FROM signal_confidence_current
            WHERE composite_confidence IS NOT NULL
            ORDER BY composite_confidence DESC
        """, conn)

        if conf_df.empty:
            st.info("暂无信号置信度数据，请先运行回测引擎（run_analysis 阶段六b）。")
            conn.close()
            return

        ind_map = {
            "ma_signal": "均线信号", "macd_signal": "MACD信号",
            "rsi_status": "RSI状态", "kdj_signal": "KDJ信号",
            "bollinger": "布林带位置", "trend": "趋势信号",
            "combo": "组合信号",
        }

        total_signals = len(conf_df)
        grade_a = len(conf_df[conf_df["composite_grade"] == "A"])
        grade_b = len(conf_df[conf_df["composite_grade"] == "B"])
        avg_conf = conf_df["composite_confidence"].mean()

        cc1, cc2, cc3, cc4 = st.columns(4)
        regime_labels = {"bull": "牛市", "bear": "熊市", "sideways": "震荡"}
        _regime_vals = conf_df["market_regime"].dropna()
        current_regime_label = regime_labels.get(_regime_vals.iloc[0] if not _regime_vals.empty else "all", "未知")
        cc1.metric("有效信号数", total_signals)
        cc2.metric("A级(高置信)", grade_a)
        cc3.metric("B级(中置信)", grade_b)
        cc4.metric(f"当前: {current_regime_label}", f"{avg_conf:.1f}")

        etf_options = conf_df[["code", "name"]].drop_duplicates().sort_values("name")
        etf_options["label"] = etf_options["name"] + " (" + etf_options["code"] + ")"

        sel_col1, sel_col2 = st.columns([3, 1])
        with sel_col1:
            sel_etf = st.selectbox(
                "选择ETF查看信号置信度",
                options=etf_options["code"].tolist(),
                format_func=lambda c: etf_options[etf_options["code"] == c]["label"].iloc[0],
                key="sig_conf_etf",
            )
        with sel_col2:
            show_all = st.checkbox("显示全部ETF", value=False, key="sig_conf_all")

        if show_all:
            display_df = conf_df.copy()
        else:
            display_df = conf_df[conf_df["code"] == sel_etf].copy()

        if display_df.empty:
            st.info("该ETF暂无有效信号置信度数据")
            conn.close()
            return

        st.markdown("**信号置信度矩阵（分数/等级）**")
        display_df = display_df.copy()
        display_df["指标"] = display_df["indicator"].map(lambda x: ind_map.get(x, x))
        display_df["当前信号"] = display_df["signal_value"]
        display_df["方向"] = display_df["signal_direction"].map(lambda d: "看多" if d > 0 else ("看空" if d < 0 else "中性"))
        if "scope" in display_df.columns:
            display_df["scope"] = display_df["scope"].map(lambda s: "ETF" if s == "etf" else "全市场" if s == "all" else s)
        else:
            display_df["scope"] = "-"
        display_df["综合"] = display_df.apply(
            lambda r: f"{r['composite_confidence']:.1f} [{r['composite_grade']}]", axis=1
        )

        for n, label in [(5, "5日"), (10, "10日"), (20, "20日"), (30, "30日"), (60, "60日")]:
            col = f"conf_{n}d"
            display_df[label] = display_df.apply(
                lambda r: f"{r[col]:.1f}" if pd.notna(r[col]) else "-", axis=1
            )

        table_cols = ["name", "指标", "当前信号", "方向", "scope", "5日", "10日", "20日", "30日", "60日", "综合"]
        st.dataframe(
            display_df[table_cols].rename(columns={"name": "ETF"}),
            use_container_width=True,
            hide_index=True,
            height=min(200 + len(display_df) * 35, 500),
        )

        st.markdown("**各信号命中率（偏离50%越大越有效）**")
        if not show_all:
            etf_conf = conf_df[conf_df["code"] == sel_etf].copy()
        else:
            etf_conf = conf_df.copy()

        etf_conf = etf_conf[etf_conf["signal_direction"] != 0]
        if not etf_conf.empty:
            windows = [5, 10, 20, 30, 60]
            hr_cols = [f"hit_rate_{n}d" for n in windows]
            hr_labels = [f"{n}日" for n in windows]

            fig = go.Figure()
            for _, row in etf_conf.iterrows():
                ind_label = ind_map.get(row["indicator"], row["indicator"])
                sig_label = f"{row['name']} - {ind_label}({row['signal_value']})"
                values = [row[c] if pd.notna(row[c]) else None for c in hr_cols]
                fig.add_trace(go.Scatter(
                    x=hr_labels, y=values, mode="lines+markers",
                    name=sig_label, hovertemplate="%{y:.1%}<extra></extra>",
                ))
            fig.add_hline(y=0.5, line_dash="dash", line_color="gray",
                          annotation_text="随机基准(50%)")
            fig.update_layout(
                yaxis_title="命中率", xaxis_title="前瞻窗口",
                height=350, margin=dict(l=40, r=20, t=20, b=40),
                legend=dict(font_size=10, orientation="h", yanchor="bottom", y=-0.3),
            )
            render_chart(fig)
        else:
            st.info("该ETF当前无方向性信号")

        conn.close()

    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        st.warning(f"信号置信度数据加载失败: {e}")


def _render_backtest_heatmap():
    """渲染全市场信号回测热力图与排名。

    展示所有信号的回测统计结果：
    - 热力图：指标信号 × 前瞻窗口，颜色编码置信度分数
    - 排名表：按置信度排序的Top/Bottom信号
    - 指标汇总：各指标平均置信度与命中率
    """
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '信号回测统计<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '全市场历史回测：6类指标x22种信号x5个前瞻窗口x4种市场状态，共420组统计。含收益加权命中率+市场状态分层。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    try:
        conn = get_db_connection()

        # 市场状态选择器
        regime_options = {"all": "全部市场状态", "bull": "牛市", "bear": "熊市", "sideways": "震荡市"}
        sel_regime = st.selectbox(
            "选择市场状态",
            options=list(regime_options.keys()),
            format_func=lambda r: regime_options[r],
            key="backtest_regime_sel",
        )

        stats_df = pd.read_sql_query("""
            SELECT indicator, signal_value, signal_direction, forward_window,
                   market_regime, sample_count, hit_count, hit_rate,
                   weighted_hit_rate, avg_return, std_return,
                   t_statistic, p_value, confidence_score, confidence_grade
            FROM signal_backtest_stats
            WHERE market_regime = ?
            ORDER BY confidence_score DESC
        """, conn, params=(sel_regime,))
        conn.close()

        if stats_df.empty:
            st.info("暂无回测统计数据，请先运行回测引擎。")
            return

        ind_map = {
            "ma_signal": "均线", "macd_signal": "MACD",
            "rsi_status": "RSI", "kdj_signal": "KDJ",
            "bollinger": "布林带", "trend": "趋势",
            "combo": "组合",
        }

        st.markdown("**置信度热力图（信号 x 前瞻窗口）**")
        pivot_conf = stats_df.pivot_table(
            index=["indicator", "signal_value"],
            columns="forward_window",
            values="confidence_score",
            aggfunc="first",
        )
        pivot_conf = pivot_conf.reindex(
            sorted(pivot_conf.index, key=lambda x: (x[0], x[1]))
        )
        y_labels = [f"{ind_map.get(ind, ind)} - {sig}" for ind, sig in pivot_conf.index]

        fig_heat = go.Figure(data=go.Heatmap(
            z=pivot_conf.values,
            x=[f"{c}日" for c in pivot_conf.columns],
            y=y_labels,
            colorscale=[[0, "#1a1a2e"], [0.3, "#16213e"], [0.5, "#0f3460"],
                        [0.7, "#533483"], [0.85, "#e94560"], [1, "#22c55e"]],
            text=[[f"{v:.1f}" if pd.notna(v) else "" for v in row]
                  for row in pivot_conf.values],
            texttemplate="%{text}",
            hovertemplate="信号: %{y}<br>窗口: %{x}<br>置信度: %{z:.1f}<extra></extra>",
            colorbar=dict(title="置信度", x=1.02),
        ))
        fig_heat.update_layout(
            height=max(400, len(y_labels) * 28),
            margin=dict(l=10, r=60, t=10, b=20),
            yaxis=dict(autorange="reversed"),
        )
        render_chart(fig_heat)

        tc1, tc2 = st.columns(2)
        with tc1:
            st.markdown("**Top 10 高置信度信号**")
            top_df = stats_df.head(10).copy()
            top_df["指标"] = top_df["indicator"].map(lambda x: ind_map.get(x, x))
            top_df["信号"] = top_df["signal_value"]
            top_df["窗口"] = top_df["forward_window"].map(lambda x: f"{x}日")
            top_df["样本"] = top_df["sample_count"]
            top_df["命中率"] = top_df["hit_rate"].map(lambda x: f"{x:.1%}")
            top_df["加权命中"] = top_df["weighted_hit_rate"].map(lambda x: f"{x:+.1%}" if pd.notna(x) else "-")
            top_df["均收益"] = top_df["avg_return"].map(lambda x: f"{x:+.2%}")
            top_df["p值"] = top_df["p_value"].map(lambda x: f"{x:.4f}" if x > 0 else "<0.0001")
            top_df["置信度"] = top_df.apply(
                lambda r: f"{r['confidence_score']:.1f} [{r['confidence_grade']}]", axis=1
            )
            st.dataframe(
                top_df[["指标", "信号", "窗口", "样本", "命中率", "加权命中", "均收益", "p值", "置信度"]],
                use_container_width=True, hide_index=True, height=300,
            )

        with tc2:
            st.markdown("**Bottom 10 低置信度信号**")
            bot_df = stats_df.tail(10).copy()
            bot_df["指标"] = bot_df["indicator"].map(lambda x: ind_map.get(x, x))
            bot_df["信号"] = bot_df["signal_value"]
            bot_df["窗口"] = bot_df["forward_window"].map(lambda x: f"{x}日")
            bot_df["样本"] = bot_df["sample_count"]
            bot_df["命中率"] = bot_df["hit_rate"].map(lambda x: f"{x:.1%}")
            bot_df["加权命中"] = bot_df["weighted_hit_rate"].map(lambda x: f"{x:+.1%}" if pd.notna(x) else "-")
            bot_df["均收益"] = bot_df["avg_return"].map(lambda x: f"{x:+.2%}")
            bot_df["p值"] = bot_df["p_value"].map(lambda x: f"{x:.4f}" if x > 0 else "<0.0001")
            bot_df["置信度"] = bot_df.apply(
                lambda r: f"{r['confidence_score']:.1f} [{r['confidence_grade']}|-]", axis=1
            )
            st.dataframe(
                bot_df[["指标", "信号", "窗口", "样本", "命中率", "加权命中", "均收益", "p值", "置信度"]],
                use_container_width=True, hide_index=True, height=300,
            )

        st.markdown("**各指标回测汇总**")
        summary_df = stats_df.groupby("indicator").agg(
            统计组数=("confidence_score", "count"),
            平均置信度=("confidence_score", "mean"),
            最高置信度=("confidence_score", "max"),
            平均命中率=("hit_rate", "mean"),
            平均收益=("avg_return", "mean"),
        ).round(2).sort_values("平均置信度", ascending=False)
        summary_df.index = [ind_map.get(x, x) for x in summary_df.index]
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        st.warning(f"回测统计数据加载失败: {e}")


def _render_market_events(positions, summary):
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
        ev_conn = get_db_connection()
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
    except (pd.errors.DatabaseError, sqlite3.OperationalError, ImportError, ValueError, KeyError) as e:
        st.warning(f"市场事件信号加载失败: {e}")


def _render_data_export(positions, summary, selected_benchmark, selected_date):
    # ========== 数据导出 ==========
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">数据导出<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">将当前投资组合数据导出为 Excel 专业报告，包含持仓明细、收益汇总、风险分析、技术指标和告警记录。</span></div>',
        unsafe_allow_html=True,
    )

    ec1, ec2 = st.columns(2)
    with ec1:
        if st.button("📊 导出 Excel 报告", type="primary", key="export_excel_advice"):
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
                        use_container_width=True,
                    key="dl_excel_advice",
                    )
            except (OSError, ValueError, ImportError, RuntimeError) as e:
                st.error(f"导出失败: {e}")
    with ec2:
        if st.button("📄 导出 HTML 日报", key="export_html_advice"):
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
                        use_container_width=True,
                    key="dl_html_advice",
                    )
            except (OSError, ValueError, ImportError, RuntimeError) as e:
                st.error(f"导出失败: {e}")



def _render_feedback_tracking(positions, summary):
    # ========== 闭环反馈: 建议历史追踪 ==========
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '闭环反馈: 建议历史追踪'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '追踪每条建议的生命周期：生成→确认→执行→验证效果。点击状态可更新建议进展。</span></div>',
        unsafe_allow_html=True,
    )
    try:
        fb_conn = get_db_connection()
        fb_df = pd.read_sql("""
            SELECT id, created_at, advice_type, priority, title, confidence,
                   related_codes, source, action_taken, status, feedback, resolved_at
            FROM advice_history
            ORDER BY created_at DESC
            LIMIT 50
        """, fb_conn)
        fb_conn.close()

        if not fb_df.empty:
            # Stats cards
            fb_s1, fb_s2, fb_s3, fb_s4 = st.columns(4)
            total_adv = len(fb_df)
            pending_cnt = len(fb_df[fb_df['status'] == 'pending'])
            executed_cnt = len(fb_df[fb_df['status'] == 'executed'])
            effective_cnt = len(fb_df[fb_df['status'] == 'effective'])
            fb_s1.metric("建议总数", total_adv)
            fb_s2.metric("待处理", pending_cnt)
            fb_s3.metric("已执行", executed_cnt)
            fb_s4.metric("已验证有效", effective_cnt)

            # Type distribution
            type_counts = fb_df['advice_type'].value_counts()
            type_map = {
                'rebalance': '再平衡', 'risk_mgmt': '风险管理', 'technical': '技术分析',
                'concentration': '集中度', 'opportunity': '机会识别', 'fund_flow': '资金流',
                'sentiment': '市场情绪', 'macro': '宏观环境', 'news': '新闻事件',
                'market_event': '市场事件', 'strategy': '策略建议', 'margin': '融资融券',
                'research': '机构调研', 'block_trade': '大宗交易',
            }
            st.markdown("**建议类型分布 (近50条)**")
            type_bar = pd.DataFrame({
                '类型': [type_map.get(t, t) for t in type_counts.index],
                '数量': type_counts.values
            })
            st.bar_chart(type_bar, x='类型', y='数量', height=200)

            # Advice history table with status update
            st.markdown("**建议详情**")
            status_options = ['pending', 'executed', 'ignored', 'effective', 'ineffective']
            status_labels = {
                'pending': '⏳ 待处理', 'executed': '✅ 已执行', 'ignored': '⏭️ 已忽略',
                'effective': '🎯 有效', 'ineffective': '❌ 无效',
            }

            # 分页: 每页15条
            PAGE_SIZE = 15
            total_pages = (len(fb_df) + PAGE_SIZE - 1) // PAGE_SIZE
            page = st.session_state.get("fb_page", 0)
            page = min(page, total_pages - 1) if total_pages > 0 else 0
            start_idx = page * PAGE_SIZE
            end_idx = min(start_idx + PAGE_SIZE, len(fb_df))
            page_df = fb_df.iloc[start_idx:end_idx].copy()

            # 格式化列
            page_df['优先级'] = page_df['priority'].str.upper()
            page_df['类型'] = page_df['advice_type'].map(lambda t: type_map.get(t, t))
            page_df['标题'] = page_df['title'].str[:60]
            page_df['状态'] = page_df['status'].map(lambda s: status_labels.get(s, s))
            page_df['置信度'] = page_df['confidence'].map(lambda c: f"{c:.0%}" if c else '-')
            page_df['相关标的'] = page_df['related_codes'].str[:30].fillna('-')
            page_df['时间'] = page_df['created_at']

            display_cols = ['时间', '优先级', '类型', '标题', '状态', '置信度', '相关标的']
            st.dataframe(
                page_df[display_cols],
                use_container_width=True,
                hide_index=True,
                height=min(400, 35 * len(page_df) + 40),
            )

            # 状态更新: 固定widget, 不在循环内创建
            with st.expander("更新建议状态", expanded=False):
                upd_cols = st.columns([2, 2, 1])
                with upd_cols[0]:
                    upd_id = st.selectbox(
                        "选择建议",
                        options=page_df['id'].tolist(),
                        format_func=lambda i: f"#{i} - {page_df[page_df['id']==i]['标题'].iloc[0][:40]}",
                        key="upd_advice_id",
                    )
                with upd_cols[1]:
                    upd_status = st.selectbox(
                        "新状态",
                        options=status_options,
                        format_func=lambda s: status_labels.get(s, s),
                        key="upd_advice_status",
                    )
                with upd_cols[2]:
                    st.write("")
                    if st.button("确认更新", key="upd_advice_btn", type="primary"):
                        try:
                            upd_conn = get_db_connection()
                            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            upd_conn.execute(
                                "UPDATE advice_history SET status=?, resolved_at=? WHERE id=?",
                                (upd_status, now_str, upd_id)
                            )
                            upd_conn.commit()
                            upd_conn.close()
                            st.toast(f"建议 #{upd_id} 状态更新为: {status_labels.get(upd_status, upd_status)}")
                            st.rerun()
                        except (pd.errors.DatabaseError, sqlite3.OperationalError, sqlite3.ProgrammingError, KeyError, ValueError) as upd_e:
                            st.warning(f"状态更新失败: {upd_e}")

            # 分页控件
            if total_pages > 1:
                nav_cols = st.columns([1, 2, 1])
                with nav_cols[0]:
                    if st.button("← 上一页", disabled=(page <= 0), key="fb_prev"):
                        st.session_state["fb_page"] = page - 1
                        st.rerun()
                with nav_cols[1]:
                    st.markdown(f"<div style='text-align:center;padding-top:8px;color:#8b949e;font-size:13px;'>"
                                f"第 {page + 1} / {total_pages} 页 "
                                f"({start_idx + 1}-{end_idx} / 共 {len(fb_df)} 条)</div>",
                                unsafe_allow_html=True)
                with nav_cols[2]:
                    if st.button("下一页 →", disabled=(page >= total_pages - 1), key="fb_next"):
                        st.session_state["fb_page"] = page + 1
                        st.rerun()
        else:
            st.info("暂无建议历史记录。建议会在每日分析时自动记录。")
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        st.warning(f"建议历史加载失败: {e}")


    # === 闭环反馈: 市场环境快览面板 ===
    with st.expander("📊 市场环境快览", expanded=False):
        try:
            env_conn = get_db_connection()
            # 资金流快览
            ff_sql = ("SELECT code, SUM(net_inflow) as total_net, COUNT(*) as days "
                      "FROM fund_flows WHERE category='etf' AND date >= date('now','-7 days') "
                      "GROUP BY code ORDER BY total_net DESC LIMIT 10")
            ff_df = pd.read_sql(ff_sql, env_conn)
            if not ff_df.empty:
                st.subheader("ETF资金流向 (近7日)")
                st.dataframe(ff_df, hide_index=True)

            # 市场情绪快览
            ms_sql = ("SELECT name, value, date "
                      "FROM market_sentiment WHERE date >= date('now','-3 days') "
                      "ORDER BY date DESC")
            ms_df = pd.read_sql(ms_sql, env_conn)
            if not ms_df.empty:
                st.subheader("市场情绪指标")
                ms_latest = ms_df.drop_duplicates('name', keep='first')
                st.dataframe(ms_latest, hide_index=True)

            # 宏观指标快览
            mc_sql = ("SELECT name, value, 'N/A' as unit, date "
                      "FROM macro_daily WHERE date >= date('now','-3 days') "
                      "ORDER BY date DESC")
            mc_df = pd.read_sql(mc_sql, env_conn)
            if not mc_df.empty:
                st.subheader("宏观指标")
                mc_latest = mc_df.drop_duplicates('name', keep='first')
                st.dataframe(mc_latest, hide_index=True)

            env_conn.close()
        except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
            st.warning(f"市场环境数据加载失败: {e}")
def render_tab8():
    selected_date = st.session_state.get("selected_date", "")
    selected_benchmark = st.session_state.get("selected_benchmark", "sh000300")
    positions = load_positions(selected_date)
    show_days = st.session_state.get("show_days", 250)
    summary = load_summary(show_days, selected_date)
    """渲染Tab8: 操作建议"""
    technical = pd.DataFrame()
    volatility = None
    max_dd = None
    sharpe = None
    
    st.caption("💡 基于技术信号和持仓状态，生成具体操作建议")
    
    if not positions.empty:
        suggestions, action_colors = _render_suggestions_compute_v2(positions, summary)
        _render_suggestion_cards(suggestions, action_colors)
        _render_suggestion_pie(suggestions, action_colors)
        _render_multi_factor_radar(suggestions)
        _render_suggestion_details(suggestions, action_colors)
        _render_signal_confidence(positions)
        _render_position_advice_panel(positions)
    else:
        st.info("暂无持仓数据")
    
    _render_market_events(positions, summary)
    _render_data_export(positions, summary, selected_benchmark, selected_date)
    _render_feedback_tracking(positions, summary)

    # 信号回测统计
    _render_backtest_heatmap()

    # P2-F: 盘前/盘后分析助手
    _render_pre_market_panel()
    _render_post_market_panel()




def _render_position_advice_panel(positions):
    """渲染仓位管理建议面板。"""
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '仓位管理建议</div>', unsafe_allow_html=True,
    )
    try:
        from data_loader import load_position_advices, load_sector_exposures
        advices = load_position_advices(positions)
        exposures = load_sector_exposures(positions)
    except (ImportError, ValueError, TypeError, KeyError) as e:
        logger.warning(f"Advice load error: {e}")
        st.info("仓位建议数据加载失败")
        return

    # 行业暴露度
    if exposures:
        st.markdown("**行业暴露度**")
        exp_data = []
        for e in exposures:
            color = "#ef4444" if e.status == "超限" else ("#f59e0b" if e.status == "偏高" else "#22c55e")
            exp_data.append({
                "行业": e.sector,
                "占比": f"{e.total_weight:.1f}%",
                "ETF数量": e.etf_count,
                "状态": e.status,
                "建议": e.advice if e.advice else "-",
            })
        st.dataframe(exp_data, use_container_width=True, hide_index=True,
                     height=min(200 + len(exp_data) * 28, 400))

    # 仓位建议明细
    if advices:
        st.markdown("**个股仓位建议**")
        adj_data = []
        for a in advices:
            adj_data.append({
                "代码": a.code, "名称": a.name, "行业": a.sector,
                "当前占比": f"{a.current_weight:.1f}%",
                "综合评分": f"{a.mf_total:.0f}",
                "操作": a.adjust_action,
                "调整幅度": f"{a.adjust_min_pct:.0%}-{a.adjust_max_pct:.0%}" if a.adjust_action != "维持" else "-",
                "目标占比": f"{a.target_weight_min:.1f}-{a.target_weight_max:.1f}%",
                "建议": a.advice_text,
            })
        st.dataframe(adj_data, use_container_width=True, hide_index=True,
                     height=min(200 + len(adj_data) * 28, 500))


# ============================================================
#  P2-F: 盘前/盘后分析助手
# ============================================================

def _render_pre_market_panel():
    """渲染盘前研判面板"""
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '盘前研判</div>',
        unsafe_allow_html=True,
    )
    try:
        from data_loader import load_pre_market_report
        report = load_pre_market_report()
        if not report:
            st.info("暂无盘前数据")
            return
        if report.summary_text:
            st.info(report.summary_text)
        if report.index_changes:
            idx_data = [{"指数": ic.name, "涨跌幅": f"{ic.change_pct:+.2f}%"} for ic in report.index_changes]
            st.dataframe(idx_data, use_container_width=True, hide_index=True,
                         height=min(200 + len(idx_data) * 28, 400))
        if report.macro_alerts:
            st.markdown("**宏观异动**")
            for a in report.macro_alerts:
                icon = {"warning": "🔴", "caution": "🟡"}.get(a.alert_level, "ℹ️")
                st.markdown(f"{icon} **{a.indicator_name}**: {a.description}")
        if report.etf_signals:
            st.markdown("**持仓信号预览**")
            sig_data = [{"名称": es.name, "代码": es.code, "趋势": es.trend,
                         "MACD": es.macd_signal, "RSI": f"{es.rsi_value:.0f}",
                         "技术评分": f"{es.signal_score:.0f}", "风险评分": f"{es.risk_score:.0f}",
                         "资金流(万)": f"{es.fund_flow_net:+.0f}"} for es in report.etf_signals]
            st.dataframe(sig_data, use_container_width=True, hide_index=True,
                         height=min(200 + len(sig_data) * 28, 500))
        ns = report.news_sentiment
        if ns.get("total", 0) > 0:
            nc1, nc2, nc3 = st.columns(3)
            nc1.metric("近3日新闻", ns["total"])
            nc2.metric("正面", ns["positive"])
            nc3.metric("负面", ns["negative"])
        if report.risk_warnings:
            with st.expander("⚠️ 风险预警", expanded=True):
                for w in report.risk_warnings:
                    st.markdown(f"**{w['code']}**: 综合评分 {w['total_score']:.0f} "
                                f"(波动{w['volatility']:.0f}/折价{w['discount']:.0f}/"
                                f"流动{w['liquidity']:.0f}/下行{w['downside']:.0f}/偏离{w['deviation']:.0f})")
    except Exception as e:
        st.warning(f"盘前研判加载失败: {e}")



def _render_post_market_panel():
    """渲染盘后复盘面板"""
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '盘后复盘</div>',
        unsafe_allow_html=True,
    )
    try:
        from data_loader import load_post_market_report
        report = load_post_market_report()
        if not report:
            st.info("暂无盘后数据")
            return
        if report.summary_text:
            st.info(report.summary_text)
        pnl = report.portfolio_pnl
        if pnl:
            pc1, pc2, pc3, pc4 = st.columns(4)
            with pc1:
                c = "#22c55e" if pnl.get("total_pnl", 0) >= 0 else "#ef4444"
                st.markdown(
                    f'<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
                    f'<div style="font-size:11px;color:#666;">总盈亏</div>'
                    f'<div style="font-size:18px;font-weight:bold;color:{c};">{pnl["total_pnl"]:+,.0f}</div></div>',
                    unsafe_allow_html=True)
            with pc2:
                ret = pnl.get("total_return_pct", 0)
                rc = "#22c55e" if ret >= 0 else "#ef4444"
                st.markdown(
                    f'<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
                    f'<div style="font-size:11px;color:#666;">收益率</div>'
                    f'<div style="font-size:18px;font-weight:bold;color:{rc};">{ret:+.2f}%</div></div>',
                    unsafe_allow_html=True)
            with pc3:
                st.markdown(
                    f'<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
                    f'<div style="font-size:11px;color:#666;">盈/亏</div>'
                    f'<div style="font-size:18px;font-weight:bold;">{pnl.get("profit_count",0)} / {pnl.get("loss_count",0)}</div></div>',
                    unsafe_allow_html=True)
            with pc4:
                st.markdown(
                    f'<div style="text-align:center;padding:10px;border-radius:6px;background:#f8f9fa;">'
                    f'<div style="font-size:11px;color:#666;">胜率</div>'
                    f'<div style="font-size:18px;font-weight:bold;">{pnl.get("win_rate",0):.0f}%</div></div>',
                    unsafe_allow_html=True)
        if report.pnl_attribution:
            attr_data = [{"名称": a["name"], "代码": a["code"], "盈亏": a["pnl"],
                          "收益率%": round(a["pnl_rate"], 2),
                          "贡献度%": round(a["contribution_pct"], 1)}
                         for a in sorted(report.pnl_attribution, key=lambda x: x["pnl"], reverse=True)]
            st.dataframe(attr_data, use_container_width=True, hide_index=True,
                         height=min(200 + len(attr_data) * 28, 500))
        if report.signal_changes:
            st.markdown("**技术信号变化**")
            for sc in report.signal_changes:
                for ch in sc["changes"]:
                    st.markdown(f"- **{sc['code']}** {ch['dimension']}: {ch['from']} → {ch['to']}")
        if report.fund_flow_changes:
            with st.expander("资金流向变化", expanded=False):
                flow_data = [{"代码": fc["code"], "今日(万)": round(fc["today_flow"]),
                              "昨日(万)": round(fc["yesterday_flow"]),
                              "变化(万)": round(fc["flow_change"])} for fc in report.fund_flow_changes[:20]]
                st.dataframe(flow_data, use_container_width=True, hide_index=True)
        if report.news_highlights:
            with st.expander("今日重大新闻", expanded=False):
                for nh in report.news_highlights:
                    icon = "🟢" if nh["sentiment"] == "正面" else ("🔴" if nh["sentiment"] == "负面" else "⚪")
                    st.markdown(f"{icon} [{nh['date']}] **{nh['title']}** — {nh['category']}")
    except Exception as e:
        st.warning(f"盘后复盘加载失败: {e}")
