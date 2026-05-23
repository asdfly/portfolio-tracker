"""
Tab7: 资讯与评估
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import math
from config.settings import ETF_CATEGORIES, SECTOR_COLORS
from tabs._helpers import _load_latest_news, _load_tech_signals
from src.utils.database import get_db_connection


def render_tab7(positions, summary, index_quotes, selected_date, selected_benchmark, **kwargs):
    # 从kwargs获取额外的变量
    technical = kwargs.get('technical', pd.DataFrame())
    volatility = kwargs.get('volatility', None)
    max_dd = kwargs.get('max_dd', None)
    sharpe = kwargs.get('sharpe', None)

    # """渲染Tab7: 资讯与评估"""
    
    st.caption("📰 持仓相关市场资讯与综合评估，帮助把握投资时机")

    # ===== 资讯面板 =====
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">市场资讯<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示与持仓板块相关的最新市场新闻，按行业板块分类。</span></div>',
        unsafe_allow_html=True,
    )

    news_categories_map = {
        "医药": "医药板块",
        "金融": "券商板块",
        "军工": "军工板块",
        "新能源": "大盘行情",
        "科技": "AI板块",
        "宽基": "ETF市场",
        "红利": "大盘行情",
        "债券": "大盘行情",
    }
    if not positions.empty:
        held_sectors = set()
        for _, pos in positions.iterrows():
            code = str(pos["code"])
            cat_info = ETF_CATEGORIES.get(code)
            if cat_info:
                held_sectors.add(cat_info["sector"])

        news_cats_to_load = set()
        for sector in held_sectors:
            cat = news_categories_map.get(sector, "大盘行情")
            news_cats_to_load.add(cat)
        news_cats_to_load.add("大盘行情")
        news_cats_to_load.add("ETF市场")
    else:
        news_cats_to_load = ["大盘行情", "ETF市场"]

    news_df = _load_latest_news(tuple(news_cats_to_load))

    if not news_df.empty:
        # Category filter
        all_cats = sorted(news_df["category"].unique())
        selected_cat = st.selectbox(
            "筛选板块", ["全部"] + all_cats, key="news_cat_filter", label_visibility="collapsed"
        )
        if selected_cat != "全部":
            filtered_news = news_df[news_df["category"] == selected_cat]
        else:
            filtered_news = news_df

        cat_color_map = {
            "大盘行情": "#58a6ff",
            "ETF市场": "#06b6d4",
            "医药板块": "#22c55e",
            "券商板块": "#58a6ff",
            "军工板块": "#ef4444",
            "AI板块": "#a855f7",
            "新能源": "#f59e0b",
        }

        # 每个分类最多显示 5 条，避免单一板块刷屏
        if not filtered_news.empty:
            _display = filtered_news.groupby("category").head(5).reset_index(drop=True)
        else:
            _display = filtered_news
        for _, row in _display.iterrows():
            cat_color = cat_color_map.get(row["category"], "#8b949e")
            summary_text = row.get("summary", "") or ""
            summary_html = (
                f'<div style="font-size:12px;color:#6e7681;margin-top:4px;line-height:1.5;">{summary_text[:150]}{"..." if len(summary_text) > 150 else ""}</div>'
                if summary_text
                else ""
            )
            url_html = (
                f'<a href="{row["url"]}" target="_blank" style="font-size:11px;color:#58a6ff;">{row["source"]} | {row.get("publish_time", "")[:16]}</a>'
                if pd.notna(row.get("url")) and row["url"]
                else f'<span style="font-size:11px;color:#484f58;">{row["source"]}</span>'
            )
            st.markdown(
                f'<div style="background:#161b22;border-radius:6px;padding:12px 14px;margin-bottom:6px;border-left:3px solid {cat_color};">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<span style="font-size:11px;color:{cat_color};background:{cat_color}15;padding:2px 8px;border-radius:3px;">{row["category"]}</span>'
                f'<span style="font-size:11px;color:#484f58;">{row["date"]}</span>'
                f"</div>"
                f'<div style="font-size:13px;color:#e6edf3;font-weight:bold;margin-top:6px;line-height:1.4;">{row["title"]}</div>'
                f"{summary_html}"
                f'<div style="margin-top:6px;">{url_html}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
    else:
        st.info("暂无市场资讯数据，请检查数据采集服务是否正常运行")

    st.markdown("---")

    # ===== 综合评估面板 =====
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">综合评估<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">基于收益、风险、技术面多维度的综合投资评分。</span></div>',
        unsafe_allow_html=True,
    )

    if not summary.empty and not positions.empty:

        # 收益评分 (30分)
        port_daily = summary["total_value"].pct_change().dropna()
        total_ret = (
            (summary["total_value"].iloc[-1] / summary["total_value"].iloc[0] - 1)
            if summary["total_value"].iloc[0] > 0
            else 0
        )
        ann_ret = port_daily.mean() * 252 if len(port_daily) > 0 else 0
        if total_ret > 0.1:
            score_return = 30
        elif total_ret > 0.05:
            score_return = 24
        elif total_ret > 0:
            score_return = 18
        elif total_ret > -0.05:
            score_return = 10
        else:
            score_return = 5

        # 风险评分 (30分)
        if volatility and not np.isnan(volatility):
            if volatility < 10:
                score_risk = 28
            elif volatility < 15:
                score_risk = 24
            elif volatility < 20:
                score_risk = 18
            elif volatility < 25:
                score_risk = 12
            else:
                score_risk = 6
        else:
            score_risk = 15

        if max_dd and not np.isnan(max_dd):
            dd = abs(max_dd)
            if dd < 5:
                score_risk = min(score_risk + 2, 30)
            elif dd > 15:
                score_risk = max(score_risk - 5, 0)

        # 技术面评分 (25分)
        conn2 = get_db_connection()
        try:
            held_codes = positions["code"].tolist()[:5]
            if held_codes:
                tech_df = _load_tech_signals(tuple(held_codes), _full=False)
            else:
                tech_df = pd.DataFrame()

        except Exception:
            tech_df = pd.DataFrame()
        finally:
            conn2.close()

        tech_score = 0
        tech_signals = []
        if not tech_df.empty:
            latest_tech = tech_df.drop_duplicates("code", keep="first")
            for _, tr in latest_tech.iterrows():
                etf_name = ETF_CATEGORIES.get(str(tr["code"]), {}).get("name", tr["code"])
                etf_score = 0
                if tr.get("ma_signal") == "多头排列":
                    etf_score += 3
                    tech_signals.append(f"{etf_name}: 均线多头排列")
                elif tr.get("ma_signal") == "空头排列":
                    etf_score -= 1
                macd_val = tr.get("macd_signal")
                if macd_val in ("金叉", "多头", "看多"):
                    etf_score += 2
                    tech_signals.append(f"{etf_name}: MACD{macd_val}")
                elif macd_val in ("死叉", "空头"):
                    etf_score -= 1
                rsi_st = tr.get("rsi_status")
                if rsi_st in ("超卖", "严重超卖"):
                    etf_score += 1
                elif rsi_st in ("超买", "严重超买"):
                    etf_score -= 1
                _trend = str(tr.get("trend", ""))
                if "上涨" in _trend:
                    etf_score += 2
                elif _trend in ("下跌", "温和下跌"):
                    etf_score -= 1
                tech_score += etf_score
            tech_score = max(0, min(25, 10 + tech_score))

        # 持仓健康度评分 (15分)
        score_health = 15
        total_mv = positions["market_value"].sum()
        max_weight = positions["market_value"].max() / total_mv if total_mv > 0 else 0
        if max_weight > 30:
            score_health -= 5
        elif max_weight > 20:
            score_health -= 2
        loss_ratio = len(positions[positions["pnl"] < 0]) / len(positions) if len(positions) > 0 else 0
        if loss_ratio > 0.6:
            score_health -= 5
        elif loss_ratio > 0.4:
            score_health -= 2
        score_health = max(0, score_health)

        total_score = score_return + score_risk + tech_score + score_health
        score_color = "#22c55e" if total_score >= 70 else "#f59e0b" if total_score >= 45 else "#ef4444"
        score_label = (
            "优秀"
            if total_score >= 70
            else "良好" if total_score >= 55 else "一般" if total_score >= 40 else "较差"
        )

        # 渲染评分
        col_score1, col_score2 = st.columns([1, 2])
        with col_score1:
            fig_score_gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=total_score,
                    number={"suffix": "分", "font": {"size": 42, "color": score_color}},
                    gauge={
                        "axis": {"range": [0, 100], "tickcolor": "#8b949e", "tickfont": {"size": 10}},
                        "bar": {"color": score_color},
                        "bgcolor": "#161b22",
                        "steps": [
                            {"range": [0, 40], "color": "rgba(239,68,68,0.12)"},
                            {"range": [40, 70], "color": "rgba(245,158,11,0.12)"},
                            {"range": [70, 100], "color": "rgba(34,197,94,0.12)"},
                        ],
                        "threshold": {
                            "line": {"color": score_color, "width": 3},
                            "thickness": 0.8,
                            "value": total_score,
                        },
                    },
                )
            )
            fig_score_gauge.update_layout(
                height=220,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#c9d1d9"),
                margin=dict(l=20, r=20, t=5, b=5),
            )
            st.plotly_chart(fig_score_gauge, width="stretch")
            st.markdown(
                f'<div style="text-align:center;color:{score_color};font-size:15px;font-weight:bold;">{score_label}</div>',
                unsafe_allow_html=True,
            )

        with col_score2:
            score_items = [
                ("收益能力", score_return, 30, "累计收益表现"),
                ("风险控制", score_risk, 30, "波动率与回撤水平"),
                ("技术面", tech_score, 25, "均线/MACD/RSI信号"),
                ("持仓健康", score_health, 15, "分散度与盈亏比"),
            ]
            for name, score, max_s, desc in score_items:
                pct = score / max_s * 100 if max_s > 0 else 0
                bar_color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 40 else "#ef4444"
                st.markdown(
                    f'<div style="margin-bottom:8px;">'
                    f'<div style="display:flex;justify-content:space-between;font-size:13px;">'
                    f'<span style="color:#c9d1d9;">{name} <span style="color:#484f58;font-size:11px;">{desc}</span></span>'
                    f'<span style="color:{bar_color};font-weight:bold;">{score}/{max_s}</span>'
                    f"</div>"
                    f'<div style="height:6px;background:#21262d;border-radius:3px;overflow:hidden;margin-top:3px;">'
                    f'<div style="height:100%;width:{pct}%;background:{bar_color};border-radius:3px;transition:width 0.3s;"></div>'
                    f"</div></div>",
                    unsafe_allow_html=True,
                )

            if tech_signals:
                with st.expander("技术面信号详情", expanded=False):
                    for sig in tech_signals[:10]:
                        st.markdown(
                            f'<div style="font-size:12px;color:#8b949e;padding:3px 0;">{sig}</div>',
                            unsafe_allow_html=True,
                        )
    else:
        st.info("数据不足，暂无法生成综合评估")

    # ===== 市场情绪仪表盘 =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        "市场情绪仪表盘"
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        "基于持仓ETF的涨跌分布，计算市场宽度、涨跌比、情绪偏向等指标，辅助判断整体市场情绪。"
        "</span></div>",
        unsafe_allow_html=True,
    )
    

    if not positions.empty:
        total_count = len(positions)
        up_count = len(positions[positions["pnl"] > 0])
        dn_count = len(positions[positions["pnl"] < 0])
        flat_count = total_count - up_count - dn_count
        up_ratio = up_count / total_count * 100 if total_count > 0 else 50

        if up_ratio >= 75:
            emotion = ("极度乐观", "#22c55e", "多数持仓上涨，市场情绪高涨，注意短期过热风险")
        elif up_ratio >= 60:
            emotion = ("偏乐观", "#4ade80", "多数持仓上涨，市场情绪偏暖")
        elif up_ratio >= 45:
            emotion = ("中性", "#f59e0b", "涨跌互现，市场情绪中性")
        elif up_ratio >= 30:
            emotion = ("偏悲观", "#fb923c", "多数持仓下跌，市场情绪偏冷，关注企稳信号")
        else:
            emotion = ("极度悲观", "#ef4444", "多数持仓下跌，市场情绪低迷，可能存在超跌反弹机会")

        ec1, ec2, ec3, ec4 = st.columns(4)
        with ec1:
            st.markdown(
                f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {emotion[1]};text-align:center;">'
                f'<div style="font-size:10px;color:#8b949e;">市场情绪</div>'
                f'<div style="font-size:16px;font-weight:bold;color:{emotion[1]};">{emotion[0]}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with ec2:
            st.markdown(
                f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid #58a6ff;text-align:center;">'
                f'<div style="font-size:10px;color:#8b949e;">涨跌比</div>'
                f'<div style="font-size:16px;font-weight:bold;">'
                f'<span style="color:#22c55e;">{up_count}</span>'
                f' / <span style="color:#ef4444;">{dn_count}</span>'
                f' / <span style="color:#8b949e;">{flat_count}</span>'
                f"</div></div>",
                unsafe_allow_html=True,
            )
        with ec3:
            adv_color = "#22c55e" if up_ratio >= 50 else "#ef4444"
            st.markdown(
                f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {adv_color};text-align:center;">'
                f'<div style="font-size:10px;color:#8b949e;">上涨占比</div>'
                f'<div style="font-size:16px;font-weight:bold;color:{adv_color};">{up_ratio:.0f}%</div>'
                f"</div>",
                unsafe_allow_html=True,
            )
        with ec4:
            avg_pnl = positions["pnl_rate"].mean() if "pnl_rate" in positions.columns else 0
            avg_c = "#22c55e" if avg_pnl >= 0 else "#ef4444"
            st.markdown(
                f'<div style="padding:8px;border-radius:6px;background:#161b22;border-left:3px solid {avg_c};text-align:center;">'
                f'<div style="font-size:10px;color:#8b949e;">平均收益率</div>'
                f'<div style="font-size:16px;font-weight:bold;color:{avg_c};">{avg_pnl:+.2f}%</div>'
                f"</div>",
                unsafe_allow_html=True,
            )

        st.markdown(
            f'<div style="padding:8px 12px;border-radius:6px;background:#161b22;font-size:12px;color:#8b949e;'
            f'border:1px solid #21262d;margin:6px 0;">{emotion[2]}</div>',
            unsafe_allow_html=True,
        )

        st.markdown(
            '<div style="font-size:14px;color:#c9d1d9;font-weight:bold;margin:10px 0 6px 0;">行业涨跌热力图</div>',
            unsafe_allow_html=True,
        )
        sector_pnl = {}
        for _, pos in positions.iterrows():
            code = str(pos["code"])
            cat_info = ETF_CATEGORIES.get(code)
            if cat_info:
                sector = cat_info["sector"]
                if sector not in sector_pnl:
                    sector_pnl[sector] = {"total_mv": 0, "total_pnl_rate": 0, "count": 0}
                sector_pnl[sector]["total_mv"] += pos.get("market_value", 0)
                sector_pnl[sector]["total_pnl_rate"] += pos.get("pnl_rate", 0) * pos.get("market_value", 0)
                sector_pnl[sector]["count"] += 1
        if sector_pnl:
            sector_list = sorted(sector_pnl.items(), key=lambda x: x[1]["total_pnl_rate"], reverse=True)
            # 预计算所有行业加权平均收益率，用于动态缩放
            avg_values = [
                sd["total_pnl_rate"] / sd["total_mv"] if sd["total_mv"] > 0 else 0 for _, sd in sector_list
            ]
            max_val = max(abs(v) for v in avg_values) if avg_values else 1
            html_bars = '<div style="display:flex;flex-direction:column;gap:6px;">'
            for (sector_name, sdata), avg_s in zip(sector_list, avg_values):
                # pnl_rate 以百分比形式存储（如44.24=44.24%），加权平均后直接为百分比
                bar_c = "#22c55e" if avg_s >= 0 else "#ef4444"
                # sqrt缩放：最大值映射到45%，最小3%，避免极端值压制小值
                bar_width = max(math.sqrt(abs(avg_s)) / math.sqrt(max_val) * 45, 3)
                color = SECTOR_COLORS.get(sector_name, "#8b949e")
                if avg_s >= 0:
                    bar_html = f'<div style="position:absolute;top:0;left:50%;width:{bar_width}%;height:100%;background:{bar_c};border-radius:0 3px 3px 0;"></div>'
                else:
                    bar_html = f'<div style="position:absolute;top:0;right:50%;width:{bar_width}%;height:100%;background:{bar_c};border-radius:3px 0 0 3px;"></div>'
                html_bars += (
                    f'<div style="display:flex;align-items:center;gap:8px;">'
                    f'<div style="width:50px;font-size:12px;color:{color};font-weight:bold;flex-shrink:0;">{sector_name}</div>'
                    f'<div style="flex:1;position:relative;height:20px;background:#161b22;border-radius:3px;overflow:hidden;">'
                    f'<div style="position:absolute;top:0;left:50%;width:1px;height:100%;background:#484f58;"></div>'
                    f"{bar_html}</div>"
                    f'<div style="width:55px;text-align:right;font-size:12px;color:{bar_c};font-weight:bold;flex-shrink:0;">{avg_s:+.2f}%</div>'
                    f'<div style="width:40px;text-align:right;font-size:10px;color:#484f58;flex-shrink:0;">{sdata["count"]}只</div></div>'
                )
            html_bars += "</div>"
            st.markdown(html_bars, unsafe_allow_html=True)

        pc1, pc2 = st.columns([1, 2])
        with pc1:
            fig_pie = go.Figure(
                go.Pie(
                    labels=["上涨", "下跌", "持平"],
                    values=[up_count, dn_count, flat_count],
                    marker_colors=["#22c55e", "#ef4444", "#8b949e"],
                    hole=0.6,
                    textinfo="label+percent",
                    textfont=dict(size=11, color="#c9d1d9"),
                    hovertemplate="%{label}: %{value}只 (%{percent})<extra></extra>",
                )
            )
            fig_pie.update_layout(
                height=220,
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(font=dict(size=10, color="#8b949e"), orientation="h", yanchor="bottom", y=-0.1),
            )
            st.plotly_chart(fig_pie, width="stretch")

        with pc2:
            if "pnl_rate" in positions.columns and not positions.empty:
                pnl_rates = positions["pnl_rate"].dropna().values
                fig_pnl_dist = go.Figure()
                fig_pnl_dist.add_trace(
                    go.Histogram(
                        x=pnl_rates, nbinsx=max(5, min(20, total_count)), opacity=0.85, marker_color="#58a6ff"
                    )
                )
                fig_pnl_dist.update_layout(
                    height=220,
                    plot_bgcolor="#0d1117",
                    paper_bgcolor="#0d1117",
                    font=dict(color="#c9d1d9", size=11),
                    margin=dict(l=40, r=20, t=10, b=30),
                    xaxis=dict(title="收益率 (%)", showgrid=True, gridcolor="#21262d"),
                    yaxis=dict(title="数量", showgrid=True, gridcolor="#21262d"),
                    bargap=0.15,
                )
                st.plotly_chart(fig_pnl_dist, width="stretch")

    # ===== 新闻情感分析 =====
    st.markdown("---")
    st.markdown(
        '<div class="tip-title" style="font-size:16px;border-bottom:none;padding:5px 0;">'
        '新闻情感分析'
        '<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span>'
        '<span class="tip-text" style="left: 4px; top: calc(100% + 10px);">'
        '基于SnowNLP+金融词典混合评分，对近7天新闻进行情感分析。'
        '</span></div>',
        unsafe_allow_html=True,
    )

    try:
        from snownlp import SnowNLP
        import jieba

        conn_sent = get_db_connection()
        try:
            sent_df = pd.read_sql_query(
                "SELECT date, category, title, summary FROM daily_news WHERE date >= date('now', '-7 days') ORDER BY date DESC",
                conn_sent)
        finally:
            conn_sent.close()

        if not sent_df.empty:
            # 金融情感词典（SnowNLP对金融文本偏差大，需校正）
            FIN_POS = {"上涨": 2, "大涨": 2, "增长": 1.5, "突破": 1.5, "新高": 2, "反弹": 1.5,
                        "利好": 2, "强势": 1.5, "净流入": 2, "增持": 1.5, "景气": 1, "复苏": 1.5,
                        "超预期": 2, "回暖": 1.5, "看多": 2, "金叉": 1.5, "多头": 1.5,
                        "盈利": 1.5, "创新高": 2, "加速": 1, "涨停": 2, "降准": 1.5, "降息": 1.5,
                        "放量": 1, "收红": 1.5, "领涨": 1.5, "走强": 1.5, "企稳": 1, "触底": 1}
            FIN_NEG = {"下跌": 1, "暴跌": 2, "大跌": 2, "跌破": 2, "风险": 1, "减持": 1.5,
                        "净流出": 2, "利空": 2, "疲软": 1.5, "收紧": 1, "回调": 1, "崩盘": 2,
                        "空头": 1.5, "死叉": 1.5, "缩量": 1, "亏损": 1.5, "下行": 1, "承压": 1.5,
                        "警惕": 1, "放缓": 1, "跌停": 2, "加息": 1, "抛售": 2, "恐慌": 2,
                        "熊市": 2, "破位": 1.5, "阴跌": 1.5, "杀跌": 2, "跳水": 2, "收绿": 1}

            def _fin_sentiment(text):
                if not isinstance(text, str) or not text.strip():
                    return 0.5
                # 基础SnowNLP分
                base = SnowNLP(text).sentiments
                # jieba分词 + 金融词典校正
                words = list(jieba.cut(text))
                fin_score = sum(FIN_POS.get(w, 0) - FIN_NEG.get(w, 0) for w in words)
                # 混合: base权重0.3, 金融词典权重0.7, 归一化到0~1
                adjusted = base * 0.3 + (fin_score / 4.0) * 0.7 + 0.5
                return max(0.0, min(1.0, adjusted))

            # 对title+summary合并评分
            sent_df["text"] = (sent_df["title"].fillna("") + " " + sent_df["summary"].fillna("")).str.strip()
            sent_df["sentiment"] = sent_df["text"].apply(_fin_sentiment)

            # 各板块平均情绪
            cat_sent = sent_df.groupby("category").agg(
                cnt=("title", "count"),
                avg_s=("sentiment", "mean"),
                pos=("sentiment", lambda x: (x > 0.6).sum()),
                neg=("sentiment", lambda x: (x < 0.4).sum()),
            ).sort_values("avg_s", ascending=True).reset_index()

            # 情绪对比图（0=负面, 0.5=中性, 1=正面）
            colors_s = ["#22c55e" if v > 0.55 else "#f59e0b" if v >= 0.45 else "#ef4444"
                        for v in cat_sent["avg_s"]]
            fig_sent = go.Figure(go.Bar(
                x=cat_sent["avg_s"], y=cat_sent["category"], orientation="h",
                marker_color=colors_s,
                text=[f"{v:.2f}" for v in cat_sent["avg_s"]], textposition="auto",
            ))
            fig_sent.update_layout(height=max(200, len(cat_sent)*30),
                                   margin=dict(l=110, r=20, t=5, b=10),
                                   xaxis_title="情绪得分(0负面~0.5中性~1正面)", range=[0, 1],
                                   yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_sent, width='stretch')

            # 情绪概要卡片
            sc1, sc2, sc3, sc4 = st.columns(4)
            avg_all = sent_df["sentiment"].mean()
            tp = (sent_df["sentiment"] > 0.6).sum()
            tn = (sent_df["sentiment"] < 0.4).sum()
            sc1.metric("平均情绪", f"{avg_all:.2f}")
            sc2.metric("正面新闻", tp)
            sc3.metric("负面新闻", tn)
            sc4.metric("中性新闻", len(sent_df) - tp - tn)

            # 各板块情绪明细表
            with st.expander("各板块情绪明细", expanded=False):
                disp_s = cat_sent[["category", "cnt", "avg_s", "pos", "neg"]].copy()
                disp_s.columns = ["板块", "新闻数", "平均情绪", "正面数", "负面数"]
                st.dataframe(disp_s, use_container_width=True, hide_index=True, height=250)

            # 负面新闻列表
            neg_news = sent_df[sent_df["sentiment"] < 0.4].nsmallest(5, "sentiment")
            if not neg_news.empty:
                with st.expander("负面新闻 TOP5", expanded=False):
                    for _, nr in neg_news.iterrows():
                        st.markdown(
                            f'<div style="font-size:12px;color:#ef4444;padding:2px 0;">'
                            f'[{nr["category"]}] {nr["date"]} (情绪:{nr["sentiment"]:.2f}) - {nr["title"]}</div>',
                            unsafe_allow_html=True)
        else:
            st.info("近7天无新闻数据")
    except Exception:
        st.info("新闻情感分析暂不可用（需安装snownlp和jieba）")

