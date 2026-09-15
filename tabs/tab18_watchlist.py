"""Tab18: 已清仓·观察中 —— 清仓标的的行情与估值跟踪。

设计意图
--------
清仓不等于不再关心。本 Tab 跟踪已清仓标的的**行情、估值、技术面**，
回答的核心问题是：**"这个清仓决策事后看对不对"**——
即「清仓日至今的涨跌」，若清仓后大跌说明卖对了，若大涨则说明卖早了。

与持仓 Tab 的边界（重要，勿混淆）
--------------------------------
- 本 Tab 标的**不进持仓统计、不进再平衡、不进预测底座**（由 config.DELISTED_CODES 保证）
- 本 Tab **只观察、不产生任何调仓建议**，不参与风险指标与组合权重
- 数据来源：行情 etf_price_history / 技术面 etf_technical（由 scripts/fetch_watchlist_quotes.py 补采）；
  估值 index_pe_history（跟踪指数口径，日常链路持续在采）

诚实声明：本 Tab 是事后复盘工具，不构成任何买入依据。
"""
import streamlit as st
import pandas as pd

from src.utils.database import get_db_connection

# 指数代码 -> 指数名称（仅用于展示，取不到时直接显示代码）
_INDEX_NAMES = {
    "931743": "中证消费电子",
}

# A 股惯例：涨红跌绿
_UP_COLOR = "#C62828"
_DOWN_COLOR = "#0B8043"
_FLAT_COLOR = "#9E9E9E"


def _color(v):
    if v is None or pd.isna(v):
        return _FLAT_COLOR
    return _UP_COLOR if v > 0 else (_DOWN_COLOR if v < 0 else _FLAT_COLOR)


def _fmt_pct(v, digits=2):
    if v is None or pd.isna(v):
        return "—"
    return f"{v:+.{digits}f}%"


@st.cache_data(ttl=3600, show_spinner="正在加载观察名单…")
def _load_watch_data(code):
    """返回 (price_df, tech_row, clear_date) —— 全部只读。"""
    conn = get_db_connection()
    try:
        price_df = pd.read_sql_query(
            "SELECT date, close FROM etf_price_history "
            "WHERE code=? ORDER BY date",
            conn, params=(code,),
        )
        tech_df = pd.read_sql_query(
            "SELECT * FROM etf_technical WHERE code=? ORDER BY date DESC LIMIT 1",
            conn, params=(code,),
        )
    finally:
        conn.close()
    tech_row = tech_df.iloc[0].to_dict() if not tech_df.empty else None
    return price_df, tech_row


@st.cache_data(ttl=3600, show_spinner="正在加载估值数据…")
@st.cache_data(ttl=3600)
def _load_watch_items():
    """观察名单的元信息，全部从配置/数据推导，不在本文件写死标的。

    新增观察标的只需往 config.WATCHLIST_CODES 加代码，本 Tab 自动纳入。
    """
    from config.settings import WATCHLIST_CODES, ETF_CATEGORIES
    from src.analysis.etf_position import ETF_TO_INDEX

    if not WATCHLIST_CODES:
        return []

    conn = get_db_connection()
    try:
        items = []
        for code in sorted(WATCHLIST_CODES):
            info = ETF_CATEGORIES.get(code) or {}
            # 清仓日优先用配置，配置没有则从末次持仓快照推导
            clear_date = info.get("delisted_date")
            if not clear_date:
                row = conn.execute(
                    "SELECT MAX(date) FROM portfolio_snapshots WHERE code=?",
                    (code,),
                ).fetchone()
                clear_date = row[0] if row and row[0] else None
            items.append({
                "code": code,
                "name": info.get("name") or code,
                "index_code": ETF_TO_INDEX.get(code),
                "clear_date": clear_date,
            })
        return items
    finally:
        conn.close()


def _load_pe(index_code):
    """返回 (pe_df, latest_pe, percentile)。分位按全历史升序百分比。"""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, pe FROM index_pe_history "
            "WHERE index_code=? AND pe IS NOT NULL ORDER BY date",
            conn, params=(index_code,),
        )
    finally:
        conn.close()
    if df.empty:
        return df, None, None
    latest_pe = float(df["pe"].iloc[-1])
    pct = float((df["pe"] <= latest_pe).mean() * 100)
    return df, latest_pe, pct


def _ret_from(price_df, since_date):
    """计算 since_date（含）至今的涨跌幅；找不到基准日返回 None。"""
    if price_df.empty:
        return None, None, None
    df = price_df[price_df["date"] >= since_date]
    if len(df) < 2:
        return None, None, None
    base = float(df["close"].iloc[0])
    last = float(df["close"].iloc[-1])
    return (last / base - 1) * 100, base, last


def _ret_n(price_df, n):
    """近 n 个交易日涨跌幅。"""
    if len(price_df) <= n:
        return None
    base = float(price_df["close"].iloc[-(n + 1)])
    last = float(price_df["close"].iloc[-1])
    return (last / base - 1) * 100


def render_tab18():
    st.subheader("👀 已清仓 · 观察中")
    st.caption(
        "清仓标的的行情与估值跟踪，**不进持仓统计、不进再平衡、不产生调仓建议**。"
        "核心用途：事后复盘「清仓决策对不对」。"
    )

    watch_items = _load_watch_items()
    if not watch_items:
        st.info("观察名单为空。往 `config.WATCHLIST_CODES` 加入代码即可纳入跟踪。")
        st.divider()
        return

    for item in watch_items:
        code, name = item["code"], item["name"]
        idx_code, clear_date = item["index_code"], item["clear_date"]
        idx_name = _INDEX_NAMES.get(idx_code, idx_code or "—")

        price_df, tech_row = _load_watch_data(code)
        _, pe_now, pe_pct = _load_pe(idx_code) if idx_code else (None, None, None)

        with st.container(border=True):
            st.markdown(f"#### {name}（{code}）")
            st.caption(
                f"跟踪指数：{idx_name}（{idx_code}） · "
                f"清仓日：{clear_date or '未知'}"
            )

            if price_df.empty:
                st.warning(
                    f"尚未采集到 {code} 的行情数据。"
                    f"请先运行 `scripts/fetch_watchlist_quotes.py` 补采。"
                )
            else:
                last = float(price_df["close"].iloc[-1])
                d1 = _ret_n(price_df, 1)
                d5 = _ret_n(price_df, 5)
                d20 = _ret_n(price_df, 20)
                # clear_date 为空时无法定位基准日，直接跳过而不是拿 None 去比日期
                since_ret, base, _ = (
                    _ret_from(price_df, clear_date) if clear_date
                    else (None, None, None)
                )

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("最新价", f"{last:.3f}")
                c2.metric("日涨跌", _fmt_pct(d1))
                c3.metric("近 5 日", _fmt_pct(d5))
                c4.metric("近 20 日", _fmt_pct(d20))

                st.markdown("**清仓以来**")
                if since_ret is None:
                    st.caption(
                        "无法计算：清仓日未知，或清仓日之后暂无足够行情数据。"
                    )
                else:
                    verdict = (
                        "卖对了（清仓后下跌）" if since_ret < 0
                        else "卖早了（清仓后上涨）" if since_ret > 0
                        else "持平"
                    )
                    st.markdown(
                        f"<span style='color:{_color(since_ret)};font-size:26px;"
                        f"font-weight:700'>{_fmt_pct(since_ret)}</span>"
                        f"　<span style='color:#666'>{verdict}</span>",
                        unsafe_allow_html=True,
                    )
                    st.caption(
                        f"基准：{clear_date} 收盘 {base:.3f} → "
                        f"最新 {last:.3f}（口径：未考虑交易成本与分红）"
                    )

            # 估值
            st.markdown("**估值（跟踪指数口径）**")
            if pe_now is None:
                st.caption(f"暂无 {idx_code} 的 PE 数据。")
            else:
                ec1, ec2 = st.columns(2)
                ec1.metric("PE-TTM", f"{pe_now:.2f}")
                ec2.metric("历史分位", f"{pe_pct:.1f}%")
                if pe_pct >= 80:
                    st.caption("⚠️ 处于历史高位区间")
                elif pe_pct <= 20:
                    st.caption("✅ 处于历史低位区间")

            # 技术面
            if tech_row:
                st.markdown("**技术面（最新）**")
                tc1, tc2, tc3, tc4 = st.columns(4)
                tc1.metric("MA", tech_row.get("ma_signal") or "—")
                tc2.metric("MACD", tech_row.get("macd_signal") or "—")
                rsi = tech_row.get("rsi_value")
                tc3.metric("RSI", f"{rsi:.1f}" if rsi is not None else "—")
                tc4.metric("趋势", tech_row.get("trend") or "—")

    st.divider()
    st.caption(
        "数据来源：etf_price_history / etf_technical（观察名单补采）、index_pe_history（跟踪指数）。"
        "本页仅作事后复盘，不构成买入依据。"
    )
