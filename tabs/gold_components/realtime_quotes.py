"""
SGE多品种行情面板：品种表展示、分组筛选、点击品种查看K线走势
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from tabs.gold_components.gold_utils import (
    fetch_sge_hist, DARK_BG, DARK_FONT_COLOR, GRID_COLOR,
)

# 品种分组定义
SYMBOL_GROUPS = {
    "标准现货": ["Au99.99", "Au99.95", "Au100g", "Au99.5"],
    "延期合约": ["Au(T+D)", "mAu(T+D)", "Au(T+N1)", "Au(T+N2)"],
    "国际板": ["iAu99.99", "iAu100g", "iAu99.5"],
    "白银/铂金": ["Ag(T+D)", "Ag99.99", "Pt99.95"],
    "其他": ["PGC30g", "NYAuTN06", "NYAuTN12"],
}

@st.cache_data(ttl=600)
def _get_symbol_table():
    """获取SGE品种表"""
    try:
        import akshare as ak
        df = ak.spot_symbol_table_sge()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            for c in df.columns:
                if "品种" in c:
                    df = df.rename(columns={c: "symbol"})
            return df["symbol"].tolist() if "symbol" in df.columns else df.iloc[:, -1].tolist()
    except Exception:
        pass
    return [s for group in SYMBOL_GROUPS.values() for s in group]

@st.cache_data(ttl=300)
def _get_quotations():
    """获取SGE实时行情，失败时回退到历史数据最新交易日"""
    # 尝试 akshare 实时接口
    try:
        import akshare as ak
        df = ak.spot_quotations_sge()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            return df
    except Exception:
        pass

    # 回退方案：只获取5个核心品种，避免17次串行网络请求
    # fetch_sge_hist 已有 @st.cache_data 缓存，同参数第二次调用命中缓存
    try:
        core_symbols = ["Au99.99", "Au99.95", "Au(T+D)", "mAu(T+D)", "iAu99.99"]
        rows = []
        for sym in core_symbols:
            hist = fetch_sge_hist(symbol=sym)
            if hist is not None and not hist.empty:
                latest = hist.iloc[-1]
                prev = hist.iloc[-2] if len(hist) > 1 else latest
                close_val = float(latest["close"])
                prev_close = float(prev["close"])
                chg = close_val - prev_close
                chg_pct = chg / prev_close * 100 if prev_close != 0 else 0
                rows.append({
                    "品种": sym,
                    "最新价": close_val,
                    "涨跌": chg,
                    "涨跌幅(%)": chg_pct,
                    "开盘": float(latest["open"]),
                    "最高": float(latest["high"]),
                    "最低": float(latest["low"]),
                    "日期": str(latest["date"])[:10],
                    "数据来源": "历史收盘",
                })
        if rows:
            import pandas as pd
            result = pd.DataFrame(rows)
            return result
    except Exception:
        pass
    return None

@st.cache_data(ttl=3600)
def _get_hist_for_symbol(symbol, n_days=90):
    """获取指定品种历史数据"""
    return fetch_sge_hist(symbol=symbol)


def _render_symbol_table():
    """渲染品种表展示（分组）"""
    symbols = _get_symbol_table()

    st.markdown("#### SGE交易品种一览")

    # 使用卡片形式展示各分组
    for group_name, group_symbols in SYMBOL_GROUPS.items():
        available = [s for s in group_symbols if s in symbols]
        if not available:
            continue

        cols = st.columns(min(len(available), 4))
        for i, sym in enumerate(available):
            with cols[i % len(cols)]:
                # 品种标签颜色
                if sym.startswith("Au"):
                    tag_color = "#FFD700"
                elif sym.startswith("Ag"):
                    tag_color = "#C0C0C0"
                elif sym.startswith("Pt"):
                    tag_color = "#E5E4E2"
                elif sym.startswith("iA"):
                    tag_color = "#66BB6A"
                else:
                    tag_color = "#42A5F5"

                st.markdown(
                    f'<div style="background:#252540;padding:8px 12px;border-radius:6px;'
                    f'border-left:3px solid {tag_color};margin-bottom:4px;">'
                    f'<span style="color:{tag_color};font-weight:bold;font-size:13px;">{sym}</span></div>',
                    unsafe_allow_html=True,
                )


def _render_kline_panel(symbol, n_days=90):
    """渲染单个品种的K线走势"""
    df = _get_hist_for_symbol(symbol, n_days)
    if df is None or df.empty:
        st.warning(f"{symbol} 暂无历史数据")
        return

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"], name="K线",
        increasing_line_color="#ef5350", decreasing_line_color="#26a69a",
    ))
    # 添加MA5和MA20
    df["MA5"] = df["close"].rolling(5).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    if df["MA5"].notna().sum() > 0:
        fig.add_trace(go.Scatter(x=df["date"], y=df["MA5"], mode="lines", name="MA5", line=dict(color="#FFD700", width=1)))
    if df["MA20"].notna().sum() > 0:
        fig.add_trace(go.Scatter(x=df["date"], y=df["MA20"], mode="lines", name="MA20", line=dict(color="#FF69B4", width=1)))

    fig.update_layout(
        title=dict(text=f"{symbol} K线走势", font=dict(size=13)),
        xaxis_rangeslider_visible=False, height=420,
        plot_bgcolor=DARK_BG, paper_bgcolor=DARK_BG,
        font=dict(color=DARK_FONT_COLOR),
        xaxis=dict(gridcolor=GRID_COLOR), yaxis=dict(gridcolor=GRID_COLOR),
        margin=dict(l=50, r=30, t=35, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, width='stretch')

    # 最新数据
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else None
    close_val = float(latest["close"])
    if prev is not None:
        prev_close = float(prev["close"])
        chg = close_val - prev_close
        chg_pct = chg / prev_close * 100
        sign = "+" if chg >= 0 else ""
    else:
        chg, chg_pct, sign = 0, 0, ""
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("最新价", f"\u00a5{close_val:.2f}", f"{sign}{chg:.2f} ({sign}{chg_pct:.2f}%)")
    mc2.metric("最高", f"\u00a5{float(latest['high']):.2f}")
    mc3.metric("最低", f"\u00a5{float(latest['low']):.2f}")
    mc4.metric("日期", str(latest["date"]))


def render_realtime_quotes():
    """渲染SGE多品种实时行情面板"""

    st.markdown(
        '<div class="tip-title" style="font-size:14px;border-bottom:none;padding:5px 0;">实时行情<span class="tip-arrow" style="left: 4px; top: calc(100% + 5px);"></span><span class="tip-text" style="left: 4px; top: calc(100% + 10px);">展示上海黄金交易所各品种的最新报价、涨跌幅、成交量等实时行情数据。</span></div>',
        unsafe_allow_html=True,
    )
    # 上方：品种表 + 实时行情
    top_left, top_right = st.columns([1, 2])

    with top_left:
        _render_symbol_table()

    with top_right:
        st.markdown("#### 实时行情")
        q_df = _get_quotations()
        if q_df is not None and not q_df.empty:
            # 美化显示
            st.dataframe(q_df, width='stretch', height=400, hide_index=True)
        else:
            st.info("SGE实时行情接口暂不可用，请稍后重试。\n\n上海金交所交易时间：\n- 日盘：周一至周五 09:50-15:30\n- 夜盘：19:50-02:30")

    st.markdown("---")

    # 下方：品种选择 + K线走势
    st.markdown("#### 品种K线走势")
    ctrl_c1, ctrl_c2 = st.columns([2, 2])
    with ctrl_c1:
        # 按分组组织selectbox选项
        options = []
        for group_name, group_symbols in SYMBOL_GROUPS.items():
            options.append(f"--- {group_name} ---")
            options.extend(group_symbols)
        selected = st.selectbox("选择品种", options, index=1, key="quote_symbol")
        # 去掉分组分隔符
        if selected.startswith("---"):
            selected = None
    with ctrl_c2:
        period = st.selectbox("周期", ["近30天", "近90天", "近180天", "近1年"], key="quote_period")

    if selected:
        period_map = {"近30天": 30, "近90天": 90, "近180天": 180, "近1年": 365}
        n_days = period_map.get(period, 90)
        _render_kline_panel(selected, n_days)
    else:
        st.info("请从左侧选择一个品种查看K线走势")
