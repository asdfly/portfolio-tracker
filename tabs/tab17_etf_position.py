"""Tab17: ETF 高低位定位 — 三因子集成的价格位置量化评估 (Phase A)。

核心区别于 Tab16(风险展望): 本 Tab 回答的是「当前价格站在历史什么位置」(状态定位),
不是「未来涨跌」(方向预测 — 项目已用 walk-forward 证伪, Tier1 IC<0.02 全线 VETO)。
定位是描述性、可回溯、置信度可量化的问题, 因此能给出高置信结论。

展示内容:
- 22 只 ETF 的位置分数 P ∈ [-100,+100] + 置信度 C + 五档标签
- 因子拆解: F1 价格分布(多周期百分位/稳健z/52周高低距) / F3 资金流(反向) / F2 估值(闸门)
- 组合层面市值加权位置 (组合整体站位)
- 单 ETF 下钻: 价格轨迹 + 历史分位带 (P10/P25/P50/P75/P90)
- 极端位置清单 (|P|>=60 且 C>=0.5) + CSV 导出

诚实声明: 估值因子 F2 当前因 index_pe_history 历史不足(<250 交易日)自动禁用;
本 Tab 仅作位置参考, 不自动调仓。
"""
from components.ui import render_chart
import streamlit as st
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection
import plotly.graph_objects as go

# 五档标签配色: 遵循 A 股惯例(涨红跌绿) —— 价格处高位=已涨多=红(警惕), 低位=绿(机会)
_LABEL_COLOR = {
    "极低(黄金区)": "#0B8043",
    "偏低": "#5FA777",
    "中性": "#9E9E9E",
    "偏高": "#E06666",
    "极高(警惕区)": "#C62828",
}
# 极端位置阈值: |P| 达到该值且置信度足够才进入行动清单
_EXTREME_P = 60.0
_EXTREME_C = 0.50


@st.cache_data(ttl=3600, show_spinner="正在计算 ETF 高低位定位…")
def _compute_positions():
    """批量评估 + 拉取最新持仓权重/名称。缓存 1 小时(纯历史数据, 日内不变)。"""
    from src.analysis.etf_position import evaluate_all, portfolio_position
    conn = get_db_connection()
    try:
        results = evaluate_all(conn=conn)
        latest = conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
        holdings = pd.read_sql_query(
            "SELECT code, name, market_value FROM portfolio_snapshots WHERE date=?",
            conn, params=[latest])
    finally:
        conn.close()

    total_mv = float(holdings["market_value"].sum()) if not holdings.empty else 0.0
    name_map = dict(zip(holdings["code"].astype(str), holdings["name"]))
    weight_map = {}
    if total_mv > 0:
        weight_map = {str(r.code): float(r.market_value) / total_mv
                      for r in holdings.itertuples()}
    # 组合聚合仅统计权益 ETF：债券高低位由利率/久期驱动，与权益口径不可混算
    pf = portfolio_position([r for r in results if r.get("type") != "bond"], weight_map)
    return results, name_map, weight_map, pf, latest


@st.cache_data(ttl=3600, show_spinner=False)
def _load_basis_series(code: str):
    """加载单标的的价格定位基准序列(与打分同源: 宽基走指数长历史)。"""
    from src.analysis.etf_position import price_basis_series
    conn = get_db_connection()
    try:
        series, basis = price_basis_series(conn, code)
    finally:
        conn.close()
    return series, basis


def _to_frame(results, name_map, weight_map) -> pd.DataFrame:
    rows = []
    for r in results:
        code = r["code"]
        price = r.get("factors", {}).get("price", {})
        flow = r.get("factors", {}).get("flow", {})
        val = r.get("factors", {}).get("valuation", {})
        rows.append({
            "代码": code,
            "名称": name_map.get(code, "-"),
            "权重%": round(weight_map.get(code, 0.0) * 100, 2),
            "位置P": r["P"],
            "标签": r["label"],
            "置信度C": r["C"],
            "类型": "债券" if r.get("type") == "bond" else "权益",
            "价格分位": price.get("pct_score"),
            "稳健z": price.get("z_score"),
            "52周高低距": price.get("hl_score"),
            "资金流z": flow.get("z_inflow"),
            "估值分位": val.get("pe_percentile") if val.get("available") else None,
            "基准": price.get("basis", "-"),
            "样本数": price.get("n_points"),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("位置P", ascending=False).reset_index(drop=True)


def _render_header(pf, latest, results):
    st.markdown("### ETF 高低位定位")
    st.caption(
        "回答「当前价格在历史中处于高位还是低位」——**状态定位**，非涨跌预测。"
        "P ∈ [-100,+100]：-100 极低/便宜，0 中位，+100 极高/昂贵；C 为置信度"
        "（数据充分度 × 因子一致性）。红=高位警惕、绿=低位机会（A 股涨红跌绿惯例）。"
    )

    n_val = sum(1 for r in results
                if r.get("factors", {}).get("valuation", {}).get("available"))
    n_equity = sum(1 for r in results if r.get("type") != "bond")
    c1, c2, c3, c4 = st.columns(4)
    if pf:
        c1.metric("权益加权位置", f"{pf['P']:+.1f}", pf["label"])
        c2.metric("权益置信度", f"{pf['C']:.2f}", f"覆盖 {pf['coverage']*100:.0f}% 市值")
    else:
        c1.metric("权益加权位置", "N/A")
        c2.metric("权益置信度", "N/A")
    extreme = [r for r in results
               if abs(r["P"]) >= _EXTREME_P and r["C"] >= _EXTREME_C]
    c3.metric("极端位置标的", f"{len(extreme)} 只", f"|P|≥{_EXTREME_P:.0f} 且 C≥{_EXTREME_C:.2f}")
    c4.metric("估值因子可用", f"{n_val}/{n_equity}",
              "PE 历史不足已禁用" if n_val < n_equity else "全部启用")
    st.caption(f"持仓基准日：{latest}；价格数据来自 etf_price_history / index_quotes。")


def _render_ranking(df: pd.DataFrame):
    st.markdown("#### 位置排序")
    plot_df = df.sort_values("位置P")
    colors = [_LABEL_COLOR.get(lb, "#9E9E9E") for lb in plot_df["标签"]]
    labels = [f"{c} {n}" for c, n in zip(plot_df["代码"], plot_df["名称"])]
    fig = go.Figure(go.Bar(
        x=plot_df["位置P"], y=labels, orientation="h",
        marker_color=colors,
        text=[f"P={p:+.1f} C={c:.2f}" for p, c in zip(plot_df["位置P"], plot_df["置信度C"])],
        textposition="outside",
        hovertemplate="%{y}<br>位置 P=%{x:.1f}<extra></extra>",
    ))
    for x, dash in [(-60, "dot"), (-25, "dash"), (25, "dash"), (60, "dot")]:
        fig.add_vline(x=x, line_dash=dash, line_color="#BDBDBD", line_width=1)
    fig.update_layout(
        height=max(420, 26 * len(plot_df)), xaxis_title="位置分数 P（左=低位，右=高位）",
        xaxis_range=[-115, 115], margin=dict(l=10, r=10, t=20, b=40),
        showlegend=False, plot_bgcolor="white",
    )
    render_chart(fig)


def _render_table(df: pd.DataFrame):
    st.markdown("#### 明细与因子拆解")
    only_holdings = st.checkbox("仅显示当前持仓（权重>0）", value=False, key="tab17_holdonly")
    min_c = st.slider("最低置信度 C", 0.0, 1.0, 0.0, 0.05, key="tab17_minc")
    view = df.copy()
    if only_holdings:
        view = view[view["权重%"] > 0]
    view = view[view["置信度C"] >= min_c]
    if view.empty:
        st.info("当前筛选条件下无标的。")
        return

    def _color(row):
        c = _LABEL_COLOR.get(row["标签"], "")
        return [f"color: {c}; font-weight: 600" if col in ("位置P", "标签") else ""
                for col in row.index]

    st.dataframe(
        view.style.apply(_color, axis=1).format({
            "权重%": "{:.2f}", "位置P": "{:+.1f}", "置信度C": "{:.2f}",
            "价格分位": "{:+.1f}", "稳健z": "{:+.1f}", "52周高低距": "{:+.1f}",
            "资金流z": "{:+.2f}", "估值分位": "{:.1f}",
        }, na_rep="—"),
        width="stretch", hide_index=True,
    )
    st.download_button(
        "导出 CSV", view.to_csv(index=False).encode("utf-8-sig"),
        file_name="etf_position.csv", mime="text/csv", key="tab17_dl",
    )
    st.caption(
        "价格分位=多周期百分位加权（1/3/5年+全历史）；稳健z=median/MAD 抗离群 z；"
        "资金流z=20日滚动净流入稳健 z（**反向解读**：大幅净流入=情绪高涨=偏高位）；"
        "估值分位=跟踪指数 PE 近5年分位。"
    )


def _render_extremes(df: pd.DataFrame):
    hot = df[(df["位置P"] >= _EXTREME_P) & (df["置信度C"] >= _EXTREME_C)]
    cold = df[(df["位置P"] <= -_EXTREME_P) & (df["置信度C"] >= _EXTREME_C)]
    if hot.empty and cold.empty:
        st.info(f"当前无 |P|≥{_EXTREME_P:.0f} 且 C≥{_EXTREME_C:.2f} 的极端位置标的——"
                "全部处于中性/偏离区间内。")
        return
    def _lines(sub: pd.DataFrame):
        for _, r in sub.iterrows():
            wt = f"，权重 {r['权重%']:.2f}%" if r["权重%"] > 0 else ""
            st.markdown(f"- **{r['代码']} {r['名称']}**：P={r['位置P']:+.1f}"
                        f"（{r['标签']}），C={r['置信度C']:.2f}{wt}")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**高位警惕区**")
        if hot.empty:
            st.caption("无")
        else:
            _lines(hot)
    with col2:
        st.markdown("**低位机会区**")
        if cold.empty:
            st.caption("无")
        else:
            _lines(cold)
    st.caption("仅为位置提示，不构成调仓指令（系统红线：不自动调仓）。"
               "高位=历史相对昂贵，可考虑放缓加仓/分批止盈；低位=历史相对便宜，"
               "需先确认基本面逻辑未破坏再考虑布局。")


def _render_drilldown(df: pd.DataFrame):
    st.markdown("#### 单标的下钻")
    opts = [f"{c} {n}" for c, n in zip(df["代码"], df["名称"])]
    sel = st.selectbox("选择标的", opts, key="tab17_sel")
    code = sel.split()[0]
    row = df[df["代码"] == code].iloc[0]

    series, basis = _load_basis_series(code)
    if series.empty or len(series) < 60:
        st.info("该标的价格样本不足，无法绘制分位带。")
        return

    arr = series.values.astype(float)
    q = {p: float(np.percentile(arr, p)) for p in (10, 25, 50, 75, 90)}
    show = series.iloc[-750:]  # 近 3 年轨迹, 分位带取全历史
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=show.index, y=show.values, mode="lines", name="价格",
        line=dict(color="#1565C0", width=1.6),
    ))
    band_style = {10: ("#0B8043", "dot"), 25: ("#5FA777", "dash"), 50: ("#616161", "solid"),
                  75: ("#E06666", "dash"), 90: ("#C62828", "dot")}
    for p, (color, dash) in band_style.items():
        fig.add_hline(y=q[p], line_color=color, line_dash=dash, line_width=1,
                      annotation_text=f"P{p}", annotation_position="right")
    fig.update_layout(
        height=380, margin=dict(l=10, r=50, t=20, b=30),
        yaxis_title="价格（定位基准）", plot_bgcolor="white", showlegend=False,
    )
    render_chart(fig)
    st.caption(f"基准：{basis}；分位带取全历史 {len(series)} 个交易日"
               f"（{series.index[0]:%Y-%m-%d} 起），曲线显示近 3 年。")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("位置 P", f"{row['位置P']:+.1f}", row["标签"])
    c2.metric("置信度 C", f"{row['置信度C']:.2f}")
    c3.metric("价格分位", "—" if pd.isna(row["价格分位"]) else f"{row['价格分位']:+.1f}")
    c4.metric("资金流 z", "—" if pd.isna(row["资金流z"]) else f"{row['资金流z']:+.2f}")

    if row["类型"] == "债券":
        st.warning("债券 ETF：价格高低位由利率/久期驱动，权益估值口径不适用，"
                   "此处仅作价格分布定位参考。")


def render_tab17():
    """渲染 Tab17: ETF 高低位定位。"""
    try:
        results, name_map, weight_map, pf, latest = _compute_positions()
    except Exception as e:  # noqa: BLE001
        st.error(f"高低位评估失败：{e}")
        return
    if not results:
        st.info("暂无 ETF 价格历史数据，请先运行 predictor 数据底座回填。")
        return

    df = _to_frame(results, name_map, weight_map)
    _render_header(pf, latest, results)
    st.divider()
    _render_ranking(df)
    st.divider()
    _render_extremes(df)
    st.divider()
    _render_table(df)
    st.divider()
    _render_drilldown(df)
    st.divider()
    with st.expander("方法论与数据诚实声明", expanded=False):
        st.markdown(
            "- **定位 ≠ 预测**：本模块只回答「现在处于历史什么位置」。项目已用 "
            "walk-forward（embargo 60 + HAC t）证明 ETF 短期**方向**不可预测"
            "（Tier1 六窗口 IC<0.02 全线 VETO），因此本模块刻意不给涨跌判断。\n"
            "- **估值因子已启用（19/20 权益 ETF）**：2026-09-04 已用 akshare csindex 长历史回填"
            "`index_pe_history`（2018→至今，约 2.8 万行，单指数 PE 历史 1500~2045 日），"
            "引擎 ≥250 日闸门通过即自动纳入（250 日给 0.6 置信，1250 日给满置信）。"
            "仅 159949/399673 创业板50 仍禁用——其跟踪指数为国证指数，中证/国证官方"
            "均无免费可采集的单指数 PE 接口（国证仅有行业板块级 PE）。\n"
            "- **宽基基准修正**：宽基 ETF 用 `index_quotes` 长历史（部分回溯至 2002 年）"
            "替代 ETF 自身成立后窗口，修正「成立日恰在低位」造成的百分位系统性偏高。\n"
            "- **资金流反向解读**：20 日滚动净流入的稳健 z 越高，说明情绪越热、"
            "越可能靠近阶段高位；置信上限封顶 0.65（该序列噪声大、覆盖率有限）。\n"
            "- **无未来函数**：所有分位/z 只用截止当日的历史数据。\n"
            "- **不自动调仓**：输出仅为位置参考，任何仓位变动由人工决策。"
        )
