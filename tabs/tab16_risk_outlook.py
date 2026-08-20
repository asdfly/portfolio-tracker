"""Tab16: ETF 风险展望 — 基于预测底座的波动率 + 回撤预测可视化。

展示 22 只持仓 ETF 的未来风险：
- 1月（20日）/ 1季（60日）窗口的预期年化波动率 + 预期最大回撤
- 预测 vs 历史已实现波动率对比（升波/降波/平稳）
- 三档分类（绝对阈值：低 <18% / 中 18-30% / 高 >30%）+ 绝对阈值预警
- 组合层面：加权组合波动率、风险贡献、高波动权重占比
- 单 ETF 下钻：历史波动率轨迹 + 历史分位
- 模型回测表现（OOS R² / IC / 分类 AUC）

数据来自 src.analysis.predictor（etf_features → LightGBM 波动率/回撤模型）。
仅作风险参考，不自动调仓。
"""
from components.ui import render_chart
import streamlit as st
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection
import plotly.graph_objects as go

_WINDOW_LABELS = {5: "1周", 20: "1月", 60: "1季"}
# 绝对阈值（预期年化波动率 %）：三档分类
_VOL_LO = 18.0
_VOL_HI = 30.0
# 变动方向阈值（预测 - 历史，年化波动率 pp）
_DELTA_PP = 1.0


@st.cache_data(ttl=7200, show_spinner="正在训练波动率预测模型（LightGBM）…")
def _compute_risk_outlook():
    """训练波动率模型并产出最新预测 + 回测指标（缓存，避免重复重训）。"""
    from src.analysis.predictor.models import predict_risk_latest, run_risk_prediction
    conn = get_db_connection()
    try:
        pred = predict_risk_latest(conn, model="lgb")
        backtest = run_risk_prediction(conn)
    finally:
        conn.close()
    return pred, backtest


@st.cache_data(ttl=7200)
def _load_hist_drawdown(window=60):
    """从 etf_price_history 算每只 ETF 近 window 日历史最大回撤（参照，非预测）。

    回撤预测模型样本外 R² 为负、AUC 仅 0.63-0.68（未达标），故不接模型，
    降级为历史已实现最大回撤参照。
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT date, code, close FROM etf_price_history ORDER BY code, date", conn)
    finally:
        conn.close()
    if df.empty:
        return pd.DataFrame()
    rows = []
    for code, g in df.groupby("code"):
        g = g.sort_values("date")
        tail = g.tail(window)
        if len(tail) < 5:
            continue
        close = tail["close"].astype(float)
        dd = close / close.cummax() - 1.0  # 负值，历史最大回撤 = min
        rows.append((str(code), float(dd.min())))
    return pd.DataFrame(rows, columns=["code", "hist_max_dd"])


@st.cache_data(ttl=7200)
def _load_name_map():
    """从最新持仓快照取 code → name 映射（22 只 ETF）。"""
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT MAX(date) FROM portfolio_snapshots")
        latest = cur.fetchone()[0]
        df = pd.read_sql_query(
            "SELECT DISTINCT code, name FROM portfolio_snapshots WHERE date=?",
            conn, params=[latest],
        )
    finally:
        conn.close()
    return {str(c): n for c, n in zip(df["code"], df["name"])}


@st.cache_data(ttl=7200)
def _load_hist_features():
    """读 etf_features：最新日历史波动率 + 全历史 vol 序列（轨迹/历史分位用）。"""
    conn = get_db_connection()
    try:
        latest_date = conn.execute("SELECT MAX(date) FROM etf_features").fetchone()[0]
        latest = pd.read_sql_query(
            "SELECT date, code, vol_20d, vol_60d, vol_5d FROM etf_features WHERE date=?",
            conn, params=[latest_date])
        hist = pd.read_sql_query(
            "SELECT date, code, vol_20d, vol_60d FROM etf_features ORDER BY code, date", conn)
    finally:
        conn.close()
    return latest, hist


@st.cache_data(ttl=7200)
def _load_weights():
    """读最新快照 market_value（后续与预测覆盖的 ETF 交集归一化为权重）。"""
    conn = get_db_connection()
    try:
        latest = conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
        df = pd.read_sql_query(
            "SELECT code, market_value FROM portfolio_snapshots WHERE date=?", conn, params=[latest])
    finally:
        conn.close()
    return df


def _ann(v):
    """日波动率（对数收益标准差）→ 年化百分比。"""
    return pd.to_numeric(v, errors="coerce") * (252 ** 0.5) * 100


def _cls3(vol_ann):
    """绝对阈值三档：低 / 中 / 高。"""
    if pd.isna(vol_ann):
        return "—"
    if vol_ann < _VOL_LO:
        return "低波动"
    if vol_ann <= _VOL_HI:
        return "中波动"
    return "高波动"


def _build_window_df(pred, window, name_map, hist_latest):
    """取指定窗口预测，join 历史波动率，计算变动方向与三档分类。"""
    df = pred[pred["forward_window"] == window].copy()
    if df.empty:
        return df
    df["name"] = df["code"].astype(str).map(name_map).fillna(df["code"].astype(str))
    df["pred_vol_ann"] = _ann(df["pred_vol"])
    # join 历史已实现波动率（最新特征日）
    hist_col = "vol_20d" if window == 20 else "vol_60d"
    if hist_latest is not None and not hist_latest.empty:
        h = hist_latest[["code", hist_col]].copy()
        h["hist_vol_ann"] = _ann(h[hist_col])
        df = df.merge(h[["code", "hist_vol_ann"]], on="code", how="left")
    else:
        df["hist_vol_ann"] = np.nan
    # 变动方向：预测 vs 当前历史
    diff = df["pred_vol_ann"] - df["hist_vol_ann"]
    df["trend"] = np.select(
        [diff > _DELTA_PP, diff < -_DELTA_PP],
        ["升波", "降波"], default="平稳")
    df["trend"] = df["trend"].where(df["hist_vol_ann"].notna(), "—")
    # 三档分类（绝对阈值，用预测值）
    df["cls"] = df["pred_vol_ann"].apply(_cls3)
    df["pct_rank"] = (df["pred_vol"].rank(pct=True) * 100).round(1)
    df = df.sort_values("pred_vol").reset_index(drop=True)
    return df


def _render_backtest(backtest):
    st.markdown("#### 模型回测表现（walk-forward · LightGBM）")
    st.caption("样本外（OOS）验证，衡量模型对未来已实现波动率的预测能力。"
               "R²/IC 越高、分类 AUC 越接近 1 越好。")
    rows = []
    for w, by_m in backtest.get("results", {}).items():
        for m, r in by_m.items():
            rows.append({
                "窗口": _WINDOW_LABELS.get(w, w),
                "模型": m,
                "OOS R²": r.get("r2"),
                "IC": r.get("ic_pearson"),
                "分类AUC": r.get("auc"),
                "样本数": r.get("n_test"),
            })
    bt = pd.DataFrame(rows)
    st.dataframe(bt, width="stretch", hide_index=True)

    lgb = {w: backtest["results"][w]["lgb"]
           for w in (20, 60) if w in backtest.get("results", {})}
    if lgb:
        parts = [f"{_WINDOW_LABELS.get(w, w)} AUC={r.get('auc')} / R²={r.get('r2')}"
                 for w, r in lgb.items()]
        st.success("结论：" + "；".join(parts) + "。波动率预测显著有效，可作仓位/回撤预警参考。")


def _render_portfolio_agg(df_w, weights_df, window):
    """组合层面：加权组合波动率、风险贡献 Top、高波动权重占比。"""
    if df_w.empty or weights_df is None or weights_df.empty:
        return
    wd = weights_df.merge(df_w[["code", "pred_vol_ann", "cls"]].astype({"code": str}),
                          left_on="code", right_on="code", how="inner")
    if wd.empty or wd["market_value"].sum() <= 0:
        return
    wd["w"] = wd["market_value"] / wd["market_value"].sum()
    port_vol = float((wd["w"] * wd["pred_vol_ann"]).sum())
    # 简化风险贡献 = w_i × σ_i / Σ(w_j × σ_j)
    wd["contrib"] = wd["w"] * wd["pred_vol_ann"]
    wd["contrib_pct"] = wd["contrib"] / wd["contrib"].sum() * 100
    hi_w = float(wd.loc[wd["cls"] == "高波动", "w"].sum() * 100)

    st.markdown("#### 组合风险概览（ETF 子组合，按市值加权）")
    c1, c2, c3 = st.columns(3)
    c1.metric("组合预期年化波动率", f"{port_vol:.1f}%",
              help=f"窗口：{_WINDOW_LABELS.get(window, window)}，Σ 权重×个股预期波动率（简化，未计相关性）")
    c2.metric("高波动 ETF 权重占比", f"{hi_w:.1f}%",
              help="预期波动率 >30% 的持仓占 ETF 子组合市值比例，衡量尾部风险集中度")
    c3.metric("覆盖 ETF 市值占比", f"{wd['w'].sum() * 100:.0f}%",
              help="预测底座覆盖的 ETF 占最新快照市值比例")

    top = wd.sort_values("contrib_pct", ascending=False).head(5)
    top = top.assign(name=top["code"].map(_load_name_map()).fillna(top["code"]))
    st.caption("风险贡献 Top 5（占组合预期波动的比例）")
    st.dataframe(
        top[["name", "code", "pred_vol_ann", "w", "contrib_pct"]]
        .rename(columns={"name": "名称", "code": "代码", "pred_vol_ann": "预期波动率%",
                         "w": "权重%", "contrib_pct": "风险贡献%"}),
        width="stretch", hide_index=True)


def _render_drawdown(hist_dd, name_map, window, weights_df):
    """历史最大回撤参照块（回撤预测未达标，降级为历史参照）。"""
    st.markdown("#### 历史最大回撤参照（近 60 日已实现）")
    st.caption("回撤预测模型样本外验证未达标（R² 为负、AUC 0.63-0.68），故不展示预测，"
               "改为历史已实现最大回撤参照，辅助判断持仓的回撤风险。")
    if hist_dd.empty:
        st.caption("暂无价格历史数据，无法计算回撤参照。")
        return
    df = hist_dd.copy()
    df["name"] = df["code"].map(name_map).fillna(df["code"])
    df["dd_pct"] = df["hist_max_dd"] * 100  # 负值
    df["dd_rank"] = (df["hist_max_dd"].rank(pct=True) * 100).round(1)
    df = df.sort_values("hist_max_dd").reset_index(drop=True)

    colors = ["#ef4444" if v <= df["hist_max_dd"].median() else "#f59e0b" for v in df["hist_max_dd"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["dd_pct"], y=df["name"], orientation="h", marker_color=colors,
        text=df["dd_pct"].round(1).astype(str) + "%", textposition="outside",
        hovertemplate="%{y}<br>近60日最大回撤 %{x:.1f}%<br>截面分位 %{customdata}%<extra></extra>",
        customdata=df["dd_rank"],
    ))
    fig.update_layout(
        height=max(360, 26 * len(df)), plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
        font=dict(color="#c9d1d9", size=11),
        margin=dict(l=130, r=45, t=10, b=30),
        xaxis=dict(title="近 60 日最大回撤 %（越负越深）", showgrid=True, gridcolor="#21262d"),
        yaxis=dict(autorange="reversed"),
    )
    render_chart(fig)

    if weights_df is not None and not weights_df.empty:
        wd = weights_df.merge(df[["code", "hist_max_dd"]].astype({"code": str}),
                              on="code", how="inner")
        if not wd.empty and wd["market_value"].sum() > 0:
            wd["w"] = wd["market_value"] / wd["market_value"].sum()
            port_dd = float((wd["w"] * wd["hist_max_dd"]).sum()) * 100
            st.caption(f"组合历史最大回撤（近 60 日，市值加权）：**{port_dd:.1f}%**")


def _render_hist_track(code, name, hist, name_map):
    """单 ETF 下钻：历史滚动波动率轨迹 + 当前点 + 历史分位。"""
    if hist is None or hist.empty:
        return
    g = hist[hist["code"] == code].copy()
    if g.empty:
        return
    g = g.sort_values("date")
    g["vol_ann"] = _ann(g["vol_20d"])
    g = g.dropna(subset=["vol_ann"])
    if g.empty:
        return
    tail = g.tail(252)  # 近 1 年
    cur = float(g["vol_ann"].iloc[-1])
    hist_pct = float((g["vol_ann"] < cur).mean() * 100)

    st.markdown(f"##### {name}（{code}）历史波动率轨迹")
    st.caption(f"当前 20 日已实现波动率 {cur:.1f}%，处于历史 "
               f"{hist_pct:.0f}% 分位（历史越高越接近 100%）。")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=tail["date"], y=tail["vol_ann"], mode="lines",
        line=dict(color="#378ADD", width=1.5), name="20日已实现波动率"))
    fig.add_trace(go.Scatter(
        x=[tail["date"].iloc[-1]], y=[cur], mode="markers",
        marker=dict(color="#ef4444", size=8), name="当前值"))
    fig.update_layout(
        height=260, plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
        font=dict(color="#c9d1d9", size=11),
        margin=dict(l=40, r=20, t=10, b=30),
        xaxis=dict(title="日期", showgrid=False),
        yaxis=dict(title="年化波动率 %", showgrid=True, gridcolor="#21262d"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    render_chart(fig)


def _render_alert(df_w):
    """绝对阈值高波动预警清单 + 择时参考文案（不自动调仓）。"""
    hi = df_w[df_w["cls"] == "高波动"]
    if hi.empty:
        st.info("当前无预期波动率 >30% 的持仓，风险整体可控。")
        return
    names = "、".join(hi["name"].astype(str).tolist())
    st.warning(
        f"**高波动预警**：{len(hi)} 只持仓预期年化波动率 >30%（{names}）。"
        "建议关注仓位与回撤风险；如需控制风险，可考虑降低相关持仓权重或分散至低波动标的。"
        "（仅风险参考，系统不自动调仓。）")


def _render_window(df, window):
    n = len(df)
    hi = int((df["cls"] == "高波动").sum())
    md = int((df["cls"] == "中波动").sum())
    lo = n - hi - md
    c1, c2, c3 = st.columns(3)
    c1.metric("覆盖 ETF", f"{n} 只")
    c2.metric("高波动(>30%)", hi)
    c3.metric("中/低波动", f"{md} / {lo}")

    st.markdown(f"**预测窗口：{_WINDOW_LABELS.get(window, window)}（未来 {window} 个交易日）**")

    colors = {"低波动": "#22c55e", "中波动": "#f59e0b", "高波动": "#ef4444"}
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["pred_vol_ann"], y=df["name"], orientation="h",
        marker_color=[colors.get(v, "#888780") for v in df["cls"]],
        text=df["pred_vol_ann"].round(1).astype(str) + "%", textposition="outside",
        hovertemplate="%{y}<br>预期年化波动率 %{x:.1f}%<br>截面分位 %{customdata[0]}%<br>趋势 %{customdata[1]}<extra></extra>",
        customdata=df[["pct_rank", "trend"]].values,
    ))
    fig.update_layout(
        height=max(360, 26 * n), plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
        font=dict(color="#c9d1d9", size=11),
        margin=dict(l=130, r=45, t=10, b=30),
        xaxis=dict(title="预期年化波动率 %", showgrid=True, gridcolor="#21262d"),
        yaxis=dict(autorange="reversed"),
    )
    render_chart(fig)

    # 筛选 + 导出
    csel, cexp = st.columns([1, 1])
    with csel:
        cats = ["全部", "高波动", "中波动", "低波动"]
        sel = st.selectbox("按波动档位筛选", cats, key=f"risk_cls_{window}")
    disp = df[["name", "code", "pred_vol_ann", "hist_vol_ann", "trend", "pct_rank", "cls"]].copy()
    disp.columns = ["名称", "代码", "预期年化波动率%", "历史年化波动率%", "变动方向", "截面分位%", "波动档位"]
    for c in ("预期年化波动率%", "历史年化波动率%"):
        disp[c] = disp[c].round(2)
    if sel != "全部":
        disp = disp[disp["波动档位"] == sel]
    with cexp:
        st.download_button(
            "导出 CSV", disp.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"risk_outlook_{window}d.csv", mime="text/csv",
            key=f"risk_csv_{window}")
    st.dataframe(disp, width="stretch", hide_index=True, height=min(400, 36 * len(disp) + 60))


def render_tab16():
    """渲染 Tab16: ETF 风险展望。"""
    st.markdown('<div style="font-size:18px;font-weight:bold;padding:4px 0 6px;">'
                '🔮 ETF 风险展望</div>', unsafe_allow_html=True)
    st.caption("基于预测底座（etf_features → LightGBM）对 22 只持仓 ETF 的未来已实现波动率与最大回撤预测。"
               "波动档位按绝对阈值（低 <18% / 中 18-30% / 高 >30%）；变动方向 = 预测 vs 当前历史波动率。"
               "仅作风险参考，不自动调仓。")

    with st.spinner("正在计算风险预测…"):
        pred, backtest = _compute_risk_outlook()
    name_map = _load_name_map()
    hist_latest, hist = _load_hist_features()
    weights_df = _load_weights()

    if pred.empty:
        st.info("暂无波动率预测数据，请先运行预测底座补采"
                 "（`python -m src.analysis.predictor.build_base`）。")
        return

    pred_date = str(pred["date"].max())[:10]
    st.caption(f"预测基准日：**{pred_date}**。若当日部分 ETF 特征未更新（T+1 数据滞后），"
               "预测基于最近可用特征日，次日重跑自动补齐。")

    _render_backtest(backtest)

    wlabel = st.radio("预测窗口", ["1月（20日）", "1季（60日）"],
                      horizontal=True, key="risk_outlook_win")
    window = 20 if wlabel.startswith("1月") else 60
    df_w = _build_window_df(pred, window, name_map, hist_latest)
    if df_w.empty:
        st.warning(f"窗口 {window} 暂无预测数据。")
        return

    _render_portfolio_agg(df_w, weights_df, window)
    _render_alert(df_w)
    _render_window(df_w, window)

    # 历史最大回撤参照（回撤预测未达标，降级）
    st.markdown("---")
    hist_dd = _load_hist_drawdown(window)
    _render_drawdown(hist_dd, name_map, window, weights_df)

    # 单 ETF 下钻
    st.markdown("---")
    st.markdown("#### 单 ETF 历史波动率下钻")
    codes = df_w["code"].astype(str).tolist()
    labels = [f"{name_map.get(c, c)}（{c}）" for c in codes]
    idx = 0
    if "risk_track_sel" in st.session_state:
        sel_label = st.session_state["risk_track_sel"]
        if sel_label in labels:
            idx = labels.index(sel_label)
    sel_label = st.selectbox("选择 ETF", labels, index=idx, key="risk_track_sel")
    sel_code = codes[labels.index(sel_label)]
    _render_hist_track(sel_code, name_map.get(sel_code, sel_code), hist, name_map)
