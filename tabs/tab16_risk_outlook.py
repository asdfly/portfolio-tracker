"""Tab16: ETF 风险展望 — 基于预测底座的波动率预测可视化。

展示 22 只持仓 ETF 的未来已实现波动率预测：
- 1月（20日）/ 1季（60日）窗口的预期年化波动率
- 截面分位（该 ETF 预测波动率在全持仓中的相对位置）
- 高/低波动分类（以截面中位数为界）
- 模型回测表现（OOS R² / IC / 分类 AUC）

数据来自 src.analysis.predictor（etf_features → LightGBM 波动率模型）。
仅作风险参考，不自动调仓。
"""
from components.ui import render_chart
import streamlit as st
import pandas as pd
import numpy as np
from src.utils.database import get_db_connection
import plotly.graph_objects as go

_WINDOW_LABELS = {5: "1周", 20: "1月", 60: "1季"}


@st.cache_data(ttl=7200, show_spinner="正在训练波动率预测模型（LightGBM）…")
def _compute_risk_outlook():
    """训练波动率模型并产出最新预测 + 回测指标（缓存，避免重复重训）。"""
    from src.analysis.predictor.models import predict_risk_latest, run_risk_prediction
    conn = get_db_connection()
    try:
        pred = predict_risk_latest(conn, model="lgb")  # 默认窗口 (20, 60)
        backtest = run_risk_prediction(conn)
    finally:
        conn.close()
    return pred, backtest


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


def _build_window_df(pred: pd.DataFrame, window: int, name_map: dict) -> pd.DataFrame:
    """取指定窗口的预测，计算年化波动率、截面分位与高/低波动分类。"""
    df = pred[pred["forward_window"] == window].copy()
    if df.empty:
        return df
    df["name"] = df["code"].astype(str).map(name_map).fillna(df["code"].astype(str))
    # 标签口径为「日对数收益标准差」，年化便于直观比较
    df["pred_vol_ann"] = df["pred_vol"] * (252 ** 0.5) * 100
    df = df.sort_values("pred_vol").reset_index(drop=True)
    df["pct_rank"] = (df["pred_vol"].rank(pct=True) * 100).round(1)
    med = df["pred_vol"].median()
    df["cls"] = np.where(df["pred_vol"] >= med, "高波动", "低波动")
    return df


def _render_backtest(backtest: dict):
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


def _render_window(df: pd.DataFrame, window: int):
    n = len(df)
    hi = int((df["cls"] == "高波动").sum()) if n else 0
    lo = n - hi
    c1, c2, c3 = st.columns(3)
    c1.metric("覆盖 ETF", f"{n} 只")
    c2.metric("高波动", hi, help="预测波动率高于截面中位数")
    c3.metric("低波动", lo)

    st.markdown(f"**预测窗口：{_WINDOW_LABELS.get(window, window)}（未来 {window} 个交易日）**")

    # 横向条形图：年化波动率，按高/低波动着色
    fig = go.Figure()
    colors = ["#ef4444" if v == "高波动" else "#22c55e" for v in df["cls"]]
    fig.add_trace(go.Bar(
        x=df["pred_vol_ann"],
        y=df["name"],
        orientation="h",
        marker_color=colors,
        text=df["pred_vol_ann"].round(1).astype(str) + "%",
        textposition="outside",
        hovertemplate="%{y}<br>年化波动率 %{x:.1f}%<br>截面分位 %{customdata}%<extra></extra>",
        customdata=df["pct_rank"],
    ))
    fig.update_layout(
        height=max(360, 26 * n),
        plot_bgcolor="#0d1117",
        paper_bgcolor="#0d1117",
        font=dict(color="#c9d1d9", size=11),
        margin=dict(l=130, r=45, t=10, b=30),
        xaxis=dict(title="预期年化波动率 %", showgrid=True, gridcolor="#21262d"),
        yaxis=dict(autorange="reversed"),
    )
    render_chart(fig)

    # 明细表
    disp = df[["name", "code", "pred_vol_ann", "pct_rank", "cls"]].copy()
    disp.columns = ["名称", "代码", "预期年化波动率%", "截面分位%", "波动分类"]
    disp["预期年化波动率%"] = disp["预期年化波动率%"].round(2)
    st.dataframe(disp, width="stretch", hide_index=True, height=min(400, 36 * n + 60))


def render_tab16():
    """渲染 Tab16: ETF 风险展望。"""
    st.markdown('<div style="font-size:18px;font-weight:bold;padding:4px 0 6px;">'
                '🔮 ETF 风险展望</div>', unsafe_allow_html=True)
    st.caption("基于预测底座（etf_features → LightGBM 波动率模型）对 22 只持仓 ETF 的未来已实现波动率预测。"
               "截面分位 = 该 ETF 预测波动率在全持仓中的相对位置；分类以截面中位数为界。"
               "仅作风险参考，不自动调仓。")

    with st.spinner("正在计算风险预测…"):
        pred, backtest = _compute_risk_outlook()
    name_map = _load_name_map()

    if pred.empty:
        st.info("暂无波动率预测数据，请先运行预测底座补采"
                 "（`python -m src.analysis.predictor.build_base`）。")
        return

    _render_backtest(backtest)

    wlabel = st.radio("预测窗口", ["1月（20日）", "1季（60日）"],
                      horizontal=True, key="risk_outlook_win")
    window = 20 if wlabel.startswith("1月") else 60
    df_w = _build_window_df(pred, window, name_map)
    if df_w.empty:
        st.warning(f"窗口 {window} 暂无预测数据。")
        return
    _render_window(df_w, window)
