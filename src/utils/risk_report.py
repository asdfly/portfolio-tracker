#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""风险展望块：为日报(enhanced_report / email_report)准备数据并生成主题化 HTML。

设计原则：
- 纯历史统计：只读 etf_features 最新特征日的 vol_20d / vol_60d 已实现波动率并年化。
- 不再依赖 etf_predictions(model='risk_lgb')。该模型已下线：walk-forward 样本外验证中
  其截面排序能力（Spearman IC 0.613）未跑赢零成本的 vol_20d 基线（IC 0.743），
  ΔIC 的 HAC t = −4.51（BH-FDR q < 0.0001），未通过上线门禁。
  注意下线依据是 IC（排序能力）而非 R² —— 模型 OOS R²(−0.244) 实际优于基线(−2.696)，
  但两者皆为负，且 R² 转正也不构成上线理由，门槛是必须显著超越免费基线。
- 任何异常都被吞掉并返回 ok=False，绝不让风险块拖垮整份日报。
"""
from __future__ import annotations

import logging
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

WINDOWS = (20, 60)
_ANN = 252 ** 0.5 * 100.0  # 日波动率 → 年化百分比


def _name_map(conn) -> Dict[str, str]:
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(date) FROM portfolio_snapshots")
        latest = cur.fetchone()[0]
        if not latest:
            return {}
        rows = cur.execute(
            "SELECT DISTINCT code, name FROM portfolio_snapshots WHERE date=?", (latest,)
        ).fetchall()
        return {c: (n or c) for c, n in rows}
    except Exception:
        return {}


def _read_hist_vol(conn) -> pd.DataFrame:
    """读最新特征日的已实现波动率（vol_20d / vol_60d），无需任何模型。"""
    try:
        latest = conn.execute("SELECT MAX(date) FROM etf_features").fetchone()[0]
        if not latest:
            return pd.DataFrame()
        return pd.read_sql_query(
            "SELECT date, code, vol_20d, vol_60d FROM etf_features WHERE date=?",
            conn, params=[latest])
    except Exception as exc:
        logger.warning("读取 etf_features 失败: %s", exc)
        return pd.DataFrame()


def _cls3(vol_ann):
    """绝对阈值三档分类：低 <18% / 中 18-30% / 高 >30%（与前端 Tab16 口径一致）。"""
    if vol_ann is None or pd.isna(vol_ann):
        return "—"
    if vol_ann < 18.0:
        return "低波动"
    if vol_ann <= 30.0:
        return "中波动"
    return "高波动"


def _read_weights(conn) -> pd.DataFrame:
    """读最新快照 market_value（后续与预测交集归一化为权重）。"""
    try:
        latest = conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
        if not latest:
            return pd.DataFrame()
        return pd.read_sql_query(
            "SELECT code, market_value FROM portfolio_snapshots WHERE date=?", conn, params=[latest])
    except Exception:
        return pd.DataFrame()


def _read_hist_drawdown(conn, window: int = 60) -> Dict[str, float]:
    """从 etf_price_history 算每只 ETF 近 window 日历史最大回撤（负值小数）。"""
    try:
        df = pd.read_sql_query(
            "SELECT date, code, close FROM etf_price_history ORDER BY code, date", conn)
    except Exception:
        return {}
    if df.empty:
        return {}
    out: Dict[str, float] = {}
    for code, g in df.groupby("code"):
        g = g.sort_values("date").tail(window)
        if len(g) < 5:
            continue
        close = g["close"].astype(float)
        out[str(code)] = float((close / close.cummax() - 1.0).min())
    return out


def get_risk_outlook(conn, db_path: str = "") -> Dict:
    """返回风险展望结构化数据，供 HTML 生成使用。永不抛异常（失败时 ok=False）。

    db_path 仅为兼容既有调用方（enhanced_report / email_report）保留，本函数已不读缓存文件。
    """
    out = {"ok": False, "note": "", "stat_date": "", "windows": {},
           "high_count": 0, "mid_count": 0, "low_count": 0,
           "port_vol": None, "hi_w": None, "high_list": [], "hist_dd": {}}
    try:
        feat = _read_hist_vol(conn)
        if feat.empty:
            out["note"] = "风险统计暂不可用（etf_features 无最新特征日数据）。"
            return out
        nm = _name_map(conn)
        windows: Dict[int, List[Dict]] = {}
        for w in WINDOWS:
            col = "vol_20d" if w == 20 else "vol_60d"
            g = feat[["code", col]].dropna(subset=[col]).copy()
            if g.empty:
                continue
            g["vol_ann"] = pd.to_numeric(g[col], errors="coerce") * _ANN
            g["pct"] = g["vol_ann"].rank(pct=True) * 100.0
            recs = []
            for r in g.to_dict("records"):
                code = str(r["code"])
                recs.append({
                    "name": nm.get(code, code),
                    "code": code,
                    "vol_ann": round(float(r["vol_ann"]), 1) if pd.notna(r["vol_ann"]) else None,
                    "pct": round(float(r["pct"]), 1) if pd.notna(r["pct"]) else None,
                    "cls": _cls3(r["vol_ann"]),  # 绝对阈值三档（与前端口径一致）
                })
            windows[w] = sorted(recs, key=lambda x: (x["vol_ann"] is None, -(x["vol_ann"] or 0)))
        out["windows"] = windows
        out["stat_date"] = str(feat["date"].max())[:10]
        out["ok"] = bool(windows)
        if 20 in windows:
            out["high_count"] = sum(1 for x in windows[20] if x["cls"] == "高波动")
            out["mid_count"] = sum(1 for x in windows[20] if x["cls"] == "中波动")
            out["low_count"] = sum(1 for x in windows[20] if x["cls"] == "低波动")
            out["high_list"] = [x["name"] for x in windows[20] if x["cls"] == "高波动"]
        # 组合聚合（市值加权）+ 历史回撤参照
        try:
            wd = _read_weights(conn)
            if 20 in windows and wd is not None and not wd.empty:
                w20 = {x["code"]: x for x in windows[20]}
                wd = wd[wd["code"].isin(list(w20.keys()))].copy()
                if not wd.empty and wd["market_value"].sum() > 0:
                    wd["w"] = wd["market_value"] / wd["market_value"].sum()
                    port_vol = 0.0
                    hi_w = 0.0
                    for _, r in wd.iterrows():
                        rec = w20.get(r["code"])
                        if rec and rec.get("vol_ann") is not None:
                            port_vol += r["w"] * rec["vol_ann"]
                            if rec["cls"] == "高波动":
                                hi_w += r["w"]
                    out["port_vol"] = round(port_vol, 1)
                    out["hi_w"] = round(hi_w * 100, 1)
            out["hist_dd"] = _read_hist_drawdown(conn, 60)
        except Exception as exc:
            logger.warning("风险展望组合聚合失败: %s", exc)
    except Exception as exc:
        logger.warning("get_risk_outlook 失败: %s", exc)
        out["note"] = f"风险展望生成失败: {exc}"
    return out


def build_risk_outlook_html(outlook: Dict, theme: str = "dark") -> str:
    """生成风险展望区块 HTML。theme: 'dark'(enhanced_report) / 'light'(email_report)。"""
    if not outlook.get("ok"):
        note = outlook.get("note") or "风险展望暂不可用。"
        return ('<div class="sec"><div class="st">🔮 ETF 风险展望</div>'
                '<div style="padding:10px 12px;font-size:12px;color:#95a5a6;">'
                'ℹ️ ' + note + '</div></div>')

    dark = theme == "dark"
    txt = "#e0e6ed" if dark else "#2c3e50"
    sub = "#8899aa" if dark else "#7f8c8d"
    row_bg = ("#162447", "#1a2d50") if dark else ("#f8f9fa", "#ffffff")
    hi_c = "#e74c3c"
    md_c = "#f39c12"
    lo_c = "#27ae60"
    stat_date = outlook.get("stat_date", "")

    intro = ('<p style="font-size:11px;color:' + sub + ';margin:4px 0 8px;">'
             '持仓 ETF 的<b>历史已实现波动率</b>统计（取自 etf_features 的 vol_20d / vol_60d 并年化），'
             '<b>不含模型预测</b>。原波动率预测模型已因样本外排序能力未跑赢零成本的 vol_20d 基线而下线。'
             '统计基准日: <b>' + stat_date + '</b>。</p>')

    w20 = {x["code"]: x for x in outlook["windows"].get(20, [])}
    w60 = {x["code"]: x for x in outlook["windows"].get(60, [])}
    codes = list(w20.keys()) + [c for c in w60 if c not in w20]
    hist_dd = outlook.get("hist_dd", {}) or {}

    rows_html = ""
    for i, code in enumerate(codes):
        a = w20.get(code, {})
        b = w60.get(code, {})
        name = a.get("name") or b.get("name") or code
        cls = a.get("cls") or "—"
        cls_c = hi_c if cls == "高波动" else (md_c if cls == "中波动" else lo_c)
        cls_badge = '<span style="font-size:11px;font-weight:600;color:' + cls_c + ';">' + cls + '</span>'
        vol20 = (f'{a["vol_ann"]:.1f}' if a.get("vol_ann") is not None else '—')
        pct20 = (f'{a["pct"]:.0f}' if a.get("pct") is not None else '—')
        vol60 = (f'{b["vol_ann"]:.1f}' if b.get("vol_ann") is not None else '—')
        pct60 = (f'{b["pct"]:.0f}' if b.get("pct") is not None else '—')
        dd = hist_dd.get(code)
        dd_str = (f'{dd * 100:.1f}' if dd is not None else '—')
        dd_c = hi_c if (dd is not None and dd < -0.10) else sub
        bg = row_bg[i % 2]
        rows_html += (
            '<tr style="background:' + bg + ';">'
            '<td style="padding:6px 8px;font-size:11px;font-weight:500;color:' + txt + ';">' + name + '</td>'
            '<td style="padding:6px 8px;font-size:11px;color:' + sub + ';">' + code + '</td>'
            '<td style="padding:6px 8px;font-size:11px;text-align:right;color:' + txt + ';">' + vol20 + '%</td>'
            '<td style="padding:6px 8px;font-size:11px;text-align:right;color:' + sub + ';">' + pct20 + '</td>'
            '<td style="padding:6px 8px;font-size:11px;text-align:right;color:' + txt + ';">' + vol60 + '%</td>'
            '<td style="padding:6px 8px;font-size:11px;text-align:right;color:' + sub + ';">' + pct60 + '</td>'
            '<td style="padding:6px 8px;font-size:11px;text-align:right;color:' + dd_c + ';">' + dd_str + '%</td>'
            '<td style="padding:6px 8px;font-size:11px;text-align:center;">' + cls_badge + '</td>'
            '</tr>'
        )

    port_vol = outlook.get("port_vol")
    hi_w = outlook.get("hi_w")
    port_line = ""
    if port_vol is not None:
        port_line = ('<p style="font-size:12px;color:' + txt + ';margin:8px 0 4px;">'
                     '组合年化波动率(1月,市值加权,已实现): <b style="color:' + hi_c + ';">'
                     + f'{port_vol:.1f}' + '%</b>'
                     + ('　高波动ETF权重占比: <b>' + f'{hi_w:.1f}' + '%</b>' if hi_w is not None else '')
                     + '</p>')

    badges = ('<div style="margin:8px 0;display:flex;gap:10px;">'
              '<div style="flex:1;padding:8px 10px;border-radius:6px;background:' + ('#2d1a1a' if dark else '#fef5f5') + ';">'
              '<span style="font-size:11px;color:' + hi_c + ';">高波动(>30%)</span><br>'
              '<b style="font-size:16px;color:' + hi_c + ';">' + str(outlook.get("high_count", 0)) + '</b> 只</div>'
              '<div style="flex:1;padding:8px 10px;border-radius:6px;background:' + ('#3a2e16' if dark else '#fdf6e3') + ';">'
              '<span style="font-size:11px;color:' + md_c + ';">中波动(18-30%)</span><br>'
              '<b style="font-size:16px;color:' + md_c + ';">' + str(outlook.get("mid_count", 0)) + '</b> 只</div>'
              '<div style="flex:1;padding:8px 10px;border-radius:6px;background:' + ('#162d1f' if dark else '#eaf7ee') + ';">'
              '<span style="font-size:11px;color:' + lo_c + ';">低波动(<18%)</span><br>'
              '<b style="font-size:16px;color:' + lo_c + ';">' + str(outlook.get("low_count", 0)) + '</b> 只</div>'
              '</div>')

    high_list = outlook.get("high_list", []) or []
    alert = ""
    if high_list:
        alert = ('<p style="font-size:11px;color:' + hi_c + ';margin:0 0 8px;">'
                 '高波动预警：' + '、'.join(str(x) for x in high_list) +
                 ' 近端已实现年化波动率 >30%，建议关注仓位与回撤风险（仅参考，不自动调仓）。</p>')

    return (
        '<div class="sec"><div class="st">🔮 ETF 风险展望</div>'
        + intro + port_line + alert + badges
        + '<table><thead><tr>'
        '<th>名称</th><th>代码</th><th>1月年化波动</th><th>1月分位</th>'
        '<th>1季年化波动</th><th>1季分位</th><th>历史回撤</th><th>波动档位</th>'
        '</tr></thead><tbody>' + rows_html + '</tbody></table>'
        '<p style="font-size:10px;color:' + sub + ';margin:6px 0 0;">'
        '年化波动率 = 日已实现波动率 ×√252（1月取 vol_20d、1季取 vol_60d）；'
        '分位为同截面 22 只相对排名(0–100)；'
        '档位按绝对阈值（低 <18% / 中 18-30% / 高 >30%）；历史回撤为近 60 日已实现最大回撤。'
        '仅供风险预警参考，非买卖建议。</p>'
        '</div>'
    )
