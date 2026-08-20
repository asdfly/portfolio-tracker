#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""风险展望块：为日报(enhanced_report / email_report)准备数据并生成主题化 HTML。

设计原则：
- 只读已落表的 etf_predictions(model='risk_lgb')，避免日报构建时重训模型（耗时）。
- 若预测日落后于最新特征日，自动刷新落表（run_risk_predict_latest）。
- 回测指标(AUC/R²)缓存到 data/risk_backtest.json（30天），避免每日重算 walk-forward。
- 任何异常都被吞掉并返回 ok=False，绝不让风险块拖垮整份日报。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

WINDOWS = (20, 60)
CACHE_MAX_AGE_DAYS = 30


def _root(db_path: str) -> Path:
    return Path(db_path).resolve().parents[2]


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


def _read_preds(conn) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            "SELECT date, code, forward_window, direction, score, probability, confidence "
            "FROM etf_predictions WHERE model='risk_lgb'", conn)
    except Exception as exc:
        logger.warning("读取 etf_predictions 失败: %s", exc)
        return pd.DataFrame()


def _ensure_preds(conn) -> pd.DataFrame:
    preds = _read_preds(conn)
    if preds.empty:
        try:
            from src.analysis.predictor import models
            models.run_risk_predict_latest(conn, model="lgb")
            preds = _read_preds(conn)
        except Exception as exc:
            logger.warning("风险预测落表失败: %s", exc)
            return preds
    # 与最新特征日对齐：若预测日早于最新特征日，刷新
    try:
        feat = pd.read_sql_query("SELECT MAX(date) m FROM etf_features", conn)["m"].iloc[0]
        pd_date = preds["date"].max()
        if feat and pd_date and str(feat)[:10] > str(pd_date)[:10]:
            from src.analysis.predictor import models
            models.run_risk_predict_latest(conn, model="lgb")
            preds = _read_preds(conn)
    except Exception as exc:
        logger.warning("风险预测刷新检查失败: %s", exc)
    return preds


def _backtest(conn, root: Path) -> Optional[Dict]:
    cache = root / "data" / "risk_backtest.json"
    if cache.exists():
        try:
            age = (datetime.now() - datetime.fromtimestamp(cache.stat().st_mtime)).days
            if age <= CACHE_MAX_AGE_DAYS:
                data = json.loads(cache.read_text(encoding="utf-8"))
                if all(str(w) in data for w in WINDOWS):
                    return data
        except Exception:
            pass
    try:
        from src.analysis.predictor import models
        bt = models.run_risk_prediction(conn)
        data = {}
        for w, by_m in bt.get("results", {}).items():
            data[str(w)] = {m: {k: by_m[m].get(k) for k in ("r2", "ic_pearson", "auc", "n_test")}
                            for m in by_m}
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return data
    except Exception as exc:
        logger.warning("风险回测计算失败: %s", exc)
        return None


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


def get_risk_outlook(conn, db_path: str) -> Dict:
    """返回风险展望结构化数据，供 HTML 生成使用。永不抛异常（失败时 ok=False）。"""
    out = {"ok": False, "note": "", "pred_date": "", "windows": {},
           "backtest": None, "high_count": 0, "mid_count": 0, "low_count": 0,
           "port_vol": None, "hi_w": None, "high_list": [], "hist_dd": {}}
    try:
        preds = _ensure_preds(conn)
        if preds.empty:
            out["note"] = "风险预测数据暂不可用（etf_predictions 无 risk_lgb 记录）。"
            return out
        preds = preds.copy()
        preds["vol_ann"] = pd.to_numeric(preds["probability"], errors="coerce") * 100.0
        preds["pct"] = pd.to_numeric(preds["confidence"], errors="coerce")
        preds["cls"] = preds["vol_ann"].apply(_cls3)  # 绝对阈值三档（与前端口径一致）
        nm = _name_map(conn)
        preds["name"] = preds["code"].map(lambda c: nm.get(c, c))
        windows: Dict[int, List[Dict]] = {}
        for w in WINDOWS:
            g = preds[preds["forward_window"] == w]
            if g.empty:
                continue
            recs = []
            for r in g.to_dict("records"):
                recs.append({
                    "name": r.get("name") or r["code"],
                    "code": r["code"],
                    "vol_ann": round(float(r["vol_ann"]), 1) if pd.notna(r["vol_ann"]) else None,
                    "pct": round(float(r["pct"]), 1) if pd.notna(r["pct"]) else None,
                    "cls": r["cls"],
                })
            windows[w] = sorted(recs, key=lambda x: (x["vol_ann"] is None, -(x["vol_ann"] or 0)))
        out["windows"] = windows
        out["pred_date"] = str(preds["date"].max())[:10]
        out["backtest"] = _backtest(conn, _root(db_path))
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


def _fmt(v):
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.3f}"
    except (TypeError, ValueError):
        return "N/A"


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
    pred_date = outlook.get("pred_date", "")
    bt = outlook.get("backtest") or {}
    bt20 = (bt.get("20") or {}).get("lgb", {})
    bt60 = (bt.get("60") or {}).get("lgb", {})

    intro = ('<p style="font-size:11px;color:' + sub + ';margin:4px 0 8px;">'
             '基于 LightGBM 波动率预测模型（walk-forward 样本外回测）对持仓 ETF 未来波动率预判。'
             '预测基准日: <b>' + pred_date + '</b>。</p>')

    bt_line = ""
    if bt20 and bt60:
        bt_line = ('<p style="font-size:11px;color:' + sub + ';margin:0 0 8px;">'
                   '回测(样本外): 1月 AUC ' + _fmt(bt20.get("auc")) + ' / R² ' + _fmt(bt20.get("r2"))
                   + '；1季 AUC ' + _fmt(bt60.get("auc")) + ' / R² ' + _fmt(bt60.get("r2"))
                   + '（模型: LightGBM，波动率越高越易识别）</p>')

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
                     '组合预期年化波动率(1月,市值加权): <b style="color:' + hi_c + ';">'
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
                 ' 预期年化波动率 >30%，建议关注仓位与回撤风险（仅参考，不自动调仓）。</p>')

    return (
        '<div class="sec"><div class="st">🔮 ETF 风险展望</div>'
        + intro + bt_line + port_line + alert + badges
        + '<table><thead><tr>'
        '<th>名称</th><th>代码</th><th>1月年化波动</th><th>1月分位</th>'
        '<th>1季年化波动</th><th>1季分位</th><th>历史回撤</th><th>波动档位</th>'
        '</tr></thead><tbody>' + rows_html + '</tbody></table>'
        '<p style="font-size:10px;color:' + sub + ';margin:6px 0 0;">'
        '年化波动率 = 日波动率预测 ×√252；分位为同截面 22 只相对排名(0–100)；'
        '档位按绝对阈值（低 <18% / 中 18-30% / 高 >30%）；历史回撤为近 60 日已实现最大回撤。'
        '仅供风险预警参考，非买卖建议。</p>'
        '</div>'
    )
