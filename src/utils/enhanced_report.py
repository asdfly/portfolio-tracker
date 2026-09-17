#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""增强版HTML邮件报告生成器 - 内嵌图表图片"""
import sqlite3
import logging
import base64
import io
from datetime import datetime
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
# 配置中文字体，解决图表中文标签显示为方块的问题
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun']
matplotlib.rcParams['axes.unicode_minus'] = False
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from data_loader import get_db_connection

logger = logging.getLogger(__name__)

# 建议优先级归一化表。
# 背景：邮件正文里的建议是用正则从 Markdown 的 "### 1. [高] 标题" 抓出来的（见 _load_advice），
# priority 是中文单字 '高'/'中'/'低'；而下面的展示映射表 pm 用英文键 'high'/'medium'/'low'。
# 两者对不上会导致 pm.get() 全部 miss，所有建议（含高优先级）都被渲染成兜底的「⚪ 低」。
_PRIORITY_ALIASES = {
    'high': 'high', 'h': 'high', '高': 'high', '高优先级': 'high',
    'medium': 'medium', 'mid': 'medium', 'm': 'medium', '中': 'medium', '中优先级': 'medium',
    'low': 'low', 'l': 'low', '低': 'low', '低优先级': 'low',
}


def _norm_priority(p):
    """把中英文混用 / AdvicePriority 枚举的优先级统一成 'high'|'medium'|'low'。

    无法识别时归为 'low'，但调用方仍需保留兜底标签，避免把未知值粉饰成「低」。
    """
    if p is None:
        return 'low'
    v = getattr(p, 'value', p)  # 兼容直接传入 AdvicePriority 枚举对象的情况
    return _PRIORITY_ALIASES.get(str(v).strip().lower(), 'low')

# 降级报告的机器可读标记（渲染成 HTML 注释，供调用方/测试判定）。
#
# 背景（2026-09-15 事故）：本报告的两个核心区块来自两张不同的表 ——
# 页头/指标来自 portfolio_summary，持仓明细来自 portfolio_snapshots。
# 当日管线在阶段一崩溃，portfolio_summary 停在 09-14，而 portfolio_snapshots
# 已被写到 09-15，于是"重生"出来的报告页头口径是 09-14、持仓口径是 09-15，
# 两者相差 6,735 元却没有任何提示，最终被推送给了真实用户。
# 现在的硬约束：任何调用方都不可能在不带此标记的情况下拿到这种拼接报告 ——
# 宽松模式(strict=False)会强制在页面顶部插红色降级横幅，严格模式(strict=True)直接抛异常。
DEGRADED_MARKER = "DATA_INCONSISTENT_DEGRADED"

# "软滞后提示"标记：只有**允许合法滞后**的日期源与报告数据日期不一致时使用。
# 与 DEGRADED_MARKER 的区别是刻意的：软滞后必须**可见**（琥珀色提示条逐条列出，
# 含各自日期、滞后天数与原因），但**不触发硬拒绝** —— 例如非交易日没有指数行情、
# 或软阶段(prediction_base/nav_rebuild)被软截止跳过，都是设计内降级，
# 若一并硬判就会把正常日误拦成降级。
LAG_NOTICE_MARKER = "DATASOURCE_LAG_NOTICE"

# 单条告警在报告中展示的上限（与原实现一致）。
_ALERTS_LIMIT = 5

# ---------------------------------------------------------------------------
# 报告实际读取的**全部**日期源清单（2026-09-16 补齐：原来只挡了前两张表）
# ---------------------------------------------------------------------------
# 字段：(表名, 展示名, hard, 可合法滞后的原因)
#
# hard=True  —— 必须等于报告数据日期，不等即"硬不一致"(降级横幅 + 严格模式抛错)。
#               判据：与 portfolio_summary 由**同一硬阶段**(run_analysis.py 阶段一
#               basic，无 try/except 包裹，挂了即 rc=1)写入、且都用同一个 self.today
#               作键；历史 80 个交易日中"summary 有而它没有"的次数为 0。
# hard=False —— 可合法滞后：只进提示条，不做硬判定（原因见第 4 列，均有证据）。
#
# 证据（只读生产库，2026-09-16 取数）：
#   portfolio_snapshots  近 80 个 summary 交易日缺该日 0 次；反向(有 snapshot 无
#                        summary)仅 2026-09-15 一天 ⇒ 硬判在历史上只命中过那一次事故。
#   index_quotes         近 80 日缺 1 次(2026-05-31，周日 —— 非交易日没有行情，
#                        属合法滞后)；反向 2026-09-15。
#   etf_technical        同 index_quotes(缺 2026-05-31)；另外单标的 K 线取数失败
#                        只记 warning(portfolio.py:371-374)不中断，可合法缺。
#   etf_features         由软阶段 prediction_base 构建(run_analysis.py:1150-1154
#                        try 包裹，skipped=设计内降级)。
#   etf_price_history    由软阶段 nav_rebuild(run_analysis.py:1372-1376)与补采脚本
#                        写入，近 80 日缺 2 次(2026-05-31 / 2026-06-19)。
_DATE_SOURCES = (
    ("portfolio_summary",   "组合汇总",     True,
     ""),
    ("portfolio_snapshots", "持仓快照",     True,
     ""),
    ("index_quotes",        "基准指数行情", False,
     "非交易日无行情；且单个指数取数失败仅记 warning 不中断(portfolio.py:335-339)"),
    ("etf_technical",       "技术指标",     False,
     "非交易日无K线；且单标的取数失败仅记 warning 不中断(portfolio.py:362-374)"),
    ("etf_features",        "波动率特征",   False,
     "由软阶段 prediction_base 构建，该阶段被软截止跳过属设计内降级(run_analysis.py:1150-1154)"),
    ("etf_price_history",   "历史价格",     False,
     "由软阶段 nav_rebuild 与补采脚本写入，可滞后(run_analysis.py:1372-1376)"),
)

# 硬判定源的表名集合（供调用方/测试引用，避免魔数字符串散落）。
HARD_DATE_SOURCES = tuple(t for t, _l, hard, _r in _DATE_SOURCES if hard)


class ReportDataInconsistentError(RuntimeError):
    """必须与报告日期一致的日期源出现了不一致（硬不一致）。

    strict=True 时由 build_full_report 抛出，调用方据此拒绝产出一份
    跨日期拼接的报告（例如定时邮件应当据此跳过当天推送）。

    mismatches: [{"table","label","date","lag_days","reason"}, ...]，只含硬源。
    """

    def __init__(self, report_date, mismatches):
        self.report_date = report_date
        self.mismatches = list(mismatches or [])
        detail = "；".join(
            f"{m['label']}({m['table']})数据日期={m['date'] or '(空)'}"
            for m in self.mismatches
        ) or "(无)"
        super().__init__(
            f"报告数据日期不一致：报告数据日期={report_date or '(空)'}，"
            f"但与以下必须同日的日期源不符：{detail}。"
            "拒绝在未标记的情况下产出跨日期拼接报告。"
        )


# 主题颜色映射（深色 dashboard 版 / 浅色邮件版）。语义色（涨跌/优先级）不在此处，
# 两主题共用 #27ae60(绿) #e74c3c(红) #f39c12(琥珀) #1a73e8(蓝) #3498db(蓝) 等。
THEMES = {
    "dark": {
        "row1": "#162447", "row2": "#1a2d50", "text": "#e0e6ed", "sub": "#8899aa",
        "muted": "#95a5a6", "faint": "#607080", "gray": "#7f8c8d",
        "border": "#2a3f5f", "alert_bg": "#2d1a1a", "ok_bg": "#1a2d1a",
        "warn_bg": "#2d2618", "risk_green": "#162d1f", "risk_red": "#2d1a1a",
        "risk_amber": "#2d2618", "advice_bg": "#162447", "header_sub": "#b0c4de",
        "card_bg": "#111d35", "body_bg": "#0a1628",
    },
    "light": {
        "row1": "#f8f9fa", "row2": "#ffffff", "text": "#2c3e50", "sub": "#6b7a8d",
        "muted": "#7f8c8d", "faint": "#9aa5b1", "gray": "#7f8c8d",
        "border": "#e5e9f0", "alert_bg": "#fef5f5", "ok_bg": "#eaf7ee",
        "warn_bg": "#fdf6e3", "risk_green": "#eaf7ee", "risk_red": "#fef5f5",
        "risk_amber": "#fdf6e3", "advice_bg": "#f0f4fb", "header_sub": "#e8f0fe",
        "card_bg": "#ffffff", "body_bg": "#f5f7fa",
    },
}


def _pick(d, *keys, default=None):
    """按优先级尝试多个键名，返回第一个非 None 的值。

    报告层的数据来源键名并不统一：DB 落表用英文键（total_value / market_value /
    sharpe_ratio ...），而券商导出（见 src/utils/position_reader.py 的列名映射）与
    smart_report 落盘的建议优先级用中文键（最新市值 / 持仓盈亏 / 盈亏率% / 高 / 中 / 低）。
    统一走本函数可以避免散落的 `a.get(x) or a.get(y) or z`，也避免漏掉某一侧别名
    导致有值却取不到、渲染成 N/A 或占位符。
    """
    if not isinstance(d, dict):
        return default
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return default


def _fmt_dr_coverage(cov) -> str:
    """把 `daily_return_coverage` 渲染成一句**可核事实**文案（口径 B，2026-09-17）。

    只用可核字段（纳入只数 / 可比只数 / 前日可比市值占比）拼装，**不写任何推断句** ——
    涉及未知定义的因果（例如「因此与历史不可比」）一律不写：那是推断，而这里只允许放
    能逐项核对的代码事实。

    读不到时必须返回「覆盖度未记录」，**不许静默省略**：离线「只读库 + 独立重渲染」
    路径拿不到运行时的覆盖度，那正是最容易被悄悄省掉的地方。
    """
    if not isinstance(cov, dict):
        return "覆盖度未记录"
    parts = []
    inc, cmp_n = cov.get("included_n"), cov.get("comparable_n")
    if inc is not None and cmp_n is not None:
        parts.append(f"覆盖 {inc}/{cmp_n} 只")
    share = cov.get("value_share")
    if share is not None:
        try:
            parts.append(f"占前日可比市值 {float(share) * 100:.2f}%")
        except (TypeError, ValueError):
            pass
    if cov.get("caliber") == "total_value_fallback":
        parts.append("口径回退：全持仓 total_value/prev_value")
    return " · ".join(parts) if parts else "覆盖度未记录"

_CSS_DARK = (
    "body{margin:0;padding:0;background:#0a1628;font-family:-apple-system,BlinkMacSystemFont,"
    "'Segoe UI',Roboto,Arial,sans-serif;}"
    ".c{max-width:720px;margin:20px auto;background:#111d35;border-radius:12px;overflow:hidden;"
    "box-shadow:0 2px 12px rgba(0,0,0,0.08);}"
    ".hd{background:linear-gradient(135deg,#1a73e8,#0d47a1);color:#fff;padding:24px 20px;text-align:center;}"
    ".hd h1{margin:0;font-size:20px;letter-spacing:1px;}"
    ".hd p{margin:4px 0 0;font-size:12px;color:#b0c4de;}"
    ".ms{display:flex;flex-wrap:wrap;gap:10px;padding:16px 20px;}"
    ".m{flex:1 1 30%;min-width:130px;padding:12px 14px;background:#162447;border-radius:8px;text-align:center;}"
    ".m .l{font-size:10px;color:#8899aa;margin-bottom:3px;text-transform:uppercase;letter-spacing:0.5px;}"
    ".m .v{font-size:18px;font-weight:700;}"
    ".m .s{font-size:10px;color:#95a5a6;margin-top:2px;}"
    ".sec{padding:0 20px 14px;}"
    ".st{font-size:14px;font-weight:600;color:#e0e6ed;margin:14px 0 8px;padding-bottom:5px;border-bottom:2px solid #2a3f5f;}"
    "table{width:100%;border-collapse:collapse;}"
    "th{padding:7px 10px;font-size:10px;color:#8899aa;text-transform:uppercase;letter-spacing:0.5px;"
    "text-align:left;background:#1a2d50;border-bottom:2px solid #2a3f5f;}"
    "th:nth-child(n+3),td:nth-child(n+3){text-align:right;}"
    ".ft{padding:14px 20px;text-align:center;font-size:10px;color:#607080;border-top:1px solid #1e3050;background:#0e1a2e;}"
    ".rg{display:flex;gap:10px;}"
    ".rc{flex:1;padding:12px;border-radius:8px;text-align:center;}"
    ".rc .rl{font-size:10px;color:#8899aa;}"
    ".rc .rv{font-size:16px;font-weight:700;margin-top:3px;}"
)

_CSS_LIGHT = (
    "body{margin:0;padding:0;background:#f5f7fa;font-family:-apple-system,BlinkMacSystemFont,"
    "'Segoe UI',Roboto,Arial,sans-serif;}"
    ".c{max-width:720px;margin:20px auto;background:#ffffff;border-radius:12px;overflow:hidden;"
    "box-shadow:0 2px 12px rgba(0,0,0,0.06);border:1px solid #e5e9f0;}"
    ".hd{background:linear-gradient(135deg,#1a73e8,#0d47a1);color:#fff;padding:24px 20px;text-align:center;}"
    ".hd h1{margin:0;font-size:20px;letter-spacing:1px;}"
    ".hd p{margin:4px 0 0;font-size:12px;color:#e8f0fe;}"
    ".ms{display:flex;flex-wrap:wrap;gap:10px;padding:16px 20px;}"
    ".m{flex:1 1 30%;min-width:130px;padding:12px 14px;background:#f0f4fb;border-radius:8px;text-align:center;}"
    ".m .l{font-size:10px;color:#6b7a8d;margin-bottom:3px;text-transform:uppercase;letter-spacing:0.5px;}"
    ".m .v{font-size:18px;font-weight:700;color:#2c3e50;}"
    ".m .s{font-size:10px;color:#7f8c8d;margin-top:2px;}"
    ".sec{padding:0 20px 14px;}"
    ".st{font-size:14px;font-weight:600;color:#2c3e50;margin:14px 0 8px;padding-bottom:5px;border-bottom:2px solid #e5e9f0;}"
    "table{width:100%;border-collapse:collapse;}"
    "th{padding:7px 10px;font-size:10px;color:#6b7a8d;text-transform:uppercase;letter-spacing:0.5px;"
    "text-align:left;background:#eef1f6;border-bottom:2px solid #e5e9f0;}"
    "th:nth-child(n+3),td:nth-child(n+3){text-align:right;}"
    ".ft{padding:14px 20px;text-align:center;font-size:10px;color:#9aa5b1;border-top:1px solid #e5e9f0;background:#fafbfc;}"
    ".rg{display:flex;gap:10px;}"
    ".rc{flex:1;padding:12px;border-radius:8px;text-align:center;}"
    ".rc .rl{font-size:10px;color:#6b7a8d;}"
    ".rc .rv{font-size:16px;font-weight:700;margin-top:3px;color:#2c3e50;}"
)


class EnhancedReportBuilder:

    def __init__(self, db_path: str, theme: str = "dark"):
        self.db_path = db_path
        self.theme = theme

    def build_full_report(self, news_data=None, theme: Optional[str] = None,
                          strict: bool = False) -> str:
        """构建完整 HTML 报告。

        Args:
            news_data: 新闻/资讯数据；None 时该板块整体不渲染。
            theme: 'dark' | 'light'，覆盖实例主题。
            strict: True 时，若**必须同日**的日期源（见 HARD_DATE_SOURCES：
                portfolio_summary / portfolio_snapshots）与报告数据日期不一致，
                抛 ReportDataInconsistentError，而不是产出一份跨日期拼接的报告。
                默认 False（宽松）：仍产出报告，但**必定**在页面顶部插入
                红色降级横幅 + DEGRADED_MARKER 标记，不存在"静默拼接"这条路。

        一致性口径（2026-09-15 事故修复 + 2026-09-16 补齐日期源）：
          报告对外声明的"数据日期"一律取自 portfolio_summary 的最新日期；
          持仓明细、告警、智能建议、30日前价格、历史曲线全部按该日期取数。
          其余日期源分两类（清单与证据见模块级 _DATE_SOURCES）：
            · 硬源(必须同日)：不一致 ⇒ 降级横幅 + DEGRADED_MARKER，strict 时抛错；
            · 软源(可合法滞后)：不一致 ⇒ 琥珀色提示条 + LAG_NOTICE_MARKER，
              逐条列出各自日期/滞后天数/原因，但**不**触发硬拒绝（避免把
              非交易日、软阶段被跳过这类设计内降级误拦成降级）。
        """
        theme = theme or self.theme
        T = THEMES.get(theme, THEMES["dark"])
        css = _CSS_LIGHT if theme == "light" else _CSS_DARK
        self._T = T
        self._theme = theme

        # --- 日期源一致性判定（必须先于任何区块取数）---
        src_dates = self._load_source_dates()
        self._src_dates = src_dates

        summary = self._load_summary()
        # 报告数据日期以 summary 为准；summary 整表为空时退回快照日期，避免页头日期为空。
        report_date = src_dates.get("portfolio_summary") or src_dates.get("portfolio_snapshots")
        # summary 行内日期与聚合 MAX(date) 不一致(理论上不会发生)时以行为准。
        _row_date = _pick(summary, 'date', '日期') if summary else None
        if _row_date:
            report_date = _row_date
        self._data_date = report_date

        hard_mismatches, soft_mismatches = self._split_date_mismatches(report_date, src_dates)
        inconsistent = bool(hard_mismatches)
        if inconsistent and strict:
            raise ReportDataInconsistentError(report_date, hard_mismatches)

        positions = self._load_positions(report_date)
        alerts = self._load_alerts(report_date)
        advice = self._load_advice(report_date)
        history = self._load_history(60, report_date)
        index_today = self._load_index_today()
        technical = self._load_technical()
        price_30d = self._load_price_30d_ago(report_date)

        banner = self._build_date_notice(report_date, hard_mismatches, soft_mismatches)
        if not summary or not positions:
            return banner + "<p>暂无足够数据生成报告</p>"

        nav_b64 = self._build_nav_chart(history) if len(history) > 2 else ""
        dd_b64 = self._build_drawdown_chart(history) if len(history) > 5 else ""

        now = datetime.now()
        # 页头日期必须来自数据，不得用 datetime.now()（它永远是"今天"，会把
        # 09-14 的指标渲染成 09-16 的日报）。数据日期与生成时间分行展示。
        date_str, weekday = self._fmt_data_date(report_date)

        dr = _pick(summary, 'daily_return', '日收益率', default=0) or 0
        # 口径 B（2026-09-17 裁定）：`daily_return` 只覆盖「当日价新鲜」的标的，
        # 而 `total_value` 是全持仓 ⇒ 两者**口径不同，必须并列印出并显式声明不可相乘**
        # （读者拿 1,527,929 × daily_return 会算出错金额）。读不到覆盖度时印
        # 「覆盖度未记录」，不许静默省略。
        dr_cov = summary.get('daily_return_coverage') if isinstance(summary, dict) else None
        dr_cov_note = _fmt_dr_coverage(dr_cov)
        _cov_total_n = dr_cov.get('total_n') if isinstance(dr_cov, dict) else None
        _tv_scope = ("全部持仓 " + str(_cov_total_n) + " 只") if _cov_total_n else "全部持仓"
        dr_caliber_note = (
            '注：「当日盈亏 / 日收益率」按<strong>当日有行情价的标的</strong>计算（'
            + dr_cov_note + '）；「总市值」为' + _tv_scope
            + '。两者口径不同，<strong>不可相乘</strong>。'
            '「基准指数对比」中的「跑赢/跑输」同样基于前者。'
        )
        tp = _pick(summary, 'total_pnl', '总盈亏', default=0) or 0
        tc = _pick(summary, 'total_cost', '总成本', default=1) or 1
        total_ret = tp / tc * 100 if tc > 0 else 0
        dp = _pick(summary, 'daily_pnl', '当日盈亏', default=0) or 0

        def sign(v):
            return '+' if v >= 0 else ''
        def clr(v):
            return '#27ae60' if v >= 0 else '#e74c3c'

        sharpe = _pick(summary, 'sharpe_ratio', '夏普比率')
        max_dd = _pick(summary, 'max_drawdown', '最大回撤')
        vol = _pick(summary, 'volatility', '波动率')

        def sf(v, fmt='.2f'):
            if v is None:
                return 'N/A'
            try:
                fv = float(v)
                if fv != fv:
                    return 'N/A'
                return f'{fv:{fmt}}'
            except (ValueError, TypeError):
                return 'N/A'

        ss = sf(sharpe, '.3f')
        dds = sf(max_dd)
        vss = sf(vol)
        sc = '#27ae60' if sharpe and float(sharpe) > 0.5 else '#f39c12' if sharpe else '#95a5a6'
        ddc = '#e74c3c' if max_dd and abs(float(max_dd)) > 10 else '#f39c12' if max_dd and abs(float(max_dd)) > 5 else '#27ae60'
        vc = '#e74c3c' if vol and float(vol) > 25 else '#f39c12' if vol and float(vol) > 15 else '#27ae60'
        vbg = T['risk_amber'] if vol and float(vol) > 15 else T['risk_green']

        # 持仓表格
        tv = _pick(summary, 'total_value', '总市值', default=1) or 1
        pos_rows = ''
        for i, p in enumerate(positions):
            pnl = _pick(p, 'pnl', '持仓盈亏', default=0) or 0
            pnl_rate = _pick(p, 'pnl_rate', '盈亏率%', default=0) or 0
            mv = _pick(p, 'market_value', '最新市值', default=0) or 0
            wt = mv / tv * 100
            pc = clr(pnl)
            ps = sign(pnl)
            bg = T['row1'] if i % 2 == 0 else T['row2']
            p_name = _pick(p, 'name', '名称', default='')
            p_code = _pick(p, 'code', '代码', default='')
            p_qty = _pick(p, 'quantity', '证券数量', default=0) or 0
            p_cost = _pick(p, 'cost_price', '成本价', default=0) or 0
            p_last = _pick(p, 'current_price', '现价', default=0) or 0
            pos_rows += (
                '<tr style="background:' + bg + ';">'
                '<td style="padding:7px 10px;font-size:12px;font-weight:500;">' + p_name + '</td>'
                '<td style="padding:7px 10px;font-size:12px;color:' + T['gray'] + ';">' + p_code + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;">' + f"{p_qty:,.0f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;">' + f"{p_cost:.3f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;font-weight:500;">' + f"{p_last:.3f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;">' + ps + '¥' + f"{mv:,.0f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;color:' + pc + ';font-weight:500;">' + ps + '¥' + f"{pnl:,.0f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;color:' + pc + ';">' + ps + f"{pnl_rate:.2f}" + '%</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;color:' + T['gray'] + ';">' + f"{wt:.1f}" + '%</td>'
                '</tr>'
            )

        # 告警（口径 = 报告数据日期当天，见 _load_alerts）
        if alerts:
            ai = ''
            for a in alerts:
                lc = '#e74c3c' if a['level'] == 'error' else '#f39c12'
                ic = '🔴' if a['level'] == 'error' else '🟡'
                ai += '<tr><td style="padding:6px 10px;font-size:12px;">' + ic + ' <span style="color:' + lc + ';font-weight:600;">[' + a['level'].upper() + ']</span> ' + a['message'] + '</td></tr>'
            ab = '<div style="margin:14px 0;padding:14px;background:' + T['alert_bg'] + ';border-radius:8px;border-left:4px solid #e74c3c;"><div style="font-size:13px;font-weight:600;color:#e74c3c;margin-bottom:8px;">⚠️ 告警 (' + str(len(alerts)) + ' 条，数据日期 ' + str(report_date) + ')</div><table style="width:100%;border-collapse:collapse;">' + ai + '</table></div>'
        else:
            ab = '<div style="margin:14px 0;padding:14px;background:' + T['ok_bg'] + ';border-radius:8px;border-left:4px solid #27ae60;"><span style="font-size:12px;color:#27ae60;">✅ 数据日期 ' + str(report_date) + ' 无告警</span></div>'

        # 智能建议
        adv_block = ''
        if advice:
            pm = {'high': ('🔴 高优先级', '#e74c3c'), 'medium': ('🟡 中优先级', '#f39c12'), 'low': ('🟢 低优先级', '#27ae60')}
            items = ''
            for a in advice[:6]:
                pl, pcolor = pm.get(_norm_priority(a.get('priority')), ('⚪ 未分级', '#95a5a6'))
                items += '<div style="padding:6px 0;border-bottom:1px solid ' + T['border'] + ';font-size:12px;"><span style="font-weight:600;color:' + pcolor + ';">' + pl + '</span> ' + a.get('title', '') + '</div>'
            adv_block = '<div style="margin:14px 0;padding:14px;background:' + T['advice_bg'] + ';border-radius:8px;"><div style="font-size:13px;font-weight:600;color:' + T['text'] + ';margin-bottom:8px;">💡 智能建议 (' + str(len(advice)) + ')</div>' + items + '</div>'

        # 图表
        cb = ''
        if nav_b64:
            cb += '<div style="margin:14px 0;"><div style="font-size:13px;font-weight:600;color:' + T['text'] + ';margin-bottom:8px;">📈 组合净值走势（vs 沪深300）</div><img src="data:image/png;base64,' + nav_b64 + '" style="width:100%;border-radius:6px;" /></div>'
        if dd_b64:
            cb += '<div style="margin:14px 0;"><div style="font-size:13px;font-weight:600;color:' + T['text'] + ';margin-bottom:8px;">📉 回撤曲线</div><img src="data:image/png;base64,' + dd_b64 + '" style="width:100%;border-radius:6px;" /></div>'

        # 基准对比板块
        index_block = self._build_index_comparison(index_today, summary, clr, sign)

        # 行业资讯板块
        news_block = self._build_news_section(news_data)

        # 技术信号板块
        tech_block = self._build_technical_signals(technical, price_30d)

        # ETF 风险展望板块
        risk_block = self._build_risk_outlook()

        # 组装HTML
        html = (
            '<!DOCTYPE html><html><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<style>' + css + '</style></head><body>'
            '<div class="c">'
            + banner +
            '<div class="hd"><h1>📊 投资组合日报</h1><p>数据日期: ' + date_str + ' ' + weekday + '</p>'
            '<p style=\'font-size:10px;margin-top:6px;color:' + T['sub'] + ';\'>数据来源: 新浪财经 / 东方财富 | 生成时间: '
            + now.strftime('%Y-%m-%d %H:%M:%S') + '</p></div>'
            '<div class="ms">'
            '<div class="m"><div class="l">总市值</div><div class="v" style="color:#1a73e8;">¥' + f"{summary['total_value']:,.0f}" + '</div></div>'
            '<div class="m"><div class="l">当日盈亏</div><div class="v" style="color:' + clr(dr) + ';">' + sign(dr) + '¥' + f"{dp:,.0f}" + '</div><div class="s">' + sign(dr) + f"{dr:.2f}" + '%</div><div class="s" style="font-size:9px;">' + dr_cov_note + '</div></div>'
            '<div class="m"><div class="l">累计盈亏</div><div class="v" style="color:' + clr(tp) + ';">' + sign(tp) + '¥' + f"{tp:,.0f}" + '</div><div class="s">' + sign(tp) + f"{total_ret:.2f}" + '%</div></div>'
            '</div>'
            '<div style="font-size:9px;line-height:1.6;color:' + T['sub'] + ';padding:6px 2px 0;">'
            + dr_caliber_note + '</div>'
            '<div class="sec"><div class="st">⚠️ 风险指标</div>'
            '<div class="rg">'
            '<div class="rc" style="background:' + T['risk_green'] + ';"><div class="rl">夏普比率</div><div class="rv" style="color:' + sc + ';">' + ss + '</div></div>'
            '<div class="rc" style="background:' + T['risk_red'] + ';"><div class="rl">最大回撤</div><div class="rv" style="color:' + ddc + ';">' + dds + '%</div></div>'
            '<div class="rc" style="background:' + vbg + ';"><div class="rl">年化波动率</div><div class="rv" style="color:' + vc + ';">' + vss + '%</div></div>'
            '</div></div>'
            + index_block
            + ab + cb
            + '<div class="sec"><div class="st">📋 持仓明细 (' + str(len(positions)) + '只，盈' + str(summary.get('profit_count', 0)) + '亏' + str(summary.get('loss_count', 0)) + ')</div>'
            '<table><thead><tr><th>名称</th><th>代码</th><th>持仓量</th><th>成本</th><th>现价</th><th>市值</th><th>盈亏</th><th>收益率</th><th>占比</th></tr></thead>'
            '<tbody>' + pos_rows + '</tbody></table></div>'
            + tech_block
            + risk_block
            + adv_block
            + news_block
            + '<div class="ft">投资组合跟踪分析系统 v2.0 自动生成<br>本报告仅供参考，不构成任何投资建议或买卖操作指令。投资有风险，入市需谨慎。 | 生成时间: ' + now.strftime('%Y-%m-%d %H:%M:%S') + '</div>'
            '</div></body></html>'
        )
        return html

    def _build_nav_chart(self, history):
        fig, ax = plt.subplots(figsize=(7, 2.5), dpi=150)
        fig.patch.set_facecolor('#ffffff')
        ax.set_facecolor('#ffffff')
        dates = pd.to_datetime(history['date'])
        # 使用 corrected daily_return 累积净值法，避免 total_value 跳变导致净值突增
        if 'daily_return' in history.columns:
            nav = (1 + history['daily_return'] / 100).fillna(0).cumprod() * 100
        else:
            base = history.iloc[0]['total_value']
            nav = history['total_value'] / base * 100
        ax.plot(dates, nav, color='#1a73e8', linewidth=1.8, label='组合净值', zorder=3)
        ax.fill_between(dates, nav, alpha=0.06, color='#1a73e8')
        hs300 = self._load_index_history('sh000300', 60)
        if not hs300.empty:
            hs_dates = pd.to_datetime(hs300['date'])
            hs_base = hs300.iloc[0]['close']
            hs_nav = hs300['close'] / hs_base * 100
            ax.plot(hs_dates, hs_nav, color='#f59e0b', linewidth=1.2, linestyle='--', label='沪深300', alpha=0.8)
        ax.legend(fontsize=9, loc='upper left', framealpha=0.8)
        ax.grid(True, alpha=0.2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        ax.tick_params(axis='both', labelsize=8)
        ax.set_ylabel('净值', fontsize=9)
        fig.autofmt_xdate()
        plt.tight_layout(pad=1.0)
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', facecolor='#ffffff')
        buf.seek(0)
        b64 = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        return b64

    def _build_drawdown_chart(self, history):
        fig, ax = plt.subplots(figsize=(7, 1.8), dpi=150)
        fig.patch.set_facecolor('#ffffff')
        ax.set_facecolor('#ffffff')
        dates = pd.to_datetime(history['date'])
        values = history['total_value'].values
        peak = np.maximum.accumulate(values)
        dd = (values - peak) / peak * 100
        ax.fill_between(dates, dd, 0, alpha=0.2, color='#e74c3c')
        ax.plot(dates, dd, color='#e74c3c', linewidth=1.2)
        ax.grid(True, alpha=0.2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        ax.tick_params(axis='both', labelsize=8)
        ax.set_ylabel('回撤 (%)', fontsize=9)
        fig.autofmt_xdate()
        plt.tight_layout(pad=1.0)
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', facecolor='#ffffff')
        buf.seek(0)
        b64 = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        return b64

    def save_report(self, html, filename=None, news_data=None):
        if not filename:
            filename = datetime.now().strftime('daily_report_%Y%m%d.html')
        report_dir = Path(self.db_path).parent.parent.parent / 'data' / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        filepath = report_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("报告已保存: " + str(filepath))
        # 同步latest_report.html快捷链接
        latest_path = report_dir / 'latest_report.html'
        with open(latest_path, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("已同步: " + str(latest_path))
        return str(filepath)

    @staticmethod
    def _fmt_data_date(report_date):
        """把 'YYYY-MM-DD' 数据日期渲染成 ('2026年09月14日', '周一')。

        无法解析时原样返回、星期留空 —— 绝不回退到 datetime.now()，
        否则又会把数据日期伪装成"今天"。
        """
        wd_map = {0: '周一', 1: '周二', 2: '周三', 3: '周四', 4: '周五', 5: '周六', 6: '周日'}
        if not report_date:
            return '', ''
        try:
            d = datetime.strptime(str(report_date)[:10], '%Y-%m-%d')
        except (ValueError, TypeError):
            return str(report_date), ''
        return d.strftime('%Y年%m月%d日'), wd_map.get(d.weekday(), '')

    @staticmethod
    def _lag_days(report_date, source_date):
        """source_date 相对 report_date 的滞后天数（正=更旧，负=更新，None=算不出）。"""
        if not report_date or not source_date:
            return None
        try:
            a = datetime.strptime(str(report_date)[:10], '%Y-%m-%d')
            b = datetime.strptime(str(source_date)[:10], '%Y-%m-%d')
        except (ValueError, TypeError):
            return None
        return (a - b).days

    @staticmethod
    def _fmt_lag(lag):
        """把滞后天数渲染成人话；None 表示无法计算。"""
        if lag is None:
            return '日期不可比'
        if lag == 0:
            return '同日'
        if lag > 0:
            return f'滞后 {lag} 天'
        return f'超前 {abs(lag)} 天'

    def _split_date_mismatches(self, report_date, src_dates):
        """按模块级 _DATE_SOURCES 把日期源不一致拆成 (硬不一致, 软滞后) 两组。

        Returns:
            (hard, soft)，每项形如
            {"table","label","date","lag_days","reason"}。

        判据：必须同日的硬源(portfolio_summary/portfolio_snapshots)不等即硬不一致；
        允许合法滞后的软源(指数行情/技术指标/波动率特征/历史价格)只进软滞后组，
        不做硬判定（否则非交易日或软阶段被跳过会把正常日误报成降级）。
        表内完全无数据的源(MAX(date) 为 None)**不参与判定** —— 没有数据就没有
        日期错配可言，硬凑一个"不一致"只会制造噪声。
        """
        report_date = str(report_date)[:10] if report_date else None
        hard, soft = [], []
        for table, label, is_hard, reason in _DATE_SOURCES:
            src_date = src_dates.get(table)
            if not src_date:
                continue
            src_date = str(src_date)[:10]
            if report_date and src_date == report_date:
                continue
            item = {
                "table": table,
                "label": label,
                "date": src_date,
                "lag_days": self._lag_days(report_date, src_date),
                "reason": reason,
            }
            (hard if is_hard else soft).append(item)
        return hard, soft

    def _build_date_notice(self, report_date, hard_mismatches, soft_mismatches):
        """渲染页面顶部的日期口径提示条，逐条列出**全部**不一致的日期源。

        - 存在硬不一致 ⇒ 红色降级横幅 + DEGRADED_MARKER（调用方据此拒绝推送）；
        - 仅软源滞后   ⇒ 琥珀色提示条 + LAG_NOTICE_MARKER（可见但不拒发）。
        两者都渲染在 <div class="c"> 之后、页头之前，任何消费方第一眼就能看到。
        """
        def _lines(items):
            return ''.join(
                '<br>&nbsp;&nbsp;· ' + it["label"] + '(' + it["table"] + ')数据日期: '
                + it["date"] + '（' + self._fmt_lag(it["lag_days"]) + '）'
                + ('｜原因: ' + it["reason"] if it["reason"] else '')
                for it in items
            )

        if hard_mismatches:
            return (
                '<!-- ' + DEGRADED_MARKER + ' -->'
                '<div style="padding:12px 20px;background:#fdecea;border-bottom:3px solid #e74c3c;'
                'color:#b3261e;font-size:12px;line-height:1.7;font-weight:600;">'
                '⛔ 数据降级：本报告必须同日的日期源出现不一致，数据不完整。'
                '<br>· 报告数据日期: ' + str(report_date or '(缺失)')
                + '（组合汇总 portfolio_summary）'
                + _lines(hard_mismatches)
                + _lines(soft_mismatches)
                + '<br>持仓明细/告警/智能建议已统一按 ' + str(report_date or '(未知)')
                + ' 口径取值，以保证单份报告内部自洽。'
                '<br>本报告仅供内部排查，<span style="text-decoration:underline;">不可作为决策依据</span>。'
                '</div>'
            )
        if soft_mismatches:
            return (
                '<!-- ' + LAG_NOTICE_MARKER + ' -->'
                '<div style="padding:10px 20px;background:#fdf6e3;border-bottom:2px solid #f39c12;'
                'color:#8a6d1f;font-size:12px;line-height:1.7;">'
                '⚠️ 数据口径提示：以下区块的数据日期与报告数据日期'
                '(' + str(report_date or '未知') + '，组合汇总 portfolio_summary) 不同，'
                '属允许的滞后（各区块标题内已标注实际日期）：'
                + _lines(soft_mismatches) +
                '</div>'
            )
        return ''

    def _block_date_suffix(self, table):
        """区块标题用的"数据日期"后缀。

        允许合法滞后的区块（指数行情 / 技术指标）必须**自带实际数据日期**，
        否则读者会把昨天的指数收盘、昨天的技术信号当成报告数据日期当天的 ——
        这正是 09-15 事故里"旧告警被当成今日告警"的同一类隐蔽错配。
        与报告数据日期一致时也照常标注（口径透明，不产生歧义）。
        """
        src_date = (getattr(self, '_src_dates', {}) or {}).get(table)
        if not src_date:
            return ''
        if str(src_date) == str(getattr(self, '_data_date', '') or ''):
            return '（数据日期 ' + str(src_date) + '）'
        lag = self._lag_days(getattr(self, '_data_date', None), src_date)
        return ('（数据日期 ' + str(src_date) + '，' + self._fmt_lag(lag)
                + '；报告数据日期 ' + str(getattr(self, '_data_date', '') or '未知') + '）')

    def _load_source_dates(self):
        """一次性取出**全部**日期源表的最新日期。

        Returns:
            {表名: MAX(date)，无数据/表不存在为 None}
        """
        conn = get_db_connection(self.db_path)
        try:
            cursor = conn.cursor()
            out = {}
            for table, _label, _hard, _reason in _DATE_SOURCES:
                try:
                    cursor.execute(f"SELECT MAX(date) FROM {table}")
                    row = cursor.fetchone()
                    out[table] = (str(row[0])[:10] if row and row[0] else None)
                except sqlite3.OperationalError:
                    # 表不存在（精简单元库/全新库）等同"无数据"，不参与判定
                    out[table] = None
        finally:
            conn.close()
        return out

    def _load_summary(self):
        conn = get_db_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM portfolio_summary ORDER BY date DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def _load_positions(self, report_date=None):
        """持仓快照。给定 report_date 时**只取该日**，与页头口径强制一致。

        原实现固定取 `MAX(date)`，与页头取值的 portfolio_summary 最新日期相互独立，
        正是 09-15 报告"页头 09-14 / 持仓 09-15"的直接成因。
        """
        conn = get_db_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if report_date:
            cursor.execute(
                "SELECT * FROM portfolio_snapshots WHERE date = ? ORDER BY market_value DESC",
                (str(report_date)[:10],),
            )
        else:
            cursor.execute("SELECT * FROM portfolio_snapshots WHERE date = (SELECT MAX(date) FROM portfolio_snapshots) ORDER BY market_value DESC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_history(self, days, report_date=None):
        """净值/回撤曲线的历史序列。

        report_date 给定时只取 <= 该日期的行，避免曲线画到数据日期之后
        （回填/跨日期场景下与页头声明的数据日期矛盾）。日期一致时行为不变。
        """
        conn = get_db_connection(self.db_path)
        if report_date:
            df = pd.read_sql_query(
                "SELECT * FROM portfolio_summary WHERE date <= ? ORDER BY date DESC LIMIT ?",
                conn, params=(str(report_date)[:10], days))
        else:
            df = pd.read_sql_query("SELECT * FROM portfolio_summary ORDER BY date DESC LIMIT ?", conn, params=(days,))
        conn.close()
        return df.sort_values('date').reset_index(drop=True)

    def _load_index_history(self, code, days):
        conn = get_db_connection(self.db_path)
        df = pd.read_sql_query("SELECT date, close FROM index_quotes WHERE code = ? ORDER BY date DESC LIMIT ?", conn, params=(code, days))
        conn.close()
        return df.sort_values('date').reset_index(drop=True)

    def _load_alerts(self, report_date=None):
        """只取【与报告数据日期同一天】的告警。

        原实现 `ORDER BY id DESC LIMIT 5` 取的是"最近 5 条"，与报告数据日期无关：
        09-15 管线崩溃、summary 停在 09-14 时，报告会把 09-14 15:31 的旧告警
        原样渲染成「今日告警」，读者完全看不出来（这正是事故报告里最隐蔽的一处）。
        alerts.created_at 为 ISO 时间戳，date() 可直接取自然日。
        """
        if report_date is None:
            report_date = self._load_source_dates().get("portfolio_summary")
        if not report_date:
            return []
        conn = get_db_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT rule_name, level, message FROM alerts "
            "WHERE date(created_at) = ? ORDER BY id DESC LIMIT ?",
            (str(report_date)[:10], _ALERTS_LIMIT),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_advice(self, report_date=None):
        """读取 smart_report 的建议条目。

        给定 report_date 时**只认与报告日期同日**的那份 md —— 原实现按文件名排序
        取最新一份，与报告数据日期无关，会把另一天的建议挂到本报告上。
        """
        report_dir = Path(self.db_path).parent.parent.parent / 'data' / 'reports'
        if not report_dir.exists():
            return []
        if report_date:
            target = report_dir / ('smart_report_' + str(report_date)[:10].replace('-', '') + '.md')
            if not target.exists():
                return []
        else:
            reports = sorted(report_dir.glob('smart_report_*.md'), reverse=True)
            if not reports:
                return []
            target = reports[0]
        advices = []
        with open(target, 'r', encoding='utf-8') as f:
            content = f.read()
        import re
        for m in re.finditer(r'### \d+\.\s+\[(高|中|低)\]\s+(.+?)(?:\n|$)', content):
            advices.append({'priority': m.group(1), 'title': m.group(2).strip()})
        return advices


    def _load_index_today(self):
        conn = get_db_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT code, name, close, change_pct FROM index_quotes WHERE date = (SELECT MAX(date) FROM index_quotes)")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_technical(self):
        conn = get_db_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""SELECT t.code, p.name, p.current_price, t.ma_signal, t.macd_signal, t.rsi_value, t.rsi_status,
                          t.kdj_signal, t.bollinger_position, t.atr_pct, t.trend
                          FROM etf_technical t LEFT JOIN portfolio_snapshots p 
                          ON t.code = p.code AND t.date = p.date
                          WHERE t.date = (SELECT MAX(date) FROM etf_technical)""")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]


    def _load_price_30d_ago(self, report_date=None):
        """加载30个交易日前的持仓价格，用于计算30日涨跌幅。

        report_date 给定时，起点与 30 日窗口都锚定在该数据日期上，
        避免"起点是 09-15、对照价来自 09-14 之前"的跨日期混算。
        """
        try:
            import sqlite3
            anchor_date = str(report_date)[:10] if report_date else None
            conn = get_db_connection(self.db_path)
            cur = conn.cursor()
            if anchor_date:
                cur.execute("""
                    SELECT a.code, b.current_price as price_30d_ago
                    FROM (SELECT DISTINCT code FROM portfolio_snapshots WHERE date = ?) a
                    JOIN portfolio_snapshots b ON a.code = b.code
                    AND b.date = (
                        SELECT date FROM portfolio_snapshots
                        WHERE date <= ?
                        ORDER BY date DESC LIMIT 1 OFFSET 29
                    )
                """, (anchor_date, anchor_date))
            else:
                cur.execute("""
                    SELECT a.code, b.current_price as price_30d_ago
                    FROM (SELECT DISTINCT code FROM portfolio_snapshots WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)) a
                    JOIN portfolio_snapshots b ON a.code = b.code
                    AND b.date = (
                        SELECT date FROM portfolio_snapshots
                        ORDER BY date DESC LIMIT 1 OFFSET 29
                    )
                """)
            result = {row[0]: row[1] for row in cur.fetchall()}
            conn.close()
            return result
        except sqlite3.OperationalError:
            return {}

    def _build_index_comparison(self, index_today, summary, clr, sign):
        T = getattr(self, '_T', THEMES['dark'])
        if not index_today:
            return ""
        dr = _pick(summary, 'daily_return', '日收益率', default=0) or 0
        rows_html = ""
        for i, idx in enumerate(index_today):
            chg = _pick(idx, 'change_pct', '涨跌幅', default=0) or 0
            c = clr(chg)
            s = sign(chg)
            name = _pick(idx, 'name', '名称', 'code', '代码', default='')
            close = _pick(idx, 'close', '收盘价', default=0)
            close_str = f"{close:,.2f}" if close > 100 else f"{close:.4f}"
            # 原写法用 len(rows_html)==0 判断，累加字符串首次之后恒为非空，
            # 导致第 3 行起全部落 row2、斑马纹失效。改用行号奇偶（与其余表格一致）。
            bg = T['row1'] if i % 2 == 0 else T['row2']
            # 标记组合表现
            compare = ""
            if _pick(idx, 'code', '代码') == 'sh000300' and dr != 0:
                diff = dr - chg
                if abs(diff) > 0.1:
                    tag = "跑赢" if diff > 0 else "跑输"
                    tc = '#27ae60' if diff > 0 else '#e74c3c'
                    compare = '<span style="font-size:11px;color:' + tc + ';font-weight:600;margin-left:6px;">' + tag + f"{abs(diff):.2f}%</span>"
            rows_html += (
                '<tr style="background:' + bg + ';">'
                '<td style="padding:6px 10px;font-size:12px;font-weight:500;">' + name + '</td>'
                '<td style="padding:6px 10px;font-size:12px;text-align:right;">' + close_str + '</td>'
                '<td style="padding:6px 10px;font-size:12px;text-align:right;color:' + c + ';font-weight:500;">' + s + f"{chg:.2f}" + '%</td>'
                '<td style="padding:6px 10px;font-size:12px;">' + compare + '</td>'
                '</tr>'
            )
        return (
            '<div class="sec"><div class="st">📊 基准指数对比' + self._block_date_suffix("index_quotes") + '</div>'
            '<table><thead><tr><th>指数</th><th>收盘价</th><th>涨跌幅</th><th>vs组合</th></tr></thead>'
            '<tbody>' + rows_html + '</tbody></table></div>'
        )

    def _build_technical_signals(self, technical, price_30d=None):
        T = getattr(self, '_T', THEMES['dark'])
        if not technical:
            return ""
        rows_html = ""
        for i, t in enumerate(technical):
            bg = T['row1'] if i % 2 == 0 else T['row2']
            name = _pick(t, 'name', '名称', 'code', '代码', default='未知')
            # 30日涨跌幅
            chg30_cell = '<span style="color:' + T['gray'] + ';">--</span>'
            if price_30d and isinstance(price_30d, dict):
                code = _pick(t, 'code', '代码', default='')
                old_price = price_30d.get(code)
                cur_price = _pick(t, 'current_price', '现价', default=0) or 0
                if old_price and old_price > 0 and cur_price > 0:
                    pct = (cur_price - old_price) / old_price * 100
                    chg30_c = '#27ae60' if pct >= 0 else '#e74c3c'
                    chg30_cell = '<span style="color:' + chg30_c + ';">' + f"{pct:+.1f}" + '%</span>'
            # MA信号颜色
            ma = t.get('ma_signal', '--')
            ma_c = '#27ae60' if '多头' in str(ma) else '#e74c3c' if '空头' in str(ma) else '#7f8c8d'
            # RSI颜色
            rsi = t.get('rsi_value', 0) or 0
            rsi_s = t.get('rsi_status', '--')
            # 用子串匹配覆盖「超买/严重超买」「超卖/严重超卖」四个状态。
            # 原写法只判了相等，漏掉 DB 中实际存在的 '严重超卖'（479 行），把它渲染成了中性灰。
            rsi_c = '#e74c3c' if '超买' in str(rsi_s) else '#27ae60' if '超卖' in str(rsi_s) else '#7f8c8d'
            # MACD信号
            macd = t.get('macd_signal', '--')
            macd_c = '#27ae60' if '买入' in str(macd) else '#e74c3c' if '卖出' in str(macd) else '#7f8c8d'
            # KDJ信号
            kdj = t.get('kdj_signal', '--')
            kdj_c = '#27ae60' if '金叉' in str(kdj) else '#e74c3c' if '死叉' in str(kdj) else '#7f8c8d'
            # 趋势
            trend = t.get('trend', '--')
            trend_c = '#27ae60' if '上涨' in str(trend) else '#e74c3c' if '下跌' in str(trend) else '#f39c12'
            # 布林带位置
            bp = t.get('bollinger_position', 0)
            bp_bar = ""
            if bp:
                bp = float(bp)
                bar_w = max(5, min(80, bp * 0.8))
                bar_c = '#e74c3c' if bp > 80 else '#f39c12' if bp > 60 else '#27ae60'
                bp_bar = '<div style="background:' + T['row2'] + ';border-radius:3px;height:6px;width:80px;display:inline-block;vertical-align:middle;"><div style="background:' + bar_c + ';border-radius:3px;height:6px;width:' + f"{bar_w:.0f}" + 'px;"></div></div> <span style="font-size:10px;color:' + T['gray'] + ';">' + f"{bp:.0f}" + '%</span>'
            rows_html += (
                '<tr style="background:' + bg + ';">'
                '<td style="padding:5px 8px;font-size:11px;font-weight:500;">' + name + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + ma_c + ';">' + str(ma) + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + macd_c + ';">' + str(macd) + '</td>'
                '<td style="padding:5px 8px;font-size:11px;"><span style="color:' + rsi_c + ';">' + f"{rsi:.1f}" + '</span> <span style="font-size:10px;color:' + T['gray'] + ';">' + str(rsi_s) + '</span></td>'
                '<td style="padding:5px 8px;font-size:11px;">' + chg30_cell + '</td>'
                '<td style="padding:5px 8px;font-size:11px;">' + bp_bar + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + kdj_c + ';">' + str(kdj) + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + trend_c + ';">' + str(trend) + '</td>'
                '</tr>'
            )
        return (
            '<div class="sec"><div class="st">🔍 技术信号汇总 (' + str(len(technical)) + '只)'
            + self._block_date_suffix("etf_technical") + '</div>'
            '<table><thead><tr><th>名称</th><th>均线</th><th>MACD</th><th>RSI</th><th>30日涨跌</th><th>布林位置</th><th>KDJ</th><th>趋势</th></tr></thead>'
            '<tbody>' + rows_html + '</tbody></table></div>'
        )

    def _build_news_section(self, news_data):
        T = getattr(self, '_T', THEMES['dark'])
        if not news_data:
            return ""
        news = news_data.get('news', {})
        impacts = news_data.get('impacts', [])
        rotation = news_data.get('rotation', {})
        if not news and not impacts and not rotation:
            return ""
        blocks = ""
        # 资讯列表
        if news:
            for topic_key, topic_val in news.items():
                label = topic_val.get('label', topic_key)
                items = topic_val.get('news', [])
                if not items:
                    continue
                items_html = ""
                for n in items[:3]:
                    title = n.get('title', '')
                    source = n.get('source', '')
                    url = n.get('url', '')
                    title_html = ('<a href="' + url + '" target="_blank" style="color:' + T['text'] + ';text-decoration:none;">' + title + '</a>') if url else title
                    items_html += '<div style="padding:4px 0;font-size:12px;border-bottom:1px solid ' + T['border'] + ';">' + title_html + ' <span style="font-size:10px;color:' + T['muted'] + ';">' + source + '</span></div>'
                blocks += (
                    '<div style="margin:8px 0;padding:10px;background:' + T['advice_bg'] + ';border-radius:6px;">'
                    '<div style="font-size:12px;font-weight:600;color:' + T['sub'] + ';margin-bottom:6px;">' + label + '</div>'
                    + items_html + '</div>'
                )
        # 新闻影响评估
        if impacts:
            imp_items = ""
            for imp in impacts[:5]:
                title = imp.get('title', '')
                sentiment = imp.get('sentiment', 'neutral')
                affected = imp.get('affected_positions', [])
                if sentiment == 'positive':
                    s_icon = '<span style="color:#27ae60;font-weight:600;">[利好]</span>'
                elif sentiment == 'negative':
                    s_icon = '<span style="color:#e74c3c;font-weight:600;">[利空]</span>'
                else:
                    s_icon = '<span style="color:#7f8c8d;">[中性]</span>'
                aff_str = ""
                if affected:
                    aff_str = ' <span style="font-size:10px;color:#3498db;">影响: ' + '、'.join(affected[:3]) + '</span>'
                imp_items += '<div style="padding:4px 0;font-size:12px;border-bottom:1px solid ' + T['border'] + ';">' + s_icon + ' ' + title + aff_str + '</div>'
            if imp_items:
                blocks += (
                    '<div style="margin:8px 0;padding:10px;background:' + T['warn_bg'] + ';border-radius:6px;border-left:3px solid #f39c12;">'
                    '<div style="font-size:12px;font-weight:600;color:#f59e0b;margin-bottom:6px;">📰 新闻影响评估</div>'
                    + imp_items + '</div>'
                )
        # 行业轮动
        if rotation:
            leaders = rotation.get('leaders', [])
            laggards = rotation.get('laggards', [])
            trend = rotation.get('trend', '')
            if leaders or laggards or trend:
                rot_html = '<div style="font-size:12px;color:' + T['muted'] + ';margin-bottom:6px;">' + trend + '</div>'
                if leaders:
                    rot_html += '<div style="font-size:11px;color:' + T['gray'] + ';margin-bottom:4px;">领涨:</div>'
                    for l in leaders[:3]:
                        rot_html += '<span style="display:inline-block;margin-right:12px;font-size:12px;color:#27ae60;">' + l.get('name', '') + ' ' + f"{l.get('change_pct', 0):+.2f}" + '%</span>'
                if laggards:
                    rot_html += '<div style="font-size:11px;color:' + T['gray'] + ';margin:4px 0;">领跌:</div>'
                    for l in laggards[:3]:
                        rot_html += '<span style="display:inline-block;margin-right:12px;font-size:12px;color:#e74c3c;">' + l.get('name', '') + ' ' + f"{l.get('change_pct', 0):+.2f}" + '%</span>'
                blocks += (
                    '<div style="margin:8px 0;padding:10px;background:' + T['risk_green'] + ';border-radius:6px;">'
                    '<div style="font-size:12px;font-weight:600;color:#27ae60;margin-bottom:6px;">🔄 行业轮动</div>'
                    + rot_html + '</div>'
                )
        if not blocks:
            return ""
        return '<div class="sec"><div class="st">📰 行业资讯与影响分析</div>' + blocks + '</div>'

    def _build_risk_outlook(self):
        """ETF 风险展望区块（纯历史已实现波动率，读 etf_features 的 vol_20d/vol_60d）。

        2026-09-15 起不再读取或触发 etf_predictions 的波动率预测：原 risk_lgb 模型
        已因样本外截面 IC 未跑赢免费的 vol_20d 基线而下线，此处改为纯历史统计。
        """
        try:
            from src.utils.risk_report import build_risk_outlook_html, get_risk_outlook
            conn = get_db_connection(self.db_path)
            try:
                outlook = get_risk_outlook(conn, self.db_path)
            finally:
                conn.close()
            return build_risk_outlook_html(outlook, theme=getattr(self, '_theme', 'dark'))
        except Exception as exc:
            logger.warning("风险展望块生成失败: %s", exc)
            return ""
