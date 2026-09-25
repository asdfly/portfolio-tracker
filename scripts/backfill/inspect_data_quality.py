"""现有数据质量全面检查方案（诊断型，只读，不写库）。

扫描 portfolio.db 中所有与"东财主源(akshare EM)"相关的表，量化：
  - 行数 / 覆盖代码数 / 最新日期 / source 分布
  - 是否"卡点"(最新日期 < 期望交易日) —— 典型症状：host 级 RST 阻断 EM 主源
  - 跨表一致性(各表最新日期是否收敛)
  - 缺口检测(期望最近 N 个交易日是否齐备)
  - 误标个股污染(code 落在本应是 ETF 的表里)

输出：
  - JSON: scripts/backfill/data/data_quality_report.json
  - Markdown: 控制台 + 返回 dict 供其它脚本消费

用法：
  venv313/Scripts/python.exe scripts/backfill/inspect_data_quality.py [--expect YYYY-MM-DD]
  expect 默认 = 最近一个工作日(简单取今天-1天，调用方应按交易日历校准)
"""
from __future__ import annotations
import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH, is_otc_fund, is_delisted  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_JSON = os.path.join(HERE, "data", "data_quality_report.json")

# 东财主源(akshare *em*) 表 -> (主源 akshare 函数, 是否受 EM RST 阻断影响)
# is_em=True 表示该表主源是东财 EM 接口，存在被 host 级 RST 阻断的风险；
# index_pe_history 主源是 csindex（非EM），1 天滞后通常是良性 T+1，不应误报为 RST 阻断。
EM_PRIMARY_TABLES = {
    "etf_fundamental": ("fund_etf_spot_em", True),
    "etf_top_holdings": ("fund_portfolio_hold_em", True),
    "etf_industry_alloc": ("fund_portfolio_industry_allocation_em", True),
    "fund_flows": ("fund_etf_spot_em (资金流字段)", True),
    "etf_price_history": ("fund_etf_hist_em", True),
    "index_pe_history": ("stock_zh_index_value_csindex (非EM)", False),
}

# 误标污染：任何场外基金(is_otc_fund)或已清仓标的(is_delisted)都不应出现在 ETF 数据表。
# 不再硬编码具体代码——通用规则覆盖 OTC_FUND_CODES 全集合，作为代码层防御(is_otc_fund
# 拦截)的回归自检。若仍检出，说明采集入口绕过或未生效。


def _q(c, sql, args=()):
    return c.execute(sql, args).fetchall()


def _col_exists(c, table, col):
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})")]
    return col in cols


def _latest_date(c, table, date_col="date"):
    r = _q(c, f"SELECT MAX({date_col}) FROM {table}")
    return r[0][0] if r and r[0][0] is not None else None


def _expected_trading_day(now=None):
    """粗略期望最新交易日：今天往前找第一个非周末（非精确，仅用于告警阈值）。"""
    now = now or datetime.now()
    d = now.date()
    while d.weekday() >= 5:  # 5=Sat 6=Sun
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def inspect(c, expect_date):
    report = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "expect_latest_date": expect_date,
        "tables": {},
        "cross_table": {},
        "em_blocked_tables": [],
        "recommendations": [],
    }

    # ---- 逐表扫描 ----
    for table, (em_src, is_em) in EM_PRIMARY_TABLES.items():
        info = {"exists": False}
        try:
            cnt = _q(c, f"SELECT COUNT(*) FROM {table}")[0][0]
        except Exception as e:
            info["error"] = str(e)
            report["tables"][table] = info
            continue
        info["exists"] = True
        info["rows"] = cnt
        info["em_primary_source"] = em_src
        info["is_em_primary"] = is_em

        # 覆盖代码数（优先 code 列）
        if _col_exists(c, table, "code"):
            info["distinct_codes"] = _q(c, f"SELECT COUNT(DISTINCT code) FROM {table}")[0][0]
            # 误标污染检测：ETF 数据表不得含场外基金(is_otc_fund)或已清仓标的(is_delisted)
            if table in ("etf_fundamental", "etf_top_holdings", "etf_industry_alloc",
                         "etf_technical", "fund_flows"):
                if table == "fund_flows":
                    all_codes = [r[0] for r in _q(c, "SELECT DISTINCT code FROM fund_flows WHERE category='etf'")]
                else:
                    all_codes = [r[0] for r in _q(c, f"SELECT DISTINCT code FROM {table}")]
                bad = sorted({cd for cd in all_codes if is_otc_fund(cd) or is_delisted(cd)})
                if bad:
                    info["non_etf_contamination"] = bad

        # 日期维度
        date_col = "date" if _col_exists(c, table, "date") else None
        if date_col:
            info["max_date"] = _latest_date(c, table, date_col)
            info["distinct_dates"] = _q(c, f"SELECT COUNT(DISTINCT {date_col}) FROM {table}")[0][0]
            if info["max_date"] and info["max_date"] < expect_date:
                info["stuck"] = True
                info["days_behind"] = (datetime.strptime(expect_date, "%Y-%m-%d") - datetime.strptime(info["max_date"], "%Y-%m-%d")).days
            else:
                info["stuck"] = False

        # source 分布
        if _col_exists(c, table, "source"):
            src = _q(c, f"SELECT source, COUNT(*) FROM {table} GROUP BY source ORDER BY 2 DESC")
            info["source_distribution"] = {s: n for s, n in src}
        else:
            info["source_distribution"] = None
            info["note"] = "无 source 列 —— 无法区分主源/兜底，建议加 source/is_estimated/confidence"

        if info.get("stuck") and info.get("is_em_primary"):
            report["em_blocked_tables"].append(table)
            info["block_reason"] = "EM 主源 RST 阻断（卡点）"
        elif info.get("stuck") and not info.get("is_em_primary"):
            info["block_reason"] = "非EM主源滞后（可能良性 T+1，需人工确认）"
        report["tables"][table] = info

    # ---- 跨表一致性：各表最新日期是否收敛 ----
    latest_map = {}
    for t, info in report["tables"].items():
        if info.get("max_date"):
            latest_map[t] = info["max_date"]
    report["cross_table"]["latest_date_by_table"] = latest_map
    if latest_map:
        vals = list(latest_map.values())
        report["cross_table"]["min_latest"] = min(vals)
        report["cross_table"]["max_latest"] = max(vals)
        report["cross_table"]["diverged"] = (min(vals) != max(vals))
        report["cross_table"]["divergence_days"] = (
            datetime.strptime(max(vals), "%Y-%m-%d") - datetime.strptime(min(vals), "%Y-%m-%d")
        ).days

    # ---- 建议 ----
    em_stuck = [t for t, i in report["tables"].items() if i.get("stuck") and i.get("is_em_primary")]
    if em_stuck:
        report["recommendations"].append(
            f"EM 主源被 host 级 RST 阻断，卡点表: {', '.join(em_stuck)}。"
            "建议对这些表套用『连接器+MCP 兜底』(neodata/westock) 并加 source 列。"
        )
    non_em_lag = [t for t, i in report["tables"].items() if i.get("stuck") and not i.get("is_em_primary")]
    if non_em_lag:
        report["recommendations"].append(
            f"非EM主源滞后(疑似良性 T+1): {', '.join(non_em_lag)}，需人工确认是否真缺失。"
        )
    for t, info in report["tables"].items():
        if info.get("source_distribution") is None and info.get("exists"):
            report["recommendations"].append(f"{t}: 缺 source 列，无法审计数据来源，建议加 source/is_estimated/confidence。")
        if info.get("non_etf_contamination"):
            report["recommendations"].append(
                f"{t}: 检测到非ETF污染(场外基金/已清仓) {info['non_etf_contamination']}，"
                f"代码层已用 is_otc_fund/is_delisted 拦截，若仍检出说明采集入口绕过或未生效。"
            )

    return report


def render_md(report):
    lines = ["# 现有数据质量检查报告", ""]
    lines.append(f"- 生成时间: {report['generated_at']}")
    lines.append(f"- 期望最新交易日: {report['expect_latest_date']}")
    lines.append("")
    lines.append("## 逐表概览")
    lines.append("")
    lines.append("| 表 | 行数 | 代码数 | 最新日期 | 卡点 | source 分布 | 备注 |")
    lines.append("|---|---|---|---|---|---|---|")
    for t, info in report["tables"].items():
        if not info.get("exists"):
            lines.append(f"| {t} | - | - | - | - | - | 表不存在: {info.get('error','')} |")
            continue
        src = info.get("source_distribution")
        src_s = ", ".join(f"{k}={v}" for k, v in (src or {}).items()) if src else "无source列"
        stuck = "🔴是" if info.get("stuck") else ("—" if info.get("max_date") is None else "✅否")
        notes = info.get("note", "")
        if info.get("non_etf_contamination"):
            notes += f" 含误标{info['non_etf_contamination']}"
        lines.append(
            f"| {t} | {info.get('rows')} | {info.get('distinct_codes','-')} | "
            f"{info.get('max_date','-')} | {stuck} | {src_s} | {notes} |"
        )
    lines.append("")
    lines.append("## 跨表一致性")
    ct = report["cross_table"]
    lines.append(f"- 各表最新日期: {ct.get('latest_date_by_table')}")
    lines.append(f"- 是否发散: {'是 ⚠️' if ct.get('diverged') else '否 ✅'} (跨度 {ct.get('divergence_days')} 天)")
    lines.append("")
    lines.append("## 建议")
    for r in report["recommendations"]:
        lines.append(f"- {r}")
    lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expect", default=None, help="期望最新交易日 YYYY-MM-DD")
    ap.add_argument("--print", action="store_true", help="打印 markdown 到 stdout")
    args = ap.parse_args()

    expect = args.expect or _expected_trading_day()
    c = sqlite3.connect(str(DATABASE_PATH))
    try:
        report = inspect(c, expect)
    finally:
        c.close()

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    md = render_md(report)
    if args.print:
        print(md)
    print(f"[inspect] 报告已写出: {OUT_JSON}")
    print(f"[inspect] 卡点表: {report['em_blocked_tables'] or '无'}")
    print(f"[inspect] 跨表发散: {report['cross_table'].get('diverged')} (跨度 {report['cross_table'].get('divergence_days')} 天)")
    return report


if __name__ == "__main__":
    main()
