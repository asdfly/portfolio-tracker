#!/usr/bin/env python3
"""验证 5 只 ETF 新浪兜底白名单逻辑(无网络、内存库隔离)。

确认:
- 白名单内代码(511520/159650/511380/001323/002152)走新浪兜底时 -> 不报警
- 白名单外代码(如 588000)意外走兜底 -> 触发 sina_iopv_proxy 质量告警
- stats 正确拆分 sina_fallback_known / sina_fallback_unexpected
"""
import sys
from datetime import date
from pathlib import Path
import sqlite3
import pandas as pd

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

import src.data_sources.etf_fundamental as ef
import src.data_sources.collect_core as cc

TODAY = date.today().strftime("%Y-%m-%d")

# --- 内存库, 隔离真实 DB ---
mem = sqlite3.connect(":memory:")
ef.get_db_connection = lambda: mem

# --- spy: 捕获质量告警调用(函数内为局部 from collect_core import, 故 patch 源头) ---
calls = []
def spy_record_quality_issue(conn, start, end, source, issues):
    for it in issues:
        calls.append(it)
cc.record_quality_issue = spy_record_quality_issue

# --- mock 网络函数 ---
KNOWN = ["511520", "159650", "511380", "001323", "002152"]
UNEXPECTED = ["588000"]
ALL_SINA = KNOWN + UNEXPECTED

def fake_spot_batch(codes):
    rows = [{"code": c, "name": "", "iopv": 1.0, "price": 1.0,
             "open": 1.0, "high": 1.0, "low": 1.0, "pre_close": None,
             "volume": 0.0, "amount": 0.0, "change_pct": None,
             "data_date": TODAY} for c in codes]
    df = pd.DataFrame(rows)
    return df, ALL_SINA

ef.fetch_etf_spot_batch = fake_spot_batch
ef.fetch_index_valuation = lambda idx: {}
ef.fetch_industry_allocation = lambda code: pd.DataFrame()
ef.fetch_top_holdings = lambda code, top_n=10: pd.DataFrame()

# --- 跑采集(目标日=今天, 让真实性闸门放行 spot) ---
cats = {c: {"name": c, "sector": "测试", "color": "#000"} for c in KNOWN + UNEXPECTED}
stats = ef.run_etf_fundamental_collection(
    list(cats.keys()), cats, target_date=TODAY)

print("stats:", {k: v for k, v in stats.items() if k != "errors"})
print("quality_issue 调用数:", len(calls))
for c in calls:
    print("  ->", c.get("issue_type"), c.get("sample"), "n=", c.get("n_affected"))

# --- 断言 ---
assert stats.get("sina_fallback_known") == 5, stats
assert stats.get("sina_fallback_unexpected") == 1, stats
assert len(calls) == 1, calls
assert calls[0]["issue_type"] == "sina_iopv_proxy"
assert "588000" in calls[0]["sample"], calls[0]
assert "511520" not in calls[0]["sample"], "白名单代码不应出现在告警中!"
print("\nVERIFY_OK: 白名单逻辑正确(已知5只不报警, 意外588000报警)")
mem.close()
