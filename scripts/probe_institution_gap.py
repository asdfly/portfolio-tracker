#!/usr/bin/env python3
"""Task #2 探针: 确认机构调研 2026-08-27/08-28/08-31 三日是否源侧永久空洞。

方法: 对 08-24..09-03 每个查询日直接调东方财富 stock_jgdy_detail_em,
收集所有返回的调研日期(research_date)并集, 检查目标三日是否出现过。
- 若目标三日从未作为 research_date 出现 -> 源侧确实无该日调研事件 -> 永久空洞
- 若出现 -> 此前回填遗漏, 需进一步处理
"""
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.data_sources.market_events import fetch_institution_research_data

GAP_DATES = {"2026-08-27", "2026-08-28", "2026-08-31"}
START = date(2026, 8, 24)
END = date(2026, 9, 3)

all_research_dates = set()
per_query = {}
for i in range((END - START).days + 1):
    q = (START + timedelta(days=i)).strftime("%Y%m%d")
    try:
        df = fetch_institution_research_data(q)
    except Exception as e:
        per_query[q] = f"ERR: {e}"
        continue
    if df is None or df.empty:
        per_query[q] = []
        continue
    dates = df["date"].dropna().unique().tolist() if "date" in df.columns else []
    per_query[q] = dates
    all_research_dates.update(dates)

print("=== 逐查询日返回的调研日期 ===")
for q, dates in per_query.items():
    if isinstance(dates, str):
        print(f"  {q}: {dates}")
    elif dates:
        print(f"  {q}: {dates}")
    else:
        print(f"  {q}: (空)")

print("\n=== 目标三日是否出现 ===")
for d in sorted(GAP_DATES):
    present = d in all_research_dates
    print(f"  {d}: {'出现' if present else '未出现(源侧空洞)'}")

print(f"\n并集覆盖的调研日期总数: {len(all_research_dates)}")
print(f"并集日期范围: {min(all_research_dates)} .. {max(all_research_dates)}"
      if all_research_dates else "无")
