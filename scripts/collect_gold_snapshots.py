#!/usr/bin/env python
"""更新 tests/snapshots/ 中的黄金相关 parquet 快照文件。

用法:
    python scripts/collect_gold_snapshots.py

数据源: akshare（spot_golden_benchmark_sge, spot_hist_sge 等）
输出:   tests/snapshots/*.parquet

建议定期运行（如每周或每次 akshare 升级后），保持快照新鲜度。
test_gold_snapshots.py 依赖这些快照做数据契约测试。
"""

import os
import sys
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

# 确保项目根目录在 path 中
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

SNAPSHOT_DIR = os.path.join(project_root, "tests", "snapshots")
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

COLLECTORS = {
    "sge_benchmark": lambda: _collect_sge_benchmark(),
    "sge_hist_au9999": lambda: _collect_sge_hist(),
    "bond_yields_3y": lambda: _collect_via_gold_utils("fetch_bond_yields", years=3),
    "china_cpi": lambda: _collect_via_gold_utils("fetch_china_cpi"),
    "china_reserve_data": lambda: _collect_via_gold_utils("fetch_china_reserve_data"),
    "comex_inventory": lambda: _collect_via_gold_utils("fetch_comex_inventory"),
    "global_etf_holdings_2y": lambda: _collect_via_gold_utils("fetch_global_etf_holdings", years=2),
    "usdcny_hist": lambda: _collect_via_gold_utils("fetch_usdcny_hist", symbol="USDCNH"),
}


def _collect_sge_benchmark():
    import akshare as ak
    df = ak.spot_golden_benchmark_sge()
    if df is not None and not df.empty:
        df.columns = [c.strip() for c in df.columns]
        for c in df.columns:
            if "日期" in c or "date" in c.lower() or "交易时间" in c:
                df = df.rename(columns={c: "date"})
    return df


def _collect_sge_hist():
    import akshare as ak
    return ak.spot_hist_sge(symbol="Au99.99")


def _collect_via_gold_utils(func_name, **kwargs):
    from tabs.gold_components.gold_utils import fetch_bond_yields, fetch_china_cpi, \
        fetch_china_reserve_data, fetch_comex_inventory, fetch_global_etf_holdings, fetch_usdcny_hist
    func_map = {
        "fetch_bond_yields": fetch_bond_yields,
        "fetch_china_cpi": fetch_china_cpi,
        "fetch_china_reserve_data": fetch_china_reserve_data,
        "fetch_comex_inventory": fetch_comex_inventory,
        "fetch_global_etf_holdings": fetch_global_etf_holdings,
        "fetch_usdcny_hist": fetch_usdcny_hist,
    }
    func = func_map[func_name]
    return func(**kwargs)


def main():
    print(f"Snapshot dir: {SNAPSHOT_DIR}")
    print("=" * 60)

    ok_count = 0
    fail_count = 0

    for name, collector in COLLECTORS.items():
        try:
            df = collector()
            if df is not None and not df.empty:
                path = os.path.join(SNAPSHOT_DIR, f"{name}.parquet")
                df.to_parquet(path, index=False)
                latest = df["date"].iloc[-1] if "date" in df.columns else "N/A"
                print(f"  OK: {name:30s} {len(df):>5d} rows, latest={latest}")
                ok_count += 1
            else:
                print(f"  SKIP: {name:30s} (empty result)")
                fail_count += 1
        except Exception as e:
            print(f"  FAIL: {name:30s} {e}")
            fail_count += 1

    print("=" * 60)
    print(f"Result: {ok_count} updated, {fail_count} failed")


if __name__ == "__main__":
    main()
