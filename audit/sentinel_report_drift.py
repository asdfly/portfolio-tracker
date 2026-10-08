# -*- coding: utf-8 -*-
"""只读哨兵 A：监测报告库漂移 DIFFER 份数「只增不减」。

判据严格沿用 08_report_library_drift.md / 探针 probe_report_drift_nowcast_20261008.py
的精确口径（阈值 0.51、精确正则，避免 #1a73e8 误命中）：
  - SQLite 以 file:...?mode=ro 只读连接
  - HTML 仅做文本读取（前 30000 字节）
  - 状态仅持久化到本脚本同目录的 .sentinel_state.json（不碰生产库、不碰 data/reports）
  - 当轮 DIFFER 份数 > 上次记录值 时，判定「漂移扩大」，写告警日志并 exit 1

退出码：0 = 正常（DIFFER 未增或首跑基线初始化）/ 1 = 漂移扩大告警。

用法（绝对路径，venv 解释器）：
  D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker/venv313/Scripts/python.exe \
      D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker/audit/sentinel_report_drift.py
"""

import os
import re
import sys
import json
import sqlite3
import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB = "file:data/database/portfolio.db?mode=ro"
REPORTS = os.path.join(ROOT, "data", "reports")
STATE = os.path.join(HERE, ".sentinel_state.json")
ALERT_LOG = os.path.join(HERE, "sentinel_alerts.log")
THRESHOLD = 0.51  # HTML 为整数千分位、库内 2 位小数 ⇒ 真实误差只来自四舍五入

VAL_RE = re.compile(
    r'总市值</div><div class="v"[^>]*>\s*[¥￥]?\s*([0-9][0-9,]*(?:\.[0-9]+)?)'
)
DATE_RE = re.compile(r"enhanced_report_(\d{8})\.html")


def compute() -> dict:
    """严格只读：生产库 mode=ro，HTML 文本读取。返回三态计数。"""
    conn = sqlite3.connect(DB, uri=True)
    try:
        base = dict(conn.execute(
            "SELECT date, total_value FROM portfolio_summary"
        ).fetchall())
    finally:
        conn.close()

    count = {"MATCH": 0, "DIFFER": 0, "NO_DB_ROW": 0, "NO_HEADER": 0}
    if not os.path.isdir(REPORTS):
        return count
    for f in os.listdir(REPORTS):
        m = DATE_RE.fullmatch(f)
        if not m:
            continue
        raw = m.group(1)
        d = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
        path = os.path.join(REPORTS, f)
        with open(path, encoding="utf-8", errors="replace") as fh:
            head = fh.read(30000)
        vm = VAL_RE.search(head)
        html_val = None if vm is None else float(vm.group(1).replace(",", ""))
        db_val = base.get(d)
        if html_val is None:
            v = "NO_HEADER"
        elif db_val is None:
            v = "NO_DB_ROW"
        elif abs(html_val - db_val) < THRESHOLD:
            v = "MATCH"
        else:
            v = "DIFFER"
        count[v] += 1
    return count


def main() -> int:
    count = compute()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur_differ = count["DIFFER"]

    prev = None
    if os.path.exists(STATE):
        try:
            with open(STATE, encoding="utf-8") as fh:
                prev = json.load(fh)
        except Exception:
            prev = None
    prev_differ = prev.get("DIFFER") if isinstance(prev, dict) else None

    alert = (prev_differ is not None and cur_differ > prev_differ)

    state = {
        "checked_at": now,
        "total_html": sum(count.values()),
        "MATCH": count["MATCH"],
        "DIFFER": cur_differ,
        "NO_DB_ROW": count["NO_DB_ROW"],
        "NO_HEADER": count["NO_HEADER"],
        "prev_DIFFER": prev_differ,
        "alert": alert,
    }
    with open(STATE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)

    print(f"[{now}] 报告库漂移哨兵：HTML={state['total_html']} "
          f"MATCH={count['MATCH']} DIFFER={cur_differ} "
          f"NO_DB_ROW={count['NO_DB_ROW']} NO_HEADER={count['NO_HEADER']}")

    if prev_differ is None:
        print(f"  基线已初始化（首跑无历史对比）。下次 DIFFER 基线 = {cur_differ}。")
        return 0
    if alert:
        print(f"  [ALERT] DIFFER 由 {prev_differ} 升至 {cur_differ} "
              f"(+{cur_differ - prev_differ})，历史被再次改写！")
        with open(ALERT_LOG, "a", encoding="utf-8") as fh:
            fh.write(f"{now} ALERT DIFFER {prev_differ}->{cur_differ}\n")
        return 1
    print(f"  [OK] DIFFER 未增（基线 {prev_differ}），无新增漂移。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
