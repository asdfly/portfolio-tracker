#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
归档异常运行报告到 git（防 nightly churn 的精准方案）。

背景
----
data/reports/run_report_<date>.json 是 nightly 管线每晚重写的机器可读运行账本。
若全部纳入版本控制，会产生持续 git churn。但 2026-09-15 事故复盘表明：
当管线 rc=1 却写出 dq_score=100/alerts=[] 时，run_report 是唯一客观物证。
因此采用「默认忽略 + 仅异常纳入」策略：

  - .gitignore 用 `data/reports/*` 默认忽略整目录；
  - 本脚本扫描全部 run_report，对「异常」报告执行 `git add -f`（强制突破忽略），
    对恢复正常的报告若曾被跟踪则 `git rm --cached` 退回忽略。

异常判定（基于报告权威字段，非 alerts 数量——alerts 多为良性源超时告警）
--------------------------------------------------------------------------
  run_status in {degraded, failed}  -> 异常
  dq_score is None                  -> 异常（数据质量破损，无评分）
  其余（run_status=ok/缺失 且 dq_score 有值）-> 正常，保持忽略

用法
----
  python scripts/commit_anomaly_reports.py            # 仅调整索引(stage/unstage)，不提交
  python scripts/commit_anomaly_reports.py --commit   # 有变更时自动本地提交（nightly 接入用）
  python scripts/commit_anomaly_reports.py --commit --push  # 提交后推 origin/master（2026-10-08 授权）

推送策略
--------
- 默认绝不 push（无授权不 push 铁律）。
- `--push` 为显式授权：仅在本脚本本次实际产生了提交(新增/更新/退回异常报告)后，
  才 `git push origin master`；无变更则不推送、不报错。
- nightly 自动化 `818293dc` 已用 `--commit --push`，使异常报告物证同步到远程，
  避免本地丢失(2026-09-15 事故教训：仅本地物证不可靠)。
"""
import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(REPO, "data", "reports")


def is_anomaly(report: dict):
    """返回 (是否异常, 原因)。"""
    st = report.get("run_status")
    if st in ("degraded", "failed"):
        return True, f"run_status={st}"
    if report.get("dq_score") is None:
        return True, "dq_score=null"
    return False, ""


def _git(*args):
    return subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True
    )


def _is_tracked(rel_path: str) -> bool:
    r = _git("ls-files", "--error-unmatch", rel_path)
    return r.returncode == 0


# P1 推进（合并而非重建）：run_sentinel 已在 16:30 独立调度兜底"应产出未产出"硬告警
# （execution_logs id=650 实证 2026-10-23 命中 sentinel_run_report_missing）。
# 归档层不重建交易日历缺口检测，仅只读消费哨兵已写出的缺口信号，在归档输出二次暴露，
# 并对 sentinel 曾标记缺失的日期做"是否已产出报告物证"的解析追踪。
# DB 只读（mode=ro）；任何异常都不影响归档主流程（哨兵若不可用，归档照常）。
SENTINEL_GAP_TASKS = (
    "sentinel_run_report_missing",
    "sentinel_run_report_corrupt",
    "sentinel_snapshot_degraded",
    "sentinel_snapshot_orphan",
    "sentinel_snapshot_marker_mismatch",
    "sentinel_index_quotes_degraded",
)


def _consume_sentinel_signals(window_days=30, db_path=None):
    """只读消费 run_sentinel 已写入 execution_logs 的缺口信号。

    返回 list[dict]: {ts, task, status, detail, date}
      - date: 从 message 正则提取的业务日期(YYYY-MM-DD)，用于与归档扫描交叉核对。
      - 空 list 表示近窗口内无哨兵缺口信号（或 DB/查询不可用，已静默降级）。
    """
    import sqlite3
    db = db_path or os.environ.get("ARCHIVE_SENTINEL_DB") or os.path.join(
        REPO, "data", "database", "portfolio.db")
    if not os.path.exists(db):
        return []
    since = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            placeholders = ",".join("?" * len(SENTINEL_GAP_TASKS))
            q = (f"SELECT created_at, task_name, status, message FROM execution_logs "
                 f"WHERE task_name IN ({placeholders}) AND created_at >= ? "
                 f"ORDER BY created_at DESC")
            for ts, task, status, msg in cur.execute(q, (*SENTINEL_GAP_TASKS, since)).fetchall():
                m = re.search(r"(\d{4}-\d{2}-\d{2})", msg or "")
                rows.append({"ts": ts, "task": task, "status": status,
                             "detail": msg or "", "date": m.group(1) if m else None})
        finally:
            con.close()
    except Exception as exc:
        print(f"[anomaly-archive] 消费 sentinel 信号失败（不影响归档）: {exc}")
        return []
    return rows


def main():
    do_commit = "--commit" in sys.argv
    do_push = "--push" in sys.argv
    pattern = os.path.join(REPORTS_DIR, "run_report_*.json")
    files = sorted(glob.glob(pattern))

    added, removed, updated = [], [], []
    for f in files:
        rel = os.path.relpath(f, REPO)
        try:
            with open(f, encoding="utf-8") as fh:
                report = json.load(fh)
        except Exception as e:  # 解析失败也视为物证，强制纳入
            if not _is_tracked(rel):
                _git("add", "-f", rel)
                added.append((rel, f"PARSE_ERROR:{e}"))
            else:
                _git("add", rel)
                updated.append((rel, "PARSE_ERROR:updated"))
            continue

        anom, why = is_anomaly(report)
        if anom:
            if not _is_tracked(rel):
                _git("add", "-f", rel)
                added.append((rel, why))
            else:
                _git("add", rel)  # 已跟踪则更新内容
                updated.append((rel, why))
        else:
            if _is_tracked(rel):
                _git("rm", "--cached", rel)
                removed.append(rel)
            # 未跟踪的正常报告：保持忽略，不动

    # 汇总输出
    print(f"[anomaly-archive] 扫描 {len(files)} 份 run_report")
    print(f"  纳入(新增/更新): {len(added) + len(updated)}  退回忽略: {len(removed)}")
    for rel, why in added:
        print(f"  + {rel}  [{why}]")
    for rel, why in updated:
        print(f"  ~ {rel}  [{why}]")
    for rel in removed:
        print(f"  - {rel}  (恢复正常, 退回忽略)")

    # P2#1 标注：若当日 run_report 尚未产出（早于 15:30 管线窗口），显式说明
    # 本次"0 新增"是时序使然而非"无新增异常"，避免 06:01 等早班运行被误读。
    # 以"当日 run_report 文件是否存在"判定，对节假日/管线延迟同样鲁棒。
    today = datetime.now().strftime("%Y-%m-%d")
    today_report = os.path.join(REPORTS_DIR, f"run_report_{today}.json")
    if not os.path.exists(today_report):
        print(
            f"[anomaly-archive] 注：当日 run_report_{today}.json 尚未产出"
            f"（早于 15:30 管线窗口），本次仅归档历史异常证据；"
            f"当日报告留待晚间运行归档，勿将上方\"0 新增\"误读为\"无新增异常\"。"
        )

    # P1 推进（合并而非重建）：消费 run_sentinel 已写出的缺口信号，归档层二次暴露 + 解析追踪。
    # 不重建交易日历缺口检测（run_sentinel 已在 16:30 独立兜底，id=650 实证命中）。
    if "--no-sentinel" not in sys.argv:
        sent_signals = _consume_sentinel_signals()
        if sent_signals:
            print("\n[anomaly-archive] === 运行后哨兵缺口信号（归档层二次暴露，近30日）===")
            flagged_dates = set()
            for s in sent_signals:
                bdate = s["date"]
                if bdate:
                    flagged_dates.add(bdate)
                rep_exists = bool(bdate) and os.path.exists(
                    os.path.join(REPORTS_DIR, f"run_report_{bdate}.json"))
                if rep_exists:
                    tag = ("✓报告在/数据质量缺口" if s["task"].startswith("sentinel_snapshot")
                           or s["task"].startswith("sentinel_index_quotes")
                           else "✓已产出物证")
                else:
                    tag = "✗仍缺物证" if bdate else "—无日期"
                print(f"  - {s['ts']} [{s['task']}] {s['status']} "
                      f"日期={bdate or '?'} {tag}")
                d = s["detail"]
                if d and len(d) > 140:
                    d = d[:140] + "…"
                if d:
                    print(f"      {d}")
            still_missing = sorted(
                d for d in flagged_dates
                if not os.path.exists(os.path.join(REPORTS_DIR, f"run_report_{d}.json")))
            if still_missing:
                print(f"  >> sentinel 曾报缺失但归档扫描仍未发现报告物证的日期: "
                      f"{', '.join(still_missing)}")
                print("     （缺口硬告警由 run_sentinel 兜底，本归档层仅二次暴露，不重建检测）")
            else:
                print("  >> 近30日 sentinel 标记日期均已产出报告物证，归档扫描与哨兵信号一致")
        else:
            print("\n[anomaly-archive] 运行后哨兵缺口信号：近30日无 (run_sentinel 未报缺口)")
    else:
        print("\n[anomaly-archive] 已跳过 sentinel 信号消费（--no-sentinel）")

    # #7 修复：仅当索引确有实质变更才提交，避免对已跟踪但未改内容的报告产生空提交
    # / "提交失败"误报（pre-commit 钩子对空提交报"提交失败"，实为无内容可提交）。
    # `git diff --cached --quiet` 返回 0 表示索引相对 HEAD 无差异（无 diff），非 0 表示有差异。
    has_staged_diff = do_commit and _git("diff", "--cached", "--quiet").returncode != 0
    if has_staged_diff:
        parts = []
        if added:
            parts.append(f"新增异常{len(added)}")
        if updated:
            parts.append(f"更新{len(updated)}")
        if removed:
            parts.append(f"退回忽略{len(removed)}")
        msg = (
            f"chore(reports): 异常运行报告归档 {datetime.now():%Y-%m-%d %H:%M} "
            f"({'/'.join(parts)})"
        )
        r = _git("commit", "-m", msg)
        if r.returncode == 0:
            print(f"[anomaly-archive] 已提交: {msg}")
            committed = True
        else:
            print(f"[anomaly-archive] 提交失败: {r.stderr.strip()}")
            committed = False
    elif do_commit:
        print("[anomaly-archive] 索引无实质变更，未提交（与已跟踪内容一致）")
        committed = False
    else:
        committed = False

    if do_push:
        if committed:
            p = _git("push", "origin", "master")
            if p.returncode == 0:
                print("[anomaly-archive] 已推送 origin/master")
            else:
                print(f"[anomaly-archive] 推送失败: {p.stderr.strip()}")
        else:
            print("[anomaly-archive] 本次无提交，跳过推送")


if __name__ == "__main__":
    main()
