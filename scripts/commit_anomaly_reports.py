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
import subprocess
import sys
from datetime import datetime

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

    if do_commit and (added or updated or removed):
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
        print("[anomaly-archive] 无变更，未提交")
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
