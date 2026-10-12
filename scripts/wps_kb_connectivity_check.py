#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WPS 知识库连通性自检（轻量、零依赖、纯标准库）。

设计说明
--------
WPS 知识库的实际上传依赖会话内注入的 MCP 工具 mcp__wps-knowledgebase__*。
连接器在本地 plugin-account-state 里可能标记为 enabled=true，
但平台未必把它的工具加载进当前会话（本工程 2026-10-10 实测即如此：
enabled=true 但调用报 "not found"）。这类"假连通"只能靠运行期 MCP 探针发现。

本脚本做两件事：
  1) 本地侧：读取 connector 的 enabled 标记 + 列出待补导报告；
  2) 运行期侧：读取最近一次 MCP 探针结果（data/reports/_wps_health.jsonl），
     给出"是否真的可调用"的结论与处置建议。

运行期探针由自动化「WPS 知识库连通性自检」执行（尝试调用
mcp__wps-knowledgebase__get_knowledge_base_list，捕获 not found），并写入 _wps_health.jsonl。
本脚本不调用 MCP，仅消费探针结果，故可随时本地运行做静态核验。

数据纪律：所有结论标注来源；无法核实标 "—"，严禁编造。
"""

import glob
import json
import os
import sys
from datetime import datetime, timezone, timedelta

TZ = timezone(timedelta(hours=8))  # GMT+8

REPORTS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "reports")
)
ACCOUNT_STATE_GLOB = os.path.join(
    os.path.expanduser("~"), ".workbuddy", "plugin-account-state", "*.json"
)
WPS_PLUGIN_KEY = "wps-knowledgebase@workbuddy-connector-plugins-official"
HEALTH_LOG = os.path.join(REPORTS_DIR, "_wps_health.jsonl")
PUSH_LOG = os.path.join(REPORTS_DIR, "_kb_push_log.jsonl")


def load_account_state():
    for path in sorted(glob.glob(ACCOUNT_STATE_GLOB)):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:  # noqa: BLE001
            continue
        plugins = data.get("plugins", {})
        if WPS_PLUGIN_KEY in plugins:
            return plugins[WPS_PLUGIN_KEY], path
    return None, None


def last_health():
    if not os.path.exists(HEALTH_LOG):
        return None
    last = None
    try:
        with open(HEALTH_LOG, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    last = json.loads(line)
                except Exception:  # noqa: BLE001
                    continue
    except Exception:  # noqa: BLE001
        return None
    return last


def pending_wps():
    pending = []
    if not os.path.exists(PUSH_LOG):
        return pending
    with open(PUSH_LOG, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if rec.get("target") == "wps_knowledgebase" and rec.get("status") != "ok":
                pending.append(rec)
    return pending


def all_reports():
    if not os.path.isdir(REPORTS_DIR):
        return []
    return sorted(
        f
        for f in os.listdir(REPORTS_DIR)
        if f.startswith("组合大盘综合视角_") and f.endswith(".html")
    )


def main():
    acct, acct_path = load_account_state()
    enabled = acct.get("enabled") if acct else "—"
    health = last_health()
    pending = pending_wps()
    reports = all_reports()

    # 运行期探针结论
    if health is None:
        rt_status = "NO_PROBE_YET"
        rt_msg = "尚无运行期 MCP 探针记录；请运行自动化「WPS 知识库连通性自检」或手动调用 mcp__wps-knowledgebase__get_knowledge_base_list 探测"
    else:
        rt_status = health.get("status", "—")
        rt_msg = health.get("detail", "")

    callable_ok = rt_status == "ok"

    print("=" * 60)
    print("WPS 知识库连通性自检")
    print("=" * 60)
    print(f"[1] 本地连接器配置 (plugin-account-state):")
    print(f"    wps-knowledgebase.enabled = {enabled}   ({'账号已授权' if enabled is True else '未授权/未知'})")
    if acct_path:
        print(f"    来源: {acct_path}")
    print()
    print(f"[2] 运行期 MCP 工具探测 (data/reports/_wps_health.jsonl 最近一条):")
    if health:
        print(f"    探测时间: {health.get('ts', '—')}")
    print(f"    结果: {rt_status}")
    if rt_msg:
        print(f"    说明: {rt_msg}")
    print()

    verdict = "OK" if (enabled is True and callable_ok) else (
        "PENDING_ENABLE" if enabled is not True else "NOT_CALLABLE"
    )
    print(f"[3] 综合判定: {verdict}")
    print(f"    本地 enabled={enabled} / 运行期可调用={callable_ok}")
    print()

    print(f"[4] 待补导报告 (WPS 线尚未成功): 共 {len(pending)} 条")
    for p in pending[:20]:
        print(f"    - {p.get('report')}  [{p.get('status')}] {p.get('ts','')}")
    if not pending:
        print("    （_kb_push_log.jsonl 中无 WPS 失败记录；如需全量补导请见主日报自动化步4）")
    print()

    print("[5] 处置建议:")
    if verdict == "OK":
        print("    WPS 知识库已可调用，主日报自动化步4会自动补导待推送报告。")
    elif verdict == "NOT_CALLABLE":
        print("    连接器 enabled=true 但工具未注入本会话（平台层工具加载问题，非授权问题）。")
        print("    建议：在客户端左下角「+」菜单断开并重连 WPS 知识库，或等待平台刷新工具索引；")
        print("    工具可用后主日报自动化步4 WPS 分支会自动补导，亦可手动重跑本自检附带的补导。")
    else:  # PENDING_ENABLE
        print("    WPS 知识库本地未启用，请先在客户端左下角「+」菜单启用后再试。")
    print("=" * 60)

    out = {
        "tool": "wps_kb_connectivity_check",
        "checked_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "local_enabled": enabled,
        "runtime_callable": callable_ok,
        "runtime_status": rt_status,
        "runtime_detail": rt_msg,
        "verdict": verdict,
        "pending_wps_count": len(pending),
        "pending_wps": pending[:20],
        "total_reports": len(reports),
    }
    print("\nJSON:")
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if verdict == "OK" else 2


if __name__ == "__main__":
    sys.exit(main())
