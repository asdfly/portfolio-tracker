#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fallback fetcher for etf_price_history backfill.

westock-mcp data_kline is unavailable in this automation session (not connected to
the deferred tool index), so we use akshare's Sina ETF history endpoint, which is
the project's own documented backup source and avoids the East Money push2his RST.

Empirically verified against the westock reference sample (etf_westock_510300.json)
that akshare-Sina yields bit-identical OHLC, and:
  - volume (Sina, in shares) == westock volume (in lots) * 100  -> divide by 100
  - amount  (Sina, in yuan)   == round(westock amount / 10000) * 10000 -> round to 万
So the emitted JSON is byte-compatible with westock-mcp data_kline output.

Rate-limited fetch in two rounds: round 1 quick-scans every code once (fast verdict on
whole-network health), round 2 re-tries only the round-1 failures with the remaining
attempt budget (default 5 total per code, was 8 in-place retries; worst case for all 23
codes down drops from ~43min to ~22min). NODATA (empty response, or no nodes in window,
e.g. holiday/unlisted) is deterministic and not retried; only transient SKIP
(network/exception) re-tries.
Writes etf_westock_<CODE6>.json (idempotent), one file per ETF, as the backfill expects.
"""
import sys
import os
import time
import json
import argparse
from datetime import date

import akshare as ak

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTDIR = os.path.join(ROOT, "scripts", "backfill", "data")

# (prefix+code) as given by the task
CODES = [
    "sz159220", "sz159267", "sz159300", "sz159650", "sz159732", "sz159770",
    "sz159796", "sz159819", "sz159949", "sz159992", "sh510300", "sh510500",
    "sh511380", "sh511520", "sh512010", "sh512100", "sh512810", "sh515010",
    "sh515120", "sh516160", "sh561910", "sh563020", "sh588000",
]

START = "2026-09-20"
END = "2026-10-09"
MAX_ATTEMPTS = 5   # 每码总尝试预算（跨轮分配；原 8 次原地重试，全 23 码挂机最坏 ~43min 超预算）
ROUNDS = 2         # 轮数：第 1 轮快扫（每码 1 次，快速判定全网状态），第 2 轮聚焦重试第一轮失败码
ROUND_SLEEP = 20.0  # 轮间冷却秒数


def norm_date(s):
    s = str(s).strip()
    if "/" in s:
        y, m, d = s.split("/")
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return s[:10]


def fetch_one(symbol, max_attempts):
    """单码取数，最多 max_attempts 次尝试，12~15s 退避。返回 (df, err)。

    三态：df 非空=成功；err 以 "NODATA:" 开头=确定性无数据（空响应，重试无意义）；
    其余 err=瞬时失败（网络/异常），调用方应安排重试。
    """
    last_err = None
    for attempt in range(max_attempts):
        try:
            df = ak.fund_etf_hist_sina(symbol=symbol)
            if df is None or len(df) == 0:
                return None, "NODATA:empty response"
            return df, None
        except Exception as e:  # noqa: BLE001
            last_err = repr(e)
            if attempt < max_attempts - 1:
                time.sleep(12 + (attempt % 4))  # 12~15s backoff
    return None, last_err


def main():
    ap = argparse.ArgumentParser(description="akshare-Sina fallback fetcher for etf_price_history backfill")
    ap.add_argument("--codes", default=",".join(CODES),
                    help="逗号分隔的 prefix+code 列表，默认全量 23 只")
    ap.add_argument("--start", default=START, help=f"起始日(含)，默认 {START}")
    ap.add_argument("--end", default=date.today().isoformat(),
                    help="截止日(含)，默认今天")
    ap.add_argument("--sleep", type=float, default=2.0, help="每只之间的间隔秒数，默认 2")
    ap.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS,
                    help=f"每码总尝试预算(跨轮分配)，默认 {MAX_ATTEMPTS}（原 8 次原地重试）")
    ap.add_argument("--rounds", type=int, default=ROUNDS,
                    help=f"轮数：第 1 轮快扫(每码 1 次)，后续轮聚焦重试失败码，默认 {ROUNDS}")
    ap.add_argument("--round-sleep", type=float, default=ROUND_SLEEP,
                    help=f"轮间冷却秒数，默认 {ROUND_SLEEP}")
    args = ap.parse_args()

    symbols = [s.strip() for s in args.codes.split(",") if s.strip()]
    start, end = args.start, args.end

    results = {}
    pending = symbols
    for rnd in range(1, args.rounds + 1):
        if not pending:
            break
        # 预算分配：第 1 轮快扫每码 1 次（快速判定全网状态）；第 2 轮均分剩余预算
        per_code = 1 if rnd == 1 else max(1, args.max_attempts - 1)
        if rnd > 1:
            print(f"[ROUND {rnd}] 聚焦重试 {len(pending)} 只失败码（每码 {per_code} 次尝试"
                  f"（总预算 {args.max_attempts}，第 1 轮已用 1），冷却 {args.round_sleep}s）")
            time.sleep(args.round_sleep)
        still_pending = []
        for sym in pending:
            code6 = sym[2:]  # strip sz/sh/bj
            df, err = fetch_one(sym, per_code)
            if df is None:
                if err and err.startswith("NODATA:"):
                    # 空响应=确定性无数据（未上市/停牌/接口无该 symbol），重试无意义
                    print(f"[SKIP] {sym}: {err}")
                    results[sym] = "NODATA"
                else:
                    print(f"[SKIP] {sym}: {err}")
                    results[sym] = "SKIP"
                    still_pending.append(sym)  # 瞬时失败，进下一轮聚焦重试
                time.sleep(args.sleep)
                continue

            nodes = []
            for _, row in df.iterrows():
                d = norm_date(row["date"])
                if d < start or d > end:
                    continue
                vol = row.get("volume")
                amt = row.get("amount")
                nodes.append({
                    "date": d,
                    "open": float(row["open"]),
                    "last": float(row["close"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "volume": round(float(vol) / 100.0) if vol is not None else 0,
                    "amount": (round(float(amt) / 10000.0) * 10000) if amt is not None else None,
                })

            if not nodes:
                # 窗口内无节点是确定性结果（如节假日未上市/无行情），重试无意义，不进下一轮
                print(f"[SKIP] {sym}: no rows in [{start},{end}]")
                results[sym] = "NODATA"
                time.sleep(args.sleep)
                continue

            out = {"ok": True, "data": {"nodes": nodes}}
            path = os.path.join(OUTDIR, f"etf_westock_{code6}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(out, fh, ensure_ascii=False)
            print(f"[OK] {sym} ({code6}): {len(nodes)} nodes, max={nodes[-1]['date']}")
            results[sym] = "OK"
            time.sleep(args.sleep)  # gentle between codes
        pending = still_pending

    ok = [k for k, v in results.items() if v == "OK"]
    bad = {k: v for k, v in results.items() if v != "OK"}
    print(f"\nSUMMARY ok={len(ok)} bad={len(bad)}")
    if bad:
        print("BAD:", bad)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
