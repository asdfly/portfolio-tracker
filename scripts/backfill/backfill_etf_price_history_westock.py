#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
经 westock-mcp data_kline 回填 / 维护 etf_price_history（ETF 日频 OHLCV）。

背景：
- portfolio 主取数（em/tx via 东财 push2his）在本机被 host 级 RST，导致 9 只行业 ETF
  长期卡在最后成功日（如 2026-09-21），实时增量补数也因同一 RST 失败、缺口跨日累积。
- westock-mcp（腾讯自选股 data_kline）经实测可返回 ETF 完整日K（收盘字段名 last），
  且与 etf_price_history 既有数据逐日精确吻合（验证 510300：09-22 close 4.613 /
  volume 6445874 完全一致），故作为「连接器 + MCP 兜底」数据源，绕开东财 RST。

本脚本与 backfill_index_quotes_westock.py 同构：只读 westock 原始 JSON 并幂等 upsert，
不负责取数（取数由 WB 自动化调 westock-mcp 完成，落 JSON 后本脚本消费）。

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_etf_price_history_westock.py \
      --dir scripts/backfill/data --pattern "etf_westock_*.json" --verify

每支 ETF 的数据文件命名 etf_westock_<CODE6>.json，内容为 westock data_kline 原始响应：
  {"ok":true,"data":{"nodes":[{"date","open","last","high","low","volume","amount"},...]}}
字段映射：close=last，adj_close=last（腾讯 qfq 即前复权），source='westock_mcp'。
volume 单位=手（与 etf_price_history 现有数据一致，禁止换算）。
幂等：INSERT OR REPLACE，重复运行安全。
"""
import argparse
import glob
import json
import math
import os
import sqlite3
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH  # noqa: E402

SOURCE = "westock_mcp"
_COLS = ["date", "code", "open", "high", "low", "close", "volume", "amount", "adj_close", "source"]


def _f(v):
    """数值安全转换：NaN/Inf/None -> None（sqlite 存 NULL），其余 -> float。"""
    try:
        fv = float(v)
        return fv if math.isfinite(fv) else None
    except (TypeError, ValueError):
        return None


def upsert_from_file(path, code, conn, verify=False):
    """读取一支 ETF 的 westock JSON，幂等 upsert 入 etf_price_history。返回写入行数。"""
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    nodes = (j.get("data") or {}).get("nodes") or []
    if not nodes:
        print(f"[WARN] {code}: 空数据，跳过")
        return 0

    # verify：upsert 前先抓现有重叠日，用于交叉校验（避免「写完即一致」的回环）
    pre = {}
    if verify:
        for r in conn.execute(
            "SELECT date,close,volume FROM etf_price_history WHERE code=?", (code,)
        ).fetchall():
            pre[r[0]] = (r[1], r[2])

    rows = []
    for n in nodes:
        try:
            d = str(n["date"])
            o = float(n["open"])
            c = float(n["last"])
            h = float(n["high"])
            l = float(n["low"])
            v = float(n.get("volume") or 0)
            a = _f(n.get("amount"))
        except (KeyError, TypeError, ValueError) as e:
            print(f"[SKIP] {code} {n}: {e}")
            continue
        rows.append((d, code, o, h, l, c, v, a, c, SOURCE))

    if not rows:
        print(f"[WARN] {code}: 无有效行，跳过")
        return 0

    conn.executemany(
        """
        INSERT OR REPLACE INTO etf_price_history
            (date, code, open, high, low, close, volume, amount, adj_close, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    if verify and pre:
        mism = []
        for (d, _c, _o, _h, _l, c, v, _a, _ac, _s) in rows:
            if d in pre:
                pc, pv = pre[d]
                close_diff = abs((pc or 0) - (c or 0))
                vol_diff = abs((pv or 0) - (v or 0))
                if close_diff > 1e-6 or vol_diff > 1:
                    mism.append((d, pc, c, pv, v))
        if mism:
            print(f"[VERIFY][WARN] {code}: {len(mism)} 行与现有 DB 重叠日不一致"
                  f"（可能为陈旧/错误行，已用 westock 覆盖）:")
            for m in mism[:5]:
                print(f"    {m[0]} db_close={m[1]} ws_close={m[2]} db_vol={m[3]} ws_vol={m[4]}")
        else:
            print(f"[VERIFY][OK] {code}: 重叠日与现有 DB 完全一致")

    print(f"[OK] {code}: upsert {len(rows)} 行（{rows[0][0]}~{rows[-1][0]}）")
    return len(rows)


def _code_from_filename(base):
    """etf_westock_159949.json -> 159949"""
    name = base
    if name.startswith("etf_westock_"):
        name = name[len("etf_westock_"):]
    if name.endswith(".json"):
        name = name[:-len(".json")]
    return name


def main():
    p = argparse.ArgumentParser(description="经 westock-mcp 回填 etf_price_history（ETF OHLCV）")
    p.add_argument("--dir", required=True, help="含 etf_westock_*.json 的目录")
    p.add_argument("--pattern", default="etf_westock_*.json", help="文件名通配（默认 etf_westock_*.json）")
    p.add_argument("--verify", action="store_true", help="交叉校验重叠日 + 缺口回填校验")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.dir, args.pattern)))
    if not files:
        print(f"[ERROR] 在 {args.dir} 下未找到匹配 {args.pattern} 的文件")
        sys.exit(1)

    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        total = 0
        codes = []
        for fp in files:
            code = _code_from_filename(os.path.basename(fp))
            codes.append(code)
            total += upsert_from_file(fp, code, conn, args.verify)

        asof = date.today().strftime("%Y-%m-%d")
        print(f"\n=== 缺口回填校验（期望 max(date) >= {asof} 或至少 2026-09-24）===")
        all_ok = True
        for code in codes:
            r = conn.execute(
                "SELECT MAX(date), COUNT(*) FROM etf_price_history WHERE code=?", (code,)
            ).fetchone()
            maxd = r[0] or ""
            ok = maxd >= "2026-09-24"
            if not ok:
                all_ok = False
            print(f"  {code}: max={maxd} rows={r[1]} [{'OK' if ok else 'GAP'}]")
        print(f"\n[DONE] 共 upsert {total} 行，{len(codes)} 只"
              + ("；全部缺口已补齐 ✅" if all_ok else "；仍有缺口 ⚠️"))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
