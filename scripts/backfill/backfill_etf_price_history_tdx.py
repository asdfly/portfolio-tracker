#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
经 tdx-connector (通达信) 回填 / 维护 etf_price_history（ETF 日频 OHLCV）。

背景：
- 原 westock 路径（backfill_etf_price_history_westock.py）在 2026-10-09 自动运行中
  触发 westock-mcp 硬性配额限频，仅 3/9 成功；剩余 6 只改用已验证的 tdx 备用源
  （见 docs/westock_alt_source_evaluation_20261009.md）。本脚本与 westock 版同构，
  仅数据源与字段映射不同。
- 取数由 WB 自动化调 tdx-connector 完成，落 JSON（etf_tdx_<CODE>_latest.json）后
  本脚本消费；本脚本只读 JSON 并幂等 upsert，不负责取数。

字段映射（tdx 前复权日K）：
- date = Data(YYYYMMDD)，已由生成器规整为 YYYY-MM-DD
- close = Close，adj_close = Close（tdx 前复权即前复权）
- volume = Volume：单位「手」（tdx 已按 Unit 除好，最终可用值，禁再换算）
- amount = Amount：单位「元」
- source = 'tdx'

与 westock 版对称：下游读 etf_price_history 不按 source 过滤，故 tdx 行写入且日期
更新后即自动成为 em/tx 失败时的兜底（price_history.py 兜底日志已同步识别 'tdx'）。
幂等：INSERT OR REPLACE，重复运行安全。

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_etf_price_history_tdx.py \
      --dir scripts/backfill/data --pattern "etf_tdx_*_latest.json" --verify
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

SOURCE = "tdx"
_COLS = ["date", "code", "open", "high", "low", "close", "volume", "amount", "adj_close", "source"]


def _f(v):
    """数值安全转换：NaN/Inf/None -> None（sqlite 存 NULL），其余 -> float。"""
    try:
        fv = float(v)
        return fv if math.isfinite(fv) else None
    except (TypeError, ValueError):
        return None


def upsert_from_file(path, code, conn, verify=False):
    """读取一支 ETF 的 tdx JSON（rows[]），幂等 upsert 入 etf_price_history。返回写入行数。"""
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    # 兼容两种格式：生成器写的 {rows:[...]} 或裸 [{...}]
    rows_src = j.get("rows") if isinstance(j, dict) else j
    if not rows_src:
        print(f"[WARN] {code}: 空数据，跳过")
        return 0

    # verify：upsert 前先抓现有重叠日，用于交叉校验（避免「写完即一致」的回环）
    pre = {}
    if verify:
        for r in conn.execute(
            "SELECT date,close,volume FROM etf_price_history WHERE code=?", (code,)
        ).fetchall():
            pre[r[0]] = (r[1], r[2])

    out = []
    for n in rows_src:
        try:
            d = str(n["date"])
            o = float(n["open"])
            c = float(n["close"])
            h = float(n["high"])
            l = float(n["low"])
            v = float(n.get("volume") or 0)
            a = _f(n.get("amount"))
        except (KeyError, TypeError, ValueError) as e:
            print(f"[SKIP] {code} {n}: {e}")
            continue
        out.append((d, code, o, h, l, c, v, a, c, SOURCE))

    if not out:
        print(f"[WARN] {code}: 无有效行，跳过")
        return 0

    conn.executemany(
        """
        INSERT OR REPLACE INTO etf_price_history
            (date, code, open, high, low, close, volume, amount, adj_close, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        out,
    )
    conn.commit()

    if verify and pre:
        mism = []
        for (d, _c2, _o, _h, _l, c, v, _a, _ac, _s) in out:
            if d in pre:
                pc, pv = pre[d]
                close_diff = abs((pc or 0) - (c or 0))
                vol_diff = abs((pv or 0) - (v or 0))
                if close_diff > 1e-6 or vol_diff > 1:
                    mism.append((d, pc, c, pv, v))
        if mism:
            print(f"[VERIFY][WARN] {code}: {len(mism)} 行与现有 DB 重叠日不一致"
                  f"（可能为陈旧/错误行，已用 tdx 覆盖）:")
            for m in mism[:5]:
                print(f"    {m[0]} db_close={m[1]} tdx_close={m[2]} db_vol={m[3]} tdx_vol={m[4]}")
        else:
            print(f"[VERIFY][OK] {code}: 重叠日与现有 DB 完全一致")

    print(f"[OK] {code}: upsert {len(out)} 行（{out[0][0]}~{out[-1][0]}）")
    return len(out)


def _code_from_filename(base):
    """etf_tdx_159949.json -> 159949 （兼容 etf_tdx_159949_latest.json -> 159949）"""
    name = base
    if name.startswith("etf_tdx_"):
        name = name[len("etf_tdx_"):]
    if name.endswith(".json"):
        name = name[:-len(".json")]
    if name.endswith("_latest"):
        name = name[:-len("_latest")]
    return name


def main():
    p = argparse.ArgumentParser(description="经 tdx-connector 回填 etf_price_history（ETF OHLCV）")
    p.add_argument("--dir", required=True, help="含 etf_tdx_*.json 的目录")
    p.add_argument("--pattern", default="etf_tdx_*.json", help="文件名通配（默认 etf_tdx_*.json）")
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
