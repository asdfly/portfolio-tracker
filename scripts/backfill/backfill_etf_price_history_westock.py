#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
经 westock-mcp data_kline / akshare-Sina JSON 回填 / 维护 etf_price_history（ETF 日频 OHLCV）。

背景：
- portfolio 主取数（em/tx via 东财 push2his）在本机被 host 级 RST，实时增量补数因同一
  RST 失败、缺口跨日累积。
- 兜底数据源两条，均落成本脚本消费的 JSON（字段与 westock data_kline 字节兼容）：
  1) westock-mcp（腾讯自选股 data_kline，收盘字段名 last）——夜链覆盖 9 只行业 ETF；
  2) akshare `fund_etf_hist_sina`（`_akshare_fetch_westock.py` 拉取，volume=股已 ÷100 成手）
     ——每日兜底巡检对缺口/全量 23 只拉取。经实测与库内既有数据逐日精确吻合
     （10-14 全量回填 336 行，重叠日 close/volume 零偏差）。
- em/tx 双源自 2026-09-18 起完全停写，westock/akshare 是现写者。

本脚本与 backfill_index_quotes_westock.py 同构：只读 JSON 并幂等 upsert，不负责取数。

用法：
  venv313/Scripts/python.exe scripts/backfill/backfill_etf_price_history_westock.py \
      --dir scripts/backfill/data --pattern "etf_westock_*.json" --verify \
      --source akshare_sina --safe
  （--source 默认 westock_mcp；akshare-Sina 数据必须显式传 akshare_sina，见
   docs/westock_alt_source_evaluation_20261009.md 对「源标签造假」的判定。
   --pattern 传 "etf_westock_[0-9]*.json" 可排除 _latest 变体只处理 base 文件。
   --safe：重叠日与 DB 不一致时拒写该码，防源侧脏数据覆盖；默认关闭=覆盖+warn。）

每支 ETF 的数据文件命名 etf_westock_<CODE6>.json，内容为 data_kline 兼容响应：
  {"ok":true,"data":{"nodes":[{"date","open","last","high","low","volume","amount"},...]}}
字段映射：close=last，adj_close=last（qfq 前复权），source 由 --source 指定。
volume 单位=手（与 etf_price_history 现有数据一致，禁止换算）。
幂等：INSERT OR REPLACE，重复运行安全；同码多文件按节点日期排序，更新者后写（precedence 显式）。
缺口校验阈值默认取库内 market_last（>=2 码覆盖的最近交易日），不写死日期。
"""
import argparse
import glob
import json
import math
import os
import sqlite3
import sys

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


def _market_last(conn, min_codes=2):
    """库内「最近交易日」：按日聚合 >= min_codes 个不同 code 有行的最大日期。

    与 check_etf_price_gaps.py 同判据：节假日两源都不写行 ⇒ 不会把节假日当期望日。
    """
    row = conn.execute(
        """SELECT date FROM etf_price_history
           GROUP BY date HAVING COUNT(DISTINCT code) >= ?
           ORDER BY date DESC LIMIT 1""",
        (min_codes,),
    ).fetchone()
    return row[0] if row else None


def upsert_from_file(path, code, conn, verify=False, safe=False, source=SOURCE):
    """读取一支 ETF 的 JSON，幂等 upsert 入 etf_price_history。返回写入行数（safe 拒写返回 0）。"""
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    nodes = (j.get("data") or {}).get("nodes") or []
    if not nodes:
        print(f"[WARN] {code}: 空数据，跳过")
        return 0

    # verify/safe：upsert 前先抓现有重叠日，用于交叉校验（避免「写完即一致」的回环）
    pre = {}
    if verify or safe:
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
        rows.append((d, code, o, h, l, c, v, a, c, source))

    if not rows:
        print(f"[WARN] {code}: 无有效行，跳过")
        return 0

    # 重叠日不一致清单（verify 报告 / safe 拒写共用）
    mism = []
    if pre:
        for (d, _c, _o, _h, _l, c, v, _a, _ac, _s) in rows:
            if d in pre:
                pc, pv = pre[d]
                close_diff = abs((pc or 0) - (c or 0))
                vol_diff = abs((pv or 0) - (v or 0))
                if close_diff > 1e-6 or vol_diff > 1:
                    mism.append((d, pc, c, pv, v))

    if safe and mism:
        print(f"[SAFE][SKIP] {code}: {len(mism)} 行与现有 DB 重叠日不一致，--safe 模式下拒写"
              f"（疑似源侧异常值；如确认应以本文件覆盖，去掉 --safe 重跑）:")
        for m in mism[:5]:
            print(f"    {m[0]} db_close={m[1]} new_close={m[2]} db_vol={m[3]} new_vol={m[4]}")
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

    if pre:
        if mism:
            print(f"[VERIFY][WARN] {code}: {len(mism)} 行与现有 DB 重叠日不一致"
                  f"（可能为陈旧/错误行，已用新数据覆盖）:")
            for m in mism[:5]:
                print(f"    {m[0]} db_close={m[1]} new_close={m[2]} db_vol={m[3]} new_vol={m[4]}")
        else:
            print(f"[VERIFY][OK] {code}: 重叠日与现有 DB 完全一致")

    print(f"[OK] {code}: upsert {len(rows)} 行（{rows[0][0]}~{rows[-1][0]}）source={source}")
    return len(rows)


def _code_from_filename(base):
    """etf_westock_159949.json -> 159949 （兼容 etf_westock_159949_latest.json -> 159949）"""
    name = base
    if name.startswith("etf_westock_"):
        name = name[len("etf_westock_"):]
    if name.endswith(".json"):
        name = name[:-len(".json")]
    if name.endswith("_latest"):
        name = name[:-len("_latest")]
    return name


def main():
    p = argparse.ArgumentParser(description="经 westock/akshare JSON 回填 etf_price_history（ETF OHLCV）")
    p.add_argument("--dir", required=True, help="含 etf_westock_*.json 的目录")
    p.add_argument("--pattern", default="etf_westock_*.json", help="文件名通配（默认 etf_westock_*.json）")
    p.add_argument("--verify", action="store_true", help="交叉校验重叠日 + 缺口回填校验")
    p.add_argument("--safe", action="store_true",
                   help="重叠日与 DB 不一致时拒写该码（防源侧脏数据覆盖；默认关闭=覆盖+warn）")
    p.add_argument("--source", default=SOURCE,
                   help=f"写入 source 标签（默认 {SOURCE}；akshare-Sina 数据传 akshare_sina）")
    p.add_argument("--expect-max", default=None,
                   help="缺口校验期望的 max(date) 下限；默认自动取 market_last（>=2 码覆盖的最近交易日）")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.dir, args.pattern)))
    if not files:
        print(f"[ERROR] 在 {args.dir} 下未找到匹配 {args.pattern} 的文件")
        sys.exit(1)

    # 预读每文件最大节点日期：同码多文件（base 与 _latest）时更旧者先写、更新者后写，
    # 覆盖 precedence 显式化（不再依赖 glob 字典序巧合）。
    entries = []  # (code, max_node_date, path)
    for fp in files:
        code = _code_from_filename(os.path.basename(fp))
        try:
            with open(fp, encoding="utf-8") as fh:
                j = json.load(fh)
            nodes = (j.get("data") or {}).get("nodes") or []
            mx = max((str(n["date"]) for n in nodes), default="")
        except Exception as e:  # 预读失败不阻塞：按空日期排最前，upsert 时再报
            print(f"[WARN] {fp}: 预读失败（{type(e).__name__}），按原样处理")
            mx = ""
        entries.append((code, mx, fp))
    entries.sort(key=lambda t: (t[0], t[1], t[2]))
    codes = list(dict.fromkeys(c for c, _, _ in entries))  # 去重（_latest 变体曾致重复列码）

    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        total = 0
        for code, _mx, fp in entries:
            total += upsert_from_file(fp, code, conn, args.verify, args.safe, args.source)

        expect_max = args.expect_max or _market_last(conn) or ""
        print(f"\n=== 缺口回填校验（期望 max(date) >= {expect_max or 'NONE'}）===")
        all_ok = True
        for code in codes:
            r = conn.execute(
                "SELECT MAX(date), COUNT(*) FROM etf_price_history WHERE code=?", (code,)
            ).fetchone()
            maxd = r[0] or ""
            ok = bool(expect_max) and maxd >= expect_max
            if not ok:
                all_ok = False
            print(f"  {code}: max={maxd} rows={r[1]} [{'OK' if ok else 'GAP'}]")
        print(f"\n[DONE] 共 upsert {total} 行，{len(codes)} 只"
              + ("；全部缺口已补齐 ✅" if all_ok else "；仍有缺口 ⚠️"))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
