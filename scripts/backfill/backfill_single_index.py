#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单只指数历史回填：index_quotes（新浪K线）+ index_pe_history（NeoData 估值）。

用途：新增跟踪指数（如中证2000 sh932000）后，一次性补齐该指数的历史数据，
      使其进入 index_quotes / index_pe_history 受跟踪宇宙，与既有 11 只基准指数一致。

安全约束（铁律 12_）：
  - 写库前先备份 portfolio.db 到 data/backups/（shutil.copy2 + 大小校验）；
  - index_quotes / index_pe_history 均 INSERT OR REPLACE 幂等，重复运行安全；
  - 仅新浪K线走直连（proxies=None），不动全局代理环境，避免污染 NeoData 子进程。

用法：
  python scripts/backfill/backfill_single_index.py --code sh932000 --name 中证2000
  python scripts/backfill/backfill_single_index.py --code sh932000 --name 中证2000 --no-pe
"""
import os
import sys
import shutil
import argparse
import sqlite3
import datetime
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from config.settings import DATABASE_PATH, BACKUP_DIR
from src.data_sources.neodata_valuation import fetch_index_valuation, save_valuation
from src.utils.database import get_db_connection

SINA_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"


def _to_sina_code(code: str) -> str:
    code = str(code).strip().lower()
    if code.startswith("sh") or code.startswith("sz"):
        return code
    if code.startswith(("51", "56", "58", "6")):
        return f"sh{code}"
    return f"sz{code}"


def fetch_klines_sina(code: str, max_datalen: int = 10000) -> list:
    """新浪日K线全历史（自包含，直连不走代理）。返回 [{day,open,high,low,close,volume,amount}]。"""
    sina_code = _to_sina_code(code)
    all_klines = []
    datalen = max_datalen
    while True:
        params = {"symbol": sina_code, "scale": 240, "ma": "no", "datalen": datalen}
        try:
            resp = requests.get(
                SINA_KLINE_URL, params=params, timeout=30,
                headers={"Referer": "https://finance.sina.com.cn"},
                proxies={"http": None, "https": None},
            )
            resp.encoding = "utf-8"
            data = resp.json()
        except Exception as e:
            print(f"      [新浪] {code} 请求失败: {type(e).__name__}: {e}")
            break
        if not isinstance(data, list) or len(data) == 0:
            break
        all_klines.extend(data)
        if len(data) < datalen:
            break
        if len(all_klines) >= max_datalen:
            break
        earliest = data[0].get("day", "")
        if earliest and len(earliest) >= 10:
            try:
                if int(earliest[:4]) < 2000:
                    break
            except ValueError:
                pass
        datalen = min(datalen, 5000)
    return all_klines


def fetch_klines_tencent(code: str) -> list:
    """腾讯财经 gtimg 指数日K线（直连，覆盖新浪/东财均取不到的指数，如中证2000）。

    返回 [{day,open,high,low,close,volume,amount}]，与 save_index_quotes 同构。
    gtimg day 字符串格式：date,open,close,high,low,volume[,amount]
    （收盘紧挨开盘之后，故 p[2]=close、p[3]=high、p[4]=low）。
    """
    sina_code = _to_sina_code(code)
    url = f"https://web.ifzq.gtimg.cn/appstuff/app/chart/kline?param={sina_code},day,,,320,qfq"
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)
    try:
        resp = requests.get(
            url, timeout=20,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"},
            proxies={"http": None, "https": None},
        )
        resp.encoding = "utf-8"
        data = resp.json()
    except Exception as e:
        print(f"      [腾讯] {code} 请求失败: {type(e).__name__}: {e}")
        return []
    if not isinstance(data, dict) or data.get("code") != 0:
        print(f"      [腾讯] {code} 无数据(code={data.get('code') if isinstance(data, dict) else '?'}, "
              f"msg={data.get('msg') if isinstance(data, dict) else ''})")
        return []
    node = data.get("data")
    # node 可能是 {code: {day:[...]}} 或 {day:[...]}
    if isinstance(node, dict):
        for v in node.values():
            if isinstance(v, dict) and ("day" in v or "qfqday" in v):
                node = v
                break
    if not isinstance(node, dict):
        return []
    arr = node.get("qfqday") or node.get("day") or []
    out = []
    for s in arr:
        if not isinstance(s, str) or "," not in s:
            continue
        p = s.split(",")
        if len(p) < 6:
            continue
        try:
            out.append({
                "day": p[0],
                "open": float(p[1]),
                "close": float(p[2]),
                "high": float(p[3]),
                "low": float(p[4]),
                "volume": float(p[5]) if p[5] else 0.0,
                "amount": float(p[6]) if len(p) > 6 and p[6] else 0.0,
            })
        except ValueError:
            continue
    return out


def akshare_index_history(code: str, retries: int = 5) -> list:
    """东方财富（经 akshare）取指数日线全历史。

    注意：环境代理环境变量指向不可达代理，必须清掉并走直连；且 eastmoney 上游
    间歇性 RemoteDisconnected（与 09-22 系统级抖动同源），故加有界重试。
    返回 [{day,open,high,low,close,volume,amount}] 与 save_index_quotes 同构。
    """
    import time
    sym = code.replace("sh", "").replace("sz", "")
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    import akshare as ak

    last = None
    for i in range(retries):
        try:
            df = ak.index_zh_a_hist(
                symbol=sym, period="daily",
                start_date="20040101",
                end_date=datetime.date.today().strftime("%Y%m%d"),
            )
            if df is not None and len(df):
                out = []
                for _, r in df.iterrows():
                    out.append({
                        "day": str(r.get("日期", "")),
                        "open": r.get("开盘"), "high": r.get("最高"),
                        "low": r.get("最低"), "close": r.get("收盘"),
                        "volume": r.get("成交量"), "amount": r.get("成交额"),
                    })
                return out
            last = "empty"
        except Exception as e:  # 瞬断重试
            last = f"{type(e).__name__}: {e}"
            time.sleep(2 + i)
    print(f"      [东方财富] {code} 重试 {retries} 次仍失败: {last}")
    return []


def backup_db() -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f"portfolio_PRE_CS2000_BACKFILL_{ts}.db")
    shutil.copy2(str(DATABASE_PATH), dst)
    if os.path.getsize(dst) != os.path.getsize(str(DATABASE_PATH)):
        raise RuntimeError("备份大小校验失败，中止写入")
    return dst


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True, help="带市场前缀的指数代码，如 sh932000")
    ap.add_argument("--index-code", default=None, help="NeoData 用的纯指数代码，如 932000（默认去前缀）")
    ap.add_argument("--name", required=True, help="指数中文名，如 中证2000")
    ap.add_argument("--no-pe", action="store_true", help="跳过 PE 回填（仅行情）")
    ns = ap.parse_args()

    code = ns.code
    idx_code = ns.index_code or code.replace("sh", "").replace("sz", "")
    name = ns.name
    db = str(DATABASE_PATH)

    print(f"[1/4] 备份 portfolio.db ...")
    bk = backup_db()
    print(f"      备份: {bk} ({os.path.getsize(bk)/1e6:.1f} MB)")

    print(f"[2/4] 回填 index_quotes ({code} {name}) ...")
    klines = fetch_klines_sina(code)
    src_label = "新浪"
    if not klines:
        print(f"      新浪无数据({code})，回退腾讯gtimg...")
        klines = fetch_klines_tencent(code)
        src_label = "腾讯"
    if not klines:
        print(f"      腾讯无数据({code})，回退东方财富(akshare, 有界重试)...")
        klines = akshare_index_history(code)
        src_label = "东方财富"
    print(f"      {src_label} 返回 {len(klines)} 根K线")
    if klines:
        conn = get_db_connection(db)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT date FROM index_quotes WHERE code=?", (code,))
        existing = {r[0] for r in cur.fetchall()}
        conn.close()
        # 复用 backfill_full_history 的落库逻辑（幂等，跳过已有日期与周末）
        from scripts.backfill.backfill_full_history import save_index_quotes
        n_q = save_index_quotes(db, code, name, klines, existing)
        print(f"      index_quotes 新增 {n_q} 行 (source={src_label})")
    else:
        n_q = 0
        print(f"      所有源均无数据，index_quotes 跳过")

    if not ns.no_pe:
        print(f"[3/4] 回填 index_pe_history ({idx_code}) via NeoData ...")
        rows = fetch_index_valuation(idx_code)
        print(f"      NeoData 返回 {len(rows)} 行")
        if rows:
            c2 = get_db_connection(db)
            n_pe = save_valuation(c2, idx_code, rows)
            c2.close()
            print(f"      index_pe_history 新增 {n_pe} 行")
        else:
            print(f"      NeoData 无数据（token 过期或代码未识别），index_pe_history 跳过")
    else:
        print(f"[3/4] 跳过 PE 回填（--no-pe）")

    print(f"[4/4] 校验 ...")
    c3 = get_db_connection(db)
    q = c3.execute("SELECT COUNT(*), MAX(date) FROM index_quotes WHERE code=?", (code,)).fetchone()
    pe = c3.execute("SELECT COUNT(*), MAX(date) FROM index_pe_history WHERE index_code=?", (idx_code,)).fetchone()
    c3.close()
    print(f"      index_quotes : {code} 行数={q[0]} 最新={q[1]}")
    print(f"      index_pe_history: {idx_code} 行数={pe[0]} 最新={pe[1]}")
    print("完成。")


if __name__ == "__main__":
    main()
