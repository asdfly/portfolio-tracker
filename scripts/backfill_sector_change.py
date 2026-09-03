#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性回溯脚本：将重仓相关申万板块的每日涨跌幅写入 portfolio.db 的
sector_daily_change 表，支撑「组合+大盘综合视角」报告中「军工主线连续领涨天数」
等跨日信号。

数据来源：NeoData 统一行情查询（按板块名查询区间 K 线，一次返回整段日线）。
幂等：以 (date, sector_name) 为主键 INSERT OR REPLACE。

用法（managed python 执行）：
  python scripts/backfill_sector_change.py [--start 2026-07-01] [--end 2026-09-02] [--no-save-token]
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'database', 'portfolio.db')
# neodata-financial-search skill 路径按当前用户主目录推导，避免硬编码用户名
QUERY_PY = os.path.join(os.path.expanduser('~'), '.workbuddy', 'skills',
                        'neodata-financial-search', 'scripts', 'query.py')
# 使用当前解释器（项目 venv313），避免硬编码 WorkBuddy 私有 python 路径
PY = sys.executable

# 重仓相关板块观测池（名称须与 NeoData 返回一致；部分为「Ⅱ」后缀）
WATCHLIST = [
    '地面兵装Ⅱ', '航空装备Ⅱ', '军工电子Ⅱ',   # 军工系
    '化学制药', '生物制品', '医疗服务',          # 医药系
    '证券Ⅱ',                                       # 证券/金融
    '电池', '光伏设备',                            # 新能源
    '半导体', '通信设备',                          # 科技
    '种植业',                                      # 农业
]

# 板块 -> 主题归属（用于信号解读）
THEME = {
    '地面兵装Ⅱ': '军工', '航空装备Ⅱ': '军工', '军工电子Ⅱ': '军工',
    '化学制药': '医药', '生物制品': '医药', '医疗服务': '医药',
    '证券Ⅱ': '证券',
    '电池': '新能源', '光伏设备': '新能源',
    '半导体': '科技', '通信设备': '科技',
    '种植业': '农业',
}


def ensure_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS sector_daily_change (
            date         TEXT NOT NULL,
            sector_code  TEXT,
            sector_name  TEXT NOT NULL,
            change_pct   REAL,
            close        REAL,
            prev_close   REAL,
            source       TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, sector_name)
        )
    """)


def parse_kline(content):
    """从 NeoData 返回的 content（json.loads 后的字符串，含真实换行）中解析 K 线明细表，
    返回 list of (date, code, name, chg_pct, close, prev_close)。"""
    rows = []
    m_code = re.search(r'\*\*标的代码\*\*:\s*([\w.]+)', content)
    m_name = re.search(r'\*\*标的名称\*\*:\s*([^\n]+)', content)
    code = m_code.group(1).strip() if m_code else None
    name = m_name.group(1).strip() if m_name else None
    for line in content.splitlines():
        if not line.strip().startswith('|'):
            continue
        if 'K线归属时点' in line or ':---:' in line:
            continue
        parts = [p.strip() for p in line.strip().strip('|').split('|')]
        # parts: [date, open, high, low, close, prev_close, change, change_pct, amp, vol, amount, turnover, vr]
        if len(parts) < 8:
            continue
        d = parts[0]
        if not re.match(r'\d{4}-\d{2}-\d{2}', d):
            continue
        if parts[1] in ('-', '未开盘') or parts[7] in ('-', ''):
            continue  # 非交易日
        try:
            chg = float(parts[7])
            close = float(parts[4]) if parts[4] not in ('-', '') else None
            prev = float(parts[5]) if parts[5] not in ('-', '') else None
        except ValueError:
            continue
        rows.append((d, code, name, chg, close, prev))
    return rows


def query_sector(sector, start, end):
    q = f"{sector} {start} 至 {end} 每个交易日涨跌幅（%）"
    out = os.path.join(ROOT, 'data', '.neotmp', f'bf_{sector}.txt')
    err = os.path.join(ROOT, 'data', '.neotmp', f'bf_{sector}.err')
    with open(out, 'w', encoding='utf-8') as fo, open(err, 'w', encoding='utf-8') as fe:
        rc = subprocess.run([PY, QUERY_PY, '--query', q], stdout=fo, stderr=fe,
                            cwd=ROOT, text=True)
    if rc.returncode != 0:
        return None, f"rc={rc.returncode}"
    txt = open(out, encoding='utf-8').read()
    try:
        obj = json.loads(txt)
        content = obj['data']['apiData']['apiRecall'][0]['content']
        sec_name = obj['data']['apiData']['apiRecall'][0]['entity'][0]['name'] \
            if obj['data']['apiData']['apiRecall'][0].get('entity') else sector
    except Exception as e:
        return None, f"json-parse:{e}"
    rows = parse_kline(content)
    return rows, sec_name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2026-07-01')
    ap.add_argument('--end', default='2026-09-02')
    ap.add_argument('--sectors', nargs='*', default=WATCHLIST)
    args = ap.parse_args()

    os.makedirs(os.path.join(ROOT, 'data', '.neotmp'), exist_ok=True)
    con = sqlite3.connect(DB)
    ensure_table(con)
    total = 0
    for sec in args.sectors:
        try:
            rows, sec_name = query_sector(sec, args.start, args.end)
        except Exception as e:
            print(f"  [ERR] {sec}: {e}")
            continue
        if rows is None:
            print(f"  [SKIP] {sec}: {sec_name}")
            continue
        for d, code, name, chg, close, prev in rows:
            con.execute(
                "INSERT OR REPLACE INTO sector_daily_change "
                "(date, sector_code, sector_name, change_pct, close, prev_close, source) "
                "VALUES (?,?,?,?,?,?,?)",
                (d, code, name or sec_name, chg, close, prev, 'neodata_kline'))
            total += 1
        con.commit()
        if rows:
            ds = [r[0] for r in rows]
            print(f"  [OK] {sec_name:10s} rows={len(rows):2d}  {min(ds)}~{max(ds)}")
        else:
            print(f"  [EMPTY] {sec}: 解析到 0 行（可能名称不匹配或接口异常）")
        time.sleep(1.0)  # 轻量限速，避免触发频率限制
    con.close()
    print(f"\nDONE total upserted={total}")


if __name__ == '__main__':
    main()
