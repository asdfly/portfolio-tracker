# -*- coding: utf-8 -*-
"""worker 批次 relevance 核查探针（全程只读）。

目的：对上一轮派工但已 paused 的约 40 个 worker，核查其对应事项
在 HEAD=7a5cfe1 下是否仍成立。

铁律遵守：
- 生产库一律 file:...?mode=ro + uri=True
- 数字要么实测、要么显式标 UNKNOWN，不臆造
- 不写任何库、不改任何文件

用法： venv313/Scripts/python.exe audit/_worker_relevance_probe.py
"""
import sqlite3
import datetime
import os

DB = 'data/database/portfolio.db'
URI = 'file:' + DB + '?mode=ro'

OTC = ['001194', '001323', '001407', '001437', '001765',
       '002152', '007994', '008269', '100032', '166301',
       '519770', '880013']


def main():
    if not os.path.exists(DB):
        print('PROD_DB_MISSING', DB)
        return
    con = sqlite3.connect(URI, uri=True)
    cur = con.cursor()

    def cols(t):
        cur.execute('PRAGMA table_info(%s)' % t)
        return [r[1] for r in cur.fetchall()]

    print('== [1] fix-conversion-guard: is_split_merge 列是否存在于生产表 ==')
    for t in ('etf_features', 'portfolio_nav'):
        try:
            c = cols(t)
            print('  %-14s ncols=%-4d HAS_is_split_merge=%s'
                  % (t, len(c), 'is_split_merge' in c))
        except Exception as e:
            print('  %-14s ERR %s' % (t, e))

    print('== [2] 最高危: 最近 12 日快照行数（基线 34 / 周五 35）==')
    cur.execute('SELECT date, COUNT(*), ROUND(SUM(market_value),2) '
                'FROM portfolio_snapshots GROUP BY date '
                'ORDER BY date DESC LIMIT 12')
    for d, n, s in cur.fetchall():
        flag = '' if n in (34, 35) else '   <== 偏离基线'
        print('  %s rows=%-3d sum_mv=%s%s' % (d, n, s, flag))

    print('== [3] diag-otc-coverage: 最近 8 日场外 12 只在快照中的覆盖 ==')
    cur.execute('SELECT date, COUNT(*) FROM portfolio_snapshots '
                'WHERE code IN (%s) GROUP BY date ORDER BY date DESC LIMIT 8'
                % ','.join(['?'] * len(OTC)), OTC)
    for d, n in cur.fetchall():
        print('  %s otc_rows=%d / 12' % (d, n))

    print('== [4] fix-etf-price-gap 复核: etf_price_history 最近 8 日 distinct code ==')
    cur.execute('SELECT date, COUNT(DISTINCT code) FROM etf_price_history '
                'GROUP BY date ORDER BY date DESC LIMIT 8')
    for d, n in cur.fetchall():
        print('  %s codes=%d' % (d, n))

    print('== [5] #140 残留①: fund_flows 中 12 只场外的行数 ==')
    cur.execute('SELECT code, COUNT(*) FROM fund_flows '
                'WHERE code IN (%s) GROUP BY code'
                % ','.join(['?'] * len(OTC)), OTC)
    rows = cur.fetchall()
    print('  有行的场外:', rows if rows else 'NONE (0 只) —— 无数据即裸 0.0')

    print('== [6] portfolio_summary 最近 8 行 ==')
    cur.execute('SELECT date, total_value, daily_return '
                'FROM portfolio_summary ORDER BY date DESC LIMIT 8')
    for d, tv, dr in cur.fetchall():
        print('  %s total_value=%s daily_return=%s' % (d, tv, dr))

    print('== [7] 最近 10 条 error 级告警 ==')
    try:
        cur.execute('SELECT created_at, alert_type, message FROM alerts '
                    'WHERE level="error" ORDER BY id DESC LIMIT 10')
        for r in cur.fetchall():
            print('  ', r)
    except Exception as e:
        print('  ERR', e)

    con.close()
    print('PROBE_DONE', datetime.datetime.now())


if __name__ == '__main__':
    main()
