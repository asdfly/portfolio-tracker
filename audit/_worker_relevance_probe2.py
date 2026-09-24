# -*- coding: utf-8 -*-
"""worker 批次 relevance 核查探针 · 第二轮（全程只读）。

第一轮发现两处需深挖：
  (a) etf_price_history 在 2026-09-22 只有 1 个 distinct code（前序日均 23）
  (b) alerts 表无 alert_type 列（探针列名错误，需先取真实 schema）

铁律：只读 file:...?mode=ro；数字不臆造。
"""
import sqlite3
import datetime

DB = 'data/database/portfolio.db'
URI = 'file:' + DB + '?mode=ro'

OTC = ['001194', '001323', '001407', '001437', '001765',
       '002152', '007994', '008269', '100032', '166301',
       '519770', '880013']


def main():
    con = sqlite3.connect(URI, uri=True)
    cur = con.cursor()

    print('== A. etf_price_history 09-16 起每日 code 数 / 行数 ==')
    cur.execute('SELECT date, COUNT(DISTINCT code), COUNT(*) '
                'FROM etf_price_history WHERE date >= "2026-09-16" '
                'GROUP BY date ORDER BY date DESC')
    for d, ncode, nrow in cur.fetchall():
        flag = '   <== 断崖' if ncode < 20 else ''
        print('  %s distinct_code=%-3d rows=%-4d%s' % (d, ncode, nrow, flag))

    print('== A2. 09-22 具体是哪个 code 有数据 ==')
    cur.execute('SELECT code, close, volume FROM etf_price_history '
                'WHERE date = "2026-09-22"')
    rows = cur.fetchall()
    print('  rows=', rows if rows else 'EMPTY')

    print('== B. is_split_merge 是否真的落到生产数据（fix-conversion-guard）==')
    cur.execute('SELECT COUNT(*), '
                'SUM(CASE WHEN is_split_merge IS NULL THEN 1 ELSE 0 END), '
                'SUM(CASE WHEN is_split_merge = 1 THEN 1 ELSE 0 END) '
                'FROM etf_features')
    tot, nnull, ntrue = cur.fetchone()
    print('  etf_features total=%s is_null=%s is_true=%s' % (tot, nnull, ntrue))

    print('== C. alerts 表真实 schema + 最近 error ==')
    cur.execute('PRAGMA table_info(alerts)')
    cols = [r[1] for r in cur.fetchall()]
    print('  cols=', cols)
    lvlcol = 'level' if 'level' in cols else ('severity' if 'severity' in cols else None)
    if lvlcol:
        cur.execute('SELECT COUNT(*) FROM alerts WHERE %s = "error"' % lvlcol)
        print('  error 总数=', cur.fetchone()[0])
        namecol = 'type' if 'type' in cols else (
            'alert_type' if 'alert_type' in cols else cols[1])
        cur.execute('SELECT id, %s, %s, created_at FROM alerts WHERE %s = "error" '
                    'ORDER BY id DESC LIMIT 8'
                    % (namecol, 'message' if 'message' in cols else cols[2],
                       lvlcol))
        for r in cur.fetchall():
            print('   ', r)
    else:
        print('  UNKNOWN: 找不到分级列，cols=', cols)

    print('== D. 场外 12 只：最近 2 行净值是否同值（滞后/复制行粗检）==')
    for code in OTC:
        cur.execute('SELECT date, current_price FROM portfolio_snapshots '
                    'WHERE code = ? ORDER BY date DESC LIMIT 2', (code,))
        rs = cur.fetchall()
        if len(rs) < 2:
            print('  %s 行数不足: %s' % (code, rs))
            continue
        same = abs((rs[0][1] or 0) - (rs[1][1] or 0)) < 1e-9
        print('  %s  %s=%s  %s=%s  same=%s'
              % (code, rs[0][0], rs[0][1], rs[1][0], rs[1][1], same))

    print('== E. fund_flows 中 12 只场外：最近 as-of 日期（#140 残留①）==')
    cur.execute('SELECT code, MAX(date), COUNT(*) FROM fund_flows '
                'WHERE code IN (%s) GROUP BY code'
                % ','.join(['?'] * len(OTC)), OTC)
    rows = cur.fetchall()
    have = dict((r[0], r) for r in rows)
    for code in OTC:
        if code in have:
            print('  %s 有数据 max_date=%s rows=%s' % (code, have[code][1], have[code][2]))
    missing = [c for c in OTC if c not in have]
    print('  无数据（UI 上裸 0.0 风险）: %s 只 -> %s' % (len(missing), missing))

    con.close()
    print('PROBE2_DONE', datetime.datetime.now())


if __name__ == '__main__':
    main()
