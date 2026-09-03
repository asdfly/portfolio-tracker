#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_data.py
封装 NeoData 金融搜索查询与解析，产出 data/.neotmp/neodata_market.json 固定 schema，
供 gen_combo_report.py 消费（其内含跨日跟踪⑤块 + sidecar 续接逻辑）。

- 查询失败 / token 过期：以 exit(2) 退出并打印 TOKEN_EXPIRED，交由自动化 agent 调
  connect_cloud_service 取凭证 → --save-token → 重跑（最多 1 次）。
- 解析失败字段填 None / 空，严禁编造；所有数值标注来源与时间。
- 主资金净流入采用本地 fund_flows.sector 合计代理（main_fund_is_proxy=True），
  因 NeoData 全市场主力资金字段历史上返回的是沪深300而非全市场。

用法：
  python scripts/fetch_market_data.py
"""
import subprocess, json, re, os, sqlite3, sys
from datetime import datetime

ROOT = r'D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker'
PY = r'C:\Users\HUAWEI\.workbuddy\binaries\python\versions\3.13.12\python.exe'
QS = r'C:\Users\HUAWEI\.workbuddy\skills\neodata-financial-search\scripts\query.py'
OUT = os.path.join(ROOT, 'data', '.neotmp', 'neodata_market.json')
DB = os.path.join(ROOT, 'data', 'database', 'portfolio.db')

# 重仓相关板块（与 gen_combo_report.py 交叉验证键名保持一致）
HEAVY = ['航天装备Ⅱ', '军工电子Ⅱ', '地面兵装Ⅱ', '航空装备Ⅱ',
         '化学制药', '生物制品', '医疗服务', '证券Ⅱ',
         '电池', '光伏设备', '半导体', '通信设备']


class TokenExpired(Exception):
    pass


def run_query(q):
    """调用 NeoData query.py，返回 apiRecall[0].content 文本。"""
    try:
        r = subprocess.run([PY, QS, '--query', q], cwd=ROOT,
                           capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        raise RuntimeError('query timeout')
    out = r.stdout
    if 'TOKEN_MISSING' in out or 'TOKEN_EXPIRED' in out:
        raise TokenExpired(out[:160])
    if r.returncode != 0:
        raise RuntimeError(f'query rc={r.returncode}: {r.stderr[:160]}')
    # 容错：stdout 可能夹带日志，截取首个 JSON 对象
    try:
        obj = json.loads(out)
    except Exception:
        s = out.find('{'); e = out.rfind('}')
        if s == -1 or e == -1:
            raise RuntimeError(f'non-json: {out[:160]}')
        obj = json.loads(out[s:e + 1])
    recall = obj.get('data', {}).get('apiData', {}).get('apiRecall', [])
    if not recall:
        raise RuntimeError('empty apiRecall')
    return recall[0].get('content', '')


def query_time_of(content):
    m = re.search(r'数据查询时间[：:]\s*([\d]{4}-[\d]{2}-[\d]{2} \d{2}:\d{2}:\d{2})', content)
    return m.group(1) if m else datetime.now().strftime('%Y-%m-%d %H:%M')


def parse_breadth(content):
    d = {'up': None, 'down': None, 'flat': None, 'halt': None,
         'zt': None, 'dt': None, 'up_pct': None,
         'amount_yi': None, 'amount_chg_yi': None,
         'margin_yi': None, 'margin_chg_pct': None}
    m = re.search(r'上涨\s*([\d,]+)\s*家.*?涨停\s*([\d,]+)\s*家.*?下跌\s*([\d,]+)\s*家'
                  r'.*?跌停\s*([\d,]+)\s*家.*?平盘\s*([\d,]+)\s*家.*?停牌\s*([\d,]+)\s*家',
                  content, re.S)
    if m:
        d['up'] = int(m.group(1).replace(',', '')); d['zt'] = int(m.group(2).replace(',', ''))
        d['down'] = int(m.group(3).replace(',', '')); d['dt'] = int(m.group(4).replace(',', ''))
        d['flat'] = int(m.group(5).replace(',', '')); d['halt'] = int(m.group(6).replace(',', ''))
    up_pct = re.search(r'上涨家数占比全市场\s*(\d+)%', content)
    if up_pct:
        d['up_pct'] = int(up_pct.group(1))
    amt = re.search(r'\|\s*两市汇总\s*\|[^|]*\|\s*([\d,]+\.?\d*)\s*\|', content)
    if amt:
        d['amount_yi'] = round(float(amt.group(1).replace(',', '')) / 1e8, 2)
    amt_chg = re.search(r'\|\s*两市汇总\s*\|[^|]*\|[^|]*\|\s*([+-]?[\d,]+\.?\d*)\s*\|', content)
    if amt_chg:
        d['amount_chg_yi'] = round(float(amt_chg.group(1).replace(',', '')) / 1e8, 2)
    return d


def parse_gainers(content):
    res = []
    for line in content.splitlines():
        if '.PT' not in line:
            continue
        m = re.match(r'\|\s*([^|]+?)\s*\|\s*[\w.]+\.PT\s*\|\s*[\d.]+?\s*\|\s*[\d.+-]+?\s*\|\s*([+-]?\d+\.?\d*)',
                     line)
        if m:
            res.append([m.group(1).strip(), float(m.group(2))])
    return res


def parse_pmi(content):
    pmi = {k: None for k in ['mfg', 'mfg_prev', 'large', 'large_prev', 'mid', 'mid_prev',
                             'hightech', 'hightech_prev', 'equip', 'equip_prev',
                             'composite', 'nonmfg', 'service', 'construction']}
    stat = None
    for line in content.splitlines():
        if not line.strip().startswith('|'):
            continue
        parts = [x.strip() for x in line.strip().strip('|').split('|')]
        if len(parts) < 5 or not parts[0]:
            continue
        name = parts[0]
        if parts[2] and re.match(r'\d{14}', parts[2]) and stat is None:
            stat = parts[2][:8]
        try:
            fv = float(parts[4])
        except Exception:
            continue
        if '制造业PMI' in name and '非制造业' not in name:
            if '大型' in name: pmi['large'] = fv
            elif '中型' in name: pmi['mid'] = fv
            elif '小型' in name: pmi['small'] = fv
            elif '高技术' in name: pmi['hightech'] = fv
            elif '装备' in name: pmi['equip'] = fv
            else:
                if pmi['mfg'] is None:
                    pmi['mfg'] = fv
        elif '综合PMI' in name and '环比' not in name:
            if pmi['composite'] is None:
                pmi['composite'] = fv
        elif ('非制造业PMI:商务活动' in name and '服务业' not in name and '建筑业' not in name
              and '环比' not in name and '从业人员' not in name):
            pmi['nonmfg'] = fv
        elif '服务业:商务活动' in name and '环比' not in name and '从业人员' not in name:
            pmi['service'] = fv
        elif '建筑业:商务活动' in name and '环比' not in name and '从业人员' not in name:
            pmi['construction'] = fv
    if stat:
        y, m, d = stat[:4], stat[4:6], stat[6:8]
        pmi['stat'] = f"{y}-{m}（国家统计局/央行，发布 {y}-{m}-{d}）"
    else:
        pmi['stat'] = None
    return pmi


def parse_heavy(content, today):
    """从 K 线 content 取当日涨跌幅（%）。返回 float 或 None。"""
    rows = []
    for line in content.splitlines():
        if not line.strip().startswith('|'):
            continue
        if 'K线归属时点' in line or ':---:' in line:
            continue
        parts = [p.strip() for p in line.strip().strip('|').split('|')]
        if len(parts) < 8:
            continue
        d = parts[0]
        if not re.match(r'\d{4}-\d{2}-\d{2}', d):
            continue
        if parts[1] in ('-', '未开盘') or parts[7] in ('-', ''):
            continue
        try:
            chg = float(parts[7])
        except Exception:
            continue
        rows.append((d, chg))
    for d, chg in rows:
        if d == today:
            return chg
    if rows:
        return rows[-1][1]
    return None


def local_latest_date():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT MAX(date) FROM fund_flows WHERE category='sector'")
    d = cur.fetchone()[0]
    con.close()
    return d


def local_breadth_local():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT MAX(date) FROM market_breadth")
    d = cur.fetchone()[0]
    if not d:
        con.close()
        return {'zt': None, 'dt': None, 'max_lianban': None, 'top_industry': None, 'date': None}
    cur.execute("SELECT zt_count, dt_count, max_lianban, top_industry FROM market_breadth WHERE date=?", (d,))
    row = cur.fetchone()
    con.close()
    if row:
        return {'zt': row[0], 'dt': row[1], 'max_lianban': row[2], 'top_industry': row[3], 'date': d}
    return {'zt': None, 'dt': None, 'max_lianban': None, 'top_industry': None, 'date': d}


def local_main_fund_proxy(date):
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT SUM(net_inflow) FROM fund_flows WHERE date=? AND category='sector'", (date,))
    s = cur.fetchone()[0]
    con.close()
    return round(s / 1e8, 1) if s else None


def main():
    today = datetime.now().strftime('%Y-%m-%d')
    TODAY = local_latest_date() or today
    breadth_local = local_breadth_local()

    try:
        cb = run_query("今日A股涨跌家数（上涨、下跌、平盘各多少家）、涨停数、跌停数、上涨家数占比、"
                       "两市合计成交额（亿元）及较昨日变化量")
        cg = run_query("今日申万一级行业板块涨幅排行前10（板块名称与涨跌幅%）")
        cp = run_query("最新一期中国官方制造业PMI、综合PMI产出指数、非制造业商务活动PMI、"
                       "服务业PMI、建筑业PMI及统计发布日期")
    except TokenExpired:
        print("TOKEN_EXPIRED")
        sys.exit(2)

    breadth = parse_breadth(cb)
    gainers = parse_gainers(cg)
    pmi = parse_pmi(cp)
    qtime = query_time_of(cb)

    heavy = {}
    for s in HEAVY:
        try:
            ch = run_query(f"今日申万行业板块 {s} 的涨跌幅（%）")
            heavy[s] = parse_heavy(ch, TODAY)
        except TokenExpired:
            print("TOKEN_EXPIRED")
            sys.exit(2)
        except Exception as e:
            heavy[s] = None
            print(f"WARN heavy parse {s}: {e}")

    main_fund_yi = local_main_fund_proxy(TODAY)

    # 主线文本（基于真实数据，不做主观判断）
    top3 = gainers[:3]
    top3_txt = '、'.join(f"{n}{p:+.2f}%" for n, p in top3)
    mil_vals = [heavy.get('地面兵装Ⅱ'), heavy.get('航空装备Ⅱ'), heavy.get('军工电子Ⅱ')]
    mil_vals = [v for v in mil_vals if v is not None]
    if mil_vals:
        mil_best = max(mil_vals)
        mil_names = {'地面兵装Ⅱ': '地面兵装', '航空装备Ⅱ': '航空装备', '军工电子Ⅱ': '军工电子'}
        best_name = next((mil_names[k] for k in ['地面兵装Ⅱ', '航空装备Ⅱ', '军工电子Ⅱ']
                          if heavy.get(k) == mil_best), '')
        main_line = (f"板块涨幅居前：{top3_txt}。"
                     f"重仓相关板块中军工系（地面兵装Ⅱ/航空装备Ⅱ/军工电子Ⅱ）最优涨跌幅 "
                     f"{mil_best:+.2f}%（{best_name}），为当日主线方向。")
    else:
        main_line = f"板块涨幅居前：{top3_txt}。军工系涨跌幅数据缺失。"

    out = {
        "query_time": qtime,
        "source": f"NeoData 金融搜索（查询时间 {qtime}）",
        "breadth": breadth,
        "breadth_local": breadth_local,
        "sector_gainers": gainers,
        "sector_money_in": [],
        "heavy_sector_pct": heavy,
        "main_fund_yi": main_fund_yi,
        "main_fund_is_proxy": True,
        "main_line": main_line,
        "pmi": pmi,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("OK", OUT)
    print("breadth:", breadth)
    print("gainers:", gainers)
    print("heavy:", heavy)
    print("main_fund_yi(proxy):", main_fund_yi)
    print("main_line:", main_line)
    print("pmi.stat:", pmi.get('stat'))


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--save-token', default=None,
                    help='NeoData token 字符串，传入后先写入 NeoData 缓存再查询（用于凭证过期时刷新）')
    ns = ap.parse_args()
    if ns.save_token:
        r = subprocess.run([PY, QS, '--save-token', ns.save_token], cwd=ROOT, text=True,
                          capture_output=True)
        print('[INFO] NeoData token saved via --save-token, rc=', r.returncode)
    main()
