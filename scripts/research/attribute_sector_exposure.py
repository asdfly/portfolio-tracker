# -*- coding: utf-8 -*-
"""组合行业暴露归因（证监会门类级）· 整合方案 Tier 4（D1=方案A）。

回答的问题：「今天组合的涨跌，是哪些行业贡献的？我重仓的行业资金在流入还是流出？」

链路：
    portfolio_snapshots(持仓市值)
      → etf_industry_alloc(ETF→证监会门类权重, report_date='2026-03-31')
      → sector_fund_flow(申万一级行业日度涨跌/资金流) --[映射聚合]--> 证监会门类
      → 组合门类暴露 × 门类市场表现 = 贡献度

════════════════════════════════════════════════════════════════════════
⚠️ 方法论自审（必须随产物一同呈现，不得隐去）
════════════════════════════════════════════════════════════════════════
1. 【口径】组合侧用证监会门类（etf_industry_alloc），市场侧原始为申万一级
   （sector_fund_flow）。两套体系不同源、不同颗粒度，映射为**一对多且带信息
   损失**。证监会「制造业」一个门类吞掉申万 14 个一级行业，无法再细分。
2. 【滞后】etf_industry_alloc 的 report_date=2026-03-31（季报口径，滞后约
   6 个月）。归因用的是**半年前的持仓结构**，与当前实际持仓存在偏移。
3. 【近似】门类内的申万行业权重未知（无行业成交额/市值数据），故门类市场
   涨跌幅采用**等权平均**、资金流采用**求和**。等权平均是近似，非真实加权。
4. 【未归因】场外基金（无门类数据）+ 4 只缺门类数据的 ETF 计入「未归因」，
   如实占位，**不摊派、不编造**。
5. 【低置信】4 个跨门类行业（社会服务/传媒/石油石化/环保）的映射为「取主体
   构成」的近似，产物中单列披露。

产物中所有数字均带口径标注；无法核实的一律 NULL，不做推算填充。
════════════════════════════════════════════════════════════════════════

用法：
    venv313\\Scripts\\python.exe scripts/research/attribute_sector_exposure.py
    venv313\\Scripts\\python.exe scripts/research/attribute_sector_exposure.py --date 2026-10-13 --verify
    venv313\\Scripts\\python.exe scripts/research/attribute_sector_exposure.py --report
"""
import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.getcwd())
from config.settings import DATABASE_PATH

HERE = os.path.dirname(os.path.abspath(__file__))
MAP_PATH = os.path.join(HERE, 'data', 'sw_to_csrc_map.json')

# 仅采用该报告期的门类明细。理由（实测诊断，2026-10-14）：
#   · '2026-03-31'  19 只 ETF 的完整门类明细，各 ETF 权重合计 96.4~100.0，唯一成体系；
#   · '2026-06-30'  仅 7 行零碎补丁（多为 0.0/0.01），不成体系，排除后损失极小；
#   · 'neodata_latest' 5 只 ETF 的笼统「其他」行，分类体系非证监会门类，且与其
#     03-31 明细**并存导致权重合计超 100%（如 512100 达 106.96）**，是重复污染源。
ALLOC_REPORT_DATE = '2026-03-31'

# 场内 ETF canonical 23 只（与 sync_rotation_to_db.py / 「ETF OHLCV 全量兜底巡检」
# 自动化保持一致）。用于把「未归因」进一步拆分为「场外基金」与「场内 ETF 缺门类数据」。
CANONICAL_ETF = {
    '159220', '159267', '159300', '159650', '159732', '159770', '159796', '159819',
    '159949', '159992', '510300', '510500', '511380', '511520', '512010', '512100',
    '512810', '515010', '515120', '516160', '561910', '563020', '588000',
}


def load_map():
    with open(MAP_PATH, encoding='utf-8') as fh:
        return json.load(fh)


def _max_date(cur, table, col):
    cur.execute(f"SELECT MAX({col}) FROM {table}")
    r = cur.fetchone()
    return r[0] if r else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='组合快照日（默认取 portfolio_snapshots 最新日）')
    ap.add_argument('--verify', action='store_true', help='打印校验信息')
    ap.add_argument('--report', action='store_true',
                    help='额外落盘 markdown 报告到 research/sector_rotation/')
    args = ap.parse_args()

    mj = load_map()
    sw2csrc = {k: v for k, v in mj['map'].items()}
    fallback = mj['unmapped_fallback']['csrc']

    con = sqlite3.connect(str(DATABASE_PATH))
    cur = con.cursor()

    # ---------- 1. 组合侧：持仓市值 → 门类暴露 ----------
    snap_date = args.date or _max_date(cur, 'portfolio_snapshots', 'date')
    if not snap_date:
        print('[ERROR] portfolio_snapshots 无数据')
        return 2

    cur.execute("SELECT code, SUM(market_value) FROM portfolio_snapshots "
                "WHERE date=? GROUP BY code", (snap_date,))
    holdings = [(r[0], r[1] or 0.0) for r in cur.fetchall() if (r[1] or 0) > 0]
    total_mv = sum(v for _, v in holdings)
    if not total_mv:
        print('[ERROR] 快照日无正市值持仓')
        return 2

    cur.execute("SELECT code, industry, weight_pct FROM etf_industry_alloc "
                "WHERE report_date=?", (ALLOC_REPORT_DATE,))
    alloc = defaultdict(list)
    for code, industry, w in cur.fetchall():
        if w is None:
            continue
        alloc[code].append((industry, float(w)))

    exposure = defaultdict(float)     # csrc -> 组合暴露 %
    unattr = defaultdict(float)       # 原因 -> 未归因占比
    for code, mv in holdings:
        pct = mv / total_mv * 100
        if code not in alloc:
            if code in CANONICAL_ETF:
                unattr['场内 ETF 缺门类数据（etf_industry_alloc 未采集）'] += pct
            else:
                unattr['场外基金（无门类配置数据）'] += pct
            continue
        for industry, w in alloc[code]:
            exposure[industry] += pct * (w / 100.0)

    # ---------- 2. 市场侧：申万一级 → 门类聚合 ----------
    mkt_date = _max_date(cur, 'sector_fund_flow', 'date')
    agg = defaultdict(lambda: {'chg': [], 'flow': [], 'members': [], 'conf': []})
    unmapped_sw = []
    if mkt_date:
        cur.execute("SELECT sector_name, change_pct, main_net_inflow FROM sector_fund_flow "
                    "WHERE date=?", (mkt_date,))
        for name, chg, flow in cur.fetchall():
            m = sw2csrc.get(name)
            if m is None:
                unmapped_sw.append(name)
                csrc = fallback
            else:
                csrc = m['csrc']
            a = agg[csrc]
            if chg is not None:
                a['chg'].append(float(chg))
            if flow is not None:
                a['flow'].append(float(flow))
            a['members'].append(name)
            a['conf'].append(m['confidence'] if m else 'none')

    # ---------- 3. 合并计算 ----------
    conf_rank = {'exact': 0, 'high': 1, 'low': 2, 'none': 3}
    rows = []
    all_csrc = set(exposure) | set(agg)
    for c in all_csrc:
        a = agg.get(c, {'chg': [], 'flow': [], 'members': [], 'conf': []})
        mkt_chg = sum(a['chg']) / len(a['chg']) if a['chg'] else None
        mkt_flow = sum(a['flow']) if a['flow'] else None
        exp = exposure.get(c, 0.0)
        contrib = (exp * mkt_chg / 100.0) if (mkt_chg is not None and exp) else None
        worst = max(a['conf'], key=lambda x: conf_rank.get(x, 9)) if a['conf'] else 'none'
        rows.append({
            'csrc_sector': c,
            'exposure_pct': round(exp, 4),
            'mkt_change_pct': round(mkt_chg, 4) if mkt_chg is not None else None,
            'mkt_fund_flow_yi': round(mkt_flow, 2) if mkt_flow is not None else None,
            'contribution_pct': round(contrib, 4) if contrib is not None else None,
            'sw_members': a['members'],
            'map_confidence': worst,
        })
    rows.sort(key=lambda r: -r['exposure_pct'])

    # ---------- 4. 入库 ----------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS csrc_sector_exposure (
            date TEXT NOT NULL,
            mkt_date TEXT,
            csrc_sector TEXT NOT NULL,
            exposure_pct REAL,
            mkt_change_pct REAL,
            mkt_fund_flow_yi REAL,
            contribution_pct REAL,
            sw_members TEXT,
            map_confidence TEXT,
            alloc_report_date TEXT,
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, csrc_sector)
        )""")
    for r in rows:
        cur.execute("""
            INSERT OR REPLACE INTO csrc_sector_exposure
            (date, mkt_date, csrc_sector, exposure_pct, mkt_change_pct, mkt_fund_flow_yi,
             contribution_pct, sw_members, map_confidence, alloc_report_date, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (snap_date, mkt_date, r['csrc_sector'], r['exposure_pct'],
             r['mkt_change_pct'], r['mkt_fund_flow_yi'], r['contribution_pct'],
             json.dumps(r['sw_members'], ensure_ascii=False), r['map_confidence'],
             ALLOC_REPORT_DATE, 'portfolio_snapshots+etf_industry_alloc+sector_fund_flow'))
    con.commit()

    # ---------- 5. 输出 ----------
    unattr_total = sum(unattr.values())
    attr_total = sum(r['exposure_pct'] for r in rows)
    eff = [r for r in rows if r['exposure_pct'] > 0 and r['mkt_change_pct'] is not None]
    gap = [r for r in rows if r['exposure_pct'] > 0 and r['mkt_change_pct'] is None]
    eff_exp = sum(r['exposure_pct'] for r in eff)
    gap_exp = sum(r['exposure_pct'] for r in gap)
    contrib_sum = sum(r['contribution_pct'] for r in eff)

    # 真值对照
    cur.execute("SELECT daily_return FROM portfolio_summary WHERE date=?", (snap_date,))
    _r = cur.fetchone()
    true_ret = float(_r[0]) if (_r and _r[0] is not None) else None

    print(f"\n{'='*82}")
    print(f"组合行业暴露归因（证监会门类级）　快照日 {snap_date}　行业行情日 {mkt_date}")
    print(f"{'='*82}")
    print(f"{'门类':<26}{'暴露%':>8}{'涨跌%':>9}{'资金流亿':>11}{'贡献pct':>10}  置信  状态")
    print('-' * 82)
    for r in rows:
        if r['exposure_pct'] <= 0 and not r['sw_members']:
            continue
        e = f"{r['exposure_pct']:.2f}"
        c = f"{r['mkt_change_pct']:+.2f}" if r['mkt_change_pct'] is not None else '—'
        f = f"{r['mkt_fund_flow_yi']:+,.1f}" if r['mkt_fund_flow_yi'] is not None else '—'
        k = f"{r['contribution_pct']:+.4f}" if r['contribution_pct'] is not None else '—'
        st = '映射缺口' if r['mkt_change_pct'] is None else '可对照'
        print(f"{r['csrc_sector']:<26}{e:>8}{c:>9}{f:>11}{k:>10}  {r['map_confidence']:<5} {st}")
    print('-' * 82)
    print(f"{'有效归因（可对照市场）':<26}{eff_exp:>8.2f}")
    print(f"{'已归入门类但无市场数据':<26}{gap_exp:>8.2f}   ← 映射缺口")
    print(f"{'未归因':<26}{unattr_total:>8.2f}")
    for reason, v in sorted(unattr.items(), key=lambda x: -x[1]):
        print(f"  └ {reason}: {v:.2f}%")

    print(f"\n【与真实回报对照】")
    if true_ret is not None:
        print(f"  组合真实当日回报（portfolio_summary.daily_return）：{true_ret:+.4f}%")
        print(f"  归因模型贡献合计（仅覆盖 {eff_exp:.1f}%）：{contrib_sum:+.4f} pct")
        diff = contrib_sum - true_ret
        print(f"  偏差：{diff:+.4f} pct" +
              ("　⚠ **方向相反**" if (contrib_sum * true_ret < 0) else ""))
        print(f"  ⚠ 结论：当前归因**不可用于解释当日盈亏**——覆盖不足（{eff_exp:.1f}%）、")
        print(f"    持仓结构滞后约 6 个月、门类内等权平均为近似，三者叠加导致方向性偏差。")
        print(f"    本产物当前定位：**门类级结构快照**（看「钱在哪个门类」），非盈亏归因。")
    else:
        print(f"  portfolio_summary {snap_date} 无 daily_return，无法对照")

    print(f"\n【口径与偏差披露】")
    print(f"  · 门类涨跌幅 = 门类内申万一级行业的**等权平均**（缺门类内行业权重，属近似，非真实加权）")
    print(f"  · 门类资金流 = 门类内申万一级行业主力净流入**求和**（亿元）")
    print(f"  · 持仓结构为 report_date={ALLOC_REPORT_DATE} 季报口径（滞后约 6 个月）")
    low = [r['csrc_sector'] for r in rows if r['map_confidence'] == 'low']
    if low:
        print(f"  · ⚠ 含低置信映射的门类（跨门类拆分，归因存在已知偏差）：{('、'.join(low))}")
    if gap:
        print(f"  · ⚠ 映射缺口门类（组合侧有暴露、但无对应申万一级行业来源）："
              f"{('、'.join(r['csrc_sector'] for r in gap))}")
        print(f"    成因：① 证监会门类（如科学研究和技术服务业/卫生和社会工作）在申万一级无对应项；")
        print(f"          ② 个别 ETF（实测 159220）采用非证监会分类体系（公用事业/必须消费品/材料等别名）。")
        print(f"    处置：市场数据一律 NULL，**不猜测、不套用近似替代**。")
    if unmapped_sw:
        print(f"  · ⚠ 未匹配的申万行业（映射表需更新）：{('、'.join(unmapped_sw))}")

    if args.verify:
        print(f"\n【校验】")
        print(f"  快照日持仓数：{len(holdings)}　有门类数据：{len([h for h in holdings if h[0] in alloc])}")
        print(f"  └ 场内 ETF：{len([h for h in holdings if h[0] in CANONICAL_ETF])}　"
              f"场外：{len([h for h in holdings if h[0] not in CANONICAL_ETF])}")
        print(f"  映射表条目：{len(sw2csrc)}（申万一级 31 行业应全覆盖）")
        print(f"  有效归因 {eff_exp:.2f}% + 映射缺口 {gap_exp:.2f}% + 未归因 {unattr_total:.2f}% "
              f"= {eff_exp + gap_exp + unattr_total:.2f}%（残差为 ETF 持仓现金等，非 100 属预期）")
        cur.execute("SELECT COUNT(*) FROM csrc_sector_exposure WHERE date=?", (snap_date,))
        print(f"  入库行数：{cur.fetchone()[0]}（幂等：重跑行数不变）")

    if args.report:
        outdir = os.path.join('research', 'sector_rotation')
        os.makedirs(outdir, exist_ok=True)
        p = os.path.join(outdir, f'attribution_{snap_date}.md')
        with open(p, 'w', encoding='utf-8') as fh:
            fh.write(f"# 组合行业暴露归因（证监会门类级）· {snap_date}\n\n")
            fh.write(f"- 组合快照日：{snap_date}　行业行情日：{mkt_date}\n")
            fh.write(f"- 持仓结构口径：`etf_industry_alloc.report_date = {ALLOC_REPORT_DATE}`（季报，滞后约 6 个月）\n")
            fh.write(f"- 有效归因（可对照市场）：**{eff_exp:.2f}%**　映射缺口：{gap_exp:.2f}%　未归因：{unattr_total:.2f}%\n\n")
            if true_ret is not None:
                fh.write(f"## 与真实回报对照\n\n")
                fh.write(f"- 组合真实当日回报：**{true_ret:+.4f}%**\n")
                fh.write(f"- 归因模型贡献合计（覆盖 {eff_exp:.1f}%）：{contrib_sum:+.4f} pct\n")
                fh.write(f"- 偏差：{contrib_sum - true_ret:+.4f} pct"
                         + ("（**方向相反**）\n" if contrib_sum * true_ret < 0 else "\n"))
                fh.write(f"\n> ⚠ 当前归因**不可用于解释当日盈亏**。覆盖不足、持仓滞后 6 个月、\n")
                fh.write(f"> 门类内等权平均为近似，三者叠加导致方向性偏差。本产物定位为\n")
                fh.write(f"> **门类级结构快照**（看「钱在哪个门类」），非盈亏归因。\n\n")
            fh.write("## 门类明细\n\n")
            fh.write("| 门类 | 暴露% | 涨跌% | 资金流(亿) | 贡献(pct) | 映射置信 | 状态 |\n|---|---|---|---|---|---|---|\n")
            for r in rows:
                if r['exposure_pct'] <= 0 and not r['sw_members']:
                    continue
                c = f"{r['mkt_change_pct']:+.2f}" if r['mkt_change_pct'] is not None else '—'
                f = f"{r['mkt_fund_flow_yi']:+,.1f}" if r['mkt_fund_flow_yi'] is not None else '—'
                k = f"{r['contribution_pct']:+.4f}" if r['contribution_pct'] is not None else '—'
                st = '映射缺口' if r['mkt_change_pct'] is None else '可对照'
                fh.write(f"| {r['csrc_sector']} | {r['exposure_pct']:.2f} | {c} | {f} | {k} | {r['map_confidence']} | {st} |\n")
            fh.write(f"\n## 未归因构成\n\n")
            for reason, v in sorted(unattr.items(), key=lambda x: -x[1]):
                fh.write(f"- {reason}：{v:.2f}%\n")
            fh.write(f"\n## 方法论自审\n\n")
            fh.write("1. 组合侧证监会门类 vs 市场侧申万一级，映射**一对多且带信息损失**；\n")
            fh.write("2. 门类涨跌幅为门类内申万行业的**等权平均**（近似，非真实加权）；\n")
            fh.write("3. 持仓结构滞后约 6 个月；\n")
            fh.write("4. 未归因部分（场外基金 + 缺门类数据的 ETF）**不摊派、不编造**；\n")
            fh.write(f"5. 低置信映射门类：{('、'.join(low)) if low else '无'}；\n")
            fh.write(f"6. 映射缺口门类：{('、'.join(r['csrc_sector'] for r in gap)) if gap else '无'}，市场数据一律 NULL，不猜测替代。\n")
        print(f"\n[REPORT] {p}")

    con.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
