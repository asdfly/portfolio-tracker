# -*- coding: utf-8 -*-
"""组合行业暴露归因（证监会门类级）· 整合方案 Tier 4（D1=方案A）。

回答的问题：「今天组合的涨跌，是哪些行业贡献的？我重仓的行业资金在流入还是流出？」

链路：
    portfolio_snapshots(持仓市值)
      → etf_industry_alloc(ETF→证监会门类权重, 取最新有效 report_date)
      → sector_fund_flow(申万一级行业日度涨跌/资金流) --[映射聚合]--> 证监会门类
      → 组合门类暴露 × 门类市场表现 = 贡献度

════════════════════════════════════════════════════════════════════════
⚠️ 方法论自审（必须随产物一同呈现，不得隐去）
════════════════════════════════════════════════════════════════════════
1. 【口径】组合侧用证监会门类（etf_industry_alloc），市场侧原始为申万一级
   （sector_fund_flow）。两套体系不同源、不同颗粒度，映射为**一对多且带信息
   损失**。证监会「制造业」一个门类吞掉申万 14 个一级行业，无法再细分。
2. 【滞后】etf_industry_alloc 为季报口径，脚本自动取**最新有效 report_date**
   （如 2026-06-30，滞后约 3.5 个月）。归因用的是**上一季报的持仓结构**，
   与当前实际持仓存在偏移，季度间调仓不可见。
3. 【近似】门类内的申万行业权重未知（无行业成交额/市值数据），故门类市场
   涨跌幅采用**等权平均**、资金流采用**求和**。等权平均是近似，非真实加权。
4. 【未归因】场外基金（无门类数据）+ 债券类 ETF（无行业配置属业务性质）
   + 采集失败的代码，均计入「未归因」，如实占位，**不摊派、不编造**。
5. 【低置信】4 个跨门类行业（社会服务/传媒/石油石化/环保）的映射为「取主体
   构成」的近似，产物中单列披露。
6. 【分类体系混用】部分基金（实测 008269 大成睿享混合A、159220）的持仓含
   QDII/港股部分，其行业标签采用 **GICS 风格**（通讯/科技/工业/必需消费品/
   非必需消费品/医疗保健/能源/材料 等）而非证监会门类。脚本采用**证监会门类
   白名单**过滤：不在白名单内的标签一律计入「非证监会分类（不可映射）」桶，
   **不猜测映射、不强行归类**。混用基金的可归因部分会因此被低估。

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
import re
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.getcwd())
from config.settings import DATABASE_PATH

HERE = os.path.dirname(os.path.abspath(__file__))
MAP_PATH = os.path.join(HERE, 'data', 'sw_to_csrc_map.json')
GICS_MAP_PATH = os.path.join(HERE, 'data', 'gics_to_csrc_map.json')

# 报告期口径：动态取 etf_industry_alloc 中**最新且为日期格式**的 report_date。
#
# 历史背景（2026-10-14 修复，勿回退）：
#   原表 UNIQUE(code, industry) 缺 report_date，而东财 EM 源一次返回多期，
#   save_to_db 用 INSERT OR REPLACE 逐行覆盖 → 新报告期被旧报告期反向覆盖，
#   导致 06-30 只剩 7 行零碎补丁。修复后（refresh_etf_industry_alloc.py --rebuild
#   --refresh）06-30 已完整落库 226 行 / 30 只，覆盖率由 50.84% 提升至 88.65%。
#
# 必须过滤非日期格式的 report_date（如 'neodata_latest'）：
#   字符串比较下 'neodata_latest' > '2026-xx-xx'，若不过滤会被 MAX() 选中，
#   而其只有笼统的「其他」一行，将导致归因完全失真。
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# 证监会门类白名单（A 股标准分类）。凡不在此集合内的行业标签，一律视为
# 非证监会分类体系（多为 QDII/港美股持仓的 GICS 风格标签），计入不可映射桶。
# 依据：中国证监会《上市公司行业分类指引》门类。
CSRC_SECTORS = {
    '农、林、牧、渔业',
    '采矿业',
    '制造业',
    '电力、热力、燃气及水生产和供应业',
    '建筑业',
    '批发和零售业',
    '交通运输、仓储和邮政业',
    '住宿和餐饮业',
    '信息传输、软件和信息技术服务业',
    '金融业',
    '房地产业',
    '租赁和商务服务业',
    '科学研究和技术服务业',
    '水利、环境和公共设施管理业',
    '教育',
    '卫生和社会工作',
    '文化、体育和娱乐业',
    '综合',
}

# 场内 ETF canonical 23 只（与 sync_rotation_to_db.py / 「ETF OHLCV 全量兜底巡检」
# 自动化保持一致）。用于把「未归因」进一步拆分为「场外基金」与「场内 ETF 缺门类数据」。
CANONICAL_ETF = {
    '159220', '159267', '159300', '159650', '159732', '159770', '159796', '159819',
    '159949', '159992', '510300', '510500', '511380', '511520', '512010', '512100',
    '512810', '515010', '515120', '516160', '561910', '563020', '588000',
}

# 债券类 ETF：业务性质上无股票行业配置，缺数据属预期，非采集缺陷。
BOND_ETF = {'511380', '511520', '159650'}


def load_map():
    with open(MAP_PATH, encoding='utf-8') as fh:
        return json.load(fh)


def load_gics_map():
    """加载 GICS 风格标签 → 证监会门类映射。返回 (map, aliases)。"""
    if not os.path.exists(GICS_MAP_PATH):
        return {}, {}
    with open(GICS_MAP_PATH, encoding='utf-8') as fh:
        j = json.load(fh)
    return j.get('map', {}), j.get('aliases', {})


def _max_date(cur, table, col):
    cur.execute(f"SELECT MAX({col}) FROM {table}")
    r = cur.fetchone()
    return r[0] if r else None


def _latest_alloc_report_date(cur):
    """取 etf_industry_alloc 中最新且为日期格式的 report_date。

    过滤 neodata_latest 等非日期值：字符串比较下它们大于任何 'YYYY-MM-DD'，
    不过滤会被 MAX() 误选，而其数据为笼统的「其他」单行业行，将致归因失真。
    """
    cur.execute("SELECT DISTINCT report_date FROM etf_industry_alloc")
    dates = [r[0] for r in cur.fetchall() if r[0] and DATE_RE.match(str(r[0]))]
    return sorted(dates)[-1] if dates else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='组合快照日（默认取 portfolio_snapshots 最新日）')
    ap.add_argument('--verify', action='store_true', help='打印校验信息')
    ap.add_argument('--report', action='store_true',
                    help='额外落盘 markdown 报告到 research/sector_rotation/')
    ap.add_argument('--no-gics-map', action='store_true',
                    help='关闭 GICS 标签→证监会门类的二次映射（退回全部计入不可映射桶）')
    args = ap.parse_args()

    mj = load_map()
    sw2csrc = {k: v for k, v in mj['map'].items()}
    fallback = mj['unmapped_fallback']['csrc']

    gics_map, gics_aliases = load_gics_map()
    gics_enabled = (not args.no_gics_map) and bool(gics_map)

    con = sqlite3.connect(str(DATABASE_PATH))
    cur = con.cursor()

    # ---------- 1. 组合侧：持仓市值 → 门类暴露 ----------
    # 【归因基准日的选择 —— 必须同日口径】
    # 组合快照由 15:30 主分析写入（当日盘中即可入库），而投研线行业行情
    # sector_fund_flow 要等 18:15 才同步当日。若直接取 MAX(快照日)，会在
    # 15:30~18:15 之间出现「当日持仓 × 前一日行情」的跨日期拼接 —— 这正是
    # 项目历史上「portfolio_summary 与 portfolio_snapshots 跨日期拼接、
    # 金额差 6735 元」同类陷阱。故默认以**行情日**为基准，保证同日自洽。
    snap_max = _max_date(cur, 'portfolio_snapshots', 'date')
    mkt_date = _max_date(cur, 'sector_fund_flow', 'date')
    if not snap_max:
        print('[ERROR] portfolio_snapshots 无数据')
        return 2

    if args.date:
        snap_date, date_note = args.date, '命令行 --date 指定'
    elif mkt_date and mkt_date <= snap_max and \
            cur.execute("SELECT COUNT(*) FROM portfolio_snapshots WHERE date=?",
                        (mkt_date,)).fetchone()[0] > 0:
        snap_date = mkt_date
        date_note = ('同日口径：以行情日 %s 为准' % mkt_date)
        if mkt_date != snap_max:
            date_note += ('（快照已更新至 %s，行情仅到 %s，已回退到行情日避免跨期拼接）'
                          % (snap_max, mkt_date))
    else:
        snap_date, date_note = snap_max, '回退到快照最新日 %s（无可用行情日）' % snap_max

    alloc_rd = _latest_alloc_report_date(cur)
    if not alloc_rd:
        print('[ERROR] etf_industry_alloc 无有效日期格式的 report_date')
        return 2

    cur.execute("SELECT code, SUM(market_value) FROM portfolio_snapshots "
                "WHERE date=? GROUP BY code", (snap_date,))
    holdings = [(r[0], r[1] or 0.0) for r in cur.fetchall() if (r[1] or 0) > 0]
    total_mv = sum(v for _, v in holdings)
    if not total_mv:
        print('[ERROR] 快照日无正市值持仓')
        return 2

    cur.execute("SELECT code, industry, weight_pct FROM etf_industry_alloc "
                "WHERE report_date=?", (alloc_rd,))
    alloc = defaultdict(list)
    for code, industry, w in cur.fetchall():
        if w is None:
            continue
        alloc[code].append((industry, float(w)))

    exposure = defaultdict(float)     # csrc -> 组合暴露 %（结构快照口径，含 GICS 映射）
    base = defaultdict(float)         # csrc -> 可用于计算贡献的暴露（剔除跨市场错配）
    gics_exp = defaultdict(float)     # csrc -> 其中来自 GICS 二次映射的暴露
    offshore_exp = defaultdict(float)  # csrc -> 纯境外持仓贡献的暴露（结构可见，贡献不可用）
    unattr = defaultdict(float)       # 原因 -> 未归因占比
    non_csrc = defaultdict(lambda: defaultdict(float))  # code -> 非证监会标签 -> 权重%
    unmap_tags = defaultdict(float)   # 未命中映射的标签 -> 权重合计
    mixed_codes = {}                  # code -> 明细 dict
    offshore_codes = set()            # 非证监会权重占比 > 90%（视为纯境外持仓）
    for code, mv in holdings:
        pct = mv / total_mv * 100
        if code not in alloc:
            if code in BOND_ETF:
                unattr['债券类 ETF（业务性质无股票行业配置）'] += pct
            elif code in CANONICAL_ETF:
                unattr['场内 ETF 缺门类数据（etf_industry_alloc 未采集）'] += pct
            else:
                unattr['场外基金（无门类配置数据）'] += pct
            continue

        rows_alloc = alloc[code]
        in_sum = sum(w for ind, w in rows_alloc if ind in CSRC_SECTORS)
        out_sum = sum(w for ind, w in rows_alloc if ind not in CSRC_SECTORS)
        total_w = in_sum + out_sum
        # 纯境外持仓（如港股通 ETF）：当地市场与 A 股申万不联动，贡献数字不成立
        is_off = total_w > 0 and (out_sum / total_w) > 0.9
        if is_off:
            offshore_codes.add(code)

        mapped_sum = unmapped_sum = 0.0
        for industry, w in rows_alloc:
            contrib_w = pct * (w / 100.0)
            if industry in CSRC_SECTORS:
                exposure[industry] += contrib_w
                base[industry] += contrib_w
                continue
            non_csrc[code][industry] += w
            key = gics_aliases.get(industry, industry)
            m = gics_map.get(key) if gics_enabled else None
            if m and m.get('csrc'):
                t = m['csrc']
                exposure[t] += contrib_w
                gics_exp[t] += contrib_w
                mapped_sum += w
                if is_off:
                    offshore_exp[t] += contrib_w      # 结构可见，但贡献不可用
                else:
                    base[t] += contrib_w
            else:
                unmapped_sum += w
                unmap_tags[key] += w

        if out_sum > 0.5:             # 仅记录实质混用的，忽略 0.0 占位噪声
            mixed_codes[code] = {
                'csrc': in_sum, 'non_csrc': out_sum,
                'mapped': mapped_sum, 'unmapped': unmapped_sum, 'offshore': is_off,
            }
        if unmapped_sum > 0:
            unattr['非证监会标签且无映射（不可映射）'] += pct * (unmapped_sum / 100.0)

    # ---------- 2. 市场侧：申万一级 → 门类聚合 ----------
    # mkt_date 已在第 1 步提前取得（用于选择归因基准日），此处不再重复取
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
        b = base.get(c, 0.0)
        # 贡献只基于 base（已剔除纯境外持仓的跨市场错配部分）
        contrib = (b * mkt_chg / 100.0) if (mkt_chg is not None and b) else None
        worst = max(a['conf'], key=lambda x: conf_rank.get(x, 9)) if a['conf'] else 'none'
        rows.append({
            'csrc_sector': c,
            'exposure_pct': round(exp, 4),
            'base_pct': round(b, 4),
            'gics_exposure_pct': round(gics_exp.get(c, 0.0), 4),
            'offshore_exposure_pct': round(offshore_exp.get(c, 0.0), 4),
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
    # 增量加列（老库平滑升级）：GICS 映射引入的新字段
    _cols = {r[1] for r in cur.execute("PRAGMA table_info(csrc_sector_exposure)")}
    for col, typ in (('base_pct', 'REAL'), ('gics_exposure_pct', 'REAL'),
                     ('offshore_exposure_pct', 'REAL')):
        if col not in _cols:
            cur.execute(f"ALTER TABLE csrc_sector_exposure ADD COLUMN {col} {typ}")
    for r in rows:
        cur.execute("""
            INSERT OR REPLACE INTO csrc_sector_exposure
            (date, mkt_date, csrc_sector, exposure_pct, base_pct, gics_exposure_pct,
             offshore_exposure_pct, mkt_change_pct, mkt_fund_flow_yi,
             contribution_pct, sw_members, map_confidence, alloc_report_date, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (snap_date, mkt_date, r['csrc_sector'], r['exposure_pct'], r['base_pct'],
             r['gics_exposure_pct'], r['offshore_exposure_pct'],
             r['mkt_change_pct'], r['mkt_fund_flow_yi'], r['contribution_pct'],
             json.dumps(r['sw_members'], ensure_ascii=False), r['map_confidence'],
             alloc_rd, 'portfolio_snapshots+etf_industry_alloc+sector_fund_flow'))
    con.commit()

    # ---------- 5. 输出 ----------
    unattr_total = sum(unattr.values())
    attr_total = sum(r['exposure_pct'] for r in rows)
    eff = [r for r in rows if r['exposure_pct'] > 0 and r['mkt_change_pct'] is not None]
    gap = [r for r in rows if r['exposure_pct'] > 0 and r['mkt_change_pct'] is None]
    eff_exp = sum(r['exposure_pct'] for r in eff)
    gap_exp = sum(r['exposure_pct'] for r in gap)
    contrib_sum = sum(r['contribution_pct'] for r in eff)
    base_total = sum(r['base_pct'] for r in eff)   # 仅可对照门类，即真正参与贡献的部分

    # 真值对照
    cur.execute("SELECT daily_return FROM portfolio_summary WHERE date=?", (snap_date,))
    _r = cur.fetchone()
    true_ret = float(_r[0]) if (_r and _r[0] is not None) else None

    # 滞后月数（用于口径披露）
    try:
        from datetime import date as _date
        _y1, _m1 = int(alloc_rd[:4]), int(alloc_rd[5:7])
        _y2, _m2 = int(snap_date[:4]), int(snap_date[5:7])
        lag_months = (_y2 - _y1) * 12 + (_m2 - _m1)
    except Exception:
        lag_months = None
    lag_txt = f'{lag_months} 个月' if lag_months is not None else '未知'

    print(f"\n{'='*82}")
    print(f"组合行业暴露归因（证监会门类级）　归因基准日 {snap_date}　行业行情日 {mkt_date}")
    print(f"持仓结构口径：etf_industry_alloc.report_date = {alloc_rd}（滞后约 {lag_txt}）")
    print(f"基准日选择：{date_note}")
    if snap_date != mkt_date:
        print(f"⚠ 持仓日({snap_date}) ≠ 行情日({mkt_date})，两者跨期，"
              f"以下贡献数字不成立，仅供参考")
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
    print(f"{'有效归因（可对照市场）':<26}{eff_exp:>8.2f}   ← 结构覆盖口径")
    print(f"{'其中：贡献计算基数':<26}{base_total:>8.2f}   ← 已剔除跨市场错配部分")
    print(f"{'已归入门类但无市场数据':<26}{gap_exp:>8.2f}   ← 映射缺口")
    print(f"{'未归因':<26}{unattr_total:>8.2f}")
    for reason, v in sorted(unattr.items(), key=lambda x: -x[1]):
        print(f"  └ {reason}: {v:.2f}%")

    print(f"\n【与真实回报对照】")
    if true_ret is not None:
        print(f"  组合真实当日回报（portfolio_summary.daily_return）：{true_ret:+.4f}%")
        print(f"  归因模型贡献合计（基数 {base_total:.1f}%，非覆盖 {eff_exp:.1f}%）："
              f"{contrib_sum:+.4f} pct")
        diff = contrib_sum - true_ret
        print(f"  偏差：{diff:+.4f} pct" +
              ("　⚠ **方向相反**" if (contrib_sum * true_ret < 0) else ""))
        if abs(diff) < 0.05 and (contrib_sum * true_ret > 0):
            print(f"  ✓ 偏差在 0.05pct 内且方向一致，可用于**结构性解释**（非精确盈亏归因）。")
        else:
            print(f"  ⚠ 结论：当前归因**不可用于解释当日盈亏**——覆盖不足（{eff_exp:.1f}%）、")
            print(f"    持仓结构滞后约 {lag_txt}、门类内等权平均为近似，三者叠加导致方向性偏差。")
            print(f"    本产物当前定位：**门类级结构快照**（看「钱在哪个门类」），非盈亏归因。")
    else:
        print(f"  portfolio_summary {snap_date} 无 daily_return，无法对照")

    print(f"\n【口径与偏差披露】")
    print(f"  · 门类涨跌幅 = 门类内申万一级行业的**等权平均**（缺门类内行业权重，属近似，非真实加权）")
    print(f"  · 门类资金流 = 门类内申万一级行业主力净流入**求和**（亿元）")
    print(f"  · 持仓结构为 report_date={alloc_rd} 季报口径（滞后约 {lag_txt}）")
    low = [r['csrc_sector'] for r in rows if r['map_confidence'] == 'low']
    if low:
        print(f"  · ⚠ 含低置信映射的门类（跨门类拆分，归因存在已知偏差）：{('、'.join(low))}")
    if mixed_codes:
        tag = '已启用 GICS 二次映射' if gics_enabled else '已关闭映射(--no-gics-map)'
        print(f"  · ⚠ 分类体系混用基金（含 QDII/港股 GICS 风格标签，{tag}）：")
        for code, d in sorted(mixed_codes.items(), key=lambda x: -x[1]['non_csrc']):
            tags = '、'.join(sorted(non_csrc[code], key=lambda k: -non_csrc[code][k])[:6])
            flag = '　🌏 纯境外持仓→贡献不可用' if d['offshore'] else ''
            print(f"      {code}：证监会 {d['csrc']:.2f}% + 非证监会 {d['non_csrc']:.2f}%"
                  f"（已映射 {d['mapped']:.2f}% / 不可映射 {d['unmapped']:.2f}%）{flag}")
            print(f"         标签例：{tags}")
    if gics_exp:
        tot_g = sum(gics_exp.values())
        tot_o = sum(offshore_exp.values())
        print(f"  · ⚠ GICS 二次映射引入暴露 {tot_g:.2f}%（**二级归类，误差累积**）；"
              f"其中跨市场错配 {tot_o:.2f}% 已剔除出贡献计算")
        print(f"    跨市场错配含义：这些持仓在港股/境外市场，与 A 股申万行业涨跌不联动，"
              f"用 A 股行情归因不成立。")
    if unmap_tags:
        print(f"  · ⚠ 未命中映射的标签（保留在不可映射桶，不猜测）："
              f"{('、'.join(sorted(unmap_tags, key=lambda k: -unmap_tags[k])))}")
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
            fh.write(f"- 持仓结构口径：`etf_industry_alloc.report_date = {alloc_rd}`（季报，滞后约 {lag_txt}）\n")
            fh.write(f"- 有效归因（可对照市场）：**{eff_exp:.2f}%**　映射缺口：{gap_exp:.2f}%　未归因：{unattr_total:.2f}%\n\n")
            if true_ret is not None:
                fh.write(f"## 与真实回报对照\n\n")
                fh.write(f"- 组合真实当日回报：**{true_ret:+.4f}%**\n")
                fh.write(f"- 归因模型贡献合计（覆盖 {eff_exp:.1f}%）：{contrib_sum:+.4f} pct\n")
                fh.write(f"- 偏差：{contrib_sum - true_ret:+.4f} pct"
                         + ("（**方向相反**）\n" if contrib_sum * true_ret < 0 else "\n"))
                if abs(contrib_sum - true_ret) < 0.05 and (contrib_sum * true_ret > 0):
                    fh.write(f"\n> ✓ 偏差在 0.05pct 内且方向一致，可用于**结构性解释**（非精确盈亏归因）。\n\n")
                else:
                    fh.write(f"\n> ⚠ 当前归因**不可用于解释当日盈亏**。覆盖不足（{eff_exp:.1f}%）、\n")
                    fh.write(f"> 持仓滞后约 {lag_txt}、门类内等权平均为近似，三者叠加导致方向性偏差。\n")
                    fh.write(f"> 本产物定位为**门类级结构快照**（看「钱在哪个门类」），非盈亏归因。\n\n")
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
            fh.write(f"3. 持仓结构为季报口径，**滞后约 {lag_txt}**；\n")
            fh.write("4. 未归因部分（场外基金 + 债券类 ETF + 采集失败代码）**不摊派、不编造**；\n")
            fh.write(f"5. 低置信映射门类：{('、'.join(low)) if low else '无'}；\n")
            fh.write(f"6. 映射缺口门类：{('、'.join(r['csrc_sector'] for r in gap)) if gap else '无'}，市场数据一律 NULL，不猜测替代；\n")
            if mixed_codes:
                act = ('已通过 GICS→证监会二次映射并入门类'
                       if gics_enabled else '未启用二次映射，全部计入不可映射')
                fh.write(f"7. 分类体系混用基金（QDII/港股 GICS 标签）{act}，明细如下：\n\n")
                fh.write("| 代码 | 证监会权重% | 非证监会权重% | 已映射% | 不可映射% | 纯境外持仓 | 标签（例） |\n"
                         "|---|---|---|---|---|---|---|\n")
                for code, d in sorted(mixed_codes.items(), key=lambda x: -x[1]['non_csrc']):
                    tags = '、'.join(sorted(non_csrc[code], key=lambda k: -non_csrc[code][k])[:6])
                    off = '是（贡献不可用）' if d['offshore'] else '否'
                    fh.write(f"| {code} | {d['csrc']:.2f} | {d['non_csrc']:.2f} | "
                             f"{d['mapped']:.2f} | {d['unmapped']:.2f} | {off} | {tags} |\n")
                fh.write("\n")
            if gics_exp:
                fh.write(f"8. **跨市场口径错配（关键）**：GICS 二次映射引入暴露 "
                         f"{sum(gics_exp.values()):.2f}%，其中 {sum(offshore_exp.values()):.2f}% "
                         f"来自纯境外持仓基金（实测 159220 港股通红利低波ETF，98.70% 为港股）。\n"
                         f"   港股与 A 股申万行业涨跌**不联动**，该部分仅计入结构快照，"
                         f"**不参与贡献计算**。\n")
        print(f"\n[REPORT] {p}")

    con.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
