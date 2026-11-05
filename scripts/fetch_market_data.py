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
import subprocess, json, re, os, sqlite3, sys, time
from datetime import datetime

# 项目根目录按脚本位置推导，避免硬编码迁移前的绝对路径
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 使用当前解释器（项目 venv313），避免硬编码 WorkBuddy 私有 python 路径
PY = sys.executable
# neodata-financial-search skill 路径按当前用户主目录推导，避免硬编码用户名
QS = os.path.join(os.path.expanduser('~'), '.workbuddy', 'skills',
                  'neodata-financial-search', 'scripts', 'query.py')
OUT = os.path.join(ROOT, 'data', '.neotmp', 'neodata_market.json')
DB = os.path.join(ROOT, 'data', 'database', 'portfolio.db')

# 重仓相关板块（与 gen_combo_report.py 交叉验证键名保持一致）
HEAVY = ['航天装备Ⅱ', '军工电子Ⅱ', '地面兵装Ⅱ', '航空装备Ⅱ', '航海装备Ⅱ',
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
    # NeoData 凭证过期信息有时落在 stderr，需一并检测
    if 'TOKEN_MISSING' in r.stderr or 'TOKEN_EXPIRED' in r.stderr:
        raise TokenExpired(r.stderr[:160])
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


def run_query_retry(q, tries=2, delay=4.0):
    """带重试的查询（2026-10-30 P2-1 新增）：NeoData 语义搜索偶发 `empty apiRecall`
    （实测 CN 10Y 查询同一句式数分钟后即成功，属瞬时抖动非永久失败）。
    TokenExpired 立即上抛（不重试，交由外层走凭证刷新流程）；其余异常重试一次后
    仍失败则上抛，由调用方按「单点失败不阻断全链路」降级为「—」。"""
    last = None
    for i in range(tries):
        try:
            return run_query(q)
        except TokenExpired:
            raise
        except Exception as e:
            last = e
            if i < tries - 1:
                print(f"WARN query 第{i+1}次失败（{type(e).__name__}: {e}），{delay:.0f}s 后重试")
                time.sleep(delay)
    raise last


def run_query_first_ok(queries, tries=2, delay=4.0):
    """多句式顺序查询（2026-10-30 遗留项1 新增）。

    根因实证（2026-10-30 18:2x 五句式探测）：NeoData 语义搜索的召回对**句式**敏感——
    「中国10年期国债收益率最新值」自 17:12 起连续 4 次 empty apiRecall，而
    「中债10年期国债到期收益率」「10年期国债收益率 中国」「中国国债收益率 10年 最新」
    均稳定召回（10-29 值 1.6233，与 17:12 实测报文一致）。原判「NeoData 限流」被证伪：
    限流不会只限同族两条查询中的一条（美债同句式同时段成功）。
    同句式重试治不了召回漂移（非瞬时抖动），必须换句式 ⇒ 本函数按列表顺序尝试，
    每个句式内部仍走 run_query_retry（抖动重试一次）；全部失败才上抛。
    TokenExpired 立即上抛（凭证问题重试无意义，交由外层走凭证刷新流程）。"""
    last = None
    for q in queries:
        try:
            return run_query_retry(q, tries=tries, delay=delay)
        except TokenExpired:
            raise
        except Exception as e:
            last = e
            print(f"WARN 句式 [{q}] 失败（{type(e).__name__}: {str(e)[:80]}），尝试下一句式")
    raise last


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
    # 2026-10-19 健壮性修复：NeoData 偶发不返回「上涨家数占比全市场 X%」字面量
    # 时 up_pct 为 None，致下游 gen_combo_report 在 `100 - up_pct` 崩溃（exit1）。
    # 回退用 up/(up+down) 推算占比（非编造，属同口径派生），确保 schema 字段不缺失。
    elif d['up'] is not None and d['down'] is not None:
        tot = d['up'] + d['down']
        if tot:
            d['up_pct'] = round(d['up'] / tot * 100, 1)
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
    """解析 PMI 查询返回的 markdown（2026-10-30 重写；审计缺陷 P0-1 列位错位+名称来源错误）。

    NeoData 实际报文结构（data/.neotmp/fixtures/raw_pmi_20261030.txt 实证）：
      - 指标名在 `**指标名称**：XXX` 前缀行，**不在表格首列**（首列是「数据来源」）；
      - 表格五列 `数据来源|发布日期|统计截止日期|数值|单位` ⇒ **数值在 parts[3]**；
      - 同一指标名前缀下存在「…:季调:环比增减量:月」**诱饵行**（MoM 变化量，如 1.2），
        与水平值（如 50.2）仅差后缀，必须按名称显式排除；
      - 每个指标块首行=当期值、次行=前值（如 2026-09-30 / 2026-08-31）。

    原实现两处硬错导致全字段静默 None（数据其实一直在）：
      ① `float(parts[4])` 读的是单位列（'%'/'-'）⇒ 必败 ⇒ 整表丢弃；
      ② `name = parts[0]` 取的是「数据来源」（国家统计局/聚源计算），指标名匹配永不命中。
    """
    pmi = {k: None for k in ['mfg', 'mfg_prev', 'large', 'large_prev', 'mid', 'mid_prev',
                             'small', 'small_prev', 'hightech', 'hightech_prev',
                             'equip', 'equip_prev', 'composite', 'composite_prev',
                             'nonmfg', 'nonmfg_prev', 'service', 'service_prev',
                             'construction', 'construction_prev']}
    stat = None
    cur_name = None
    counts = {}

    def _assign(field, v):
        # 同一指标块首行=当期值、次行=前值；第三次及以后忽略（防御异常多行）
        n = counts.get(field, 0)
        if n == 0:
            pmi[field] = v
        elif n == 1:
            pmi[field + '_prev'] = v
        counts[field] = n + 1

    for line in content.splitlines():
        s = line.strip()
        m = re.match(r'\*\*指标名称\*\*：(.+)$', s)
        if m:
            cur_name = m.group(1).strip()
            continue
        if not s.startswith('|') or cur_name is None:
            continue
        parts = [x.strip() for x in s.strip('|').split('|')]
        if len(parts) < 5 or not parts[0]:
            continue
        if stat is None and re.match(r'\d{14}', parts[2] or ''):
            stat = parts[2][:8]
        try:
            fv = float(parts[3])   # 🔴 数值列：index 3（原读 index 4=单位列，全表静默丢弃）
        except Exception:
            continue               # 表头/分隔行/非数值行
        name = cur_name            # 🔴 指标名：取自 `**指标名称**：` 前缀行（原取表格首列=数据来源）
        if '环比增减量' in name:
            continue               # 诱饵行：环比变化量，非景气水平值
        if '制造业PMI' in name and '非制造业' not in name:
            # 注意 '非制造业PMI' 含子串 '制造业PMI'，必须显式排除
            if '大型' in name: _assign('large', fv)
            elif '中型' in name: _assign('mid', fv)
            elif '小型' in name: _assign('small', fv)
            elif '高技术' in name: _assign('hightech', fv)
            elif '装备' in name: _assign('equip', fv)
            else: _assign('mfg', fv)
        elif '综合PMI' in name:
            _assign('composite', fv)
        elif '非制造业PMI' in name:
            if '服务业' in name and '商务活动' in name: _assign('service', fv)
            elif '建筑业' in name and '商务活动' in name: _assign('construction', fv)
            elif '商务活动' in name: _assign('nonmfg', fv)
        elif 'PMI:' in name or name.startswith('PMI'):
            # 央行口径高技术/装备制造业（名为 'PMI:高技术制造业:月'，不含 '制造业PMI' 子串）
            if '高技术' in name: _assign('hightech', fv)
            elif '装备' in name: _assign('equip', fv)
    if stat:
        y, m, d = stat[:4], stat[4:6], stat[6:8]
        pmi['stat'] = f"{y}-{m}（国家统计局/央行，发布 {y}-{m}-{d}）"
    else:
        pmi['stat'] = None
    return pmi


def parse_rate(content, key_substr):
    """解析利率类指标（2026-10-30 审计 P2-1；表头 `查询关键词|数据日期|[发布日期|]数据单位|数据值`）。

    与 parse_pmi 的五列固定结构不同：利率表**列数不固定**（中债带「发布日期」列、美债不带），
    故按**表头名**定位「数据日期」「数据值」列下标——禁硬编码列位（parse_pmi 的旧伤
    正是硬编码 parts[4] 撞上单位列导致全表静默丢弃）。
    只解析指标名含 key_substr 的块（语义搜索会顺带召回大量邻近期限，如 2个月/15年/20年）。
    返回 (值, 数据日期, 前值, 前值日期)；行序为日期降序（最新在前）。
    """
    result = (None, None, None, None)
    cur_name, val_idx, date_idx, rows = None, None, None, []

    def _finalize():
        nonlocal result
        if cur_name and key_substr in cur_name and rows and result[0] is None:
            result = (rows[0][1], rows[0][0],
                      rows[1][1] if len(rows) > 1 else None,
                      rows[1][0] if len(rows) > 1 else None)

    for line in content.splitlines():
        s = line.strip()
        m = re.match(r'\*\*指标名称\*\*：(.+)$', s)
        if m:
            _finalize()                     # 收尾上一指标块（块间不连续，勿混行）
            cur_name = m.group(1).strip()
            val_idx = date_idx = None
            rows = []
            continue
        if not s.startswith('|'):
            continue
        parts = [x.strip() for x in s.strip('|').split('|')]
        if '数据值' in parts and '数据日期' in parts:
            val_idx = parts.index('数据值')
            date_idx = parts.index('数据日期')
            continue
        if val_idx is None or cur_name is None or key_substr not in cur_name:
            continue
        if len(parts) <= max(val_idx, date_idx):
            continue
        d = parts[date_idx]
        if not re.match(r'\d{4}-\d{2}-\d{2}', d):
            continue
        try:
            v = float(parts[val_idx])
        except Exception:
            continue
        rows.append((d, v))
    _finalize()
    return result


def parse_heavy(content, today):
    """从 K 线 content 取当日涨跌幅（%）。返回 (值, 实际归属日期)（2026-10-30 审计 P1-1）。

    原实现的日期错位缺陷：调用方传入的 `today` 实为 `local_latest_date()`（fund_flows
    sector 类 MAX=2026-10-15，该类自 10-15 停摆），与 K 线响应中的真实日期（10-29）永不相等；
    仅靠「rows[-1]」兜底才碰巧正确——若响应含多日行且行序非日期升序，就会静默返回非当日值。
    现改为：① 优先精确匹配运行日；② 未命中时取响应中**日期最大**的一行（不假设行序）；
    ③ 返回该行日期，由调用方在 heavy_sector_pct_date 中标注实际归属日（≠运行日即显式告警）。
    """
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
        if parts[1] in ('-', '未开盘') or parts[7] in ('-', '', '—'):
            continue
        try:
            # 涨跌幅列可能带 % 后缀（NeoData 格式变动），需先剥离
            chg_str = parts[7].replace('%', '').replace(',', '').strip()
            chg = float(chg_str)
        except Exception:
            continue
        rows.append((d, chg))
    for d, chg in rows:
        if d == today:
            return chg, d
    if rows:
        best = max(rows, key=lambda x: x[0])
        return best[1], best[0]
    return None, None


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


def local_main_fund_proxy():
    """90 申万板块合计作为全市场主力资金代理。返回 (值_亿元, 实际数据日)（2026-10-30 审计 P0-6）。

    原实现按调用方传入的 date 查询——而该 date 是 sector 类 MAX（停更于 10-15），
    于是 10-15 的合计被当作「今日」主力资金写进报告（双口径混同：fetch 用 sector-only
    MAX 算值、gen 用全类别 MAX 标注）。现改为函数内自取 sector 类真实 MAX 并随值返回，
    下游必须把该日期与代理值一起呈现，禁止把陈旧合计讲成当日值。
    sector 类自 2026-10-15 起断采（同花顺接口网络层阻断，见 src/data_sources/fund_flow.py
    fetch_sector_fund_flow 注释）⇒ 数据日将长期停留在 10-15，属预期状态而非新故障。
    """
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT MAX(date) FROM fund_flows WHERE category='sector'")
    d = cur.fetchone()[0]
    if not d:
        con.close()
        return None, None
    # 🔴 同日单源优先（2026-10-30 人工补数连带）：sector 类可能同日混有 ths/NULL（主线采集）
    #    与 westock_mcp（交互会话人工补数）行。若直接 SUM 当日全表，ths 恢复回填后两块
    #    股票 universe 高度重叠的板块集被重复计入 ⇒ 代理值虚高。按源优先级只合计单一来源。
    cur.execute("SELECT source, SUM(net_inflow) FROM fund_flows WHERE date=? AND category='sector' "
                "GROUP BY source", (d,))
    _by_src = cur.fetchall()
    con.close()
    if not _by_src:
        return None, d
    _PRI = {'ths': 0, None: 0, 'westock_mcp': 1, 'neodata_mcp': 2}
    s = min(_by_src, key=lambda r: (_PRI.get(r[0], 9), str(r[0])))[1]
    return (round(s / 1e8, 1) if s else None), d


def main():
    today = datetime.now().strftime('%Y-%m-%d')
    # 2026-10-30（审计 P1-1）：原 TODAY = local_latest_date()（fund_flows sector 类 MAX）。
    # 该类自 10-15 停摆 ⇒ TODAY 恒为 10-15，与「今日」K 线响应的真实日期（10-29）永不相等，
    # parse_heavy 的 d==today 匹配全程落空、仅靠 rows[-1] 兜底。改为直接用运行日；
    # 响应非当日行时由 parse_heavy 返回实际归属日并在此处显式告警（heavy_date）。
    TODAY = today
    breadth_local = local_breadth_local()

    # 2026-10-10 健壮性修复：三条主查询各自独立容错，单条失败（如 PMI 偶发
    # empty apiRecall）不再中断整次 fetch，缺失字段交由下游以「—」诚实呈现
    # （承接 evolution #7 PMI 整体值长期难取、#43 单点查询失败不应阻断全链路）。
    try:
        cb = run_query("今日A股涨跌家数（上涨、下跌、平盘各多少家）、涨停数、跌停数、上涨家数占比、"
                       "两市合计成交额（亿元）及较昨日变化量")
    except TokenExpired:
        print("TOKEN_EXPIRED"); sys.exit(2)
    except Exception as e:
        cb = ""; print(f"WARN breadth query failed: {e}")
    try:
        cg = run_query("今日申万一级行业板块涨幅排行前10（板块名称与涨跌幅%）")
    except TokenExpired:
        print("TOKEN_EXPIRED"); sys.exit(2)
    except Exception as e:
        cg = ""; print(f"WARN gainers query failed: {e}")
    try:
        cp = run_query_retry("最新一期中国官方制造业PMI、综合PMI产出指数、非制造业商务活动PMI、"
                             "服务业PMI、建筑业PMI及统计发布日期")
    except TokenExpired:
        print("TOKEN_EXPIRED"); sys.exit(2)
    except Exception as e:
        cp = ""; print(f"WARN pmi query failed (将标「—」): {e}")
    # 2026-10-30（审计 P0-2）：制造业分规模值（整体/大/中/小型）在主查询的语义召回 top10 里
    # 稳定缺席（fixture 实证：主查询返回的 10 个标的全为非制造业/综合变体），需专用结构化
    # 查询补齐。失败不阻断（evolution #43：单点查询失败不应阻断全链路），字段留 None。
    try:
        cm = run_query_retry("制造业PMI整体及大型企业、中型企业、小型企业PMI月度值最新一期")
    except TokenExpired:
        print("TOKEN_EXPIRED"); sys.exit(2)
    except Exception as e:
        cm = ""; print(f"WARN mfg pmi query failed (将标「—」): {e}")
    # 2026-10-30（审计 P2-1）：利率维度本地 macro_daily 从未采集 ⇒ 1.6 利率行恒「—」。
    # NeoData 全球利率库可取：中债 10Y 为**日频**（新），美债 10Y 仅**月末值**（09-30）——
    # 两腿数据日不同 ⇒ 下游利差必须按口径混算值显式标注，禁静默相减。
    # 2026-10-30 18:2x 遗留项1：CN 10Y 连续 4 次 empty apiRecall，根因为查询句式语义召回
    # 为空（非限流——美债同句式同时段成功）。改多句式回退：主句式「中债10年期国债
    # 到期收益率」实测稳定召回，原句式降为末位备选（召回漂移时可能恢复）。
    try:
        cr = run_query_first_ok([
            "中债10年期国债到期收益率",
            "10年期国债收益率 中国",
            "中国10年期国债收益率最新值",
        ])
    except TokenExpired:
        print("TOKEN_EXPIRED"); sys.exit(2)
    except Exception as e:
        cr = ""; print(f"WARN cn rate query failed (将标「—」): {e}")
    try:
        ur = run_query_first_ok([
            "美国10年期国债收益率最新值",
            "美国10年期国债到期收益率",
        ])
    except TokenExpired:
        print("TOKEN_EXPIRED"); sys.exit(2)
    except Exception as e:
        ur = ""; print(f"WARN us rate query failed (将标「—」): {e}")

    breadth = parse_breadth(cb)
    gainers = parse_gainers(cg)
    pmi = parse_pmi(cp)
    pmi_m = parse_pmi(cm)
    # 合并制造业专用查询：仅填补主查询未取到的字段，绝不覆盖主查询真值
    for _k in ['mfg', 'mfg_prev', 'large', 'large_prev', 'mid', 'mid_prev',
               'small', 'small_prev', 'hightech', 'hightech_prev', 'equip', 'equip_prev']:
        if pmi.get(_k) is None and pmi_m.get(_k) is not None:
            pmi[_k] = pmi_m[_k]
    if pmi.get('stat') is None:
        pmi['stat'] = pmi_m.get('stat')
    cn10y, cn10y_date, cn10y_prev, cn10y_prev_date = parse_rate(cr, '10年')
    us10y, us10y_date, us10y_prev, us10y_prev_date = parse_rate(ur, '10年')
    if cn10y_date and us10y_date and cn10y_date != us10y_date:
        print(f"WARN 利差两腿数据日不同（中 {cn10y_date} / 美 {us10y_date}）⇒ 下游按口径混算值标注")
    qtime = query_time_of(cb)

    heavy = {}
    heavy_date = {}
    for s in HEAVY:
        try:
            # 2026-10-30（审计 P1-1）：原句式「今日申万行业板块 X 的涨跌幅（%）」对 4 个板块
            # （化学制药/医疗服务/电池/光伏设备）返回的是涨幅排行表而非 K 线表 ⇒ 解析恒 None
            # ⇒ sector_daily_change 每日仅 9 行 < 12 行门槛 ⇒ 军工 streak 自 10-20 静默冻结。
            # 实测显式 K 线句式可稳定取到 dated 行（化学制药 10-29 = -1.18%）。
            ch = run_query_retry(f"申万行业板块 {s} 最近3个交易日K线 日期 收盘 涨跌幅")
            v, vd = parse_heavy(ch, today)
            heavy[s] = v
            heavy_date[s] = vd
            if vd and vd != today:
                print(f"WARN heavy date-mismatch {s}: 值归属 {vd} ≠ 运行日 {today}（接口返回非当日行）")
        except TokenExpired:
            print("TOKEN_EXPIRED")
            sys.exit(2)
        except Exception as e:
            heavy[s] = None
            heavy_date[s] = None
            print(f"WARN heavy parse {s}: {e}")

    # 2026-09-10 进化落地：heavy 板块全 None 自愈告警
    # 若 13 个重仓板块涨跌幅解析结果全为 None，疑似 NeoData 字段格式再次变动导致静默
    # 缺失（2026-09-09 曾因涨跌幅带 % 后缀被 float() 静默吞掉）。打印显式 WARN（不中止），
    # 报告将按数据纪律以「—」呈现，待人工核对，避免未来格式再变时无感知。
    if heavy and all(v is None for v in heavy.values()):
        print("WARN heavy_all_none: 13 个重仓板块涨跌幅解析结果全为 None，"
              "疑似 NeoData 字段格式再次变动导致静默缺失；报告将标「—」，请人工核对。")

    main_fund_yi, main_fund_date = local_main_fund_proxy()
    if main_fund_date and main_fund_date != today:
        print(f"WARN main_fund proxy 数据日 {main_fund_date} ≠ 运行日 {today}"
              f"（sector 类停更，代理值为陈旧合计，下游已要求显式标注）")

    # 主线文本（基于真实数据，不做主观判断）
    top3 = gainers[:3]
    top3_txt = '、'.join(f"{n}{p:+.2f}%" for n, p in top3)
    # 军工系集合须与 gen_combo_report.MIL_SECTORS 同口径（2026-10-27 修 evolution #2026-09-18）：
    #   原集合仅 4 个、漏掉 航天装备Ⅱ——该板块在 HEAVY 里一直有采（实测 10-26 为 -0.69%），
    #   漏掉会让「系内最优」报出非真实最优值（9-18 航天 +3.29 强于航海 +2.26 却被排除在 max 之外）。
    _MIL_ALL = ['地面兵装Ⅱ', '航空装备Ⅱ', '军工电子Ⅱ', '航海装备Ⅱ', '航天装备Ⅱ']
    mil_pairs = [(s, heavy.get(s)) for s in _MIL_ALL if heavy.get(s) is not None]
    if mil_pairs:
        best_name_s, mil_best = max(mil_pairs, key=lambda x: x[1])
        _mil_set_txt = '/'.join(_MIL_ALL)
        if mil_best < 0:
            # 全系为负时「最不差者」不是主线方向（2026-10-27 修 evolution #2026-09-28）：
            #   普跌日 max(heavy)<0 仍称「为当日主线方向」，会让快读读者误判该方向当日是正向主线。
            main_line = (f"板块涨幅居前：{top3_txt}。重仓相关板块中军工系（{_mil_set_txt}）"
                         f"最优涨跌幅 {mil_best:+.2f}%（{best_name_s}）——"
                         f"全系当日均为负值，属普跌中最抗跌方向，不可读作正向主线。")
        else:
            main_line = (f"板块涨幅居前：{top3_txt}。重仓相关板块中军工系（{_mil_set_txt}）"
                         f"最优涨跌幅 {mil_best:+.2f}%（{best_name_s}），为当日主线方向。")
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
        "heavy_sector_pct_date": heavy_date,
        "main_fund_yi": main_fund_yi,
        "main_fund_yi_date": main_fund_date,
        "main_fund_is_proxy": True,
        "main_line": main_line,
        "pmi": pmi,
        "rates": {
            "cn10y": cn10y, "cn10y_date": cn10y_date,
            "cn10y_prev": cn10y_prev, "cn10y_prev_date": cn10y_prev_date,
            "us10y": us10y, "us10y_date": us10y_date,
            "us10y_prev": us10y_prev, "us10y_prev_date": us10y_prev_date,
        },
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("OK", OUT)
    print("breadth:", breadth)
    print("gainers:", gainers)
    print("heavy:", heavy)
    print("heavy_date:", heavy_date)
    print("main_fund_yi(proxy):", main_fund_yi, "date:", main_fund_date)
    print("main_line:", main_line)
    print("pmi.stat:", pmi.get('stat'))
    print("pmi.mfg:", pmi.get('mfg'), "large:", pmi.get('large'), "mid:", pmi.get('mid'),
          "composite:", pmi.get('composite'), "nonmfg:", pmi.get('nonmfg'),
          "service:", pmi.get('service'), "construction:", pmi.get('construction'))
    print("rates cn10y:", cn10y, cn10y_date, "| us10y:", us10y, us10y_date)


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
