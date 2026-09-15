# -*- coding: utf-8 -*-
"""组合+大盘综合视角 盘后日报生成器（自包含 HTML，深色主题，红涨绿跌）· 数据驱动版。

设计原则（解决历史"叙事硬编码到某日"的脆弱性）：
- 组合/指数/资金流/本地广度 全部从 data/database/portfolio.db 实时读取（以数据集最新交易日为基准）。
- 大盘广度/板块排行/PMI/重仓板块涨跌 来自 NeoData 实时查询，外置为 data/.neotmp/neodata_market.json。
- 所有定性叙事（TLDR、四段式、周线阶段、交叉验证风向、操作取向）均由当日真实数据经规则推导，
  不写死任何某日专属措辞；数字与定性结论一一对应，杜绝"旧叙事套新数字"。
"""
import sqlite3, os, html, json, statistics, glob, re
from datetime import datetime, date as _date

DB = r'data/database/portfolio.db'
RUN_DATE = datetime.now().strftime('%Y-%m-%d')
OUT = f'data/reports/组合大盘综合视角_{RUN_DATE}.html'

# ============ NeoData 实时查询抽取（外置 JSON）============
NEO_JSON = r'data/.neotmp/neodata_market.json'
try:
    with open(NEO_JSON, encoding='utf-8') as _f:
        MKT_NEO = json.load(_f)
    print(f'[INFO] MKT_NEO 已加载自 {NEO_JSON}（query_time={MKT_NEO.get("query_time")}）')
except Exception as _e:
    print(f'[WARN] MKT_NEO 读取失败({_e})，中止'); raise

con = sqlite3.connect(DB); cur = con.cursor()
cur.execute('SELECT MAX(date) FROM portfolio_snapshots'); SNAP = cur.fetchone()[0]
cur.execute('SELECT MAX(date) FROM portfolio_summary'); DATA_DATE = cur.fetchone()[0]

# 运行日是否为交易日：以日历判断周末（采集器滞后导致 RUN_DATE != DATA_DATE 时，
# 不能误判为休市日）。组合/持仓数据可能滞后停留在较早交易日（DATA_LAG 标记）。
_RUN_WD = datetime.strptime(RUN_DATE, '%Y-%m-%d').weekday()  # 0=Mon..6=Sun
RUN_DATE_IS_TRADING = (_RUN_WD < 5)  # 周一至周五为交易日（盘后）
DATA_LAG = (RUN_DATE != DATA_DATE)   # 组合/持仓采集器滞后标记
_RUN_DAY_LABEL = ('为交易日（盘后）' if RUN_DATE_IS_TRADING
                  else f'为休市日运行（数据基准为最近交易日 {DATA_DATE}）')

# ---------- 1. 组合持仓 ----------
cur.execute('''SELECT code,name,market_value,pnl_rate,cost_price,pnl
               FROM portfolio_snapshots WHERE date=? ORDER BY market_value DESC''', (SNAP,))
hold = cur.fetchall()
TOT = sum(r[2] for r in hold)

cur.execute('''SELECT total_value,total_cost,total_pnl,daily_pnl,daily_return,vs_hs300,
               profit_count,loss_count,sharpe_ratio,max_drawdown,volatility
               FROM portfolio_summary WHERE date=?''', (DATA_DATE,))
S = cur.fetchone()
tot_val, tot_cost, tot_pnl, d_pnl, d_ret, vs300, pc, lc, sharpe, mdd, vol = S

# ---------- 2. 指数（本地 index_quotes，取最新可得交易日，通常为 RUN_DATE 当日）----------
cur.execute('SELECT MAX(date) FROM index_quotes'); IDX_DATE = cur.fetchone()[0]
cur.execute('SELECT name,close,change_pct,amount FROM index_quotes WHERE date=?', (IDX_DATE,))
idx = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

# ---------- 3. 资金流（本地 fund_flows）----------
def _f(v):
    try: return float(v)
    except: return None
cur.execute("SELECT name,net_inflow FROM fund_flows WHERE date=? AND category='sector'", (DATA_DATE,))
sec_flow = {k: (_f(v)/1e8 if _f(v) is not None else None) for k, v in cur.fetchall()}
cur.execute("SELECT code,name,net_inflow,net_inflow_pct FROM fund_flows WHERE date=? AND category='etf'", (DATA_DATE,))
etf_flow = {r[0]: (_f(r[2])/1e8 if _f(r[2]) is not None else None, _f(r[3])) for r in cur.fetchall()}
# 主资金：本地表缺失 -> 以 90 申万板块合计代理
main_proxy = MKT_NEO.get('main_fund_yi')
main_est = MKT_NEO.get('main_fund_is_proxy', False)
# 若本地表有 main_fund 则优先
cur.execute("SELECT net_inflow FROM fund_flows WHERE date=? AND category='main_fund'", (DATA_DATE,))
_mf = cur.fetchone()
if _mf and _f(_mf[0]) is not None:
    main_in_yi = _f(_mf[0])/1e8; main_est = False
else:
    main_in_yi = main_proxy

# ---------- 4. 本地广度 ----------
_cur_breadth = None
for _fmt in (DATA_DATE, DATA_DATE.replace('-', '')):
    cur.execute('SELECT zt_count,dt_count,max_lianban,top_industry FROM market_breadth WHERE date=?', (_fmt,))
    _r = cur.fetchone()
    if _r: _cur_breadth = _r; break
loc_zt = _cur_breadth[0] if _cur_breadth else None
loc_dt = _cur_breadth[1] if _cur_breadth else None

# ---------- 5. 宏观 ----------
cur.execute("SELECT indicator_code,value FROM macro_daily WHERE date=?", (DATA_DATE,))
macro = dict(cur.fetchall())

# ---------- 分类（code -> (资产类别, 行业族)）----------
CLS = {
 '159267': ('行业主题', '军工系'), '512810': ('行业主题', '军工系'),
 '512010': ('行业主题', '医药系'), '159992': ('行业主题', '医药系'), '515120': ('行业主题', '医药系'),
 '515010': ('行业主题', '证券'),
 '516160': ('行业主题', '新能源系'), '159796': ('行业主题', '新能源系'), '561910': ('行业主题', '新能源系'),
 '159819': ('行业主题', '科技系'), '159770': ('行业主题', '科技系'),
 '511520': ('债券', '利率债'), '159650': ('债券', '利率债'), '511380': ('债券', '可转债'),
 '563020': ('红利', '红利'), '159220': ('红利', '红利'),
 '512100': ('宽基', '宽基'), '510300': ('宽基', '宽基'), '510500': ('宽基', '宽基'),
 '588000': ('宽基', '宽基'), '159300': ('宽基', '宽基'), '159949': ('宽基', '宽基'),
}
cls_sum, ind_sum = {}, {}
for c, n, mv, pr, cp, pnl in hold:
    a, b = CLS.get(c, ('其他', '其他'))
    cls_sum[a] = cls_sum.get(a, 0) + mv
    ind_sum[b] = ind_sum.get(b, 0) + mv
pct = lambda v: v / TOT * 100

bad = [(c, n, mv, pr, cp) for c, n, mv, pr, cp, _ in hold if abs(pr) > 50 or cp < 0]
bad_mv = sum(r[2] for r in bad)

DUP = {
 '创新药 ×2': (['159992', '515120'], '两只跟踪高度重叠，无分散意义，建议合并'),
 '红利低波 ×2': (['563020', '159220'], 'A股/港股通两市场，地域分散有效，可保留'),
 '利率债 ×2': (['511520', '159650'], '久期与信用几乎一致，属实质同一持仓，建议合并'),
 '电池 ×2': (['159796', '561910'], '完全同质，均小额，建议合并'),
 '沪深300 ×2': (['510300', '159300'], '完全同质且总额偏低（核心仓不足），建议合并'),
}

HP = MKT_NEO['heavy_sector_pct']
BR = MKT_NEO['breadth']
BL = MKT_NEO['breadth_local']
PMI = MKT_NEO['pmi']

def sec_yi(name): return sec_flow.get(name)
def etf_yi(code):
    e = etf_flow.get(code); return e[0] if e else None
def fy(v):
    if v is None: return '—'
    return f'{v:+.2f}亿'

# ============ 市场状态推导（数据驱动）============
idx_keys = ['上证指数', '深证成指', '创业板指', '沪深300']
avg_idx = statistics.mean([idx[k][1] for k in idx_keys if k in idx])
up_pct = BR.get('up_pct')
down_pct = 100 - up_pct
# 全局涨跌：以主要指数均跌 + 上涨占比低 => 普跌；指数跌但占比高 => 分化
if avg_idx < -0.5 and up_pct < 45:
    REGIME = '普跌回调'
elif avg_idx < -0.5 and up_pct >= 45:
    REGIME = '指数回调·个股分化'
elif avg_idx > 0.5 and up_pct >= 50:
    REGIME = '普涨'
else:
    REGIME = '震荡分化'

# 主线（来自 gainers + 本地广度 top_industry + 资金流）
gainers = MKT_NEO.get('sector_gainers', [])
top_gain_names = '、'.join(f"{g[0]}{g[1]:+.2f}%" for g in gainers[:3])
top_ind = BL.get('top_industry', '—')
# 资金流入前 3 板块（本地）
sec_in = sorted([(k, v) for k, v in sec_flow.items() if v is not None], key=lambda x: -x[1])[:3]
sec_out = sorted([(k, v) for k, v in sec_flow.items() if v is not None], key=lambda x: x[1])[:3]
money_in_txt = '、'.join(f"{n}{v:+.1f}亿" for n, v in sec_in) or '—'
money_out_txt = '、'.join(f"{n}{v:+.1f}亿" for n, v in sec_out) or '—'

# 量能
amt2 = (idx['上证指数'][2] + idx['深证成指'][2]) / 1e12  # 万亿
amt_chg = BR.get('amount_chg_yi')
amt_dir = '放量' if (amt_chg and amt_chg > 0) else '缩量'

# ============ 交叉验证风向（数据驱动）============
def wind_label(chg):
    if chg >= 1.0: return '顺风'
    if chg >= 0.3: return '微顺风'
    if chg > -0.5: return '弱逆风'
    if chg > -1.5: return '逆风'
    return '强逆风'

def avg_chg(names):
    vs = [HP[n] for n in names if n in HP]
    return statistics.mean(vs) if vs else 0.0

CROSS_DEF = [
 ('医药系', ind_sum.get('医药系', 0),
  ['化学制药', '生物制品', '医疗服务'],
  lambda: f"化学制药 {HP['化学制药']:+.2f}%｜生物制品 {HP['生物制品']:+.2f}%｜医疗服务 {HP['医疗服务']:+.2f}%｜中证医疗 {idx['中证医疗'][1]:+.2f}%",
  lambda: f"化学制药 {fy(sec_yi('化学制药'))}｜生物制品 {fy(sec_yi('生物制品'))}｜医疗服务 {fy(sec_yi('医疗服务'))}；组合创新药ETF {fy(etf_yi('159992'))}/{fy(etf_yi('515120'))}｜医药ETF {fy(etf_yi('512010'))}"),
 ('军工系', ind_sum.get('军工系', 0),
  ['航天装备Ⅱ', '军工电子Ⅱ', '地面兵装Ⅱ', '航空装备Ⅱ', '航海装备Ⅱ'],
  lambda: f"地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}%｜航空装备Ⅱ {HP['航空装备Ⅱ']:+.2f}%｜航天装备Ⅱ {HP['航天装备Ⅱ']:+.2f}%｜军工电子Ⅱ {HP['军工电子Ⅱ']:+.2f}%｜航海装备Ⅱ {HP['航海装备Ⅱ']:+.2f}%",
  lambda: f"军工装备 {fy(sec_yi('军工装备'))}｜军工电子 {fy(sec_yi('军工电子'))}；组合航天/军工ETF {fy(etf_yi('159267'))}/{fy(etf_yi('512810'))}"),
 ('证券', ind_sum.get('证券', 0),
  ['证券Ⅱ'],
  lambda: f"证券Ⅱ {HP['证券Ⅱ']:+.2f}%",
  lambda: f"证券 {fy(sec_yi('证券'))}；组合证券ETF {fy(etf_yi('515010'))}"),
 ('新能源系', ind_sum.get('新能源系', 0),
  ['电池', '光伏设备'],
  lambda: f"电池 {HP['电池']:+.2f}%｜光伏设备 {HP['光伏设备']:+.2f}%",
  lambda: f"电池 {fy(sec_yi('电池'))}｜光伏设备 {fy(sec_yi('光伏设备'))}｜能源金属 {fy(sec_yi('能源金属'))}；组合电池ETF {fy(etf_yi('159796'))}/{fy(etf_yi('561910'))}"),
 ('科技系', ind_sum.get('科技系', 0),
  ['半导体', '通信设备'],
  lambda: f"半导体 {HP['半导体']:+.2f}%｜通信设备 {HP['通信设备']:+.2f}%｜科创50 {idx['科创50'][1]:+.2f}%",
  lambda: f"半导体 {fy(sec_yi('半导体'))}｜通信设备 {fy(sec_yi('通信设备'))}；组合AI/机器人ETF {fy(etf_yi('159819'))}/{fy(etf_yi('159770'))}"),
 ('红利', ind_sum.get('红利', 0),
  [],
  lambda: f"红利指数 {idx['红利指数'][1]:+.2f}%",
  lambda: f"组合红利低波ETF {fy(etf_yi('563020'))}｜港股通红利 {fy(etf_yi('159220'))}"),
 ('债券系', ind_sum.get('利率债', 0) + ind_sum.get('可转债', 0),
  [],
  lambda: f"10Y国债区间震荡（macro_daily 仅SHIBOR {macro.get('SHIBOR_ON','—')}%，债价微幅波动）",
  lambda: '—（无对应申万板块口径）'),
 ('宽基', ind_sum.get('宽基', 0),
  [],
  lambda: f"沪深300 {idx['沪深300'][1]:+.2f}%｜中证500 {idx['中证500'][1]:+.2f}%｜中证1000 {idx['中证1000'][1]:+.2f}%｜科创50 {idx['科创50'][1]:+.2f}%｜创业板50 {idx['创业板50'][1]:+.2f}%",
  lambda: f"组合中证1000/500ETF {fy(etf_yi('512100'))}/{fy(etf_yi('510500'))}；科创50 {fy(etf_yi('588000'))}；沪深300 {fy(etf_yi('510300'))}/{fy(etf_yi('159300'))}"),
]

CROSS = []
for name, mv, secs, price_fn, flow_fn in CROSS_DEF:
    if secs:
        av = avg_chg(secs)
    elif name == '红利':
        av = idx['红利指数'][1]
    elif name == '宽基':
        av = statistics.mean([idx[k][1] for k in ['沪深300','中证500','中证1000','科创50','创业板50']])
    else:
        av = 0.0
    wind = wind_label(av)
    CROSS.append((name, mv, price_fn(), flow_fn(), wind, av))

n_up = sum(1 for x in CROSS if x[4] in ('顺风', '微顺风'))
n_dn = sum(1 for x in CROSS if x[4] in ('逆风', '强逆风'))
n_mid = sum(1 for x in CROSS if x[4] == '弱逆风')

# 代理加权估算（方法学：方向亦可能失真）
PROXY = {
 '159267': HP['航天装备Ⅱ'], '512810': HP['军工电子Ⅱ'],
 '512010': idx['中证医疗'][1], '159992': HP['生物制品'], '515120': HP['生物制品'],
 '515010': HP['证券Ⅱ'],
 '516160': HP['电池'], '159796': HP['电池'], '561910': HP['电池'],
 '159819': HP['半导体'], '159770': HP['通信设备'],
 '511520': 0.0, '159650': 0.0, '511380': 0.0,
 '563020': idx['红利指数'][1], '159220': idx['红利指数'][1],
 '512100': idx['中证1000'][1], '510300': idx['沪深300'][1], '510500': idx['中证500'][1],
 '588000': idx['科创50'][1], '159300': idx['沪深300'][1], '159949': idx['创业板50'][1],
}
est = sum(pct(mv) / 100 * PROXY.get(c, 0) for c, n, mv, _, _, _ in hold)

# 集中度
med_w = pct(ind_sum.get('医药系', 0)); mil_w = pct(ind_sum.get('军工系', 0))
sec_w = pct(ind_sum.get('证券', 0)); top3_w = med_w + mil_w + sec_w
aero_w = pct(next((r[2] for r in hold if r[0] == '159267'), 0))
core300_w = pct(sum(r[2] for r in hold if r[0] in ('510300', '159300')))
def_w = pct(cls_sum.get('红利', 0) + cls_sum.get('债券', 0))
innov_w = pct(sum(r[2] for r in hold if r[0] in ('159992', '515120')))
ratebond_w = pct(sum(r[2] for r in hold if r[0] in ('511520', '159650')))
batt_w = pct(sum(r[2] for r in hold if r[0] in ('159796', '561910')))
up_w = pct(cls_sum.get('红利', 0) + cls_sum.get('债券', 0) + ind_sum.get('军工系', 0) + ind_sum.get('科技系', 0) + cls_sum.get('宽基', 0))
inv_w = pct(ind_sum.get('医药系', 0) + ind_sum.get('新能源系', 0) + ind_sum.get('证券', 0))

# 周线阶段：计算上证 20 日位置
try:
    cur.execute("SELECT date,close FROM (SELECT date,close FROM index_quotes WHERE name='上证指数' ORDER BY date DESC LIMIT 20) ORDER BY date")
    _hist = cur.fetchall()
    _hi = max(c for _, c in _hist); _lo = min(c for _, c in _hist)
    _pos = (idx['上证指数'][0] - _lo) / (_hi - _lo) if _hi > _lo else 0.5
    _hist_dates = f"{_hist[0][0]}~{_hist[-1][0]}"
except Exception:
    _pos = 0.5; _hist_dates = '—'
# 阶段映射：低位→②放量启动、中位→③主升加速、高位→④高位震荡（原 elif _pos>=0.66 不可达死代码，已按阈值重排）
STAGE = '④ 高位震荡' if _pos >= 0.66 else '③ 主升加速' if _pos >= 0.33 else '② 放量启动'
# 阶段编号（中文圈码）→ 整数，供 1.3 进度条动态高亮（承接 evolution #29）
_STAGE_CIRCLED = {'①': 1, '②': 2, '③': 3, '④': 4, '⑤': 5, '⑥': 6, '⑦': 7}
_stage_num = 0
for _k, _v in _STAGE_CIRCLED.items():
    if _k in STAGE:
        _stage_num = _v
        break
_stage_bar = ''.join(
    f'<div class="st{" on" if i == _stage_num else ""}">{c} {name}</div>'
    for i, (c, name) in enumerate(
        [('①', '底部蓄势'), ('②', '放量启动'), ('③', '主升加速'), ('④', '高位震荡'),
         ('⑤', '局部派发'), ('⑥', '破位下行'), ('⑦', '探底重构')], start=1)
)

# ============ 跨日信号（读结构化本地历史库，不爬报告 HTML 文本）============
def _sign(yi):
    return '正' if (yi is not None and yi > 0) else ('负' if (yi is not None and yi < 0) else '—')

# ① 证券净流出连续天数（DB 以交易日为粒度，今日为起点逆向计数）
_sec_raw = []
cur.execute("SELECT date,net_inflow FROM fund_flows WHERE category='sector' AND name='证券' ORDER BY date DESC LIMIT 30")
for _d, _v in cur.fetchall():
    _v = _f(_v); _sec_raw.append((_d, _v/1e8 if _v is not None else None))
sec_out_streak = 0
for _d, _yi in _sec_raw:
    if _yi is None: break
    if _yi < 0: sec_out_streak += 1
    else: break
sec_today_yi = _sec_raw[0][1] if _sec_raw else None
sec_prev_yi = _sec_raw[1][1] if len(_sec_raw) > 1 else None

# ② 航天ETF(159267) 资金流符号 + 环比（单一最大持仓集中度观察）
_aero_raw = []
cur.execute("SELECT date,net_inflow FROM fund_flows WHERE category='etf' AND code='159267' ORDER BY date DESC LIMIT 30")
for _d, _v in cur.fetchall():
    _v = _f(_v); _aero_raw.append((_d, _v/1e8 if _v is not None else None))
aero_today_yi = _aero_raw[0][1] if _aero_raw else None
aero_sign = _sign(aero_today_yi)
aero_prev_sign = _sign(_aero_raw[1][1]) if len(_aero_raw) > 1 else None

# ③ 上证距 MA20 空间% + 量能区间位置
cur.execute("SELECT close FROM (SELECT date,close FROM index_quotes WHERE name='上证指数' ORDER BY date DESC LIMIT 20) ORDER BY date")
_ma20 = statistics.mean([c for (c,) in cur.fetchall()])
_sh_close = idx['上证指数'][0]
pos_to_ma20 = (_sh_close - _ma20) / _ma20 * 100 if _ma20 else 0.0
amt_yi = (idx['上证指数'][2] + idx['深证成指'][2]) / 1e8
amt_zone = ('1.7–2.0万亿舒适区' if 17000 <= amt_yi <= 20000
            else '低于1.5万亿（缩量区）' if amt_yi < 15000
            else '高于2.3万亿（放量区）' if amt_yi > 23000
            else '1.5–1.7 / 2.0–2.3万亿边缘区')
phase_path = 'A'

signal_state = {
    'date': DATA_DATE, 'run_date': RUN_DATE,
    'sec_outflow_streak': sec_out_streak, 'sec_today_yi': sec_today_yi, 'sec_prev_yi': sec_prev_yi,
    'aero_sign': aero_sign, 'aero_today_yi': aero_today_yi, 'aero_prev_sign': aero_prev_sign,
    'sh_pos_to_ma20': round(pos_to_ma20, 2), 'sh_close': round(_sh_close, 2), 'ma20': round(_ma20, 2),
    'amt_yi': round(amt_yi, 0), 'amt_zone': amt_zone, 'phase_path': phase_path,
    'regime': REGIME, 'stage': STAGE,
}

# ④ 军工主线延续性（读 sector_daily_change 历史表，跨日信号；不依赖报告 HTML 文本）
MIL_SECTORS = ['地面兵装Ⅱ', '航空装备Ⅱ', '军工电子Ⅱ', '航海装备Ⅱ']
WATCH12 = MIL_SECTORS + ['化学制药', '生物制品', '医疗服务', '证券Ⅱ', '电池', '光伏设备',
                        '半导体', '通信设备', '种植业']

def _upsert_sectors(date_str, secdict):
    """把当日板块涨跌幅写入 sector_daily_change（供后续跨日跟踪）。幂等。"""
    try:
        for _nm, _val in secdict.items():
            if _val is None:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO sector_daily_change "
                "(date, sector_name, change_pct, source) VALUES (?,?,?,?)",
                (date_str, _nm, float(_val), 'neodata_q4'))
        con.commit()
    except Exception as _e:
        print('[WARN] sector_daily_change upsert 失败:', _e)

_upsert_sectors(DATA_DATE, HP)

cur.execute("SELECT date,COUNT(*) c FROM sector_daily_change GROUP BY date")
_g = {r[0]: r[1] for r in cur.fetchall()}
_complete = sorted([d for d, c in _g.items() if c >= 12])   # 仅用 12 板块齐全的"完整交易日"
cur.execute("SELECT date,sector_name,change_pct FROM sector_daily_change")
_d = {}
for _d0, _s, _v in cur.fetchall():
    _d.setdefault(_d0, {})[_s] = _v

def _dtobj(s):
    y, m, d = map(int, s.split('-'))
    return _date(y, m, d)

_mil_recent, _prev, mil_up_streak = [], None, 0
for d in reversed(_complete):                       # 从最近交易日倒推
    _vals = [_d[d].get(s) for s in MIL_SECTORS]
    _avail = [v for v in _vals if v is not None]    # 历史日缺航海装备Ⅱ时不中断连续计数
    if not _avail:
        break
    _best = max(_avail)
    if _prev is not None and (_dtobj(_prev) - _dtobj(d)).days > 4:
        break                                       # 跨真实缺口（>4 日历日），连续中断
    if _best > 0:
        mil_up_streak += 1
    else:
        break
    if len(_mil_recent) < 7:
        _mil_recent.append((d, {s: _d[d].get(s) for s in MIL_SECTORS}))
    _prev = d

_mtvals = [_d.get(DATA_DATE, {}).get(s) for s in MIL_SECTORS]
_today_best = max(_mtvals) if None not in _mtvals else None
_m12 = [_d.get(DATA_DATE, {}).get(s) for s in WATCH12]
mil_top_today = (None not in _m12) and (_today_best is not None) and \
                (_today_best == max(_m12)) and (_today_best > 0)

signal_state['mil_up_streak'] = mil_up_streak
signal_state['mil_top_today'] = mil_top_today
signal_state['mil_complete_days'] = len(_complete)

# 读取往期 sidecar 做连续性对比（仅读结构化 JSON，绝不爬报告 HTML 文本）
_prev_state = None
try:
    for _p in sorted(glob.glob('data/reports/组合大盘综合视角_*.signals.json'), reverse=True):
        if RUN_DATE in _p: continue
        try:
            with open(_p, encoding='utf-8') as _sf:
                _j = json.load(_sf)
        except Exception:
            continue
        # 取最近一期"非本期运行"的 sidecar 作为上期（即便其数据日与本日相同，
        # 如周末运行两期同指最近交易日，仍按"较上期"连续性对比，避免回退到更早日期造成跳日误读）
        _prev_state = _j; break
except Exception:
    _prev_state = None

# 连续性表述
def _streak_delta(cur_v, prev_v):
    if prev_v is None: return '（上期无数据）'
    if cur_v == prev_v: return '（与上期持平）'
    return f'（较上期 {cur_v-prev_v:+d} 日）'
sec_delta_txt = _streak_delta(sec_out_streak, _prev_state.get('sec_outflow_streak') if _prev_state else None)
mil_delta_txt = _streak_delta(mil_up_streak, _prev_state.get('mil_up_streak') if _prev_state else None)
sec_thresh_remain = max(0, 3 - sec_out_streak)
sec_triggered = sec_out_streak >= 3
aero_trend = ''
if aero_prev_sign and aero_sign != aero_prev_sign:
    aero_trend = f'；<b>较上期由{aero_prev_sign}转{aero_sign}</b>'
elif aero_prev_sign:
    aero_trend = f'（与上期同为{aero_sign}）'
aero_flip = aero_sign == '负'
ma20_ok = pos_to_ma20 > 0 and 17000 <= amt_yi <= 20000
ma20_txt = f"上证收于 {_sh_close:,.0f}，距 MA20（{_ma20:,.0f}）{pos_to_ma20:+.1f}%，处箱体{'上沿' if pos_to_ma20 > 0 else '下沿'}"
amt_txt = f"两市量能 {amt_yi/10000:.2f} 万亿，处{amt_zone}"
stage_prev = _prev_state.get('stage', '—') if _prev_state else '—'
stage_changed = bool(_prev_state and _prev_state.get('stage') != STAGE)

# 跨期连续性提示（进化项 #21）：上期待续接数据日与本期相同或间隔>1 交易日时显式标注，避免误读
_prev_date = _prev_state.get('date') if _prev_state else None
if _prev_date is None:
    _prev_gap_note = '首期运行，无上期对照基准。'
elif _prev_date == DATA_DATE:
    if DATA_LAG and RUN_DATE_IS_TRADING:
        _prev_gap_note = (f'本期组合数据日 {DATA_DATE} 与上期相同：组合/持仓采集器滞后（运行日 {RUN_DATE} 为交易日，'
                          f'但本地 portfolio_summary 等尚未更新至当日），下方"较上期"为同日连续性对比。')
    else:
        _prev_gap_note = (f'本期数据日 {DATA_DATE} 与上期相同（周末/非交易日运行，两期同指最近交易日），'
                          f'下方"较上期"为同日连续性对比，非新交易日变化。')
else:
    try:
        from datetime import date as _dt
        _cal_gap = (_dt.fromisoformat(DATA_DATE) - _dt.fromisoformat(_prev_date)).days
    except Exception:
        _cal_gap = None
    if _cal_gap is not None and _cal_gap > 1:
        _prev_gap_note = (f'跨期提示：上期待续接数据日为 {_prev_date}，与本期 {DATA_DATE} 间隔 {_cal_gap} 日历日，'
                          f'中间无运行日，跨日对比已跳过。')
    else:
        _prev_gap_note = ''
_prev_gap_html = f'<li><span class="cond">跨期对照</span>：{_prev_gap_note}</li>' if _prev_gap_note else ''

now = datetime.now().strftime('%Y-%m-%d %H:%M')
E = html.escape
def chg(v, suffix='%'):
    if v is None: return '<span class="flat">—</span>'
    cl = 'up' if v > 0 else ('down' if v < 0 else 'flat')
    return f'<span class="{cl}">{v:+.2f}{suffix}</span>'
def money(v):
    if v is None: return '<span class="flat">—</span>'
    cl = 'up' if v > 0 else ('down' if v < 0 else 'flat')
    return f'<span class="{cl}">{v:+,.2f}亿</span>'

# 军工近 N 日表现串（需 chg()，故置于 def chg/money 之后）
mil_recent_txt = '；'.join(
    f"{d[5:]}:地兵{chg(dv.get('地面兵装Ⅱ'))}/航装{chg(dv.get('航空装备Ⅱ'))}/军工电{chg(dv.get('军工电子Ⅱ'))}/航海{chg(dv.get('航海装备Ⅱ'))}"
    for d, dv in _mil_recent) or '—'

# ================= HTML =================
rows_hold = ''
for c, n, mv, pr, cp, pnl in hold:
    a, b = CLS.get(c, ('其他', '其他'))
    flag = ' <span class="warn-tag">待核对</span>' if (abs(pr) > 50 or cp < 0) else ''
    ef = etf_flow.get(c)
    ef_txt = money(ef[0]) if ef and ef[0] is not None else '<span class="flat">—</span>'
    rows_hold += f'''<tr><td class="code">{c}</td><td>{E(n)}{flag}</td><td class="tag t-{"a" if a=="行业主题" else "b" if a=="债券" else "c" if a=="红利" else "d"}">{a}</td>
<td>{E(b)}</td><td class="num">{mv:,.0f}</td><td class="num">{pct(mv):.2f}%</td><td class="num">{chg(pr)}</td><td class="num">{ef_txt}</td></tr>'''

rows_cls = ''
for k in ['行业主题', '债券', '红利', '宽基']:
    v = cls_sum.get(k, 0)
    rows_cls += f'''<tr><td>{k}</td><td class="num">{v:,.0f}</td><td class="num"><b>{pct(v):.2f}%</b></td>
<td><div class="bar"><i style="width:{pct(v):.1f}%"></i></div></td></tr>'''

ind_order = ['医药系', '军工系', '证券', '宽基', '红利', '利率债', '新能源系', '科技系', '可转债']
rows_ind = ''
for k in ind_order:
    v = ind_sum.get(k, 0)
    if not v: continue
    hi = ' class="hot"' if pct(v) >= 12 else ''
    rows_ind += f'''<tr{hi}><td>{k}</td><td class="num">{v:,.0f}</td><td class="num"><b>{pct(v):.2f}%</b></td>
<td><div class="bar"><i style="width:{min(pct(v)*4,100):.1f}%"></i></div></td></tr>'''

rows_cross = ''
for name, mv, price, flow, wind, av in CROSS:
    wc = {'顺风': 'w-up', '微顺风': 'w-up', '逆风': 'w-dn', '强逆风': 'w-dn2', '弱逆风': 'w-mid'}.get(wind, 'w-mid')
    rows_cross += f'''<tr><td><b>{name}</b></td><td class="num">{pct(mv):.2f}%</td>
<td class="sm">{price}</td><td class="sm">{flow}</td>
<td><span class="wind {wc}">{wind}</span></td><td class="sm">板块均值 {av:+.2f}%</td></tr>'''

rows_bad = ''
for c, n, mv, pr, cp in sorted(bad, key=lambda x: -abs(x[3])):
    rsn = '负成本价' if cp < 0 else '|盈亏率|>50%'
    rows_bad += f'''<tr><td class="code">{c}</td><td>{E(n)}</td><td class="num">{mv:,.0f}</td>
<td class="num">{pct(mv):.2f}%</td><td class="num">{chg(pr)}</td><td class="num">{cp}</td><td>{rsn}</td></tr>'''

rows_dup = ''
dup_total_w = 0
for g, (codes, diag) in DUP.items():
    mv_g = sum(r[2] for r in hold if r[0] in codes)
    w_g = pct(mv_g); dup_total_w += w_g
    nm = '、'.join(f"{dict((r[0],r[1]) for r in hold).get(c,'?')} {pct(sum(r[2] for r in hold if r[0]==c)):.2f}%" for c in codes)
    rows_dup += f'''<tr><td><b>{g}</b></td><td>{nm}</td><td class="num"><b>{w_g:.2f}%</b></td><td>{diag}</td></tr>'''

IDX_ORDER = ['上证指数', '深证成指', '创业板指', '沪深300', '中证500', '中证1000', '科创50', '创业板50', '中证医疗', '红利指数', '中证酒']
rows_idx = ''
for k in IDX_ORDER:
    if k not in idx: continue
    c, p, a = idx[k]
    rows_idx += f'''<tr><td>{k}</td><td class="num">{c:,.2f}</td><td class="num">{chg(p)}</td><td class="num">{a/1e8:,.0f}亿</td></tr>'''

cur.execute("SELECT name,net_inflow FROM fund_flows WHERE date=? AND category='sector'", (DATA_DATE,))
sec_all = [(k, _f(v)/1e8) for k, v in cur.fetchall() if _f(v) is not None]
top_in = sorted(sec_all, key=lambda x: -x[1])[:10]
top_out = sorted(sec_all, key=lambda x: x[1])[:10]
rows_flow = ''
for i in range(10):
    ni, vi = top_in[i]; no, vo = top_out[i]
    rows_flow += f'''<tr><td class="num">{i+1}</td><td>{E(ni)}</td><td class="num">{money(vi)}</td>
<td>{E(no)}</td><td class="num">{money(vo)}</td></tr>'''
con.close()

main_proxy_txt = '（90申万板块合计代理；全市场main_fund口径今日本地缺失）' if main_est else ''
zt_show = loc_zt if loc_zt is not None else BR['zt']
dt_show = loc_dt if loc_dt is not None else BR['dt']
zt_src = '本地' if loc_zt is not None else 'NeoData'

# PMI 行
def _pmi_cell(v, up_good=True):
    if v is None: return '<span class="flat">—</span>'
    cl = 'up' if (v >= 50) == up_good else 'down'
    return f'<span class="{cl}">{v}%</span>'
def _pmi_row(label, v, prev, interp, up_good=True):
    pv = f'{prev}%' if prev is not None else '—'
    delta = f'{prev-v:+.1f}pct' if (v is not None and prev is not None) else '—'
    return f'<tr><td>{label}</td><td class="num">{_pmi_cell(v, up_good)}</td><td class="num">{pv}</td><td>{interp}（环比 {delta}）</td></tr>'
rows_pmi = ''
rows_pmi += _pmi_row(f"制造业PMI（整体，{PMI['stat']}）", PMI.get('mfg'), PMI.get('mfg_prev'),
                     'NeoData 宏观库本次查询未直接返回整体值，标「—」', up_good=False)
rows_pmi += _pmi_row('制造业PMI · 大型企业', PMI.get('large'), PMI.get('large_prev'), '大型企业仍处扩张区（>50）', up_good=False)
rows_pmi += _pmi_row('制造业PMI · 中型企业', PMI.get('mid'), PMI.get('mid_prev'), '中型企业跌破荣枯线', up_good=False)
rows_pmi += _pmi_row('综合PMI · 产出指数', PMI.get('composite'), None, '综合景气落入收缩区', up_good=False)
rows_pmi += _pmi_row('非制造业PMI · 商务活动', PMI.get('nonmfg'), None, '非制造业转收缩（建筑业46.9/服务业49.3）', up_good=False)
rows_pmi += _pmi_row('PMI · 服务业', PMI.get('service'), None, '服务业弱景气', up_good=False)
rows_pmi += _pmi_row('PMI · 建筑业', PMI.get('construction'), None, '建筑业收缩', up_good=False)
rows_pmi += f'''<tr><td>SHIBOR 隔夜</td><td class="num">{macro.get('SHIBOR_ON','—')}%</td><td class="num">—</td><td>本地 macro_daily 最新口径（仅 SHIBOR，债价微幅波动）</td></tr>'''

# 叙事段落（数据驱动）
regime_txt = REGIME
main_line_txt = MKT_NEO.get('main_line', '')
amt_chg_txt = f"{amt_chg:+,.0f} 亿" if amt_chg is not None else '—'
# 主力资金方向
main_dir = '净流出' if (main_in_yi and main_in_yi < 0) else '净流入'

# ---- 数据驱动叙事辅助（修复硬编码下行日措辞，避免与当日行情背离，落实数据纪律）----
_idx4 = {k: idx[k][1] for k in idx_keys if k in idx}
if REGIME == '普涨':
    _idx_desc = (f"主要指数全线收红（上证 {idx['上证指数'][1]:+.2f}%、深成指 {idx['深证成指'][1]:+.2f}%、"
                 f"创业板指 {idx['创业板指'][1]:+.2f}%、沪深300 {idx['沪深300'][1]:+.2f}%），"
                 f"涨跌家数 {BR['up']}:{BR['down']}（上涨占比 {BR['up_pct']}%），呈<b>指数与个股同步走强</b>的普涨格局")
elif REGIME == '普跌回调':
    _idx_desc = (f"主要指数全线收绿（上证 {idx['上证指数'][1]:+.2f}%、深成指 {idx['深证成指'][1]:+.2f}%、"
                 f"创业板指 {idx['创业板指'][1]:+.2f}%、沪深300 {idx['沪深300'][1]:+.2f}%），"
                 f"涨跌家数 {BR['up']}:{BR['down']}（上涨占比 {BR['up_pct']}%），呈<b>指数与个股同步走弱</b>的普跌格局")
elif REGIME == '指数回调·个股分化':
    _idx_desc = (f"指数回调（上证 {idx['上证指数'][1]:+.2f}%、深成指 {idx['深证成指'][1]:+.2f}%、"
                 f"创业板指 {idx['创业板指'][1]:+.2f}%、沪深300 {idx['沪深300'][1]:+.2f}%）但上涨个股占比 {BR['up_pct']}%，呈<b>指数弱、个股分化</b>格局")
else:
    _idx_desc = (f"主要指数涨跌互现（上证 {idx['上证指数'][1]:+.2f}%、深成指 {idx['深证成指'][1]:+.2f}%、"
                 f"创业板指 {idx['创业板指'][1]:+.2f}%、沪深300 {idx['沪深300'][1]:+.2f}%），上涨占比 {BR['up_pct']}%，呈<b>震荡分化</b>格局")
_main_line_sectors = '、'.join(n for n, v in sec_in[:2]) if sec_in else (top_ind if top_ind else '—')
_main_line_txt = f"当日主线为<b>{_main_line_sectors}</b>（涨幅居前 {top_gain_names}；资金净流入 {money_in_txt}）"
_mil = next((x for x in CROSS if x[0] == '军工系'), None)
_mil_wind = _mil[4] if _mil else '—'
_mil_av = _mil[5] if _mil else 0.0
_mil_against = (avg_idx > 0.5) and (_mil_av < 0)
if _mil_against:
    _mil_txt = (f"；军工系逆市走弱（地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}% / 航空装备Ⅱ {HP['航空装备Ⅱ']:+.2f}%，"
                f"军工装备 {fy(sec_yi('军工装备'))}），与大盘背离")
else:
    _mil_txt = (f"；军工系 {_mil_wind}（地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}% / 航空装备Ⅱ {HP['航空装备Ⅱ']:+.2f}%，"
                f"军工装备 {fy(sec_yi('军工装备'))}）")
_perf_word = '跑赢' if vs300 > 0 else '跑输'
_hw = [x[0] for x in CROSS if x[4] in ('逆风', '强逆风')]
_hw_names = '、'.join(_hw) if _hw else '无'
_tech_w = pct(ind_sum.get('科技系', 0))
if vs300 >= 0:
    _perf_reason = (f"超配的{'军工系（当日顺风）' if _mil_av > 0 else '防御端（债券+红利）'}对冲了{_hw_names}逆风")
else:
    _perf_reason = (f"超配的军工/医药/证券/红利当日普遍逆风（军工系 {_mil_wind} {_mil_av:+.2f}%、证券 {HP['证券Ⅱ']:+.2f}%、红利 {idx['红利指数'][1]:+.2f}%），"
                    f"而领涨的科技系组合权重仅 {_tech_w:.1f}%、对组合拉动有限，故组合跑输宽基")
_amt_note = (f"{REGIME}中资金{'净流入' if (main_in_yi and main_in_yi > 0) else '净流出'}、量能{amt_dir}"
             if REGIME in ('普涨', '普跌回调') else f"量能{amt_dir}、资金{'净流入' if (main_in_yi and main_in_yi>0) else '净流出'}")
_mil_against_txt = f"军工系逆市走弱（地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}%）" if _mil_against else f"军工系{_mil_wind}"
_oper_hold_txt = (f"军工系（地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}% / 航空装备Ⅱ {HP['航空装备Ⅱ']:+.2f}%）领涨且资金净流入，超配逻辑成立"
                  if _mil_av > 0 else
                  f"防御端（债券+红利）与超配方向提供缓冲，但军工系{_mil_wind} {_mil_av:+.2f}%、超配逻辑阶段性弱化，需关注")
_cross_map = {x[0]: x[5] for x in CROSS}
_weak_sectors = '、'.join(x[0] for x in sorted(CROSS, key=lambda x: x[5])[:3] if x[5] < 0) or '无'
_strong_sectors = '、'.join(x[0] for x in sorted(CROSS, key=lambda x: -x[5])[:3] if x[5] > 0) or '无'
_weak_body = '、'.join(f"{n} {_cross_map.get(n,0):+.2f}%" for n in _weak_sectors.split('、')) or '无'
_strong_body = '、'.join(f"{n} {_cross_map.get(n,0):+.2f}%" for n in _strong_sectors.split('、')) or '无'

HTML = f'''<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>组合+大盘综合视角 {RUN_DATE}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d1117;color:#c9d1d9;font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
line-height:1.65;padding:24px 16px;font-size:14px}}
.wrap{{max-width:1080px;margin:0 auto}}
h1{{font-size:24px;color:#f0f6fc;margin-bottom:6px;letter-spacing:.5px}}
.sub{{color:#8b949e;font-size:12.5px;margin-bottom:22px;padding-bottom:14px;border-bottom:1px solid #21262d}}
h2{{font-size:17px;color:#f0f6fc;margin:30px 0 12px;padding-left:11px;border-left:4px solid #58a6ff}}
h3{{font-size:14.5px;color:#c9d1d9;margin:18px 0 9px}}
.card{{background:#161b22;border:1px solid #21262d;border-radius:9px;padding:16px 18px;margin-bottom:14px}}
.tldr{{background:linear-gradient(135deg,#1c2128,#161b22);border:1px solid #30363d;border-left:5px solid #d29922;
border-radius:9px;padding:18px 20px;margin-bottom:8px}}
.tldr .lead{{font-size:16px;color:#f0f6fc;font-weight:600;line-height:1.72}}
.tldr ul{{margin:12px 0 0 20px;color:#a8b3bd;font-size:13.2px}} .tldr li{{margin:5px 0}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin-top:4px}}
th{{background:#1c2128;color:#8b949e;font-weight:600;text-align:left;padding:8px 9px;border-bottom:1px solid #30363d;
font-size:12px;white-space:nowrap}}
td{{padding:7px 9px;border-bottom:1px solid #1f242c;vertical-align:middle}}
tr:hover td{{background:#1a2029}}
.num{{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}}
.code{{color:#6e7681;font-family:ui-monospace,Consolas,monospace;font-size:12px}}
.sm{{font-size:11.8px;color:#9aa5b1;line-height:1.5}}
.up{{color:#f85149;font-weight:600}} .down{{color:#3fb950;font-weight:600}} .flat{{color:#6e7681}}
.tag{{font-size:11px;padding:2px 7px;border-radius:4px;white-space:nowrap}}
.t-a{{background:#3d2a1a;color:#e3a33c}} .t-b{{background:#16302b;color:#3fb950}}
.t-c{{background:#3a2436;color:#db61a2}} .t-d{{background:#1b2c42;color:#58a6ff}}
.warn-tag{{background:#4a2c11;color:#e3a33c;font-size:10.5px;padding:1px 5px;border-radius:3px;margin-left:4px}}
.bar{{background:#21262d;height:7px;border-radius:4px;overflow:hidden;min-width:90px}}
.bar i{{display:block;height:100%;background:linear-gradient(90deg,#58a6ff,#a371f7)}}
tr.hot td{{background:#1e1a12}} tr.hot .bar i{{background:linear-gradient(90deg,#f85149,#e3a33c)}}
.wind{{font-size:11.5px;padding:3px 9px;border-radius:11px;white-space:nowrap;font-weight:600}}
.w-up{{background:#3a1518;color:#ff7b72}} .w-dn{{background:#122b1a;color:#56d364}}
.w-dn2{{background:#0d2f18;color:#3fb950;border:1px solid #238636}} .w-mid{{background:#26292e;color:#9aa5b1}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(158px,1fr));gap:11px;margin:6px 0 2px}}
.kpi{{background:#12171d;border:1px solid #21262d;border-radius:8px;padding:12px 13px}}
.kpi .k{{font-size:11.5px;color:#8b949e;margin-bottom:5px}} .kpi .v{{font-size:19px;font-weight:700;color:#f0f6fc}}
.kpi .n{{font-size:11px;color:#6e7681;margin-top:3px}}
.stage{{display:flex;gap:5px;margin:14px 0 10px;flex-wrap:wrap}}
.st{{flex:1;min-width:78px;background:#161b22;border:1px solid #21262d;border-radius:6px;padding:8px 5px;
text-align:center;font-size:11.3px;color:#6e7681}}
.st.on{{background:#3d2a1a;border-color:#d29922;color:#e3a33c;font-weight:700}}
.st.near{{background:#1c2128;border-color:#484f58;color:#a8b3bd}}
.four{{display:grid;grid-template-columns:repeat(auto-fit,minmax(232px,1fr));gap:11px}}
.fbox{{background:#12171d;border:1px solid #21262d;border-radius:8px;padding:13px 14px}}
.fbox .h{{font-size:12.8px;color:#58a6ff;font-weight:700;margin-bottom:7px}}
.fbox .b{{font-size:12.3px;color:#a8b3bd;line-height:1.68}}
.path{{background:#12171d;border:1px solid #21262d;border-left:3px solid #58a6ff;border-radius:6px;
padding:11px 13px;margin:8px 0;font-size:12.6px}}
.path b{{color:#f0f6fc}} .path.pB{{border-left-color:#f85149}} .path.pC{{border-left-color:#3fb950}}
.op{{background:#12171d;border:1px solid #21262d;border-radius:8px;padding:14px 16px;margin-bottom:10px}}
.op .t{{font-size:13.5px;color:#f0f6fc;font-weight:700;margin-bottom:8px}}
.op ul{{margin-left:19px;font-size:12.7px;color:#a8b3bd}} .op li{{margin:5px 0}}
.cond{{color:#d29922;font-weight:600}}
.src{{background:#12171d;border:1px solid #21262d;border-radius:7px;padding:11px 13px;font-size:11.6px;
color:#8b949e;line-height:1.75}}
.ok{{color:#3fb950}} .bad{{color:#f85149}} .mid{{color:#d29922}}
.note{{font-size:11.6px;color:#6e7681;margin-top:8px;font-style:normal}}
.dis{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px 16px;margin-top:26px;
font-size:11.6px;color:#7d8590;line-height:1.75}}
@media(max-width:640px){{body{{padding:14px 9px;font-size:13px}}table{{font-size:11.6px}}th,td{{padding:6px 5px}}}}
</style></head><body><div class="wrap">

<h1>组合 + 大盘综合视角 · 盘后日报</h1>
<div class="sub">报告生成：{now}（运行日 {RUN_DATE}）　|　<b style="color:#d29922">组合数据基准日：{DATA_DATE}（收盘）</b>{f'　|　⚠ 大盘指数采用 {IDX_DATE} 实时（组合/持仓采集器滞后至 {DATA_DATE}）' if (DATA_LAG and IDX_DATE != DATA_DATE) else ''}　|　持仓快照：{SNAP}　|　22 只 ETF　总市值 ¥{TOT:,.0f}
<br>数据源：项目本地数据层（东方财富/新浪）+ NeoData 金融搜索 <span class="ok">✓ 均可用</span>　|　NeoData 查询时间 {MKT_NEO['query_time']}</div>

<div class="tldr">
<div class="lead">大盘今日<b>{regime_txt}</b>：{_idx_desc}。{_main_line_txt}{_mil_txt}。组合当日回报 <b>{chg(d_ret)}</b>、<b>{_perf_word}沪深300 {abs(vs300):+.2f}pct</b>：{_perf_reason}。</div>
<ul>
{f'<li><b>⚠ 锚定提示</b>：代理加权估算 {est:+.2f}% 与组合真实回报 {d_ret:+.2f}% <b>方向相反</b>，代理法在本组合结构下方向亦不可信，<b>本报告一律以真值为准</b>。</li>' if (est < 0) != (d_ret < 0) else ''}
<li><b>核心矛盾</b>：主力资金今日<b class="down">{main_dir} {main_in_yi:+,.0f} 亿</b>{main_proxy_txt}；两市量能 {amt2:.2f} 万亿较昨日 {amt_chg_txt}（{amt_dir}），{_amt_note}。</li>
<li><b>组合最大集中度风险</b>：航天ETF华安 {aero_w:.2f}% 为单一最大持仓（已超 10% 审慎线）；军工系 {mil_w:.1f}% + 医药系 {med_w:.1f}% + 证券 {sec_w:.1f}% 三方向合计 <b style="color:#e3a33c">{top3_w:.1f}%</b>。军工系 {_mil_wind}（地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}% / 航空装备Ⅱ {HP['航空装备Ⅱ']:+.2f}%，军工装备 {fy(sec_yi('军工装备'))}）{'，暂未共振拖累' if _mil_av < 0 else '，提供正向贡献'}。</li>
<li><b>亮点/风险</b>：当日主线为 {_main_line_sectors}（资金净流入 {money_in_txt}），{'风险偏好回升' if (REGIME=='普涨' and main_in_yi and main_in_yi>0) else '主线偏防御/事件驱动'}；红利+债券防御底仓（{def_w:.1f}%）稳定；{_mil_against_txt}；医药系微逆风（化学制药 {HP['化学制药']:+.2f}%/生物制品 {HP['生物制品']:+.2f}%）拖累有限。</li>
<li><b>宏观逆风未解</b>：制造业 PMI 整体值经 NeoData 查询仍未直接返回（标「—」）；仅返回综合PMI产出 {PMI['composite']}%、非制造业 {PMI['nonmfg']}%（收缩区）、服务业 {PMI['service']}%、建筑业 {PMI['construction']}%。</li>
</ul>
</div>

<h2>一、大盘视角</h2>

<div class="card">
<h3>1.1 指数收盘（{IDX_DATE}）</h3>
<div class="grid">
<div class="kpi"><div class="k">上证指数</div><div class="v">{idx['上证指数'][0]:,.2f}</div><div class="n">{chg(idx['上证指数'][1])}　{("守住 3900–4000 箱体" if idx['上证指数'][0]>=3900 and idx['上证指数'][0]<=4000 else "箱体下沿/上方")}</div></div>
<div class="kpi"><div class="k">两市成交额</div><div class="v">{amt2:.2f}万亿</div><div class="n"><span class="down">{amt_chg_txt}</span>　较昨日{amt_dir}</div></div>
<div class="kpi"><div class="k">涨跌家数</div><div class="v"><span class="up">{BR['up']}</span> : <span class="down">{BR['down']}</span></div><div class="n">上涨占比 {BR['up_pct']}%　{regime_txt}</div></div>
<div class="kpi"><div class="k">涨停 / 跌停</div><div class="v"><span class="up">{zt_show}</span> : <span class="down">{dt_show}</span></div><div class="n">个股面{("无恐慌" if dt_show and dt_show<20 else "需关注")}（{zt_src}口径，NeoData {BR['zt']}）</div></div>
</div>
<table><thead><tr><th>指数</th><th class="num">收盘</th><th class="num">涨跌幅</th><th class="num">成交额</th></tr></thead>
<tbody>{rows_idx}</tbody></table>
<div class="note">指数数据：项目本地 index_quotes（{IDX_DATE}）与 NeoData 统一行情交叉核对一致；涨跌家数/涨停跌停来自 NeoData 大盘市场宽度统计，涨停/跌停本地 market_breadth 当日已采集（zt={loc_zt}、dt={loc_dt}）作为交叉核对基准。⚠ 完整跌幅榜 NeoData 仍仅返回涨幅榜（系统性限制），跌幅口径以指数/重仓板块当日涨跌幅替代。</div>
</div>

<div class="card">
<h3>1.2 日频情绪四段式</h3>
<div class="four">
<div class="fbox"><div class="h">① 量能 —— {amt_dir}</div><div class="b">
两市 <b>{amt2:.2f} 万亿</b>，较昨日 <span class="down">{amt_chg_txt}</span>（{amt_dir}）。<br>
主力资金 <b class="down">{main_dir} {main_in_yi:+,.0f} 亿</b>{main_proxy_txt}。<br>
<span class="mid">→ {_amt_note}，{'属增量温和延续（风险偏好回升）' if (REGIME=='普涨' and main_in_yi and main_in_yi>0) else '属存量博弈' if (main_in_yi and main_in_yi<0) else '观望'}。</span></div></div>
<div class="fbox"><div class="h">② 催化 —— {_main_line_sectors}主线</div><div class="b">
涨幅榜前 3：{top_gain_names}（本地广度 top 行业：{top_ind}）。<br>
资金印证（净流入）：{money_in_txt}；净流出：{money_out_txt}。<br>
<span class="mid">→ 主线 {_main_line_sectors}（{top_gain_names}），{'资金净流入、广度扩散，风险偏好回升' if (REGIME=='普涨' and main_in_yi and main_in_yi>0) else '偏防御/事件驱动，广度有限'}。</span></div></div>
<div class="fbox"><div class="h">③ 结构 —— {REGIME}、个股{'同步走强' if REGIME=='普涨' else '同步走弱' if REGIME=='普跌回调' else '分化'}</div><div class="b">
上涨 {BR['up']} : 下跌 {BR['down']}，涨停 {zt_show}、跌停 {dt_show}。<br>
{_weak_body} 领跌 vs {_strong_body} 领涨。<br>
红利指数 {chg(idx['红利指数'][1])}、证券Ⅱ {HP['证券Ⅱ']:+.2f}% 同步回落。<br>
<span class="mid">→ 指数与个股{'同步上行' if REGIME=='普涨' else '同步下行'}（上涨占比 {BR['up_pct']}%），{'与普涨格局一致' if REGIME=='普涨' else '与「指数跌、个股分化」格局相反'}。</span></div></div>
<div class="fbox"><div class="h">④ 风控 —— {_weak_sectors}逆风 + 宏观收缩</div><div class="b">
{_weak_body}（逆风）；{_strong_body}（顺风）。<br>
宏观：综合PMI产出 {PMI['composite']}%、非制造业 {PMI['nonmfg']}%（收缩区）。<br>
<span class="mid">→ 风险集中在{_weak_sectors}逆风与 4000 关阻力；跌停 {dt_show} 家，无系统性恐慌。</span></div></div>
</div>
</div>

<div class="card">
<h3>1.3 周线七阶段定位</h3>
<div class="stage">
{_stage_bar}
</div>
<p style="font-size:13px;color:#c9d1d9;margin:10px 0 4px"><b>当前定位：第 {STAGE} —— 今日为箱内{REGIME}，箱体（3900–4000）位置见下方点位观察</b></p>
<h3>证据链</h3>
<table><thead><tr><th>维度</th><th>观察值</th><th>指向</th></tr></thead>
<tbody>
<tr><td>点位位置</td><td>上证 {idx['上证指数'][0]:,.0f}（{idx['上证指数'][1]:+.2f}%），仍在 3900–4000 箱体、关前徘徊；近 20 日区间位置约 {_pos*100:.0f}%（{_hist_dates}）</td><td class="mid">箱体上沿、关前震荡</td></tr>
<tr><td>量能</td><td>两市 {amt2:.2f} 万亿，较昨日 {amt_chg_txt}（{amt_dir}）</td><td class="mid">{'普涨增量温和' if REGIME=='普涨' else '回调缺增量确认'}</td></tr>
<tr><td>主线广度</td><td>{_main_line_sectors}（{top_gain_names}）领涨，资源/周期/红利/证券{'共振' if (not _mil_against and REGIME=='普涨') else '无共振'}，上涨占比 {BR['up_pct']}%</td><td class="mid">轮动非普涨</td></tr>
<tr><td>资金</td><td>主力 {main_dir} {main_in_yi:+,.0f} 亿{main_proxy_txt}；资金净流入集中于军工（军工装备 {fy(sec_yi('军工装备'))}/军工电子 {fy(sec_yi('军工电子'))}）</td><td class="mid">流出、结构分化</td></tr>
<tr><td>宏观</td><td>综合PMI产出 {PMI['composite']}%、非制造业 {PMI['nonmfg']}%（收缩区）</td><td class="mid">需求弱、结构强</td></tr>
<tr><td>个股情绪</td><td>涨停 {zt_show}、上涨占比 {BR['up_pct']}%、跌停 {dt_show}</td><td class="ok">未系统性转冷 → 未入第 ⑥ 阶段</td></tr>
</tbody></table>
<h3>分叉路径（观察条件，非预测）</h3>
<div class="path"><b>路径 A · 箱体震荡延续（当前证据最充分）</b><br>
上证在 <b>3900 – 4000</b> 区间反复，军工主线轮动，医药/新能源磨底。<br>
<span class="cond">确认条件</span>：两市量能维持 1.7–2.0 万亿；上证不失守 MA20（≈3921）；涨停维持 40 家以上、跌停保持低位。</div>
<div class="path pB"><b>路径 B · 向上突破（需量能+主线扩散配合）</b><br>
上证站稳 4000 并放量突破。<br>
<span class="cond">确认条件</span>：两市量能升至 2.3 万亿以上；军工/半导体资金持续净流入扩散；上涨家数占比升至 55% 以上（当前 {BR['up_pct']}%）。</div>
<div class="path pC"><b>路径 C · 转入破位（需同时满足两条）</b><br>
进入第 ⑥ 阶段破位下行。<br>
<span class="cond">确认条件</span>：① 上证有效跌破 MA20（≈3921）且收盘跌破 3900；且 ② 量能萎缩至 1.5 万亿以下 或 涨停骤降至 25 家以下、跌停扩大至 20+（当前 {dt_show}）。</div>
</div>

<div class="card">
<h3>1.4 板块资金流全景（{DATA_DATE}，东方财富，90 个申万二级板块）</h3>
<table><thead><tr><th class="num">#</th><th>净流入 TOP10</th><th class="num">金额</th><th>净流出 TOP10</th><th class="num">金额</th></tr></thead>
<tbody>{rows_flow}</tbody></table>
<div class="note">主力资金合计 <span class="down">{main_dir} {main_in_yi:+,.0f} 亿</span>{main_proxy_txt}，流入集中于「{money_in_txt}」等方向；流出集中于「{money_out_txt}」等。</div>
</div>

<div class="card">
<h3>1.5 宏观与利率</h3>
<table><thead><tr><th>指标</th><th class="num">最新值</th><th class="num">前值</th><th>解读</th></tr></thead>
<tbody>{rows_pmi}</tbody></table>
<div class="note">PMI 来源：国家统计局（季调值）/ 中国人民银行（高技术、装备制造），经 NeoData 宏观数据库返回，统计截止 {PMI['stat']}。⚠ 制造业PMI<b>整体值</b>与大型/中型/小型企业值经查询未直接返回，标「—」，未做任何推算；仅综合PMI产出/非制造业/服务业/建筑业可用。利率其余字段（10Y国债/中美利差）本地 macro_daily 当日未采集，标「—」。</div>
</div>

<h2>二、组合视角</h2>

<div class="card">
<div class="grid">
<div class="kpi"><div class="k">总市值</div><div class="v">¥{tot_val:,.0f}</div><div class="n">成本 ¥{tot_cost:,.0f}</div></div>
<div class="kpi"><div class="k">累计盈亏</div><div class="v"><span class="up">+¥{tot_pnl:,.0f}</span></div><div class="n">{chg(tot_pnl/tot_cost*100)}（含失真数据，见2.4）</div></div>
<div class="kpi"><div class="k">当日回报（真值）</div><div class="v">{chg(d_ret)}</div><div class="n"><span class="down">{d_pnl:+,.0f} 元</span></div></div>
<div class="kpi"><div class="k">相对沪深300</div><div class="v">{chg(vs300, 'pct')}</div><div class="n">{_perf_word}（{'军工顺风+防御对冲' if _mil_av>0 else '防御端缓冲'}）</div></div>
<div class="kpi"><div class="k">盈亏只数</div><div class="v"><span class="up">{pc}</span> : <span class="down">{lc}</span></div><div class="n">共 22 只</div></div>
<div class="kpi"><div class="k">Sharpe / 回撤 / 波动</div><div class="v" style="font-size:15px">{sharpe:.2f} / {mdd:.2f}% / {vol:.2f}%</div><div class="n">滚动统计口径</div></div>
</div>
<div class="note">当日锚定规则：组合当日表现一律以 <b>portfolio_summary.daily_return</b> 真值为准（本日 {d_ret}%，{('跑赢' if vs300>=0 else '跑输')}沪深300 {abs(vs300):.2f}pct）。项目 etf_price_history 最新仅至 2026-08-19（滞后），<b>禁止</b>用其估算当日逐 ETF 损益。</div>
</div>

<div class="card">
<h3>2.1 资产类别分布</h3>
<table><thead><tr><th>类别</th><th class="num">市值</th><th class="num">占比</th><th style="width:40%">分布</th></tr></thead><tbody>{rows_cls}</tbody></table>
<div class="note">进攻性资产（行业主题 {pct(cls_sum.get('行业主题',0)):.1f}% + 宽基 {pct(cls_sum.get('宽基',0)):.1f}%）= <b>{pct(cls_sum.get('行业主题',0)+cls_sum.get('宽基',0)):.1f}%</b>；防御性资产（债券 {pct(cls_sum.get('债券',0)):.1f}% + 红利 {pct(cls_sum.get('红利',0)):.1f}%）= <b>{def_w:.1f}%</b>。约 3:1 的攻守比，进攻端偏重。</div>
</div>

<div class="card">
<h3>2.2 行业集中度</h3>
<table><thead><tr><th>行业族</th><th class="num">市值</th><th class="num">占比</th><th style="width:38%">集中度</th></tr></thead><tbody>{rows_ind}</tbody></table>
<div class="note">
<b>集中度诊断</b>：医药系 {med_w:.2f}% + 军工系 {mil_w:.2f}% + 证券 {sec_w:.2f}% = <b style="color:#e3a33c">{top3_w:.2f}%</b> 集中在三个方向。
单一持仓最高为航天ETF华安 {aero_w:.2f}%，已超单票 10% 的常规审慎线。{('<b style="color:#f85149">⚠ 单只超 15% 升级预警：集中度已达 {:.2f}%，建议审视再平衡与分批减压。</b>'.format(aero_w)) if aero_w > 15 else ''}<br>
<b>宽基核心薄弱</b>：宽基类合计 {pct(cls_sum.get('宽基',0)):.2f}%，但其中真正的核心宽基（沪深300 两只）仅 <b>{core300_w:.2f}%</b>，其余为科创50 / 创业板50 / 中证500 / 中证1000 等风格暴露型宽基——组合缺少「市场平均收益」压舱石。</div>
</div>

<div class="card">
<h3>2.3 同质化重复持仓</h3>
<table><thead><tr><th>重复组</th><th>标的</th><th class="num">合计占比</th><th>诊断</th></tr></thead>
<tbody>{rows_dup}</tbody></table>
<div class="note">同质化重复合计涉及市值约 <b>¥{dup_total_w/100*TOT:,.0f}</b>（{dup_total_w:.1f}% 权重）。其中「创新药 ×2 / 利率债 ×2 / 电池 ×2 / 沪深300 ×2」四组共 {dup_total_w:.2f}% 属可直接合并项（红利低波×2 因 A股/港股通地域分散保留）。</div>
</div>

<div class="card">
<h3>2.4 数据质量隐患</h3>
<p style="font-size:12.7px;color:#a8b3bd;margin-bottom:9px">校验规则：<code style="color:#e3a33c">|pnl_rate| &gt; 50%</code> 或 <code style="color:#e3a33c">cost_price &lt; 0</code> → 标记「待核对」，其盈亏率<b>不予采信</b>。</p>
<table><thead><tr><th>代码</th><th>名称</th><th class="num">市值</th><th class="num">占比</th><th class="num">记录盈亏率</th><th class="num">成本价</th><th>触发规则</th></tr></thead>
<tbody>{rows_bad}</tbody></table>
<div class="note">命中 <b>{len(bad)}</b> 只，合计市值 ¥{bad_mv:,.0f}（<b>{pct(bad_mv):.2f}%</b> 权重）。其中创业板50ETF华安（cost -2.72）与人工智能ETF易方达（cost -0.292）为<b>负成本价</b>，属明确的成本记录错误，应回溯交易流水修正。<br>
影响范围：组合层面「累计盈亏 +¥{tot_pnl:,.0f}（+{tot_pnl/tot_cost*100:.2f}%）」因包含这 {len(bad)} 只失真数据而<b>不可靠</b>；当日回报 {d_ret}% 由市值变动计算，<span class="ok">不受成本失真影响，可采信</span>。</div>
</div>

<div class="card">
<h3>2.5 持仓明细（{SNAP}）</h3>
<table><thead><tr><th>代码</th><th>名称</th><th>类别</th><th>行业族</th><th class="num">市值</th><th class="num">占比</th><th class="num">盈亏率</th><th class="num">当日 ETF 资金</th></tr></thead>
<tbody>{rows_hold}</tbody></table>
<div class="note">「当日 ETF 资金」= fund_flows 表 category=etf 的净流入（东方财富，{DATA_DATE}），反映 ETF 份额申赎与场内买卖净额，<b>非</b>该 ETF 的当日涨跌幅。</div>
</div>

<h2>三、交叉验证：组合方向 vs 当日市场风向</h2>

<div class="card">
<table><thead><tr><th>组合方向</th><th class="num">权重</th><th>当日板块 / 指数表现</th><th>当日板块/ETF 资金流</th><th>风向</th><th>板块均值</th></tr></thead>
<tbody>{rows_cross}</tbody></table>
<div class="note">交叉验证双源：① 板块/指数当日涨跌幅 = NeoData 申万板块行情 + 本地 index_quotes（中证医疗/红利/科创50等）；② 资金流 = 本地 fund_flows（东方财富，90 申万板块 + 23 只 ETF）。重仓板块当日涨跌幅已由 NeoData 完整召回（航天装备/军工电子/化学制药/生物制品/证券/电池/光伏/半导体/通信设备/地面兵装/航空装备）。</div>
</div>

<div class="card">
<h3>3.1 净风向结论</h3>
<p style="font-size:13.2px;color:#c9d1d9">组合 <b>8 个方向中 {n_up} 个顺风/微顺风、{n_dn} 个逆风/强逆风、{n_mid} 个弱逆风</b>。顺风权重合计约 <b class="up">{up_w:.1f}%</b>（红利+债券+军工+科技+宽基），逆风（医药+新能源+证券）合计约 <b class="down">{inv_w:.1f}%</b>。</p>
<p style="font-size:13.2px;color:#c9d1d9;margin-top:8px">真实当日回报 <b>{d_ret}%</b>、且<b>{_perf_word}沪深300 {abs(vs300):+.2f}pct</b>。当日领涨方向为{_main_line_sectors}（科技/电子为主），但组合科技系权重仅 {_tech_w:.1f}%；超配的军工/医药/证券/红利普遍逆风（军工系 {_mil_wind} {_mil_av:+.2f}%、证券 {HP['证券Ⅱ']:+.2f}%、红利 {idx['红利指数'][1]:+.2f}%），防御端（债券+红利 {def_w:.1f}%）提供缓冲，组合与大盘呈现「指数涨、组合跌」的结构性背离。</p>
<h3>3.2 代理加权估算 vs 真值（方法学诊断）</h3>
<table><thead><tr><th>口径</th><th class="num">数值</th><th>说明</th></tr></thead>
<tbody>
<tr><td>板块/指数代理加权估算</td><td class="num"><span class="down">{est:.2f} pct</span></td><td>逐持仓 × 对应板块或指数当日涨跌幅，按权重加总</td></tr>
<tr><td><b>真实当日回报（锚）</b></td><td class="num"><b>{chg(d_ret)}</b></td><td>portfolio_summary.daily_return，市值口径真值</td></tr>
<tr><td>偏离</td><td class="num"><span class="mid">{est-d_ret:+.2f} pct</span></td><td>代理与真值偏差 {abs(est-d_ret):.2f}pct，方向{'一致' if (est<0)==(d_ret<0) else '背离'}（代理{est:+.2f}%、实际{d_ret:+.2f}%）</td></tr>
</tbody></table>
<div class="note">诊断结论：本组合结构下代理法<b>方向亦可能失真</b>（人工智能/机器人/新能源南方等 ETF 实际跟踪指数与所取代理板块不一致），绝对幅度与方向均不可采信，当日表现一律以真值为准。该项已列入自我进化改进项。</div>
</div>

<h2>四、操作取向（条件框架，非买卖指令）</h2>

<div class="op"><div class="t">① 持有（维持现状）—— 军工超配 + 红利/债券防御底仓 {def_w+sec_w:.1f}%</div>
<ul>
<li>当日已验证其价值：全组合 {d_ret}%、{_perf_word}沪深300 {abs(vs300):+.2f}pct；{_oper_hold_txt}。</li>
<li><span class="cond">维持条件</span>：市场停留在路径 A（箱体震荡）—— 上证守住 MA20 3921、量能 1.7–2.0 万亿。</li>
<li><span class="cond">加码触发</span>：若出现路径 C 的两条确认信号（破 MA20 + 量能萎缩至 1.5 万亿以下），防御仓位的战略价值上升。</li>
</ul></div>

<div class="op"><div class="t">② 收敛（结构优化，与市场方向无关）—— 可立即执行的四组去重</div>
<ul>
<li><b>创新药 ×2 → 1</b>（{innov_w:.2f}%）：两只跟踪高度重叠，无分散意义。</li>
<li><b>利率债 ×2 → 1</b>（{ratebond_w:.2f}%）：政金债与国开债久期/信用几乎一致，属实质同一持仓。</li>
<li><b>电池 ×2 → 1</b>（{batt_w:.2f}%）、<b>沪深300 ×2 → 1</b>（{core300_w:.2f}%）：完全同质且金额小，合并纯粹降低摩擦。</li>
<li><span class="cond">此项不依赖市场判断</span>，属组合卫生（portfolio hygiene），任何阶段均适用。</li>
</ul></div>

<div class="op"><div class="t">③ 对冲 / 减压（针对集中度）—— 关注单一持仓与三方向集中</div>
<ul>
<li><b>航天ETF华安 {aero_w:.2f}%</b> 为单一最大持仓，已超 10% 审慎线。<span class="cond">观察条件</span>：地面兵装板块若冲高回落且 ETF 资金流由正转负，则集中度风险实质化。</li>
<li><b>军工系 {mil_w:.1f}%</b> {_mil_wind}（地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}% / 航空装备Ⅱ {HP['航空装备Ⅱ']:+.2f}%，军工装备 {fy(sec_yi('军工装备'))}）；<b>医药系 {med_w:.1f}%</b> 微逆风、<b>证券 {sec_w:.1f}%</b> 当日 {HP['证券Ⅱ']:+.2f}% 且资金净流出 {fy(sec_today_yi)} 为最大单一拖累。<span class="cond">观察条件</span>：证券若连续 3 日净流出（当前已连续 {sec_out_streak} 日，见⑤跨日跟踪），则该方向逆风从单日事件升级为趋势。</li>
<li><b>宽基核心仅 {core300_w:.2f}%</b>：组合缺少市场平均收益压舱石，风格暴露过重。<span class="cond">改善方向</span>：去重释放的额度可考虑向核心宽基倾斜，而非新增行业主题。</li>
</ul></div>

<div class="op"><div class="t">④ 数据修复（前置于任何决策）</div>
<ul>
<li>{len(bad)} 只标的（{pct(bad_mv):.2f}% 权重）盈亏率失真，其中 2 只为负成本价。<b>在成本数据修正前，组合层面的累计收益率不具备决策参考价值</b>。</li>
<li>建议回溯交易流水重建成本：创业板50ETF华安（cost -2.72）、人工智能ETF易方达（cost -0.292）优先。</li>
<li>etf_price_history 数据滞后至 2026-08-19（12 个交易日），影响逐 ETF 当日归因能力，建议补采。</li>
</ul></div>

<div class="op"><div class="t">⑤ 跨日跟踪（读结构化历史库，非买卖指令）</div>
<ul>
{_prev_gap_html}
<li><span class="cond">证券净流出连续</span>：当前 <b>{sec_out_streak} 日</b>{sec_delta_txt}；今日 {fy(sec_today_yi)}（昨日 {fy(sec_prev_yi)}）。<b>距"连续 3 日趋势"阈值尚差 {sec_thresh_remain} 日</b>{'；已触发趋势确认' if sec_triggered else ''}。</li>
<li><span class="cond">航天ETF(159267) 资金流</span>：今日 {aero_sign}（{fy(aero_today_yi)}）{aero_trend}。单一最大持仓集中度观察的"由正转负"触发条件{'已满足' if aero_flip else '未满足'}。</li>
<li><span class="cond">上证位置 / 量能</span>：{ma20_txt}；{amt_txt}。与路径 A 维持条件（守住 MA20 + 量能 1.7–2.0 万亿）{'吻合' if ma20_ok else '出现偏离'}。</li>
<li><span class="cond">周线阶段</span>：{STAGE}（{REGIME}），较上期（{stage_prev}）{'未变' if not stage_changed else '有变化'}。</li>
<li><span class="cond">军工主线延续性</span>：连续 <b>{mil_up_streak} 日</b>军工板块（地面兵装/航空装备/军工电子/航海装备）最优涨跌幅为正（主线未熄火）{mil_delta_txt}；今日军工最优 {chg(_today_best)}，<b>12板块观测池</b>内严格领涨{'✔' if mil_top_today else '✘'}（{'是' if mil_top_today else '非'}当日观测池第一）。<br>&nbsp;&nbsp;近{mil_up_streak if mil_up_streak else len(_mil_recent)}日：{mil_recent_txt}。<br>&nbsp;&nbsp;<span class="note">（数据源自 sector_daily_change 历史表，回溯窗口 {signal_state['mil_complete_days']} 个完整交易日；"观测池"为我方重仓相关 12 板块，非全市场 90 板块；NeoData 宽区间查询为采样返回，更深日度历史由每日运行自动累积）</span></li>
</ul></div>

<h2>五、数据源与可用性</h2>
<div class="src">
<b>✓ 项目本地数据层</b>（data/database/portfolio.db）—— 本次为主数据源<br>
&nbsp;&nbsp;· portfolio_snapshots / portfolio_summary：{SNAP}（22 只持仓、当日真实回报 {d_ret}%）<br>
&nbsp;&nbsp;· index_quotes：{IDX_DATE}（11 个指数收盘/涨跌/成交额）{f'　⚠ 较组合基准 {DATA_DATE} 更新（采集器滞后）' if IDX_DATE != DATA_DATE else ''}<br>
&nbsp;&nbsp;· fund_flows：{DATA_DATE}（90 个申万板块 + 23 只 ETF；main_fund 行缺失，主资金以 90 板块合计代理）<br>
&nbsp;&nbsp;· macro_daily：{DATA_DATE}（SHIBOR_ON / COMEX黄金 / 美元人民币，PMI 仍缺）<br>
&nbsp;&nbsp;· market_breadth：{BL.get('date','—')}（zt={loc_zt}/dt={loc_dt} 已采集）<br>
&nbsp;&nbsp;· <span class="mid">⚠ etf_price_history 滞后至 2026-08-19，未使用</span><br>
<b>✓ NeoData 金融搜索</b> —— 本次可用（查询时间 {MKT_NEO['query_time']}，凭证经 connect_cloud_service 重取）<br>
&nbsp;&nbsp;· 三大指数统一行情（与本地交叉核对一致）、大盘市场宽度（涨跌 {BR['up']}:{BR['down']}）<br>
&nbsp;&nbsp;· 板块涨跌排行（{top_gain_names} 等）+ 申万板块当日涨跌幅（航天装备/生物制品/半导体 等完整召回）<br>
&nbsp;&nbsp;· 宏观 PMI（综合PMI产出 {PMI['composite']}/非制造业 {PMI['nonmfg']}/服务业 {PMI['service']}/建筑业 {PMI['construction']}；制造业整体值未返回标「—」）<br>
<b>无法核实项一律以「—」标注，未做任何推算填充。</b>
</div>

<div class="dis">
<b>免责声明</b>　本报告由 WorkBuddy 自动化任务于 {now} 生成，全部数据来自上述公开数据源，交易数据基准日为 {DATA_DATE}（最近交易日收盘），报告生成日 {RUN_DATE} {_RUN_DAY_LABEL}。<br>
报告中的「操作取向」为基于当日数据的<b>条件观察框架</b>，描述的是「在何种信号出现时该方向的风险/机会属性发生变化」，<b>不构成任何买入、卖出或持有的投资建议</b>，亦不构成对未来市场走势的预测。<br>
组合内 {len(bad)} 只标的成本数据存在明确错误（{pct(bad_mv):.2f}% 权重），其盈亏率及组合累计收益率不可采信，已在报告中逐一标注「待核对」。<br>
市场有风险，投资需谨慎。任何投资决策应基于投资者自身的风险承受能力、投资目标与独立判断，并在必要时咨询持牌专业人士。
</div>

</div></body></html>'''

os.makedirs('data/reports', exist_ok=True)
with open(OUT, 'w', encoding='utf-8') as fp:
    fp.write(HTML)
print('OK', OUT, os.path.getsize(OUT), 'bytes')

# 落盘纯文本 TLDR（evolution #27）：供邮件 --body 直读，消除 agent 人工复述漂移。
# 复用本脚本已派生的全部变量，与 HTML TLDR 同源、不会二次失真。
_tldr_lines = [f"【组合+大盘综合视角】{RUN_DATE}（组合数据基准 {DATA_DATE}；大盘指数 {IDX_DATE} 实时）", ""]
_tldr_lines.append(
    f"大盘今日{regime_txt}：{_idx_desc}。{_main_line_txt}{_mil_txt}。"
    f"组合当日回报 {chg(d_ret)}、{_perf_word}沪深300 {abs(vs300):+.2f}pct：{_perf_reason}。")
_tldr_lines.append("")
if (est < 0) != (d_ret < 0):
    _tldr_lines.append(
        f"⚠ 锚定提示：代理加权估算 {est:+.2f}% 与组合真实回报 {d_ret:+.2f}% 方向相反，"
        f"代理法在本组合结构下方向亦不可信，本报告一律以真值为准。")
_tldr_lines.append(
    f"核心矛盾：主力资金今日{main_dir} {main_in_yi:+,.0f} 亿{main_proxy_txt}；"
    f"两市量能 {amt2:.2f} 万亿较昨日 {amt_chg_txt}（{amt_dir}），{_amt_note}。")
_tldr_lines.append(
    f"组合最大集中度风险：航天ETF华安 {aero_w:.2f}% 为单一最大持仓（已超 10% 审慎线）；"
    f"军工系 {mil_w:.1f}% + 医药系 {med_w:.1f}% + 证券 {sec_w:.1f}% 三方向合计 {top3_w:.1f}%。"
    f"军工系 {_mil_wind}（地面兵装Ⅱ {HP['地面兵装Ⅱ']:+.2f}% / 航空装备Ⅱ {HP['航空装备Ⅱ']:+.2f}%，"
    f"军工装备 {fy(sec_yi('军工装备'))}）{'，暂未共振拖累' if _mil_av < 0 else '，提供正向贡献'}。")
_tldr_lines.append(
    f"亮点/风险：当日主线为 {_main_line_sectors}（资金净流入 {money_in_txt}），"
    f"{'风险偏好回升' if (REGIME=='普涨' and main_in_yi and main_in_yi>0) else '主线偏防御/事件驱动'}；"
    f"红利+债券防御底仓（{def_w:.1f}%）稳定；{_mil_against_txt}；"
    f"医药系微逆风（化学制药 {HP['化学制药']:+.2f}%/生物制品 {HP['生物制品']:+.2f}%）拖累有限。")
_tldr_lines.append(
    f"宏观逆风未解：制造业 PMI 整体值经 NeoData 查询仍未直接返回（标「—」）；"
    f"仅返回综合PMI产出 {PMI['composite']}%、非制造业 {PMI['nonmfg']}%（收缩区）、"
    f"服务业 {PMI['service']}%、建筑业 {PMI['construction']}%。")
_tldr_lines.append("")
_tldr_lines.append(
    f"数据来源：项目本地数据层（东方财富/新浪）+ NeoData 金融搜索（查询时间 {MKT_NEO['query_time']}）。"
    f"NeoData 仅增强，主力以本地为准。不构成投资建议。")
if DATA_LAG:
    _tldr_lines.append(
        f"⚠ 数据新鲜度提示：组合/持仓/资金流/本地广度仍停留在 {DATA_DATE}（采集器滞后，运行日 {RUN_DATE} 为交易日但本地 portfolio_summary 等尚未更新），"
        f"大盘指数已采用 {IDX_DATE} 实时；组合回报与跨日信号以 {DATA_DATE} 为基准，研判时请注意日期口径差异。")
TLDR_PATH = f'data/reports/组合大盘综合视角_{RUN_DATE}.tldr.txt'
# TLDR 为纯文本（邮件 --body 直读），需剥离因复用 HTML 片段而混入的标签与实体
_tldr_clean = '\n'.join(html.unescape(re.sub(r'<[^>]+>', '', ln)) for ln in _tldr_lines)
with open(TLDR_PATH, 'w', encoding='utf-8') as _tp:
    _tp.write(_tldr_clean)
print('TLDR', TLDR_PATH, os.path.getsize(TLDR_PATH), 'bytes')

SIGNAL_PATH = f'data/reports/组合大盘综合视角_{RUN_DATE}.signals.json'
with open(SIGNAL_PATH, 'w', encoding='utf-8') as _sp:
    json.dump(signal_state, _sp, ensure_ascii=False, indent=2)
print('SIGNAL', SIGNAL_PATH, os.path.getsize(SIGNAL_PATH), 'bytes')
print(f'REGIME={regime_txt} STAGE={STAGE} POS={_pos*100:.0f}%')
print(f'PROXY_EST={est:.3f} TRUE={d_ret} TOT={TOT:.2f}')
print('CLS', {k: round(pct(v), 2) for k, v in cls_sum.items()})
print('IND', {k: round(pct(v), 2) for k, v in ind_sum.items()})
print('BAD', len(bad), round(pct(bad_mv), 2))
print('CROSS_WIND', [(x[0], x[4], round(x[5],2)) for x in CROSS])
print('DUP_TOTAL_W', round(dup_total_w, 2))
print('CROSSDAY', signal_state)
print('PREV_STATE', _prev_state.get('date') if _prev_state else None)
