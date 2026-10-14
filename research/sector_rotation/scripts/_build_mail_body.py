# -*- coding: utf-8 -*-
"""
_build_mail_body.py —— 生成《A股行业轮动与资金流向监控仪表盘》的完整邮件正文
（纯文本 + 浅色 HTML 两版），并可选直接经 smtp-mail-sender 发送。

设计目的：把「报告主要内容」直接写进邮件正文，收件人不必打开附件即可读完全文。

用法：
  python _build_mail_body.py                      # 用 history.json 最新快照生成正文
  python _build_mail_body.py --date 2026-09-07    # 指定交易日
  python _build_mail_body.py --send               # 生成后立即发送（含 HTML 附件仪表盘）
  python _build_mail_body.py --print              # 仅打印纯文本正文到 stdout

输出：
  outputs/mail_body_<date>.txt
  outputs/mail_body_<date>.html
"""
import argparse
import io
import json
import os
import re
import subprocess
import sys
import unicodedata

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, 'outputs')
HISTORY = os.path.join(OUT_DIR, 'history.json')
DASHBOARD = os.path.join(OUT_DIR, 'A股行业轮动与资金流向监控仪表盘.html')

SEND_SCRIPT = r'C:\Users\HUAWEI\.workbuddy\skills\smtp-mail-sender\send_generic_email.py'
# 本工作区专用 .env（2026-10-14 解耦）：不再跨工作区依赖 portfolio_tracker/.env
ENV_PATH = r'C:\Users\HUAWEI\WorkBuddy\2026-08-04-15-30-49\.env'
MAIL_TO = 'asdfl@qq.com'
FROM_NAME = 'WorkBuddy 行业轮动仪表盘'

UP, DOWN = '涨', '跌'


# ---------------------------------------------------------------- 基础工具
def wlen(s):
    """显示宽度（中日韩全角计 2）"""
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)


def pad(s, width, align='l'):
    s = str(s)
    space = max(0, width - wlen(s))
    return s + ' ' * space if align == 'l' else ' ' * space + s


def strip_tags(s):
    if not s:
        return ''
    s = re.sub(r'<br\s*/?>', '\n', s)
    s = re.sub(r'</p>|</div>|</li>', '\n', s)
    s = re.sub(r'<[^>]+>', '', s)
    s = (s.replace('&nbsp;', ' ').replace('&amp;', '&')
          .replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"'))
    return re.sub(r'\n{3,}', '\n\n', s).strip()


def fmt_pct(v, nd=2, sign=True):
    if v is None or isinstance(v, str):
        return '—' if v is None else v
    return ('%+.{0}f%%'.format(nd) if sign else '%.{0}f%%'.format(nd)) % v


def fmt_num(v, nd=2, sign=True):
    if v is None or isinstance(v, str):
        return '—' if v is None else v
    return ('%+.{0}f'.format(nd) if sign else '%.{0}f'.format(nd)) % v


def parse_amt(s):
    """'8979.04亿' / '约1.95万亿' -> 亿元 float；失败返回 None"""
    if s is None:
        return None
    s = str(s).strip().replace(',', '')
    m = re.search(r'([\d.]+)\s*万亿', s)
    if m:
        return float(m.group(1)) * 10000
    m = re.search(r'([\d.]+)\s*亿', s)
    if m:
        return float(m.group(1))
    return None


def pick_nb_top(tops, per=5):
    """沪股通前 per + 深股通前 per；不足则按原顺序补足"""
    if not tops:
        return []
    sh = [x for x in tops if x.get('m') == '沪'][:per]
    sz = [x for x in tops if x.get('m') == '深'][:per]
    out = sh + sz
    if not out:
        out = tops[:per * 2]
    return out


def cls_of(v):
    if v is None or isinstance(v, str):
        return 'flat'
    return 'up' if v > 0 else ('down' if v < 0 else 'flat')


def esc(s):
    return (str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))


# ---------------------------------------------------------------- 数据装载
def load_history(date=None):
    with io.open(HISTORY, encoding='utf-8') as f:
        data = json.load(f)
    snaps = data['snapshots']
    if not snaps:
        raise SystemExit('history.json 无快照')
    cur = None
    if date:
        for s in snaps:
            if s['date'] == date:
                cur = s
        if cur is None:
            raise SystemExit('history.json 未找到 %s' % date)
    else:
        cur = snaps[-1]
    idx = snaps.index(cur)
    prev = snaps[idx - 1] if idx > 0 else None
    return data, cur, prev, len(snaps)


def load_snap(date):
    """从 HTML 的 SNAPS 中取该日期（CALLS/compass/banner 等），失败返回 {}"""
    tmp = os.path.join(ROOT, '_tmp_snap_%s.json' % date)
    node = r'C:\Users\HUAWEI\.workbuddy\binaries\node\versions\22.22.2-6\node.exe'
    try:
        if os.path.exists(tmp):
            os.remove(tmp)
        subprocess.run([node, os.path.join(ROOT, '_snap_dump.js'), date, tmp],
                       cwd=ROOT, check=True, capture_output=True)
        with io.open(tmp, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


# ---------------------------------------------------------------- 纯文本正文
def build_text(cur, prev, snap, total_snaps, snap_keys):
    L = []
    A = L.append
    date = cur['date']
    A('【A股行业轮动与资金流向监控】%s（%s）收盘复盘' % (date, cur.get('weekday', '')))
    A('抓取时点：%s ｜ 历史库快照：%d 条 ｜ 仪表盘日期键：%s' % (
        cur.get('fetchedAt', '—'), total_snaps, snap_keys if snap_keys else '—'))
    A('')
    A('=' * 60)
    if cur.get('note'):
        A('■ 一句话结论')
        A(strip_tags(cur['note']))
        A('')
    if snap.get('banner'):
        b = strip_tags(snap['banner'])
        A('■ 主线叙述')
        A(b)
        A('')

    # 一、指数
    A('=' * 60)
    A('一、主要指数')
    A('-' * 60)
    A(pad('指数', 12) + pad('收盘', 12) + pad('涨跌幅', 10) + pad('涨跌点', 10) + pad('成交额', 14) + 'PE')
    for it in cur['indices']:
        A(pad(it['n'], 12) + pad(it.get('pt', '—'), 12) +
          pad(fmt_pct(it.get('chg')), 10) + pad(fmt_num(it.get('pts')), 10) +
          pad(it.get('amt', '—'), 14) + str(it.get('pe', '—')))
    if snap.get('idxNote'):
        A('注：' + strip_tags(snap['idxNote']))
    A('')

    # 二、市场宽度
    A('=' * 60)
    A('二、市场宽度')
    A('-' * 60)
    for s in cur.get('strip', []):
        A('  ' + pad(s['k'], 12) + str(s['v']) + ('（%s）' % strip_tags(s['x']) if s.get('x') else ''))
    A('')

    # 三、行业
    secs = cur.get('sectors', [])
    valid_p = [s for s in secs if s.get('p') is not None]
    valid_f = [s for s in secs if s.get('f') is not None]
    by_p = sorted(valid_p, key=lambda x: -x['p'])
    by_f = sorted(valid_f, key=lambda x: -x['f'])
    A('=' * 60)
    A('三、申万一级行业（%d/%d 有涨跌幅，%d/%d 有主力资金；★=核心跟踪）'
      % (len(valid_p), len(secs), len(valid_f), len(secs)))
    A('-' * 60)
    A('  涨幅前 5：')
    for s in by_p[:5]:
        A('    ' + pad(('★' if s.get('core') else '·') + s['n'], 12) + fmt_pct(s['p']))
    A('  跌幅前 5：')
    for s in by_p[-5:][::-1]:
        A('    ' + pad(('★' if s.get('core') else '·') + s['n'], 12) + fmt_pct(s['p']))
    if valid_f:
        A('  主力净流入 TOP5（亿元）：')
        for s in by_f[:5]:
            A('    ' + pad(('★' if s.get('core') else '·') + s['n'], 12) + pad(fmt_num(s['f']), 10)
              + '｜涨跌 ' + fmt_pct(s.get('p')))
        A('  主力净流出 TOP5（亿元）：')
        for s in by_f[-5:][::-1]:
            A('    ' + pad(('★' if s.get('core') else '·') + s['n'], 12) + pad(fmt_num(s['f']), 10)
              + '｜涨跌 ' + fmt_pct(s.get('p')))
    A('')

    # 四、资金
    A('=' * 60)
    A('四、资金流向')
    A('-' * 60)
    if valid_f:
        tot = sum(s['f'] for s in valid_f)
        A('  31 行业主力净流入合计：%s 亿元（可得 %d/%d 行业）' % (fmt_num(tot), len(valid_f), len(secs)))
    else:
        A('  当日主力资金未披露/未取到，全部标 null。')
    if snap.get('flowSrc'):
        A('  口径：' + strip_tags(snap['flowSrc']))
    A('')

    # 五、风格
    A('=' * 60)
    A('五、风格与规模')
    A('-' * 60)
    lad = cur.get('ladder', [])
    if lad:
        A('  规模谱系：' + ' ｜ '.join('%s(%s) %s' % (x['n'], x.get('t', ''), fmt_pct(x.get('v')))
                                      for x in lad))
    for g in cur.get('gv', []):
        A('  %s %s  vs  %s %s   → 剪刀差 %s pct' % (
            pad(g.get('g', ''), 10), fmt_pct(g.get('gv')),
            pad(g.get('v', ''), 10), fmt_pct(g.get('vv')), fmt_num(g.get('gap'))))
    cp = (snap.get('compass') or {})
    t, p = cp.get('today'), cp.get('prev')
    if t:
        A('  风格罗盘：今日 x=%.0f（0大盘→100小盘），y=%.0f（0成长→100价值）'
          % (t.get('x', 0), t.get('y', 0)))
        if p:
            A('            前一交易日 x=%.0f, y=%.0f → 位移 Δx=%+.0f, Δy=%+.0f'
              % (p.get('x', 0), p.get('y', 0), t.get('x', 0) - p.get('x', 0), t.get('y', 0) - p.get('y', 0)))
        else:
            A('            无前一日快照，无法计算位移')
    if snap.get('compassNote'):
        A('  ' + strip_tags(snap['compassNote']))
    A('')

    # 六、北向
    A('=' * 60)
    A('六、北向资金')
    A('-' * 60)
    nb = cur.get('northbound', [])
    today_key = date[5:].replace('-', '-')
    cur_nb = None
    for x in nb:
        if x.get('d') == today_key:
            cur_nb = x.get('v')
    prev_nb = None
    for x in nb[-6:]:
        if x.get('v') is not None and x.get('d') != today_key:
            prev_nb = x.get('v')
    amt_total = (parse_amt(cur['indices'][0].get('amt')) or 0) + (parse_amt(cur['indices'][1].get('amt')) or 0)
    if cur_nb is None:
        A('  当日北向成交总额：待披露（官方 17:40 后发布，后续回补）')
    else:
        A('  当日北向成交总额：%.2f 亿元' % cur_nb)
        if amt_total:
            A('  占两市成交比：%.2f%%（两市合计约 %.0f 亿）' % (cur_nb / amt_total * 100, amt_total))
        if prev_nb:
            A('  较前一已披露交易日：%s 亿元' % fmt_num(cur_nb - prev_nb))
    A('  口径：自 2024-08-18 起官方已取消每日净买入/净卖出披露，本表只列成交总额，不编造净买卖数字。')
    tops = pick_nb_top(cur.get('nbTop', []))
    if tops:
        A('  十大成交股（沪股通前 5 + 深股通前 5）：')
        A('    ' + pad('市场', 6) + pad('名称', 12) + pad('成交额(亿)', 12) + '当日涨跌')
        for x in tops:
            A('    ' + pad(x.get('m', ''), 6) + pad(x.get('n', ''), 12) +
              pad(fmt_num(x.get('a'), sign=False), 12) + fmt_pct(x.get('c')))
    A('')

    # 七、配置研判
    calls = snap.get('CALLS') or []
    A('=' * 60)
    A('七、配置研判（AI 自动生成，需人工复核）')
    A('-' * 60)
    if not calls:
        A('  当日未生成配置研判。')
    for c in calls:
        A('  【%s】%s ｜ %s' % (c.get('no', ''), c.get('title', ''), c.get('rate', '')))
        if c.get('variant'):
            A('    变异认知：' + strip_tags(c['variant']))
        if c.get('evidence'):
            A('    证据：' + strip_tags(c['evidence']))
        if c.get('falsify'):
            A('    证伪条件：' + strip_tags(c['falsify']))
        if c.get('action'):
            A('    行动：' + strip_tags(c['action']))
        if c.get('tags'):
            A('    标签：' + ' / '.join(c['tags']))
        A('')
    if snap.get('callsSrc'):
        A('  ' + strip_tags(snap['callsSrc']))
        A('')

    # 八、缺口与来源
    A('=' * 60)
    A('八、数据缺口与来源')
    A('-' * 60)
    gaps = []
    if len(valid_f) < len(secs):
        gaps.append('主力资金：%d/%d 行业可得' % (len(valid_f), len(secs)))
    if len(valid_p) < len(secs):
        gaps.append('行业涨跌幅：%d/%d 可得' % (len(valid_p), len(secs)))
    if cur_nb is None:
        gaps.append('北向成交总额：待披露')
    for s in cur.get('strip', []):
        if '—' in str(s.get('v', '')):
            gaps.append('%s：%s' % (s['k'], s['v']))
    A('  缺口：' + ('；'.join(gaps) if gaps else '无显著缺口'))
    for i, s in enumerate(cur.get('sources', []), 1):
        A('  [%d] %s' % (i, strip_tags(s)))
    A('')
    A('=' * 60)
    A('说明：完整交互版仪表盘见邮件附件（HTML，可直接浏览器打开，支持历史日回看与双日对比）。')
    A('免责：数据来自公开渠道，无法核实项一律标“—”/待披露，未作任何编造；配置研判由 AI 生成，仅供研究参考，不构成投资建议。')
    return '\n'.join(L)


# ---------------------------------------------------------------- HTML 正文（浅色·邮件客户端友好）
CSS = """
body{margin:0;padding:0;background:#f5f6f8;font-family:-apple-system,"Segoe UI","Microsoft YaHei",Arial,sans-serif;color:#1f2329;}
.wrap{max-width:760px;margin:0 auto;background:#ffffff;padding:24px 28px 32px;}
h1{font-size:20px;margin:0 0 4px;color:#111418;}
h2{font-size:15px;margin:26px 0 8px;padding-left:9px;border-left:4px solid #2f6fed;color:#111418;}
.sub{font-size:12px;color:#6b7280;margin-bottom:6px;}
table{width:100%;border-collapse:collapse;font-size:13px;}
th{background:#f0f2f5;color:#4b5563;font-weight:600;text-align:right;padding:7px 8px;border-bottom:1px solid #e5e7eb;}
th:first-child{text-align:left;}
td{padding:7px 8px;border-bottom:1px solid #f0f1f3;text-align:right;color:#1f2329;}
td:first-child{text-align:left;}
.up{color:#c62828;font-weight:600;}
.down{color:#2e7d32;font-weight:600;}
.flat{color:#6b7280;}
.note{font-size:12px;color:#6b7280;line-height:1.7;margin:6px 0 0;}
.box{background:#fafbfc;border:1px solid #eceff2;border-radius:6px;padding:12px 14px;font-size:13px;line-height:1.75;color:#1f2329;margin-bottom:10px;}
.call{border:1px solid #e5e7eb;border-left:4px solid #2f6fed;border-radius:6px;padding:12px 14px;margin-bottom:12px;}
.call .t{font-weight:700;font-size:14px;color:#111418;margin-bottom:4px;}
.call .r{font-size:12px;color:#2f6fed;margin-bottom:8px;}
.call .kv{font-size:12.5px;line-height:1.75;color:#374151;margin:3px 0;}
.call .k{color:#6b7280;font-weight:600;}
.tag{display:inline-block;background:#eef2ff;color:#2f6fed;border-radius:3px;padding:1px 7px;font-size:11px;margin-right:5px;}
.foot{margin-top:24px;padding-top:12px;border-top:1px solid #e5e7eb;font-size:11.5px;color:#9ca3af;line-height:1.7;}
.warn{background:#fff8e6;border:1px solid #ffe2a8;color:#8a5a00;border-radius:6px;padding:10px 12px;font-size:12px;line-height:1.7;}
"""


def build_html(cur, prev, snap, total_snaps, snap_keys):
    date = cur['date']
    H = []
    A = H.append
    A('<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">')
    A('<meta name="viewport" content="width=device-width,initial-scale=1"><title>行业轮动 %s</title>' % date)
    A('<style>%s</style></head><body><div class="wrap">' % CSS)
    A('<h1>A股行业轮动与资金流向监控 · %s（%s）</h1>' % (date, cur.get('weekday', '')))
    A('<div class="sub">抓取时点 %s ｜ 历史库 %d 条快照 ｜ 仪表盘日期键 %s</div>'
      % (cur.get('fetchedAt', '—'), total_snaps, snap_keys or '—'))
    if cur.get('note'):
        A('<div class="box"><b>一句话结论：</b>%s</div>' % strip_tags(cur['note']).replace('\n', '<br>'))
    if snap.get('banner'):
        A('<div class="box">%s</div>' % strip_tags(snap['banner']).replace('\n', '<br>'))

    # 指数
    A('<h2>一、主要指数</h2><table><tr><th>指数</th><th>收盘</th><th>涨跌幅</th><th>涨跌点</th><th>成交额</th><th>PE</th></tr>')
    for it in cur['indices']:
        A('<tr><td>%s</td><td>%s</td><td class="%s">%s</td><td class="%s">%s</td><td>%s</td><td>%s</td></tr>'
          % (esc(it['n']), it.get('pt', '—'), cls_of(it.get('chg')), fmt_pct(it.get('chg')),
             cls_of(it.get('pts')), fmt_num(it.get('pts')), esc(it.get('amt', '—')), esc(it.get('pe', '—'))))
    A('</table>')
    if snap.get('idxNote'):
        A('<p class="note">%s</p>' % strip_tags(snap['idxNote']))

    # 宽度
    A('<h2>二、市场宽度</h2><table><tr><th>指标</th><th>数值</th><th>说明</th></tr>')
    for s in cur.get('strip', []):
        c = {'up': 'up', 'down': 'down'}.get(s.get('c', ''), 'flat')
        A('<tr><td>%s</td><td class="%s"><b>%s</b></td><td style="text-align:left;color:#6b7280;font-size:12px">%s</td></tr>'
          % (esc(s['k']), c, esc(s['v']), esc(strip_tags(s.get('x', '')))))
    A('</table>')

    # 行业
    secs = cur.get('sectors', [])
    valid_p = [s for s in secs if s.get('p') is not None]
    valid_f = [s for s in secs if s.get('f') is not None]
    by_p = sorted(valid_p, key=lambda x: -x['p'])
    by_f = sorted(valid_f, key=lambda x: -x['f'])
    A('<h2>三、申万一级行业（涨跌幅 %d/%d，主力资金 %d/%d；★核心跟踪）</h2>'
      % (len(valid_p), len(secs), len(valid_f), len(secs)))
    A('<table><tr><th>涨幅前 5</th><th>涨跌幅</th><th>跌幅前 5</th><th>涨跌幅</th></tr>')
    lows = by_p[-5:][::-1]
    for i in range(5):
        a = by_p[i] if i < len(by_p) else None
        b = lows[i] if i < len(lows) else None
        A('<tr><td>%s</td><td class="%s">%s</td><td>%s</td><td class="%s">%s</td></tr>'
          % (esc(('★' if a and a.get('core') else '·') + (a['n'] if a else '—')),
             cls_of(a['p']) if a else 'flat', fmt_pct(a['p']) if a else '—',
             esc(('★' if b and b.get('core') else '·') + (b['n'] if b else '—')),
             cls_of(b['p']) if b else 'flat', fmt_pct(b['p']) if b else '—'))
    A('</table>')
    if valid_f:
        A('<table style="margin-top:12px"><tr><th>主力净流入 TOP5</th><th>净额(亿)</th><th>涨跌</th>'
          '<th>主力净流出 TOP5</th><th>净额(亿)</th><th>涨跌</th></tr>')
        fl = by_f[-5:][::-1]
        for i in range(5):
            a = by_f[i] if i < len(by_f) else None
            b = fl[i] if i < len(fl) else None
            A('<tr><td>%s</td><td class="%s">%s</td><td class="%s">%s</td>'
              '<td>%s</td><td class="%s">%s</td><td class="%s">%s</td></tr>'
              % (esc(('★' if a and a.get('core') else '·') + (a['n'] if a else '—')),
                 cls_of(a['f']) if a else 'flat', fmt_num(a['f']) if a else '—',
                 cls_of(a.get('p')) if a else 'flat', fmt_pct(a.get('p')) if a else '—',
                 esc(('★' if b and b.get('core') else '·') + (b['n'] if b else '—')),
                 cls_of(b['f']) if b else 'flat', fmt_num(b['f']) if b else '—',
                 cls_of(b.get('p')) if b else 'flat', fmt_pct(b.get('p')) if b else '—'))
        A('</table>')

    # 资金
    A('<h2>四、资金流向</h2>')
    if valid_f:
        A('<div class="box"><b>31 行业主力净流入合计：%s 亿元</b>（可得 %d/%d 行业，大单+特大单口径）</div>'
          % (fmt_num(sum(s['f'] for s in valid_f)), len(valid_f), len(secs)))
    else:
        A('<div class="box">当日主力资金未披露/未取到，全部标 null。</div>')
    if snap.get('flowSrc'):
        A('<p class="note">%s</p>' % strip_tags(snap['flowSrc']))

    # 风格
    A('<h2>五、风格与规模</h2>')
    lad = cur.get('ladder', [])
    if lad:
        A('<table><tr>' + ''.join('<th>%s</th>' % esc(x.get('t') or x['n']) for x in lad) + '</tr><tr>'
          + ''.join('<td class="%s">%s</td>' % (cls_of(x.get('v')), fmt_pct(x.get('v'))) for x in lad)
          + '</tr><tr>' + ''.join('<td style="font-size:11px;color:#9ca3af">%s</td>' % esc(x['n']) for x in lad)
          + '</tr></table>')
    gv = cur.get('gv', [])
    if gv:
        A('<table style="margin-top:12px"><tr><th>成长</th><th>涨跌幅</th><th>价值</th><th>涨跌幅</th><th>剪刀差(pct)</th></tr>')
        for g in gv:
            A('<tr><td>%s</td><td class="%s">%s</td><td>%s</td><td class="%s">%s</td><td class="%s">%s</td></tr>'
              % (esc(g.get('g', '')), cls_of(g.get('gv')), fmt_pct(g.get('gv')),
                 esc(g.get('v', '')), cls_of(g.get('vv')), fmt_pct(g.get('vv')),
                 cls_of(g.get('gap')), fmt_num(g.get('gap'))))
        A('</table>')
    cp = (snap.get('compass') or {})
    t, p = cp.get('today'), cp.get('prev')
    if t:
        txt = '风格罗盘：今日 x=%.0f（0 大盘→100 小盘），y=%.0f（0 成长→100 价值）' % (t.get('x', 0), t.get('y', 0))
        if p:
            txt += '；较前一交易日 Δx=%+.0f，Δy=%+.0f' % (t.get('x', 0) - p.get('x', 0), t.get('y', 0) - p.get('y', 0))
        A('<div class="box">%s</div>' % txt)
    if snap.get('compassNote'):
        A('<p class="note">%s</p>' % strip_tags(snap['compassNote']))

    # 北向
    A('<h2>六、北向资金</h2>')
    nb = cur.get('northbound', [])
    tk = date[5:]
    cur_nb = next((x.get('v') for x in nb if x.get('d') == tk), None)
    prev_nb = None
    for x in nb[-6:]:
        if x.get('v') is not None and x.get('d') != tk:
            prev_nb = x.get('v')
    amt_total = (parse_amt(cur['indices'][0].get('amt')) or 0) + (parse_amt(cur['indices'][1].get('amt')) or 0)
    if cur_nb is None:
        A('<div class="warn">当日北向成交总额：待披露（官方约 17:40 发布，后续回补）。</div>')
    else:
        s = '<b>成交总额 %.2f 亿元</b>' % cur_nb
        if amt_total:
            s += ' ｜ 占两市成交 %.2f%%（两市约 %.0f 亿）' % (cur_nb / amt_total * 100, amt_total)
        if prev_nb:
            s += ' ｜ 较前一已披露日 %s 亿' % fmt_num(cur_nb - prev_nb)
        A('<div class="box">%s</div>' % s)
    A('<div class="warn">披露口径：自 2024-08-18 起官方已取消每日净买入/净卖出披露，本表仅列成交总额与十大成交股，不编造任何净买卖数字。</div>')
    tops = pick_nb_top(cur.get('nbTop', []))
    if tops:
        A('<table style="margin-top:12px"><tr><th>市场</th><th>名称</th><th>成交额(亿)</th><th>当日涨跌</th></tr>')
        for x in tops:
            A('<tr><td>%s</td><td>%s</td><td>%s</td><td class="%s">%s</td></tr>'
              % (esc(x.get('m', '')), esc(x.get('n', '')), fmt_num(x.get('a'), sign=False),
                 cls_of(x.get('c')), fmt_pct(x.get('c'))))
        A('</table>')

    # 配置研判
    calls = snap.get('CALLS') or []
    A('<h2>七、配置研判（AI 自动生成，需人工复核）</h2>')
    if not calls:
        A('<p class="note">当日未生成配置研判。</p>')
    for c in calls:
        A('<div class="call"><div class="t">%s. %s</div><div class="r">%s</div>'
          % (c.get('no', ''), esc(c.get('title', '')), esc(c.get('rate', ''))))
        for k, label in (('variant', '变异认知'), ('evidence', '证据'),
                         ('falsify', '证伪条件'), ('action', '行动')):
            if c.get(k):
                A('<div class="kv"><span class="k">%s：</span>%s</div>' % (label, strip_tags(c[k])))
        if c.get('tags'):
            A('<div style="margin-top:6px">' + ''.join('<span class="tag">%s</span>' % esc(t) for t in c['tags']) + '</div>')
        A('</div>')
    if snap.get('callsSrc'):
        A('<p class="note">%s</p>' % strip_tags(snap['callsSrc']))

    # 缺口与来源
    A('<h2>八、数据缺口与来源</h2>')
    gaps = []
    if len(valid_f) < len(secs):
        gaps.append('主力资金 %d/%d 行业可得' % (len(valid_f), len(secs)))
    if len(valid_p) < len(secs):
        gaps.append('行业涨跌幅 %d/%d 可得' % (len(valid_p), len(secs)))
    if cur_nb is None:
        gaps.append('北向成交总额待披露')
    for s in cur.get('strip', []):
        if '—' in str(s.get('v', '')):
            gaps.append('%s 缺' % s['k'])
    A('<div class="box"><b>缺口：</b>%s</div>' % (esc('；'.join(gaps)) if gaps else '无显著缺口'))
    srcs = cur.get('sources', [])
    if srcs:
        A('<p class="note">' + '<br>'.join('[%d] %s' % (i, strip_tags(s)) for i, s in enumerate(srcs, 1)) + '</p>')

    A('<div class="foot">完整交互版仪表盘见邮件附件（HTML，浏览器直接打开，支持历史日回看与双日对比）。<br>'
      '数据来自公开渠道，无法核实项一律标“—”/待披露，未作任何编造；配置研判由 AI 生成，仅供研究参考，不构成投资建议。</div>')
    A('</div></body></html>')
    return '\n'.join(H)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', default=None, help='交易日 YYYY-MM-DD，默认最新快照')
    ap.add_argument('--send', action='store_true', help='生成后立即发送邮件')
    ap.add_argument('--print', dest='do_print', action='store_true', help='打印纯文本正文')
    ap.add_argument('--to', default=MAIL_TO)
    args = ap.parse_args()

    data, cur, prev, total = load_history(args.date)
    date = cur['date']
    snap = load_snap(date)
    snap_keys = ''
    if snap:
        try:
            with io.open(os.path.join(ROOT, '_tmp_keys.json'), encoding='utf-8') as f:
                snap_keys = str(len(json.load(f)['keys']))
        except Exception:
            snap_keys = '—'

    text = build_text(cur, prev, snap, total, snap_keys)
    html = build_html(cur, prev, snap, total, snap_keys)

    txt_path = os.path.join(OUT_DIR, 'mail_body_%s.txt' % date)
    html_path = os.path.join(OUT_DIR, 'mail_body_%s.html' % date)
    with io.open(txt_path, 'w', encoding='utf-8') as f:
        f.write(text)
    with io.open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)

    if args.do_print:
        sys.stdout.reconfigure(encoding='utf-8')
        print(text)

    print('[OK] text -> %s (%d chars)' % (txt_path, len(text)))
    print('[OK] html -> %s (%d chars)' % (html_path, len(html)))

    if args.send:
        py = sys.executable
        cmd = [py, SEND_SCRIPT,
               '--env-path', ENV_PATH,
               '--to', args.to,
               '--from-name', FROM_NAME,
               '--subject', '【行业轮动仪表盘】%s' % date,
               '--body', text,
               '--html-file', html_path,
               '--attach', DASHBOARD]
        r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding='utf-8', errors='replace')
        print('[SEND] exit=%d' % r.returncode)
        if r.stdout:
            print(r.stdout.strip())
        if r.stderr:
            print(r.stderr.strip())
        return r.returncode
    return 0


if __name__ == '__main__':
    sys.exit(main())
