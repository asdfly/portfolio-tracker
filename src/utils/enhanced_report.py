#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""增强版HTML邮件报告生成器 - 内嵌图表图片"""
import sqlite3
import logging
import base64
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

CSS = (
    "body{margin:0;padding:0;background:#0a1628;font-family:-apple-system,BlinkMacSystemFont,"
    "'Segoe UI',Roboto,Arial,sans-serif;}"
    ".c{max-width:720px;margin:20px auto;background:#111d35;border-radius:12px;overflow:hidden;"
    "box-shadow:0 2px 12px rgba(0,0,0,0.08);}"
    ".hd{background:linear-gradient(135deg,#1a73e8,#0d47a1);color:#fff;padding:24px 20px;text-align:center;}"
    ".hd h1{margin:0;font-size:20px;letter-spacing:1px;}"
    ".hd p{margin:4px 0 0;font-size:12px;color:#b0c4de;}"
    ".ms{display:flex;flex-wrap:wrap;gap:10px;padding:16px 20px;}"
    ".m{flex:1 1 30%;min-width:130px;padding:12px 14px;background:#162447;border-radius:8px;text-align:center;}"
    ".m .l{font-size:10px;color:#8899aa;margin-bottom:3px;text-transform:uppercase;letter-spacing:0.5px;}"
    ".m .v{font-size:18px;font-weight:700;}"
    ".m .s{font-size:10px;color:#95a5a6;margin-top:2px;}"
    ".sec{padding:0 20px 14px;}"
    ".st{font-size:14px;font-weight:600;color:#e0e6ed;margin:14px 0 8px;padding-bottom:5px;border-bottom:2px solid #2a3f5f;}"
    "table{width:100%;border-collapse:collapse;}"
    "th{padding:7px 10px;font-size:10px;color:#8899aa;text-transform:uppercase;letter-spacing:0.5px;"
    "text-align:left;background:#1a2d50;border-bottom:2px solid #2a3f5f;}"
    "th:nth-child(n+3),td:nth-child(n+3){text-align:right;}"
    ".ft{padding:14px 20px;text-align:center;font-size:10px;color:#607080;border-top:1px solid #1e3050;background:#0e1a2e;}"
    ".rg{display:flex;gap:10px;}"
    ".rc{flex:1;padding:12px;border-radius:8px;text-align:center;}"
    ".rc .rl{font-size:10px;color:#8899aa;}"
    ".rc .rv{font-size:16px;font-weight:700;margin-top:3px;}"
)


class EnhancedReportBuilder:

    def __init__(self, db_path: str):
        self.db_path = db_path

    def build_full_report(self, news_data=None) -> str:
        summary = self._load_summary()
        positions = self._load_positions()
        alerts = self._load_alerts()
        advice = self._load_advice()
        history = self._load_history(60)
        index_today = self._load_index_today()
        technical = self._load_technical()
        price_30d = self._load_price_30d_ago()

        if not summary or not positions:
            return "<p>暂无足够数据生成报告</p>"

        nav_b64 = self._build_nav_chart(history) if len(history) > 2 else ""
        dd_b64 = self._build_drawdown_chart(history) if len(history) > 5 else ""

        now = datetime.now()
        date_str = now.strftime('%Y年%m月%d日')
        wd_map = {0:'周一',1:'周二',2:'周三',3:'周四',4:'周五',5:'周六',6:'周日'}
        weekday = wd_map.get(now.weekday(), '')

        dr = summary.get('daily_return', 0) or 0
        tp = summary.get('total_pnl', 0) or 0
        tc = summary.get('total_cost', 1) or 1
        total_ret = tp / tc * 100 if tc > 0 else 0
        dp = summary.get('daily_pnl', 0) or 0

        def sign(v):
            return '+' if v >= 0 else ''
        def clr(v):
            return '#27ae60' if v >= 0 else '#e74c3c'

        sharpe = summary.get('sharpe_ratio')
        max_dd = summary.get('max_drawdown')
        vol = summary.get('volatility')

        def sf(v, fmt='.2f'):
            if v is None:
                return 'N/A'
            try:
                fv = float(v)
                if fv != fv:
                    return 'N/A'
                return f'{fv:{fmt}}'
            except Exception:
                return 'N/A'

        ss = sf(sharpe, '.3f')
        dds = sf(max_dd)
        vss = sf(vol)
        sc = '#27ae60' if sharpe and float(sharpe) > 0.5 else '#f39c12' if sharpe else '#95a5a6'
        ddc = '#e74c3c' if max_dd and abs(float(max_dd)) > 10 else '#f39c12' if max_dd and abs(float(max_dd)) > 5 else '#27ae60'
        vc = '#e74c3c' if vol and float(vol) > 25 else '#f39c12' if vol and float(vol) > 15 else '#27ae60'
        vbg = '#2d2618' if vol and float(vol) > 15 else '#162d1f'

        # 持仓表格
        tv = summary.get('total_value', 1) or 1
        pos_rows = ''
        for i, p in enumerate(positions):
            pnl = p.get('pnl', 0) or 0
            pnl_rate = p.get('pnl_rate', 0) or 0
            mv = p.get('market_value', 0) or 0
            wt = mv / tv * 100
            pc = clr(pnl)
            ps = sign(pnl)
            bg = '#162447' if i % 2 == 0 else '#1a2d50'
            pos_rows += (
                '<tr style="background:' + bg + ';">'
                '<td style="padding:7px 10px;font-size:12px;font-weight:500;">' + p['name'] + '</td>'
                '<td style="padding:7px 10px;font-size:12px;color:#7f8c8d;">' + p['code'] + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;">' + f"{p['quantity']:,.0f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;">' + f"{p['cost_price']:.3f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;font-weight:500;">' + f"{p['current_price']:.3f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;">' + ps + '¥' + f"{mv:,.0f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;color:' + pc + ';font-weight:500;">' + ps + '¥' + f"{pnl:,.0f}" + '</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;color:' + pc + ';">' + ps + f"{pnl_rate:.2f}" + '%</td>'
                '<td style="padding:7px 10px;font-size:12px;text-align:right;color:#7f8c8d;">' + f"{wt:.1f}" + '%</td>'
                '</tr>'
            )

        # 告警
        if alerts:
            ai = ''
            for a in alerts:
                lc = '#e74c3c' if a['level'] == 'error' else '#f39c12'
                ic = '🔴' if a['level'] == 'error' else '🟡'
                ai += '<tr><td style="padding:6px 10px;font-size:12px;">' + ic + ' <span style="color:' + lc + ';font-weight:600;">[' + a['level'].upper() + ']</span> ' + a['message'] + '</td></tr>'
            ab = '<div style="margin:14px 0;padding:14px;background:#2d1a1a;border-radius:8px;border-left:4px solid #e74c3c;"><div style="font-size:13px;font-weight:600;color:#e74c3c;margin-bottom:8px;">⚠️ 今日告警 (' + str(len(alerts)) + ')</div><table style="width:100%;border-collapse:collapse;">' + ai + '</table></div>'
        else:
            ab = '<div style="margin:14px 0;padding:14px;background:#1a2d1a;border-radius:8px;border-left:4px solid #27ae60;"><span style="font-size:12px;color:#27ae60;">✅ 今日无告警，投资组合运行正常</span></div>'

        # 智能建议
        adv_block = ''
        if advice:
            pm = {'high': ('🔴 高优先级', '#e74c3c'), 'medium': ('🟡 中优先级', '#f39c12'), 'low': ('🟢 低优先级', '#27ae60')}
            items = ''
            for a in advice[:6]:
                pl, pcolor = pm.get(a.get('priority', 'low'), ('⚪ 低', '#95a5a6'))
                items += '<div style="padding:6px 0;border-bottom:1px solid #2a3f5f;font-size:12px;"><span style="font-weight:600;color:' + pcolor + ';">' + pl + '</span> ' + a.get('title', '') + '</div>'
            adv_block = '<div style="margin:14px 0;padding:14px;background:#162447;border-radius:8px;"><div style="font-size:13px;font-weight:600;color:#e0e6ed;margin-bottom:8px;">💡 智能建议 (' + str(len(advice)) + ')</div>' + items + '</div>'

        # 图表
        cb = ''
        if nav_b64:
            cb += '<div style="margin:14px 0;"><div style="font-size:13px;font-weight:600;color:#e0e6ed;margin-bottom:8px;">📈 组合净值走势（vs 沪深300）</div><img src="data:image/png;base64,' + nav_b64 + '" style="width:100%;border-radius:6px;" /></div>'
        if dd_b64:
            cb += '<div style="margin:14px 0;"><div style="font-size:13px;font-weight:600;color:#e0e6ed;margin-bottom:8px;">📉 回撤曲线</div><img src="data:image/png;base64,' + dd_b64 + '" style="width:100%;border-radius:6px;" /></div>'

        # 基准对比板块
        index_block = self._build_index_comparison(index_today, summary, clr, sign)

        # 行业资讯板块
        news_block = self._build_news_section(news_data)

        # 技术信号板块
        tech_block = self._build_technical_signals(technical, price_30d)

        # 组装HTML
        html = (
            '<!DOCTYPE html><html><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<style>' + CSS + '</style></head><body>'
            '<div class="c">'
            '<div class="hd"><h1>📊 投资组合日报</h1><p>' + date_str + ' ' + weekday + '</p><p style=\'font-size:10px;margin-top:6px;color:#8899aa;\'>' + '数据来源: 新浪财经 / 东方财富 | 更新时间: ' + now.strftime('%H:%M:%S') + '</p></div>'
            '<div class="ms">'
            '<div class="m"><div class="l">总市值</div><div class="v" style="color:#1a73e8;">¥' + f"{summary['total_value']:,.0f}" + '</div></div>'
            '<div class="m"><div class="l">当日盈亏</div><div class="v" style="color:' + clr(dr) + ';">' + sign(dr) + '¥' + f"{dp:,.0f}" + '</div><div class="s">' + sign(dr) + f"{dr:.2f}" + '%</div></div>'
            '<div class="m"><div class="l">累计盈亏</div><div class="v" style="color:' + clr(tp) + ';">' + sign(tp) + '¥' + f"{tp:,.0f}" + '</div><div class="s">' + sign(tp) + f"{total_ret:.2f}" + '%</div></div>'
            '</div>'
            '<div class="sec"><div class="st">⚠️ 风险指标</div>'
            '<div class="rg">'
            '<div class="rc" style="background:#162d1f;"><div class="rl">夏普比率</div><div class="rv" style="color:' + sc + ';">' + ss + '</div></div>'
            '<div class="rc" style="background:#2d1a1a;"><div class="rl">最大回撤</div><div class="rv" style="color:' + ddc + ';">' + dds + '%</div></div>'
            '<div class="rc" style="background:' + vbg + ';"><div class="rl">年化波动率</div><div class="rv" style="color:' + vc + ';">' + vss + '%</div></div>'
            '</div></div>'
            + index_block
            + ab + cb
            + '<div class="sec"><div class="st">📋 持仓明细 (' + str(len(positions)) + '只，盈' + str(summary.get('profit_count', 0)) + '亏' + str(summary.get('loss_count', 0)) + ')</div>'
            '<table><thead><tr><th>名称</th><th>代码</th><th>持仓量</th><th>成本</th><th>现价</th><th>市值</th><th>盈亏</th><th>收益率</th><th>占比</th></tr></thead>'
            '<tbody>' + pos_rows + '</tbody></table></div>'
            + tech_block
            + adv_block
            + news_block
            + '<div class="ft">投资组合跟踪分析系统 v1.3 自动生成<br>本报告仅供参考，不构成任何投资建议或买卖操作指令。投资有风险，入市需谨慎。 | 生成时间: ' + now.strftime('%Y-%m-%d %H:%M:%S') + '</div>'
            '</div></body></html>'
        )
        return html

    def _build_nav_chart(self, history):
        fig, ax = plt.subplots(figsize=(7, 2.5), dpi=150)
        fig.patch.set_facecolor('#ffffff')
        ax.set_facecolor('#ffffff')
        dates = pd.to_datetime(history['date'])
        base = history.iloc[0]['total_value']
        nav = history['total_value'] / base * 100
        ax.plot(dates, nav, color='#1a73e8', linewidth=1.8, label='组合净值', zorder=3)
        ax.fill_between(dates, nav, alpha=0.06, color='#1a73e8')
        hs300 = self._load_index_history('sh000300', 60)
        if not hs300.empty:
            hs_dates = pd.to_datetime(hs300['date'])
            hs_base = hs300.iloc[0]['close']
            hs_nav = hs300['close'] / hs_base * 100
            ax.plot(hs_dates, hs_nav, color='#f59e0b', linewidth=1.2, linestyle='--', label='沪深300', alpha=0.8)
        ax.legend(fontsize=9, loc='upper left', framealpha=0.8)
        ax.grid(True, alpha=0.2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        ax.tick_params(axis='both', labelsize=8)
        ax.set_ylabel('净值', fontsize=9)
        fig.autofmt_xdate()
        plt.tight_layout(pad=1.0)
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', facecolor='#ffffff')
        buf.seek(0)
        b64 = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        return b64

    def _build_drawdown_chart(self, history):
        fig, ax = plt.subplots(figsize=(7, 1.8), dpi=150)
        fig.patch.set_facecolor('#ffffff')
        ax.set_facecolor('#ffffff')
        dates = pd.to_datetime(history['date'])
        values = history['total_value'].values
        peak = np.maximum.accumulate(values)
        dd = (values - peak) / peak * 100
        ax.fill_between(dates, dd, 0, alpha=0.2, color='#e74c3c')
        ax.plot(dates, dd, color='#e74c3c', linewidth=1.2)
        ax.grid(True, alpha=0.2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        ax.tick_params(axis='both', labelsize=8)
        ax.set_ylabel('回撤 (%)', fontsize=9)
        fig.autofmt_xdate()
        plt.tight_layout(pad=1.0)
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', facecolor='#ffffff')
        buf.seek(0)
        b64 = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        return b64

    def save_report(self, html, filename=None, news_data=None):
        if not filename:
            filename = datetime.now().strftime('daily_report_%Y%m%d.html')
        report_dir = Path(self.db_path).parent.parent.parent / 'data' / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        filepath = report_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("报告已保存: " + str(filepath))
        # 同步latest_report.html快捷链接
        latest_path = report_dir / 'latest_report.html'
        with open(latest_path, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("已同步: " + str(latest_path))
        return str(filepath)

    def _load_summary(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM portfolio_summary ORDER BY date DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def _load_positions(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM portfolio_snapshots WHERE date = (SELECT MAX(date) FROM portfolio_snapshots) ORDER BY market_value DESC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_history(self, days):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM portfolio_summary ORDER BY date DESC LIMIT ?", conn, params=(days,))
        conn.close()
        return df.sort_values('date').reset_index(drop=True)

    def _load_index_history(self, code, days):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT date, close FROM index_quotes WHERE code = ? ORDER BY date DESC LIMIT ?", conn, params=(code, days))
        conn.close()
        return df.sort_values('date').reset_index(drop=True)

    def _load_alerts(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT rule_name, level, message FROM alerts ORDER BY id DESC LIMIT 5")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_advice(self):
        report_dir = Path(self.db_path).parent.parent.parent / 'data' / 'reports'
        if not report_dir.exists():
            return []
        reports = sorted(report_dir.glob('smart_report_*.md'), reverse=True)
        if not reports:
            return []
        advices = []
        with open(reports[0], 'r', encoding='utf-8') as f:
            content = f.read()
        import re
        for m in re.finditer(r'### \d+\.\s+\[(高|中|低)\]\s+(.+?)(?:\n|$)', content):
            advices.append({'priority': m.group(1), 'title': m.group(2).strip()})
        return advices


    def _load_index_today(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT code, name, close, change_pct FROM index_quotes WHERE date = (SELECT MAX(date) FROM index_quotes)")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_technical(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""SELECT t.code, p.name, p.current_price, t.ma_signal, t.macd_signal, t.rsi_value, t.rsi_status,
                          t.kdj_signal, t.bollinger_position, t.atr_pct, t.trend
                          FROM etf_technical t LEFT JOIN portfolio_snapshots p 
                          ON t.code = p.code AND t.date = p.date
                          WHERE t.date = (SELECT MAX(date) FROM etf_technical)""")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]


    def _load_price_30d_ago(self):
        """加载30个交易日前的持仓价格，用于计算30日涨跌幅"""
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("""
                SELECT a.code, b.current_price as price_30d_ago
                FROM (SELECT DISTINCT code FROM portfolio_snapshots WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)) a
                JOIN portfolio_snapshots b ON a.code = b.code
                AND b.date = (
                    SELECT date FROM portfolio_snapshots
                    ORDER BY date DESC LIMIT 1 OFFSET 29
                )
            """)
            result = {row[0]: row[1] for row in cur.fetchall()}
            conn.close()
            return result
        except Exception:
            return {}

    def _build_index_comparison(self, index_today, summary, clr, sign):
        if not index_today:
            return ""
        dr = summary.get('daily_return', 0) or 0
        rows_html = ""
        for idx in index_today:
            chg = idx.get('change_pct', 0) or 0
            c = clr(chg)
            s = sign(chg)
            name = idx.get('name', idx.get('code', ''))
            close = idx.get('close', 0)
            close_str = f"{close:,.2f}" if close > 100 else f"{close:.4f}"
            bg = '#162447' if len(rows_html) == 0 else '#1a2d50'
            # 标记组合表现
            compare = ""
            if idx.get('code') == 'sh000300' and dr != 0:
                diff = dr - chg
                if abs(diff) > 0.1:
                    tag = "跑赢" if diff > 0 else "跑输"
                    tc = '#27ae60' if diff > 0 else '#e74c3c'
                    compare = '<span style="font-size:11px;color:' + tc + ';font-weight:600;margin-left:6px;">' + tag + f"{abs(diff):.2f}%</span>"
            rows_html += (
                '<tr style="background:' + bg + ';">'
                '<td style="padding:6px 10px;font-size:12px;font-weight:500;">' + name + '</td>'
                '<td style="padding:6px 10px;font-size:12px;text-align:right;">' + close_str + '</td>'
                '<td style="padding:6px 10px;font-size:12px;text-align:right;color:' + c + ';font-weight:500;">' + s + f"{chg:.2f}" + '%</td>'
                '<td style="padding:6px 10px;font-size:12px;">' + compare + '</td>'
                '</tr>'
            )
        return (
            '<div class="sec"><div class="st">📊 基准指数对比</div>'
            '<table><thead><tr><th>指数</th><th>收盘价</th><th>涨跌幅</th><th>vs组合</th></tr></thead>'
            '<tbody>' + rows_html + '</tbody></table></div>'
        )

    def _build_technical_signals(self, technical, price_30d=None):
        if not technical:
            return ""
        rows_html = ""
        for i, t in enumerate(technical):
            bg = '#162447' if i % 2 == 0 else '#1a2d50'
            name = t.get('name') or t.get('code', '未知')
            # 30日涨跌幅
            chg30_cell = '<span style="color:#7f8c8d;">--</span>'
            if price_30d and isinstance(price_30d, dict):
                code = t.get('code', '')
                old_price = price_30d.get(code)
                cur_price = t.get('current_price') or 0
                if old_price and old_price > 0 and cur_price > 0:
                    pct = (cur_price - old_price) / old_price * 100
                    chg30_c = '#27ae60' if pct >= 0 else '#e74c3c'
                    chg30_cell = '<span style="color:' + chg30_c + ';">' + f"{pct:+.1f}" + '%</span>'
            # MA信号颜色
            ma = t.get('ma_signal', '--')
            ma_c = '#27ae60' if '多头' in str(ma) else '#e74c3c' if '空头' in str(ma) else '#7f8c8d'
            # RSI颜色
            rsi = t.get('rsi_value', 0) or 0
            rsi_s = t.get('rsi_status', '--')
            rsi_c = '#e74c3c' if rsi_s == '严重超买' else '#f39c12' if rsi_s == '超买' else '#27ae60' if rsi_s == '超卖' else '#7f8c8d'
            # MACD信号
            macd = t.get('macd_signal', '--')
            macd_c = '#27ae60' if '买入' in str(macd) else '#e74c3c' if '卖出' in str(macd) else '#7f8c8d'
            # KDJ信号
            kdj = t.get('kdj_signal', '--')
            kdj_c = '#27ae60' if '金叉' in str(kdj) else '#e74c3c' if '死叉' in str(kdj) else '#7f8c8d'
            # 趋势
            trend = t.get('trend', '--')
            trend_c = '#27ae60' if '上涨' in str(trend) else '#e74c3c' if '下跌' in str(trend) else '#f39c12'
            # 布林带位置
            bp = t.get('bollinger_position', 0)
            bp_bar = ""
            if bp:
                bp = float(bp)
                bar_w = max(5, min(80, bp * 0.8))
                bar_c = '#e74c3c' if bp > 80 else '#f39c12' if bp > 60 else '#27ae60'
                bp_bar = '<div style="background:#1a2d50;border-radius:3px;height:6px;width:80px;display:inline-block;vertical-align:middle;"><div style="background:' + bar_c + ';border-radius:3px;height:6px;width:' + f"{bar_w:.0f}" + 'px;"></div></div> <span style="font-size:10px;color:#7f8c8d;">' + f"{bp:.0f}" + '%</span>'
            rows_html += (
                '<tr style="background:' + bg + ';">'
                '<td style="padding:5px 8px;font-size:11px;font-weight:500;">' + name + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + ma_c + ';">' + str(ma) + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + macd_c + ';">' + str(macd) + '</td>'
                '<td style="padding:5px 8px;font-size:11px;"><span style="color:' + rsi_c + ';">' + f"{rsi:.1f}" + '</span> <span style="font-size:10px;color:#7f8c8d;">' + str(rsi_s) + '</span></td>' + chg30_cell +
                '<td style="padding:5px 8px;font-size:11px;">' + bp_bar + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + kdj_c + ';">' + str(kdj) + '</td>'
                '<td style="padding:5px 8px;font-size:11px;color:' + trend_c + ';">' + str(trend) + '</td>'
                '</tr>'
            )
        return (
            '<div class="sec"><div class="st">🔍 技术信号汇总 (' + str(len(technical)) + '只)</div>'
            '<table><thead><tr><th>名称</th><th>均线</th><th>MACD</th><th>RSI</th><th>30日涨跌</th><th>布林位置</th><th>KDJ</th><th>趋势</th></tr></thead>'
            '<tbody>' + rows_html + '</tbody></table></div>'
        )

    def _build_news_section(self, news_data):
        if not news_data:
            return ""
        news = news_data.get('news', {})
        impacts = news_data.get('impacts', [])
        rotation = news_data.get('rotation', {})
        if not news and not impacts and not rotation:
            return ""
        blocks = ""
        # 资讯列表
        if news:
            for topic_key, topic_val in news.items():
                label = topic_val.get('label', topic_key)
                items = topic_val.get('news', [])
                if not items:
                    continue
                items_html = ""
                for n in items[:3]:
                    title = n.get('title', '')
                    source = n.get('source', '')
                    url = n.get('url', '')
                    title_html = ('<a href="' + url + '" target="_blank" style="color:#e0e6ed;text-decoration:none;">' + title + '</a>') if url else title
                    items_html += '<div style="padding:4px 0;font-size:12px;border-bottom:1px solid #2a3f5f;">' + title_html + ' <span style="font-size:10px;color:#95a5a6;">' + source + '</span></div>'
                blocks += (
                    '<div style="margin:8px 0;padding:10px;background:#162447;border-radius:6px;">'
                    '<div style="font-size:12px;font-weight:600;color:#b0c4de;margin-bottom:6px;">' + label + '</div>'
                    + items_html + '</div>'
                )
        # 新闻影响评估
        if impacts:
            imp_items = ""
            for imp in impacts[:5]:
                title = imp.get('title', '')
                sentiment = imp.get('sentiment', 'neutral')
                affected = imp.get('affected_positions', [])
                if sentiment == 'positive':
                    s_icon = '<span style="color:#27ae60;font-weight:600;">[利好]</span>'
                elif sentiment == 'negative':
                    s_icon = '<span style="color:#e74c3c;font-weight:600;">[利空]</span>'
                else:
                    s_icon = '<span style="color:#7f8c8d;">[中性]</span>'
                aff_str = ""
                if affected:
                    aff_str = ' <span style="font-size:10px;color:#3498db;">影响: ' + '、'.join(affected[:3]) + '</span>'
                imp_items += '<div style="padding:4px 0;font-size:12px;border-bottom:1px solid #2a3f5f;">' + s_icon + ' ' + title + aff_str + '</div>'
            if imp_items:
                blocks += (
                    '<div style="margin:8px 0;padding:10px;background:#2d2618;border-radius:6px;border-left:3px solid #f39c12;">'
                    '<div style="font-size:12px;font-weight:600;color:#f59e0b;margin-bottom:6px;">📰 新闻影响评估</div>'
                    + imp_items + '</div>'
                )
        # 行业轮动
        if rotation:
            leaders = rotation.get('leaders', [])
            laggards = rotation.get('laggards', [])
            trend = rotation.get('trend', '')
            if leaders or laggards or trend:
                rot_html = '<div style="font-size:12px;color:#95a5a6;margin-bottom:6px;">' + trend + '</div>'
                if leaders:
                    rot_html += '<div style="font-size:11px;color:#7f8c8d;margin-bottom:4px;">领涨:</div>'
                    for l in leaders[:3]:
                        rot_html += '<span style="display:inline-block;margin-right:12px;font-size:12px;color:#27ae60;">' + l.get('name', '') + ' ' + f"{l.get('change_pct', 0):+.2f}" + '%</span>'
                if laggards:
                    rot_html += '<div style="font-size:11px;color:#7f8c8d;margin:4px 0;">领跌:</div>'
                    for l in laggards[:3]:
                        rot_html += '<span style="display:inline-block;margin-right:12px;font-size:12px;color:#e74c3c;">' + l.get('name', '') + ' ' + f"{l.get('change_pct', 0):+.2f}" + '%</span>'
                blocks += (
                    '<div style="margin:8px 0;padding:10px;background:#162d1f;border-radius:6px;">'
                    '<div style="font-size:12px;font-weight:600;color:#27ae60;margin-bottom:6px;">🔄 行业轮动</div>'
                    + rot_html + '</div>'
                )
        if not blocks:
            return ""
        return '<div class="sec"><div class="st">📰 行业资讯与影响分析</div>' + blocks + '</div>'
