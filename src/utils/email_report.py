#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML邮件报告生成器 - 生成专业的投资组合日报HTML内容
"""
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class EmailReportBuilder:
    """HTML邮件报告构建器"""

    # 行业ETF配色
    SECTOR_COLORS = {
        '医药': '#e74c3c',
        '证券': '#3498db',
        '创新药': '#e67e22',
        '航天': '#9b59b6',
        '红利': '#27ae60',
        '债': '#1abc9c',
        '电池': '#f1c40f',
        '1000': '#e91e63',
        '可转债': '#00bcd4',
        '机器人': '#ff5722',
        '新能源': '#4caf50',
        '300': '#2196f3',
        '消费': '#ff9800',
        '科创': '#673ab7',
        '人工智能': '#f44336',
        '军工': '#795548',
        '创业板': '#607d8b',
    }

    def __init__(self, db_path: str):
        self.db_path = db_path

    def build_daily_report(self) -> str:
        """构建完整日报HTML"""
        summary = self._load_summary()
        positions = self._load_positions()
        alerts = self._load_alerts()
        advice = self._load_advice()

        if not summary or not positions:
            logger.warning("数据不足，无法生成报告")
            return "<p>暂无足够数据生成报告</p>"

        now = datetime.now()
        date_str = now.strftime('%Y年%m月%d日')
        weekday_map = {0: '周一', 1: '周二', 2: '周三', 3: '周四', 4: '周五', 5: '周六', 6: '周日'}
        weekday = weekday_map.get(now.weekday(), '')

        # 日收益率颜色
        dr = summary.get('daily_return', 0)
        dr_color = '#27ae60' if dr >= 0 else '#e74c3c'
        dr_sign = '+' if dr >= 0 else ''

        # 总盈亏颜色
        tp = summary.get('total_pnl', 0)
        tp_color = '#27ae60' if tp >= 0 else '#e74c3c'
        tp_sign = '+' if tp >= 0 else ''

        # 风险指标
        sharpe = summary.get('sharpe_ratio')
        max_dd = summary.get('max_drawdown')
        vol = summary.get('volatility')

        sharpe_color = '#27ae60' if sharpe and sharpe > 0.5 else '#f39c12' if sharpe else '#95a5a6'
        dd_color = '#e74c3c' if max_dd and abs(max_dd) > 10 else '#f39c12' if max_dd and abs(max_dd) > 5 else '#27ae60'
        vol_color = '#e74c3c' if vol and vol > 25 else '#f39c12' if vol and vol > 15 else '#27ae60'

        sharpe_str = f'{sharpe:.2f}' if sharpe and sharpe == sharpe else 'N/A'
        dd_str = f'{max_dd:.2f}' if max_dd and max_dd == max_dd else 'N/A'
        vol_str = f'{vol:.2f}' if vol and vol == vol else 'N/A'

        # 持仓表格行
        position_rows = ''
        for i, p in enumerate(positions):
            pnl = p.get('pnl', 0)
            pnl_rate = p.get('pnl_rate', 0)
            mv = p.get('market_value', 0)
            total_v = summary.get('total_value', 1)
            weight = mv / total_v * 100 if total_v > 0 else 0

            pnl_c = '#27ae60' if pnl >= 0 else '#e74c3c'
            pnl_sign = '+' if pnl >= 0 else ''
            bg = '#f8f9fa' if i % 2 == 0 else '#ffffff'

            position_rows += (
                '<tr style="background:{bg};">'
                '<td style="padding:8px 12px;font-size:13px;font-weight:500;">{name}</td>'
                '<td style="padding:8px 12px;font-size:13px;color:#7f8c8d;">{code}</td>'
                '<td style="padding:8px 12px;font-size:13px;text-align:right;">{qty:,.0f}</td>'
                '<td style="padding:8px 12px;font-size:13px;text-align:right;">{cost:.3f}</td>'
                '<td style="padding:8px 12px;font-size:13px;text-align:right;font-weight:500;">{price:.3f}</td>'
                '<td style="padding:8px 12px;font-size:13px;text-align:right;">¥{mv:,.0f}</td>'
                '<td style="padding:8px 12px;font-size:13px;text-align:right;color:{pnl_c};font-weight:500;">{pnl_sign}¥{pnl:,.0f}</td>'
                '<td style="padding:8px 12px;font-size:13px;text-align:right;color:{pnl_c};">{pnl_sign}{rate:.2f}%</td>'
                '<td style="padding:8px 12px;font-size:13px;text-align:right;color:#7f8c8d;">{weight:.1f}%</td>'
                '</tr>'
            ).format(
                bg=bg, name=p['name'], code=p['code'],
                qty=p['quantity'], cost=p['cost_price'], price=p['current_price'],
                mv=mv, pnl=pnl, rate=pnl_rate, weight=weight,
                pnl_c=pnl_c, pnl_sign=pnl_sign
            )

        # 告警区块
        alert_block = ''
        if alerts:
            alert_items = ''
            for a in alerts:
                level_color = '#e74c3c' if a['level'] == 'error' else '#f39c12'
                level_icon = '🚨' if a['level'] == 'error' else '⚠️'
                alert_items += (
                    '<tr><td style="padding:8px 12px;font-size:13px;">{icon} {msg}</td></tr>'
                ).format(icon=level_icon, msg=a['message'])
            alert_block = '''
            <div style="margin:16px 0;padding:16px;background:#fff5f5;border-radius:8px;border-left:4px solid #e74c3c;">
                <h3 style="margin:0 0 8px 0;font-size:15px;color:#e74c3c;">⚠️ 今日告警 ({count})</h3>
                <table style="width:100%;border-collapse:collapse;">{items}</table>
            </div>
            '''.format(count=len(alerts), items=alert_items)
        else:
            alert_block = '''
            <div style="margin:16px 0;padding:16px;background:#f0fff4;border-radius:8px;border-left:4px solid #27ae60;">
                <span style="font-size:13px;color:#27ae60;">✅ 今日无告警，投资组合运行正常</span>
            </div>
            '''

        # 智能建议区块
        advice_block = ''
        if advice:
            advice_items = ''
            priority_map = {'high': ('🔴 高', '#e74c3c'), 'medium': ('🟡 中', '#f39c12'), 'low': ('🟢 低', '#27ae60')}
            for a in advice[:5]:
                p_label, p_color = priority_map.get(a.get('priority', 'low'), ('⚪ 低', '#95a5a6'))
                advice_items += (
                    '<div style="padding:8px 0;border-bottom:1px solid #ecf0f1;">'
                    '<span style="font-size:13px;font-weight:600;color:{color};">{label}</span> '
                    '<span style="font-size:13px;">{title}</span></div>'
                ).format(color=p_color, label=p_label, title=a.get('title', ''))
            advice_block = '''
            <div style="margin:16px 0;padding:16px;background:#f8f9fa;border-radius:8px;">
                <h3 style="margin:0 0 8px 0;font-size:15px;color:#2c3e50;">💡 智能建议 ({count})</h3>
                {items}
            </div>
            '''.format(count=len(advice), items=advice_items)

        html = '''<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
    body {{ margin:0;padding:0;background:#ecf0f1;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif; }}
    .container {{ max-width:720px;margin:20px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08); }}
    .header {{ background:linear-gradient(135deg,#1a73e8,#0d47a1);color:#fff;padding:28px 24px;text-align:center; }}
    .header h1 {{ margin:0;font-size:22px;letter-spacing:1px; }}
    .header p {{ margin:6px 0 0 0;font-size:13px;opacity:0.85; }}
    .metrics {{ display:flex;flex-wrap:wrap;gap:12px;padding:20px 24px;background:#fff; }}
    .metric {{ flex:1 1 30%;min-width:140px;padding:14px 16px;background:#f8f9fa;border-radius:8px;text-align:center; }}
    .metric .label {{ font-size:11px;color:#7f8c8d;margin-bottom:4px;text-transform:uppercase;letter-spacing:0.5px; }}
    .metric .value {{ font-size:20px;font-weight:700; }}
    .metric .sub {{ font-size:11px;color:#95a5a6;margin-top:2px; }}
    .section {{ padding:0 24px 16px; }}
    .section-title {{ font-size:15px;font-weight:600;color:#2c3e50;margin:16px 0 10px;padding-bottom:6px;border-bottom:2px solid #ecf0f1; }}
    table {{ width:100%;border-collapse:collapse; }}
    th {{ padding:8px 12px;font-size:11px;color:#7f8c8d;text-transform:uppercase;letter-spacing:0.5px;text-align:left;background:#f8f9fa;border-bottom:2px solid #ecf0f1; }}
    th:last-child, td:last-child {{ text-align:right; }}
    th:nth-child(n+3), td:nth-child(n+3) {{ text-align:right; }}
    .footer {{ padding:16px 24px;text-align:center;font-size:11px;color:#95a5a6;border-top:1px solid #ecf0f1;background:#f8f9fa; }}
    .risk-grid {{ display:flex;gap:12px; }}
    .risk-card {{ flex:1;padding:14px;border-radius:8px;text-align:center; }}
    .risk-card .r-label {{ font-size:11px;color:#7f8c8d; }}
    .risk-card .r-value {{ font-size:18px;font-weight:700;margin-top:4px; }}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 投资组合日报</h1>
        <p>{date} {weekday}</p>
    </div>

    <div class="metrics">
        <div class="metric">
            <div class="label">总市值</div>
            <div class="value" style="color:#1a73e8;">¥{total_value:,.0f}</div>
        </div>
        <div class="metric">
            <div class="label">当日盈亏</div>
            <div class="value" style="color:{dr_color};">{dr_sign}¥{daily_pnl:,.0f}</div>
            <div class="sub">{dr_sign}{daily_return:.2f}%</div>
        </div>
        <div class="metric">
            <div class="label">累计盈亏</div>
            <div class="value" style="color:{tp_color};">{tp_sign}¥{total_pnl:,.0f}</div>
            <div class="sub">{tp_sign}{total_return:.2f}%</div>
        </div>
    </div>

    <div class="section">
        <div class="section-title">⚠️ 风险指标</div>
        <div class="risk-grid">
            <div class="risk-card" style="background:#eaf7ee;">
                <div class="r-label">夏普比率</div>
                <div class="r-value" style="color:{sharpe_color};">{sharpe_str}</div>
            </div>
            <div class="risk-card" style="background:#fef5f5;">
                <div class="r-label">最大回撤</div>
                <div class="r-value" style="color:{dd_color};">{dd_str}%</div>
            </div>
            <div class="risk-card" style="background:{vol_bg};">
                <div class="r-label">年化波动率</div>
                <div class="r-value" style="color:{vol_color};">{vol_str}%</div>
            </div>
        </div>
    </div>

    {alert_block}

    <div class="section">
        <div class="section-title">📋 持仓明细 ({pos_count}只，盈{profit_count}亏{loss_count})</div>
        <table>
            <thead><tr>
                <th>名称</th><th>代码</th><th>持仓量</th><th>成本</th><th>现价</th><th>市值</th><th>盈亏</th><th>收益率</th><th>占比</th>
            </tr></thead>
            <tbody>{position_rows}</tbody>
        </table>
    </div>

    {advice_block}

    <div class="footer">
        投资组合跟踪分析系统 v1.3 自动生成<br>
        本报告仅供参考，不构成投资建议 | 生成时间: {now}
    </div>
</div>
</body>
</html>'''.format(
            date=date_str, weekday=weekday,
            total_value=summary['total_value'],
            daily_pnl=summary.get('daily_pnl', 0),
            daily_return=dr, dr_color=dr_color, dr_sign=dr_sign,
            total_pnl=tp, total_return=tp / summary.get('total_cost', 1) * 100 if summary.get('total_cost') else 0,
            tp_color=tp_color, tp_sign=tp_sign,
            sharpe_color=sharpe_color, sharpe_str=sharpe_str,
            dd_color=dd_color, dd_str=dd_str,
            vol_color=vol_color, vol_str=vol_str,
            vol_bg='#fef9f0' if vol and vol > 15 else '#eaf7ee',
            alert_block=alert_block,
            pos_count=len(positions),
            profit_count=summary.get('profit_count', 0),
            loss_count=summary.get('loss_count', 0),
            position_rows=position_rows,
            advice_block=advice_block,
            now=now.strftime('%Y-%m-%d %H:%M:%S')
        )

        return html

    def build_alert_email(self, alerts: List[Dict]) -> str:
        """构建告警邮件HTML"""
        now = datetime.now()
        alert_rows = ''
        for a in alerts:
            level_icon = '🚨' if a['level'] == 'error' else '⚠️'
            level_color = '#e74c3c' if a['level'] == 'error' else '#f39c12'
            alert_rows += (
                '<tr><td style="padding:10px 12px;font-size:13px;">'
                '{icon} <strong style="color:{color};">[{level}]</strong> {msg}'
                '</td></tr>'
            ).format(
                icon=level_icon, color=level_color,
                level=a['level'].upper(), msg=a['message']
            )

        return '''<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
    body {{ margin:0;padding:20px;background:#ecf0f1;font-family:-apple-system,Arial,sans-serif; }}
    .box {{ max-width:600px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08); }}
    .header {{ background:#e74c3c;color:#fff;padding:20px 24px;text-align:center; }}
    .header h1 {{ margin:0;font-size:20px; }}
    .body {{ padding:20px 24px; }}
    table {{ width:100%;border-collapse:collapse; }}
</style></head>
<body>
<div class="box">
    <div class="header"><h1>🚨 投资组合告警通知</h1><p style="margin:4px 0 0;font-size:13px;opacity:0.85;">{now}</p></div>
    <div class="body"><table>{rows}</table></div>
</div>
</body></html>'''.format(now=now.strftime('%Y-%m-%d %H:%M:%S'), rows=alert_rows)

    def save_report(self, html: str, filename: str = None) -> str:
        """保存报告为HTML文件"""
        if not filename:
            filename = datetime.now().strftime('report_%Y%m%d_%H%M%S.html')

        report_dir = Path(self.db_path).parent.parent.parent / 'data' / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)

        filepath = report_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)

        logger.info(f"HTML报告已保存: {filepath}")
        return str(filepath)

    def _load_summary(self) -> Optional[Dict]:
        """加载最新汇总"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM portfolio_summary ORDER BY date DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def _load_positions(self) -> List[Dict]:
        """加载最新持仓"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM portfolio_snapshots 
            WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
            ORDER BY market_value DESC
        """)
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_alerts(self) -> List[Dict]:
        """加载最近告警"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT rule_name, level, message FROM alerts ORDER BY id DESC LIMIT 5")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _load_advice(self) -> List[Dict]:
        """加载智能建议（从最新报告解析）"""
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
        # 解析建议: ### 1. [高] 标题
        pattern = r'### \d+\.\s+\[(高|中|低)\]\s+(.+?)(?:\n|$)'
        for m in re.finditer(pattern, content):
            priority, title = m.group(1), m.group(2).strip()
            advices.append({'priority': priority, 'title': title})

        return advices