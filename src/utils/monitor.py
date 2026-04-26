"""
监控和告警模块 - 运行状态监控和异常检测
"""
import os
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class AlertRule:
    """告警规则"""
    name: str
    condition: str  # 条件表达式
    threshold: float
    level: str  # warning, error, critical
    message_template: str
    enabled: bool = True


@dataclass
class Alert:
    """告警记录"""
    id: int = 0
    rule_name: str = ""
    level: str = ""
    message: str = ""
    created_at: str = ""
    acknowledged: bool = False


class Monitor:
    """监控器"""

    # 默认告警规则
    DEFAULT_RULES = [
        AlertRule(
            name="daily_loss_limit",
            condition="daily_return",
            threshold=-3.0,
            level="warning",
            message_template="单日跌幅超过{threshold}%: 当前{daily_return}%"
        ),
        AlertRule(
            name="drawdown_limit",
            condition="max_drawdown",
            threshold=-10.0,
            level="error",
            message_template="最大回撤超过{threshold}%: 当前{max_drawdown}%"
        ),
        AlertRule(
            name="concentration_risk",
            condition="concentration_hhi",
            threshold=0.5,
            level="warning",
            message_template="持仓集中度偏高(HHI>{threshold}): 当前{concentration_hhi}"
        ),
        AlertRule(
            name="volatility_spike",
            condition="volatility",
            threshold=30.0,
            level="warning",
            message_template="波动率异常升高(>{threshold}%): 当前{volatility}%"
        ),
        AlertRule(
            name="sharpe_low",
            condition="sharpe_ratio",
            threshold=0.5,
            level="warning",
            message_template="夏普比率偏低(<{threshold}): 当前{sharpe_ratio}"
        ),
    ]

    def __init__(self, db_path: str, config: dict = None):
        self.db_path = db_path
        self.config = config or {}
        self.rules: List[AlertRule] = []
        self._init_rules()
        self._init_tables()

    def _init_rules(self):
        """初始化告警规则"""
        custom_rules = self.config.get('alert_rules', [])

        # 加载默认规则
        for rule in self.DEFAULT_RULES:
            # 检查是否有自定义配置
            custom = next((r for r in custom_rules if r.get('name') == rule.name), None)
            if custom:
                rule.threshold = custom.get('threshold', rule.threshold)
                rule.enabled = custom.get('enabled', rule.enabled)
            self.rules.append(rule)

        # 添加用户自定义规则
        for custom in custom_rules:
            if not any(r.name == custom.get('name') for r in self.rules):
                self.rules.append(AlertRule(**custom))

    def _init_tables(self):
        """初始化监控表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 告警记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name TEXT,
                level TEXT,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                acknowledged BOOLEAN DEFAULT 0
            )
        ''')

        # 运行日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS execution_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                status TEXT,
                message TEXT,
                duration_seconds REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()

    def check_alerts(self, portfolio_data: dict, risk_data: dict) -> List[Alert]:
        """检查告警条件"""
        alerts = []

        # 合并数据
        data = {**portfolio_data, **risk_data}

        for rule in self.rules:
            if not rule.enabled:
                continue

            value = data.get(rule.condition)
            if value is None:
                continue

            triggered = False
            if rule.condition in ['daily_return', 'max_drawdown', 'sharpe_ratio']:
                triggered = value < rule.threshold
            else:
                triggered = value > rule.threshold

            if triggered:
                message = rule.message_template.format(
                    threshold=rule.threshold,
                    **data
                )
                alert = Alert(
                    rule_name=rule.name,
                    level=rule.level,
                    message=message
                )
                alerts.append(alert)
                self._save_alert(alert)

        return alerts

    def _save_alert(self, alert: Alert):
        """保存告警到数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO alerts (rule_name, level, message, created_at)
            VALUES (?, ?, ?, ?)
        ''', (alert.rule_name, alert.level, alert.message, datetime.now()))

        conn.commit()
        conn.close()

    def log_execution(self, task_name: str, status: str, message: str = "", duration: float = 0):
        """记录任务执行日志"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO execution_logs (task_name, status, message, duration_seconds, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (task_name, status, message, duration, datetime.now()))

        conn.commit()
        conn.close()

        logger.info(f"任务执行记录: {task_name} - {status}")

    def get_recent_alerts(self, hours: int = 24) -> List[Alert]:
        """获取最近的告警"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        since = datetime.now() - timedelta(hours=hours)

        cursor.execute('''
            SELECT id, rule_name, level, message, created_at, acknowledged
            FROM alerts
            WHERE created_at > ?
            ORDER BY created_at DESC
        ''', (since,))

        rows = cursor.fetchall()
        conn.close()

        return [Alert(*row) for row in rows]

    def get_execution_stats(self, days: int = 7) -> Dict:
        """获取执行统计"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        since = datetime.now() - timedelta(days=days)

        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                AVG(duration_seconds) as avg_duration
            FROM execution_logs
            WHERE created_at > ?
        ''', (since,))

        row = cursor.fetchone()
        conn.close()

        total, success, failed, avg_duration = row

        return {
            'total': total or 0,
            'success': success or 0,
            'failed': failed or 0,
            'success_rate': (success / total * 100) if total else 0,
            'avg_duration': round(avg_duration or 0, 2)
        }

    def acknowledge_alert(self, alert_id: int):
        """确认告警"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE alerts SET acknowledged = 1 WHERE id = ?
        ''', (alert_id,))

        conn.commit()
        conn.close()

    def get_health_status(self) -> Dict:
        """获取系统健康状态"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 检查最近执行状态
        cursor.execute('''
            SELECT status, created_at
            FROM execution_logs
            ORDER BY created_at DESC
            LIMIT 1
        ''')
        last_execution = cursor.fetchone()

        # 检查未确认告警数
        cursor.execute('''
            SELECT COUNT(*) FROM alerts WHERE acknowledged = 0
        ''')
        unacknowledged = cursor.fetchone()[0]

        # 检查今日告警数
        today = datetime.now().replace(hour=0, minute=0, second=0)
        cursor.execute('''
            SELECT COUNT(*) FROM alerts WHERE created_at > ?
        ''', (today,))
        today_alerts = cursor.fetchone()[0]

        conn.close()

        status = "healthy"
        if unacknowledged > 5:
            status = "critical"
        elif unacknowledged > 0 or today_alerts > 3:
            status = "warning"

        return {
            'status': status,
            'last_execution': last_execution[1] if last_execution else None,
            'last_execution_status': last_execution[0] if last_execution else None,
            'unacknowledged_alerts': unacknowledged,
            'today_alerts': today_alerts
        }
