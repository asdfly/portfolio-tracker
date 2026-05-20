"""
数据质量监控模块 - Phase D

提供全库数据完整性检查、新鲜度验证、质量评分等功能。
可被 run_analysis.py 调用，也可被 Dashboard Tab 展示。
"""

import sqlite3
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """数据质量检查器"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def check_table_freshness(self) -> List[Dict]:
        """
        检查每张表的数据新鲜度（最新日期 vs 今天）。
        返回 [{"table": str, "latest_date": str, "days_lag": int, "status": str}, ...]
        """
        today = date.today()
        conn = self._conn()
        cur = conn.cursor()

        checks = [
            ("portfolio_snapshots", "date", "交易日快照"),
            ("etf_technical", "date", "技术指标"),
            ("fund_flows", "date", "资金流"),
            ("index_quotes", "date", "指数行情"),
            ("daily_news", "date", "新闻资讯"),
            ("macro_daily", "date", "宏观数据"),
            ("market_sentiment", "date", "市场情绪"),
            ("portfolio_summary", "date", "组合摘要"),
        ]

        results = []
        for table, date_col, label in checks:
            try:
                cur.execute(f"SELECT MAX({date_col}) FROM {table}")
                row = cur.fetchone()
                if row and row[0]:
                    latest = datetime.strptime(str(row[0]), "%Y-%m-%d").date()
                    lag = (today - latest).days
                    # 周末自然会有1-2天延迟
                    if lag <= 1:
                        status = "OK"
                    elif lag <= 3:
                        status = "WARN"
                    else:
                        status = "STALE"
                else:
                    latest = None
                    lag = 999
                    status = "EMPTY"
            except Exception as e:
                latest = None
                lag = 999
                status = "ERROR"

            results.append({
                "table": table,
                "label": label,
                "latest_date": str(latest) if latest else "N/A",
                "days_lag": lag,
                "status": status,
            })

        conn.close()
        return results

    def check_data_coverage(self) -> Dict[str, Dict]:
        """
        检查各核心表的数据覆盖情况。
        返回 {"table": {"total_rows": int, "distinct_codes": int, "date_range": str}, ...}
        """
        conn = self._conn()
        cur = conn.cursor()

        tables_info = {
            "portfolio_snapshots": {"code_col": "code", "date_col": "date"},
            "etf_technical": {"code_col": "code", "date_col": "date"},
            "fund_flows": {"code_col": "code", "date_col": "date"},
            "index_quotes": {"code_col": "code", "date_col": "date"},
            "macro_daily": {"code_col": None, "date_col": "date"},
            "market_sentiment": {"code_col": None, "date_col": "date"},
            "daily_news": {"code_col": None, "date_col": "date"},
        }

        results = {}
        for table, info in tables_info.items():
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                total = cur.fetchone()[0]

                cur.execute(f"SELECT MIN({info['date_col']}), MAX({info['date_col']}) FROM {table}")
                row = cur.fetchone()
                date_range = f"{row[0]} ~ {row[1]}" if row[0] else "N/A"

                codes = 0
                if info["code_col"]:
                    cur.execute(f"SELECT COUNT(DISTINCT {info['code_col']}) FROM {table}")
                    codes = cur.fetchone()[0]

                results[table] = {
                    "total_rows": total,
                    "distinct_codes": codes,
                    "date_range": date_range,
                }
            except Exception:
                results[table] = {"total_rows": 0, "distinct_codes": 0, "date_range": "ERROR"}

        conn.close()
        return results

    def check_indicator_backtest(self) -> Dict:
        """检查自定义指标回测覆盖情况"""
        conn = self._conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM custom_indicators WHERE is_template = 1")
        template_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM indicator_backtest_results")
        result_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT indicator_id) FROM indicator_backtest_results
        """)
        covered_indicators = cur.fetchone()[0]

        # 检查每个指标的回测覆盖ETF数
        cur.execute("""
            SELECT ci.name, COUNT(DISTINCT ibr.test_period) as periods
            FROM indicator_backtest_results ibr
            JOIN custom_indicators ci ON ci.id = ibr.indicator_id
            GROUP BY ci.name
        """)
        coverage = {row[0]: row[1] for row in cur.fetchall()}

        conn.close()
        return {
            "template_count": template_count,
            "result_count": result_count,
            "covered_indicators": covered_indicators,
            "periods_per_indicator": coverage,
        }

    def compute_quality_score(self) -> Dict:
        """
        计算综合数据质量评分（0-100）。
        基于：新鲜度、覆盖率、回测完整度三个维度。
        """
        freshness = self.check_table_freshness()
        coverage = self.check_data_coverage()
        backtest = self.check_indicator_backtest()

        # 1. 新鲜度评分 (0-40分)
        freshness_score = 0
        weight_per_table = 40 / len(freshness)
        for f in freshness:
            if f["status"] == "OK":
                freshness_score += weight_per_table
            elif f["status"] == "WARN":
                freshness_score += weight_per_table * 0.5
            elif f["status"] == "STALE":
                freshness_score += weight_per_table * 0.2
            # EMPTY/ERROR: 0

        # 2. 覆盖度评分 (0-30分)
        coverage_score = 0
        expected_tables = ["portfolio_snapshots", "etf_technical", "fund_flows",
                          "index_quotes", "macro_daily", "market_sentiment"]
        per_table = 30 / len(expected_tables)
        for t in expected_tables:
            info = coverage.get(t, {})
            rows = info.get("total_rows", 0)
            if t in ["macro_daily", "market_sentiment"]:
                threshold = 1000  # 宏观数据至少1000条
            else:
                threshold = 1000  # 其他表也至少1000条
            if rows >= threshold:
                coverage_score += per_table
            elif rows >= 100:
                coverage_score += per_table * 0.5
            elif rows > 0:
                coverage_score += per_table * 0.2

        # 3. 回测完整度 (0-30分)
        backtest_score = 0
        if backtest["template_count"] > 0:
            ratio = backtest["covered_indicators"] / backtest["template_count"]
            backtest_score += 15 * ratio
            # 期望每个指标至少有23个ETF x 2个周期
            expected_results = backtest["template_count"] * 23 * 2
            if expected_results > 0:
                result_ratio = min(backtest["result_count"] / expected_results, 1.0)
                backtest_score += 15 * result_ratio

        total = round(freshness_score + coverage_score + backtest_score, 1)

        # 等级评定
        if total >= 90:
            grade = "A"
        elif total >= 80:
            grade = "B+"
        elif total >= 70:
            grade = "B"
        elif total >= 60:
            grade = "C"
        else:
            grade = "D"

        return {
            "total_score": total,
            "grade": grade,
            "freshness_score": round(freshness_score, 1),
            "coverage_score": round(coverage_score, 1),
            "backtest_score": round(backtest_score, 1),
            "details": {
                "freshness": freshness,
                "coverage": coverage,
                "backtest": backtest,
            }
        }

    def run_full_check(self) -> Dict:
        """执行完整的数据质量检查，返回综合报告"""
        score = self.compute_quality_score()
        return score

    def get_freshness_summary(self) -> str:
        """生成新鲜度检查的简要文本报告"""
        results = self.check_table_freshness()
        lines = []
        for r in results:
            status_icon = {"OK": "V", "WARN": "!", "STALE": "X", "EMPTY": "-", "ERROR": "?"}.get(r["status"], "?")
            lines.append(f"  [{status_icon}] {r['label']:8s} | 最新: {r['latest_date']:12s} | 延迟: {r['days_lag']}天 | {r['status']}")
        return "\n".join(lines)
