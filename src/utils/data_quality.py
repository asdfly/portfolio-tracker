"""
数据质量监控模块 - Phase D

提供全库数据完整性检查、新鲜度验证、质量评分等功能。
可被 run_analysis.py 调用，也可被 Dashboard Tab 展示。
"""

import sqlite3
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List

logger = logging.getLogger(__name__)

from src.utils.db_schema import QUALITY_CHECK_TABLES
from data_loader import get_db_connection


class DataQualityChecker:
    """数据质量检查器"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        return get_db_connection(self.db_path)

    def check_table_freshness(self) -> List[Dict]:
        """
        检查每张表的数据新鲜度（最新日期 vs 今天）。
        返回 [{"table": str, "latest_date": str, "days_lag": int, "status": str}, ...]
        """
        today = date.today()
        conn = self._conn()
        cur = conn.cursor()

        results = []
        for table, info in QUALITY_CHECK_TABLES.items():
            date_col = info["date_col"]
            label = info["label"]
            try:
                if "source_table" in info:
                    # 虚拟表: 从 source_table 按 indicator_code 过滤
                    cur.execute(
                        f"SELECT MAX({date_col}) FROM {info['source_table']} WHERE indicator_code = ?",
                        (info["indicator_code"],)
                    )
                else:
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
            except (sqlite3.OperationalError, ValueError, TypeError):
                latest = None
                lag = 999
                status = "ERROR"

            # 用户手动管理的表(如交易流水)非自动采集源：展示最新日期但不告警/不计惩罚
            if info.get("user_managed") and status in ("STALE", "WARN"):
                status = "USER"

            results.append({
                "table": table,
                "label": label,
                "latest_date": str(latest) if latest else "N/A",
                "days_lag": lag,
                "status": status,
                "user_managed": bool(info.get("user_managed", False)),
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

        results = {}
        for table, info in QUALITY_CHECK_TABLES.items():
            try:
                if "source_table" in info:
                    src = info["source_table"]
                    cur.execute(f"SELECT COUNT(*) FROM {src} WHERE indicator_code = ?", (info["indicator_code"],))
                    total = cur.fetchone()[0]
                    cur.execute(f"SELECT MIN({info['date_col']}), MAX({info['date_col']}) FROM {src} WHERE indicator_code = ?", (info["indicator_code"],))
                    row = cur.fetchone()
                    date_range = f"{row[0]} ~ {row[1]}" if row[0] else "N/A"
                    codes = 0
                else:
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
            except sqlite3.OperationalError:
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
        # 用户管理的表(如交易流水)非自动采集源，不参与新鲜度评分
        auto_freshness = [f for f in freshness if not f.get("user_managed")]
        freshness_score = 0
        weight_per_table = 40 / len(auto_freshness) if auto_freshness else 0
        for f in auto_freshness:
            if f["status"] == "OK":
                freshness_score += weight_per_table
            elif f["status"] == "WARN":
                freshness_score += weight_per_table * 0.5
            elif f["status"] == "STALE":
                freshness_score += weight_per_table * 0.2
            # EMPTY/ERROR/USER: 0

        # 2. 覆盖度评分 (0-30分)
        coverage_score = 0
        expected_tables = ["portfolio_snapshots", "etf_technical", "fund_flows",
                          "index_quotes", "macro_daily", "market_sentiment",
                          "_gold_comex", "_gold_sge"]
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

    def check_null_rates(self) -> List[Dict]:
        """检查各表关键列的NULL比例，返回需要关注的项。"""
        conn = self._conn()
        cur = conn.cursor()
        # 每个表需要检查的关键列(排除id和created_at)
        key_cols_map = {
            "fund_flows": ["net_inflow"],
            "stock_lhb": ["code", "net_inflow"],
            "stock_margin": ["code", "margin_balance"],
            "stock_institution_research": ["code", "institution"],
            "stock_block_trade": ["code", "amount"],
            "daily_news": ["title", "category", "source"],
            "index_quotes": ["close", "change_pct"],
            "portfolio_snapshots": ["total_value", "daily_change_pct"],
        }
        results = []
        for table, cols in key_cols_map.items():
            if table not in QUALITY_CHECK_TABLES:
                continue
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                total = cur.fetchone()[0]
                if total == 0:
                    continue
                for col in cols:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE [{col}] IS NULL")
                    nulls = cur.fetchone()[0]
                    rate = nulls / total
                    if rate > 0.01:  # 超过1%才报告
                        results.append({
                            "table": table,
                            "column": col,
                            "null_count": nulls,
                            "total_count": total,
                            "null_rate": round(rate, 4),
                            "severity": "HIGH" if rate > 0.1 else "MEDIUM" if rate > 0.05 else "LOW",
                        })
            except (sqlite3.OperationalError, KeyError, ValueError):  # 空值检查失败，跳过该列
                pass
        conn.close()
        return results

    def check_gold_data(self) -> Dict:
        """黄金数据专项质量检查。

        检查 macro_daily 表中 COMEX_GOLD 和 SGE_GOLD 指标的:
        - 数据新鲜度
        - 价格连续性 (日涨跌幅异常)
        - COMEX vs SGE 价差合理性
        - NULL 值比例
        """
        conn = self._conn()
        cur = conn.cursor()
        result = {"indicators": [], "anomalies": []}

        for code, name in [("COMEX_GOLD", "COMEX黄金"), ("SGE_GOLD", "上海金基准")]:
            try:
                cur.execute("""
                    SELECT COUNT(*), MIN(date), MAX(date),
                           AVG(value), MIN(value), MAX(value),
                           SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END),
                           SUM(CASE WHEN change_pct IS NULL THEN 1 ELSE 0 END)
                    FROM macro_daily WHERE indicator_code = ?
                """, (code,))
                row = cur.fetchone()
                total, min_date, max_date, avg_val, min_val, max_val, null_val, null_chg = row

                # NULL 率
                null_val_rate = null_val / total if total > 0 else 0
                null_chg_rate = null_chg / total if total > 0 else 0

                # 日涨跌幅异常 (|change_pct| > 10%)
                cur.execute("""
                    SELECT date, value, change_pct FROM macro_daily
                    WHERE indicator_code = ? AND ABS(change_pct) > 10
                    ORDER BY date DESC LIMIT 10
                """, (code,))
                anomalies = cur.fetchall()

                # 近7天数据
                cur.execute("""
                    SELECT date, value, change_pct FROM macro_daily
                    WHERE indicator_code = ? ORDER BY date DESC LIMIT 7
                """, (code,))
                recent = cur.fetchall()

                result["indicators"].append({
                    "code": code,
                    "name": name,
                    "total_rows": total,
                    "date_range": f"{min_date} ~ {max_date}",
                    "avg_value": round(avg_val, 2) if avg_val else 0,
                    "value_range": f"{min_val} ~ {max_val}",
                    "null_value_rate": round(null_val_rate, 4),
                    "null_change_rate": round(null_chg_rate, 4),
                    "recent_data": [{"date": r[0], "value": r[1], "change_pct": r[2]} for r in recent],
                    "price_anomalies": [{"date": r[0], "value": r[1], "change_pct": r[2]} for r in anomalies],
                })

                # 异常告警
                latest_date = max_date
                if latest_date:
                    try:
                        from datetime import datetime as dt2
                        latest = dt2.strptime(str(latest_date), "%Y-%m-%d").date()
                        lag = (date.today() - latest).days
                        if lag > 5:
                            result["anomalies"].append({
                                "severity": "HIGH",
                                "indicator": code,
                                "message": f"{name} 数据过期: 最新 {latest_date}, 延迟 {lag} 天",
                            })
                    except ValueError:
                        pass

                if null_val_rate > 0.01:
                    result["anomalies"].append({
                        "severity": "MEDIUM",
                        "indicator": code,
                        "message": f"{name} value NULL率 {null_val_rate*100:.1f}%",
                    })

                if null_chg_rate > 0.3:
                    result["anomalies"].append({
                        "severity": "LOW",
                        "indicator": code,
                        "message": f"{name} change_pct NULL率 {null_chg_rate*100:.1f}% (早期数据可能无涨跌幅)",
                    })

            except sqlite3.OperationalError as e:
                result["indicators"].append({
                    "code": code, "name": name, "error": str(e),
                })

        # COMEX vs SGE 价差检查 (同日比值应在合理范围)
        try:
            cur.execute("""
                SELECT a.date, a.value as comex, b.value as sge,
                       CAST(a.value AS REAL) / CAST(b.value AS REAL) as ratio
                FROM macro_daily a
                JOIN macro_daily b ON a.date = b.date
                WHERE a.indicator_code = 'COMEX_GOLD' AND b.indicator_code = 'SGE_GOLD'
                  AND a.value IS NOT NULL AND b.value IS NOT NULL AND b.value > 0
                ORDER BY a.date DESC LIMIT 5
            """)
            ratio_rows = cur.fetchall()
            if ratio_rows:
                ratios = [r[3] for r in ratio_rows if r[3] and r[3] > 0]
                if ratios:
                    avg_ratio = sum(ratios) / len(ratios)
                    # COMEX 美元/盎司 vs SGE 元/克, 合理比值约 7-10 (取决于汇率和单位)
                    result["price_ratio"] = {
                        "comex_sge_avg_ratio": round(avg_ratio, 4),
                        "recent": [{"date": r[0], "comex": r[1], "sge": r[2], "ratio": round(r[3], 4)} for r in ratio_rows],
                    }
        except sqlite3.OperationalError:
            pass

        # COMEX-SGE 比值一致性检查 (同日比值应在合理范围)
        # COMEX: USD/oz, SGE: CNY/g, 理论比值 = USD_CNY / 31.1035
        # 汇率 6.5-7.5 时, 合理比值约 3.4-4.8
        try:
            cur.execute("""
                SELECT a.date, a.value as comex, b.value as sge,
                       CAST(a.value AS REAL) / CAST(b.value AS REAL) as ratio
                FROM macro_daily a
                JOIN macro_daily b ON a.date = b.date
                WHERE a.indicator_code = 'COMEX_GOLD' AND b.indicator_code = 'SGE_GOLD'
                  AND a.value IS NOT NULL AND b.value IS NOT NULL AND b.value > 0
                  AND (CAST(a.value AS REAL) / CAST(b.value AS REAL)) NOT BETWEEN 3.0 AND 5.0
                ORDER BY a.date DESC LIMIT 20
            """)
            ratio_anomalies = cur.fetchall()
            if ratio_anomalies:
                result["ratio_anomalies"] = [
                    {"date": r[0], "comex": r[1], "sge": r[2], "ratio": round(r[3], 4)}
                    for r in ratio_anomalies
                ]
                result["anomalies"].append({
                    "severity": "HIGH",
                    "indicator": "COMEX/SGE_RATIO",
                    "message": f"COMEX/SGE 比值异常: {len(ratio_anomalies)} 天比值超出 3.0-5.0 范围, "
                               f"最近: {ratio_anomalies[0][0]} ratio={ratio_anomalies[0][3]:.4f}",
                })
        except sqlite3.OperationalError:
            pass

        conn.close()
        return result

    def check_date_gaps(self, lookback_days: int = 10) -> List[Dict]:
        """检查近N天内各表是否存在日期缺口。"""
        conn = self._conn()
        cur = conn.cursor()
        today = date.today()
        cutoff = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        results = []
        daily_tables = [
            ("portfolio_snapshots", "date"), ("fund_flows", "date"),
            ("daily_news", "date"), ("index_quotes", "date"),
            ("stock_lhb", "date"), ("stock_margin", "date"),
        ]
        for table, date_col in daily_tables:
            try:
                cur.execute(f"SELECT DISTINCT {date_col} FROM {table} WHERE {date_col} >= ? ORDER BY {date_col}", (cutoff,))
                dates = [datetime.strptime(str(r[0]), "%Y-%m-%d").date() for r in cur.fetchall()]
                if len(dates) < 2:
                    continue
                # 检查连续性
                gaps = []
                for i in range(1, len(dates)):
                    diff = (dates[i] - dates[i-1]).days
                    if diff > 3:  # 超过3天视为缺口(排除周末)
                        gaps.append({"from": str(dates[i-1]), "to": str(dates[i]), "gap_days": diff})
                if gaps:
                    results.append({"table": table, "latest": str(dates[-1]), "gaps": gaps})
            except (sqlite3.OperationalError, ValueError):  # 连续性检查失败，跳过该表
                pass
        conn.close()
        return results

    def generate_alerts(self) -> List[Dict]:
        """生成数据质量告警列表，按严重程度排序。"""
        alerts = []
        score_data = self.compute_quality_score()

        # 1. 新鲜度告警
        for f in score_data["details"]["freshness"]:
            if f["status"] == "STALE":
                alerts.append({
                    "severity": "HIGH",
                    "category": "freshness",
                    "table": f["table"],
                    "message": f"{f['label']} 数据严重过期 (最新: {f['latest_date']}, 延迟{f['days_lag']}天)",
                    "suggestion": f"检查 {f['table']} 采集任务是否正常运行，必要时手动回填",
                })
            elif f["status"] == "WARN":
                alerts.append({
                    "severity": "MEDIUM",
                    "category": "freshness",
                    "table": f["table"],
                    "message": f"{f['label']} 数据延迟 (最新: {f['latest_date']}, 延迟{f['days_lag']}天)",
                })

        # 2. NULL率告警
        null_issues = self.check_null_rates()
        for n in null_issues:
            alerts.append({
                "severity": n["severity"],
                "category": "null_rate",
                "table": n["table"],
                "message": f"{n['table']}.{n['column']} NULL率 {n['null_rate']*100:.1f}% ({n['null_count']}/{n['total_count']})",
                "suggestion": "检查采集源是否缺少该字段，或用默认值填充",
            })

        # 3. 日期缺口告警
        gap_issues = self.check_date_gaps()
        for g in gap_issues:
            for gap in g["gaps"]:
                alerts.append({
                    "severity": "HIGH" if gap["gap_days"] > 5 else "MEDIUM",
                    "category": "date_gap",
                    "table": g["table"],
                    "message": f"{g['table']} 存在缺口: {gap['from']} ~ {gap['to']} ({gap['gap_days']}天)",
                    "suggestion": "运行 backfill_market_events 或对应采集器的回填函数",
                })

        # 4. 黄金数据告警
        gold_check = self.check_gold_data()
        for anomaly in gold_check.get("anomalies", []):
            alerts.append({
                "severity": anomaly["severity"],
                "category": "gold_data",
                "table": anomaly["indicator"],
                "message": anomaly["message"],
            })

        # 5. 评分告警
        if score_data["total_score"] < 70:
            alerts.append({
                "severity": "HIGH",
                "category": "score",
                "table": "全局",
                "message": f"数据质量评分过低: {score_data['total_score']}/100 ({score_data['grade']})",
            })
        elif score_data["total_score"] < 80:
            alerts.append({
                "severity": "LOW",
                "category": "score",
                "table": "全局",
                "message": f"数据质量评分偏低: {score_data['total_score']}/100 ({score_data['grade']})",
            })

        # 按严重程度排序
        severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        alerts.sort(key=lambda x: severity_order.get(x["severity"], 99))
        return alerts

    def get_freshness_summary(self) -> str:
        """生成新鲜度检查的简要文本报告"""
        results = self.check_table_freshness()
        lines = []
        for r in results:
            status_icon = {"OK": "V", "WARN": "!", "STALE": "X", "EMPTY": "-", "ERROR": "?", "USER": "U"}.get(r["status"], "?")
            lines.append(f"  [{status_icon}] {r['label']:8s} | 最新: {r['latest_date']:12s} | 延迟: {r['days_lag']}天 | {r['status']}")
        return "\n".join(lines)
