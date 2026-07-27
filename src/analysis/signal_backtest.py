"""
ETF 趋势信号回测引擎

对 etf_technical 表中 6 类技术指标信号进行历史回测，
量化各信号在不同前瞻时间窗口（5/10/20/30/60 交易日）下
对收益方向的预测准确率，并计算置信度评分。
"""
import logging
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

# ============================================================
# 常量定义
# ============================================================

FORWARD_WINDOWS = [5, 10, 20, 30, 60]

SIGNAL_DIRECTIONS: Dict[str, Dict[str, int]] = {
    "ma_signal": {
        "多头排列": 1, "金叉": 1,
        "空头排列": -1, "死叉": -1,
    },
    "macd_signal": {
        "多头": 1, "金叉": 1, "看多": 1,
        "空头": -1, "死叉": -1,
        "中性": 0,
    },
    "rsi_status": {
        "超卖": 1, "严重超卖": 1,
        "超买": -1, "严重超买": -1,
        "正常": 0,
    },
    "kdj_signal": {
        "金叉": 1, "死叉": -1, "中性": 0,
    },
    "trend": {
        "强势上涨": 1, "温和上涨": 1,
        "下跌": -1, "温和下跌": -1, "强势下跌": -1,
        "震荡整理": 0,
    },
}

# 布林带: 数值型，特殊处理
BOLLINGER_BUY_THRESHOLD = 20
BOLLINGER_SELL_THRESHOLD = 80

MIN_SAMPLE_SIZE = 10  # 最小样本量


# ============================================================
# 置信度评分
# ============================================================

def compute_confidence(n: int, hit_rate: float, p_value: float,
                       avg_return: float) -> Tuple[float, str]:
    """计算置信度评分 (0-100) 和等级 (A/B/C/D)。

    权重:
      命中率偏离 50% 的程度  40 分
      样本量 (100+ 满分)     20 分
      统计显著性 (p值梯度)   25 分
      平均收益 (5%+ 满分)     15 分
    """
    hit_component = min(abs(hit_rate - 0.5) * 80, 40)
    sample_component = min(n / 100, 1.0) * 20

    if p_value < 0.01:
        sig_component = 25
    elif p_value < 0.05:
        sig_component = 20
    elif p_value < 0.10:
        sig_component = 10
    else:
        sig_component = 0

    return_component = min(abs(avg_return) / 0.05, 1.0) * 15

    score = round(hit_component + sample_component + sig_component + return_component, 1)
    grade = "A" if score >= 70 else "B" if score >= 50 else "C" if score >= 30 else "D"
    return score, grade


# ============================================================
# 回测核心
# ============================================================

def _load_tech_with_price(conn) -> pd.DataFrame:
    """从数据库加载技术指标 + 价格数据"""
    tech = pd.read_sql_query(
        "SELECT date, code, ma_signal, macd_signal, rsi_value, rsi_status, "
        "kdj_signal, bollinger_position, trend, atr_pct "
        "FROM etf_technical ORDER BY code, date", conn)

    prices = pd.read_sql_query(
        "SELECT date, code, current_price AS close "
        "FROM portfolio_snapshots ORDER BY code, date", conn)

    df = tech.merge(prices, on=["date", "code"], how="inner")
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    logger.info(f"信号回测数据: {len(df)} 行, {df['code'].nunique()} 只ETF, "
                f"{df['date'].min()} ~ {df['date'].max()}")
    return df


def _compute_forward_returns(df: pd.DataFrame,
                             windows: List[int] = None) -> pd.DataFrame:
    """计算前瞻收益"""
    if windows is None:
        windows = FORWARD_WINDOWS

    for n in windows:
        df[f"fwd_ret_{n}"] = df.groupby("code")["close"].transform(
            lambda x: x.shift(-n) / x - 1
        )
    return df


def _backtest_categorical(df: pd.DataFrame, indicator: str,
                          direction_map: Dict[str, int],
                          windows: List[int]) -> List[dict]:
    """对分类信号（如 ma_signal='多头排列'）进行回测"""
    results = []
    for signal_val, direction in direction_map.items():
        if direction == 0:
            continue
        mask = df[indicator] == signal_val
        subset = df[mask]
        for n in windows:
            ret_col = f"fwd_ret_{n}"
            valid = subset[ret_col].dropna()
            n_samples = len(valid)
            if n_samples < MIN_SAMPLE_SIZE:
                continue

            hits = int(((direction > 0) & (valid > 0)).sum() +
                       ((direction < 0) & (valid < 0)).sum())
            hit_rate = hits / n_samples
            avg_ret = float(valid.mean())
            std_ret = float(valid.std()) if n_samples > 1 else 0.0

            # 二项检验 (正态近似)
            se = 0.5 / (n_samples ** 0.5)
            t_stat = (hit_rate - 0.5) / se if se > 0 else 0.0
            p_value = 2 * (1 - stats.norm.cdf(abs(t_stat)))

            conf, grade = compute_confidence(n_samples, hit_rate, p_value, avg_ret)

            results.append({
                "indicator": indicator,
                "signal_value": signal_val,
                "signal_direction": direction,
                "forward_window": n,
                "sample_count": n_samples,
                "hit_count": hits,
                "hit_rate": round(hit_rate, 4),
                "avg_return": round(avg_ret, 6),
                "std_return": round(std_ret, 6),
                "t_statistic": round(t_stat, 4),
                "p_value": round(p_value, 6),
                "confidence_score": conf,
                "confidence_grade": grade,
            })
    return results


def _backtest_bollinger(df: pd.DataFrame, windows: List[int]) -> List[dict]:
    """对布林带数值型信号进行回测"""
    results = []
    boll = df["bollinger_position"].dropna()
    if boll.empty:
        return results

    for direction, lo, hi, label in [
        (1, 0, BOLLINGER_BUY_THRESHOLD, "低位(≤20)"),
        (-1, BOLLINGER_SELL_THRESHOLD, 100, "高位(≥80)"),
    ]:
        mask = (df["bollinger_position"] >= lo) & (df["bollinger_position"] < hi + 1)
        subset = df[mask]
        for n in windows:
            ret_col = f"fwd_ret_{n}"
            valid = subset[ret_col].dropna()
            n_samples = len(valid)
            if n_samples < MIN_SAMPLE_SIZE:
                continue

            hits = int(((direction > 0) & (valid > 0)).sum() +
                       ((direction < 0) & (valid < 0)).sum())
            hit_rate = hits / n_samples
            avg_ret = float(valid.mean())
            std_ret = float(valid.std()) if n_samples > 1 else 0.0

            se = 0.5 / (n_samples ** 0.5)
            t_stat = (hit_rate - 0.5) / se if se > 0 else 0.0
            p_value = 2 * (1 - stats.norm.cdf(abs(t_stat)))
            conf, grade = compute_confidence(n_samples, hit_rate, p_value, avg_ret)

            results.append({
                "indicator": "bollinger",
                "signal_value": label,
                "signal_direction": direction,
                "forward_window": n,
                "sample_count": n_samples,
                "hit_count": hits,
                "hit_rate": round(hit_rate, 4),
                "avg_return": round(avg_ret, 6),
                "std_return": round(std_ret, 6),
                "t_statistic": round(t_stat, 4),
                "p_value": round(p_value, 6),
                "confidence_score": conf,
                "confidence_grade": grade,
            })
    return results


def run_backtest(conn=None) -> pd.DataFrame:
    """执行完整回测，返回统计结果 DataFrame"""
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        df = _load_tech_with_price(conn)
        df = _compute_forward_returns(df)

        all_results = []
        for ind_col, dir_map in SIGNAL_DIRECTIONS.items():
            all_results.extend(_backtest_categorical(df, ind_col, dir_map, FORWARD_WINDOWS))

        all_results.extend(_backtest_bollinger(df, FORWARD_WINDOWS))

        result_df = pd.DataFrame(all_results)
        logger.info(f"回测完成: {len(result_df)} 组统计")
        return result_df
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 数据库写入
# ============================================================

def save_backtest_results(result_df: pd.DataFrame, conn=None) -> int:
    """保存回测结果到 signal_backtest_stats 表"""
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        # 建表（如果不存在）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_backtest_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator TEXT NOT NULL,
                signal_value TEXT NOT NULL,
                signal_direction INTEGER NOT NULL,
                forward_window INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                hit_count INTEGER NOT NULL,
                hit_rate REAL NOT NULL,
                avg_return REAL NOT NULL,
                std_return REAL,
                t_statistic REAL,
                p_value REAL,
                confidence_score REAL NOT NULL,
                confidence_grade TEXT,
                backtest_date TEXT NOT NULL,
                UNIQUE(indicator, signal_value, forward_window)
            )
        """)
        conn.execute("DELETE FROM signal_backtest_stats")
        conn.commit()

        today = datetime.now().strftime("%Y-%m-%d")
        rows = result_df.copy()
        rows["backtest_date"] = today

        rows.to_sql("signal_backtest_stats", conn, if_exists="append",
                    index=False, method="multi")
        conn.commit()
        logger.info(f"回测结果已保存: {len(rows)} 行")
        return len(rows)
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 当前信号置信度
# ============================================================

def _get_bollinger_signal(val) -> Tuple[str, int]:
    """布林带数值 → (信号标签, 方向)"""
    if pd.isna(val):
        return ("中性", 0)
    v = float(val)
    if v <= BOLLINGER_BUY_THRESHOLD:
        return (f"低位({v:.0f}%)", 1)
    if v >= BOLLINGER_SELL_THRESHOLD:
        return (f"高位({v:.0f}%)", -1)
    return (f"中位({v:.0f}%)", 0)


def get_current_confidence(conn=None) -> pd.DataFrame:
    """获取当前各ETF最新信号及其置信度

    返回列: code, name, date, indicator, signal_value, signal_direction,
           conf_5d, conf_10d, conf_20d, conf_30d, conf_60d,
           hit_rate_5d, hit_rate_10d, hit_rate_20d, hit_rate_30d, hit_rate_60d,
           composite_confidence, composite_grade
    """
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        # 1. 检查回测统计表是否存在
        cnt = conn.execute(
            "SELECT COUNT(*) FROM signal_backtest_stats").fetchone()[0]
        if cnt == 0:
            logger.warning("signal_backtest_stats 表为空，请先运行 run_backtest + save_backtest_results")
            return pd.DataFrame()

        # 2. 获取每只ETF最新日的技术指标
        latest = pd.read_sql_query("""
            SELECT t.date, t.code, t.ma_signal, t.macd_signal,
                   t.rsi_value, t.rsi_status, t.kdj_signal,
                   t.bollinger_position, t.trend, t.atr_pct,
                   s.name
            FROM etf_technical t
            LEFT JOIN portfolio_snapshots s
                ON t.code = s.code AND t.date = s.date
            WHERE t.date = (SELECT MAX(date) FROM etf_technical)
        """, conn)
        if latest.empty:
            return pd.DataFrame()

        # 3. 读取回测统计
        stats_df = pd.read_sql_query(
            "SELECT * FROM signal_backtest_stats", conn)

        # 构建查找索引: (indicator, signal_value, forward_window) → row
        stats_lookup = {}
        for _, r in stats_df.iterrows():
            key = (r["indicator"], r["signal_value"], int(r["forward_window"]))
            stats_lookup[key] = r

        # 4. 为每只ETF的每个指标匹配置信度
        results = []
        for _, etf in latest.iterrows():
            code = etf["code"]
            name = etf["name"] if pd.notna(etf["name"]) else code
            date = etf["date"]

            # 当前各指标信号值
            current_signals = {
                "ma_signal": (str(etf["ma_signal"]),
                              SIGNAL_DIRECTIONS["ma_signal"].get(str(etf["ma_signal"]), 0)),
                "macd_signal": (str(etf["macd_signal"]),
                                SIGNAL_DIRECTIONS["macd_signal"].get(str(etf["macd_signal"]), 0)),
                "rsi_status": (str(etf["rsi_status"]),
                               SIGNAL_DIRECTIONS["rsi_status"].get(str(etf["rsi_status"]), 0)),
                "kdj_signal": (str(etf["kdj_signal"]),
                               SIGNAL_DIRECTIONS["kdj_signal"].get(str(etf["kdj_signal"]), 0)),
                "trend": (str(etf["trend"]),
                          SIGNAL_DIRECTIONS["trend"].get(str(etf["trend"]), 0)),
            }

            # 布林带特殊处理
            boll_label, boll_dir = _get_bollinger_signal(etf["bollinger_position"])
            # 布林带回测统计中 signal_value 用 "低位(≤20)" / "高位(≥80)"
            if boll_dir == 1:
                boll_lookup_val = "低位(≤20)"
            elif boll_dir == -1:
                boll_lookup_val = "高位(≥80)"
            else:
                boll_lookup_val = None
            current_signals["bollinger"] = (boll_label, boll_dir, boll_lookup_val)

            for ind, sig_info in current_signals.items():
                if ind == "bollinger":
                    sig_val_display = sig_info[0]
                    direction = sig_info[1]
                    lookup_val = sig_info[2]
                else:
                    sig_val_display = sig_info[0]
                    direction = sig_info[1]
                    lookup_val = sig_val_display if direction != 0 else None

                row = {
                    "code": code, "name": name, "date": date,
                    "indicator": ind,
                    "signal_value": sig_val_display,
                    "signal_direction": direction,
                }

                confs = []
                for n in FORWARD_WINDOWS:
                    col_conf = f"conf_{n}d"
                    col_hr = f"hit_rate_{n}d"
                    if lookup_val is not None:
                        key = (ind, lookup_val, n)
                        s = stats_lookup.get(key)
                        if s is not None:
                            row[col_conf] = float(s["confidence_score"])
                            row[col_hr] = float(s["hit_rate"])
                            confs.append(float(s["confidence_score"]))
                        else:
                            row[col_conf] = None
                            row[col_hr] = None
                    else:
                        row[col_conf] = None
                        row[col_hr] = None

                # 综合置信度: 有方向信号的窗口置信度加权平均
                # 权重: 5d=0.10, 10d=0.15, 20d=0.25, 30d=0.25, 60d=0.25
                if confs:
                    weights = [0.10, 0.15, 0.25, 0.25, 0.25]
                    valid_pairs = [(c, w) for c, w in zip(confs, weights) if c is not None]
                    if valid_pairs:
                        total_w = sum(w for _, w in valid_pairs)
                        composite = sum(c * w for c, w in valid_pairs) / total_w
                        row["composite_confidence"] = round(composite, 1)
                        row["composite_grade"] = (
                            "A" if composite >= 70 else
                            "B" if composite >= 50 else
                            "C" if composite >= 30 else "D"
                        )
                    else:
                        row["composite_confidence"] = None
                        row["composite_grade"] = None
                else:
                    row["composite_confidence"] = None
                    row["composite_grade"] = None

                results.append(row)

        return pd.DataFrame(results)
    finally:
        if close_conn:
            conn.close()


def save_current_confidence(conf_df: pd.DataFrame, conn=None) -> int:
    """保存当前信号置信度到 signal_confidence_current 表"""
    close_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        close_conn = True

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_confidence_current (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                date TEXT NOT NULL,
                indicator TEXT NOT NULL,
                signal_value TEXT NOT NULL,
                signal_direction INTEGER NOT NULL,
                conf_5d REAL,
                conf_10d REAL,
                conf_20d REAL,
                conf_30d REAL,
                conf_60d REAL,
                composite_confidence REAL,
                composite_grade TEXT,
                hit_rate_5d REAL,
                hit_rate_10d REAL,
                hit_rate_20d REAL,
                hit_rate_30d REAL,
                hit_rate_60d REAL,
                updated_at TEXT NOT NULL,
                UNIQUE(code, indicator, date)
            )
        """)
        conn.execute("DELETE FROM signal_confidence_current")
        conn.commit()

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = conf_df.copy()
        rows["updated_at"] = now

        rows.to_sql("signal_confidence_current", conn, if_exists="append",
                    index=False, method="multi")
        conn.commit()
        logger.info(f"当前置信度已保存: {len(rows)} 行")
        return len(rows)
    finally:
        if close_conn:
            conn.close()


# ============================================================
# 一键运行
# ============================================================

def run_full_backtest_pipeline() -> dict:
    """完整回测流程: 回测 → 保存 → 当前置信度 → 保存

    Returns:
        {"backtest_rows": int, "confidence_rows": int}
    """
    from src.utils.database import get_db_connection
    conn = get_db_connection()
    try:
        # 1. 回测
        result_df = run_backtest(conn)
        # 2. 保存回测结果
        bt_n = save_backtest_results(result_df, conn)
        # 3. 计算当前置信度
        conf_df = get_current_confidence(conn)
        # 4. 保存当前置信度
        if not conf_df.empty:
            conf_n = save_current_confidence(conf_df, conn)
        else:
            conf_n = 0
        logger.info(f"回测流程完成: {bt_n} 统计行, {conf_n} 置信度行")
        return {"backtest_rows": bt_n, "confidence_rows": conf_n}
    finally:
        conn.close()
