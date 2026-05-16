"""
黄金分析公共工具函数：数据获取、指标计算、通用图表配置
"""

import pandas as pd
import numpy as np


# ---------- 统一图表样式 ----------
DARK_BG = "#1a1a2e"
DARK_FONT_COLOR = "#ddd"
GRID_COLOR = "#333"

COMMON_LAYOUT = dict(
    plot_bgcolor=DARK_BG,
    paper_bgcolor=DARK_BG,
    font=dict(color=DARK_FONT_COLOR),
    margin=dict(l=50, r=30, t=40, b=30),
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)


def base_layout(**overrides):
    layout = dict(COMMON_LAYOUT)
    layout.update(overrides)
    return layout



# ---------- 数据获取 ----------

def fetch_sge_benchmark():
    try:
        import akshare as ak
        df = ak.spot_golden_benchmark_sge()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            date_col = close_col = None
            for c in df.columns:
                cl = c.lower()
                if "日期" in c or "date" in cl or "交易时间" in c:
                    date_col = c
                if "收盘" in c or "close" in cl or "基准价" in c or "价格" in c or "晚盘价" in c or "早盘价" in c:
                    close_col = c
            # 如果没有精确匹配到close列，取第二个数值列（通常为晚盘价）
            if close_col is None and len(df.columns) > 1:
                for c in df.columns:
                    if c != date_col:
                        close_col = c
                        break
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            if close_col:
                df = df.rename(columns={close_col: "close"})
            if date_col:
                df = df.rename(columns={date_col: "date"})
            if 'date' in df.columns:
                df = df.dropna(subset=['date']).sort_values('date').reset_index(drop=True)
            return df
    except Exception as e:
        logger.warning(f"[gold_utils] fetch_sge_benchmark 失败: {e}")
    return None


def fetch_sge_hist(symbol="Au99.99"):
    try:
        import akshare as ak
        df = ak.spot_hist_sge(symbol=symbol)
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            for c in df.columns:
                if "日期" in c or "date" in c.lower():
                    df = df.rename(columns={c: "date"})
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            return df
    except Exception as e:
        logger.warning(f"[gold_utils] fetch_sge_hist 失败: {e}")
    return None


def fetch_china_reserve():
    try:
        import akshare as ak
        df = ak.macro_china_fx_gold()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            date_col = gold_col = fx_col = None
            for c in df.columns:
                if "月份" in c or "日期" in c:
                    date_col = c
                if "黄金储备" in c and "数值" in c:
                    gold_col = c
                if "外汇储备" in c and "数值" in c:
                    fx_col = c
            if date_col and (gold_col or fx_col):
                import re
                _match = df[date_col].str.extract(r"(\d{4})年(\d{2})月份")
                df["_ym_str"] = _match[0] + "-" + _match[1]
                df["date"] = pd.to_datetime(df["_ym_str"], format="%Y-%m")
                df = df.drop(columns=["_ym_str"])
                if gold_col:
                    df = df.rename(columns={gold_col: "gold_reserve"})
                if fx_col:
                    df = df.rename(columns={fx_col: "fx_reserve"})
                if 'date' in df.columns:
                    df = df.dropna(subset=['date']).sort_values('date').reset_index(drop=True)
                return df
    except Exception as e:
        logger.warning(f"[gold_utils] fetch_china_reserve 失败: {e}")
    return None



# ---------- 指标计算 ----------

def calc_monthly_returns(df, date_col="date", close_col="close"):
    df = df.copy()
    df["date"] = pd.to_datetime(df[date_col])
    df = df.dropna(subset=[close_col])
    df = df.sort_values("date")
    monthly = df.set_index("date")[close_col].resample("ME").last()
    monthly_ret = monthly.pct_change().dropna()
    result = pd.DataFrame({
        "year": monthly_ret.index.year,
        "month": monthly_ret.index.month,
        "monthly_return": monthly_ret.values,
    })
    return result


def calc_bollinger(series, window=20, num_std=2):
    middle = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = middle + num_std * std
    lower = middle - num_std * std
    return pd.DataFrame({"middle": middle, "upper": upper, "lower": lower})


def calc_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "hist": hist})


def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    return rsi
