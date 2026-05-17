"""
黄金分析公共工具函数：数据获取、指标计算、通用图表配置
"""

import logging
import streamlit as st
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


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
    """获取上海金基准价数据"""
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
            if "date" in df.columns:
                df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            return df
    except Exception as e:
        logger.warning("[gold_utils] fetch_sge_benchmark 失败: %s", e)
    return None


def fetch_sge_hist(symbol="Au99.99"):
    """获取SGE历史K线"""
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
        logger.warning("[gold_utils] fetch_sge_hist 失败: %s", e)
    return None


def fetch_china_reserve():
    """获取中国黄金储备+外汇储备"""
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
                if "date" in df.columns:
                    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
                return df
    except Exception as e:
        logger.warning("[gold_utils] fetch_china_reserve 失败: %s", e)
    return None


def fetch_usdcny_hist(symbol="USDCNH"):
    """获取美元兑人民币汇率历史（日频）"""
    try:
        import akshare as ak
        df = ak.forex_hist_em(symbol=symbol)
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            date_col = price_col = None
            for c in df.columns:
                if "日期" in c:
                    date_col = c
                if "最新价" in c:
                    price_col = c
            if date_col and price_col:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                df = df.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)
                return df.rename(columns={date_col: "date", price_col: "close"})
        return df
    except Exception as e:
        logger.warning("[gold_utils] fetch_usdcny_hist 失败: %s", e)
    return None


def fetch_bond_yields(years=3):
    """获取中美10年期国债收益率（默认近3年）"""
    try:
        import akshare as ak
        df = ak.bond_zh_us_rate()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            date_col = cn_col = us_col = None
            for c in df.columns:
                if "日期" in c:
                    date_col = c
                if "中国国债收益率10年" in c:
                    cn_col = c
                if "美国国债收益率10年" in c:
                    us_col = c
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                df = df.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)
                cutoff = pd.Timestamp.now() - pd.DateOffset(years=years)
                df = df[df[date_col] >= cutoff]
            result = df[[date_col]].copy() if date_col else df.copy()
            if cn_col:
                result = result.join(df[cn_col])
                result = result.rename(columns={cn_col: "cn_10y"})
            if us_col:
                result = result.join(df[us_col])
                result = result.rename(columns={us_col: "us_10y"})
            if date_col:
                result = result.rename(columns={date_col: "date"})
            return result
    except Exception as e:
        logger.warning("[gold_utils] fetch_bond_yields 失败: %s", e)
    return None


def fetch_china_cpi():
    """获取中国CPI月度数据（同比+环比）"""
    try:
        import akshare as ak
        df = ak.macro_china_cpi()
        if df is not None and not df.empty:
            df.columns = [c.strip() for c in df.columns]
            month_col = yoy_col = mom_col = None
            for c in df.columns:
                if "月份" in c:
                    month_col = c
                if "全国" in c and "同比" in c:
                    yoy_col = c
                if "全国" in c and "环比" in c:
                    mom_col = c
            if month_col:
                import re
                _match = df[month_col].str.extract(r"(\d{4})年(\d{2})月份")
                df["_ym_str"] = _match[0] + "-" + _match[1]
                df["date"] = pd.to_datetime(df["_ym_str"], format="%Y-%m")
                df = df.drop(columns=["_ym_str"])
                df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            result = df[["date"]].copy()
            if yoy_col:
                result["cpi_yoy"] = pd.to_numeric(df[yoy_col], errors="coerce")
            if mom_col:
                result["cpi_mom"] = pd.to_numeric(df[mom_col], errors="coerce")
            return result
    except Exception as e:
        logger.warning("[gold_utils] fetch_china_cpi 失败: %s", e)
    return None



# ---------- 指标计算 ----------

def calc_monthly_returns(df, date_col="date", close_col="close"):
    """计算月度收益率"""
    df = df.copy()
    df["date"] = pd.to_datetime(df[date_col])
    df = df.dropna(subset=[close_col])
    df = df.sort_values("date")
    monthly = df.set_index("date")[close_col].resample("ME").last()
    monthly_ret = monthly.pct_change().dropna()
    return pd.DataFrame({
        "year": monthly_ret.index.year,
        "month": monthly_ret.index.month,
        "monthly_return": monthly_ret.values,
    })


def calc_bollinger(series, window=20, num_std=2):
    """布林带"""
    middle = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = middle + num_std * std
    lower = middle - num_std * std
    return pd.DataFrame({"middle": middle, "upper": upper, "lower": lower})


def calc_macd(series, fast=12, slow=26, signal=9):
    """MACD"""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "hist": hist})


def calc_rsi(series, period=14):
    """RSI"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    return rsi


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_china_reserve_data():
    """获取中国黄金储备和外汇储备月度数据（macro_china_fx_gold）
    Returns: DataFrame with columns [月份, 黄金储备, 外汇储备, 黄金储备同比, 黄金储备环比]
    """
    try:
        import akshare as ak
        df = ak.macro_china_fx_gold()
        df = df.rename(columns={
            '月份': 'month',
            '黄金储备-数值': 'gold_reserve',
            '黄金储备-同比': 'gold_reserve_yoy',
            '黄金储备-环比': 'gold_reserve_mom',
            '国家外汇储备-数值': 'fx_reserve',
            '国家外汇储备-同比': 'fx_reserve_yoy',
            '国家外汇储备-环比': 'fx_reserve_mom',
        })
        df['month'] = pd.to_datetime(df['month'].str.replace('年', '-').str.replace('月份', ''), format='%Y-%m')
        return df
    except Exception as e:
        import streamlit as st
        st.warning(f"获取中国储备数据失败: {e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_global_etf_holdings():
    """获取全球黄金ETF持仓数据（macro_cons_gold）
    Returns: DataFrame with columns [date, total_holdings, change, total_value]
    """
    try:
        import akshare as ak
        df = ak.macro_cons_gold()
        df = df[df['商品'] == '黄金'].copy()
        df = df.rename(columns={
            '日期': 'date',
            '总库存': 'total_holdings',
            '增持/减持': 'change',
            '总价值': 'total_value',
        })
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        return df
    except Exception as e:
        import streamlit as st
        st.warning(f"获取全球ETF持仓数据失败: {e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_comex_inventory():
    """获取COMEX黄金库存数据（futures_comex_inventory）
    Returns: DataFrame with columns [date, inventory_ton, inventory_oz]
    """
    try:
        import akshare as ak
        df = ak.futures_comex_inventory()
        df = df.rename(columns={
            '日期': 'date',
            'COMEX黄金库存量-吨': 'inventory_ton',
            'COMEX黄金库存量-盎司': 'inventory_oz',
        })
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        return df
    except Exception as e:
        import streamlit as st
        st.warning(f"获取COMEX库存数据失败: {e}")
        return None