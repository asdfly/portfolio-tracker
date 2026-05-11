"""
K线形态识别模块 - Phase 5 Batch 3

内置常见K线形态识别算法，基于 OHLC 数据检测形态信号。
支持的形态类型：
  - 反转形态：十字星、锤子线、上吊线、吞没（看涨/看跌）、启明星、黄昏星
  - 持续形态：红三兵、三只乌鸦、上升三法
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple


def detect_candle_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    对 OHLC 数据进行K线形态识别
    
    Args:
        df: DataFrame，需包含 open, high, low, close 列，按日期升序
    
    Returns:
        DataFrame 新增 pattern 列，标记识别到的形态（多个形态以逗号分隔）
    """
    if df.empty or len(df) < 3:
        df['pattern'] = ''
        return df
    
    required = ['open', 'high', 'low', 'close']
    for col in required:
        if col not in df.columns:
            df['pattern'] = ''
            return df
    
    df = df.copy()
    df['pattern'] = ''
    
    # 计算辅助列
    df['body'] = abs(df['close'] - df['open'])
    df['body_mid'] = (df['open'] + df['close']) / 2
    df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)
    df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']
    df['is_bullish'] = df['close'] >= df['open']
    
    # 计算平均实体大小（最近20日）
    df['avg_body'] = df['body'].rolling(20, min_periods=5).mean()
    
    patterns = {
        'doji': _detect_doji,
        'hammer': _detect_hammer,
        'hanging_man': _detect_hanging_man,
        'bullish_engulfing': _detect_bullish_engulfing,
        'bearish_engulfing': _detect_bearish_engulfing,
        'morning_star': _detect_morning_star,
        'evening_star': _detect_evening_star,
        'three_white_soldiers': _detect_three_white_soldiers,
        'three_black_crows': _detect_three_black_crows,
    }
    
    for idx in range(2, len(df)):
        found = []
        for pname, pfunc in patterns.items():
            try:
                if pfunc(df, idx):
                    found.append(pname)
            except (IndexError, KeyError):
                pass
        if found:
            df.at[df.index[idx], 'pattern'] = ','.join(found)
    
    # 清理辅助列
    df.drop(columns=['body', 'body_mid', 'upper_shadow', 'lower_shadow',
                      'is_bullish', 'avg_body'], inplace=True, errors='ignore')
    return df


def _detect_doji(df: pd.DataFrame, idx: int) -> bool:
    """十字星：实体极小，上下影线较长"""
    row = df.iloc[idx]
    if pd.isna(row.get('avg_body')) or row['avg_body'] == 0:
        return False
    body_ratio = row['body'] / row['avg_body']
    total_range = row['high'] - row['low']
    if total_range == 0:
        return False
    # 实体 < 平均实体的10%，且上下影线都存在
    return body_ratio < 0.1 and row['upper_shadow'] > total_range * 0.2 and row['lower_shadow'] > total_range * 0.2


def _detect_hammer(df: pd.DataFrame, idx: int) -> bool:
    """锤子线（看涨）：出现在下跌趋势中，下影线长，上影线短或无，实体小"""
    row = df.iloc[idx]
    if idx < 1:
        return False
    prev = df.iloc[idx - 1]
    # 前一日为阴线（下跌趋势中）
    if prev['close'] >= prev['open']:
        return False
    total_range = row['high'] - row['low']
    if total_range == 0:
        return False
    # 下影线 > 实体的2倍，上影线 < 实体
    return (row['lower_shadow'] > row['body'] * 2 and
            row['upper_shadow'] < row['body'] * 0.5 and
            row['lower_shadow'] > total_range * 0.6)


def _detect_hanging_man(df: pd.DataFrame, idx: int) -> bool:
    """上吊线（看跌）：形状同锤子线，但出现在上涨趋势中"""
    row = df.iloc[idx]
    if idx < 1:
        return False
    prev = df.iloc[idx - 1]
    # 前一日为阳线（上涨趋势中）
    if prev['close'] < prev['open']:
        return False
    total_range = row['high'] - row['low']
    if total_range == 0:
        return False
    return (row['lower_shadow'] > row['body'] * 2 and
            row['upper_shadow'] < row['body'] * 0.5 and
            row['lower_shadow'] > total_range * 0.6)



def _detect_bullish_engulfing(df: pd.DataFrame, idx: int) -> bool:
    """看涨吞没：前日阴线，今日阳线完全包含前日实体"""
    if idx < 1:
        return False
    prev = df.iloc[idx - 1]
    curr = df.iloc[idx]
    # 前日阴线
    if prev['close'] >= prev['open']:
        return False
    # 今日阳线
    if curr['close'] <= curr['open']:
        return False
    # 今日实体完全包含前日实体
    return (curr['open'] <= prev['close'] and
            curr['close'] >= prev['open'] and
            curr['body'] > prev['body'] * 1.1)


def _detect_bearish_engulfing(df: pd.DataFrame, idx: int) -> bool:
    """看跌吞没：前日阳线，今日阴线完全包含前日实体"""
    if idx < 1:
        return False
    prev = df.iloc[idx - 1]
    curr = df.iloc[idx]
    # 前日阳线
    if prev['close'] <= prev['open']:
        return False
    # 今日阴线
    if curr['close'] >= curr['open']:
        return False
    return (curr['open'] >= prev['close'] and
            curr['close'] <= prev['open'] and
            curr['body'] > prev['body'] * 1.1)


def _detect_morning_star(df: pd.DataFrame, idx: int) -> bool:
    """启明星（看涨反转）：三日K线，大阴+小实体（星）+大阳"""
    if idx < 2:
        return False
    d1 = df.iloc[idx - 2]  # 大阴
    d2 = df.iloc[idx - 1]  # 小星
    d3 = df.iloc[idx]      # 大阳
    
    # d1: 大阴线
    if d1['close'] >= d1['open']:
        return False
    # d2: 小实体（星），与d1之间有跳空
    if d2['body'] > d1['body'] * 0.3:
        return False
    # d3: 大阳线，收盘超过d1实体中点
    if d3['close'] <= d3['open']:
        return False
    d1_mid = (d1['open'] + d1['close']) / 2
    return d3['close'] > d1_mid and d3['body'] > d1['body'] * 0.5


def _detect_evening_star(df: pd.DataFrame, idx: int) -> bool:
    """黄昏星（看跌反转）：三日K线，大阳+小实体（星）+大阴"""
    if idx < 2:
        return False
    d1 = df.iloc[idx - 2]  # 大阳
    d2 = df.iloc[idx - 1]  # 小星
    d3 = df.iloc[idx]      # 大阴
    
    # d1: 大阳线
    if d1['close'] <= d1['open']:
        return False
    # d2: 小实体
    if d2['body'] > d1['body'] * 0.3:
        return False
    # d3: 大阴线，收盘低于d1实体中点
    if d3['close'] >= d3['open']:
        return False
    d1_mid = (d1['open'] + d1['close']) / 2
    return d3['close'] < d1_mid and d3['body'] > d1['body'] * 0.5


def _detect_three_white_soldiers(df: pd.DataFrame, idx: int) -> bool:
    """红三兵（看涨持续）：连续三根阳线，每根收盘逐步升高"""
    if idx < 2:
        return False
    for j in range(idx - 2, idx + 1):
        row = df.iloc[j]
        if row['close'] <= row['open']:
            return False
    d1, d2, d3 = df.iloc[idx-2], df.iloc[idx-1], df.iloc[idx]
    return d2['close'] > d1['close'] and d3['close'] > d2['close']


def _detect_three_black_crows(df: pd.DataFrame, idx: int) -> bool:
    """三只乌鸦（看跌持续）：连续三根阴线，每根收盘逐步降低"""
    if idx < 2:
        return False
    for j in range(idx - 2, idx + 1):
        row = df.iloc[j]
        if row['close'] >= row['open']:
            return False
    d1, d2, d3 = df.iloc[idx-2], df.iloc[idx-1], df.iloc[idx]
    return d2['close'] < d1['close'] and d3['close'] < d2['close']


# 形态名称中文映射
PATTERN_NAME_MAP = {
    'doji': '十字星',
    'hammer': '锤子线',
    'hanging_man': '上吊线',
    'bullish_engulfing': '看涨吞没',
    'bearish_engulfing': '看跌吞没',
    'morning_star': '启明星',
    'evening_star': '黄昏星',
    'three_white_soldiers': '红三兵',
    'three_black_crows': '三只乌鸦',
}

PATTERN_SIGNAL = {
    'doji': 'neutral',
    'hammer': 'bullish',
    'hanging_man': 'bearish',
    'bullish_engulfing': 'bullish',
    'bearish_engulfing': 'bearish',
    'morning_star': 'bullish',
    'evening_star': 'bearish',
    'three_white_soldiers': 'bullish',
    'three_black_crows': 'bearish',
}
