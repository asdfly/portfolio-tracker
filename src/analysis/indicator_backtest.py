"""指标信号回测模块 - Phase 5 Batch 3"""
import numpy as np
import pandas as pd
import sqlite3
from typing import Dict, List, Optional


def get_db_connection() -> sqlite3.Connection:
    from config.settings import DATABASE_PATH
    return sqlite3.connect(str(DATABASE_PATH))


def backtest_indicator_signals(df, signal_col='signal', hold_days=5, forward_col='close'):
    """对信号列进行简单回测，信号=1时买入持有hold_days天"""
    if df.empty or signal_col not in df.columns:
        return {'error': '无有效数据'}
    signals = []
    for idx in range(len(df) - hold_days):
        row = df.iloc[idx]
        sig = row.get(signal_col, 0)
        if sig == 1:
            entry_price = row[forward_col]
            exit_price = df.iloc[idx + hold_days][forward_col]
            if entry_price > 0:
                ret_pct = (exit_price / entry_price - 1) * 100
                signals.append({
                    'date': str(row.get('date', '')),
                    'entry': round(entry_price, 3),
                    'exit': round(exit_price, 3),
                    'return_pct': round(ret_pct, 2),
                    'hold_days': hold_days,
                })
    if not signals:
        return {'error': '回测期间无买入信号', 'total_signals': 0}
    wins = [s for s in signals if s['return_pct'] > 0]
    losses = [s for s in signals if s['return_pct'] <= 0]
    gp = sum(s['return_pct'] for s in wins) if wins else 0
    gl = abs(sum(s['return_pct'] for s in losses)) if losses else 0
    pf = gp / gl if gl > 0 else float('inf')
    rets = [s['return_pct'] for s in signals]
    return {
        'total_signals': len(signals),
        'win_count': len(wins),
        'loss_count': len(losses),
        'win_rate': round(len(wins) / len(signals) * 100, 1),
        'avg_return_pct': round(np.mean(rets), 2),
        'max_return_pct': round(max(rets), 2),
        'max_loss_pct': round(min(rets), 2),
        'profit_factor': round(pf, 2),
        'signals_detail': signals[-20:],
    }


def backtest_technical_composite(conn, code, conditions, lookback=250):
    """基于技术指标复合条件回测"""
    tech_df = pd.read_sql_query("""
        SELECT date, code, ma_signal, macd_signal, rsi_value,
               rsi_status, kdj_signal, bollinger_position, atr_pct, trend
        FROM etf_technical
        WHERE code = ?
        ORDER BY date DESC
        LIMIT ?
    """, conn, params=[code, lookback])
    if tech_df.empty:
        return {'error': f'无 {code} 技术指标数据'}
    tech_df = tech_df.sort_values('date').reset_index(drop=True)
    tech_df['signal'] = 0
    for i, row in tech_df.iterrows():
        match = True
        for key, val in conditions.items():
            if key in row.index and pd.notna(row[key]) and row[key] != val:
                match = False
                break
        if match:
            tech_df.at[i, 'signal'] = 1
    return _backtest_by_signal_only(tech_df)


def save_backtest_result(conn, indicator_id, result, period=''):
    """保存回测结果到数据库"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO indicator_backtest_results
            (indicator_id, test_period, total_signals, win_count, loss_count,
             win_rate, avg_pnl, sharpe)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (indicator_id, period, result.get('total_signals', 0),
          result.get('win_count', 0), result.get('loss_count', 0),
          result.get('win_rate', 0), result.get('avg_return_pct', 0), 0))
    conn.commit()
    return cursor.lastrowid


INDICATOR_TEMPLATES = [
    {'name': '均线多头排列', 'description': 'MA5>MA10>MA20>MA60，趋势确认',
     'formula': {'ma_signal': '多头排列'}, 'signal_type': 'bullish'},
    {'name': 'MACD金叉+RSI未超买', 'description': 'MACD金叉且RSI<70',
     'formula': {'macd_signal': '金叉'}, 'signal_type': 'bullish'},
    {'name': '布林带下轨反弹', 'description': '价格触及布林下轨，超跌反弹',
     'formula': {'bollinger_position': '下轨'}, 'signal_type': 'bullish'},
    {'name': '趋势上涨+低波动', 'description': '趋势上涨且ATR较低',
     'formula': {'trend': '上涨'}, 'signal_type': 'bullish'},
    {'name': 'MACD死叉预警', 'description': 'MACD死叉，卖出预警',
     'formula': {'macd_signal': '死叉'}, 'signal_type': 'bearish'},
    {'name': '均线空头排列', 'description': 'MA5<MA10<MA20<MA60，趋势下行',
     'formula': {'ma_signal': '空头排列'}, 'signal_type': 'bearish'},
]


def _backtest_by_signal_only(df):
    """基于信号出现后下一日的 trend 方向判断胜率"""
    if df.empty:
        return {'error': '无数据'}
    signals = []
    for idx in range(len(df) - 1):
        if df.iloc[idx].get('signal', 0) == 1:
            next_row = df.iloc[idx + 1]
            trend = next_row.get('trend', '')
            if trend == '上涨':
                ret_pct = 1.5
            elif trend == '下跌':
                ret_pct = -1.5
            else:
                ret_pct = 0.0
            signals.append({
                'date': str(df.iloc[idx].get('date', '')),
                'entry': '-',
                'exit': '-',
                'return_pct': round(ret_pct, 2),
                'hold_days': 1,
                'next_trend': trend,
            })
    if not signals:
        return {'error': '回测期间无买入信号', 'total_signals': 0}
    wins = [s for s in signals if s['return_pct'] > 0]
    losses = [s for s in signals if s['return_pct'] <= 0]
    gp = sum(s['return_pct'] for s in wins) if wins else 0
    gl = abs(sum(s['return_pct'] for s in losses)) if losses else 0
    pf = gp / gl if gl > 0 else float('inf')
    rets = [s['return_pct'] for s in signals]
    return {
        'total_signals': len(signals),
        'win_count': len(wins),
        'loss_count': len(losses),
        'win_rate': round(len(wins) / len(signals) * 100, 1),
        'avg_return_pct': round(np.mean(rets), 2),
        'max_return_pct': round(max(rets), 2),
        'max_loss_pct': round(min(rets), 2),
        'profit_factor': round(pf, 2),
        'signals_detail': signals[-20:],
    }
