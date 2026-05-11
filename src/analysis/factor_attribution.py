"""
因子归因分析模块 - Phase 5 Batch 2

基于公开指数收益率构造多因子模型，分析组合收益来源。
因子定义：
  - 市场因子 (Rm-Rf): 沪深300超额收益（相对无风险利率）
  - 规模因子 (SMB):   中证1000 - 沪深300 收益率差
  - 价值因子 (HML):   红利指数 - 科创50 收益率差
  - 动量因子 (MOM):   过去20日动量（迟滞收益率）
  - 质量因子 (QMJ):   盈利质量（ROE近似：红利指数 vs 成长 proxy）
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
from typing import Optional, Dict, Tuple, List

# 无风险利率（年化），用于计算超额收益
RISK_FREE_RATE_ANNUAL = 0.02  # 2% 年化

# 因子构建所需的指数代码
FACTOR_INDEX_CODES = {
    'hs300': 'sh000300',     # 沪深300 - 市场基准
    'zz1000': 'sh000852',    # 中证1000 - 小盘代表
    'dividend': 'sh000015',  # 红利指数 - 价值代表
    'kc50': 'sh000688',      # 科创50 - 成长代表
}


def get_db_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    from config.settings import DATABASE_PATH
    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _load_index_prices(conn: sqlite3.Connection, codes: List[str],
                       start_date: str, end_date: str) -> pd.DataFrame:
    """加载指定指数在日期范围内的日收盘价"""
    if isinstance(codes, (list, tuple)):
        code_list = list(codes)
    else:
        code_list = [codes]
    placeholders = ','.join(['?'] * len(code_list))
    query = f"""
        SELECT date, code, close, change_pct
        FROM index_quotes
        WHERE code IN ({placeholders})
          AND date BETWEEN ? AND ?
        ORDER BY date, code
    """
    df = pd.read_sql_query(query, conn, params=code_list + [start_date, end_date])
    return df


def _build_factor_returns(conn: sqlite3.Connection,
                          start_date: str,
                          end_date: str) -> pd.DataFrame:
    """
    构造日频因子收益率序列
    
    Returns:
        DataFrame with columns: [date, Rm_Rf, SMB, HML, MOM, QMJ]
        Rm_Rf: 市场超额收益 = 沪深300日收益率 - 无风险利率/252
        SMB: 规模因子 = 中证1000日收益率 - 沪深300日收益率
        HML: 价值因子 = 红利指数日收益率 - 科创50日收益率
        MOM: 动量因子 = 沪深300过去20日累计收益率
        QMJ: 质量因子 = 红利指数日收益率 - 创业板指日收益率
    """
    codes = list(FACTOR_INDEX_CODES.values())
    # 需要额外的指数用于 MOM 和 QMJ
    extra_codes = ['sz399006']  # 创业板指
    all_codes = codes + extra_codes

    df = _load_index_prices(conn, all_codes, start_date, end_date)

    if df.empty:
        return pd.DataFrame()

    # 转为宽表
    pivot = df.pivot_table(index='date', columns='code', values='close')
    
    # 计算日收益率
    returns = pivot.pct_change().dropna()

    # 无风险日收益率
    rf_daily = RISK_FREE_RATE_ANNUAL / 252

    # 构造因子
    hs300 = FACTOR_INDEX_CODES['hs300']
    zz1000 = FACTOR_INDEX_CODES['zz1000']
    dividend = FACTOR_INDEX_CODES['dividend']
    kc50 = FACTOR_INDEX_CODES['kc50']

    factors = pd.DataFrame(index=returns.index)
    factors['Rm_Rf'] = returns[hs300] - rf_daily
    factors['SMB'] = returns[zz1000] - returns[hs300]
    factors['HML'] = returns[dividend] - returns[kc50]

    # MOM: 过去20日累计收益率（沪深300的迟滞动量）
    factors['MOM'] = returns[hs300].rolling(20).apply(lambda x: np.prod(1 + x) - 1, raw=True)

    # QMJ: 质量 = 红利 - 创业板（价值/盈利质量 vs 高估值成长）
    if 'sz399006' in returns.columns:
        factors['QMJ'] = returns[dividend] - returns['sz399006']
    else:
        factors['QMJ'] = returns[dividend] - returns[kc50]

    factors = factors.dropna().reset_index()
    factors.rename(columns={'index': 'date'}, inplace=True)
    return factors


def compute_factor_attribution(portfolio_returns: pd.Series,
                               factor_returns: pd.DataFrame,
                               method: str = 'ols') -> Dict:
    """
    对组合收益进行多因子回归归因
    
    Args:
        portfolio_returns: 组合日收益率序列 (index=date, values=return)
        factor_returns: 因子收益率 DataFrame (columns: Rm_Rf, SMB, HML, MOM, QMJ)
        method: 回归方法，目前仅支持 'ols'
    
    Returns:
        dict 包含:
            - alpha: 超额收益（年化）
            - beta_factors: 各因子暴露度 {factor_name: beta}
            - r_squared: 模型解释力
            - factor_contributions: 各因子对收益的贡献 {factor_name: contribution_pct}
            - factor_returns_contribution: 各因子贡献的累计收益
            - residual_std: 残差标准差
            - n_obs: 观测天数
            - regression_date: 回归期间描述
    """
    # 合并日期
    if isinstance(portfolio_returns.index, pd.DatetimeIndex):
        portfolio_returns.index = portfolio_returns.index.strftime('%Y-%m-%d')
    if isinstance(factor_returns['date'].dtype, object):
        pass  # 已经是字符串
    
    factor_returns_indexed = factor_returns.set_index('date')
    
    # 合并
    merged = pd.DataFrame({
        'portfolio': portfolio_returns,
    })
    merged = merged.join(factor_returns_indexed, how='inner')
    merged = merged.dropna()
    
    if len(merged) < 30:
        return {'error': '数据不足（需要至少30个交易日）', 'n_obs': len(merged)}
    
    # 准备回归变量
    factor_cols = ['Rm_Rf', 'SMB', 'HML', 'MOM', 'QMJ']
    available_factors = [f for f in factor_cols if f in merged.columns]
    
    if not available_factors:
        return {'error': '无可用因子数据'}
    
    X = merged[available_factors].values
    y = merged['portfolio'].values
    n = len(y)
    k = len(available_factors)
    
    # OLS: beta = (X'X)^(-1) X'y
    X_with_intercept = np.column_stack([np.ones(n), X])  # 加入截距
    try:
        # 使用 numpy 最小二乘
        beta, residuals, rank, sv = np.linalg.lstsq(X_with_intercept, y, rcond=None)
    except np.linalg.LinAlgError:
        return {'error': '矩阵奇异，无法求解'}
    
    alpha_daily = beta[0]  # 截距 = alpha
    betas = beta[1:]  # 因子暴露度
    
    # 拟合值和残差
    y_fitted = X_with_intercept @ beta
    residuals_arr = y - y_fitted
    residual_std = np.std(residuals_arr)
    
    # R-squared
    ss_res = np.sum(residuals_arr ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    
    # 各因子对收益的贡献
    total_portfolio_return = np.sum(y)
    factor_contributions = {}
    for i, factor_name in enumerate(available_factors):
        contribution = np.sum(X[:, i] * betas[i])
        pct = contribution / total_portfolio_return * 100 if total_portfolio_return != 0 else 0
        factor_contributions[factor_name] = {
            'beta': float(betas[i]),
            'contribution': float(contribution),
            'contribution_pct': float(pct),
        }
    
    alpha_contribution = float(np.sum(np.ones(n) * alpha_daily))
    alpha_pct = alpha_contribution / total_portfolio_return * 100 if total_portfolio_return != 0 else 0
    
    # 各因子的累计收益贡献
    factor_cum_contrib = {}
    for i, factor_name in enumerate(available_factors):
        cum = np.cumsum(X[:, i] * betas[i])
        factor_cum_contrib[factor_name] = cum.tolist()
    alpha_cum = np.cumsum(np.ones(n) * alpha_daily).tolist()
    
    # 日期列表
    dates = merged.index.tolist() if isinstance(merged.index, pd.Index) else list(range(n))
    
    start_date = dates[0] if dates else 'N/A'
    end_date = dates[-1] if dates else 'N/A'
    
    result = {
        'alpha': float(alpha_daily * 252),  # 年化 alpha
        'alpha_contribution_pct': float(alpha_pct),
        'beta_factors': {name: float(betas[i]) for i, name in enumerate(available_factors)},
        'r_squared': float(r_squared),
        'factor_contributions': factor_contributions,
        'factor_cum_contributions': factor_cum_contrib,
        'alpha_cumulative': alpha_cum,
        'dates': [str(d) for d in dates],
        'residual_std': float(residual_std),
        'n_obs': n,
        'regression_period': f'{start_date} ~ {end_date}',
        'total_return': float(total_portfolio_return * 100),
    }
    
    return result


def compute_style_exposure(positions: pd.DataFrame,
                           etf_categories: dict) -> Dict:
    """
    计算组合的风格暴露度
    
    基于 ETF 类别标签加权计算各维度暴露：
    - 规模维度: 大盘/中盘/小盘
    - 风格维度: 价值/成长/均衡
    - 行业维度: 各行业权重
    
    Args:
        positions: 持仓 DataFrame (code, name, market_value)
        etf_categories: ETF分类字典 {code: {name, sector, style, size}}
    
    Returns:
        dict 包含:
            - size_exposure: 规模暴露 {大盘: x%, 中盘: x%, 小盘: x%}
            - style_exposure: 风格暴露 {价值: x%, 成长: x%, 均衡: x%}
            - sector_exposure: 行业暴露 {行业: x%}
            - total_value: 总市值
    """
    if positions.empty:
        return {}
    
    total_value = positions['market_value'].sum()
    if total_value <= 0:
        return {}
    
    # 初始化暴露度计数器
    size_weights = {'大盘': 0.0, '中盘': 0.0, '小盘': 0.0}
    style_weights = {'价值': 0.0, '成长': 0.0, '均衡': 0.0}
    sector_weights = {}
    
    for _, row in positions.iterrows():
        code = str(row['code'])
        mv = row['market_value']
        weight = mv / total_value
        
        cat_info = etf_categories.get(code, {})
        
        # 规模暴露
        size = cat_info.get('size', '中盘')
        if size in size_weights:
            size_weights[size] += weight
        
        # 风格暴露
        style = cat_info.get('style', '均衡')
        if style in style_weights:
            style_weights[style] += weight
        
        # 行业暴露
        sector = cat_info.get('sector', '其他')
        sector_weights[sector] = sector_weights.get(sector, 0.0) + weight
    
    # 转为百分比
    size_exposure = {k: round(v * 100, 2) for k, v in size_weights.items()}
    style_exposure = {k: round(v * 100, 2) for k, v in style_weights.items()}
    sector_exposure = {k: round(v * 100, 2) for k, v in sorted(sector_weights.items(), key=lambda x: -x[1])}
    
    return {
        'size_exposure': size_exposure,
        'style_exposure': style_exposure,
        'sector_exposure': sector_exposure,
        'total_value': float(total_value),
    }


def compute_sector_rotation(conn: sqlite3.Connection,
                            periods: List[str] = None) -> Dict:
    """
    计算行业轮动指标
    
    分析各行业在不同时间窗口的收益排名变化，衡量轮动速度。
    
    Args:
        conn: 数据库连接
        periods: 时间窗口列表，默认 ['20d', '60d', '120d', '250d']
    
    Returns:
        dict 包含:
            - sector_rankings: {period: {sector: rank}}
            - rotation_speed: {period: rotation_score}
            - sector_period_returns: {period: {sector: return}}
    """
    if periods is None:
        periods = ['20d', '60d', '120d', '250d']
    
    # 获取所有行业相关的指数代码
    sector_codes = {
        '医药': 'sz399989',
        '白酒': 'sz399987',
        '科技': 'sz399989',  # 用医疗 proxy
        '金融': 'sh000015',  # 用红利 proxy
    }
    
    # 可用行业指数
    all_index_query = "SELECT DISTINCT code, name FROM index_quotes"
    all_indices = pd.read_sql_query(all_index_query, conn)
    
    # 构建行业代码映射（使用主要ETF对应的行业指数）
    sector_map = {
        '医药': ['sz399989'],
        '消费': ['sz399987'],
        '金融': ['sh000015'],
        '科技': ['sz399989'],
        '大盘': ['sh000300'],
        '中小盘': ['sh000905', 'sh000852'],
        '创业板': ['sz399006', 'sz399673'],
        '红利': ['sh000015'],
    }
    
    # 实际上我们用 index_quotes 中所有指数来做行业轮动
    # 因为数据库中行业指数有限，我们直接用所有指数做分析
    end_date = pd.read_sql_query("SELECT MAX(date) FROM index_quotes", conn).iloc[0, 0]
    
    # 加载所有指数数据
    all_data = pd.read_sql_query("""
        SELECT date, code, name, close, change_pct
        FROM index_quotes
        WHERE date >= (SELECT date FROM index_quotes ORDER BY date DESC LIMIT 1 OFFSET 300)
        ORDER BY date, code
    """, conn)
    
    if all_data.empty:
        return {'error': '无指数数据'}
    
    result = {
        'sector_rankings': {},
        'rotation_speed': {},
        'sector_period_returns': {},
    }
    
    # 计算各时间窗口的收益
    for period in periods:
        days = int(period.replace('d', ''))
        
        # 获取指定天数的唯一日期
        unique_dates = sorted(all_data['date'].unique())
        if len(unique_dates) < days + 1:
            continue
        
        start_dt = unique_dates[-(days + 1)]
        end_dt = unique_dates[-1]
        
        period_data = all_data[
            (all_data['date'] >= start_dt) & (all_data['date'] <= end_dt)
        ]
        
        # 首尾价格计算收益率
        pivot = period_data.pivot_table(index='date', columns='code', values='close')
        if pivot.shape[0] < 2:
            continue
        
        first_valid = pivot.apply(lambda col: col.first_valid_index())
        last_valid = pivot.apply(lambda col: col.last_valid_index())
        
        period_returns = {}
        for code in pivot.columns:
            fvi = first_valid[code]
            lvi = last_valid[code]
            if pd.notna(fvi) and pd.notna(lvi) and fvi != lvi:
                start_price = pivot.loc[fvi, code]
                end_price = pivot.loc[lvi, code]
                if start_price > 0:
                    ret = (end_price / start_price - 1) * 100
                    name = period_data[period_data['code'] == code]['name'].iloc[0]
                    period_returns[name] = round(ret, 2)
        
        # 排序排名
        sorted_sectors = sorted(period_returns.items(), key=lambda x: -x[1])
        rankings = {name: rank + 1 for rank, (name, _) in enumerate(sorted_sectors)}
        
        result['sector_rankings'][period] = rankings
        result['sector_period_returns'][period] = dict(sorted_sectors)
        
        # 轮动速度 = 收益排名的标准差（标准差越大说明行业分化越大）
        if len(sorted_sectors) > 1:
            ranks_only = list(range(1, len(sorted_sectors) + 1))
            # 用收益的离散度衡量
            returns_only = [r for _, r in sorted_sectors]
            std_dev = np.std(returns_only)
            result['rotation_speed'][period] = round(float(std_dev), 2)
    
    return result


def run_full_attribution(conn: sqlite3.Connection,
                         positions: pd.DataFrame,
                         etf_categories: dict,
                         lookback_days: int = 250) -> Dict:
    """
    运行完整的归因分析（一站式入口）
    
    Args:
        conn: 数据库连接
        positions: 当前持仓 DataFrame
        etf_categories: ETF 分类字典
        lookback_days: 回溯天数
    
    Returns:
        dict 包含:
            - factor_attribution: 多因子归因结果
            - style_exposure: 风格暴露
            - sector_rotation: 行业轮动
    """
    result = {}
    
    # 1. 获取组合历史收益率
    summary = pd.read_sql_query("""
        SELECT date, daily_return
        FROM portfolio_summary
        ORDER BY date DESC
        LIMIT ?
    """, conn, params=[lookback_days + 50])  # 多取一些确保数据量
    
    if summary.empty or len(summary) < 50:
        result['factor_attribution'] = {'error': '组合历史数据不足'}
    else:
        summary = summary.sort_values('date').reset_index(drop=True)
        port_returns = summary.set_index('date')['daily_return'].dropna()
        
        # 构造因子
        start_date = summary['date'].iloc[0]
        end_date = summary['date'].iloc[-1]
        factor_returns = _build_factor_returns(conn, start_date, end_date)
        
        if factor_returns.empty:
            result['factor_attribution'] = {'error': '因子数据不足'}
        else:
            result['factor_attribution'] = compute_factor_attribution(
                port_returns, factor_returns
            )
    
    # 2. 风格暴露
    result['style_exposure'] = compute_style_exposure(positions, etf_categories)
    
    # 3. 行业轮动
    result['sector_rotation'] = compute_sector_rotation(conn)
    
    return result


# 因子名称中文映射
FACTOR_NAME_MAP = {
    'Rm_Rf': '市场因子',
    'SMB': '规模因子',
    'HML': '价值因子',
    'MOM': '动量因子',
    'QMJ': '质量因子',
}

FACTOR_DESCRIPTION = {
    'Rm_Rf': '沪深300超额收益（扣除无风险利率）',
    'SMB': '中证1000 - 沪深300（小盘相对大盘的超额）',
    'HML': '红利指数 - 科创50（价值相对成长的超额）',
    'MOM': '沪深300过去20日累计收益率',
    'QMJ': '红利指数 - 创业板指（盈利质量 proxy）',
}
