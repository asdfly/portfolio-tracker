"""
宏观数据采集模块
每日采集美元汇率、中美国债收益率、黄金价格、LPR、Shibor、两融余额等指标。
数据存入 macro_daily 和 market_sentiment 表。
"""

import pandas as pd
import sqlite3
import logging
import os
from datetime import datetime, date

logger = logging.getLogger(__name__)

# 禁用代理
for _k in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy']:
    os.environ.pop(_k, None)


from src.utils.database import get_db_connection


# ==================== macro_daily 指标采集 ====================

def fetch_usd_cny(date_str: str = None) -> list:
    """美元兑人民币汇率 (中行牌价)"""
    import akshare as ak
    records = []
    try:
        if date_str:
            start = date_str.replace("-", "")
            df = ak.currency_boc_sina(symbol='美元', start_date=start, end_date=start)
        else:
            from datetime import timedelta
            today_str = date.today().strftime("%Y%m%d")
            month_ago = (date.today() - timedelta(days=30)).strftime("%Y%m%d")
            df = ak.currency_boc_sina(symbol='美元', start_date=month_ago, end_date=today_str)
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                dt = str(row['日期'])
                if isinstance(row['日期'], (datetime, date)):
                    dt = row['日期'].strftime('%Y-%m-%d')
                mid = row.get('央行中间价')
                if pd.isna(mid):
                    mid = (float(row.get('中行汇买价', 0)) + float(row.get('中行钞卖价/汇卖价', 0))) / 2
                value = float(mid) / 100 if mid and float(mid) > 100 else float(mid)
                records.append({
                    'date': dt, 'indicator_code': 'USD_CNY', 'name': '美元兑人民币',
                    'value': round(value, 4), 'change_pct': None, 'source': 'BOC_SINA',
                })
    except ValueError as e:
        logger.warning(f"USD/CNY 采集失败: {e}")
    return records


def fetch_bond_yields(date_str: str = None) -> list:
    """中美国债收益率 (含2年/5年/10年/30年)"""
    import akshare as ak
    records = []
    try:
        start = date_str.replace("-", "") if date_str else "20260501"
        df = ak.bond_zh_us_rate(start_date=start)
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                dt = str(row['日期'])
                if isinstance(row['日期'], (datetime, date)):
                    dt = row['日期'].strftime('%Y-%m-%d')
                
                indicators = [
                    ('CN_10Y_BOND', '中国10Y国债', '中国国债收益率10年'),
                    ('CN_2Y_BOND', '中国2Y国债', '中国国债收益率2年'),
                    ('CN_30Y_BOND', '中国30Y国债', '中国国债收益率30年'),
                    ('US_10Y_BOND', '美国10Y国债', '美国国债收益率10年'),
                    ('US_2Y_BOND', '美国2Y国债', '美国国债收益率2年'),
                    ('US_30Y_BOND', '美国30Y国债', '美国国债收益率30年'),
                    ('CN_US_SPREAD', '中美利差(10Y)', None),  # 需计算
                ]
                
                for code, name, col in indicators:
                    if code == 'CN_US_SPREAD':
                        cn10 = row.get('中国国债收益率10年')
                        us10 = row.get('美国国债收益率10年')
                        if pd.notna(cn10) and pd.notna(us10):
                            value = round(float(cn10) - float(us10), 4)
                        else:
                            continue
                    else:
                        value = row.get(col)
                        if pd.isna(value):
                            continue
                        value = round(float(value), 4)
                    
                    records.append({
                        'date': dt, 'indicator_code': code, 'name': name,
                        'value': value, 'change_pct': None, 'source': 'EASTMONEY',
                    })
    except ValueError as e:
        logger.warning(f"国债收益率采集失败: {e}")
    return records


def fetch_comex_gold(days: int = 5) -> list:
    """COMEX黄金期货历史数据"""
    import akshare as ak
    records = []
    try:
        df = ak.futures_foreign_hist(symbol="ZSD")
        if df is not None and not df.empty:
            df = df.tail(days)
            for i, (_, row) in enumerate(df.iterrows()):
                dt = str(row['date'])
                if isinstance(row['date'], pd.Timestamp):
                    dt = row['date'].strftime('%Y-%m-%d')
                value = float(row['close'])
                # 计算日涨跌幅
                change_pct = None
                if i > 0:
                    prev_close = float(df.iloc[i-1]['close'])
                    if prev_close > 0:
                        change_pct = round((value / prev_close - 1) * 100, 4)
                records.append({
                    'date': dt, 'indicator_code': 'COMEX_GOLD', 'name': 'COMEX黄金',
                    'value': value, 'change_pct': change_pct, 'source': 'SINA_FUTURES',
                })
    except ValueError as e:
        logger.warning(f"COMEX黄金采集失败: {e}")
    return records


def fetch_sge_gold(days: int = 5) -> list:
    """上海金基准价"""
    import akshare as ak
    records = []
    try:
        df = ak.spot_golden_benchmark_sge()
        if df is not None and not df.empty:
            df = df.tail(days)
            for i, (_, row) in enumerate(df.iterrows()):
                dt = str(row['交易时间'])
                if isinstance(row['交易时间'], (datetime, date)):
                    dt = row['交易时间'].strftime('%Y-%m-%d')
                # 取早盘价和晚盘价的均值
                morning = float(row.get('早盘价', 0) or 0)
                evening = float(row.get('晚盘价', 0) or 0)
                value = (morning + evening) / 2 if (morning > 0 and evening > 0) else max(morning, evening)
                if value <= 0:
                    continue
                change_pct = None
                if i > 0:
                    prev_morning = float(df.iloc[i-1].get('早盘价', 0) or 0)
                    prev_evening = float(df.iloc[i-1].get('晚盘价', 0) or 0)
                    prev = (prev_morning + prev_evening) / 2 if (prev_morning > 0 and prev_evening > 0) else max(prev_morning, prev_evening)
                    if prev > 0:
                        change_pct = round((value / prev - 1) * 100, 4)
                records.append({
                    'date': dt, 'indicator_code': 'SGE_GOLD', 'name': '上海金基准价',
                    'value': round(value, 2), 'change_pct': change_pct, 'source': 'SGE',
                })
    except ValueError as e:
        logger.warning(f"上海金采集失败: {e}")
    return records


def fetch_lpr(days: int = 30) -> list:
    """LPR利率"""
    import akshare as ak
    records = []
    try:
        df = ak.macro_china_lpr()
        if df is not None and not df.empty:
            df = df.tail(days)
            for _, row in df.iterrows():
                dt = str(row['TRADE_DATE'])
                if isinstance(row['TRADE_DATE'], (datetime, date)):
                    dt = row['TRADE_DATE'].strftime('%Y-%m-%d')
                lpr1y = row.get('LPR1Y')
                lpr5y = row.get('LPR5Y')
                if pd.notna(lpr1y):
                    records.append({
                        'date': dt, 'indicator_code': 'LPR_1Y', 'name': '1年期LPR',
                        'value': float(lpr1y), 'change_pct': None, 'source': 'EASTMONEY',
                    })
                if pd.notna(lpr5y):
                    records.append({
                        'date': dt, 'indicator_code': 'LPR_5Y', 'name': '5年期LPR',
                        'value': float(lpr5y), 'change_pct': None, 'source': 'EASTMONEY',
                    })
    except ValueError as e:
        logger.warning(f"LPR采集失败: {e}")
    return records


def fetch_shibor(days: int = 5) -> list:
    """Shibor隔夜利率"""
    import akshare as ak
    records = []
    try:
        df = ak.macro_china_shibor_all()
        if df is not None and not df.empty:
            df = df.tail(days)
            for _, row in df.iterrows():
                dt = str(row.get('日期', ''))
                on_price = row.get('O/N-定价')
                if pd.isna(on_price) or not dt:
                    continue
                on_change = row.get('O/N-涨跌幅')
                records.append({
                    'date': dt, 'indicator_code': 'SHIBOR_ON', 'name': 'Shibor隔夜',
                    'value': float(on_price), 'change_pct': float(on_change) if pd.notna(on_change) else None,
                    'source': 'JIN10',
                })
    except ValueError as e:
        logger.warning(f"Shibor采集失败: {e}")
    return records


# ==================== market_sentiment 指标采集 ====================

def fetch_margin_balance(days: int = 5) -> list:
    """两融余额 (上海+深圳)"""
    import akshare as ak
    records = []
    try:
        dfs = []
        for func, label in [(ak.macro_china_market_margin_sh, '上海'), (ak.macro_china_market_margin_sz, '深圳')]:
            df = func()
            if df is not None and not df.empty:
                df['市场'] = label
                dfs.append(df)
        if not dfs:
            return records
        
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined.tail(days * 2)  # 两市场
        
        for _, row in combined.iterrows():
            dt = str(row['日期'])
            if isinstance(row['日期'], (datetime, date)):
                dt = row['日期'].strftime('%Y-%m-%d')
            market = row.get('市场', '')
            
            # 融资余额
            rzye = row.get('融资余额')
            if pd.notna(rzye) and float(rzye) > 0:
                records.append({
                    'date': dt, 'indicator_code': f'MARGIN_{market[0]}',
                    'name': f'{market}融资余额',
                    'value': float(rzye),
                    'change_value': None, 'change_pct': None,
                    'source': 'JIN10',
                })
            
            # 融资买入额
            rzmre = row.get('融资买入额')
            if pd.notna(rzmre):
                records.append({
                    'date': dt, 'indicator_code': f'MARGIN_BUY_{market[0]}',
                    'name': f'{market}融资买入额',
                    'value': float(rzmre),
                    'change_value': None, 'change_pct': None,
                    'source': 'JIN10',
                })
        
        # 计算两融合计
        if records:
            df_rec = pd.DataFrame(records)
            margin_total = df_rec[df_rec['indicator_code'].str.startswith('MARGIN_') & 
                                   ~df_rec['indicator_code'].str.contains('BUY')]
            if not margin_total.empty:
                grouped = margin_total.groupby('date')['value'].sum()
                for dt, val in grouped.items():
                    records.append({
                        'date': dt, 'indicator_code': 'MARGIN_TOTAL',
                        'name': '两融合计余额',
                        'value': round(float(val), 2),
                        'change_value': None, 'change_pct': None,
                        'source': 'CALCULATED',
                    })
    except ValueError as e:
        logger.warning(f"两融数据采集失败: {e}")
    return records



def fetch_pledge_profile(days: int = 8) -> list:
    """股权质押市场概况（周频）。
    
    数据来源: akshare stock_gpzy_profile_em
    包含指标: A股质押总比例、质押公司数、质押笔数、质押总股数、质押总市值
    """
    import akshare as ak
    records = []
    try:
        df = ak.stock_gpzy_profile_em()
        if df is None or df.empty:
            return records
        
        col_map = {
            '交易日期': 'date', 'A股质押总比例': 'pledge_ratio',
            '质押公司数量': 'pledge_company_count', '质押笔数': 'pledge_count',
            '质押总股数': 'pledge_total_shares', '质押总市值': 'pledge_total_mv',
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df = df.tail(days)  # 取最近N条（周频数据）
        
        for _, row in df.iterrows():
            dt = str(row['date'])
            if isinstance(row['date'], (datetime, date)):
                dt = row['date'].strftime('%Y-%m-%d')
            
            # 质押比例（核心指标，单位%）
            ratio = row.get('pledge_ratio')
            if pd.notna(ratio):
                records.append({
                    'date': dt, 'indicator_code': 'PLEDGE_RATIO',
                    'name': 'A股质押总比例',
                    'value': round(float(ratio), 4),
                    'change_value': None, 'change_pct': None,
                    'source': 'EASTMONEY',
                })
            
            # 质押公司数
            cc = row.get('pledge_company_count')
            if pd.notna(cc):
                records.append({
                    'date': dt, 'indicator_code': 'PLEDGE_COMPANY_COUNT',
                    'name': '质押公司数量',
                    'value': float(cc),
                    'change_value': None, 'change_pct': None,
                    'source': 'EASTMONEY',
                })
            
            # 质押总市值（亿元）
            mv = row.get('pledge_total_mv')
            if pd.notna(mv):
                records.append({
                    'date': dt, 'indicator_code': 'PLEDGE_TOTAL_MV',
                    'name': '质押总市值',
                    'value': round(float(mv), 2),
                    'change_value': None, 'change_pct': None,
                    'source': 'EASTMONEY',
                })
    except ValueError as e:
        logger.warning(f"股权质押数据采集失败: {e}")
    return records

# ==================== 保存函数 ====================

def save_macro_daily(conn: sqlite3.Connection, records: list) -> int:
    """批量保存宏观数据"""
    if not records:
        return 0
    cur = conn.cursor()
    count = 0
    for r in records:
        try:
            cur.execute("""
                INSERT OR REPLACE INTO macro_daily (date, indicator_code, name, value, change_pct, source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (r['date'], r['indicator_code'], r['name'], r['value'], r.get('change_pct'), r.get('source')))
            count += 1
        except sqlite3.OperationalError as e:
            logger.debug(f"保存失败 {r}: {e}")
    conn.commit()
    return count


def save_market_sentiment(conn: sqlite3.Connection, records: list) -> int:
    """批量保存市场情绪数据"""
    if not records:
        return 0
    cur = conn.cursor()
    count = 0
    for r in records:
        try:
            cur.execute("""
                INSERT OR REPLACE INTO market_sentiment 
                (date, indicator_code, name, value, change_value, change_pct, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (r['date'], r['indicator_code'], r['name'], r['value'],
                  r.get('change_value'), r.get('change_pct'), r.get('source')))
            count += 1
        except (sqlite3.OperationalError, sqlite3.IntegrityError, KeyError, TypeError) as e:
            logger.debug(f"保存失败 {r}: {e}")
    conn.commit()
    return count


# ==================== 主采集函数 ====================



def fetch_sge_hist_data(symbol: str = "Au99.99", days: int = 365) -> "pd.DataFrame":
    """采集SGE黄金K线历史数据

    Args:
        symbol: SGE品种代码, 如 Au99.99
        days: 采集最近N天数据

    Returns:
        DataFrame[date, symbol, open, high, low, close, volume]
    """
    import akshare as ak
    try:
        df = ak.spot_hist_sge(symbol=symbol)
        if df is None or df.empty:
            logger.warning("SGE K线数据为空: symbol=%s", symbol)
            return pd.DataFrame()
        df.columns = [c.strip() for c in df.columns]
        # 统一列名
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if "日期" in c or "date" in cl:
                col_map[c] = "date"
            elif "开" in c or "open" in cl:
                col_map[c] = "open"
            elif "高" in c or "high" in cl:
                col_map[c] = "high"
            elif "低" in c or "low" in cl:
                col_map[c] = "low"
            elif "收" in c or "close" in cl:
                col_map[c] = "close"
            elif "量" in c or "volume" in cl or "vol" in cl:
                col_map[c] = "volume"
        df = df.rename(columns=col_map)
        if "date" not in df.columns:
            logger.warning("SGE K线缺少date列: %s", list(df.columns))
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
        df["symbol"] = symbol
        # 只保留需要的列
        keep = ["date", "symbol", "open", "high", "low", "close", "volume"]
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        # 取最近N天
        df = df.sort_values("date").tail(days).reset_index(drop=True)
        logger.info("SGE K线采集: %d条 (symbol=%s)", len(df), symbol)
        return df
    except (ValueError, ConnectionError, OSError) as e:
        logger.warning("SGE K线采集失败: %s", e)
        return pd.DataFrame()


def fetch_global_etf_holdings_data(days: int = 365) -> "pd.DataFrame":
    """采集全球黄金ETF持仓数据

    Args:
        days: 采集最近N天数据

    Returns:
        DataFrame[date, total_holdings, change, total_value]
    """
    import akshare as ak
    try:
        df = ak.macro_cons_gold()
        if df is None or df.empty or "商品" not in df.columns:
            logger.warning("全球ETF持仓数据为空")
            return pd.DataFrame()
        df = df[df["商品"] == "黄金"].copy()
        col_map = {
            "日期": "date",
            "总库存": "total_holdings",
            "增持/减持": "change",
            "总价值": "total_value",
        }
        df = df.rename(columns=col_map)
        df = df.drop(columns=["商品"], errors="ignore")
        if "date" not in df.columns:
            logger.warning("ETF持仓缺少date列: %s", list(df.columns))
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
        # total_value 原始单位为美元, 转为亿美元
        if "total_value" in df.columns:
            df["total_value"] = pd.to_numeric(df["total_value"], errors="coerce") / 1e8
        for c in ["total_holdings", "change"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        keep = ["date", "total_holdings", "change", "total_value"]
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df.sort_values("date").tail(days).reset_index(drop=True)
        logger.info("全球ETF持仓采集: %d条", len(df))
        return df
    except (ValueError, ConnectionError, OSError) as e:
        logger.warning("全球ETF持仓采集失败: %s", e)
        return pd.DataFrame()


def save_gold_sge_hist(conn, df) -> int:
    """将SGE K线数据写入gold_sge_hist表

    Args:
        conn: sqlite3连接
        df: DataFrame[date, symbol, open, high, low, close, volume]

    Returns:
        写入行数
    """
    if df is None or df.empty:
        return 0
    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute(
                """INSERT OR REPLACE INTO gold_sge_hist
                   (date, symbol, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (row["date"], row["symbol"], row.get("open"),
                 row.get("high"), row.get("low"), row.get("close"),
                 row.get("volume")),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return count


def save_gold_etf_holdings(conn, df) -> int:
    """将全球ETF持仓数据写入gold_etf_holdings表

    Args:
        conn: sqlite3连接
        df: DataFrame[date, total_holdings, change, total_value]

    Returns:
        写入行数
    """
    if df is None or df.empty:
        return 0
    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute(
                """INSERT OR REPLACE INTO gold_etf_holdings
                   (date, total_holdings, change, total_value)
                   VALUES (?, ?, ?, ?)""",
                (row["date"], row.get("total_holdings"),
                 row.get("change"), row.get("total_value")),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return count


def fetch_all_macro_daily() -> dict:
    """采集全部宏观数据并保存"""
    conn = get_db_connection()
    stats = {}
    
    # macro_daily 指标
    macro_fetchers = [
        ('USD_CNY', fetch_usd_cny, {}),
        ('BOND_YIELDS', fetch_bond_yields, {}),
        ('COMEX_GOLD', fetch_comex_gold, {'days': 5}),
        ('SGE_GOLD', fetch_sge_gold, {'days': 5}),
        ('LPR', fetch_lpr, {'days': 30}),
        ('SHIBOR', fetch_shibor, {'days': 5}),
    ]
    
    for name, func, kwargs in macro_fetchers:
        try:
            records = func(**kwargs)
            count = save_macro_daily(conn, records)
            stats[name] = count
            logger.info(f"宏观[{name}]: {count}条")
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            logger.warning(f"宏观[{name}] 采集失败: {e}")
            stats[name] = 0
    
    # market_sentiment 指标
    sentiment_fetchers = [
        ('MARGIN', fetch_margin_balance, {'days': 5}),
        ('PLEDGE', fetch_pledge_profile, {'days': 8}),
    ]
    
    for name, func, kwargs in sentiment_fetchers:
        try:
            records = func(**kwargs)
            count = save_market_sentiment(conn, records)
            stats[name] = count
            logger.info(f"情绪[{name}]: {count}条")
        except Exception as e:
            logger.warning(f"情绪[{name}] 采集失败: {e}")
            stats[name] = 0

    # 黄金扩展数据 (gold_sge_hist + gold_etf_holdings)
    gold_fetchers = [
        ("SGE_HIST", fetch_sge_hist_data, {"days": 5}),
        ("GOLD_ETF", fetch_global_etf_holdings_data, {"days": 5}),
    ]
    for name, func, kwargs in gold_fetchers:
        try:
            records = func(**kwargs)
            if name == "SGE_HIST":
                count = save_gold_sge_hist(conn, records)
            else:
                count = save_gold_etf_holdings(conn, records)
            stats[name] = count
            logger.info(f"黄金[{name}]: {count}条")
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            logger.warning(f"黄金[{name}] 采集失败: {e}")
            stats[name] = 0

    conn.close()
    return stats
