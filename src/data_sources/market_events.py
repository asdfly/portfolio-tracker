"""
市场事件数据采集模块 (Phase 1)
使用 AKShare 采集龙虎榜、融资融券、股东增减持、机构调研、大宗交易数据。
"""
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
import time

logger = logging.getLogger(__name__)


def get_db_connection() -> sqlite3.Connection:
    from config.settings import DATABASE_PATH
    return sqlite3.connect(str(DATABASE_PATH))


# ============================================================
#  龙虎榜 (stock_lhb)
# ============================================================

def fetch_lhb_data(date_str: str) -> pd.DataFrame:
    """获取指定日期的龙虎榜数据。
    
    Args:
        date_str: 日期字符串, 格式 YYYYMMDD
        
    Returns:
        DataFrame, 标准化列名
    """
    try:
        import akshare as ak
        df = ak.stock_lhb_detail_em(start_date=date_str, end_date=date_str)
        if df is None or df.empty:
            return pd.DataFrame()
        
        col_map = {
            '序号': '_seq',
            '代码': 'code', '名称': 'name',
            '收盘价': 'close', '涨跌幅': 'change_pct',
            '龙虎榜净买额': 'lhb_net_buy', '龙虎榜买入额': 'lhb_buy_amount',
            '龙虎榜卖出额': 'lhb_sell_amount', '龙虎榜成交额': 'lhb_volume',
            '市场总成交额': 'market_volume', '净买额占总成交比': 'net_buy_ratio',
            '成交额占总成交比': 'volume_ratio', '换手率': 'turnover_rate',
            '流通市值': 'float_mv', '上榜原因': 'reason',
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        if '_seq' in df.columns:
            df = df.drop(columns=['_seq'])
        
        df['date'] = pd.to_datetime(df['上榜日'], errors='coerce').dt.strftime('%Y-%m-%d').fillna(datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d'))
        
        # 仅保留DB列
        keep = ['date', 'code', 'name', 'close', 'change_pct', 'lhb_net_buy',
                'lhb_buy_amount', 'lhb_sell_amount', 'lhb_volume', 'market_volume',
                'net_buy_ratio', 'volume_ratio', 'turnover_rate', 'float_mv', 'reason']
        df = df[[c for c in keep if c in df.columns]]
        return df
    except Exception as e:
        logger.warning(f"获取龙虎榜数据失败 ({date_str}): {e}")
        return pd.DataFrame()


# ============================================================
#  融资融券 (stock_margin)
# ============================================================

def fetch_margin_data(date_str: str) -> pd.DataFrame:
    """获取指定日期的融资融券数据（上交所）。"""
    try:
        import akshare as ak
        try:
            df = ak.stock_margin_detail_sse(date=date_str)
        except (ValueError, TypeError):
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        col_map = {
            '信用交易日期': 'date',
            '标的证券代码': 'code', '标的证券简称': 'name',
            '融资余额': 'margin_balance', '融资买入额': 'margin_buy',
            '融资偿还额': 'margin_repay', '融券余量': 'short_volume',
            '融券卖出量': 'short_sell', '融券偿还量': 'short_repay',
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        keep = ['date', 'code', 'name', 'margin_balance', 'margin_buy',
                'margin_repay', 'short_volume', 'short_sell', 'short_repay']
        df = df[[c for c in keep if c in df.columns]]
        if 'date' in df.columns:
            df['date'] = df['date'].apply(lambda x: (
                f'{x[:4]}-{x[4:6]}-{x[6:8]}' if isinstance(x, str) and len(x) == 8 else x))
        return df
    except Exception as e:
        logger.warning(f'获取融资融券数据失败 ({date_str}): {e}')
        return pd.DataFrame()

def fetch_holder_change_data(date_str: str) -> pd.DataFrame:
    """获取指定日期的股东增减持明细数据。

    使用 stock_gdfx_free_holding_detail_em 返回个股级别增减持明细。
    回退方案: 若detail API失败，使用holding_change_em汇总数据。

    Args:
        date_str: 日期字符串, 格式 YYYYMMDD

    Returns:
        DataFrame, 标准化列名
    """
    try:
        import akshare as ak

        # 首选: stock_gdfx_free_holding_detail_em (个股明细)
        try:
            df = ak.stock_gdfx_free_holding_detail_em(date=date_str)
        except (TypeError, ValueError, KeyError):
            df = pd.DataFrame()

        if df is not None and not df.empty:
            col_map = {
                '序号': '_seq',
                '股东名称': 'holder_name', '股东类型': 'holder_type',
                '股票代码': 'code', '股票简称': 'name',
                '报告期': 'report_period',
                '期末持股-数量': 'holding_qty',
                '期末持股-数量变化': 'qty_change',
                '期末持股-数量变化比例': 'qty_change_pct',
                '期末持股-持股变动': 'change_type',
                '期末持股-流通市值': 'float_mv',
                '公告日': 'announce_date',
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            if '_seq' in df.columns:
                df = df.drop(columns=['_seq'])
            df['date'] = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
            # 标准化日期列
            for dcol in ['report_period', 'announce_date']:
                if dcol in df.columns:
                    df[dcol] = df[dcol].apply(lambda x: (
                        x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else
                        str(x)[:10] if x and str(x) != 'nan' else None
                    ))
            keep = ['date', 'holder_name', 'holder_type', 'code', 'name',
                    'report_period', 'holding_qty', 'qty_change', 'qty_change_pct',
                    'change_type', 'float_mv', 'announce_date']
            df = df[[c for c in keep if c in df.columns]]
            return df

        # 回退: stock_gdfx_free_holding_change_em (汇总级别)
        try:
            df2 = ak.stock_gdfx_free_holding_change_em(date=date_str)
        except (TypeError, ValueError, KeyError):
            return pd.DataFrame()
        if df2 is None or df2.empty:
            return pd.DataFrame()
        col_map2 = {
            '序号': '_seq',
            '股东名称': 'holder_name', '股东类型': 'holder_type',
            '流通市值统计': 'float_mv',
        }
        df2 = df2.rename(columns={k: v for k, v in col_map2.items() if k in df2.columns})
        if '_seq' in df2.columns:
            df2 = df2.drop(columns=['_seq'])
        df2['date'] = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
        keep2 = ['date', 'holder_name', 'holder_type', 'float_mv']
        return df2[[c for c in keep2 if c in df2.columns]]

    except Exception as e:
        logger.warning(f'获取股东增减持数据失败 ({date_str}): {e}')
        return pd.DataFrame()

def fetch_institution_research_data(date_str: str) -> pd.DataFrame:
    """获取指定日期的机构调研数据。
    
    Args:
        date_str: 日期字符串, 格式 YYYYMMDD
        
    Returns:
        DataFrame, 标准化列名
    """
    try:
        import akshare as ak
        try:
            df = ak.stock_jgdy_detail_em(date=date_str)
        except (TypeError, KeyError):
            # AKShare API返回None result (无数据或日期超出范围)
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        
        col_map = {
            '序号': '_seq',
            '代码': 'code', '名称': 'name',
            '最新价': 'price', '涨跌幅': 'change_pct',
            '调研机构': 'institution', '机构类型': 'inst_type',
            '调研人员': 'researchers', '接待方式': 'receive_method',
            '接待人员': 'receive_person', '接待地点': 'receive_location',
            '调研日期': 'research_date', '公告日期': 'announce_date',
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        if '_seq' in df.columns:
            df = df.drop(columns=['_seq'])
        
        df['date'] = pd.to_datetime(df['research_date'], errors='coerce').dt.strftime('%Y-%m-%d').fillna(datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d'))
        
        keep = ['date', 'code', 'name', 'price', 'change_pct', 'institution',
                'inst_type', 'researchers', 'receive_method', 'receive_person',
                'receive_location', 'research_date', 'announce_date']
        df = df[[c for c in keep if c in df.columns]]
        return df
    except Exception as e:
        logger.warning(f"获取机构调研数据失败 ({date_str}): {e}")
        return pd.DataFrame()


# ============================================================
#  大宗交易 (stock_block_trade)
# ============================================================

def fetch_block_trade_data(start_date: str, end_date: str) -> pd.DataFrame:
    """获取大宗交易数据。
    
    Args:
        start_date: 开始日期, 格式 YYYYMMDD
        end_date: 结束日期, 格式 YYYYMMDD
        
    Returns:
        DataFrame, 标准化列名
    """
    try:
        import akshare as ak
        df = ak.stock_dzjy_mrmx(symbol="A股", start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return pd.DataFrame()
        
        col_map = {
            '序号': '_seq',
            '交易日期': 'date',
            '证券代码': 'code', '证券简称': 'name',
            '涨跌幅': 'change_pct', '收盘价': 'close',
            '成交价': 'trade_price', '折溢率': 'premium_rate',
            '成交量': 'volume', '成交额': 'amount',
            '成交额/流通市值': 'amount_to_float_mv',
            '买方营业部': 'buyer_broker', '卖方营业部': 'seller_broker',
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        if '_seq' in df.columns:
            df = df.drop(columns=['_seq'])
        
        keep = ['date', 'code', 'name', 'change_pct', 'close', 'trade_price',
                'premium_rate', 'volume', 'amount', 'amount_to_float_mv',
                'buyer_broker', 'seller_broker']
        df = df[[c for c in keep if c in df.columns]]
        return df
    except Exception as e:
        logger.warning(f"获取大宗交易数据失败 ({start_date}~{end_date}): {e}")
        return pd.DataFrame()


# ============================================================
#  通用保存函数
# ============================================================

def save_market_events(conn: sqlite3.Connection, df: pd.DataFrame, table_name: str,
                       unique_columns: List[str]) -> int:
    """通用保存函数：INSERT OR REPLACE 写入市场事件表。
    
    Args:
        conn: 数据库连接
        df: 待写入 DataFrame
        table_name: 目标表名
        unique_columns: 用于判断重复的列名列表
        
    Returns:
        写入/更新行数
    """
    if df is None or df.empty:
        return 0
    
    # 清理NaN
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or str(x).strip() == '' else str(x))
        else:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
    
    count = 0
    cursor = conn.cursor()
    
    # 获取目标表的列名
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_cols = {row[1] for row in cursor.fetchall()}
    
    # 仅保留表中存在的列
    cols = [c for c in df.columns if c in table_cols]
    if not cols:
        return 0
    df = df[cols]
    
    for _, row in df.iterrows():
        values = {col: row[col] for col in cols}
        placeholders = ', '.join(['?' for _ in cols])
        col_names = ', '.join(cols)
        
        try:
            cursor.execute(
                f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})",
                list(values.values())
            )
            count += 1
        except Exception as e:
            logger.debug(f"写入 {table_name} 失败: {e}")
    
    conn.commit()
    return count


# ============================================================
#  每日采集入口
# ============================================================

def run_market_events_collection(target_date: Optional[str] = None) -> Dict[str, Any]:
    """执行全部市场事件数据采集。
    
    Args:
        target_date: 目标日期 (YYYY-MM-DD), 默认今天
        
    Returns:
        采集统计: {source_name: count, ...}
    """
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")
    
    date_param = target_date.replace("-", "")
    logger.info(f"[市场事件] 开始采集, 目标日期: {target_date}")
    
    stats = {
        "lhb": 0, "margin": 0, "holder_change": 0,
        "institution_research": 0, "block_trade": 0, "errors": []
    }
    
    conn = get_db_connection()
    try:
        # 1. 龙虎榜
        try:
            df = fetch_lhb_data(date_param)
            stats["lhb"] = save_market_events(conn, df, "stock_lhb", ["date", "code"])
            logger.info(f"  龙虎榜: {stats['lhb']} 条")
        except Exception as e:
            stats["errors"].append(f"龙虎榜: {e}")
            logger.warning(f"  龙虎榜失败(跳过): {e}")
        
        time.sleep(1)
        
        # 2. 融资融券
        try:
            df = fetch_margin_data(date_param)
            stats["margin"] = save_market_events(conn, df, "stock_margin", ["date", "code"])
            logger.info(f"  融资融券: {stats['margin']} 条")
        except Exception as e:
            stats["errors"].append(f"融资融券: {e}")
            logger.warning(f"  融资融券失败(跳过): {e}")
        
        time.sleep(1)
        
        # 3. 股东增减持
        try:
            df = fetch_holder_change_data(date_param)
            stats["holder_change"] = save_market_events(
                conn, df, "stock_holder_change", ["date", "holder_name"])
            logger.info(f"  股东增减持: {stats['holder_change']} 条")
        except Exception as e:
            stats["errors"].append(f"股东增减持: {e}")
            logger.warning(f"  股东增减持失败(跳过): {e}")
        
        time.sleep(1)
        
        # 4. 机构调研
        try:
            df = fetch_institution_research_data(date_param)
            stats["institution_research"] = save_market_events(
                conn, df, "stock_institution_research", ["date", "code", "institution"])
            logger.info(f"  机构调研: {stats['institution_research']} 条")
        except Exception as e:
            stats["errors"].append(f"机构调研: {e}")
            logger.warning(f"  机构调研失败(跳过): {e}")
        
        time.sleep(1)
        
        # 5. 大宗交易
        try:
            df = fetch_block_trade_data(date_param, date_param)
            stats["block_trade"] = save_market_events(
                conn, df, "stock_block_trade", ["date", "code", "buyer_broker", "seller_broker"])
            logger.info(f"  大宗交易: {stats['block_trade']} 条")
        except Exception as e:
            stats["errors"].append(f"大宗交易: {e}")
            logger.warning(f"  大宗交易失败(跳过): {e}")

        # 6. 数据源健康检查 - 检测并回填缺失交易日
        try:
            _check_and_backfill_stale(conn, target_date, stats)
        except Exception as e:
            logger.warning(f"数据源健康检查异常(不影响主流程): {e}")

    finally:
        conn.close()
    
    total = sum(v for k, v in stats.items() if k != "errors")
    logger.info(f"[市场事件] 采集完成, 合计 {total} 条")
    return stats


# ============================================================
#  历史回填
# ============================================================

def backfill_market_events(start_date: str, end_date: Optional[str] = None,
                           sources: Optional[List[str]] = None) -> Dict[str, int]:
    """批量回填历史市场事件数据。
    
    Args:
        start_date: 起始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD), 默认今天
        sources: 要回填的数据源列表, 默认全部
        
    Returns:
        {source_name: total_count}
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    all_sources = ["lhb", "margin", "holder_change", "institution_research", "block_trade"]
    if sources:
        all_sources = [s for s in sources if s in all_sources]
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    day = start
    
    totals = {s: 0 for s in all_sources}
    trading_days = 0
    
    conn = get_db_connection()
    try:
        while day <= end:
            date_str = day.strftime("%Y%m%d")
            date_display = day.strftime("%Y-%m-%d")
            weekday = day.weekday()
            
            # 跳过周末
            if weekday >= 5:
                day += timedelta(days=1)
                continue
            
            trading_days += 1
            logger.info(f"[回填] {date_display} (第{trading_days}个交易日)")
            
            if "lhb" in all_sources:
                try:
                    df = fetch_lhb_data(date_str)
                    n = save_market_events(conn, df, "stock_lhb", ["date", "code"])
                    totals["lhb"] += n
                except Exception as e:
                    logger.warning(f"  龙虎榜失败: {e}")
            
            time.sleep(1)
            
            if "margin" in all_sources:
                try:
                    df = fetch_margin_data(date_str)
                    n = save_market_events(conn, df, "stock_margin", ["date", "code"])
                    totals["margin"] += n
                except Exception as e:
                    logger.warning(f"  融资融券失败: {e}")
            
            time.sleep(1)
            
            if "holder_change" in all_sources:
                try:
                    df = fetch_holder_change_data(date_str)
                    n = save_market_events(conn, df, "stock_holder_change",
                                          ["date", "holder_name"])
                    totals["holder_change"] += n
                except Exception as e:
                    logger.warning(f"  股东增减持失败: {e}")
            
            time.sleep(1)
            
            if "institution_research" in all_sources:
                try:
                    df = fetch_institution_research_data(date_str)
                    n = save_market_events(conn, df, "stock_institution_research",
                                          ["date", "code", "institution"])
                    totals["institution_research"] += n
                except Exception as e:
                    logger.warning(f"  机构调研失败: {e}")
            
            time.sleep(1)
            
            if "block_trade" in all_sources:
                try:
                    df = fetch_block_trade_data(date_str, date_str)
                    n = save_market_events(conn, df, "stock_block_trade",
                                          ["date", "code", "buyer_broker", "seller_broker"])
                    totals["block_trade"] += n
                except Exception as e:
                    logger.warning(f"  大宗交易失败: {e}")
            
            time.sleep(0.5)
            day += timedelta(days=1)
    
    finally:
        conn.close()
    
    logger.info(f"[回填] 完成, 交易日 {trading_days} 天, 总计: {totals}")
    return totals


# ============================================================
#  数据源健康检查 + 自动回填
# ============================================================

# 数据源配置: (表名, fetch函数名, 去重列, 最大允许滞后天数)
SOURCE_CONFIG = [
    ("stock_margin", "fetch_margin_data", ["date", "code"], 2),
    ("stock_lhb", "fetch_lhb_data", ["date", "code"], 2),
    ("stock_holder_change", "fetch_holder_change_data", ["date", "holder_name"], 5),
    ("stock_institution_research", "fetch_institution_research_data",
     ["date", "code", "institution"], 2),
]

# 每日采集最大回填天数(防止无限回填)
MAX_BACKFILL_DAYS = 5


def _check_and_backfill_stale(conn: sqlite3.Connection, target_date: str,
                              stats: Dict[str, Any]) -> None:
    """检查数据源新鲜度，自动回填缺失的交易日数据。

    使用index_quotes表的交易日历判断哪些日期应有数据，
    对STALE超过阈值的数据源执行逐日回填(最多MAX_BACKFILL_DAYS天)。

    Args:
        conn: 数据库连接
        target_date: 当前采集日期 (YYYY-MM-DD)
        stats: 当前采集统计(用于追加回填数量)
    """
    cursor = conn.cursor()

    # 获取交易日历(用index_quotes判断)
    cursor.execute("""
        SELECT DISTINCT date FROM index_quotes
        WHERE date >= DATE(?, '-30 days') AND date <= ?
        ORDER BY date
    """, (target_date, target_date))
    trading_days = [r[0] for r in cursor.fetchall()]

    if not trading_days:
        return

    fetch_map = {
        "stock_margin": fetch_margin_data,
        "stock_lhb": fetch_lhb_data,
        "stock_holder_change": fetch_holder_change_data,
        "stock_institution_research": fetch_institution_research_data,
    }

    for table, fetch_name, unique_cols, max_stale in SOURCE_CONFIG:
        fetch_fn = fetch_map.get(table)
        if not fetch_fn:
            continue

        # 检查该表最新数据日期
        cursor.execute(f"SELECT MAX(date) FROM {table}")
        row = cursor.fetchone()
        latest = row[0] if row and row[0] else None

        if not latest:
            # 表完全为空，跳过(由backfill_market_events处理)
            continue

        # 计算滞后天数
        try:
            latest_dt = datetime.strptime(latest, "%Y-%m-%d")
            target_dt = datetime.strptime(target_date, "%Y-%m-%d")
            stale_days = (target_dt - latest_dt).days
        except (ValueError, TypeError):
            continue

        if stale_days <= max_stale:
            continue  # 新鲜度OK

        logger.info(f"[健康检查] {table}: 最新{latest}, 滞后{stale_days}天(阈值{max_stale}), 触发回填")

        # 找出缺失的交易日
        cursor.execute(f"SELECT DISTINCT date FROM {table} WHERE date >= DATE(?, '-30 days')", (target_date,))
        have_dates = set(r[0] for r in cursor.fetchall())
        missing = [d for d in trading_days if d not in have_dates]

        # 限制回填范围
        if len(missing) > MAX_BACKFILL_DAYS:
            missing = missing[-MAX_BACKFILL_DAYS:]
            logger.info(f"  {table}: 缺{len(missing)}天(仅回填最近{MAX_BACKFILL_DAYS}天): {missing}")

        # 逐日回填
        backfill_total = 0
        for d in missing:
            try:
                df = fetch_fn(d.replace("-", ""))
                n = save_market_events(conn, df, table, unique_cols)
                backfill_total += n
                if n > 0:
                    logger.info(f"  {table} {d}: 回填 {n} 条")
            except Exception as e:
                logger.debug(f"  {table} {d} 回填失败: {e}")
            time.sleep(0.5)

        if backfill_total > 0:
            stats[f"{table}_backfill"] = backfill_total
            logger.info(f"[健康检查] {table}: 回填完成, 共{backfill_total}条")
