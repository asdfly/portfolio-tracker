"""
资金流数据采集模块
使用 AKShare / 东方财富 datacenter-web 采集行业资金流、ETF资金流、主力资金数据。
"""

import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import logging
import os
import urllib.request
import json
import time
import requests as _requests

logger = logging.getLogger(__name__)

for _proxy_key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']:
    os.environ.pop(_proxy_key, None)

# 禁用 requests 代理。
# 使用 deepcopy 保存原始 __init__ 的完整副本，避免在共享 Python 进程中
# （如 akshare 可能也 patch Session）形成递归调用链。
import copy
_OriginalSessionInit = copy.deepcopy(_requests.Session.__init__)


def _determine_trading_date() -> str:
    """确定当前交易日日期
    
    在开盘前（9:30前）运行时，API返回的是前一交易日的数据，
    因此应使用前一交易日作为日期。
    """
    from datetime import datetime, timedelta
    now = datetime.now()
    current_time = now.hour * 100 + now.minute
    if current_time < 930:
        days_back = 3 if now.weekday() == 0 else 1
        trading_date = now - timedelta(days=days_back)
        return trading_date.strftime('%Y-%m-%d')
    return now.strftime('%Y-%m-%d')


def _stamp(df: pd.DataFrame, source: str, is_estimated: bool = False, confidence: float = 1.0) -> pd.DataFrame:
    """P1-A: 为资金流 DataFrame 打可信度标签。

    Args:
        source: 数据源标识 (em_push2his / em_spot / ths / ths_agg / ths_decomp / em_hsgt / kline_est)
        is_estimated: True=估算/反推(非交易所直采); False=真实采集
        confidence: 0~1 置信度, 真实源=1.0, 估算源按精度递减
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    df['source'] = source
    df['is_estimated'] = 1 if is_estimated else 0
    df['confidence'] = float(confidence)
    return df



def _NoProxySessionInit(self, *args, **kwargs):
    _OriginalSessionInit(self, *args, **kwargs)
    self.trust_env = False

_requests.Session.__init__ = _NoProxySessionInit

from src.utils.database import get_db_connection

def _urllib_get_json(url, retries=2, delay=1.0):
    proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy_handler)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = opener.open(req, timeout=15)
            return json.loads(resp.read().decode("utf-8"))
        except sqlite3.OperationalError as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.warning(f"urllib GET failed {url}: {e}")
                return None

def fetch_sector_fund_flow(date_str=None) -> pd.DataFrame:
    try:
        import akshare as ak
        df = ak.stock_fund_flow_industry(symbol='即时')
        if df is None or df.empty:
            return pd.DataFrame()
        col_map = {
            '序号': 'code', '行业': 'name', '行业-涨跌幅': 'change_pct',
            '净额': 'net_inflow', '流入资金': 'buy_amount', '流出资金': 'sell_amount',
        }
        df = df.rename(columns=col_map)
        # 同花顺原始单位为亿元，数据库约定统一为元 → ×1e8
        for _col in ['net_inflow', 'buy_amount', 'sell_amount']:
            if _col in df.columns:
                df[_col] = df[_col].apply(lambda v: float(v) * 1e8 if pd.notna(v) else None)
        df['category'] = 'sector'
        df['date'] = date_str or _determine_trading_date()
        keep_cols = ['date', 'code', 'name', 'change_pct', 'net_inflow', 'buy_amount', 'sell_amount', 'category']
        df = df[[c for c in keep_cols if c in df.columns]]
        return _stamp(df, 'ths', is_estimated=False, confidence=1.0)
    except ValueError as e:
        logger.warning(f"获取行业资金流失败: {e}")
    return pd.DataFrame()

def fetch_etf_fund_flow(code: str, name: str = '') -> pd.DataFrame:
    """获取单只ETF的资金流数据（东方财富push2his接口）。
    
    防御性设计：
    - akshare 内部使用 requests.get 调用 push2his API，当系统代理开启或
      东方财富封禁 IP 时，requests 可能抛出 ProxyError / ConnectionError，
      或返回的 JSON 中 data 字段为 None（导致 NoneType subscriptable）。
    - 本函数对所有异常路径做统一处理，仅输出 WARNING 级别日志，不中断主流程。
    """
    try:
        import akshare as ak
        market = "sh" if code.startswith('5') or code.startswith('15') or code.startswith('56') or code.startswith('58') else "sz"
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        # 空值检查：API 返回的 JSON 中 data 字段可能为 None
        if df is None:
            logger.debug(f"ETF {code} 资金流: API返回空数据(None)")
            return pd.DataFrame()
        if hasattr(df, 'empty') and df.empty:
            logger.debug(f"ETF {code} 资金流: API返回空DataFrame")
            return pd.DataFrame()
        # 正常数据
        df = df.rename(columns={
            '日期': 'date', '收盘价': 'close', '涨跌幅': 'change_pct',
            '主力净流入-净额': 'net_inflow', '主力净流入-净占比': 'net_inflow_pct',
            '超大单净流入-净额': 'super_large_inflow', '大单净流入-净额': 'large_inflow',
            '中单净流入-净额': 'medium_inflow', '小单净流入-净额': 'small_inflow',
        })
        df['code'] = code
        df['name'] = name
        df['category'] = 'etf'
        keep_cols = ['date', 'code', 'name', 'close', 'change_pct', 'net_inflow', 'net_inflow_pct', 'super_large_inflow', 'large_inflow', 'medium_inflow', 'small_inflow', 'category']
        df = df[[c for c in keep_cols if c in df.columns]]
        return _stamp(df, 'em_push2his', is_estimated=False, confidence=1.0)
    except (ConnectionError, OSError) as e:
        # 网络层错误：代理、封禁、连接中断等 —— 降级为 DEBUG 避免日志轰炸
        logger.debug(f"ETF {code} 资金流: 网络错误({type(e).__name__}), "
                      f"可能因代理或数据源封禁, 将由回填机制补充")
    except Exception as e:
        logger.warning(f"获取ETF {code} 资金流失败: {e}")
    return pd.DataFrame()

def fetch_main_fund_flow(days: int = 120, date_str: str = None) -> pd.DataFrame:
    """获取A股大盘主力资金净流入数据。
    方案A: push2his urllib 直连（完整数据，含超大单/大单/中单/小单细分）
    方案B: 同花顺行业资金流聚合（仅当日快照，无细分，作为 fallback）

    Args:
        days: 方案A 返回的历史天数上限
        date_str: 回填模式下的目标日期 YYYY-MM-DD。仅作用于方案B——
            方案A 由 push2his 返回真实历史日期序列，不可覆盖。
            为 None 时沿用 _determine_trading_date()，普通每日运行行为不变。
    """
    # --- 方案A: push2his ---
    try:
        url = (
            "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
            "?lmt=0&klt=101&secid=1.000001&secid2=0.399001"
            "&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
            f"&ut=b2884a393a59ad64002292a3e90d46a5&_={int(time.time()*1000)}"
        )
        data = _urllib_get_json(url)
        if data and data.get("data", {}).get("klines"):
            klines = data["data"]["klines"]
            df = pd.DataFrame([item.split(",") for item in klines])
            df.columns = [
                "date", "main_net", "small_net", "medium_net",
                "large_net", "super_large_net", "main_pct",
                "small_pct", "medium_pct", "large_pct",
                "super_large_pct", "_sh", "_shc", "_sz", "_szc",
            ]
            for col in df.columns[1:]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.rename(columns={
                'main_net': 'net_inflow', 'main_pct': 'net_inflow_pct',
                'super_large_net': 'super_large_inflow', 'super_large_pct': 'super_large_pct',
                'large_net': 'large_inflow', 'large_pct': 'large_pct',
                'medium_net': 'medium_inflow', 'medium_pct': 'medium_pct',
                'small_net': 'small_inflow', 'small_pct': 'small_pct',
            })
            df['code'] = 'main_fund'
            df['name'] = '主力资金'
            df['category'] = 'main_fund'
            keep = ['date','code','name','net_inflow','net_inflow_pct',
                     'super_large_inflow','super_large_pct','large_inflow','large_pct',
                     'medium_inflow','medium_pct','small_inflow','small_pct','category']
            df = df[[c for c in keep if c in df.columns]]
            df = df.dropna(subset=['net_inflow']).sort_values('date').tail(days).reset_index(drop=True)
            logger.info(f"主力资金(push2his): {len(df)} 条")
            return _stamp(df, 'em_push2his', is_estimated=False, confidence=1.0)
    except Exception as e:
        logger.debug(f"push2his 方案失败: {e}")

    # --- 方案B: 同花顺行业资金流聚合 ---
    logger.info("push2his 不可用, 回退到同花顺行业资金流聚合")
    try:
        import akshare as ak
        df = ak.stock_fund_flow_industry(symbol='即时')
    except _requests.RequestException as e:
        logger.warning(f"同花顺也失败: {e}")
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    # 同花顺原始单位为亿元，数据库约定统一为元 -> x1e8
    tb = pd.to_numeric(df.get('流入资金',0), errors='coerce').sum() * 1e8
    ts = pd.to_numeric(df.get('流出资金',0), errors='coerce').sum() * 1e8
    tn = pd.to_numeric(df.get('净额',0), errors='coerce').sum() * 1e8
    np_ = round(tn/tb*100,2) if tb > 0 else 0.0
    today = date_str or _determine_trading_date()
    result = pd.DataFrame([{
        'date': today, 'code': 'main_fund', 'name': '主力资金',
        'net_inflow': round(tn,2), 'net_inflow_pct': np_,
        'buy_amount': round(tb,2), 'sell_amount': round(ts,2),
        'super_large_inflow': 0.0, 'super_large_pct': 0.0,
        'large_inflow': 0.0, 'large_pct': 0.0,
        'medium_inflow': 0.0, 'medium_pct': 0.0,
        'small_inflow': round(-tn,2), 'small_pct': 0.0,
        'category': 'main_fund',
    }])
    logger.info(f"主力资金(同花顺聚合): date={today}, 净流入={tn/1e8:.2f}亿 -> {tn:.0f}元")
    return _stamp(result, 'ths_agg', is_estimated=True, confidence=0.4)


def fetch_north_flow(days: int = 60) -> pd.DataFrame:
    """获取北向资金（2024-08-19起停更，保留以备恢复）。"""
    all_dfs = []
    for symbol in ["沪股通", "深股通"]:
        try:
            import akshare as ak
            df = ak.stock_hsgt_hist_em(symbol=symbol)
            if df is not None and not df.empty:
                df = df.rename(columns={'日期': 'date', '当日成交净买额': 'net_inflow', '买入成交额': 'buy_amount', '卖出成交额': 'sell_amount'})
                df['code'] = 'north'; df['name'] = '北向资金'; df['category'] = 'north'
                keep_cols = ['date', 'code', 'name', 'net_inflow', 'buy_amount', 'sell_amount', 'category']
                df = df[[c for c in keep_cols if c in df.columns]]
                all_dfs.append(df)
        except _requests.RequestException as e:
            logger.warning(f"获取{symbol}数据失败: {e}")
    if not all_dfs:
        return pd.DataFrame()
    combined = pd.concat(all_dfs, ignore_index=True)
    agg = combined.groupby('date').agg({'code': 'first', 'name': 'first', 'net_inflow': 'sum', 'buy_amount': 'sum', 'sell_amount': 'sum', 'category': 'first'}).reset_index()
    agg = agg.dropna(subset=['net_inflow', 'buy_amount', 'sell_amount'], how='all')
    agg = agg.sort_values('date').tail(days).reset_index(drop=True)
    return _stamp(agg, 'em_hsgt', is_estimated=False, confidence=1.0)

def backfill_etf_fund_flow_from_kline(
    conn, etf_map, target_days=120, estimate_ratio=0.15,
) -> dict:
    """ETF资金流历史回填。数据源: 腾讯日K线 > 新浪日K线。"""
    import urllib.request
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    from datetime import datetime, timedelta
    stats = {}
    for code, name in etf_map.items():
        try:
            existing_dates = set(
                r[0] for r in conn.execute(
                    "SELECT DISTINCT date FROM fund_flows WHERE code=? AND category='etf'", (code,)
                ).fetchall()
            )
            if len(existing_dates) >= target_days:
                stats[code] = 0
                continue
            market = "sz" if code.startswith("1") else "sh"
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=target_days + 60)
            rows, source = [], ""
            # 数据源1: 腾讯日K线
            try:
                url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                       f"?param={market}{code},day,{start_dt.strftime('%Y-%m-%d')},"
                       f"{end_dt.strftime('%Y-%m-%d')},200,qfq")
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                resp = opener.open(req, timeout=15)
                data = json.loads(resp.read().decode("utf-8"))
                klines = data.get("data", {}).get(f"{market}{code}", {}).get("qfqday", [])
                if klines:
                    source = "tencent"
                    for k in reversed(klines):
                        dv = str(k[0])
                        if dv in existing_dates:
                            continue
                        op_, cp_ = float(k[1]), float(k[2])
                        vol = float(k[5])
                        to_ = (op_ + cp_) / 2 * vol * 100
                        chg = (cp_ - op_) / op_ * 100 if op_ > 0 else 0.0
                        ni = to_ * chg * estimate_ratio / 100
                        rows.append({'date': dv, 'code': code, 'name': name,
                                     'net_inflow': round(ni, 2),
                                     'buy_amount': round(to_ / 2, 2),
                                     'sell_amount': round(to_ / 2, 2),
                                     'category': 'etf'})
            except (ValueError, KeyError, IndexError):  # 基金数据解析异常，跳过该条
                pass
            # 数据源2: 新浪日K线(fallback)
            if not rows:
                try:
                    url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                           f"CN_MarketData.getKLineData?symbol={market}{code}"
                           f"&scale=240&ma=no&datalen=200")
                    req = urllib.request.Request(url, headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://finance.sina.com.cn"})
                    resp = opener.open(req, timeout=15)
                    klines = json.loads(resp.read().decode("utf-8"))
                    if klines:
                        source = "sina"
                        for k in klines:
                            dv = str(k["day"])
                            if dv in existing_dates:
                                continue
                            op_, cp_ = float(k["open"]), float(k["close"])
                            vol = float(k["volume"])
                            to_ = cp_ * vol
                            chg = (cp_ - op_) / op_ * 100 if op_ > 0 else 0.0
                            ni = to_ * chg * estimate_ratio / 100
                            rows.append({'date': dv, 'code': code, 'name': name,
                                         'net_inflow': round(ni, 2),
                                         'buy_amount': round(to_ / 2, 2),
                                         'sell_amount': round(to_ / 2, 2),
                                         'category': 'etf'})
                except (ValueError, KeyError, IndexError):  # 新浪日K线数据解析异常，跳过
                    pass
            if rows:
                _df = _stamp(pd.DataFrame(rows), 'kline_est', is_estimated=True, confidence=0.3)
                n = save_fund_flows(conn, _df)
                stats[code] = n
                logger.info(f"  ETF回填 {code} {name}: {n}天({source})")
            else:
                stats[code] = 0
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            logger.warning(f"  ETF回填 {code} 失败: {e}")
            stats[code] = -1
    return stats



def estimate_etf_fund_flow_one(code: str, date_str: str) -> dict:
    """从腾讯/新浪日K线估算单只ETF当日资金流(降级用, 精度有限)。

    降级链路: 与 ETF spot 同理 —— EM(fund_etf_spot_em) 缺漏的持仓ETF
    (如债券ETF 511380) 无法取得真实主力净流入, 退而用当日成交额×涨跌幅
    ×经验系数(0.15)估算净额。仅用于"至少有点数据"的降级, 不替代真实源。

    Returns:
        {'date','code','name','net_inflow','buy_amount','sell_amount','category'}
        或 None(双源均失败)
    """
    import urllib.request
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        market = "sz" if code.startswith("1") else "sh"
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=10)
        est_ratio = 0.15

        # 数据源1: 腾讯日K线
        try:
            url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                   f"?param={market}{code},day,{start_dt.strftime('%Y-%m-%d')},"
                   f"{end_dt.strftime('%Y-%m-%d')},20,qfq")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = opener.open(req, timeout=15)
            data = json.loads(resp.read().decode("utf-8"))
            klines = data.get("data", {}).get(f"{market}{code}", {}).get("qfqday", [])
            if klines:
                k = klines[-1]
                op_, cp_ = float(k[1]), float(k[2])
                vol = float(k[5])
                to_ = (op_ + cp_) / 2 * vol * 100
                chg = (cp_ - op_) / op_ * 100 if op_ > 0 else 0.0
                ni = to_ * chg * est_ratio / 100
                return {
                    'date': date_str, 'code': code, 'name': '',
                    'net_inflow': round(ni, 2),
                    'buy_amount': round(to_ / 2, 2),
                    'sell_amount': round(to_ / 2, 2),
                    'category': 'etf',
                    'source': 'kline_est', 'is_estimated': 1, 'confidence': 0.3,
                }
        except (ValueError, KeyError, IndexError, OSError, json.JSONDecodeError):
            pass

        # 数据源2: 新浪日K线
        try:
            url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                   f"CN_MarketData.getKLineData?symbol={market}{code}"
                   f"&scale=240&ma=no&datalen=20")
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.sina.com.cn"})
            resp = opener.open(req, timeout=15)
            klines = json.loads(resp.read().decode("utf-8"))
            if klines:
                k = klines[-1]
                op_, cp_ = float(k["open"]), float(k["close"])
                vol = float(k["volume"])
                to_ = cp_ * vol
                chg = (cp_ - op_) / op_ * 100 if op_ > 0 else 0.0
                ni = to_ * chg * est_ratio / 100
                return {
                    'date': date_str, 'code': code, 'name': '',
                    'net_inflow': round(ni, 2),
                    'buy_amount': round(to_ / 2, 2),
                    'sell_amount': round(to_ / 2, 2),
                    'category': 'etf',
                    'source': 'kline_est', 'is_estimated': 1, 'confidence': 0.3,
                }
        except (ValueError, KeyError, IndexError, OSError, json.JSONDecodeError):
            pass
    except Exception as e:
        logger.debug(f"ETF {code} 资金流K线估算失败: {e}")
    return None


def _ensure_fund_flows_columns(conn: sqlite3.Connection):
    """P1-A: 存量库迁移——补齐 source/is_estimated/confidence 三列（幂等）。

    新库由 db_schema.py 的 CREATE TABLE 直接建好；旧库需 ALTER 补齐。
    列已存在时 ALTER 抛 OperationalError，忽略即可。
    """
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(fund_flows)").fetchall()}
    except sqlite3.OperationalError:
        return
    for col, ddl in [("source", "TEXT"), ("is_estimated", "BOOLEAN DEFAULT 0"), ("confidence", "REAL")]:
        if col not in cols:
            try:
                conn.execute(f"ALTER TABLE fund_flows ADD COLUMN {col} {ddl}")
            except sqlite3.OperationalError:
                pass


def save_fund_flows(conn: sqlite3.Connection, df: pd.DataFrame):
    """保存资金流数据到数据库（upsert），支持扩展列。

    P1-A: 每条记录可携带 source/is_estimated/confidence 可信度标签。
    缺省约定：is_estimated=0（真实），confidence=1.0（完全可信）。
    """
    if df.empty:
        return 0
    for col in ['date', 'category']:
        if col not in df.columns:
            return 0
    _ensure_fund_flows_columns(conn)
    extra_cols = ['net_inflow_pct', 'super_large_inflow', 'super_large_pct',
                  'large_inflow', 'large_pct', 'medium_inflow', 'medium_pct', 'small_inflow', 'small_pct']
    # P1-A 可信度标签列
    meta_cols = ['source', 'is_estimated', 'confidence']
    cursor = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        date_val = str(row.get('date', ''))
        code_val = str(row.get('code', ''))
        cat_val = str(row.get('category', ''))
        def _float(v):
            return float(v) if pd.notna(v) else None
        cursor.execute("SELECT id FROM fund_flows WHERE date = ? AND code = ? AND category = ?", (date_val, code_val, cat_val))
        existing = cursor.fetchone()
        base_vals = (str(row.get('name', '')), _float(row.get('net_inflow')), _float(row.get('buy_amount')), _float(row.get('sell_amount')))
        if existing:
            set_parts = ["name=?", "net_inflow=?", "buy_amount=?", "sell_amount=?"]
            vals = list(base_vals)
            for ec in extra_cols:
                if ec in row.index:
                    set_parts.append(f"{ec}=?")
                    vals.append(_float(row.get(ec)))
            # UPDATE 仅在有显式标签时覆盖，避免清空既有可信度标记
            for mc in meta_cols:
                if mc in row.index and pd.notna(row.get(mc)):
                    set_parts.append(f"{mc}=?")
                    v = row.get(mc)
                    vals.append(int(v) if mc == 'is_estimated' else (float(v) if mc == 'confidence' else str(v)))
            vals.append(existing[0])
            cursor.execute(f"UPDATE fund_flows SET {', '.join(set_parts)} WHERE id=?", vals)
        else:
            ins_cols = ['date', 'code', 'name', 'net_inflow', 'buy_amount', 'sell_amount', 'category']
            ins_vals = [date_val, code_val, str(row.get('name', '')), _float(row.get('net_inflow')), _float(row.get('buy_amount')), _float(row.get('sell_amount')), cat_val]
            placeholders = '?,?,?,?,?,?,?'
            for ec in extra_cols:
                if ec in row.index:
                    ins_cols.append(ec)
                    ins_vals.append(_float(row.get(ec)))
                    placeholders += ',?'
            # INSERT 始终写入可信度标签（缺省=真实源: source空/is_estimated=0/confidence=1.0）
            src = str(row.get('source')) if ('source' in row.index and pd.notna(row.get('source'))) else ''
            est = int(row.get('is_estimated')) if ('is_estimated' in row.index and pd.notna(row.get('is_estimated'))) else 0
            conf = float(row.get('confidence')) if ('confidence' in row.index and pd.notna(row.get('confidence'))) else 1.0
            for mc, mv in [('source', src), ('is_estimated', est), ('confidence', conf)]:
                ins_cols.append(mc)
                ins_vals.append(mv)
                placeholders += ',?'
            cursor.execute(f"INSERT INTO fund_flows ({', '.join(ins_cols)}) VALUES ({placeholders})", ins_vals)
        count += 1
    conn.commit()
    return count



def backfill_sector_fund_flow(conn, trading_days=None, apply_market_trend_scaling=False):
    """行业资金流历史回填，利用同花顺多周期排行(3/5/10/20日)差值分解估算每日数据。

    Args:
        conn: SQLite 连接
        trading_days: 可选交易日窗口(date 对象列表)；为 None 时取最近 25 个 ETF 交易日
        apply_market_trend_scaling: 可选增强。为 True 时，在主分解(最近25日)之外，
            基于 ETF 资金流日度趋势对更早的缺失日期做等比缩放估算
            （逻辑源自 scripts/backfill/backfill_sector_enhanced.py:backfill_earlier_days，
             已抽离为 _sector_market_trend_scaling 挂到主线，消除双实现分歧）。
    """
    import akshare as ak
    from datetime import datetime

    try:
        # 获取各周期排行数据
        all_data = {}
        for sym in ['即时', '3日排行', '5日排行', '10日排行', '20日排行']:
            df = ak.stock_fund_flow_industry(symbol=sym)
            if df is not None and not df.empty:
                df = df.rename(columns={'行业': 'name', '净额': 'net'})
                df['net'] = pd.to_numeric(df['net'], errors='coerce')
                all_data[sym] = df[['name', 'net']].copy()
                logger.info(f"  sector backfill: {sym} {len(df)}个行业")
            else:
                logger.warning(f"  sector backfill: {sym} 为空")
                return 0

        # 合并
        m = all_data['即时'].copy()
        m.columns = ['name', 'n1']
        for sym, col in [('3日排行', 'n3'), ('5日排行', 'n5'), ('10日排行', 'n10'), ('20日排行', 'n20')]:
            if sym in all_data:
                tmp = all_data[sym].copy()
                tmp.columns = ['name', col]
                m = m.merge(tmp, on='name')

        # 获取code映射
        raw = ak.stock_fund_flow_industry(symbol='即时')
        code_map = dict(zip(raw['行业'].values, raw['序号'].values))

        # 构建交易日历(基于ETF数据的真实交易日)
        if trading_days is None:
            cursor2 = conn.cursor()
            etf_dates_raw = cursor2.execute(
                "SELECT DISTINCT date FROM fund_flows WHERE category='etf' "
                "ORDER BY date DESC LIMIT 25").fetchall()
            trading_days = [datetime.strptime(r[0], '%Y-%m-%d').date() for r in etf_dates_raw]
            trading_days.sort()

        # 分解各天
        records = []
        for _, row in m.iterrows():
            name = row['name']
            code = str(code_map.get(name, ''))
            n1 = float(row.get('n1', 0) or 0)
            n3 = float(row.get('n3', 0) or 0)
            n5 = float(row.get('n5', 0) or 0)
            n10 = float(row.get('n10', 0) or 0)
            n20 = float(row.get('n20', 0) or 0)

            def _add(date_idx, net_yi, est=True):
                if abs(date_idx) > len(trading_days):
                    return
                dt = str(trading_days[date_idx])
                net = net_yi * 1e8
                buy = abs(net) * 0.525 + max(net, 0)
                sell = buy - net
                records.append({
                    'date': dt, 'code': code, 'name': name,
                    'net_inflow': net, 'buy_amount': buy, 'sell_amount': sell,
                    'category': 'sector',
                })

            if len(trading_days) >= 1:
                _add(-1, n1, False)
            if len(trading_days) >= 2:
                _add(-2, n3 - n1, True)            # 昨天(精确)
            if len(trading_days) >= 4:
                _add(-3, (n5 - n3) / 2, True)      # 均分
                _add(-4, (n5 - n3) / 2, True)
            if len(trading_days) >= 9:
                for i in range(-9, -4):
                    _add(i, (n10 - n5) / 5, True)  # 前5天日均
            if len(trading_days) >= 19:
                for i in range(-19, -9):
                    _add(i, (n20 - n10) / 10, True) # 前10天日均

        df_new = pd.DataFrame(records, columns=['date', 'code', 'name',
                                                'net_inflow', 'buy_amount', 'sell_amount',
                                                'category'])

        # 跳过已有日期
        cursor = conn.cursor()
        existing = {r[0] for r in cursor.execute(
            "SELECT DISTINCT date FROM fund_flows WHERE category='sector'").fetchall()}
        df_new = df_new[~df_new['date'].isin(existing)]

        # 可选增强：市场趋势缩放（填充主分解未覆盖的更早日期）
        if apply_market_trend_scaling:
            main_dates = set(df_new['date'])
            cursor2 = conn.cursor()
            ext_raw = cursor2.execute(
                "SELECT DISTINCT date FROM fund_flows WHERE category='etf' "
                "ORDER BY date DESC LIMIT 150").fetchall()
            ext_days = [r[0] for r in ext_raw]
            covered = existing | main_dates
            scaled = _sector_market_trend_scaling(conn, ext_days, covered)
            if scaled:
                df_scaled = pd.DataFrame(scaled)
                df_scaled = df_scaled[~df_scaled['date'].isin(main_dates)]
                df_new = df_scaled if df_new.empty else pd.concat([df_new, df_scaled], ignore_index=True)

        if df_new.empty:
            logger.info(f"  sector backfill: 无新数据(已有{len(existing)}天)")
            return 0

        count = save_fund_flows(conn, _stamp(df_new, 'ths_decomp', is_estimated=True, confidence=0.35))
        logger.info(f"  sector backfill: {count}条, {df_new['date'].nunique()}天, "
                     f"{df_new['name'].nunique()}个行业"
                     + ("，含市场趋势缩放增强" if apply_market_trend_scaling else ""))
        return count

    except sqlite3.OperationalError as e:
        logger.error(f"行业资金流回填失败: {e}")
        return 0


def _sector_market_trend_scaling(conn, trading_days, existing_dates):
    """可选增强：基于 ETF 资金流日度趋势，对主分解未覆盖的更早日期做等比缩放估算。

    逻辑源自 scripts/backfill/backfill_sector_enhanced.py:backfill_earlier_days，
    抽离为内部函数挂到主线 backfill_sector_fund_flow（apply_market_trend_scaling=True 时调用），
    消除 sector 资金流回填的双实现分歧。
    trading_days / existing_dates 均为字符串(YYYY-MM-DD)。
    """
    import numpy as np
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, AVG(net_inflow) as avg_net
        FROM fund_flows
        WHERE category='sector' AND net_inflow IS NOT NULL
        GROUP BY name
    """)
    industry_avg = {r[0]: r[1] for r in cursor.fetchall()}
    if not industry_avg:
        return []

    if existing_dates:
        emin, emax = min(existing_dates), max(existing_dates)
        cursor.execute("""
            SELECT date, SUM(COALESCE(net_inflow, 0)) as total_net
            FROM fund_flows
            WHERE category='etf' AND date < ?
            GROUP BY date ORDER BY date
        """, (emin,))
        etf_trend = {r[0]: r[1] for r in cursor.fetchall()}
        cursor.execute("""
            SELECT date, SUM(COALESCE(net_inflow, 0)) as total_net
            FROM fund_flows
            WHERE category='etf' AND date >= ? AND date <= ?
            GROUP BY date ORDER BY date
        """, (emin, emax))
        recent_etf = [r[1] for r in cursor.fetchall()]
        recent_avg = float(np.mean(recent_etf)) if recent_etf else 1.0
    else:
        etf_trend = {}
        recent_avg = 1.0

    covered = set(existing_dates)
    records = []
    for dt in sorted(trading_days):
        if dt in covered:
            continue
        scale = 1.0
        if dt in etf_trend and recent_avg:
            scale = etf_trend[dt] / recent_avg
            scale = max(0.3, min(scale, 3.0))  # 限制缩放范围，避免极端值
        np.random.seed(hash(dt) % (2**31))
        noise = np.random.uniform(0.85, 1.15)  # ±15% 扰动，避免所有日期完全相同
        for name, avg_net in industry_avg.items():
            net = avg_net * scale * noise
            buy = abs(net) * 0.525 + max(net, 0)
            sell = buy - net
            cursor.execute("SELECT code FROM fund_flows WHERE category='sector' AND name=? LIMIT 1", (name,))
            code_row = cursor.fetchone()
            code = code_row[0] if code_row else ''
            records.append({
                'date': dt, 'code': code, 'name': name,
                'net_inflow': net, 'buy_amount': buy, 'sell_amount': sell,
                'category': 'sector',
            })
    return records

def check_push2his_available(timeout=5) -> bool:
    """快速探测 push2his API 是否可用（urllib 直连，绕过系统代理）。
    Returns True if push2his returns valid data.
    """
    try:
        url = (
            "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
            "?lmt=1&klt=101&secid=1.000001&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55"
            f"&ut=b2884a393a59ad64002292a3e90d46a5&_={int(time.time()*1000)}"
        )
        data = _urllib_get_json(url)
        return bool(data and data.get("data", {}).get("klines"))
    except (ConnectionError, OSError, ValueError, KeyError, TypeError) as e:
        logger.debug(f"Push2his check error: {e}")
        return False


def fetch_etf_fund_flow_batch(etf_codes: list, date_str: str = None,
                              etf_map: dict = None) -> pd.DataFrame:
    """基于 fund_etf_spot_em 批量获取ETF当日资金流数据。

    优势：
    - 单次请求获取全市场ETF数据（~1400只），无需逐只调用 push2his
    - 返回完整字段：主力净流入/净占比、超大单、大单、中单、小单
    - 数据源为东方财富 datacenter-web（与 push2his 不同端点），不受 push2his 封禁影响

    Args:
        etf_codes: 需要筛选的ETF代码列表
        date_str: 回填模式下的目标日期 YYYY-MM-DD。为 None 时沿用
            _determine_trading_date()，普通每日运行行为不变。
        etf_map: 可选 {code: name} 映射, 供降级行补齐名称。

    Returns:
        DataFrame with columns: [date, code, name, close, change_pct, net_inflow,
                                 net_inflow_pct, super_large_inflow, super_large_pct,
                                 large_inflow, large_pct, medium_inflow, medium_pct,
                                 small_inflow, small_pct, category]
    """
    today_str = date_str or _determine_trading_date()
    result = pd.DataFrame()
    try:
        import akshare as ak
        df = ak.fund_etf_spot_em()
        if df is not None and not df.empty:
            df['代码'] = df['代码'].astype(str)
            matched = df[df['代码'].isin(etf_codes)]
            if not matched.empty:
                rows = []
                for _, row in matched.iterrows():
                    rows.append({
                        'date': today_str,
                        'code': str(row['代码']),
                        'name': str(row.get('名称', '')),
                        'close': float(row.get('最新价', 0) or 0),
                        'change_pct': float(row.get('涨跌幅', 0) or 0),
                        'net_inflow': float(row.get('主力净流入-净额', 0) or 0),
                        'net_inflow_pct': float(row.get('主力净流入-净占比', 0) or 0),
                        'super_large_inflow': float(row.get('超大单净流入-净额', 0) or 0),
                        'super_large_pct': float(row.get('超大单净流入-净占比', 0) or 0),
                        'large_inflow': float(row.get('大单净流入-净额', 0) or 0),
                        'large_pct': float(row.get('大单净流入-净占比', 0) or 0),
                        'medium_inflow': float(row.get('中单净流入-净额', 0) or 0),
                        'medium_pct': float(row.get('中单净流入-净占比', 0) or 0),
                        'small_inflow': float(row.get('小单净流入-净额', 0) or 0),
                        'small_pct': float(row.get('小单净流入-净占比', 0) or 0),
                        'category': 'etf',
                    })
                result = _stamp(pd.DataFrame(rows), 'em_spot', is_estimated=False, confidence=1.0)
                logger.info(f"ETF资金流(批量): {len(result)} 只ETF, {today_str}")
    except ValueError as e:
        logger.warning(f"ETF批量资金流获取失败: {e}")

    # ---- P3 降级: EM 缺漏的持仓ETF(如债券ETF 511380)走K线估算 ----
    present = set(result['code'].tolist()) if not result.empty else set()
    missing = [c for c in etf_codes if c not in present]
    if missing:
        est_rows = []
        for code in missing:
            est = estimate_etf_fund_flow_one(code, today_str)
            if est:
                if etf_map and code in etf_map:
                    est['name'] = etf_map[code]
                est_rows.append(est)
        if est_rows:
            est_df = _stamp(pd.DataFrame(est_rows), 'kline_est', is_estimated=True, confidence=0.3)
            result = pd.concat([result, est_df], ignore_index=True) if not result.empty else est_df
            est_codes = [r['code'] for r in est_rows]
            logger.warning(f"[P3降级] ETF资金流K线估算补齐 {len(est_rows)} 只: {est_codes}")

    return result
