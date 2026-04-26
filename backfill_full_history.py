#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
完整历史数据回填脚本 - 补充23只ETF从上市以来的全部历史数据
以及11个基准指数的完整历史数据

数据源: 新浪财经K线接口 (https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData)
备选: 腾讯财经K线接口 (备用，新浪失败时自动切换)

数据用途:
  - portfolio_snapshots: ETF历史净值走势（用于夏普比率、最大回撤、波动率计算）
  - etf_technical: 技术指标历史（用于30日涨跌幅、布林带位置等）
  - index_quotes: 指数历史行情（用于基准对比、行业轮动分析）
  - portfolio_summary: 组合汇总历史（用于净值曲线图表）

使用方法:
    python backfill_full_history.py                # 回填全部历史
    python backfill_full_history.py --dry-run      # 试运行
    python backfill_full_history.py --etf-only     # 仅回填ETF
    python backfill_full_history.py --index-only   # 仅回填指数
"""

import os
import sys
import argparse
import logging
import time
import json
import requests
from datetime import datetime, date

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import INDEX_CODES, DATABASE_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_full")

# 禁用系统代理
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'
for k in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    os.environ[k] = ''

# 新浪K线接口
SINA_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"

# 创建不使用代理的requests session
_session = requests.Session()
_session.trust_env = False


def _to_sina_code(code: str) -> str:
    """转换为新浪代码格式"""
    code = str(code).strip().lower()
    if code.startswith('sh') or code.startswith('sz'):
        return code
    # 上海市场: 5xx, 56x, 58x, 6xx
    if code.startswith('51') or code.startswith('56') or code.startswith('58') or code.startswith('6'):
        return f'sh{code}'
    else:
        return f'sz{code}'


def fetch_klines_sina(code: str, max_datalen: int = 10000) -> list:
    """通过新浪K线接口获取完整历史数据（自动分页获取直到无新数据）"""
    sina_code = _to_sina_code(code)
    all_klines = []
    datalen = max_datalen  # 首次请求大数量

    while True:
        params = {
            'symbol': sina_code,
            'scale': 240,
            'ma': 'no',
            'datalen': datalen
        }
        try:
            resp = _session.get(
                SINA_KLINE_URL,
                params=params,
                timeout=30,
                headers={'Referer': 'https://finance.sina.com.cn'}
            )
            resp.encoding = 'utf-8'
            data = json.loads(resp.text)

            if not isinstance(data, list) or len(data) == 0:
                break

            all_klines.extend(data)

            # 如果返回量小于请求量，说明已取完
            if len(data) < datalen:
                break

            # 已获取足够多，停止
            if len(all_klines) >= max_datalen:
                break

            # 已获取到足够早的日期（超过25年数据），停止
            earliest = data[0].get('day', '')
            if earliest and len(earliest) >= 10:
                try:
                    earliest_year = int(earliest[:4])
                    if earliest_year < 2000:
                        break
                except ValueError:
                    pass

            # 递减请求量继续获取更早数据
            datalen = min(datalen, 5000)
            time.sleep(0.3)

        except Exception as e:
            logger.error(f"  新浪K线获取异常 {code}: {e}")
            break

    # 去重并按日期排序
    seen = set()
    unique = []
    for item in reversed(all_klines):  # 逆序去重保留最新
        d = item.get('day', '')
        if d not in seen:
            seen.add(d)
            unique.append(item)
    unique.reverse()  # 正序排列（从早到晚）

    return unique


def fetch_klines_tencent(code: str) -> list:
    """通过腾讯K线接口获取数据（新浪的备用方案）"""
    sina_code = _to_sina_code(code)
    # 腾讯K线接口
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    # 腾讯市场代码: sh -> 1, sz -> 0
    market = '1' if sina_code.startswith('sh') else '0'
    symbol = sina_code[2:]  # 去掉sh/sz前缀

    all_klines = []
    for page in range(100):  # 最多100页
        params = {
            '_var': 'kline_dayqfq',
            'param': f'{market},{symbol},day,,,,{page},10000,qfq'
        }
        try:
            resp = _session.get(url, params=params, timeout=15)
            # 去除JSONP前缀
            text = resp.text.strip()
            if text.startswith('kline_dayqfq='):
                text = text[len('kline_dayqfq='):]

            data = json.loads(text)
            day_data = data.get('data', {}).get(market + symbol, {}).get('day', [])
            if not day_data:
                break

            for item in day_data:
                parts = item.split(' ')
                if len(parts) >= 6:
                    all_klines.append({
                        'day': parts[0],
                        'open': parts[1],
                        'close': parts[2],
                        'high': parts[3],
                        'low': parts[4],
                        'volume': parts[5]
                    })

            if len(day_data) < 800:
                break
            time.sleep(0.2)

        except Exception as e:
            logger.error(f"  腾讯K线获取异常 {code}: {e}")
            break

    return all_klines


def fetch_full_klines(code: str, name: str = "") -> list:
    """获取完整历史K线（新浪优先，失败自动切换腾讯）"""
    # 先尝试新浪
    klines = fetch_klines_sina(code, max_datalen=10000)
    if klines:
        return klines

    logger.warning(f"  新浪接口无数据 {code} {name}, 切换腾讯...")
    time.sleep(0.5)
    klines = fetch_klines_tencent(code)
    if klines:
        logger.info(f"  腾讯接口获取成功 {code} {name}: {len(klines)} 条")
        return klines

    return []


def get_etf_positions(db_path):
    """从数据库获取持仓ETF列表（最新快照）"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("""
        SELECT code, name, quantity, cost_price
        FROM portfolio_snapshots
        WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
        ORDER BY code
    """)
    rows = cur.fetchall()
    conn.close()
    return [{"code": r[0], "name": r[1], "quantity": r[2], "cost_price": r[3]} for r in rows]


def get_existing_dates(db_path, table, code=None):
    """获取数据库中已有的日期集合"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    if code:
        cur.execute(f"SELECT DISTINCT date FROM {table} WHERE code = ?", (code,))
    else:
        cur.execute(f"SELECT DISTINCT date FROM {table}")
    dates = {row[0] for row in cur.fetchall()}
    conn.close()
    return dates


def save_etf_snapshots(db_path, code, name, quantity, cost_price, klines, existing_dates):
    """批量保存ETF历史快照，返回新增条数"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    new_count = 0
    today_str = date.today().strftime("%Y-%m-%d")

    for idx, k in enumerate(klines):
        kdate = k.get("day", "")
        if not kdate:
            continue
        if kdate in existing_dates:
            continue
        if kdate >= today_str:
            continue

        try:
            close = float(k.get("close", 0))
            open_p = float(k.get("open", 0))
            high = float(k.get("high", 0))
            low = float(k.get("low", 0))
            volume = float(k.get("volume", 0))
        except (ValueError, TypeError):
            continue

        if close <= 0:
            continue

        mv = close * quantity if quantity else 0
        pnl = mv - (cost_price * quantity) if (cost_price and quantity) else 0
        pnl_rate = (pnl / (cost_price * quantity) * 100) if (cost_price and quantity and cost_price > 0) else 0

        # 计算日涨跌幅
        change_pct = 0
        if idx > 0:
            prev_close_str = klines[idx - 1].get("close", "0")
            try:
                prev_close = float(prev_close_str)
                if prev_close > 0:
                    change_pct = (close - prev_close) / prev_close * 100
            except (ValueError, TypeError):
                pass

        cur.execute("""
            INSERT OR REPLACE INTO portfolio_snapshots
            (date, code, name, quantity, cost_price, current_price,
             market_value, pnl, pnl_rate, ytd_return, beta)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
        """, (kdate, code, name, quantity, cost_price, close, mv, pnl, pnl_rate))
        new_count += 1

    conn.commit()
    conn.close()
    return new_count


def save_index_quotes(db_path, code, name, klines, existing_dates):
    """批量保存指数历史行情"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    new_count = 0
    today_str = date.today().strftime("%Y-%m-%d")

    for idx, k in enumerate(klines):
        kdate = k.get("day", "")
        if not kdate:
            continue
        if kdate in existing_dates:
            continue
        if kdate >= today_str:
            continue

        try:
            close = float(k.get("close", 0))
            volume = float(k.get("volume", 0))
        except (ValueError, TypeError):
            continue

        change_pct = 0
        if idx > 0:
            prev_close_str = klines[idx - 1].get("close", "0")
            try:
                prev_close = float(prev_close_str)
                if prev_close > 0:
                    change_pct = (close - prev_close) / prev_close * 100
            except (ValueError, TypeError):
                pass

        cur.execute("""
            INSERT OR REPLACE INTO index_quotes
            (date, code, name, close, change_pct, volume, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (kdate, code, name, close, change_pct, volume, 0))
        new_count += 1

    conn.commit()
    conn.close()
    return new_count


def rebuild_portfolio_summary(db_path):
    """基于完整快照数据重建组合汇总历史"""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("DELETE FROM portfolio_summary")
    conn.commit()

    cur.execute("SELECT DISTINCT date FROM portfolio_snapshots ORDER BY date")
    dates = [row[0] for row in cur.fetchall()]

    generated = 0
    prev_value = 0

    for dt in dates:
        cur.execute("""
            SELECT SUM(market_value), SUM(cost_price * quantity),
                   SUM(pnl), SUM(CASE WHEN pnl >= 0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END)
            FROM portfolio_snapshots WHERE date = ?
        """, (dt,))
        row = cur.fetchone()
        if not row or not row[0]:
            if prev_value > 0:
                prev_value = 0
            continue

        total_value = row[0]
        total_cost = row[1] or 0
        total_pnl = row[2] or 0
        profit_count = int(row[3] or 0)
        loss_count = int(row[4] or 0)

        daily_return = 0
        daily_pnl = 0
        if prev_value and prev_value > 0:
            daily_pnl = total_value - prev_value
            daily_return = daily_pnl / prev_value * 100

        vs_hs300 = 0
        cur.execute("SELECT change_pct FROM index_quotes WHERE code='sh000300' AND date=?", (dt,))
        idx_row = cur.fetchone()
        if idx_row and idx_row[0]:
            vs_hs300 = daily_return - idx_row[0]

        # 风险指标
        cur.execute("""
            SELECT daily_return FROM portfolio_summary
            WHERE date < ? ORDER BY date DESC LIMIT 60
        """, (dt,))
        hist_returns = [r[0] for r in cur.fetchall() if r[0] is not None]

        sharpe = None
        max_dd = None
        vol = None

        if len(hist_returns) >= 20:
            import statistics
            avg_ret = statistics.mean(hist_returns)
            std_ret = statistics.stdev(hist_returns)
            if std_ret > 0:
                sharpe = (avg_ret / std_ret) * (252 ** 0.5)
            vol = std_ret * (252 ** 0.5)

        if len(hist_returns) >= 5:
            peak = total_value
            dd = 0
            cur.execute("""
                SELECT total_value FROM portfolio_summary
                WHERE date <= ? ORDER BY date
            """, (dt,))
            for vr in cur.fetchall():
                if vr[0] and vr[0] > peak:
                    peak = vr[0]
                if peak > 0:
                    drawdown = (peak - vr[0]) / peak * 100
                    if drawdown > dd:
                        dd = drawdown
            max_dd = dd

        cur.execute("""
            INSERT OR REPLACE INTO portfolio_summary
            (date, total_value, total_cost, total_pnl, daily_pnl, daily_return,
             vs_hs300, profit_count, loss_count, sharpe_ratio, max_drawdown, volatility)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (dt, total_value, total_cost, total_pnl, daily_pnl, daily_return,
              vs_hs300, profit_count, loss_count, sharpe, max_dd, vol))

        prev_value = total_value
        generated += 1

    conn.commit()
    conn.close()
    return generated


def rebuild_etf_technical(db_path):
    """基于完整K线数据重建ETF技术指标历史"""
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("DELETE FROM etf_technical")
    conn.commit()

    cur.execute("SELECT DISTINCT code, name FROM portfolio_snapshots ORDER BY code")
    etfs = cur.fetchall()

    total = 0
    for code, name in etfs:
        df = pd.read_sql_query(
            "SELECT date, current_price FROM portfolio_snapshots "
            "WHERE code = ? AND current_price > 0 ORDER BY date",
            conn, params=(code,)
        )

        if len(df) < 20:
            continue

        prices = df["current_price"].values
        dates = df["date"].values
        tech_rows = []

        for i in range(20, len(prices)):
            window = prices[:i + 1]
            close = prices[i]

            # MA5/MA20
            ma5 = window[-5:].mean()
            ma20 = window[-20:].mean()

            # MA信号
            if i >= 21:
                prev_ma5 = window[-6:-1].mean()
                prev_ma20 = window[-21:-1].mean()
                if ma5 > ma20 and prev_ma5 <= prev_ma20:
                    ma_signal = "金叉"
                elif ma5 < ma20 and prev_ma5 >= prev_ma20:
                    ma_signal = "死叉"
                elif ma5 > ma20:
                    ma_signal = "多头排列"
                else:
                    ma_signal = "空头排列"
            else:
                ma_signal = "多头排列" if ma5 > ma20 else "空头排列"

            # RSI(14)
            if i >= 14:
                deltas = [window[j] - window[j - 1] for j in range(max(1, i - 13), i + 1)]
                gains = [d for d in deltas if d > 0]
                losses = [-d for d in deltas if d < 0]
                avg_gain = sum(gains) / 14
                avg_loss = sum(losses) / 14 if losses else 0.001
                rsi = 100 - 100 / (1 + avg_gain / avg_loss)
            else:
                rsi = 50

            if rsi >= 85:
                rsi_status = "严重超买"
            elif rsi >= 70:
                rsi_status = "超买"
            elif rsi <= 15:
                rsi_status = "严重超卖"
            elif rsi <= 30:
                rsi_status = "超卖"
            else:
                rsi_status = "正常"

            # MACD (EMA12/EMA26)
            if i >= 35:
                series = pd.Series(window)
                ema12 = series.ewm(span=12, adjust=False).mean().iloc[-1]
                ema26 = series.ewm(span=26, adjust=False).mean().iloc[-1]
                dif = ema12 - ema26
                # DEA: DIF的9日EMA
                dif_series = series.ewm(span=12, adjust=False).mean() - series.ewm(span=26, adjust=False).mean()
                dea = dif_series.ewm(span=9, adjust=False).mean().iloc[-1]
                macd_val = 2 * (dif - dea)

                if dif > 0 and dea > 0:
                    macd_signal = "多头"
                elif dif < 0 and dea < 0:
                    macd_signal = "空头"
                elif dif > dea and (i < 36 or dif_series.iloc[-2] <= dif_series.ewm(span=9, adjust=False).mean().iloc[-2]):
                    macd_signal = "金叉"
                elif dif < dea:
                    macd_signal = "死叉"
                else:
                    macd_signal = "看多" if dif > 0 else "看空"
            else:
                macd_signal = "中性"

            # 布林带位置
            boll_window = window[-20:]
            boll_mid = boll_window.mean()
            boll_std = pd.Series(boll_window).std()
            boll_upper = boll_mid + 2 * boll_std
            boll_lower = boll_mid - 2 * boll_std
            boll_range = boll_upper - boll_lower
            boll_position = ((close - boll_lower) / boll_range * 100) if boll_range > 0 else 50

            # ATR
            if i >= 14:
                recent = window[-15:]
                trs = [abs(recent[j] - recent[j - 1]) for j in range(1, len(recent))]
                atr = sum(trs) / len(trs) if trs else 0
                atr_pct = atr / close * 100 if close > 0 else 0
            else:
                atr_pct = 0

            # KDJ (简化)
            high_n = max(window[-15:])
            low_n = min(window[-15:])
            rsv = (close - low_n) / (high_n - low_n) * 100 if high_n != low_n else 50
            kdj_signal = "金叉" if rsv > 50 else "死叉"

            # 趋势判断
            if ma5 > ma20 and rsi > 50:
                trend = "强势上涨"
            elif ma5 > ma20:
                trend = "温和上涨"
            elif ma5 < ma20 and rsi < 50:
                trend = "下跌"
            else:
                trend = "震荡整理"

            tech_rows.append((
                str(dates[i]), code, ma_signal, macd_signal,
                round(rsi, 2), rsi_status, kdj_signal,
                round(boll_position, 2), round(atr_pct, 2), trend
            ))

        if tech_rows:
            cur.executemany("""
                INSERT OR REPLACE INTO etf_technical
                (date, code, ma_signal, macd_signal, rsi_value, rsi_status,
                 kdj_signal, bollinger_position, atr_pct, trend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tech_rows)
            conn.commit()
            total += len(tech_rows)

    conn.close()
    return total


def run_backfill(etf_only=False, index_only=False, dry_run=False):
    """执行完整历史数据回填"""
    start_time = time.time()
    db_path = str(DATABASE_PATH)

    print("=" * 60)
    print(f"  完整历史数据回填 (新浪财经K线)")
    print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if dry_run:
        print("  *** 试运行模式 ***")
    print("=" * 60)

    # === 阶段一: ETF历史K线 ===
    if not index_only:
        positions = get_etf_positions(db_path)
        print(f"\n--- 阶段一: ETF历史K线 ({len(positions)}只) ---")

        etf_total = 0
        etf_success = 0
        for i, pos in enumerate(positions):
            code = pos["code"]
            name = pos["name"]
            pct = (i + 1) / len(positions) * 100

            if dry_run:
                print(f"  [{pct:5.1f}%] {code} {name} (试运行)")
                continue

            existing = get_existing_dates(db_path, "portfolio_snapshots", code)
            klines = fetch_full_klines(code, name)

            if not klines:
                print(f"  [{pct:5.1f}%] {code} {name} - 无数据")
                continue

            first_date = klines[0].get("day", "?")
            last_date = klines[-1].get("day", "?")
            new = save_etf_snapshots(
                db_path, code, name, pos["quantity"], pos["cost_price"],
                klines, existing
            )
            etf_total += new
            if new > 0:
                etf_success += 1
            print(f"  [{pct:5.1f}%] {code} {name} - 获取{len(klines)}条, 新增{new}条 ({first_date} ~ {last_date})")
            time.sleep(0.2)  # 礼貌延迟

        print(f"\n  ETF回填完成: {etf_success}/{len(positions)}只成功, 新增 {etf_total} 条快照")

    # === 阶段二: 指数历史K线 ===
    if not etf_only:
        print(f"\n--- 阶段二: 指数历史K线 ({len(INDEX_CODES)}个) ---")

        idx_total = 0
        idx_success = 0
        for i, (code, name) in enumerate(INDEX_CODES.items()):
            pct = (i + 1) / len(INDEX_CODES) * 100

            if dry_run:
                print(f"  [{pct:5.1f}%] {code} {name} (试运行)")
                continue

            existing = get_existing_dates(db_path, "index_quotes", code)
            klines = fetch_full_klines(code, name)

            if not klines:
                print(f"  [{pct:5.1f}%] {code} {name} - 无数据")
                continue

            first_date = klines[0].get("day", "?")
            last_date = klines[-1].get("day", "?")
            new = save_index_quotes(db_path, code, name, klines, existing)
            idx_total += new
            if new > 0:
                idx_success += 1
            print(f"  [{pct:5.1f}%] {code} {name} - 新增{new}条 ({first_date} ~ {last_date})")
            time.sleep(0.2)

        print(f"\n  指数回填完成: {idx_success}/{len(INDEX_CODES)}个成功, 新增 {idx_total} 条行情")

    if dry_run:
        elapsed = time.time() - start_time
        print(f"\n试运行完成, 耗时 {elapsed:.1f}秒")
        return True

    # === 阶段三: 重建组合汇总 ===
    print(f"\n--- 阶段三: 重建组合汇总 ---")
    summary_count = rebuild_portfolio_summary(db_path)
    print(f"  生成 {summary_count} 天组合汇总（含夏普/回撤/波动率）")

    # === 阶段四: 重建ETF技术指标 ===
    print(f"\n--- 阶段四: 重建ETF技术指标 ---")
    tech_count = rebuild_etf_technical(db_path)
    print(f"  生成 {tech_count} 条技术指标记录")

    # === 验证 ===
    print(f"\n--- 数据验证 ---")
    import sqlite3
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT MIN(date), MAX(date), COUNT(DISTINCT date) FROM portfolio_snapshots")
    r = cur.fetchone()
    print(f"  portfolio_snapshots: {r[0]} ~ {r[1]} ({r[2]}个交易日)")

    cur.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM portfolio_summary")
    r = cur.fetchone()
    print(f"  portfolio_summary: {r[0]} ~ {r[1]} ({r[2]}条)")

    cur.execute("SELECT MIN(date), MAX(date), COUNT(DISTINCT date) FROM index_quotes")
    r = cur.fetchone()
    print(f"  index_quotes: {r[0]} ~ {r[1]} ({r[2]}个交易日)")

    cur.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM etf_technical")
    r = cur.fetchone()
    print(f"  etf_technical: {r[0]} ~ {r[1]} ({r[2]}条)")

    # 各ETF的数据范围
    print(f"\n  各ETF数据范围:")
    cur.execute("""
        SELECT code, name, MIN(date), MAX(date), COUNT(*)
        FROM portfolio_snapshots
        GROUP BY code ORDER BY code
    """)
    for row in cur.fetchall():
        print(f"    {row[0]:8s} {row[1]:14s} {row[2]} ~ {row[3]} ({row[4]}天)")

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n  总耗时: {elapsed:.1f}秒")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="完整历史数据回填")
    parser.add_argument("--dry-run", action="store_true", help="试运行")
    parser.add_argument("--etf-only", action="store_true", help="仅回填ETF")
    parser.add_argument("--index-only", action="store_true", help="仅回填指数")
    args = parser.parse_args()

    run_backfill(etf_only=args.etf_only, index_only=args.index_only, dry_run=args.dry_run)