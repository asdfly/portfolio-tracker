"""
投资组合分析器 - 整合所有分析功能（含风险分析）
"""
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Any
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    DATA_SOURCES, INDEX_CODES, TECH_INDICATORS, 
    RISK_CONFIG,
)
from src.data_sources import DataSourceManager
from src.analysis.technical import TechnicalAnalyzer
from src.analysis.risk import RiskAnalyzer
from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
from src.utils.database import DatabaseManager
from src.utils.position_reader import PositionReader
from data_loader import get_db_connection
import sqlite3

logger = logging.getLogger(__name__)


class PortfolioAnalyzer:
    """投资组合分析器"""

    def __init__(self):
        self.ds_manager = DataSourceManager(DATA_SOURCES)
        self.tech_analyzer = TechnicalAnalyzer(TECH_INDICATORS)
        self.risk_analyzer = RiskAnalyzer(
            risk_free_rate=RISK_CONFIG['risk_free_rate'],
            trading_days_per_year=RISK_CONFIG['trading_days_per_year']
        )
        self.portfolio_risk_analyzer = PortfolioRiskAnalyzer(
            risk_free_rate=RISK_CONFIG['risk_free_rate']
        )
        self.db = DatabaseManager()
        self.position_reader = PositionReader()
        self.today = self._determine_trading_date()

    def _determine_trading_date(self) -> str:
        """确定当前交易日日期
        
        在开盘前（9:30前）运行时，API返回的是前一交易日的收盘数据，
        因此应使用前一交易日作为日期，而非date.today()。
        
        Returns:
            交易日字符串，格式 YYYY-MM-DD
        """
        now = datetime.now()
        current_time = now.hour * 100 + now.minute  # e.g. 930 = 9:30
        
        if current_time < 930:
            # 开盘前：使用前一交易日
            days_back = 3 if now.weekday() == 0 else 1  # 周一→上周五, 其他→昨天
            trading_date = now - timedelta(days=days_back)
            logger.info(
                f"开盘前运行({now.strftime('%H:%M')}), "
                f"使用前一交易日: {trading_date.strftime('%Y-%m-%d')}"
            )
            return trading_date.strftime('%Y-%m-%d')
        else:
            return now.strftime('%Y-%m-%d')

    def _detect_position_file_updated(self) -> bool:
        """检测持仓文件是否比上次写入DB的日期更新
        
        通过比较持仓文件日期与数据库中最新持仓记录日期来判断。
        如果文件日期 > 数据库最新日期，说明有新的持仓文件需要导入。
        同日文件更新检测：当file_date == db_date时，进一步比较文件修改时间
        与DB记录的created_at，或比较持仓数量是否有变化。
        """
        from config.settings import _find_latest_position_file, _extract_position_file_date

        file_path = _find_latest_position_file()
        file_date = _extract_position_file_date(file_path)

        if not file_date:
            logger.warning(f"无法从持仓文件名提取日期: {file_path}")
            return False

        # 查询数据库中最新的持仓记录日期
        latest_db = self.db.get_latest_portfolio()
        if not latest_db:
            # 数据库中没有任何记录，需要导入
            logger.info("数据库无持仓记录，需要导入持仓文件")
            return True

        db_date = latest_db[0].get('date', '')
        if file_date > db_date:
            logger.info(f"持仓文件日期 {file_date} > 数据库最新日期 {db_date}，需要更新持仓")
            return True

        # 同日文件更新检测
        if file_date == db_date:
            # 方法1: 比较文件修改时间与DB快照时间
            try:
                conn = get_db_connection(self.db.db_path)
                cur = conn.cursor()
                # 查询DB中当天snapshot的记录数和各code的数量
                cur.execute(
                    "SELECT code, quantity FROM portfolio_snapshots WHERE date=?",
                    (db_date,)
                )
                db_rows = {r[0]: r[1] for r in cur.fetchall()}
                conn.close()

                # 读取文件中的持仓数量
                positions = self.position_reader.read_positions()
                file_rows = {p['code']: p['quantity'] for p in positions}

                # 比较持仓列表是否一致
                if set(file_rows.keys()) != set(db_rows.keys()):
                    logger.info(f"同日持仓品种变化: 文件{len(file_rows)}只 vs DB{len(db_rows)}只，需要更新")
                    return True

                # 比较各品种数量
                changed_codes = []
                for code in file_rows:
                    if file_rows[code] != db_rows.get(code):
                        changed_codes.append(f"{code}({db_rows.get(code)}→{file_rows[code]})")

                if changed_codes:
                    logger.info(f"同日持仓数量变化: {', '.join(changed_codes)}，需要更新")
                    return True
                else:
                    logger.info(f"持仓文件日期 {file_date} == 数据库最新日期 {db_date}，持仓数量一致，无需更新")
                    return False
            except (sqlite3.OperationalError, sqlite3.IntegrityError, KeyError, ValueError) as e:
                logger.warning(f"同日持仓比对失败，跳过更新检测: {e}")
                return False

        logger.info(f"持仓文件日期 {file_date} < 数据库最新日期 {db_date}，持仓无更新，保持不变")
        return False

    def run_daily_analysis(self) -> Dict[str, Any]:
        """执行每日分析
        
        逻辑：
        - 如果持仓文件有更新 → 读取新持仓文件，更新持仓快照 + 行情 + 技术指标 + 风险指标
        - 如果持仓文件无更新 → 从数据库读取最近一次持仓，仅更新行情（价格、市值、盈亏等）
        - 指数行情、技术指标每天都会重新采集/计算
        """
        logger.info("=" * 60)
        logger.info("开始执行投资组合每日分析")
        logger.info("=" * 60)

        results = {
            'date': self.today,
            'positions': [],
            'indices': {},
            'technical': {},
            'risk': {},
            'summary': {}
        }

        try:
            # 检测持仓文件是否有更新
            position_updated = self._detect_position_file_updated()

            if position_updated:
                # ========== 持仓有更新：读取新文件 ==========
                logger.info("步骤1: 检测到新持仓文件，读取持仓数据...")
                positions = self.position_reader.read_positions()
                results['positions'] = positions
                logger.info(f"读取到 {len(positions)} 条持仓记录")

                # 更新实时行情
                logger.info("步骤2: 获取实时行情...")
                self._update_realtime_quotes(positions)

                # 保存持仓快照（新持仓数据写入数据库）
                logger.info("步骤2.5: 保存新持仓快照到数据库...")
                self.db.save_portfolio_snapshot(self.today, positions)

            else:
                # ========== 持仓无更新：从数据库加载最近持仓 ==========
                logger.info("步骤1: 持仓文件无更新，从数据库加载最近持仓数据...")
                db_positions = self.db.get_latest_portfolio()

                if not db_positions:
                    raise RuntimeError("数据库中无持仓记录，且持仓文件无更新，无法继续分析")

                # 将数据库字段映射为分析器需要的格式
                positions = []
                for row in db_positions:
                    pos = dict(row)
                    # 确保关键字段存在
                    pos.setdefault('code', row.get('code'))
                    pos.setdefault('name', row.get('name'))
                    pos.setdefault('quantity', row.get('quantity'))
                    pos.setdefault('cost_price', row.get('cost_price'))
                    pos.setdefault('current_price', row.get('current_price'))
                    pos.setdefault('market_value', row.get('market_value'))
                    pos.setdefault('pnl', row.get('pnl'))
                    pos.setdefault('pnl_rate', row.get('pnl_rate'))
                    pos.setdefault('ytd_return', row.get('ytd_return'))
                    pos.setdefault('beta', row.get('beta'))
                    positions.append(pos)

                results['positions'] = positions
                logger.info(f"从数据库加载 {len(positions)} 条持仓记录")

                # 更新实时行情（用最新价格重新计算市值和盈亏）
                logger.info("步骤2: 获取实时行情并更新持仓价格...")
                self._update_realtime_quotes(positions)

                # 用更新后的价格重新保存今日持仓快照（保持持仓结构不变，仅更新价格）
                logger.info("步骤2.5: 更新持仓价格数据到数据库...")
                self.db.save_portfolio_snapshot(self.today, positions)

            # ========== 以下步骤每天都会执行 ==========

            # 获取指数行情
            logger.info("步骤3: 获取指数行情...")
            index_quotes = self._fetch_index_quotes()
            results['indices'] = index_quotes

            # 保存指数行情
            self.db.save_index_quotes(self.today, index_quotes)

            # 计算技术指标
            logger.info("步骤4: 计算技术指标...")
            tech_results = self._calculate_technical_indicators(positions)
            results['technical'] = tech_results

            # 保存技术指标
            for code, indicators in tech_results.items():
                self.db.save_technical_indicators(self.today, code, indicators)

            # 风险分析
            logger.info("步骤5: 风险分析...")
            risk_results = self.portfolio_risk_analyzer.analyze_portfolio_risk(
                positions, index_quotes
            )
            results['risk'] = risk_results

            # 计算汇总数据
            logger.info("步骤6: 计算汇总数据...")
            summary = self._calculate_summary(positions, index_quotes, risk_results)
            results['summary'] = summary

            # 保存组合汇总
            logger.info("步骤7: 保存汇总数据到数据库...")
            risk_summary = summary.get('risk_summary', {})
            summary_with_risk = {
                **summary,
                'sharpe_ratio': risk_summary.get('sharpe_ratio'),
                'max_drawdown': risk_summary.get('max_drawdown'),
                'volatility': risk_summary.get('annual_volatility')
            }
            self.db.save_portfolio_summary(self.today, summary_with_risk)

            logger.info("分析完成!")
            return results

        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            logger.error(f"分析过程出错: {e}", exc_info=True)
            raise

    def _update_realtime_quotes(self, positions: List[Dict[str, Any]]):
        """更新实时行情到持仓数据"""
        codes = [p['code'] for p in positions]
        quotes = self.ds_manager.get_batch_quotes(codes)

        for pos in positions:
            code = pos['code']
            if code in quotes:
                quote = quotes[code]
                price = quote.get('price', 0)

                # 价格合理性校验：价格必须为正，且与当前价格偏离不超过±30%
                current_price = pos.get('current_price', 0)
                if price > 0 and current_price > 0 and abs(price - current_price) / current_price > 0.3:
                    logger.warning(
                        f"行情价格异常: {code}({pos.get('name')}) "
                        f"实时={price}, 当前={current_price}, 偏离={abs(price-current_price)/current_price*100:.1f}%, 跳过更新"
                    )
                    continue

                if price > 0:
                    pos['realtime_price'] = price
                else:
                    pos['realtime_price'] = current_price

                pos['realtime_change_pct'] = quote.get('change_pct', 0)
                pos['volume'] = quote.get('volume', 0)
                pos['amount'] = quote.get('amount', 0)
                pos['high'] = quote.get('high', 0)
                pos['low'] = quote.get('low', 0)
                pos['open'] = quote.get('open', 0)
                pos['pre_close'] = quote.get('pre_close', 0)

                # 重新计算市值和盈亏
                pos['realtime_market_value'] = pos['realtime_price'] * pos['quantity']
                pos['realtime_pnl'] = (pos['realtime_price'] - pos['cost_price']) * pos['quantity']
            else:
                # 无实时行情时，使用已有价格计算盈亏（覆盖场外基金等无行情品种）
                cur_p = pos.get('current_price', 0) or 0
                cost_p = pos.get('cost_price', 0) or 0
                qty = pos.get('quantity', 0) or 0
                pos['realtime_price'] = cur_p
                pos['realtime_market_value'] = cur_p * qty
                pos['realtime_pnl'] = (cur_p - cost_p) * qty
                pos['realtime_change_pct'] = 0
                pos['pre_close'] = 0

    def _fetch_index_quotes(self) -> Dict[str, Dict[str, Any]]:
        """获取指数行情"""
        quotes = {}
        for code in INDEX_CODES.keys():
            try:
                quote = self.ds_manager.get_quote(code)
                quotes[code] = quote
            except OSError as e:
                logger.warning(f"获取指数 {code} 失败: {e}")
        return quotes

    def _calculate_technical_indicators(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算技术指标"""
        from src.data_sources.base import DataSourceError

        results = {}

        for pos in positions:
            code = pos['code']
            # 场内ETF/LOF代码规则: 51/56/58开头为上海，15开头为深圳
            # 场外基金（519/001/002/003/004/005/007/008/100/166等）无K线，直接跳过
            is_on_market = (code.startswith('51') or code.startswith('56')
                           or code.startswith('58') or code.startswith('15'))
            if not is_on_market:
                logger.debug(f"跳过非场内标的 {code}({pos.get('name', '')})的技术指标计算")
                continue
            if code.startswith('51') or code.startswith('56') or code.startswith('58'):
                ds_code = f'sh{code}'
            else:
                ds_code = f'sz{code}'

            try:
                kline = self.ds_manager.get_kline(ds_code, period='day', count=40)

                if len(kline) >= 30:
                    indicators = self.tech_analyzer.calculate_all(kline)
                    results[code] = indicators
                else:
                    logger.warning(f"{code} K线数据不足")

            except DataSourceError as e:
                logger.warning(f"获取 {code}({pos.get('name', '')}) K线失败，跳过技术指标计算: {e}")
            except (ValueError, KeyError, TypeError, IndexError) as e:
                logger.warning(f"计算 {code} 技术指标失败: {e}")

        return results

    def _calculate_summary(self, positions: List[Dict[str, Any]], 
                          index_quotes: Dict[str, Dict[str, Any]],
                          risk_results: Dict[str, Any]) -> Dict[str, Any]:
        """计算汇总数据"""
        # 使用实时价格计算
        total_value = sum(p.get('realtime_market_value', p['market_value']) for p in positions)
        total_cost = sum(p['cost_price'] * p['quantity'] for p in positions)
        total_pnl = sum(p.get('realtime_pnl') or p.get('pnl') or 0 for p in positions)

        # 计算日涨跌（校正版：用共同持仓相同数量×当日价格 vs 前日市值，避免新增/加仓导致跳变）
        daily_pnl = 0
        daily_return = 0
        prev_value = 0
        try:
            conn = get_db_connection(self.db.db_path)
            cur = conn.cursor()
            # 获取前一交易日
            cur.execute(
                "SELECT date FROM portfolio_summary WHERE date < ? ORDER BY date DESC LIMIT 1",
                (self.today,)
            )
            prev_row = cur.fetchone()
            if prev_row and prev_row[0]:
                prev_dt = prev_row[0]
                cur.execute(
                    "SELECT total_value FROM portfolio_summary WHERE date = ?",
                    (prev_dt,)
                )
                prev_summary_row = cur.fetchone()
                prev_value = prev_summary_row[0] if prev_summary_row and prev_summary_row[0] else 0
                # 获取前日各标的持仓
                cur.execute(
                    "SELECT code, quantity, market_value FROM portfolio_snapshots WHERE date = ?",
                    (prev_dt,)
                )
                prev_snapshots = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
                # 当日持仓代码集合
                curr_codes = {p['code']: p for p in positions}
                common_codes = set(curr_codes.keys()) & set(prev_snapshots.keys())
                # 用共同持仓的（前日数量×当日价格）vs（前日市值）计算纯价格收益
                price_adj_mv = 0
                prev_common_mv = 0
                for code in common_codes:
                    prev_qty, prev_mv = prev_snapshots[code]
                    curr_pos = curr_codes[code]
                    # FIX: 优先使用 realtime_price（_update_realtime_quotes 更新后的最新价格）
                    curr_price = curr_pos.get('realtime_price', curr_pos.get('current_price', 0))
                    price_adj_mv += curr_price * prev_qty
                    prev_common_mv += prev_mv
                if prev_common_mv > 0:
                    daily_pnl = price_adj_mv - prev_common_mv
                    daily_return = daily_pnl / prev_common_mv * 100
            conn.close()
        except (sqlite3.OperationalError, KeyError):
            pass

        # fallback: 用 total_value 简单对比（仅当无前日快照数据时）
        if daily_return == 0 and prev_value > 0:
            daily_pnl = total_value - prev_value
            daily_return = daily_pnl / prev_value * 100

        # 对比沪深300
        hs300_quote = index_quotes.get('sh000300', {})
        hs300_change = hs300_quote.get('change_pct', 0)
        vs_hs300 = daily_return - hs300_change

        # 盈亏统计
        profit_count = len([p for p in positions if (p.get('realtime_pnl') or p.get('pnl') or 0) > 0])
        loss_count = len([p for p in positions if (p.get('realtime_pnl') or p.get('pnl') or 0) < 0])

        # 最大贡献/拖累
        sorted_by_pnl = sorted(positions, 
                              key=lambda x: x.get('realtime_pnl') or x.get('pnl') or 0, 
                              reverse=True)

        # 风险指标摘要
        risk_summary = {}
        portfolio_metrics = risk_results.get('portfolio_metrics', {})

        if 'risk_adjusted_metrics' in portfolio_metrics:
            ram = portfolio_metrics['risk_adjusted_metrics']
            risk_summary['sharpe_ratio'] = ram.get('sharpe_ratio')
            risk_summary['sharpe_grade'] = ram.get('sharpe_grade')

        if 'drawdown_metrics' in portfolio_metrics:
            dm = portfolio_metrics['drawdown_metrics']
            risk_summary['max_drawdown'] = dm.get('max_drawdown')
            risk_summary['current_drawdown'] = dm.get('current_drawdown')

        if 'volatility_metrics' in portfolio_metrics:
            vm = portfolio_metrics['volatility_metrics']
            risk_summary['annual_volatility'] = vm.get('annual_volatility')

        concentration = risk_results.get('concentration_risk', {})
        risk_summary['max_weight'] = concentration.get('max_weight')
        risk_summary['hhi'] = concentration.get('hhi')

        return {
            'date': self.today,
            'total_value': round(total_value, 2),
            'total_cost': round(total_cost, 2),
            'total_pnl': round(total_pnl, 2),
            'total_return_pct': round(total_pnl / total_cost * 100, 2) if total_cost and total_cost > 0 else 0,
            'daily_pnl': round(daily_pnl, 2),
            'daily_return': round(daily_return, 2),
            'vs_hs300': round(vs_hs300, 2),
            'profit_count': profit_count,
            'loss_count': loss_count,
            'position_count': len(positions),
            'top_contributor': sorted_by_pnl[0]['name'] if sorted_by_pnl else '',
            'top_drag': sorted_by_pnl[-1]['name'] if sorted_by_pnl else '',
            'hs300_change': hs300_change,
            'risk_summary': risk_summary,
            'risk_warnings': risk_results.get('risk_warnings', [])
        }

    def _save_to_database(self, positions: List[Dict[str, Any]], 
                         summary: Dict[str, Any],
                         index_quotes: Dict[str, Dict[str, Any]],
                         tech_results: Dict[str, Any]):
        """保存数据到数据库（已内联到 run_daily_analysis 中，保留此方法作为向后兼容）"""
        self.db.save_portfolio_snapshot(self.today, positions)

        risk_summary = summary.get('risk_summary', {})
        summary_with_risk = {
            **summary,
            'sharpe_ratio': risk_summary.get('sharpe_ratio'),
            'max_drawdown': risk_summary.get('max_drawdown'),
            'volatility': risk_summary.get('annual_volatility')
        }
        self.db.save_portfolio_summary(self.today, summary_with_risk)

        self.db.save_index_quotes(self.today, index_quotes)

        for code, indicators in tech_results.items():
            self.db.save_technical_indicators(self.today, code, indicators)

        logger.info("数据保存完成")
