"""
投资组合分析器 - 整合所有分析功能（含风险分析）
"""
import logging
from datetime import datetime, date
from typing import Dict, List, Any
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    DATA_SOURCES, INDEX_CODES, MAJOR_ETFS, 
    TECH_INDICATORS, RISK_CONFIG
)
from src.data_sources import DataSourceManager
from src.analysis.technical import TechnicalAnalyzer
from src.analysis.risk import RiskAnalyzer
from src.analysis.portfolio_risk import PortfolioRiskAnalyzer
from src.utils.database import DatabaseManager
from src.utils.position_reader import PositionReader

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
        self.today = date.today().strftime('%Y-%m-%d')

    def run_daily_analysis(self) -> Dict[str, Any]:
        """执行每日分析"""
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
            # 1. 读取持仓数据
            logger.info("步骤1: 读取持仓数据...")
            positions = self.position_reader.read_positions()
            results['positions'] = positions
            logger.info(f"读取到 {len(positions)} 条持仓记录")

            # 2. 获取实时行情
            logger.info("步骤2: 获取实时行情...")
            self._update_realtime_quotes(positions)

            # 3. 获取指数行情
            logger.info("步骤3: 获取指数行情...")
            index_quotes = self._fetch_index_quotes()
            results['indices'] = index_quotes

            # 4. 计算技术指标
            logger.info("步骤4: 计算技术指标...")
            tech_results = self._calculate_technical_indicators(positions)
            results['technical'] = tech_results

            # 5. 风险分析
            logger.info("步骤5: 风险分析...")
            risk_results = self.portfolio_risk_analyzer.analyze_portfolio_risk(
                positions, index_quotes
            )
            results['risk'] = risk_results

            # 6. 计算汇总数据
            logger.info("步骤6: 计算汇总数据...")
            summary = self._calculate_summary(positions, index_quotes, risk_results)
            results['summary'] = summary

            # 7. 保存到数据库
            logger.info("步骤7: 保存数据到数据库...")
            self._save_to_database(positions, summary, index_quotes, tech_results)

            logger.info("分析完成!")
            return results

        except Exception as e:
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
                pos['realtime_price'] = quote.get('price', pos['current_price'])
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

    def _fetch_index_quotes(self) -> Dict[str, Dict[str, Any]]:
        """获取指数行情"""
        quotes = {}
        for code in INDEX_CODES.keys():
            try:
                quote = self.ds_manager.get_quote(code)
                quotes[code] = quote
            except Exception as e:
                logger.warning(f"获取指数 {code} 失败: {e}")
        return quotes

    def _calculate_technical_indicators(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算技术指标"""
        results = {}

        for pos in positions:
            code = pos['code']
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

            except Exception as e:
                logger.warning(f"计算 {code} 技术指标失败: {e}")

        return results

    def _calculate_summary(self, positions: List[Dict[str, Any]], 
                          index_quotes: Dict[str, Dict[str, Any]],
                          risk_results: Dict[str, Any]) -> Dict[str, Any]:
        """计算汇总数据"""
        # 使用实时价格计算
        total_value = sum(p.get('realtime_market_value', p['market_value']) for p in positions)
        total_cost = sum(p['cost_price'] * p['quantity'] for p in positions)
        total_pnl = sum(p.get('realtime_pnl', p['pnl']) for p in positions)

        # 计算日涨跌
        prev_value = sum(p['market_value'] / (1 + p['daily_change_pct']/100) 
                        for p in positions if p['daily_change_pct'] != 0)
        daily_pnl = total_value - prev_value if prev_value > 0 else 0
        daily_return = daily_pnl / prev_value * 100 if prev_value > 0 else 0

        # 对比沪深300
        hs300_quote = index_quotes.get('sh000300', {})
        hs300_change = hs300_quote.get('change_pct', 0)
        vs_hs300 = daily_return - hs300_change

        # 盈亏统计
        profit_count = len([p for p in positions if p.get('realtime_pnl', p['pnl']) > 0])
        loss_count = len([p for p in positions if p.get('realtime_pnl', p['pnl']) < 0])

        # 最大贡献/拖累
        sorted_by_pnl = sorted(positions, 
                              key=lambda x: x.get('realtime_pnl', x['pnl']), 
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
            'total_return_pct': round(total_pnl / total_cost * 100, 2) if total_cost > 0 else 0,
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
        """保存数据到数据库"""
        # 保存持仓快照
        self.db.save_portfolio_snapshot(self.today, positions)

        # 保存组合汇总（包含风险指标）
        risk_summary = summary.get('risk_summary', {})
        summary_with_risk = {
            **summary,
            'sharpe_ratio': risk_summary.get('sharpe_ratio'),
            'max_drawdown': risk_summary.get('max_drawdown'),
            'volatility': risk_summary.get('annual_volatility')
        }
        self.db.save_portfolio_summary(self.today, summary_with_risk)

        # 保存指数行情
        self.db.save_index_quotes(self.today, index_quotes)

        # 保存技术指标
        for code, indicators in tech_results.items():
            self.db.save_technical_indicators(self.today, code, indicators)

        logger.info("数据保存完成")
