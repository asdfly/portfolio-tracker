"""
持仓数据读取模块 - 解析通达信导出的持仓文件
"""
import pandas as pd
import re
from typing import List, Dict, Any
from datetime import datetime
import logging

from config.settings import POSITION_FILE

logger = logging.getLogger(__name__)


class PositionReader:
    """持仓数据读取器"""

    def __init__(self, file_path: str = None):
        self.file_path = file_path or POSITION_FILE

    def read_positions(self) -> List[Dict[str, Any]]:
        """读取持仓数据"""
        try:
            df = pd.read_csv(self.file_path, encoding='gb2312', sep='\t')

            # 过滤掉数据来源行
            df = df[df['名称'].notna()]
            df = df[~df['名称'].str.contains('数据来源', na=False)]

            # 提取纯代码
            df['纯代码'] = df['代码'].apply(lambda x: re.sub(r'[=""]', '', str(x)).strip())

            # 转换为标准格式
            positions = []
            for _, row in df.iterrows():
                positions.append({
                    'code': row['纯代码'],
                    'name': row['名称'],
                    'quantity': float(row['证券数量']) if pd.notna(row['证券数量']) else 0,
                    'cost_price': float(row['成本价']) if pd.notna(row['成本价']) else 0,
                    'current_price': float(row['现价']) if pd.notna(row['现价']) else 0,
                    'market_value': float(row['最新市值']) if pd.notna(row['最新市值']) else 0,
                    'pnl': float(row['持仓盈亏']) if pd.notna(row['持仓盈亏']) else 0,
                    'pnl_rate': float(row['盈亏率%']) if pd.notna(row['盈亏率%']) else 0,
                    'ytd_return': float(row['年初至今%']) if pd.notna(row['年初至今%']) else 0,
                    'beta': float(row['贝塔系数']) if pd.notna(row['贝塔系数']) else 0,
                    'daily_change_pct': float(row['涨幅%']) if pd.notna(row['涨幅%']) else 0,
                    'industry': row.get('细分行业', ''),
                    'consecutive_days': float(row['连涨天']) if pd.notna(row['连涨天']) else 0,
                })

            logger.info(f"成功读取 {len(positions)} 条持仓记录")
            return positions

        except Exception as e:
            logger.error(f"读取持仓文件失败: {e}")
            raise

    def get_summary(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算持仓汇总信息"""
        total_value = sum(p['market_value'] for p in positions)
        total_cost = sum(p['cost_price'] * p['quantity'] for p in positions)
        total_pnl = sum(p['pnl'] for p in positions)

        profit_count = len([p for p in positions if p['pnl'] > 0])
        loss_count = len([p for p in positions if p['pnl'] < 0])

        # 行业分类
        industry_map = {
            '512010': '医药', '159992': '医药', '515120': '医药',
            '515010': '券商',
            '159267': '军工', '512810': '军工',
            '159796': '新能源', '561910': '新能源', '516160': '新能源',
            '159819': 'AI', '159770': 'AI',
            '159732': '科技',
            '510300': '宽基', '159300': '宽基', '512100': '宽基', 
            '510500': '宽基', '159949': '宽基', '588000': '宽基',
            '563020': '红利', '159220': '红利',
            '159650': '债券', '511520': '债券', '511380': '债券'
        }

        industry_values = {}
        for p in positions:
            ind = industry_map.get(p['code'], '其他')
            industry_values[ind] = industry_values.get(ind, 0) + p['market_value']

        return {
            'total_value': total_value,
            'total_cost': total_cost,
            'total_pnl': total_pnl,
            'total_return_pct': (total_pnl / total_cost * 100) if total_cost > 0 else 0,
            'position_count': len(positions),
            'profit_count': profit_count,
            'loss_count': loss_count,
            'industry_distribution': industry_values
        }
