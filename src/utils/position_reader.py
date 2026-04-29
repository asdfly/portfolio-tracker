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
        """读取持仓数据

        支持通达信导出的 .xls (实际为TSV格式) 和 .tsv 文件，
        自动处理代码字段的 ="..." 格式、列名差异和无效数据行。
        """
        try:
            # 通达信导出的.xls实际为TSV格式，优先尝试gb2312编码
            for encoding in ['gb2312', 'gbk', 'gb18030', 'utf-8']:
                try:
                    df = pd.read_csv(self.file_path, encoding=encoding, sep='\t')
                    break
                except (UnicodeDecodeError, UnicodeError):
                    continue
            else:
                raise ValueError(f"无法以支持编码读取文件: {self.file_path}")

            # 列名标准化映射（不同通达信版本导出的列名可能略有差异）
            column_map = {
                '名称': '名称', '代码': '代码', '涨幅%': '涨幅%', '现价': '现价',
                '成本价': '成本价', '证券数量': '证券数量', '最新市值': '最新市值',
                '持仓盈亏': '持仓盈亏', '盈亏率%': '盈亏率%', '年初至今%': '年初至今%',
                '贝塔系数': '贝塔系数', '连涨天': '连涨天', '细分行业': '细分行业',
            }
            # 只保留已知列，忽略通达信导出的额外列
            available_cols = [c for c in column_map.keys() if c in df.columns]
            if len(available_cols) < 10:
                logger.warning(f"文件列数不足: 找到 {len(available_cols)} 列 (期望>=10), 实际列={list(df.columns)}")
            df = df[[c for c in df.columns if c in column_map or c == 'Unnamed: 30']]

            # 过滤掉数据来源行和非数据行
            df = df[df['名称'].notna()]
            df = df[~df['名称'].str.contains('数据来源|合计|小计', na=False)]

            # 提取纯代码：处理 ="512010" 格式
            def _clean_code(raw: str) -> str:
                cleaned = re.sub(r'[=""]', '', str(raw)).strip()
                # 去除可能残留的前后缀
                cleaned = cleaned.strip('.')
                return cleaned

            df['纯代码'] = df['代码'].apply(_clean_code)
            df = df[df['纯代码'].str.len() >= 4]  # 过滤无效代码行

            # 数值转换辅助函数
            def _to_float(val, default=0.0) -> float:
                try:
                    if pd.isna(val) or val in ('--', '-', 'NaN', ''):
                        return default
                    return float(val)
                except (ValueError, TypeError):
                    return default

            # 转换为标准格式
            positions = []
            for _, row in df.iterrows():
                positions.append({
                    'code': row['纯代码'],
                    'name': str(row['名称']).strip(),
                    'quantity': _to_float(row.get('证券数量')),
                    'cost_price': _to_float(row.get('成本价')),
                    'current_price': _to_float(row.get('现价')),
                    'market_value': _to_float(row.get('最新市值')),
                    'pnl': _to_float(row.get('持仓盈亏')),
                    'pnl_rate': _to_float(row.get('盈亏率%')),
                    'ytd_return': _to_float(row.get('年初至今%')),
                    'beta': _to_float(row.get('贝塔系数')),
                    'daily_change_pct': _to_float(row.get('涨幅%')),
                    'industry': str(row.get('细分行业', '')).strip(),
                    'consecutive_days': _to_float(row.get('连涨天')),
                })

            logger.info(f"成功读取持仓文件: {self.file_path}")
            logger.info(f"读取到 {len(positions)} 条持仓记录")
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
