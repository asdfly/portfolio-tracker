"""
AKShare数据源 - 开源免费金融数据接口
"""
from typing import Dict, List, Any
import logging

from .base import BaseDataSource, DataSourceError

logger = logging.getLogger(__name__)


class AKShareDataSource(BaseDataSource):
    """AKShare数据源（备用）"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._ak = None
        self._init_akshare()

    def _init_akshare(self):
        """延迟初始化AKShare"""
        try:
            import akshare as ak
            self._ak = ak
            logger.info("AKShare初始化成功")
        except ImportError:
            logger.warning("AKShare未安装，备用数据源不可用")
            self._ak = None

    def _normalize_code(self, code: str) -> str:
        """标准化代码"""
        code = str(code).strip().lower()
        if code.startswith('sh') or code.startswith('sz'):
            return code[2:]
        return code

    def get_quote(self, code: str) -> Dict[str, Any]:
        """获取实时行情"""
        if not self._ak:
            raise DataSourceError("AKShare未初始化")

        try:
            # 使用akshare获取ETF实时行情
            symbol = self._normalize_code(code)
            df = self._ak.fund_etf_spot_em()
            row = df[df['代码'] == symbol]

            if row.empty:
                raise DataSourceError(f"未找到行情数据: {code}")

            data = row.iloc[0]
            return {
                'code': code,
                'name': data.get('名称', ''),
                'price': float(data.get('最新价', 0)),
                'open': float(data.get('开盘价', 0)),
                'pre_close': float(data.get('昨收', 0)),
                'high': float(data.get('最高价', 0)),
                'low': float(data.get('最低价', 0)),
                'volume': float(data.get('成交量', 0)),
                'amount': float(data.get('成交额', 0)),
                'change_pct': float(data.get('涨跌幅', 0))
            }
        except Exception as e:
            raise DataSourceError(f"AKShare获取行情失败: {e}")

    def get_kline(self, code: str, period: str = "day", count: int = 40) -> List[Dict[str, Any]]:
        """获取K线数据"""
        if not self._ak:
            raise DataSourceError("AKShare未初始化")

        try:
            symbol = self._normalize_code(code)

            # 判断交易所
            if symbol.startswith('51') or symbol.startswith('56') or symbol.startswith('58'):
                market = "sh"
            else:
                market = "sz"

            # 获取历史K线
            df = self._ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                adjust="qfq"
            )

            # 取最近count条
            df = df.tail(count)

            result = []
            for _, row in df.iterrows():
                result.append({
                    'date': row['日期'],
                    'open': float(row['开盘']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'close': float(row['收盘']),
                    'volume': float(row['成交量'])
                })
            return result

        except Exception as e:
            raise DataSourceError(f"AKShare获取K线失败: {e}")
