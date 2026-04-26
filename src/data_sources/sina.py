"""
新浪财经数据源
"""
import json
import re
from typing import Dict, List, Any
import logging

from .base import BaseDataSource, DataSourceError

logger = logging.getLogger(__name__)


class SinaDataSource(BaseDataSource):
    """新浪财经数据源"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("base_url", "http://hq.sinajs.cn")
        self.kline_url = config.get("kline_url")

    def _to_sina_code(self, code: str) -> str:
        """转换为新浪代码格式"""
        code = str(code).strip().lower()
        if code.startswith('sh') or code.startswith('sz'):
            return code
        if code.startswith('51') or code.startswith('56') or code.startswith('58'):
            return f'sh{code}'
        else:
            return f'sz{code}'

    def get_quote(self, code: str) -> Dict[str, Any]:
        """获取实时行情"""
        sina_code = self._to_sina_code(code)
        url = f"{self.base_url}/list={sina_code}"
        headers = {'Referer': 'https://finance.sina.com.cn'}
        text = self._request(url, headers=headers, encoding='gbk')

        pattern = rf'var hq_str_{sina_code}="([^"]*)"'
        match = re.search(pattern, text)
        if not match:
            raise DataSourceError(f"无法解析行情数据: {code}")

        data = match.group(1).split(',')
        if len(data) < 10:
            raise DataSourceError(f"行情数据不完整: {code}")

        return {
            'code': code,
            'sina_code': sina_code,
            'name': data[0],
            'open': float(data[1]) if data[1] else 0,
            'pre_close': float(data[2]) if data[2] else 0,
            'price': float(data[3]) if data[3] else 0,
            'high': float(data[4]) if data[4] else 0,
            'low': float(data[5]) if data[5] else 0,
            'volume': float(data[8]) if data[8] else 0,
            'amount': float(data[9]) if data[9] else 0,
            'change_pct': (float(data[3]) - float(data[2])) / float(data[2]) * 100 
                          if data[2] and float(data[2]) > 0 else 0
        }

    def get_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取行情"""
        results = {}
        sina_codes = [self._to_sina_code(c) for c in codes]
        batch_size = 25

        for i in range(0, len(sina_codes), batch_size):
            batch = sina_codes[i:i+batch_size]
            original_batch = codes[i:i+batch_size]
            url = f"{self.base_url}/list={','.join(batch)}"
            headers = {'Referer': 'https://finance.sina.com.cn'}

            try:
                text = self._request(url, headers=headers, encoding='gbk')
                for sina_code, orig_code in zip(batch, original_batch):
                    pattern = rf'var hq_str_{sina_code}="([^"]*)"'
                    match = re.search(pattern, text)
                    if match:
                        data = match.group(1).split(',')
                        if len(data) >= 10:
                            results[orig_code] = {
                                'code': orig_code,
                                'sina_code': sina_code,
                                'name': data[0],
                                'open': float(data[1]) if data[1] else 0,
                                'pre_close': float(data[2]) if data[2] else 0,
                                'price': float(data[3]) if data[3] else 0,
                                'high': float(data[4]) if data[4] else 0,
                                'low': float(data[5]) if data[5] else 0,
                                'volume': float(data[8]) if data[8] else 0,
                                'amount': float(data[9]) if data[9] else 0,
                                'change_pct': (float(data[3]) - float(data[2])) / float(data[2]) * 100 
                                              if data[2] and float(data[2]) > 0 else 0
                            }
            except Exception as e:
                logger.error(f"批量获取失败: {e}")
                for orig_code in original_batch:
                    try:
                        results[orig_code] = self.get_quote(orig_code)
                    except Exception as e2:
                        logger.warning(f"获取 {orig_code} 失败: {e2}")
        return results

    def get_kline(self, code: str, period: str = "day", count: int = 40) -> List[Dict[str, Any]]:
        """获取K线数据"""
        sina_code = self._to_sina_code(code)
        scale_map = {"day": 240, "60min": 60, "30min": 30, "15min": 15, "5min": 5}
        scale = scale_map.get(period, 240)
        url = f"{self.kline_url}?symbol={sina_code}&scale={scale}&ma=no&datalen={count}"

        try:
            text = self._request(url)
            data = json.loads(text)
            result = []
            for item in data:
                result.append({
                    'date': item.get('day', ''),
                    'open': float(item.get('open', 0)),
                    'high': float(item.get('high', 0)),
                    'low': float(item.get('low', 0)),
                    'close': float(item.get('close', 0)),
                    'volume': float(item.get('volume', 0))
                })
            return result
        except Exception as e:
            raise DataSourceError(f"获取K线失败 {code}: {e}")
