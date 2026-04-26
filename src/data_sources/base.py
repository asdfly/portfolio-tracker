"""
数据源基类模块
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import logging
import time
import requests

logger = logging.getLogger(__name__)


class DataSourceError(Exception):
    """数据源异常"""
    pass


class BaseDataSource(ABC):
    """数据源基类"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config.get("name", "Unknown")
        self.timeout = config.get("timeout", 10)
        self.retry = config.get("retry", 3)
        self.delay = config.get("delay", 0.3)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def _request(self, url: str, params: Optional[Dict] = None, 
                 headers: Optional[Dict] = None, encoding: str = 'utf-8') -> str:
        """发送HTTP请求（带重试）"""
        for attempt in range(self.retry):
            try:
                if headers:
                    self.session.headers.update(headers)
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.encoding = encoding
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp.text
            except requests.RequestException as e:
                logger.warning(f"{self.name} 请求失败 (尝试 {attempt+1}/{self.retry}): {e}")
                if attempt < self.retry - 1:
                    time.sleep(self.delay * (attempt + 1))
                else:
                    raise DataSourceError(f"{self.name} 请求失败: {e}")
        return ""

    @abstractmethod
    def get_quote(self, code: str) -> Dict[str, Any]:
        """获取实时行情"""
        pass

    @abstractmethod
    def get_kline(self, code: str, period: str = "day", 
                  count: int = 40) -> List[Dict[str, Any]]:
        """获取K线数据"""
        pass

    def get_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取行情（默认逐个获取，子类可优化）"""
        results = {}
        for code in codes:
            try:
                results[code] = self.get_quote(code)
            except Exception as e:
                logger.warning(f"获取 {code} 行情失败: {e}")
        return results
