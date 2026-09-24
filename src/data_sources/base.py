"""
数据源基类模块
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import logging
import os
import time
import requests

logger = logging.getLogger(__name__)


def resolve_env_proxies() -> Optional[Dict[str, str]]:
    """从环境变量解析出网代理（兼容大小写 HTTPS_PROXY/HTTP_PROXY/ALL_PROXY）。

    返回 {'http':..., 'https':...} 或 None（环境无代理时直连）。
    这是 B1「代理/异地出网」的代码侧落地：运行环境只要配置了可用出网代理，
    数据源即自动经代理取数，无需改动调用方。
    """
    d: Dict[str, str] = {}
    for key in ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY",
                "https_proxy", "http_proxy", "all_proxy"):
        val = os.environ.get(key)
        if val:
            scheme = "https" if key.lower().startswith("https") else (
                "http" if key.lower().startswith("http") else "http")
            d[scheme] = val
    if not d:
        return None
    # 归一：http/https 都指向同一代理（多数本地代理 http/https 同端口）
    https = d.get("https") or d.get("http")
    http = d.get("http") or d.get("https")
    return {"http": http, "https": https}


class DataSourceError(Exception):
    """数据源异常"""


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
        # B1: 显式注入环境代理，使「运行环境配了出网代理即自动生效」可观测、确定
        self._proxies = resolve_env_proxies()
        if self._proxies:
            self.session.proxies.update(self._proxies)
        logger.info(
            f"[{self.name}] 出网模式: {'代理 ' + str(self._proxies) if self._proxies else '直连(环境无代理)'}"
        )

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

    @abstractmethod
    def get_kline(self, code: str, period: str = "day", 
                  count: int = 40) -> List[Dict[str, Any]]:
        """获取K线数据"""

    def get_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取行情（默认逐个获取，子类可优化）"""
        results = {}
        for code in codes:
            try:
                results[code] = self.get_quote(code)
            except Exception as e:
                logger.warning(f"获取 {code} 行情失败: {e}")
        return results
