"""
数据源管理器 - 支持多源切换和故障转移
"""
from typing import Dict, List, Any, Optional
import logging

from .sina import SinaDataSource
from .akshare_ds import AKShareDataSource
from .base import DataSourceError

logger = logging.getLogger(__name__)


class DataSourceManager:
    """数据源管理器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sources: Dict[str, Any] = {}
        self.source_order: List[str] = []
        self._init_sources()

    def _init_sources(self):
        """初始化数据源"""
        source_classes = {
            'sina': SinaDataSource,
            'akshare': AKShareDataSource,
        }

        # 按优先级排序
        sorted_sources = sorted(
            self.config.items(),
            key=lambda x: x[1].get('priority', 99)
        )

        for name, cfg in sorted_sources:
            if cfg.get('enabled', False) and name in source_classes:
                try:
                    self.sources[name] = source_classes[name](cfg)
                    self.source_order.append(name)
                    logger.info(f"数据源初始化成功: {name}")
                except Exception as e:
                    logger.error(f"数据源初始化失败 {name}: {e}")

    def get_quote(self, code: str) -> Dict[str, Any]:
        """获取行情（自动切换数据源）"""
        for source_name in self.source_order:
            try:
                result = self.sources[source_name].get_quote(code)
                logger.debug(f"使用 {source_name} 获取 {code} 成功")
                return result
            except Exception as e:
                logger.warning(f"{source_name} 获取 {code} 失败: {e}")
                continue

        raise DataSourceError(f"所有数据源都无法获取 {code}")

    def get_batch_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取行情"""
        # 优先使用第一个支持批量的数据源
        for source_name in self.source_order:
            try:
                source = self.sources[source_name]
                if hasattr(source, 'get_batch_quotes'):
                    return source.get_batch_quotes(codes)
            except Exception as e:
                logger.warning(f"{source_name} 批量获取失败: {e}")
                continue

        # 回退到逐个获取
        results = {}
        for code in codes:
            try:
                results[code] = self.get_quote(code)
            except Exception as e:
                logger.error(f"获取 {code} 失败: {e}")
        return results

    def get_kline(self, code: str, period: str = "day", count: int = 40) -> List[Dict[str, Any]]:
        """获取K线（自动切换数据源）"""
        for source_name in self.source_order:
            try:
                result = self.sources[source_name].get_kline(code, period, count)
                logger.debug(f"使用 {source_name} 获取 {code} K线成功")
                return result
            except Exception as e:
                logger.warning(f"{source_name} 获取 {code} K线失败: {e}")
                continue

        raise DataSourceError(f"所有数据源都无法获取 {code} K线")

    def get_source_status(self) -> Dict[str, bool]:
        """获取数据源状态"""
        return {name: name in self.sources for name in self.config.keys()}
