"""技术分析测试（原 src.analysis.technical 模块已移除，标记skip）"""
import pytest


@pytest.mark.skip(reason="src.analysis.technical 模块已不存在，技术指标函数已内联到各Tab模块中")
class TestTechnicalAnalysis:
    def test_calculate_ma(self):
        pass

    def test_calculate_macd(self):
        pass

    def test_calculate_rsi(self):
        pass
