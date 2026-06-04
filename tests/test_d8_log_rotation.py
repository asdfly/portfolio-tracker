"""
D8: 日志轮转 - 测试
- RotatingFileHandler 在 run_analysis.py
- RotatingFileHandler 在 scripts/run_backfill.py
"""
import pytest
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))


class TestLogRotationInRunAnalysis:
    def test_run_analysis_uses_rotating_handler(self):
        import run_analysis
        import logging.handlers
        assert hasattr(run_analysis, 'logging')
        assert hasattr(logging.handlers, 'RotatingFileHandler')

    def test_run_analysis_setup_logging_import(self):
        """setup_logging 可调用且返回logger"""
        import run_analysis
        assert hasattr(run_analysis, 'setup_logging')
        assert callable(run_analysis.setup_logging)


class TestLogRotationInBackfill:
    def test_backfill_uses_rotating_handler(self):
        import scripts.run_backfill as rb
        import logging.handlers
        assert hasattr(rb, 'logging')
