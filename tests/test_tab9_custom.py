"""Tests for tab9_custom.py rendering and indicator_backtest module."""
import pytest


class TestTab9CustomIndicator:
    """Test tab9 custom indicator module imports and dependencies."""

    def test_import_indicator_backtest_module(self):
        """indicator_backtest module should be importable."""
        from src.analysis.indicator_backtest import INDICATOR_TEMPLATES, backtest_technical_composite
        assert isinstance(INDICATOR_TEMPLATES, list)
        assert len(INDICATOR_TEMPLATES) > 0
        assert callable(backtest_technical_composite)

    def test_indicator_templates_have_required_keys(self):
        """Each template should have name, description, and formula."""
        from src.analysis.indicator_backtest import INDICATOR_TEMPLATES
        for tmpl in INDICATOR_TEMPLATES:
            assert "name" in tmpl, f"Template missing 'name': {tmpl}"
            assert "description" in tmpl, f"Template missing 'description': {tmpl}"
            assert "formula" in tmpl, f"Template missing 'formula': {tmpl}"

    def test_indicator_templates_formula_is_dict(self):
        """Each template's formula should be a dict with signal conditions."""
        from src.analysis.indicator_backtest import INDICATOR_TEMPLATES
        for tmpl in INDICATOR_TEMPLATES:
            assert isinstance(tmpl["formula"], dict), f"{tmpl['name']}: formula should be dict"
            # Each formula key should be a known signal column
            for key in tmpl["formula"]:
                assert isinstance(key, str), f"{tmpl['name']}: key {key} should be str"

    def test_indicator_templates_unique_names(self):
        """Template names should be unique."""
        from src.analysis.indicator_backtest import INDICATOR_TEMPLATES
        names = [t["name"] for t in INDICATOR_TEMPLATES]
        assert len(names) == len(set(names)), "Template names should be unique"

    def test_indicator_templates_have_signal_type(self):
        """Each template should specify signal_type (bullish/bearish/neutral)."""
        from src.analysis.indicator_backtest import INDICATOR_TEMPLATES
        valid_types = {"bullish", "bearish", "neutral"}
        for tmpl in INDICATOR_TEMPLATES:
            assert "signal_type" in tmpl, f"{tmpl['name']}: missing signal_type"
            assert tmpl["signal_type"] in valid_types, \
                f"{tmpl['name']}: invalid signal_type '{tmpl['signal_type']}'"
