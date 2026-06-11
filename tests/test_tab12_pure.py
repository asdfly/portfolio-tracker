"""Tests for tabs/tab12_macro.py pure functions."""
import pytest
from tabs.tab12_macro import _style_fig
import plotly.graph_objects as go

class TestStyleFig:
    def test_basic_styling(self):
        fig = go.Figure(data=[go.Scatter(x=[1,2,3], y=[1,2,3])])
        result = _style_fig(fig, title="Test")
        assert result is not None
        assert isinstance(result, go.Figure)

    def test_title_applied(self):
        fig = go.Figure(data=[go.Scatter(x=[1,2,3], y=[1,2,3])])
        result = _style_fig(fig, title="MyTitle")
        # Title should be in layout
        layout = result.to_dict().get("layout", {})
        assert layout.get("title", {}).get("text", "") == "MyTitle" or "MyTitle" in str(layout)

    def test_empty_title(self):
        fig = go.Figure()
        result = _style_fig(fig)
        assert isinstance(result, go.Figure)