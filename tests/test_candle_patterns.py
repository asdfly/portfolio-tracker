"""Tests for src/analysis/candle_patterns.py."""
import pytest, pandas as pd
from src.analysis.candle_patterns import detect_candle_patterns, PATTERN_NAME_MAP, PATTERN_SIGNAL

def make_df(ohlc):
    return pd.DataFrame(ohlc, columns=["open","high","low","close"])

def find_pat(r, key):
    return [c for c in r["pattern"] if c and key in str(c)]

class TestDoji:
    def test_detected(self):
        # body=0 at idx=4 with large prior bodies making avg_body>0
        r = detect_candle_patterns(make_df([
            (100,110,90,108),(108,118,105,115),(115,130,115,125),
            (125,140,125,135),(135,155,115,135)]))
        assert len(find_pat(r, "doji")) >= 1

    def test_no_false_positive(self):
        r = detect_candle_patterns(make_df([
            (10,20,5,18),(18,28,10,25),(25,35,20,30)]))
        assert len(find_pat(r, "doji")) == 0

class TestHammer:
    def test_after_decline(self):
        # upper_shadow=0 (high=max(open,close)), lower_shadow>>body*2
        r = detect_candle_patterns(make_df([
            (100,105,95,104),(104,108,94,95),(95,97,68,97)]))
        assert len(find_pat(r, "hammer")) >= 1

class TestHangingMan:
    def test_after_rise(self):
        r = detect_candle_patterns(make_df([
            (100,105,95,104),(104,112,102,109),(109,111,80,111)]))
        assert len(find_pat(r, "hanging_man")) >= 1

class TestBullishEngulfing:
    def test_detected(self):
        r = detect_candle_patterns(make_df([
            (100,105,95,104),(104,108,100,103),(103,115,100,112)]))
        assert len(find_pat(r, "bullish_engulfing")) >= 1

class TestBearishEngulfing:
    def test_detected(self):
        r = detect_candle_patterns(make_df([
            (100,105,95,104),(104,112,102,109),(110,115,95,99)]))
        assert len(find_pat(r, "bearish_engulfing")) >= 1

class TestMorningStar:
    def test_detected(self):
        r = detect_candle_patterns(make_df([
            (100,105,95,96),(96,99,93,97),(97,115,96,110)]))
        assert len(find_pat(r, "morning_star")) >= 1

class TestEveningStar:
    def test_detected(self):
        r = detect_candle_patterns(make_df([
            (100,115,96,110),(110,113,107,112),(112,115,90,95)]))
        assert len(find_pat(r, "evening_star")) >= 1

class TestThreeWhiteSoldiers:
    def test_detected(self):
        r = detect_candle_patterns(make_df([
            (100,105,98,104),(104,110,103,108),(108,115,107,112)]))
        assert len(find_pat(r, "three_white_soldiers")) >= 1

class TestThreeBlackCrows:
    def test_detected(self):
        r = detect_candle_patterns(make_df([
            (112,113,105,106),(106,107,100,102),(102,103,96,98)]))
        assert len(find_pat(r, "three_black_crows")) >= 1

class TestEdgeCases:
    def test_empty(self):
        assert len(detect_candle_patterns(
            pd.DataFrame(columns=["open","high","low","close"]))) == 0

    def test_single_row(self):
        assert len(detect_candle_patterns(make_df([(100,110,90,100)]))) == 1

    def test_two_rows(self):
        # len<3 returns empty patterns
        r = detect_candle_patterns(make_df([(100,110,90,108),(108,118,105,115)]))
        assert all(p == "" for p in r["pattern"])

class TestSignalConsistency:
    def test_bullish_count(self):
        assert len([k for k,v in PATTERN_SIGNAL.items() if v == "bullish"]) >= 3

    def test_bearish_count(self):
        assert len([k for k,v in PATTERN_SIGNAL.items() if v == "bearish"]) >= 3

    def test_all_have_chinese_names(self):
        for k in PATTERN_NAME_MAP:
            assert PATTERN_NAME_MAP[k] is not None