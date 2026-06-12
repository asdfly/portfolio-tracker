"""ETF 技术信号综合评分模块

基于 etf_technical 表的多维指标数据，计算单只 ETF 的综合技术评分（0-100）。
信号维度：趋势(30%)、动量(25%)、波动(20%)、超买超卖(15%)、成交量(10%)。

返回结构：
    {
        "total_score": float,       # 0-100 综合评分
        "grade": str,               # "强烈买入"/"买入"/"持有"/"卖出"/"强烈卖出"
        "signals": {
            "trend": {"score": float, "detail": str, "weight": 0.30},
            "momentum": {"score": float, "detail": str, "weight": 0.25},
            "volatility": {"score": float, "detail": str, "weight": 0.20},
            "oversold_overbought": {"score": float, "detail": str, "weight": 0.15},
            "volume": {"score": float, "detail": str, "weight": 0.10},
        }
    }
"""

import pandas as pd


# ===== 评分映射表 =====

MA_SIGNAL_MAP = {
    "多头排列": 100, "多头": 75, "中性": 50, "空头": 25, "空头排列": 0,
}

TREND_MAP = {
    "上升趋势": 90, "震荡上行": 75, "震荡整理": 50,
    "震荡下行": 25, "下降趋势": 10,
}

MACD_MAP = {"金叉": 85, "多头": 70, "中性": 50, "空头": 30, "死叉": 15}

KDJ_MAP = {"金叉": 80, "超卖": 85, "中性": 50, "超买": 15, "死叉": 20}


def _rsi_to_score(rsi):
    """RSI 值转换为 0-100 评分（超卖=高分反弹预期，超买=低分回落风险）。"""
    if pd.isna(rsi):
        return 50
    if rsi <= 20:
        return 95
    if rsi <= 30:
        return 85
    if rsi <= 40:
        return 65
    if rsi <= 60:
        return 50
    if rsi <= 70:
        return 35
    if rsi <= 80:
        return 15
    return 5


def _boll_to_score(boll):
    """布林带位置转换为 0-100 评分（下轨=高分超卖，上轨=低分超买）。"""
    if pd.isna(boll):
        return 50
    if boll <= 10:
        return 90
    if boll <= 25:
        return 75
    if boll <= 40:
        return 60
    if boll <= 60:
        return 50
    if boll <= 75:
        return 40
    if boll <= 90:
        return 25
    return 10


def _atr_to_score(atr):
    """ATR 百分比转换为 0-100 评分（低波动=高分稳定，高波动=低分风险）。"""
    if pd.isna(atr):
        return 50
    if atr <= 1.0:
        return 80
    if atr <= 1.5:
        return 70
    if atr <= 2.5:
        return 50
    if atr <= 4.0:
        return 30
    return 10


def _score_trend(row):
    """趋势信号评分（0-100），权重 30%。"""
    ma_score = MA_SIGNAL_MAP.get(row.get("ma_signal", "中性"), 50)
    trend_score = TREND_MAP.get(row.get("trend", "震荡整理"), 50)
    combined = ma_score * 0.6 + trend_score * 0.4

    ma = row.get("ma_signal", "?")
    trd = row.get("trend", "?")
    if combined >= 80:
        detail = "均线{}，趋势{}，趋势强劲".format(ma, trd)
    elif combined >= 60:
        detail = "均线{}，趋势{}，偏多".format(ma, trd)
    elif combined >= 40:
        detail = "均线{}，趋势{}，方向不明".format(ma, trd)
    elif combined >= 20:
        detail = "均线{}，趋势{}，偏空".format(ma, trd)
    else:
        detail = "均线{}，趋势{}，趋势疲弱".format(ma, trd)

    return {"score": combined, "detail": detail, "weight": 0.30}


def _score_momentum(row):
    """动量信号评分（0-100），权重 25%。"""
    macd_score = MACD_MAP.get(row.get("macd_signal", "中性"), 50)
    rsi_score = _rsi_to_score(row.get("rsi_value", 50))
    kdj_score = KDJ_MAP.get(row.get("kdj_signal", "中性"), 50)

    combined = macd_score * 0.35 + rsi_score * 0.40 + kdj_score * 0.25

    rsi_val = row.get("rsi_value", 50)
    rsi_label = row.get("rsi_status", "正常")
    rsi_str = "{:.1f}".format(float(rsi_val)) if pd.notna(rsi_val) else "?"
    detail = "MACD {}，RSI {}（{}），KDJ {}".format(
        row.get("macd_signal", "?"), rsi_str, rsi_label, row.get("kdj_signal", "?")
    )

    return {"score": combined, "detail": detail, "weight": 0.25}


def _score_volatility(row):
    """波动率信号评分（0-100），权重 20%。"""
    boll_score = _boll_to_score(row.get("bollinger_position", 50))
    atr_score = _atr_to_score(row.get("atr_pct", 2.0))

    combined = boll_score * 0.65 + atr_score * 0.35

    boll = row.get("bollinger_position", 50)
    atr = row.get("atr_pct", 2.0)
    boll_str = "{:.1f}".format(float(boll)) if pd.notna(boll) else "?"
    atr_str = "{:.2f}%".format(float(atr)) if pd.notna(atr) else "?%"

    if pd.notna(boll) and boll <= 20:
        detail = "布林位置 {}（接近下轨），ATR {}".format(boll_str, atr_str)
    elif pd.notna(boll) and boll >= 80:
        detail = "布林位置 {}（接近上轨），ATR {}".format(boll_str, atr_str)
    else:
        detail = "布林位置 {}（中轨附近），ATR {}".format(boll_str, atr_str)

    return {"score": combined, "detail": detail, "weight": 0.20}


def _score_oversold_overbought(row):
    """超买超卖综合评分（0-100），权重 15%。"""
    rsi = row.get("rsi_value", 50)
    boll = row.get("bollinger_position", 50)
    kdj = row.get("kdj_signal", "中性")

    oversold_count = 0
    overbought_count = 0

    if pd.notna(rsi):
        if rsi < 30:
            oversold_count += 1
        elif rsi > 70:
            overbought_count += 1

    if pd.notna(boll):
        if boll < 20:
            oversold_count += 1
        elif boll > 80:
            overbought_count += 1

    if kdj in ("超卖", "金叉"):
        oversold_count += 1
    elif kdj in ("超买", "死叉"):
        overbought_count += 1

    total = 3
    if oversold_count > 0 and overbought_count == 0:
        score = 50 + (oversold_count / total) * 50
        detail = "超卖信号 {}/{}, 反弹预期".format(oversold_count, total)
    elif overbought_count > 0 and oversold_count == 0:
        score = 50 - (overbought_count / total) * 50
        detail = "超买信号 {}/{}, 回调风险".format(overbought_count, total)
    elif oversold_count > 0 and overbought_count > 0:
        score = 50
        detail = "多空信号矛盾，建议观望"
    else:
        score = 50
        detail = "无明显超买超卖信号"

    return {"score": score, "detail": detail, "weight": 0.15}


def _score_volume(row):
    """成交量信号评分（0-100），权重 10%。

    etf_technical 表不含成交量字段，基础版本返回中性分。
    """
    return {"score": 50, "detail": "成交量信号需结合资金流数据综合判断", "weight": 0.10}


def compute_signal_score(row):
    """计算单只 ETF 的综合技术信号评分。

    Parameters
    ----------
    row : pd.Series
        etf_technical 表的一行数据

    Returns
    -------
    dict : {"total_score": float, "grade": str, "signals": {...}}
    """
    signals = {
        "trend": _score_trend(row),
        "momentum": _score_momentum(row),
        "volatility": _score_volatility(row),
        "oversold_overbought": _score_oversold_overbought(row),
        "volume": _score_volume(row),
    }

    total_score = sum(s["score"] * s["weight"] for s in signals.values())

    if total_score >= 75:
        grade = "强烈买入"
    elif total_score >= 60:
        grade = "买入"
    elif total_score >= 40:
        grade = "持有"
    elif total_score >= 25:
        grade = "卖出"
    else:
        grade = "强烈卖出"

    return {
        "total_score": round(total_score, 1),
        "grade": grade,
        "signals": signals,
    }


def compute_signal_scores(df):
    """批量计算多只 ETF 的技术信号评分。

    Parameters
    ----------
    df : pd.DataFrame
        etf_technical 表数据，需包含 code 列

    Returns
    -------
    pd.DataFrame : 包含 code, total_score, grade, trend_score, momentum_score,
                   volatility_score, ob_score, volume_score 列
    """
    if df.empty:
        return pd.DataFrame()

    results = []
    for _, row in df.iterrows():
        score = compute_signal_score(row)
        results.append({
            "code": row.get("code", ""),
            "date": row.get("date", ""),
            "total_score": score["total_score"],
            "grade": score["grade"],
            "trend_score": score["signals"]["trend"]["score"],
            "momentum_score": score["signals"]["momentum"]["score"],
            "volatility_score": score["signals"]["volatility"]["score"],
            "ob_score": score["signals"]["oversold_overbought"]["score"],
            "volume_score": score["signals"]["volume"]["score"],
        })

    return pd.DataFrame(results)
