"""
ETF 技术信号综合评分模块

基于 etf_technical 表的多维指标数据，计算单只 ETF 的综合技术评分（0-100）。
信号维度：趋势(30%)、动量(25%)、波动(20%)、超买超卖(15%)、成交量(10%)。
"""

import pandas as pd


def _score_trend(row):
    """趋势信号评分（0-100），权重 30%。"""
    ma_map = {"多头排列": 100, "多头": 75, "中性": 50, "空头": 25, "空头排列": 0}
    trend_map = {"上升趋势": 90, "震荡上行": 75, "震荡整理": 50, "震荡下行": 25, "下降趋势": 10}
    ma_score = ma_map.get(row.get("ma_signal", "中性"), 50)
    trend_score = trend_map.get(row.get("trend", "震荡整理"), 50)
    combined = ma_score * 0.6 + trend_score * 0.4
    ma = row.get("ma_signal", "?")
    tr = row.get("trend", "?")
    if combined >= 80:
        detail = "均线%s，趋势%s，趋势强劲" % (ma, tr)
    elif combined >= 60:
        detail = "均线%s，趋势%s，偏多" % (ma, tr)
    elif combined >= 40:
        detail = "均线%s，趋势%s，方向不明" % (ma, tr)
    elif combined >= 20:
        detail = "均线%s，趋势%s，偏空" % (ma, tr)
    else:
        detail = "均线%s，趋势%s，趋势疲弱" % (ma, tr)
    return {"score": combined, "detail": detail, "weight": 0.30}


def _score_momentum(row):
    """动量信号评分（0-100），权重 25%。"""
    macd_map = {"金叉": 85, "多头": 70, "中性": 50, "空头": 30, "死叉": 15}
    macd_score = macd_map.get(row.get("macd_signal", "中性"), 50)

    rsi = row.get("rsi_value", 50)
    if pd.isna(rsi):
        rsi_score = 50
    elif rsi <= 20:
        rsi_score = 95
    elif rsi <= 30:
        rsi_score = 85
    elif rsi <= 40:
        rsi_score = 65
    elif rsi <= 60:
        rsi_score = 50
    elif rsi <= 70:
        rsi_score = 35
    elif rsi <= 80:
        rsi_score = 15
    else:
        rsi_score = 5

    kdj_map = {"金叉": 80, "超卖": 85, "中性": 50, "超买": 15, "死叉": 20}
    kdj_score = kdj_map.get(row.get("kdj_signal", "中性"), 50)
    combined = macd_score * 0.35 + rsi_score * 0.40 + kdj_score * 0.25

    rsi_val = round(rsi, 1) if pd.notna(rsi) else "?"
    rsi_label = row.get("rsi_status", "正常")
    detail = "MACD %s，RSI %s（%s），KDJ %s" % (row.get("macd_signal", "?"), rsi_val, rsi_label, row.get("kdj_signal", "?"))
    return {"score": combined, "detail": detail, "weight": 0.25}


def _score_volatility(row):
    """波动率信号评分（0-100），权重 20%。"""
    boll = row.get("bollinger_position", 50)
    if pd.isna(boll):
        boll_score = 50
    elif boll <= 10:
        boll_score = 90
    elif boll <= 25:
        boll_score = 75
    elif boll <= 40:
        boll_score = 60
    elif boll <= 60:
        boll_score = 50
    elif boll <= 75:
        boll_score = 40
    elif boll <= 90:
        boll_score = 25
    else:
        boll_score = 10

    atr = row.get("atr_pct", 2.0)
    if pd.isna(atr):
        atr_score = 50
    elif atr <= 1.0:
        atr_score = 80
    elif atr <= 1.5:
        atr_score = 70
    elif atr <= 2.5:
        atr_score = 50
    elif atr <= 4.0:
        atr_score = 30
    else:
        atr_score = 10

    combined = boll_score * 0.65 + atr_score * 0.35
    boll_str = "接近下轨" if boll <= 20 else ("接近上轨" if boll >= 80 else "中轨附近")
    atr_str = "正常" if atr <= 2.5 else "偏高"
    detail = "布林位置 %.1f（%s），ATR %.2f%%（%s）" % (boll if pd.notna(boll) else 50, boll_str, atr if pd.notna(atr) else 2.0, atr_str)
    return {"score": combined, "detail": detail, "weight": 0.20}


def _score_oversold_overbought(row):
    """超买超卖综合评分（0-100），权重 15%。"""
    rsi = row.get("rsi_value", 50)
    boll = row.get("bollinger_position", 50)
    kdj = row.get("kdj_signal", "中性")
    oversold_count = 0
    overbought_count = 0
    if pd.notna(rsi):
        if rsi < 30: oversold_count += 1
        elif rsi > 70: overbought_count += 1
    if pd.notna(boll):
        if boll < 20: oversold_count += 1
        elif boll > 80: overbought_count += 1
    if kdj in ("超卖", "金叉"): oversold_count += 1
    elif kdj in ("超买", "死叉"): overbought_count += 1
    total = 3
    if oversold_count > 0 and overbought_count == 0:
        score = 50 + (oversold_count / total) * 50
        status = "超卖信号 %d/%d，反弹预期" % (oversold_count, total)
    elif overbought_count > 0 and oversold_count == 0:
        score = 50 - (overbought_count / total) * 50
        status = "超买信号 %d/%d，回调风险" % (overbought_count, total)
    elif oversold_count > 0 and overbought_count > 0:
        score = 50
        status = "多空信号矛盾，建议观望"
    else:
        score = 50
        status = "无明显超买超卖信号"
    return {"score": score, "detail": status, "weight": 0.15}


def _score_volume(row):
    """成交量信号评分（0-100），权重 10%。etf_technical 无直接成交量字段，返回中性。"""
    return {"score": 50, "detail": "成交量信号需结合资金流数据综合判断", "weight": 0.10}


def compute_signal_score(row):
    """计算单只 ETF 的综合技术信号评分。

    Parameters
    ----------
    row : pd.Series
        etf_technical 表的一行数据

    Returns
    -------
    dict : {total_score, grade, signals}
    """
    signals = {
        "trend": _score_trend(row),
        "momentum": _score_momentum(row),
        "volatility": _score_volatility(row),
        "oversold_overbought": _score_oversold_overbought(row),
        "volume": _score_volume(row),
    }
    total_score = sum(s["score"] * s["weight"] for s in signals.values())
    if total_score >= 75: grade = "强烈买入"
    elif total_score >= 60: grade = "买入"
    elif total_score >= 40: grade = "持有"
    elif total_score >= 25: grade = "卖出"
    else: grade = "强烈卖出"
    return {"total_score": round(total_score, 1), "grade": grade, "signals": signals}


def compute_signal_scores(df):
    """批量计算多只 ETF 的技术信号评分。

    Parameters
    ----------
    df : pd.DataFrame
        etf_technical 表数据

    Returns
    -------
    pd.DataFrame : code, date, total_score, grade, 各维度分数
    """
    if df.empty:
        return pd.DataFrame()
    results = []
    for _, row in df.iterrows():
        sc = compute_signal_score(row)
        results.append({
            "code": row.get("code", ""),
            "date": row.get("date", ""),
            "total_score": sc["total_score"],
            "grade": sc["grade"],
            "trend_score": sc["signals"]["trend"]["score"],
            "momentum_score": sc["signals"]["momentum"]["score"],
            "volatility_score": sc["signals"]["volatility"]["score"],
            "ob_score": sc["signals"]["oversold_overbought"]["score"],
            "volume_score": sc["signals"]["volume"]["score"],
        })
    return pd.DataFrame(results)