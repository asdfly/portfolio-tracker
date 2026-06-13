"""ETF 单品风险全景扫描模块

对单只 ETF 进行 5 维风险扫描，输出综合风险评分（0-100，越高越危险）。
维度：波动率(25%)、折价风险(20%)、流动性(20%)、下行压力(20%)、偏离度(15%)。
数据来源：etf_technical（技术指标）、etf_fundamental（基本面）、portfolio_snapshots（历史净值）。
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Optional


def _score_volatility(tech_row: pd.Series, hist_prices: Optional[pd.Series] = None) -> Dict[str, Any]:
    """波动率维度评分（0-100，越高风险越大），权重 25%。"""
    score = 50
    details = []

    atr = tech_row.get("atr_pct", None)
    if pd.notna(atr):
        atr = float(atr)
        if atr >= 5.0:
            atr_score, atr_label = 90, "极高"
        elif atr >= 3.5:
            atr_score, atr_label = 75, "偏高"
        elif atr >= 2.0:
            atr_score, atr_label = 50, "正常"
        elif atr >= 1.0:
            atr_score, atr_label = 30, "偏低"
        else:
            atr_score, atr_label = 15, "极低"
        details.append(f"ATR {atr:.2f}%（{atr_label}）")
    else:
        atr_score = 50

    boll = tech_row.get("bollinger_position", None)
    if pd.notna(boll):
        boll_risk = abs(float(boll) - 50) / 50 * 100
        details.append(f"布林位置 {float(boll):.1f}")
    else:
        boll_risk = 0

    hist_vol = 50
    if hist_prices is not None and len(hist_prices) >= 20:
        returns = hist_prices.pct_change().dropna()
        if len(returns) >= 10:
            daily_vol = returns.std() * np.sqrt(252) * 100
            if daily_vol >= 35:
                hist_vol, vol_label = 90, "极高"
            elif daily_vol >= 25:
                hist_vol, vol_label = 70, "偏高"
            elif daily_vol >= 15:
                hist_vol, vol_label = 45, "正常"
            else:
                hist_vol, vol_label = 20, "偏低"
            details.append(f"年化波动率 {daily_vol:.1f}%（{vol_label}）")

    score = atr_score * 0.35 + boll_risk * 0.25 + hist_vol * 0.40
    score = float(np.clip(score, 0, 100))

    return {
        "score": round(score, 1),
        "detail": "；".join(details) if details else "数据不足",
        "weight": 0.25,
        "level": "高风险" if score >= 70 else ("中风险" if score >= 45 else "低风险"),
    }


def _score_discount(fund_row: pd.Series) -> Dict[str, Any]:
    """折价风险维度评分（0-100，越高风险越大），权重 20%。"""
    score = 50
    details = []

    discount = fund_row.get("discount_rate", None)
    if pd.notna(discount):
        discount = float(discount)
        if discount <= -1.0:
            score, label = 85, "大幅折价"
        elif discount <= -0.3:
            score, label = 65, "折价"
        elif discount <= 0.3:
            score, label = 40, "小幅溢价"
        elif discount <= 1.0:
            score, label = 55, "溢价"
        else:
            score, label = 80, "大幅溢价"
        details.append(f"折价率 {discount:.2f}%（{label}）")

    volume_ratio = fund_row.get("volume_ratio", None)
    if pd.notna(volume_ratio):
        vr = float(volume_ratio)
        if vr >= 3.0:
            details.append(f"量比 {vr:.1f}（异常放量）")
        elif vr >= 1.5:
            details.append(f"量比 {vr:.1f}（温和放量）")
        elif vr <= 0.5:
            details.append(f"量比 {vr:.1f}（缩量）")
        else:
            details.append(f"量比 {vr:.1f}（正常）")

    turnover = fund_row.get("turnover_rate", None)
    if pd.notna(turnover):
        details.append(f"换手率 {turnover:.2f}%")

    return {
        "score": round(float(score), 1),
        "detail": "；".join(details) if details else "数据不足",
        "weight": 0.20,
        "level": "高风险" if score >= 70 else ("中风险" if score >= 45 else "低风险"),
    }


def _score_liquidity(fund_row: pd.Series, tech_row: pd.Series) -> Dict[str, Any]:
    """流动性风险维度评分（0-100，越高风险越大），权重 20%。"""
    score = 50
    details = []

    amount = fund_row.get("amount", None)
    if pd.notna(amount):
        amount_val = float(amount)
        if amount_val >= 1e9:
            amt_score, amt_label = 15, "超活跃（>10亿）"
        elif amount_val >= 3e8:
            amt_score, amt_label = 25, "活跃（3-10亿）"
        elif amount_val >= 1e8:
            amt_score, amt_label = 40, "正常（1-3亿）"
        elif amount_val >= 3e7:
            amt_score, amt_label = 60, "偏低（0.3-1亿）"
        else:
            amt_score, amt_label = 85, "极低（<0.3亿）"
        details.append(f"成交额 {amount_val / 1e8:.2f}亿（{amt_label}）")
        score = amt_score
    else:
        volume = fund_row.get("volume", None)
        if pd.notna(volume):
            vol_val = float(volume)
            details.append(f"成交量 {vol_val / 1e4:.0f}万手")
            if vol_val < 1e6:
                score = 70

    shares = fund_row.get("shares", None)
    if pd.notna(shares):
        shares_val = float(shares)
        if shares_val >= 1e9:
            details.append(f"份额 {shares_val / 1e8:.1f}亿份（大盘）")
        elif shares_val >= 1e8:
            details.append(f"份额 {shares_val / 1e8:.1f}亿份（中盘）")
        elif shares_val >= 5e7:
            details.append(f"份额 {shares_val / 1e8:.2f}亿份（偏小）")
        else:
            details.append(f"份额 {shares_val / 1e8:.2f}亿份（迷你盘，清盘风险）")
            score = max(score, 80)

    return {
        "score": round(float(score), 1),
        "detail": "；".join(details) if details else "数据不足",
        "weight": 0.20,
        "level": "高风险" if score >= 70 else ("中风险" if score >= 45 else "低风险"),
    }


def _score_downside(tech_row: pd.Series) -> Dict[str, Any]:
    """下行压力维度评分（0-100，越高下行压力越大），权重 20%。"""
    ma_map = {"多头排列": 10, "多头": 25, "中性": 50, "空头": 75, "空头排列": 95}
    macd_map = {"金叉": 15, "多头": 30, "中性": 50, "空头": 70, "死叉": 90}
    kdj_map = {"金叉": 20, "超卖": 25, "中性": 50, "超买": 75, "死叉": 85}
    trend_map = {"上升趋势": 10, "震荡上行": 25, "震荡整理": 50, "震荡下行": 75, "下降趋势": 95}

    ma_signal = tech_row.get("ma_signal", "中性")
    macd_signal = tech_row.get("macd_signal", "中性")
    kdj_signal = tech_row.get("kdj_signal", "中性")
    trend = tech_row.get("trend", "震荡整理")

    ma_score = ma_map.get(ma_signal, 50)
    macd_score = macd_map.get(macd_signal, 50)
    kdj_score = kdj_map.get(kdj_signal, 50)
    trend_score = trend_map.get(trend, 50)

    score = ma_score * 0.30 + macd_score * 0.25 + kdj_score * 0.20 + trend_score * 0.25
    score = float(np.clip(score, 0, 100))

    details = [f"均线 {ma_signal}", f"MACD {macd_signal}",
               f"KDJ {kdj_signal}", f"趋势 {trend}"]
    bearish = sum(1 for s in [ma_signal, macd_signal, kdj_signal, trend]
                  if s in ("空头排列", "空头", "死叉", "震荡下行", "下降趋势"))
    if bearish >= 3:
        details.append("极强空头共振")
    elif bearish >= 2:
        details.append("偏空")
    else:
        details.append("多空分歧" if bearish == 1 else "无明显下行压力")

    return {
        "score": round(score, 1),
        "detail": "；".join(details),
        "weight": 0.20,
        "level": "高风险" if score >= 70 else ("中风险" if score >= 45 else "低风险"),
    }


def _score_deviation(fund_row: pd.Series, hist_snapshot: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """跟踪偏离度维度评分（0-100，越高偏离越大），权重 15%。"""
    score = 50
    details = []

    iopv = fund_row.get("iopv", None)
    price = fund_row.get("price", None)
    if pd.notna(iopv) and pd.notna(price) and float(iopv) > 0:
        dev = abs(float(price) - float(iopv)) / float(iopv) * 100
        if dev <= 0.1:
            details.append(f"IOPV偏离 {dev:.3f}%（正常）")
        elif dev <= 0.5:
            details.append(f"IOPV偏离 {dev:.2f}%（偏大）")
            score = max(score, 55)
        else:
            details.append(f"IOPV偏离 {dev:.2f}%（异常）")
            score = max(score, 75)

    beta = fund_row.get("beta", None)
    if pd.isna(beta) and hist_snapshot is not None and not hist_snapshot.empty and "beta" in hist_snapshot.columns:
        beta_val = hist_snapshot["beta"].iloc[-1]
        if pd.notna(beta_val):
            beta = beta_val
    if pd.notna(beta):
        b = float(beta)
        if abs(b - 1.0) <= 0.1:
            details.append(f"Beta {b:.2f}（紧密跟踪）")
        elif abs(b - 1.0) <= 0.3:
            details.append(f"Beta {b:.2f}（略有偏离）")
        elif abs(b - 1.0) <= 0.8:
            details.append(f"Beta {b:.2f}（行业特征偏离）")
            score = max(score, 50 + abs(b - 1.0) * 15)
        elif abs(b - 1.0) <= 1.5:
            details.append(f"Beta {b:.2f}（高偏离）")
            score = max(score, 60 + (abs(b - 1.0) - 0.8) * 20)
        else:
            details.append(f"Beta {b:.2f}（极端偏离）")
            score = max(score, 80)

    return {
        "score": round(float(np.clip(score, 0, 100)), 1),
        "detail": "；".join(details) if details else "数据不足",
        "weight": 0.15,
        "level": "高风险" if score >= 70 else ("中风险" if score >= 45 else "低风险"),
    }


def compute_etf_risk_scan(
    code: str,
    tech_row: Optional[pd.Series] = None,
    fund_row: Optional[pd.Series] = None,
    hist_prices: Optional[pd.Series] = None,
    hist_snapshot: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """计算单只 ETF 的风险全景评分。

    Parameters
    ----------
    code : str
        ETF 代码
    tech_row : pd.Series, optional
        etf_technical 最新一行
    fund_row : pd.Series, optional
        etf_fundamental 最新一行
    hist_prices : pd.Series, optional
        历史收盘价序列（用于波动率计算）
    hist_snapshot : pd.DataFrame, optional
        portfolio_snapshots 中该 ETF 的历史数据

    Returns
    -------
    dict : {code, total_score, risk_level, grade, dimensions, summary}
    """
    empty = pd.Series(dtype=float)
    tech = tech_row if tech_row is not None else empty
    fund = fund_row if fund_row is not None else empty

    dimensions = {
        "volatility": _score_volatility(tech, hist_prices),
        "discount": _score_discount(fund),
        "liquidity": _score_liquidity(fund, tech),
        "downside": _score_downside(tech),
        "deviation": _score_deviation(fund, hist_snapshot),
    }

    total_score = sum(d["score"] * d["weight"] for d in dimensions.values())

    if total_score >= 70:
        risk_level, grade = "高风险", "危险"
    elif total_score >= 55:
        risk_level, grade = "中高风险", "警告"
    elif total_score >= 40:
        risk_level, grade = "中等风险", "关注"
    elif total_score >= 25:
        risk_level, grade = "中低风险", "安全"
    else:
        risk_level, grade = "低风险", "非常安全"

    dim_labels = {"volatility": "波动率", "discount": "折价", "liquidity": "流动性",
                  "downside": "下行压力", "deviation": "偏离度"}
    high_risk_dims = [k for k, d in dimensions.items() if d["score"] >= 65]
    if len(high_risk_dims) >= 3:
        summary = f"多维度风险共振（{len(high_risk_dims)}/5），建议减仓或回避"
    elif len(high_risk_dims) >= 2:
        summary = f"部分维度风险偏高（{len(high_risk_dims)}/5），建议密切关注"
    elif len(high_risk_dims) == 1:
        summary = f"{dim_labels.get(high_risk_dims[0], '')}偏高，其他维度正常"
    else:
        summary = f"整体风险可控，各维度均处于安全区间"

    return {
        "code": code,
        "total_score": round(total_score, 1),
        "risk_level": risk_level,
        "grade": grade,
        "dimensions": dimensions,
        "summary": summary,
    }


def compute_all_etf_risk_scans(
    tech_df: pd.DataFrame,
    fund_df: pd.DataFrame,
    snapshot_df: Optional[pd.DataFrame] = None,
    price_cache: Optional[Dict[str, pd.Series]] = None,
) -> pd.DataFrame:
    """批量计算所有 ETF 的风险评分。

    Parameters
    ----------
    tech_df : pd.DataFrame - etf_technical 数据（最新日期）
    fund_df : pd.DataFrame - etf_fundamental 数据
    snapshot_df : pd.DataFrame, optional - portfolio_snapshots
    price_cache : dict, optional - {code: pd.Series(historical_prices)}

    Returns
    -------
    pd.DataFrame : code, total_score, risk_level, grade, 各维度分数
    """
    if tech_df.empty or fund_df.empty:
        return pd.DataFrame()

    results = []
    for code in fund_df["code"].astype(str).unique():
        tech_rows = tech_df[tech_df["code"].astype(str) == code]
        fund_rows = fund_df[fund_df["code"].astype(str) == code]

        tr = tech_rows.iloc[-1] if not tech_rows.empty else None
        fr = fund_rows.iloc[0] if not fund_rows.empty else None

        hp = price_cache.get(code) if price_cache else None
        hs = None
        if snapshot_df is not None and not snapshot_df.empty:
            hs_snap = snapshot_df[snapshot_df["code"].astype(str) == code]
            hs = hs_snap if not hs_snap.empty else None

        scan = compute_etf_risk_scan(code, tr, fr, hp, hs)
        results.append({
            "code": code,
            "total_score": scan["total_score"],
            "risk_level": scan["risk_level"],
            "grade": scan["grade"],
            "vol_score": scan["dimensions"]["volatility"]["score"],
            "disc_score": scan["dimensions"]["discount"]["score"],
            "liq_score": scan["dimensions"]["liquidity"]["score"],
            "down_score": scan["dimensions"]["downside"]["score"],
            "dev_score": scan["dimensions"]["deviation"]["score"],
        })

    return pd.DataFrame(results)
