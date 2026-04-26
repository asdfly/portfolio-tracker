"""
技术分析模块 - 计算各类技术指标
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class TechnicalAnalyzer:
    """技术分析器"""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}

    def calculate_all(self, kline_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算所有技术指标"""
        if not kline_data or len(kline_data) < 30:
            logger.warning("K线数据不足，无法计算完整技术指标")
            return {}

        closes = np.array([d['close'] for d in kline_data])
        highs = np.array([d['high'] for d in kline_data])
        lows = np.array([d['low'] for d in kline_data])
        volumes = np.array([d['volume'] for d in kline_data])

        results = {
            'ma': self.calculate_ma(closes),
            'macd': self.calculate_macd(closes),
            'rsi': self.calculate_rsi(closes),
            'kdj': self.calculate_kdj(highs, lows, closes),
            'bollinger': self.calculate_bollinger(closes),
            'atr': self.calculate_atr(highs, lows, closes),
            'volume_ma': self.calculate_volume_ma(volumes),
            'trend': self.analyze_trend(closes, volumes),
        }

        return results

    def calculate_ma(self, closes: np.ndarray, fast: int = 5, slow: int = 20) -> Dict[str, Any]:
        """计算移动平均线"""
        if len(closes) < slow:
            return {}

        ma_fast = np.mean(closes[-fast:])
        ma_slow = np.mean(closes[-slow:])

        # 判断金叉/死叉
        prev_fast = np.mean(closes[-fast-1:-1])
        prev_slow = np.mean(closes[-slow-1:-1])

        signal = '金叉' if ma_fast > ma_slow and prev_fast <= prev_slow else \
                 ('死叉' if ma_fast < ma_slow and prev_fast >= prev_slow else \
                  ('多头排列' if ma_fast > ma_slow else '空头排列'))

        return {
            'MA5': round(ma_fast, 4),
            'MA20': round(ma_slow, 4),
            'signal': signal,
            'trend': '上升' if ma_fast > ma_slow else '下降'
        }

    def calculate_macd(self, closes: np.ndarray, fast: int = 12, 
                       slow: int = 26, signal: int = 9) -> Dict[str, Any]:
        """计算MACD指标"""
        if len(closes) < slow + signal:
            return {}

        # 计算EMA
        ema_fast = self._ema(closes, fast)
        ema_slow = self._ema(closes, slow)

        # DIF = EMA12 - EMA26
        dif = ema_fast - ema_slow

        # DEA = EMA(DIF, 9)
        dea = self._ema(dif, signal)

        # MACD = 2 * (DIF - DEA)
        macd = 2 * (dif - dea)

        # 判断信号
        current_signal = '金叉' if dif[-1] > dea[-1] and dif[-2] <= dea[-2] else \
                        ('死叉' if dif[-1] < dea[-1] and dif[-2] >= dea[-2] else '中性')

        # 判断背离
        divergence = self._check_divergence(closes, macd)

        return {
            'DIF': round(dif[-1], 4),
            'DEA': round(dea[-1], 4),
            'MACD': round(macd[-1], 4),
            'signal': current_signal,
            'divergence': divergence,
            'trend': '多头' if macd[-1] > 0 else '空头'
        }

    def calculate_rsi(self, closes: np.ndarray, period: int = 14) -> Dict[str, Any]:
        """计算RSI指标"""
        if len(closes) < period + 1:
            return {}

        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        # 使用简单移动平均计算RS
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])

        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        # 判断状态
        if rsi > 80:
            status = '严重超买'
        elif rsi > 70:
            status = '超买'
        elif rsi < 20:
            status = '严重超卖'
        elif rsi < 30:
            status = '超卖'
        else:
            status = '正常'

        return {
            'RSI': round(rsi, 2),
            'status': status,
            'signal': '卖出' if rsi > 70 else ('买入' if rsi < 30 else '观望')
        }

    def calculate_kdj(self, highs: np.ndarray, lows: np.ndarray, 
                      closes: np.ndarray, n: int = 9, m1: int = 3, m2: int = 3) -> Dict[str, Any]:
        """计算KDJ指标"""
        if len(closes) < n:
            return {}

        # RSV = (收盘价 - N日内最低价) / (N日内最高价 - N日内最低价) * 100
        rsv_list = []
        for i in range(n-1, len(closes)):
            period_high = np.max(highs[i-n+1:i+1])
            period_low = np.min(lows[i-n+1:i+1])
            if period_high == period_low:
                rsv = 50
            else:
                rsv = (closes[i] - period_low) / (period_high - period_low) * 100
            rsv_list.append(rsv)

        rsv = np.array(rsv_list)

        # K = 2/3 * 前K + 1/3 * RSV
        # D = 2/3 * 前D + 1/3 * K
        k = np.zeros_like(rsv)
        d = np.zeros_like(rsv)

        k[0] = rsv[0]
        d[0] = rsv[0]

        for i in range(1, len(rsv)):
            k[i] = 2/3 * k[i-1] + 1/3 * rsv[i]
            d[i] = 2/3 * d[i-1] + 1/3 * k[i]

        # J = 3K - 2D
        j = 3 * k - 2 * d

        # 判断信号
        current_k, current_d, current_j = k[-1], d[-1], j[-1]

        if current_k > current_d and k[-2] <= d[-2]:
            signal = '金叉'
        elif current_k < current_d and k[-2] >= d[-2]:
            signal = '死叉'
        else:
            signal = '中性'

        if current_j > 100:
            status = '超买区'
        elif current_j < 0:
            status = '超卖区'
        else:
            status = '徘徊区'

        return {
            'K': round(current_k, 2),
            'D': round(current_d, 2),
            'J': round(current_j, 2),
            'signal': signal,
            'status': status
        }

    def calculate_bollinger(self, closes: np.ndarray, period: int = 20, 
                           num_std: int = 2) -> Dict[str, Any]:
        """计算布林带"""
        if len(closes) < period:
            return {}

        ma = np.mean(closes[-period:])
        std = np.std(closes[-period:])

        upper = ma + num_std * std
        lower = ma - num_std * std

        current_price = closes[-1]

        # 计算价格在布林带中的位置 (0-100%)
        if upper != lower:
            position = (current_price - lower) / (upper - lower) * 100
        else:
            position = 50

        # 判断状态
        if current_price > upper:
            status = '突破上轨'
        elif current_price < lower:
            status = '突破下轨'
        elif position > 80:
            status = '接近上轨'
        elif position < 20:
            status = '接近下轨'
        else:
            status = '中轨附近'

        # 带宽（波动率指标）
        bandwidth = (upper - lower) / ma * 100 if ma > 0 else 0

        return {
            'upper': round(upper, 4),
            'middle': round(ma, 4),
            'lower': round(lower, 4),
            'position': round(position, 2),
            'status': status,
            'bandwidth': round(bandwidth, 2),
            'squeeze': bandwidth < 10  # 布林带收窄
        }

    def calculate_atr(self, highs: np.ndarray, lows: np.ndarray, 
                      closes: np.ndarray, period: int = 14) -> Dict[str, Any]:
        """计算ATR（平均真实波幅）"""
        if len(closes) < period + 1:
            return {}

        # 真实波幅 = max(当日最高-最低, |当日最高-昨日收盘|, |当日最低-昨日收盘|)
        tr_list = []
        for i in range(1, len(closes)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            tr_list.append(max(tr1, tr2, tr3))

        tr = np.array(tr_list)
        atr = np.mean(tr[-period:])

        # ATR百分比
        atr_pct = atr / closes[-1] * 100

        return {
            'ATR': round(atr, 4),
            'ATR_pct': round(atr_pct, 2),
            'volatility': '高' if atr_pct > 3 else ('中' if atr_pct > 1.5 else '低')
        }

    def calculate_volume_ma(self, volumes: np.ndarray, fast: int = 5, 
                           slow: int = 20) -> Dict[str, Any]:
        """计算成交量均线"""
        if len(volumes) < slow:
            return {}

        vol_ma5 = np.mean(volumes[-fast:])
        vol_ma20 = np.mean(volumes[-slow:])
        current_vol = volumes[-1]

        # 量比
        volume_ratio = current_vol / vol_ma5 if vol_ma5 > 0 else 1

        # 量能趋势
        if current_vol > vol_ma5 * 1.5:
            trend = '放量'
        elif current_vol < vol_ma5 * 0.7:
            trend = '缩量'
        else:
            trend = '正常'

        return {
            'VOL_MA5': round(vol_ma5, 0),
            'VOL_MA20': round(vol_ma20, 0),
            'volume_ratio': round(volume_ratio, 2),
            'trend': trend
        }

    def analyze_trend(self, closes: np.ndarray, volumes: np.ndarray) -> Dict[str, Any]:
        """综合分析趋势"""
        if len(closes) < 20:
            return {}

        # 计算涨跌幅
        change_5d = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else 0
        change_20d = (closes[-1] - closes[-20]) / closes[-20] * 100

        # 计算波动率
        returns = np.diff(closes) / closes[:-1]
        volatility = np.std(returns[-20:]) * np.sqrt(252) * 100  # 年化波动率

        # 判断趋势
        if change_20d > 10:
            trend = '强势上涨'
        elif change_20d > 5:
            trend = '温和上涨'
        elif change_20d < -10:
            trend = '强势下跌'
        elif change_20d < -5:
            trend = '温和下跌'
        else:
            trend = '震荡整理'

        # 连涨连跌天数
        consecutive = 0
        direction = 0
        for i in range(len(closes)-1, 0, -1):
            if closes[i] > closes[i-1]:
                if direction == 1 or direction == 0:
                    consecutive += 1
                    direction = 1
                else:
                    break
            elif closes[i] < closes[i-1]:
                if direction == -1 or direction == 0:
                    consecutive += 1
                    direction = -1
                else:
                    break
            else:
                break

        return {
            'trend': trend,
            'change_5d': round(change_5d, 2),
            'change_20d': round(change_20d, 2),
            'volatility': round(volatility, 2),
            'consecutive_days': consecutive,
            'consecutive_direction': '上涨' if direction == 1 else ('下跌' if direction == -1 else '持平')
        }

    def _ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """计算指数移动平均"""
        multiplier = 2 / (period + 1)
        ema = np.zeros_like(data)
        ema[0] = data[0]

        for i in range(1, len(data)):
            ema[i] = (data[i] - ema[i-1]) * multiplier + ema[i-1]

        return ema

    def _check_divergence(self, prices: np.ndarray, indicator: np.ndarray) -> str:
        """检查背离"""
        if len(prices) < 20 or len(indicator) < 20:
            return '无'

        # 顶背离：价格新高，指标未新高
        price_high_idx = np.argmax(prices[-20:])
        indicator_high_idx = np.argmax(indicator[-20:])

        if price_high_idx > indicator_high_idx and prices[-20+price_high_idx] > prices[-20]:
            return '顶背离'

        # 底背离：价格新低，指标未新低
        price_low_idx = np.argmin(prices[-20:])
        indicator_low_idx = np.argmin(indicator[-20:])

        if price_low_idx > indicator_low_idx and prices[-20+price_low_idx] < prices[-20]:
            return '底背离'

        return '无'
