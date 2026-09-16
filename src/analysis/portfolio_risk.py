"""
组合风险分析器 - 整合风险指标计算
"""
import numpy as np
import pandas as pd
from datetime import date as _date
from typing import Dict, List, Any, Optional, Tuple
import logging

from .risk import RiskAnalyzer
from src.utils.database import DatabaseManager

logger = logging.getLogger(__name__)


# ============================================================================
# 相关性输入口径守卫（2026-09-16 量化整改）
# ----------------------------------------------------------------------------
# 背景：`_analyze_correlations` 的输入是 portfolio_snapshots.current_price（持仓快照价）。
# 场外基金的快照序列是「月末桩 + 复制行 + 日频」混合体（复制行本体见
# docs/handover/07_known_data_issues.md 问题十一：2026-06-15~06-29 共 10 行装的是 06-12 的官方净值），
# 且 2026-07 整月只有 07-31 一条，于是 06-30 -> 07-31 这根 31 天收益被当成「日收益」喂进相关矩阵。
# 实测（真实 34 只持仓，副本 data/backups/portfolio.db.bak_verify_corr_20260916_095652.db）：
#   场外×ETF 242 对均值 ρ 0.1277（含跨期） -> 0.3090（剔除跨期）
#   average_correlation 0.2618 -> 0.3480，diversification_score 0.7382 -> 0.6520
#   对照组 ETF×ETF 462 对 Δ=0.0000（0/462 变化），证明是系统性偏低而非噪声。
# ============================================================================

# 单个「交易日」允许的最大自然日间隔；超出即视为跨期（桩/空洞），该位置不产出收益。
#
# 实测依据（全历史相邻快照行间隔，自然日）：
#   * ETF 侧最长间隔 = 11 天（2026-02-13->02-24 春节、2024-02-08->02-19 春节、
#     2023-09-28->10-09 国庆）；ETF 侧落在 12~20 天区间的间隔数为 0；
#   * 场外侧最小「桩/空洞」间隔 = 15 天（2026-05-31->06-15），另有 28/30/31 天月末桩。
# 合法侧上限 11 与桩侧下限 15 之间存在 12~14 的空档，故取 ≥11 且最紧的 12。
#
# 刻意不依赖 src/utils/trading_calendar.py：其 _HOLIDAY_RANGES 只覆盖 2024/2025/2026，
# 而快照价回溯到 2012，覆盖范围外的日期无法判定，用它反而会引入静默退化。
MAX_SINGLE_SESSION_GAP_DAYS = 12

# 折算/拆分尖峰阈值（单日 log 收益绝对值）；命中即把该观测置 NaN，并把标的记入
# unreliable_codes（聚合指标对此不敏感，必须显式留痕）。
#
# 实测依据：
#   * A 股 ETF 涨跌停 ±20% 对应 |log_ret| ≤ 0.2231，真实行情不会超过它
#     （含 20% 涨跌停的 588000/159949 全历史实测最大 |log_ret| 亦在该范围内）；
#   * 已确认的份额折算：159220 2025-11-07->11-10 单位净值 ×0.5056、份额 ×2.0000
#     → log_ret = -0.6819；512810 2025-06-20->06-23 同为 1:2 折算。
# 0.30 > 0.2231（真实上限）、< 0.6819（折算量级），故能干净区分。
#
# 顺序约束（实测，不可交换）：跨期守卫必须先于本阈值判定。原因：窗口内真实存在的
# 跨期区间收益量级与本阈值重叠——2026-06-30 -> 07-31 这 31 天里 001437 -0.4598、
# 001407 -0.3910、166301 -0.3797、001323 -0.3339、519770 -0.3107 全部超过 0.30。
# 若先判尖峰，这 5 只场外基金会被误标成「折算」写进 unreliable_codes（假阳性）。
SPLIT_SPIKE_LOG_RET = 0.30

# 单日 |log_ret| > SPLIT_SPIKE_LOG_RET 的标的（折算特征）整只标记为不可信。
#
# ⚠️ 这里有两个方向相反的历史误判，口径一律以 docs/handover/07_known_data_issues.md
# 问题十一 为准（该文给出官方单位净值逐条比对）：
#   * `159949` 2024-10-08 的 ±18% 是**真实行情**（创业板涨跌停 20%），"顺手清洗"会删掉真实信息；
#   * `2026-06-30` 场外 +15%~+18% **不是收益**，两重证据：
#       1) 06-30 行的 current_price 逐个精确等于官方 **06-29** 净值，而官方 06-30 当日只涨
#          +3.60%~+4.97%（例 519770 6.9511 → 7.2009）⇒ 那根跳变是「陈旧价 → 真值」的
#          **水平修正**，不是当日涨跌；
#       2) 更关键的是**基期不合法**：其前一行（06-29）装的是官方 **06-12** 的净值
#          （06-15~06-29 为复制行，10 只标的连续 10 行 current_price 与 quantity 全同）
#          ⇒ 被贴上「1 个交易日」标签的那根 +18.51%，实为 **06-12 → 06-29** 的收益
#          （17 自然日 / 11 个交易日）。
#     故 06-30 那行不构成「06-30 当日」的有效观测点，须由**有效基期**（last_real_date）
#     规则拦下，**不能指望本阈值** —— 它 |log_ret| 仅 0.14~0.17，远低于 0.30，会被直接放行。


def _calendar_gap_days(prev: str, cur: str) -> Optional[int]:
    """两个 'YYYY-MM-DD' 之间的自然日间隔；无法解析时返回 None（调用方按不可判定处理）。"""
    try:
        return (_date.fromisoformat(str(cur)) - _date.fromisoformat(str(prev))).days
    except (TypeError, ValueError):
        return None


class PortfolioRiskAnalyzer:
    """组合风险分析器"""

    def __init__(self, risk_free_rate: float = 0.025):
        self.risk_analyzer = RiskAnalyzer(risk_free_rate)
        self.db = DatabaseManager()

    def analyze_portfolio_risk(self, positions: List[Dict[str, Any]],
                               index_quotes: Dict[str, Dict[str, Any]],
                               days: int = 60) -> Dict[str, Any]:
        """分析组合整体风险"""
        logger.info("开始组合风险分析...")

        results = {
            'portfolio_metrics': self._calculate_portfolio_metrics(positions, days),
            'concentration_risk': self._analyze_concentration(positions),
            'correlation_analysis': self._analyze_correlations(positions, days),
            'stress_test': self._run_stress_test(positions),
            'risk_warnings': []
        }

        # 生成风险预警
        results['risk_warnings'] = self._generate_warnings(results)

        logger.info("组合风险分析完成")
        return results

    def _calculate_portfolio_metrics(self, positions: List[Dict[str, Any]], 
                                     days: int = 60) -> Dict[str, Any]:
        """计算组合风险指标"""
        # 从数据库获取历史净值数据
        history = self._get_portfolio_history(days)

        if len(history) < 20:
            logger.warning("历史数据不足，无法计算完整风险指标")
            return {'error': '历史数据不足'}

        # P1-3: 回撤需 60d/1Y/ALL 三档 → 取全历史净值序列供 drawdown 使用；
        # sharpe/波动率仍用最近 `days` 窗口（与历史口径一致）。
        full_history = self._get_portfolio_history(100000)
        full_daily = np.array(
            [h.get('daily_return', 0) or 0 for h in full_history], dtype=float) / 100.0
        if np.any(full_daily != 0):
            full_prices = np.cumprod(1 + full_daily)
        else:
            full_prices = np.array([h['total_value'] for h in full_history], dtype=float)

        # 计算日收益率（使用 corrected daily_return，避免 total_value 跳变影响）
        # daily_return 在 DB 中以百分比格式存储（如 1.5 = 1.5%），需 /100 转小数
        values = np.array([h['total_value'] for h in history])
        daily_returns_pct = np.array([h.get('daily_return', 0) or 0 for h in history])
        # 优先使用 corrected daily_return；若全部为 0 则 fallback 到 total_value.pct_change
        if np.any(daily_returns_pct != 0):
            returns = daily_returns_pct / 100
            # 用全历史 corrected 累积净值供 drawdown 分窗口（60d/1Y/ALL）
            dd_prices = full_prices
        else:
            returns = np.diff(values) / values[:-1]
            dd_prices = values

        # 获取沪深300作为基准
        benchmark_returns = self._get_benchmark_returns('sh000300', days)

        # 计算风险指标（sharpe/波动率用 `returns`(days 窗口)；
        # drawdown 用全历史 `dd_prices`，由 risk.py 内部切 60d/1Y/ALL）
        metrics = self.risk_analyzer.calculate_all(returns, dd_prices, benchmark_returns)

        # 添加组合特定信息
        metrics['data_period'] = len(history)
        metrics['start_date'] = history[0]['date']
        metrics['end_date'] = history[-1]['date']

        return metrics

    def _analyze_concentration(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析集中度风险"""
        total_value = sum(p['market_value'] for p in positions)

        if total_value == 0:
            return {}

        # 计算各品种权重
        weights = np.array([p['market_value'] / total_value for p in positions])

        # 计算赫芬达尔指数
        concentration = self.risk_analyzer.calculate_concentration_risk(weights)

        # 添加品种明细
        position_weights = []
        for pos in positions:
            position_weights.append({
                'code': pos['code'],
                'name': pos['name'],
                'weight': round(pos['market_value'] / total_value * 100, 2),
                'market_value': pos['market_value']
            })

        # 按权重排序
        position_weights.sort(key=lambda x: x['weight'], reverse=True)

        concentration['positions'] = position_weights

        # 行业集中度
        industry_weights = {}
        industry_map = {
            '512010': '医药', '159992': '医药', '515120': '医药',
            '515010': '券商',
            '159267': '军工', '512810': '军工',
            '159796': '新能源', '561910': '新能源', '516160': '新能源',
            '159819': 'AI', '159770': 'AI',
            '159732': '科技',
            '510300': '宽基', '159300': '宽基', '512100': '宽基', 
            '510500': '宽基', '159949': '宽基', '588000': '宽基',
            '563020': '红利', '159220': '红利',
            '159650': '债券', '511520': '债券', '511380': '债券'
        }

        for pos in positions:
            ind = industry_map.get(pos['code'], '其他')
            industry_weights[ind] = industry_weights.get(ind, 0) + pos['market_value']

        # 计算行业HHI
        ind_values = np.array(list(industry_weights.values()))
        ind_weights = ind_values / np.sum(ind_values)
        ind_hhi = np.sum(ind_weights ** 2)

        concentration['industry_hhi'] = round(ind_hhi, 4)
        concentration['industry_distribution'] = [
            {'industry': k, 'value': v, 'weight': round(v/total_value*100, 2)}
            for k, v in sorted(industry_weights.items(), key=lambda x: x[1], reverse=True)
        ]

        return concentration

    def _analyze_correlations(self, positions: List[Dict[str, Any]], 
                              days: int = 60) -> Dict[str, Any]:
        """分析品种间相关性（P0 修复：按日期对齐，容忍不等长序列）。

        各标的可得历史行数不等（场外基金净值回填区间较短，ETF 可达 days 上限），
        原实现把裸 numpy 数组直接塞进 returns_dict，pd.DataFrame 会抛
        "All arrays must be of the same length" 致每日管线中断。
        改为：按 date 升序排序后取日收益，构造带日期索引的 pd.Series，
        由 pandas 按索引对齐（缺失为 NaN），再用 min_periods 控制最小重叠。

        输入口径守卫（2026-09-16，输入是未复权快照价 portfolio_snapshots.current_price）：
          1) 跨期不产出收益：相邻快照自然日间隔 > MAX_SINGLE_SESSION_GAP_DAYS 时，
             该位置记 NaN。场外基金是「月末桩 + 日频」混合序列，桩后第一天原本会
             产出 28~31 天的区间收益并被当成日收益，系统性压低与 ETF 的相关性
             （实测场外×ETF 242 对均值 ρ 0.1277 -> 0.3090）。注意是**置 NaN 而不是
             丢整行/整列**：桩之后的日频段仍然进入矩阵，只是少一个观测点。
          2) 折算尖峰置 NaN：单日 |log_ret| > SPLIT_SPIKE_LOG_RET 属份额折算/拆分
             （非行情），该观测记 NaN 并登记到 unreliable_codes。阈值判定用 log 收益
             （对称），写入矩阵的值仍是简单收益，保持既有口径不变。
          3) overlap_days 改为循环内登记 + 按列刷新：原实现成功返回时用 df 的列
             整表重建，被 continue 掉的标的会从报告里静默消失。
        """
        min_overlap = 20  # 最小有效重叠观测数（不对 days 打折，不静默跳过场外）
        if days <= min_overlap:
            # 窗口不比重叠门槛长时，任何标的都不可能达标，函数必然只剩 'error'。
            # 原实现对此完全沉默，调用方拿到「数据不足」无法区分数据问题与参数问题。
            logger.warning("相关性窗口过窄：days=%d <= min_overlap=%d，"
                           "所有标的都会因有效观测不足被丢弃，相关矩阵必然为空；"
                           "请把 days 调到 > %d", days, min_overlap, min_overlap)

        returns_dict: Dict[str, pd.Series] = {}
        overlap_days: Dict[str, int] = {}
        unreliable_codes: Dict[str, Dict[str, Any]] = {}
        cross_period_voided: Dict[str, int] = {}
        skipped_codes: List[Dict[str, Any]] = []

        for pos in positions:
            code = pos['code']
            label = f"{code}_{str(pos.get('name', ''))[:6]}"
            history = self.db.get_price_history(code, days)
            if len(history) < 2:
                logger.warning("相关性输入不足：%s 只有 %d 行快照价，跳过",
                               label, len(history))
                overlap_days[label] = 0
                skipped_codes.append({'code': label, 'reason': 'rows_lt_2',
                                      'rows': len(history), 'valid_returns': 0})
                continue

            # get_price_history 为 date DESC；必须先升序，否则 diff 的时间轴反向
            hist_sorted = sorted(history, key=lambda h: h['date'])
            dates = [h['date'] for h in hist_sorted]
            values = np.array([h['current_price'] for h in hist_sorted], dtype=float)

            if len(values) < min_overlap + 1:
                # 收益观测不足 min_overlap，记录后丢弃（避免薄样本污染相关矩阵）
                logger.warning("相关性输入过薄：%s 只有 %d 行快照价（需 > %d），丢弃",
                               label, len(values), min_overlap)
                overlap_days[label] = max(len(values) - 1, 0)
                skipped_codes.append({'code': label, 'reason': 'rows_le_min_overlap',
                                      'rows': len(values), 'valid_returns': 0})
                continue

            if float(np.ptp(values)) == 0.0:
                # 价格全程恒定（如货币基金 880013 单位净值恒为 1.0）：收益恒 0、
                # 方差为 0，相关系数无定义；显式丢弃并登记，不让它以 0/NaN 混进矩阵。
                logger.warning("相关性输入退化：%s 快照价恒为 %s（方差为 0），丢弃",
                               label, values[0])
                overlap_days[label] = 0
                skipped_codes.append({'code': label, 'reason': 'zero_variance',
                                      'rows': len(values), 'valid_returns': 0})
                continue

            # 逐相邻观测构造收益：跨期与折算尖峰位置留 NaN（保留该日期位置）
            rets = np.full(len(values) - 1, np.nan, dtype=float)
            cross_gaps: List[Dict[str, Any]] = []
            spike_dates: List[str] = []
            max_abs_log_ret = 0.0
            for k in range(1, len(values)):
                gap = _calendar_gap_days(dates[k - 1], dates[k])
                if gap is None or gap <= 0 or gap > MAX_SINGLE_SESSION_GAP_DAYS:
                    # gap 为 None（日期不可解析）或非正（同日重复采集，当前生产库
                    # 实测 0 例）同样不是「单个交易日」，一并按跨期处理。
                    cross_gaps.append({'date': dates[k],
                                       'gap_days': 'unparsable' if gap is None else gap})
                    continue
                prev = values[k - 1]
                cur = values[k]
                if not (prev > 0 and cur > 0):
                    continue  # 非正价（脏价），log 无定义，保持 NaN
                log_ret = float(np.log(cur / prev))
                if abs(log_ret) > SPLIT_SPIKE_LOG_RET:
                    spike_dates.append(dates[k])
                    max_abs_log_ret = max(max_abs_log_ret, abs(log_ret))
                    continue
                rets[k - 1] = cur / prev - 1.0

            valid_returns = int(np.isfinite(rets).sum())
            if spike_dates:
                unreliable_codes[label] = {
                    'reason': 'split_or_split_like_spike',
                    'hit_dates': list(spike_dates),
                    'max_abs_log_ret': round(max_abs_log_ret, 4),
                    'observations_voided': len(spike_dates),
                }
            if cross_gaps:
                cross_period_voided[label] = len(cross_gaps)

            if valid_returns < min_overlap:
                logger.warning(
                    "相关性输入在守卫后过薄：%s 有效收益 %d 条 < min_overlap=%d"
                    "（跨期置 NaN %d 条、折算尖峰置 NaN %d 条），丢弃",
                    label, valid_returns, min_overlap, len(cross_gaps), len(spike_dates))
                overlap_days[label] = valid_returns
                skipped_codes.append({'code': label,
                                      'reason': 'valid_returns_lt_min_overlap',
                                      'rows': len(values), 'valid_returns': valid_returns})
                continue

            returns_dict[label] = pd.Series(rets, index=dates[1:], name=label)
            overlap_days[label] = valid_returns

        if len(returns_dict) < 2:
            return {'error': '数据不足',
                    # days 不比重叠门槛长时必然是这里失败，给出可判读的原因
                    'reason': ('window_too_narrow' if days <= min_overlap
                               else 'series_lt_2'),
                    'overlap_days': overlap_days,
                    'unreliable_codes': unreliable_codes,
                    'cross_period_voided': cross_period_voided,
                    'skipped_codes': skipped_codes,
                    'min_overlap': min_overlap}

        # 按日期索引对齐（缺失为 NaN），并丢弃有效观测 < min_overlap 的标的
        df = pd.DataFrame(returns_dict)
        valid_counts = df.notna().sum()
        kept = [c for c in df.columns if int(valid_counts.get(c, 0)) >= min_overlap]
        # 注意 kept 已按同一门槛过滤，故成功路径下 dropped_thin 恒为空（历史遗留字段，
        # 仅为兼容取值方保留；真正被丢弃的标的在 skipped_codes 里带原因）。
        dropped_thin = {c: int(valid_counts.get(c, 0)) for c in df.columns if c not in kept}
        df = df[kept]

        if df.shape[1] < 2:
            return {'error': '数据不足',
                    'reason': 'columns_lt_2',
                    'overlap_days': overlap_days,
                    'unreliable_codes': unreliable_codes,
                    'cross_period_voided': cross_period_voided,
                    'skipped_codes': skipped_codes,
                    'dropped_thin': dropped_thin,
                    'min_overlap': min_overlap}

        # 计算相关系数矩阵（传入带日期索引的 Series，由 risk 层对齐）
        corr_matrix = self.risk_analyzer.calculate_correlation_matrix(
            {c: df[c] for c in df.columns}, min_periods=min_overlap
        )

        cols = list(corr_matrix.columns)
        # 找出高相关性品种对
        high_corr_pairs = []
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                corr = corr_matrix.iloc[i, j]
                if pd.notna(corr) and abs(corr) > 0.8:  # 高相关性阈值
                    high_corr_pairs.append({
                        'asset1': cols[i],
                        'asset2': cols[j],
                        'correlation': round(float(corr), 4),
                        'type': '强正相关' if corr > 0 else '强负相关'
                    })

        # 计算平均相关性（仅统计有效重叠达标的标对，避免 NaN 污染）
        off_diag = [
            float(corr_matrix.iloc[i, j])
            for i in range(len(cols))
            for j in range(i + 1, len(cols))
            if pd.notna(corr_matrix.iloc[i, j])
        ]
        avg_correlation = float(np.mean(off_diag)) if off_diag else 0.0

        # overlap_days：循环内登记的条目（含被 continue 掉的标的）全部保留，
        # 参与矩阵的列用对齐后的真实有效观测数刷新 —— 原实现在成功返回时整表重建，
        # 被 continue 的标的会从报告里静默消失（与 P0 不等长问题是同一类缺口）。
        overlap_days.update({c: int(df[c].notna().sum()) for c in df.columns})

        if unreliable_codes:
            logger.warning("相关性输入存在折算尖峰，已置 NaN 并登记：%s",
                           {k: v['hit_dates'] for k, v in unreliable_codes.items()})
        if cross_period_voided:
            logger.info("相关性输入剔除跨期观测（%s 自然日上限）：%s",
                        MAX_SINGLE_SESSION_GAP_DAYS, cross_period_voided)

        return {
            'correlation_matrix': corr_matrix.round(4).to_dict(),
            'high_correlation_pairs': high_corr_pairs,
            'average_correlation': round(avg_correlation, 4),
            'diversification_score': round(1 - abs(avg_correlation), 4),
            # P0 修复附带：各标的可用重叠天数，便于识别场外基金样本偏薄
            'overlap_days': overlap_days,
            'dropped_thin': dropped_thin,
            'min_overlap': min_overlap,
            # 折算/拆分尖峰处置留痕：聚合指标（average_correlation 等）对此不敏感，
            # 必须独立暴露，否则「置 NaN」等于静默。
            'unreliable_codes': unreliable_codes,
            # 被跨期守卫置 NaN 的观测条数（label -> count），解释场外基金 overlap_days
            # 为何小于行数，也便于验证守卫确实生效。
            'cross_period_voided': cross_period_voided,
            # 未进入矩阵的标的及原因（行数不足 / 有效观测不足 / 零方差）
            'skipped_codes': skipped_codes,
        }

    def _run_stress_test(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """运行压力测试"""
        total_value = sum(p['market_value'] for p in positions)

        stress_results = self.risk_analyzer.stress_test(total_value, positions)

        # 添加风险评估
        for scenario, result in stress_results.items():
            loss_pct = result['loss_pct']
            if loss_pct > 20:
                result['risk_level'] = '极高'
            elif loss_pct > 15:
                result['risk_level'] = '高'
            elif loss_pct > 10:
                result['risk_level'] = '中高'
            elif loss_pct > 5:
                result['risk_level'] = '中等'
            else:
                result['risk_level'] = '低'

        return stress_results

    def _generate_warnings(self, risk_results: Dict[str, Any]) -> List[Dict[str, str]]:
        """生成风险预警"""
        warnings = []

        # 1. 集中度预警
        concentration = risk_results.get('concentration_risk', {})
        if concentration.get('max_weight', 0) > 25:
            warnings.append({
                'level': '高',
                'type': '集中度风险',
                'message': f"单一品种占比超过25% ({concentration.get('max_weight')}%)，建议分散投资"
            })

        if concentration.get('hhi', 0) > 0.25:
            warnings.append({
                'level': '中',
                'type': '集中度风险',
                'message': f"赫芬达尔指数较高({concentration.get('hhi')})，组合不够分散"
            })

        # 2. 回撤预警
        metrics = risk_results.get('portfolio_metrics', {})
        drawdown = metrics.get('drawdown_metrics', {})
        if drawdown.get('current_drawdown', 0) > 10:
            warnings.append({
                'level': '中',
                'type': '回撤风险',
                'message': f"当前回撤{drawdown.get('current_drawdown')}%，接近历史最大回撤"
            })

        # 3. 波动率预警
        volatility = metrics.get('volatility_metrics', {})
        if volatility.get('annual_volatility', 0) > 30:
            warnings.append({
                'level': '高',
                'type': '波动率风险',
                'message': f"年化波动率{volatility.get('annual_volatility')}%，属于高波动组合"
            })

        # 4. 相关性预警
        correlation = risk_results.get('correlation_analysis', {})
        high_corr = correlation.get('high_correlation_pairs', [])
        if len(high_corr) > 0:
            warnings.append({
                'level': '中',
                'type': '相关性风险',
                'message': f"发现{len(high_corr)}对高相关性品种，分散化效果有限"
            })

        # 5. 夏普比率预警
        risk_adj = metrics.get('risk_adjusted_metrics', {})
        if risk_adj.get('sharpe_ratio', 0) < 0.5:
            warnings.append({
                'level': '中',
                'type': '收益风险比',
                'message': f"夏普比率{risk_adj.get('sharpe_ratio')}较低，风险补偿不足"
            })

        return warnings

    def _get_portfolio_history(self, days: int) -> List[Dict[str, Any]]:
        """获取组合历史数据"""
        return self.db.get_portfolio_history(days)

    def _get_benchmark_returns(self, index_code: str, days: int) -> np.ndarray:
        """从 index_quotes 读取基准指数日收益序列（小数），与组合历史对齐。

        P0-4 修复：原实现直接返回空数组，导致 RiskAnalyzer.calculate_all 中
        `if benchmark_returns is not None and len(benchmark_returns) == len(returns)`
        分支永不进入，Beta/Alpha/TE/IR 相对收益指标从未计算。
        """
        try:
            import sqlite3
            import pandas as pd
            from config.settings import DATABASE_PATH

            conn = sqlite3.connect(str(DATABASE_PATH))
            df = pd.read_sql_query(
                "SELECT date, close FROM index_quotes WHERE code=? ORDER BY date",
                conn,
                params=(index_code,),
            )
            conn.close()
            if df.empty or len(df) < 2:
                logger.warning(f"基准 {index_code} 数据不足，无法计算相对收益")
                return np.array([])
            df["ret"] = df["close"].pct_change()
            recent = df["ret"].dropna().tail(days)
            if len(recent) < 2:
                return np.array([])
            return recent.values.astype(float)
        except Exception as e:
            logger.warning(f"获取基准收益失败({index_code}): {e}")
            return np.array([])
