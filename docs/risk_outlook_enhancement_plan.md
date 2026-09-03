# ETF 风险展望前端展示信息完善方案

> 生成日期：2026-08-20
> 范围：`tabs/tab16_risk_outlook.py`（前端）+ `src/analysis/predictor/`（建模）+ `src/utils/risk_report.py`（日报）
> 原则：仅作风险参考，不自动调仓。

---

## 一、现状盘点

### 1.1 当前 Tab16 已实现
- 模型回测表（walk-forward OOS：R² / IC / 分类 AUC / 样本数）
- 1月(20日) / 1季(60日) 窗口 radio 切换
- 预期年化波动率横向条形图（红=高波动 / 绿=低波动）
- 明细表：名称 / 代码 / 预期年化波动率% / 截面分位% / 波动分类

### 1.2 数据底座已具备、但前端未利用的资产

| 资产 | 位置 | 现状 |
|---|---|---|
| 历史已实现波动率 `vol_20d`/`vol_60d`/`vol_5d`/`vol_ratio_5_20`/`parkinson_vol_20d` | `etf_features` | 仅作模型特征，未展示 |
| 未来最大回撤标签 `fwd_max_dd_5/20/60` | `etf_forward_returns` | 已计算，未建模、未展示 |
| 持仓市值 `market_value` | `portfolio_snapshots` | 未用于组合聚合 |
| 分模型回测指标（lgb + ridge） | `run_risk_prediction` | 前端只展示 lgb 的 AUC/R²，ridge 未展示 |

---

## 二、核心洞察

当前 Tab16 是「**单点横截面**」——只回答"今天 22 只 ETF 谁高波动、谁低波动"。存在三个结构性缺失：

1. **无参照系**：预测值未与历史波动率对比，无法判断"波动率处于历史什么位置、模型预测它在升还是在降"。
2. **无回撤**：波动率抽象，用户真正关心的是"最多可能亏多少"，而 `fwd_max_dd` 标签已经算好却闲置。
3. **无组合视角**：22 只各自为战，缺少组合整体风险、单只风险贡献、尾部风险集中度。

---

## 三、完善方案（分优先级）

### P0 — 高价值 / 低成本 / 数据已就绪（纯前端 + 轻查询，可立即落地）

**P0-1 预测 vs 历史波动率对比 + 变动方向**
- 目标：明细表/图新增"历史 20 日已实现波动率"列，以及"预测 − 历史"的变动方向（↑ 升波 / ↓ 降波 / → 平稳）。
- 数据：`SELECT date, code, vol_20d, vol_60d FROM etf_features WHERE date = 最新`（无需训练）。
- 实现：`_build_window_df` join 历史波动率，新增"变动方向"列；条形图叠加历史值参考点。
- 工作量：0.5 天。

**P0-2 预测基准日 + 数据新鲜度提示**
- 目标：显式展示 `pred_date`（预测基准日）+ 说明 T+1 滞后（新浪源当日 K 滞后一个交易日，最新特征行可能 NULL 一天）。
- 实现：顶部 metric 加"预测基准日 YYYY-MM-DD"；新鲜度直接复用 `data_quality` 结果。
- 工作量：0.25 天。

**P0-3 三档分类 + 绝对阈值预警**
- 目标：把二分类（中位数切）改为三档（低/中/高），并设绝对阈值（如年化 <18% 低、18–30% 中、>30% 高）触发"高波动预警"清单。
- 理由：中位数切分永远一半高一半低，不可操作；绝对阈值让"哪些 ETF 风险真的偏高"一目了然。
- 实现：`_build_window_df` 分类逻辑改为「阈值 + 分位」双轨；新增"高波动预警"红色清单块。
- 工作量：0.5 天。

**P0-4 排序 / 筛选 / 导出**
- 目标：明细表支持按波动率/分位排序、按分类筛选、`st.download_button` 导出 CSV。
- 实现：Streamlit 原生能力，零依赖。
- 工作量：0.25 天。

### P1 — 高价值 / 需少量建模或聚合

**P1-1 组合层面聚合视图**
- 目标：新增"组合风险概览"块——组合整体预期波动率（按市值加权）、各 ETF 风险贡献、高波动 ETF 权重占比。
- 数据：`portfolio_snapshots.market_value`（算权重）+ 预测波动率。
- 实现：新增 `_render_portfolio_agg()`；权重 = `market_value / Σ market_value`（仅 ETF 子组合）；组合波动率 = `Σ w·σ`（简化口径，可后续升级为含相关性的 `√(w' Σ w)`）。
- 注意：聚合范围 = 22 只 ETF 子组合；若快照含非 ETF 持仓需标注"仅 ETF 部分"。
- 工作量：1 天。

**P1-2 预期最大回撤（新增建模）**
- 目标：复用 `risk_walkforward_evaluate` 框架，把目标列从 `fwd_vol` 换成 `fwd_max_dd`，产出"预期未来最大回撤"（−x%），比波动率更直观。
- 数据：`etf_forward_returns.fwd_max_dd_20/60`（已就绪）。
- 实现：`models.py` 新增 `run_drawdown_prediction` / `predict_drawdown_latest`（镜像波动率流程）；walk-forward 验证 R²/IC/AUC 达标才上线；落表复用 `etf_predictions`（`model='risk_dd_lgb'`，无需新表）。
- 红线：验证不达标则降级为"只展示历史 60 日最大回撤参照"（从 `etf_price_history` 计算）。
- 工作量：2–3 天（含验证）。

**P1-3 历史波动率轨迹（单 ETF 下钻）**
- 目标：选择单只 ETF，展示其 60 日滚动已实现波动率时间序列 + 当前预测点 + 历史分位（当前波动率处于过去 2 年什么位置）。
- 数据：从 `etf_price_history` 计算 rolling vol，或读 `etf_features.vol_20d` 历史序列。
- 实现：`st.selectbox` 选 ETF → plotly 折线 + 标注预测值。
- 工作量：1 天。

### P2 — 增强，视需要

- **P2-1 预测置信区间**：模型残差分布 → 预测区间（±1σ），点估计变区间估计。
- **P2-2 波动率择时 / 回撤预警文案**：把风险预测映射为"仓位/回撤预警"建议（严守不自动调仓，仅参考）。
- **P2-3 与再平衡引擎联动**：高波动预警反哺 `rebalance_engine` 风险提示（可选）。

---

## 四、实施顺序与红线

**建议顺序**：P0 四项（约 1.5 天）→ P1-1 组合聚合（1 天）→ P1-2 回撤建模（2–3 天）→ P1-3 轨迹下钻（1 天）→ P2 视需要。

**红线**：
1. 不自动调仓（延续既有原则，风险仅作参考）。
2. 不新增外部数据源依赖（全部基于现有 `etf_features` / `etf_forward_returns` / `etf_predictions` / `portfolio_snapshots`）。
3. 回撤建模沿用 walk-forward + R²/IC/AUC 门禁，不达标不上线。
4. UI 遵守 P0 规则：不用 emoji 作功能图标、避免紫粉渐变；沿用现有「红=高波动 / 绿=低波动」配色。

---

## 五、与日报联动

- 前端新增的组合聚合、预期回撤、绝对阈值预警，可同步注入 `risk_report.get_risk_outlook` 的 HTML 块（enhanced / email），保持日报与前端口径一致。
- 但日报保持轻量：只加「组合整体波动率 + 高波动预警清单 + 预期最大回撤 Top N」，不加时序图等重元素。

---

## 六、改动文件锚点

| 改动 | 文件 |
|---|---|
| 前端展示（P0 全部 + P1-1/P1-3） | `tabs/tab16_risk_outlook.py` |
| 回撤建模（P1-2） | `src/analysis/predictor/models.py`（新增）+ `labels.py`（已就绪） |
| 日报同步（可选） | `src/utils/risk_report.py` |
| 表结构 | `src/utils/db_schema.py`（回撤复用 `etf_predictions.model` 字段，无需新表） |
