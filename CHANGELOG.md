# Changelog

所有重大变更均记录在此文件中。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/)。

---

## [v2.1] - 2026-05-13

### 新增
- **黄金市场分析Tab (Tab11)**: 上海金交所金价K线走势、实时分时行情、SPDR Gold Trust持仓趋势、中国黄金储备图表
- **行业资金流历史回填**: 基于同花顺多周期排行差值分解法，自动回填行业历史资金流数据
- **自动增量回填集成**: 行业资金流回填集成到每日定时任务，自动跳过已有日期

### 修复
- 资金流趋势图/热力图过滤数据稀疏行业（最低10天数据覆盖要求）
- ETF资金流查询去掉LIMIT 2000截断
- 同花顺资金流数据单位统一（亿元 -> 元）
- requests Session monkey-patch递归Bug
- Plotly 6.x titlefont废弃属性迁移为title_font_color
- 中国黄金储备日期解析支持 YYYY年MM月份 格式

---

## [v2.0] - 2026-05-08

### 新增
- **Streamlit Dashboard**: 11个主Tab的交互式分析仪表盘
  - Tab1 净值走势: 累计净值曲线、多基准对比、区间收益分析
  - Tab2 持仓分布: 饼图、行业分布、相关性矩阵
  - Tab3 风险分析: 夏普/索提诺/卡玛比率、VaR、最大回撤、压力测试
  - Tab4 收益日历: 年度/月度收益概览、日历视图
  - Tab5 高级分析: 因子归因、Brinson分解
  - Tab6 技术信号: 雷达图、信号柱状图、布林带/RSI分布
  - Tab7 资讯与评估: 自动新闻聚合、市场情绪评估
  - Tab8 操作建议: 智能建议引擎（再平衡、风险管理、机会识别）
  - Tab9 自定义指标: 技术指标回测、K线形态识别
  - Tab10 资金动向: 行业资金流趋势/热力图、ETF资金流、主力资金
  - Tab11 黄金市场: 金价K线、实时行情、SPDR持仓、中国黄金储备
- **资金流数据采集**: 行业资金流（同花顺）、ETF资金流、主力资金、北向资金
- **数据导出**: CSV持仓/汇总导出、一键PDF报告导出

### 优化
- Dashboard性能优化: 缓存、降采样、索引优化
- 深色主题UI，统一视觉风格
- 标签栏CSS flex-wrap换行适配

---

## [v1.8] - 2026-04-30

### 新增
- 持仓详情面板全宽展示
- 数据导出增强（持仓CSV、汇总CSV）
- 组合归因深化（Brinson分解）

### 修复
- 变量名错误修正（compute_return_attribution、export_positions_csv）
- 行业涨跌热力图条形不可见

---

## [v1.7] - 2026-04-29

### 新增
- 侧边栏增强、持仓技术信号列
- 一键报告导出（PDF/截图）

---

## [v1.6] - 2026-04-28

### 新增
- Tab7 资讯与评估
- Tab8 操作建议
- Tab4 收益日历事件

---

## [v1.5] - 2026-04-27

### 新增
- Tab1 基准对比表
- Tab2 累计盈亏柱状图
- Tab3 风险提示面板

---

## [v1.3] - 2026-04-24

### 初始版本
- 多数据源支持（新浪财经 + AKShare）
- 7种技术指标（MA/MACD/RSI/KDJ/布林带/ATR/成交量）
- SQLite数据持久化
- 通达信持仓文件解析
- 自动异常处理与数据源切换

## [v2.2] - 2026-05-17

### 新增
- **Tab11 黄金市场 Phase 4**: 央行购金全球趋势追踪、供需平衡分析、国际金价对比（上海金溢价）
- **test_gold_utils.py**: 27个单元测试，覆盖calc_rsi/calc_macd/calc_bollinger/calc_monthly_returns纯函数和fetch_*异常处理
- **Tab渲染测试扩展**: 新增tab4/tab8/tab9/tab10正常数据渲染测试、tab11空数据测试

### 优化
- **correlation.py**: 移除死调用(fetch_usdcny_hist)、移除未使用CPI加载、_load_all_factors并发化(ThreadPoolExecutor)、fetch_bond_yields添加years=3过滤
- **central_bank_trends.py**: resample("M")迁移为resample("ME")避免FutureWarning

### 修复
- **gold_utils.py**: 添加import streamlit as st（@st.cache_data依赖）
- **supply_demand.py**: 列名修复(交易时间->date, 晚盘价->close)
- **gold_components全面None guard**: central_bank_trends/supply_demand/international_comparison/technical_signals所有df.empty检查添加None判断
- **fetch_global_etf_holdings/fetch_china_reserve_data/fetch_comex_inventory**: 添加try/except异常处理
- **test_gold_utils.py**: RSI测试用双向波动数据替代单调序列、FetchFunctions通过__wrapped__绕过st.cache_data缓存
- **test_tab_render.py**: 修复函数签名语法错误、添加akshare mock防止tab11真实网络请求

### 文档
- **README.md**: 更新项目结构(tabs模块化拆分)、Tab11从4功能扩展为10子Tab、添加测试说明和开发指南
- **CHANGELOG.md**: 添加v2.2变更记录
- **gold_analysis_improvement_plan.md**: 标记Phase 4完成

### 测试
- 测试套件: 91 -> 115用例（删除3个冗余文件、新增test_gold_utils.py 27用例、扩展test_tab_render.py 8用例）
- 全部115 passed, 0 failed

