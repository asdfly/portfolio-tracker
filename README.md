# Portfolio Tracker - 投资组合智能分析系统

基于 Python + Streamlit 的投资组合自动化跟踪分析系统。支持多数据源采集、技术分析、风险评估、资金流监控、黄金市场分析，提供交互式 Dashboard 和每日定时任务。

## 功能概览

### Streamlit Dashboard（11个分析Tab）

| Tab | 名称 | 核心功能 |
|-----|------|----------|
| 1 | 净值走势 | 累计净值曲线、多基准对比、区间收益分析 |
| 2 | 持仓分布 | 饼图、行业分布、相关性矩阵 |
| 3 | 风险分析 | 夏普/索提诺/卡玛比率、VaR、最大回撤、压力测试 |
| 4 | 收益日历 | 年度/月度收益概览、日历热力图 |
| 5 | 高级分析 | 因子归因、Brinson分解、Monte Carlo模拟 |
| 6 | 技术信号 | 雷达图、信号柱状图、布林带/RSI分布 |
| 7 | 资讯与评估 | 自动新闻聚合、市场情绪评估 |
| 8 | 操作建议 | 智能建议引擎（再平衡、风险管理、机会识别） |
| 9 | 自定义指标 | 技术指标回测、K线形态识别 |
| 10 | 资金动向 | 行业资金流趋势/热力图、ETF资金流、主力资金 |
| 11 | 黄金市场 | 10个子Tab（见下表） |

### Tab11 黄金市场分析（10个子Tab）

| 子Tab | 名称 | 数据源 | 核心功能 |
|-------|------|--------|----------|
| 1 | 金价走势 | SGE `spot_hist_sge` | K线图 + MA5/MA20/MA60 |
| 2 | 实时行情 | SGE `spot_quotations_sge` | 多品种实时行情面板 |
| 3 | 基准价对比 | SGE `spot_golden_benchmark_sge` | 上海金基准价 vs Au99.99 价差分析 |
| 4 | 季节性规律 | SGE历史K线 | 22年月度涨跌统计、热力图 |
| 5 | 技术信号 | 自算MACD/RSI/Bollinger | 三指标组合技术分析面板 |
| 6 | 定价因子 | 中美利差/CPI | 金价与宏观因子相关性矩阵、趋势对比 |
| 7 | 储备分析 | WGC `macro_cons_gold` | SPDR持仓趋势、中国黄金储备与外汇占比 |
| 8 | 央行购金 | WGC `macro_cons_gold` | 全球央行购金趋势、东西方持仓对比 |
| 9 | 供需平衡 | COMEX/SGE/WGC | COMEX库存、ETF持仓变化、供需指标 |
| 10 | 国际对比 | WGC ETF数据 | ETF推算国际金价、隐含汇率、上海金溢价分析 |

### 数据采集

| 数据类型 | 数据源 | 说明 |
|----------|--------|------|
| 行情数据 | 新浪财经(主) + AKShare(备) | ETF日K线、收盘价、成交量 |
| 技术指标 | 自动计算 | MA/MACD/RSI/KDJ/布林带/ATR |
| 行业资金流 | 同花顺 | 90个行业板块主力净流入，排行差值回填 |
| ETF资金流 | AKShare + 估算 | 23只ETF净流入/流出，K线估算回填 |
| 主力资金 | 东方财富 | 大单/中单/小单净流入 |
| 北向资金 | 东方财富 | 沪股通+深股通合并 |
| 黄金行情 | 上海金交所 | Au99.99/Au99.95/Au(T+D)历史K线+实时分时 |
| 黄金持仓 | SPDR/央行 | SPDR Gold Trust持仓、中国黄金储备 |
| 定价因子 | AKShare | 中美国债收益率利差、中国CPI |

### 智能分析

- **风险指标**: 夏普比率、索提诺比率、卡玛比率、VaR(95%/99%)、CVaR、Beta/Alpha
- **集中度分析**: HHI指数、行业分布
- **压力测试**: 多场景模拟损失
- **策略回测**: 5种再平衡策略（买入持有/定期/阈值/风险平价/动量）
- **智能建议**: 再平衡建议、风险管理、机会识别（含置信度评估）
- **告警系统**: 8项自动监控规则，三级告警（warning/error/critical）

## 项目结构

```
portfolio_tracker/
├── config/
│   └── settings.py              # 全局配置（数据源、阈值、通知等）
├── src/
│   ├── data_sources/            # 数据采集层
│   │   ├── base.py              # 数据源基类
│   │   ├── sina.py              # 新浪财经
│   │   ├── akshare_ds.py        # AKShare
│   │   └── fund_flow.py         # 资金流采集（行业/ETF/主力/北向）
│   ├── analysis/                # 分析引擎
│   │   ├── technical.py         # 技术指标计算
│   │   ├── portfolio.py         # 组合分析器
│   │   ├── portfolio_risk.py    # 风险指标计算
│   │   ├── risk.py              # 风险分析
│   │   ├── advisor.py           # 智能建议引擎
│   │   ├── backtest.py          # 策略回测引擎
│   │   ├── indicator_backtest.py # 指标回测
│   │   ├── candle_patterns.py   # K线形态识别
│   │   └── factor_attribution.py # 因子归因
│   ├── report/                  # 报告生成
│   │   ├── smart_report.py      # 智能报告
│   │   ├── excel_report.py      # Excel报告
│   │   └── risk_report.py       # 风险报告
│   └── utils/                   # 工具层
│       ├── database.py          # 数据库管理
│       ├── position_reader.py   # 通达信持仓解析
│       ├── monitor.py           # 运行监控
│       ├── notification.py      # 通知管理（邮件/企业微信）
│       ├── email_report.py      # 邮件报告
│       ├── enhanced_report.py   # 增强HTML报告
│       ├── news_fetcher.py      # 新闻资讯抓取
│       ├── backfill.py          # 历史数据回填
│       └── chart_utils.py       # 图表工具函数
├── tabs/                        # 模块化Dashboard Tab
│   ├── __init__.py              # 11个Tab render函数导出
│   ├── _helpers.py              # Tab公共辅助函数
│   ├── tab1_net_value.py        # 净值走势
│   ├── tab2_position.py         # 持仓分布
│   ├── tab3_risk.py             # 风险分析
│   ├── tab4_calendar.py         # 收益日历
│   ├── tab5_advanced.py         # 高级分析
│   ├── tab6_technical.py        # 技术信号
│   ├── tab7_news.py             # 资讯与评估
│   ├── tab8_advice.py           # 操作建议
│   ├── tab9_custom.py           # 自定义指标
│   ├── tab10_fund_flow.py       # 资金动向
│   ├── tab11_gold.py            # 黄金市场（10子Tab入口）
│   └── gold_components/         # 黄金分析子模块
│       ├── gold_utils.py        #   公共工具（数据获取+指标计算）
│       ├── price_comparison.py  #   Phase1: 基准价对比
│       ├── seasonality.py       #   Phase1: 季节性规律
│       ├── reserve_analysis.py  #   Phase1: 储备分析
│       ├── technical_signals.py #   Phase2: 技术信号面板
│       ├── correlation.py       #   Phase2: 定价因子相关性
│       ├── realtime_quotes.py   #   Phase3: 多品种实时行情
│       ├── central_bank_trends.py # Phase4: 央行购金趋势
│       ├── supply_demand.py     #   Phase4: 供需平衡分析
│       └── international_comparison.py # Phase4: 国际金价对比
├── components/                  # Dashboard UI组件
│   ├── layouts.py / charts.py / metrics.py / tables.py
├── tests/                       # 测试套件（115用例，全部通过）
│   ├── conftest.py              # 共享fixture
│   ├── test_config.py           # L0: 配置完整性(5)
│   ├── test_imports.py          # L0: 模块导入(15)
│   ├── test_chart_utils.py      # L1: 纯函数测试(25)
│   ├── test_database_new.py     # L2: 数据库测试(12)
│   ├── test_gold_utils.py       # L1: 黄金工具函数(27)
│   ├── test_tab_render.py       # L3: Tab渲染测试(25)
│   └── test_integration.py      # L4: 集成冒烟(4)
├── data/
│   ├── database/portfolio.db    # SQLite主数据库
│   ├── raw/ / processed/ / reports/
├── docs/                        # 设计文档
├── dashboard_main.py            # 模块化Dashboard入口
├── run_analysis.py              # 每日定时任务入口
├── run_enhanced.py              # 增强分析（含监控管理）
├── run_smart.py                 # 智能分析入口
├── backfill_full_history.py     # 历史数据回填
├── requirements.txt             # Python依赖
├── requirements-dev.txt         # 开发依赖
├── pytest.ini / pyproject.toml  # 项目配置
├── CHANGELOG.md                 # 变更日志
└── README.md                    # 本文件
```

## 快速开始

### 环境要求

- Python 3.9+
- Windows（持仓数据依赖通达信导出文件）

### 安装

```bash
git clone https://github.com/asdfly/portfolio-tracker.git
cd portfolio-tracker
pip install -r requirements.txt
pip install -r requirements-dev.txt   # 开发/测试依赖
```

### 配置

编辑 `config/settings.py`，主要配置项：

```python
# 持仓文件路径（通达信导出目录自动检测最新文件）
POSITION_FILE = _find_latest_position_file()

# 通知配置（可选）
NOTIFICATION_CONFIG = {
    'email': {'enabled': False, 'smtp_server': 'smtp.qq.com', ...},
    'wechat': {'enabled': False, 'webhook_url': '...', ...},
}

# 风险阈值
RISK_CONFIG = {
    'max_concentration': 0.25,
    'max_drawdown_alert': 0.10,
    'max_volatility_alert': 0.30,
}
```

### 运行

```bash
streamlit run dashboard_main.py        # 启动Dashboard
python run_analysis.py                # 每日分析
python run_smart.py                   # 智能分析
python run_enhanced.py --run          # 增强分析
python run_enhanced.py --health       # 健康检查
python run_enhanced.py --stats 7      # 近7天统计
```

### 测试

```bash
pytest tests/ -v                       # 全部115个用例
pytest tests/test_imports.py -v        # L0 导入检查
pytest tests/test_chart_utils.py -v    # L1 纯函数
pytest tests/test_gold_utils.py -v     # L1 黄金工具
pytest tests/test_database_new.py -v   # L2 数据库
pytest tests/test_tab_render.py -v     # L3 Tab渲染
pytest tests/test_integration.py -v    # L4 集成
```

### 定时任务

以管理员身份运行 PowerShell：`.\setup_scheduler.ps1`，触发器设为每个交易日 15:10。

## 数据库结构

| 表名 | 用途 | 关键列 |
|------|------|--------|
| portfolio_snapshots | 每日持仓快照 | date, code, quantity, cost_price, current_price, market_value, pnl |
| portfolio_summary | 组合日汇总 | date, total_value, total_pnl, daily_return, vs_hs300 |
| index_quotes | 指数行情 | date, code, close, change_pct, volume |
| etf_technical | 技术指标 | date, code, ma/macd/rsi/kdj信号及数值 |
| fund_flows | 资金流数据 | date, code, category, net_inflow, buy_amount, sell_amount |
| daily_news | 新闻资讯 | date, category, title, source, url, summary |
| alerts | 告警记录 | rule_name, level, message, created_at |
| execution_logs | 执行日志 | task_name, status, duration_seconds |
| custom_indicators | 自定义指标 | name, formula, description |

## 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Python 3.9+ |
| Dashboard | Streamlit + Plotly |
| 数据库 | SQLite |
| 数据源 | 新浪财经、AKShare、同花顺、东方财富、上海金交所 |
| 技术计算 | NumPy, SciPy, Pandas |
| 报告生成 | Python-docx, OpenPyXL |
| 通知 | SMTP, 企业微信 Webhook |
| 自动化 | Windows 任务计划程序 |
| 测试 | pytest, unittest.mock |

## 开发指南

### 代码规范

- 遵循 PEP 8，pre-commit hook 自动检查
- 使用 type hints
- 每个模块包含 docstring
- 错误处理使用 try/except + logging
- 黄金组件中 DataFrame 参数需做 None guard（`if df is None or df.empty`）

### 项目统计

| 维度 | 数据 |
|------|------|
| Python 文件 | 88 个 |
| 代码总行数 | ~28,000 行 |
| 测试用例 | 115 个（全部通过） |
| Dashboard Tab | 11 个（Tab11含10个子Tab） |
| 数据库表 | 9 个 |
| Plotly 图表 | 25+ 个 |

### 添加新数据源

1. 在 `src/data_sources/` 创建新模块，继承 `DataSourceBase`
2. 实现 `fetch_data()` 方法
3. 在 `__init__.py` 中注册
4. 在 `config/settings.py` 中添加配置

### 添加新分析Tab

1. 在 `tabs/` 创建新模块 `tabN_xxx.py`
2. 实现 `render_tabN(positions, summary, index_quotes, selected_date, selected_benchmark)` 函数
3. 在 `tabs/__init__.py` 中注册
4. 在 `dashboard_main.py` 中添加 Tab 入口

### 添加黄金分析子模块

1. 在 `tabs/gold_components/` 创建新模块
2. 数据获取函数放在 `gold_utils.py` 并加 `@st.cache_data` + `try/except`
3. 在 `tab11_gold.py` 中添加子Tab入口

## License

MIT License
