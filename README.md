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
| 5 | 高级分析 | 因子归因、Brinson分解 |
| 6 | 技术信号 | 雷达图、信号柱状图、布林带/RSI分布 |
| 7 | 资讯与评估 | 自动新闻聚合、市场情绪评估 |
| 8 | 操作建议 | 智能建议引擎（再平衡、风险管理、机会识别） |
| 9 | 自定义指标 | 技术指标回测、K线形态识别 |
| 10 | 资金动向 | 行业资金流趋势/热力图、ETF资金流、主力资金 |
| 11 | 黄金市场 | 金价K线、实时行情、SPDR持仓、中国黄金储备 |

### 数据采集

| 数据类型 | 数据源 | 说明 |
|----------|--------|------|
| 行情数据 | 新浪财经(主) + AKShare(备) | ETF日K线、收盘价、成交量 |
| 技术指标 | 自动计算 | MA/MACD/RSI/KDJ/布林带/ATR |
| 行业资金流 | 同花顺 | 90个行业板块主力净流入，支持排行差值回填 |
| ETF资金流 | AKShare + 估算 | 23只ETF净流入/流出，支持K线估算回填 |
| 主力资金 | 东方财富 | 大单/中单/小单净流入 |
| 北向资金 | 东方财富 | 沪股通+深股通合并 |
| 黄金行情 | 上海金交所 | Au99.99/Au99.95/Au(T+D)历史K线+实时分时 |
| 黄金持仓 | SPDR/央行 | SPDR Gold Trust持仓、中国黄金储备 |

### 智能分析

- **风险指标**: 夏普比率、索提诺比率、卡玛比率、VaR(95%/99%)、CVaR、Beta/Alpha
- **集中度分析**: HHI指数、行业分布
- **压力测试**: 多场景模拟损失
- **策略回测**: 5种再平衡策略（买入持有/定期/阈值/风险平价/动量）
- **智能建议**: 再平衡建议、风险管理、机会识别（含置信度评估）
- **告警系统**: 5项自动监控规则，三级告警（warning/error/critical）

## 项目结构

```
portfolio_tracker/
├── config/
│   ├── settings.py              # 全局配置（数据源、阈值、通知等）
│   └── __init__.py
├── src/
│   ├── data_sources/
│   │   ├── base.py              # 数据源基类
│   │   ├── sina.py              # 新浪财经数据源
│   │   ├── akshare_ds.py        # AKShare数据源
│   │   ├── fund_flow.py         # 资金流采集（行业/ETF/主力/北向）
│   │   └── __init__.py
│   ├── analysis/
│   │   ├── technical.py         # 技术指标计算
│   │   ├── portfolio.py         # 组合分析器
│   │   ├── portfolio_risk.py    # 风险指标计算
│   │   ├── risk.py              # 风险分析
│   │   ├── advisor.py           # 智能建议引擎
│   │   ├── backtest.py          # 策略回测引擎
│   │   ├── indicator_backtest.py # 指标回测
│   │   ├── candle_patterns.py   # K线形态识别
│   │   ├── factor_attribution.py # 因子归因
│   │   └── __init__.py
│   ├── report/
│   │   ├── smart_report.py      # 智能报告生成
│   │   ├── chart_generator.py   # 图表生成
│   │   ├── excel_report.py      # Excel报告
│   │   └── __init__.py
│   └── utils/
│       ├── database.py          # 数据库管理
│       ├── position_reader.py   # 通达信持仓解析
│       ├── monitor.py           # 运行监控
│       ├── notification.py      # 通知管理（邮件/企业微信）
│       ├── email_report.py      # 邮件报告
│       ├── enhanced_report.py   # 增强HTML报告
│       ├── news_fetcher.py      # 新闻资讯抓取
│       ├── backfill.py          # 历史数据回填
│       └── __init__.py
├── data/
│   ├── database/portfolio.db    # SQLite主数据库
│   ├── raw/                     # 原始数据
│   └── reports/                 # 生成的报告
├── dashboard.py                 # Streamlit Dashboard
├── run_analysis.py              # 每日定时任务入口
├── run_enhanced.py              # 增强分析（含监控管理）
├── run_smart.py                 # 智能分析入口
├── scheduled_run.bat            # Windows定时任务脚本
├── requirements.txt             # Python依赖
├── CHANGELOG.md                 # 变更日志
└── README.md                    # 项目文档
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
    'max_concentration': 0.25,    # 单一品种最大占比
    'max_drawdown_alert': 0.10,   # 回撤告警阈值
    'max_volatility_alert': 0.30, # 波动率告警阈值
}
```

### 运行

```bash
# 启动 Dashboard（浏览器访问 http://localhost:8501）
streamlit run dashboard.py

# 运行每日完整分析
python run_analysis.py

# 运行智能分析
python run_smart.py

# 增强分析 + 监控管理
python run_enhanced.py --run       # 执行分析
python run_enhanced.py --health    # 健康检查
python run_enhanced.py --stats 7   # 近7天执行统计
```

### 定时任务

以管理员身份运行 PowerShell：
```powershell
.\setup_scheduler.ps1
```

或手动在 Windows 任务计划程序中创建，触发器设为每个交易日 15:10。

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

## 开发指南

### 代码规范

- 遵循 PEP 8
- 使用 type hints
- 每个模块包含 docstring
- 错误处理使用 try/except + logging

### 添加新数据源

1. 在 `src/data_sources/` 创建新模块，继承 `DataSourceBase`
2. 实现 `fetch_data()` 方法
3. 在 `__init__.py` 中注册
4. 在 `config/settings.py` 中添加配置

### 添加新分析指标

1. 在 `src/analysis/` 对应模块中添加计算函数
2. 在 `run_analysis.py` 中集成到分析流程
3. 在 `dashboard.py` 中添加展示组件

## License

MIT License
