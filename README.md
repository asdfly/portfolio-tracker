# Portfolio Tracker - 投资组合智能分析系统

基于 Python + Streamlit 的投资组合自动化跟踪分析系统。支持多数据源采集、技术分析、风险评估、资金流监控、黄金市场分析，提供交互式 Dashboard 和每日定时任务。

## 功能概览

### Streamlit Dashboard（14个分析Tab）

| Tab | 名称 | 核心功能 |
|-----|------|----------|
| 1 | 净值走势 | 累计净值曲线、多基准对比、区间收益分析、年度收益图 |
| 2 | 持仓分布 | 饼图、行业分布、相关性矩阵、HHI集中度、Beta贡献 |
| 3 | 风险分析 | 夏普/索提诺/卡玛比率、VaR、最大回撤、压力测试 |
| 4 | 收益日历 | 年度/月度收益概览、日历热力图 |
| 5 | 高级分析 | 因子归因、Brinson分解、Monte Carlo模拟、VaR直方图、再平衡模拟 |
| 6 | 技术信号 | 雷达图、信号柱状图、布林带/RSI分布 |
| 7 | 资讯与评估 | 自动新闻聚合、SnowNLP+jieba 情感评分、市场情绪评估 |
| 8 | 操作建议 | 智能建议引擎（再平衡/风险管理/机会识别），反馈闭环追踪 |
| 9 | 自定义指标 | 技术指标回测、K线形态识别、DB回测历史 |
| 10 | 资金动向 | 行业资金流趋势/热力图、ETF资金流、主力资金、北向资金 |
| 11 | 黄金市场 | 10个子Tab（金价走势/实时行情/基准价对比/季节性/技术信号/定价因子/储备分析/央行购金/供需平衡/国际对比） |
| 12 | 宏观经济 | 宏观数据面板（汇率/债市/金价基准/利率/融资融券） |
| 13 | 数据质量 | 数据质量评分环、新鲜度热力图、覆盖度表格、回测摘要 |
| 14 | 市场事件 | 涨停板/融资融券/股东增减持/机构调研/大宗交易深度分析 |

### 数据采集

| 数据类型 | 数据源 | 说明 |
|----------|--------|------|
| 行情数据 | 新浪财经(主) + AKShare(备) | ETF日K线、收盘价、成交量 |
| 技术指标 | 自动计算 | MA/MACD/RSI/KDJ/布林带/ATR |
| 行业资金流 | 同花顺 | 90个行业板块主力净流入，排行差值回填 |
| ETF资金流 | AKShare + 估算 | 23只ETF净流入/流出，K线估算回填 |
| 主力资金 | 东方财富 | 大单/中单/小单净流入 |
| 北向资金 | 东方财富 | 沪股通+深股通合并 |
| 市场事件 | AKShare | 涨停板、融资融券、股东增减持、机构调研、大宗交易 |
| 黄金行情 | 上海金交所 | Au99.99/Au99.95/Au(T+D)历史K线+实时分时 |
| 黄金持仓 | SPDR/央行 | SPDR Gold Trust持仓、中国黄金储备 |
| 定价因子 | AKShare | 中美国债收益率利差、中国CPI、Shibor |
| 宏观情绪 | AKShare | 融资融券余额、股权质押比例 |

### 智能分析

- **风险指标**: 夏普比率、索提诺比率、卡玛比率、VaR(95%/99%)、CVaR、Beta/Alpha
- **集中度分析**: HHI指数、行业分布
- **压力测试**: 多场景模拟损失
- **策略回测**: 5种再平衡策略（买入持有/定期/阈值/动量/均值回归）
- **智能建议**: 17个分析步骤，13个分析维度（含置信度评估），建议→执行→效果闭环
- **告警系统**: 9条自动监控规则（数据源中断/数据质量/持仓变化/市值变化/回撤/集中度/夏普/波动率/异常），告警去重

## 项目结构

```
portfolio_tracker/
├── config/
│   ├── settings.py              # 全局配置（支持.env环境变量覆盖）
│   └── db_schema.py             # 数据库DDL集中定义（20张表）
├── src/
│   ├── data_sources/            # 数据采集层
│   │   ├── base.py / sina.py / akshare_ds.py
│   │   ├── fund_flow.py         # 资金流采集（行业/ETF/主力/北向）
│   │   ├── market_events.py     # 市场事件采集（涨停/融资/股东/机构/大宗）
│   │   ├── macro_daily.py       # 宏观日度数据
│   │   ├── news_fetcher.py     # 新闻资讯抓取+情感分析
│   │   └── monitor_push2his.py  # 资金流推送监控
│   ├── analysis/                # 分析引擎
│   │   ├── technical.py         # 技术指标计算
│   │   ├── portfolio.py / portfolio_risk.py / risk.py
│   │   ├── advisor.py           # 智能建议引擎（17方法/13步骤）
│   │   ├── backtest.py          # 策略回测引擎（5种策略）
│   │   ├── market_event_signals.py  # 市场事件信号引擎
│   │   ├── indicator_backtest.py / candle_patterns.py / factor_attribution.py
│   └── utils/                   # 工具层
│       ├── database.py / db_schema.py
│       ├── monitor.py           # 运行监控（9条规则+去重）
│       ├── notification.py      # 通知管理（邮件/企业微信）
│       ├── data_quality.py     # 数据质量评估（新鲜度40+覆盖度30+回测度30）
│       ├── position_reader.py / chart_utils.py / backfill.py
│       └── email_report.py / enhanced_report.py / smart_report.py
├── tabs/                        # 模块化Dashboard Tab（14个）
│   ├── tab1~tab14               # 各Tab渲染函数
│   ├── _helpers.py              # 公共辅助函数
│   └── gold_components/         # 黄金分析子模块（9个）
├── components/                  # Dashboard UI组件
├── scripts/                     # 运维脚本
│   ├── backfill/                # 6个回填脚本（history/indicators/macro/news/sector/full）
│   ├── setup/setup_notification.py
│   └── run_backfill.py          # 统一backfill入口
├── tests/                       # 测试套件（655用例）
├── data/database/               # SQLite数据库
├── Dockerfile                   # Docker容器化部署
├── .github/workflows/ci.yml     # GitHub Actions CI
├── .env.example                 # 环境变量模板
├── requirements.txt / requirements-dev.txt
├── pytest.ini / pyproject.toml
├── LICENSE / CHANGELOG.md
└── README.md
```

## 快速开始

### 环境要求

- Python 3.12+
- Windows（持仓数据依赖通达信导出文件）

### 安装

```bash
git clone https://github.com/asdfly/portfolio-tracker.git
cd portfolio-tracker
pip install -r requirements.txt
pip install -r requirements-dev.txt   # 开发/测试依赖
```

### 配置

支持两种配置方式：

1. **环境变量（推荐）**: 复制 `.env.example` 为 `.env`，填入实际值
2. **直接编辑**: `config/settings.py` 中修改默认值

```bash
cp .env.example .env
# 编辑 .env 填入敏感配置（邮箱密码、webhook地址等）
```

### 运行

```bash
streamlit run dashboard.py              # 启动Dashboard（端口8501）
python run_analysis.py                  # 每日分析（Stage1-4）
python scripts/run_backfill.py all      # 全量回填
python scripts/run_backfill.py history  # 仅行情回填
```

### Docker部署

```bash
docker build -t portfolio-tracker .
docker run -p 8501:8501 -v ./data:/app/data portfolio-tracker
```

### 测试

```bash
pytest tests/ -v                         # 全部655个用例
pytest tests/ -k "d8 or d9 or d10" -v    # 特定阶段测试
```

### CI/CD

GitHub Actions 在每次 push/PR 到 master 分支时自动运行 pytest。

## 数据库结构

20张表，核心表如下：

| 表名 | 用途 | 关键列 |
|------|------|--------|
| portfolio_snapshots | 每日持仓快照 | date, code, quantity, cost_price, current_price, market_value, pnl |
| portfolio_summary | 组合日汇总 | date, total_value, total_pnl, daily_return, vs_hs300 |
| index_quotes | 指数行情 | date, code, close, change_pct, volume |
| etf_technical | 技术指标 | date, code, ma/macd/rsi/kdj信号及数值 |
| fund_flows | 资金流数据 | date, code, category, net_inflow(元) |
| daily_news | 新闻资讯 | date, category, title, source, sentiment_score |
| alerts | 告警记录 | rule_name, level, message, created_at |
| advice_history | 建议追踪 | status, action_taken, feedback, resolved_at |
| execution_logs | 执行日志 | task_name, status, duration_seconds |
| market_sentiment | 市场情绪 | name, value, date |
| macro_daily | 宏观数据 | name, value, date |

## 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Python 3.12 |
| Dashboard | Streamlit + Plotly |
| 数据库 | SQLite |
| 数据源 | 新浪财经、AKShare、同花顺、东方财富、上海金交所 |
| 技术计算 | NumPy, SciPy, Pandas |
| 报告生成 | Python-docx, OpenPyXL |
| 通知 | SMTP, 企业微信 Webhook |
| 容器化 | Docker (python:3.12-slim) |
| CI/CD | GitHub Actions (pytest) |
| 测试 | pytest 9.0.3 |

## 开发指南

### 代码规范

- 遵循 PEP 8，pre-commit hook 自动检查（Python语法 + pytest gate）
- 错误处理使用 try/except + logging，日志自动轮转（10MB, 5备份）
- 敏感配置通过 `.env` 环境变量管理，不硬编码
- 黄金组件中 DataFrame 参数需做 None guard

### 项目统计

| 维度 | 数据 |
|------|------|
| 测试用例 | 655 个（全部通过） |
| Dashboard Tab | 14 个（Tab11含10个子Tab） |
| 数据库表 | 20 个 |
| 数据行数 | 320,000+ 行 |
| 告警规则 | 9 条（含去重） |
| 回测策略 | 5 种 |
| Plotly 图表 | 30+ 个 |

## License

[MIT License](LICENSE)
