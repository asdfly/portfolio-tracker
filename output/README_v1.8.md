# 投资组合跟踪分析系统

> 基于 Python + Streamlit 的全栈式 ETF 投资组合跟踪、分析与可视化平台

## 系统概览

| 属性 | 说明 |
|------|------|
| 当前版本 | **v1.8** |
| 主界面 | Streamlit Dashboard (`dashboard.py`) |
| 数据库 | SQLite (`data/database/portfolio.db`, ~19 MB) |
| 基准指数 | 11 个 (上证/深证/沪深300/中证500/中证1000/创业板等) |
| ETF 分类 | 23 只, 覆盖 8 个行业 (医药/金融/军工/新能源/科技/宽基/红利/债券) |
| 代码规模 | 36 个 Python 文件, 12,705 行 |
| 图表数量 | 25 个 Plotly 交互图表 |
| 数据跨度 | 2012 年至今, ~170,000 条记录 |

---

## 功能架构

```
投资组合跟踪分析系统 v1.8
│
├── Phase 1: 基础架构
│   ├── 多数据源 (新浪财经 + 东方财富 + AKShare 自动切换)
│   ├── 7 种技术指标 (MA/MACD/RSI/KDJ/布林带/ATR/成交量MA)
│   ├── SQLite 数据持久化
│   └── 通达信持仓文件自动解析
│
├── Phase 2: 风险分析
│   ├── 收益风险比 (夏普/索提诺/卡玛比率)
│   ├── 回撤分析 (最大回撤/当前回撤)
│   ├── 尾部风险 (VaR 95%/99%, CVaR)
│   ├── 系统性风险 (Beta/Alpha/R²/信息比率)
│   ├── 集中度分析 (HHI指数/等效品种数)
│   └── 压力测试 (多场景模拟)
│
├── Phase 3: 自动化部署
│   ├── 邮件通知 (SMTP)
│   ├── 企业微信通知 (Webhook)
│   ├── 5 项自动告警规则
│   ├── 执行日志记录
│   └── Windows 定时任务集成
│
├── Phase 4: 智能分析
│   ├── 5 种再平衡策略回测
│   ├── 智能建议引擎 (5 类建议)
│   └── 智能报告生成
│
└── Phase 5: Dashboard 可视化 (v1.3 ~ v1.8)
    ├── 8 个功能 Tab + 4 个子 Tab
    ├── 25 个交互式图表
    ├── 多基准对比 & 区间收益分析
    ├── ETF 智能筛选器
    ├── 告警中心 (8 条实时规则引擎)
    ├── 市场情绪仪表盘
    ├── Monte Carlo 模拟 & 持仓压力测试
    └── 收益日历 & 技术信号面板
```

---

## Dashboard 功能详解 (v1.8)

### Tab 1: 净值走势
- 组合净值曲线 + 基准对比 (Plotly 交互图表)
- 日收益率分布直方图
- 每日盈亏柱状图
- 滚动夏普/波动率/回撤指标图
- **[Phase 4]** 多基准叠加对比 (最多同时对比 5 个指数)
- **[Phase 4]** 区间收益分析 (累计收益/年化收益/夏普/回撤/波动率/胜率/盈亏比)

### Tab 2: 持仓分布
- **[Phase 4]** ETF 智能筛选器 (行业/收益状态/多维排序)
- 持仓分布环形图
- 行业权重堆叠面积图
- 相关性矩阵热力图
- 累计盈亏柱状图

### Tab 3: 风险分析
- Brinson 收益归因分析
- 风险提示面板
- **[Phase 4]** 告警中心 (8 条实时规则引擎 + 历史告警 + 告警统计)

### Tab 4: 收益日历
- 年度/月度热力图日历
- 月度收益统计卡片

### Tab 5: 高级分析
- Monte Carlo 模拟 (Bootstrap 采样, 未来收益区间预测)
- VaR 风险价值估计
- **[Phase 4]** 持仓压力测试 (5 种极端情景: 温和下跌/大幅下跌/极端暴跌/震荡盘整/结构牛市)
- 再平衡建议 (目标权重 vs 实际权重)

### Tab 6: 技术信号
- 信号概览卡片 (多空信号/趋势/超买超卖)
- 信号强度雷达图
- 技术信号详情表
- 布林带位置分布图
- RSI 分布图

### Tab 7: 资讯与评估
- 新闻资讯面板 (自动抓取分类财经新闻)
- 综合评估面板 (组合收益/基准收益/超额收益)
- **[Phase 4]** 市场情绪仪表盘 (情绪等级/涨跌比/行业热力图/收益分布)

### Tab 8: 操作建议
- 操作建议汇总卡片
- 建议详情列表
- 技术指标增强详情 (点击持仓行查看)
- 智能建议
- 数据导出

### 侧边栏
- 日期选择器
- 时间范围 (30/60/90/180/365天)
- 基准指数选择 (11个)
- 系统信息

---

## 项目结构

```
portfolio_tracker/
├── config/
│   └── settings.py              # 全局配置 (指数/ETF分类/技术指标/风险/通知)
├── dashboard.py                 # Streamlit 主界面 (5,049 行, v1.8)
├── run_analysis.py              # 定时任务入口 (四阶段完整流程)
├── run_enhanced.py              # 增强分析 (健康检查/告警/统计)
├── run_smart.py                 # 智能分析 (建议+报告)
├── backfill_full_history.py     # 历史数据回填
├── backfill_history.py          # 增量数据回填
├── start_dashboard.bat          # 启动 Dashboard
├── run_dashboard.bat            # 启动 Dashboard (兼容)
├── run_all.bat                  # 交互式菜单入口
├── monitor.bat                  # 监控面板
├── scheduled_run.bat            # 定时运行
├── run_analysis.bat             # Windows 任务计划程序调用
├── setup_scheduler.ps1          # PowerShell 定时任务配置
├── setup_notification.py        # 通知配置向导
├── requirements.txt             # Python 依赖
│
├── src/
│   ├── analysis/
│   │   ├── portfolio.py         # 组合分析器
│   │   ├── portfolio_risk.py    # 组合风险分析
│   │   ├── risk.py              # 风险指标计算
│   │   ├── technical.py         # 技术指标计算
│   │   ├── advisor.py           # 智能建议引擎
│   │   └── backtest.py          # 策略回测引擎
│   ├── data_sources/
│   │   ├── base.py              # 数据源基类
│   │   ├── sina.py              # 新浪财经接口
│   │   ├── akshare_ds.py        # AKShare 接口
│   │   └── __init__.py          # 数据源管理器
│   ├── report/
│   │   ├── risk_report.py       # 风险报告
│   │   └── smart_report.py      # 智能报告
│   └── utils/
│       ├── database.py          # 数据库管理
│       ├── position_reader.py   # 持仓读取
│       ├── monitor.py           # 监控告警
│       ├── notification.py      # 通知管理
│       ├── news_fetcher.py      # 新闻抓取
│       ├── email_report.py      # 邮件报告
│       ├── enhanced_report.py   # 增强报告 (HTML)
│       └── backfill.py          # 数据回填工具
│
├── data/
│   ├── database/
│   │   └── portfolio.db         # SQLite 数据库
│   ├── raw/                     # 原始数据
│   ├── processed/               # 处理后数据
│   └── reports/                 # 生成的报告 (HTML/Markdown)
│
├── logs/                        # 日志文件
├── output/                      # 输出文件 (截图/文档)
└── report/
    └── templates/               # 报告模板
```

---

## 数据库结构

| 表名 | 行数 | 时间范围 | 说明 |
|------|------|----------|------|
| `portfolio_snapshots` | 33,604 | 2012-05-28 ~ 2026-05-08 | 持仓快照 (12列: code/name/quantity/cost/current/mv/pnl/pnl_rate/ytd/beta) |
| `portfolio_summary` | 3,390 | 2012-05-28 ~ 2026-05-08 | 组合汇总 (12列: total_value/cost/pnl/daily_return/sharpe/drawdown/volatility) |
| `index_quotes` | 49,904 | 1990-12-19 ~ 2026-05-08 | 指数行情 (11个指数, 7列) |
| `etf_technical` | 33,144 | 2012-06-26 ~ 2026-05-08 | 技术指标 (10列: MA/MACD/RSI/KDJ/布林/ATR/trend) |
| `daily_news` | 414 | 2026-04-26 ~ 2026-05-08 | 新闻资讯 (9列) |
| `alerts` | 24 | -- | 告警记录 (6列: rule/level/message/acknowledged) |
| `execution_logs` | 46 | -- | 执行日志 (6列: task/status/duration) |

---

## 快速开始

### 1. 安装依赖

```bash
cd portfolio_tracker
pip install -r requirements.txt
```

### 2. 启动 Dashboard

```bash
streamlit run dashboard.py
```

或双击 `start_dashboard.bat`

### 3. 运行数据分析

```bash
# 运行完整分析 (四阶段)
python run_analysis.py

# 监控管理
python run_enhanced.py --health
python run_enhanced.py --alerts 24
python run_enhanced.py --stats 7
```

### 4. 配置定时任务 (可选)

```powershell
# 以管理员身份运行
.\setup_scheduler.ps1
```

触发器: 每周一至周五 15:10 (交易日收盘后)

---

## 配置说明

### 核心配置 (`config/settings.py`)

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `INDEX_CODES` | 11 个基准指数代码 | 沪深300/中证500/中证1000等 |
| `ETF_CATEGORIES` | 23 只 ETF 行业分类 | 医药/金融/军工/新能源/科技/宽基/红利/债券 |
| `SECTOR_COLORS` | 8 个行业颜色 | 用于图表统一配色 |
| `TECH_INDICATORS` | 技术指标参数 | MA(5,20)/MACD(12,26,9)/RSI(14)等 |
| `RISK_CONFIG` | 风险参数 | 无风险利率2.5%/回撤预警15% |
| `NOTIFICATION` | 通知配置 | 邮件+企业微信 (默认关闭) |
| `MONITOR_CONFIG` | 告警规则 | 5项规则 (日跌幅/回撤/集中度/波动率) |
| `SMART_ANALYSIS_CONFIG` | 智能分析 | 建议引擎+回测 (默认开启) |

---

## 版本历史

| 版本 | 日期 | 主要变更 |
|------|------|----------|
| v1.0 | 2026-04-23 | Phase 1: 多数据源 + 技术指标 + SQLite |
| v1.1 | 2026-04-24 | Phase 3: 通知机制 + 监控告警 + 运行日志 |
| v1.2 | 2026-04-25 | Phase 4: 策略回测 + 智能建议 + 智能报告; 定时任务整合 |
| v1.3 | 2026-04-27 | Dashboard 基础架构 (5 Tab) + 净值曲线/持仓分布/风险分析 |
| v1.4 | 2026-04-28 | 收益日历 (热力图) + 基准指数行情加载 |
| v1.5 | 2026-04-29 | 技术信号面板 (雷达图/详情表/布林带/RSI分布) |
| v1.6 | 2026-04-30 | 资讯面板 + 综合评估 + 操作建议 + 数据导出 |
| v1.7 | 2026-05-01 | Brinson归因 + Monte Carlo + 再平衡建议; 优化性能/样式/交互 |
| **v1.8** | **2026-05-10** | **Phase 4 增强: 多基准对比/区间收益/ETF筛选器/告警中心/市场情绪/压力测试** |

---

## 后续规划 (Phase 5)

### 5.1 自定义指标工作台
- 用户自建技术指标 (K线形态识别、自定义公式)
- 指标回测与信号验证
- 指标模板库 (共享常用组合)

### 5.2 资金流分析
- 行业资金流入/流出追踪
- 北向资金/南向资金关联分析
- 大单/主力资金监控

### 5.3 数据导出增强
- Excel 多Sheet报告 (持仓明细/收益汇总/风险报告/技术指标)
- PDF 专业报告 (含图表)
- 定时自动发送邮件报告

### 5.4 组合归因深化
- 多因子归因 (Fama-French 三因子/五因子模型)
- 行业轮动分析
- 风格暴露分析 (大/小盘, 价值/成长)

### 5.5 系统体验优化
- 性能优化 (大数据量渲染提速)
- 响应式布局 (适配不同屏幕)
- 数据刷新策略优化 (增量更新)
- 多用户/多组合支持

---

## License

MIT License

---

*最后更新: 2026年5月10日 | 系统版本: v1.8*
