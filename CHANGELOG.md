# Changelog

所有重大变更均记录在此文件中。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/)。

---

## [Unreleased]

---

## [v2.2] - 2026-06-04

### 新增
- **D1 闲置数据分析激活**: 融资融券/机构调研/大宗交易 12.8 万行数据接入 advisor 分析引擎，新增 3 个分析方法和 20 个测试
- **D2 数据源修复**: 重写股东增减持采集器，修复 4 个 STALE 数据源，增加自动健康检查
- **D3 闭环反馈**: advice_history 表写入打通（13 列 31 行），Tab8 反馈面板 SQL 修复，追踪"建议→执行→效果"完整链条
- **D4 告警多样化**: Monitor DEFAULT_RULES 从 5 条扩展到 9 条（新增数据源中断/数据质量/持仓变化/总市值变化），添加去重逻辑 `_get_recent_alert_rules`
- **D5 配置环境变量化**: settings.py 添加 `_load_env_file()` + `env()` 辅助函数，敏感配置（邮箱密码/webhook/数据库路径等）支持 .env 文件覆盖，创建 .env.example 模板
- **D6 根目录清理**: 20 个根目录 .py 文件缩减至 2 个核心文件，创建 scripts/backfill/（6 个脚本）+ scripts/setup/，统一 backfill 入口 scripts/run_backfill.py，归档 14 个遗留脚本到 archive/
- **D7**: 确认已在 D3 完成（advice_history 闭环写入）
- **D8 日志轮转**: run_analysis.py 和 run_backfill.py 改用 RotatingFileHandler（10MB，5 备份）
- **D9 回测策略扩充**: backtest.py 新增 `backtest_momentum`（动量策略）+ `backtest_mean_reversion`（均值回归策略），策略数 3→5
- **D10 容器化 CI**: 创建 Dockerfile（python:3.12-slim）+ GitHub Actions CI pipeline（pytest on push/PR）
- **54 个 P2 辅助模块测试**: 覆盖 11 个辅助模块（531→585 total）
- **P0+P1 关键测试**: 55 个测试用例覆盖 Monitor/Notification/SmartReport/RunAnalysis 核心链路

### 修复
- 消除全部 15 个 pytest warnings（477 passed，0 warnings）
- 资金流阈值单位校准（亿元→元）和显示格式统一
- advice 反馈 SQL 列名不匹配修复（3 处）
- 融资融券数据单位统一（亿元→元）
- ETF 资金流 keep_cols 缺失列补全（超大/大/中/小流入）
- start_with_venv.bat 引用路径更新
- news sentiment 分析集成 + Plotly range 弃用迁移

### 变更
- 测试总数 421 → 655（+234 tests）
- 数据库表 advice_history schema 扩展（+4 列：status/action_taken/feedback/resolved_at）
- .gitignore 添加 .env/.env.local/archive/ 排除规则

---

## [v2.1] - 2026-05-13

### 新增
- **黄金市场分析 Tab (Tab11)**: 上海金交所金价 K 线走势、实时分时行情、SPDR Gold Trust 持仓趋势、中国黄金储备图表
- **行业资金流历史回填**: 基于同花顺多周期排行差值分解法，自动回填行业历史资金流数据
- **自动增量回填集成**: 行业资金流回填集成到每日定时任务，自动跳过已有日期

### 修复
- 资金流趋势图/热力图过滤数据稀疏行业（最低 10 天数据覆盖要求）
- ETF 资金流查询去掉 LIMIT 2000 截断
- 同花顺资金流数据单位统一（亿元 → 元）
- requests Session monkey-patch 递归 Bug
- Plotly 6.x titlefont 废弃属性迁移为 title_font_color
- 中国黄金储备日期解析支持 YYYY年MM月份 格式

---

## [v2.0] - 2026-05-11

### 新增
- **Phase 5 全面升级**:
  - Tab9 自定义指标工作台（模板回测、信号追踪）
  - Tab10 资金流分析（主力资金/行业资金流/ETF 资金流三维度）
  - 组合归因深化（Brinson 分解、风格因子暴露）
  - 数据导出增强（CSV 一键导出持仓/摘要/指标）
- **数据采集层**: 资金流数据采集双方案 fallback + push2his 封锁监测工具
- **行业资金流**: TOP10 时间趋势折线图、持仓 ETF 合计资金流日净流入趋势图

### 修复
- 夏普比率计算错误及数据质量问题
- 新闻资讯去重 + 减少页面显示条目
- 资金流模块代理拦截 + 行业资金流接口切换
- 指标回测模板条件不匹配（趋势方向/信号类型对齐）
- 多处变量名错误修复（csv1/df_prev1/current_mv1/prev_mv1/bench_prev1）
- 行业涨跌热力图条形不可见问题
- 智能分析报告风险指标为 0 和行业热力图收益率异常

---

## [v1.7] - 2026-05-10

### 新增
- Tab7 资讯与评估面板
- Tab8 操作建议面板
- Tab4 事件日历
- 侧边栏增强、持仓技术信号列、一键报告导出

---

## [v1.6] - 2026-05-09

### 新增
- Tab1 基准对比表
- Tab2 累计盈亏柱状图
- Tab3 风险提示面板
- Tab6 技术信号面板（概览卡片、雷达图、信号柱状图、HTML 详情表、布林带/RSI 分布图）

---

## [v1.5] - 2026-05-09

### 新增
- Tab4 事件日历模块
- Tab6 技术信号模块
- 项目文档

---

## [v1.3] - 2026-04-30

### 新增
- Dashboard 性能优化（缓存/降采样/索引/日期选择器）
- 完整历史数据回填脚本（新浪 K 线接口）
- 收益日历模块
- 持仓相关性矩阵、ETF 详情抽屉、多基准指数选择器
- 截图和 PDF 导出（Selenium + webdriver-manager）
- 模块标题和指标卡片 hover 提示

### 修复
- 每日快照写入防陈旧机制
- 废弃 API 调用和 SQL 注入风险消除
- Dashboard 3 个运行时错误修复
- daily_return 数据不一致修复

---

## [v1.0] - 2026-04-26

### 新增
- 投资组合跟踪分析系统基线版本
- Streamlit Dashboard（11 个标签页）
- SQLite 数据库（20 张表）
- 自动化数据采集与分析流程
