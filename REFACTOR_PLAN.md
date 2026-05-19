# main() 拆分重构计划

## 当前状态
- dashboard.py: 6,986 行, main() = 5,063 行 (72%)
- 194 个单元测试全部通过 (commit ce6fe7f)
- 回退点: git tag pre-refactor-dashboard

## 拆分策略
将 main() 按 Tab 边界拆分为独立渲染函数，每个函数接收必要的 DataFrame 参数。
main() 保留为纯编排层：sidebar + 数据加载 + st.tabs 创建 + 调用渲染函数。

## 步骤

### Step 1: 提取 _render_overview() — 概览指标区 (L191-332, ~142行)
- 概览卡片(6列)、快速指标条(最大持仓/技术信号/行业分布)
- 参数: positions, summary, technical, effective_max_dd
- 无副作用（纯 st 渲染）

### Step 2: 提取 _render_tab1_net_value() — Tab1 净值走势 (L333-1004, ~672行)
- 净值曲线、收益分布、滚动指标、基准对比表、多基准对比、区间分析
- 参数: positions, summary, selected_date, show_days, selected_benchmark, technical
- 注意: 包含内部的 st.tabs 嵌套

### Step 3: 提取 _render_tab2_position() — Tab2 持仓详情 (L1005-1429, ~425行)
- ETF多维筛选、持仓表格、行业权重、相关性矩阵、累计盈亏
- 参数: positions, summary, selected_date

### Step 4: 提取 _render_tab3_risk() — Tab3 风险分析 (L1430-2070, ~641行)
- 风险评分、回撤曲线、收益归因、多因子归因、风险提示、风格暴露、行业轮动
- 参数: positions, summary, technical, selected_date, ext_risk

### Step 5: 提取 _render_tab4_calendar() — Tab4 收益日历 (L2318-2741, ~424行)
- 月份概览、日历热力图、月度收益、事件日历
- 参数: positions, summary

### Step 6: 提取 _render_tab6_technical() — Tab6 技术信号 (L2742-3116, ~375行)
- 信号概览、雷达图、信号汇总、布林带分布、RSI分布
- 参数: technical

### Step 7: 提取 _render_tab7_news() — Tab7 资讯评估 (L3117-3482, ~366行)
- 新闻面板、综合评估、市场情绪
- 参数: positions, summary, technical

### Step 8: 提取 _render_tab8_advice() — Tab8 操作建议 (L3483-3744, ~262行)
- 技术面判断、建议生成、建议汇总
- 参数: positions, summary, technical

### Step 9: 提取 _render_tab5_advanced() — Tab5 高级分析 (L3745-4306, ~562行)
- Monte Carlo、压力测试、再平衡建议、智能建议、数据导出
- 参数: positions, summary, technical, ext_risk

### Step 10: 提取 _render_tab9_custom() — Tab9 自定义指标 (L4307-4539, ~233行)
- 指标回测、K线形态
- 参数: positions, summary, technical

### Step 11: 提取 _render_tab10_fund_flow() — Tab10 资金动向 (L4540-5047, ~508行)
- 行业资金流、ETF资金流、主力资金
- 参数: positions (仅需 code 列)

### Step 12: 提取 _render_sidebar() + _render_footer()
- sidebar: 日期选择、预设、基准选择 (L116-170, ~55行)
- footer: 页脚 (L5048-5061)
- 验证 main() 缩减至 < 200 行

## 每步流程
1. 提取函数代码到 dashboard.py 顶部（与其他顶层函数并列）
2. 在 main() 中用函数调用替换原代码块
3. 运行 pytest 确认 194 passed
4. git commit

## 预期结果
- main() 从 5,063 行 → < 200 行（编排层）
- 各渲染函数独立可测试
- Tab 1-11 均可独立渲染，与 tabs/tabXX.py 形成对称结构
