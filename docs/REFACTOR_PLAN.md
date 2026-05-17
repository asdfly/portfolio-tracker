# dashboard.py main() 重构方案

## 一、背景与目标

### 1.1 现状
- `dashboard.py` 共 6948 行，`main()` 占 5242 行（75%）
- UI/业务计算/数据获取全部内联 → 无法单独测试业务逻辑
- 已导致 UnboundLocalError 在测试中无法被发现

### 1.2 目标
- 将纯计算逻辑提取为顶层独立函数，main() 仅保留 UI 编排
- 所有提取函数可独立单元测试
- **行为完全不变**

### 1.3 不在范围
- 不改阈值、不改 UI 布局、不拆分文件、不引入新依赖

## 二、安全措施
- `git tag pre-refactor-dashboard` → 回退点
- 每步 commit + pytest 115 passed 验证
- 全量回退：`git checkout pre-refactor-dashboard -- dashboard.py`

## 三、提取函数清单

| # | 函数 | 提取自 | 说明 |
|---|------|--------|------|
| F1 | `get_indicator_color(value, thresholds)` | 概览卡片 4处 | 通用阈值颜色映射 |
| F2 | `get_risk_label(score)` + `get_risk_color(score)` | tab3 行3176-3177 | 风险评分→颜色/标签 |
| F3 | `compute_risk_score(volatility, max_dd, sharpe)` | tab3 行3154-3175 | 风险评分 0-100 |
| F4 | `get_warnings(positions, max_dd, vol, sharpe, profit_count, loss_count)` | tab3 行3562-3652 | 风险告警列表 |
| F5 | `compute_comprehensive_score(...)` | tab7 行5037-5141 | 综合评分 100分制 |

## 四、实施步骤
- Step 0: Git tag
- Step 1: F1+F2 颜色辅助函数
- Step 2: F3 风险评分函数
- Step 3: F4 告警函数
- Step 4: F5 综合评分函数
- Step 5: 新增单元测试 + 最终验证
