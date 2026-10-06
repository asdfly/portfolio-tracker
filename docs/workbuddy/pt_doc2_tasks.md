# 定时任务清单与自进化机制

> 归档于 WorkBuddy 资料库「portfolio_tracker 项目知识库」

## 一、邮件发送任务（已免疫 10054 失败队列）
所有走 `send_generic_email.py` 的任务均加 `--queue-on-fail`，发送失败自动入队，由「邮件补发队列消费者（每小时）」延时补发：

| 任务 | 自动化 ID | 触发 |
|---|---|---|
| 元UP 2026款上市跟踪 | automation-1787104504506 | 按需 |
| 组合+大盘盘后日报 | automation-1787708904209 | 每日收盘后 |
| 投资组合次日补采 | automation-1785979940493 | 每日 09:00 |
| 投资组合每日巡检补采 | automation-1785911636011 | 每日 16:30 |
| A股轮动复盘·周度 | automation-1785830717443 | 每周一 00:00 |
| A股轮动复盘·月度 | automation-1785830717736 | 每月 1 日 |

- 补发队列目录统一：`C:/Users/HUAWEI/.workbuddy/mail_retry_queue`
- 消费者：`a535ea36`（每小时；`attempts>=6` 或存活 >24h 转死信）

## 二、其他分析 / 复盘任务
- 主分析：Windows 任务计划 15:30（非 WorkBuddy automation）
- 圆桌投资大师专家团（贺知衡主理）：周度验证（周五 16:00）、9/30 终验复盘，落盘 `deliverables/investment-masters/`
- 板块深度研究（创新药、券商）到标的级，配套可证伪命题

## 三、自我进化机制（automation-self-evolution）
「载入 → 执行 → 反思 → 记录」闭环：

- **部署标志**：任务 prompt 含「step 0 载入 evolution 文件」+「末尾反思步骤」；evolution 文件（`.workbuddy/evolution/*.md`）真实存在且有待办项
- **执行**：每次运行先 Read evolution 文件，纳入待执行改进项；运行后反思新问题追加到「待执行改进项」，已解决移「历史改进记录」（≤20 条）
- **现状**：全部 9 个管线 / 分析任务已部署并实际执行（evolution 文件真实，每任务 7~30 条待办）

## 四、铁律（自动化执行约束）
- 仅 D: 盘 `portfolio.db` 为生产库；生产库只读分析
- git 显式 pathspec，不 `git add -A`；无授权不 push
- 数字需显式或显式拒绝；数据源标注来源与时间，无法核实标「—」，严禁编造
- 不构成投资建议
