# 交接评估总览 (Handover Assessment Overview)

> 生成日期: 2026-09-03 | 团队: MVP 开发专家团 (portfolio-tracker-takeover)
> 范围: lingxi-claw / portfolio_tracker（Streamlit + SQLite 量化投资组合分析仪表盘）
> 真实工程目录: `D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`（原 C: 路径已空壳）
> 性质: 只读交接评估，**未改动任何生产代码与数据库**

---

## 0. 执行摘要 (TL;DR)

系统**确实在跑**——连续 4 个月日备份、多调度器驱动每日采集与日报，证明生产链路可用。但「能跑」≠「可交付 / 可接手」：

- **架构 (B/7)**：骨架健康、分层单向无环、质量工具链到位；但有 5 个 >1000 行 god file、分析模块直连 DB 绕过连接池、依赖未锁版本。
- **测试 (B−)**：套件有料、断言真实；但**隔离执行不及格**——全量跑会概率性触碰生产库 mtime，且存在陈旧硬编码断言误报。
- **运维 (未达 Bronze)**：不可回退、不可复现、不可交接。历史提交含 87.9MB 真实 DB blob、158 commit 未推送、5 个生产脚本 + 4 个自动化 runbook 从未入库、git 当前不可用。
- **产品 (健康)**：17 个 Tab 全部真实可用、无空壳占位；最大问题是**文档严重滞后于代码**（README 称 15 实已 17）。

**头号风险**：`P0-A` 历史含真实 DB blob + 158 commit 未推送 → **清理窗口就在「首次 push 之前」**，若先 push 再清即构成数据泄露自动化。

---

## 1. 四维评级速览

| 维度 | 负责人 | 评级 | 一句话 |
|------|--------|------|--------|
| 架构 | 高见远 (architect) | **B (7/10)** | 骨架健康，需定向瘦身与加固 |
| 测试 | 严过关 (qa) | **B−** | 有料但隔离不及格，暂不可作"不碰生产"回归基线 |
| 运维 | 卜宕机 (devops) | **未达 Bronze** | 能跑 ≠ 可交付，三黑洞（回退/复现/交接） |
| 产品 | 许清楚 (pm) | **健康** | 17 Tab 全可用，文档债是主缺口 |

详细文档：`01_architecture_assessment.md` / `02_test_assessment.md` / `03_devops_assessment.md` / `04_product_assessment.md`

---

## 2. 统一风险登记册 (跨维度合并，按优先级)

### P0（接手/推送前必须解决）
| ID | 风险 | 来源 | 修复要点 |
|----|------|------|----------|
| **P0-A** | 历史含真实 DB blob（commit `24574df` 含 87.9MB `portfolio.db`，仍 HEAD 可达）+ 158 commit 未推送 | devops | **必须在首次 push 前** `git filter-repo --path data/database/portfolio.db --path portfolio.db --path data/portfolio.db --path portfolio_data.db --invert-paths --force`；先 push 再清 = 泄露自动化 |
| **P0-B** | git 不可用：`.pytest_cache/.gitignore` NTFS ACL 损坏 → `git status` 崩溃 (err_win_to_posix) | devops | `rm -rf .pytest_cache` + `.gitignore` 补 `.pytest_cache/`；绕过 `git status --porcelain -- . ':!.pytest_cache'` |
| **P0-C** | 生产知识未固化：5 个生产脚本（gen_combo_report.py 58KB / run_analysis.bat / fetch_morning.py 等）+ 4 个自动化 runbook 从未入库，158 commit 未推 | devops | 先冷备，再补脚本+runbook 入库，说明 158 commit 来由 |
| **P0-D** | 测试隔离泄漏：全量跑**概率性**触碰生产库 mtime（qa 实测 run#1 触发 conftest P0 报警，run#2 未复现，CI 可能漏检） | qa | 重定向器按文件名+归一匹配；`DATABASE_PATH` 常驻 env；验收=全量跑 + guard 干净通过 |
| **P0-E** | 依赖未锁版：`requirements.txt` 全 `>=`，pandas 3.0.5 / numpy 2.5.1 已明显漂移（声明 2.0.0 / 1.24.0） | architect/devops | 出 `requirements.lock`（pip-tools / uv） |
| **P0-F** | 备份同盘 / 零演练：备份与库同物理盘，仅 7 天，无异地副本、无恢复 drill | devops | 备份拷到对象存储/另一物理盘 + 季度 `source.backup()` 还原演练 |
| **P0-G** | 机器专属硬编码路径：neodata_valuation.py 的 `.workbuddy` 技能目录、settings.py `TDX_EXPORT_DIR=C:\zd_zsone`、多处 `C:/Users/HUAWEI/Downloads`、setup_scheduler.ps1 硬编码 C: 且时间错(15:10 vs 真实 15:30) | architect/devops | 环境变量化 + 删除残留 `.env.QA_RESIDUE_20260805` |

### P1（尽快）
| ID | 风险 | 来源 |
|----|------|------|
| P1-A | Dockerfile 不可用：python:3.12-slim（venv 是 3.13）、`COPY . .` 无 `.dockerignore`（127MB DB → ~2GB 构建上下文）、无 VOLUME/HEALTHCHECK/Playwright、root | devops |
| P1-B | CI 形同虚设：仅 `:memory:` test job、pre-commit 未接入、远程 158 落后从未触发 | devops |
| P1-C | pre-commit 大文件门禁是空操作（`LARGE_FILES` 赋值后从未被引用） | devops |
| P1-D | 数据层直连泄漏：4 分析模块绕过 `get_db_connection` 直连 sqlite3（etf_position.py:57 / nav_engine.py:41 / portfolio_risk.py:281 / rebalance_engine.py:561），且未设 `check_same_thread=False` | architect |
| P1-E | 陈旧断言 `test_advisor_function_count`：`assert 32==23` 误报（advisor 已演进到 32 函数） | qa |
| P1-F | 文档债：README 称 15 Tab 实已 17、CHANGELOG `[Unreleased]` 空、advisor 17→23、库表 26 vs 30 计数不一致 | pm |
| P1-G | 数据空洞：collect_core 两融 retry 耗尽残留（2026-08-08）+ tab16 缺数据充分性红线告警 | pm |
| P1-H | NeoData token 12h TTL 是 16:40 日报链硬外部依赖（过期 exit(2)+TOKEN_EXPIRED 需 agent 刷新） | devops |

### P2（长期）
| ID | 风险 | 来源 |
|----|------|------|
| P2-A | god file 拆分：data_loader.py(2036) / advisor.py(1939) / tabs/_helpers.py(1706) / tab8_advice.py(1677) / signal_backtest.py(1399) | architect |
| P2-B | UI 层弱断言：多数 tab 90%+ 行覆盖但由 render 冒烟测试（仅不崩）达成，高行覆盖 ≠ 高断言质量 | qa |
| P2-C | 核心盲区 0–13%：portfolio.py 13.4% / indicator_backtest.py 10.6% / fund_flow.py 8.8% / monitor_push2his.py 0% / news_fetcher.py 16.7% / chart_generator.py 0% / excel_report.py 11.9% / tab14 10.6% / run_analysis.py 16.6% / sidebar.py 14% | qa |
| P2-D | 迁移无版本化（幂等 ALTER，无 Alembic） | architect |
| P2-E | `eastmoney` 悬空配置（DataSourceManager 声明 3 源，全仓无 eastmoney.py） | architect |
| P2-F | D12 缺失（D1–D15 中唯一无 `test_d12`、全仓无引用的项） | pm |
| P2-G | tab17 F2 估值因子待 `index_pe_history` 积累（<250 交易日）解锁 | pm |

---

## 3. 各维度核心结论

- **架构**：分层单向（UI→service→domain→config），TAB_REGISTRY 数据驱动注册；`src/models.py` 领域值对象清晰；`tests/conftest.py` 守卫 sqlite3 改道临时副本是强安全网；pre-commit 已配 black/isort/flake8/mypy/bandit。短板在体量集中与 DB 访问不一致。
- **测试**：85 文件 / 1498 测试函数 / 1571 collected / 2050 assert；全量 `2 failed, 1565 passed, 4 skipped`。覆盖率总体 50.6%（tabs 57.9% / src 47.6% / advisor 68.5%）。**分析/指标层（financial_metrics 91 例、risk、equity_risk_premium、rebalance_engine、etf_position、advisor）立即可信**；整仓"不碰生产"门禁暂不可信；UI/数据源层弱可信。
- **运维**：5 调度器实际在跑（Win 计划任务 15:30 + 4 WorkBuddy 自动化），但文档只写 1 个且时间/方法错。备份用 SQLite online backup API + 7 天保留（机制不差，硬伤在同盘/零演练）。Docker/CI/pre-commit 门禁均不可用或空操作。
- **产品**：17 Tab 全部真实可用，无 TODO/FIXME/空壳占位；advisor 闭环最成熟（23 函数、advice_history 1789 条）。唯一真缺口 D12；tab17 的 F2 因数据不足自动禁用（设计正确，数据够了自启）。

---

## 4. 优先级修复路线图

**阶段 0 — 解锁 git（P0-B，~10 分钟，零风险）**
1. `rm -rf .pytest_cache`；`.gitignore` 补 `.pytest_cache/` `data/` `__pycache__/` 等。
2. 验证 `git status --porcelain -- . ':!.pytest_cache'` 可用。

**阶段 1 — 止血历史 DB + 固化知识（P0-A / P0-C，需用户授权）**
3. 冷备：`cp -r data data_bak_$(date +%Y%m%d)`。
4. 确认 GitHub 远端 **Private** 后：`git filter-repo` 清除 DB blob（**必须早于首次 push**）。
5. 提交 5 生产脚本 + 4 自动化 runbook，补 `requirements.lock`。

**阶段 2 — 隔离与断言卫生（P0-D / P1-E，~半日至一日）**
6. 修 conftest 重定向器（文件名+归一匹配），使 `DATABASE_PATH` 永不回退生产默认；`test_d5` reload 前后确保常驻 env。验收：全量跑 + conftest guard 干净。
7. 修 `test_advisor_function_count`（`==23` → `>=23` 或删）。

**阶段 3 — 文档债清零（P1-F，~半天，零代码风险）**
8. README 补 tab16/tab17 并改 15→17；CHANGELOG 补 `[Unreleased]`；统一计数；改掉 `_helpers.py` 的「Stub」误导注释。

**阶段 4 — 加固（P0-E/G / P1-A~D / P2）**
9. 依赖锁版；清理机器专属路径；Dockerfile + .dockerignore + HEALTHCHECK；CI 接入 pre-commit + 禁连真库；统一 DB 访问入口；拆分 god file。

**阶段 5 — 备份与演练（P0-F）**
10. 异地备份 + 季度恢复 drill。

---

## 5. 需用户决策 / 确认的开放项

1. **GitHub 仓库 `asdfly/portfolio-tracker` 可见性（Private / Public）** —— 直接决定 P0-A 清理与推送的紧急度与安全性（gh 未登录无法自证）。
2. **Windows 计划任务 15:30 是否真实注册**（schtasks 被沙箱安全策略拦截，未直证；从 backups 时间戳反推调度在跑）。
3. **是否授权执行 P0-A 的 `git filter-repo` 历史清理**（须先确认远端 Private）。
4. **是否授权提交评估文档 + 5 生产脚本 / 4 runbook 入库**（注意：当前 git 仍受 P0-B 影响，需先阶段 0 解锁）。

---

## 6. 评估过程透明声明

- 评估中 qa 全量运行测试两次，**run#1 概率性触碰生产库 mtime 一次**（字节数不变、只读 `integrity_check=ok`、40 表行数正常）——此即 **P0-D 隔离缺陷的实证**，非数据损坏。
- 未改动任何生产代码 / 数据库内容，未提交任何改动；4 份评估文档均新建于 `docs/handover/`。
- 所有结论均带 file:line 证据，详见对应维度文档。
