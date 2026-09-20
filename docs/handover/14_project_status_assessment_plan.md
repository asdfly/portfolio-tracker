# 14 · 项目状态全面评估方案（PLAN — 仅设计，未执行）

> **状态**：本文件是评估**方案设计稿**，不含任何评估结果。实际评估须另按本方案独立执行。
> **评估对象**：`lingxi-claw/portfolio_tracker`（真实工程目录 `D:\HuaweiMoveData\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker`）。
> **设计基线**：方案设计于本机时钟实测日；执行时须重新打点（见 §1 铁律）。

---

## 0. 目标与边界

**目标**：给出项目在 8 个维度（架构 / 测试 / 数据 / 运维 / 文档 / 产品 / 安全 / 性能）的**当前**健康度，产出带证据、带优先级的风险登记，供后续整改排期。

**边界（明确不做）**：
- 不改任何生产代码 / 数据库；
- 不跑线上日报管线（`run_analysis.py` / `scheduled_run.bat`）—— 避免污染生产库 mtime（旧 P0-D 隔离缺陷）；
- 不推送、不擅自提交整改（整改须单独立项，用户拍板）；
- 不信任 2026-09-03 交接评估（00~06）的任何数字，一律重测。

---

## 1. 方法论铁律（沿用本项目既有规矩）

| 铁律 | 说明 |
|---|---|
| 证据优先 | 每条结论带 `file:line` 或**只读 SQL** 结果；无证据的结论不写 |
| 生产库只读 | 一律 `file:data/database/portfolio.db?mode=ro`、`uri=True`；评估期间**零写入** |
| 本机时钟为准 | 涉时限先 `python -c "import datetime;print(datetime.datetime.now())"`，会话头 `<current_time>` 不可信 |
| 信任但验证 | 09-03 评估的数字、worker 总结一律重跑验证，不采信 |
| 诊断 / 判定分离 | 先列事实与证据，再给裁定（GO / CONDITIONAL / NO-GO），两节分开 |
| 显式基线 | 报告头固定写：评估日期、所测 commit SHA、环境（venv313 / python 3.13.12） |

---

## 2. 评估维度、证据源与方法

| # | 维度 | 主要证据源 | 方法 |
|---|---|---|---|
| 1 | 代码架构与健康度 | `src/data_loader.py`(2036) / `src/advisor.py`(1939) / `tabs/_helpers.py`(1706) / `tab8_advice.py`(1677) / `signal_backtest.py`(1399)；`src/analysis/*.py` 直连 DB 情况 | 行数 / 圈复杂度静态扫描；`grep` 绕过 `get_db_connection` 的 `sqlite3.connect`（`portfolio_risk.py` / `nav_engine.py` / `etf_position.py` / `rebalance_engine.py`）；死代码与重复检测 |
| 2 | 测试健康度 | `tests/`（~85 文件 / ~1570 用例）、`tests/conftest.py` 守卫、`pytest.ini` | 全量跑（**后台或加超时**，默认 120s 会在 ~82% SIGTERM）、覆盖率、conftest 隔离守卫干净度、`test_imports` 门禁；陈旧断言（如 `test_advisor_function_count`）复核 |
| 3 | 数据完整性 | `docs/handover/07_known_data_issues.md` 各项、`12_engineering_invariants.md` 判据、`portfolio_snapshots` / `etf_features` / `fund_flows` 等表 | 只读 SQL 重验 07 的 13 例折算、复制行 `replica_void` 守卫、OTC NAV 覆盖（`#138` 裁决）、`fund_flows` NULL 诚实化、已封存 `etf_predictions` 零调用 |
| 4 | 运维就绪度 | `scheduled_run.bat` / `run_analysis.bat`、`data/backups/`、`logs/scheduled_run.log`、`.env.example`、`Dockerfile`、`requirements.txt`、CI 配置 | 备份保留/同盘/演练；调度存在性（**沙箱内 `schtasks` 被拦，须用户本地确认**）；日志 rc 与阶段完整性；依赖锁版（`>=` 漂移）；Docker/CI 可用性 |
| 5 | 文档准确性 | `docs/handover/00~13`、`12_`、`README.md`、`CHANGELOG.md`、本方案 §6 | 交叉引用完整性、过时横幅有效性、与代码/库实测的一致性（已部分完成，见 §6 复评） |
| 6 | 产品 / 功能完整性 | 17 个 Tab、`advisor` 闭环、`D1~D15` 路线、tab17 F2 门控 | 逐一核对 Tab 是否真实可用（非空壳）、`D12` 缺口、tab17 `F2` 数据门控、市场事件「待补采」空洞（`collect_core.py` / `market_events.py`） |
| 7 | 安全 | `.env.example`、硬编码路径（`settings.py` `TDX_EXPORT_DIR`、`C:/Users/HUAWEI/Downloads`、`setup_scheduler.ps1`）、依赖 CVE、DB 是否在 VCS（**已实测：`.gitignore` 排除 `data/database/`，历史无 `.db` blob**） | 密钥残留扫描、机器专属路径环境变量化检查、依赖 `pip-audit` / `uv` 锁版核对、确认生产库未入库 |
| 8 | 性能 / 可扩展性 | `logs/scheduled_run.log` 各阶段耗时、`database is locked` 出现次数（旧 `03:531` 实测 0 次）、WAL 模式 | 日报全流程耗时分段、并发写窗口（16:30 / 16:40 自动化）冲突、大表行数增长趋势 |

---

## 3. 执行路线（分阶段，可并行）

**Phase 0 · 基线锁定（~10 min）**
- 打本机时钟；`git rev-parse HEAD` 记所测 SHA；确认 `venv313` 与 python 3.13.12；生产库只读 URI 自测连通。
- 产出：报告头基线块。

**Phase 1 · 仓库静态证据（可并行，只读）**
- `git log --oneline -N` 看近期演进；`git ls-files` 清点文件 / 行数；`requirements.txt` 依赖漂移扫描；`grep` 全仓 `TODO/FIXME/密钥/硬编码路径`。
- 并行：维度 1、7 的静态部分。

**Phase 2 · 代码健康度（工具辅助）**
- 用 `radon` / `flake8` 或自写行数+复杂度扫描 god file；DB 直连审计（`grep -n "sqlite3.connect"` 排除 `conftest` 重定向）；死代码检测。
- 产出：god file 清单、DB 访问一致性表、复杂度热点。

**Phase 3 · 测试健康度（注意超时）**
- **后台跑全量**（`run_in_background` 或超时提到 ≥400s）：`pytest -q -p no:cacheprovider`，记 `collected/passed/failed/skipped`。
- 验证 conftest 隔离守卫在跑后**干净**（无生产库 mtime 污染告警）；复核陈旧断言是否仍误报。
- ⚠️ 默认 120s 超时会在 ~82% SIGTERM，看似失败实则被切 —— 必须后台或加超时。

**Phase 4 · 数据完整性（只读 SQL）**
- 按 `07` 逐项重验：13 例折算台阶 `qty_ratio/mv_ratio`、复制行 `replica_void` 行数与不变量（如 `100032` 短段不得 void、ETF×ETF=231）、OTC NAV 覆盖（双口径：全量 vs OTC）、`fund_flows` NULL 行保留情况、已封存 `etf_predictions` 零调用方。
- 每条结论贴 SQL 与结果。

**Phase 5 · 运维就绪度（需用户配合项单列）**
- 备份：列 `data/backups/` 顶层保留数 / 体积 / 是否同盘；还原演练为**计划项**（不实际执行破坏性 drill 除非用户授权）。
- 调度：备份时间戳反推 + **请用户本地 `schtasks` 确认** 15:30 任务注册。
- Docker / CI / pre-commit 门禁可用性复查。

**Phase 6 · 产品 / 安全 / 性能**
- 17 Tab 可用性核对（渲染冒烟 + 数据非空）；`D12` / `F2` 门控；市场事件空洞清单。
- 安全：依赖 `pip-audit`、密钥/路径扫描。
- 性能：日志耗时分段 + `database is locked` 计数。

**Phase 7 · 综合与交付**
- 汇总 8 维裁定 → 状态矩阵；重排 P0/P1/P2 风险登记（含 owner / 修复建议 / 是否需用户授权）；与 09-03 评估做**漂移对照**（哪些 P0 已解决、哪些新增）。

---

## 4. 输出物规格

每份维度小节固定结构：
```
## 维度N · <名称>
- 范围：<评估了什么>
- 方法：<命令/SQL/工具>
- 证据表：| 发现 | 证据(file:line 或 SQL) | 严重度 |
- 发现：<事实罗列>
- 裁定：GO / CONDITIONAL / NO-GO（附条件）
- 建议：<可执行的下一步>
```

总览交付：
1. **执行摘要（TL;DR）**：1 段结论 + 状态矩阵（维度 × 健康度评级）。
2. **风险登记册**：P0/P1/P2，每项含 来源维度、证据、修复要点、是否需用户授权。
3. **漂移对照表**：vs 2026-09-03 交接评估（00~06）—— 已解决 / 仍开放 / 新增。
4. **待用户拍板项**：明确列出需授权才能做的整改（如 filter-repo 已无需、但备份演练 / 依赖锁版 / 调度确认等）。

---

## 5. 验收 / 出口准则

- [ ] 8 个维度**全部**给出裁定（不允许"未评估"留白，除非显式标注"需用户配合无法本地验证"）。
- [ ] 每条 P0/P1 发现都有 `file:line` 或只读 SQL 证据。
- [ ] conftest 隔离守卫在测试跑后**干净**（无生产库污染告警）。
- [ ] 风险登记按 P0/P1/P2 优先级排序且可操作。
- [ ] 与 `12_engineering_invariants.md` / `07_known_data_issues.md` 的引用一致、无冲突。
- [ ] 报告经用户审阅，整改另行立项（不在本评估内）。

---

## 6. 文档维度复评（衔接已完成的审计）

文档审计已于 2026-09-20 完成并推送（`7d49cb1`）：
- `handover/00~06` + `OPS_HANDOVER_ASSESSMENT.md` 已加「09-03 时点快照 / 已过时」横幅；
- 9 个根目录重复评估文档已归档至 `docs/_archive/2026-09-03_initial_assessment/`（含 README 对照表）；
- 活跃权威源 = `07`（问题台账）、`12`（铁律总表）、`11`/`10`/`13`。
- **本评估的文档维度只需**：确认横幅仍准确、交叉引用无断链、活跃文档与代码/库实测一致，不再重复归档动作。

---

## 7. 护栏（禁止项）

- 🚫 评估期间对生产库**任何写入**（含 `INSERT/UPDATE/DELETE`、回填脚本、`ALTER`）。
- 🚫 跑线上日报管线 / 调度，避免 mtime 污染与隔离缺陷误报。
- 🚫 推送或提交整改代码（整改单独立项）。
- 🚫 在沙箱内做 git 网络操作（push/fetch 静默失败，须 `dangerouslyDisableSandbox`，但评估本身不需网络）。
- 🚫 采信 09-03 评估数字而不重测。
- 🚫 把「行数多」直接等同「坏」、把「覆盖率数值」等同「断言质量」——须结合证据判定。

---

## 8. 待用户拍板项（执行前确认）

1. **范围裁剪**：8 维全做，还是先聚焦某几维（如 代码+测试+数据）？
2. **安全维度深度**：仅静态扫描（密钥/路径/依赖），还是含依赖 CVE 渗透级核查？
3. **排期与并发**：是否启用后台全量测试（Phase 3）与并行静态扫描（Phase 1/2）？
4. **交付落库**：评估报告是否写入 `docs/handover/15_project_status_assessment_report.md` 并提交？推送是否授权？
5. **调度确认**：Phase 5 的 15:30 计划任务是否请你本地 `schtasks` 自查后回填结论？
