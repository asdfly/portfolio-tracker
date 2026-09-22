# 新会话准备指引 (NEW_SESSION_PREP)

> 用途：在 WorkBuddy 启动**新会话**继续 `portfolio_tracker` 项目前的准备清单。
> 本文件是**简明版**；完整铁律见 `docs/handover/12_engineering_invariants.md`（随仓库走，权威源）。
> 最近更新：2026-09-22，仓库 HEAD = `cd22c4e`（已推送、工作区干净）。

## 0. 一句话交接
新会话开局第一句：
> 继续 portfolio_tracker，先读 `docs/handover/12_engineering_invariants.md` 与 `.workbuddy/memory/MEMORY.md`，再动手。

## 1. 不要开成全新空工程（最重要）
本项目沉淀了重型"铁律索引"，新会话若不加载它，**必重蹈覆辙**（静默改坏生产库、假成功、任务名错配等）。
- **首选**：重开同一个 WorkBuddy 工程（根目录 `D:/HuaweiMoveData/Users/HUAWEI/Documents/lingxi-claw/portfolio_tracker`），`.workbuddy/memory/` 自动载入（28 个日期日志 + `MEMORY.md`）。
- **若必须开新工程**：开局先令其读 `docs/handover/12_engineering_invariants.md`（铁律全集）+ `MEMORY.md`。

## 2. 起点状态（已实测，直接继承）
- 仓库 HEAD = `cd22c4e`，工作区干净（tracked 无 diff vs HEAD）
- 生产定时任务「投资组合每日分析」Ready，每周工作日 15:30 → `scheduled_run.bat`
- `venv313` 在；Python 3.13 已对齐（Docker/CI）；备份上限 = 7；P1-D 直连已收敛；setup_scheduler 已修
- `scripts/restore_db.py` **仍缺失**（P0-F 待办，见 §4）

## 3. 必须继承的"静默失效高发"铁律（摘要）
- 只认 `data/database/portfolio.db`；其余 `.db` 全是副本
- 诊断库用 `file:...?mode=ro` + `uri=True`；写库前先备份
- Git：显式 pathspec（**禁 `git add -A` / `git add .`**）、`git status` 已禁（用 `git diff --name-status HEAD`）、`git push` 须 `dangerouslyDisableSandbox` + `git ls-remote` 逐字验真
- Git Bash **无 coreutils**（ls/cat/head/tail/grep/wc 均 127）→ 用 python / Read / Grep / Glob
- `python -c` 禁反引号；`&&` 断链静默跳过 → 收尾 `echo "chain OK"` 验链
- 数字要么显式标记、要么显式拒答，不许静默；**不信任会话头的 `<current_time>`**，以本机时钟为准
- 测试计数**必须看 `collected`**（基线 1881 collected / 1877 passed / 4 skipped / 0 failed @ `e957464`）；默认 120s 超时会在 ~82% 处 SIGTERM，全量须 `run_in_background` 或加大超时
- 删除操作须**精确文件名清单**，禁前缀/glob
- 全量回归：`venv313/Scripts/python.exe -m pytest -q -p no:cacheprovider`（约 3m20s）

## 4. 可直接接手的真待办
1. 给依赖真实 DB/网络的用例补 `@pytest.mark.integration`（目前 CI 的 `-m "not integration"` 因 0 处打标是空操作）
2. `scripts/` 下 20+ 处 `sqlite3.connect`（一次性脚本/探针）尚未收敛到 `get_db_connection`
3. **P0-F 异地副本 + 恢复演练：`scripts/restore_db.py` 仍 MISSING**，需创建
4. `snapshot_gate` / `price_history_gate` 两处直连（busy_timeout=15000，拒收 `file:` URI）待定
5. evolution 台账若干项（`#129/#130` fund_flows 覆盖链、`#140` tab8 哨兵值 `50` 与中性值碰撞等）

## 5. 关键路径速查
| 项 | 路径 |
|----|------|
| 铁律全集 | `docs/handover/12_engineering_invariants.md` |
| 生产库 | `data/database/portfolio.db` |
| Python | `venv313/Scripts/python.exe` |
| 调度脚本 | `setup_scheduler.ps1`（任务名 = `投资组合每日分析`） |
| 备份目录 | `data/backups/`（顶层留最近 7 个） |
| 运行账本 | `data/reports/run_report_<date>.json`（回填会被就地覆盖，非不可变物证） |
