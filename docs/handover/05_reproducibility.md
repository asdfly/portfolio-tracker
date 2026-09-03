# 05 - 可复现构建与换机器交接指南（Reproducibility & Handover）

> 本文档对应整改任务 **P0-E（锁定依赖）** 与 **P0-G（清理机器相关硬编码路径）**。
> 目的：让另一位工程师在**全新机器**上从零复现本项目的运行环境，无需原机专属信息。
> 生成日期：2026-09-03。配套产物：`requirements.lock`（项目根）、`requirements.txt`（已补全直接依赖）。

---

## 1. 环境要求

| 项目 | 要求 | 说明 |
|------|------|------|
| 操作系统 | Windows 10/11 | 项目依赖 `playwright`、`.bat` 定时脚本、通达信导出等，当前仅验证 Windows |
| Python | **3.13.x** | 实测 venv 为 `Python 3.13.14`（CPython）。锁文件基于此版本生成，建议保持一致以避免 pandas 3.0 / numpy 2.5 的 ABI 差异 |
| 虚拟环境 | 项目自带 `venv313/` | 位于项目根目录；重建步骤见第 3 节 |
| 包管理器 | pip（随 venv 提供） | 锁文件用 `pip freeze` 生成 |

> 注意：`venv313/` 本身**不入库**（`.gitignore` 已忽略 `venv/` 类目录，但本目录名为 `venv313`，请确认提交前不会被误带）。换机器时按第 3 节重建，**不要**直接拷贝 `venv313/`。

---

## 2. 依赖管理：两个文件的分工

| 文件 | 内容 | 用途 |
|------|------|------|
| `requirements.txt` | 直接依赖声明（包名 + `>=` 最低版本），**人类可读** | 日常阅读、升级依赖时编辑 |
| `requirements.lock` | `pip freeze` 完整快照（运行时 + 开发依赖全部 `==` 锁定，共 92 行） | **换机器复现**时安装，保证字节级一致 |

**关键结论**：`requirements.txt` 只声明"直接依赖 + 最低版本"；`requirements.lock` 是"完整可复现快照"。复现环境务必用 `requirements.lock`，不要只用 `requirements.txt`（后者未锁定间接依赖版本，换机器可能装到不同版本）。

`requirements.lock` 已做清理：
- 构建工具（`pip` / `setuptools` / `wheel`）：`pip freeze` 默认不输出，本快照不含，无需剔除。
- 本地文件路径行（`xxx @ file:///...`）：本次 freeze **未出现**，无需剔除。
- 全部 92 行均为可跨机器解析的 PyPI 包 `==` 锁定。

---

## 3. 从零复现步骤

```bat
:: 1) 安装 Python 3.13.x（与 venv313 一致），并加入 PATH

:: 2) 进入项目根目录，创建虚拟环境
python -m venv venv313

:: 3) 激活虚拟环境
venv313\Scripts\activate

:: 4) 用锁文件安装完整依赖（可复现，约数百 MB）
python -m pip install -r requirements.lock

:: 5) 从模板创建本机 .env（详见第 5 节变量清单）
copy .env.example .env
::    然后手工编辑 .env，填入本机凭证（尤其是邮件授权码）

:: 6) 验证
python -m pytest tests/test_d5_env_config.py tests/test_d6_root_cleanup.py -q -p no:cacheprovider
```

> 复现环境时**不要**跑 `run_analysis.py` / `run_morning.py` / `scripts/*` 等采集入口，也不要动 `data/database/portfolio.db`（生产库，116MB）。

---

## 4. 换机器必须手工改的「环境绑定点」清单

以下为**有意保留**的环境绑定（无法也不应改为相对路径），换机器时必须手工调整。

### 4.1 Windows 任务计划程序调度绑定（`setup_scheduler.ps1`）

任务计划程序**必须使用绝对路径**，因此以下两处保留为原机绝对路径，是"有意保留的环境绑定点"：

| 文件 | 行号 | 原内容（片段） | 换机器操作 |
|------|------|----------------|------------|
| `setup_scheduler.ps1` | 12 | `$ScriptPath = "C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker\run_analysis.bat"` | 改为新机器实际项目路径 |
| `setup_scheduler.ps1` | 35 | `-WorkingDirectory "C:\Users\HUAWEI\Documents\lingxi-claw\portfolio_tracker"` | 改为新机器实际项目路径 |

> 另：由该脚本注册到 Windows 任务计划程序的"任务动作"同样使用绝对路径，注册时需手工填入新机器路径。
> ⚠️ 本项目的 `.bat` 文件**必须是 CRLF 换行**（已校验：根目录 5 个 `.bat` 与 `setup_scheduler.ps1` 均为 CRLF、零裸 LF）。若日后编辑 `.bat`，改完务必用 `od -c` 确认行尾为 `\r\n`，否则 cmd.exe 解析错乱会导致"日志假成功、邮件静默未发"。

### 4.2 NeoData / WorkBuddy 运行时依赖（已改为按用户主目录推导，但仍需环境具备）

以下脚本原本硬编码了 `C:/Users/HUAWEI/.workbuddy/...`，本次整改已改为按当前用户主目录推导（不再含用户名）：
- `scripts/fetch_market_data.py`（`ROOT` 改为脚本位置推导；`PY` 改为 `sys.executable`；`QS` 改为 `~/ .workbuddy/...`）
- `scripts/backfill_sector_change.py`（`QUERY_PY` 改为 `~/ .workbuddy/...`；`PY` 改为 `sys.executable`）
- `src/data_sources/neodata_valuation.py`（`NEODATA_SKILL_DIR` 改为 `Path.home() / ".workbuddy/..."`）

**换机器仍需**：安装 WorkBuddy，且 `neodata-financial-search` skill 存在于默认位置（`~/.workbuddy/skills/neodata-financial-search`）。这是"软绑定"（不硬编码用户名），但依赖 WorkBuddy 运行时存在。生产调度（`scheduled_run.bat` 的纯 Python）无法调用 NeoData，相关回填需在 WorkBuddy 会话内执行。

### 4.3 通达信持仓导出路径（第三方软件标准位置）

非本项目机器路径，而是通达信软件的标准导出目录，任何装了通达信的 Windows 都在该位置：
- `config/settings.py`：`TDX_EXPORT_DIR` 默认 `r"C:\zd_zsone\T0002\export"`（可用环境变量覆盖）
- `scripts/import_aug_2026.py:42`：默认 `c:/zd_zsone/T0002/export/持仓股20260831.xls`

如导出目录不同，设置环境变量 `TDX_EXPORT_DIR` 覆盖即可。

### 4.4 `.env` 文件

`.env` **不入库**（`.gitignore` 已忽略）。换机器必须从 `.env.example` 复制并填入本机凭证（见第 5 节）。

---

## 5. `.env` 需要的变量（仅变量名 + 用途，不含任何值）

> ⚠️ `EMAIL_PASSWORD`、`WECHAT_WEBHOOK_URL` 属敏感凭证，**严禁写入任何文档或提交到 Git**，仅在本机 `.env` 中填写。

| 变量名 | 用途 | 是否敏感 | 默认值/说明 |
|--------|------|----------|-------------|
| `DATABASE_PATH` | 数据库文件路径 | 否 | 默认 `data/database/portfolio.db` |
| `TDX_EXPORT_DIR` | 通达信持仓导出目录 | 否 | 默认 `C:\zd_zsone\T0002\export` |
| `EMAIL_ENABLED` | 是否启用邮件通知 | 否 | `true` / `false` |
| `EMAIL_SMTP_SERVER` | SMTP 服务器地址 | 否 | 默认 `smtp.qq.com` |
| `EMAIL_SMTP_PORT` | SMTP 端口 | 否 | 默认 `587` |
| `EMAIL_USERNAME` | 发件人邮箱 | 否 | 需填写 |
| `EMAIL_PASSWORD` | 邮箱**授权码**（非登录密码） | **是** | 需填写，勿提交 |
| `EMAIL_RECIPIENTS` | 收件人列表（逗号分隔） | 否 | 需填写 |
| `WECHAT_ENABLED` | 是否启用企业微信通知 | 否 | `true` / `false` |
| `WECHAT_WEBHOOK_URL` | 企业微信 Webhook URL | **是** | 需填写，勿提交 |
| `ALERT_DAILY_LOSS_THRESHOLD` | 单日跌幅告警阈值（%） | 否 | 默认 `-3.0` |
| `ALERT_DRAWDOWN_THRESHOLD` | 最大回撤告警阈值（%） | 否 | 默认 `-10.0` |
| `ALERT_DEDUP_INTERVAL_HOURS` | 告警去重间隔（小时） | 否 | 默认 `6` |
| `ADVICE_ENABLED` | 是否启用智能建议 | 否 | 默认 `true` |
| `ADVICE_MIN_CONFIDENCE` | 建议最小置信度 | 否 | 默认 `0.6` |

---

## 6. 已知测试问题（与本次整改无关，记录供参考）

- **`tests/conftest.py` 存在预置递归 bug（与 P0-E / P0-G 无关）**：
  `conftest.py` 中 `_install_sqlite_redirector()` 的临时诊断代码把 `_real_connect` 重指向自身（`_real_connect_traced` 内部又调用 `_real_connect`），导致**任何 `sqlite3.connect(...)` 调用无限递归**（`RecursionError`）。
  影响：`test_db_schema.py` 中实际打开连接的用例、以及 `test_dashboard_logic.py`（收集阶段即触发）等全部"触碰 sqlite"的测试失败/收集报错。
  本次整改**未改动** `conftest.py`、`test_db_schema.py`、业务 DB 连接代码，这些失败为**既有问题**，不在本次 P0-E/P0-G 范围，建议由测试负责人（qa-3）清理该临时诊断代码后复测。
- **本次路径相关回归结果**：`test_d5_env_config.py`、`test_d6_root_cleanup.py` 全部通过（共 48 passed）；失败项均为上述 conftest 递归，非本次改动引起。

---

## 7. 本次整改变更清单（P0-E / P0-G）

**P0-E 锁定依赖**
- 新增 `requirements.lock`（项目根）：`pip freeze` 完整快照，92 个包 `==` 锁定，含注释头（生成日期 2026-09-03 / Python 3.13.14 / 生成命令 / 用法说明）。
- 校验：`pip install --dry-run --no-deps -r requirements.lock` 返回 exit 0（全部 "Requirement already satisfied"，可解析）；另用 `packaging.requirements.Requirement` 离线逐行校验 92/92 通过，无本地路径行。
- `requirements.txt` 补全缺失直接依赖：
  - 新增 `scikit-learn>=1.9.0`（freeze 实测 1.9.0）
  - 新增 `lightgbm>=4.7.0`（freeze 实测 4.7.0）
  - 确认已存在：`pandas`、`numpy`、`akshare`、`plotly`、`streamlit`（均已在列）

**P0-G 清理硬编码路径**
- 已修（`.py` 源码写死绝对路径 → 改为项目相对 / 用户主目录推导）：
  - `scripts/fetch_market_data.py:20-24` — `ROOT` 改 `os.path` 基于 `__file__`；`PY` 改 `sys.executable`；`QS` 改 `os.path.expanduser('~')`
  - `scripts/backfill_sector_change.py:25-28` — `QUERY_PY` 改 `expanduser('~')`；`PY` 改 `sys.executable`
  - `src/data_sources/neodata_valuation.py:29` — `NEODATA_SKILL_DIR` 改 `Path.home()`
  - `scripts/import_aug_2026.py:41`（代码）+ `:5`（文档字符串）— `PDF_PATH` 改 `expanduser('~')`；docstring 改为 `~/Downloads` 占位
- 有意保留（不修改，换机器手工改，见第 4 节）：
  - `setup_scheduler.ps1:12`、`:35` — 任务计划程序所需绝对路径
- 未改动：`venv313/`（虚拟环境）、`logs/*.log`、`archive/`（均已 gitignore）、`docs/*.md`（说明性路径）、根目录 `.bat`（经校验无绝对路径、均为 CRLF）
