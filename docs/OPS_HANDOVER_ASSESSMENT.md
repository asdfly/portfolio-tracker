# 运维交接评估 - portfolio_tracker

卜宕机（运维）｜2026-08-05｜HEAD 24574df｜远端 github.com/asdfly/portfolio-tracker

## 摘要（结论先行，900 字内）

结论 **REJECT**。不可按"成熟项目"直接接手；但缺陷集中且可在一周内收敛，最危险的一项仍在可逆窗口内。

**四条阻断项**

1. 84MB 真实持仓 SQLite 已提交在本地 24574df，**尚未 push**（origin/master 停在 7cbada1，本地领先 130 提交）。推上去不可撤回，且按日提交约增 28MB，数周内触顶 GitHub 单文件 100MB 硬限。接手方第一件事是**先别 push**。
2. 两道大文件防线都是摆设：手写 pre-commit hook 把检查结果赋给变量后从不使用，框架的 `check-added-large-files` 因此从未运行。库是 `git add -f` 绕过 `.gitignore` 进来的。
3. 自动备份停摆 65 天，唯一一份备份是人工拷贝，同盘、无异地、无恢复演练；两套备份实现的清理 glob 还会互删对方产物。
4. **不存在可用的回归基线**：CI 最后一次运行（6/5）本身就是红的，`ci.yml` 此后 130 个提交一字未改。

**一条贯穿全局的根因：系统性静默降级。** 通达信路径失配、缺 snownlp/jieba、缺 notification.json、库不存在跳过备份、sqlite 静默建 0 字节库、集成测试宽泛 `except`——六处失败全部无告警、无退出码。备份能停 65 天无人察觉不是意外，是这套失败处理风格的必然结果。先让失败可见，才谈得上运维。

**前三步动作**

- **P0-1（push 之前）**：撤销 24574df，`git rm --cached` 三个 db，换回 pre-commit 框架并启用大文件门禁。
- **P0-1.5（与 push 同批）**：删掉 `ci.yml:28` 的 `DATABASE_PATH=":memory:"`，改用 `db_schema.init_all_tables()` 建 fixture 库。不做这步，首次 CI 预期红约 67，真实的 3.12 与 playwright 兼容性问题会被淹没。
- **P0-2（24 小时内）**：备份收敛为 `backup_db.py` 一套，重建计划任务并确认写日志，加异地副本。

**生产就绪总档：Bronze 以下**，短板为安全、发布安全、测试回归三项。

以下为详证与逐条行号依据，供接手方复核，不计入摘要长度约束。

## 一、当前交付与部署方式

无线上部署，实为**本机 Windows 单机运行**：`run_all.bat` 或 `streamlit run dashboard.py --server.port 8501`。

Dockerfile 存在但从未验证：无 docker-compose、无 HEALTHCHECK、**无 .dockerignore**，`COPY . .` 会把 84MB 库、`venv/`、`.env` 全烤进镜像。

CI 仅 1 个 job：master 上跑 `pytest --timeout=120`（Py3.12，`DATABASE_PATH=:memory:`）。无 lint、无 build、无部署——pre-commit 配了 black/flake8/mypy/bandit，CI 一个都没调。

**Python 版本五分裂**：生产 `scheduled_run.bat:6` 硬编码 `C:\Program Files\Python310\python.exe`（绝对路径，换机即静默失败）｜本地 venv 3.10.11｜系统 3.13｜CI 与 Docker 3.12｜`pyproject.toml` black `py39`、mypy `3.9`。注意 3.9 已于 2025-10-31 EOL，而 **3.10 将于 2026-10 EOL，距今约 2 个月**。

## 二、关键风险

### (a) 84MB 库入 git（P0，但窗口仍在）

blob 87,949,312 字节，提交于 24574df。**关键：尚未推送**——`origin/master` 停在 7cbada1，本地领先 130 提交，`merge-base --is-ancestor` 判 NO。只要不 push，修复近乎零成本。

`.gitignore` L23/L85 本已覆盖，系 `git add -f` 绕过。两道防线全废：`.git/hooks/pre-commit` 是手写脚本，其大文件检查把结果赋给 `LARGE_FILES` 后**从不输出、不累加 ERRORS**，构造性空操作；框架的 `check-added-large-files` 因此从未运行。

持续危害：工作区库已涨到 88,752,128 且处于 modified。SQLite 整文件变更，按日提交约增 28MB/天，GitHub 单文件硬限 100MB，**数周内 push 必然硬失败**。库内是真实持仓流水，推上去无法真正撤回。

修复：`git reset --soft HEAD~1` → `git rm --cached` 三个 db → 确认忽略生效再 push。历史另有 4 个早期空 db 提交，需彻底清理再用 git-filter-repo（勿用已弃用的 filter-branch）。

### (b) 依赖未锁版本 + 三个依赖未声明

13 个包全 `>=`，无 lock。akshare/streamlit/plotly 高频变更，**代码不改也可能今天绿明天红**；镜像不可复现，回滚镜像≠回滚依赖。

更严重的是三个实际使用的包**根本没进 requirements.txt**，且两类故障模式截然不同：

- **pillow → 硬崩**。`dashboard.py:26` 是顶层 `from PIL import Image`，无 try 保护。目前靠 matplotlib 的传递依赖侥幸可用，一旦 matplotlib 换版本或后端，**整个 Dashboard 起不来**。
- **snownlp / jieba → 静默失真**。`news_fetcher.py:26-27` 在函数内 import，L45 `except (ImportError, ...)` 直接 `return 0.5`，**连 logger 都不调**。缺包时全部新闻情感分变成中性 0.5，页面照常渲染，无人可知。比崩溃更危险。

另：`news_fetcher.py:22-25` 把 **Windows 专用路径** `venv/Lib/site-packages` 硬编码插进 `sys.path`。Linux 容器下该路径不存在（应为 `lib/python3.x/site-packages`），叠加上面的 except 兜底，容器内情感分析必然静默退化为常量。

修复：`pip freeze` 固化 + 补三个依赖 + 删掉硬编码 sys.path 注入。

### (c) 备份（P0，比预想更糟）

- **自动备份停摆 65 天**：`logs/scheduled_run.log` 末次写入 2026-06-01；而今日 `portfolio_20260805.log` 是新的——分析仍在跑，但走的是**不含 Stage 0 备份**的另一入口。
- **只有 1 份**：`portfolio_db_20260803.db`，命名既不匹配 `backup_db.py` 也不匹配 bat 产物，**是人工拷贝**。声称保留 7 天，实际 1 份。
- **两套实现打架**：`backup_db.py`（清理 glob `*.db`）会误删 bat（`backup_*.db`）的产物。
- **同盘同目录、无异地、从无恢复演练**。

### (d) 系统性"静默降级"反模式（根因）

备份能停摆 65 天无人察觉，不是孤例，而是全项目一致的失败处理风格——**所有故障都被吞掉，无日志、无告警、无退出码**：

| 位置 | 失败时行为 | 可见性 |
|---|---|---|
| `settings.py:56-70` | 找不到通达信目录 → fallback 到旧 `positions.tsv` | 无任何提示 |
| `news_fetcher.py:45` | 缺 snownlp/jieba → 情感分恒为 0.5 | 连 logger 都不调 |
| `run_analysis.py:523` | 缺 notification.json → 跳过全部通知 | 仅 INFO 级日志 |
| `scheduled_run.bat:38` | 库不存在 → 跳过备份 | 仅写日志，无退出码 |

后果：**系统"看起来一直正常"，实际关键功能可能已失效数月**。这是可观测维度只能给 Bronze 的根因，也是修复优先级要高于单点 bug 的原因——先让失败可见，才谈得上运维。

**第 5 例（架构师补充，本次补上源头）**：`sqlite3.connect()` 对不存在的路径不报错，静默创建 0 字节文件，查询时才炸。根目录 3 个 0 字节 .db 与今日日志里的 `no such table: stock_block_trade`（主库该表实有 9255 行）同源。前 4 例是功能降级，这一例是**数据落到错的文件**。

追加源头：**0 字节的 `portfolio.db` 本身就在 git 里**（origin/master 与 HEAD 都是 blob `e69de29`，即空文件）。任何一次 clone/checkout 都会把它铺到工作区，于是"文件存在但没有表"成为每台机器的默认初始状态——这不是偶发误操作，是被版本库固化下来的。

### (e) 该反模式已侵入测试套件本身（本次新发现，影响回归可信度）

架构师指出 4 个测试文件依赖真实 `portfolio.db`，其中 2 处硬编码 `C:\Users\HUAWEI\...` 绝对路径。我逐一追到被调用方核实，**结论与"CI 会因此变红"相反**：

| 测试文件 | 缺库时行为 | 机制 |
|---|---|---|
| `test_fix_data_quality.py` TestD4Metrics | **真失败（4 个，已逐行核实）** | L22/L35/L42/L49 四处裸 `cur.execute()` 无保护 → OperationalError 直接抛出 |
| `test_d1_margin_research_block.py` TestD1Integration（3 个） | **假绿** | `advisor.py:925/1053/1260` 捕获 `sqlite3.OperationalError` → warning → 返回 `[]`；测试仅断言 `isinstance(result, list)` |
| `test_market_event_signals.py` | **假绿** | `market_event_signals.py:61` `except Exception` 吞掉全部 5 个分析器；`test_signal_fields` 还有 `if signals:` 护栏 |
| `test_p2_reports_utils.py:63` | **假绿** | `save_report()` 不查库，`db_path` 仅存不用 |

**这比变红严重**：这些标注"使用真实数据库的集成测试"的用例，**对着一个空库会给出完全相同的绿色结果**。生产代码的宽泛 `except` 使集成测试在结构上丧失了失败能力——它们验证的是"不抛异常"，而不是"结果正确"。

直接推论：**本地实测的 "1465 passed"（QA 实跑，非 CHANGELOG 的 966）含未知比例空转用例**，不能作为回归兜底的证据。

### (f) 决定性事实：CI 从来就没绿过（本次核实，推翻共同前提）

我和架构师此前都默认"6/5 那次 CI 是绿的基线"。**该前提不成立**，四条独立证据：

1. `git cat-file -e origin/master:tests/test_fix_data_quality.py` → **存在**。四个假绿/真红文件**全部已在 origin/master**，不属于"31 个新文件"，两类问题须分开登记。
2. `git show origin/master:.github/workflows/ci.yml` → 第 28 行已是 `DATABASE_PATH: ":memory:"`；且 `git diff --stat origin/master HEAD -- .github/workflows/ci.yml` **无输出**——ci.yml 在 130 个提交里一字未改，坏配置从 6/5 延续至今。
3. `git show origin/master:tests/test_fix_data_quality.py` → 6/5 版本 L20-49 **已经是**裸 `cur.execute` 查 `portfolio_snapshots` / `portfolio_summary` / `execution_logs`。
4. **决定性的一条**：`git ls-tree -r origin/master --long | grep '\.db$'` 只有一条——根目录 `portfolio.db`，**blob e69de29，0 字节**。`data/database/portfolio.db` **在 origin/master 里根本不存在**。

第 4 条比 `:memory:` 更硬：该测试文件 L8 自算 `DB_PATH = PROJECT_ROOT/data/database/portfolio.db`，**从不读 `DATABASE_PATH` 环境变量**，所以与 env 无关。6/5 的 runner 上这个路径不存在，`sqlite3.connect()` 静默造一个 0 字节文件（即静默降级第 5 例，此处直接发生在 CI 内），四个用例全部 `no such table`。**最后一次 CI 运行就是红的**，且此后两个月无人查看。

顺带一个反常识推论，必须写明以防误操作：**把 84MB 库 push 上去，反而会让这四个用例在 CI 里转绿**——因为 HEAD 里 `data/database/portfolio.db` 是真库。绿灯将由"泄露真实持仓"换来。正解是 fixture 建表，不是留库在 git。

QA 用 CI 的 env 在本地复现，隔离出单一根因：

| 配置 | 结果 |
|---|---|
| 无 env（本地基线） | 7 failed / 1465 passed |
| 带 CI 三个 env | **67 failed** / 1405 passed |
| A/B/C 对照：仅设 `DATABASE_PATH=":memory:"` | 23 failed / 2 passed |
| 仅设 EMAIL/WECHAT=false｜什么都不设 | 均 25 passed |

根因是 `ci.yml:28` 的 `DATABASE_PATH: ":memory:"` 单一变量——空内存库无任何表，所有读真实数据的用例 `no such table`。

**对 P0-1 的关键推论：移库出 git 不会让 CI 变红，因为 CI 从来没绿过，且 `:memory:` 会覆盖 `DATABASE_PATH`——即便把 84MB 推上去也救不了 CI。** 架构师提出的"先解依赖再移库"排序，其前提（移库导致变红）不成立；但结论方向仍对，只是真正的前置项从"给测试加守卫"换成"修 `ci.yml:28`"。

## 三、环境变量与密钥

正面：`.env`、`secrets.toml` 已忽略，**无硬编码密钥**。

缺口三处：

1. **DEPLOYMENT.md 变量名全错**——文档写 `SMTP_HOST/SMTP_USER/SMTP_PASSWORD/WECOM_WEBHOOK`，代码实读 `EMAIL_SMTP_SERVER/EMAIL_USERNAME/EMAIL_PASSWORD/WECHAT_WEBHOOK_URL`。照文档配置将**静默失效**。
2. `.env.example` 漏 `BACKUP_DIR`（settings.py:50）、`STALE_THRESHOLD_DAYS`（settings.py:271）。
3. **第三套配置未文档化**：`run_analysis.py:523` 依赖 `config/notification.json`，缺失即跳过全部通知（日志已实证），该文件不存在。

## 四、接手后前 3 动作

| 优先级 | 动作 | 判定标准 |
|---|---|---|
| P0-1（push 之前） | 撤销 24574df，`git rm --cached` 三个 db；手写 hook 换回 pre-commit 框架并启用大文件门禁。**不再以修测试为前置**（见 (f)：CI 本就是红的，移库不改变这一点） | `git ls-files｜grep .db` 为空；故意 add 大文件被拦 |
| P0-1.5（与 push 同批） | 修 `ci.yml:28`：删掉 `DATABASE_PATH: ":memory:"`，改为在 `conftest.py` 用现成的 `db_schema.init_all_tables(conn)`（`src/utils/db_schema.py:478`，22 张 CREATE TABLE）建 fixture 库。**注意：那 4 个真红用例不读该环境变量**（自算 `PROJECT_ROOT/data/database/portfolio.db`），只删 env 治不了它们——fixture 必须落到它们硬编码的两个路径（`data/database/portfolio.db` 与根 `portfolio.db`），或改测试改走 fixture | CI 首跑红数从 ~67 收敛到个位数，且剩余红项每条能对上一个具体缺陷 |
| P0-2（24h 内） | 备份收敛为 `backup_db.py` 一套，重建计划任务并确认写日志，加异地副本 | 连续 3 天自动生成；`--list` ≥3 份 |
| P1（本周，约 1 天） | 恢复演练（还原到临时库核对 30 表行数）；`pip freeze` 出 lock + 补 pillow/snownlp/jieba；加 `.dockerignore`；CI 补 lint job；版本统一到 **3.12**（非 3.10，见下）；订正文档变量名 | 演练归档；CI 用 lock 连绿两次 |

**版本统一目标选 3.12 而非 3.10**：3.10 距 EOL 仅 2 个月，且 3.10.11 已是最后一个提供 Windows 二进制安装包的版本，锁定它等于给新机器部署埋坑。改动面上，CI 与 Dockerfile 本就是 3.12，只需改 `scheduled_run.bat` 与 `pyproject.toml` 两处；反向统一到 3.10 要改 CI+Docker，且换来一个两个月后失去安全支持的基线。

兼容性依据（经架构师复核后修正）：**不能引用"CI 已在 3.12 跑绿"**——CI 最后一次运行的是 2026-06-05 的 origin/master，彼时 requirements.txt 尚含 selenium、不含 playwright，该绿灯验证的是一个已不存在的依赖集。真实依据是对**今天的** 186 个项目自有 .py 扫描 3.12 已移除的 stdlib（distutils/imp/asyncore/asynchat/smtpd/telnetlib/cgi/pipes/crypt/nntplib）**命中 0 处**。

**同时不要跟进 3.13**：本机系统 Python 为 3.13，而 `cgi` 在 3.13 被移除，依赖链中 `nltk/tree/prettyprinter.py:25` 仍是 `from cgi import escape`。锁 3.12。

迁移不因改两行而完成——**首次 push 触发的 CI 才是 3.12 在当前依赖集上的第一次真实验证**。若不先做 P0-1.5，这次 CI 预期红约 67（QA 用 CI env 的本地复现值），其中绝大多数是 `:memory:` 造成的 `no such table`，会把真正的 3.12/playwright 兼容性问题淹掉。**修 ci.yml 与 push 必须同批**，否则第一份 CI 报告没有诊断价值。

注意 67 是**下界不是终值**：该值在 QA 本机测得，而本机磁盘上 `data/database/portfolio.db` 是那个 84MB 真库，所有走硬编码路径的用例在她那儿仍然通过；CI runner 上没有这个文件，会在 67 之外额外再红一批。同理，**本地 1465 passed 这个数字本身也依赖一个即将被移出 git 的文件**——新机器 clone 后首跑不会是 1465，接手方不要误判为自己环境有问题。

## 五、生产就绪记分卡（总档取最低）

| 维度 | 档位 | 依据 |
|---|---|---|
| 测试+回归 | **Bronze 以下**（自 Silver 连降两档） | 三重证据：(1) CI 最后一次运行（6/5）**本身就是红的**，`ci.yml:28` 的 `:memory:` 与裸查真实表在当时已并存，即从无绿色基线；(2) 此后 130 commits／181 文件变更／31 个新测试文件从未进过 CI；(3) 集成测试结构性假绿，本地 1465 passed 含未知比例空转用例 |
| 契约 | Bronze | 单体 Streamlit，无对外接口 |
| 安全 | **Bronze 以下** | 真实持仓入 git，大文件防线空转 |
| 无障碍 | Bronze | 未评估 |
| 性能 | Silver | 已有索引与缓存优化 |
| 可观测 | Bronze | 仅文件日志；定时任务停摆 65 天无人察觉 |
| 发布安全 | **Bronze 以下** | 无部署即无回滚；备份不可靠 |

**总档：Bronze 以下**，短板为安全与发布安全，补齐路径即 P0-1/P0-2。

## RoleVerdict

**verdict**：`REJECT` — 运维就绪度不达标，不可按"成熟项目"直接接手上线；但缺陷均可 1 周内修复，最危险项仍在可逆窗口。

**blocking**

1. 84MB 真实持仓库已提交本地 24574df，**push 即不可撤回泄露**，且数周内触顶 100MB 硬限。
2. 大文件防线构造性失效（手写 hook 不阻断 + 框架门禁从未生效）。
3. 自动备份停摆 65 天，唯一备份为人工拷贝且同盘、无异地、无演练。
4. **不存在可用的回归基线**：CI 最后一次运行（6/5）本身即红，配置两个月未改；接手方若按"CI 有绿灯"理解风险，会把所有改动建立在不存在的安全网上。
4. **CI 从无绿色基线**：`ci.yml:28` `DATABASE_PATH=":memory:"` 与"测试裸查真实表"结构性冲突，6/5 最后一次运行即为红，此后两月无人查看。不修此行，CI 永无转绿可能，"回归有兜底"无从谈起。

**advisory**

1. 依赖全 `>=`，CI 与镜像不可复现；pillow/snownlp/jieba 三个实用依赖未声明。
2. 缺 `.dockerignore`，Dockerfile 未验证，无 HEALTHCHECK，无 `playwright install chromium`（截图/PDF 导出在镜像内必失败）。
3. DEPLOYMENT.md 的 SMTP/Webhook 变量名与代码不符，告警会静默失效。
4. `config/notification.json` 未文档化且缺失。
5. Python 版本五处不一致（3.9/3.10/3.10.11/3.12/3.13），建议统一 3.12。
6. 两套备份实现命名与清理 glob 冲突，需合并。
7. `news_fetcher.py:22-25` 硬编码 Windows venv 路径注入 sys.path，容器内失效。

**evidence**

- `git cat-file -s 9d713df`=87,949,312；`merge-base --is-ancestor 24574df origin/master`→NO；`rev-list --count origin/master..HEAD`=130。
- `git ls-files` 命中 `data/database/portfolio.db`、`portfolio.db`、`portfolio_data.db`。
- `.gitignore` L23 `data/database/`、L85 `*.db`（证明 `add -f` 绕过）。
- `.git/hooks/pre-commit` L32-39：`LARGE_FILES` 赋值后无引用。
- `logs/scheduled_run.log` mtime 2026-06-01 vs `logs/portfolio_20260805.log` mtime 2026-08-05。
- `data/backups/` 仅 1 文件，命名不匹配任一自动化产物。
- `config/settings.py` L50、L271 两变量未见于 `.env.example`。
- `run_analysis.py` L523 依赖 `config/notification.json`；`config/` 下仅 `settings.py`、`__init__.py`。
- 无 `.dockerignore`、无 `HEALTHCHECK`、无 `docker-compose.yml`、Dockerfile 内无 playwright 安装步骤。
- `dashboard.py:26` 顶层 `from PIL import Image`；`news_fetcher.py:26-27` 函数内 import snownlp/jieba，L45 静默 `return 0.5`；三者均不在 `requirements.txt`。
- `pyproject.toml` L3 `target-version=['py39']`、L27 `python_version="3.9"`（3.9 已于 2025-10-31 EOL）；Python 3.10 EOL 2026-10、3.10.11 为末个二进制安装包版本（PEP 619 / devguide.python.org 核实）。
- `git ls-tree -r origin/master --long | grep '\.db$'` → 仅 `portfolio.db` blob `e69de29` 0 字节；`data/database/portfolio.db` 不在 origin/master。HEAD 侧同命令 → 三条，含 `9d713df` 87,949,312。
- `git diff --stat origin/master HEAD -- .github/workflows/ci.yml` → 空（ci.yml 130 提交未改）；同命令对 `requirements.txt` → 1+/2-，即 `-selenium>=4.0.0 -webdriver-manager>=4.0.0 +playwright>=1.40.0`。
- `git show origin/master:.github/workflows/ci.yml` L23 `pip install -r requirements.txt`、L28 `DATABASE_PATH: ":memory:"`，全文无 `playwright install`。
- `git show origin/master:tests/test_fix_data_quality.py` L8 `DB_PATH = PROJECT_ROOT/"data"/"database"/"portfolio.db"`（不读环境变量）、L15 `sqlite3.connect(str(DB_PATH))`、L22/35/42/49 裸 `cur.execute`。
- 本地实测（QA 卜卦）：无 env 7 failed / 1465 passed / 4 skipped / 268.78s；套用 CI 三个 env → 67 failed / 1405 passed；A/B/C 对照定位单一根因为 `DATABASE_PATH=":memory:"`。架构师报告记 1464/8，与 1465/7 差 1 例，属两次运行间抖动，不改变结论。
- `gh auth status` → 未登录任何 GitHub 主机，**无法调 API 读取真实 workflow run 结论**。上述"6/5 最后一次运行且为红"是由提交时间 + 配置 + 代码三方推定，非 API 实证；接手方拿到仓库权限后应以 `gh run list --limit 5` 复核。
- `git cat-file -e origin/master:tests/test_fix_data_quality.py` 返回 0（文件已在 6/5 版本中）；`git show origin/master:.github/workflows/ci.yml` L28 已为 `DATABASE_PATH: ":memory:"`；`git show origin/master:tests/test_fix_data_quality.py` L23 已为 `sqlite3.connect(str(DB_PATH))` + 裸 `cur.execute` 查真实表 → 三者并存即证 6/5 CI 必红。
- QA 实测 A/B/C 对照：仅设 `DATABASE_PATH=":memory:"` → 23 failed / 2 passed；仅设 EMAIL/WECHAT 或不设 → 均 25 passed。
- `src/utils/db_schema.py:478` `init_all_tables(conn)` 与 22 条 `CREATE TABLE` 已存在，`tests/conftest.py` 已存在 → fixture 库方案有现成抓手，无需新写 DDL。
