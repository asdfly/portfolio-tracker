# 09 · portfolio_summary 缺口修复计划（窗口 2026-09-03 ~ 2026-09-16）

| 项 | 值 |
|---|---|
| 文档编号 | 09 |
| 对应任务 | #65（读证）+ #59 第 5 项（落地） |
| 编写时间 | 2026-09-16 11:20（生产冻结窗口 15:00–15:30 之前） |
| 本轮范围 | **只写计划 + 只读取证 + 副本实测**；不写生产库、不改 `is_suspect` 逻辑、不动 `docs/handover/07_known_data_issues.md` |
| 生产库基线 | `data/database/portfolio.db`，141,017,088 B，mtime `2026-09-16 09:09:04`，**sha256[:16] = `261a4a6ed03396cd`** |
| 副本 | 全部在 `%TEMP%`（`p9_exp2A.db` / `p9_exp2B.db`），跑完即删；取证脚本也不在仓库内 |

> 基线可信度：当前生产库与该 sha256 逐字节相同的副本存在于
> `data/backups/portfolio.db.bak_verify_corr_20260916_095652.db`，
> 说明 **09:09:04 之后没有任何进程写过生产库**。本计划的 before 数值全部以此为准。

---

## 一、结论摘要

1. **缺口不是 1 个日期，是 2 个**：`portfolio_snapshots` 有、`portfolio_summary` 无的日期，
   全历史（2012-05-28 ~ 2026-09-15）**只有 2 个** —— `2026-09-03` 与 `2026-09-15`。
   反方向（summary 有快照无）为 0；`portfolio_nav` 的日期集合与 summary **完全一致**（无孤儿行）。
2. **`2026-09-03` 是同一个缺陷在 13 天前的一次完整复发**：它已经污染了 `2026-09-04` 的
   `daily_return` / `daily_pnl` / `vs_hs300`，且**全程没有任何通道报出来**（`is_suspect` 全窗口为 0）。
3. **把 `2026-09-04` 放进窗口重算，用现有脚本也修不好** —— 实测重算后 `daily_return`
   与现库**逐位相同**（`-0.4170264005939382`）。真正的因是**日期源**，不是公式。
4. **现有脚本结构上补不了缺口**：`scripts/recompute_summary_window.py:70-72` 的日期清单
   取自 `portfolio_summary` 自己，缺口日不在清单里 → 实测 dry-run 只覆盖 7 天，09-03/09-15 不在其中。
5. **第二个缺陷（本轮新发现）**：一旦窗口里出现"新增日期"，`scripts/recompute_summary_window.py:197-203`
   的打印块会崩（`o = old[dt]` 直接格式化旧行），**在写库之前**以 `rc=1` 退出。
   第 5 项必须一并修，否则修好日期源之后脚本反而跑不动。
6. **写入面（副本实测，以生产库为基线）**：
   - `portfolio_summary`：**新增 2 行**、**修改 7 个既有行**、删除 0；
   - `portfolio_nav`：**新增 2 行**、**修改 7 个既有行**（仅 `mwr_return`，09-04 另含 `total_units`）、删除 0；
   - **`twr_cumulative` / `unit_nav` / `net_flow` / `is_suspect` 的既有行 0 变动 ⇒ 已发布的 TWR 历史不被改写。**
7. **`mwr_return` 会有 7 行小幅重算**（幅度 ≤ 0.018）。这是 `_trailing_mwr_annualized` 用
   **行号窗口**（`nav_engine.py:255` `start = max(0, idx - 365)`）的结构性后果，需要 lead 明确"接受"。
8. **`is_suspect` 的 md-vs-r 通道无法用作本缺陷的判据**：实测 09-04 的 `|md−r|` 在修复前后
   几乎不动（2.734 pp → 2.729 pp），因为 09-04 有一笔约 **+50,000** 的入金**不在 `trade_records` 里**
   （`nav.net_flow = 0.0`）。硬化的主判据必须换成**交易日跨度**，见 §七。

---

## 二、缺陷清单（日期 / 标的 / 影响行）

### 2.0 基线行数与日期跨度

| 表 | 行数 | min | max |
|---|---|---|---|
| `portfolio_summary` | 3,477 | 2012-05-28 | **2026-09-14** |
| `portfolio_snapshots` | 36,173 | 2012-05-28 | **2026-09-15** |
| `portfolio_nav` | 3,477 | 2012-05-28 | **2026-09-14** |
| `index_quotes` | 50,850 | 1990-12-19 | 2026-09-15 |

### 2.1 差值集（快照有、汇总无）

```
全历史 snapshots-only  n=2   ['2026-09-03', '2026-09-15']
反方向 summary-only(无快照) n=0
nav 中存在但 summary 无的行 n=0
summary 中存在但 nav 无的行 n=0
```

> 本轮交集步骤排除 **0** 个（此前记录的 4 个月末非交易日快照
> `2026-01-31 / 2026-02-28 / 2026-05-31 / 2026-06-19` 在两张表里**都有**，不落在差值集里）。

### 2.2 标的

09-03 与 09-15 的快照标的集合**完全相同，34 个**（写库时快照行数也是 34）：

```
001194,001323,001407,001437,001765,002152,007994,008269,100032,159220,159267,159300,
159650,159770,159796,159819,159949,159992,166301,510300,510500,511380,511520,512010,
512100,512810,515010,515120,516160,519770,561910,563020,588000,880013
```

09-04 是 35 个（比 09-03 多 1 只，见 §七 的入金线索）。

### 2.3 逐日明细（缺陷行）

| 日期 | 星期 | 快照行数 | `summary` 现状 | `nav` 现状 | 判定 |
|---|---|---|---|---|---|
| 2026-09-03 | 四 | 34 | **无行** | **无行** | 【缺口】应新增 1 行 |
| 2026-09-04 | 五 | 35 | 有行，但 3 列错 | 有行 | 【错误行】应重算 |
| 2026-09-15 | 二 | 34 | **无行** | **无行** | 【缺口】应新增 1 行 |
| 2026-09-16 | 三 | 0 | 无行 | 无行 | 今日 15:30 未跑，暂无数据 |

**2026-09-03**（快照侧，供补行用）

| 字段 | 值 |
|---|---|
| `SUM(market_value)` | `1,528,186.8599999999` |
| `SUM(cost_price*quantity)` | `1,475,165.58708092` |
| `SUM(pnl)` | `53,027.22` |
| `profit/loss` | `19 / 15` |
| `sh000300.change_pct` | `+0.10163906822404326` |

**2026-09-15**（快照侧）

| 字段 | 值 |
|---|---|
| `SUM(market_value)` | `1,507,806.178325` |
| `SUM(cost_price*quantity)` | `1,475,167.33960889` |
| `SUM(pnl)` | `32,638.84255811` |
| `profit/loss` | `19 / 15` |
| `sh000300.change_pct` | `-0.6705658362823141` |

**2026-09-04**（现库错值 vs 正确值）

| 列 | 现库（错） | 应为 | 说明 |
|---|---|---|---|
| `daily_pnl` | `-6362.3717879999895` | `-8896.899887999753` | 多日口径 |
| `daily_return` | `-0.4170264005939382` | `-0.5821866501325468` | **这是 09-02→09-04 两日链** |
| `vs_hs300` | `-0.31755529648290637` | `-0.4827155460215149` | 随 `daily_return` 连带错 |
| `total_value` / `total_cost` / `total_pnl` / `profit_count` / `loss_count` | — | 不变 | 与"前一行是谁"无关 |

### 2.4 口径复刻与链式自证

复刻口径与 `scripts/recompute_summary_window.py:94-105`、`src/analysis/portfolio.py:496-541` 一致
（前后两日共同持仓、`quantity` 取前一日、`prev_price = prev_mv/prev_qty`）：

| 区间 | 共同持仓 | `daily_pnl` | `daily_return` |
|---|---|---|---|
| 09-02 → 09-03 | 34 | `+2,534.53` | **`+0.166128%`**（09-03 的应有值）|
| 09-02 → 09-04 | 34 | `-6,362.37` | `-0.417026%`（**现库 09-04 记的就是它**）|
| 09-03 → 09-04 | 34 | `-8,896.90` | **`-0.582187%`**（09-04 的真单日值）|
| 09-14 → 09-15 | 34 | `-6,735.11` | **`-0.444697%`**（09-15 的应有值）|

```
链式校验：(1 + 0.166128%)(1 + (-0.582187%)) - 1 = -0.417026284%
现库 09-04 daily_return                        = -0.417026401%
差                                             = 1.16e-07 pp
```

⇒ 09-04 存的就是"09-03 与 09-04 两天链起来的收益"，误差仅来自浮点末位。
**这是"缺行 ⇒ 下一日变多日"的直接证据，不是推断。**

### 2.5 `prev_dt` 实际取到什么（`src/analysis/portfolio.py:482-488`）

```
SELECT date FROM portfolio_summary WHERE date < ? ORDER BY date DESC LIMIT 1
```

| 当天 | summary 取到的 prev_dt | 真实上一快照日 | 判定 |
|---|---|---|---|
| 2026-09-03 | 2026-09-02 | 2026-09-02 | 一致 |
| 2026-09-04 | **2026-09-02** | 2026-09-03 | **跨过缺口** |
| 2026-09-15 | 2026-09-14 | 2026-09-14 | 一致 |
| 2026-09-16 | **2026-09-14** | 2026-09-15 | **跨过缺口（今日跑就是 2 日值）** |

---

## 三、为什么窗口必须按 `2026-09-03 : 2026-09-16` 规划

`2026-09-03` 与 `2026-09-15` 是**必须补**的缺口日，这没有争议。争议点是 **09-04 为什么必须在窗口内**。
论证如下，三步都是实测值而非推理：

1. **09-04 当前的值就是 09-03 缺口造成的。** §2.4 的链式恒等式把现库值复现到 `1.16e-07 pp`。
   不补 09-03，09-04 就永远停在多日值上；补了 09-03 而**不**重算 09-04，
   则 09-04 会与它自己的前一行（09-03）**口径不一致** —— 09-03 记单日、09-04 记两日，
   相邻两行的 `daily_return` 不是同一段区间，任何按日序列做的统计（波动率、胜率、连续涨跌）都会错。
2. **重算 09-04 会真实改变数字**（不是幂等空转）：`daily_return` 由 `-0.417026%` 变 `-0.582187%`，
   绝对差 **0.165161 pp**；`daily_pnl` 差 **-2,534.53**；`vs_hs300` 差 **-0.165160 pp**。
   幅度不小，属于必须回填而非"可有可无"。
3. **不重算 09-04 就修不好"今天"**：09-16 的 `prev_dt` 依赖 summary 的最大日期。
   如果只补 09-15 而漏掉 09-04，09-04 的多日值仍会进入 09-07 起的 `hist` 窗口（`sharpe`/`volatility`
   的 60 日样本），污染面一路传到今天。

**窗口建议：`--start-date 2026-09-03 --end-date 2026-09-16`。**

- 起点 09-03：缺口日，必须包含。
- 终点 09-16：今天尚未跑，`compute()` 在 `:87-88` 会因 `SUM(market_value)` 为空而跳过，
  所以现在放进来**无害**；一旦 15:30 跑完写出 09-16，同一命令自然把 09-16 一起纳入，
  无需改参数（是否要在 09-16 已落库后重算它，见 §八 第 3 条）。
- 窗口内其它日期（09-07 ~ 09-14）会连带重算，但已实测为"只动风险两列"，见 §五。

---

## 四、dry-run 命令与副本行级 before/after

### 4.1 实验设计（可复现）

- 生产库**只以 `mode=ro`（`file:...?mode=uri`）打开**，全程无写。
- 写操作全部发生在 `%TEMP%` 下的副本：`copyfile(生产库 → %TEMP%\p9_exp2X.db)`。
- 子进程通过环境变量指向副本（`config/settings.py:57` `DATABASE_PATH = Path(env('DATABASE_PATH', ...))`，
  且 `_load_env_file` 明确"不覆盖已有环境变量"，故 env 生效）：

  ```
  DATABASE_PATH=%TEMP%\p9_exp2A.db  venv313\Scripts\python.exe scripts\recompute_summary_window.py ...
  ```

- 对照组设计：
  - **实验 A** = 副本（原样）→ 现有脚本原样跑，看它能不能补缺口；
  - **实验 B** = 副本 + 在 `portfolio_summary` 植入 2 行**占位行**（`INSERT INTO portfolio_summary(date) VALUES('2026-09-03')` 等），
    其余代码一字不改 → 等价于"只把日期源补上"这一处修复。
    占位行只是让 `:70-72` 查得到这两个日期，`compute()` 的数值仍全部来自 `portfolio_snapshots`，
    `apply_summary()` 随后用 12 列 `INSERT OR REPLACE` 覆盖占位行。

### 4.2 dry-run 命令（正式写法，本轮未在生产库执行）

```bat
:: 1) 先备份（脚本自带，见 scripts/recompute_summary_window.py:59-64 / :182-183）
venv313\Scripts\python.exe scripts\recompute_summary_window.py --backup

:: 2) dry-run（默认就是 dry-run，只读打印，不写库）
venv313\Scripts\python.exe scripts\recompute_summary_window.py ^
    --start-date 2026-09-03 --end-date 2026-09-16

:: 3) 落地（第 5 项修复完成、闸门就绪之后再执行；+ 连续重建 nav 避免中间态）
venv313\Scripts\python.exe scripts\recompute_summary_window.py ^
    --start-date 2026-09-03 --end-date 2026-09-16 --apply --rebuild-nav
```

### 4.3 实验 A：现有脚本原样（副本）

命令：`--start-date 2026-09-03 --end-date 2026-09-16`（默认 dry-run），`rc=0`，输出：

```
portfolio_summary 窗口重算  模式=DRY-RUN  区间=2026-09-03 ~ 2026-09-16  rebuild_nav=False
date                  tv旧          tv新     dr旧%     dr新% ...
2026-09-04      1,561,001    1,561,001   -0.417   -0.417 ...
2026-09-07      1,541,964    1,541,964    1.492    1.492 ...
2026-09-08      1,543,547    1,543,547    0.103    0.103 ...
2026-09-09      1,545,312    1,545,312    0.114    0.114 ...
2026-09-10      1,535,484    1,535,484   -0.636   -0.636 ...
2026-09-11      1,564,453    1,564,453   -0.943   -0.943 ...
2026-09-14      1,514,541    1,514,541   -0.425   -0.425 ...

共 7 天
[DRY-RUN] 未写库。
```

再在副本上真正落盘（`compute()` + `apply_summary()` + `rebuild_portfolio_nav()`）：

```
compute() 覆盖 7 天: ['2026-09-04','2026-09-07','2026-09-08','2026-09-09','2026-09-10','2026-09-11','2026-09-14']
apply_summary 写 7 天；rebuild_portfolio_nav 写 3477 行
副本 summary=3477  nav=3477

[A/summary] 新增 0 ；删除 0 ；既有 30 行中变动 0
[A/nav]     新增 0 ；删除 0 ；既有 30 行中变动 0
```

**A 的三条结论**

1. 缺口日 **09-03 / 09-15 根本不在重算清单里**（`共 7 天`）⇒ 现有脚本结构上补不了缺口。
2. 09-04 重算后 `daily_return = -0.4170264005939382`，与现库**逐位相同** ⇒ **把 09-04 塞进窗口也修不好**，
   因在日期源（`prev_dt` 取到 09-02）。
3. 既有行 **0 变动** ⇒ 现有脚本对"已覆盖窗口"是**幂等**的。这条很重要：它使得实验 B 的
   全部差异都可以**干净地归因于"日期源补上 2 个缺口"这一处改动**，而不是脚本本身的口径漂移。

### 4.4 实验 B：日期源修复后（副本 + 2 行占位）

先照原样跑同一条命令（dry-run），观察 `main()` 的打印块：

```
rc=1
...
  File "scripts/recompute_summary_window.py", line 199, in main
    print(f"{dt:<12}{o[1]:>13,.0f}{n['total_value']:>13,.0f}...")
TypeError: unsupported format string passed to NoneType.__format__
```

绕过打印块（直接调用脚本自身的 `compute()` + `apply_summary()`），再连续 `rebuild_portfolio_nav()`：

```
compute() 覆盖 9 天: ['2026-09-03','2026-09-04','2026-09-07','2026-09-08','2026-09-09',
                     '2026-09-10','2026-09-11','2026-09-14','2026-09-15']
apply_summary 写 9 天；rebuild_portfolio_nav 写 3479 行
副本 summary=3479  nav=3479
```

### 4.5 行级 before/after（before = 生产库原值）

**2026-09-03 / `portfolio_summary`**（新增行）

| 列 | before | after |
|---|---|---|
| `total_value` | `<无行>` | `1528186.8599999999` |
| `total_cost` | `<无行>` | `1475165.58708092` |
| `total_pnl` | `<无行>` | `53027.22` |
| `daily_pnl` | `<无行>` | `2534.528175999876` |
| `daily_return` | `<无行>` | `0.16612753822948226` |
| `vs_hs300` | `<无行>` | `0.064488470005439` |
| `profit_count` / `loss_count` | `<无行>` | `19` / `15` |
| `sharpe_ratio` | `<无行>` | `1.0854` |
| `max_drawdown` | `<无行>` | `6.720171323433938` |
| `volatility` | `<无行>` | `22.5584` |
| `snapshot_type` | `<无行>` | `daily` |

**2026-09-04 / `portfolio_summary`**（既有行）

| 列 | before | after |
|---|---|---|
| `daily_pnl` | `-6362.3717879999895` | `-8896.899887999753` |
| `daily_return` | `-0.4170264005939382` | `-0.5821866501325468` |
| `vs_hs300` | `-0.31755529648290637` | `-0.4827155460215149` |
| `sharpe_ratio` | `1.0854` | `1.514` |
| `volatility` | `22.5584` | `22.1339` |
| `total_value` / `total_cost` / `total_pnl` / `profit_count` / `loss_count` | 不变 | 不变 |

**2026-09-15 / `portfolio_summary`**（新增行）

| 列 | before | after |
|---|---|---|
| `total_value` | `<无行>` | `1507806.178325` |
| `total_cost` | `<无行>` | `1475167.33960889` |
| `total_pnl` | `<无行>` | `32638.84255811` |
| `daily_pnl` | `<无行>` | `-6735.112114999909` |
| `daily_return` | `<无行>` | `-0.44469650048298837` |
| `vs_hs300` | `<无行>` | `0.22586933579932578` |
| `profit_count` / `loss_count` | `<无行>` | `19` / `15` |
| `sharpe_ratio` | `<无行>` | `0.6839` |
| `max_drawdown` | `<无行>` | `6.720171323433923` |
| `volatility` | `<无行>` | `22.1005` |
| `snapshot_type` | `<无行>` | `daily` |

**2026-09-03 / `portfolio_nav`**（新增行）

| 列 | after |
|---|---|
| `unit_nav` | `2.649294` |
| `total_units` | `1525652.04`（= 上一行 `total_value`，见下注）|
| `total_value` | `1528186.86` |
| `net_flow` | `0.0` |
| `twr_cumulative` | `1.649294` |
| `mwr_return` | `0.414468` |
| `is_suspect` | `0` |

**2026-09-15 / `portfolio_nav`**（新增行）

| 列 | after |
|---|---|
| `unit_nav` | `2.613959` |
| `total_units` | `1514541.29`（= 09-14 的 `total_value`）|
| `total_value` | `1507806.18` |
| `net_flow` | `0.0` |
| `twr_cumulative` | `1.613959` |
| `mwr_return` | `0.434956` |
| `is_suspect` | `0` |

**2026-09-04 / `portfolio_nav`**（既有行）

| 列 | before | after |
|---|---|---|
| `total_units` | `1525652.04` | `1528186.86` |
| `mwr_return` | `0.452103` | `0.44966` |
| `twr_cumulative` | `1.63387` | **`1.63387`（不变）** |
| `unit_nav` | `2.63387` | **`2.63387`（不变）** |
| `is_suspect` | `0` | `0` |
| `net_flow` | `0.0` | `0.0` |

> 注：`portfolio_nav.total_units` 的定义就是 `round(prev_v, 2)`（`nav_engine.py:343`），
> 即"上一行的 `total_value`"，不是份额数。缺口补上后 09-04 的"上一行"由 09-02 变成 09-03，
> 于是这一列跟着变 —— 属于定义行为，不是误差。

---

## 五、写入面清单（副本实测，以生产库为基线）

### 5.1 `portfolio_summary`

| 项 | 数量 | 日期 |
|---|---|---|
| 新增 | **2** | `2026-09-03`、`2026-09-15` |
| 删除 | 0 | — |
| 既有行修改 | **7** | `2026-09-04`、`09-07`、`09-08`、`09-09`、`09-10`、`09-11`、`09-14` |

按列统计（变动行数）：`sharpe_ratio` 7、`volatility` 7、`daily_pnl` 1、`daily_return` 1、`vs_hs300` 1。
**`total_value` / `total_cost` / `total_pnl` / `profit_count` / `loss_count` / `snapshot_type` 0 变动。**
`max_drawdown` 仅出现 `7e-14` 量级的浮点末位噪声（`6.720171323433938 → 6.720171323433867`），等价于不变。

连带重算行（09-07 ~ 09-14）只动风险两列：

| 日期 | `sharpe_ratio` | `volatility` |
|---|---|---|
| 09-07 | `1.4014 → 1.2455` | `22.1635 → 22.1356` |
| 09-08 | `1.5177 → 1.5283` | `22.2934 → 22.3098` |
| 09-09 | `1.5489 → 1.6221` | `22.2895 → 22.2827` |
| 09-10 | `1.645 → 1.3209` | `22.2621 → 22.0183` |
| 09-11 | `1.1974 → 0.9909` | `22.0536 → 21.9767` |
| 09-14 | `0.8074 → 0.8103` | `22.0605 → 22.0788` |

> 机制：`sharpe_ratio` / `volatility` 由"前 60 个交易日 `daily_return`"（`:112-125`）算出。
> 09-04 的 `daily_return` 一变，09-04 之后所有日期的 60 日窗口都含变值。`max_drawdown`
> 用同一序列的累乘峰值，量级差异被淹没在末位上，故实质不变。

### 5.2 `portfolio_nav`

| 项 | 数量 | 说明 |
|---|---|---|
| 新增 | **2** | `2026-09-03`、`2026-09-15` |
| 删除 | 0 | — |
| 既有行修改 | **7** | 仅 `mwr_return` 一列；09-04 另含 `total_units` |

按列统计既有行变动数：

```
twr_cumulative   0     ← 已发布的历史 TWR 不被改写
unit_nav         0
total_value      0
net_flow         0
is_suspect       0
total_units      1     （09-04，定义行为）
mwr_return       7
```

`mwr_return` 变动明细：`09-04 0.452103→0.449660`、`09-07 0.427862→0.432034`、
`09-08 0.433851→0.438679`、`09-09 0.440713→0.424909`、`09-10 0.413714→0.431196`、
`09-11 0.464453→0.494973`、`09-14 0.437061→0.454804`。**最大幅度 0.0175。**

成因（`nav_engine.py:214-259` `_trailing_mwr_annualized`）：窗口是按**行号**取的
`start = max(0, idx - 365)`，不是自然日。窗口中间插入 2 行 ⇒ 每行的 `v_begin` / `net_in`
端点位移 ⇒ 既有行 `mwr_return` 全体小幅重算。这是结构性副作用，
**是 nav 唯一被改写的既有列，需要 lead 明确接受**。

### 5.3 TWR 链不变性（乘式恒等，实测）

```
twr(09-02)=1.6449   twr(09-03)=1.649294   twr(09-04)=1.63387

(1+twr(09-02))(1+r0903) - 1          = 1.649293907259   vs twr(09-03)=1.649294   差 -9.27e-08
(1+twr(09-02))(1+r0903)(1+r0904) - 1 = 1.633870071808   vs twr(09-04)=1.633870   差  7.18e-08

修复前 twr(09-04) = 1.63387
修复后 twr(09-04) = 1.63387
```

数量在分子分母约掉 ⇒ `1 + r_t = V_t / V_{t-1}` 精确成立 ⇒ 链式乘积与多日收益**精确相等**，
所以补上 09-03（并把 09-04 从两日改成单日）**不会改变任何已发布的 TWR 值**。
`twr_cumulative` 既有行 0 变动即此项的实测确认。

---

## 六、回滚方式

### 6.1 本轮前置防线：脚本自带备份（**推荐，唯一正确目标**）

`scripts/recompute_summary_window.py:59-64`

```python
def backup(db_path: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = f"{db_path}.bak_recompute_{stamp}"
    shutil.copyfile(db_path, dst)
```

- 加 `--backup` 后产出：`data/database/portfolio.db.bak_recompute_<YYYYmmdd_HHMMSS>`（约 141 MB）。
- **当前 `data/database/` 下只有 `portfolio.db` 一个文件，不存在任何 `.bak_recompute_*`**
  （已列举确认）。所以真重算必须**先**跑 `--backup`。
- 优点：它是在 `--apply` **紧前**做的整库拷贝，因此**天然包含执行时刻的全部最新状态**
  （如果 15:30 已跑完，备份里就有 09-16），回滚语义最干净。

### 6.2 独立回滚点（`data/backups/`，已在库，可直接用）

`data/backups/` 现有 **23 个**备份、合计 **2,801.4 MB**。与本窗口最接近的两个：

| 文件 | 大小 | 内容（只读校验）| sha256[:16] |
|---|---|---|---|
| `portfolio_20260915_153004.db` | 140,845,056 B | summary 3477 / max 09-14；nav 3477 / max 09-14；**snapshots 36,139 / max 09-14** | `5621e2bf5123d1e6` |
| `portfolio.db.bak_verify_corr_20260916_095652.db` | 141,017,088 B | summary 3477 / max 09-14；nav 3477 / max 09-14；snapshots 36,173 / max 09-15 | `261a4a6ed03396cd` |

⚠️ **`portfolio_20260915_153004.db` 不能作为本轮回滚目标**：它缺 09-15 的 34 行快照
（36,139 行 vs 现在 36,173 行），回滚它会**连带丢掉 09-15 的快照**，缺口反而变大。
它的价值在于"daily pipeline 自己也会备份"这一事实（`run_analysis` 每跑一次都先备份，
09-03 的日志里有 `数据库备份完成: ...\data\backups\portfolio_20260903_153016.db`，
但该文件已被轮转删除，`含 20260903 的备份 = []`）。

第二个（`bak_verify_corr_20260916_095652.db`）与**当前生产库 sha256 完全相同**，
可作为"确认基线未被改动"的比对基准。

### 6.3 恢复命令

```bat
:: 0) 先确认没有正在跑的写入进程（15:30 批次 / refresh 任务）
::    回滚必须在没有任何进程写库时进行，否则会被下一次自动备份再次覆盖

:: 1) 保留现场（不要直接覆盖，先改名留证）
move data\database\portfolio.db data\database\portfolio.db.bad_<时间戳>

:: 2) 恢复
copy /Y "data\database\portfolio.db.bak_recompute_<时间戳>" data\database\portfolio.db

:: 3) 校验（期望值见 6.4）
```

Git Bash / bash 等价写法：

```bash
mv data/database/portfolio.db "data/database/portfolio.db.bad_$(date +%Y%m%d_%H%M%S)"
cp -v "data/database/portfolio.db.bak_recompute_<时间戳>" data/database/portfolio.db
```

### 6.4 恢复后校验 SQL（期望值）

```sql
SELECT COUNT(*), MAX(date) FROM portfolio_summary;   -- 回滚到执行前：3477 / 2026-09-14
SELECT COUNT(*), MAX(date) FROM portfolio_nav;       -- 回滚到执行前：3477 / 2026-09-14
SELECT COUNT(*) FROM portfolio_summary
  WHERE date IN ('2026-09-03','2026-09-15');         -- 回滚到执行前：0
SELECT daily_return FROM portfolio_summary
  WHERE date='2026-09-04';                           -- 回滚到执行前：-0.4170264005939382
```

若回滚到"修复后"状态，对应期望为 `3479 / 2026-09-15`、`3479 / 2026-09-15`、`2`、`-0.5821866501325468`。

**本轮基线锚点**：`sha256[:16] = 261a4a6ed03396cd`（= 执行前生产库的整库哈希）。

---

## 七、09-04 回溯案例：同类缺陷的判别特征

> 本节**只写依据**，为后续 `is_suspect` 硬化留材料。**本轮不改 `nav_engine.py` 任何逻辑。**

### 7.1 现场重建（09-03 当天发生了什么）

来自 `logs/portfolio_20260903.log`（UTF-8，1,451 行）：

```
[103] 2026-09-03 15:30:29,292 - __main__ - INFO - 数据库备份完成: ...\data\backups\portfolio_20260903_153016.db
[110] 2026-09-03 15:30:29,320 - src.utils.monitor - INFO - 任务执行记录: portfolio_daily_analysis - running
[122] 2026-09-03 15:30:44,856 - src.analysis.portfolio - INFO - 持仓文件日期 2026-08-31 < 数据库最新日期 2026-09-02，持仓无更新，保持不变
[124] 2026-09-03 15:30:44,858 - src.analysis.portfolio - INFO - 从数据库加载 22 条持仓记录
[127] 2026-09-03 15:30:45,232 - src.utils.database - INFO - 保存持仓快照: 2026-09-03, 22条记录
[129] 2026-09-03 15:30:48,648 - src.analysis.portfolio - INFO - 步骤4: 计算技术指标...
        ← 此后该批次没有任何后续日志行
```

对照 `execution_logs`：

```
2026-09-03 : 1 条
    {'id': 501, 'task_name': 'portfolio_daily_analysis', 'status': 'running',
     'message': '开始每日完整分析', 'created_at': '2026-09-03T15:30:29.305132'}
```

`data/reports/run_report_*.json` 共 27 个，**不含 09-03**。

⇒ 09-03 的 15:30 批次：**备份→起跑→写入快照→进入"步骤4 计算技术指标"→进程硬死**。
快照落了（22 条，后被 08-03 那轮场外回填补齐到 34 条），`portfolio_summary` 没落，
`execution_logs` 只有 `running` 没有终态，`run_report` 没生成。
**没有超时、没有 failed 记录、没有告警 —— 这就是它藏了 13 天的原因。**

对照 09-15（今天的那个 crash）是**同一形状**：

```
2026-09-15 : 2 条
    {'id': 523, 'status': 'running', 'created_at': '2026-09-15T15:30:06.249449'}
    {'id': 524, 'status': 'failed', 'message': 'All arrays must be of the same length',
     'duration_seconds': 30.145523071289062, 'created_at': '2026-09-15T15:30:36.453933'}
```

区别只在于 09-15 **留下了 failed 终态**（所以被抓到），09-03 连终态都没有（所以没人抓）。

### 7.2 缺陷在无人察觉下存活 13 天的直接证据

`logs/portfolio_20260904.log` 第 1 条：

```
2026-09-04 15:31:30,492 - src.analysis.portfolio - INFO -
    持仓文件日期 2026-08-31 < 数据库最新日期 2026-09-03，持仓无更新，保持不变
```

09-04 那天系统自己认为"数据库最新日期 = 2026-09-03"，
但 `portfolio_summary` 里**根本没有 09-03 这一行** —— 它看的是快照表的最新日期，
而写汇总时用的是汇总表的最新日期。两张表的"最新日期"各自为政，就是这个缺陷能静默传播的技术根因。

### 7.3 为什么全链路没有报警

| 通道 | 09-04 的实际状态 | 为什么没报 |
|---|---|---|
| `RunReporter` / `run_status`（#55 修的那套） | 09-04 的 `run_status` 是 `ok` | 那天的运行**确实成功了**，缺行不是当天产生的 |
| `execution_logs` | `success` | 同上 |
| `DataQualityChecker` | 09-04 `score=96.5, alerts=4`；09-14 `score=90.6, alerts=9` | 没有一条检查项比对"快照日期集合 vs 汇总日期集合" |
| `portfolio_nav.is_suspect` | **09-01 ~ 09-14 全为 0** | 见 7.4 |

### 7.4 `is_suspect` 为什么必然漏掉（实测，非推断）

代码：`src/analysis/nav_engine.py:36` / `:311` / `:316-332` / `:343` / `:348`

```python
_SUSPECT_DIVERGENCE = 0.30                     # :36
prev_v = float(df["total_value"].iloc[i])      # :318  ← df 就是 portfolio_summary，缺行直接跳过
r = float(df["dr"].iloc[i])                    # :327  daily_return/100
denom = prev_v + c * 0.5                       # :329
md = (v - prev_v - c) / denom                  # :330
if abs(md - r) > _SUSPECT_DIVERGENCE:          # :331
    suspects.append(d_str)
```

实测（`c` 取 `_daily_net_cashflow` 的实际值，09-04 取 0.0）：

| 口径 | 日期 | nav 的上一行 | `r` | `md` | `|md−r|` pp |
|---|---|---|---|---|
| 修复前 | 09-04 | `2026-09-02` | `-0.417026%` | `+2.316977%` | **2.734003** |
| 修复后 | 09-03 | `2026-09-02` | `+0.166128%` | `+0.166147%` | `0.000019` |
| 修复后 | 09-04 | `2026-09-03` | `-0.582187%` | `+2.147262%` | **2.729449** |
| 修复后 | 09-15 | `2026-09-14` | `-0.444697%` | `-0.444696%` | `0.000000` |

三条硬结论：

1. **阈值单位问题**：`md` 与 `r` 都是**小数**，所以 `0.30` 实际等于 **30 个百分点**，
   不是 0.3 pp。任何 0.3 pp 量级的错位都过不了这道网。
2. **自校验同向失效**：`prev_v` 取自 summary 的上一行。缺行日不存在 ⇒ `prev_v` 跨过缺口 ⇒
   `r` 与 `md` 被**同一段多日区间**污染 ⇒ 两者同向偏移 ⇒ 差值消失。09-04 修复前
   `|md−r| = 2.734 pp`，**远小于 30 pp**，所以不报。
3. **md-vs-r 通道对本缺陷无判别力**：修复前后 `|md−r|` 几乎不动（`2.734 → 2.729 pp`）。
   原因是 09-04 有一笔约 **+50,000** 的入金（`SUM(cost_price*quantity)` 由
   `1,475,165.58708092` 增到 `1,525,166.24491989`，差 **`+50,000.6578`**；
   快照标的数 34 → 35），而它**不在 `trade_records` 里**：

   ```
   trade_records 列: ['id','date','market','code','name','action','quantity','price',
                      'amount','commission','stamp_tax','change_amount']
   09-03 / 09-04 / 09-15 的流水记录数 = 0
   ```

   ⇒ `_daily_net_cashflow` 取不到 ⇒ `nav.net_flow = 0.0` ⇒ `md` 被未建模的现金流整个抬起来。
   **把阈值收紧到 0.3 pp 也不能判别**：修复前后都会报 09-04，同样不具备判别力。

### 7.5 建议的判据（供 `is_suspect` 硬化参考，本轮不实现）

**主判据：交易日跨度 `span_days`。** 对每一行，取"summary 自己的上一行日期"与"快照表的上一日"，
若二者不同，或二者之间的**交易日距离** > 1，则该行为可疑。09-16 修复前的跨度应为 **2**（09-14 → 09-16）。

**辅判据 1：日期集合差集。** 快照日期集合 − 汇总日期集合 ≠ ∅ ⇒ 立即报缺口（09-03、09-15 会被直接抓到）。

**辅判据 2：链式自证。** 若 `(1+r_{prev})(1+r_today) − 1` 与 `r_today` 的偏离远小于
`(1+r_{prev})(1+r_today) − 1` 与"两日合并值"的距离，说明 `r_today` 记的是多日链。
09-04 实测差 `1.16e-07 pp`，判别力极强且不受入金干扰。

**先决条件：** 若还要保留 md-vs-r 这一路，必须**先让 09-04 那笔入金进入 `trade_records`**，
否则 `md` 永远指向错误方向（本节 7.4 第 3 条）。`mwr_return` 用同一套 `cf`，
这也解释了它为什么与直觉不符。

---

## 八、交易日历覆盖退化（仅证据保留，本轮不修）

`src/utils/trading_calendar.py:41` `_HOLIDAY_RANGES` 的键只有 **2024 / 2025 / 2026**：

```
_HOLIDAY_RANGES 覆盖年份 = [2024, 2025, 2026]
has_official_calendar(2026)=True ; (2025)=True ; (2023)=False
```

窗口内逐日判定（实测）：

```
2026-09-03  星期四  is_trading_day=True  日历覆盖=有官方休市表
2026-09-04  星期五  is_trading_day=True  日历覆盖=有官方休市表
2026-09-15  星期二  is_trading_day=True  日历覆盖=有官方休市表
2026-09-16  星期三  is_trading_day=True  日历覆盖=有官方休市表
本次调用触发退化的年份 = 无
```

**结论：本窗口（09-03 ~ 09-16）全部落在 2026，有官方休市表，判定未退化，结论可信。**
差值集的 2 个日期与窗口内所有日期都在 2026，不受 2012–2023 退化影响。

2012–2023 共 12 条 `交易日历无 XXXX 年官方休市表` warning **只作为证据保留在本文件**，
修复归属 **#66**，本轮不动（`trading_calendar.py` 当前已有 `has_official_calendar()` /
`degraded_years()` 作为可编程痕迹，见 `:83-89` / `:79-80`）。

---

## 九、执行前仍缺什么（按顺序）

| # | 缺口 | 说明 | 归属 |
|---|---|---|---|
| 1 | **日期源修复** | `scripts/recompute_summary_window.py:70-72` 改为从快照表取日期（或取"快照∩汇总"的并集），否则 09-03/09-15 永远进不了清单。实测：现脚本只覆盖 7 天 | #59 第 5 项 |
| 2 | **打印块修复** | 同文件 `:197-203`，`o = old[dt]` 必须容忍"新增日期"（`old.get(dt)` + `None` 分支）。否则日期源一改，脚本以 `rc=1` 崩在写库之前 | #59 第 5 项 |
| 3 | **09-16 时序决策** | 15:30 跑完会先写出 09-16（其 `prev_dt` 仍是 09-14 ⇒ 又是一个两日值）。需定：先重算再让 09-16 落库（则 09-16 一次性正确），还是后重算（则 09-16 也要进窗口一起改）。**本计划建议把 `--end-date` 保持 `2026-09-16`，用同一条命令覆盖两种情况** | lead 决策 |
| 4 | **写入面验收基线** | 以 §5 的实测为准：`summary 3477→3479 (+2)`、`nav 3477→3479 (+2)`、既有行 summary 改 7 / nav 改 7 | lead 确认 |
| 5 | **`mwr_return` 7 行重算是否接受** | 幅度 ≤ 0.0175，成因见 §5.2（行号窗口）。若不接受，需要另立"行号窗口改自然日窗口"的独立任务 | lead 决策 |
| 6 | **闸门就绪** | G1（`history_write_guard.py`，默认 dry-run + `--confirm-window` + 含备份路径与 sha256 的审计 JSON）、G2（各写库脚本的 `--confirm-window`）、G3（`data/reports/history_write.lock` + 交叉窗口需 `--force` + 理由）。本条是第 5 项的**前置** | #59 |
| 7 | **备份** | 生产库当前**不存在** `.bak_recompute_*`，真重算第一条命令必须是 `--backup`；执行前记录整库 `sha256`（本轮锚点 `261a4a6ed03396cd`） | 执行者 |
| 8 | **`--rebuild-nav` 务必带上** | 不加则 summary 多 2 行、nav 少 2 行，NAV/look-through 面板会读到旧口径 | 执行者 |
| 9 | **不动项（明确排除）** | `max_drawdown_60d` / `max_drawdown_1y` / `max_drawdown_all` 三列本脚本不写（现状 NULL，`INSERT` 不含这三列）；`snapshot_type` 由 `:156-157` 的 `COALESCE` 保留 | — |

---

## 十、命令速查

```bat
:: 0) 基线锚点（执行前记录）
venv313\Scripts\python.exe -c "import hashlib,pathlib; p=pathlib.Path(r'data\database\portfolio.db'); print(hashlib.sha256(p.read_bytes()).hexdigest()[:16], p.stat().st_size)"

:: 1) 备份（必须）
venv313\Scripts\python.exe scripts\recompute_summary_window.py --backup

:: 2) dry-run（第 1、2 项修复完成后应打印「共 9 天」并列出 09-03 / 09-15）
venv313\Scripts\python.exe scripts\recompute_summary_window.py --start-date 2026-09-03 --end-date 2026-09-16

:: 3) 落地
venv313\Scripts\python.exe scripts\recompute_summary_window.py ^
    --start-date 2026-09-03 --end-date 2026-09-16 --apply --rebuild-nav

:: 4) 落实验收（期望：3479 / 3479；缺口行数 2；09-04 daily_return = -0.5821866501325468；
::              nav.twr_cumulative 既有行与备份逐行相同）
venv313\Scripts\python.exe -c "import sqlite3; c=sqlite3.connect(r'data\database\portfolio.db'); print(c.execute('SELECT COUNT(*) FROM portfolio_summary').fetchone(), c.execute('SELECT COUNT(*),MAX(date) FROM portfolio_nav').fetchone(), c.execute(\"SELECT COUNT(*) FROM portfolio_summary WHERE date IN ('2026-09-03','2026-09-15')\").fetchone(), c.execute(\"SELECT daily_return FROM portfolio_summary WHERE date='2026-09-04'\").fetchone())"
```

**回滚**：`copy /Y "data\database\portfolio.db.bak_recompute_<时间戳>" data\database\portfolio.db`
（校验期望值见 §6.4）。

---

## 附：本轮取证脚本与产物（均在 `%TEMP%`，不入仓库）

| 脚本 | 产物 | 用途 |
|---|---|---|
| `%TEMP%\p9_forensic.py` | `p9_forensic.txt` | 生产库只读取证（差值集 / 逐日 / `prev_dt` / 执行痕迹） |
| `%TEMP%\p9_exp2.py` | `p9_exp2.txt` | 副本 A/B 对照实验、行级 before/after、TWR 恒等、日历 |
| `%TEMP%\p9_diag.py` | `p9_diag.txt` | 定位 `:197-203` 打印块崩溃（缺陷 2） |
| `%TEMP%\p9_log2.py` | `p9_log2.txt` | 09-03 日志现场重建 |
| `%TEMP%\p9_bak.py` / `p9_bakchk.py` | `p9_bak.txt` / `p9_bakchk.txt` | 备份清单与回滚候选校验 |

未执行项：**没有对生产库做过任何写操作**（全部 `mode=ro`）；副本 `p9_exp2A.db` / `p9_exp2B.db` 已删除。
