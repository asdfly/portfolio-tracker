# 09 · portfolio_summary 缺口修复计划（窗口 2026-09-03 ~ 2026-09-16）

| 项 | 值 |
|---|---|
| 文档编号 | 09 |
| 对应任务 | #65（读证）+ #59 第 5 项（落地） |
| 编写时间 | 2026-09-16 11:20 初稿；**约 13:45 按 lead 裁定增补**（§三b 09-16 确认、§7.1b 看门狗证伪、§7.4b 两条独立缺陷、§九 状态表、§十一 已完成的代码修复与自验） |
| 本轮范围 | **计划 + 只读取证 + 副本实测 + 前置代码修复**；不写生产库、不改 `is_suspect` 逻辑、不动 `docs/handover/07_known_data_issues.md` |
| 代码修复 | `scripts/recompute_summary_window.py`（日期源 ∪ 对照表新增分支）+ `tests/test_recompute_summary_window.py`（19 例），内容随提交 `8a7173c` 入库 |
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
   **行号窗口**（`nav_engine.py:255` `start = max(0, idx - 365)`）的结构性后果，
   **已接受（lead 2026-09-16）**，永久留痕为已知代价，见 §5.2。
8. **`is_suspect` 的 md-vs-r 通道无法用作本缺陷的判据**：实测 09-04 的 `|md−r|` 在修复前后
   几乎不动（2.734 pp → 2.729 pp），因为 09-04 有一笔约 **+50,000** 的入金**不在 `trade_records` 里**
   （`nav.net_flow = 0.0`）。硬化的主判据必须换成**交易日跨度**，见 §七。
9. **今天这根 09-16 也是同一个缺陷的复发**：`_determine_trading_date()` 在 15:30 返回
   `now.date()`（`portfolio.py:85-86`），而 `prev_dt` 取自汇总表（`portfolio.py:482-488`）——
   汇总表里没有 09-15，所以 `prev_dt` 会是 **09-14**。⇒ 今天日报的「今日涨跌」是两日值。
   详见 §十一（含 15:45 一键对撞脚本）。
10. **09-03 的死因不是看门狗**：全量 112 个日志里 `[WATCHDOG]` 只出现 **1 次**（08-19 16:22:35），
    09-03 日志里没有 ⇒ 09-03 是在 **15:30:48 ~ 16:20:29** 之间被其它原因硬杀的。详见 §7.1。
11. **两个前置代码缺陷已修**（日期源 / 对照表新增分支），修好后同一条 dry-run 覆盖 9 天、`rc=0`、
    打印 2 个 `NEW`，且 `compute()` 结果与已认下的写入面**逐位一致**。详见 §十二。
12. **`mwr_return` 那 7 行小改写已被 lead 接受**（2026-09-16），作为已知代价保留，见 §5.2。

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

## 三b、今天（2026-09-16）这根必然也是两日值 —— 代码确认 + 15:45 对撞方案

### 3b.1 推理链（两步都由代码与数据坐实，不是猜测）

**第一步：今天写出的行日期是 `2026-09-16`。** `src/analysis/portfolio.py:64-86`：

```python
64  def _determine_trading_date(self) -> str:
73      now = datetime.now()
74      current_time = now.hour * 100 + now.minute      # 15:30 -> 1530
76      if current_time < 930:
             ...                                            # 开盘前才回退
85      else:
86          return now.strftime('%Y-%m-%d')                 # >= 9:30 ⇒ 直接取 now.date()
```

15:30 运行 ⇒ `current_time = 1530 >= 930` ⇒ 返回 `now.date()` = `2026-09-16`。
（注：`_determine_trading_date` 的 `days_back` 回退分支只在 9:30 **之前**才走，
所以"15:30 跑出来的日期就是当天"这条成立。）

**第二步：它的 `prev_dt` 会是 `2026-09-14`。** `src/analysis/portfolio.py:482-488`：

```sql
SELECT date FROM portfolio_summary WHERE date < ? ORDER BY date DESC LIMIT 1
```

当前 `portfolio_summary` 的 `max(date) = 2026-09-14`（09-15 那一行不存在）⇒ `prev_dt = 2026-09-14`
⇒ 今天写出的 `daily_return` 是 **09-14 → 09-16 的两日值**，与 09-04 **同形**。

⇒ **今天 15:30 发出日报的「今日涨跌」字段本身就是这个缺陷的又一次复发。**

### 3b.2 两个数的定义与代数桥

对同一组共同持仓（数量不变），共同持仓法的链式恒等式给出：

```
数1（管线会写出的，prev_dt = 09-14）
      = (1 + r_0915)(1 + 数2) - 1
数2（真·单日 = 09-15 收盘 → 09-16 收盘）
      = 今天的单日涨跌
偏差  数1 - 数2 ≈ r_0915 = -0.444697%   （加上二阶项 -r_0915·数2/100，量级 <1e-5 pp）
```

其中 `r_0915 = -0.444697%` 已由 09-14/09-15 快照实测得出（§2.4）。
⇒ 日报的「今日涨跌」将比真实单日**低约 0.44 pp**，`vs_hs300` 继承同量级偏差
（`index_quotes` 当日涨跌两边相减会自动抵消，所以偏差全部落在 `daily_return` 侧）。

### 3b.3 为什么现在给不出这两个数（依赖未就绪，实测）

09-16 的行情/持仓数据**此刻在库里完全不存在**。已逐表扫描 41 张含 `date` 列的表：

```
含 2026-09-16 数据的表: 无
portfolio_snapshots 09-16 行数 = 0        ⇒ 数2 无法计算
portfolio_summary   09-16 行数 = 0        ⇒ 数1 无法读取
（全库 max(date) 最靠前的也只有 etf_price_history / index_quotes / portfolio_snapshots 的 2026-09-15）
```

⇒ 两个数**必须等 15:30 批次把 09-16 写进库之后**才能产出。**任何"现在就算出来"的说法都是假的。**

### 3b.4 15:45 一键对撞（已就绪，无需改动）

脚本已写好并试跑通过（当依赖缺失时它会明确报"尚未就绪"，不会给假数）：

```bat
venv313\Scripts\python.exe %TEMP%\p9_0916_verify.py
```

全程 `mode=ro`。它输出并判定：

| 输出 | 含义 |
|---|---|
| 数1 | `portfolio_summary['2026-09-16'].daily_return` —— 管线实际写出的值 |
| 数2 | 用 09-15/09-16 快照按共同持仓口径算出的**真·单日**值 |
| 链式对撞 | `(1+r_0915)(1+数2)-1` 与 数1 的差 |
| 判定 | `|数1 - 链式值| < 0.01pp` 且 `|数1 - 数2| > 0.01pp` ⇒ **确认复发**；反之若 `|数1 - 数2| < 0.01pp` ⇒ 未复发 |

**为什么用链式对撞而不是比大小**：只看"数1 比 数2 低 0.44 pp"不能排除巧合；
而 `(1+r_0915)(1+数2)-1` 与 数1 在 `1e-7 pp` 量级上吻合，是**只有"数1 记的是两日链"才能解释**的指纹
（09-04 的 1.16e-07 pp 就是这样验出来的）。

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
**是 nav 唯一被改写的既有列**。

> **处置：已接受（lead 2026-09-16）。** 理由：① 头条是 TWR，而 `twr_cumulative` 既有行
> **0 变动**已有实测；② 变动是行号窗口的结构性后果，幅度 ≤ 0.0175；③ 把行号窗口改成
> 自然日窗口是独立任务，塞进本轮会把一个已验证的修复变成口径工程。
> 本条在此**永久留痕为已知代价**，后续若有人发现 `mwr_return` 变了，不是漏项。

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

`scripts/recompute_summary_window.py` 的 `backup()` —— **落点已修，落在 `data/backups/`**：

```python
def backup(db_path: str, backup_dir=None) -> str:
    """把整库备份到 data/backups/（config.settings.BACKUP_DIR），返回备份路径。"""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_dir = Path(backup_dir) if backup_dir is not None else Path(BACKUP_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    dst = str(target_dir / f"{os.path.basename(db_path)}.bak_recompute_{stamp}")
    shutil.copyfile(db_path, dst)
```

- 加 `--backup` 后产出：**`data/backups/portfolio.db.bak_recompute_<YYYYmmdd_HHMMSS>`**（约 141 MB）。
- ⚠️ **原实现写 `f"{db_path}.bak_recompute_{stamp}"`，备份会直接躺在 `data/database/` 里。**
  本仓库约定 **`data/database/` 只允许有 `portfolio.db`**（立规理由：防 worker 连错库），
  所以原实现**一次 `--backup` 就把这条约定破了，并留下一个"看起来能连"的库**。
  已改为落 `BACKUP_DIR`，这一步**不再依赖任何人记得搬**（lead 2026-09-16 补的落点约束）。
- 证据：`data/backups/portfolio.db.bak_recompute_20260915_110813` **已存在同款命名**（09-15 11:08，
  早于本会话），说明这条路此前有人走过；同时 `data/database/` 目前**只有 `portfolio.db`**，
  即当时被清干净了（或为手工挪动）。无论哪种，落点都不该靠手工保证 —— 故本次直接改脚本。
- 优点：它是在 `--apply` **紧前**做的整库拷贝，因此**天然包含执行时刻的全部最新状态**
  （如果 15:30 已跑完，备份里就有 09-16），回滚语义最干净。
- **执行后必须核验**：`data/database/` 下**只有** `portfolio.db`（把目录列表作为证据回填）。

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

### 7.1b 死因核查：**不是**看门狗（假设被证伪）

曾有一个很有解释力的假设：这次硬死是 `src/data_sources/collect_core.py:167` 的全局墙钟看门狗
`os._exit(1)` 造成的 —— 因为 `os._exit` 既不走 `except` 也不走 `finally`，正好能解释
"连 failed 终态都没有"。**实测证伪，全部证据如下：**

看门狗机制（确认存在）：

```
collect_core.py:150  def start_watchdog(minutes: float = 50, logger_=None):
collect_core.py:159      def _fire():
collect_core.py:160          msg = f"[WATCHDOG] 全局墙钟超时 {minutes}min，强制退出以避免无限挂死"
collect_core.py:161          if logger_:
collect_core.py:162              logger_.error(msg)
collect_core.py:164          sys.stderr.write(msg + "\n")
collect_core.py:167          os._exit(1)  # 硬退出
run_analysis.py:998  _WATCHDOG_MINUTES = 50
run_analysis.py:1000 _wd = start_watchdog(minutes=_WATCHDOG_MINUTES, logger_=logger)
```

关键点是 `logger_` **确实被传进来了**（`run_analysis.py:1000`），所以一旦触发，
日志文件里必然留下一行 `[WATCHDOG] ...`。而实测：

```
112 个 logs/portfolio_*.log 中 "WATCHDOG" 只出现 1 次：
  portfolio_20260819.log:248   2026-08-19 16:22:35,324 - __main__ - ERROR -
      [WATCHDOG] 全局墙钟超时 50min，强制退出以避免无限挂死
09-03 日志中 "WATCHDOG / 墙钟 / 强制退出 / 看门狗" 命中数 = 0
```

**反证**：唯一一次确认的看门狗强杀（08-19）发生在 `signal_backtest` 阶段（16:21 还在跑
Per-ETF 回测），而 08-19 的落库结果是 `summary=1 / nav=1` —— 也就是说
**那次强杀并没有造成缺口**，因为 `portfolio_summary` 早在 `步骤5` 就写完了。
看门狗是在"写完之后"才开火的，它是**报告期的兜底**，不是缺口制造者。

**因此 09-03 的死因另有其人**，且可由看门狗的 50 分钟时限**反推出一个硬边界**：

- 批次约 `15:30:29` 起跑（`run_analysis.py:1000` 此刻布防）⇒ 若活着到 `16:20:29`，
  必然写下 `[WATCHDOG]` 行；
- 实测没有该行，且该批次最后一行是 `15:30:48` `步骤4: 计算技术指标`
  ⇒ **进程死在 `15:30:48` ~ `16:20:29` 之间**，且是**外部/硬性**终止
  （全程无 Traceback、无 ERROR、无 finally 产物）。

剩余候选（**均未证实，需项目所有者手工核查**）：计划任务的时间上限勒停、控制台窗口被关闭、
机器休眠/关机、原生库硬崩溃。本机 `schtasks.exe` 被安全策略黑名单拦截
（`PROGRAM BLOCKED BY SECURITY POLICY`），**我无法读取计划任务配置，且按规则不做任何绕行尝试**。
建议所有者手工执行并核对"如果任务运行时间超过以下时间，停止任务"这一项：

```bat
schtasks /query /tn PortfolioDailyAnalysis /v /fo LIST
```

### 7.1c 一个仍然成立的残留洞（与 #55 有关，值得单独记一笔）

即使死因不是看门狗，**看门狗这条路依然能造出"无终态"**：`RunReporter` 的报告是在
`finally` 里出的（`run_analysis.py:1002` 注释"贯穿全程增量记录, finally 出报告"），
而 `os._exit(1)` **会绕过 `finally`** ⇒ 看门狗开火时，`run_report` 与 `execution_logs`
的终态**都写不出来**。今天 #55 补的 `mark_run_failed` / 阶段完整性同样救不了这一路。

⇒ 残留结论：**任何 `os._exit` 式的硬退出（含外部强杀）天然是观测盲区**，
只能靠"外部巡检"（例如次日检查 `summary` 是否缺行，即正式工单 G5 的复发检测）兜底，
不能指望进程内的 finally。这条并入 §7.5 的判据依据。

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
| 09-03 那次运行本身 | `execution_logs` 只有 `{status:'running'}`（id 501），**无终态**；`data/reports/` 27 个 run_report **不含 09-03** | 进程被硬性终止（非看门狗，见 §7.1b），`finally` 没走到 ⇒ 连"我失败了"都没人喊 |
| `RunReporter` / `run_status`（#55 修的那套） | 09-04 的 `run_status` 是 `ok` | 那天的运行**确实成功了**，缺行不是当天产生的 |
| `execution_logs` | `success` | 同上 |
| `DataQualityChecker` | 09-04 `score=96.5, alerts=4`；09-14 `score=90.6, alerts=9` | 没有一条检查项比对"快照日期集合 vs 汇总日期集合" |
| `portfolio_nav.is_suspect` | **09-01 ~ 09-14 全为 0** | 见 §7.4 |

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

### 7.4b 顺带挖出的两条**独立缺陷**（已登记，归属单独工单，**今天不动**）

| # | 缺陷 | 位置 | 后果 | 归属 |
|---|---|---|---|---|
| D-1 | **阈值单位错**：`md` 与 `r` 都是**小数**，而阈值写 `0.30` ⇒ 实际等于 **30 个百分点**，不是 0.3pp | `src/analysis/nav_engine.py:36`（阈值）、`:329-332`（比较） | 任何 0.3 pp 量级的错位都过不了这道网。"守卫看着在、实际从不触发"——与今天 P0 那条同一家族 | 单独工单 |
| D-2 | **现金流入账缺口**：09-04 有约 **+50,000** 入金（`SUM(cost_price*quantity)` 增加 `50,000.6578`，标的 34→35；`trade_records` 的 `max(date)` 仅到 **2026-08-31**），`_daily_net_cashflow` 取不到 ⇒ `nav.net_flow = 0.0` ⇒ `md` 被未建模现金流整个抬起 | `src/analysis/nav_engine.py:56-97`（`_daily_net_cashflow`） | `md` 通道**恒指错方向**；实测把阈值收到 0.3 pp 后，修复前后**都会**报 09-04 ⇒ 光调阈值无用，必须先把现金流量进表 | 单独工单 |

> D-1 与 D-2 的联合结论已写进 §7.5：`is_suspect` 硬化的**主判据必须是交易日跨度**，
> 不能依赖 md-vs-r。

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

## 九、执行前清单（含 2026-09-16 裁定后的状态）

> 排序裁定（lead 2026-09-16）：**数据修复在前，G1/G2/G3 在后**，但 G1/G2/G3 仍要做，
> 并让"重算 09-03~09-16"成为 **G1 的第一个真实用户**（G1 出世即带真实案例，不空转）。
> 理由：现有脚本已自带 dry-run + backup，加上整库 sha256 锚点 + 逐行预期值 + 验证 SQL，
> 框架前置会把一个已验证透彻的修复拖成框架工程；且数据缺口每多挂一天，
> `sharpe`/`volatility` 的 60 日窗口就多带一天错值。

| # | 事项 | 状态 | 说明 |
|---|---|---|---|
| 1 | **日期源修复** | **✅ 已闭环** | 新增 `resolve_dates()`，取 `portfolio_snapshots` ∪ `portfolio_summary` 并集；并**显式返回/报出**反方向（只有汇总行、没有快照）的日期，不静默丢。前驱日期同样取并集。见提交 `8a7173c` 中的 `scripts/recompute_summary_window.py` |
| 2 | **打印块修复** | **✅ 已闭环** | 抽出 `format_diff_table()`，`old.get(dt)` + 新增分支，行尾标 `NEW`，汇总行打印新增日期清单。**未**用大 `try/except` 吞掉（并有测试护栏：异常必须上抛） |
| 3 | **09-16 时序** | **✅ 已定** | 采纳 `--end-date 2026-09-16` 固定；今天这根的两日值已由代码坐实，见 §三b |
| 4 | **写入面验收基线** | **✅ 已认下** | `summary 3477→3479 (+2)`、`nav 3477→3479 (+2)`、既有行 summary 改 7 / nav 改 7；修复后 `compute()` 与基线**逐位一致**（Δ=0.00e+00，见 §十一） |
| 5 | **`mwr_return` 7 行重算** | **✅ 已接受** | lead 2026-09-16；幅度 ≤ 0.0175，成因见 §5.2。已永久留痕为已知代价 |
| 6 | **闸门（G1/G2/G3）** | ⏳ **排在数据修复之后** | G1：`src/utils/history_write_guard.py`，默认 dry-run，`mode="apply"` 需 `--confirm-window`（与 G2 合并为同一个开关），审计 JSON 含**备份文件 sha256 + 备份路径**。G2：`backfill_full_history.py` / `recompute_summary_window.py` / `fetch_otc_fund_nav.py` 加 `--confirm-window`，须与实际计算出的窗口完全相等。G3：`data/reports/history_write.lock`，窗口交集需 `--force` + 理由。**"重算 09-03~09-16"作为 G1 的第一个真实用户** |
| 7 | **G5 复发检测** | ⏳ 排在重算之后 | 写 `portfolio_summary` 前检查 `prev_dt` 与今天之间是否"有快照无汇总行"，有则**显式告警**（列出缺失日期与跨度）并往 `run_report` 丢 `summary_gap_before_write`。warn + continue（拒绝写入会让当天报告完全没有数据）。验收：pre-fix 副本必须报**两组**（09-03 一组、09-15 一组），post-fix 副本不报 |
| 8 | **`is_suspect` 硬化** | ⏳ 最后 | 交易日跨度 `span_days` 为主判据（见 §7.5）。双向验收：pre-fix 必须同时报 09-04 与 09-16，post-fix 都不报；**两条断言缺一不可**。注意 09-16 那一半只能等今天落库后才能验 |
| 9 | **备份（含落点已修）** | **✅ 落点已闭环** / ⏳ 执行者第一步 | `backup()` 已改为落 `data/backups/`（原先落 `data/database/`，会破"该目录只允许 portfolio.db"的约定，见 §6.1）。真重算第一条命令仍是 `--backup`；执行前记整库 `sha256`（本轮锚点 `261a4a6ed03396cd`），执行后核验 `data/database/` **只有** `portfolio.db`。⚠️ `--backup` 本身是写操作，不得落在 15:00–15:30 |
| 10 | **`--rebuild-nav` 务必带上** | ⏳ 执行者 | 不加则 summary 多 2 行、nav 少 2 行，NAV/look-through 面板会读到旧口径 |
| 11 | **不动项（明确排除）** | — | `docs/handover/07`（他人未提交改动，由 lead 统一提交）；`trading_calendar` 覆盖外年份退化（→ #66）；`is_suspect` 逻辑本身；`max_drawdown_60d/1y/all` 三列（本脚本不写，现状 NULL）；`snapshot_type` 由 `:186-187` 的 `COALESCE` 保留 |

---

## 十、命令速查

```bat
:: 0) 基线锚点（执行前记录）
venv313\Scripts\python.exe -c "import hashlib,pathlib; p=pathlib.Path(r'data\database\portfolio.db'); print(hashlib.sha256(p.read_bytes()).hexdigest()[:16], p.stat().st_size)"

:: 1) 备份（必须；落点已是 data/backups/，无需再手工 mv）
::    先记执行前锚点
venv313\Scripts\python.exe -c "import hashlib,pathlib; p=pathlib.Path(r'data\database\portfolio.db'); print('BEFORE', hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_size)"
venv313\Scripts\python.exe scripts\recompute_summary_window.py --backup
::    回填备份的 sha256（G1 审计要素）
venv313\Scripts\python.exe -c "import hashlib,glob; f=sorted(glob.glob(r'data\backups\portfolio.db.bak_recompute_*'))[-1]; print(f, hashlib.sha256(open(f,'rb').read()).hexdigest())"

:: 2) dry-run 复核（应打印「[缺口] ... ['2026-09-03','2026-09-15']」与「共 9 天（其中新增 2 天…）」）
venv313\Scripts\python.exe scripts\recompute_summary_window.py --start-date 2026-09-03 --end-date 2026-09-16

:: 3) 落地
venv313\Scripts\python.exe scripts\recompute_summary_window.py ^
    --start-date 2026-09-03 --end-date 2026-09-16 --apply --rebuild-nav

:: 4) 落实验收（期望：3479 / 3479；缺口行数 2；09-04 daily_return = -0.5821866501325468；
::              nav.twr_cumulative 既有行与备份逐行相同）
venv313\Scripts\python.exe -c "import sqlite3; c=sqlite3.connect(r'data\database\portfolio.db'); print(c.execute('SELECT COUNT(*) FROM portfolio_summary').fetchone(), c.execute('SELECT COUNT(*),MAX(date) FROM portfolio_nav').fetchone(), c.execute(\"SELECT COUNT(*) FROM portfolio_summary WHERE date IN ('2026-09-03','2026-09-15')\").fetchone(), c.execute(\"SELECT daily_return FROM portfolio_summary WHERE date='2026-09-04'\").fetchone())"

:: 5) 落点核验（操作单硬性两条之一）：data/database/ 下【只有】portfolio.db
venv313\Scripts\python.exe -c "import pathlib; print(sorted(p.name for p in pathlib.Path(r'data\database').iterdir()))"
```

**回滚**：`copy /Y "data\backups\portfolio.db.bak_recompute_<时间戳>" data\database\portfolio.db`
（校验期望值见 §6.4）。

### 10.1 操作单（lead 2026-09-16 追加的两条硬性约束）

```
① 备份产物必须落在 data/backups/ —— 已由脚本保证（backup() 落 BACKUP_DIR），不再依赖手工 mv
② 执行完毕后核验：data/database/ 下【只有】portfolio.db（把目录列表作为证据回填）
③ --backup 本身也是写操作 ⇒ 不能落在 15:00–15:30 只读窗口内
④ 任何硬退出（os._exit / 外部强杀）之前必须先落痕 —— 见 §11.6
```

---

## 十一、本轮已完成的代码修复与自验（2026-09-16，**未执行任何生产库写入**）

### 11.1 改了什么

`scripts/recompute_summary_window.py`（+131 / -16）：

| 位置 | 改动 |
|---|---|
| 新增 `resolve_dates(cur, start, end)` | 返回 `(dates, snapshots_only, summary_only)`。`dates` 取快照 ∪ 汇总的**并集**；`summary_only`（反方向）**显式返回**，由 `compute()` 打印警告，绝不静默丢 |
| `compute()` 头部 | 用 `resolve_dates()` 取日期；`snaps_only` 非空时打印 `[缺口] ... 将补算`；`sums_only` 非空时打印 `[警告] ... 将被跳过（请人工确认）` |
| `compute()` 的 `prev_dates` | 由"只查 summary"改为 **union**：避免窗口起点之前的缺口日被跨过、把缺口复制到窗口第一天。（对当前窗口行为等价，见 §11.2 逐位比对） |
| 新增 `format_diff_table(computed, old)` + `_cell_money/_cell_float3/_cell_str` | 取代 `main()` 里内联的打印循环。`old.get(dt)` + 新增分支；新增行旧列渲染 `<无行>`、行尾标 `NEW`；列宽常量固定，行长度一致 |
| `backup()` 落点 | 由 `f"{db_path}.bak_recompute_{stamp}"`（落在 `data/database/`，破"该目录只允许 portfolio.db"的约定）改为落 `config.settings.BACKUP_DIR`（= `data/backups/`）并自动建目录；新增可选 `backup_dir` 参数便于注入测试。文件名沿用既有惯例 |
| `main()` 打印段 | 调 `format_diff_table()`；有新增日期时打印 `共 N 天（其中新增 M 天: [...]）` 与提示行 |

**刻意不做的事**：没有用一个大 `try/except` 包住打印块（那只是把崩溃变成静默），
也没有在 `--apply` 里加任何"猜你想干什么"的自动修复。

### 11.2 自验一：修好后在**副本**上跑同一条 dry-run

生产库全程 `mode=ro`；副本 `%TEMP%\p9_fixcopy.db` 跑完即删；未调用 `--backup`、未调用 `--apply`。

```
$ venv313/Scripts/python.exe scripts/recompute_summary_window.py --start-date 2026-09-03 --end-date 2026-09-16
  rc = 0
  [缺口] 窗口内 2 个日期只有快照、没有汇总行，将补算: ['2026-09-03', '2026-09-15']
  date                  tv旧          tv新     dr旧%     dr新%  ...  盈亏旧     盈亏新    标记
  2026-09-03           <无行>    1,528,187     <无行>    0.166  ...  <无行>   19/15   NEW
  2026-09-04      1,561,001    1,561,001   -0.417   -0.582  ...   20/15   20/15
  2026-09-07      1,541,964    1,541,964    1.492    1.492  ...   19/15   19/15
  2026-09-08      1,543,547    1,543,547    0.103    0.103  ...   20/14   20/14
  2026-09-09      1,545,312    1,545,312    0.114    0.114  ...   20/14   20/14
  2026-09-10      1,535,484    1,535,484   -0.636   -0.636  ...   20/14   20/14
  2026-09-11      1,564,453    1,564,453   -0.943   -0.943  ...   19/16   19/16
  2026-09-14      1,514,541    1,514,541   -0.425   -0.425  ...   19/15   19/15
  2026-09-15           <无行>    1,507,806     <无行>   -0.445  ...  <无行>   19/15   NEW

  共 9 天（其中新增 2 天: ['2026-09-03', '2026-09-15']）
  [DRY-RUN] 未写库。

  dry-run 后 summary=3477 nav=3477 缺口行=0  ⇒ 未写库（正确）
```

对照修复前：同一条命令只覆盖 **7 天**，且加缺口场景 `rc=1`（`TypeError`）。
**现在 `rc=0`、9 天、2 个 NEW 全部可见、副本零写入。**

### 11.3 自验二：`compute()` 输出与被认下的写入面**逐位一致**

```
2026-09-03  dr 期望 +0.166127538229 实得 +0.166127538229  Δ=0.00e+00 | tv Δ=0.00e+00
2026-09-04  dr 期望 -0.582186650133 实得 -0.582186650133  Δ=0.00e+00 | tv Δ=0.00e+00
2026-09-15  dr 期望 -0.444696500483 实得 -0.444696500483  Δ=0.00e+00 | tv Δ=0.00e+00
结论：修复后的输出与已认下的写入面逐位一致
```

即：**本次代码修复没有改变任何一个将被写入的数值**，只改变了"这些日期能不能进清单、
以及进不了/新增了会不会被看见"。

### 11.4 自验三：测试

新增 `tests/test_recompute_summary_window.py`，**28 例全绿**，全部使用合成库（`tmp_path`，
含一个与真实布局同形的 `<tmp>/database/portfolio.db`），不触碰生产库。覆盖：

- `resolve_dates`：含缺口日 / 报出反方向 / 窗口边界 / 无快照日期被警告并跳过
- **缺口补上后次日为单日 +1.00%**；对照组（拿掉缺口日快照）退化为**两日 +2.01%**
  —— 把缺陷机理写成了可执行证据，而不是注释
- `format_diff_table`：新增日期不崩、`<无行>`/`NEW` 可见、`None` 列与 `<无行>` 不混淆、列宽一致
- **备份落点**：落在 `data/backups/`、不在库同目录、库目录事后仍**只有** `portfolio.db`、
  目录不存在时自动创建、命名沿用惯例、不注入时默认取 `config.settings.BACKUP_DIR`
- `main()` 端到端：dry-run 不写库 / apply 真的写入缺口日且 `snapshot_type` 兜底 `daily` /
  二次 apply 不再报新增
- **护栏**：`format_diff_table` 与 `apply_summary` 抛错必须上抛 ——
  直接锁死"不许把崩溃变成静默"

### 11.5 自验四：写入面未被本次改动影响

改完 `backup()` 落点后，在副本上重跑同一条 dry-run 并重做逐位比对：

```
rc = 0
[缺口] 窗口内 2 个日期只有快照、没有汇总行，将补算: ['2026-09-03', '2026-09-15']
共 9 天（其中新增 2 天: ['2026-09-03', '2026-09-15']）
dry-run 后 summary=3477 nav=3477 缺口行=0  ⇒ 未写库（正确）
2026-09-03  dr 期望 +0.166127538229 实得 +0.166127538229  Δ=0.00e+00
2026-09-04  dr 期望 -0.582186650133 实得 -0.582186650133  Δ=0.00e+00
2026-09-15  dr 期望 -0.444696500483 实得 -0.444696500483  Δ=0.00e+00
结论：修复后的输出与已认下的写入面逐位一致
```

⇒ 落点改动只影响"备份写到哪儿"，**`compute()`/`apply_summary()` 的输出一字未动**。

### 11.6 硬约束（lead 2026-09-16 批准并要求写成**可测**形式）

| 约束 | 落地要求 | 可测形式 |
|---|---|---|
| C-1 **任何硬退出之前必须先落痕** | G1 的 audit 落盘必须在任何 `os._exit` / `sys.exit` **之前**完成 | 测试断言：模拟退出路径时 audit 文件**已存在且完整**（不是"退出时顺手写"） |
| C-2 **告警必须外部可读** | G5 的 `summary_gap_before_write` 必须走 `run_report`（文件），**不能只写进程内状态** | 测试断言：缺口场景下 `run_report_*.json` 里能读到该 alert，而不只是内存里有 |
| C-3 **不用 `os._exit`** | 写脚本时避免 `os._exit`；必须用时按 C-1 先落痕 | 本脚本现状：`os._exit` 出现 **0 次**，只有一处 `sys.exit(main())` |

> 立项理由见 §7.1c：`os._exit` 绕过 `finally` ⇒ `run_report` 与 `execution_logs` 终态都写不出，
> #55 补的 `mark_run_failed` 也救不了这一路。**进程内兜底在原理上做不到，只能靠外部可读的留痕 + 外部巡检。**

### 11.7 提交与一个需要协调者知悉的插曲（诚实记录）

- 修复内容已进入提交 **`8a7173c`**（该提交的信息是 `docs(handover): 10 相关性复制行守卫判据决策留痕`，
  属另一位队友的文档提交）。
- 原因：共享工作树下 `git add` 写的是**共享索引**。我把两个文件 `git add` 之后，
  另一位队友在此刻执行了一次不带 pathspec 的 `git commit`，于是把它们一并带入；
  我自己的 `git commit <path>` 同时因 `.git/index.lock` 被占用而 `rc=128` 失败。
- 内容核对无误：`scripts/recompute_summary_window.py` +131/-16、
  `tests/test_recompute_summary_window.py` +371 均已入库，工作区对这两个文件已干净。
- **没有做任何历史改写**（不 amend、不 rebase 共享提交）。
- 建议（供协调者决定）：共享工作树下**一律用 `git commit <显式路径>` 直接提交、不要先 `git add`**，
  否则任何队友的并发提交都会把别人暂存的文件卷走。

---

## 附：本轮取证脚本与产物（均在 `%TEMP%`，不入仓库）

| 脚本 | 产物 | 用途 |
|---|---|---|
| `%TEMP%\p9_forensic.py` | `p9_forensic.txt` | 生产库只读取证（差值集 / 逐日 / `prev_dt` / 执行痕迹） |
| `%TEMP%\p9_exp2.py` | `p9_exp2.txt` | 副本 A/B 对照实验、行级 before/after、TWR 恒等、日历 |
| `%TEMP%\p9_diag.py` | `p9_diag.txt` | 定位 `:197-203` 打印块崩溃（缺陷 2） |
| `%TEMP%\p9_log2.py` | `p9_log2.txt` | 09-03 日志现场重建（15:30 批次最后一行、小时分布、异常行） |
| `%TEMP%\p9_wd2.py` | `p9_wd2.txt` | 看门狗假设证伪：112 个日志中 `WATCHDOG` 仅 1 次（08-19），09-03 为 0 |
| `%TEMP%\p9_bak.py` / `p9_bakchk.py` | `p9_bak.txt` / `p9_bakchk.txt` | 备份清单与回滚候选校验（含 sha256） |
| `%TEMP%\p9_0916chk.py` | `p9_0916chk.txt` | 41 张表扫描：确认 09-16 数据此刻不存在（两数不可算） |
| **`%TEMP%\p9_0916_verify.py`** | `p9_0916_verify.txt` | **15:45 一键对撞**：产出数1/数2 并做链式判定（依赖未就绪时明确报"尚未就绪"，不给假数） |
| `%TEMP%\p9_fixverify.py` | `p9_fixverify.txt` | 修复后在副本上 dry-run 自验 + 与写入面逐位比对 |

未执行项：**没有对生产库做过任何写操作**（全部 `mode=ro`）；
本轮所有副本（`p9_exp2A/B.db`、`p9_diag.db`、`p9_fixcopy.db`）均已删除；
未调用过 `recompute_summary_window.py --backup` 或 `--apply`。
