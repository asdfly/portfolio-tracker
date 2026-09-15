# 07 · 已知数据问题与口径陷阱

> 记录日期：2026-08-26
> 状态：**✅ 已修复**（2026-09-03 随提交落地：代码正则 + 生产库脏数据双修，见各节"✅ 已修复"标注）
> 证据获取方式：对 `data/database/portfolio.db` 以 `file:...?mode=ro` 只读连接执行 SQL，未做任何写入。

本文档登记交接评估期间实测发现的数据正确性问题。这些问题**不影响现有功能运行**（不会报错、不会崩溃），但会让统计口径失真、并可能污染下游分析。因为影响面是"数字不对"而非"跑不起来"，容易长期潜伏，故单列成篇。

---

## 问题一：`_is_etf` 代码正则存在假阳性，把场外基金判成 ETF

### 位置

`src/analysis/predictor/build_base.py:23-32`

```python
_ETF_CODE_RE = re.compile(r"^(5\d{5}|1[56]\d{4})$")

def _is_etf(code: str, name: str) -> bool:
    """判定持仓记录是否为 ETF：name 含 'ETF' 或代码符合 ETF 模式。"""
    if name and "ETF" in str(name).upper():
        return True
    if code and _ETF_CODE_RE.match(code):
        return True
    return False
```

### 问题

`5\d{5}` 会吞掉整个 `5xxxxx` 段，但 **`519xxx` 是场外开放式基金代码段**，不是场内 ETF。同理 `1[56]\d{4}` 会吞掉 `166xxx`（LOF / 分级基金）。

实测对全历史 36 只标的逐一判定，正则与"名称含 ETF"两种判据冲突 3 例：

| 代码 | 名称 | 正则判定 | 名称判定 | 真实身份 | 结论 |
|------|------|---------|---------|---------|------|
| `166301` | 华商新趋势优选灵活配置混合型证券投资基 | ✅ ETF | ❌ | 混合型 LOF | **正则假阳性** |
| `519770` | 交银优择回报灵活配置混合A | ✅ ETF | ❌ | 场外混合基金 | **正则假阳性** |
| `512810` | 国防军工 | ✅ ETF | ❌ | 华宝中证军工 ETF | **名称判据假阴性**（库里存简称，无 "ETF" 字样） |

因为 `_is_etf` 是 `name` 判据与 `code` 判据的**或**关系，`512810` 靠正则救回来了（结果正确），但 `166301` / `519770` 被正则错误放行（结果错误）。

### 实际影响：目前为零，但是"侥幸"

`resolve_target_codes`（同文件 L35+）只对**最新持仓快照**跑 `_is_etf`。当前持仓 22 只已全为真 ETF，`166301` / `519770` 早已清仓、不在最新快照里，所以两个假阳性**当前没有进入预测底座**。

这是运气，不是设计。一旦这两只（或任何 `519xxx` / `166xxx` 场外基金）被重新买入并出现在最新快照，它们就会：

1. 被纳入 `resolve_target_codes` 的目标域；
2. 触发 `backfill_etf_price_history` 去东方财富 / 新浪拉**场内 ETF 日 K**——而场外基金没有场内日 K，采集会失败或拉到空数据；
3. 进而在 `etf_features` / `etf_forward_returns` 里产生残缺行，污染风险模型训练集。

### 建议修法

不要继续在正则上打补丁（代码段规则会变）。改为**以配置为准**：

- 首选：判定改为 `code in config.settings.ETF_CATEGORIES`（实测该配置全集 = 23 只 = `etf_technical` 集合，是干净的权威源）。
- 若必须保留正则兜底，至少排除已知场外段：`519xxx`、`166xxx`，即把 `5\d{5}` 收紧为 `5(0|1|2|3|5|6|8)\d{4}` 之类的白名单式写法，并补单元测试固定住 `166301` / `519770` / `512810` 三个样本的期望判定。

### 建议回归用例

```python
# 三个真实样本，把两类错误都钉住
assert _is_etf("512810", "国防军工") is True       # 真 ETF，名称无 ETF 字样
assert _is_etf("166301", "华商新趋势优选混合") is False  # LOF，勿判 ETF
assert _is_etf("519770", "交银优择回报混合A") is False   # 场外，勿判 ETF
```

### ✅ 已修复（2026-09-03，随提交落地）

未采用"查 `config.ETF_CATEGORIES`"方案，改用**收紧正则**：

```python
_ETF_CODE_RE = re.compile(r"^(?!166|519)(5\d{5}|1[56]\d{4})$")
```

理由：查表会把 `512810`（华宝中证军工 ETF，库内存简称"国防军工"、无 "ETF" 字样、且**不在** `config.ETF_CATEGORIES` 代码列表里）误杀——它会因 name 无 ETF、code 不在表而被判非 ETF，反而漏掉一只真 ETF。收紧正则保留 `5\d{5}` 兜住 `512810`，同时用负向前瞻 `(?!166|519)` 排除 `166xxx`(LOF) / `519xxx`(场外) 两个已知场外段，`name含ETF` 仍为主信号。9 例验证全过（含 `512810`→True、`166301`/`519770`/`001323`/`002152`/`001194`→False）。

---

## 问题二：`etf_fundamental` 表混入 2 只场外混合基金

### 现状

`etf_fundamental` distinct `code` = **25**，比真实场内 ETF 全集（23）多出 2 只：

| 代码 | 名称 | `etf_fundamental` 行数 |
|------|------|----------------------|
| `001323` | 东吴移动互联混合A | 19 |
| `002152` | 华宝核心优势混合 | 19 |

这两只是**场外混合基金**，不该出现在一张名为 `etf_fundamental` 的表里。集合关系实测：

```
etf_fundamental - etf_technical = {001323, 002152}
etf_technical  - etf_fundamental = {} （空）
```

即 `etf_fundamental` ⊃ `etf_technical`，多出来的正好是这 2 只脏数据。

### 影响

- 任何直接 `SELECT COUNT(DISTINCT code) FROM etf_fundamental` 当作"ETF 覆盖数"的统计都会虚高 2 只（这正是交接文档一度把覆盖写成 25 的来源）。
- 若有分析逻辑遍历 `etf_fundamental` 做横向对比（如估值/规模排序），这 2 只场外基金会作为不可比标的混入结果。

### ✅ 已修复（2026-09-03，随提交落地）

1. **写入来源已定位且确认安全**：`etf_fundamental` 的写入函数是 `src/data_sources/etf_fundamental.py:445` 的 `save_to_db(conn, "etf_fundamental", ...)`，包裹在 `run_etf_fundamental_collection` 内；该函数**仅被 `scripts/verify_sina_whitelist.py` 调用，不在主采集链**（`build_base` / `run_analysis` 不碰它）。因此历史脏数据是早期一次性写入，不会在后续例行采集中被重新写回——可直接清库。
2. **已执行清理**（生产库 `data/database/portfolio.db`，非 git 跟踪；已备份验证）：
   ```sql
   DELETE FROM etf_fundamental WHERE code IN ('001323','002152');  -- 各 19 行，共 38 行
   ```
   distinct `code` 由 **25 → 23**，与 `etf_technical` / `config.ETF_CATEGORIES` 对齐。复查 `fund - tech == set()`（空集）。
3. 采集期过滤：写入前已可用修好的 `_is_etf`（见问题一）过一遍，杜绝再次混入。

---

## 附：ETF 数量口径速查（防止再次数错）

| 数字 | 含义 | 出处 | 可否作分母 |
|------|------|------|-----------|
| **36** | 全历史全部标的 = 23 场内 ETF + 13 场外标的 | `portfolio_snapshots` distinct `code` | ✅ 全标的口径 |
| **23** | 场内 ETF 全集 | `etf_technical` distinct `code`，实测 == `config/settings.py` 的 `ETF_CATEGORIES` | ✅ **推荐的 ETF 权威分母** |
| **22** | 预测底座 / 风险模型覆盖 | `etf_features`、`etf_price_history` | ✅ 模型覆盖口径（= 23 − 已清仓 `159732`） |
| **23** | `etf_fundamental` 标的数 | 2026-09-03 已清 `001323`/`002152` 脏数据，现与 `etf_technical` 对齐 | ✅ 可用 |
| **25** | 用代码正则判出的 ETF 数 | 含 2 只假阳性 `166301`/`519770` | ❌ **不可用**（与上一行数值相同但集合不同，纯属巧合） |
| **59** | `(code, name)` 去重对数 | 同一代码有简称/全称两种写法 | ❌ 不是标的数 |

> **59 的成因示例**：`159220` 在 `portfolio_snapshots` 中同时存在"港股通红利低波ETF华宝"与"港红利"两种 `name`。按 `(code,name)` 去重会把同一只 ETF 数成 2 只。历史上曾据此误报"48 只 ETF"，实为此类假象。

### 场外标的清单（13 只，供核对）

`001194` 景顺长城稳健回报灵活配置混合A、`001323` 东吴移动互联混合A、`001407` 景顺长城稳健回报灵活配置混合C、`001437` 易方达瑞享灵活配置混合I、`001765` 前海开源嘉鑫混合A类、`002152` 华宝核心优势混合、`007994` 华夏中证500指数增强A、`008269` 大成睿享混合A、`027293` 东吴产业趋势混合A、`100032` 富国中证红利指数增强前端、`880013` 天添利（现金管理类）、`166301` 华商新趋势优选混合（LOF）、`519770` 交银优择回报混合A。

---

## 复核脚本

以下脚本可随时重跑以验证本文档结论（**只读，不写库**）：

```python
import sqlite3, re
con = sqlite3.connect('file:data/database/portfolio.db?mode=ro', uri=True)
cur = con.cursor()
RE = re.compile(r"^(5\d{5}|1[56]\d{4})$")

snap = dict(cur.execute("SELECT code, MAX(name) FROM portfolio_snapshots GROUP BY code").fetchall())
tech = {r[0] for r in cur.execute("SELECT DISTINCT code FROM etf_technical")}
fund = {r[0] for r in cur.execute("SELECT DISTINCT code FROM etf_fundamental")}
feat = {r[0] for r in cur.execute("SELECT DISTINCT code FROM etf_features")}

regex_set = {c for c in snap if RE.match(c)}
name_set  = {c for c in snap if 'ETF' in (snap[c] or '').upper()}

assert len(snap) == 36
assert len(tech) == 23
assert regex_set - tech == {'166301', '519770'}   # 正则假阳性
assert tech - name_set == {'512810'}              # 名称判据假阴性
# 修复后：etf_fundamental 已清脏数据，fund - tech 应为空集
assert fund - tech == set()                         # etf_fundamental 脏数据已清理（2026-09-03）
assert tech - feat == {'159732'}                  # 已清仓，故预测底座少 1
print("全部结论复核通过")
```

---

## 优先级判定

| 问题 | 严重度 | 紧急度 | 理由 |
|------|--------|--------|------|
| 一 · `_is_etf` 假阳性 | 中 | 低 | 当前无实际影响（两只已清仓），但属**埋雷**：一旦买入同类场外基金即触发采集失败 + 训练集污染。建议随下次 predictor 模块改动一并修掉。 |
| 二 · `etf_fundamental` 脏数据 | 低 | 低 | 仅影响统计口径，不影响运行。**必须先定位写入来源再清库**，否则会被采集重新写回。 |
| 三 · `portfolio_nav.total_units` 错位 | 中 | 中 | 字段 100% 错误，当前无下游读取故影响为零，但任何新消费方都会踩坑（见下）。 |
| 四 · `portfolio_nav.mwr_return` 与 TWR 矛盾 | 中 | 中 | **用户可见**：该值已进入 `load_portfolio_nav()` 返回值，展示出来是 −73.5%。 |

前两者均**不构成上线/交接阻断项**，但应写入待办，不要遗忘。

---

## 问题三：`portfolio_nav.total_units` 存的是「前一个交易日的市值」

> 记录时间 2026-09-15 · 发现人 data-engineer · **状态：仅记录，未修复**
> 本轮同时在做「场外净值写入 portfolio_snapshots」，为避免事后分不清是哪个改动引起的，本 bug 不在本轮修复。

### 复现查询

```sql
SELECT date, total_units, total_value,
       LAG(total_value) OVER (ORDER BY date) AS prev_day_total_value
FROM portfolio_nav ORDER BY date DESC LIMIT 15;
```

| date | total_units | 当日 total_value | 前一日 total_value | 是否相等 |
|---|---|---|---|---|
| 2026-09-14 | 933294.5 | 933891.5 | 933294.5 | ✅ |
| 2026-09-11 | 945046.1 | 933294.5 | 945046.1 | ✅ |
| 2026-09-10 | 950179.2 | 945046.1 | 950179.2 | ✅ |
| 2026-09-09 | 950728.9 | 950179.2 | 950728.9 | ✅ |

```sql
-- 全表命中率
SELECT COUNT(*) FROM (
  SELECT total_units, LAG(total_value) OVER (ORDER BY date) pv FROM portfolio_nav
) WHERE pv IS NOT NULL AND ABS(total_units - pv) < 0.01;
-- => 3476   （全表 3477 行，唯一未命中的是首行，因为它没有前值）
```

**结论：3476 / 3477 = 99.97% 命中，是 100% 系统性错位，不是偶发。**

### 根因（源码行号）

`src/analysis/nav_engine.py:222-234`：

```python
rows.append({
    "date": d_str,
    "unit_nav": round(unit_nav, 6),
    "total_units": round(prev_v, 2),      # ← BUG：prev_v 是上一轮的 v
    "total_value": round(v, 2),
    ...
})
prev_v = v                                # ← 在 append 之后才推进
```

`prev_v` 在 `append` **之后**才被赋成当日 `v`，因此写入本行的 `total_units` 恒等于**前一个交易日**的 `total_value`。

### 影响范围（已核实）

- **当前影响 = 0**：全项目 grep `total_units` 只有 3 处，全是写入侧
  （`db_schema.py:448` 建表、`nav_engine.py:226` 赋值、`nav_engine.py:249/266` 建表与 INSERT）。
  `nav_engine.py:294` `load_portfolio_nav()` 的 SELECT 列里**没有** `total_units`，即无任何下游读取。
- **字段语义**（`db_schema.py:448` 注释）：`总份额（无申赎则恒定=初始市值）`。
  正确值应为 `total_value / unit_nav`；现状是滞后一天的市值，语义完全不对。
- **未受污染的部分**：`unit_nav` 是独立计算的（`nav_engine.py:221` `unit_nav = prev_nav * (1 + r_total)`），
  并非 `total_value / total_units`。实测 09-14：`total_value/total_units = 1.0006`，而 `unit_nav = 2.5247`，
  两者无关 —— **所以 unit_nav 序列没有被这个 bug 污染**，不必担心 NAV 曲线滞后一天。

### 建议修法（待排期）

```python
"total_units": round(v / unit_nav, 2) if unit_nav else None,   # nav_engine.py:226
```
改后需全量 `rebuild_portfolio_nav()` 重刷，并补一条断言：`total_units * unit_nav ≈ total_value`。

---

## 问题四：`portfolio_nav.mwr_return` —— MWR 当前不可用（IRR 求解器发散）

> 记录时间 2026-09-15 · 发现人 data-engineer
> **状态：MWR 当前不可用 —— 求解器发散待修；护栏已加、坏值已清（2026-09-15）**
> 结论一句话：**这个字段现在不要用**，值不可信，且每次重建都会变。

### 复现查询

```sql
SELECT mwr_return, COUNT(*) FROM portfolio_nav GROUP BY mwr_return;
-- 2026-09-15 重建前: -0.735121 | 3477
-- 2026-09-15 重建后: 10.444292 | 3477     ← 值变了，但「全表同值」的性质没变
```

### 需要修正的两处误判

**其一**：初判为「从 2026-08-27 起恒为死值」，实测**不是区间问题，是全表问题**——
全表 3477 行（2012-05-28 ~ 2026-09-14）永远是同一个值。

**其二**：这个值**不是固定常数，每次 `rebuild_portfolio_nav()` 都会重算并改变**。
2026-09-15 因场外净值补齐触发重建，`mwr_return` 由 **−0.735121 变为 10.444292**（即 +1044%）。
所以不要把它当成「死值」写进任何断言。

另外「每行相同」本身**不是 bug，是设计**：`nav_engine.py:237-241` 注释写得很清楚——
「全周期 MWR，写入每行便于任意行读取」，即把算出来的全周期 IRR 广播到每一行。

### 真正的 bug：IRR 结果与 TWR 差了一个数量级

| 时点 | `unit_nav`（09-14） | 累计 TWR | `mwr_return` | 折算 |
|---|---|---|---|---|
| 重建前 | 2.524669 | **+152.47%** | −0.735121 | −73.51% |
| 重建后 | 2.626177 | **+162.62%** | 10.444292 | **+1044.43%** |

同一个组合、同一段现金流，TWR 与 MWR 应同号且量级相近。
重建前是 +152% vs −73.5%（差 225pp），重建后是 +162% vs +1044%（差 882pp）——
**两次都错，且重建后更离谱**。说明 `_solve_period_irr(df, cf, div)`（`nav_engine.py:238`）
的解根本不可信，而不是「某次算错了」。
可能原因：现金流符号/日期对齐错误，或 IRR 在无实根/多根时返回了边界值。

### 影响范围（已核实）

- `mwr_return` **有下游读取**：`nav_engine.py:294` `load_portfolio_nav()` 的 SELECT 含该列
  （列清单：`date, unit_nav, total_value, net_flow, twr_cumulative, mwr_return, is_suspect`），
  会被基准对比 / 回撤 / 归因等下游消费 → **用户可见的错误数字**。
- 建议：修复前先在展示侧隐藏该字段，避免 −73.5% 被当成真实收益展示。

### 已做的处置（2026-09-15）

按「先不产生错误数字、再不展示错误数字、最后才修算法」的顺序：

1. **护栏**：`src/analysis/nav_engine.py` 新增 `_MWR_MIN = -0.99` / `_MWR_MAX = 5.0`，
   在 `_solve_period_irr()` 内用 `_guard()` 包裹所有返回路径。
   解非有限值或越界 → 返回 `None` + `logger.warning` 打印输入摘要
   （现金笔数、净流入、首末笔、分红合计、期初期末市值）。
   同时给原本静默的「无实根」分支补了 warning。
   `rebuild_portfolio_nav()` 里 `if mwr is not None` 的分支不变，
   因此返回 `None` 时各行 `mwr_return` 自然写入 `NULL`。

2. **清坏值**：`scripts/purge_bad_mwr.py`（新建，判据与 `_MWR_MIN/_MWR_MAX` 同源）。
   已执行：`UPDATE portfolio_nav SET mwr_return = NULL WHERE ... 越界`，
   **清理 3477 行**，现全表 `mwr_return IS NULL`。

3. **展示侧容错**：`get_nav_series()`（原 `load_portfolio_nav` 位置的读取函数）
   docstring 已注明 `mwr_return` 可能为 NULL/NaN，下游必须显示"暂不可用"，
   **不要**渲染成 nan / None / 0，也不要用 0 参与运算。
   ⚠️ 核实结论：全项目 grep `mwr_return` 只有 `nav_engine.py` 内部 +
   `db_schema.py` 建表 + 本文件，**当前没有任何展示层调用 `get_nav_series()`**，
   所以暂无实际渲染点需要改。未来新增消费方时请回读本条。

4. **端到端验证**：重新 `rebuild_portfolio_nav()` 后确认
   `越界坏值行数 = 0`、`mwr_return IS NULL = 3477/3477`，
   且护栏 warning 正常打印，其余字段（unit_nav 2.626177 / TWR +162.62% / is_suspect 0）不受影响。

### 为什么区间取 [-0.99, 5.0] 而不是更宽

不要用 `mwr < 10` 这类宽区间：+1044% 能被拦，但 +900% 同样会被放行，
而那依然是错的。取 +500% 作为上界，已远超任何合理组合收益，
足以把发散解挡住，又不至于误伤正常值。

### 修法（待排期，属另一个专项）

**本轮不修 IRR 算法本身。** 排查线索（护栏 warning 打印的输入摘要，2026-09-15）：

```
期间 2012-05-28~2026-09-14 (3477 天), 现金流 380 笔, 净流入 1,109,683.60,
首笔 29,000.37@2012-05-29, 末笔 -8,000.00, 分红合计 4,903.79,
期初市值 10,416.00, 期末市值 1,505,090.13   →  解出 10.444292
```

两个可疑点，供后续专项参考：

- **时间权重口径**：`_solve_period_irr` 里 `frac = (t - i) / t`（`t` 为总天数），
  折现因子是 `(1 + r) ** frac`，这把 `r` 定义成**整个持有期的收益率**而非年化。
  但字段名与注释都指向"全周期资金加权收益"，语义与用法是否对齐需先确认。
- **现金流符号**：`dep = -float(cf.get(...))`，净流入 110 万而期末市值仅 150 万、
  期初 1 万，直觉上全期收益应在数十个百分点量级，解出 +1044% 需核对
  `trade_records` 里是否存在符号或口径（如分红、转存）错记。

建议步骤：
1. 先给 `_solve_period_irr()` 补单元测试：构造已知 IRR 的现金流，断言输出。
2. 确认 `frac` 的时间口径（全期 vs 年化）与字段语义一致。
3. 抽样核对 `trade_records` 中若干笔大额的 `change_amount` 符号。
4. 修好后重刷 `portfolio_nav`，并同步更新 README / 日报口径说明。

---

## 问题五：TWR 序列是「历史段场内主导 + 近期全组合」的拼接口径

> 记录时间 2026-09-15 · 发现人 data-engineer · **状态：刻意保留，不修**
> 本条不是 bug，是**口径限制**。读 `portfolio_nav` / `portfolio_summary` 前必读，
> 否则会把不同口径的收益串在一起比较。

### 背景

2026-09-15 用 `scripts/fetch_otc_fund_nav.py` 补齐了场外 13 只基金 2026-08-03 ~ 2026-09-14
的日频净值快照：**共 378 行**（12 只普通场外 347 行 + 货币型 `880013` 31 行，口径见问题六）。
此前场外基金**只有月末快照**，详见本文「场外标的清单」。

### 口径现状

| 时间区间 | 场外 13 只覆盖 | daily_return 实际由谁贡献 |
|---|---|---|
| 2012-05-28 ~ 2026-01-30 | 无 | 场内 |
| 2026-01-31 ~ 2026-07-30 | **仅月末那一天** | 月末日含场外，其余日仍只有场内 |
| 2026-08-03 ~ 2026-09-14 | **日频**（本次补齐） | 全组合（22 场内 + 13 场外） |
| 2026-09-15 起 | 日频（若采集器接入日常链路） | 全组合 |

**即：修正后的 TWR 是一段拼接序列，不是全周期一致口径。**
07-31 及之前，绝大多数交易日的收益只反映场内 22 只（约占总市值 62%）；
08-03 起才反映全组合。跨过 2026-08-03 做区间收益对比时，口径发生了切换。

### 为什么不做全历史回补

**不要**顺手把场外历史也全量回补成日频。原因：

- 场外基金的历史**份额变动不可考**。`portfolio_snapshots` 只在月末那一天有记录，
  中间的申购/赎回无从还原。
- 用「当前份额 × 历史净值」倒推历史市值，会凭空造出从未持有过的资产，
  引入的错误比现在更大。
- 现有月末快照里的 `quantity` 是当天的真实份额，只有那一天的市值是可靠的。

### 若要彻底一致，需要什么

1. 拿到场外 13 只的**完整申赎流水**（券商/基金App 导出的对账单），按日重建份额；
2. 或用 `trade_records` 反推（当前表内是否覆盖场外申赎需另行核实）；
3. 之后才能做全历史日频回补并重刷 `portfolio_nav`。

**在此之前，任何跨 2026-08-03 的收益对比都必须注明口径切换。**

---

## 问题六：`880013` 净值取数 —— 已解决，按「货币基金份额增长」口径计价

> 记录时间 2026-09-15 · 发现人 / 处置人 data-engineer
> **状态：已解决（有可用源）。`NO_NAV_SOURCE_CODES` 标记未添加 —— 因为源是通的。**

### 背景

场外 13 只里，`880013` 是唯一一只长期停在 **2026-07-31**（45 天未更新）的标的。
此前评估结论是「无净值源，跳过」，陈旧估值金额 9,443.09 元。

### 为什么此前判定取不到

`880013` = **招商资管智远天添利货币**，类型 **货币型-普通货币**
（用 `ak.fund_name_em()` 可查到，属公募目录内）。
它是**券商资管现金管理类产品**，不是标准开放式净值型基金，所以：

| 试过的接口 | 结果 |
|---|---|
| `ak.fund_open_fund_info_em(symbol="880013")` | ❌ `JSEvalException: Data_netWorthTrend is not defined` —— 页面里根本没有单位净值曲线 |
| `ak.fund_money_fund_info_em(symbol="880013")` | ❌ `ValueError: Expected axis has 14 elements, new values have 13` —— **不是没数据**，是 akshare 把返回列名硬编码成 14 个，货币型接口只回 13 列，赋值时对不上 |
| `ak.fund_name_em()` | ✅ 能查到名称与类型，确认是货币型 |
| 直连 `api.fund.eastmoney.com/f10/lsjz`（`FSRQ`/`DWJZ`/`LJJZ`） | ✅ **成功**，`TotalCount=1462`，覆盖 2026-04-08 ~ 2026-09-14，含 每万份收益 / 7 日年化（≈0.70%） |

**根因**：akshare 的封装层问题，不是数据源缺失。绕开 akshare 直连东方财富原始
JSON 接口即可拿到数据。

### 当前按什么口径计价

货币型基金**单位净值恒为 1.0**，收益不体现在净值上，而体现在**份额增长**：

```
每万份收益 X 元  ⇒  份额 ×= (1 + X / 10000)
market_value = 份额 × 1.0
成本总额固定     = 份额₀ × 成本单价（成本单价不随收益变）
pnl            = market_value − 成本总额
```

即：价格列恒 `1.0`，市值增长完全由 `quantity` 列承载。
这与普通场外基金（净值变、份额不变）**是两套相反的口径**，
做横向汇总或写校验断言时不要混用。

### 落地结果

- 写入 **31 行**（2026-08-03 ~ 2026-09-14，只在 `portfolio_snapshots` 已有交易日写入，
  不凭空造交易日）。
- 份额 9,443.09 → **9,451.16**，区间累计收益 **8.07 元**（约 0.085%，与 7 日年化 0.70% 量级一致）。
- 零改写校验（对照写入前备份）：36,108 → 36,139 行，**旧行被修改 0，旧行丢失 0**。
- 陈旧估值金额：**9,443.09 → 0.00**（13 只场外全部更新到 09-11 ~ 09-14）。
- 占比：9,451.16 / 1,514,541.29 = **0.624%**（2026-09-14 组合总市值口径）。

### 代码位置

`scripts/fetch_otc_fund_nav.py`：

- `MONEY_FUND_CODES = {"880013"}` / `MONEY_FUND_NAV = 1.0`
- `fetch_money_fund_yield(code, max_pages)` —— 直连东方财富，绕开 akshare 14 列硬编码
- `build_money_fund_rows(...)` —— 按上述份额增长口径生成行

`run_otc_nav()` 与 CLI `main()` **两条路径都已接上货币型分支**（此前只接了一条，
导致 dry-run 仍失败，已修）。

日常链路：`run_analysis.py:79` `run_stage0_otc_nav()` 在阶段一之前调用 `run_otc_nav()`，
所以 `880013` 会随每日跑批自动续更，不是一次性手工补数。
（`run_analysis.py:94` 原注释「880013 无公开净值源，按设计跳过」已同步更正。）

### 兜底约定（重要）

若东方财富直连接口日后再次失效，采集器会 `logger.warning` 并**跳过该代码**，
**绝不硬编码净值、也绝不沿用上一次的旧值**。届时：

- 该标的按**成本计价**；
- 日报 / 展示必须显式标注「**净值不可用，按成本计价**」，不能静默复用 45 天前的数字。

是否需要为此在 `config/settings.py` 的 `SNAPSHOT_STALE_DAYS_OVERRIDE` 旁新增
`NO_NAV_SOURCE_CODES`（语义「无净值源，按现金等价物处理，豁免陈旧告警」）——
**当前不需要**：源是通的，`880013` 已能日频更新到 09-14，天然不触发陈旧告警。
等真正出现取不到的那天再加，避免把一个活着的源提前标成死的。

> 展示层怎么处理这只标的（价格恒 1.0 会被当成"没涨"），见下一节
> **「附：货币型场外基金的展示口径」**。

---

## 附：货币型场外基金的展示口径

> 记录时间 2026-09-15 · **只约束展示层，不改代码、不改数据口径**
> 适用范围：`880013`（招商资管智远天添利，货币型-普通货币），
> 以及将来任何新增进 `MONEY_FUND_CODES` 的货币型标的。

### 数据特征

| 字段 | 普通场外 | 货币型（880013） |
|---|---|---|
| `current_price` | 净值，逐日变动 | **恒为 1.0** |
| `quantity` | 不变（无申赎时） | **单调增长**（收益转份额） |
| `market_value` | 份额 × 净值 | 份额 × 1.0，**数值上等于 quantity** |
| 收益体现 | 价格上涨 | **份额增长** |

即：货币基金不是"不涨"，是**涨在份额上而不是价格上**。

### 展示要求（必须）

1. **必须标注**「**货币基金 · 净值 1.000 · 收益计入份额**」。
2. **不要只渲染 `current_price`** —— 它恒为 `1.000`，用户看到会以为这只标的一个月没动。
3. **收益/涨跌幅取 `pnl_rate`**（= `pnl` / 固定成本总额），**不要**用
   `(current_price − cost_price) / cost_price` —— 后者对货币型恒为 0，
   会把每天的小额正收益显示成"平盘"。
4. 若某组件按**价格涨跌幅**排序 / 着色 / 打标签，880013 会恒为 0%，
   永远落入"平盘"分组，既不涨也不跌 —— **实际它是每天小幅正收益**。
   这类组件应对货币型走市值变动（`market_value` 日差）或直接跳过。

### 两个数值陷阱

- **市值与份额数值相同**：09-14 的 `market_value = 9,451.16 元`、`quantity = 9,451.16 份`，
  两者数字一样但单位不同（元 vs 份）。别因为"看起来相等"就互相代用或当成校验通过。
- **别写「quantity 恒定」的断言**：普通场外无申赎时份额不变，这条断言成立；
  但对货币型**必然误报**——它的份额每天增。校验脚本需按 `MONEY_FUND_CODES` 分支处理。
  而 `market_value == quantity × current_price` 这条**仍然成立**（乘以 1.0），不要据此认为口径一致。

### 源失效时的兜底文案

若东方财富直连源失效、采集器跳过该标的（按成本计价），展示文案改为：

> **净值不可用，按成本计价**

**不要**静默沿用上一次的旧值——那会让它看起来还在正常更新。

---

## 问题七：`etf_predictions` 表已封存 —— 主动封存，不要再补

> 记录时间 2026-09-15 · 记录人 data-engineer
> **状态：✅ 主动封存。本表自 2026-08-19 起不再产出新行；表内 5 个 model 全部已 VETO 下线，
> 不可用于任何决策或展示。**

### ⚠️ 先区分：这是「主动封存」，不是「数据待补」

本轮同时处理了两类停在 8 月的表，**性质完全相反，不要混为一谈**：

| 表 | 停在 8 月的原因 | 处置 | 现状 |
|---|---|---|---|
| `portfolio_snapshots` / `portfolio_summary` / `portfolio_nav` | **被动停更**（管线断了 / 场外无日频源） | **已补齐并接入日常管线** | 已到 **2026-09-14** |
| `etf_predictions` | **主动封存**（5 个模型全部 VETO，产出即错） | **不再补，也不要补** | 停在 2026-08-19，保持原样 |

两张情况都跟「2026-08-19 前后」这个日期沾边，极易被误判成同一件事。
**看到 `etf_predictions` 停在 08-19，正确反应是「这是对的」，不是「又要补数据」。**

### 表内现状（2026-09-15 实测，只读）

```
总行数 286，只有 2 个日期：2026-08-18（66 行）、2026-08-19（220 行）
```

| model | 行数 | 日期 | forward_window | 预测目标 | 状态 |
|---|---|---|---|---|---|
| `tier0_signal_ensemble` | 66 | 2026-08-18 | 5 / 20 / 60（各 22） | 方向 | ❌ VETO |
| `lgb` | 66 | 2026-08-19 | 5 / 20 / 60（各 22） | 方向 | ❌ VETO |
| `ridge` | 66 | 2026-08-19 | 5 / 20 / 60（各 22） | 方向 | ❌ VETO |
| `risk_lgb` | 44 | 2026-08-19 | 20 / 60（各 22） | 波动率 | ❌ VETO |
| `risk_lgb_v2` | 44 | 2026-08-19 | 20 / 60（各 22） | 波动率 | ❌ VETO |

**5 个 model 全部 VETO，表内没有任何一行可用于决策或展示。**

### 封存依据（已定稿的结论，勿再翻案）

1. **方向模型（Tier0 / lgb / ridge）**：walk-forward 方向命中率 **46–50%**，与抛硬币无异；
   Tier1 样本外截面 **IC = 0.006**，远低于否决线 `VETO_IC = 0.02`
   （`src/analysis/predictor/models.py:43`、判据在 `models.py:191-200` 的 `_verdict()`）。
   同源结论亦见 `src/analysis/etf_position.py:6`：
   「项目已用 walk-forward 证明 ETF 短期方向不可测（Tier1 VETO, IC<0.02）」。
2. **波动率模型（risk_lgb / risk_lgb_v2）**：修正标签（窗口 `[t+1..t+n]`，消除未来函数）后，
   样本外截面 Spearman IC **0.613** 未跑赢零成本的 `vol_20d` 基线 **0.743**，
   ΔIC 的 **HAC t = −4.51**，3 窗口 × 2 模型 **6/6 全部 VETO**（BH-FDR q < 0.0001）。
   原文见 `models.py:363-368` 的 `_DEPRECATED_RISK_MSG`。

替代口径：波动率直接读 `etf_features.vol_20d / vol_60d`，
见 `tabs/tab16_risk_outlook.py`、`src/utils/risk_report.py`、`src/analysis/rebalance_engine.py:664`。

### 代码护栏：别顺手把它跑起来

`src/analysis/predictor/models.py:372-379`：

```python
def _guard_deprecated(name: str, allow: bool) -> None:
    """已下线函数的运行时护栏：防止有人顺手 import 后重写 etf_predictions。"""
    if allow:
        return
    raise NotImplementedError(
        f"{name} 已停用（能力已移除），禁止调用。{_DEPRECATED_RISK_MSG}"
        " 确需重跑审计时显式传 allow_deprecated=True。"
    )
```

`run_risk_prediction()`（:382）与 `run_risk_predict_latest()`（:442）首行即调用
`_guard_deprecated(...)`，**默认 `allow_deprecated=False` → 直接抛 `NotImplementedError`**。
只有显式传 `allow_deprecated=True` 才会执行（会重写已封存的 `risk_lgb` 44 行，仅用于重跑审计）。

### 为什么它不会自己长出新行

`run_tier0()`（`tier0.py:102`）与 `run_tier1()`（`models.py:230`）**在生产链路中零调用方**——
全项目 grep 只有 `models.py:591` 的手动入口与 `tests/test_predictor_models.py` 引用。
所以 08-19 之后没有新行是**预期行为**，不是管线故障。

### 使用约定

- **不要**读本表做任何择时、调仓、加仓减仓判断。
- **不要**在日报 / 看板 / Tab 里展示本表的 `direction` / `score` / `confidence`。
- **不要**为了「把数据补到 09-14」而重跑 `run_tier0` / `run_tier1` / `run_risk_prediction`
  —— 补出来的行是已知错误的产出。
- 确需审计重跑时，显式传 `allow_deprecated=True` 并**先备份数据库**。
- 历史 286 行**保留不删**，作为「当时做过什么、为什么否决」的留档
  （`scripts/verify_risk_downgrade.py` 会校验 `risk_lgb` 44 行仍在）。

---

---

## 问题八：`etf_technical` 是**三套口径**混在一张表里（含「日期与 K 线末日错位一天」）

> 记录时间 2026-09-15 · 发现人 data-engineer
> **状态：仅记录，未修。** 本条是给「159732 观察名单采集」做的副产品核查，
> 结论适用于**全部标的**，不只对 159732。

### 三套口径分别是什么

| # | 口径 | 写入方 | 价格来源 | 典型特征 |
|---|---|---|---|---|
| ① | backfill 口径 | `scripts/backfill/backfill_full_history.py:498` `rebuild_etf_technical()` | `portfolio_snapshots.current_price` | `trend` ∈ {强势上涨, 温和上涨, **下跌**, 震荡整理}；`kdj_signal` 只有 金叉/死叉 |
| ② | 日频 TechnicalAnalyzer 口径 | `src/analysis/portfolio.py:234` `save_technical_indicators()` | `ds_manager.get_kline(count=40)` | `trend` 含 **强势下跌 / 温和下跌**；`kdj_signal` 含 中性 |
| ③ | 观察名单口径（本次新增） | `scripts/fetch_watchlist_quotes.py` | `etf_price_history` | 同 ② 的算法，但 K 线末日 = 行日期（见下） |

实测各口径占比（2026-09-15）：

| 标的 | 总行数 | ① backfill | ② TechnicalAnalyzer | 切换点 |
|---|---|---|---|---|
| 512010 | 2676 | 2649 | 27 | 2026-05-18 |
| 512810 | 2421 | 2388 | 33 | 2026-05-22 |
| 159732 | 1206 | 1179 | 27 | 2026-07-16 |

**即：把 `etf_technical` 当一条连续时间序列来读，是错的。** 跨切换点比较
`rsi_value` / `bollinger_position` 时，算法和输入价格都换过。

### 关键缺陷：日期与 K 线末日错位一个交易日

`portfolio.py:234` 用 `self.today` 当行日期，但 K 线取的是**当时能拿到的最近 40 根**。
跑批时点若早于行情源更新，K 线末日就是**上一交易日**，于是：

> **行上写的日期是 D，指标实际算的是截至 D−1 的 K 线。**

实测（159732，用库内行情按各日 K 线末日重算 RSI，与存量行逐一对位）：

| K 线末日 | 重算 RSI | 存到哪个日期 | 错位 |
|---|---|---|---|
| 2026-07-24 | 33.19 | **07-27** | +1 交易日 |
| 2026-07-28 | 34.04 | **07-29** | +1 交易日（07-28 那行是同日，不一致） |
| 2026-07-29 | 25.14 | **07-30** | +1 交易日 |
| 2026-07-30 | 25.85 | **07-31** | +1 交易日 |
| 2026-07-31 | 33.17 | （清仓后停更，无 08-03 行） | — |

且**不是每天都错**：07-28 那行是同日，07-29 又是滞后一天——
取决于当天跑批时点行情源是否已更新。所以这是**间歇性**错位，不是固定偏移，
无法用一个统一 offset 校正。

### 为什么本次的新行不沿用这个错位

`fetch_watchlist_quotes.py` 取「K 线末日 = 行日期」，即**行上写 D 就是算到 D**。
这是正确语义，但意味着 159732 的序列在 2026-08-03 处从「滞后一天」切成「同日」。
选择正确口径而不是迁就存量 bug —— 若要严格连续，得把三套口径全量重刷（见下）。

### 影响

- 技术面展示（Tab / 日报）拿 `etf_technical` 画 RSI / KDJ 曲线时，
  最近若干日的点比真实晚一天；遇到大涨大跌日会看出"指标慢半拍"。
- 任何用 `etf_technical` 做的回测 / 信号统计，日期与指标不对齐 = 变相引入未来函数
  的**反面**（信息滞后），会低估信号效果。

### 修法（待排期）

1. 先统一口径：让日频写入也走 `etf_price_history`（已验证与 `ds_manager.get_kline`
   价格**逐字段完全一致**，40 天重叠期 0 处不一致），并把行日期改为
   **K 线末日**而不是 `self.today`；
2. 再用统一算法全量 `rebuild_etf_technical()` 重刷历史，消除 ①/② 算法差异；
3. 补断言：`etf_technical.date` == 该行指标所用 K 线的最后一根日期。

**在此之前，不要把 `etf_technical` 当连续序列跨 2026-05-18 前后比较。**

---

## 优先级判定（补充）

| 问题 | 严重度 | 紧急度 | 理由 |
|------|--------|--------|------|
| 六 · `880013` 无净值源 | ~~中~~ | ~~中~~ | **已解决**（2026-09-15）。有可用源，按货币型份额增长口径日频写入；陈旧金额归零。仅余「源失效时的兜底口径」需遵守。 |
| 七 · `etf_predictions` 封存 | 低（已处置） | 低 | 5 个 model 全部 VETO，产出即错。已加 `_guard_deprecated` 运行时护栏 + 生产链路零调用方。**风险在于被误当成「待补数据」而重新跑起来**，故在此显式标注。 |
| 八 · `etf_technical` 三套口径混用 | 中 | 中 | ①/② 两套算法 + 输入价格都不同，日期还比 K 线末日**晚一天**（间歇性，无法用统一 offset 校）。影响所有技术面展示与信号统计。修要全量重刷，属独立专项。 |
| 八 · `excel_report` 中文 fallback | ~~中~~ → **低** | 低 | 2026-09-15 复核：风险低于此前判断，**暂不改动**。详见问题八。 |

---

## 问题八：`excel_report.py` 全英文键无中文 fallback —— 复核后降级，暂不改

### 原判定
交接评估列为「与 P0-6 同类风险」：`src/report/excel_report.py` 有 57 处英文键取值点
（去重 34 个键），**0 处中文 fallback**，建议补上。

### 2026-09-15 复核结论：风险显著低于原判断，暂不改动

关键在于**数据源性质不同**，这一点原评估没有区分：

| | P0-6 出问题的地方 | excel_report |
|---|---|---|
| 数据来源 | `smart_report.py` **解析 Markdown 的产出** | **直接读 DB 表**（`portfolio_summary` / `etf_technical` / `portfolio_snapshots`） |
| 键名从哪来 | 正则 `### \d+\.\s+\[(高\|中\|低)\]` 抓出的**中文单字** | **DB 列名**，`total_value` / `sharpe_ratio` / `max_drawdown` … |
| 会变吗 | 会——改模板格式就会变 | 不会——列名是 schema 契约，改列要动 `db_schema.py` |

P0-6 的中英文键冲突是**解析产物 vs 展示表**的口径差；`excel_report` 的键名直接来自
DB 列名，不存在"上游某天突然改成中文键"的路径。实测文件内 0 个中文键取值点，
与 DB 列名完全一致，当前无实际故障。

### 为什么不顺手改掉
改 57 处取值点是纯机械替换，会把"当前正确"的代码批量改一遍，引入回归风险，
却换不来任何当下的正确性收益。**没有故障的防御性改动，性价比不成立。**

### 什么情况下必须回来改（触发条件）
若 `excel_report` 的数据入口从「直接读 DB」变为「消费上游解析产物」
（例如改为接收 `smart_report` 产出的 dict，或接第三方导出），
则键名不再由 schema 保证，**此时必须补中文 fallback**，并复用
`src/utils/enhanced_report.py` 的 `_pick()` / `_norm_priority()` 那套别名机制，不要另写一套。

### 若真要改，正确姿势
不要改 57 处。在 `_load_all_data()`（`excel_report.py:83`，统一数据入口）返回前
做一次键名归一即可——一处改动覆盖全部下游。

---

## 问题九：push2his 会阻尼式拒绝 —— **不是代理问题**（2026-09-15 实测定论）

> 注：本文档存在两个「问题八」（`etf_technical` 三套口径、`excel_report` 全英文键），
> 为历史编号失误，暂不重排以免打乱既有引用。本节顺延为「问题九」。

### 此前误判
早期记录（见 `docs/自动化采集完善方案.md` 的 D3 项）写的是
「主源 EM 被代理墙挡：`push2his.eastmoney.com` 返回 ProxyError」。
**这个结论是错的**，按它去查代理配置会查不到东西。

### 实测真相（2026-09-15）

| 检查项 | 结果 |
|---|---|
| 模块是否已绕代理 | **是**。`fund_flow.py:18-19` 弹出全部 `*_proxy` 环境变量；`:62-66` 把 `requests.Session.__init__` 改成默认 `trust_env=False`；`_urllib_get_json` 用 `ProxyHandler({})`。实测 `requests.Session().trust_env == False` |
| 绕代理后能否连通 | **能**。`curl --noproxy '*'` 直连 `push2his` 返回 200；`check_push2his_available()` 返回 `True` |
| 那为什么还失败 | push2his 对**高频直连**做阻尼：约 20~40% 的请求被掐断或返回 `data: null` |
| 失败形态 | 两种：① TLS/HTTP 层 `RemoteDisconnected`（`urllib` 与 `requests` 都会遇到）；② HTTP 200 但 JSON 里 `data: null`，akshare 解包时抛 `'NoneType' object is not subscriptable` |
| 是否与标的有关 | **无关**。同一标的连续 4 次重试仍可能全部 `data: null`，换一只立刻成功；反之亦然。纯粹是请求频率函数 |

### 真正的代码缺陷（已修）

1. **`_urllib_get_json` 的 except 类型写错**（`de6b312` / `61976ba` 两轮
   "fine-grained exception handling" 重构误收窄）：
   写成 `except sqlite3.OperationalError` —— HTTP 请求永远不抛 SQLite 异常，
   于是 `URLError` / `RemoteDisconnected` / JSON 解析错误**全部漏网**：
   重试形同虚设、异常直接上抛、失败只剩一条 debug 日志。
   → 已改为捕获 `(urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError)`，
   并在重试耗尽后 `logger.warning`，默认 `retries` 2 → 3。

2. **`fetch_etf_fund_flow` 完全没有重试**：单只失败即 0 行。
   34 只逐只采集（失败率 p）时，连续 5 只全失败的概率在 p=0.3 时约 0.24%，
   在 p=0.4 时约 1%——看起来不高，但**每天跑一次，一个月就会撞上**，
   表现为「ETF资金流: 连续5只失败，跳过剩余29只」→ 整天 ETF 资金流落 0 行
   （2026-09-14 生产日志正是如此）。
   → 已加 3 次重试 + 指数退避（0.8s / 1.6s），并把三类抖动都纳入可重试：
   网络异常、`empty response`、`data: null` 解包异常（`TypeError/KeyError/ValueError/IndexError`）。

### ⚠️ 度量陷阱：不要用「刚压测完的成功率」评估这个修复
本轮排查期间连续高频打 push2his，IP 被阻尼后同一批标的成功率会从 6/10 掉到 3/8。
**这不是修复变坏了**，是自伤。要看真实效果，请对比修复前后**各一个正常交易日**
15:30 定时任务的日志：
- 修前：`ETF资金流: 连续5只失败，跳过剩余N只` + `资金流采集完成: ... ETF0条`
- 修后：期望看到 `ETF资金流(push2his): N 条 (M/34 只)` 且 M 显著大于 0

### 仍未解决（接受）
push2his 的阻尼无法从客户端消除。现有的兜底链保持不变且必须保留：
`逐只 push2his` → `fetch_etf_fund_flow_batch`（datacenter-web 端点，单次请求，
**不受 push2his 阻尼影响**）→ `backfill_etf_fund_flow_from_kline`（K 线估算）。
即「逐只拿历史、批量保当日、K线补空缺」三层，任何一层都不要删。
