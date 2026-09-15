# -*- coding: utf-8 -*-
"""ETF 跟踪指数 PE 长历史回补模块（供 ETF 价格高低位评估引擎使用）。

背景
----
`src/analysis/etf_position.py`（ETF 价格高低位评估引擎）的估值因子(valuation)
依赖 `index_pe_history` 表中**跟踪指数**的 PE-TTM 长历史；但该表原由
`neodata_valuation.py` 采集，仅 ~10~36 个交易日，远低于估值闸门
(`VAL_MIN_DAYS=250`)，导致估值因子对全部 23 只 ETF **禁用**，引擎长期只用
价格 + 资金流两因子。

方法（参考 WorkBuddy space 复盘报告《ETF 多区间收益筛选与 PE 数据采集》）
-------------------------------------------------------------
- ETF 的「市盈率」= 其**跟踪指数的 PE-TTM（整体法）**，这是行业共识口径。
- 中证指数公司 `index-perf` 接口（akshare `stock_zh_index_hist_csindex`，
  传入 `end_date` 至今）可返回 **2018 年至今每日「滚动市盈率」**，覆盖绝大多数
  A 股指数，是稳定可用的长历史 PE 源。

本模块用该方法把 23 只 ETF 跟踪指数的 PE 长历史回补进 `index_pe_history`，
使估值因子闸门打开（历史 ≥ 1250 日给满置信）。

幂等与安全性
------------
- 使用 `INSERT OR IGNORE`：csindex 的历史行（仅有 pe）不会覆盖 `neodata`
  已有的近期行（那些行还带 pb / div_yield），仅补充历史空白；
- 重复运行安全（同一 (index_code, date) 不会重复插入）。

覆盖说明（上游数据缺口，非实现问题）
--------------------------------------
16 个跟踪指数中 **15 个**可经 csindex 取到 2018→至今全量 PE（覆盖 22/23 ETF）。
仅 1 个为真实上游缺口，估值因子对该 ETF 保持禁用，引擎自动跳过：

- `399673` 创业板50 → `159949` 创业板50ETF（深交所体系，中证指数公司不发布 PE-TTM）

其余此前一度误判为"上游无 PE"的指数，实为早期映射代码填错所致，已修正并回补：

- `H30590` 中证机器人（原误填 `930006`=中证A50美元指数）→ `159770` 机器人ETF天弘
- `930914` 港股通高股息低波（原误填 `h11118`=中证两岸三地500美元）→ `159220` 港股通红利低波ETF华宝
- `931743` 中证消费电子 → `159732` 消费电子ETF华夏（原 etf_position 误填 `930006`，本次一并纠正并回补）

创业板50(399673) 的 PE 已由乐咕(legulegu) 经 `fetch_legulegu_pe` 补齐：
`ak.stock_index_pe_lg(symbol="创业板50")` 可稳定取得 2009 至今月度 PE-TTM（整体法），
与 neodata 同日偏离 <3.1%，是国证/深交所体系的最佳可用源。
（早期文档称「乐咕 SSL 失败 / csindex+乐咕均不可得」已过时，实测乐咕可用。）
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# csindex 该接口最早数据约 2018-05；回溯起点固定。
CSINDEX_START = "20180526"
# 默认取到“今天”，保证写入的 PE 是最新的（含当前值）。
# 原为硬编码 "20260904"，会随日期推移把回补窗口卡死在过去，故改为运行日动态计算。
DEFAULT_END = datetime.now().strftime("%Y%m%d")

# csindex 未发布 PE 历史的上游缺口指数（供覆盖率报告标注）。
# 930006 / h11118 已从该集合移除：二者不是"上游缺口"，而是早期 ETF→指数映射
# 填错的产物（930006=中证A50美元指数、h11118=中证两岸三地500美元指数），
# 正确映射为 H30590(159770) / 930914(159220)，这两个 csindex 均可取，见模块文档。
# csindex 未发布 PE 历史的上游缺口指数（仅 csindex 分支；乐咕分支独立覆盖 399673）。
# 注：399673 创业板50 属国证/深交所体系，csindex 确实不发布其 PE，但乐咕(legulegu)
# 经 ak.stock_index_pe_lg 可稳定取得 2009 至今月度 PE-TTM，故已从本集合移除，改由
# LEGULEGU_NAME_MAP + fetch_legulegu_pe 在 backfill 中补齐。
CSINDEX_UNAVAILABLE_INDICES = set()

# 乐咕(legulegu) 指数名映射：代码 -> 乐咕使用的指数名（非代码）。
# 仅收录 csindex 不发布、但乐咕可取的国证/深交所体系指数。
LEGULEGU_NAME_MAP = {"399673": "创业板50"}


def _target_indices() -> List[str]:
    """23 只 ETF 的全部跟踪指数（去重），优先取自 etf_position.ETF_TO_INDEX。"""
    try:
        from src.analysis.etf_position import ETF_TO_INDEX
        return sorted(set(ETF_TO_INDEX.values()))
    except Exception:
        # 兜底硬编码（与 etf_position.ETF_TO_INDEX 保持一致）
        # 注: 早期兜底表把 159770/159220 误填成 930006/h11118, 已按正确映射更正
        return ["000300", "000905", "000852", "000688", "399673", "399959",
                "399975", "930713", "H30590", "931743", "931152", "399989",
                "399808", "931157", "930914", "H30269"]


def fetch_csindex_pe(index_code: str, end_date: str = DEFAULT_END,
                     retries: int = 3) -> List[Tuple[str, float]]:
    """从中证 index-perf 取指数 PE-TTM 长历史。

    Returns:
        [(date_str 'YYYY-MM-DD', pe), ...]；失败（接口限频 / 该指数无数据）返回空列表。
    """
    import akshare as ak
    import pandas as pd

    last_err: Optional[str] = None
    for attempt in range(retries):
        try:
            df = ak.stock_zh_index_hist_csindex(
                symbol=index_code, start_date=CSINDEX_START, end_date=end_date)
            if df is None or df.empty or "滚动市盈率" not in df.columns:
                last_err = "empty/no-pe-col"
                time.sleep(1.2)
                continue
            out: List[Tuple[str, float]] = []
            for d, pe in zip(df["日期"], df["滚动市盈率"]):
                if pe is None or (isinstance(pe, float) and pe != pe):  # NaN
                    continue
                try:
                    pe_f = float(pe)
                except (TypeError, ValueError):
                    continue
                if pe_f <= 0:
                    continue
                ds = pd.to_datetime(d).strftime("%Y-%m-%d")
                out.append((ds, round(pe_f, 4)))
            if out:
                return out
            last_err = "no-valid-pe"
        except Exception as e:  # 限频 / 网络抖动 / 列重命名崩
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(1.5)
    logger.warning("[PE回补] %s 取数失败: %s", index_code, last_err)
    return []


def fetch_legulegu_pe(index_code: str, end_date: str = DEFAULT_END,
                      retries: int = 3) -> List[Tuple[str, float]]:
    """从乐咕(legulegu) 取指数 PE-TTM 长历史（月度）。

    乐咕用指数名而非代码；399673 创业板50 属国证/深交所体系，csindex 不发布其 PE，
    乐咕是最佳可用源（ak.stock_index_pe_lg）。返回 [(date 'YYYY-MM-DD', pe), ...]
    （整体法 滚动市盈率）。与 neodata 同日偏离 <3.1%，口径与 csindex 一致。

    Returns:
        [(date_str 'YYYY-MM-DD', pe), ...]；失败返回空列表。
    """
    import akshare as ak
    import pandas as pd

    name = LEGULEGU_NAME_MAP.get(index_code)
    if not name:
        return []
    last_err: Optional[str] = None
    for _ in range(retries):
        try:
            df = ak.stock_index_pe_lg(symbol=name)
            if df is None or df.empty or "滚动市盈率" not in df.columns:
                last_err = "empty/no-pe-col"
                time.sleep(1.2)
                continue
            out: List[Tuple[str, float]] = []
            for d, pe in zip(df["日期"], df["滚动市盈率"]):
                try:
                    pe_f = float(pe)
                except (TypeError, ValueError):
                    continue
                if pe_f is None or pe_f != pe_f or pe_f <= 0:  # NaN/非正
                    continue
                out.append((pd.to_datetime(d).strftime("%Y-%m-%d"), round(pe_f, 4)))
            if out:
                return out
            last_err = "no-valid-pe"
        except Exception as e:  # 限频 / 网络抖动 / 列重命名崩
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(1.5)
    logger.warning("[PE回补] %s 乐咕取数失败: %s", index_code, last_err)
    return []


def _ensure_source_column(conn) -> None:
    """幂等：index_pe_history 增加 source 列（区分 csindex / neodata 口径）。"""
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(index_pe_history)").fetchall()}
    except Exception:
        return
    if "source" not in cols:
        conn.execute("ALTER TABLE index_pe_history ADD COLUMN source TEXT DEFAULT 'unknown'")
        conn.commit()


#: 多源优先级 upsert：按 source 优先级写入，高优先级源不被低优先级覆盖。
#: 优先级 csindex(3) > legulegu(2) > neodata(1)，用 CASE 内联 rank 实现
#: （SQLite 无自定义函数）。高优先级(或同优先级)写入时覆盖 pe/source；
#: 低优先级遇到高优先级现存行则整条跳过（不动 pb / div_yield，避免清空 neodata 富字段）。
SRC_RANK = {"csindex": 3, "legulegu": 2, "neodata": 1, "unknown": 0}
_RANK_SQL = ("CASE source WHEN 'csindex' THEN 3 WHEN 'legulegu' THEN 2 "
             "WHEN 'neodata' THEN 1 ELSE 0 END")
UPSERT_PE_SQL = f"""
INSERT INTO index_pe_history (index_code, date, pe, source)
VALUES (?, ?, ?, ?)
ON CONFLICT(index_code, date) DO UPDATE SET
    pe = excluded.pe,
    source = excluded.source
WHERE ({_RANK_SQL.replace('source', 'excluded.source')}) >= ({_RANK_SQL})
"""


def upsert_pe(conn, index_code: str, date: str, pe: float,
              source: str = "csindex") -> bool:
    """条件 upsert 一条 PE（按 source 优先级）。返回是否实际改动了行。"""
    cur = conn.execute(UPSERT_PE_SQL,
                       (index_code, _norm_date(date), pe, source))
    return cur.rowcount > 0


def _norm_date(d) -> str:
    """'YYYYMMDD' -> 'YYYY-MM-DD'（表内 date 为 TEXT，混用两种格式会破坏排序语义）。"""
    d = str(d).strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def _count(conn, index_code: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM index_pe_history WHERE index_code=?",
        (index_code,)).fetchone()[0]


def backfill(conn, end_date: str = DEFAULT_END,
             indices: Optional[List[str]] = None) -> Dict[str, int]:
    """回补 PE 长历史到 `index_pe_history`（INSERT OR IGNORE，不覆盖 neodata 的 pb/dy）。

    Args:
        conn: 已打开的 sqlite 连接（建议由调用方用 get_db_connection 传入）。
        end_date: 取到哪天（默认 DEFAULT_END=今天附近）。
        indices: 指定指数；None 则取全部 23 只 ETF 的跟踪指数。

    Returns:
        {index_code: 回补后该指数在 index_pe_history 的总行数}
    """
    codes = indices or _target_indices()
    result: Dict[str, int] = {}
    _ensure_source_column(conn)
    for code in codes:
        rows = fetch_csindex_pe(code, end_date=end_date)
        src = "csindex"
        if not rows and code in LEGULEGU_NAME_MAP:
            rows = fetch_legulegu_pe(code, end_date=end_date)
            src = "legulegu"
        for ds, pe in rows:
            try:
                upsert_pe(conn, code, ds, pe, source=src)
            except Exception as e:
                logger.debug("写入 %s/%s 失败: %s", code, ds, e)
        conn.commit()
        final = _count(conn, code)
        result[code] = final
        note = ""
        if not rows and code in CSINDEX_UNAVAILABLE_INDICES:
            note = "（csindex无PE，乐咕亦未配置，未补）"
        elif not rows and code in LEGULEGU_NAME_MAP:
            note = "（乐咕取数失败，未补）"
        logger.info("[PE回补] %s 源=%s 尝试 %d 行, 回补后表内共 %d 行%s",
                    code, src, len(rows), final, note)
    return result


def coverage_report(conn) -> Dict[str, dict]:
    """逐 ETF 报告估值因子可用性（PE 历史是否达到估值闸门）。

    闸门按源区分：月频源(legulegu) 用 120 月 ≈ 10 年；日频源用 250 日 ≈ 1 年。
    与 etf_position.valuation_position 的闸门口径保持一致。

    Returns: {etf_code: {"index":.., "pe_n":.., "valuation_ready":bool, "note":..}}
    """
    try:
        from src.analysis.etf_position import ETF_TO_INDEX, BOND_ETFS
    except Exception:
        ETF_TO_INDEX = {}
        BOND_ETFS = set()
    rep: Dict[str, dict] = {}
    for etf, idx in ETF_TO_INDEX.items():
        if etf in BOND_ETFS:
            rep[etf] = {"index": idx, "pe_n": 0, "valuation_ready": False,
                        "note": "债券ETF：无权益PE，走独立利率定位"}
            continue
        n = conn.execute(
            "SELECT COUNT(*) FROM index_pe_history WHERE index_code=? AND pe>0",
            (idx,)).fetchone()[0]
        has_legulegu = conn.execute(
            "SELECT 1 FROM index_pe_history WHERE index_code=? AND source='legulegu' LIMIT 1",
            (idx,)).fetchone() is not None
        min_n = 120 if has_legulegu else 250  # 月频点跨度远大于同数日频点
        ready = n >= min_n
        note = ""
        if idx in CSINDEX_UNAVAILABLE_INDICES and not has_legulegu:
            note = "csindex未发布PE，且乐咕未配置，估值因子禁用"
        elif idx in CSINDEX_UNAVAILABLE_INDICES and has_legulegu:
            note = "csindex未发布PE，已用乐咕(legulegu)月度源补齐"
        elif not ready:
            note = f"PE历史仅{n}点(<{min_n}闸门)"
        rep[etf] = {"index": idx, "pe_n": n,
                    "valuation_ready": ready, "note": note}
    return rep


def _print_coverage(rep: Dict[str, dict]) -> None:
    ready = [c for c, v in rep.items() if v["valuation_ready"]]
    print(f"\n估值因子可用 ETF: {len(ready)}/{len(rep)}")
    print("-" * 70)
    for c, v in sorted(rep.items()):
        flag = "✅" if v["valuation_ready"] else "❌"
        print(f"  {c} -> {v['index']}: {flag} pe_n={v['pe_n']:>4}  {v['note']}")


if __name__ == "__main__":
    import logging as _logging
    from config.settings import DATABASE_PATH
    from src.utils.database import get_db_connection

    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    _conn = get_db_connection(str(DATABASE_PATH))
    try:
        print("=== 回补 23 只 ETF 跟踪指数 PE 长历史 ===")
        stats = backfill(_conn)
        print("\n=== 各指数回补后行数 ===")
        for k, v in sorted(stats.items()):
            tag = "  (csindex上游无PE)" if k in CSINDEX_UNAVAILABLE_INDICES else ""
            print(f"  {k}: {v}{tag}")
        rep = coverage_report(_conn)
        _print_coverage(rep)
    finally:
        _conn.close()
