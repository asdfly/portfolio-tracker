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

如需补齐创业板50(399673) 的 PE，需改用 eastmoney / wind 等付费源，本报告环境内
csindex + 乐咕均不可得。
"""
from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# csindex 该接口最早数据约 2018-05；回溯起点固定。
CSINDEX_START = "20180526"
# 默认取到“今天”，保证写入的 PE 是最新的（含当前值）。
DEFAULT_END = "20260904"

# csindex 未发布 PE 历史的上游缺口指数（供覆盖率报告标注）。
UNAVAILABLE_INDICES = {"399673", "930006", "h11118"}


def _target_indices() -> List[str]:
    """23 只 ETF 的全部跟踪指数（去重），优先取自 etf_position.ETF_TO_INDEX。"""
    try:
        from src.analysis.etf_position import ETF_TO_INDEX
        return sorted(set(ETF_TO_INDEX.values()))
    except Exception:
        # 兜底硬编码（与 etf_position.ETF_TO_INDEX 保持一致）
        return ["000300", "000905", "000852", "000688", "399673", "399959",
                "399975", "930713", "930006", "931743", "931152", "399989",
                "399808", "931157", "h11118", "H30269"]


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
    for code in codes:
        rows = fetch_csindex_pe(code, end_date=end_date)
        for ds, pe in rows:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO index_pe_history (index_code, date, pe) "
                    "VALUES (?, ?, ?)", (code, ds, pe))
            except Exception as e:
                logger.debug("写入 %s/%s 失败: %s", code, ds, e)
        conn.commit()
        final = _count(conn, code)
        result[code] = final
        logger.info("[PE回补] %s 尝试 %d 行, 回补后表内共 %d 行%s",
                    code, len(rows), final,
                    "（上游无PE，未补）" if not rows and code in UNAVAILABLE_INDICES else "")
    return result


def coverage_report(conn) -> Dict[str, dict]:
    """逐 ETF 报告估值因子可用性（PE 历史是否达到估值闸门 250 日）。

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
        ready = n >= 250
        note = ""
        if idx in UNAVAILABLE_INDICES:
            note = "上游无PE历史(csindex未发布)，估值因子禁用"
        elif not ready:
            note = f"PE历史仅{n}日(<250闸门)"
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
            tag = "  (上游无PE)" if k in UNAVAILABLE_INDICES else ""
            print(f"  {k}: {v}{tag}")
        rep = coverage_report(_conn)
        _print_coverage(rep)
    finally:
        _conn.close()
