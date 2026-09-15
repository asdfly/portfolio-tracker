"""Phase 0 编排入口：建表 -> 补采 OHLCV -> 建特征 -> 建标签。

运行（从项目根目录）：
    python -m src.analysis.predictor.build_base

也可作为库函数调用 build_prediction_base(conn=...)。
"""
import logging
import re
from typing import List, Optional

import pandas as pd

from config.settings import MAJOR_ETFS, DATABASE_PATH, ETF_CATEGORIES
from src.utils.db_schema import init_all_tables
from .features import build_feature_matrix, upsert_features, _norm_code
from .labels import build_labels, upsert_labels
from .price_history import backfill_etf_price_history

logger = logging.getLogger(__name__)


# 场内 ETF 代码段：沪市 5 系（5xxxxx，如 510300/512810/511380）+ 深市 1[56] 系
# （15xxxx/16xxxx，如 159915）。并用负向前瞻排除已知场外代码段：
#   - 166xxx 是 LOF / 分级基金（如 166301 华商新趋势优选混合 LOF）
#   - 519xxx 是场外开放式基金（如 519770 交银优择回报混合A）
# 名称含 'ETF' 仍是主信号（覆盖无 ETF 字样的真 ETF，如 512810 国防军工）。
# 场外基金（001194/007994/100032 等）首段非 5/1[56]，不会被此正则命中。
# 详见 docs/handover/07_known_data_issues.md。
_ETF_CODE_RE = re.compile(r"^(?!166|519)(5\d{5}|1[56]\d{4})$")


def _is_etf(code: str, name: str) -> bool:
    """判定持仓记录是否为 ETF：name 含 'ETF' 或代码符合场内 ETF 模式。

    主信号是 name 含 'ETF' 字样（可覆盖无 ETF 字样的真 ETF，如 '国防军工' 512810）；
    代码正则仅作防御兜底，且已收紧为场内 ETF 前缀白名单，避免把场外基金
    （166xxx LOF / 519xxx 场外）误判为 ETF。详见 docs/handover/07_known_data_issues.md。
    """
    if name and "ETF" in str(name).upper():
        return True
    if code and _ETF_CODE_RE.match(code):
        return True
    return False


def _is_delisted(c6: str) -> bool:
    """ETF_CATEGORIES 中被显式标记 delisted（已退市/已清仓）的场外/历史标的。

    159732 消费电子ETF华夏 已于 2026-07-30 前后清仓：自 2026-07-31 起不再出现在任何
    持仓快照中（同批 22 只场内标的每日都在），见 docs/handover/07_known_data_issues.md。
    一旦有人在 ETF_CATEGORIES 给它打上 delisted 标记，这里即时生效；即便没有标记，
    resolve_target_codes 以「最新快照日」为口径，159732 也不会回到标的域。
    """
    return bool((ETF_CATEGORIES.get(c6) or {}).get("delisted"))


def resolve_target_codes(conn) -> List[str]:
    """目标域 = 最新快照中属于 ETF 类的当前持仓（自动跟随真实持仓）。

    根治历史白名单过窄问题：不再死守固定 MAJOR_ETFS 白名单，而是直接以
    portfolio_snapshots 最新日期的 ETF 类持仓为目标域，确保新增持仓自动纳入、
    不会漏覆盖。防御：若从持仓推导为空（异常），回退 MAJOR_ETFS。

    口径说明：这里取「最新快照日」而非「每只标的最新快照」，因此已清仓标的
    （如 159732，快照只到 2026-07-30）不会复辟；再叠加 delisted 显式过滤做双保险。
    """
    cur = conn.execute("SELECT MAX(date) FROM portfolio_snapshots")
    latest = cur.fetchone()[0]
    codes: List[str] = []
    if latest:
        cur.execute(
            "SELECT DISTINCT code, name FROM portfolio_snapshots WHERE date=?",
            (latest,),
        )
        for code, name in cur.fetchall():
            if _is_etf(code, name or ""):
                c6 = _norm_code(code)
                if c6 and not _is_delisted(c6):
                    codes.append(c6)
    if not codes:
        codes = [c for c in (_norm_code(x) for x in MAJOR_ETFS) if c and not _is_delisted(c)]
    return sorted(set(codes))


def build_prediction_base(conn=None, backfill_ohlcv: bool = True,
                          as_of: Optional[str] = None, log=print,
                          full_refresh_ohlcv: bool = False) -> dict:
    """在给定连接上构建/增量维护预测底座三表，返回汇总字典。

    幂等：etf_features / etf_forward_returns 按 (date, code) upsert，重复运行结果一致；
    etf_price_history 按每标的 MAX(date) 增量补到 as_of（full_refresh_ohlcv=True 时全量重拉）。
    as_of 为 None 时追到最新可得交易日。
    """
    own_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        own_conn = True
    try:
        init_all_tables(conn)  # 确保三张新表已创建
        codes = resolve_target_codes(conn)
        log(f"[Base] 目标域 {len(codes)} 只 ETF: {codes}")

        target = as_of or conn.execute("SELECT MAX(date) FROM portfolio_snapshots").fetchone()[0]
        ohlcv_rows = 0
        if backfill_ohlcv:
            # 三表统一以 target 为截止日：避免 OHLCV 落半日未收盘行造成底座内部错位
            ohlcv_rows = backfill_etf_price_history(
                conn, codes, end=target, force=full_refresh_ohlcv, log=log)
            log(f"[Base] OHLCV 补采合计 {ohlcv_rows} 行 (截止 {target})")

        feat = build_feature_matrix(conn, codes, as_of=target)
        n_feat = upsert_features(conn, feat)

        lab = build_labels(conn, codes)
        if target and not lab.empty:
            lab = lab[lab["date"] <= target]
        n_lab = upsert_labels(conn, lab)

        summary = {
            "target_codes": codes,
            "ohlcv_rows": ohlcv_rows,
            "feature_rows": n_feat,
            "label_rows": n_lab,
            "feature_date_range": (feat["date"].min(), feat["date"].max()) if not feat.empty else None,
            "label_date_range": (lab["date"].min(), lab["date"].max()) if not lab.empty else None,
        }
        log(f"[Base] 特征 {n_feat} 行，标签 {n_lab} 行")
        return summary
    finally:
        if own_conn:
            conn.close()


def _main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from src.utils.database import get_db_connection

    conn = get_db_connection(str(DATABASE_PATH))
    summary = build_prediction_base(conn=conn, log=logger.info)
    print("=== Phase 0 汇总 ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    _main()
