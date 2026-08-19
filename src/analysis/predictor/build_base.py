"""Phase 0 编排入口：建表 -> 补采 OHLCV -> 建特征 -> 建标签。

运行（从项目根目录）：
    python -m src.analysis.predictor.build_base

也可作为库函数调用 build_prediction_base(conn=...)。
"""
import logging
import re
from typing import List, Optional

import pandas as pd

from config.settings import MAJOR_ETFS, DATABASE_PATH
from src.utils.db_schema import init_all_tables
from .features import build_feature_matrix, upsert_features, _norm_code
from .labels import build_labels, upsert_labels
from .price_history import backfill_etf_price_history

logger = logging.getLogger(__name__)


_ETF_CODE_RE = re.compile(r"^(5\d{5}|1[56]\d{4})$")


def _is_etf(code: str, name: str) -> bool:
    """判定持仓记录是否为 ETF：name 含 'ETF' 或代码符合 ETF 模式。"""
    if name and "ETF" in str(name).upper():
        return True
    if code and _ETF_CODE_RE.match(code):
        return True
    return False


def resolve_target_codes(conn) -> List[str]:
    """目标域 = 最新快照中属于 ETF 类的当前持仓（自动跟随真实持仓）。

    根治历史白名单过窄问题：不再死守固定 MAJOR_ETFS 白名单，而是直接以
    portfolio_snapshots 最新日期的 ETF 类持仓为目标域，确保新增持仓自动纳入、
    不会漏覆盖。防御：若从持仓推导为空（异常），回退 MAJOR_ETFS。
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
                if c6:
                    codes.append(c6)
    if not codes:
        codes = [_norm_code(c) for c in MAJOR_ETFS]
    return sorted(set(codes))


def build_prediction_base(conn=None, backfill_ohlcv: bool = True,
                          as_of: Optional[str] = None, log=print) -> dict:
    """在给定连接上构建预测底座三表，返回汇总字典。"""
    own_conn = False
    if conn is None:
        from src.utils.database import get_db_connection
        conn = get_db_connection()
        own_conn = True
    try:
        init_all_tables(conn)  # 确保三张新表已创建
        codes = resolve_target_codes(conn)
        log(f"[Base] 目标域 {len(codes)} 只 ETF: {codes}")

        ohlcv_rows = 0
        if backfill_ohlcv:
            ohlcv_rows = backfill_etf_price_history(conn, codes, log=log)
            log(f"[Base] OHLCV 补采合计 {ohlcv_rows} 行")

        feat = build_feature_matrix(conn, codes, as_of=as_of)
        n_feat = upsert_features(conn, feat)

        lab = build_labels(conn, codes)
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
