"""预测底座包（Phase 0）：ETF 短期走势预测的数据底座。

对外暴露核心编排与构建函数，供 run_analysis 后续阶段接入。
"""
from .features import (
    FEAT_VERSION,
    ALL_FEATURE_COLS,
    build_feature_matrix,
    compute_technical_from_close,
    upsert_features,
    aggregate_fund_flows,
    market_factors,
    load_ohlc,
    _norm_code,
)
from .labels import (
    FORWARD_WINDOWS,
    build_labels,
    compute_forward_returns,
    upsert_labels,
)
from .price_history import (
    backfill_etf_price_history,
    fetch_etf_ohlcv_akshare,
)
from .build_base import build_prediction_base, resolve_target_codes

__all__ = [
    "FEAT_VERSION",
    "ALL_FEATURE_COLS",
    "FORWARD_WINDOWS",
    "build_feature_matrix",
    "compute_technical_from_close",
    "upsert_features",
    "aggregate_fund_flows",
    "market_factors",
    "load_ohlc",
    "_norm_code",
    "build_labels",
    "compute_forward_returns",
    "upsert_labels",
    "backfill_etf_price_history",
    "fetch_etf_ohlcv_akshare",
    "build_prediction_base",
    "resolve_target_codes",
]
