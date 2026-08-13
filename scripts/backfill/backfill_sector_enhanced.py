#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
行业资金流增强回填（可选增强封装）

保留原 backfill_sector_enhanced 的调用入口与注册表兼容：
实际逻辑统一走主线 src.data_sources.fund_flow.backfill_sector_fund_flow，
并开启 apply_market_trend_scaling=True 以挂接"市场趋势缩放"增强
（原 backfill_earlier_days 逻辑，已抽离为 fund_flow._sector_market_trend_scaling）。

这样消除 sector 资金流回填的双实现分歧：
- 主分解（最近25日，同花顺多周期差值分解）来自主线
- 更早日期的市场趋势缩放增强来自主线可选开关
- 本文件只负责"以增强模式调用主线"，不再维护第二套分解实现

使用方法:
    python backfill_sector_enhanced.py
"""

import os
import sys
import logging

# 项目根目录（scripts/backfill -> scripts -> 项目根）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 清除代理环境变量，避免 akshare / 直连请求被代理干扰
for k in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
    os.environ.pop(k, None)

import sqlite3
from config.settings import DATABASE_PATH
from data_loader import get_db_connection
from src.data_sources.fund_flow import backfill_sector_fund_flow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sector_backfill")


def run_backfill():
    """执行行业资金流增强回填（主线 + 市场趋势缩放增强）"""
    conn = get_db_connection(DATABASE_PATH)
    try:
        count = backfill_sector_fund_flow(conn, apply_market_trend_scaling=True)
        logger.info(f"增强回填完成: 新增 {count} 条（含市场趋势缩放估算）")
        return count
    finally:
        conn.close()


if __name__ == "__main__":
    run_backfill()
