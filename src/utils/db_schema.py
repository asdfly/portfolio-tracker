import sqlite3
"""
数据库 Schema 定义 - 所有表的 DDL 集中管理。

规则:
  - 新增表必须在此注册，由 DatabaseManager._init_db() 统一执行
  - 表结构变更需同步更新此文件
  - DataQualityChecker 等监控模块从此模块读取表注册信息
"""

# ============================================================
#  表 DDL 列表
#  格式: (table_name, ddl_sql, [index_sql, ...])
# ============================================================

TABLE_DEFS = [
    # --- 核心交易数据 ---
    ("portfolio_snapshots", """
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            quantity REAL,
            cost_price REAL,
            current_price REAL,
            market_value REAL,
            pnl REAL,
            pnl_rate REAL,
            ytd_return REAL,
            beta REAL,
            UNIQUE(date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_snapshot_date ON portfolio_snapshots(date)",
        "CREATE INDEX IF NOT EXISTS idx_snap_code_date ON portfolio_snapshots(code, date)",
    ]),

    ("portfolio_summary", """
        CREATE TABLE IF NOT EXISTS portfolio_summary (
            date TEXT PRIMARY KEY,
            total_value REAL,
            total_cost REAL,
            total_pnl REAL,
            daily_pnl REAL,
            daily_return REAL,
            vs_hs300 REAL,
            profit_count INTEGER,
            loss_count INTEGER,
            sharpe_ratio REAL,
            max_drawdown REAL,
            volatility REAL
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_summary_date ON portfolio_summary(date)",
    ]),

    ("index_quotes", """
        CREATE TABLE IF NOT EXISTS index_quotes (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            close REAL,
            change_pct REAL,
            volume REAL,
            amount REAL,
            PRIMARY KEY (date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_index_date ON index_quotes(date)",
        "CREATE INDEX IF NOT EXISTS idx_idx_quote_code_date ON index_quotes(code, date)",
    ]),

    ("etf_technical", """
        CREATE TABLE IF NOT EXISTS etf_technical (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            ma_signal TEXT,
            macd_signal TEXT,
            rsi_value REAL,
            rsi_status TEXT,
            kdj_signal TEXT,
            bollinger_position REAL,
            atr_pct REAL,
            trend TEXT,
            PRIMARY KEY (date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_tech_date ON etf_technical(date)",
        "CREATE INDEX IF NOT EXISTS idx_tech_code_date ON etf_technical(code, date)",
    ]),

    # --- 资金流 ---
    ("fund_flows", """
        CREATE TABLE IF NOT EXISTS fund_flows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT,
            name TEXT,
            net_inflow REAL,
            buy_amount REAL,
            sell_amount REAL,
            category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            net_inflow_pct REAL,
            super_large_inflow REAL,
            super_large_pct REAL,
            large_inflow REAL,
            large_pct REAL,
            medium_inflow REAL,
            medium_pct REAL,
            small_inflow REAL,
            small_pct REAL,
            -- P1-A 数据可信度标签: 标记每条资金流的来源与可靠性
            source TEXT,            -- 数据源标识: em_push2his / em_spot / ths / ths_agg / ths_decomp / em_hsgt / kline_est
            is_estimated BOOLEAN DEFAULT 0,  -- 1=估算/反推(非交易所直采), 0=真实采集
            confidence REAL         -- 0~1 置信度 (真实源=1.0, 估算源按精度递减)
        )
    """, []),

    # --- 宏观/情绪 ---
    ("macro_daily", """
        CREATE TABLE IF NOT EXISTS macro_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            indicator_code TEXT NOT NULL,
            name TEXT,
            value REAL,
            change_pct REAL,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, indicator_code)
        )
    """, []),

    ("market_sentiment", """
        CREATE TABLE IF NOT EXISTS market_sentiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            indicator_code TEXT NOT NULL,
            name TEXT,
            value REAL,
            change_value REAL,
            change_pct REAL,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, indicator_code)
        )
    """, []),

    # --- 新闻 ---
    ("daily_news", """
        CREATE TABLE IF NOT EXISTS daily_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            category TEXT,
            title TEXT,
            source TEXT,
            url TEXT,
            summary TEXT,
            publish_time TEXT,
            created_at TEXT,
            sentiment_score REAL DEFAULT NULL
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_news_date ON daily_news(date)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_date_title ON daily_news(date, title)",
    ]),

    # --- 监控 ---
    ("alerts", """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name TEXT,
            level TEXT,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            acknowledged BOOLEAN DEFAULT 0
        )
    """, []),

    ("execution_logs", """
        CREATE TABLE IF NOT EXISTS execution_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT,
            status TEXT,
            message TEXT,
            duration_seconds REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """, []),

    # --- 指标/回测 ---
    ("custom_indicators", """
        CREATE TABLE IF NOT EXISTS custom_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            formula TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_template BOOLEAN DEFAULT 0
        )
    """, []),

    ("indicator_backtest_results", """
        CREATE TABLE IF NOT EXISTS indicator_backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_id INTEGER,
            test_period TEXT,
            total_signals INTEGER,
            win_count INTEGER,
            loss_count INTEGER,
            win_rate REAL,
            avg_pnl REAL,
            sharpe REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (indicator_id) REFERENCES custom_indicators(id)
        )
    """, []),

    # --- 市场事件 (Phase 1) ---
    ("stock_lhb", """
        CREATE TABLE IF NOT EXISTS stock_lhb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            close REAL,
            change_pct REAL,
            lhb_net_buy REAL,
            lhb_buy_amount REAL,
            lhb_sell_amount REAL,
            lhb_volume REAL,
            market_volume REAL,
            net_buy_ratio REAL,
            volume_ratio REAL,
            turnover_rate REAL,
            float_mv REAL,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_lhb_date ON stock_lhb(date)",
        "CREATE INDEX IF NOT EXISTS idx_lhb_code_date ON stock_lhb(code, date)",
    ]),

    ("stock_margin", """
        CREATE TABLE IF NOT EXISTS stock_margin (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            margin_balance REAL,
            margin_buy REAL,
            margin_repay REAL,
            short_volume REAL,
            short_sell REAL,
            short_repay REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_margin_date ON stock_margin(date)",
        "CREATE INDEX IF NOT EXISTS idx_margin_code_date ON stock_margin(code, date)",
    ]),

    ("stock_holder_change", """
        CREATE TABLE IF NOT EXISTS stock_holder_change (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            holder_name TEXT NOT NULL,
            holder_type TEXT,
            code TEXT NOT NULL,
            name TEXT,
            report_period TEXT,
            holding_qty REAL,
            qty_change REAL,
            qty_change_pct REAL,
            change_type TEXT,
            float_mv REAL,
            announce_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, holder_name, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_holder_change_date ON stock_holder_change(date)",
        "CREATE INDEX IF NOT EXISTS idx_holder_change_code ON stock_holder_change(code)",
    ]),

    ("stock_institution_research", """
        CREATE TABLE IF NOT EXISTS stock_institution_research (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            price REAL,
            change_pct REAL,
            institution TEXT,
            inst_type TEXT,
            researchers TEXT,
            receive_method TEXT,
            receive_person TEXT,
            receive_location TEXT,
            research_date TEXT,
            announce_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, code, institution)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_jgdy_date ON stock_institution_research(date)",
        "CREATE INDEX IF NOT EXISTS idx_jgdy_code ON stock_institution_research(code)",
    ]),

    ("trade_records", """
        CREATE TABLE IF NOT EXISTS trade_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            market TEXT,
            code TEXT,
            name TEXT,
            action TEXT,
            quantity REAL,
            price REAL,
            amount REAL,
            commission REAL,
            stamp_tax REAL,
            change_amount REAL
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_trade_date ON trade_records(date)",
        "CREATE INDEX IF NOT EXISTS idx_trade_code ON trade_records(code)",
    ]),

    ("signal_backtest_stats", """
        CREATE TABLE IF NOT EXISTS signal_backtest_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator TEXT NOT NULL,
            signal_value TEXT NOT NULL,
            signal_direction INTEGER NOT NULL,
            forward_window INTEGER NOT NULL,
            market_regime TEXT NOT NULL DEFAULT 'all',
            scope TEXT NOT NULL DEFAULT 'all',
            code TEXT,
            signal_strength TEXT NOT NULL DEFAULT 'all',
            stability_score REAL,
            sample_count INTEGER NOT NULL,
            hit_count INTEGER NOT NULL,
            hit_rate REAL NOT NULL,
            weighted_hit_rate REAL,
            avg_return REAL NOT NULL,
            std_return REAL,
            t_statistic REAL,
            p_value REAL,
            confidence_score REAL NOT NULL,
            confidence_grade TEXT,
            backtest_date TEXT NOT NULL,
            UNIQUE(indicator, signal_value, forward_window, market_regime, scope, code, signal_strength)
        )
    """, []),

    ("signal_confidence_current", """
        CREATE TABLE IF NOT EXISTS signal_confidence_current (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            date TEXT NOT NULL,
            indicator TEXT NOT NULL,
            signal_value TEXT NOT NULL,
            signal_direction INTEGER NOT NULL,
            market_regime TEXT,
            scope TEXT,
            signal_strength TEXT,
            conf_5d REAL,
            conf_10d REAL,
            conf_20d REAL,
            conf_30d REAL,
            conf_60d REAL,
            composite_confidence REAL,
            composite_grade TEXT,
            hit_rate_5d REAL,
            hit_rate_10d REAL,
            hit_rate_20d REAL,
            hit_rate_30d REAL,
            hit_rate_60d REAL,
            stability_score REAL,
            updated_at TEXT NOT NULL,
            UNIQUE(code, indicator, signal_value, date)
        )
    """, []),

    ("stock_block_trade", """
        CREATE TABLE IF NOT EXISTS stock_block_trade (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            change_pct REAL,
            close REAL,
            trade_price REAL,
            premium_rate REAL,
            volume REAL,
            amount REAL,
            amount_to_float_mv REAL,
            buyer_broker TEXT,
            seller_broker TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, code, buyer_broker, seller_broker)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_block_date ON stock_block_trade(date)",
        "CREATE INDEX IF NOT EXISTS idx_block_code ON stock_block_trade(code)",
    ]),
    # gold_sge_hist: SGE黄金K线数据
    ("gold_sge_hist", """
        CREATE TABLE IF NOT EXISTS gold_sge_hist (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            UNIQUE(date, symbol)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_gold_sge_date ON gold_sge_hist(date)",
        "CREATE INDEX IF NOT EXISTS idx_gold_sge_symbol ON gold_sge_hist(symbol)",
    ]),
    # gold_etf_holdings: 全球黄金ETF持仓量
    ("gold_etf_holdings", """
        CREATE TABLE IF NOT EXISTS gold_etf_holdings (
            date TEXT NOT NULL,
            total_holdings REAL,
            change REAL,
            total_value REAL,
            UNIQUE(date)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_gold_etf_date ON gold_etf_holdings(date)",
    ]),

    # --- P0-2: 单位净值账本（时间加权收益 TWR） ---
    ("portfolio_nav", """
        CREATE TABLE IF NOT EXISTS portfolio_nav (
            date TEXT PRIMARY KEY,
            unit_nav REAL,          -- 单位净值，起始 1.0
            total_units REAL,       -- 总份额（无申赎则恒定=初始市值）
            total_value REAL,       -- 当日组合市值（冗余，便于校验）
            net_flow REAL,          -- 当日净现金流（流入为正，来自 trade_records）
            twr_cumulative REAL,    -- 累计时间加权收益（剔除申赎扰动）
            mwr_return REAL,        -- 资金加权收益（基于现金流 IRR，可选）
            is_suspect BOOLEAN DEFAULT 0  -- P0-2: total_value 跳变与日收益/现金流不符(疑似数据失真)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_nav_date ON portfolio_nav(date)",
    ]),

    # --- 预测底座 (Phase 0): OHLCV历史 / 数值特征 / 前瞻收益标签 ---
    ("etf_price_history", """
        CREATE TABLE IF NOT EXISTS etf_price_history (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            adj_close REAL,
            source TEXT,
            PRIMARY KEY (date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_eph_code_date ON etf_price_history(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_eph_date ON etf_price_history(date)",
    ]),

    ("etf_features", """
        CREATE TABLE IF NOT EXISTS etf_features (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            feat_version TEXT,
            ma5 REAL, ma10 REAL, ma20 REAL, ma60 REAL,
            macd REAL, macd_signal REAL, macd_hist REAL,
            rsi_14 REAL,
            boll_mid REAL, boll_upper REAL, boll_lower REAL, boll_pctb REAL,
            kdj_k REAL, kdj_d REAL, kdj_j REAL,
            atr_14 REAL, atr_pct REAL,
            ret_1d REAL, ret_5d REAL, ret_20d REAL,
            vol_20d REAL, mom_20d REAL,
            vol_5d REAL, vol_60d REAL, vol_ratio_5_20 REAL,
            ret_60d REAL, mom_5d REAL, range_20d REAL,
            parkinson_vol_20d REAL, hl_range_20d REAL, volume_zscore_20d REAL,
            ff_net_inflow_5d REAL, ff_net_inflow_20d REAL,
            ff_super_net_5d REAL, ff_large_net_5d REAL,
            hs300_ret_20d REAL, hs300_vol_20d REAL,
            PRIMARY KEY (date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_feat_code_date ON etf_features(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_feat_date ON etf_features(date)",
    ]),

    ("etf_forward_returns", """
        CREATE TABLE IF NOT EXISTS etf_forward_returns (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            fwd_ret_5 REAL, fwd_ret_20 REAL, fwd_ret_60 REAL,
            is_up_5 INTEGER, is_up_20 INTEGER, is_up_60 INTEGER,
            fwd_vol_5 REAL, fwd_vol_20 REAL, fwd_vol_60 REAL,
            fwd_max_dd_5 REAL, fwd_max_dd_20 REAL, fwd_max_dd_60 REAL,
            PRIMARY KEY (date, code)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_fr_code_date ON etf_forward_returns(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_fr_date ON etf_forward_returns(date)",
    ]),

    ("etf_predictions", """
        CREATE TABLE IF NOT EXISTS etf_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            model TEXT NOT NULL,
            forward_window INTEGER NOT NULL,
            direction INTEGER,
            score REAL,
            probability REAL,
            confidence REAL,
            grade TEXT,
            features TEXT,
            created_at TEXT,
            UNIQUE (date, code, model, forward_window)
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_pred_code_date ON etf_predictions(code, date)",
        "CREATE INDEX IF NOT EXISTS idx_pred_model ON etf_predictions(model)",
    ]),
]

# ============================================================
#  DataQualityChecker 使用的表注册信息
#  key = 表名, value = dict(date_col, code_col, label)
#  code_col = None 表示该表无个股维度（如 macro_daily）
# ============================================================

QUALITY_CHECK_TABLES = {
    "portfolio_snapshots": {"date_col": "date", "code_col": "code", "label": "交易日快照"},
    "etf_technical": {"date_col": "date", "code_col": "code", "label": "技术指标"},
    "fund_flows": {"date_col": "date", "code_col": "code", "label": "资金流"},
    "index_quotes": {"date_col": "date", "code_col": "code", "label": "指数行情"},
    "daily_news": {"date_col": "date", "code_col": None, "label": "新闻资讯"},
    "macro_daily": {"date_col": "date", "code_col": None, "label": "宏观数据"},
    "market_sentiment": {"date_col": "date", "code_col": None, "label": "市场情绪"},
    "portfolio_summary": {"date_col": "date", "code_col": None, "label": "组合摘要"},
    "stock_lhb": {"date_col": "date", "code_col": "code", "label": "龙虎榜"},
    "stock_margin": {"date_col": "date", "code_col": "code", "label": "融资融券"},
    "stock_holder_change": {"date_col": "date", "code_col": "code", "label": "股东增减持"},
    "stock_institution_research": {"date_col": "date", "code_col": "code", "label": "机构调研"},
    "trade_records": {"date_col": "date", "code_col": "code", "label": "交易流水",
                      "user_managed": True},  # 用户手动导入(招商证券对账单)，非自动采集源
    "stock_block_trade": {"date_col": "date", "code_col": "code", "label": "大宗交易"},
    # 黄金相关指标 (存储在 macro_daily 中, 用 indicator_code 区分)
    # 使用虚拟表名, DataQualityChecker 中特殊处理
    "_gold_comex": {"date_col": "date", "code_col": None, "label": "COMEX黄金", "source_table": "macro_daily", "indicator_code": "COMEX_GOLD"},
    "_gold_sge": {"date_col": "date", "code_col": None, "label": "上海金基准", "source_table": "macro_daily", "indicator_code": "SGE_GOLD"},
    "gold_sge_hist": {"date_col": "date", "code_col": "symbol", "label": "SGE K线"},
    "gold_etf_holdings": {"date_col": "date", "code_col": None, "label": "全球ETF持仓"},
}

# ============================================================
#  便捷函数
# ============================================================

def get_all_table_names():
    """返回所有已注册表名的列表"""
    return [t[0] for t in TABLE_DEFS]


_RISK_LABEL_COLS = [
    ("fwd_vol_5", "REAL"), ("fwd_vol_20", "REAL"), ("fwd_vol_60", "REAL"),
    ("fwd_max_dd_5", "REAL"), ("fwd_max_dd_20", "REAL"), ("fwd_max_dd_60", "REAL"),
]

_V2_FEATURE_COLS = [
    ("vol_5d", "REAL"), ("vol_60d", "REAL"), ("vol_ratio_5_20", "REAL"),
    ("ret_60d", "REAL"), ("mom_5d", "REAL"), ("range_20d", "REAL"),
    ("parkinson_vol_20d", "REAL"), ("hl_range_20d", "REAL"), ("volume_zscore_20d", "REAL"),
]


def _ensure_columns(conn, table, cols):
    """幂等补齐指定表的列（ALTER TABLE ADD COLUMN，迁移用）。"""
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        existing = {r[1] for r in cur.fetchall()}
    except sqlite3.OperationalError:
        return
    for col, typ in cols:
        if col not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
    conn.commit()


def ensure_etf_forward_returns_risk_columns(conn):
    """对已存在的 etf_forward_returns 表幂等补齐风险标签列（迁移用）。"""
    _ensure_columns(conn, "etf_forward_returns", _RISK_LABEL_COLS)


def ensure_etf_features_v2_columns(conn):
    """对已存在的 etf_features 表幂等补齐 v2 特征列（迁移用）。"""
    _ensure_columns(conn, "etf_features", _V2_FEATURE_COLS)


def init_all_tables(conn):
    """在给定连接上执行所有 DDL（建表+索引）"""
    cur = conn.cursor()
    for table_name, ddl, indexes in TABLE_DEFS:
        cur.execute(ddl)
        for idx_sql in indexes:
            try:
                cur.execute(idx_sql)
            except sqlite3.OperationalError:  # 索引创建失败可忽略
                pass
    conn.commit()
    # 迁移：补齐已存在表的新增列
    ensure_etf_forward_returns_risk_columns(conn)
    ensure_etf_features_v2_columns(conn)
