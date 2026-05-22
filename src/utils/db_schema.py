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
            small_pct REAL
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
            created_at TEXT
        )
    """, [
        "CREATE INDEX IF NOT EXISTS idx_news_date ON daily_news(date)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_title ON daily_news(title)",
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
    "stock_block_trade": {"date_col": "date", "code_col": "code", "label": "大宗交易"},
}

# ============================================================
#  便捷函数
# ============================================================

def get_all_table_names():
    """返回所有已注册表名的列表"""
    return [t[0] for t in TABLE_DEFS]


def init_all_tables(conn):
    """在给定连接上执行所有 DDL（建表+索引）"""
    cur = conn.cursor()
    for table_name, ddl, indexes in TABLE_DEFS:
        cur.execute(ddl)
        for idx_sql in indexes:
            try:
                cur.execute(idx_sql)
            except Exception:
                pass
    conn.commit()
