"""Tab15 交易复盘 — 纯函数单元测试"""
import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock


# ==================== Fixtures ====================

@pytest.fixture
def trades_buy_sell():
    """基本买卖数据：A 买入2笔，卖出1笔"""
    return pd.DataFrame([
        {"date": "2026-01-05", "market": "上海", "code": "512810", "name": "国防军工",
         "action": "证券买入", "quantity": 3000, "price": 0.80, "amount": 2400,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -2405},
        {"date": "2026-01-06", "market": "上海", "code": "512810", "name": "国防军工",
         "action": "证券买入", "quantity": 5000, "price": 0.82, "amount": 4100,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -4105},
        {"date": "2026-01-07", "market": "上海", "code": "512810", "name": "国防军工",
         "action": "证券卖出", "quantity": -4000, "price": 0.90, "amount": 3600,
         "commission": 5.0, "stamp_tax": 3.6, "change_amount": 3586.4},
    ])


@pytest.fixture
def trades_multi_code():
    """多证券买卖"""
    return pd.DataFrame([
        {"date": "2026-01-05", "market": "上海", "code": "510300", "name": "300ETF",
         "action": "证券买入", "quantity": 1000, "price": 4.0, "amount": 4000,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -4005},
        {"date": "2026-01-06", "market": "上海", "code": "510300", "name": "300ETF",
         "action": "证券卖出", "quantity": -500, "price": 4.2, "amount": 2100,
         "commission": 5.0, "stamp_tax": 2.1, "change_amount": 2087.9},
        {"date": "2026-01-05", "market": "深圳", "code": "159949", "name": "创业板50",
         "action": "证券买入", "quantity": 2000, "price": 1.0, "amount": 2000,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -2005},
        {"date": "2026-01-06", "market": "深圳", "code": "159949", "name": "创业板50",
         "action": "证券卖出", "quantity": -2000, "price": 1.1, "amount": 2200,
         "commission": 5.0, "stamp_tax": 2.2, "change_amount": 2187.8},
    ])


@pytest.fixture
def trades_sell_before_buy():
    """先卖后买（应产生 0 配对，卖出无对应买入）"""
    return pd.DataFrame([
        {"date": "2026-01-07", "market": "上海", "code": "512810", "name": "国防军工",
         "action": "证券卖出", "quantity": -1000, "price": 0.90, "amount": 900,
         "commission": 5.0, "stamp_tax": 0.9, "change_amount": 894.1},
        {"date": "2026-01-08", "market": "上海", "code": "512810", "name": "国防军工",
         "action": "证券买入", "quantity": 1000, "price": 0.85, "amount": 850,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -855},
    ])


@pytest.fixture
def trades_no_trades():
    """无买卖记录"""
    return pd.DataFrame([
        {"date": "2026-01-05", "market": "资金", "code": "", "name": "银行转存",
         "action": "银行转存", "quantity": 0, "price": 0, "amount": 0,
         "commission": 0, "stamp_tax": 0, "change_amount": 200},
    ])


@pytest.fixture
def trades_dca():
    """定投数据"""
    return pd.DataFrame([
        {"date": "2026-01-30", "market": "场外开基", "code": "007994", "name": "华夏500",
         "action": "产品定时定额投资确认", "quantity": 200, "price": 1.5, "amount": 200,
         "commission": 0, "stamp_tax": 0, "change_amount": -200},
        {"date": "2026-02-28", "market": "场外开基", "code": "007994", "name": "华夏500",
         "action": "产品定时定额投资确认", "quantity": 180, "price": 1.6, "amount": 200,
         "commission": 0, "stamp_tax": 0, "change_amount": -200},
        {"date": "2026-03-30", "market": "场外开基", "code": "100032", "name": "富国红利",
         "action": "产品定时定额投资确认", "quantity": 150, "price": 1.0, "amount": 200,
         "commission": 0, "stamp_tax": 0, "change_amount": -200},
    ])


@pytest.fixture
def snapshots_basic():
    """基本持仓快照"""
    return pd.DataFrame([
        {"date": "2026-06-15", "code": "007994", "name": "华夏500",
         "quantity": 380, "cost_price": 1.55, "current_price": 2.0, "market_value": 760},
        {"date": "2026-06-15", "code": "100032", "name": "富国红利",
         "quantity": 150, "cost_price": 1.0, "current_price": 0.9, "market_value": 135},
    ])


@pytest.fixture
def trades_with_fees():
    """含交易费用的数据"""
    return pd.DataFrame([
        {"date": "2026-01-05", "market": "上海", "code": "512810", "name": "军工",
         "action": "证券买入", "quantity": 3000, "price": 0.80, "amount": 2400,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -2405},
        {"date": "2026-01-06", "market": "上海", "code": "512810", "name": "军工",
         "action": "证券卖出", "quantity": -3000, "price": 0.90, "amount": 2700,
         "commission": 5.0, "stamp_tax": 2.7, "change_amount": 2687.3},
        {"date": "2026-02-01", "market": "上海", "code": "512810", "name": "军工",
         "action": "证券买入", "quantity": 2000, "price": 0.85, "amount": 1700,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -1705},
        {"date": "2026-02-05", "market": "深圳", "code": "159949", "name": "创业板50",
         "action": "证券买入", "quantity": 1000, "price": 1.0, "amount": 1000,
         "commission": 5.0, "stamp_tax": 0.0, "change_amount": -1005},
    ])


@pytest.fixture
def trades_cashflow():
    """资金流数据"""
    return pd.DataFrame([
        {"date": "2026-01-05", "market": "资金", "code": "", "name": "银行转存",
         "action": "银行转存", "quantity": 0, "price": 0, "amount": 0,
         "commission": 0, "stamp_tax": 0, "change_amount": 200},
        {"date": "2026-01-05", "market": "场外开基", "code": "880013", "name": "天添利",
         "action": "产品申购确认", "quantity": 100, "price": 1.0, "amount": 200,
         "commission": 0, "stamp_tax": 0, "change_amount": -200},
        {"date": "2026-01-29", "market": "场外开基", "code": "880013", "name": "天添利",
         "action": "产品赎回确认", "quantity": -100, "price": 1.0, "amount": 200,
         "commission": 0, "stamp_tax": 0, "change_amount": 200},
        {"date": "2026-02-05", "market": "资金", "code": "", "name": "银行转存",
         "action": "银行转存", "quantity": 0, "price": 0, "amount": 0,
         "commission": 0, "stamp_tax": 0, "change_amount": 200},
        {"date": "2026-02-05", "market": "场外开基", "code": "007994", "name": "华夏500",
         "action": "产品定时定额投资确认", "quantity": 50, "price": 1.5, "amount": 200,
         "commission": 0, "stamp_tax": 0, "change_amount": -200},
    ])

# ==================== TestCalcEtfTradePnl ====================

class TestCalcEtfTradePnl:
    """测试场内 ETF 买卖配对盈亏（FIFO）"""

    def test_basic_buy_sell(self, trades_buy_sell):
        """基本买卖：买入 3000@0.80 + 5000@0.82，卖出 4000@0.90（FIFO 拆2行）"""
        from tabs.tab15_trade_review import calc_etf_trade_pnl
        result = calc_etf_trade_pnl(trades_buy_sell)
        # FIFO: 先配 3000@0.80，再配 1000@0.82，产生 2 行
        assert len(result) == 2
        assert result['quantity'].sum() == 4000
        total_pnl = result['net_pnl'].sum()
        expected_buy_cost = 3000 * 0.80 + 1000 * 0.82
        expected_sell_revenue = 4000 * 0.90
        expected_fee = 5.0 + 3.6
        expected_pnl = expected_sell_revenue - expected_buy_cost - expected_fee
        assert abs(total_pnl - round(expected_pnl, 2)) < 0.01

    def test_multi_code(self, trades_multi_code):
        """多证券：510300 + 159949 各有买卖"""
        from tabs.tab15_trade_review import calc_etf_trade_pnl
        result = calc_etf_trade_pnl(trades_multi_code)
        codes = result['code'].unique()
        assert set(codes) == {'510300', '159949'}
        row_300 = result[result['code'] == '510300'].iloc[0]
        expected_pnl = 500 * 4.2 - 500 * 4.0 - (5.0 + 2.1)
        assert abs(row_300['net_pnl'] - round(expected_pnl, 2)) < 0.01

    def test_no_trades(self, trades_no_trades):
        """无买卖记录（仅有银行转存），应返回空"""
        from tabs.tab15_trade_review import calc_etf_trade_pnl
        result = calc_etf_trade_pnl(trades_no_trades)
        assert result.empty

    def test_partial_sell(self, trades_with_fees):
        """买入3000@0.80后全部卖出3000@0.90"""
        from tabs.tab15_trade_review import calc_etf_trade_pnl
        result = calc_etf_trade_pnl(trades_with_fees)
        rows = result[result['code'] == '512810']
        assert len(rows) >= 1
        total_qty = rows['quantity'].sum()
        assert total_qty == 3000

    def test_pnl_rate_positive(self, trades_buy_sell):
        """盈利交易 pnl_rate > 0"""
        from tabs.tab15_trade_review import calc_etf_trade_pnl
        result = calc_etf_trade_pnl(trades_buy_sell)
        assert result.iloc[0]['pnl_rate'] > 0

    def test_columns(self, trades_buy_sell):
        """返回 DataFrame 列名正确"""
        from tabs.tab15_trade_review import calc_etf_trade_pnl
        result = calc_etf_trade_pnl(trades_buy_sell)
        expected_cols = {'code', 'name', 'buy_price', 'sell_price', 'quantity',
                         'gross_pnl', 'fee', 'net_pnl', 'pnl_rate', 'sell_date'}
        assert set(result.columns) == expected_cols

    def test_win_loss_count(self, trades_buy_sell):
        """胜/亏笔数统计"""
        from tabs.tab15_trade_review import calc_etf_trade_pnl
        result = calc_etf_trade_pnl(trades_buy_sell)
        win = len(result[result['net_pnl'] > 0])
        loss = len(result[result['net_pnl'] <= 0])
        assert win == 2
        assert loss == 0

# ==================== TestCalcDcaTracking ====================

class TestCalcDcaTracking:
    """测试定投基金追踪"""

    def test_basic_dca(self, trades_dca, snapshots_basic):
        """基本定投：007994 定投2次(400)，隐含成本=380*1.55=589"""
        from tabs.tab15_trade_review import calc_dca_tracking
        result = calc_dca_tracking(trades_dca, snapshots_basic)
        assert len(result) == 2
        row_994 = result[result['code'] == '007994'].iloc[0]
        assert row_994['dca_count'] == 2
        # 隐含成本 = quantity * cost_price = 380 * 1.55 = 589.0
        assert abs(row_994['total_invested'] - 589.0) < 0.01
        assert row_994['current_mv'] == 760.0
        # profit = 760 - 589 = 171.0
        assert abs(row_994['profit'] - 171.0) < 0.01

    def test_dca_no_trades(self, trades_buy_sell, snapshots_basic):
        """无定投记录，返回空"""
        from tabs.tab15_trade_review import calc_dca_tracking
        result = calc_dca_tracking(trades_buy_sell, snapshots_basic)
        assert result.empty

    def test_dca_sorted_by_invested(self, trades_dca, snapshots_basic):
        """结果按 total_invested 降序排列"""
        from tabs.tab15_trade_review import calc_dca_tracking
        result = calc_dca_tracking(trades_dca, snapshots_basic)
        assert result.iloc[0]['total_invested'] >= result.iloc[1]['total_invested']

    def test_dca_columns(self, trades_dca, snapshots_basic):
        """返回 DataFrame 列名正确"""
        from tabs.tab15_trade_review import calc_dca_tracking
        result = calc_dca_tracking(trades_dca, snapshots_basic)
        expected = {'code', 'name', 'dca_count', 'first_date', 'last_date',
                    'dca_amount', 'manual_amount', 'div_income',
                    'total_invested', 'implied_cost',
                    'current_mv', 'profit', 'profit_rate', 'current_qty'}
        assert set(result.columns) == expected

    def test_dca_dates(self, trades_dca, snapshots_basic):
        """首次/末次定投日期正确"""
        from tabs.tab15_trade_review import calc_dca_tracking
        result = calc_dca_tracking(trades_dca, snapshots_basic)
        row_994 = result[result['code'] == '007994'].iloc[0]
        assert row_994['first_date'] == '2026-01-30'
        assert row_994['last_date'] == '2026-02-28'

# ==================== TestCalcTradeCostSummary ====================

class TestCalcTradeCostSummary:
    """测试交易成本汇总"""

    def test_basic_cost(self, trades_with_fees):
        """基本费用统计"""
        from tabs.tab15_trade_review import calc_trade_cost_summary
        monthly, by_code, total_row = calc_trade_cost_summary(trades_with_fees)
        # date格式'2026-01-05', str[:6]='2026-0', 全部4条同月
        assert len(monthly) >= 1
        assert total_row['trade_count'] == 4
        assert abs(total_row['total_fee'] - 22.7) < 0.01

    def test_no_fees(self, trades_no_trades):
        """无交易费用，返回空 DataFrame 和空 dict"""
        from tabs.tab15_trade_review import calc_trade_cost_summary
        monthly, by_code, total_row = calc_trade_cost_summary(trades_no_trades)
        assert monthly.empty
        assert by_code.empty
        assert total_row == {}

    def test_by_code_sorted(self, trades_with_fees):
        """按证券费用降序排列"""
        from tabs.tab15_trade_review import calc_trade_cost_summary
        _, by_code, _ = calc_trade_cost_summary(trades_with_fees)
        fees = by_code['total_fee'].values
        assert all(fees[i] >= fees[i+1] for i in range(len(fees)-1))

    def test_dca_no_fees(self, trades_dca):
        """定投记录无佣金/印花税，返回空"""
        from tabs.tab15_trade_review import calc_trade_cost_summary
        monthly, by_code, total_row = calc_trade_cost_summary(trades_dca)
        assert monthly.empty
        assert by_code.empty

    def test_total_commission(self, trades_with_fees):
        """总佣金正确"""
        from tabs.tab15_trade_review import calc_trade_cost_summary
        _, _, total_row = calc_trade_cost_summary(trades_with_fees)
        assert abs(total_row['total_commission'] - 20.0) < 0.01

    def test_total_stamp_tax(self, trades_with_fees):
        """总印花税正确"""
        from tabs.tab15_trade_review import calc_trade_cost_summary
        _, _, total_row = calc_trade_cost_summary(trades_with_fees)
        assert abs(total_row['total_stamp_tax'] - 2.7) < 0.01

# ==================== TestCalcMonthlyCashflow ====================

class TestCalcMonthlyCashflow:
    """测试月度资金流向 pivot"""

    def test_basic_cashflow(self, trades_cashflow):
        """基本资金流 pivot"""
        from tabs.tab15_trade_review import calc_monthly_cashflow
        pivot = calc_monthly_cashflow(trades_cashflow)
        assert 'month' in pivot.columns
        assert '银转存' in pivot.columns

    def test_action_mapping(self, trades_cashflow):
        """action_map 正确映射"""
        from tabs.tab15_trade_review import calc_monthly_cashflow
        pivot = calc_monthly_cashflow(trades_cashflow)
        assert '银转存' in pivot.columns
        assert '定投' in pivot.columns
        assert '赎回' in pivot.columns

    def test_cashflow_total(self, trades_cashflow):
        """银转存总金额正确: 200+200=400"""
        from tabs.tab15_trade_review import calc_monthly_cashflow
        pivot = calc_monthly_cashflow(trades_cashflow)
        total_deposit = pivot['银转存'].sum()
        assert abs(total_deposit - 400) < 0.01

    def test_empty_df(self):
        """空 DataFrame"""
        from tabs.tab15_trade_review import calc_monthly_cashflow
        df = pd.DataFrame(columns=['date', 'market', 'code', 'name', 'action',
                                    'quantity', 'price', 'amount', 'commission',
                                    'stamp_tax', 'change_amount'])
        pivot = calc_monthly_cashflow(df)
        assert pivot is not None

# ==================== TestLoadFunctions ====================

class TestLoadTradeRecords:
    """测试 load_trade_records（DB mock）"""

    def test_load_returns_dataframe(self):
        """返回 DataFrame"""
        from tabs.tab15_trade_review import load_trade_records
        mock_conn = MagicMock()
        with patch("tabs.tab15_trade_review.get_db_connection", return_value=mock_conn):
            with patch("tabs.tab15_trade_review.pd.read_sql_query") as mock_sql:
                mock_sql.return_value = pd.DataFrame([
                    {"date": "2026-01-05", "market": "上海", "code": "512810",
                     "name": "军工", "action": "证券买入", "quantity": 3000,
                     "price": 0.80, "amount": 2400, "commission": 5.0,
                     "stamp_tax": 0.0, "change_amount": -2405},
                ])
                result = load_trade_records()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['code'] == "512810"

class TestLoadPortfolioSnapshots:
    """测试 load_portfolio_snapshots（DB mock）"""

    def test_load_returns_dataframe(self):
        """返回 DataFrame"""
        from tabs.tab15_trade_review import load_portfolio_snapshots
        mock_conn = MagicMock()
        with patch("tabs.tab15_trade_review.get_db_connection", return_value=mock_conn):
            with patch("tabs.tab15_trade_review.pd.read_sql_query") as mock_sql:
                mock_sql.return_value = pd.DataFrame([
                    {"date": "2026-06-15", "code": "007994", "name": "华夏500",
                     "quantity": 380, "cost_price": 1.55, "current_price": 2.0,
                     "market_value": 760},
                ])
                result = load_portfolio_snapshots()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['code'] == "007994"
