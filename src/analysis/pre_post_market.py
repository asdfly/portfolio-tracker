"""
盘前/盘后分析助手 (P2-F)
盘前研判：隔夜市场变化、宏观指标异动、持仓技术信号预览、新闻情绪、风险预警
盘后复盘：当日盈亏归因、资金流变化、技术信号变化、同类ETF表现、重大新闻
"""
import pandas as pd
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

@dataclass
class IndexChange:
    name: str; code: str; close: float; change_pct: float; date: str

@dataclass
class MacroAlert:
    indicator_name: str; indicator_code: str; current_value: float
    previous_value: float; change_pct: float; alert_level: str; description: str

@dataclass
class EtfSignalPreview:
    code: str; name: str; trend: str; ma_signal: str; macd_signal: str
    rsi_value: float; rsi_status: str; signal_score: float; risk_score: float; fund_flow_net: float

@dataclass
class PreMarketReport:
    report_time: str; report_date: str
    index_changes: List[IndexChange] = field(default_factory=list)
    macro_alerts: List[MacroAlert] = field(default_factory=list)
    etf_signals: List[EtfSignalPreview] = field(default_factory=list)
    news_sentiment: Dict = field(default_factory=dict)
    risk_warnings: List[Dict] = field(default_factory=list)
    summary_text: str = ""

@dataclass
class PostMarketReport:
    report_time: str; report_date: str
    portfolio_pnl: Dict = field(default_factory=dict)
    pnl_attribution: List[Dict] = field(default_factory=list)
    fund_flow_changes: List[Dict] = field(default_factory=list)
    signal_changes: List[Dict] = field(default_factory=list)
    peer_performance: List[Dict] = field(default_factory=list)
    news_highlights: List[Dict] = field(default_factory=list)
    summary_text: str = ""


# ============================================================
#  盘前研判
# ============================================================

def generate_pre_market_report(conn) -> PreMarketReport:
    """生成盘前研判报告：指数+宏观+ETF信号+新闻+风险"""
    now = datetime.now()
    report = PreMarketReport(
        report_time=now.strftime("%Y-%m-%d %H:%M"),
        report_date=now.strftime("%Y-%m-%d"),
    )
    try:
        report.index_changes = _load_index_changes(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘前-指数变动加载失败: %s", e)
    try:
        report.macro_alerts = _load_macro_alerts(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘前-宏观指标加载失败: %s", e)
    try:
        report.etf_signals = _load_etf_signal_previews(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘前-ETF信号加载失败: %s", e)
    try:
        report.news_sentiment = _load_news_sentiment(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘前-新闻情绪加载失败: %s", e)
    try:
        report.risk_warnings = _load_risk_warnings(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘前-风险预警加载失败: %s", e)
    report.summary_text = _compose_pre_summary(report)
    return report


def _load_index_changes(conn) -> List[IndexChange]:
    """加载指数最新变动"""
    df = pd.read_sql_query("""
        SELECT code, name, close, change_pct, date
        FROM index_quotes
        WHERE date >= (SELECT MAX(date) - 1 FROM index_quotes)
        ORDER BY date DESC, code
    """, conn)
    if df.empty:
        return []
    changes, seen = [], set()
    for _, row in df.iterrows():
        code = str(row["code"])
        if code in seen:
            continue
        seen.add(code)
        changes.append(IndexChange(
            name=str(row.get("name", code)), code=code,
            close=float(row.get("close", 0)),
            change_pct=float(row.get("change_pct", 0)),
            date=str(row.get("date", "")),
        ))
    return changes


def _load_macro_alerts(conn) -> List[MacroAlert]:
    """加载宏观指标异动"""
    df = pd.read_sql_query("""
        SELECT indicator_code, name, value, date
        FROM macro_daily
        WHERE indicator_code IN ('USD_CNY','SHIBOR_ON','US_10Y_BOND','COMEX_GOLD','COMEX_OIL')
        ORDER BY date DESC
    """, conn)
    if df.empty or len(df) < 2:
        return []
    alerts, seen = [], set()
    for i in range(len(df) - 1):
        row, prev = df.iloc[i], df.iloc[i + 1]
        code = str(row["indicator_code"])
        if code in seen:
            continue
        seen.add(code)
        try:
            cur_val, prev_val = float(row["value"]), float(prev["value"])
            if prev_val == 0:
                continue
            change_pct = (cur_val - prev_val) / abs(prev_val) * 100
        except (ValueError, TypeError):
            continue
        level, desc = _classify_macro_change(code, cur_val, change_pct)
        if level != "info":
            alerts.append(MacroAlert(
                indicator_name=str(row.get("name", code)), indicator_code=code,
                current_value=cur_val, previous_value=prev_val,
                change_pct=change_pct, alert_level=level, description=desc,
            ))
    return alerts


def _classify_macro_change(code: str, value: float, change_pct: float) -> Tuple[str, str]:
    """分类宏观指标变化严重程度"""
    if code == "USD_CNY":
        if value >= 7.35:
            return "warning", f"人民币汇率承压 {value:.4f}，贬值{abs(change_pct):.2f}%"
        if value >= 7.25:
            return "caution", f"汇率接近压力位 {value:.4f}"
    elif code == "SHIBOR_ON":
        if value >= 3.0:
            return "warning", f"银行间利率偏高 {value:.3f}%，流动性偏紧"
        if value >= 2.5:
            return "caution", f"SHIBOR回升至 {value:.3f}%"
    elif code == "US_10Y_BOND":
        if value >= 4.75:
            return "warning", f"美债收益率高企 {value:.2f}%，全球资产承压"
        if value >= 4.5:
            return "caution", f"美债收益率 {value:.2f}%，关注外资流向"
    elif code == "COMEX_GOLD":
        return "info", f"金价 {value:.0f}美元/盎司，{'避险情绪浓厚' if value > 2800 else '温和波动'}"
    elif code == "COMEX_OIL":
        if value > 100:
            return "warning", f"油价 {value:.1f}美元/桶，通胀压力上升"
        if value < 60:
            return "caution", f"油价 {value:.1f}美元/桶，低于60关注产油国动向"
    return "info", ""


def _load_etf_signal_previews(conn) -> List[EtfSignalPreview]:
    """加载持仓ETF技术信号预览"""
    pos_df = pd.read_sql_query("""
        SELECT DISTINCT code, name FROM portfolio_snapshots
        WHERE date = (SELECT MAX(date) FROM portfolio_snapshots)
    """, conn)
    if pos_df.empty:
        return []
    name_map = dict(zip(pos_df["code"], pos_df["name"]))
    previews = []
    for code in pos_df["code"].tolist():
        preview = EtfSignalPreview(
            code=code, name=name_map.get(code, code),
            trend="--", ma_signal="--", macd_signal="--",
            rsi_value=50.0, rsi_status="--",
            signal_score=50.0, risk_score=50.0, fund_flow_net=0.0,
        )
        tech_df = pd.read_sql_query(
            "SELECT trend,ma_signal,macd_signal,rsi_value,rsi_status "
            "FROM etf_technical WHERE code=? ORDER BY date DESC LIMIT 1",
            conn, params=(code,))
        if not tech_df.empty:
            tr = tech_df.iloc[0]
            preview.trend = str(tr.get("trend", "--"))
            preview.ma_signal = str(tr.get("ma_signal", "--"))
            preview.macd_signal = str(tr.get("macd_signal", "--"))
            preview.rsi_value = float(tr.get("rsi_value", 50))
            preview.rsi_status = str(tr.get("rsi_status", "--"))
        try:
            from data_loader import load_signal_score
            sig_dict = load_signal_score(code)
            if sig_dict and "total_score" in sig_dict:
                preview.signal_score = float(sig_dict["total_score"])
        except (pd.errors.DatabaseError, sqlite3.OperationalError, ImportError):
            pass
        try:
            from data_loader import load_etf_risk_scan
            risk_dict = load_etf_risk_scan(code)
            if risk_dict and "total_score" in risk_dict:
                preview.risk_score = float(risk_dict["total_score"])
        except (pd.errors.DatabaseError, sqlite3.OperationalError, ImportError):
            pass
        try:
            flow_df = pd.read_sql_query(
                "SELECT net_inflow FROM fund_flows WHERE code=? "
                "AND category = 'etf' ORDER BY date DESC LIMIT 1",
                conn, params=(code,))
            if not flow_df.empty:
                preview.fund_flow_net = float(flow_df.iloc[0]["net_inflow"]) / 1e4
        except (pd.errors.DatabaseError, sqlite3.OperationalError):
            pass
        previews.append(preview)
    return previews

def _load_news_sentiment(conn) -> Dict:
    df = pd.read_sql_query("""
        SELECT title, date, category, sentiment_score, source
        FROM daily_news WHERE date >= date('now', '-3 days')
        ORDER BY date DESC, publish_time DESC LIMIT 100
    """, conn)
    if df.empty:
        return {"total": 0, "positive": 0, "negative": 0, "neutral": 0,
                "positive_ratio": 0, "negative_ratio": 0, "top_headlines": []}
    total = len(df)
    pos_count = sum(1 for _, r in df.iterrows()
                   if not pd.isna(r.get("sentiment_score")) and r["sentiment_score"] > 0.1)
    neg_count = sum(1 for _, r in df.iterrows()
                   if not pd.isna(r.get("sentiment_score")) and r["sentiment_score"] < -0.1)
    headlines = []
    for _, row in df.head(10).iterrows():
        s = row.get("sentiment_score")
        if pd.isna(s): s = 0
        headlines.append({"title": str(row.get("title","")), "date": str(row.get("date",""))[:10],
                          "category": str(row.get("category","")),
                          "sentiment": "正面" if s>0.1 else ("负面" if s<-0.1 else "中性")})
    return {"total": total, "positive": pos_count, "negative": neg_count,
            "neutral": total-pos_count-neg_count,
            "positive_ratio": pos_count/total if total>0 else 0,
            "negative_ratio": neg_count/total if total>0 else 0,
            "top_headlines": headlines}

def _load_risk_warnings(conn) -> List[Dict]:
    try:
        df = pd.read_sql_query("""
            SELECT code, total_score, volatility_score, discount_risk_score,
                   liquidity_score, downside_score, deviation_score
            FROM etf_risk_scan
            WHERE code IN (SELECT DISTINCT code FROM portfolio_snapshots
                           WHERE date=(SELECT MAX(date) FROM portfolio_snapshots))
            AND total_score > 60 ORDER BY total_score DESC
        """, conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError):
        return []
    return [{"code":str(r["code"]),"total_score":float(r["total_score"]),
             "volatility":float(r.get("volatility_score",0)),
             "discount":float(r.get("discount_risk_score",0)),
             "liquidity":float(r.get("liquidity_score",0)),
             "downside":float(r.get("downside_score",0)),
             "deviation":float(r.get("deviation_score",0))} for _,r in df.iterrows()]

def _compose_pre_summary(report):
    parts = []
    if report.index_changes:
        major = [ic for ic in report.index_changes if ic.code in ("sh000300","sz399001","sz399006")]
        if major:
            avg = sum(ic.change_pct for ic in major)/len(major)
            d = "偏多" if avg>0.3 else ("偏空" if avg<-0.3 else "中性震荡")
            parts.append(f"A股三大指数最新变动{avg:+.2f}%，市场情绪{d}。")
    if report.macro_alerts:
        high = [a for a in report.macro_alerts if a.alert_level=="warning"]
        if high:
            parts.append("宏观风险: "+" ; ".join(a.description for a in high[:3])+"。")
    if report.risk_warnings:
        parts.append(f"持仓风险预警: {len(report.risk_warnings)}只ETF风险评分>60。")
    ns = report.news_sentiment
    if ns.get("total",0)>0:
        nr = ns["negative_ratio"]
        if nr>0.3: parts.append(f"近3日新闻偏负面({nr:.0%})。")
        elif nr<0.1: parts.append(f"近3日新闻偏正面({ns['positive_ratio']:.0%})。")
    if not parts:
        parts.append("当前市场环境平稳，未检测到明显风险信号。")
    return "".join(parts)


# ============================================================
#  盘后复盘
# ============================================================

def generate_post_market_report(conn) -> PostMarketReport:
    now = datetime.now()
    report = PostMarketReport(
        report_time=now.strftime("%Y-%m-%d %H:%M"),
        report_date=now.strftime("%Y-%m-%d"),
    )
    try:
        report.portfolio_pnl, report.pnl_attribution = _load_portfolio_pnl(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘后-盈亏加载失败: %s", e)
    try:
        report.fund_flow_changes = _load_fund_flow_changes(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘后-资金流加载失败: %s", e)
    try:
        report.signal_changes = _load_signal_changes(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘后-信号变化加载失败: %s", e)
    try:
        report.peer_performance = _load_peer_performance(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘后-同类表现加载失败: %s", e)
    try:
        report.news_highlights = _load_news_highlights(conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError, KeyError, ValueError) as e:
        logger.warning("盘后-新闻加载失败: %s", e)
    report.summary_text = _compose_post_summary(report)
    return report

def _load_portfolio_pnl(conn) -> Tuple[Dict, List[Dict]]:
    latest_df = pd.read_sql_query("SELECT MAX(date) as d FROM portfolio_snapshots", conn)
    if latest_df.empty: return {}, []
    latest = latest_df.iloc[0]["d"]
    pos = pd.read_sql_query("""
        SELECT code,name,quantity,cost_price,current_price,market_value,pnl,pnl_rate,beta
        FROM portfolio_snapshots WHERE date=? ORDER BY market_value DESC
    """, conn, params=(latest,))
    if pos.empty: return {}, []
    total_mv = pos["market_value"].sum()
    total_pnl = pos["pnl"].sum()
    total_cost = (pos["cost_price"]*pos["quantity"]).sum()
    total_ret = (total_pnl/total_cost*100) if total_cost>0 else 0
    pc, lc = len(pos[pos["pnl"]>0]), len(pos[pos["pnl"]<0])
    summary = {"date":str(latest),"total_market_value":total_mv,"total_pnl":total_pnl,
               "total_return_pct":total_ret,"profit_count":pc,"loss_count":lc,
               "win_rate":pc/(pc+lc)*100 if (pc+lc)>0 else 0}
    attr = []
    for _, r in pos.iterrows():
        contrib = (r["pnl"]/total_pnl*100) if total_pnl!=0 else 0
        attr.append({"code":str(r["code"]),"name":str(r["name"]),
                     "market_value":float(r["market_value"]),"pnl":float(r["pnl"]),
                     "pnl_rate":float(r["pnl_rate"]),"contribution_pct":contrib})
    return summary, attr

def _load_fund_flow_changes(conn) -> List[Dict]:
    df = pd.read_sql_query("""
        WITH latest AS (
            SELECT code,date,net_inflow,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) as rn
            FROM fund_flows WHERE category IN ('etf_flow','sector')
        )
        SELECT l1.code, l1.net_inflow as today_flow, l2.net_inflow as yesterday_flow,
               l1.net_inflow - COALESCE(l2.net_inflow,0) as flow_change
        FROM latest l1 LEFT JOIN latest l2 ON l1.code=l2.code AND l2.rn=2
        WHERE l1.rn=1 ORDER BY flow_change DESC
    """, conn)
    if df.empty: return []
    return [{"code":str(r["code"]),"today_flow":float(r["today_flow"])/1e4,
             "yesterday_flow":float(r.get("yesterday_flow",0) or 0)/1e4,
             "flow_change":float(r.get("flow_change",0) or 0)/1e4} for _,r in df.iterrows()]


def _load_signal_changes(conn) -> List[Dict]:
    df = pd.read_sql_query("""
        WITH ranked AS (
            SELECT code,date,ma_signal,macd_signal,rsi_status,kdj_signal,trend,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) as rn
            FROM etf_technical
        )
        SELECT r1.code, r1.trend as today_trend, r2.trend as prev_trend,
               r1.macd_signal as today_macd, r2.macd_signal as prev_macd,
               r1.rsi_status as today_rsi, r2.rsi_status as prev_rsi,
               r1.ma_signal as today_ma, r2.ma_signal as prev_ma
        FROM ranked r1 LEFT JOIN ranked r2 ON r1.code=r2.code AND r2.rn=2
        WHERE r1.rn=1
    """, conn)
    if df.empty: return []
    changes = []
    dim_labels = {"trend":"趋势","macd":"MACD","rsi":"RSI","ma":"均线"}
    for _, row in df.iterrows():
        diffs = []
        for dim in ["trend","macd","rsi","ma"]:
            tv = str(row.get(f"today_{dim}",""))
            pv = str(row.get(f"prev_{dim}",""))
            if tv and pv and tv != pv:
                diffs.append({"dimension":dim_labels.get(dim,dim),"from":pv,"to":tv})
        if diffs:
            changes.append({"code":str(row["code"]),"changes":diffs})
    return changes

def _load_peer_performance(conn) -> List[Dict]:
    try:
        df = pd.read_sql_query("""
            SELECT code, name, change_pct, discount_rate, turnover_rate
            FROM etf_fundamental ORDER BY change_pct DESC
        """, conn)
    except (pd.errors.DatabaseError, sqlite3.OperationalError):
        return []
    if df.empty: return []
    return [{"code":str(r["code"]),"name":str(r["name"]),
             "change_pct":float(r.get("change_pct",0)),
             "discount_rate":float(r.get("discount_rate",0)),
             "turnover_rate":float(r.get("turnover_rate",0))} for _,r in df.iterrows()]

def _load_news_highlights(conn) -> List[Dict]:
    df = pd.read_sql_query("""
        SELECT title, date, category, sentiment_score, source
        FROM daily_news WHERE date >= date('now','-1 days')
        ORDER BY CASE WHEN ABS(COALESCE(sentiment_score,0))>0.5 THEN 0 ELSE 1 END,
                 date DESC LIMIT 15
    """, conn)
    if df.empty: return []
    hl = []
    for _, row in df.iterrows():
        s = row.get("sentiment_score")
        if pd.isna(s): s = 0
        hl.append({"title":str(row.get("title","")),"date":str(row.get("date",""))[:10],
                    "category":str(row.get("category","")),
                    "sentiment":"正面" if s>0.1 else ("负面" if s<-0.1 else "中性")})
    return hl

def _compose_post_summary(report):
    parts = []
    pnl = report.portfolio_pnl
    if pnl:
        tp = pnl.get("total_pnl",0); ret = pnl.get("total_return_pct",0); wr = pnl.get("win_rate",0)
        d = "盈利" if tp>=0 else "亏损"
        parts.append(f"组合{d} {abs(tp):,.0f}元（{ret:+.2f}%），"
                     f"{pnl.get('profit_count',0)}盈{pnl.get('loss_count',0)}亏，胜率{wr:.0f}%。")
    if report.fund_flow_changes:
        inc = [f for f in report.fund_flow_changes if f["flow_change"]>0]
        out = [f for f in report.fund_flow_changes if f["flow_change"]<0]
        if inc and out: parts.append(f"资金分化：{len(inc)}只流入增加，{len(out)}只流出增加。")
        elif inc: parts.append(f"资金偏多：{len(inc)}只流入增加。")
        elif out: parts.append(f"资金偏空：{len(out)}只流出增加。")
    if report.signal_changes:
        parts.append(f"{len(report.signal_changes)}只ETF技术信号变化，关注趋势切换。")
    if report.news_highlights:
        neg = sum(1 for n in report.news_highlights if n["sentiment"]=="负面")
        if neg>=3: parts.append(f"负面新闻较多({neg}条)，关注板块影响。")
    if not parts:
        parts.append("今日市场平稳，暂无特别事件。")
    return "".join(parts)
