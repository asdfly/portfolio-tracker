"""市场事件驱动风险信号引擎"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class SignalType(Enum):
    RISK = "risk"
    OPPORTUNITY = "opp"
    NEUTRAL = "neutral"

class SignalLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

@dataclass
class MarketSignal:
    source: str
    signal_type: SignalType
    level: SignalLevel
    code: str
    name: str
    date: str
    title: str
    description: str
    raw_data: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.5

class MarketEventSignalEngine:
    LHB_ATTENTION_REASONS = {
        "日涨幅偏离值达7%", "日振幅值达15%", "日换手率达20%",
        "连续三个交易日涨幅偏离值累计达20%",
        "连续三个交易日跌幅偏离值累计达20%",
    }
    MARGIN_ZSCORE_THRESHOLD = 2.0
    BLOCK_DISCOUNT_THRESHOLD = -0.15
    INST_BATCH_THRESHOLD = 5

    def __init__(self, db_connection):
        self.conn = db_connection

    def generate_all_signals(self, end_date=None, lookback_days=5):
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.strptime(end_date, "%Y-%m-%d")
                      - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        all_signals = []
        for fn, label in [
            (self._analyze_lhb, "龙虎榜"), (self._analyze_margin, "融资融券"),
            (self._analyze_holder_change, "股东增减持"),
            (self._analyze_block_trade, "大宗交易"),
            (self._analyze_institution_research, "机构调研"),
        ]:
            try:
                all_signals.extend(fn(start_date, end_date))
            except Exception as e:
                logger.warning(f"{label}信号分析失败: {e}")
        lo = {SignalLevel.HIGH: 0, SignalLevel.MEDIUM: 1, SignalLevel.LOW: 2}
        all_signals.sort(key=lambda s: (lo[s.level], -s.confidence))
        return all_signals

    def _analyze_lhb(self, start_date, end_date):
        signals = []
        df = pd.read_sql_query("SELECT * FROM stock_lhb WHERE date>=? AND date<=?",
                               self.conn, params=(start_date, end_date))
        if df.empty:
            return signals
        for _, row in df.iterrows():
            reason = str(row.get("reason", ""))
            cp = float(row.get("change_pct", 0) or 0)
            for att in self.LHB_ATTENTION_REASONS:
                if att in reason:
                    if abs(cp) > 5:
                        st = SignalType.OPPORTUNITY if cp > 5 else SignalType.RISK
                        lv = SignalLevel.HIGH if abs(cp) >= 9.5 else SignalLevel.MEDIUM
                        tag = "涨幅偏离" if st == SignalType.OPPORTUNITY else "跌幅偏离"
                        signals.append(MarketSignal(
                            source="lhb", signal_type=st, level=lv,
                            code=str(row.get("code", "")), name=str(row.get("name", "")),
                            date=str(row.get("date", "")),
                            title=f"龙虎榜: {row.get('name','')} {tag}上榜",
                            description=f"{reason}, 涨幅: {cp:.1f}%",
                            raw_data=row.to_dict(),
                            confidence=min(0.5 + abs(cp) / 20, 0.9)))
                    break
        if "buyer_type" in df.columns and "buy_amount" in df.columns:
            inst = df[df["buyer_type"].astype(str).str.contains("机构", na=False)].copy()
            if not inst.empty:
                inst["net"] = inst["buy_amount"].fillna(0) - inst["sell_amount"].fillna(0)
                top = inst.groupby(["date", "code", "name"])["net"].sum().nlargest(5)
                for (dt, code, name), net in top.items():
                    if net > 0:
                        signals.append(MarketSignal(
                            source="lhb", signal_type=SignalType.OPPORTUNITY,
                            level=SignalLevel.MEDIUM, code=str(code), name=str(name),
                            date=str(dt), title=f"机构净买入: {name}",
                            description=f"机构席位净买入 {net:,.0f} 万元",
                            raw_data={}, confidence=0.6))
        return signals

    def _analyze_margin(self, start_date, end_date):
        signals = []
        df = pd.read_sql_query("SELECT * FROM stock_margin WHERE date>=? AND date<=?",
                               self.conn, params=(start_date, end_date))
        if df.empty or "rzye" not in df.columns:
            return signals
        h0 = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        hist = pd.read_sql_query("SELECT code,rzmre,rzche FROM stock_margin WHERE date>=? AND date<?",
                                 self.conn, params=(h0, start_date))
        for _, row in df.iterrows():
            code = str(row.get("code", ""))
            net = float(row.get("rzmre", 0) or 0) - float(row.get("rzche", 0) or 0)
            if net == 0:
                continue
            ch = hist[hist["code"] == code] if not hist.empty else pd.DataFrame()
            if not ch.empty:
                ch = ch.copy(); ch["n"] = ch["rzmre"].fillna(0) - ch["rzche"].fillna(0)
                mu, sig = ch["n"].mean(), ch["n"].std()
                if sig > 0:
                    z = abs(net - mu) / sig
                    if z > self.MARGIN_ZSCORE_THRESHOLD:
                        d = "大幅融资买入" if net > 0 else "大幅融资偿还"
                        signals.append(MarketSignal(
                            source="margin",
                            signal_type=SignalType.RISK if net > 0 else SignalType.OPPORTUNITY,
                            level=SignalLevel.HIGH if z > 3 else SignalLevel.MEDIUM,
                            code=code, name=str(row.get("name", "")),
                            date=str(row.get("date", "")),
                            title=f"融资异常: {row.get('name','')} {d}",
                            description=f"净买入 {net:,.0f} 元, Z={z:.1f}x",
                            raw_data={"net_buy": net, "z_score": round(z, 2)},
                            confidence=min(0.5 + z * 0.1, 0.9)))
        return signals

    def _analyze_holder_change(self, start_date, end_date):
        signals = []
        df = pd.read_sql_query("SELECT * FROM stock_holder_change WHERE date>=? AND date<=?",
                               self.conn, params=(start_date, end_date))
        if df.empty or "change_ratio" not in df.columns:
            return signals
        for _, row in df.iterrows():
            r = float(row.get("change_ratio", 0) or 0)
            h = str(row.get("holder_name", "")); ct = str(row.get("change_type", ""))
            if r < -0.5:
                signals.append(MarketSignal(
                    source="holder_change", signal_type=SignalType.RISK,
                    level=SignalLevel.HIGH if r < -1 else SignalLevel.MEDIUM,
                    code=str(row.get("code","")), name=str(row.get("name","")),
                    date=str(row.get("date","")),
                    title=f"大额减持: {row.get('name','')}",
                    description=f"{h} {ct} {r:.2f}%", raw_data=row.to_dict(),
                    confidence=min(0.6+abs(r)*0.1, 0.9)))
            elif r > 0.5:
                signals.append(MarketSignal(
                    source="holder_change", signal_type=SignalType.OPPORTUNITY,
                    level=SignalLevel.HIGH if r > 1 else SignalLevel.MEDIUM,
                    code=str(row.get("code","")), name=str(row.get("name","")),
                    date=str(row.get("date","")),
                    title=f"大额增持: {row.get('name','')}",
                    description=f"{h} {ct} {r:.2f}%", raw_data=row.to_dict(),
                    confidence=min(0.6+abs(r)*0.1, 0.9)))
        return signals

    def _analyze_block_trade(self, start_date, end_date):
        signals = []
        df = pd.read_sql_query("SELECT * FROM stock_block_trade WHERE date>=? AND date<=?",
                               self.conn, params=(start_date, end_date))
        if df.empty:
            return signals
        for _, row in df.iterrows():
            p = float(row.get("premium_rate", 0) or 0)
            a = float(row.get("trade_amount", 0) or 0)
            if p < self.BLOCK_DISCOUNT_THRESHOLD:
                signals.append(MarketSignal(
                    source="block_trade", signal_type=SignalType.RISK,
                    level=SignalLevel.HIGH if p < -0.20 else SignalLevel.MEDIUM,
                    code=str(row.get("code","")), name=str(row.get("name","")),
                    date=str(row.get("date","")),
                    title=f"折价大宗: {row.get('name','')}",
                    description=f"折价率 {p:.1f}%, 成交额 {a:,.0f}万",
                    raw_data=row.to_dict(), confidence=min(0.5+abs(p)*3, 0.9)))
            elif p > 0.05:
                signals.append(MarketSignal(
                    source="block_trade", signal_type=SignalType.OPPORTUNITY,
                    level=SignalLevel.MEDIUM,
                    code=str(row.get("code","")), name=str(row.get("name","")),
                    date=str(row.get("date","")),
                    title=f"溢价大宗: {row.get('name','')}",
                    description=f"溢价率 {p:.1f}%, 成交额 {a:,.0f}万",
                    raw_data=row.to_dict(), confidence=0.6))
        return signals

    def _analyze_institution_research(self, start_date, end_date):
        signals = []
        df = pd.read_sql_query("SELECT * FROM stock_institution_research WHERE date>=? AND date<=?",
                               self.conn, params=(start_date, end_date))
        if df.empty:
            return signals
        daily = df.groupby(["date","code","name"]).agg(
            cnt=("institution","nunique"),
            tp=("inst_type", lambda x: ", ".join(x.dropna().unique()[:5]))
        ).reset_index()
        for _, r in daily.iterrows():
            if r["cnt"] >= self.INST_BATCH_THRESHOLD:
                signals.append(MarketSignal(
                    source="institution", signal_type=SignalType.OPPORTUNITY,
                    level=SignalLevel.HIGH if r["cnt"]>=10 else SignalLevel.MEDIUM,
                    code=str(r.get("code","")), name=str(r.get("name","")),
                    date=str(r.get("date","")),
                    title=f"密集调研: {r.get('name','')}",
                    description=f"{int(r['cnt'])}家机构调研",
                    raw_data=r.to_dict(), confidence=min(0.5+r["cnt"]*0.03, 0.9)))
        return signals

    def get_signal_summary(self, signals):
        by_type = {t.value: 0 for t in SignalType}
        by_level = {l.value: 0 for l in SignalLevel}
        by_source = {}
        risk_codes, opp_codes = set(), set()
        for s in signals:
            by_type[s.signal_type.value] += 1
            by_level[s.level.value] += 1
            by_source[s.source] = by_source.get(s.source, 0) + 1
            if s.signal_type == SignalType.RISK: risk_codes.add(s.code)
            elif s.signal_type == SignalType.OPPORTUNITY: opp_codes.add(s.code)
        return {
            "total": len(signals), "by_type": by_type, "by_level": by_level,
            "by_source": by_source, "risk_codes": list(risk_codes),
            "opportunity_codes": list(opp_codes),
            "top_risk": [s for s in signals if s.signal_type == SignalType.RISK][:5],
            "top_opportunity": [s for s in signals if s.signal_type == SignalType.OPPORTUNITY][:5],
        }

    def get_portfolio_signal_report(self, signals, held_codes):
        held_set = set(str(c) for c in held_codes)
        related = [s for s in signals if s.code in held_set]
        lo = {SignalLevel.HIGH: 0, SignalLevel.MEDIUM: 1, SignalLevel.LOW: 2}
        affected = {}
        for s in related:
            affected.setdefault(s.code, {"code": s.code, "name": s.name, "signals": []})
            affected[s.code]["signals"].append(s)
        al = []
        for code, info in affected.items():
            lvls = [s.level for s in info["signals"]]
            hi = min(lvls, key=lambda l: lo[l])
            al.append({"code": code, "name": info["name"], "signal_count": len(info["signals"]),
                        "highest_level": hi.value,
                        "signals": sorted(info["signals"], key=lambda s: lo[s.level])})
        al.sort(key=lambda x: lo[SignalLevel(x["highest_level"])])
        has_h = any(s.signal_type==SignalType.RISK and s.level==SignalLevel.HIGH for s in related)
        has_m = any(s.signal_type==SignalType.RISK and s.level==SignalLevel.MEDIUM for s in related)
        return {
            "related_count": len(related),
            "related_signals": sorted(related, key=lambda s: lo[s.level]),
            "portfolio_risk_level": "high" if has_h else "medium" if has_m else "low",
            "affected_positions": al,
        }
