"""
投资组合跟踪分析系统配置文件
支持环境变量覆盖: .env 文件或系统环境变量
优先级: 环境变量 > .env文件 > 默认值
"""
import os
from pathlib import Path

# ---------- 环境变量加载 ----------
def _load_env_file(env_path=None):
    """加载 .env 文件（不覆盖已有环境变量）

    Args:
        env_path: 可选的 .env 文件路径。默认读取项目根目录下的 .env。
            显式传入可用于测试，避免读写真实的项目 .env 文件。
    """
    if env_path is None:
        env_path = Path(__file__).parent.parent / ".env"
    else:
        env_path = Path(env_path)
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, _, value = line.partition('=')
                    key = key.strip()
                    value = value.strip().strip("'\"")
                    if key and key not in os.environ:
                        os.environ[key] = value

def env(key: str, default=None):
    """获取环境变量（支持 .env 文件和系统环境变量）"""
    return os.environ.get(key, default)

# 延迟加载（在 PROJECT_ROOT 定义后）

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
_load_env_file()  # 加载 .env 文件

# 数据目录
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
DATABASE_DIR = DATA_DIR / "database"

# 日志目录
LOGS_DIR = PROJECT_ROOT / "logs"

# 报告目录
REPORT_DIR = PROJECT_ROOT / "report"

# 数据库配置
DATABASE_PATH = Path(env('DATABASE_PATH', str(DATABASE_DIR / "portfolio.db")))
BACKUP_DIR = Path(env('BACKUP_DIR', str(PROJECT_ROOT / "data" / "backups")))

# 持仓文件路径（通达信导出）
# 自动查找通达信导出目录中最新的持仓股文件
def _find_latest_position_file() -> str:
    """自动查找通达信导出目录中最新的持仓股文件"""
    export_dir = env("TDX_EXPORT_DIR", r"C:\zd_zsone\T0002\export")
    if not os.path.isdir(export_dir):
        # fallback到历史文件
        return os.path.join(os.path.dirname(__file__), "..", "data", "raw", "positions.tsv")
    candidates = []
    for fname in os.listdir(export_dir):
        if fname.startswith("持仓股") and (fname.endswith(".xls") or fname.endswith(".tsv")):
            fpath = os.path.join(export_dir, fname)
            candidates.append((os.path.getmtime(fpath), fpath))
    if not candidates:
        # fallback
        return os.path.join(os.path.dirname(__file__), "..", "data", "raw", "positions.tsv")
    candidates.sort(reverse=True)
    latest = candidates[0][1]
    return latest


def _extract_position_file_date(file_path: str) -> str:
    """从持仓文件名中提取日期（格式: 持仓股20260427.xls -> 2026-04-27）
    
    返回格式为 YYYY-MM-DD。如果无法提取则返回 None。
    """
    import re
    fname = os.path.basename(file_path)
    # 匹配 "持仓股YYYYMMDD" 格式
    m = re.search(r'持仓股(\d{4})(\d{2})(\d{2})', fname)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


POSITION_FILE = _find_latest_position_file()
POSITION_FILE_DATE = _extract_position_file_date(POSITION_FILE)

# 数据源配置
DATA_SOURCES = {
    "sina": {
        "name": "新浪财经",
        "enabled": True,
        "priority": 1,
        "timeout": 10,
        "retry": 3,
        "delay": 0.3,
        "base_url": "http://hq.sinajs.cn",
        "kline_url": "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    },
    "eastmoney": {
        "name": "东方财富",
        "enabled": True,
        "priority": 2,
        "timeout": 10,
        "retry": 2,
        "delay": 0.5,
        "base_url": "https://push2.eastmoney.com/api"
    },
    "akshare": {
        "name": "AKShare",
        "enabled": True,
        "priority": 3,
        "timeout": 15,
        "retry": 2,
        "delay": 0.5
    }
}

# 性能与缓存配置
CACHE_TTL = {"short": 300, "medium": 600, "long": 3600}  # Streamlit cache TTL (秒)
DOWNSAMPLE_MAX_POINTS = 500       # 图表降采样最大点数
CHART_DAYS = {"short": 120, "default": 250, "long": 5000}  # 图表时间窗口 (交易日)

# 指数代码配置
INDEX_CODES = {
    "sh000001": "上证指数",
    "sz399001": "深证成指",
    "sh000300": "沪深300",
    "sh000905": "中证500",
    "sh000852": "中证1000",
    "sz399006": "创业板指",
    "sh000688": "科创50",
    "sz399673": "创业板50",
    "sz399987": "中证酒",
    "sz399989": "中证医疗",
    "sh000015": "红利指数"
}
# 反向映射：中文名 -> 代码（用于侧边栏选择器转code）
BENCHMARK_NAME_TO_CODE = {v: k for k, v in INDEX_CODES.items()}

# 主要持仓ETF（用于K线分析）
MAJOR_ETFS = [
    "sh512010",  # 医药ETF
    "sh515010",  # 证券ETF
    "sz159992",  # 创新药ETF
    "sh515120",  # 创新药ETF广发
    "sz159267",  # 航天ETF
    "sz159796",  # 电池ETF
    "sh561910",  # 电池ETF招商
    "sh512100",  # 中证1000ETF
    "sh516160",  # 新能源ETF
    "sh510300",  # 沪深300ETF
    "sh588000",  # 科创50ETF
    "sz159819",  # AI ETF
    "sh512810",  # 军工ETF
    "sz159949",  # 创业板50ETF
    "sh511380",  # 可转债ETF
]

# 技术指标配置
TECH_INDICATORS = {
    "ma": {"fast": 5, "slow": 20},
    "macd": {"fast": 12, "slow": 26, "signal": 9},
    "rsi": {"period": 14},
    "kdj": {"k": 9, "d": 3, "j": 3},
    "bollinger": {"period": 20, "std": 2},
    "atr": {"period": 14}
}

# 风险指标配置
RISK_CONFIG = {
    "risk_free_rate": 0.025,  # 无风险利率 2.5%
    "trading_days_per_year": 252,
    "var_confidence": 0.95,
    "max_drawdown_warning": 0.15,  # 15%回撤预警
    "concentration_warning": 0.25   # 25%集中度预警
}

# P0-1: 统一导出，消除收益口径三处硬编码不一致（无风险利率 / 年化天数）
# 所有分析模块（data_loader / risk / factor_attribution）一律引用这两个常量，
# 禁止再出现 0.025 / 0.02 / 252 等字面量。
RISK_FREE_RATE = RISK_CONFIG["risk_free_rate"]
TRADING_DAYS_PER_YEAR = RISK_CONFIG["trading_days_per_year"]

# 通知配置
NOTIFICATION = {
    "enabled": False,  # 默认关闭，需手动配置
    "wecom_webhook": "",  # 企业微信webhook
    "email": {
        "smtp_server": "smtp.qq.com",
        "smtp_port": 465,
        "sender": "",
        "password": "",  # 授权码
        "receiver": ""
    }
}

# 报告配置
REPORT_CONFIG = {
    "output_format": ["html", "pdf"],  # 输出格式
    "chart_dpi": 150,
    "chart_format": "png",
    "retention_days": 90,  # 报告保留天数
    "upload_to_cloud": True
}

# 日志配置
LOGGING = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file_max_bytes": 10 * 1024 * 1024,  # 10MB
    "file_backup_count": 5
}

# 定时任务配置
SCHEDULER = {
    "enabled": False,  # 默认关闭
    "run_time": "15:10",  # 每日执行时间
    "run_days": [0, 1, 2, 3, 4],  # 周一至周五
    "timezone": "Asia/Shanghai"
}


# ==================== 通知配置 ====================
NOTIFICATION_CONFIG = {
    'email': {
        'enabled': env('EMAIL_ENABLED', 'false').lower() == 'true',
        'smtp_server': env('EMAIL_SMTP_SERVER', 'smtp.qq.com'),
        'smtp_port': int(env('EMAIL_SMTP_PORT', '587')),
        'username': env('EMAIL_USERNAME', ''),
        'password': env('EMAIL_PASSWORD', ''),
        'sender': env('EMAIL_USERNAME', ''),
        'recipients': [r.strip() for r in env('EMAIL_RECIPIENTS', '').split(',') if r.strip()],
    },
    'wechat': {
        'enabled': env('WECHAT_ENABLED', 'false').lower() == 'true',
        'webhook_url': env('WECHAT_WEBHOOK_URL', ''),
    }
}

# ==================== 监控告警配置 ====================
MONITOR_CONFIG = {
    'alert_rules': [
        {
            'name': 'daily_loss_limit',
            'condition': 'daily_return',
            'threshold': -3.0,  # 单日跌幅超过3%告警
            'level': 'warning',
            'enabled': True
        },
        {
            'name': 'drawdown_limit',
            'condition': 'max_drawdown',
            'threshold': -10.0,  # 最大回撤超过10%告警
            'level': 'error',
            'enabled': True
        },
        {
            'name': 'concentration_risk',
            'condition': 'concentration_hhi',
            'threshold': 0.5,  # 持仓集中度超过0.5告警
            'level': 'warning',
            'enabled': True
        },
        {
            'name': 'volatility_spike',
            'condition': 'volatility',
            'threshold': 30.0,  # 年化波动率超过30%告警
            'level': 'warning',
            'enabled': True
        },
    ],
    'auto_notify': True,  # 自动发送通知
    'dedup_interval_hours': int(env('ALERT_DEDUP_INTERVAL_HOURS', '6')),
    'stale_threshold_days': int(env('STALE_THRESHOLD_DAYS', '7')),
    'log_level': 'INFO',
}


# ==================== 智能分析配置 ====================
SMART_ANALYSIS_CONFIG = {
    'advice_enabled': env('ADVICE_ENABLED', 'true').lower() != 'false',  # 启用智能建议
    'backtest_enabled': True,  # 启用回测分析
    'min_confidence': 0.6,  # 建议最小置信度
    'max_advices': 10,  # 最大建议数量
    'rebalance_threshold': 0.05,  # 再平衡阈值（5%偏离）
    'momentum_lookback': 20,  # 动量观察期（交易日）
    'risk_parity_target': 0.2,  # 风险平价目标波动率
}

# ==================== ETF 行业分类 ====================
ETF_CATEGORIES = {
    # 医药
    "512010": {"name": "医药ETF易方达", "sector": "医药", "color": "#22c55e"},
    "159992": {"name": "创新药ETF银华", "sector": "医药", "color": "#22c55e"},
    "515120": {"name": "创新药ETF广发", "sector": "医药", "color": "#22c55e"},
    # 金融
    "515010": {"name": "证券ETF华夏", "sector": "金融", "color": "#58a6ff"},
    # 军工
    "512810": {"name": "军工ETF华宝", "sector": "军工", "color": "#ef4444"},
    "159267": {"name": "航天ETF华安", "sector": "军工", "color": "#ef4444"},
    # 新能源
    "516160": {"name": "新能源ETF南方", "sector": "新能源", "color": "#f59e0b"},
    "561910": {"name": "电池ETF招商", "sector": "新能源", "color": "#f59e0b"},
    "159796": {"name": "电池ETF汇添富", "sector": "新能源", "color": "#f59e0b"},
    # 科技/AI
    "159819": {"name": "人工智能ETF易方达", "sector": "科技", "color": "#a855f7"},
    "159770": {"name": "机器人ETF天弘", "sector": "科技", "color": "#a855f7"},
    # 已清仓：2026-07-30 后连续 46 天无快照，而同批其余 22 只场内标的每日均有快照；
    # 与 docs/handover/07_known_data_issues.md「已清仓」记载一致。
    # 组合口径一律排除（35 只 = 22 场内 + 13 场外）。
    # 注意：此处保留条目不删除（ETF_CATEGORIES 被当作 ETF 权威分母，删除会改变分母口径）；
    # 采集/再平衡侧如需过滤，请判断 info.get("delisted")，不要依赖 key 是否存在。
    "159732": {"name": "消费电子ETF华夏", "sector": "科技", "color": "#a855f7",
               "delisted": True, "delisted_date": "2026-07-30",
               "delisted_note": "已清仓：末次快照 2026-07-30，此后 46 天连续缺席，组合口径排除"},
    # 宽基
    "510300": {"name": "沪深300ETF华泰柏瑞", "sector": "宽基", "color": "#8b949e"},
    "159300": {"name": "沪深300ETF富国", "sector": "宽基", "color": "#8b949e"},
    "510500": {"name": "中证500ETF南方", "sector": "宽基", "color": "#8b949e"},
    "512100": {"name": "中证1000ETF南方", "sector": "宽基", "color": "#8b949e"},
    "159949": {"name": "创业板50ETF华安", "sector": "宽基", "color": "#8b949e"},
    "588000": {"name": "科创50ETF华夏", "sector": "宽基", "color": "#8b949e"},
    # 红利
    "159220": {"name": "港股通红利低波ETF华宝", "sector": "红利", "color": "#06b6d4"},
    "563020": {"name": "红利低波ETF易方达", "sector": "红利", "color": "#06b6d4"},
    # 债券
    "511520": {"name": "政金债ETF富国", "sector": "债券", "color": "#ec4899"},
    "159650": {"name": "国开债ETF博时", "sector": "债券", "color": "#ec4899"},
    "511380": {"name": "可转债ETF博时", "sector": "可转债", "color": "#ec4899"},
    # === 场外基金 ===
    # 现金管理
    "880013": {"name": "招商资管智远天添利货币", "sector": "现金管理", "color": "#94a3b8"},
    # 混合/偏债
    "519770": {"name": "交银优择回报灵活配置混合A", "sector": "混合/偏债", "color": "#fb923c"},
    # 宽基
    "007994": {"name": "华夏中证500指数增强A", "sector": "宽基", "color": "#8b949e"},
    # 红利
    "100032": {"name": "富国中证红利指数增强前端", "sector": "红利", "color": "#06b6d4"},
    # 科技/TMT
    "001323": {"name": "东吴移动互联灵活配置混合A", "sector": "科技", "color": "#a855f7"},
    # 混合/灵活配置
    "166301": {"name": "华商新趋势优选灵活配置混合", "sector": "混合/灵活配置", "color": "#f472b6"},
    "001407": {"name": "景顺长城稳健回报灵活配置混合C", "sector": "混合/灵活配置", "color": "#f472b6"},
    "008269": {"name": "大成睿享混合A", "sector": "混合/灵活配置", "color": "#f472b6"},
    "001437": {"name": "易方达瑞享灵活配置混合I", "sector": "混合/灵活配置", "color": "#f472b6"},
    "002152": {"name": "华宝核心优势混合", "sector": "混合/灵活配置", "color": "#f472b6"},
    "001765": {"name": "前海开源嘉鑫混合A类", "sector": "混合/灵活配置", "color": "#f472b6"},
}

# ==================== 交易单位 ====================
# 场内 ETF 最小交易单位：1 手 = 100 份
ETF_LOT_SIZE = 100

# 场外基金代码集合（按金额申购/赎回，无「手」概念，不受 100 份/手 约束）
# 口径来源：portfolio_snapshots 全历史 36 只标的 − etf_technical 的 23 只场内 ETF
# 复核见 docs/handover/07_known_data_issues.md「场外标的清单（13 只，供核对）」
OTC_FUND_CODES = frozenset({
    "001194",  # 景顺长城稳健回报灵活配置混合A
    "001323",  # 东吴移动互联混合A
    "001407",  # 景顺长城稳健回报灵活配置混合C
    "001437",  # 易方达瑞享灵活配置混合I
    "001765",  # 前海开源嘉鑫混合A类
    "002152",  # 华宝核心优势混合
    "007994",  # 华夏中证500指数增强A
    "008269",  # 大成睿享混合A
    "027293",  # 东吴产业趋势混合A
    "100032",  # 富国中证红利指数增强前端
    "166301",  # 华商新趋势优选灵活配置混合（LOF）
    "519770",  # 交银优择回报灵活配置混合A
    "880013",  # 天添利（现金管理类）
})


def is_otc_fund(code: str) -> bool:
    """场外基金（按金额申赎）→ True；场内 ETF（按手交易）→ False。"""
    return str(code) in OTC_FUND_CODES


# ==================== 已清仓 / 退市标的 ====================
# 这些标的在 portfolio_snapshots 里仍有历史残留快照，但**实际已清仓**，
# 不能进入当前持仓集合，否则会被当成真实持仓生成调仓建议（159732 曾因此凭空
# 多出一笔 35,783 元的买入建议）。
#
# 判定硬证据（data-engineer 提供）：159732 自 2026-07-31 起连续 46 天未出现在
# 任何快照中，而同批 22 只场内标的每天都在；另见
# docs/handover/07_known_data_issues.md「159732 已清仓」。
DELISTED_CODES = frozenset({
    "159732",  # 消费电子ETF华夏，2026-07-30 后清仓
})


def is_delisted(code: str) -> bool:
    """已清仓/退市标的 → True，不进入当前持仓集合。

    两处来源取并集：
    1. DELISTED_CODES 常量（当前唯一生效来源）；
    2. ETF_CATEGORIES 条目上带 {"delisted": True} 的标记（data-engineer 后续会补，
       补上后无需再改本函数）。
    """
    c = str(code)
    if c in DELISTED_CODES:
        return True
    entry = ETF_CATEGORIES.get(c)
    return bool(isinstance(entry, dict) and entry.get("delisted"))


# ==================== 观察名单（已清仓但保持关注） ====================
# 与 DELISTED_CODES 是**并列关系，不是替代关系**：159732 同时命中两者。
#
# 语义（新增消费方前必读，别用错）：
#   ✅ 采集：照常补 etf_price_history（OHLCV）+ etf_technical（技术指标），
#      供前端「观察区」展示走势与技术面；
#   ❌ 不进持仓统计 —— 不计入 portfolio_snapshots / portfolio_summary 的任何口径；
#   ❌ 不进再平衡 —— 不生成任何买入/卖出建议（159732 曾因未被排除而凭空多出一笔
#      35,783 元买入建议，见 DELISTED_CODES 处注释）；
#   ❌ 不进预测底座 —— 不写 etf_features / etf_forward_returns，不参与任何模型训练
#      或推理（build_prediction_base 的标的域来自 resolve_target_codes，该函数已
#      排除 delisted，所以只写行情表是安全的；改动前请先复验这条）；
#
# 一句话：**给它数据，不给它决策权。**
WATCHLIST_CODES = frozenset({
    "159732",  # 消费电子ETF华夏，2026-07-30 后清仓，用户要求保持关注
})


def is_watchlist(code: str) -> bool:
    """观察名单标的 → True：只保留行情/技术面采集，不进持仓/再平衡/预测底座。

    注意：本函数**不**解除 is_delisted() 的排除。两者必须同时为真的场景
    （如 159732）是设计意图，不是冲突——delisted 管"别拿它做决策"，
    watchlist 管"但它的数据要继续采"。
    """
    return str(code) in WATCHLIST_CODES


# ==================== 快照披露节奏（决定「陈旧告警」阈值） ====================
# 不能对所有标的用同一个阈值：场外基金**没有日更链路**，只在每月最后一天导入一次
# （data-engineer 2026-09-15 确认：13 只场外全部只出现在 01-31/02-28/…/07-31 等月末）。
# 用场内那套 7 天阈值，场外会整月误报，把正常披露节奏当成采集故障。
#
# 35 = 最长月 31 天 + 4 天导入延迟宽限。若场外日后接上日更链路，请把这个值收紧到 7~10。
SNAPSHOT_STALE_DAYS_OTC = 35

# 按产品实际披露频率的显式覆盖（覆盖上面的分类默认值）
SNAPSHOT_STALE_DAYS_OVERRIDE = {
    # 东吴产业趋势混合A：净值每周五更新，天然比日更标的晚 ≤7 天，不是采集失败
    "027293": 14,
}


def stale_threshold_days(code: str, default: int = 7) -> int:
    """按标的披露节奏返回「快照陈旧」告警阈值（自然日）。

    default 为场内 ETF 的日更阈值，由调用方传入（rebalance_engine.STALE_SNAPSHOT_DAYS）。
    """
    c = str(code)
    if c in SNAPSHOT_STALE_DAYS_OVERRIDE:
        return SNAPSHOT_STALE_DAYS_OVERRIDE[c]
    if is_otc_fund(c):
        return SNAPSHOT_STALE_DAYS_OTC
    return default

# 行业颜色映射（用于图表）
SECTOR_COLORS = {
    "医药": "#22c55e",
    "金融": "#58a6ff",
    "军工": "#ef4444",
    "新能源": "#f59e0b",
    "科技": "#a855f7",
    "宽基": "#8b949e",
    "红利": "#06b6d4",
    "债券": "#ec4899",
    "可转债": "#f43f5e",
    "现金管理": "#94a3b8",
    "混合/灵活配置": "#f472b6",
    "混合/偏债": "#fb923c",
}

# ==================== 再平衡：类别基准权重（分层，非等权） ====================
# 用于 propose_layered 策略：不再把所有标的拉向 1/n 等权，
# 而是按资产类别设定基准权重，类别内再按当前市值占比分配（类别内也不均）。
# 规则：
#   - 在表中的 sector -> 目标总权重 = 该基准值
#   - 不在表中的 sector（现金管理 / 混合类 / 未知）-> 保持当前占比（不强行再平衡）
#   - 受管控 sector 的基准和若与"保持类"当前占比叠加后 ≠ 1，则按比例缩放受管控部分归一化
# 注意：基准值之和不必预先等于 1，引擎会自动归一化；此处给出的是"理想配置偏向"。
SECTOR_TARGET_WEIGHTS = {
    "宽基": 0.35,      # 核心底仓，最大头
    "债券": 0.20,      # 稳健压舱（纯利率债：政金债/国开债）
    "可转债": 0.03,    # 可转债：股债混合，单独成 sleeve，不计入纯债桶
    "医药": 0.10,      # 行业主题
    "金融": 0.07,
    "军工": 0.07,
    "新能源": 0.06,
    "科技": 0.05,
    "红利": 0.05,
    "其他": 0.05,      # 兜底：未知 sector 的小幅配置
}
# 再平衡收缩系数（shrinkage）：触发后再平衡的"完整度"系数。
# 目标权重 = (1-λ)·当前 + λ·分层战略基准。
#   λ=1.0 -> 一步恢复到战略权重（推荐，战略纪律最严格，本项目默认）
#   λ<1.0 -> 渐进式再平衡，单次只移动到目标的一部分（降低交易冲击）
# 重要：触发判断由"类别偏离 > SECTOR_DEVIATION_THRESHOLD"决定，与 λ 无关。
#       旧版用 λ=0.5 把目标拉向当前权重、又只看单标偏离，导致大类偏离 24% 也被判无需调仓（P0）。
REBALANCE_SHRINKAGE = 1.0

# 类别偏离容忍阈值（绝对权重差）：任一资产类别「当前权重 vs 战略权重」的偏离超过该值即触发再平衡。
# 这是战略配置纪律的"护栏"，独立于单标偏离阈值（rebalance_threshold）。
# 5%（500bps）是机构常用的再平衡带宽（rebalancing band）默认值；债券等低波动类可更窄。
SECTOR_DEVIATION_THRESHOLD = 0.05

# 战术留痕（Tactical Overrides）：主观战术超配的类别 -> 相对权重。
# 引擎在分层再平衡时用这里的值替换 SECTOR_TARGET_WEIGHTS 中对应类别的战略基准，
# 从而「尊重」你的主观观点、只对「漂移」部分触发再平衡（而非与战术对着干）。
# 注意：这些值按「受管控类别池内的相对权重」解释，引擎会整体归一化到 100%，
# 因此填大于战略基准的值 = 相对超配，填更小的值 = 相对低配。
# 留空 {} = 不启用战术留痕，完全按战略基准执行。
# 例：{"军工": 0.18, "医药": 0.24} 表示相对超配军工/医药。
# 当前启用：军工、医药战术超配（+5pp，相对战略基准 0.07/0.10）。
# 引擎替换对应类别的战略基准后，受管控类别池自动归一化，其余类别按比例让位。
TACTICAL_OVERRIDES: dict = {"军工": 0.12, "医药": 0.15}

# 组合风险预算阈值（用于 advisor 风险指标）
PORTFOLIO_BETA_BUDGET = 1.0       # 组合加权 Beta 上限，超过则提示降低高 Beta 敞口
BOND_UNDER_TARGET_TOL = 0.02      # 债券实际占比低于(目标-该值)时提示补足
