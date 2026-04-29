"""
投资组合跟踪分析系统配置文件
"""
import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

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
DATABASE_PATH = DATABASE_DIR / "portfolio.db"

# 持仓文件路径（通达信导出）
# 自动查找通达信导出目录中最新的持仓股文件
def _find_latest_position_file() -> str:
    """自动查找通达信导出目录中最新的持仓股文件"""
    export_dir = r"C:\zd_zsone\T0002\export"
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

POSITION_FILE = _find_latest_position_file()

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
        'enabled': False,  # 是否启用邮件通知
        'smtp_server': 'smtp.qq.com',  # SMTP服务器
        'smtp_port': 587,
        'username': 'your_email@qq.com',
        'password': 'your_auth_code',  # 授权码
        'sender': 'your_email@qq.com',
        'recipients': ['recipient@example.com'],  # 收件人列表
    },
    'wechat': {
        'enabled': False,  # 是否启用企业微信通知
        'webhook_url': 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY',
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
    'log_level': 'INFO',
}


# ==================== 智能分析配置 ====================
SMART_ANALYSIS_CONFIG = {
    'advice_enabled': True,  # 启用智能建议
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
    "159732": {"name": "消费电子ETF华夏", "sector": "科技", "color": "#a855f7"},
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
    "511380": {"name": "可转债ETF博时", "sector": "债券", "color": "#ec4899"},
}

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
}
