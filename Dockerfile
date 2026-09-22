# ============================================================
# 投资组合跟踪分析系统 - Docker镜像
# 用途: 数据采集 + 定时分析 + Dashboard
# 基础镜像: python:3.13-slim，与项目 venv313 (CPython 3.13) 保持一致
# ============================================================
FROM python:3.13-slim

WORKDIR /app

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# 非 root 运行用户（在 COPY 之前创建，便于 --chown 一次性把代码属主设对）
RUN useradd -m -u 10001 appuser

# Python依赖：用 requirements.lock（venv313 pip freeze 的完整 == 快照，可跨机复现）
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock

# 项目代码（属主直接给 appuser，避免运行时只读/无权限）
COPY --chown=appuser:appuser . .

# 数据目录：镜像内只建空壳，真实数据由 VOLUME / -v 挂载注入
#   对应 config/settings.py: DATA_DIR=data, DATABASE_DIR=data/database,
#   BACKUP_DIR=data/backups, REPORT_DIR=report, LOGS_DIR=logs
RUN mkdir -p /app/data/database /app/data/raw /app/data/processed \
             /app/data/backups /app/data/reports /app/report /app/logs && \
    chown -R appuser:appuser /app/data /app/report /app/logs

# 环境变量（运行时通过 -e 或 .env 注入敏感配置）
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai
ENV HOME=/home/appuser

USER appuser

# 持久化：数据库 / 备份 / 原始与派生数据 / 日志
VOLUME ["/app/data", "/app/logs"]

# 默认启动Dashboard
EXPOSE 8501

# 健康检查：探 Streamlit 自带的 /_stcore/health 端点（HTTP 非 2xx 直接抛错退出）
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=4).read()"]

CMD ["python", "-m", "streamlit", "run", "dashboard.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true"]
