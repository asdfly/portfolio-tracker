# ============================================================
# 投资组合跟踪分析系统 - Docker镜像
# 用途: 数据采集 + 定时分析 + Dashboard
# ============================================================
FROM python:3.12-slim

WORKDIR /app

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 项目代码
COPY . .

# 数据目录
RUN mkdir -p /app/data/database /app/data/raw /app/data/processed /app/logs /app/data/reports

# 环境变量（运行时通过 -e 或 .env 注入敏感配置）
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai

# 默认启动Dashboard
EXPOSE 8501
CMD ["python", "-m", "streamlit", "run", "dashboard.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true"]
