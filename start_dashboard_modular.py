#!/usr/bin/env python3
"""
启动投资组合跟踪分析系统（模块化版本）
"""

import subprocess
import sys
import os

def main():
    # 切换到项目目录
    project_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_dir)
    
    print("启动投资组合跟踪分析系统（模块化版本）...")
    print()
    
    # 检查Python环境
    print(f"Python版本: {sys.version}")
    
    # 启动Streamlit应用
    print("启动Streamlit应用...")
    subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard_main.py", "--server.port", "8501"])

if __name__ == "__main__":
    main()
