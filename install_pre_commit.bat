@echo off
echo 安装pre-commit...
pip install pre-commit

echo 安装pre-commit钩子...
pre-commit install

echo 运行pre-commit检查所有文件...
pre-commit run --all-files

echo 完成!
pause
