"""
run_pipeline — 一键执行完整财务分析管道
========================================
按顺序调用管道中的全部 8 个步骤，从原始同花顺 XLS 数据出发，
一键生成三表标准化输出、核心指标报告和 DCF 估值模型。

使用前提
--------
1. 在 rawdata/ 目录下放置同花顺导出的四个 XLS 文件：
   - {ticker}_debt_year.xls    资产负债表
   - {ticker}_benefit_year.xls 利润表
   - {ticker}_cash_year.xls    现金流量表
   - {ticker}_price.xls        年度价格数据
2. 在 rawdata/Info.csv 中填写：总股本、当前股价、公司简称

管道步骤
--------
1. step1_convert_xls_to_csv.py     同花顺 XLS → 标准化 CSV（保留最近 10 年）
2. step2_check_statements.py       三表一致性检验（5 项勾稽）
3. step3_extract_metrics.py        核心财务指标提取与计算
4. step4_metrics_report.py         完整财务指标报告（含 YoY / CAGR / ROE）
5. rebuild_balance_sheet.py        资产负债表标准化重构（投行口径）
6. rebuild_income_statement.py     利润表标准化重构
7. rebuild_cash_flow.py            现金流量表标准化重构
8. generate_dcf_valuation.py       DCF 估值模型生成（含相对估值）

运行方式
--------
    python run_pipeline.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from pipeline_utils import ensure_output_dirs, validate_rawdata


SCRIPTS = [
    "step1_convert_xls_to_csv.py",      # Step 1: XLS → CSV（同花顺原始数据清洗）
    "step2_check_statements.py",         # Step 2: 三表一致性检验
    "step3_extract_metrics.py",          # Step 3: 核心指标提取与计算（Core_Metrics.xlsx）
    "step4_metrics_report.py",           # Step 4: 完整财务指标报告（含 Markdown）
    "rebuild_balance_sheet.py",          # Step 5: 资产负债表标准化重构
    "rebuild_income_statement.py",       # Step 6: 利润表标准化重构
    "rebuild_cash_flow.py",              # Step 7: 现金流量表标准化重构
    "generate_dcf_valuation.py",         # Step 8: DCF 估值模型
]


def run_script(script_name: str) -> None:
    """执行单个管道脚本，失败时终止整个管道。

    Parameters
    ----------
    script_name : str
        要执行的脚本文件名（相对当前工作目录）

    Raises
    ------
    SystemExit
        脚本返回非 0 退出码时终止管道
    """
    print(f"\n[RUN] {script_name}")
    completed = subprocess.run([sys.executable, script_name], check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Pipeline stopped because {script_name} failed.")


def main() -> None:
    """验证输入数据后按序执行全部管道步骤。"""
    ensure_output_dirs()

    try:
        ticker, raw_files = validate_rawdata()
    except Exception as exc:
        raise SystemExit(f"Input validation failed: {exc}") from exc

    print("Automated Financial Data Analysis for A-shares")
    print("==============================================")
    print(f"Ticker detected: {ticker}")
    print("Input files:")
    for path in raw_files.values():
        print(f"  - {path}")

    for script_name in SCRIPTS:
        if not Path(script_name).exists():
            raise SystemExit(f"Required script not found: {script_name}")
        run_script(script_name)

    print("\nPipeline completed successfully.")
    print("Check the results/ directory for CSV, Excel and Markdown outputs.")


if __name__ == "__main__":
    main()
