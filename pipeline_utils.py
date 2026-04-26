"""
pipeline_utils — 管道公共工具函数
==================================
提供目录管理、股票代码识别、原始数据文件校验等全局共用工具，
被管道中的各步骤脚本导入使用。

主要功能
--------
ensure_output_dirs()
    创建 results/ 和 results/csv/ 目录（幂等）

detect_ticker(raw_dir)
    从 rawdata/ 中的文件名推断股票代码
    例：600406_debt_year.xls → "600406"

validate_rawdata(raw_dir)
    校验 rawdata/ 下的四个必需文件是否均存在：
    - {ticker}_debt_year.xls    资产负债表
    - {ticker}_benefit_year.xls 利润表
    - {ticker}_cash_year.xls    现金流量表
    - {ticker}_price.xls        价格数据

导出常量
--------
RAW_DIR     rawdata/ 目录 Path 对象
RESULTS_DIR results/ 目录 Path 对象
CSV_DIR     results/csv/ 目录 Path 对象
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple


RAW_DIR = Path("./rawdata")
RESULTS_DIR = Path("./results")
CSV_DIR = RESULTS_DIR / "csv"

REQUIRED_SUFFIXES: Dict[str, str] = {
    "balance_sheet": "_debt_year.xls",
    "profit_loss": "_benefit_year.xls",
    "cash_flow": "_cash_year.xls",
}

OPTIONAL_SUFFIXES: Dict[str, str] = {
    "price": "_price.xls",
}


def ensure_output_dirs() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)


def detect_ticker(raw_dir: Path = RAW_DIR) -> str:
    tickers = set()
    for suffix in list(REQUIRED_SUFFIXES.values()) + list(OPTIONAL_SUFFIXES.values()):
        for path in raw_dir.glob(f"*{suffix}"):
            ticker = path.name[: -len(suffix)]
            if ticker:
                tickers.add(ticker)

    if not tickers:
        raise ValueError(
            "No valid RoyalFlush export files were found in rawdata/. "
            "Expected files like 600406_debt_year.xls and 600406_benefit_year.xls."
        )

    if len(tickers) > 1:
        joined = ", ".join(sorted(tickers))
        raise ValueError(
            "Multiple tickers were detected in rawdata/. "
            f"Please keep only one stock's files at a time. Detected: {joined}"
        )

    return next(iter(tickers))


def validate_rawdata(raw_dir: Path = RAW_DIR) -> Tuple[str, Dict[str, Path]]:
    if not raw_dir.exists():
        raise FileNotFoundError(
            "rawdata/ directory not found. Please create it and place the original "
            "RoyalFlush export files inside."
        )

    ticker = detect_ticker(raw_dir)
    files: Dict[str, Path] = {}
    missing = []

    for label, suffix in REQUIRED_SUFFIXES.items():
        path = raw_dir / f"{ticker}{suffix}"
        if path.exists():
            files[label] = path
        else:
            missing.append(path.name)

    for label, suffix in OPTIONAL_SUFFIXES.items():
        path = raw_dir / f"{ticker}{suffix}"
        if path.exists():
            files[label] = path

    if missing:
        joined = ", ".join(missing)
        raise FileNotFoundError(
            "Missing required input files in rawdata/: "
            f"{joined}. The price file is optional."
        )

    return ticker, files
