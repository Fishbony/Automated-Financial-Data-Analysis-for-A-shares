"""
pipeline_utils — 管道公共工具函数
==================================
提供目录管理、股票代码识别、输入数据文件校验等全局共用工具，
被管道中的各步骤脚本导入使用。

主要功能
--------
ensure_output_dirs()
    创建 results/ 和 results/csv/ 目录（幂等）

detect_ticker(raw_dir)
    从输入目录中的文件名推断股票代码
    例：600406_debt_year.xls → "600406"

validate_rawdata(raw_dir)
    校验输入目录下的必需文件是否均存在：
    - {ticker}_debt_year.xls    资产负债表
    - {ticker}_benefit_year.xls 利润表
    - {ticker}_cash_year.xls    现金流量表
    - {ticker}_price.xls        价格数据

导出常量
--------
RAW_DIR     demo/rawdata/ 示例输入目录 Path 对象
RESULTS_DIR results/ 目录 Path 对象
CSV_DIR     results/csv/ 目录 Path 对象
"""

from __future__ import annotations

import os
import csv
from pathlib import Path
from typing import Dict, Optional, Tuple


RAW_DIR = Path("./demo/rawdata")
RESULTS_DIR = Path(os.environ.get("AFDA_RESULTS_DIR", "./results"))
CSV_DIR = RESULTS_DIR / "01_csv"
CHECKS_DIR = RESULTS_DIR / "02_checks"
METRICS_DIR = RESULTS_DIR / "03_metrics"
REBUILT_DIR = RESULTS_DIR / "04_rebuilt_statements"
BS_REBUILT_DIR = REBUILT_DIR / "balance_sheet"
PL_REBUILT_DIR = REBUILT_DIR / "income_statement"
CF_REBUILT_DIR = REBUILT_DIR / "cash_flow"
VALUATION_DIR = RESULTS_DIR / "05_valuation"
ASSETS_DIR = RESULTS_DIR / "_assets"

REQUIRED_SUFFIXES: Dict[str, str] = {
    "balance_sheet": "_debt_year.xls",
    "profit_loss": "_benefit_year.xls",
    "cash_flow": "_cash_year.xls",
}

OPTIONAL_SUFFIXES: Dict[str, str] = {
    "price": "_price.xls",
}


def ensure_output_dirs() -> None:
    for path in [
        RESULTS_DIR,
        CSV_DIR,
        CHECKS_DIR,
        METRICS_DIR,
        BS_REBUILT_DIR,
        PL_REBUILT_DIR,
        CF_REBUILT_DIR,
        VALUATION_DIR,
        ASSETS_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def set_results_dir(results_dir: Path | str) -> Path:
    global RESULTS_DIR, CSV_DIR, CHECKS_DIR, METRICS_DIR, REBUILT_DIR
    global BS_REBUILT_DIR, PL_REBUILT_DIR, CF_REBUILT_DIR, VALUATION_DIR, ASSETS_DIR
    RESULTS_DIR = Path(results_dir)
    CSV_DIR = RESULTS_DIR / "01_csv"
    CHECKS_DIR = RESULTS_DIR / "02_checks"
    METRICS_DIR = RESULTS_DIR / "03_metrics"
    REBUILT_DIR = RESULTS_DIR / "04_rebuilt_statements"
    BS_REBUILT_DIR = REBUILT_DIR / "balance_sheet"
    PL_REBUILT_DIR = REBUILT_DIR / "income_statement"
    CF_REBUILT_DIR = REBUILT_DIR / "cash_flow"
    VALUATION_DIR = RESULTS_DIR / "05_valuation"
    ASSETS_DIR = RESULTS_DIR / "_assets"
    os.environ["AFDA_RESULTS_DIR"] = str(RESULTS_DIR)
    return RESULTS_DIR


def resolve_data_dir(raw_dir: Optional[Path | str] = None) -> Path:
    return Path(raw_dir) if raw_dir is not None else RAW_DIR


def select_data_dir_with_dialog(initial_dir: Optional[Path | str] = None) -> Optional[Path]:
    """Open a native folder picker and return the selected input directory.

    Tkinter ships with standard Python on Windows. If it is unavailable, or the
    script is running in an environment without a display, callers can fall back
    to console input.
    """

    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None

    start_dir = Path(initial_dir) if initial_dir is not None else RAW_DIR
    if not start_dir.exists():
        start_dir = Path.cwd()

    root = None
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        selected = filedialog.askdirectory(
            title="请选择包含同花顺导出文件的输入文件夹",
            initialdir=str(start_dir.expanduser().resolve()),
            mustexist=True,
        )
    except Exception:
        return None
    finally:
        if root is not None:
            root.destroy()

    return Path(selected) if selected else None


def prompt_data_dir_with_dialog(initial_dir: Optional[Path | str] = None) -> Path:
    selected = select_data_dir_with_dialog(initial_dir)
    if selected is not None:
        return selected

    while True:
        value = input("Enter data folder path: ").strip().strip('"').strip("'")
        if not value:
            print("Path cannot be empty. Please try again.")
            continue
        return Path(value)


def find_info_file(raw_dir: Path = RAW_DIR) -> Optional[Path]:
    for path in raw_dir.iterdir() if raw_dir.exists() else []:
        if path.is_file() and path.name.lower() == "info.csv":
            return path
    return None


def read_info_items(info_path: Path) -> Dict[str, str]:
    """Read Info.csv into a stripped key/value mapping.

    Expected format:
        项目,值
        公司名称,海大集团
        公司代码,002311
    """

    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "gbk"):
        try:
            with info_path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames or "项目" not in reader.fieldnames:
                    return {}
                value_col = reader.fieldnames[-1]
                items: Dict[str, str] = {}
                for row in reader:
                    key = str(row.get("项目", "")).strip()
                    value = str(row.get(value_col, "")).strip()
                    if key and value and value.lower() != "nan":
                        items[key] = value
                return items
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return {}


def company_display_name(data_dir: Path, ticker: Optional[str] = None, fallback: Optional[str] = None) -> str:
    """Return 公司名称（公司代码） when Info.csv provides both fields."""

    fallback_name = fallback or ticker or data_dir.resolve().name or "unknown-company"
    info_path = find_info_file(data_dir)
    if info_path is None:
        return fallback_name

    try:
        info = read_info_items(info_path)
    except Exception:
        return fallback_name

    name = info.get("公司名称") or info.get("公司简称") or fallback_name
    code = info.get("公司代码") or ticker
    if code and code not in name:
        return f"{name}（{code}）"
    return name


def detect_ticker(raw_dir: Path = RAW_DIR) -> str:
    raw_dir = resolve_data_dir(raw_dir)
    tickers = set()
    for suffix in list(REQUIRED_SUFFIXES.values()) + list(OPTIONAL_SUFFIXES.values()):
        for path in raw_dir.glob(f"*{suffix}"):
            ticker = path.name[: -len(suffix)]
            if ticker:
                tickers.add(ticker)

    if not tickers:
        raise ValueError(
            f"No valid RoyalFlush export files were found in {raw_dir}. "
            "Expected files like 600406_debt_year.xls and 600406_benefit_year.xls."
        )

    if len(tickers) > 1:
        joined = ", ".join(sorted(tickers))
        raise ValueError(
            f"Multiple tickers were detected in {raw_dir}. "
            f"Please keep only one stock's files at a time. Detected: {joined}"
        )

    return next(iter(tickers))


def validate_rawdata(raw_dir: Path = RAW_DIR) -> Tuple[str, Dict[str, Path]]:
    raw_dir = resolve_data_dir(raw_dir)
    if not raw_dir.exists():
        raise FileNotFoundError(
            f"Input directory not found: {raw_dir}. Please create it and place the original "
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
            f"Missing required input files in {raw_dir}: "
            f"{joined}. The price file is optional."
        )

    return ticker, files
