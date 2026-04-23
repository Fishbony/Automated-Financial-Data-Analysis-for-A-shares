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
    "price": "_price.xls",
}


def ensure_output_dirs() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)


def detect_ticker(raw_dir: Path = RAW_DIR) -> str:
    tickers = set()
    for suffix in REQUIRED_SUFFIXES.values():
        for path in raw_dir.glob(f"*{suffix}"):
            ticker = path.name[: -len(suffix)]
            if ticker:
                tickers.add(ticker)

    if not tickers:
        raise ValueError(
            "No valid RoyalFlush export files were found in rawdata/. "
            "Expected files like 600406_debt_year.xls and 600406_price.xls."
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

    if missing:
        joined = ", ".join(missing)
        raise FileNotFoundError(
            "Missing required input files in rawdata/: "
            f"{joined}"
        )

    return ticker, files
