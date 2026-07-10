"""
Step 1/11: convert RoyalFlush XLS exports to standardized CSV files.

Required input files in the data directory:
- {ticker}_debt_year.xls
- {ticker}_benefit_year.xls
- {ticker}_cash_year.xls

Optional input file:
- {ticker}_price.xls
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import xlrd

from afda.input_validation import require_valid_input
from afda.logging_config import get_logger
from afda.pipeline_utils import CSV_DIR, ensure_output_dirs, prompt_data_dir_with_dialog, validate_rawdata

logger = get_logger(__name__)


OUTPUT_DIR = CSV_DIR


def trans_csv(file_name: str, new_name: str) -> None:
    book = xlrd.open_workbook(file_name, ignore_workbook_corruption=True)

    sheet = book.sheet_by_index(0)
    data = [sheet.row_values(i) for i in range(sheet.nrows)]

    df = pd.DataFrame(data[1:], columns=data[1])
    df = df.iloc[1:, ]
    df.index = df.iloc[:, 0]
    df = df.iloc[:, 1:]
    df.replace("--", 0, inplace=True)
    df.columns = df.columns.map(lambda x: str(x).replace(".0", ""))

    df.to_csv(new_name)
    df = pd.read_csv(new_name, index_col=0)
    # Keep rows with partial missing values so the converted CSV stays easy to
    # audit against the original RoyalFlush export. Downstream scripts filter
    # only blank item-name rows before calculation.
    # df.dropna(axis=0, how="any", inplace=True)

    df = df.iloc[:, :10]
    df = df[df.columns[::-1]]

    df.to_csv(new_name)


def trans_price_csv(file_name: str, new_name: str) -> None:
    df = pd.read_csv(file_name, sep="\t", encoding="gbk")
    df.iloc[:, 0] = df.iloc[:, 0].str[:4]
    df = df.iloc[:, :-1]
    df.columns = [
        "Time",
        "Open",
        "High",
        "Low",
        "Close",
        "Change",
        "Amplitude",
        "Volume",
        "Amount",
        "Turnover%",
        "Deal Times",
    ]
    df.index = df.Time
    df = df.iloc[:, 1:]
    df = df.T
    df.index.name = "Time"
    df.to_csv(new_name, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert RoyalFlush XLS exports to CSV.")
    parser.add_argument(
        "data_dir",
        nargs="?",
        default=None,
        help="Directory containing *_debt_year.xls, *_benefit_year.xls and *_cash_year.xls. If omitted, a folder picker will open.",
    )
    parser.add_argument(
        "--data-dir",
        dest="data_dir_flag",
        default=None,
        help="Same as positional data_dir; kept for explicit pipeline usage.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir_flag or args.data_dir
    if data_dir is None:
        data_dir = prompt_data_dir_with_dialog()

    ensure_output_dirs()

    logger.info("Step 1: convert RoyalFlush XLS exports to CSV")
    logger.info("=" * 50)

    require_valid_input(data_dir)

    try:
        stocks_ticker, raw_files = validate_rawdata(data_dir)
    except Exception as exc:
        logger.error("Error: %s", exc)
        raise SystemExit(1) from exc

    logger.info("Ticker detected: %s", stocks_ticker)
    logger.info("Input directory: %s", Path(data_dir))
    logger.info("-" * 50)
    logger.info("Converting financial statements...")

    trans_csv(str(raw_files["balance_sheet"]), str(CSV_DIR / "bs.csv"))
    logger.info("    1. balance sheet saved")

    trans_csv(str(raw_files["profit_loss"]), str(CSV_DIR / "pl.csv"))
    logger.info("    2. income statement saved")

    trans_csv(str(raw_files["cash_flow"]), str(CSV_DIR / "cf.csv"))
    logger.info("    3. cash flow statement saved")

    logger.info("-" * 50)
    logger.info("Converting price file...")

    if "price" in raw_files:
        trans_price_csv(str(raw_files["price"]), str(CSV_DIR / "price.csv"))
        logger.info("    4. price file saved")
    else:
        logger.info("    4. price file not found; skipped")

    logger.info("=" * 50)
    logger.info("Step 1 completed. CSV files were saved to %s", CSV_DIR)


if __name__ == "__main__":
    main()
