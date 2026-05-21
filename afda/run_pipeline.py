"""
Run the full A-share financial analysis pipeline.

Usage:
    python -m afda.run_pipeline
    python -m afda.run_pipeline "D:/path/to/data"
    python -m afda.run_pipeline --data-dir "D:/path/to/data"
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from afda.input_validation import require_valid_input
from afda.pipeline_utils import (
    ensure_output_dirs,
    find_info_file,
    prompt_data_dir_with_dialog,
    resolve_data_dir,
    set_results_dir,
    validate_rawdata,
)


PIPELINE_MODULES = [
    "step1_convert_xls_to_csv",
    "step2_check_statements",
    "step3_extract_metrics",
    "step4_metrics_report",
    "rebuild_balance_sheet",
    "rebuild_income_statement",
    "rebuild_cash_flow",
    "validate_rebuilt_statements",
    "analyze_rebuilt_statements",
    "generate_dcf_valuation",
    "generate_html_report",
]

DATA_DIR_MODULES = {
    "step1_convert_xls_to_csv",
    "validate_rebuilt_statements",
    "analyze_rebuilt_statements",
    "generate_dcf_valuation",
    "generate_html_report",
}


def log(message: str = "") -> None:
    print(message, flush=True)


def modules_for_run(has_info: bool) -> list[str]:
    if has_info:
        return PIPELINE_MODULES
    return [
        module_name
        for module_name in PIPELINE_MODULES
        if module_name not in {"generate_dcf_valuation", "generate_html_report"}
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the A-share financial analysis pipeline.")
    parser.add_argument(
        "data_dir",
        nargs="?",
        default=None,
        help="Directory containing the statement XLS files. If omitted, a folder picker will open.",
    )
    parser.add_argument(
        "--data-dir",
        dest="data_dir_flag",
        default=None,
        help="Same as positional data_dir.",
    )
    return parser.parse_args()


def run_module(module_name: str, data_dir: Path | None = None) -> None:
    log(f"\n[RUN] afda.{module_name}")
    command = [sys.executable, "-m", f"afda.{module_name}"]
    if module_name in DATA_DIR_MODULES and data_dir is not None:
        command.extend(["--data-dir", str(data_dir)])
    completed = subprocess.run(command, check=False, env=os.environ.copy())
    if completed.returncode != 0:
        raise SystemExit(f"Pipeline stopped because afda.{module_name} failed.")


def main() -> None:
    args = parse_args()
    raw_data_dir = args.data_dir_flag or args.data_dir
    data_dir = resolve_data_dir(raw_data_dir) if raw_data_dir else prompt_data_dir_with_dialog()
    data_dir = data_dir.expanduser().resolve()
    results_dir = set_results_dir(data_dir / "results")

    require_valid_input(data_dir)

    try:
        ticker, raw_files = validate_rawdata(data_dir)
    except Exception as exc:
        raise SystemExit(f"Input validation failed: {exc}") from exc

    ensure_output_dirs()

    log("Automated Financial Data Analysis for A-shares")
    log("==============================================")
    log(f"Ticker detected: {ticker}")
    log(f"Input directory: {data_dir}")
    log(f"Output directory: {results_dir}")
    log("Input files:")
    for path in raw_files.values():
        log(f"  - {path}")

    info_file = find_info_file(data_dir)
    if info_file is None:
        log("\nInfo.csv not found in the input directory. DCF valuation and HTML dashboard will be skipped.")
    else:
        log(f"  - {info_file}")

    for module_name in modules_for_run(has_info=info_file is not None):
        run_module(module_name, data_dir)

    log("\nPipeline completed successfully.")
    log(f"Check {results_dir} for CSV, Excel, Markdown and HTML outputs.")


if __name__ == "__main__":
    main()

