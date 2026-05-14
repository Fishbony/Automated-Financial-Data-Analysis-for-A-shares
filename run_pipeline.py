"""
Run the full A-share financial analysis pipeline.

Usage:
    python run_pipeline.py
    python run_pipeline.py "D:/path/to/data"
    python run_pipeline.py --data-dir "D:/path/to/data"
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from pipeline_utils import (
    ensure_output_dirs,
    find_info_file,
    prompt_data_dir_with_dialog,
    resolve_data_dir,
    set_results_dir,
    validate_rawdata,
)


SCRIPTS = [
    "step1_convert_xls_to_csv.py",
    "step2_check_statements.py",
    "step3_extract_metrics.py",
    "step4_metrics_report.py",
    "rebuild_balance_sheet.py",
    "rebuild_income_statement.py",
    "rebuild_cash_flow.py",
    "generate_dcf_valuation.py",
    "generate_html_report.py",
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


def run_script(script_name: str, data_dir: Path | None = None) -> None:
    print(f"\n[RUN] {script_name}")
    command = [sys.executable, script_name]
    if script_name in {"step1_convert_xls_to_csv.py", "generate_dcf_valuation.py", "generate_html_report.py"} and data_dir is not None:
        command.extend(["--data-dir", str(data_dir)])
    env = os.environ.copy()
    completed = subprocess.run(command, check=False, env=env)
    if completed.returncode != 0:
        raise SystemExit(f"Pipeline stopped because {script_name} failed.")


def main() -> None:
    args = parse_args()
    raw_data_dir = args.data_dir_flag or args.data_dir
    data_dir = resolve_data_dir(raw_data_dir) if raw_data_dir else prompt_data_dir_with_dialog()
    data_dir = data_dir.expanduser().resolve()
    results_dir = set_results_dir(data_dir / "results")

    try:
        ticker, raw_files = validate_rawdata(data_dir)
    except Exception as exc:
        raise SystemExit(f"Input validation failed: {exc}") from exc

    ensure_output_dirs()

    print("Automated Financial Data Analysis for A-shares")
    print("==============================================")
    print(f"Ticker detected: {ticker}")
    print(f"Input directory: {data_dir}")
    print(f"Output directory: {results_dir}")
    print("Input files:")
    for path in raw_files.values():
        print(f"  - {path}")

    info_file = find_info_file(data_dir)
    if info_file is None:
        print("\nInfo.csv not found in the input directory. Step 8 (DCF valuation) and Step 9 (HTML dashboard) will be skipped.")
    else:
        print(f"  - {info_file}")

    for script_name in SCRIPTS:
        if script_name == "generate_dcf_valuation.py" and info_file is None:
            continue
        if not Path(script_name).exists():
            raise SystemExit(f"Required script not found: {script_name}")
        if script_name == "generate_html_report.py" and info_file is None:
            continue
        run_script(script_name, data_dir)

    print("\nPipeline completed successfully.")
    print(f"Check {results_dir} for CSV, Excel, Markdown and HTML outputs.")


if __name__ == "__main__":
    main()
