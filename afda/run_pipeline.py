"""
Run the full A-share financial analysis pipeline.

Usage:
    python -m afda.run_pipeline
    python -m afda.run_pipeline "D:/path/to/data"
    python -m afda.run_pipeline --data-dir "D:/path/to/data"
    python -m afda.run_pipeline --subprocess   # force subprocess mode
    python -m afda.run_pipeline --resume        # skip completed steps
    python -m afda.run_pipeline --force         # ignore checkpoint, run all
"""

from __future__ import annotations

import argparse
import importlib
import os
import subprocess
import sys
from pathlib import Path

from afda.checkpoint import Checkpoint
from afda.input_validation import require_valid_input
from afda.logging_config import get_logger
from afda.pipeline_utils import (
    ensure_output_dirs,
    find_info_file,
    prompt_data_dir_with_dialog,
    resolve_data_dir,
    set_results_dir,
    validate_rawdata,
)

logger = get_logger(__name__)


# Pipeline execution order (11 steps).
# Step #  Module                          Output dir
# -------  ------------------------------  ---------------------------------
#   1      step1_convert_xls_to_csv        01_csv/
#   2      step2_check_statements          02_checks/
#   3      step3_extract_metrics           03_metrics/
#   4      step4_metrics_report            03_metrics/
#   5      rebuild_balance_sheet           04_rebuilt_statements/balance_sheet/
#   6      rebuild_income_statement        04_rebuilt_statements/income_statement/
#   7      rebuild_cash_flow               04_rebuilt_statements/cash_flow/
#   8      validate_rebuilt_statements     04_rebuilt_statements/rebuilt_statement_checks/
#   9      analyze_rebuilt_statements      rebuilt_statement_deepseek_analysis.md
#  10      generate_dcf_valuation          05_valuation/
#  11      generate_html_report            financial_dcf_dashboard.html
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

# Modules that accept --data-dir via argparse
DATA_DIR_MODULES = {
    "step1_convert_xls_to_csv",
    "validate_rebuilt_statements",
    "analyze_rebuilt_statements",
    "generate_dcf_valuation",
    "generate_html_report",
}


def log(message: str = "") -> None:
    logger.info(message)


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
    parser.add_argument(
        "--subprocess",
        action="store_true",
        default=False,
        help="Force subprocess mode for all modules (slower but isolated).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Skip steps that already completed successfully (checkpoint-based).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Ignore existing checkpoint and run all steps from scratch.",
    )
    return parser.parse_args()


def run_module_subprocess(module_name: str, data_dir: Path | None = None) -> None:
    """Run a pipeline module as a subprocess (original behavior)."""
    log(f"\n[RUN:subprocess] afda.{module_name}")
    command = [sys.executable, "-m", f"afda.{module_name}"]
    if module_name in DATA_DIR_MODULES and data_dir is not None:
        command.extend(["--data-dir", str(data_dir)])
    completed = subprocess.run(command, check=False, env=os.environ.copy())
    if completed.returncode != 0:
        raise SystemExit(f"Pipeline stopped because afda.{module_name} failed.")


def run_module_inprocess(module_name: str, data_dir: Path | None = None) -> None:
    """Run a pipeline module in-process (faster, no Python cold start)."""
    log(f"\n[RUN:in-process] afda.{module_name}")

    # Set up sys.argv for modules that use argparse to receive --data-dir
    old_argv = None
    if module_name in DATA_DIR_MODULES and data_dir is not None:
        old_argv = sys.argv[:]
        sys.argv = [f"afda.{module_name}", "--data-dir", str(data_dir)]

    try:
        module = importlib.import_module(f"afda.{module_name}")
        if hasattr(module, "main"):
            module.main()
        else:
            # Module-level code already executed on import
            pass
    except SystemExit:
        raise
    except Exception as exc:
        raise SystemExit(f"Pipeline stopped because afda.{module_name} failed: {exc}") from exc
    finally:
        if old_argv is not None:
            sys.argv = old_argv


def run_module(module_name: str, data_dir: Path | None = None, force_subprocess: bool = False) -> None:
    """Run a pipeline module, choosing the best execution mode.

    - If force_subprocess is True, use subprocess for isolation.
    - Otherwise, use in-process execution (faster, no Python cold start).
    """
    if force_subprocess:
        run_module_subprocess(module_name, data_dir)
    else:
        run_module_inprocess(module_name, data_dir)


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

    # --- Checkpoint setup ---
    checkpoint = Checkpoint(results_dir)
    if args.force:
        checkpoint.clear()
        checkpoint = Checkpoint(results_dir)

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

    # --- Determine which steps to run ---
    all_modules = modules_for_run(has_info=info_file is not None)

    if args.resume:
        skipped = [m for m in all_modules if checkpoint.is_done(m)]
        to_run = checkpoint.pending_steps(all_modules)
        if skipped:
            log(f"\n[resume] Skipping {len(skipped)} completed step(s): {', '.join(skipped)}")
        if not to_run:
            log("\n[resume] All steps already completed. Use --force to re-run from scratch.")
            return
    else:
        to_run = list(all_modules)

    # --- Execute pipeline ---
    for module_name in to_run:
        run_module(module_name, data_dir, force_subprocess=args.subprocess)
        checkpoint.mark_done(module_name)

    log("\nPipeline completed successfully.")
    log(f"Check {results_dir} for CSV, Excel, Markdown and HTML outputs.")


if __name__ == "__main__":
    main()

