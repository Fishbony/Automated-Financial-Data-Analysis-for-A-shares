from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from pipeline_utils import ensure_output_dirs, validate_rawdata


SCRIPTS = [
    "01_RoyalFlushData2csv_10years.py",
    "02_CheckStatements.py",
    "03_ExtractCalc.py",
    "04_FinancialCoreMetrics.py",
]


def run_script(script_name: str) -> None:
    print(f"\n[RUN] {script_name}")
    completed = subprocess.run([sys.executable, script_name], check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Pipeline stopped because {script_name} failed.")


def main() -> None:
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
    print("Check the results/ directory for csv, Excel and markdown outputs.")


if __name__ == "__main__":
    main()
