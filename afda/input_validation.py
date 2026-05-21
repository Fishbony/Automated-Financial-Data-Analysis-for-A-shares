"""Input validation helpers for the A-share analysis pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import pandas as pd

from afda.pipeline_utils import find_info_file, validate_rawdata


REQUIRED_INFO_ITEMS = {
    "总股本": "shares outstanding used to convert equity value to price",
    "当前股价": "current share price used for upside/downside",
}

RECOMMENDED_COMPANY_NAME_ITEMS = ("公司名称", "公司简称")
RECOMMENDED_INFO_ITEMS = {
    "公司代码": "display code used with company name in Excel, Markdown, and HTML reports",
}


@dataclass
class ValidationReport:
    """Structured validation result for raw input folders."""

    data_dir: Path
    ticker: str | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def add_error(self, message: str) -> None:
        self.errors.append(message)

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)

    def format(self) -> str:
        lines = [f"Input validation report: {self.data_dir}"]
        if self.ticker:
            lines.append(f"Ticker: {self.ticker}")
        if self.errors:
            lines.append("Errors:")
            lines.extend(f"  - {message}" for message in self.errors)
        if self.warnings:
            lines.append("Warnings:")
            lines.extend(f"  - {message}" for message in self.warnings)
        if not self.errors and not self.warnings:
            lines.append("No issues found.")
        return "\n".join(lines)


def _last_value_column(info_df: pd.DataFrame) -> str:
    if len(info_df.columns) < 2:
        raise ValueError("Info.csv must contain at least two columns: 项目 and value.")
    return str(info_df.columns[-1])


def _read_info(info_path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(info_path, dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(info_path, dtype=str, encoding="gbk")


def _find_info_value(info_df: pd.DataFrame, item: str) -> str | None:
    if "项目" not in info_df.columns:
        return None
    value_col = _last_value_column(info_df)
    match = info_df.loc[info_df["项目"].astype(str).str.strip() == item, value_col]
    if match.empty:
        return None
    value = str(match.iloc[0]).strip()
    return value if value and value.lower() != "nan" else None


def _validate_numeric_item(info_df: pd.DataFrame, item: str, report: ValidationReport) -> None:
    value = _find_info_value(info_df, item)
    if value is None:
        report.add_error(f"Info.csv is missing required item: {item}.")
        return
    try:
        number = float(value.replace(",", ""))
    except ValueError:
        report.add_error(f"Info.csv item {item} must be numeric, got: {value!r}.")
        return
    if number <= 0:
        report.add_error(f"Info.csv item {item} must be positive, got: {value!r}.")


def validate_info_file(info_path: Path, report: ValidationReport) -> None:
    try:
        info_df = _read_info(info_path)
    except Exception as exc:
        report.add_error(f"Failed to read Info.csv: {exc}")
        return

    if "项目" not in info_df.columns:
        report.add_error("Info.csv must include a column named 项目.")
        return

    for item in REQUIRED_INFO_ITEMS:
        _validate_numeric_item(info_df, item, report)

    if not any(_find_info_value(info_df, item) is not None for item in RECOMMENDED_COMPANY_NAME_ITEMS):
        report.add_warning("Info.csv is missing recommended item 公司名称 or 公司简称: display name in reports.")

    for item, reason in RECOMMENDED_INFO_ITEMS.items():
        if _find_info_value(info_df, item) is None:
            report.add_warning(f"Info.csv is missing recommended item {item}: {reason}.")


def validate_input_folder(data_dir: Path | str, require_info: bool = False) -> ValidationReport:
    """Validate raw input files and optional DCF metadata before running the pipeline."""

    data_path = Path(data_dir).expanduser().resolve()
    report = ValidationReport(data_dir=data_path)

    try:
        ticker, raw_files = validate_rawdata(data_path)
        report.ticker = ticker
    except Exception as exc:
        report.add_error(str(exc))
        return report

    for label, path in raw_files.items():
        if label == "price":
            continue
        if path.stat().st_size == 0:
            report.add_error(f"Input file is empty: {path.name}.")

    info_path = find_info_file(data_path)
    if info_path is None:
        message = "Info.csv not found. DCF Excel and HTML dashboard will be skipped."
        if require_info:
            report.add_error(message)
        else:
            report.add_warning(message)
        return report

    validate_info_file(info_path, report)
    return report


def print_validation_report(report: ValidationReport, include_success: bool = False) -> None:
    if include_success or report.errors or report.warnings:
        print(report.format())


def require_valid_input(data_dir: Path | str, require_info: bool = False) -> ValidationReport:
    report = validate_input_folder(data_dir, require_info=require_info)
    print_validation_report(report)
    if not report.ok:
        raise SystemExit("Input validation failed. Fix the errors above and rerun the pipeline.")
    return report
