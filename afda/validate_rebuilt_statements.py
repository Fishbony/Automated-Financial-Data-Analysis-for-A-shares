"""Independent checks for rebuilt standardized BS/PL/CF statements."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd

import afda.pipeline_utils as pu


OUTPUT_DIR_NAME = "rebuilt_statement_checks"
OUTPUT_XLSX_NAME = "rebuilt_statement_checks.xlsx"
OUTPUT_MD_NAME = "rebuilt_statement_checks_report.md"
ABS_TOL = 1_000.0
REL_TOL = 1e-6


@dataclass
class CheckSpec:
    statement: str
    check: str
    lhs: pd.Series
    rhs: pd.Series
    scale: pd.Series
    formula: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate rebuilt standardized BS/PL/CF files.")
    parser.add_argument(
        "data_dir",
        nargs="?",
        default=None,
        help="Directory containing results/. If omitted, a folder picker will open.",
    )
    parser.add_argument("--data-dir", dest="data_dir_flag", default=None, help="Same as positional data_dir.")
    return parser.parse_args()


def configure_paths(data_dir_value: str | None) -> Path:
    data_dir = pu.resolve_data_dir(data_dir_value) if data_dir_value else pu.prompt_data_dir_with_dialog()
    data_dir = data_dir.expanduser().resolve()
    pu.set_results_dir(data_dir / "results")
    return data_dir


def existing_path(primary: Path, fallback: Path) -> Path:
    return primary if primary.exists() else fallback


def rebuilt_paths() -> Dict[str, Path]:
    return {
        "balance_sheet": existing_path(pu.BS_REBUILT_DIR / "2_standardized_bs.csv", pu.RESULTS_DIR / "BS_rebuilt_output" / "2_standardized_bs.csv"),
        "income_statement": existing_path(pu.PL_REBUILT_DIR / "2_standardized_pl.csv", pu.RESULTS_DIR / "PL_rebuilt_output" / "2_standardized_pl.csv"),
        "cash_flow": existing_path(pu.CF_REBUILT_DIR / "2_standardized_cf.csv", pu.RESULTS_DIR / "CF_rebuilt_output" / "2_standardized_cf.csv"),
    }


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required rebuilt standardized file not found: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")


def pivot(df: pd.DataFrame, item_col: str) -> pd.DataFrame:
    out = df.pivot_table(index=item_col, columns="Year", values="Value", aggfunc="sum").sort_index(axis=1)
    out.columns = [int(c) for c in out.columns]
    return out


def get(wide: pd.DataFrame, item: str) -> pd.Series:
    if item in wide.index:
        return wide.loc[item].astype(float)
    return pd.Series(0.0, index=wide.columns, dtype="float64")


def make_check(statement: str, check: str, lhs: pd.Series, rhs: pd.Series, formula: str, scale: pd.Series | None = None) -> CheckSpec:
    lhs, rhs = lhs.align(rhs, join="outer", fill_value=0.0)
    if scale is None:
        scale = pd.concat([lhs.abs(), rhs.abs()], axis=1).max(axis=1)
    else:
        scale = scale.reindex(lhs.index).fillna(0.0).abs()
    return CheckSpec(statement=statement, check=check, lhs=lhs, rhs=rhs, scale=scale, formula=formula)


def specs_to_df(specs: list[CheckSpec]) -> pd.DataFrame:
    rows = []
    for spec in specs:
        diff = spec.lhs - spec.rhs
        tolerance = spec.scale.map(lambda x: max(ABS_TOL, float(x) * REL_TOL))
        for year in spec.lhs.index:
            rows.append(
                {
                    "Statement": spec.statement,
                    "Check": spec.check,
                    "Year": int(year),
                    "LHS": float(spec.lhs.loc[year]),
                    "RHS": float(spec.rhs.loc[year]),
                    "Difference": float(diff.loc[year]),
                    "Abs Difference": float(abs(diff.loc[year])),
                    "Tolerance": float(tolerance.loc[year]),
                    "Passed": bool(abs(diff.loc[year]) <= tolerance.loc[year]),
                    "Formula": spec.formula,
                }
            )
    return pd.DataFrame(rows)


def validate_bs(bs: pd.DataFrame) -> pd.DataFrame:
    wide = pivot(bs, "StandardLineItem")
    current_assets = get(wide, "Total Current Assets")
    non_current_assets = get(wide, "Total Non-current Assets")
    total_assets = get(wide, "Total Assets")
    current_liab = get(wide, "Total Current Liabilities")
    non_current_liab = get(wide, "Total Non-current Liabilities")
    total_liab = get(wide, "Total Liabilities")
    parent_capital = get(wide, "Parent Contributed Capital")
    retained = get(wide, "Parent Retained Earnings")
    oci = get(wide, "Other Comprehensive Income (OCI)")
    minority = get(wide, "Minority Interest")
    total_equity = get(wide, "Total Equity")
    total_le = get(wide, "Total Liabilities & Equity")
    balance_diff = get(wide, "Balance Check Difference")

    specs = [
        make_check(
            "Balance Sheet",
            "Assets subtotal",
            total_assets,
            current_assets + non_current_assets,
            "Total Assets = Total Current Assets + Total Non-current Assets",
            total_assets,
        ),
        make_check(
            "Balance Sheet",
            "Liabilities subtotal",
            total_liab,
            current_liab + non_current_liab,
            "Total Liabilities = Total Current Liabilities + Total Non-current Liabilities",
            total_liab,
        ),
        make_check(
            "Balance Sheet",
            "Equity subtotal",
            total_equity,
            parent_capital + retained + oci + minority,
            "Total Equity = Parent Contributed Capital + Parent Retained Earnings + OCI + Minority Interest",
            total_equity,
        ),
        make_check(
            "Balance Sheet",
            "Liabilities and equity subtotal",
            total_le,
            total_liab + total_equity,
            "Total Liabilities & Equity = Total Liabilities + Total Equity",
            total_le,
        ),
        make_check(
            "Balance Sheet",
            "Balance difference definition",
            balance_diff,
            total_assets - total_le,
            "Balance Check Difference = Total Assets - Total Liabilities & Equity",
            total_assets,
        ),
        make_check(
            "Balance Sheet",
            "Accounting equation after recorded difference",
            total_assets,
            total_le + balance_diff,
            "Total Assets = Total Liabilities & Equity + Balance Check Difference",
            total_assets,
        ),
    ]
    return specs_to_df(specs)


def validate_pl(pl: pd.DataFrame) -> pd.DataFrame:
    wide = pivot(pl, "Standard Item")
    revenue = get(wide, "Revenue")
    cogs = get(wide, "COGS")
    taxes = get(wide, "Taxes & Surcharges")
    selling = get(wide, "Selling Expense")
    admin = get(wide, "Admin Expense")
    rnd = get(wide, "R&D Expense")
    fin = get(wide, "Financial Expense")
    impairment = get(wide, "Asset / Credit Impairment")
    other_gains = get(wide, "Other Operating Gains")
    op = get(wide, "Operating Profit")
    non_op_income = get(wide, "Non-operating Income")
    non_op_expense = get(wide, "Non-operating Expense")
    pbt = get(wide, "Profit Before Tax")
    tax = get(wide, "Income Tax")
    net_profit = get(wide, "Net Profit")
    parent_np = get(wide, "Parent Net Profit")
    minority_profit = get(wide, "Minority Interest Profit")
    parent_oci = get(wide, "Parent OCI")
    parent_comprehensive = get(wide, "Parent Comprehensive Income")

    op_calc = revenue - cogs - taxes - selling - admin - rnd - fin - impairment + other_gains
    specs = [
        make_check(
            "Income Statement",
            "Operating profit bridge",
            op,
            op_calc,
            "Operating Profit = Revenue - COGS - Taxes & Surcharges - Selling - Admin - R&D - Financial Expense - Impairment + Other Operating Gains",
            revenue,
        ),
        make_check(
            "Income Statement",
            "Profit before tax bridge",
            pbt,
            op + non_op_income - non_op_expense,
            "Profit Before Tax = Operating Profit + Non-operating Income - Non-operating Expense",
            pbt,
        ),
        make_check(
            "Income Statement",
            "Net profit bridge",
            net_profit,
            pbt - tax,
            "Net Profit = Profit Before Tax - Income Tax",
            net_profit,
        ),
        make_check(
            "Income Statement",
            "Net profit attribution",
            net_profit,
            parent_np + minority_profit,
            "Net Profit = Parent Net Profit + Minority Interest Profit",
            net_profit,
        ),
        make_check(
            "Income Statement",
            "Parent comprehensive income bridge",
            parent_comprehensive,
            parent_np + parent_oci,
            "Parent Comprehensive Income = Parent Net Profit + Parent OCI",
            parent_comprehensive,
        ),
    ]
    return specs_to_df(specs)


def validate_cf(cf: pd.DataFrame) -> pd.DataFrame:
    wide = pivot(cf, "Standard Item")
    cash_customers = get(wide, "Cash From Customers")
    tax_refunds = get(wide, "Tax Refunds")
    other_op_in = get(wide, "Other Operating Cash In")
    paid_suppliers = get(wide, "Cash Paid to Suppliers")
    paid_employees = get(wide, "Cash Paid to Employees")
    taxes_paid = get(wide, "Taxes Paid")
    other_op_out = get(wide, "Other Operating Cash Out")
    cfo = get(wide, "Operating Cash Flow")
    invest_recovery = get(wide, "Investment Recovery Cash In")
    invest_income = get(wide, "Investment Income Cash In")
    asset_disposal = get(wide, "Asset Disposal Cash In")
    other_investing_in = get(wide, "Other Investing Cash In")
    capex = get(wide, "Capex")
    investment_out = get(wide, "Investment Cash Out")
    cfi = get(wide, "Investing Cash Flow")
    equity_in = get(wide, "Equity Financing Cash In")
    debt_in = get(wide, "Debt Financing Cash In")
    other_fin_in = get(wide, "Other Financing Cash In")
    debt_repay = get(wide, "Debt Repayment Cash Out")
    dividend_interest = get(wide, "Dividend & Interest Cash Out")
    other_fin_out = get(wide, "Other Financing Cash Out")
    cff = get(wide, "Financing Cash Flow")
    fx = get(wide, "FX Impact")
    net_change = get(wide, "Net Change in Cash")
    beginning_cash = get(wide, "Beginning Cash")
    ending_cash = get(wide, "Ending Cash")

    net_profit = get(wide, "Net Profit")
    indirect_items = [
        get(wide, "Impairment Add-back"),
        get(wide, "Depreciation"),
        get(wide, "Amortization"),
        get(wide, "Asset Disposal Loss"),
        get(wide, "Fair Value Loss"),
        get(wide, "Financial Expense Bridge"),
        get(wide, "Investment Loss"),
        get(wide, "Deferred Tax Impact"),
        get(wide, "Inventory Change"),
        get(wide, "Receivables Change"),
        get(wide, "Payables Change"),
        get(wide, "Other CFO Bridge"),
    ]
    indirect_cfo = get(wide, "Indirect Operating Cash Flow")
    indirect_calc = net_profit
    for item in indirect_items:
        indirect_calc = indirect_calc + item

    op_inflows = cash_customers + tax_refunds + other_op_in
    op_outflows = paid_suppliers + paid_employees + taxes_paid + other_op_out
    investing_inflows = invest_recovery + invest_income + asset_disposal + other_investing_in
    investing_outflows = capex + investment_out
    financing_inflows = equity_in + debt_in + other_fin_in
    financing_outflows = debt_repay + dividend_interest + other_fin_out

    specs = [
        make_check(
            "Cash Flow",
            "Operating cash flow bridge",
            cfo,
            op_inflows - op_outflows,
            "Operating Cash Flow = Operating Cash Inflows - Operating Cash Outflows",
            cfo,
        ),
        make_check(
            "Cash Flow",
            "Investing cash flow bridge",
            cfi,
            investing_inflows - investing_outflows,
            "Investing Cash Flow = Investing Cash Inflows - Investing Cash Outflows",
            cfi,
        ),
        make_check(
            "Cash Flow",
            "Financing cash flow bridge",
            cff,
            financing_inflows - financing_outflows,
            "Financing Cash Flow = Financing Cash Inflows - Financing Cash Outflows",
            cff,
        ),
        make_check(
            "Cash Flow",
            "Net cash change bridge",
            net_change,
            cfo + cfi + cff + fx,
            "Net Change in Cash = CFO + CFI + CFF + FX Impact",
            net_change,
        ),
        make_check(
            "Cash Flow",
            "Ending cash bridge",
            ending_cash,
            beginning_cash + net_change,
            "Ending Cash = Beginning Cash + Net Change in Cash",
            ending_cash,
        ),
        make_check(
            "Cash Flow",
            "Indirect CFO bridge",
            indirect_cfo,
            indirect_calc,
            "Indirect Operating Cash Flow = Net Profit + non-cash and working-capital bridge items",
            indirect_cfo,
        ),
    ]
    return specs_to_df(specs)


def fmt_money(value: float) -> str:
    if abs(value) >= 1e8:
        return f"{value / 1e8:,.2f}亿"
    if abs(value) >= 1e4:
        return f"{value / 1e4:,.2f}万"
    return f"{value:,.2f}"


def build_markdown(checks: pd.DataFrame) -> str:
    summary = (
        checks.groupby("Statement")
        .agg(Total_Checks=("Passed", "size"), Passed=("Passed", "sum"), Failed=("Passed", lambda x: int((~x).sum())))
        .reset_index()
    )
    summary["Status"] = summary["Failed"].map(lambda x: "通过" if x == 0 else "需复核")

    lines = [
        "# Rebuilt Standardized 三表独立校验报告",
        "",
        f"- 绝对容忍度：{ABS_TOL:,.0f} 元",
        f"- 相对容忍度：{REL_TOL:.0e} * 校验规模",
        "",
        "## 汇总",
        "",
        summary.to_markdown(index=False),
        "",
        "## 未通过项目",
        "",
    ]

    failed = checks.loc[~checks["Passed"]].copy()
    if failed.empty:
        lines.append("所有 rebuild 后标准化报表内部校验均通过。")
    else:
        failed = failed.sort_values(["Statement", "Check", "Year"])
        for _, row in failed.iterrows():
            lines.append(
                f"- {row['Statement']} / {row['Check']} / {int(row['Year'])}: "
                f"差额 {fmt_money(float(row['Difference']))}，容忍度 {fmt_money(float(row['Tolerance']))}；公式：{row['Formula']}"
            )

    lines += ["", "## 校验公式", ""]
    for statement, group in checks.groupby("Statement", sort=False):
        lines.append(f"### {statement}")
        formulas = group[["Check", "Formula"]].drop_duplicates()
        for _, row in formulas.iterrows():
            lines.append(f"- **{row['Check']}**：`{row['Formula']}`")
        lines.append("")

    return "\n".join(lines)


def save_outputs(output_dir: Path, bs_checks: pd.DataFrame, pl_checks: pd.DataFrame, cf_checks: pd.DataFrame) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_checks = pd.concat([bs_checks, pl_checks, cf_checks], ignore_index=True)
    summary = (
        all_checks.groupby(["Statement", "Check"])
        .agg(Total_Years=("Passed", "size"), Passed_Years=("Passed", "sum"), Failed_Years=("Passed", lambda x: int((~x).sum())), Max_Abs_Diff=("Abs Difference", "max"))
        .reset_index()
    )
    summary["Status"] = summary["Failed_Years"].map(lambda x: "Passed" if x == 0 else "Review")

    xlsx_path = output_dir / OUTPUT_XLSX_NAME
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        all_checks.to_excel(writer, sheet_name="All_Checks", index=False)
        bs_checks.to_excel(writer, sheet_name="Balance_Sheet", index=False)
        pl_checks.to_excel(writer, sheet_name="Income_Statement", index=False)
        cf_checks.to_excel(writer, sheet_name="Cash_Flow", index=False)

    md_path = output_dir / OUTPUT_MD_NAME
    md_path.write_text(build_markdown(all_checks), encoding="utf-8")


def main() -> None:
    args = parse_args()
    configure_paths(args.data_dir_flag or args.data_dir)
    paths = rebuilt_paths()
    bs_checks = validate_bs(read_csv(paths["balance_sheet"]))
    pl_checks = validate_pl(read_csv(paths["income_statement"]))
    cf_checks = validate_cf(read_csv(paths["cash_flow"]))
    output_dir = pu.RESULTS_DIR / OUTPUT_DIR_NAME
    save_outputs(output_dir, bs_checks, pl_checks, cf_checks)
    failed = int((~pd.concat([bs_checks, pl_checks, cf_checks], ignore_index=True)["Passed"]).sum())
    print(f"Rebuilt statement checks generated: {output_dir}")
    if failed:
        print(f"Warning: {failed} check rows need review.")
    else:
        print("All rebuilt standardized statement checks passed.")


if __name__ == "__main__":
    main()

