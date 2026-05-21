"""DeepSeek analysis for rebuilt financial statements."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

from afda.llm_client import deepseek_configured, deepseek_enabled, generate_deepseek_analysis
import afda.pipeline_utils as pu


OUTPUT_NAME = "rebuilt_statement_deepseek_analysis.md"

BS_ITEMS = [
    "Cash & Short-term Financial Assets",
    "Core Operating Current Assets",
    "Non-operating Misc. Current Assets",
    "Total Current Assets",
    "Long-term Core Operating Assets",
    "Long-term Financial & Equity Investments",
    "Risk & Amortizing Assets",
    "Total Assets",
    "Interest-bearing Short-term Debt",
    "Operating Non-interest-bearing Current Liabilities",
    "Long-term Interest-bearing Debt",
    "Total Liabilities",
    "Minority Interest",
    "Total Equity",
    "Balance Check Difference",
]

PL_ITEMS = [
    "Revenue",
    "COGS",
    "Gross Profit",
    "Selling Expense",
    "Admin Expense",
    "R&D Expense",
    "Financial Expense",
    "Asset / Credit Impairment",
    "Operating Profit",
    "EBIT",
    "EBITDA Proxy",
    "Profit Before Tax",
    "Income Tax",
    "Parent Net Profit",
    "Basic EPS",
]

CF_ITEMS = [
    "Cash From Customers",
    "Operating Cash Flow",
    "Investing Cash Flow",
    "Financing Cash Flow",
    "Capex",
    "Free Cash Flow Proxy",
    "Cash Conversion",
    "Debt Service Cover",
    "Cash Reinvestment Ratio",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate DeepSeek analysis for rebuilt BS/PL/CF statements.")
    parser.add_argument(
        "data_dir",
        nargs="?",
        default=None,
        help="Directory containing source files and results/. If omitted, a folder picker will open.",
    )
    parser.add_argument("--data-dir", dest="data_dir_flag", default=None, help="Same as positional data_dir.")
    return parser.parse_args()


def configure_paths(data_dir_value: str | None) -> Path:
    data_dir = pu.resolve_data_dir(data_dir_value) if data_dir_value else pu.prompt_data_dir_with_dialog()
    data_dir = data_dir.expanduser().resolve()
    pu.set_results_dir(data_dir / "results")
    return data_dir


def _existing_path(primary: Path, fallback: Path) -> Path:
    return primary if primary.exists() else fallback


def statement_paths() -> Dict[str, Path]:
    return {
        "balance_sheet": _existing_path(pu.BS_REBUILT_DIR / "2_standardized_bs.csv", pu.RESULTS_DIR / "BS_rebuilt_output" / "2_standardized_bs.csv"),
        "income_statement": _existing_path(pu.PL_REBUILT_DIR / "2_standardized_pl.csv", pu.RESULTS_DIR / "PL_rebuilt_output" / "2_standardized_pl.csv"),
        "cash_flow": _existing_path(pu.CF_REBUILT_DIR / "2_standardized_cf.csv", pu.RESULTS_DIR / "CF_rebuilt_output" / "2_standardized_cf.csv"),
    }


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required rebuilt statement not found: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")


def pivot_statement(df: pd.DataFrame, item_col: str, items: Iterable[str]) -> pd.DataFrame:
    pivot = (
        df[df[item_col].isin(items)]
        .pivot_table(index=item_col, columns="Year", values="Value", aggfunc="sum")
        .sort_index(axis=1)
    )
    return pivot.reindex([item for item in items if item in pivot.index])


def row(pivot: pd.DataFrame, item: str) -> pd.Series:
    if item not in pivot.index:
        return pd.Series(dtype="float64")
    return pivot.loc[item].astype(float)


def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    return a.divide(b.replace(0, pd.NA)).astype("float64")


def pct(value: float | int | pd.NA) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value) * 100:.1f}%"


def money(value: float | int | pd.NA) -> str:
    if pd.isna(value):
        return "NA"
    value = float(value)
    if abs(value) >= 1e8:
        return f"{value / 1e8:,.2f}亿"
    if abs(value) >= 1e4:
        return f"{value / 1e4:,.2f}万"
    return f"{value:,.2f}"


def cagr(series: pd.Series) -> float | None:
    clean = series.dropna()
    if len(clean) < 2:
        return None
    start = float(clean.iloc[0])
    end = float(clean.iloc[-1])
    periods = len(clean) - 1
    if start <= 0 or end <= 0 or periods <= 0:
        return None
    return (end / start) ** (1 / periods) - 1


def build_company_label(data_dir: Path) -> str:
    fallback_name = data_dir.resolve().name or "unknown-company"
    return pu.company_display_name(data_dir, fallback=fallback_name)


def build_context(data_dir: Path) -> str:
    paths = statement_paths()
    bs = read_csv(paths["balance_sheet"])
    pl = read_csv(paths["income_statement"])
    cf = read_csv(paths["cash_flow"])

    bs_pivot = pivot_statement(bs, "StandardLineItem", BS_ITEMS)
    pl_pivot = pivot_statement(pl, "Standard Item", PL_ITEMS)
    cf_pivot = pivot_statement(cf, "Standard Item", CF_ITEMS)

    years = sorted(set(bs_pivot.columns).intersection(pl_pivot.columns).intersection(cf_pivot.columns))
    if not years:
        raise ValueError("No common years found in rebuilt BS/PL/CF statements.")
    latest = years[-1]

    revenue = row(pl_pivot, "Revenue")
    gross_profit = row(pl_pivot, "Gross Profit")
    operating_profit = row(pl_pivot, "Operating Profit")
    parent_np = row(pl_pivot, "Parent Net Profit")
    rd = row(pl_pivot, "R&D Expense")
    impairment = row(pl_pivot, "Asset / Credit Impairment")
    total_assets = row(bs_pivot, "Total Assets")
    total_liabilities = row(bs_pivot, "Total Liabilities")
    total_equity = row(bs_pivot, "Total Equity")
    cash = row(bs_pivot, "Cash & Short-term Financial Assets")
    short_debt = row(bs_pivot, "Interest-bearing Short-term Debt")
    long_debt = row(bs_pivot, "Long-term Interest-bearing Debt")
    operating_ca = row(bs_pivot, "Core Operating Current Assets")
    operating_cl = row(bs_pivot, "Operating Non-interest-bearing Current Liabilities")
    balance_diff = row(bs_pivot, "Balance Check Difference")
    cfo = row(cf_pivot, "Operating Cash Flow")
    capex = row(cf_pivot, "Capex").abs()
    fcf = row(cf_pivot, "Free Cash Flow Proxy")
    cash_from_customers = row(cf_pivot, "Cash From Customers")

    derived = pd.DataFrame(index=years)
    derived["Revenue"] = revenue.reindex(years)
    derived["Revenue YoY"] = revenue.pct_change().reindex(years)
    derived["Gross Margin"] = safe_div(gross_profit, revenue).reindex(years)
    derived["Operating Margin"] = safe_div(operating_profit, revenue).reindex(years)
    derived["Net Margin"] = safe_div(parent_np, revenue).reindex(years)
    derived["R&D / Revenue"] = safe_div(rd, revenue).reindex(years)
    derived["Impairment / Revenue"] = safe_div(impairment, revenue).reindex(years)
    derived["Liability Ratio"] = safe_div(total_liabilities, total_assets).reindex(years)
    derived["Net Cash"] = (cash - short_debt - long_debt).reindex(years)
    derived["Operating NWC"] = (operating_ca - operating_cl).reindex(years)
    derived["CFO / Revenue"] = safe_div(cfo, revenue).reindex(years)
    derived["CFO / Parent NP"] = safe_div(cfo, parent_np).reindex(years)
    derived["FCF"] = fcf.reindex(years)
    derived["FCF / Revenue"] = safe_div(fcf, revenue).reindex(years)
    derived["Cash Collection / Revenue"] = safe_div(cash_from_customers, revenue).reindex(years)
    derived["Balance Diff / Assets"] = safe_div(balance_diff.abs(), total_assets).reindex(years)

    latest_rows = []
    for item in derived.columns:
        value = derived.loc[latest, item]
        if "Margin" in item or "/" in item or "Ratio" in item or "YoY" in item:
            display = pct(value)
        else:
            display = money(value)
        latest_rows.append({"Metric": item, str(latest): display})
    latest_df = pd.DataFrame(latest_rows)

    headline = pd.DataFrame(
        [
            {"Metric": "Revenue CAGR", "Value": pct(cagr(revenue) if cagr(revenue) is not None else pd.NA)},
            {"Metric": "Parent NP CAGR", "Value": pct(cagr(parent_np) if cagr(parent_np) is not None else pd.NA)},
            {"Metric": "CFO CAGR", "Value": pct(cagr(cfo) if cagr(cfo) is not None else pd.NA)},
            {"Metric": "Latest Revenue", "Value": money(revenue.reindex(years).iloc[-1])},
            {"Metric": "Latest Parent NP", "Value": money(parent_np.reindex(years).iloc[-1])},
            {"Metric": "Latest CFO", "Value": money(cfo.reindex(years).iloc[-1])},
            {"Metric": "Latest Net Cash", "Value": money(derived["Net Cash"].iloc[-1])},
        ]
    )

    compact_years = years[-6:]
    derived_compact = derived.loc[compact_years].copy()
    for col in derived_compact.columns:
        if "Margin" in col or "/" in col or "Ratio" in col or "YoY" in col:
            derived_compact[col] = derived_compact[col].map(pct)
        else:
            derived_compact[col] = derived_compact[col].map(money)

    return f"""
公司/目录：{build_company_label(data_dir)}
覆盖年份：{years[0]}-{years[-1]}
数据来源：rebuild 后的标准化资产负债表、利润表、现金流量表。

## 核心摘要
{headline.to_markdown(index=False)}

## 最新年度关键指标
{latest_df.to_markdown(index=False)}

## 最近年份趋势
{derived_compact.reset_index(names="Year").to_markdown(index=False)}

## 标准化资产负债表关键科目
{bs_pivot.loc[:, compact_years].to_markdown()}

## 标准化利润表关键科目
{pl_pivot.loc[:, compact_years].to_markdown()}

## 标准化现金流量表关键科目
{cf_pivot.loc[:, compact_years].to_markdown()}
""".strip()


def build_prompt(context: str) -> str:
    return f"""
你是一名谨慎的 A 股基本面研究员、投行估值建模分析师和财务风险审查员。

请只基于下面 rebuild 后三张报表的数据进行分析，不要编造公司产品、客户、行业新闻或公告事实。
如果只能从财务特征推断公司类型，请明确写“从财务特征推断”。

请输出一份中文 Markdown 简要分析报告，结构必须包含：

1. 三表整体画像
   - 这组财务数据代表的公司更像什么类型的公司
   - 商业模式/经营特征的谨慎推断

2. 资产负债表分析
   - 资产结构、现金与有息负债、营运资本、权益结构
   - 可能需要关注的异常或勾稽问题

3. 利润表分析
   - 收入成长、利润率、费用结构、研发投入、减值/非经常性特征
   - 盈利质量和可持续性问题

4. 现金流量表分析
   - 收现质量、CFO、FCF、资本开支、融资现金流
   - 利润现金含量和自由现金流质量

5. 需要重点调查的问题
   - 用项目符号列出 5-10 个问题
   - 每个问题必须说明对应财务数据或趋势

6. 潜在雷点清单
   - 按 高/中/低 风险分级
   - 每个雷点必须说明为什么可能是雷点，以及下一步应该查什么原始资料

7. 一句话结论
   - 简洁说明这家公司财务画像的核心判断和最大待核查事项

财务数据上下文：

{context}
""".strip()


def write_skip_report(output_path: Path, reason: str) -> None:
    output_path.write_text(
        f"""# Rebuilt 三表 DeepSeek 分析报告

生成时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

DeepSeek 分析未执行。

原因：{reason}

如需启用，请在 `.env` 中设置：

```dotenv
ENABLE_DEEPSEEK_ANALYSIS=1
DEEPSEEK_API_KEY=your_deepseek_api_key
```
""",
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    data_dir = configure_paths(args.data_dir_flag or args.data_dir)
    output_path = pu.RESULTS_DIR / OUTPUT_NAME
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not deepseek_enabled():
        write_skip_report(output_path, "ENABLE_DEEPSEEK_ANALYSIS is not enabled.")
        print(f"DeepSeek analysis skipped; report written to {output_path}")
        return

    if not deepseek_configured():
        write_skip_report(output_path, "DEEPSEEK_API_KEY is not set.")
        print(f"DeepSeek analysis skipped; report written to {output_path}")
        return

    context = build_context(data_dir)
    try:
        analysis = generate_deepseek_analysis(
            report_context=build_prompt(context),
            temperature=0.1,
            system_prompt=(
                "你是谨慎的A股基本面研究员和财务风险审查员。"
                "只基于用户提供的三表数据分析，不编造外部事实。"
                "输出中文Markdown，观点必须有财务数据依据。"
            ),
        )
    except Exception as exc:
        write_skip_report(output_path, f"DeepSeek request failed: {exc}")
        print(f"DeepSeek analysis failed; report written to {output_path}")
        return

    output_path.write_text(
        f"""# Rebuilt 三表 DeepSeek 分析报告

生成时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

{analysis}
""",
        encoding="utf-8",
    )
    print(f"DeepSeek rebuilt statement analysis generated: {output_path}")


if __name__ == "__main__":
    main()
