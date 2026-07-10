"""
Step 6/11 — 利润表标准化重构
============================
将同花顺原始利润表 CSV 清洗、排序，并按投行建模口径重新分类，
输出标准化长表、宽表、估值输入底稿及映射追溯体系。

标准科目结构
------------
Operating
  Revenue / COGS / Taxes & Surcharges / Selling Expense /
  Admin Expense / R&D Expense / Financial Expense /
  Asset & Credit Impairment / Other Operating Gains /
  Operating Profit

Below Operating
  Non-operating Income / Non-operating Expense /
  Profit Before Tax / Income Tax / Net Profit

Equity Attribution
  Parent Net Profit / Minority Interest Profit / Adjusted Net Profit

Comprehensive Income
  Parent OCI / Parent Comprehensive Income

Per Share
  Basic EPS / Diluted EPS

估值输入指标（Valuation Input Sheet）
--------------------------------------
- Gross Profit、Core Opex、Financial Expense、Impairment
- Operating Profit、EBIT、EBITDA Proxy、Profit Before Tax、Parent Net Profit
- Gross Margin、Operating Margin、Parent Net Margin、Effective Tax Rate

输入
----
- results/csv/pl.csv    同花顺原始利润表

输出（results/PL_rebuilt_output/）
------------------------------------
- 1_preprocess_pl.csv        去重、排序后的预处理表
- 2_standardized_pl.csv      标准化长表
- 2_standardized_pl_wide.csv 标准化宽表（行为项目，列为时间）
- 3_mapping_detail.csv       原始科目 → 标准科目映射
- 4_analysis_bridge.csv      各标准科目的组成项拆解
- 5_valuation_ready_pl.xlsx  可直接用于估值的 Excel 底稿
- PL重构说明.md              本次重构的完整说明文档

运行方式
--------
    python rebuild_income_statement.py
    # 或通过主管道：
    python run_pipeline.py
"""

import os
from typing import Dict, List, Tuple

import pandas as pd
from afda.pipeline_utils import CSV_DIR, PL_REBUILT_DIR
from afda.logging_config import get_logger
from afda.statement_base import (
    apply_bilingual_fonts_to_file,
    build_standardized_item_wide as _build_item_wide,
    build_standardized_wide as _build_wide,
    ensure_output_dir,
    export_statement_excel,
    load_statement_csv,
    safe_row_sum,
)
from afda.statement_mapping import describe_source_matches, load_mapping_rules

logger = get_logger(__name__)


OUTPUT_DIR = str(PL_REBUILT_DIR)


def load_pl_csv(input_path: str) -> Tuple[pd.DataFrame, str, List[str]]:
    return load_statement_csv(input_path, item_col_name="科目", error_label="利润表")


def preprocess_pl(df: pd.DataFrame, item_col: str, year_cols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    order = [
        "一、营业总收入(元)",
        "其中：营业收入(元)",
        "二、营业总成本(元)",
        "其中：营业成本(元)",
        "营业税金及附加(元)",
        "销售费用(元)",
        "管理费用(元)",
        "研发费用(元)",
        "财务费用(元)",
        "其中：利息费用(元)",
        "利息收入(元)",
        "资产减值损失(元)",
        "信用减值损失(元)",
        "加：公允价值变动收益(元)",
        "投资收益(元)",
        "其中：联营企业和合营企业的投资收益(元)",
        "资产处置收益(元)",
        "其他收益(元)",
        "三、营业利润(元)",
        "加：营业外收入(元)",
        "减：营业外支出(元)",
        "四、利润总额(元)",
        "减：所得税费用(元)",
        "五、净利润(元)",
        "（一）持续经营净利润(元)",
        "归属于母公司所有者的净利润(元)",
        "少数股东损益(元)",
        "扣除非经常性损益后的净利润(元)",
        "（一）基本每股收益(元)",
        "（二）稀释每股收益(元)",
        "七、其他综合收益(元)",
        "归属母公司所有者的其他综合收益(元)",
        "八、综合收益总额(元)",
        "归属于母公司股东的综合收益总额(元)",
        "归属于少数股东的综合收益总额(元)",
    ]
    order_map = {name: idx for idx, name in enumerate(order)}
    df = df.drop_duplicates(subset=[item_col], keep="first").copy()
    df["_sort"] = df[item_col].map(lambda x: order_map.get(x, 999))
    df = df.sort_values(["_sort", item_col]).drop(columns="_sort").reset_index(drop=True)

    checks = []
    for year in year_cols:
        revenue = float(safe_row_sum(df, item_col, year_cols, ["一、营业总收入(元)"])[year])
        total_cost = float(safe_row_sum(df, item_col, year_cols, ["二、营业总成本(元)"])[year])
        gain_fv = float(safe_row_sum(df, item_col, year_cols, ["加：公允价值变动收益(元)"])[year])
        gain_inv = float(safe_row_sum(df, item_col, year_cols, ["投资收益(元)"])[year])
        gain_asset = float(safe_row_sum(df, item_col, year_cols, ["资产处置收益(元)"])[year])
        gain_other = float(safe_row_sum(df, item_col, year_cols, ["其他收益(元)"])[year])
        operating_profit = float(safe_row_sum(df, item_col, year_cols, ["三、营业利润(元)"])[year])
        non_oper_income = float(safe_row_sum(df, item_col, year_cols, ["加：营业外收入(元)"])[year])
        non_oper_expense = float(safe_row_sum(df, item_col, year_cols, ["减：营业外支出(元)"])[year])
        ebt = float(safe_row_sum(df, item_col, year_cols, ["四、利润总额(元)"])[year])
        income_tax = float(safe_row_sum(df, item_col, year_cols, ["减：所得税费用(元)"])[year])
        net_profit = float(safe_row_sum(df, item_col, year_cols, ["五、净利润(元)"])[year])
        parent_np = float(safe_row_sum(df, item_col, year_cols, ["归属于母公司所有者的净利润(元)"])[year])
        minority_np = float(safe_row_sum(df, item_col, year_cols, ["少数股东损益(元)"])[year])

        op_diff = revenue - total_cost + gain_fv + gain_inv + gain_asset + gain_other - operating_profit
        ebt_diff = operating_profit + non_oper_income - non_oper_expense - ebt
        net_diff = ebt - income_tax - net_profit
        parent_diff = net_profit - parent_np - minority_np
        tolerance = 200.0
        checks.append(
            {
                "Year": year,
                "营业利润校验差额": op_diff,
                "利润总额校验差额": ebt_diff,
                "净利润校验差额": net_diff,
                "归母拆分校验差额": parent_diff,
                "All_Passed": max(abs(op_diff), abs(ebt_diff), abs(net_diff), abs(parent_diff)) <= tolerance,
            }
        )

    return df, pd.DataFrame(checks)


def build_mapping_rules() -> List[Dict]:
    return load_mapping_rules("income_statement")


def build_mapping_detail(df: pd.DataFrame, item_col: str, rules: List[Dict]) -> pd.DataFrame:
    rows = []
    available = df[item_col].tolist()
    for rule in rules:
        for match in describe_source_matches(available, rule["source_items"]):
            rows.append(
                {
                    "Source Item": match["requested_item"],
                    "Matched Source Item": match["matched_item"],
                    "Standard Item": rule["standard_item"],
                    "Section": rule["statement_section"],
                    "Bucket": rule["bucket"],
                    "Exists in Source": match["exists"],
                    "Match Type": match["match_type"],
                }
            )
    return pd.DataFrame(rows)


def build_standardized_pl(df: pd.DataFrame, item_col: str, year_cols: List[str], rules: List[Dict]) -> pd.DataFrame:
    rows = []
    for rule in rules:
        values = safe_row_sum(df, item_col, year_cols, rule["source_items"])
        for year in year_cols:
            rows.append(
                {
                    "Year": year,
                    "Section": rule["statement_section"],
                    "Standard Item": rule["standard_item"],
                    "Bucket": rule["bucket"],
                    "Value": float(values[year]),
                }
            )
    return pd.DataFrame(rows)


def build_standardized_wide(standardized_df: pd.DataFrame) -> pd.DataFrame:
    return _build_wide(standardized_df, index_cols=["Section", "Bucket", "Standard Item"])


def build_standardized_item_wide(standardized_df: pd.DataFrame) -> pd.DataFrame:
    return _build_item_wide(standardized_df, item_col="Standard Item")


def build_analysis_bridge(df: pd.DataFrame, item_col: str, year_cols: List[str], rules: List[Dict]) -> pd.DataFrame:
    rows = []
    for rule in rules:
        values = safe_row_sum(df, item_col, year_cols, rule["source_items"])
        rows.append(
            {
                "Section": rule["statement_section"],
                "Bucket": rule["bucket"],
                "Standard Item": rule["standard_item"],
                "Source Items": " + ".join(rule["source_items"]),
                "Formula": rule["formula_desc"],
                **{year: float(values[year]) for year in year_cols},
            }
        )
    return pd.DataFrame(rows)


def build_valuation_input_sheet(standardized_df: pd.DataFrame) -> pd.DataFrame:
    wide = standardized_df.pivot_table(index="Standard Item", columns="Year", values="Value", aggfunc="sum")
    years = list(wide.columns)

    def get_metric(name: str) -> pd.Series:
        if name in wide.index:
            return wide.loc[name]
        return pd.Series([0.0] * len(years), index=years)

    revenue = get_metric("Revenue")
    cogs = get_metric("COGS")
    tax_surcharge = get_metric("Taxes & Surcharges")
    selling = get_metric("Selling Expense")
    admin = get_metric("Admin Expense")
    rnd = get_metric("R&D Expense")
    fin = get_metric("Financial Expense")
    impairment = get_metric("Asset / Credit Impairment")
    other_gains = get_metric("Other Operating Gains")
    operating_profit = get_metric("Operating Profit")
    ebt = get_metric("Profit Before Tax")
    income_tax = get_metric("Income Tax")
    parent_np = get_metric("Parent Net Profit")
    adjusted_np = get_metric("Adjusted Net Profit")
    parent_oci = get_metric("Parent OCI")
    basic_eps = get_metric("Basic EPS")

    gross_profit = revenue - cogs - tax_surcharge
    core_opex = selling + admin + rnd
    ebit = operating_profit + fin
    ebitda_proxy = ebit + impairment
    tax_rate = income_tax.divide(ebt.replace(0, pd.NA)).fillna(0.0)
    gross_margin = gross_profit.divide(revenue.replace(0, pd.NA)).fillna(0.0)
    operating_margin = operating_profit.divide(revenue.replace(0, pd.NA)).fillna(0.0)
    net_margin = parent_np.divide(revenue.replace(0, pd.NA)).fillna(0.0)
    adjusted_margin = adjusted_np.divide(revenue.replace(0, pd.NA)).fillna(0.0)

    rows = [
        ("Reported", "Revenue", "主营收入口径", revenue),
        ("Reported", "Gross Profit", "收入-营业成本-税金及附加", gross_profit),
        ("Reported", "Core Opex", "销售+管理+研发", core_opex),
        ("Reported", "Financial Expense", "财务费用", fin),
        ("Reported", "Impairment", "资产减值+信用减值", impairment),
        ("Reported", "Other Operating Gains", "公允价值/投资/处置/其他收益", other_gains),
        ("Reported", "Operating Profit", "营业利润", operating_profit),
        ("Reported", "EBIT", "营业利润+财务费用", ebit),
        ("Reported", "EBITDA Proxy", "EBIT+减值损失", ebitda_proxy),
        ("Reported", "Profit Before Tax", "利润总额", ebt),
        ("Reported", "Income Tax", "所得税费用", income_tax),
        ("Reported", "Parent Net Profit", "归母净利润", parent_np),
        ("Reported", "Adjusted Net Profit", "扣非归母净利润近似口径", adjusted_np),
        ("Reported", "Parent OCI", "归母其他综合收益", parent_oci),
        ("Ratio", "Gross Margin", "毛利率", gross_margin),
        ("Ratio", "Operating Margin", "营业利润率", operating_margin),
        ("Ratio", "Parent Net Margin", "归母净利率", net_margin),
        ("Ratio", "Adjusted Net Margin", "扣非净利率", adjusted_margin),
        ("Ratio", "Effective Tax Rate", "有效税率", tax_rate),
        ("Per Share", "Basic EPS", "基本每股收益", basic_eps),
    ]

    out = []
    for section, metric, note, series in rows:
        row = {"Section": section, "Metric": metric, "Note": note}
        for year in years:
            row[year] = float(series[year])
        out.append(row)
    return pd.DataFrame(out)


def generate_markdown_doc(pre_check_df: pd.DataFrame, rules: List[Dict]) -> str:
    check_lines = []
    for _, row in pre_check_df.iterrows():
        status = "通过" if bool(row["All_Passed"]) else "需复核"
        check_lines.append(
            f"- {row['Year']}: 营业利润差额={row['营业利润校验差额']:.2f}，利润总额差额={row['利润总额校验差额']:.2f}，净利润差额={row['净利润校验差额']:.2f}，归母拆分差额={row['归母拆分校验差额']:.2f}，状态={status}"
        )

    mapping_lines = [
        f"- `{rule['statement_section']} / {rule['bucket']}` -> **{rule['standard_item']}** = {rule['formula_desc']}"
        for rule in rules
    ]

    return f"""# 利润表重构说明

## 1. 目标
将原始 `pl.csv` 重构为更适合估值分析的利润表口径，输出标准化科目、映射明细、分析 bridge 和 Excel 工作簿。

## 2. 校验结果
{chr(10).join(check_lines)}

## 3. 标准化映射
{chr(10).join(mapping_lines)}

## 4. 输出文件
- `1_preprocess_pl.csv`：预处理后的利润表
- `_preprocess_check.csv`：关键勾稽校验
- `2_standardized_pl.csv`：标准化长表
- `2_standardized_pl_wide.csv`：标准化宽表（行为项目，列为时间）
- `3_mapping_detail.csv`：原始科目到标准科目的映射
- `4_analysis_bridge.csv`：估值分析 bridge
- `5_valuation_ready_pl.xlsx`：Excel 打包结果
"""


def export_excel_package(
    output_path: str,
    preprocess_df: pd.DataFrame,
    pre_check_df: pd.DataFrame,
    standardized_df: pd.DataFrame,
    mapping_detail_df: pd.DataFrame,
    bridge_df: pd.DataFrame,
    valuation_df: pd.DataFrame,
) -> None:
    standardized_wide = build_standardized_wide(standardized_df)
    readme_rows = [
        ("Preprocess_PL", "原始利润表清洗后的结果"),
        ("Preprocess_Check", "利润表关键勾稽校验"),
        ("Standardized_Long", "标准化利润表长表"),
        ("Standardized_Wide", "标准化利润表宽表"),
        ("Valuation_Input", "可直接用于估值建模的利润指标"),
        ("Mapping_Detail", "原始科目与标准科目的映射关系"),
        ("Analysis_Bridge", "估值分析 bridge"),
    ]
    sheets = {
        "Preprocess_PL": preprocess_df,
        "Preprocess_Check": pre_check_df,
        "Standardized_Long": standardized_df,
        "Standardized_Wide": standardized_wide,
        "Valuation_Input": valuation_df,
        "Mapping_Detail": mapping_detail_df,
        "Analysis_Bridge": bridge_df,
    }
    col_widths = {
        "README": [22, 48],
        "Preprocess_PL": [32] + [14] * (preprocess_df.shape[1] - 1),
        "Preprocess_Check": [10, 18, 18, 18, 18, 12],
        "Standardized_Long": [10, 20, 28, 18, 16],
        "Standardized_Wide": [18, 18, 28] + [14] * (standardized_wide.shape[1] - 3),
        "Valuation_Input": [12, 22, 28] + [14] * (valuation_df.shape[1] - 3),
        "Mapping_Detail": [32, 28, 18, 18, 12],
        "Analysis_Bridge": [18, 18, 28, 42, 36] + [14] * max(1, bridge_df.shape[1] - 5),
    }
    export_statement_excel(
        output_path=output_path,
        readme_rows=readme_rows,
        sheets=sheets,
        col_widths=col_widths,
        valuation_sheet_name="Valuation_Input",
    )


def save_outputs(
    output_dir: str,
    preprocess_df: pd.DataFrame,
    pre_check_df: pd.DataFrame,
    standardized_df: pd.DataFrame,
    mapping_detail_df: pd.DataFrame,
    bridge_df: pd.DataFrame,
    valuation_df: pd.DataFrame,
    md_text: str,
) -> None:
    ensure_output_dir(output_dir)
    preprocess_df.to_csv(os.path.join(output_dir, "1_preprocess_pl.csv"), index=False, encoding="utf-8-sig")
    pre_check_df.to_csv(os.path.join(output_dir, "_preprocess_check.csv"), index=False, encoding="utf-8-sig")
    standardized_df.to_csv(os.path.join(output_dir, "2_standardized_pl.csv"), index=False, encoding="utf-8-sig")
    build_standardized_item_wide(standardized_df).to_csv(os.path.join(output_dir, "2_standardized_pl_wide.csv"), index=False, encoding="utf-8-sig")
    mapping_detail_df.to_csv(os.path.join(output_dir, "3_mapping_detail.csv"), index=False, encoding="utf-8-sig")
    bridge_df.to_csv(os.path.join(output_dir, "4_analysis_bridge.csv"), index=False, encoding="utf-8-sig")
    pl_excel_path = os.path.join(output_dir, "5_valuation_ready_pl.xlsx")
    export_excel_package(
        output_path=pl_excel_path,
        preprocess_df=preprocess_df,
        pre_check_df=pre_check_df,
        standardized_df=standardized_df,
        mapping_detail_df=mapping_detail_df,
        bridge_df=bridge_df,
        valuation_df=valuation_df,
    )
    apply_bilingual_fonts_to_file(pl_excel_path)
    with open(os.path.join(output_dir, "PL重构说明.md"), "w", encoding="utf-8") as f:
        f.write(md_text)


def main(input_csv: str = str(CSV_DIR / "pl.csv"), output_dir: str = OUTPUT_DIR) -> None:
    df, item_col, year_cols = load_pl_csv(input_csv)
    preprocess_df, pre_check_df = preprocess_pl(df, item_col, year_cols)
    rules = build_mapping_rules()
    mapping_detail_df = build_mapping_detail(preprocess_df, item_col, rules)
    standardized_df = build_standardized_pl(preprocess_df, item_col, year_cols, rules)
    bridge_df = build_analysis_bridge(preprocess_df, item_col, year_cols, rules)
    valuation_df = build_valuation_input_sheet(standardized_df)
    md_text = generate_markdown_doc(pre_check_df, rules)
    save_outputs(output_dir, preprocess_df, pre_check_df, standardized_df, mapping_detail_df, bridge_df, valuation_df, md_text)
    logger.info("利润表重构完成，输出目录：%s", output_dir)


if __name__ == "__main__":
    main()
