import os
from typing import Dict, List, Tuple

import pandas as pd


OUTPUT_DIR = "./results/PL_rebuilt_output"


def ensure_output_dir(output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)


def normalize_item_name(name: str) -> str:
    if pd.isna(name):
        return ""
    return str(name).replace("\ufeff", "").replace("*", "").strip()


def to_numeric_frame(df: pd.DataFrame, year_cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in year_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def safe_row_sum(df: pd.DataFrame, item_col: str, year_cols: List[str], item_names: List[str]) -> pd.Series:
    mask = df[item_col].isin(item_names)
    if not mask.any():
        return pd.Series([0.0] * len(year_cols), index=year_cols)
    return df.loc[mask, year_cols].sum()


def load_pl_csv(input_path: str) -> Tuple[pd.DataFrame, str, List[str]]:
    df = pd.read_csv(input_path)
    first_col = df.columns[0]
    year_cols = [str(c) for c in df.columns[1:]]
    if not year_cols:
        raise ValueError("利润表 CSV 未识别到年份列。")
    df = df.rename(columns={first_col: "科目"})
    df["科目"] = df["科目"].apply(normalize_item_name)
    df = to_numeric_frame(df, year_cols)
    return df, "科目", year_cols


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
    return [
        {
            "standard_item": "Revenue",
            "statement_section": "Operating",
            "bucket": "Revenue",
            "source_items": ["其中：营业收入(元)"],
            "formula_desc": "营业收入",
        },
        {
            "standard_item": "COGS",
            "statement_section": "Operating",
            "bucket": "Cost",
            "source_items": ["其中：营业成本(元)"],
            "formula_desc": "营业成本",
        },
        {
            "standard_item": "Taxes & Surcharges",
            "statement_section": "Operating",
            "bucket": "Cost",
            "source_items": ["营业税金及附加(元)"],
            "formula_desc": "营业税金及附加",
        },
        {
            "standard_item": "Selling Expense",
            "statement_section": "Operating",
            "bucket": "Opex",
            "source_items": ["销售费用(元)"],
            "formula_desc": "销售费用",
        },
        {
            "standard_item": "Admin Expense",
            "statement_section": "Operating",
            "bucket": "Opex",
            "source_items": ["管理费用(元)"],
            "formula_desc": "管理费用",
        },
        {
            "standard_item": "R&D Expense",
            "statement_section": "Operating",
            "bucket": "Opex",
            "source_items": ["研发费用(元)"],
            "formula_desc": "研发费用",
        },
        {
            "standard_item": "Financial Expense",
            "statement_section": "Operating",
            "bucket": "Opex",
            "source_items": ["财务费用(元)"],
            "formula_desc": "财务费用",
        },
        {
            "standard_item": "Asset / Credit Impairment",
            "statement_section": "Operating",
            "bucket": "Opex",
            "source_items": ["资产减值损失(元)", "信用减值损失(元)"],
            "formula_desc": "资产减值损失 + 信用减值损失",
        },
        {
            "standard_item": "Other Operating Gains",
            "statement_section": "Operating",
            "bucket": "Other Income",
            "source_items": ["加：公允价值变动收益(元)", "投资收益(元)", "资产处置收益(元)", "其他收益(元)"],
            "formula_desc": "公允价值变动收益 + 投资收益 + 资产处置收益 + 其他收益",
        },
        {
            "standard_item": "Operating Profit",
            "statement_section": "Operating",
            "bucket": "Profit",
            "source_items": ["三、营业利润(元)"],
            "formula_desc": "营业利润",
        },
        {
            "standard_item": "Non-operating Income",
            "statement_section": "Below Operating",
            "bucket": "Non-operating",
            "source_items": ["加：营业外收入(元)"],
            "formula_desc": "营业外收入",
        },
        {
            "standard_item": "Non-operating Expense",
            "statement_section": "Below Operating",
            "bucket": "Non-operating",
            "source_items": ["减：营业外支出(元)"],
            "formula_desc": "营业外支出",
        },
        {
            "standard_item": "Profit Before Tax",
            "statement_section": "Below Operating",
            "bucket": "Profit",
            "source_items": ["四、利润总额(元)"],
            "formula_desc": "利润总额",
        },
        {
            "standard_item": "Income Tax",
            "statement_section": "Below Operating",
            "bucket": "Tax",
            "source_items": ["减：所得税费用(元)"],
            "formula_desc": "所得税费用",
        },
        {
            "standard_item": "Net Profit",
            "statement_section": "Below Operating",
            "bucket": "Profit",
            "source_items": ["五、净利润(元)"],
            "formula_desc": "净利润",
        },
        {
            "standard_item": "Parent Net Profit",
            "statement_section": "Equity Attribution",
            "bucket": "Attribution",
            "source_items": ["归属于母公司所有者的净利润(元)"],
            "formula_desc": "归母净利润",
        },
        {
            "standard_item": "Minority Interest Profit",
            "statement_section": "Equity Attribution",
            "bucket": "Attribution",
            "source_items": ["少数股东损益(元)"],
            "formula_desc": "少数股东损益",
        },
        {
            "standard_item": "Adjusted Net Profit",
            "statement_section": "Equity Attribution",
            "bucket": "Adjusted",
            "source_items": ["扣除非经常性损益后的净利润(元)"],
            "formula_desc": "扣非净利润",
        },
        {
            "standard_item": "Parent OCI",
            "statement_section": "Comprehensive Income",
            "bucket": "OCI",
            "source_items": ["归属母公司所有者的其他综合收益(元)"],
            "formula_desc": "归母其他综合收益",
        },
        {
            "standard_item": "Parent Comprehensive Income",
            "statement_section": "Comprehensive Income",
            "bucket": "Comprehensive",
            "source_items": ["归属于母公司股东的综合收益总额(元)"],
            "formula_desc": "归母综合收益",
        },
        {
            "standard_item": "Basic EPS",
            "statement_section": "Per Share",
            "bucket": "Per Share",
            "source_items": ["（一）基本每股收益(元)"],
            "formula_desc": "基本每股收益",
        },
        {
            "standard_item": "Diluted EPS",
            "statement_section": "Per Share",
            "bucket": "Per Share",
            "source_items": ["（二）稀释每股收益(元)"],
            "formula_desc": "稀释每股收益",
        },
    ]


def build_mapping_detail(df: pd.DataFrame, item_col: str, rules: List[Dict]) -> pd.DataFrame:
    rows = []
    available = set(df[item_col].tolist())
    for rule in rules:
        for source in rule["source_items"]:
            rows.append(
                {
                    "Source Item": source,
                    "Standard Item": rule["standard_item"],
                    "Section": rule["statement_section"],
                    "Bucket": rule["bucket"],
                    "Exists in Source": source in available,
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
    wide = standardized_df.pivot_table(
        index=["Section", "Bucket", "Standard Item"],
        columns="Year",
        values="Value",
        aggfunc="sum",
    ).reset_index()
    wide.columns.name = None
    return wide


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
    try:
        writer = pd.ExcelWriter(output_path, engine="xlsxwriter")
        use_xlsxwriter = True
    except Exception:
        writer = pd.ExcelWriter(output_path, engine="openpyxl")
        use_xlsxwriter = False

    with writer:
        readme_df = pd.DataFrame(
            {
                "Sheet": ["Preprocess_PL", "Preprocess_Check", "Standardized_Long", "Standardized_Wide", "Valuation_Input", "Mapping_Detail", "Analysis_Bridge"],
                "Description": [
                    "原始利润表清洗后的结果",
                    "利润表关键勾稽校验",
                    "标准化利润表长表",
                    "标准化利润表宽表",
                    "可直接用于估值建模的利润指标",
                    "原始科目与标准科目的映射关系",
                    "估值分析 bridge",
                ],
            }
        )
        readme_df.to_excel(writer, sheet_name="README", index=False)
        preprocess_df.to_excel(writer, sheet_name="Preprocess_PL", index=False)
        pre_check_df.to_excel(writer, sheet_name="Preprocess_Check", index=False)
        standardized_df.to_excel(writer, sheet_name="Standardized_Long", index=False)
        standardized_wide.to_excel(writer, sheet_name="Standardized_Wide", index=False)
        valuation_df.to_excel(writer, sheet_name="Valuation_Input", index=False)
        mapping_detail_df.to_excel(writer, sheet_name="Mapping_Detail", index=False)
        bridge_df.to_excel(writer, sheet_name="Analysis_Bridge", index=False)

        if use_xlsxwriter:
            workbook = writer.book
            header_fmt = workbook.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1, "align": "center"})
            num_fmt = workbook.add_format({"num_format": "#,##0.00"})
            ratio_fmt = workbook.add_format({"num_format": "0.00%"})
            widths = {
                "README": [22, 48],
                "Preprocess_PL": [32] + [14] * (preprocess_df.shape[1] - 1),
                "Preprocess_Check": [10, 18, 18, 18, 18, 12],
                "Standardized_Long": [10, 20, 28, 18, 16],
                "Standardized_Wide": [18, 18, 28] + [14] * (standardized_wide.shape[1] - 3),
                "Valuation_Input": [12, 22, 28] + [14] * (valuation_df.shape[1] - 3),
                "Mapping_Detail": [32, 28, 18, 18, 12],
                "Analysis_Bridge": [18, 18, 28, 42, 36] + [14] * max(1, bridge_df.shape[1] - 5),
            }
            for sheet_name, col_widths in widths.items():
                ws = writer.sheets[sheet_name]
                ws.freeze_panes(1, 1)
                for idx, width in enumerate(col_widths):
                    ws.set_column(idx, idx, width)
                df_map = {
                    "README": readme_df,
                    "Preprocess_PL": preprocess_df,
                    "Preprocess_Check": pre_check_df,
                    "Standardized_Long": standardized_df,
                    "Standardized_Wide": standardized_wide,
                    "Valuation_Input": valuation_df,
                    "Mapping_Detail": mapping_detail_df,
                    "Analysis_Bridge": bridge_df,
                }
                cur_df = df_map[sheet_name]
                for col_num, value in enumerate(cur_df.columns):
                    ws.write(0, col_num, value, header_fmt)
                if sheet_name == "Valuation_Input":
                    for row_idx in range(1, valuation_df.shape[0] + 1):
                        section = valuation_df.iloc[row_idx - 1]["Section"]
                        for col_idx in range(3, valuation_df.shape[1]):
                            val = valuation_df.iloc[row_idx - 1, col_idx]
                            fmt = ratio_fmt if section == "Ratio" else num_fmt
                            ws.write_number(row_idx, col_idx, float(val), fmt)


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
    mapping_detail_df.to_csv(os.path.join(output_dir, "3_mapping_detail.csv"), index=False, encoding="utf-8-sig")
    bridge_df.to_csv(os.path.join(output_dir, "4_analysis_bridge.csv"), index=False, encoding="utf-8-sig")
    export_excel_package(
        output_path=os.path.join(output_dir, "5_valuation_ready_pl.xlsx"),
        preprocess_df=preprocess_df,
        pre_check_df=pre_check_df,
        standardized_df=standardized_df,
        mapping_detail_df=mapping_detail_df,
        bridge_df=bridge_df,
        valuation_df=valuation_df,
    )
    with open(os.path.join(output_dir, "PL重构说明.md"), "w", encoding="utf-8") as f:
        f.write(md_text)


def main(input_csv: str = "./results/csv/pl.csv", output_dir: str = OUTPUT_DIR) -> None:
    df, item_col, year_cols = load_pl_csv(input_csv)
    preprocess_df, pre_check_df = preprocess_pl(df, item_col, year_cols)
    rules = build_mapping_rules()
    mapping_detail_df = build_mapping_detail(preprocess_df, item_col, rules)
    standardized_df = build_standardized_pl(preprocess_df, item_col, year_cols, rules)
    bridge_df = build_analysis_bridge(preprocess_df, item_col, year_cols, rules)
    valuation_df = build_valuation_input_sheet(standardized_df)
    md_text = generate_markdown_doc(pre_check_df, rules)
    save_outputs(output_dir, preprocess_df, pre_check_df, standardized_df, mapping_detail_df, bridge_df, valuation_df, md_text)
    print(f"利润表重构完成，输出目录：{output_dir}")


if __name__ == "__main__":
    main()
