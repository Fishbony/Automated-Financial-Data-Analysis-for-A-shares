
import os
import re
import json
from typing import Dict, List, Tuple

import pandas as pd


OUTPUT_DIR = "./results/BS_rebuilt_output"


def ensure_output_dir(output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)


def normalize_item_name(name: str) -> str:
    if pd.isna(name):
        return ""
    name = str(name).strip()
    name = name.replace("\\ufeff", "")
    name = re.sub(r"^[*＊]+", "", name)
    name = re.sub(r"\s+", "", name)
    return name


def to_numeric_frame(df: pd.DataFrame, year_cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in year_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def safe_row_sum(df: pd.DataFrame, item_col: str, year_cols: List[str], item_name: str) -> pd.Series:
    mask = df[item_col] == item_name
    if not mask.any():
        return pd.Series([0.0] * len(year_cols), index=year_cols)
    return df.loc[mask, year_cols].sum()


def load_bs_csv(input_path: str) -> Tuple[pd.DataFrame, str, List[str]]:
    df = pd.read_csv(input_path)
    first_col = df.columns[0]
    year_cols = [c for c in df.columns[1:]]
    if not year_cols:
        raise ValueError("未识别到年份列，请检查CSV格式。")
    df = df.rename(columns={first_col: "原始科目"})
    df["原始科目"] = df["原始科目"].apply(normalize_item_name)
    df = to_numeric_frame(df, year_cols)
    return df, "原始科目", year_cols


def build_drop_list() -> List[str]:
    return [
        "其中：应收票据(元)",
        "应收账款(元)",
        "其中：应收利息(元)",
        "其他应收款(元)",
        "总现金(元)",
        "其中：固定资产(元)",
        "固定资产清理(元)",
        "其中：在建工程(元)",
        "其中：应付票据(元)",
        "应付账款(元)",
        "其中：应付利息(元)",
        "应付股利(元)",
        "其他应付款(元)",
        "其中：长期应付款(元)",
        "专项应付款(元)",
    ]


def build_preprocess_order() -> List[str]:
    return [
        "资产合计(元)",
        "货币资金(元)",
        "交易性金融资产(元)",
        "一年内到期的非流动资产(元)",
        "应收票据及应收账款(元)",
        "预付款项(元)",
        "存货(元)",
        "其他应收款合计(元)",
        "其他流动资产(元)",
        "流动资产合计(元)",
        "固定资产合计(元)",
        "在建工程合计(元)",
        "无形资产(元)",
        "长期股权投资(元)",
        "其他权益工具投资(元)",
        "投资性房地产(元)",
        "商誉(元)",
        "长期待摊费用(元)",
        "递延所得税资产(元)",
        "可供出售金融资产(元)",
        "持有至到期投资(元)",
        "其他非流动资产(元)",
        "非流动资产合计(元)",
        "负债合计(元)",
        "短期借款(元)",
        "一年内到期的非流动负债(元)",
        "应付票据及应付账款(元)",
        "预收款项(元)",
        "合同负债(元)",
        "应付职工薪酬(元)",
        "应交税费(元)",
        "衍生金融负债(元)",
        "其他应付款合计(元)",
        "其他流动负债(元)",
        "流动负债合计(元)",
        "长期借款(元)",
        "应付债券(元)",
        "长期应付款合计(元)",
        "预计负债(元)",
        "递延所得税负债(元)",
        "递延收益-非流动负债(元)",
        "其他非流动负债(元)",
        "非流动负债合计(元)",
        "实收资本（或股本）(元)",
        "资本公积(元)",
        "减：库存股(元)",
        "盈余公积(元)",
        "未分配利润(元)",
        "其他综合收益(元)",
        "归属于母公司所有者权益合计(元)",
        "少数股东权益(元)",
        "所有者权益（或股东权益）合计(元)",
        "负债和所有者权益（或股东权益）合计(元)",
    ]


def preprocess_bs(df: pd.DataFrame, item_col: str, year_cols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    drop_items = set(build_drop_list())
    df = df.drop_duplicates(subset=[item_col], keep="first").copy()
    df = df[~df[item_col].isin(drop_items)].copy()
    df = df.drop_duplicates(subset=[item_col], keep="first").copy()

    order = build_preprocess_order()
    order_map = {name: idx for idx, name in enumerate(order)}
    df["排序"] = df[item_col].map(lambda x: order_map.get(x, 9999))
    df = df.sort_values(["排序", item_col]).drop(columns=["排序"]).reset_index(drop=True)

    checks = []
    current_assets = safe_row_sum(df, item_col, year_cols, "流动资产合计(元)")
    non_current_assets = safe_row_sum(df, item_col, year_cols, "非流动资产合计(元)")
    current_liab = safe_row_sum(df, item_col, year_cols, "流动负债合计(元)")
    non_current_liab = safe_row_sum(df, item_col, year_cols, "非流动负债合计(元)")
    equity = safe_row_sum(df, item_col, year_cols, "所有者权益（或股东权益）合计(元)")

    for y in year_cols:
        lhs = current_assets[y] + non_current_assets[y]
        rhs = current_liab[y] + non_current_liab[y] + equity[y]
        diff = lhs - rhs
        checks.append(
            {
                "Year": y,
                "Assets_CurrentPlusNonCurrent": float(lhs),
                "LiabPlusEquity": float(rhs),
                "Diff": float(diff),
                "Balanced": abs(diff) < 1e+6,
            }
        )

    return df, pd.DataFrame(checks)


def build_mapping_rules() -> List[Dict]:
    return [
        {
            "standard_item": "Cash & Short-term Financial Assets",
            "statement_side": "Assets",
            "bucket": "Current Assets",
            "classification": "现金及短期金融资产",
            "source_items": ["货币资金(元)", "交易性金融资产(元)", "一年内到期的非流动资产(元)"],
            "formula_desc": "货币资金 + 交易性金融资产 + 一年内到期的非流动资产",
        },
        {
            "standard_item": "Core Operating Current Assets",
            "statement_side": "Assets",
            "bucket": "Current Assets",
            "classification": "核心经营性营运流动资产",
            "source_items": ["应收票据及应收账款(元)", "预付款项(元)", "存货(元)", "其他应收款合计(元)"],
            "formula_desc": "应收票据及应收账款 + 预付款项 + 存货 + 其他应收款合计",
        },
        {
            "standard_item": "Non-operating Misc. Current Assets",
            "statement_side": "Assets",
            "bucket": "Current Assets",
            "classification": "非经营性杂项流动资产",
            "source_items": ["其他流动资产(元)"],
            "formula_desc": "其他流动资产",
        },
        {
            "standard_item": "Long-term Core Operating Assets",
            "statement_side": "Assets",
            "bucket": "Non-current Assets",
            "classification": "长期经营性核心资产",
            "source_items": ["固定资产合计(元)", "在建工程合计(元)", "无形资产(元)", "投资性房地产(元)"],
            "formula_desc": "固定资产合计 + 在建工程合计 + 无形资产 + 投资性房地产",
        },
        {
            "standard_item": "Long-term Financial & Equity Investments",
            "statement_side": "Assets",
            "bucket": "Non-current Assets",
            "classification": "长期对外财务&股权投资",
            "source_items": ["长期股权投资(元)", "其他权益工具投资(元)", "可供出售金融资产(元)", "持有至到期投资(元)"],
            "formula_desc": "长期股权投资 + 其他权益工具投资 + 可供出售金融资产 + 持有至到期投资",
        },
        {
            "standard_item": "Risk & Amortizing Assets",
            "statement_side": "Assets",
            "bucket": "Non-current Assets",
            "classification": "风险类&摊销类资产",
            "source_items": ["商誉(元)", "长期待摊费用(元)"],
            "formula_desc": "商誉 + 长期待摊费用",
        },
        {
            "standard_item": "Tax & Other Long-term Assets",
            "statement_side": "Assets",
            "bucket": "Non-current Assets",
            "classification": "税务&杂项长期资产",
            "source_items": ["递延所得税资产(元)", "其他非流动资产(元)"],
            "formula_desc": "递延所得税资产 + 其他非流动资产",
        },
        {
            "standard_item": "Interest-bearing Short-term Debt",
            "statement_side": "Liabilities",
            "bucket": "Current Liabilities",
            "classification": "刚性有息短期债务",
            "source_items": ["短期借款(元)", "一年内到期的非流动负债(元)"],
            "formula_desc": "短期借款 + 一年内到期的非流动负债",
        },
        {
            "standard_item": "Operating Non-interest-bearing Current Liabilities",
            "statement_side": "Liabilities",
            "bucket": "Current Liabilities",
            "classification": "经营性无息流动负债",
            "source_items": ["应付票据及应付账款(元)", "预收款项(元)", "合同负债(元)", "应付职工薪酬(元)", "应交税费(元)"],
            "formula_desc": "应付票据及应付账款 + 预收款项 + 合同负债 + 应付职工薪酬 + 应交税费",
        },
        {
            "standard_item": "Non-operating Misc. Current Liabilities",
            "statement_side": "Liabilities",
            "bucket": "Current Liabilities",
            "classification": "非经营杂项流动负债",
            "source_items": ["衍生金融负债(元)", "其他应付款合计(元)", "其他流动负债(元)"],
            "formula_desc": "衍生金融负债 + 其他应付款合计 + 其他流动负债",
        },
        {
            "standard_item": "Long-term Interest-bearing Debt",
            "statement_side": "Liabilities",
            "bucket": "Non-current Liabilities",
            "classification": "长期刚性有息债务",
            "source_items": ["长期借款(元)", "应付债券(元)", "长期应付款合计(元)"],
            "formula_desc": "长期借款 + 应付债券 + 长期应付款合计",
        },
        {
            "standard_item": "Long-term Operating Non-interest-bearing Liabilities",
            "statement_side": "Liabilities",
            "bucket": "Non-current Liabilities",
            "classification": "长期经营性无息负债",
            "source_items": ["预计负债(元)"],
            "formula_desc": "预计负债",
        },
        {
            "standard_item": "Tax & Subsidy-related Non-cash Liabilities",
            "statement_side": "Liabilities",
            "bucket": "Non-current Liabilities",
            "classification": "税务&补贴类非现金负债",
            "source_items": ["递延所得税负债(元)", "递延收益-非流动负债(元)", "其他非流动负债(元)"],
            "formula_desc": "递延所得税负债 + 递延收益-非流动负债 + 其他非流动负债",
        },
        {
            "standard_item": "Parent Contributed Capital",
            "statement_side": "Equity",
            "bucket": "Equity",
            "classification": "归母投入股本",
            "source_items": ["实收资本（或股本）(元)", "资本公积(元)", "减：库存股(元)"],
            "formula_desc": "实收资本（或股本） + 资本公积 - 库存股（若库存股为负则直接相加）",
        },
        {
            "standard_item": "Parent Retained Earnings",
            "statement_side": "Equity",
            "bucket": "Equity",
            "classification": "归母累计留存利润",
            "source_items": ["盈余公积(元)", "未分配利润(元)"],
            "formula_desc": "盈余公积 + 未分配利润",
        },
        {
            "standard_item": "Other Comprehensive Income (OCI)",
            "statement_side": "Equity",
            "bucket": "Equity",
            "classification": "其他综合收益（OCI）",
            "source_items": ["其他综合收益(元)"],
            "formula_desc": "其他综合收益",
        },
        {
            "standard_item": "Minority Interest",
            "statement_side": "Equity",
            "bucket": "Equity",
            "classification": "少数股东权益",
            "source_items": ["少数股东权益(元)"],
            "formula_desc": "少数股东权益",
        },
    ]


def build_mapping_detail(pre_df: pd.DataFrame, item_col: str, rules: List[Dict]) -> pd.DataFrame:
    rows = []
    existing_items = set(pre_df[item_col].tolist())
    for rule in rules:
        for src in rule["source_items"]:
            rows.append(
                {
                    "原始科目": src,
                    "标准科目": rule["standard_item"],
                    "分类": f'{rule["statement_side"]} / {rule["bucket"]} / {rule["classification"]}',
                    "是否合并": "是" if len(rule["source_items"]) > 1 else "否",
                    "是否在原始表中存在": "是" if src in existing_items else "否",
                }
            )
    return pd.DataFrame(rows)


def build_standardized_bs(pre_df: pd.DataFrame, item_col: str, year_cols: List[str], rules: List[Dict]) -> pd.DataFrame:
    records = []
    for rule in rules:
        combined = pd.Series([0.0] * len(year_cols), index=year_cols)
        for src in rule["source_items"]:
            combined = combined + safe_row_sum(pre_df, item_col, year_cols, src)
        for y in year_cols:
            records.append(
                {
                    "StatementSide": rule["statement_side"],
                    "Bucket": rule["bucket"],
                    "StandardLineItem": rule["standard_item"],
                    "Year": y,
                    "Value": float(combined[y]),
                }
            )

    std_long = pd.DataFrame(records)
    subtotal_rows = []

    def subtotal(side: str, bucket: str, line_name: str) -> None:
        grp = std_long[(std_long["StatementSide"] == side) & (std_long["Bucket"] == bucket)].groupby("Year", as_index=False)["Value"].sum()
        for _, r in grp.iterrows():
            subtotal_rows.append(
                {
                    "StatementSide": side,
                    "Bucket": bucket,
                    "StandardLineItem": line_name,
                    "Year": r["Year"],
                    "Value": float(r["Value"]),
                }
            )

    subtotal("Assets", "Current Assets", "Total Current Assets")
    subtotal("Assets", "Non-current Assets", "Total Non-current Assets")
    subtotal("Liabilities", "Current Liabilities", "Total Current Liabilities")
    subtotal("Liabilities", "Non-current Liabilities", "Total Non-current Liabilities")
    subtotal("Equity", "Equity", "Total Equity")
    std_long = pd.concat([std_long, pd.DataFrame(subtotal_rows)], ignore_index=True)

    total_rows = []
    for y in year_cols:
        tca = std_long.query("Year == @y and StandardLineItem == 'Total Current Assets'")["Value"].sum()
        tnca = std_long.query("Year == @y and StandardLineItem == 'Total Non-current Assets'")["Value"].sum()
        tcl = std_long.query("Year == @y and StandardLineItem == 'Total Current Liabilities'")["Value"].sum()
        tncl = std_long.query("Year == @y and StandardLineItem == 'Total Non-current Liabilities'")["Value"].sum()
        teq = std_long.query("Year == @y and StandardLineItem == 'Total Equity'")["Value"].sum()
        total_rows.extend(
            [
                {"StatementSide": "Assets", "Bucket": "Assets", "StandardLineItem": "Total Assets", "Year": y, "Value": float(tca + tnca)},
                {"StatementSide": "Liabilities", "Bucket": "Liabilities", "StandardLineItem": "Total Liabilities", "Year": y, "Value": float(tcl + tncl)},
                {"StatementSide": "Liabilities & Equity", "Bucket": "Liabilities & Equity", "StandardLineItem": "Total Liabilities & Equity", "Year": y, "Value": float(tcl + tncl + teq)},
            ]
        )

    std_long = pd.concat([std_long, pd.DataFrame(total_rows)], ignore_index=True)

    balance_rows = []
    for y in year_cols:
        assets = std_long.query("Year == @y and StandardLineItem == 'Total Assets'")["Value"].sum()
        liab_eq = std_long.query("Year == @y and StandardLineItem == 'Total Liabilities & Equity'")["Value"].sum()
        balance_rows.append(
            {
                "StatementSide": "Check",
                "Bucket": "Check",
                "StandardLineItem": "Balance Check Difference",
                "Year": y,
                "Value": float(assets - liab_eq),
            }
        )
    std_long = pd.concat([std_long, pd.DataFrame(balance_rows)], ignore_index=True)

    item_order = [
        "Cash & Short-term Financial Assets",
        "Core Operating Current Assets",
        "Non-operating Misc. Current Assets",
        "Total Current Assets",
        "Long-term Core Operating Assets",
        "Long-term Financial & Equity Investments",
        "Risk & Amortizing Assets",
        "Tax & Other Long-term Assets",
        "Total Non-current Assets",
        "Total Assets",
        "Interest-bearing Short-term Debt",
        "Operating Non-interest-bearing Current Liabilities",
        "Non-operating Misc. Current Liabilities",
        "Total Current Liabilities",
        "Long-term Interest-bearing Debt",
        "Long-term Operating Non-interest-bearing Liabilities",
        "Tax & Subsidy-related Non-cash Liabilities",
        "Total Non-current Liabilities",
        "Total Liabilities",
        "Parent Contributed Capital",
        "Parent Retained Earnings",
        "Other Comprehensive Income (OCI)",
        "Minority Interest",
        "Total Equity",
        "Total Liabilities & Equity",
        "Balance Check Difference",
    ]
    order_map = {name: idx for idx, name in enumerate(item_order)}
    std_long["SortKey"] = std_long["StandardLineItem"].map(lambda x: order_map.get(x, 9999))
    std_long = std_long.sort_values(["Year", "SortKey", "StatementSide", "Bucket"]).drop(columns=["SortKey"]).reset_index(drop=True)
    return std_long


def build_analysis_bridge(pre_df: pd.DataFrame, item_col: str, year_cols: List[str], rules: List[Dict]) -> pd.DataFrame:
    rows = []
    for rule in rules:
        for y in year_cols:
            component_values = {}
            total_value = 0.0
            for src in rule["source_items"]:
                val = float(safe_row_sum(pre_df, item_col, year_cols, src)[y])
                component_values[src] = val
                total_value += val
            rows.append(
                {
                    "Year": y,
                    "StatementSide": rule["statement_side"],
                    "Bucket": rule["bucket"],
                    "StandardLineItem": rule["standard_item"],
                    "Formula": rule["formula_desc"],
                    "ComponentBreakdownJSON": json.dumps(component_values, ensure_ascii=False),
                    "StandardLineItemValue": total_value,
                }
            )
    return pd.DataFrame(rows)


def build_standardized_bs_wide(standardized_df: pd.DataFrame) -> pd.DataFrame:
    wide = standardized_df.pivot_table(
        index=["StatementSide", "Bucket", "StandardLineItem"],
        columns="Year",
        values="Value",
        aggfunc="sum",
    ).reset_index()
    wide.columns.name = None
    return wide


def build_valuation_input_sheet(standardized_df: pd.DataFrame) -> pd.DataFrame:
    wide = build_standardized_bs_wide(standardized_df)
    year_cols = [c for c in wide.columns if isinstance(c, (int, float)) or str(c).isdigit()]
    year_cols = [str(int(c)) if isinstance(c, float) else str(c) for c in year_cols]
    rename_map = {c: str(int(c)) if isinstance(c, float) else str(c) for c in wide.columns if isinstance(c, (int, float))}
    wide = wide.rename(columns=rename_map)

    item_to_label = {
        "Cash & Short-term Financial Assets": ("BS Input", "Cash & Short-term Financial Assets", "现金及短期金融资产"),
        "Interest-bearing Short-term Debt": ("BS Input", "Interest-bearing Short-term Debt", "短期有息债务"),
        "Long-term Interest-bearing Debt": ("BS Input", "Long-term Interest-bearing Debt", "长期有息债务"),
        "Operating Non-interest-bearing Current Liabilities": ("BS Input", "Operating Non-interest-bearing Current Liabilities", "经营性无息流动负债"),
        "Core Operating Current Assets": ("BS Input", "Core Operating Current Assets", "核心经营性营运流动资产"),
        "Minority Interest": ("BS Input", "Minority Interest", "少数股东权益"),
        "Total Equity": ("BS Input", "Total Equity", "总权益"),
        "Total Assets": ("BS Input", "Total Assets", "总资产"),
        "Total Liabilities": ("BS Input", "Total Liabilities", "总负债"),
    }

    records = []
    for line_item, (section, metric, note) in item_to_label.items():
        row = wide[wide["StandardLineItem"] == line_item]
        if row.empty:
            vals = {y: 0.0 for y in year_cols}
        else:
            vals = {y: float(row.iloc[0].get(y, 0.0)) for y in year_cols}
        rec = {"Section": section, "Metric": metric, "Definition": note}
        rec.update(vals)
        records.append(rec)

    metric_df = pd.DataFrame(records)

    derived_rows = []
    if len(metric_df) > 0:
        lookup = {r["Metric"]: r for _, r in metric_df.iterrows()}

        def get(metric: str, year: str) -> float:
            if metric not in lookup:
                return 0.0
            return float(lookup[metric].get(year, 0.0))

        formulas = {
            "Total Interest-bearing Debt": "短期有息债务 + 长期有息债务",
            "Net Debt": "总有息债务 - 现金及短期金融资产",
            "Operating Working Capital": "核心经营性营运流动资产 - 经营性无息流动负债",
            "Debt / Equity": "总有息债务 / 总权益",
            "Net Debt / Equity": "净债务 / 总权益",
            "Minority Interest / Equity": "少数股东权益 / 总权益",
            "Liabilities / Equity": "总负债 / 总权益",
        }

        base = [
            ("Total Interest-bearing Debt", "Derived", formulas["Total Interest-bearing Debt"]),
            ("Net Debt", "Derived", formulas["Net Debt"]),
            ("Operating Working Capital", "Derived", formulas["Operating Working Capital"]),
            ("Debt / Equity", "Ratio", formulas["Debt / Equity"]),
            ("Net Debt / Equity", "Ratio", formulas["Net Debt / Equity"]),
            ("Minority Interest / Equity", "Ratio", formulas["Minority Interest / Equity"]),
            ("Liabilities / Equity", "Ratio", formulas["Liabilities / Equity"]),
        ]

        for metric, section, definition in base:
            rec = {"Section": section, "Metric": metric, "Definition": definition}
            for y in year_cols:
                total_debt = get("Interest-bearing Short-term Debt", y) + get("Long-term Interest-bearing Debt", y)
                net_debt = total_debt - get("Cash & Short-term Financial Assets", y)
                owc = get("Core Operating Current Assets", y) - get("Operating Non-interest-bearing Current Liabilities", y)
                total_equity = get("Total Equity", y)
                minority = get("Minority Interest", y)
                total_liab = get("Total Liabilities", y)
                if metric == "Total Interest-bearing Debt":
                    rec[y] = total_debt
                elif metric == "Net Debt":
                    rec[y] = net_debt
                elif metric == "Operating Working Capital":
                    rec[y] = owc
                elif metric == "Debt / Equity":
                    rec[y] = total_debt / total_equity if total_equity else 0.0
                elif metric == "Net Debt / Equity":
                    rec[y] = net_debt / total_equity if total_equity else 0.0
                elif metric == "Minority Interest / Equity":
                    rec[y] = minority / total_equity if total_equity else 0.0
                elif metric == "Liabilities / Equity":
                    rec[y] = total_liab / total_equity if total_equity else 0.0
            derived_rows.append(rec)

    return pd.concat([metric_df, pd.DataFrame(derived_rows)], ignore_index=True)


def generate_markdown_doc(pre_check_df: pd.DataFrame, rules: List[Dict]) -> str:
    balance_text = []
    for _, r in pre_check_df.iterrows():
        status = "平衡" if bool(r["Balanced"]) else "不平衡"
        balance_text.append(
            f"- {r['Year']}: 资产端={r['Assets_CurrentPlusNonCurrent']:.2f}，负债+权益端={r['LiabPlusEquity']:.2f}，差额={r['Diff']:.2f}（{status}）"
        )

    formula_lines = [f"- **{rule['standard_item']}** = {' + '.join(rule['source_items'])}" for rule in rules]
    mapping_lines = [
        f"- `{rule['statement_side']} / {rule['bucket']} / {rule['classification']}` → **{rule['standard_item']}**：{rule['formula_desc']}"
        for rule in rules
    ]

    return f"""# BS重构过程说明

## 1. 目标
将原始、杂乱的资产负债表CSV重构为可直接用于DCF / 三表联动 / SOTP建模的投行级标准资产负债表，并保留可追溯的mapping体系。

## 2. 数据清洗步骤
1. 读取原始CSV，并将第一列识别为“原始科目”。
2. 清理科目名称：去掉前导`*`、去掉多余空格。
3. 将所有年份列统一转为数值，无法识别的值按0处理。
4. 删除重复科目。
5. 删除子项/重复项，仅保留合计口径：
   - 应收票据及应收账款保留合计，删除其中：应收票据、应收账款
   - 其他应收款合计保留合计，删除其中：应收利息、其他应收款
   - 删除总现金
   - 固定资产合计保留合计，删除其中：固定资产、固定资产清理
   - 在建工程合计保留合计，删除其中：在建工程
   - 应付票据及应付账款保留合计，删除其中：应付票据、应付账款
   - 其他应付款合计保留合计，删除其中：应付利息、应付股利、其他应付款
   - 长期应付款合计保留合计，删除其中：长期应付款、专项应付款
6. 按预定义顺序输出预处理资产负债表。

## 3. 一致性校验逻辑
`流动资产合计 + 非流动资产合计 = 流动负债合计 + 非流动负债合计 + 所有者权益合计`

### 预处理后各期校验结果
{chr(10).join(balance_text)}

## 4. 科目分类逻辑（投行口径）
### Assets
- Current Assets
  - Cash & Short-term Financial Assets
  - Core Operating Current Assets
  - Non-operating Misc. Current Assets
- Non-current Assets
  - Long-term Core Operating Assets
  - Long-term Financial & Equity Investments
  - Risk & Amortizing Assets
  - Tax & Other Long-term Assets

### Liabilities
- Current Liabilities
  - Interest-bearing Short-term Debt
  - Operating Non-interest-bearing Current Liabilities
  - Non-operating Misc. Current Liabilities
- Non-current Liabilities
  - Long-term Interest-bearing Debt
  - Long-term Operating Non-interest-bearing Liabilities
  - Tax & Subsidy-related Non-cash Liabilities

### Equity
- Parent Contributed Capital
- Parent Retained Earnings
- Other Comprehensive Income (OCI)
- Minority Interest

## 5. Mapping规则说明
{chr(10).join(mapping_lines)}

## 6. 每个合并项的计算公式
{chr(10).join(formula_lines)}

## 7. 关键假设说明
1. 原始表中缺失的标准科目组成项按0处理。
2. 预处理阶段优先保留“合计”口径而非“其中”子项口径，以避免重复计算。
3. `减：库存股(元)`保留原始符号；若源数据本身为负值，则在合并时直接相加。
4. `其他应收款合计`默认归入核心经营性营运流动资产，后续可按公司具体业务进行再分类。
5. 本次新增Excel估值底稿聚焦于资产负债表可直接支撑的估值输入：现金、债务、经营营运资本、少数股东权益与资本结构比率。
6. 仅凭资产负债表无法独立完成完整DCF；完整DCF仍需接入利润表、现金流量表及经营假设。

## 8. 输出文件结构解释
输出目录：`./results/BS_rebuilt_output/`

1. `1_preprocess_bs.csv`：预处理后的资产负债表
2. `2_standardized_bs.csv`：英文标准科目输出，适合建模
3. `3_mapping_detail.csv`：原始科目到标准科目的映射关系
4. `4_analysis_bridge.csv`：原始数据 → 标准科目 的桥接过程
5. `5_valuation_ready_bs.xlsx`：可直接用于估值输入的Excel底稿
6. `BS重构过程说明.md`：本说明文档
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
    standardized_wide = build_standardized_bs_wide(standardized_df)

    try:
        writer = pd.ExcelWriter(output_path, engine="xlsxwriter")
        engine = "xlsxwriter"
    except Exception:
        writer = pd.ExcelWriter(output_path, engine="openpyxl")
        engine = "openpyxl"

    with writer:
        readme_df = pd.DataFrame(
            {
                "Sheet": ["Preprocess_BS", "Preprocess_Check", "Standardized_Long", "Standardized_Wide", "Valuation_Input", "Mapping_Detail", "Analysis_Bridge"],
                "Description": [
                    "预处理后的资产负债表",
                    "预处理平衡校验",
                    "标准化长表",
                    "标准化宽表（适合建模横向引用）",
                    "估值输入底稿：现金/债务/营运资本/资本结构指标",
                    "原始科目到标准科目映射",
                    "桥接拆解表",
                ],
            }
        )
        readme_df.to_excel(writer, sheet_name="README", index=False)
        preprocess_df.to_excel(writer, sheet_name="Preprocess_BS", index=False)
        pre_check_df.to_excel(writer, sheet_name="Preprocess_Check", index=False)
        standardized_df.to_excel(writer, sheet_name="Standardized_Long", index=False)
        standardized_wide.to_excel(writer, sheet_name="Standardized_Wide", index=False)
        valuation_df.to_excel(writer, sheet_name="Valuation_Input", index=False)
        mapping_detail_df.to_excel(writer, sheet_name="Mapping_Detail", index=False)
        bridge_df.to_excel(writer, sheet_name="Analysis_Bridge", index=False)

        if engine == "xlsxwriter":
            workbook = writer.book
            header_fmt = workbook.add_format({"bold": True, "bg_color": "#DCE6F1", "border": 1, "align": "center", "valign": "vcenter"})
            num_fmt = workbook.add_format({"num_format": "#,##0.00"})
            ratio_fmt = workbook.add_format({"num_format": "0.00%"})
            text_fmt = workbook.add_format({"text_wrap": True})
            title_fmt = workbook.add_format({"bold": True, "font_size": 14})

            sheet_cfg = {
                "README": [16, 42],
                "Preprocess_BS": [28] + [14] * max(1, preprocess_df.shape[1] - 1),
                "Preprocess_Check": [10, 20, 18, 14, 10],
                "Standardized_Long": [14, 20, 34, 10, 16],
                "Standardized_Wide": [14, 22, 36] + [16] * max(1, standardized_wide.shape[1] - 3),
                "Valuation_Input": [14, 34, 28] + [16] * max(1, valuation_df.shape[1] - 3),
                "Mapping_Detail": [28, 34, 38, 10, 12],
                "Analysis_Bridge": [10, 14, 20, 34, 36, 50, 16],
            }

            for sheet_name, widths in sheet_cfg.items():
                ws = writer.sheets[sheet_name]
                ws.freeze_panes(1, 1)
                for idx, width in enumerate(widths):
                    ws.set_column(idx, idx, width)
                if sheet_name == "README":
                    ws.set_row(0, 22)
                    ws.write("D1", "估值底稿使用提示", title_fmt)
                    ws.write("D2", "本工作簿重点解决资产负债表标准化、净债务、经营营运资本与资本结构输入。", text_fmt)
                    ws.write("D3", "若要做完整DCF，请再接入标准化利润表、现金流量表及经营预测假设。", text_fmt)

            for sheet_name, df_ in {
                "Preprocess_BS": preprocess_df,
                "Preprocess_Check": pre_check_df,
                "Standardized_Long": standardized_df,
                "Standardized_Wide": standardized_wide,
                "Valuation_Input": valuation_df,
                "Mapping_Detail": mapping_detail_df,
                "Analysis_Bridge": bridge_df,
            }.items():
                ws = writer.sheets[sheet_name]
                for col_num, value in enumerate(df_.columns.values):
                    ws.write(0, col_num, value, header_fmt)
                if sheet_name in {"Preprocess_BS", "Standardized_Long", "Standardized_Wide", "Valuation_Input", "Analysis_Bridge", "Preprocess_Check"}:
                    for col_idx in range(1, df_.shape[1]):
                        col_name = str(df_.columns[col_idx])
                        if "/" in col_name or "Ratio" in col_name:
                            continue
                    if sheet_name == "Valuation_Input":
                        for row_idx in range(1, valuation_df.shape[0] + 1):
                            section = valuation_df.iloc[row_idx - 1]["Section"]
                            for col_idx in range(3, valuation_df.shape[1]):
                                metric = valuation_df.iloc[row_idx - 1]["Metric"]
                                if " / " in metric:
                                    ws.write_number(row_idx, col_idx, valuation_df.iloc[row_idx - 1, col_idx], ratio_fmt)
                                else:
                                    ws.write_number(row_idx, col_idx, valuation_df.iloc[row_idx - 1, col_idx], num_fmt)
                    else:
                        for row_idx in range(1, df_.shape[0] + 1):
                            for col_idx in range(df_.shape[1]):
                                value = df_.iloc[row_idx - 1, col_idx]
                                if isinstance(value, (int, float)) and not pd.isna(value):
                                    ws.write_number(row_idx, col_idx, float(value), num_fmt if abs(float(value)) >= 1 else num_fmt)
                if sheet_name == "Valuation_Input":
                    ws.conditional_format(1, 0, valuation_df.shape[0], valuation_df.shape[1] - 1, {"type": "formula", "criteria": '=$A2="Derived"', "format": workbook.add_format({"bg_color": "#E2F0D9"})})
                    ws.conditional_format(1, 0, valuation_df.shape[0], valuation_df.shape[1] - 1, {"type": "formula", "criteria": '=$A2="Ratio"', "format": workbook.add_format({"bg_color": "#FFF2CC"})})

    return


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
    preprocess_df.to_csv(os.path.join(output_dir, "1_preprocess_bs.csv"), index=False, encoding="utf-8-sig")
    standardized_df.to_csv(os.path.join(output_dir, "2_standardized_bs.csv"), index=False, encoding="utf-8-sig")
    mapping_detail_df.to_csv(os.path.join(output_dir, "3_mapping_detail.csv"), index=False, encoding="utf-8-sig")
    bridge_df.to_csv(os.path.join(output_dir, "4_analysis_bridge.csv"), index=False, encoding="utf-8-sig")
    pre_check_df.to_csv(os.path.join(output_dir, "_preprocess_balance_check.csv"), index=False, encoding="utf-8-sig")
    excel_path = os.path.join(output_dir, "5_valuation_ready_bs.xlsx")
    export_excel_package(
        output_path=excel_path,
        preprocess_df=preprocess_df,
        pre_check_df=pre_check_df,
        standardized_df=standardized_df,
        mapping_detail_df=mapping_detail_df,
        bridge_df=bridge_df,
        valuation_df=valuation_df,
    )
    with open(os.path.join(output_dir, "BS重构过程说明.md"), "w", encoding="utf-8") as f:
        f.write(md_text)


def main(input_csv: str = "./results/csv/bs.csv", output_dir: str = OUTPUT_DIR) -> None:
    df, item_col, year_cols = load_bs_csv(input_csv)
    preprocess_df, pre_check_df = preprocess_bs(df, item_col, year_cols)
    rules = build_mapping_rules()
    mapping_detail_df = build_mapping_detail(preprocess_df, item_col, rules)
    standardized_df = build_standardized_bs(preprocess_df, item_col, year_cols, rules)
    bridge_df = build_analysis_bridge(preprocess_df, item_col, year_cols, rules)
    valuation_df = build_valuation_input_sheet(standardized_df)
    md_text = generate_markdown_doc(pre_check_df, rules)
    save_outputs(
        output_dir=output_dir,
        preprocess_df=preprocess_df,
        pre_check_df=pre_check_df,
        standardized_df=standardized_df,
        mapping_detail_df=mapping_detail_df,
        bridge_df=bridge_df,
        valuation_df=valuation_df,
        md_text=md_text,
    )
    print(f"完成。输出目录：{output_dir}")


if __name__ == "__main__":
    main()
