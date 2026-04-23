"""
Step 7/8 — 现金流量表标准化重构
================================
将同花顺原始现金流量表 CSV 清洗、排序，并按投行建模口径重新分类，
同时对关键勾稽项目进行六项一致性校验。

标准科目结构
------------
Operating CF
  Inflows:  Cash From Customers / Tax Refunds / Other Operating Cash In
  Outflows: Cash Paid to Suppliers / Cash Paid to Employees / Taxes Paid
  Net:      Operating Cash Flow

Investing CF
  Inflows:  Investment Recovery / Investment Income / Asset Disposal /
            Other Investing Cash In
  Outflows: Capex / Investment Cash Out
  Net:      Investing Cash Flow

Financing CF
  Inflows:  Equity Financing / Debt Financing / Other Financing Cash In
  Outflows: Debt Repayment / Dividend & Interest / Other Financing Cash Out
  Net:      Financing Cash Flow

Cash Reconciliation
  FX Impact / Net Change in Cash / Beginning Cash / Ending Cash

Indirect CFO Bridge（间接法附注）
  Depreciation / Amortization / Working Capital changes 等

六项一致性校验
--------------
1. 经营现金流：流入小计 - 流出小计 = 净额
2. 投资现金流：流入小计 - 流出小计 = 净额
3. 筹资现金流：流入小计 - 流出小计 = 净额
4. 现金净增加额：CFO + CFI + CFF + FX = 净增加额
5. 期末现金：期初 + 净增加额 = 期末
6. 间接法验证：直接法 CFO ≈ 间接法 CFO

估值输入指标（Valuation Input Sheet）
--------------------------------------
- Operating CF / Capex / Free Cash Flow Proxy / CFI / CFF
- Cash Conversion（CFO/净利润）/ Debt Service Cover / Cash Reinvestment Ratio

输入
----
- results/csv/cf.csv    同花顺原始现金流量表

输出（results/CF_rebuilt_output/）
------------------------------------
- 1_preprocess_cf.csv        去重、排序后的预处理表
- 2_standardized_cf.csv      标准化长表
- 3_mapping_detail.csv       原始科目 → 标准科目映射
- 4_analysis_bridge.csv      各标准科目的组成项拆解
- 5_valuation_ready_cf.xlsx  可直接用于估值的 Excel 底稿
- CF重构说明.md              本次重构的完整说明文档

运行方式
--------
    python rebuild_cash_flow.py
    # 或通过主管道：
    python run_pipeline.py
"""

import os
from typing import Dict, List, Tuple

import pandas as pd


OUTPUT_DIR = "./results/CF_rebuilt_output"


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


def load_cf_csv(input_path: str) -> Tuple[pd.DataFrame, str, List[str]]:
    df = pd.read_csv(input_path)
    first_col = df.columns[0]
    year_cols = [str(c) for c in df.columns[1:]]
    if not year_cols:
        raise ValueError("现金流量表 CSV 未识别到年份列。")
    df = df.rename(columns={first_col: "科目"})
    df["科目"] = df["科目"].apply(normalize_item_name)
    df = to_numeric_frame(df, year_cols)
    return df, "科目", year_cols


def preprocess_cf(df: pd.DataFrame, item_col: str, year_cols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    order = [
        "销售商品、提供劳务收到的现金(元)",
        "收到的税费与返还(元)",
        "收到其他与经营活动有关的现金(元)",
        "经营活动现金流入小计(元)",
        "购买商品、接受劳务支付的现金(元)",
        "支付给职工以及为职工支付的现金(元)",
        "支付的各项税费(元)",
        "支付其他与经营活动有关的现金(元)",
        "经营活动现金流出小计(元)",
        "经营活动产生的现金流量净额(元)",
        "收回投资收到的现金(元)",
        "取得投资收益收到的现金(元)",
        "处置固定资产、无形资产和其他长期资产收回的现金净额(元)",
        "处置子公司及其他营业单位收到的现金净额(元)",
        "收到其他与投资活动有关的现金(元)",
        "投资活动现金流入小计(元)",
        "购建固定资产、无形资产和其他长期资产支付的现金(元)",
        "投资支付的现金(元)",
        "取得子公司及其他营业单位支付的现金净额(元)",
        "支付其他与投资活动有关的现金(元)",
        "投资活动现金流出小计(元)",
        "投资活动产生的现金流量净额(元)",
        "吸收投资收到的现金(元)",
        "其中：子公司吸收少数股东投资收到的现金(元)",
        "取得借款收到的现金(元)",
        "收到其他与筹资活动有关的现金(元)",
        "筹资活动现金流入小计(元)",
        "偿还债务支付的现金(元)",
        "分配股利、利润或偿付利息支付的现金(元)",
        "其中：子公司支付给少数股东的股利、利润(元)",
        "支付其他与筹资活动有关的现金(元)",
        "筹资活动现金流出小计(元)",
        "筹资活动产生的现金流量净额(元)",
        "四、汇率变动对现金及现金等价物的影响(元)",
        "五、现金及现金等价物净增加额(元)",
        "加：期初现金及现金等价物余额(元)",
        "六、期末现金及现金等价物余额(元)",
        "净利润(元)",
        "加：资产减值准备(元)",
        "固定资产折旧、油气资产折耗、生产性生物资产折旧(元)",
        "无形资产摊销(元)",
        "长期待摊费用摊销(元)",
        "处置固定资产、无形资产和其他长期资产的损失(元)",
        "固定资产报废损失(元)",
        "公允价值变动损失(元)",
        "财务费用(元)",
        "投资损失(元)",
        "递延所得税资产减少(元)",
        "递延所得税负债增加(元)",
        "存货的减少(元)",
        "经营性应收项目的减少(元)",
        "经营性应付项目的增加(元)",
        "其他(元)",
        "间接法-经营活动产生的现金流量净额(元)",
        "现金的期末余额(元)",
        "减：现金的期初余额(元)",
        "间接法-现金及现金等价物净增加额(元)",
    ]
    order_map = {name: idx for idx, name in enumerate(order)}
    df = df.drop_duplicates(subset=[item_col], keep="first").copy()
    df["_sort"] = df[item_col].map(lambda x: order_map.get(x, 999))
    df = df.sort_values(["_sort", item_col]).drop(columns="_sort").reset_index(drop=True)

    checks = []
    for year in year_cols:
        cfo_in = float(safe_row_sum(df, item_col, year_cols, ["经营活动现金流入小计(元)"])[year])
        cfo_out = float(safe_row_sum(df, item_col, year_cols, ["经营活动现金流出小计(元)"])[year])
        cfo = float(safe_row_sum(df, item_col, year_cols, ["经营活动产生的现金流量净额(元)"])[year])
        cfi_in = float(safe_row_sum(df, item_col, year_cols, ["投资活动现金流入小计(元)"])[year])
        cfi_out = float(safe_row_sum(df, item_col, year_cols, ["投资活动现金流出小计(元)"])[year])
        cfi = float(safe_row_sum(df, item_col, year_cols, ["投资活动产生的现金流量净额(元)"])[year])
        cff_in = float(safe_row_sum(df, item_col, year_cols, ["筹资活动现金流入小计(元)"])[year])
        cff_out = float(safe_row_sum(df, item_col, year_cols, ["筹资活动现金流出小计(元)"])[year])
        cff = float(safe_row_sum(df, item_col, year_cols, ["筹资活动产生的现金流量净额(元)"])[year])
        fx = float(safe_row_sum(df, item_col, year_cols, ["四、汇率变动对现金及现金等价物的影响(元)"])[year])
        net_change = float(safe_row_sum(df, item_col, year_cols, ["五、现金及现金等价物净增加额(元)"])[year])
        begin_cash = float(safe_row_sum(df, item_col, year_cols, ["加：期初现金及现金等价物余额(元)"])[year])
        end_cash = float(safe_row_sum(df, item_col, year_cols, ["六、期末现金及现金等价物余额(元)"])[year])
        indirect_cfo = float(safe_row_sum(df, item_col, year_cols, ["间接法-经营活动产生的现金流量净额(元)"])[year])

        tolerance = 200.0
        checks.append(
            {
                "Year": year,
                "经营现金流校验差额": cfo_in - cfo_out - cfo,
                "投资现金流校验差额": cfi_in - cfi_out - cfi,
                "筹资现金流校验差额": cff_in - cff_out - cff,
                "现金净增加校验差额": cfo + cfi + cff + fx - net_change,
                "期末现金校验差额": begin_cash + net_change - end_cash,
                "间接法经营现金流差额": cfo - indirect_cfo,
                "All_Passed": max(
                    abs(cfo_in - cfo_out - cfo),
                    abs(cfi_in - cfi_out - cfi),
                    abs(cff_in - cff_out - cff),
                    abs(cfo + cfi + cff + fx - net_change),
                    abs(begin_cash + net_change - end_cash),
                    abs(cfo - indirect_cfo),
                ) <= tolerance,
            }
        )
    return df, pd.DataFrame(checks)


def build_mapping_rules() -> List[Dict]:
    return [
        {"standard_item": "Cash From Customers", "section": "Operating CF", "bucket": "Inflows", "source_items": ["销售商品、提供劳务收到的现金(元)"], "formula_desc": "销售商品、提供劳务收到的现金"},
        {"standard_item": "Tax Refunds", "section": "Operating CF", "bucket": "Inflows", "source_items": ["收到的税费与返还(元)"], "formula_desc": "收到的税费与返还"},
        {"standard_item": "Other Operating Cash In", "section": "Operating CF", "bucket": "Inflows", "source_items": ["收到其他与经营活动有关的现金(元)"], "formula_desc": "收到其他与经营活动有关的现金"},
        {"standard_item": "Cash Paid to Suppliers", "section": "Operating CF", "bucket": "Outflows", "source_items": ["购买商品、接受劳务支付的现金(元)"], "formula_desc": "购买商品、接受劳务支付的现金"},
        {"standard_item": "Cash Paid to Employees", "section": "Operating CF", "bucket": "Outflows", "source_items": ["支付给职工以及为职工支付的现金(元)"], "formula_desc": "支付给职工以及为职工支付的现金"},
        {"standard_item": "Taxes Paid", "section": "Operating CF", "bucket": "Outflows", "source_items": ["支付的各项税费(元)"], "formula_desc": "支付的各项税费"},
        {"standard_item": "Other Operating Cash Out", "section": "Operating CF", "bucket": "Outflows", "source_items": ["支付其他与经营活动有关的现金(元)"], "formula_desc": "支付其他与经营活动有关的现金"},
        {"standard_item": "Operating Cash Flow", "section": "Operating CF", "bucket": "Net", "source_items": ["经营活动产生的现金流量净额(元)"], "formula_desc": "经营活动产生的现金流量净额"},
        {"standard_item": "Investment Recovery Cash In", "section": "Investing CF", "bucket": "Inflows", "source_items": ["收回投资收到的现金(元)"], "formula_desc": "收回投资收到的现金"},
        {"standard_item": "Investment Income Cash In", "section": "Investing CF", "bucket": "Inflows", "source_items": ["取得投资收益收到的现金(元)"], "formula_desc": "取得投资收益收到的现金"},
        {"standard_item": "Asset Disposal Cash In", "section": "Investing CF", "bucket": "Inflows", "source_items": ["处置固定资产、无形资产和其他长期资产收回的现金净额(元)", "处置子公司及其他营业单位收到的现金净额(元)"], "formula_desc": "处置长期资产/子公司回款"},
        {"standard_item": "Other Investing Cash In", "section": "Investing CF", "bucket": "Inflows", "source_items": ["收到其他与投资活动有关的现金(元)"], "formula_desc": "收到其他与投资活动有关的现金"},
        {"standard_item": "Capex", "section": "Investing CF", "bucket": "Outflows", "source_items": ["购建固定资产、无形资产和其他长期资产支付的现金(元)"], "formula_desc": "购建固定资产、无形资产和其他长期资产支付的现金"},
        {"standard_item": "Investment Cash Out", "section": "Investing CF", "bucket": "Outflows", "source_items": ["投资支付的现金(元)", "取得子公司及其他营业单位支付的现金净额(元)", "支付其他与投资活动有关的现金(元)"], "formula_desc": "投资支付现金及其他投资流出"},
        {"standard_item": "Investing Cash Flow", "section": "Investing CF", "bucket": "Net", "source_items": ["投资活动产生的现金流量净额(元)"], "formula_desc": "投资活动产生的现金流量净额"},
        {"standard_item": "Equity Financing Cash In", "section": "Financing CF", "bucket": "Inflows", "source_items": ["吸收投资收到的现金(元)", "其中：子公司吸收少数股东投资收到的现金(元)"], "formula_desc": "吸收投资收到的现金"},
        {"standard_item": "Debt Financing Cash In", "section": "Financing CF", "bucket": "Inflows", "source_items": ["取得借款收到的现金(元)"], "formula_desc": "取得借款收到的现金"},
        {"standard_item": "Other Financing Cash In", "section": "Financing CF", "bucket": "Inflows", "source_items": ["收到其他与筹资活动有关的现金(元)"], "formula_desc": "收到其他与筹资活动有关的现金"},
        {"standard_item": "Debt Repayment Cash Out", "section": "Financing CF", "bucket": "Outflows", "source_items": ["偿还债务支付的现金(元)"], "formula_desc": "偿还债务支付的现金"},
        {"standard_item": "Dividend & Interest Cash Out", "section": "Financing CF", "bucket": "Outflows", "source_items": ["分配股利、利润或偿付利息支付的现金(元)", "其中：子公司支付给少数股东的股利、利润(元)"], "formula_desc": "分配股利、利润或偿付利息支付的现金"},
        {"standard_item": "Other Financing Cash Out", "section": "Financing CF", "bucket": "Outflows", "source_items": ["支付其他与筹资活动有关的现金(元)"], "formula_desc": "支付其他与筹资活动有关的现金"},
        {"standard_item": "Financing Cash Flow", "section": "Financing CF", "bucket": "Net", "source_items": ["筹资活动产生的现金流量净额(元)"], "formula_desc": "筹资活动产生的现金流量净额"},
        {"standard_item": "FX Impact", "section": "Cash Reconciliation", "bucket": "Reconciliation", "source_items": ["四、汇率变动对现金及现金等价物的影响(元)"], "formula_desc": "汇率变动影响"},
        {"standard_item": "Net Change in Cash", "section": "Cash Reconciliation", "bucket": "Reconciliation", "source_items": ["五、现金及现金等价物净增加额(元)"], "formula_desc": "现金及现金等价物净增加额"},
        {"standard_item": "Beginning Cash", "section": "Cash Reconciliation", "bucket": "Balance", "source_items": ["加：期初现金及现金等价物余额(元)"], "formula_desc": "期初现金及现金等价物"},
        {"standard_item": "Ending Cash", "section": "Cash Reconciliation", "bucket": "Balance", "source_items": ["六、期末现金及现金等价物余额(元)"], "formula_desc": "期末现金及现金等价物"},
        {"standard_item": "Net Profit", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["净利润(元)"], "formula_desc": "净利润"},
        {"standard_item": "Impairment Add-back", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["加：资产减值准备(元)"], "formula_desc": "资产减值准备"},
        {"standard_item": "Depreciation", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["固定资产折旧、油气资产折耗、生产性生物资产折旧(元)"], "formula_desc": "折旧"},
        {"standard_item": "Amortization", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["无形资产摊销(元)", "长期待摊费用摊销(元)"], "formula_desc": "无形资产及长期待摊摊销"},
        {"standard_item": "Asset Disposal Loss", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["处置固定资产、无形资产和其他长期资产的损失(元)", "固定资产报废损失(元)"], "formula_desc": "资产处置/报废损失"},
        {"standard_item": "Fair Value Loss", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["公允价值变动损失(元)"], "formula_desc": "公允价值变动损失"},
        {"standard_item": "Financial Expense Bridge", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["财务费用(元)"], "formula_desc": "财务费用"},
        {"standard_item": "Investment Loss", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["投资损失(元)"], "formula_desc": "投资损失"},
        {"standard_item": "Deferred Tax Impact", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["递延所得税资产减少(元)", "递延所得税负债增加(元)"], "formula_desc": "递延所得税影响"},
        {"standard_item": "Inventory Change", "section": "Indirect CFO Bridge", "bucket": "Working Capital", "source_items": ["存货的减少(元)"], "formula_desc": "存货变动"},
        {"standard_item": "Receivables Change", "section": "Indirect CFO Bridge", "bucket": "Working Capital", "source_items": ["经营性应收项目的减少(元)"], "formula_desc": "经营性应收项目变动"},
        {"standard_item": "Payables Change", "section": "Indirect CFO Bridge", "bucket": "Working Capital", "source_items": ["经营性应付项目的增加(元)"], "formula_desc": "经营性应付项目变动"},
        {"standard_item": "Other CFO Bridge", "section": "Indirect CFO Bridge", "bucket": "Bridge", "source_items": ["其他(元)"], "formula_desc": "其他"},
        {"standard_item": "Indirect Operating Cash Flow", "section": "Indirect CFO Bridge", "bucket": "Result", "source_items": ["间接法-经营活动产生的现金流量净额(元)"], "formula_desc": "间接法经营现金流"},
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
                    "Section": rule["section"],
                    "Bucket": rule["bucket"],
                    "Exists in Source": source in available,
                }
            )
    return pd.DataFrame(rows)


def build_standardized_cf(df: pd.DataFrame, item_col: str, year_cols: List[str], rules: List[Dict]) -> pd.DataFrame:
    rows = []
    for rule in rules:
        values = safe_row_sum(df, item_col, year_cols, rule["source_items"])
        for year in year_cols:
            rows.append(
                {
                    "Year": year,
                    "Section": rule["section"],
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
                "Section": rule["section"],
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

    cfo = get_metric("Operating Cash Flow")
    capex = get_metric("Capex")
    cfi = get_metric("Investing Cash Flow")
    cff = get_metric("Financing Cash Flow")
    end_cash = get_metric("Ending Cash")
    begin_cash = get_metric("Beginning Cash")
    net_profit = get_metric("Net Profit")
    depreciation = get_metric("Depreciation")
    amortization = get_metric("Amortization")
    impairment = get_metric("Impairment Add-back")
    inventory_change = get_metric("Inventory Change")
    receivables_change = get_metric("Receivables Change")
    payables_change = get_metric("Payables Change")
    dividend_interest = get_metric("Dividend & Interest Cash Out")
    debt_in = get_metric("Debt Financing Cash In")
    debt_out = get_metric("Debt Repayment Cash Out")

    maintenance_cf_proxy = cfo - capex
    cash_conversion = cfo.divide(net_profit.replace(0, pd.NA)).fillna(0.0)
    debt_service_cover = cfo.divide(dividend_interest.replace(0, pd.NA)).fillna(0.0)
    net_borrowing = debt_in - debt_out
    da_total = depreciation + amortization + impairment
    working_capital_delta = inventory_change + receivables_change + payables_change
    cash_reinvestment_ratio = capex.abs().divide(cfo.replace(0, pd.NA).abs()).fillna(0.0)

    rows = [
        ("Reported", "Operating Cash Flow", "经营活动现金流量净额", cfo),
        ("Reported", "Capex", "资本开支", capex),
        ("Reported", "Free Cash Flow Proxy", "经营现金流-资本开支", maintenance_cf_proxy),
        ("Reported", "Investing Cash Flow", "投资活动现金流量净额", cfi),
        ("Reported", "Financing Cash Flow", "筹资活动现金流量净额", cff),
        ("Reported", "Beginning Cash", "期初现金及现金等价物", begin_cash),
        ("Reported", "Ending Cash", "期末现金及现金等价物", end_cash),
        ("Reported", "Net Profit", "净利润", net_profit),
        ("Reported", "D&A + Impairment", "折旧摊销及减值加回", da_total),
        ("Reported", "Working Capital Delta", "存货+应收+应付变动", working_capital_delta),
        ("Reported", "Net Borrowing", "借款流入-还债流出", net_borrowing),
        ("Ratio", "Cash Conversion", "经营现金流/净利润", cash_conversion),
        ("Ratio", "Debt Service Cover", "经营现金流/分红付息现金流出", debt_service_cover),
        ("Ratio", "Cash Reinvestment Ratio", "资本开支/经营现金流", cash_reinvestment_ratio),
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
            f"- {row['Year']}: 经营={row['经营现金流校验差额']:.2f}，投资={row['投资现金流校验差额']:.2f}，筹资={row['筹资现金流校验差额']:.2f}，净增加={row['现金净增加校验差额']:.2f}，期末现金={row['期末现金校验差额']:.2f}，间接法={row['间接法经营现金流差额']:.2f}，状态={status}"
        )

    mapping_lines = [
        f"- `{rule['section']} / {rule['bucket']}` -> **{rule['standard_item']}** = {rule['formula_desc']}"
        for rule in rules
    ]

    return f"""# 现金流量表重构说明

## 1. 目标
将原始 `cf.csv` 重构为适合估值分析与现金流建模的标准口径，输出标准化科目、映射明细、分析 bridge 和 Excel 工作簿。

## 2. 校验结果
{chr(10).join(check_lines)}

## 3. 标准化映射
{chr(10).join(mapping_lines)}

## 4. 输出文件
- `1_preprocess_cf.csv`：预处理后的现金流量表
- `_preprocess_check.csv`：关键勾稽校验
- `2_standardized_cf.csv`：标准化长表
- `3_mapping_detail.csv`：原始科目到标准科目的映射
- `4_analysis_bridge.csv`：估值分析 bridge
- `5_valuation_ready_cf.xlsx`：Excel 打包结果
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
                "Sheet": ["Preprocess_CF", "Preprocess_Check", "Standardized_Long", "Standardized_Wide", "Valuation_Input", "Mapping_Detail", "Analysis_Bridge"],
                "Description": [
                    "原始现金流量表清洗后的结果",
                    "现金流量表关键勾稽校验",
                    "标准化现金流量表长表",
                    "标准化现金流量表宽表",
                    "可直接用于估值建模的现金流指标",
                    "原始科目与标准科目的映射关系",
                    "估值分析 bridge",
                ],
            }
        )
        readme_df.to_excel(writer, sheet_name="README", index=False)
        preprocess_df.to_excel(writer, sheet_name="Preprocess_CF", index=False)
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
                "Preprocess_CF": [40] + [14] * (preprocess_df.shape[1] - 1),
                "Preprocess_Check": [10, 18, 18, 18, 18, 18, 18, 12],
                "Standardized_Long": [10, 22, 28, 18, 16],
                "Standardized_Wide": [20, 18, 30] + [14] * (standardized_wide.shape[1] - 3),
                "Valuation_Input": [12, 24, 28] + [14] * (valuation_df.shape[1] - 3),
                "Mapping_Detail": [40, 28, 20, 18, 12],
                "Analysis_Bridge": [20, 18, 30, 50, 38] + [14] * max(1, bridge_df.shape[1] - 5),
            }
            for sheet_name, col_widths in widths.items():
                ws = writer.sheets[sheet_name]
                ws.freeze_panes(1, 1)
                for idx, width in enumerate(col_widths):
                    ws.set_column(idx, idx, width)
                df_map = {
                    "README": readme_df,
                    "Preprocess_CF": preprocess_df,
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
    preprocess_df.to_csv(os.path.join(output_dir, "1_preprocess_cf.csv"), index=False, encoding="utf-8-sig")
    pre_check_df.to_csv(os.path.join(output_dir, "_preprocess_check.csv"), index=False, encoding="utf-8-sig")
    standardized_df.to_csv(os.path.join(output_dir, "2_standardized_cf.csv"), index=False, encoding="utf-8-sig")
    mapping_detail_df.to_csv(os.path.join(output_dir, "3_mapping_detail.csv"), index=False, encoding="utf-8-sig")
    bridge_df.to_csv(os.path.join(output_dir, "4_analysis_bridge.csv"), index=False, encoding="utf-8-sig")
    export_excel_package(
        output_path=os.path.join(output_dir, "5_valuation_ready_cf.xlsx"),
        preprocess_df=preprocess_df,
        pre_check_df=pre_check_df,
        standardized_df=standardized_df,
        mapping_detail_df=mapping_detail_df,
        bridge_df=bridge_df,
        valuation_df=valuation_df,
    )
    with open(os.path.join(output_dir, "CF重构说明.md"), "w", encoding="utf-8") as f:
        f.write(md_text)


def main(input_csv: str = "./results/csv/cf.csv", output_dir: str = OUTPUT_DIR) -> None:
    df, item_col, year_cols = load_cf_csv(input_csv)
    preprocess_df, pre_check_df = preprocess_cf(df, item_col, year_cols)
    rules = build_mapping_rules()
    mapping_detail_df = build_mapping_detail(preprocess_df, item_col, rules)
    standardized_df = build_standardized_cf(preprocess_df, item_col, year_cols, rules)
    bridge_df = build_analysis_bridge(preprocess_df, item_col, year_cols, rules)
    valuation_df = build_valuation_input_sheet(standardized_df)
    md_text = generate_markdown_doc(pre_check_df, rules)
    save_outputs(output_dir, preprocess_df, pre_check_df, standardized_df, mapping_detail_df, bridge_df, valuation_df, md_text)
    print(f"现金流量表重构完成，输出目录：{output_dir}")


if __name__ == "__main__":
    main()
