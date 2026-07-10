"""
Step 10/11 — DCF 估值模型生成
============================
读取三表标准化输出和公司基础信息，自动计算历史财务驱动因子，
生成包含多个联动 Sheet 的 Excel DCF 估值工作簿。

工作簿结构
------------------------------
Summary          — 估值总览：DCF vs 相对估值、加权目标价、上行/下行空间
DCF_Input_Map    — 从三张标准化宽表提取的 DCF 建模输入底稿
DCF_Readiness    — 历史现金流、营运资本、CapEx 等 DCF 适用性判断
WACC_Build       — 无风险利率、Beta、ERP、债务成本与三情景 WACC 推导
Assumptions      — 可编辑假设区（WACC、永续增长率、5年逐年收入增速等）
Forecast         — 5 年经营预测与 FCFF 计算（公式联动 Assumptions）
DCF              — DCF 估值主表（EV → 股东权益价值 → 每股内在价值）
Model_Checks     — 终值、永续增长率、WACC、利润率、CapEx、营运资本检查点
Scenario_DCF     — 悲观 / 中性 / 乐观三套完整 DCF 情景
Sensitivity      — WACC / 永续增长率 敏感性矩阵（含条件格式）
Comparable       — 相对估值（PE / PB / EV/EBIT / EV/EBITDA）

自动计算的驱动因子
------------------
- 收入增速 Seed：基于最近 3 年 CAGR，夹在 [3%, 20%]
- EBIT Margin / Tax Rate / D&A% / CapEx% / NWC%：3 年均值，夹在合理区间
- 5 年预测增速：从 Seed 开始逐年平缓下台阶

输入
----
- results/BS_rebuilt_output/2_standardized_bs.csv
- results/PL_rebuilt_output/2_standardized_pl.csv
- results/CF_rebuilt_output/2_standardized_cf.csv
- results/04_rebuilt_statements/*/2_standardized_*_wide.csv
- Info.csv     （需含：总股本、当前股价、公司简称）

输出
----
- results/valuation_output/DCF_valuation_model.xlsx

运行方式
--------
    python generate_dcf_valuation.py
    # 或通过主管道：
    python run_pipeline.py
"""

import os
import argparse
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, cast

import pandas as pd
from openpyxl import Workbook

from afda.pipeline_utils import (
    BS_REBUILT_DIR,
    CF_REBUILT_DIR,
    PL_REBUILT_DIR,
    VALUATION_DIR,
    detect_ticker,
    find_info_file,
    company_display_name,
    read_info_items,
    prompt_data_dir_with_dialog,
    resolve_data_dir,
)
from afda.excel_utils import apply_bilingual_fonts
from afda.dcf_core import (
    DCF_INPUT_ITEMS,
    avg,
    build_assumption_audit_rows,
    cagr,
    clamp,
    coefficient_of_variation,
    count_positive,
    dcf_item_usage,
    get_series,
    load_item_series,
    load_wide_items,
    safe_div,
    trend_slope,
    build_valuation_risk_warnings,
)
from afda.valuation_config import get_multiple, load_valuation_config, valuation_config_source_map
from afda.logging_config import get_logger
from afda.dcf_types import DCFData
from afda.dcf_excel_sheets import (
    create_summary_sheet,
    create_dcf_input_map_sheet,
    create_readiness_sheet,
    create_wacc_build_sheet,
    create_assumptions_sheet,
    create_assumption_audit_sheet,
    create_forecast_sheet,
    create_dcf_sheet,
    create_model_checks_sheet,
    create_scenario_dcf_sheet,
    create_sensitivity_sheet,
    create_comparable_sheet,
    finalize_workbook,
)

logger = get_logger(__name__)


BASE_DIR = Path(".")
BS_PATH = BS_REBUILT_DIR / "2_standardized_bs.csv"
PL_PATH = PL_REBUILT_DIR / "2_standardized_pl.csv"
CF_PATH = CF_REBUILT_DIR / "2_standardized_cf.csv"
BS_WIDE_PATH = BS_REBUILT_DIR / "2_standardized_bs_wide.csv"
PL_WIDE_PATH = PL_REBUILT_DIR / "2_standardized_pl_wide.csv"
CF_WIDE_PATH = CF_REBUILT_DIR / "2_standardized_cf_wide.csv"
INFO_PATH = BASE_DIR / "demo" / "rawdata" / "Info.csv"
OUTPUT_DIR = VALUATION_DIR
OUTPUT_PATH = OUTPUT_DIR / "DCF_valuation_model.xlsx"


def configure_results_paths(results_dir: Path) -> None:
    global BS_PATH, PL_PATH, CF_PATH
    global BS_WIDE_PATH, PL_WIDE_PATH, CF_WIDE_PATH
    global OUTPUT_DIR, OUTPUT_PATH

    rebuilt_dir = results_dir / "04_rebuilt_statements"
    BS_PATH = rebuilt_dir / "balance_sheet" / "2_standardized_bs.csv"
    PL_PATH = rebuilt_dir / "income_statement" / "2_standardized_pl.csv"
    CF_PATH = rebuilt_dir / "cash_flow" / "2_standardized_cf.csv"
    BS_WIDE_PATH = rebuilt_dir / "balance_sheet" / "2_standardized_bs_wide.csv"
    PL_WIDE_PATH = rebuilt_dir / "income_statement" / "2_standardized_pl_wide.csv"
    CF_WIDE_PATH = rebuilt_dir / "cash_flow" / "2_standardized_cf_wide.csv"
    OUTPUT_DIR = results_dir / "05_valuation"
    OUTPUT_PATH = OUTPUT_DIR / "DCF_valuation_model.xlsx"


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def detect_company_name(ticker: str, info_path: Optional[Path] = None) -> str:
    info_path = info_path or INFO_PATH
    if info_path.exists():
        try:
            info = read_info_items(info_path)
            for key in ["公司名称", "公司简称"]:
                value = info.get(key)
                if value:
                    return value
        except Exception:
            pass
    return ticker


def build_dcf_input_rows(years: List[int]) -> List[Dict[str, object]]:
    wide_maps = {
        "PL": load_wide_items(PL_WIDE_PATH, ["Standard Item"]),
        "BS": load_wide_items(BS_WIDE_PATH, ["StandardLineItem", "Standard Item"]),
        "CF": load_wide_items(CF_WIDE_PATH, ["Standard Item"]),
    }
    rows: List[Dict[str, object]] = []
    for statement, items in DCF_INPUT_ITEMS.items():
        for item, cn_name in items:
            year_values = wide_maps.get(statement, {}).get(item, {})
            row = {
                "Statement": statement,
                "Standard Item": item,
                "中文说明": cn_name,
                "DCF用途": dcf_item_usage(item),
            }
            for year in years:
                row[year] = float(year_values.get(year, 0.0))
            rows.append(row)
    return rows


def build_readiness_checks(data: DCFData) -> List[Dict[str, object]]:
    years = data["years"]
    cfo = data["cfo"]
    fcff = data["fcff_proxy"]
    net_profit = data["parent_net_profit"]
    revenue = data["revenue"]
    nwc = data["nwc"]
    capex = data["capex"]

    cfo_positive_ratio = safe_div(count_positive(cfo), len(cfo))
    fcff_slope = trend_slope(fcff)
    cfo_np_ratios = [safe_div(c, p, 0.0) for c, p in zip(cfo, net_profit) if abs(p) > 1e-9]
    cfo_np_match = avg([min(max(ratio, 0.0), 2.0) for ratio in cfo_np_ratios], 0.0)
    nwc_revenue = [safe_div(n, r, 0.0) for n, r in zip(nwc, revenue)]
    nwc_slope = trend_slope(nwc_revenue)
    capex_revenue = [safe_div(x, r, 0.0) for x, r in zip(capex, revenue)]
    capex_cv = coefficient_of_variation(capex_revenue)

    def status(condition_good: bool, condition_watch: bool) -> str:
        if condition_good:
            return "通过"
        if condition_watch:
            return "关注"
        return "不通过"

    return [
        {
            "检查项": "多数年份 CFO 为正",
            "指标": "CFO为正年份占比",
            "结果": cfo_positive_ratio,
            "判断": status(cfo_positive_ratio >= 0.70, cfo_positive_ratio >= 0.50),
            "推断过程": f"{count_positive(cfo)}/{len(cfo)} 个历史年份经营现金流为正。",
            "模型含义": "DCF 更依赖可持续现金流，CFO长期为负会削弱 FCFF 外推可信度。",
        },
        {
            "检查项": "自由现金流趋势",
            "指标": "FCFF Proxy线性斜率",
            "结果": fcff_slope,
            "判断": status(fcff_slope > 0 and fcff[-1] > 0, fcff_slope > 0 or fcff[-1] > 0),
            "推断过程": f"FCFF Proxy = CFO - CapEx；{years[0]}A至{years[-1]}A斜率为 {fcff_slope:,.2f}，末年为 {fcff[-1]:,.2f}。",
            "模型含义": "正向或改善的 FCF 更适合以稳定增长模型估值。",
        },
        {
            "检查项": "CFO 与净利润匹配度",
            "指标": "平均 CFO / 归母净利润",
            "结果": cfo_np_match,
            "判断": status(0.70 <= cfo_np_match <= 1.50, 0.40 <= cfo_np_match <= 2.00),
            "推断过程": "逐年计算 CFO / 归母净利润，剔除净利润接近 0 的年份后取均值并限制极端值影响。",
            "模型含义": "匹配度较高通常意味着盈利质量对现金流预测更友好。",
        },
        {
            "检查项": "营运资本是否持续恶化",
            "指标": "NWC / Revenue趋势斜率",
            "结果": nwc_slope,
            "判断": status(nwc_slope <= 0.005, nwc_slope <= 0.020),
            "推断过程": "以核心经营流动资产减经营性无息流动负债，再除以收入，观察比例趋势。",
            "模型含义": "NWC占收入持续上升会吞噬 FCFF，预测期需要提高营运资本占用假设。",
        },
        {
            "检查项": "资本开支规律性",
            "指标": "CapEx / Revenue变异系数",
            "结果": capex_cv,
            "判断": status(capex_cv <= 0.50, capex_cv <= 1.00),
            "推断过程": "逐年计算 CapEx / Revenue，用变异系数衡量波动性。",
            "模型含义": "CapEx波动越高，单一均值外推越需要人工复核。",
        },
    ]


def build_historical_dataset(data_dir: Optional[Path] = None, info_path: Optional[Path] = None) -> DCFData:
    data_dir = resolve_data_dir(data_dir)
    info_path = info_path or find_info_file(data_dir)
    if info_path is None:
        raise FileNotFoundError(f"Info.csv not found in {data_dir}.")

    bs = pd.read_csv(BS_PATH)
    pl = pd.read_csv(PL_PATH)
    cf = pd.read_csv(CF_PATH)
    info_items = read_info_items(info_path)

    bs_map = load_item_series(bs, "StandardLineItem", "Year", "Value")
    pl_map = load_item_series(pl, "Standard Item", "Year", "Value")
    cf_map = load_item_series(cf, "Standard Item", "Year", "Value")

    years = sorted(set(bs["Year"]).intersection(pl["Year"]).intersection(cf["Year"]))
    years = [int(y) for y in years]
    ticker = detect_ticker(data_dir)
    company_name = detect_company_name(ticker, info_path)
    company_code = info_items.get("公司代码", ticker)
    company_label = company_display_name(data_dir, ticker=ticker)
    valuation_date = date.today().isoformat()
    valuation_config = load_valuation_config(data_dir)
    valuation_config_sources = valuation_config_source_map(data_dir)

    revenue = get_series(pl_map, "Revenue", years)
    operating_profit = get_series(pl_map, "Operating Profit", years)
    financial_expense = get_series(pl_map, "Financial Expense", years)
    ebit = [op + fin for op, fin in zip(operating_profit, financial_expense)]
    profit_before_tax = get_series(pl_map, "Profit Before Tax", years)
    income_tax = get_series(pl_map, "Income Tax", years)
    parent_net_profit = get_series(pl_map, "Parent Net Profit", years)

    depreciation = get_series(cf_map, "Depreciation", years)
    amortization = get_series(cf_map, "Amortization", years)
    capex = [abs(v) for v in get_series(cf_map, "Capex", years)]
    cfo = get_series(cf_map, "Operating Cash Flow", years)
    fcff_proxy = [c - x for c, x in zip(cfo, capex)]

    cash = get_series(bs_map, "Cash & Short-term Financial Assets", years)
    short_debt = get_series(bs_map, "Interest-bearing Short-term Debt", years)
    long_debt = get_series(bs_map, "Long-term Interest-bearing Debt", years)
    minority_interest = get_series(bs_map, "Minority Interest", years)
    total_equity = get_series(bs_map, "Total Equity", years)
    non_op_current_assets = get_series(bs_map, "Non-operating Misc. Current Assets", years)
    long_term_investments = get_series(bs_map, "Long-term Financial & Equity Investments", years)
    core_operating_current_assets = get_series(bs_map, "Core Operating Current Assets", years)
    operating_current_liab = get_series(bs_map, "Operating Non-interest-bearing Current Liabilities", years)

    nwc = [ca - cl for ca, cl in zip(core_operating_current_assets, operating_current_liab)]
    tax_rate = []
    ebit_margin = []
    da_ratio = []
    capex_ratio = []
    nwc_ratio = []
    for idx, year in enumerate(years):
        rev = revenue[idx]
        pbt = profit_before_tax[idx]
        tax_rate.append(income_tax[idx] / pbt if abs(pbt) > 1e-9 else 0.20)
        ebit_margin.append(ebit[idx] / rev if abs(rev) > 1e-9 else 0.10)
        da_ratio.append((depreciation[idx] + amortization[idx]) / rev if abs(rev) > 1e-9 else 0.03)
        capex_ratio.append(capex[idx] / rev if abs(rev) > 1e-9 else 0.03)
        nwc_ratio.append(nwc[idx] / rev if abs(rev) > 1e-9 else 0.05)

    shares_outstanding = float(info_items["总股本"].replace(",", ""))
    current_price = float(info_items["当前股价"].replace(",", ""))

    base_year = max(years)
    growth_seed = cagr(revenue[-4], revenue[-1], 3) if len(revenue) >= 4 else 0.10
    growth_seed = clamp(growth_seed, 0.03, 0.20)
    base_ebit_margin = clamp(avg(ebit_margin[-3:], ebit_margin[-1]), 0.05, 0.35)
    base_tax_rate = clamp(avg(tax_rate[-3:], tax_rate[-1]), 0.10, 0.30)
    base_da_ratio = clamp(avg(da_ratio[-3:], da_ratio[-1]), 0.01, 0.08)
    base_capex_ratio = clamp(avg(capex_ratio[-3:], capex_ratio[-1]), 0.01, 0.12)
    base_nwc_ratio = clamp(avg(nwc_ratio[-3:], nwc_ratio[-1]), 0.00, 0.25)

    forecast_years = [base_year + i for i in range(1, 6)]
    default_growths = [
        round(growth_seed, 4),
        round(max(growth_seed - 0.015, 0.04), 4),
        round(max(growth_seed - 0.030, 0.035), 4),
        round(max(growth_seed - 0.045, 0.030), 4),
        round(max(growth_seed - 0.055, 0.025), 4),
    ]

    data = {
        "years": years,
        "forecast_years": forecast_years,
        "base_year": base_year,
        "revenue": revenue,
        "operating_profit": operating_profit,
        "financial_expense": financial_expense,
        "ebit": ebit,
        "profit_before_tax": profit_before_tax,
        "income_tax": income_tax,
        "parent_net_profit": parent_net_profit,
        "depreciation": depreciation,
        "amortization": amortization,
        "capex": capex,
        "cfo": cfo,
        "fcff_proxy": fcff_proxy,
        "cash": cash,
        "short_debt": short_debt,
        "long_debt": long_debt,
        "minority_interest": minority_interest,
        "total_equity": total_equity,
        "non_op_current_assets": non_op_current_assets,
        "long_term_investments": long_term_investments,
        "nwc": nwc,
        "tax_rate": tax_rate,
        "ebit_margin": ebit_margin,
        "da_ratio": da_ratio,
        "capex_ratio": capex_ratio,
        "nwc_ratio": nwc_ratio,
        "shares_outstanding": shares_outstanding,
        "current_price": current_price,
        "default_growths": default_growths,
        "ticker": ticker,
        "company_code": company_code,
        "company_name": company_name,
        "company_label": company_label,
        "valuation_date": valuation_date,
        "base_ebit_margin": round(base_ebit_margin, 4),
        "base_tax_rate": round(base_tax_rate, 4),
        "base_da_ratio": round(base_da_ratio, 4),
        "base_capex_ratio": round(base_capex_ratio, 4),
        "base_nwc_ratio": round(base_nwc_ratio, 4),
        "valuation_config": valuation_config,
        "valuation_config_sources": valuation_config_sources,
    }
    data["dcf_input_rows"] = build_dcf_input_rows(years)
    data["readiness_checks"] = build_readiness_checks(data)
    data["assumption_audit_rows"] = build_assumption_audit_rows(data)
    data["valuation_risk_warnings"] = build_valuation_risk_warnings(data)
    return cast(DCFData, data)


def build_workbook(data: DCFData) -> Workbook:
    wb = Workbook()
    create_summary_sheet(wb, data)
    create_dcf_input_map_sheet(wb, data)
    create_readiness_sheet(wb, data)
    create_wacc_build_sheet(wb, data)
    create_assumptions_sheet(wb, data)
    create_assumption_audit_sheet(wb, data)
    create_forecast_sheet(wb, data)
    create_dcf_sheet(wb, data)
    create_model_checks_sheet(wb, data)
    create_scenario_dcf_sheet(wb, data)
    create_sensitivity_sheet(wb, data)
    create_comparable_sheet(wb, data)
    finalize_workbook(wb)
    return wb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the DCF valuation workbook.")
    parser.add_argument(
        "data_dir",
        nargs="?",
        default=None,
        help="Directory containing Info.csv and the original statement files. If omitted, a folder picker will open.",
    )
    parser.add_argument(
        "--data-dir",
        dest="data_dir_flag",
        default=None,
        help="Same as positional data_dir.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir_value = args.data_dir_flag or args.data_dir
    data_dir = resolve_data_dir(data_dir_value) if data_dir_value else prompt_data_dir_with_dialog()
    configure_results_paths(data_dir / "results")
    info_path = find_info_file(data_dir)
    if info_path is None:
        logger.info("Info.csv not found in %s. Skipping DCF valuation.", data_dir)
        return

    ensure_output_dir()
    data = build_historical_dataset(data_dir=data_dir, info_path=info_path)
    wb = build_workbook(data)
    apply_bilingual_fonts(wb)
    wb.save(OUTPUT_PATH)
    logger.info("DCF估值模型已生成：%s", OUTPUT_PATH)


if __name__ == "__main__":
    main()
