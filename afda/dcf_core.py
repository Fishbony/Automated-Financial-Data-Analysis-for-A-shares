"""Reusable DCF data and math helpers."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


DCF_INPUT_ITEMS = {
    "PL": [
        ("Revenue", "营业收入"),
        ("Operating Profit", "营业利润"),
        ("Financial Expense", "财务费用"),
        ("Profit Before Tax", "利润总额"),
        ("Income Tax", "所得税费用"),
        ("Parent Net Profit", "归母净利润"),
    ],
    "BS": [
        ("Cash & Short-term Financial Assets", "现金及短期金融资产"),
        ("Interest-bearing Short-term Debt", "短期有息债务"),
        ("Long-term Interest-bearing Debt", "长期有息债务"),
        ("Core Operating Current Assets", "核心经营性流动资产"),
        ("Operating Non-interest-bearing Current Liabilities", "经营性无息流动负债"),
        ("Minority Interest", "少数股东权益"),
        ("Total Equity", "股东权益"),
        ("Long-term Financial & Equity Investments", "长期金融/股权投资"),
        ("Non-operating Misc. Current Assets", "非经营性流动资产"),
    ],
    "CF": [
        ("Operating Cash Flow", "经营活动现金流"),
        ("Depreciation", "折旧"),
        ("Amortization", "摊销"),
        ("Capex", "资本开支"),
    ],
}


DCF_ITEM_USAGE = {
    "Revenue": "预测收入与各项比率分母",
    "Operating Profit": "计算 EBIT",
    "Financial Expense": "营业利润还原 EBIT",
    "Profit Before Tax": "估算有效税率",
    "Income Tax": "估算有效税率",
    "Parent Net Profit": "校验 CFO 与利润匹配度",
    "Cash & Short-term Financial Assets": "净债务与权益桥",
    "Interest-bearing Short-term Debt": "净债务与 WACC 资本结构",
    "Long-term Interest-bearing Debt": "净债务与 WACC 资本结构",
    "Core Operating Current Assets": "营运资本占用",
    "Operating Non-interest-bearing Current Liabilities": "营运资本占用",
    "Minority Interest": "权益价值调整",
    "Total Equity": "账面权益与资本结构交叉检查",
    "Long-term Financial & Equity Investments": "非经营资产加回",
    "Non-operating Misc. Current Assets": "非经营资产加回",
    "Operating Cash Flow": "FCF 质量与 DCF 适用性",
    "Depreciation": "FCFF 加回项",
    "Amortization": "FCFF 加回项",
    "Capex": "FCFF 扣减项",
}


def load_item_series(df: pd.DataFrame, item_col: str, year_col: str, value_col: str) -> Dict[str, Dict[int, float]]:
    out: Dict[str, Dict[int, float]] = {}
    for _, row in df.iterrows():
        item = str(row[item_col])
        year = int(row[year_col])
        value = float(row[value_col])
        out.setdefault(item, {})[year] = value
    return out


def get_series(item_map: Dict[str, Dict[int, float]], item: str, years: List[int]) -> List[float]:
    series = item_map.get(item, {})
    return [float(series.get(year, 0.0)) for year in years]


def load_wide_items(path: Path, item_col_candidates: List[str]) -> Dict[str, Dict[int, float]]:
    if not path.exists():
        return {}
    wide = pd.read_csv(path)
    item_col = next((col for col in item_col_candidates if col in wide.columns), wide.columns[0])
    year_cols = [col for col in wide.columns if str(col).isdigit()]
    out: Dict[str, Dict[int, float]] = {}
    for _, row in wide.iterrows():
        item = str(row[item_col]).strip()
        out[item] = {}
        for col in year_cols:
            value = pd.to_numeric(row[col], errors="coerce")
            out[item][int(col)] = 0.0 if pd.isna(value) else float(value)
    return out


def safe_div(numerator: float, denominator: float, fallback: float = 0.0) -> float:
    return numerator / denominator if abs(denominator) > 1e-9 else fallback


def count_positive(values: List[float]) -> int:
    return sum(1 for value in values if value > 0)


def trend_slope(values: List[float]) -> float:
    clean = [float(v) for v in values if pd.notna(v)]
    if len(clean) < 2:
        return 0.0
    x_mean = (len(clean) - 1) / 2
    y_mean = avg(clean)
    denominator = sum((i - x_mean) ** 2 for i in range(len(clean)))
    if denominator == 0:
        return 0.0
    return sum((i - x_mean) * (value - y_mean) for i, value in enumerate(clean)) / denominator


def coefficient_of_variation(values: List[float]) -> float:
    clean = [abs(float(v)) for v in values if pd.notna(v)]
    mean_value = avg(clean)
    if mean_value <= 1e-9 or len(clean) < 2:
        return 0.0
    variance = sum((value - mean_value) ** 2 for value in clean) / len(clean)
    return math.sqrt(variance) / mean_value


def cagr(start_value: float, end_value: float, periods: int) -> float:
    if start_value <= 0 or end_value <= 0 or periods <= 0:
        return 0.08
    return (end_value / start_value) ** (1 / periods) - 1


def avg(values: List[float], fallback: float = 0.0) -> float:
    clean = [float(v) for v in values if pd.notna(v)]
    return sum(clean) / len(clean) if clean else fallback


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def dcf_item_usage(item: str) -> str:
    return DCF_ITEM_USAGE.get(item, "DCF建模输入")


def _get_nested(config: dict[str, Any], dotted_key: str) -> Any:
    value: Any = config
    for part in dotted_key.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _format_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.4f}"
    if isinstance(value, list):
        return ", ".join(_format_value(x) for x in value)
    if isinstance(value, dict):
        return str(value)
    return "" if value is None else str(value)


def build_assumption_audit_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    config = data["valuation_config"]
    source_map = data.get("valuation_config_sources", {})
    rows = [
        {
            "category": "Info.csv",
            "assumption": "shares_outstanding",
            "value": data["shares_outstanding"],
            "source": "Info.csv",
            "rationale": "Converts equity value into intrinsic value per share.",
            "review_action": "Confirm latest total share capital and unit.",
        },
        {
            "category": "Info.csv",
            "assumption": "current_price",
            "value": data["current_price"],
            "source": "Info.csv",
            "rationale": "Used for upside/downside and safety margin.",
            "review_action": "Refresh against the latest market price before investment use.",
        },
        {
            "category": "DCF",
            "assumption": "dcf.wacc",
            "value": _get_nested(config, "dcf.wacc"),
            "source": source_map.get("dcf.wacc", "merged config"),
            "rationale": "Discount rate for explicit FCFF and terminal value.",
            "review_action": "Cross-check risk-free rate, beta, ERP, cost of debt, and capital structure.",
        },
        {
            "category": "DCF",
            "assumption": "dcf.terminal_growth",
            "value": _get_nested(config, "dcf.terminal_growth"),
            "source": source_map.get("dcf.terminal_growth", "merged config"),
            "rationale": "Long-term mature growth rate in Gordon terminal value.",
            "review_action": "Keep below WACC and benchmark against long-term nominal GDP / industry maturity.",
        },
        {
            "category": "DCF",
            "assumption": "dcf.dcf_weight",
            "value": _get_nested(config, "dcf.dcf_weight"),
            "source": source_map.get("dcf.dcf_weight", "merged config"),
            "rationale": "Weight applied to DCF in the blended target price.",
            "review_action": "Lower when FCFF visibility is weak or terminal value dominates.",
        },
        {
            "category": "DCF",
            "assumption": "dcf.relative_weight",
            "value": _get_nested(config, "dcf.relative_weight"),
            "source": source_map.get("dcf.relative_weight", "merged config"),
            "rationale": "Weight applied to comparable valuation in the blended target price.",
            "review_action": "Review peer set and cycle position before relying on multiples.",
        },
        {
            "category": "Forecast",
            "assumption": "default_growths",
            "value": data["default_growths"],
            "source": "historical 3-year revenue CAGR, clamped by model rules",
            "rationale": "Revenue forecast seed path.",
            "review_action": "Replace with business-segment forecast when company guidance or order data is available.",
        },
        {
            "category": "Forecast",
            "assumption": "base_ebit_margin",
            "value": data["base_ebit_margin"],
            "source": "recent historical EBIT margin, clamped by model rules",
            "rationale": "Forecast operating profitability.",
            "review_action": "Reconcile to gross margin, expense ratio, and competitive dynamics.",
        },
        {
            "category": "Relative",
            "assumption": "relative_valuation.multiples",
            "value": config.get("relative_valuation", {}).get("multiples", {}),
            "source": "mixed config; see config source map for each multiple leaf",
            "rationale": "Low/mid/high relative valuation ranges.",
            "review_action": "Update with current peer multiples and normalize cyclicality.",
        },
    ]
    for row in rows:
        row["display_value"] = _format_value(row["value"])
    return rows


def build_valuation_risk_warnings(data: dict[str, Any], assumptions: dict[str, Any] | None = None) -> list[dict[str, str]]:
    config = data["valuation_config"]
    dcf = config.get("dcf", {})
    wacc = float((assumptions or {}).get("wacc", dcf.get("wacc", data.get("base_wacc", 0.10))))
    terminal_growth = float((assumptions or {}).get("terminal_growth", dcf.get("terminal_growth", 0.03)))
    dcf_weight = float(dcf.get("dcf_weight", 0.60))
    relative_weight = float(dcf.get("relative_weight", 0.40))
    warnings: list[dict[str, str]] = []

    spread = wacc - terminal_growth
    if spread <= 0:
        warnings.append(
            {
                "level": "high",
                "title": "Terminal growth is not below WACC",
                "detail": "Gordon terminal value becomes unstable when terminal growth is greater than or equal to WACC.",
                "action": "Lower terminal growth or raise WACC before using the target price.",
            }
        )
    elif spread < 0.02:
        warnings.append(
            {
                "level": "high",
                "title": "WACC minus terminal growth spread is too thin",
                "detail": f"The current spread is {spread:.1%}, so small changes in assumptions can swing valuation materially.",
                "action": "Use a wider WACC-g buffer and review sensitivity output first.",
            }
        )
    elif spread < 0.035:
        warnings.append(
            {
                "level": "medium",
                "title": "WACC minus terminal growth spread is narrow",
                "detail": f"The current spread is {spread:.1%}; terminal value sensitivity is elevated.",
                "action": "Check whether terminal growth is justified by mature industry economics.",
            }
        )

    if terminal_growth > 0.04:
        warnings.append(
            {
                "level": "medium",
                "title": "Terminal growth is above a conservative mature-company range",
                "detail": f"Terminal growth is {terminal_growth:.1%}.",
                "action": "Benchmark against long-term inflation, nominal GDP, and industry maturity.",
            }
        )

    if wacc < 0.07:
        warnings.append(
            {
                "level": "medium",
                "title": "WACC appears low for an equity-heavy A-share valuation",
                "detail": f"WACC is {wacc:.1%}.",
                "action": "Recheck beta, equity risk premium, debt cost, and leverage assumptions.",
            }
        )

    if abs(dcf_weight + relative_weight - 1.0) > 0.001:
        warnings.append(
            {
                "level": "high",
                "title": "Blended valuation weights do not sum to 100%",
                "detail": f"DCF weight plus relative valuation weight equals {dcf_weight + relative_weight:.1%}.",
                "action": "Adjust dcf_weight and relative_weight in valuation_config.json.",
            }
        )

    if dcf_weight >= 0.75:
        warnings.append(
            {
                "level": "medium",
                "title": "DCF weight is high",
                "detail": f"DCF carries {dcf_weight:.0%} of the blended target price.",
                "action": "Use a high DCF weight only when FCFF visibility and terminal assumptions are well supported.",
            }
        )

    fcff_positive_ratio = safe_div(count_positive([float(x) for x in data.get("fcff_proxy", [])]), len(data.get("fcff_proxy", [])), 0.0)
    if fcff_positive_ratio < 0.50:
        warnings.append(
            {
                "level": "high",
                "title": "Historical FCFF quality is weak",
                "detail": f"Only {fcff_positive_ratio:.0%} of historical years have positive FCFF proxy.",
                "action": "Prefer scenario analysis, longer explicit forecasts, or relative valuation cross-checks.",
            }
        )
    elif fcff_positive_ratio < 0.70:
        warnings.append(
            {
                "level": "medium",
                "title": "Historical FCFF record is mixed",
                "detail": f"{fcff_positive_ratio:.0%} of historical years have positive FCFF proxy.",
                "action": "Review cash conversion, CapEx cycle, and working-capital assumptions.",
            }
        )

    if not warnings:
        warnings.append(
            {
                "level": "low",
                "title": "No major valuation-assumption warnings",
                "detail": "Core DCF spread, weights, and historical FCFF checks are within baseline guardrails.",
                "action": "Still refresh market data, peer multiples, and company-specific assumptions before use.",
            }
        )
    return warnings
