"""
dcf_types — DCF 数据集类型定义
================================
将 build_historical_dataset() 返回的无类型 dict 定义为 TypedDict，
提供类型安全、IDE 补全和拼错键名编译期报错。

TypedDict 在运行时与普通 dict 完全兼容，不需要修改任何消费方代码。
"""

from __future__ import annotations

from typing import Dict, List, TypedDict


class DCFData(TypedDict):
    """build_historical_dataset() 返回的 DCF 历史数据集。

    所有键在 build_historical_dataset() 执行完毕后均存在。
    消费方（create_*_sheet、build_readiness_checks 等）通过
    data["key"] 语法访问，TypedDict 提供静态类型检查。
    """

    # ── 时间维度 ──────────────────────────────────────────
    years: List[int]
    forecast_years: List[int]
    base_year: int

    # ── 利润表序列 ────────────────────────────────────────
    revenue: List[float]
    operating_profit: List[float]
    financial_expense: List[float]
    ebit: List[float]
    profit_before_tax: List[float]
    income_tax: List[float]
    parent_net_profit: List[float]

    # ── 现金流量表序列 ────────────────────────────────────
    depreciation: List[float]
    amortization: List[float]
    capex: List[float]
    cfo: List[float]
    fcff_proxy: List[float]

    # ── 资产负债表序列 ────────────────────────────────────
    cash: List[float]
    short_debt: List[float]
    long_debt: List[float]
    minority_interest: List[float]
    total_equity: List[float]
    non_op_current_assets: List[float]
    long_term_investments: List[float]

    # ── 计算指标序列 ──────────────────────────────────────
    nwc: List[float]
    tax_rate: List[float]
    ebit_margin: List[float]
    da_ratio: List[float]
    capex_ratio: List[float]
    nwc_ratio: List[float]

    # ── 市场数据 ──────────────────────────────────────────
    shares_outstanding: float
    current_price: float

    # ── 预测假设种子 ──────────────────────────────────────
    default_growths: List[float]
    base_ebit_margin: float
    base_tax_rate: float
    base_da_ratio: float
    base_capex_ratio: float
    base_nwc_ratio: float

    # ── 公司标识 ──────────────────────────────────────────
    ticker: str
    company_code: str
    company_name: str
    company_label: str
    valuation_date: str

    # ── 估值配置 ──────────────────────────────────────────
    valuation_config: Dict[str, object]
    valuation_config_sources: Dict[str, str]

    # ── 衍生数据（在 build_historical_dataset 末尾追加） ──
    dcf_input_rows: List[Dict[str, object]]
    readiness_checks: List[Dict[str, object]]
    assumption_audit_rows: List[Dict[str, object]]
    valuation_risk_warnings: List[Dict[str, object]]
