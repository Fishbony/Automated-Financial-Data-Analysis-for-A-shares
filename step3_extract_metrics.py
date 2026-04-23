"""
Step 3/8 — 核心财务指标提取与计算（基础版）
============================================
从三张财务报表中提取关键科目，计算常用投行建模指标，
输出 Excel 工作簿（含数据层、指标层、口径说明三张 Sheet）。

计算指标
--------
- 收入结构：Revenue、COGS、Gross Profit、Opex、Other operating income
- 盈利能力：EBIT（建模法 & 报表校验法）、D&A、EBITDA、Adjusted EBITDA
- 利润：Pre-tax Profit、Tax、Net profit
- 现金流：CFO、CFI、CFF、FCF
- 资本开支：CapEX、Net CapEx
- 利润率：Gross Margin、Net Margin、EBITDA Margin

注：本脚本为基础版，step4_metrics_report.py 在此基础上新增了
    YoY 增速、CAGR、ROE、资产负债率等完整分析指标。

输入
----
- results/csv/pl.csv    利润表
- results/csv/bs.csv    资产负债表
- results/csv/cf.csv    现金流量表

输出
----
- results/Core_Metrics.xlsx
    * ExtractData      — 原始科目按年份提取值
    * Processed Metrics — 计算后的建模指标
    * Metric Notes     — 各指标口径说明

运行方式
--------
    python step3_extract_metrics.py
    # 或通过主管道：
    python run_pipeline.py
"""

import pandas as pd
import numpy as np

# ── 文件路径配置 ─────────────────────────────────────────────────────────────
PL_FILE = "./results/csv/pl.csv"
BS_FILE = "./results/csv/bs.csv"
CF_FILE = "./results/csv/cf.csv"

OUTPUT_FILE = "./results/Core_Metrics.xlsx"


# ── 数据读取与预处理 ─────────────────────────────────────────────────────────

def load_statement(file_path: str) -> pd.DataFrame:
    """读取财务报表 CSV，标准化列名并转换数值。

    Parameters
    ----------
    file_path : str
        CSV 文件路径

    Returns
    -------
    pd.DataFrame
        以 "Item"（科目名）为索引的数值型 DataFrame
    """
    df = pd.read_csv(file_path)
    df = df.copy()
    df.rename(columns={df.columns[0]: "Item"}, inplace=True)
    df["Item"] = df["Item"].astype(str).str.strip()
    df.columns = ["Item"] + [str(c).strip() for c in df.columns[1:]]
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.set_index("Item")
    return df


pl = load_statement(PL_FILE)
bs = load_statement(BS_FILE)
cf = load_statement(CF_FILE)

# 取三表共同年份
years = [c for c in pl.columns if c in bs.columns and c in cf.columns]


# ── 工具函数 ─────────────────────────────────────────────────────────────────

def find_item(df: pd.DataFrame, candidates: list) -> str | None:
    """在 df.index 中匹配候选科目名，返回第一个找到的科目。

    Parameters
    ----------
    df : pd.DataFrame
        财务报表 DataFrame
    candidates : list
        候选科目名列表（按优先级排列）

    Returns
    -------
    str or None
        第一个匹配的科目名
    """
    for cand in candidates:
        if cand in df.index:
            return cand
    return None


def get_series(df: pd.DataFrame, candidates: list, years: list, default=np.nan) -> pd.Series:
    """从多个候选名中找到实际存在的科目，返回按 years 排列的 Series。

    Parameters
    ----------
    df : pd.DataFrame
        财务报表 DataFrame
    candidates : list
        候选科目名列表
    years : list
        目标年份列
    default : scalar, optional
        科目不存在时的填充值，默认 NaN

    Returns
    -------
    pd.Series
        按 years 排列的数值序列
    """
    item = find_item(df, candidates)
    if item is not None:
        return df.loc[item, years].astype(float)
    else:
        return pd.Series([default] * len(years), index=years, dtype="float64")


def safe_add(*series_list) -> pd.Series:
    """对多个 Series 做 NaN 安全加法（缺失值按 0 处理）。"""
    result = None
    for s in series_list:
        if result is None:
            result = s.copy()
        else:
            result = result.add(s, fill_value=0)
    return result


def safe_sub(a: pd.Series, b: pd.Series) -> pd.Series:
    """对两个 Series 做 NaN 安全减法（缺失值按 0 处理）。"""
    return a.subtract(b, fill_value=0)


def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    """对两个 Series 做除法，分母为 0 时返回 NaN。"""
    b = b.replace(0, np.nan)
    return a / b


# ── 科目映射表（兼容同花顺多版本字段名）────────────────────────────────────
ITEM_MAP = {
    # 利润表科目
    "一、营业总收入(元)":           {"df": pl, "candidates": ["一、营业总收入(元)", "*一、营业总收入(元)", "营业总收入(元)"]},
    "加：营业外收入(元)":           {"df": pl, "candidates": ["加：营业外收入(元)", "营业外收入(元)"]},
    "其中：营业收入(元)":           {"df": pl, "candidates": ["其中：营业收入(元)", "营业收入(元)"]},
    "其中：营业成本(元)":           {"df": pl, "candidates": ["其中：营业成本(元)", "营业成本(元)"]},
    "四、利润总额(元)":             {"df": pl, "candidates": ["四、利润总额(元)", "*四、利润总额(元)", "利润总额(元)"]},
    "营业税金及附加(元)":           {"df": pl, "candidates": ["营业税金及附加(元)"]},
    "销售费用(元)":                 {"df": pl, "candidates": ["销售费用(元)"]},
    "管理费用(元)":                 {"df": pl, "candidates": ["管理费用(元)"]},
    "研发费用(元)":                 {"df": pl, "candidates": ["研发费用(元)"]},
    "资产减值损失(元)":             {"df": pl, "candidates": ["资产减值损失(元)"]},
    "信用减值损失(元)":             {"df": pl, "candidates": ["信用减值损失(元)"]},
    "加：公允价值变动收益(元)":     {"df": pl, "candidates": ["加：公允价值变动收益(元)", "公允价值变动收益(元)"]},
    "投资收益(元)":                 {"df": pl, "candidates": ["投资收益(元)", "加：投资收益(元)"]},
    "资产处置收益(元)":             {"df": pl, "candidates": ["资产处置收益(元)", "加：资产处置收益(元)"]},
    "其他收益(元)":                 {"df": pl, "candidates": ["其他收益(元)", "加：其他收益(元)"]},
    "三、营业利润(元)":             {"df": pl, "candidates": ["三、营业利润(元)", "*三、营业利润(元)", "营业利润(元)"]},
    "其中：利息费用(元)":           {"df": pl, "candidates": ["其中：利息费用(元)", "利息费用(元)"]},
    "利息收入(元)":                 {"df": pl, "candidates": ["利息收入(元)", "其中：利息收入(元)"]},
    "归属于母公司所有者的净利润(元)": {"df": pl, "candidates": ["归属于母公司所有者的净利润(元)", "*归属于母公司所有者的净利润(元)"]},
    "扣除非经常性损益后的净利润(元)": {"df": pl, "candidates": ["扣除非经常性损益后的净利润(元)"]},
    "减：所得税费用(元)":           {"df": pl, "candidates": ["减：所得税费用(元)", "所得税费用(元)"]},
    "五、净利润(元)":               {"df": pl, "candidates": ["五、净利润(元)", "*五、净利润(元)", "净利润(元)"]},
    # 现金流量表科目（D&A 取自间接法附注）
    "固定资产折旧、油气资产折耗、生产性生物资产折旧(元)": {
        "df": cf, "candidates": [
            "固定资产折旧、油气资产折耗、生产性生物资产折旧(元)",
            "固定资产折旧、油气资产折耗、生产性生物资产折旧"
        ]
    },
    "无形资产摊销(元)":             {"df": cf, "candidates": ["无形资产摊销(元)", "无形资产摊销"]},
    "长期待摊费用摊销(元)":         {"df": cf, "candidates": ["长期待摊费用摊销(元)", "长期待摊费用摊销"]},
    "购建固定资产、无形资产和其他长期资产支付的现金(元)": {
        "df": cf, "candidates": ["购建固定资产、无形资产和其他长期资产支付的现金(元)"]
    },
    "处置固定资产、无形资产和其他长期资产收回的现金净额(元)": {
        "df": cf, "candidates": ["处置固定资产、无形资产和其他长期资产收回的现金净额(元)"]
    },
    "*经营活动产生的现金流量净额(元)": {"df": cf, "candidates": ["*经营活动产生的现金流量净额(元)", "经营活动产生的现金流量净额(元)"]},
    "*投资活动产生的现金流量净额(元)": {"df": cf, "candidates": ["*投资活动产生的现金流量净额(元)", "投资活动产生的现金流量净额(元)"]},
    "*筹资活动产生的现金流量净额(元)": {"df": cf, "candidates": ["*筹资活动产生的现金流量净额(元)", "筹资活动产生的现金流量净额(元)"]},
    # 资产负债表科目
    "归属于母公司所有者权益合计(元)": {"df": bs, "candidates": ["归属于母公司所有者权益合计(元)", "*归属于母公司所有者权益合计(元)"]},
}


# ── 提取原始数据 ─────────────────────────────────────────────────────────────
extract_df = pd.DataFrame(index=list(ITEM_MAP.keys()), columns=years, dtype="float64")
for item_name, meta in ITEM_MAP.items():
    extract_df.loc[item_name] = get_series(meta["df"], meta["candidates"], years)


# ── 计算建模指标 ─────────────────────────────────────────────────────────────
metrics = pd.DataFrame(index=[
    "营业外收入/营业总收入", "营业外收入/利润总额",
    "Revenue", "COGS", "Gross Profit", "Opex", "Other operating income",
    "EBIT_建模法", "EBIT_报表校验法",
    "D&A", "EBITDA", "Adjusted EBITDA",
    "Pre-tax Profit", "Tax", "Net profit",
    "CapEX", "Net CapEx",
    "CFO", "CFI", "CFF", "FCF",
    "Gross Margin", "Net Margin", "EBITDA Margin",
], columns=years, dtype="float64")

# 提取常用变量（方便计算）
营业总收入 = extract_df.loc["一、营业总收入(元)"]
营业外收入 = extract_df.loc["加：营业外收入(元)"]
营业收入   = extract_df.loc["其中：营业收入(元)"]
营业成本   = extract_df.loc["其中：营业成本(元)"]
利润总额   = extract_df.loc["四、利润总额(元)"]
营业税金及附加 = extract_df.loc["营业税金及附加(元)"]
销售费用   = extract_df.loc["销售费用(元)"]
管理费用   = extract_df.loc["管理费用(元)"]
研发费用   = extract_df.loc["研发费用(元)"]
资产减值损失 = extract_df.loc["资产减值损失(元)"]
信用减值损失 = extract_df.loc["信用减值损失(元)"]
公允价值变动收益 = extract_df.loc["加：公允价值变动收益(元)"]
投资收益   = extract_df.loc["投资收益(元)"]
资产处置收益 = extract_df.loc["资产处置收益(元)"]
其他收益   = extract_df.loc["其他收益(元)"]
营业利润   = extract_df.loc["三、营业利润(元)"]
利息费用   = extract_df.loc["其中：利息费用(元)"]
利息收入   = extract_df.loc["利息收入(元)"]
折旧       = extract_df.loc["固定资产折旧、油气资产折耗、生产性生物资产折旧(元)"]
无形摊销   = extract_df.loc["无形资产摊销(元)"]
长期待摊摊销 = extract_df.loc["长期待摊费用摊销(元)"]
归母净利润 = extract_df.loc["归属于母公司所有者的净利润(元)"]
扣非净利润 = extract_df.loc["扣除非经常性损益后的净利润(元)"]
所得税     = extract_df.loc["减：所得税费用(元)"]
净利润     = extract_df.loc["五、净利润(元)"]
购建长期资产支付现金 = extract_df.loc["购建固定资产、无形资产和其他长期资产支付的现金(元)"]
处置长期资产收回现金 = extract_df.loc["处置固定资产、无形资产和其他长期资产收回的现金净额(元)"]
CFO = extract_df.loc["*经营活动产生的现金流量净额(元)"]
CFI = extract_df.loc["*投资活动产生的现金流量净额(元)"]
CFF = extract_df.loc["*筹资活动产生的现金流量净额(元)"]

# 非经常性收益占比（衡量利润含金量）
metrics.loc["营业外收入/营业总收入"] = safe_div(营业外收入, 营业总收入)
metrics.loc["营业外收入/利润总额"]   = safe_div(营业外收入, 利润总额)

# 核心损益
metrics.loc["Revenue"]      = 营业收入
metrics.loc["COGS"]         = 营业成本
metrics.loc["Gross Profit"] = safe_sub(营业收入, 营业成本)

# Opex：税金及附加 + 销售 + 管理 + 研发（建模口径）
metrics.loc["Opex"] = safe_add(营业税金及附加, 销售费用, 管理费用, 研发费用)

# Other operating income：非主营业务但归属经营性的调整项
metrics.loc["Other operating income"] = (
    其他收益.fillna(0) + 投资收益.fillna(0) + 公允价值变动收益.fillna(0)
    + 资产处置收益.fillna(0) - 资产减值损失.fillna(0) - 信用减值损失.fillna(0)
)

# EBIT 建模法 = Gross Profit - Opex + Other operating income
metrics.loc["EBIT_建模法"] = (
    metrics.loc["Gross Profit"].fillna(0)
    - metrics.loc["Opex"].fillna(0)
    + metrics.loc["Other operating income"].fillna(0)
)

# EBIT 报表校验法 = 营业利润 + 利息费用 - 利息收入（用于交叉验证建模法）
metrics.loc["EBIT_报表校验法"] = (
    营业利润.fillna(0) + 利息费用.fillna(0) - 利息收入.fillna(0)
)

# D&A = 折旧 + 无形资产摊销 + 长期待摊费用摊销
metrics.loc["D&A"] = 折旧.fillna(0) + 无形摊销.fillna(0) + 长期待摊摊销.fillna(0)

metrics.loc["EBITDA"] = metrics.loc["EBIT_建模法"].fillna(0) + metrics.loc["D&A"].fillna(0)

# Adjusted EBITDA：以扣非净利润为起点还原税、净利息、D&A
净利息支出 = 利息费用.fillna(0) - 利息收入.fillna(0)
metrics.loc["Adjusted EBITDA"] = (
    扣非净利润.fillna(0) + 所得税.fillna(0) + 净利息支出 + metrics.loc["D&A"].fillna(0)
)

metrics.loc["Pre-tax Profit"] = 利润总额
metrics.loc["Tax"]            = 所得税
metrics.loc["Net profit"]     = 净利润

metrics.loc["CapEX"]     = 购建长期资产支付现金
metrics.loc["Net CapEx"] = 购建长期资产支付现金.fillna(0) - 处置长期资产收回现金.fillna(0)

metrics.loc["CFO"] = CFO
metrics.loc["CFI"] = CFI
metrics.loc["CFF"] = CFF
metrics.loc["FCF"] = CFO.fillna(0) - metrics.loc["CapEX"].fillna(0)   # FCF = CFO - CapEX

metrics.loc["Gross Margin"]  = safe_div(metrics.loc["Gross Profit"], metrics.loc["Revenue"])
metrics.loc["Net Margin"]    = safe_div(metrics.loc["Net profit"],   metrics.loc["Revenue"])
metrics.loc["EBITDA Margin"] = safe_div(metrics.loc["EBITDA"],       metrics.loc["Revenue"])


# ── 口径说明表 ────────────────────────────────────────────────────────────────
notes = pd.DataFrame({
    "Metric": ["Opex", "Other operating income", "EBIT_建模法", "EBIT_报表校验法",
                "D&A", "Adjusted EBITDA", "CapEX", "Net CapEx", "FCF"],
    "Definition": [
        "营业税金及附加 + 销售费用 + 管理费用 + 研发费用",
        "其他收益 + 投资收益 + 公允价值变动收益 + 资产处置收益 - 资产减值损失 - 信用减值损失",
        "Gross Profit - Opex + Other operating income",
        "营业利润 + 利息费用 - 利息收入",
        "折旧 + 无形资产摊销 + 长期待摊费用摊销",
        "扣非净利润 + 所得税 + 净利息支出 + D&A",
        "购建固定资产、无形资产和其他长期资产支付的现金",
        "CapEX - 处置长期资产收回现金净额",
        "CFO - CapEX",
    ]
})

# ── 导出 Excel ────────────────────────────────────────────────────────────────
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    extract_df.to_excel(writer, sheet_name="ExtractData")
    metrics.to_excel(writer, sheet_name="Processed Metrics")
    notes.to_excel(writer, sheet_name="Metric Notes", index=False)

print(f"Step 3 完成！已输出：{OUTPUT_FILE}")
print("提取数据预览：")
print(extract_df.head(10))
print("\n计算指标预览：")
print(metrics.head(10))
