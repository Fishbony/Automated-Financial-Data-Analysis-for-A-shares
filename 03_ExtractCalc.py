import pandas as pd
import numpy as np

# =========================
# 1. 文件路径
# =========================
PL_FILE = "./results/csv/pl.csv"   # 利润表
BS_FILE = "./results/csv/bs.csv"   # 资产负债表
CF_FILE = "./results/csv/cf.csv"   # 现金流量表

OUTPUT_FILE = "./results/Core_Metrics.xlsx"


# =========================
# 2. 读取与预处理
# =========================
def load_statement(file_path):
    df = pd.read_csv(file_path)
    df = df.copy()

    # 第一列改名为 Item
    df.rename(columns={df.columns[0]: "Item"}, inplace=True)

    # 去掉首尾空格
    df["Item"] = df["Item"].astype(str).str.strip()
    df.columns = ["Item"] + [str(c).strip() for c in df.columns[1:]]

    # 全部转数值
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.set_index("Item")
    return df


pl = load_statement(PL_FILE)
bs = load_statement(BS_FILE)
cf = load_statement(CF_FILE)

# 取共同年份
years = [c for c in pl.columns if c in bs.columns and c in cf.columns]


# =========================
# 3. 工具函数
# =========================
def find_item(df, candidates):
    """
    在df.index中匹配候选科目名，返回第一个找到的科目
    """
    for cand in candidates:
        if cand in df.index:
            return cand
    return None


def get_series(df, candidates, years, default=np.nan):
    """
    从多个候选名中找到实际存在的科目，并返回按 years 排列的 Series
    """
    item = find_item(df, candidates)
    if item is not None:
        return df.loc[item, years].astype(float)
    else:
        return pd.Series([default] * len(years), index=years, dtype="float64")


def safe_add(*series_list):
    result = None
    for s in series_list:
        if result is None:
            result = s.copy()
        else:
            result = result.add(s, fill_value=0)
    return result


def safe_sub(a, b):
    return a.subtract(b, fill_value=0)


def safe_div(a, b):
    b = b.replace(0, np.nan)
    return a / b


# =========================
# 4. 科目映射
# =========================
ITEM_MAP = {
    # 利润表
    "一、营业总收入(元)": {
        "df": pl,
        "candidates": ["一、营业总收入(元)", "*一、营业总收入(元)", "营业总收入(元)"]
    },
    "加：营业外收入(元)": {
        "df": pl,
        "candidates": ["加：营业外收入(元)", "营业外收入(元)"]
    },
    "其中：营业收入(元)": {
        "df": pl,
        "candidates": ["其中：营业收入(元)", "营业收入(元)"]
    },
    "其中：营业成本(元)": {
        "df": pl,
        "candidates": ["其中：营业成本(元)", "营业成本(元)"]
    },
    "四、利润总额(元)": {
        "df": pl,
        "candidates": ["四、利润总额(元)", "*四、利润总额(元)", "利润总额(元)"]
    },
    "营业税金及附加(元)": {
        "df": pl,
        "candidates": ["营业税金及附加(元)"]
    },
    "销售费用(元)": {
        "df": pl,
        "candidates": ["销售费用(元)"]
    },
    "管理费用(元)": {
        "df": pl,
        "candidates": ["管理费用(元)"]
    },
    "研发费用(元)": {
        "df": pl,
        "candidates": ["研发费用(元)"]
    },
    "资产减值损失(元)": {
        "df": pl,
        "candidates": ["资产减值损失(元)"]
    },
    "信用减值损失(元)": {
        "df": pl,
        "candidates": ["信用减值损失(元)"]
    },
    "加：公允价值变动收益(元)": {
        "df": pl,
        "candidates": ["加：公允价值变动收益(元)", "公允价值变动收益(元)"]
    },
    "投资收益(元)": {
        "df": pl,
        "candidates": ["投资收益(元)", "加：投资收益(元)"]
    },
    "资产处置收益(元)": {
        "df": pl,
        "candidates": ["资产处置收益(元)", "加：资产处置收益(元)"]
    },
    "其他收益(元)": {
        "df": pl,
        "candidates": ["其他收益(元)", "加：其他收益(元)"]
    },
    "三、营业利润(元)": {
        "df": pl,
        "candidates": ["三、营业利润(元)", "*三、营业利润(元)", "营业利润(元)"]
    },
    "其中：利息费用(元)": {
        "df": pl,
        "candidates": ["其中：利息费用(元)", "利息费用(元)"]
    },
    "利息收入(元)": {
        "df": pl,
        "candidates": ["利息收入(元)", "其中：利息收入(元)"]
    },
    "固定资产折旧、油气资产折耗、生产性生物资产折旧(元)": {
        "df": cf,
        "candidates": [
            "固定资产折旧、油气资产折耗、生产性生物资产折旧(元)",
            "固定资产折旧、油气资产折耗、生产性生物资产折旧"
        ]
    },
    "无形资产摊销(元)": {
        "df": cf,
        "candidates": ["无形资产摊销(元)", "无形资产摊销"]
    },
    "长期待摊费用摊销(元)": {
        "df": cf,
        "candidates": ["长期待摊费用摊销(元)", "长期待摊费用摊销"]
    },
    "归属于母公司所有者的净利润(元)": {
        "df": pl,
        "candidates": ["归属于母公司所有者的净利润(元)", "*归属于母公司所有者的净利润(元)"]
    },
    "扣除非经常性损益后的净利润(元)": {
        "df": pl,
        "candidates": ["扣除非经常性损益后的净利润(元)"]
    },
    "减：所得税费用(元)": {
        "df": pl,
        "candidates": ["减：所得税费用(元)", "所得税费用(元)"]
    },
    "五、净利润(元)": {
        "df": pl,
        "candidates": ["五、净利润(元)", "*五、净利润(元)", "净利润(元)"]
    },

    # 现金流量表
    "购建固定资产、无形资产和其他长期资产支付的现金(元)": {
        "df": cf,
        "candidates": ["购建固定资产、无形资产和其他长期资产支付的现金(元)"]
    },
    "处置固定资产、无形资产和其他长期资产收回的现金净额(元)": {
        "df": cf,
        "candidates": ["处置固定资产、无形资产和其他长期资产收回的现金净额(元)"]
    },
    "*经营活动产生的现金流量净额(元)": {
        "df": cf,
        "candidates": ["*经营活动产生的现金流量净额(元)", "经营活动产生的现金流量净额(元)"]
    },
    "*投资活动产生的现金流量净额(元)": {
        "df": cf,
        "candidates": ["*投资活动产生的现金流量净额(元)", "投资活动产生的现金流量净额(元)"]
    },
    "*筹资活动产生的现金流量净额(元)": {
        "df": cf,
        "candidates": ["*筹资活动产生的现金流量净额(元)", "筹资活动产生的现金流量净额(元)"]
    },

    # 资产负债表
    "归属于母公司所有者权益合计(元)": {
        "df": bs,
        "candidates": ["归属于母公司所有者权益合计(元)", "*归属于母公司所有者权益合计(元)"]
    }
}


# =========================
# 5. 提取原始数据 ExtractData
# =========================
extract_df = pd.DataFrame(index=list(ITEM_MAP.keys()), columns=years, dtype="float64")

for item_name, meta in ITEM_MAP.items():
    extract_df.loc[item_name] = get_series(
        df=meta["df"],
        candidates=meta["candidates"],
        years=years
    )


# =========================
# 6. 计算 Processed Metrics
# =========================
metrics = pd.DataFrame(index=[
    "营业外收入/营业总收入",
    "营业外收入/利润总额",
    "Revenue",
    "COGS",
    "Gross Profit",
    "Opex",
    "Other operating income",
    "EBIT_建模法",
    "EBIT_报表校验法",
    "D&A",
    "EBITDA",
    "Adjusted EBITDA",
    "Pre-tax Profit",
    "Tax",
    "Net profit",
    "CapEX",
    "Net CapEx",
    "CFO",
    "CFI",
    "CFF",
    "FCF",
    "Gross Margin",
    "Net Margin",
    "EBITDA Margin"
], columns=years, dtype="float64")


# ---- 取出常用变量 ----
营业总收入 = extract_df.loc["一、营业总收入(元)"]
营业外收入 = extract_df.loc["加：营业外收入(元)"]
营业收入 = extract_df.loc["其中：营业收入(元)"]
营业成本 = extract_df.loc["其中：营业成本(元)"]
利润总额 = extract_df.loc["四、利润总额(元)"]
营业税金及附加 = extract_df.loc["营业税金及附加(元)"]
销售费用 = extract_df.loc["销售费用(元)"]
管理费用 = extract_df.loc["管理费用(元)"]
研发费用 = extract_df.loc["研发费用(元)"]
资产减值损失 = extract_df.loc["资产减值损失(元)"]
信用减值损失 = extract_df.loc["信用减值损失(元)"]
公允价值变动收益 = extract_df.loc["加：公允价值变动收益(元)"]
投资收益 = extract_df.loc["投资收益(元)"]
资产处置收益 = extract_df.loc["资产处置收益(元)"]
其他收益 = extract_df.loc["其他收益(元)"]
营业利润 = extract_df.loc["三、营业利润(元)"]
利息费用 = extract_df.loc["其中：利息费用(元)"]
利息收入 = extract_df.loc["利息收入(元)"]
折旧 = extract_df.loc["固定资产折旧、油气资产折耗、生产性生物资产折旧(元)"]
无形摊销 = extract_df.loc["无形资产摊销(元)"]
长期待摊摊销 = extract_df.loc["长期待摊费用摊销(元)"]
归母净利润 = extract_df.loc["归属于母公司所有者的净利润(元)"]
扣非净利润 = extract_df.loc["扣除非经常性损益后的净利润(元)"]
所得税 = extract_df.loc["减：所得税费用(元)"]
净利润 = extract_df.loc["五、净利润(元)"]

购建长期资产支付现金 = extract_df.loc["购建固定资产、无形资产和其他长期资产支付的现金(元)"]
处置长期资产收回现金 = extract_df.loc["处置固定资产、无形资产和其他长期资产收回的现金净额(元)"]
CFO = extract_df.loc["*经营活动产生的现金流量净额(元)"]
CFI = extract_df.loc["*投资活动产生的现金流量净额(元)"]
CFF = extract_df.loc["*筹资活动产生的现金流量净额(元)"]

归母权益 = extract_df.loc["归属于母公司所有者权益合计(元)"]


# ---- 开始计算 ----

# 比例指标
metrics.loc["营业外收入/营业总收入"] = safe_div(营业外收入, 营业总收入)
metrics.loc["营业外收入/利润总额"] = safe_div(营业外收入, 利润总额)

# 核心损益
metrics.loc["Revenue"] = 营业收入
metrics.loc["COGS"] = 营业成本
metrics.loc["Gross Profit"] = safe_sub(营业收入, 营业成本)

# Opex：你这里按常见建模口径，先只纳入税金及附加、销售、管理、研发
metrics.loc["Opex"] = safe_add(营业税金及附加, 销售费用, 管理费用, 研发费用)

# Other operating income
# 这里按“其他经营性收益/损失调整项”近似：
# 其他收益 + 投资收益 + 公允价值变动收益 + 资产处置收益 - 资产减值损失 - 信用减值损失
metrics.loc["Other operating income"] = (
    其他收益.fillna(0)
    + 投资收益.fillna(0)
    + 公允价值变动收益.fillna(0)
    + 资产处置收益.fillna(0)
    - 资产减值损失.fillna(0)
    - 信用减值损失.fillna(0)
)

# EBIT_建模法
# = Gross Profit - Opex + Other operating income
metrics.loc["EBIT_建模法"] = (
    metrics.loc["Gross Profit"].fillna(0)
    - metrics.loc["Opex"].fillna(0)
    + metrics.loc["Other operating income"].fillna(0)
)

# EBIT_报表校验法
# = 营业利润 + 利息费用 - 利息收入
metrics.loc["EBIT_报表校验法"] = (
    营业利润.fillna(0)
    + 利息费用.fillna(0)
    - 利息收入.fillna(0)
)

# D&A
metrics.loc["D&A"] = (
    折旧.fillna(0)
    + 无形摊销.fillna(0)
    + 长期待摊摊销.fillna(0)
)

# EBITDA
metrics.loc["EBITDA"] = (
    metrics.loc["EBIT_建模法"].fillna(0)
    + metrics.loc["D&A"].fillna(0)
)

# Adjusted EBITDA
# 用扣非净利润回推的近似法：
# 扣非净利润 + 税 + 净利息支出 + D&A
净利息支出 = 利息费用.fillna(0) - 利息收入.fillna(0)
metrics.loc["Adjusted EBITDA"] = (
    扣非净利润.fillna(0)
    + 所得税.fillna(0)
    + 净利息支出.fillna(0)
    + metrics.loc["D&A"].fillna(0)
)

# Pre-tax Profit
metrics.loc["Pre-tax Profit"] = 利润总额

# Tax
metrics.loc["Tax"] = 所得税

# Net profit
metrics.loc["Net profit"] = 净利润

# CapEX
metrics.loc["CapEX"] = 购建长期资产支付现金

# Net CapEx
metrics.loc["Net CapEx"] = (
    购建长期资产支付现金.fillna(0)
    - 处置长期资产收回现金.fillna(0)
)

# CFO / CFI / CFF
metrics.loc["CFO"] = CFO
metrics.loc["CFI"] = CFI
metrics.loc["CFF"] = CFF

# FCF
metrics.loc["FCF"] = CFO.fillna(0) - metrics.loc["CapEX"].fillna(0)

# 利润率
metrics.loc["Gross Margin"] = safe_div(metrics.loc["Gross Profit"], metrics.loc["Revenue"])
metrics.loc["Net Margin"] = safe_div(metrics.loc["Net profit"], metrics.loc["Revenue"])
metrics.loc["EBITDA Margin"] = safe_div(metrics.loc["EBITDA"], metrics.loc["Revenue"])


# =========================
# 7. 增加一个说明表
# =========================
notes = pd.DataFrame({
    "Metric": [
        "Opex",
        "Other operating income",
        "EBIT_建模法",
        "EBIT_报表校验法",
        "D&A",
        "Adjusted EBITDA",
        "CapEX",
        "Net CapEx",
        "FCF"
    ],
    "Definition": [
        "营业税金及附加 + 销售费用 + 管理费用 + 研发费用",
        "其他收益 + 投资收益 + 公允价值变动收益 + 资产处置收益 - 资产减值损失 - 信用减值损失",
        "Gross Profit - Opex + Other operating income",
        "营业利润 + 利息费用 - 利息收入",
        "折旧 + 无形资产摊销 + 长期待摊费用摊销",
        "扣非净利润 + 所得税 + 净利息支出 + D&A",
        "购建固定资产、无形资产和其他长期资产支付的现金",
        "CapEX - 处置长期资产收回现金净额",
        "CFO - CapEX"
    ]
})


# =========================
# 8. 导出 Excel
# =========================
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    extract_df.to_excel(writer, sheet_name="ExtractData")
    metrics.to_excel(writer, sheet_name="Processed Metrics")
    notes.to_excel(writer, sheet_name="Metric Notes", index=False)

print(f"已输出文件: {OUTPUT_FILE}")
print("提取数据预览：")
print(extract_df.head(10))
print("\n计算指标预览：")
print(metrics.head(10))