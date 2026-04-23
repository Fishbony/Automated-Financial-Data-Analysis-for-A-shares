import pandas as pd
import numpy as np
from pathlib import Path

# =========================
# 配置区：把这里改成你的文件名
# =========================
BS_FILE = "./results/csv/bs.csv"   # 资产负债表
CF_FILE = "./results/csv/cf.csv"   # 现金流量表
PL_FILE = "./results/csv/pl.csv"   # 利润表

OUTPUT_XLSX = "./results/三表一致性检验结果.xlsx"
OUTPUT_MD = "./results/三表一致性检验报告.md"

# =========================
# 读取与预处理
# =========================
def load_statement(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "科目"})
    df["科目"] = df["科目"].astype(str).str.strip()
    df.columns = ["科目"] + [str(c).strip() for c in df.columns[1:]]
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.set_index("科目")

def find_existing(df: pd.DataFrame, candidates):
    for name in candidates:
        if name in df.index:
            return name
    return None

def get_series(df: pd.DataFrame, candidates, years, required=True):
    name = find_existing(df, candidates)
    if name is None:
        if required:
            raise KeyError(f"找不到科目：{candidates}")
        return None
    return df.loc[name, years]

def fmt(x):
    if pd.isna(x):
        return "NaN"
    return f"{x:,.2f}"

def fmt_bool(x):
    return "通过" if bool(x) else "未通过"

def near_zero(series, tol=1e3):
    return np.isclose(series.fillna(0), 0, atol=tol)

# =========================
# 主程序
# =========================
bs = load_statement(BS_FILE)
cf = load_statement(CF_FILE)
pl = load_statement(PL_FILE)

years = [c for c in bs.columns if c in cf.columns and c in pl.columns]

# ---- 科目候选名：按 A 股常见导出格式准备 ----
asset_candidates = ["*资产合计(元)", "资产总计(元)", "资产合计(元)"]
liab_candidates = ["*负债合计(元)", "负债合计(元)"]
equity_candidates = ["*所有者权益（或股东权益）合计(元)", "所有者权益合计(元)", "股东权益合计(元)"]
parent_equity_candidates = ["*归属于母公司所有者权益合计(元)", "归属于母公司所有者权益合计(元)"]
cash_bs_candidates = ["货币资金(元)", "货币资金"]

cfo_candidates = ["*经营活动产生的现金流量净额(元)", "经营活动产生的现金流量净额(元)"]
cfi_candidates = ["*投资活动产生的现金流量净额(元)", "投资活动产生的现金流量净额(元)"]
cff_candidates = ["*筹资活动产生的现金流量净额(元)", "筹资活动产生的现金流量净额(元)"]
fx_candidates = ["四、汇率变动对现金及现金等价物的影响(元)", "汇率变动对现金及现金等价物的影响(元)"]
net_cash_candidates = ["*现金及现金等价物净增加额(元)", "五、现金及现金等价物净增加额(元)", "现金及现金等价物净增加额(元)"]
begin_cash_candidates = ["加：期初现金及现金等价物余额(元)", "期初现金及现金等价物余额(元)"]
ending_cash_candidates = ["*期末现金及现金等价物余额(元)", "六、期末现金及现金等价物余额(元)", "期末现金及现金等价物余额(元)"]

net_profit_candidates = ["*净利润(元)", "净利润(元)"]
parent_np_candidates = ["*归属于母公司所有者的净利润(元)", "归属于母公司所有者的净利润(元)"]

# ---- 取数 ----
assets = get_series(bs, asset_candidates, years)
liabs = get_series(bs, liab_candidates, years)
equity = get_series(bs, equity_candidates, years)
parent_equity = get_series(bs, parent_equity_candidates, years, required=False)
cash_bs = get_series(bs, cash_bs_candidates, years, required=False)

cfo = get_series(cf, cfo_candidates, years)
cfi = get_series(cf, cfi_candidates, years)
cff = get_series(cf, cff_candidates, years)
net_cash = get_series(cf, net_cash_candidates, years)
ending_cash = get_series(cf, ending_cash_candidates, years)
begin_cash = get_series(cf, begin_cash_candidates, years, required=False)

fx_name = find_existing(cf, fx_candidates)
if fx_name is not None:
    fx = cf.loc[fx_name, years]
else:
    fx = pd.Series(0.0, index=years)

net_profit = get_series(pl, net_profit_candidates, years, required=False)
parent_np = get_series(pl, parent_np_candidates, years, required=False)

# =========================
# 检查 1：资产负债表平衡
# =========================
bs_check = pd.DataFrame(index=years)
bs_check["资产合计"] = assets
bs_check["负债合计"] = liabs
bs_check["所有者权益合计"] = equity
bs_check["负债+权益"] = bs_check["负债合计"] + bs_check["所有者权益合计"]
bs_check["差额"] = bs_check["资产合计"] - bs_check["负债+权益"]
bs_check["是否通过"] = near_zero(bs_check["差额"], tol=1e3)

# =========================
# 检查 2：现金流量表平衡
# 现金净增加额 = CFO + CFI + CFF + 汇率影响
# =========================
cf_check = pd.DataFrame(index=years)
cf_check["CFO"] = cfo
cf_check["CFI"] = cfi
cf_check["CFF"] = cff
cf_check["汇率影响"] = fx
cf_check["理论净增加额"] = cf_check["CFO"] + cf_check["CFI"] + cf_check["CFF"] + cf_check["汇率影响"]
cf_check["报表净增加额"] = net_cash
cf_check["差额"] = cf_check["报表净增加额"] - cf_check["理论净增加额"]
cf_check["是否通过"] = near_zero(cf_check["差额"], tol=1e3)

# =========================
# 检查 3：现金滚动检查
# 期初现金 + 净增加额 = 期末现金
# =========================
cash_roll_check = None
if begin_cash is not None:
    cash_roll_check = pd.DataFrame(index=years)
    cash_roll_check["期初现金"] = begin_cash
    cash_roll_check["净增加额"] = net_cash
    cash_roll_check["理论期末现金"] = cash_roll_check["期初现金"] + cash_roll_check["净增加额"]
    cash_roll_check["报表期末现金"] = ending_cash
    cash_roll_check["差额"] = cash_roll_check["报表期末现金"] - cash_roll_check["理论期末现金"]
    cash_roll_check["是否通过"] = near_zero(cash_roll_check["差额"], tol=1e3)

# =========================
# 检查 4：现金流量表 vs 资产负债表
# 期末现金及现金等价物 vs 货币资金（观察性校验）
# =========================
cash_link_check = None
if cash_bs is not None:
    cash_link_check = pd.DataFrame(index=years)
    cash_link_check["资产负债表_货币资金"] = cash_bs
    cash_link_check["现金流量表_期末现金及现金等价物"] = ending_cash
    cash_link_check["差额"] = cash_link_check["资产负债表_货币资金"] - cash_link_check["现金流量表_期末现金及现金等价物"]

# =========================
# 检查 5：利润表 vs 权益变动（趋势性，不做强等式）
# =========================
equity_link_check = None
if parent_np is not None and parent_equity is not None:
    equity_link_check = pd.DataFrame(index=years)
    equity_link_check["归母净利润"] = parent_np
    equity_link_check["归母权益期末值"] = parent_equity
    equity_link_check["归母权益增加额"] = equity_link_check["归母权益期末值"].diff()
    equity_link_check["同向性检查"] = np.where(
        (equity_link_check["归母净利润"] >= 0) & (equity_link_check["归母权益增加额"].fillna(0) >= 0),
        "大体一致",
        "需进一步看分红/回购/OCI"
    )

# =========================
# 输出到 Excel
# =========================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    bs_check.to_excel(writer, sheet_name="1_资产负债表平衡")
    cf_check.to_excel(writer, sheet_name="2_现金流量表平衡")
    if cash_roll_check is not None:
        cash_roll_check.to_excel(writer, sheet_name="3_现金滚动检查")
    if cash_link_check is not None:
        cash_link_check.to_excel(writer, sheet_name="4_货币资金对比")
    if equity_link_check is not None:
        equity_link_check.to_excel(writer, sheet_name="5_净利润与权益变动")

# =========================
# 输出 Markdown 报告
# =========================
lines = []
lines.append("# 三表一致性检验报告")
lines.append("")
lines.append("## 一、资产负债表校验")
lines.append("")
lines.append("校验公式：`资产合计 = 负债合计 + 所有者权益合计`")
lines.append("")
lines.append("| 年份 | 资产合计 | 负债+权益 | 差额 | 结果 |")
lines.append("|---|---:|---:|---:|---|")
for y, row in bs_check.iterrows():
    lines.append(f"| {y} | {fmt(row['资产合计'])} | {fmt(row['负债+权益'])} | {fmt(row['差额'])} | {fmt_bool(row['是否通过'])} |")
lines.append("")
lines.append("## 二、现金流量表校验")
lines.append("")
lines.append("校验公式：`现金及现金等价物净增加额 = CFO + CFI + CFF + 汇率影响`")
lines.append("")
lines.append("| 年份 | 理论净增加额 | 报表净增加额 | 差额 | 结果 |")
lines.append("|---|---:|---:|---:|---|")
for y, row in cf_check.iterrows():
    lines.append(f"| {y} | {fmt(row['理论净增加额'])} | {fmt(row['报表净增加额'])} | {fmt(row['差额'])} | {fmt_bool(row['是否通过'])} |")
lines.append("")

if cash_roll_check is not None:
    lines.append("## 三、现金滚动校验")
    lines.append("")
    lines.append("校验公式：`期初现金及现金等价物余额 + 本年净增加额 = 期末现金及现金等价物余额`")
    lines.append("")
    lines.append("| 年份 | 理论期末现金 | 报表期末现金 | 差额 | 结果 |")
    lines.append("|---|---:|---:|---:|---|")
    for y, row in cash_roll_check.iterrows():
        lines.append(f"| {y} | {fmt(row['理论期末现金'])} | {fmt(row['报表期末现金'])} | {fmt(row['差额'])} | {fmt_bool(row['是否通过'])} |")
    lines.append("")

if cash_link_check is not None:
    lines.append("## 四、现金流量表与资产负债表衔接")
    lines.append("")
    lines.append("观察公式：`资产负债表中的货币资金` 对比 `现金流量表中的期末现金及现金等价物余额`")
    lines.append("")
    lines.append("注意：两者**不一定严格相等**，因为货币资金可能包含受限资金、保证金、存出投资款等。")
    lines.append("")
    lines.append("| 年份 | 货币资金 | 期末现金及现金等价物 | 差额 |")
    lines.append("|---|---:|---:|---:|")
    for y, row in cash_link_check.iterrows():
        lines.append(f"| {y} | {fmt(row['资产负债表_货币资金'])} | {fmt(row['现金流量表_期末现金及现金等价物'])} | {fmt(row['差额'])} |")
    lines.append("")

if equity_link_check is not None:
    lines.append("## 五、利润表与资产负债表联动")
    lines.append("")
    lines.append("观察逻辑：`归母净利润` 应大体支持 `归母权益` 的增长，但不必严格相等。")
    lines.append("")
    lines.append("常见差异来源包括：分红、回购、增发、其他综合收益、少数股东权益变化等。")
    lines.append("")
    lines.append("| 年份 | 归母净利润 | 归母权益增加额 | 判断 |")
    lines.append("|---|---:|---:|---|")
    for y, row in equity_link_check.iterrows():
        lines.append(f"| {y} | {fmt(row['归母净利润'])} | {fmt(row['归母权益增加额'])} | {row['同向性检查']} |")
    lines.append("")

# 总结
bs_pass_n = int(bs_check["是否通过"].sum())
cf_pass_n = int(cf_check["是否通过"].sum())
lines.append("## 六、结论")
lines.append("")
lines.append(f"- 资产负债表平衡检查：**{bs_pass_n}/{len(bs_check)} 年通过**")
lines.append(f"- 现金流量表平衡检查：**{cf_pass_n}/{len(cf_check)} 年通过**")
if cash_roll_check is not None:
    cr_pass_n = int(cash_roll_check["是否通过"].sum())
    lines.append(f"- 现金滚动检查：**{cr_pass_n}/{len(cash_roll_check)} 年通过**")
lines.append("- 货币资金与期末现金及现金等价物的差异需要结合附注理解口径。")
lines.append("- 归母净利润与归母权益变动应做趋势联动分析，不应简单做机械相等。")
lines.append("")
lines.append("## 七、你以后复用这段代码时要改什么")
lines.append("")
lines.append("1. 把 `BS_FILE / CF_FILE / PL_FILE` 改成你的文件名。")
lines.append("2. 如果报错“找不到科目”，就在候选科目列表里补上你文件中的真实字段名。")
lines.append("3. 如果你希望放宽容差，把 `tol=1e3` 改成 `1e4` 或 `1e5`。")

Path(OUTPUT_MD).write_text("\n".join(lines), encoding="utf-8")

print(f"已生成：{OUTPUT_XLSX}")
print(f"已生成：{OUTPUT_MD}")
