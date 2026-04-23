# Automated Financial Data Analysis for A-shares

基于同花顺（RoyalFlush）导出数据，自动完成 A 股财务报表清洗、标准化、核心指标计算和 DCF 估值建模的全流程分析管道。

---

## 功能概览

- 将同花顺原始 XLS 报表转换为标准 CSV（保留最近 10 年）
- 五项三表勾稽一致性校验
- 投行口径财务指标计算（EBIT、EBITDA、FCF、ROE、CAGR 等）
- 资产负债表 / 利润表 / 现金流量表标准化重构（英文科目 + 分类体系）
- 自动生成可直接填写的 DCF + 相对估值 Excel 工作簿

---

## 项目结构

```
.
├── rawdata/                            原始同花顺导出文件（不入 Git）
│   ├── {ticker}_debt_year.xls          资产负债表
│   ├── {ticker}_benefit_year.xls       利润表
│   ├── {ticker}_cash_year.xls          现金流量表
│   ├── {ticker}_price.xls              年度价格数据
│   └── Info.csv                        公司基础信息（总股本、当前股价等）
│
├── results/                            管道输出（不入 Git）
│   ├── csv/                            Step 1 输出的标准化 CSV
│   ├── BS_rebuilt_output/              Step 5 资产负债表重构输出
│   ├── PL_rebuilt_output/              Step 6 利润表重构输出
│   ├── CF_rebuilt_output/              Step 7 现金流量表重构输出
│   └── valuation_output/               Step 8 DCF 估值模型
│
├── step1_convert_xls_to_csv.py         Step 1: XLS → CSV 转换
├── step2_check_statements.py           Step 2: 三表一致性检验
├── step3_extract_metrics.py            Step 3: 核心财务指标提取（基础版）
├── step4_metrics_report.py             Step 4: 完整财务指标报告（增强版）
├── rebuild_balance_sheet.py            Step 5: 资产负债表标准化重构
├── rebuild_income_statement.py         Step 6: 利润表标准化重构
├── rebuild_cash_flow.py                Step 7: 现金流量表标准化重构
├── generate_dcf_valuation.py           Step 8: DCF 估值模型生成
├── pipeline_utils.py                   公共工具函数
├── run_pipeline.py                     一键执行全部步骤
└── pyproject.toml                      项目依赖声明
```

---

## 快速开始

### 1. 环境安装

本项目使用 [uv](https://github.com/astral-sh/uv) 管理依赖：

```bash
uv venv --python 3.10
source .venv/bin/activate   # Windows: .venv\Scripts\activate
uv sync
```

依赖包括：`pandas`、`numpy`、`openpyxl`、`xlrd`、`tabulate`、`matplotlib`、`jupyter`、`ipykernel`。

### 2. 准备数据

将同花顺导出的四个 XLS 文件放入 `rawdata/` 目录，文件名格式为 `{ticker}_{type}.xls`，例如：

```
rawdata/
├── 600406_debt_year.xls
├── 600406_benefit_year.xls
├── 600406_cash_year.xls
├── 600406_price.xls
└── Info.csv
```

`Info.csv` 需包含以下字段（第一列为"项目"，最后一列为数值）：

| 项目 | 值 |
|---|---|
| 总股本 | 1234567890 |
| 当前股价 | 12.34 |
| 公司简称 | 国电南瑞 |

> 每次只放一只股票的数据，管道会自动从文件名识别股票代码。

### 3. 运行管道

```bash
python run_pipeline.py
```

也可以单独运行某一步骤：

```bash
python step1_convert_xls_to_csv.py   # 仅执行 XLS → CSV 转换
python step2_check_statements.py     # 仅执行三表一致性检验
# ...以此类推
```

---

## 管道步骤详解

### Step 1 — `step1_convert_xls_to_csv.py`
读取同花顺导出的 XLS 文件，清洗占位符（`--` → 0）、修正年份列名，保留最近 10 年数据并按时间升序排列，输出四个标准 CSV 至 `results/csv/`。

### Step 2 — `step2_check_statements.py`
对三张报表执行五项勾稽校验：资产负债表恒等式、现金流量表平衡、现金滚动、货币资金对比（观察性）、净利润与权益联动（趋势性）。输出 Excel 结果表和 Markdown 报告。

### Step 3 — `step3_extract_metrics.py`
从三表中提取关键科目，计算投行常用建模指标（Revenue、EBIT 建模法 / 报表校验法、EBITDA、FCF 等），附口径说明。输出 `results/Core_Metrics.xlsx`（3 个 Sheet）。

### Step 4 — `step4_metrics_report.py`
在 Step 3 基础上新增 YoY 增速、CAGR、ROE、资产负债率、CFO 含金量等指标，并自动生成 Markdown 分析报告和缺失科目日志。输出 `results/financial_core_metrics_plus.xlsx`（6 个 Sheet）。

### Step 5 — `rebuild_balance_sheet.py`
将原始资产负债表按投行口径重分类（现金类 / 核心经营类 / 有息债务 / 无息负债 / 权益），输出标准化长表、宽表、估值输入底稿和科目映射追溯体系至 `results/BS_rebuilt_output/`。

### Step 6 — `rebuild_income_statement.py`
将原始利润表按投行口径重分类（Revenue → Gross Profit → EBIT → Net Profit），输出标准化长表、宽表及估值输入指标至 `results/PL_rebuilt_output/`。

### Step 7 — `rebuild_cash_flow.py`
将原始现金流量表重分类（经营 / 投资 / 筹资 / 间接法桥接），执行六项一致性校验，输出标准化长表、宽表及估值输入指标至 `results/CF_rebuilt_output/`。

### Step 8 — `generate_dcf_valuation.py`
读取三表标准化输出，自动计算历史驱动因子（增速 Seed、EBIT Margin、D&A%、CapEx%、NWC%），生成包含 10 个联动 Sheet 的 DCF + 相对估值 Excel 工作簿至 `results/valuation_output/`。黄色单元格为可编辑假设，绿色为公式联动结果。

---

## 主要输出文件

| 文件 | 说明 |
|---|---|
| `results/csv/*.csv` | 清洗后的标准化三表和价格数据 |
| `results/三表一致性检验结果.xlsx` | 五项勾稽校验结果 |
| `results/Core_Metrics.xlsx` | 核心财务指标（基础版） |
| `results/financial_core_metrics_plus.xlsx` | 完整财务指标报告（含 CAGR / ROE 等） |
| `results/BS_rebuilt_output/5_valuation_ready_bs.xlsx` | 资产负债表估值底稿 |
| `results/PL_rebuilt_output/5_valuation_ready_pl.xlsx` | 利润表估值底稿 |
| `results/CF_rebuilt_output/5_valuation_ready_cf.xlsx` | 现金流量表估值底稿 |
| `results/valuation_output/DCF_valuation_model.xlsx` | DCF + 相对估值模型 |

---

## 常见问题

**Q：报错"找不到科目"怎么办？**  
A：查看 `results/missing_items_log.csv`，对照同花顺实际导出的字段名，在对应脚本的候选科目列表（`candidates`）中补充即可。

**Q：同花顺不同版本导出的字段名不一样？**  
A：各步骤均已针对常见变体做多候选名兼容处理（如带 `*` 前缀、带 / 不带括号等）。若仍不匹配，按上条处理。

**Q：DCF 模型的假设需要手动调整吗？**  
A：Step 8 会根据历史数据自动生成初始假设，但建议结合行业研究和公司指引，在 Excel 的黄色单元格中进行校准。

---

## 依赖版本

见 `pyproject.toml`：

```toml
requires-python = ">=3.10"
dependencies = [
    "pandas>=2.0", "numpy>=1.24", "openpyxl>=3.1",
    "xlrd>=2.0.1", "tabulate>=0.9", "matplotlib>=3.7",
    "jupyter>=1.0", "ipykernel>=6.0",
]
```
