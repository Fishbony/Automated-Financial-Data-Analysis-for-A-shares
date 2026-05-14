# Automated Financial Data Analysis for A-shares

基于同花顺导出的 A 股财务数据，本项目自动完成三张报表清洗、标准化重构、核心财务指标计算、DCF 估值建模，并生成可离线浏览的 HTML 财务与估值看板。

## 核心能力

- 将同花顺 PC 端导出的年度 XLS 文件转换为标准 CSV。
- 对资产负债表、利润表、现金流量表做勾稽校验。
- 提取 Revenue、EBIT、EBITDA、CFO、FCF、ROE、CAGR 等核心指标。
- 重构三张报表，输出可用于建模的标准化长表、宽表和 Excel 底稿。
- 自动生成 DCF + 相对估值 Excel 工作簿。
- 自动生成 HTML Dashboard，用本地离线 ECharts 资源展示历史趋势、预测期 FCFF 与 DCF 估值结果。
- HTML 包含两个界面：财务基本情况、估值过程与假设联动；可在浏览器里编辑预测假设并实时重算每股内在价值。
- 可选接入 DeepSeek，生成中文财务分析 memo。

## 输出目录

`results/` 会按阶段分区，避免文件堆在根目录：

```text
results/
├── 01_csv/                         # Step 1: 三表 CSV 与价格 CSV
├── 02_checks/                      # Step 2: 三表一致性校验
├── 03_metrics/                     # Step 3-4: 核心指标、分析报告、缺失项日志
├── 04_rebuilt_statements/
│   ├── balance_sheet/              # Step 5: 资产负债表重构
│   ├── income_statement/           # Step 6: 利润表重构
│   └── cash_flow/                  # Step 7: 现金流量表重构
├── 05_valuation/                   # Step 8-9: DCF Excel 与 HTML 看板
└── _assets/                        # HTML 离线静态资源，如 echarts.min.js
```

项目根目录保留脚本与默认输入目录：

```text
.
├── rawdata/
│   ├── {ticker}_debt_year.xls
│   ├── {ticker}_benefit_year.xls
│   ├── {ticker}_cash_year.xls
│   ├── {ticker}_price.xls          # 可选
│   └── Info.csv                    # DCF / HTML 所需
├── assets/
│   └── echarts.min.js              # 离线 HTML 图表资源
├── run_pipeline.py
├── generate_dcf_valuation.py
├── generate_html_report.py
└── ...
```

## 环境安装

推荐使用 Python 3.10+。

```bash
pip install -r requirements.txt
```

如果使用 `uv`：

```bash
uv venv --python 3.10
uv sync
```

## 输入数据

一个数据目录中应只放一只股票的数据，文件名需保持同花顺导出格式：

```text
600406_debt_year.xls
600406_benefit_year.xls
600406_cash_year.xls
600406_price.xls      # 可选
Info.csv              # DCF / HTML 所需
```

没有 `Info.csv` 时，流水线仍会完成报表清洗、指标计算和三表重构，但会跳过 DCF 与 HTML 看板。

## 一键运行

交互式输入数据目录：

```bash
python run_pipeline.py
```

不传路径时会自动弹出系统文件夹选择窗口，请选择包含同花顺导出文件的输入文件夹；如果当前环境无法打开图形窗口，会退回到命令行路径输入。

直接传入数据目录：

```bash
python run_pipeline.py "D:/path/to/export-folder"
python run_pipeline.py --data-dir "D:/path/to/export-folder"
```

完整流程包含：

| Step | 脚本 | 输出目录 |
|---:|---|---|
| 1 | `step1_convert_xls_to_csv.py` | `results/01_csv/` |
| 2 | `step2_check_statements.py` | `results/02_checks/` |
| 3 | `step3_extract_metrics.py` | `results/03_metrics/` |
| 4 | `step4_metrics_report.py` | `results/03_metrics/` |
| 5 | `rebuild_balance_sheet.py` | `results/04_rebuilt_statements/balance_sheet/` |
| 6 | `rebuild_income_statement.py` | `results/04_rebuilt_statements/income_statement/` |
| 7 | `rebuild_cash_flow.py` | `results/04_rebuilt_statements/cash_flow/` |
| 8 | `generate_dcf_valuation.py` | `results/05_valuation/` |
| 9 | `generate_html_report.py` | `results/05_valuation/` |

## 单独生成 DCF 与 HTML

在前置报表重构结果已经存在时，可以单独运行：

```bash
python generate_dcf_valuation.py --data-dir "D:/path/to/export-folder"
python generate_html_report.py --data-dir "D:/path/to/export-folder"
```

HTML 输出路径：

```text
results/05_valuation/financial_dcf_dashboard.html
```

HTML 看板分为两个 Tab：

- `财务基本情况`：展示历史收入、EBIT、归母净利润、CFO、FCFF Proxy 和核心财务数据表。
- `估值过程与假设联动`：展示可编辑 Assumptions、预测期明细、DCF Bridge、目标价和上涨空间。修改 Revenue Growth、EBIT Margin、Tax Rate、D&A、Capex、NWC、WACC、永续增长率等输入后，页面会即时重算估值股价。

HTML 会引用：

```text
results/_assets/echarts.min.js
```

`generate_html_report.py` 会自动把项目内的 `assets/echarts.min.js` 复制到 `results/_assets/`，因此离线环境也能打开图表。若你想替换为官方完整 ECharts，只需要把官方 `echarts.min.js` 放到项目 `assets/` 目录下，再重新运行 HTML 生成脚本。

## 主要输出

| 文件 | 说明 |
|---|---|
| `results/01_csv/*.csv` | 原始 XLS 转换后的三表 CSV |
| `results/02_checks/statement_consistency_checks.xlsx` | 报表勾稽校验 |
| `results/02_checks/statement_consistency_report.md` | 勾稽校验报告 |
| `results/03_metrics/Core_Metrics.xlsx` | 基础核心财务指标 |
| `results/03_metrics/financial_core_metrics_plus.xlsx` | 增强版核心指标、图表和模型宽表 |
| `results/03_metrics/financial_core_metrics_report.md` | 财务指标分析报告 |
| `results/03_metrics/missing_items_log.csv` | 缺失项日志 |
| `results/04_rebuilt_statements/*/5_valuation_ready_*.xlsx` | 三表估值输入底稿 |
| `results/05_valuation/DCF_valuation_model.xlsx` | DCF + 相对估值 Excel 工作簿 |
| `results/05_valuation/financial_dcf_dashboard.html` | 财务与 DCF 可视化 HTML 看板 |

## DeepSeek 分析

复制 `.env.example` 为 `.env` 后填写：

```dotenv
ENABLE_DEEPSEEK_ANALYSIS=1
DEEPSEEK_API_KEY=your_deepseek_api_key
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-v4-flash
DEEPSEEK_TIMEOUT=120
```

DeepSeek 仅用于补充中文分析文本；未配置时，核心数据处理、DCF Excel 与 HTML 看板仍可正常生成。

## 注意事项

- 同一输入目录中只保留一只股票的数据，否则 ticker 识别会失败。
- `price.xls` 是可选文件；`Info.csv` 对 DCF 与 HTML 看板是必要文件。
- DCF 默认假设来自历史数据推导，可在 Excel 的 `Assumptions` sheet 中手动调整。
- 本项目输出用于研究和建模复核，不构成投资建议。
