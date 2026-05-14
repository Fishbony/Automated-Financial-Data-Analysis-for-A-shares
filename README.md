# Automated Financial Data Analysis for A-shares

面向 A 股非金融上市公司的单公司财务报表清洗、指标计算与估值建模工具。项目读取同花顺 PC 端导出的年度财务数据，自动完成三张报表转换、勾稽校验、标准化重构、核心财务指标计算、DCF/相对估值建模，并生成 Excel、Markdown 和可离线浏览的 HTML Dashboard。

> 本项目用于研究、建模和复核，不构成投资建议。DCF、相对估值倍数和 AI memo 均需要结合行业、公司公告和人工判断复核。

## 适用范围

当前版本最适合：

- 制造、消费、科技硬件、一般服务业等非金融类工商企业
- 需要把同花顺导出的历史三表快速整理为估值模型底稿的研究场景
- 个人投资者、财务分析师、买方/投行研究员和具备 Python 基础的量化研究者

需要谨慎使用或另建模型：

- 银行、保险、券商等金融股：普通 FCFF DCF 不适用，应使用 PB、DDM、ROE-COE 等框架
- 周期股：应使用中周期利润和中周期倍数，不能简单外推最近一年景气利润
- 地产、多业务集团、强 SOTP 公司：需要拆分业务或资产后单独估值

## 核心能力

- 将同花顺年度 XLS 文件转换为标准 CSV。
- 对资产负债表、利润表、现金流量表做一致性和勾稽校验。
- 重构三张报表，输出可追溯的标准化长表、宽表和 Excel 底稿。
- 对 rebuild 后的 `standardized_*.csv` 做独立内部校验，检查 BS、PL、CF 各自的合计、桥接和现金滚动关系。
- 计算 Revenue、EBIT、EBITDA、CFO、FCF、ROE、CAGR、资产负债率、CFO/净利润等核心指标。
- 自动生成 DCF + 相对估值 Excel 工作簿，包含 PE、PB、PS、EV/EBIT、EV/EBITDA。
- 支持 WACC、永续增长率、收入增速、利润率、税率、D&A、Capex、NWC 等假设输入。
- 生成 WACC / 永续增长率敏感性矩阵、目标价区间、安全边际和风险提示。
- 对 rebuild 后的资产负债表、利润表、现金流量表进行 DeepSeek AI 分析，输出三表财务画像、关注问题和潜在雷点。
- 自动生成离线 HTML Dashboard，支持在浏览器中编辑预测假设并实时重算每股内在价值。
- 可选接入 DeepSeek，生成中文财务分析 memo。

## 项目结构

```text
.
├── demo/
│   └── rawdata/                   # 示例同花顺导出数据，可直接试跑
├── assets/                        # 离线 HTML 静态资源
├── afda/                          # 核心 Python 包与流水线脚本
│   ├── input_validation.py        # 输入文件和 Info.csv 校验
│   ├── valuation_config.py        # 估值配置加载
│   ├── run_pipeline.py            # 一键流水线入口
│   ├── step1_convert_xls_to_csv.py
│   ├── step2_check_statements.py
│   ├── step3_extract_metrics.py
│   ├── step4_metrics_report.py
│   ├── rebuild_balance_sheet.py
│   ├── rebuild_income_statement.py
│   ├── rebuild_cash_flow.py
│   ├── validate_rebuilt_statements.py
│   ├── analyze_rebuilt_statements.py
│   ├── generate_dcf_valuation.py
│   └── generate_html_report.py
├── configs/
│   ├── default_valuation.json     # DCF、相对估值、敏感性默认参数
│   └── industry_profiles.json     # 行业估值框架说明
├── results/                       # 默认输出目录
├── run_pipeline.py                # 兼容旧命令的薄入口
├── pyproject.toml
└── requirements.txt
```

运行后，每个输入目录下会生成独立 `results/`：

```text
results/
├── 01_csv/
├── 02_checks/
├── 03_metrics/
├── 04_rebuilt_statements/
│   ├── balance_sheet/
│   ├── income_statement/
│   └── cash_flow/
├── 05_valuation/
└── _assets/
```

## 环境安装

推荐 Python 3.10+。

```bash
pip install -r requirements.txt
```

如果使用 `uv`：

```bash
uv venv --python 3.10
uv sync
```

## 输入数据规范

一个输入目录中只放一只股票的数据。项目自带示例目录为 `demo/rawdata/`，你也可以使用任意外部目录并通过 `--data-dir` 指定。

文件名需保持同花顺导出格式：

```text
600406_debt_year.xls       # 资产负债表，必需
600406_benefit_year.xls    # 利润表，必需
600406_cash_year.xls       # 现金流量表，必需
600406_price.xls           # 股价数据，可选
Info.csv                   # DCF / HTML 必需
```

三张财务报表要求：

- 第一张 sheet 为年度报表数据。
- 第一列为报表科目，后续列为年份。
- 金额单位保持同花顺导出原始单位，通常为元。
- 缺失值可为 `--`，脚本会在转换阶段处理。
- 同一目录不能混放多只股票，否则 ticker 识别会失败。

`Info.csv` 最小示例：

```csv
项目,值
公司简称,国电南瑞
总股本,6694533993
当前股价,23.50
```

必需字段：

| 字段 | 说明 |
|---|---|
| `总股本` | 用于把股东权益价值转换为每股内在价值，必须为正数 |
| `当前股价` | 用于计算上涨空间和安全边际，必须为正数 |

推荐字段：

| 字段 | 说明 |
|---|---|
| `公司简称` | 用于 Excel 和 HTML 展示 |

没有 `Info.csv` 时，流水线仍会完成报表转换、指标计算和三表重构，但会跳过 DCF 与 HTML 看板。

## 一键运行

使用示例数据快速运行：

```bash
python -m afda.run_pipeline demo/rawdata
```

兼容旧命令：

```bash
python run_pipeline.py demo/rawdata
```

如果系统没有 `python` 命令，可使用 `python3` 或虚拟环境中的 `.venv/bin/python`。

不传路径时会弹出文件夹选择窗口，默认从 `demo/rawdata/` 附近开始选择。

直接传入数据目录：

```bash
python -m afda.run_pipeline "D:/path/to/export-folder"
python -m afda.run_pipeline --data-dir "D:/path/to/export-folder"
```

流水线会先执行输入校验。如果发现 `Info.csv` 缺字段、数值不可解析、多个 ticker 混放或必需报表缺失，会在正式运行前给出错误或警告。

完整流程：

| Step | 脚本 | 输出目录 |
|---:|---|---|
| 1 | `afda.step1_convert_xls_to_csv` | `results/01_csv/` |
| 2 | `afda.step2_check_statements` | `results/02_checks/` |
| 3 | `afda.step3_extract_metrics` | `results/03_metrics/` |
| 4 | `afda.step4_metrics_report` | `results/03_metrics/` |
| 5 | `afda.rebuild_balance_sheet` | `results/04_rebuilt_statements/balance_sheet/` |
| 6 | `afda.rebuild_income_statement` | `results/04_rebuilt_statements/income_statement/` |
| 7 | `afda.rebuild_cash_flow` | `results/04_rebuilt_statements/cash_flow/` |
| 8 | `afda.validate_rebuilt_statements` | `results/rebuilt_statement_checks/` |
| 9 | `afda.analyze_rebuilt_statements` | `results/rebuilt_statement_deepseek_analysis.md` |
| 10 | `afda.generate_dcf_valuation` | `results/05_valuation/` |
| 11 | `afda.generate_html_report` | `results/05_valuation/` |

## 单独生成 DCF 与 HTML

在前置报表重构结果已存在时，可以单独运行：

```bash
python -m afda.generate_dcf_valuation --data-dir "D:/path/to/export-folder"
python -m afda.generate_html_report --data-dir "D:/path/to/export-folder"
```

HTML 输出路径：

```text
results/05_valuation/financial_dcf_dashboard.html
```

HTML 看板包含：

- `财务基本情况`：历史 Revenue、EBIT、归母净利润、CFO、FCFF Proxy 和核心财务数据表。
- `估值过程与假设联动`：可编辑 Assumptions、预测期明细、DCF Bridge、目标价、上涨空间和安全边际。

## 估值配置

默认估值参数位于 `configs/default_valuation.json`：

```json
{
  "dcf": {
    "wacc": 0.10,
    "terminal_growth": 0.03,
    "dcf_weight": 0.60,
    "relative_weight": 0.40
  }
}
```

如果某家公司需要单独覆盖参数，可在该公司的输入目录放置 `valuation_config.json`，字段结构与默认配置一致。脚本会先读取项目默认配置，再用输入目录内的本地配置覆盖。

行业估值框架说明位于 `configs/industry_profiles.json`。当前配置仅作为方法提示，尚未自动切换模型公式；金融股、周期股、SOTP 公司仍需要人工调整估值框架。

## 主要输出

| 文件 | 说明 |
|---|---|
| `results/01_csv/*.csv` | 原始 XLS 转换后的三表和价格 CSV |
| `results/02_checks/statement_consistency_checks.xlsx` | 三表勾稽校验 |
| `results/02_checks/statement_consistency_report.md` | 勾稽校验报告 |
| `results/03_metrics/Core_Metrics.xlsx` | 基础核心财务指标 |
| `results/03_metrics/financial_core_metrics_plus.xlsx` | 增强版指标、图表和模型宽表 |
| `results/03_metrics/financial_core_metrics_report.md` | 财务指标分析报告 |
| `results/03_metrics/missing_items_log.csv` | 缺失科目日志 |
| `results/04_rebuilt_statements/*/5_valuation_ready_*.xlsx` | 三表估值输入底稿 |
| `results/rebuilt_statement_checks/rebuilt_statement_checks.xlsx` | Rebuild 后 BS/PL/CF 标准化 CSV 的独立校验明细 |
| `results/rebuilt_statement_checks/rebuilt_statement_checks_report.md` | Rebuild 后三表独立校验报告 |
| `results/rebuilt_statement_deepseek_analysis.md` | DeepSeek 对 rebuild 后三表的简要分析、关注问题和潜在雷点 |
| `results/05_valuation/DCF_valuation_model.xlsx` | DCF + 相对估值 Excel 工作簿 |
| `results/05_valuation/financial_dcf_dashboard.html` | 财务与 DCF 可视化 HTML 看板 |

## 当前局限

- 目前以单公司单目录为主，不是多公司批量估值系统。
- 相对估值倍数来自配置文件，不会自动抓取同行公司估值中枢。
- DCF 默认假设由历史数据外推，不能替代行业研究和公司经营预测。
- 尚未覆盖 DDM、SOTP、金融股专用模型和完整 Base/Bull/Bear 三情景。
- 运营效率指标和 ROIC 已列入后续增强方向。

## Roadmap

- 批量估值：支持 `--batch-dir`，每个 ticker 子目录独立运行并汇总结果。
- 行业模板：按金融、周期、成长、公用事业、SOTP 自动切换估值框架。
- 可比公司：接入手动 comps 表或 Tushare/AkShare/Wind/Choice 数据源。
- 情景分析：增加 Base/Bull/Bear 三情景和概率加权目标价。
- 测试体系：覆盖科目映射、报表校验、FCFF、DCF、敏感性分析。
- Dashboard：升级为 Web Dashboard 或多智能体研究系统。

## DeepSeek 分析

复制 `.env.example` 为 `.env` 后填写：

```dotenv
ENABLE_DEEPSEEK_ANALYSIS=1
DEEPSEEK_API_KEY=your_deepseek_api_key
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-v4-flash
DEEPSEEK_TIMEOUT=120
```

DeepSeek 仅用于补充中文分析文本；未配置时，核心数据处理、DCF Excel 与 HTML 看板仍可正常生成。AI memo 只基于输入财务数据和模型结果，不会自动获取公告、行业新闻或实时市场数据。

`afda.analyze_rebuilt_statements` 会在三表重构后读取标准化 BS/PL/CF，生成：

```text
results/rebuilt_statement_deepseek_analysis.md
```

报告会要求 DeepSeek 回答：这组财务数据代表什么类型的公司、三表分别反映什么经营特征、哪些问题需要关注、哪些数据可能是雷点、下一步应查什么原始资料。未启用 DeepSeek 时，该文件会写入跳过原因，流水线不会失败。

## 常见问题

| 问题 | 处理方式 |
|---|---|
| 提示多个 ticker | 清理输入目录，保证只保留一只股票的同花顺导出文件 |
| 提示缺少 `Info.csv` | 可继续跑三表和指标；若要生成 DCF/HTML，请补充 `Info.csv` |
| 提示 `总股本` 或 `当前股价` 不合法 | 检查 `Info.csv` 是否为正数，去掉逗号或单位文字 |
| XLS 读取失败 | 确认文件来自同花顺年度导出，且未被 Excel 加密或损坏 |
| DCF 结果异常 | 优先复核收入增速、EBIT率、Capex、NWC、WACC、永续增长率和股本单位 |
