# Automated Financial Data Analysis for A-shares

面向 A 股非金融上市公司的单公司财务数据清洗、三表重构、核心指标计算、DCF/相对估值建模与离线 HTML 看板生成工具。

项目当前以 `demo/` 作为默认样例数据目录和后续测试目录：同花顺导出的年度三表、`Info.csv`、流水线输出结果都会围绕 `demo/` 组织。不再保留单独的 `rawdata/` 目录。

> 本项目用于研究、建模和复核，不构成投资建议。DCF 假设、相对估值倍数、AI 分析 memo 和目标价区间都需要结合公司公告、行业研究与人工判断复核。

## 核心能力

- 读取同花顺 PC 端导出的年度 XLS 财务报表。
- 将资产负债表、利润表、现金流量表转换为标准 CSV。
- 对三张报表做基础一致性检查，并输出 Excel/Markdown 校验报告。
- 提取收入、EBIT、EBITDA、CFO、FCF、ROE、CAGR、资产负债率等核心财务指标。
- 重构三张报表，生成标准化长表、宽表、映射明细和估值可用 Excel。
- 对重构后的 BS/PL/CF 做独立校验。
- 基于历史数据和配置假设生成 DCF + 相对估值 Excel 工作簿。
- 生成可离线打开的 `financial_dcf_dashboard.html`，支持查看历史财务、估值过程和关键假设。
- 可选接入 DeepSeek，生成中文财务分析 memo。

## 适用范围

当前版本更适合：

- 制造、消费、科技硬件、一般服务业等非金融类工商企业。
- 需要把同花顺历史三表快速整理成估值模型底稿的研究场景。
- 个人投资者、财务分析师、买方/投行研究人员，以及具备 Python 基础的量化研究用户。

需要谨慎使用或另建模型的场景：

- 银行、保险、券商等金融股：普通 FCFF DCF 不适用，应使用 PB、DDM、ROE-COE 等框架。
- 强周期公司：应使用中周期利润和中周期倍数，不能简单外推最近一年景气利润。
- 房地产、多业务集团、强 SOTP 公司：通常需要先拆分业务或资产后单独估值。

## 项目结构

```text
.
├── afda/                         # 核心 Python 包与流水线脚本
│   ├── run_pipeline.py           # 一键流水线入口
│   ├── input_validation.py       # 输入文件与 Info.csv 校验
│   ├── pipeline_utils.py         # 路径、ticker 识别、输出目录工具
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
│   ├── generate_html_report.py
│   └── valuation_config.py
├── assets/                       # HTML 看板静态资源
├── configs/
│   ├── default_valuation.json    # 默认 DCF、相对估值、敏感性参数
│   └── industry_profiles.json    # 行业估值框架说明
├── demo/                         # 样例数据与后续测试目录
│   ├── 002311_debt_year.xls
│   ├── 002311_benefit_year.xls
│   ├── 002311_cash_year.xls
│   ├── Info.csv
│   └── results/                  # demo 流水线输出
├── tests/                        # 当前单元测试
├── run_pipeline.py               # 兼容旧命令的薄入口
├── pyproject.toml
├── requirements.txt
└── README.md
```

流水线运行后，会在输入目录下生成独立的 `results/`。使用 `demo/` 时，输出路径为 `demo/results/`：

```text
demo/results/
├── 01_csv/
├── 02_checks/
├── 03_metrics/
├── 04_rebuilt_statements/
│   ├── balance_sheet/
│   ├── income_statement/
│   ├── cash_flow/
│   └── rebuilt_statement_checks/
├── 05_valuation/
├── _assets/
├── financial_dcf_dashboard.html
└── rebuilt_statement_deepseek_analysis.md
```

## 环境安装

推荐 Python 3.10+。

使用 pip：

```bash
pip install -r requirements.txt
```

使用 uv：

```bash
uv venv --python 3.10
uv sync
```

也可以安装为本地命令：

```bash
pip install -e .
afda-run demo
```

## 输入数据规范

一个输入目录只放一只股票的数据。当前项目默认使用 `demo/`，也可以通过命令行指定其他目录。

必需文件命名格式：

```text
600406_debt_year.xls       # 资产负债表，必需
600406_benefit_year.xls    # 利润表，必需
600406_cash_year.xls       # 现金流量表，必需
Info.csv                   # 估值和 HTML 看板需要
```

可选文件：

```text
600406_price.xls           # 股价数据，可选
valuation_config.json      # 当前公司专用估值假设，可选
```

三张财务报表要求：

- 文件来自同花顺年度财务数据导出。
- 第一张 sheet 为年度报表数据。
- 第一列为报表科目，后续列为年份。
- 同一目录不能混放多只股票，否则 ticker 识别会失败。
- `Info.csv` 缺失时，流水线仍会完成三表转换、指标计算和三表重构，但会跳过 DCF Excel 与 HTML 看板。

`Info.csv` 最小示例：

```csv
项目,值
公司名称,海大集团
公司代码,002311
总股本,1663749970
当前股价,45.80
```

必需字段：

| 字段 | 说明 |
|---|---|
| `总股本` | 用于把股东权益价值转换为每股内在价值，必须为正数 |
| `当前股价` | 用于计算上涨空间和安全边际，必须为正数 |

推荐字段：

| 字段 | 说明 |
|---|---|
| `公司名称` | 用于 Excel、Markdown 和 HTML 展示 |
| `公司代码` | 与公司名称组合显示为 `公司名称（公司代码）` |
| `公司简称` | 兼容旧格式；没有 `公司名称` 时作为展示名称 |

## 一键运行

使用项目自带 demo 数据：

```bash
python -m afda.run_pipeline demo
```

兼容旧入口：

```bash
python run_pipeline.py demo
```

如果安装了本地命令：

```bash
afda-run demo
```

指定其他输入目录：

```bash
python -m afda.run_pipeline "D:/path/to/export-folder"
python -m afda.run_pipeline --data-dir "D:/path/to/export-folder"
```

不传路径时，程序会尝试弹出文件夹选择窗口；如果当前环境无法弹窗，会回退到命令行输入路径。

完整流水线顺序：

| Step | 模块 | 主要输出 |
|---:|---|---|
| 1 | `afda.step1_convert_xls_to_csv` | `results/01_csv/` |
| 2 | `afda.step2_check_statements` | `results/02_checks/` |
| 3 | `afda.step3_extract_metrics` | `results/03_metrics/` |
| 4 | `afda.step4_metrics_report` | `results/03_metrics/` |
| 5 | `afda.rebuild_balance_sheet` | `results/04_rebuilt_statements/balance_sheet/` |
| 6 | `afda.rebuild_income_statement` | `results/04_rebuilt_statements/income_statement/` |
| 7 | `afda.rebuild_cash_flow` | `results/04_rebuilt_statements/cash_flow/` |
| 8 | `afda.validate_rebuilt_statements` | `results/04_rebuilt_statements/rebuilt_statement_checks/` |
| 9 | `afda.analyze_rebuilt_statements` | `results/rebuilt_statement_deepseek_analysis.md` |
| 10 | `afda.generate_dcf_valuation` | `results/05_valuation/DCF_valuation_model.xlsx` |
| 11 | `afda.generate_html_report` | `results/financial_dcf_dashboard.html` |

## 主要输出

| 文件 | 说明 |
|---|---|
| `results/01_csv/bs.csv` | 资产负债表 CSV |
| `results/01_csv/pl.csv` | 利润表 CSV |
| `results/01_csv/cf.csv` | 现金流量表 CSV |
| `results/02_checks/statement_consistency_checks.xlsx` | 三表勾稽校验 Excel |
| `results/02_checks/statement_consistency_report.md` | 三表勾稽校验报告 |
| `results/03_metrics/Core_Metrics.xlsx` | 基础核心财务指标 |
| `results/03_metrics/financial_core_metrics_plus.xlsx` | 增强版指标、图表和模型宽表 |
| `results/03_metrics/financial_core_metrics_report.md` | 财务指标分析报告 |
| `results/03_metrics/missing_items_log.csv` | 缺失科目日志 |
| `results/04_rebuilt_statements/*/2_standardized_*.csv` | 标准化重构长表 |
| `results/04_rebuilt_statements/*/2_standardized_*_wide.csv` | 标准化重构宽表 |
| `results/04_rebuilt_statements/*/5_valuation_ready_*.xlsx` | 估值输入底稿 |
| `results/04_rebuilt_statements/rebuilt_statement_checks/rebuilt_statement_checks.xlsx` | 重构后三表校验明细 |
| `results/04_rebuilt_statements/rebuilt_statement_checks/rebuilt_statement_checks_report.md` | 重构后三表校验报告 |
| `results/rebuilt_statement_deepseek_analysis.md` | DeepSeek 三表分析 memo 或跳过原因 |
| `results/05_valuation/DCF_valuation_model.xlsx` | DCF + 相对估值 Excel 工作簿 |
| `results/financial_dcf_dashboard.html` | 离线 HTML 财务与 DCF 看板 |

## 估值配置

默认估值参数位于 `configs/default_valuation.json`：

```json
{
  "industry_profile": "general_industrial",
  "dcf": {
    "wacc": 0.10,
    "terminal_growth": 0.03,
    "dcf_weight": 0.60,
    "relative_weight": 0.40
  }
}
```

如需覆盖某只公司的假设，在输入目录中放置 `valuation_config.json`。程序会先读取项目默认配置，再用输入目录内的本地配置覆盖对应字段。

示例：

```json
{
  "dcf": {
    "wacc": 0.12,
    "terminal_growth": 0.025
  },
  "relative_valuation": {
    "multiples": {
      "PE": {"low": 10, "mid": 12, "high": 14}
    }
  }
}
```

## DeepSeek 分析

DeepSeek 是可选能力。未配置时，核心数据处理、三表重构、DCF Excel 和 HTML 看板仍可正常生成。

复制 `.env.example` 为 `.env` 后填写：

```dotenv
ENABLE_DEEPSEEK_ANALYSIS=1
DEEPSEEK_API_KEY=your_deepseek_api_key_here
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-v4-flash
DEEPSEEK_TIMEOUT=120
```

启用后，`afda.analyze_rebuilt_statements` 会读取重构后的标准化 BS/PL/CF，生成：

```text
results/rebuilt_statement_deepseek_analysis.md
```

AI memo 只基于输入财务数据和模型结果，不会自动获取公告、行业新闻或实时市场数据。

## 测试

当前单元测试：

```bash
python -m unittest discover -s tests
```

端到端样例测试以 `demo/` 为准：

```bash
python -m afda.run_pipeline demo
```

后续新增测试数据、测试输出和人工复核样例，统一放在 `demo/` 目录体系内实现。

## 常见问题

| 问题 | 处理方式 |
|---|---|
| 提示多个 ticker | 清理输入目录，保证只保留一只股票的同花顺导出文件 |
| 提示缺少 `Info.csv` | 可继续跑三表和指标；如需 DCF/HTML，请补充 `Info.csv` |
| 提示 `总股本` 或 `当前股价` 不合法 | 检查 `Info.csv` 是否为正数，去掉逗号、单位或额外文字 |
| XLS 读取失败 | 确认文件来自同花顺年度导出，且未被 Excel 加密或损坏 |
| DCF 结果异常 | 优先复核收入增速、EBIT 率、Capex、NWC、WACC、永续增长率和股本单位 |

## 当前局限

- 当前以单公司、单目录为主，不是多公司批量估值系统。
- 相对估值倍数来自配置文件，不会自动抓取可比公司估值中枢。
- DCF 默认假设由历史数据和配置外推，不能替代行业研究和公司经营预测。
- 金融股、强周期股、SOTP 公司仍需要人工调整估值框架。

## Roadmap

- 批量估值：支持多 ticker 子目录独立运行并汇总结果。
- 行业模板：按金融、周期、成长、公用事业、SOTP 自动切换估值框架。
- 可比公司：接入手动 comps 表或外部数据源。
- 情景分析：增加 Base/Bull/Bear 三情景和概率加权目标价。
- Dashboard：升级为更完整的 Web Dashboard 或多智能体研究系统。
