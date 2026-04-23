"""
Step 1/8 — 同花顺原始 XLS 转 CSV
==================================
将同花顺（RoyalFlush）导出的 .xls 格式财务报表与价格数据，
清洗并转换为后续管道所需的标准 CSV 文件。

处理逻辑：
- 财务报表（资产负债表 / 利润表 / 现金流量表）：
    * 去除占位符 '--'，替换为 0
    * 修正年份列名（去除 .0 后缀）
    * 保留最近 10 年，时间顺序由早到晚
- 价格数据：
    * 按 Tab 分隔、GBK 编码读取
    * 取年度价格，裁剪为年份 × 价格指标的宽表格式

输入
----
- rawdata/{ticker}_debt_year.xls    资产负债表
- rawdata/{ticker}_benefit_year.xls 利润表
- rawdata/{ticker}_cash_year.xls    现金流量表
- rawdata/{ticker}_price.xls        年度价格数据

输出
----
- results/csv/bs.csv      资产负债表
- results/csv/pl.csv      利润表
- results/csv/cf.csv      现金流量表
- results/csv/price.csv   年度价格数据

运行方式
--------
    python step1_convert_xls_to_csv.py
    # 或通过主管道：
    python run_pipeline.py
"""

import xlrd
import pandas as pd
import os
from pathlib import Path
from pipeline_utils import CSV_DIR, ensure_output_dirs, validate_rawdata

OUTPUT_DIR = CSV_DIR
ensure_output_dirs()


def trans_csv(file_name: str, new_name: str) -> None:
    """将同花顺财务报表 XLS 转为标准 CSV。

    - 以第二行作为列头（第一行为报表标题行）
    - 将占位符 '--' 替换为 0
    - 去除年份列名中的 '.0' 后缀
    - 删除含空值的行，保留最近 10 年并按升序排列

    Parameters
    ----------
    file_name : str
        原始 XLS 文件路径
    new_name : str
        输出 CSV 文件路径
    """
    book = xlrd.open_workbook(
        file_name,
        ignore_workbook_corruption=True
    )

    sheet = book.sheet_by_index(0)
    data = [sheet.row_values(i) for i in range(sheet.nrows)]

    # 第 1 行（index=1）为真正的列头行（第 0 行是报表标题）
    df = pd.DataFrame(data[1:], columns=data[1])
    df = df.iloc[1:, ]                          # 去掉复制过来的列头行
    df.index = df.iloc[:, 0]                    # 以科目名称为索引
    df = df.iloc[:, 1:]                         # 删除科目名称列（已设为索引）
    df.replace('--', 0, inplace=True)           # 同花顺缺失值占位符替换
    df.columns = df.columns.map(lambda x: str(x).replace('.0', ''))  # 修正年份列名

    df.to_csv(new_name)
    df = pd.read_csv(new_name, index_col=0)
    df.dropna(axis=0, how='any', inplace=True)
    df.astype(int)

    df = df.iloc[:, :10]        # 保留最近 10 年
    df = df[df.columns[::-1]]   # 时间由早到晚排列

    df.to_csv(new_name)


def trans_price_csv(file_name: str, new_name: str) -> None:
    """将同花顺年度价格 XLS 转为宽表 CSV。

    原始文件为 Tab 分隔、GBK 编码。转换后：
    - 行为价格指标（开盘、收盘、涨幅等）
    - 列为年份

    Parameters
    ----------
    file_name : str
        原始价格 XLS 文件路径
    new_name : str
        输出 CSV 文件路径
    """
    df = pd.read_csv(file_name, sep='\t', encoding='gbk')
    df.iloc[:, 0] = df.iloc[:, 0].str[:4]      # 截取年份（前 4 位）
    df = df.iloc[:, :-1]                        # 去掉最后一列（通常为空）
    df.columns = [
        'Time', 'Open', 'High', 'Low', 'Close',
        'Change', 'Amplitude', 'Volume', 'Amount', 'Turnover%', 'Deal Times'
    ]
    df.index = df.Time
    df = df.iloc[:, 1:]     # 去掉 Time 列（已设为索引）
    df = df.T               # 转置：行为指标，列为年份
    df.index.name = "Time"
    df.to_csv(new_name, encoding='utf-8')


# ── 主流程 ────────────────────────────────────────────────────────────────────

print("Step 1: 同花顺原始 XLS → CSV（保留最近 10 年）")
print("=" * 50)

try:
    stocks_ticker, raw_files = validate_rawdata()
except Exception as exc:
    print(f"Error: {exc}")
    exit(1)

print(f"当前处理股票：{stocks_ticker}")
print("-" * 50)
print("财务报表处理中...")

trans_csv(str(raw_files["balance_sheet"]), "./results/csv/bs.csv")
print("    1. 资产负债表转换完成")

trans_csv(str(raw_files["profit_loss"]), "./results/csv/pl.csv")
print("    2. 利润表转换完成")

trans_csv(str(raw_files["cash_flow"]), "./results/csv/cf.csv")
print("    3. 现金流量表转换完成")

print("-" * 50)
print("价格数据处理中...")

trans_price_csv(str(raw_files["price"]), "./results/csv/price.csv")
print("    4. 价格数据转换完成")

print("=" * 50)
print("Step 1 完成！CSV 文件已保存至 ./results/csv/")
