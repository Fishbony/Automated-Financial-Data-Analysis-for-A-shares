"""
statement_base — 三表重构公共工具基类
======================================
提取 rebuild_balance_sheet / rebuild_income_statement / rebuild_cash_flow
中重复的公共函数，消除约 60% 的跨模块重复代码。

提供：
- ensure_output_dir          目录创建
- normalize_item_name        科目名称标准化（统一版本）
- to_numeric_frame           年份列数值化
- safe_row_sum               安全行求和（兼容 str / List[str]）
- load_statement_csv         通用 CSV 加载器
- build_standardized_wide    长表转宽表（可配置索引列）
- build_standardized_item_wide  长表转项目宽表
- apply_bilingual_fonts_to_file  Excel 双语字体后处理
- create_excel_writer        xlsxwriter / openpyxl 回退
- export_statement_excel     通用 Excel 导出（可配置 sheet 配置）
"""

from __future__ import annotations

import os
import re
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from openpyxl import load_workbook

from afda.excel_utils import apply_bilingual_fonts
from afda.statement_mapping import sum_source_items


# ── 目录 ──────────────────────────────────────────────

def ensure_output_dir(output_dir: str) -> None:
    """创建输出目录（幂等）。"""
    os.makedirs(output_dir, exist_ok=True)


# ── 科目名称标准化 ────────────────────────────────────

def normalize_item_name(name: object) -> str:
    """统一科目名称标准化：去 BOM → strip → 去前导星号 → 去全部空白。

    合并了 BS / PL / CF 三个模块各自的实现：
    - BS 原版用 ``re.sub(r"^[*Ｚ]+", ...)`` 去前导星号 + 去全部空白
    - PL/CF 原版用 ``.replace("*", "")`` 去所有星号 + strip
    - statement_mapping 版去全部空白但不去星号

    统一版采用最严格策略：去前导星号 + 去全部空白，覆盖所有场景。
    """
    if pd.isna(name):
        return ""
    text = str(name).replace("\ufeff", "").strip()
    text = re.sub(r"^[*＊]+", "", text)
    text = re.sub(r"\s+", "", text)
    return text


# ── 数值化 ────────────────────────────────────────────

def to_numeric_frame(df: pd.DataFrame, year_cols: List[str]) -> pd.DataFrame:
    """将年份列统一转为 float，无法识别的值按 0 处理。"""
    out = df.copy()
    for col in year_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


# ── 行求和 ────────────────────────────────────────────

def safe_row_sum(
    df: pd.DataFrame,
    item_col: str,
    year_cols: List[str],
    item_names: Union[str, List[str]],
) -> pd.Series:
    """安全行求和，兼容传入单个名称或名称列表。"""
    if isinstance(item_names, str):
        item_names = [item_names]
    return sum_source_items(df, item_col, year_cols, item_names)


# ── CSV 加载 ──────────────────────────────────────────

def load_statement_csv(
    input_path: str,
    item_col_name: str = "科目",
    error_label: str = "CSV",
) -> Tuple[pd.DataFrame, str, List[str]]:
    """通用报表 CSV 加载器。

    Parameters
    ----------
    input_path : str
        CSV 文件路径
    item_col_name : str
        第一列重命名后的列名（BS 用 "原始科目"，PL/CF 用 "科目"）
    error_label : str
        错误信息中显示的报表名称

    Returns
    -------
    df, item_col, year_cols
    """
    df = pd.read_csv(input_path)
    first_col = df.columns[0]
    year_cols = [str(c) for c in df.columns[1:]]
    if not year_cols:
        raise ValueError(f"{error_label} CSV 未识别到年份列，请检查格式。")
    df = df.rename(columns={first_col: item_col_name})
    df[item_col_name] = df[item_col_name].apply(normalize_item_name)
    df = df[df[item_col_name] != ""].copy()
    df = to_numeric_frame(df, year_cols)
    return df, item_col_name, year_cols


# ── 宽表转换 ──────────────────────────────────────────

def build_standardized_wide(
    standardized_df: pd.DataFrame,
    index_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """长表转宽表（多级索引）。"""
    if index_cols is None:
        index_cols = ["Section", "Bucket", "Standard Item"]
    wide = standardized_df.pivot_table(
        index=index_cols,
        columns="Year",
        values="Value",
        aggfunc="sum",
    ).reset_index()
    wide.columns.name = None
    return wide


def build_standardized_item_wide(
    standardized_df: pd.DataFrame,
    item_col: str = "Standard Item",
) -> pd.DataFrame:
    """长表转项目宽表（单级索引，保持原始顺序）。"""
    item_order = standardized_df[item_col].drop_duplicates()
    wide = standardized_df.pivot_table(
        index=item_col,
        columns="Year",
        values="Value",
        aggfunc="sum",
        sort=False,
    ).reindex(item_order).reset_index()
    wide.columns.name = None
    return wide


# ── Excel 工具 ────────────────────────────────────────

def apply_bilingual_fonts_to_file(path: str) -> None:
    """重新打开 xlsx 文件，应用双语字体（Calibri / 黑体）后保存。"""
    wb = load_workbook(path)
    apply_bilingual_fonts(wb)
    wb.save(path)


def create_excel_writer(output_path: str) -> Tuple[pd.ExcelWriter, bool]:
    """创建 Excel writer，优先 xlsxwriter，失败回退 openpyxl。

    Returns
    -------
    writer, use_xlsxwriter : (ExcelWriter, bool)
    """
    try:
        writer = pd.ExcelWriter(output_path, engine="xlsxwriter")
        return writer, True
    except Exception:
        writer = pd.ExcelWriter(output_path, engine="openpyxl")
        return writer, False


def export_statement_excel(
    output_path: str,
    readme_rows: List[Tuple[str, str]],
    sheets: Dict[str, pd.DataFrame],
    col_widths: Dict[str, List[int]],
    preprocess_sheet_name: str = "Preprocess",
    valuation_sheet_name: str = "Valuation_Input",
    post_format_callback: Optional[Callable] = None,
) -> None:
    """通用 Excel 导出。

    Parameters
    ----------
    output_path : str
        输出 xlsx 路径
    readme_rows : list of (sheet_name, description)
        README sheet 内容，用于自动构建 README DataFrame
    sheets : dict
        sheet_name -> DataFrame。函数会自动添加 "README" sheet，
        其余 sheet 需调用方提供
    col_widths : dict
        sheet_name -> list of column widths
    preprocess_sheet_name : str
        预处理 sheet 的名称（Preprocess_BS / Preprocess_PL / Preprocess_CF）
    valuation_sheet_name : str
        估值输入 sheet 的名称
    post_format_callback : callable, optional
        xlsxwriter 模式下的额外格式化回调，签名为 (workbook, writer, sheets)
    """
    # 自动构建 README DataFrame
    readme_df = pd.DataFrame(readme_rows, columns=["Sheet", "Description"])
    sheets["README"] = readme_df

    writer, use_xlsxwriter = create_excel_writer(output_path)

    with writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        if use_xlsxwriter:
            workbook = writer.book
            header_fmt = workbook.add_format({
                "bold": True,
                "bg_color": "#DCE6F1",
                "border": 1,
                "align": "center",
            })
            num_fmt = workbook.add_format({"num_format": "#,##0.00"})
            ratio_fmt = workbook.add_format({"num_format": "0.00%"})

            for sheet_name, widths in col_widths.items():
                if sheet_name not in writer.sheets:
                    continue
                ws = writer.sheets[sheet_name]
                ws.freeze_panes(1, 1)
                for idx, width in enumerate(widths):
                    ws.set_column(idx, idx, width)

                df = sheets[sheet_name]
                for col_num, value in enumerate(df.columns):
                    ws.write(0, col_num, value, header_fmt)

                if sheet_name == valuation_sheet_name:
                    val_df = sheets[valuation_sheet_name]
                    for row_idx in range(1, val_df.shape[0] + 1):
                        section = val_df.iloc[row_idx - 1]["Section"]
                        for col_idx in range(3, val_df.shape[1]):
                            val = val_df.iloc[row_idx - 1, col_idx]
                            fmt = ratio_fmt if section == "Ratio" else num_fmt
                            ws.write_number(row_idx, col_idx, float(val), fmt)

            if post_format_callback is not None:
                post_format_callback(workbook, writer, sheets)
