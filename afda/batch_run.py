"""
batch_run — AFDA 批量多公司流水线
===================================
扫描父目录下的多个公司子目录，依次执行完整流水线。

用法：
    python -m afda.batch_run "D:/path/to/companies"
    python -m afda.batch_run "D:/path/to/companies" --resume
    python -m afda.batch_run "D:/path/to/companies" --subprocess

目录结构示例：
    companies/
    ├── 000001/          # 公司 1
    │   ├── 000001_debt_year.xls
    │   ├── 000001_benefit_year.xls
    │   ├── 000001_cash_year.xls
    │   └── Info.csv
    ├── 600519/          # 公司 2
    │   ├── 600519_debt_year.xls
    │   ├── 600519_benefit_year.xls
    │   └── 600519_cash_year.xls
    └── 002311/          # 公司 3
        └── ...

每家公司独立运行，互不干扰。一家失败不会阻止后续公司。
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from afda.logging_config import get_logger

logger = get_logger(__name__)

REQUIRED_SUFFIXES = ("_debt_year.xls", "_benefit_year.xls", "_cash_year.xls")


def find_company_dirs(parent_dir: Path) -> list[Path]:
    """扫描父目录，返回包含有效输入文件的子目录列表。

    一个子目录被认定为有效公司目录，当且仅当它同时包含
    _debt_year.xls、_benefit_year.xls 和 _cash_year.xls 文件。
    """
    if not parent_dir.is_dir():
        raise SystemExit(f"目录不存在：{parent_dir}")

    company_dirs = []
    for child in sorted(parent_dir.iterdir()):
        if not child.is_dir():
            continue
        if _has_required_files(child):
            company_dirs.append(child)
        else:
            logger.warning("跳过 %s（缺少必需的 XLS 文件）", child.name)

    return company_dirs


def _has_required_files(dir_path: Path) -> bool:
    """检查目录是否包含全部三个必需的 XLS 文件。"""
    return all(
        any(path.name.endswith(suffix) for path in dir_path.iterdir())
        for suffix in REQUIRED_SUFFIXES
    )


def run_single_company(
    company_dir: Path,
    resume: bool = False,
    force: bool = False,
    use_subprocess: bool = False,
) -> bool:
    """为单个公司目录运行流水线。

    Returns
    -------
    bool
        True 表示成功，False 表示失败。
    """
    cmd = [sys.executable, "-m", "afda.run_pipeline", str(company_dir)]
    if resume:
        cmd.append("--resume")
    if force:
        cmd.append("--force")
    if use_subprocess:
        cmd.append("--subprocess")

    logger.info("开始处理：%s", company_dir.name)
    completed = subprocess.run(cmd, check=False)
    if completed.returncode == 0:
        logger.info("✓ %s 完成", company_dir.name)
        return True
    else:
        logger.error("✗ %s 失败 (exit code %d)", company_dir.name, completed.returncode)
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-run the A-share financial analysis pipeline for multiple companies."
    )
    parser.add_argument(
        "parent_dir",
        help="Parent directory containing company subdirectories.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Skip completed steps per company (checkpoint-based).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Ignore existing checkpoints and run all steps from scratch.",
    )
    parser.add_argument(
        "--subprocess",
        action="store_true",
        default=False,
        help="Force subprocess mode for all pipeline modules.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    parent_dir = Path(args.parent_dir).expanduser().resolve()

    logger.info("AFDA 批量流水线")
    logger.info("=" * 50)
    logger.info("父目录：%s", parent_dir)

    company_dirs = find_company_dirs(parent_dir)
    if not company_dirs:
        logger.error("未找到有效的公司子目录。每家公司的目录需同时包含 *_debt_year.xls、*_benefit_year.xls 和 *_cash_year.xls。")
        raise SystemExit(1)

    logger.info("发现 %d 家公司：%s", len(company_dirs), ", ".join(d.name for d in company_dirs))
    logger.info("=" * 50)

    results: dict[str, bool] = {}
    for i, company_dir in enumerate(company_dirs, 1):
        logger.info("\n[%d/%d] 处理公司：%s", i, len(company_dirs), company_dir.name)
        success = run_single_company(
            company_dir,
            resume=args.resume,
            force=args.force,
            use_subprocess=args.subprocess,
        )
        results[company_dir.name] = success

    # --- 汇总 ---
    logger.info("\n" + "=" * 50)
    logger.info("批量运行汇总")
    logger.info("=" * 50)
    succeeded = sum(1 for s in results.values() if s)
    failed = len(results) - succeeded
    for name, success in results.items():
        status = "✓ 成功" if success else "✗ 失败"
        logger.info("  %s: %s", name, status)
    logger.info("-" * 50)
    logger.info("总计：%d 家，成功 %d，失败 %d", len(results), succeeded, failed)

    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
