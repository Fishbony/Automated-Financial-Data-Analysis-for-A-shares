"""Unit tests for batch_run module."""

from __future__ import annotations

import shutil
from pathlib import Path

from afda.batch_run import find_company_dirs, _has_required_files


def test_has_required_files_true(tmp_path: Path) -> None:
    """Directory with all three XLS files returns True."""
    for suffix in ("_debt_year.xls", "_benefit_year.xls", "_cash_year.xls"):
        (tmp_path / f"000001{suffix}").write_text("dummy")
    assert _has_required_files(tmp_path) is True


def test_has_required_files_false(tmp_path: Path) -> None:
    """Directory missing one XLS file returns False."""
    (tmp_path / "000001_debt_year.xls").write_text("dummy")
    (tmp_path / "000001_benefit_year.xls").write_text("dummy")
    # Missing _cash_year.xls
    assert _has_required_files(tmp_path) is False


def test_has_required_files_empty_dir(tmp_path: Path) -> None:
    """Empty directory returns False."""
    assert _has_required_files(tmp_path) is False


def test_find_company_dirs_finds_valid(tmp_path: Path) -> None:
    """find_company_dirs returns only subdirectories with valid input files."""
    # Company 1 — valid
    dir_a = tmp_path / "000001"
    dir_a.mkdir()
    for suffix in ("_debt_year.xls", "_benefit_year.xls", "_cash_year.xls"):
        (dir_a / f"000001{suffix}").write_text("dummy")

    # Company 2 — valid
    dir_b = tmp_path / "600519"
    dir_b.mkdir()
    for suffix in ("_debt_year.xls", "_benefit_year.xls", "_cash_year.xls"):
        (dir_b / f"600519{suffix}").write_text("dummy")

    # Company 3 — invalid (missing cash_flow)
    dir_c = tmp_path / "002311"
    dir_c.mkdir()
    (dir_c / "002311_debt_year.xls").write_text("dummy")
    (dir_c / "002311_benefit_year.xls").write_text("dummy")

    # Non-company file
    (tmp_path / "readme.txt").write_text("not a directory")

    result = find_company_dirs(tmp_path)
    assert len(result) == 2
    assert dir_a in result
    assert dir_b in result
    assert dir_c not in result


def test_find_company_dirs_sorted(tmp_path: Path) -> None:
    """find_company_dirs returns directories in sorted order."""
    for name in ("600519", "000001", "002311"):
        d = tmp_path / name
        d.mkdir()
        for suffix in ("_debt_year.xls", "_benefit_year.xls", "_cash_year.xls"):
            (d / f"{name}{suffix}").write_text("dummy")

    result = find_company_dirs(tmp_path)
    names = [d.name for d in result]
    assert names == ["000001", "002311", "600519"]


def test_find_company_dirs_empty_parent(tmp_path: Path) -> None:
    """Empty parent directory returns empty list."""
    assert find_company_dirs(tmp_path) == []
