import tempfile
import unittest
from pathlib import Path

from afda.input_validation import validate_input_folder
from afda.pipeline_utils import company_display_name


def _touch(path: Path) -> None:
    path.write_bytes(b"placeholder")


class InputValidationTests(unittest.TestCase):
    def test_validate_input_folder_accepts_minimal_info(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            ticker = "600406"
            _touch(data_dir / f"{ticker}_debt_year.xls")
            _touch(data_dir / f"{ticker}_benefit_year.xls")
            _touch(data_dir / f"{ticker}_cash_year.xls")
            (data_dir / "Info.csv").write_text(
                "项目,值\n公司名称,测试公司\n公司代码,600406\n总股本,100000000\n当前股价,12.34\n",
                encoding="utf-8",
            )

            report = validate_input_folder(data_dir)

            self.assertTrue(report.ok)
            self.assertEqual(report.ticker, ticker)
            self.assertEqual(report.errors, [])
            self.assertEqual(company_display_name(data_dir, ticker=ticker), "测试公司（600406）")

    def test_validate_input_folder_rejects_bad_info_number(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            ticker = "600406"
            _touch(data_dir / f"{ticker}_debt_year.xls")
            _touch(data_dir / f"{ticker}_benefit_year.xls")
            _touch(data_dir / f"{ticker}_cash_year.xls")
            (data_dir / "Info.csv").write_text("项目,值\n总股本,not-a-number\n当前股价,12.34\n", encoding="utf-8")

            report = validate_input_folder(data_dir)

            self.assertFalse(report.ok)
            self.assertTrue(any("总股本" in message for message in report.errors))

    def test_validate_input_folder_warns_without_info(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            ticker = "600406"
            _touch(data_dir / f"{ticker}_debt_year.xls")
            _touch(data_dir / f"{ticker}_benefit_year.xls")
            _touch(data_dir / f"{ticker}_cash_year.xls")

            report = validate_input_folder(data_dir)

            self.assertTrue(report.ok)
            self.assertTrue(any("Info.csv not found" in message for message in report.warnings))


if __name__ == "__main__":
    unittest.main()

