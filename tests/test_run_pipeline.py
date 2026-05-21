import unittest

from afda.run_pipeline import modules_for_run


class RunPipelineTests(unittest.TestCase):
    def test_modules_for_run_skips_valuation_without_info(self) -> None:
        modules = modules_for_run(has_info=False)

        self.assertIn("step1_convert_xls_to_csv", modules)
        self.assertIn("validate_rebuilt_statements", modules)
        self.assertIn("analyze_rebuilt_statements", modules)
        self.assertNotIn("generate_dcf_valuation", modules)
        self.assertNotIn("generate_html_report", modules)

    def test_modules_for_run_includes_valuation_with_info(self) -> None:
        modules = modules_for_run(has_info=True)

        self.assertIn("generate_dcf_valuation", modules)
        self.assertIn("generate_html_report", modules)


if __name__ == "__main__":
    unittest.main()
