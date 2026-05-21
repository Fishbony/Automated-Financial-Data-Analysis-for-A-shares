import tempfile
import unittest
from pathlib import Path

from afda.valuation_config import get_multiple, load_valuation_config, valuation_config_source_map


class ValuationConfigTests(unittest.TestCase):
    def test_load_valuation_config_applies_local_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            (data_dir / "valuation_config.json").write_text(
                '{"dcf": {"wacc": 0.12}, "relative_valuation": {"multiples": {"PE": {"low": 10, "mid": 12, "high": 14}}}}',
                encoding="utf-8",
            )

            config = load_valuation_config(data_dir)

            self.assertEqual(config["dcf"]["wacc"], 0.12)
            self.assertEqual(config["dcf"]["terminal_growth"], 0.03)
            self.assertEqual(get_multiple(config, "PE"), {"low": 10.0, "mid": 12.0, "high": 14.0})

            sources = valuation_config_source_map(data_dir)
            self.assertIn("local override", sources["dcf.wacc"])
            self.assertIn("local override", sources["relative_valuation.multiples.PE.mid"])


if __name__ == "__main__":
    unittest.main()

