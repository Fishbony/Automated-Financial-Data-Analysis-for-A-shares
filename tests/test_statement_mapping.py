import unittest

import pandas as pd

from afda.statement_mapping import resolve_item_name, sum_source_items


class StatementMappingTests(unittest.TestCase):
    def test_resolve_item_name_matches_prefix_and_unit_variants(self) -> None:
        available = ["营业收入(元)", "所得税费用(元)", "资产负债表项目"]

        self.assertEqual(resolve_item_name(available, "其中：营业收入"), "营业收入(元)")
        self.assertEqual(resolve_item_name(available, "减：所得税费用（元）"), "所得税费用(元)")

    def test_sum_source_items_uses_alias_matches(self) -> None:
        df = pd.DataFrame(
            {
                "科目": ["营业收入(元)", "营业成本(元)"],
                "2024": [100.0, 60.0],
                "2025": [120.0, 72.0],
            }
        )

        values = sum_source_items(df, "科目", ["2024", "2025"], ["其中：营业收入（元）"])

        self.assertEqual(values["2024"], 100.0)
        self.assertEqual(values["2025"], 120.0)


if __name__ == "__main__":
    unittest.main()
