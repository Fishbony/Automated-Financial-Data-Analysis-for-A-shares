import unittest

from afda.dcf_core import build_valuation_risk_warnings


class ValuationAuditTests(unittest.TestCase):
    def test_warns_when_terminal_growth_is_too_close_to_wacc(self) -> None:
        data = {
            "valuation_config": {
                "dcf": {
                    "wacc": 0.05,
                    "terminal_growth": 0.045,
                    "dcf_weight": 0.8,
                    "relative_weight": 0.2,
                }
            },
            "fcff_proxy": [1.0, -1.0, -2.0],
        }

        warnings = build_valuation_risk_warnings(data)
        titles = {warning["title"] for warning in warnings}

        self.assertIn("WACC minus terminal growth spread is too thin", titles)
        self.assertIn("DCF weight is high", titles)
        self.assertIn("Historical FCFF quality is weak", titles)


if __name__ == "__main__":
    unittest.main()
