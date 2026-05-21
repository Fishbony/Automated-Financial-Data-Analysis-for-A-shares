# Company Sample Fixtures

Put additional A-share export samples in subfolders here to include them in
the end-to-end regression test automatically.

Each sample folder should contain one company's files:

```text
<ticker>_debt_year.xls
<ticker>_benefit_year.xls
<ticker>_cash_year.xls
Info.csv                 # optional; enables DCF and HTML artifact checks
valuation_config.json    # optional
```

The test copies each sample into a temporary directory before running the
pipeline, so fixture files are not modified.
