import xlrd
import pandas as pd
import os
from pathlib import Path
from pipeline_utils import CSV_DIR, ensure_output_dirs, validate_rawdata

OUTPUT_DIR = CSV_DIR
ensure_output_dirs()


def trans_csv(file_name, new_name):
    book = xlrd.open_workbook(
        file_name,
        ignore_workbook_corruption=True
    )
    
    sheet = book.sheet_by_index(0)
    data = [sheet.row_values(i) for i in range(sheet.nrows)]
    df = pd.DataFrame(data[1:], columns=data[1])
    df = df.iloc[1:, ]
    df.index = df.iloc[:, 0]
    df = df.iloc[:, 1:]
    df.replace('--', 0, inplace=True)
    df.columns = df.columns.map(lambda x: str(x).replace('.0', ''))
    df.to_csv(new_name)
    df = pd.read_csv(new_name, index_col=0)
    df.dropna(axis=0, how='any', inplace=True)
    df.astype(int)
    
    # last 10 years
    df = df.iloc[:, :10]
    # reversed
    df = df[df.columns[::-1]]
    
    df.to_csv(new_name)


def trans_price_csv(file_name, new_name):
    df = pd.read_csv(file_name, sep='\t', encoding='gbk')
    df.iloc[:,0] = df.iloc[:,0].str[:4]
    df = df.iloc[:, :-1]
    df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Change', 'Amplitude', 'Volume', 'Amount', 'Turnover%', 'Deal Times']
    df.index = df.Time
    df = df.iloc[:, 1:]
    df = df.T
    df.index.name = "Time"
    df.to_csv(new_name, encoding='utf-8')

print("This script is used to transform the xls files from RoyalFlush into csv files, and keep the last 10 years data.")
print("===============================")
try:
    stocks_ticker, raw_files = validate_rawdata()
except Exception as exc:
    print(f"Error: {exc}")
    exit(1)
print("Processing stock: " + stocks_ticker)
print("------------------------------")
print("Financial statement processing...")
trans_csv(str(raw_files["balance_sheet"]), "./results/csv/bs.csv")
print("    1. Balance sheet transformed!")
trans_csv(str(raw_files["profit_loss"]), "./results/csv/pl.csv")
print("    2. Profit and loss statement transformed!")
trans_csv(str(raw_files["cash_flow"]), "./results/csv/cf.csv")
print("    3. Cash flow statement transformed!")
print("------------------------------")
print("Price data processing...")
trans_price_csv(str(raw_files["price"]), "./results/csv/price.csv")
print("    4. Price data transformed!")
print("===============================")
print("All done! Now you can find the csv files in ./results/csv/ directory.")

