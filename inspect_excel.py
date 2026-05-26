import pandas as pd
import sys

file_path = "sample data.2026.02.24.xlsx"

try:
    xl = pd.ExcelFile(file_path)
    print(f"Sheets: {xl.sheet_names}")
    
    for sheet_name in xl.sheet_names:
        print(f"\n--- Sheet: {sheet_name} ---")
        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5)
        print(df.columns.tolist())
        print(df.head())
        
except Exception as e:
    print(f"Error reading file: {e}")
