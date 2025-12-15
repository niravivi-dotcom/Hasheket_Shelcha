import pandas as pd

try:
    df = pd.read_excel('feedback report 122025.xlsx', sheet_name='d93e71ab-6f17-4fed-86a3-c95d4fe', nrows=1)
    print("Columns found:")
    print(df.columns.tolist())
except Exception as e:
    print(e)


