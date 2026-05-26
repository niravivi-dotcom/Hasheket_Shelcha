import pandas as pd
file_path = "sample data.2026.02.24.xlsx"
df = pd.read_excel(file_path)
print("FeedbackStatusId values:")
print(df['FeedbackStatusId'].value_counts())
print("\nStatusDescription values:")
print(df['StatusDescription'].value_counts())
