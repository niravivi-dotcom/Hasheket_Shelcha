import pandas as pd
import json

file_path = "sample data.2026.02.24.xlsx"
df = pd.read_excel(file_path)

# Filter for the specific record ID
target_id = '92AF86B5-C38D-47E5-A119-7A7D30CC4B5A'
record = df[df['MISPAR_MEZAHE_RESHUMA'] == target_id]

# Select relevant columns to see what's happening
cols_to_show = [
    'MISPAR_MEZAHE_RESHUMA', 
    'FeedbackId', 
    'MISPAR_MEZAHE_OVED', 
    'UpdateDate', 
    'CHODESH_MASKORET', 
    'ErrorCodeV4Id', 
    'FeedbackStatusId',
    'StatusDescription',
    'FundInstitutionName'
]

# Print the result as JSON to avoid encoding issues and see all details
print(record[cols_to_show].to_json(orient='records', force_ascii=False, indent=4))
