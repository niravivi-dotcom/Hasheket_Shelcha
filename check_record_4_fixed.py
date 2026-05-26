import pandas as pd
import json

file_path = "sample data.2026.02.24.xlsx"
df = pd.read_excel(file_path)

target_id = '92AF86B5-C38D-47E5-A119-7A7D30CC4B5A'
record = df[df['MISPAR_MEZAHE_RESHUMA'] == target_id].copy()

# Convert Timestamp to string for JSON serialization
if 'UpdateDate' in record.columns:
    record['UpdateDate'] = record['UpdateDate'].astype(str)

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

data = record[cols_to_show].to_dict(orient='records')
with open('record_analysis.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
