import pandas as pd
import json

file_path = "sample data.2026.02.24.xlsx"
df = pd.read_excel(file_path)

status_map = df[['FeedbackStatusId', 'StatusDescription']].drop_duplicates().set_index('FeedbackStatusId')['StatusDescription'].to_dict()

with open('status_mapping.json', 'w', encoding='utf-8') as f:
    json.dump(status_map, f, ensure_ascii=False, indent=4)
