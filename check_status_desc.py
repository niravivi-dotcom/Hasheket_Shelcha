import pandas as pd
import json

file_path = "sample data.2026.02.24.xlsx"
df = pd.read_excel(file_path)

status_map = df[['FeedbackStatusId', 'StatusDescription']].drop_duplicates().set_index('FeedbackStatusId')['StatusDescription'].to_dict()

# Print as JSON string to avoid terminal encoding issues for raw Hebrew
print(json.dumps(status_map, ensure_ascii=False))
