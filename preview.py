import pandas as pd

# קריאת הקובץ (שמור בתיקיה הנוכחית)
df = pd.read_excel('error cod mapping.xlsx')

# הצגת שמות העמודות
print("Columns:", list(df.columns))

# הצגת 20 השורות הראשונות
print(df.head(20).to_string(index=False))