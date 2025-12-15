## מטרה
להריץ את `pilot_engine.py` מהמחשב שלך (Windows), וב־n8n Cloud לקבל:
- יצירת הקובץ `Pilot_Results_v2.xlsx`
- יצירת **Gmail Draft** עם הקובץ מצורף

ב־n8n Cloud אין `Execute Command`, לכן צריך Runner חיצוני קטן.

---

## 1) הפעלת ה-Runner מקומית
בתיקיית הפרויקט (`C:\Users\nirav\Avivi-Solutions\השקט שלך`):

```powershell
py -3 -m pip install -r requirements_runner.txt
py -3 pilot_runner_server.py
```

בדיקה מקומית:
- `http://127.0.0.1:8787/health`
- POST ל־`http://127.0.0.1:8787/run-pilot` (יחזיר JSON)
- POST ל־`http://127.0.0.1:8787/run-pilot/file` (יוריד XLSX)

---

## 2) חשיפה ל-n8n Cloud (בחירה אחת)

### אופציה A: ngrok
1. התקנה: `https://ngrok.com/download`
2. הפעלה:

```powershell
ngrok http 8787
```

תקבל URL ציבורי כמו:
`https://xxxx-xx-xx-xx-xx.ngrok-free.app`

ה־Runner endpoint יהיה:
`https://.../run-pilot/file`

### אופציה B: Cloudflare Tunnel
(אפשר אם כבר יש לך Cloudflare; אם תרצה אכתוב הוראות מדויקות לפי הדומיין שלך)

---

## 3) ייבוא Workflow ל-n8n Cloud
ייבא את הקובץ:
`n8n_workflow_cloud_pilot_runner_to_gmail_draft.json`

ואז עדכן:
1. ב־node `HTTP Request (Run Pilot + Get XLSX)`:
   - **שימו לב**: בגרסה המעודכנת יש 2 HTTP Requests:
     - `HTTP Request (Run Pilot)` → URL: `<ngrok-url>/run-pilot` (POST, JSON)
     - `HTTP Request (Download XLSX)` → URL: `<ngrok-url>/latest/file` (GET, File)
2. ב־node `Gmail (Create Draft)`:
   - לבחור את ה־Credentials של Gmail שלך
3. יעד ה־To:
   - להגדיר משתנה סביבה ב-n8n בשם `GMAIL_TEST_TO`
   - או לשנות בקוד את `'your@email.com'`

בסיום – לחיצה על `Execute workflow` אמורה:
1) להפעיל את הפיילוט על המחשב שלך  
2) למשוך את `Pilot_Results_v2.xlsx` ל-n8n כקובץ  
3) לייצר Draft בג׳ימייל + הקובץ מצורף  


