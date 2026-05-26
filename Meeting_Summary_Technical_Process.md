# סיכום טכני סופי לאחר פגישת תיאום – "השקט שלך"

## 1. ארכיטקטורה וסביבת עבודה
*   **סביבת פיתוח:** .NET עם SQL Server.
*   **ממשק:** REST API מאובטח בתקן OAuth2 עם Bearer Token.
*   **פורמט החלפת נתונים:** JSON (קלט ופלט).
*   **ניהול תהליך:** המנוע החיצוני מבצע Pull (שליפה), עיבוד, ושליחת Push (עדכון) מרוכז ב-Bulk בסיום.

## 2. הגדרת ממשק הקלט (GET / GetErrors)
דוד יספק Endpoint שיחזיר רשימת שגיאות בפורמט JSON הכוללת:
*   **מזהה חד-ערכי (קריטי):** `MISPAR_MEZAHE_RESHUMA` (לצורך עדכון חוזר - GUID).
*   **זיהוי:** `CustomerNumber`, `KodKupa_IdentityNumber`, `KodKupa_IncomeTax`, `MISPAR_MEZAHE_OVED`.
*   **נתוני שגיאה:** `ErrorCodeV4Id`, `ErrorCodeV4Description`, `CHODESH_MASKORET`.
*   **בדיקת היסטוריה:** שדה `LastPositive_CHODESH_MASKORET` (חודש שכר אחרון שבו נקלטה רשומה תקינה לעובד זה באותה קופה).
*   **לוגיקה:** `UpdateDate` (דוד יבדוק אם זהו השדה המדויק לחישוב הוותק - תאריך תחילת השגיאה בסטטוס הנוכחי).
*   **אנשי קשר:** `CustomerContactName`, `CustomerContactEmail` (שם וכתובת מייל של איש הקשר הרלוונטי בכל שורה).
*   **סטטוסים:** `FeedbackStatusId` ו-`StatusDescription`.
*   **שדות חסרים (Open Points):** `TikMislaka`, `OriginalFileName`.

## 3. לוגיקת ה"מוח" (המנוע החיצוני)
1.  **חישוב Counter:** יבוצע בתוך המנוע (Python) על בסיס השדה שיוגדר (כנראה `UpdateDate`).
2.  **בדיקת היסטוריה:** המנוע ישווה בין `CHODESH_MASKORET` הנוכחי לבין ה-`LastSuccessfulChodesh` שהתקבל מדוד.
3.  **תהליך Aggregation:** איחוד שגיאות למיילים מרוכזים לפי מעסיק/קופה.
4.  **יצירת טיוטות:** אינטגרציה מול Gmail API.

## 4. הגדרת ממשק הפלט (POST / UpdateStatus)
בסיום כל ריצה שבועית, המנוע ישלח לדוד קריאת POST אחת (Bulk JSON) הכוללת את תוצאות העיבוד לכל רשומה שפורקה:
*   `MISPAR_MEZAHE_RESHUMA` (המפתח שקיבלנו).
*   `CalculatedCounter` (המונה שחישבנו).
*   `TreatmentStatus` (טקסט חופשי, לדוגמה: "נשלח מייל למעסיק שבוע 1", "הסלמה למנהלת תיק").

## 5. שלבים הבאים לביצוע
1.  דוד יחזיר תשובה סופית לגבי שדה ה-`UpdateDate` ויוודא הכללת השדות `TikMislaka` ו-`OriginalFileName`.
2.  דוד יספק גישה לסביבת Sandbox (URL + מפתח גישה).
3.  התאמת קוד ה-Python לעבודה עם JSON Inputs/Outputs (המנוע כבר תומך במבנה השדות החדש).
4.  בניית מסכי הבקרה בתוך "השקט שלך" על בסיס נתוני ה-Counter והסטטוס שיוחזרו.
