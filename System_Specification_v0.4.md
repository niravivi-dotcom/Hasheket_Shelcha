# מסמך איפיון מערכת: אוטומציה לטיפול בהיזון חוזר – גרסה 0.4

## 1. מטרת המערכת
אוטומציה של תהליך הטיפול בשגיאות היזון חוזר פנסיוני, החל משליפת השגיאות ממערכת "השקט שלך", סיווגן, חישוב משך הזמן שלהן (Counter), ועד ליצירת טיוטות מייל (Drafts) ב-Gmail לאנשי הקשר הרלוונטיים ודיווח חזרה למערכת המקור.

## 2. תהליך העבודה הטכני (The Pipeline)

### 2.1 שליפת נתונים (API Inbound)
הנתונים יימשכו ממערכת "השקט שלך" באמצעות REST API (JSON).
- **סינון ראשוני (בצד ה-DB):**
    - סטטוס משוב (FeedbackStatus) מתוך רשימת 4 סטטוסים שגויים.
    - קוד שגיאה (ErrorCodeV4Id) גדול מ-1.
    - תאריך עדכון (UpdateDate) גדול מתאריך עלייה לייצור.
- **שדות נדרשים:**
    - `MISPAR_MEZAHE_RESHUMA` (מזהה רשומה ייחודי - GUID).
    - `CustomerNumber`, `KodKupa_IdentityNumber`, `KodKupa_IncomeTax`, `MISPAR_MEZAHE_OVED`.
    - `ErrorCodeV4Id`, `ErrorCodeV4Description`, `CHODESH_MASKORET`.
    - `UpdateDate` (תאריך עדכון סטטוס אחרון - דורש אישור דוד כבסיס למונה).
    - `LastPositive_CHODESH_MASKORET` (חודש שכר אחרון תקין).
    - `CustomerContactName`, `CustomerContactEmail` (פרטי איש הקשר למשלוח).
    - `CaseManagerEmail` — **הוסף על ידי דוד (2026-03-14)**. מגיע ישירות מהרשומה, משמש ל-routing של ה-Draft לתיבת מנהלת התיק הרלוונטית.
    - `TikMislaka`, `OriginalFileName` (נדרש לנספחים).

### 2.2 עיבוד ולוגיקה (The Brain)
1. **חישוב מונה (Counter):** מספר השבועות שחלפו מ-`UpdateDate` ועד היום (דורש אימות מול דוד - האם זהו תאריך תחילת השגיאה בסטטוס הנוכחי?).
2. **בדיקת היסטוריה (Override Logic):** עבור קודים 4, 5, 15, 93 – אם קיים `LastPositive_CHODESH_MASKORET` והוא רלוונטי, האחריות תועבר מהמעסיק לגוף המוסדי.
3. **קבוצת נתונים (Aggregation):** איחוד רשומות למייל אחד לפי מפתח `CustomerNumber` + `Responsibility`.

### 2.3 דיווח חזרה (API Outbound)
בסיום העיבוד, המערכת תשלח קריאת POST מרוכזת (Bulk) למערכת "השקט שלך" עם העדכונים הבאים לכל רשומה:
- `MISPAR_MEZAHE_RESHUMA`
- `CalculatedCounter`
- `TreatmentStatus` (למשל: "נשלח מייל למעסיק שבוע 1")

## 3. מדיניות מיילים והסלמה (Escalation Policy)
המונה (Counter) מתחיל מ-0:
- **מונה 0:** שגיאה חדשה (פחות משבוע) – אין פעולה.
- **מונה 1:** שליחת מייל ראשון (חיצוני או פנימי בהתאם לאחריות).
- **מונה 2:** תזכורת עם הפרדה בין שגיאות חדשות (מונה 1) לוותיקות.
- **מונה 3:** מייל פנימי למנהלת תיק (בקשה לטיפול טלפוני).
- **מונה 4:** מייל פנימי למנהלת תיק + מנהלת ראשית.
- **מונה 5+:** מייל פנימי למנהלת תיק + מנהלת ראשית + הנהלה בכירה.

## 4. תוצרי המערכת (Output)
1. **טיוטות מייל (Gmail Drafts):** כולל גוף מייל מפורט ונספח אקסל עם שדות: `KupaID`, `ErrorCodeV4Id`, `ErrorCodeV4Description`, `CHODESH_MASKORET`, `MISPAR_MEZAHE_OVED`.
2. **עדכון DB:** סגירת מעגל מול מערכת "השקט שלך" לצורך תצוגה במסכי הבקרה.

## 5. הגדרות טכניות
- **Authentication:** OAuth2 (Bearer Token).
- **Format:** JSON.
- **Schedule:** ריצה שבועית קבועה.

## 6. אינטגרציית Gmail — Google Workspace של hspension

### ארכיטקטורה
n8n משתמש ב-Service Account עם Domain-wide Delegation כדי ליצור Draft בתיבת מנהלת התיק הרלוונטית — ללא צורך שכל משתמשת תאשר גישה בנפרד.

### רכיבים שהוגדרו (2026-03-14)
| רכיב | פרטים |
|------|--------|
| GCP Project | `hspension-automation` (org: hspension.co.il) |
| Gmail API | מופעל |
| Service Account | `gmail-writer@hspension-automation.iam.gserviceaccount.com` |
| Client ID | `114677208782139198778` |
| JSON Key | שמור מחוץ ל-git, יועבר ל-n8n כ-credential |
| Domain-wide Delegation | מאושר ב-Workspace Admin, scope: `gmail.compose` |

### Flow
```
n8n ← CaseManagerEmail (מה-API של דוד)
    ↓
Service Account (impersonate CaseManagerEmail)
    ↓
Gmail Draft בתיבת מנהלת התיק
```

