# Project: השקט שלך — hspension Pension Feedback Automation

## Business Context
hspension manages pension funds. Employers submit monthly salary reports ("היזון חוזר") which sometimes contain errors. The system automates the error follow-up process: detecting errors, escalating by age (counter), sending email drafts to the right party, and reporting back to the source system.

## Contacts
- **David Spectorman** (davids@go-net.co.il) — CTO Go-net, API owner
- **Ido Viner** (vinnerido@hspension.co.il) — Deputy CEO hspension, CC on all communication
- **Shahar Avivi** (shahar@hspension.co.il) — hspension, CC on all communication

---

## Architecture
```
[David's API: GetFeedbackData]
        ↓ (Bearer Token, weekly)
[n8n: Sub-workflow Token Manager (DataTable) → Fetch Data → Merge with Mapping]
        ↓
[Cloud Run (me-west1): pilot_runner_server.py → pilot_engine.py]
        ↓                    ↓
[Gmail Drafts]     [n8n: POST SetFeedbackStatus per record (batches of 50)]
```

## Business Flow (The Pipeline)

### Step 1 — Inbound: Fetch Errors
- **Endpoint:** POST `GetFeedbackData` with `{"StartDate":"2022-01-01"}`
- **Auth:** Bearer Token (access_token, refreshed via refresh_token before every run)
- **Filter (server-side by David):** FeedbackStatus in [error list], ErrorCodeV4Id > 1, UpdateDate > go-live date
- **Key fields:** MISPAR_MEZAHE_RESHUMA, CustomerNumber, ErrorCodeV4Id, UpdateDate, Counter, CustomerContactEmail, TikMislaka, OriginalFileName

### Step 2 — Processing: Python Engine (Cloud Run)
- **Counter:** calculated by David's system, arrives ready in the API response
- **Excluded error codes (no processing):** 6, 7, 16, 17, 23, 40, 41, 71, 72
- **Override Logic:** קודים שגורמים להעברת אחריות ממעסיק למוסדי — בתוקף, הרחבת הקודים תגיע עם קובץ המיפוי המעודכן של עידו
- **Responsibility classification:** לפי קובץ המיפוי (עמודת אחריות שעידו ממלא)
- **Institutional body identifier:** `FundInstitutionName` + `FundInstitutionIdentityNumber` + שם קובץ (ממתין לדוד)
- **Aggregation by responsibility:**
  - גוף מוסדי: קיבוץ לפי FundInstitutionIdentityNumber + CustomerNumber + פורמט שגיאה (1 או 2)
  - מעסיק/רו"ח/סוכן: קיבוץ לפי CustomerNumber → טיוטת מייל עם טבלה + Excel מצורף
  - מנהל תיק: מייל אמיתי (לא טיוטה) עם Excel של כל הרשומות + כל שדות ה-API
- **Email formats:**
  - גוף מוסדי פורמט 1: קודי שגיאה 4, 5, 11, 15, 24, 25, 39, 42 — רשימת שמות קבצים + ת.ז. חד-ערכיים
  - גוף מוסדי פורמט 2: קוד שגיאה 26 — פורמט נפרד
  - מעסיק/רו"ח/סוכן: טבלה עם עמודות ת.ז., שם מלא, קוד שגיאה, תאור שגיאה, טיפול נדרש
  - מנהל תיק: כל שדות ה-API, ממתין לחלוקת To/CC מעידו

### Step 3 — Escalation Policy (Counter-based)
| Counter | Action |
|---------|--------|
| 0 | New error (<1 week) — no action |
| 1 | First email (external or internal based on responsibility) |
| 2 | Reminder with separation of new vs. old errors |
| 3 | Internal email to case manager (request for phone follow-up) |
| 4 | Case manager + senior manager |
| 5+ | Case manager + senior manager + executive |

### Step 4 — Outbound: Report Back
- **Endpoint:** POST `SetFeedbackStatus` per record
- **Body:** `{"MISPAR_MEZAHE_RESHUMA": str, "TreatmentStatus": str, "Counter": int}`
- **Response:** 204 No Content (always succeeds, currently validates only)

---

## n8n Workflow — Node Map (`n8n_workflow_api_to_gmail_draft_v2.json`)

| Node | ID | Function |
|------|----|----------|
| Webhook Trigger | node-01 | HTTP trigger (production: scheduled Cron) |
| Code: Config | node-02 | Set CLIENT_ID, API_BASE, START_DATE, TOP, ACCOUNT_MANAGER_EMAIL |
| Execute Workflow: Get Token | node-03 | Calls sub-workflow "refresh token feedback email" → returns access_token |
| Code: Extract Token | node-04 | Extract access_token from sub-workflow result |
| Google Drive: Download Mapping | node-07 | Fetch error_code_mapping_final.xlsx from Drive |
| Merge: Token + Mapping | node-08 | Combine token + mapping file (mergeByIndex) |
| HTTP: Runner /from-api | node-09 | POST to Cloud Run → get drafts + update_payload |
| Code: Split Update Payload | node-12 | Split update_payload array → one item per record |
| Split in Batches | node-14 | Batch records (batchSize=50) |
| HTTP: POST Update to David | node-13 | POST SetFeedbackStatus per record |
| Wait: Delay | node-15 | 1 second between batches |

**Token Manager Sub-workflow:**
- שם: `refresh token feedback email`
- DataTable: `Feedback_email_token` (שורות: access_token, refresh_token)
- לוגיקה: בודק תוקף → אם פג, מרענן ושומר הזוג החדש ב-DataTable → מחזיר access_token
- **Token rotation נפתר** — שני הטוקנים מתעדכנים ב-DataTable אחרי כל refresh

---

## API Reference

### Auth
- **Refresh endpoint:** `POST /usilenceApi/api/auth/token/refresh`
- **Headers:** `client_id`, `Authorization: Bearer <current_access_token>`, `api_version: 1.0`
- **Body:** `access_token`, `refresh_token`, `grant_type: refresh_token`, `ApiVersion: 1.0`
- **Token expiry:** access_token = 24h | refresh_token = until 2027-03-01 (current pair)
- **Rotation:** both tokens rotate on every refresh

### GetFeedbackData
- `POST https://portalstage.hspension.co.il/usilenceApi/api/services/AutomationFeedback/GetFeedbackData`
- Body: `{"StartDate":"2022-01-01"}`

### SetFeedbackStatus
- `POST https://portalstage.hspension.co.il/usilenceApi/api/services/AutomationFeedback/SetFeedbackStatus`
- **מבנה נוכחי (POST בודד):** `{"MISPAR_MEZAHE_RESHUMA": string, "TreatmentStatus": string, "Counter": int}`
- **מבנה מוצע (bulk — ממתין לדוד):**
```json
[
  {
    "MISPAR_MEZAHE_RESHUMA": "XXX",
    "Responsibility": "employer",
    "EmailDraftId": "r-123456789"
  }
]
```
- ערכי Responsibility: `employer` / `institutional` / `case_manager` / `accountant` / `agent`
- EmailDraftId = Gmail draft_id, יהיה `null` עבור Counter=0 (לשלב הטסט — כל הרשומות נשלחות כולל Counter=0)
- **נפח:** טסט ~8K רשומות | פרודקשן ~50K+ רשומות → חובה לשלוח ב-chunks של עד 1000 רשומות לבקשה (~100KB לבקשה)
- Response: 204 No Content

---

## Infrastructure
| Component | Details |
|-----------|---------|
| Cloud Run server | `https://pilot-runner-hasheket.onrender.com` (URL ישן — לעדכן ל-Cloud Run URL) |
| Cloud Run region | `me-west1` |
| Cloud Run endpoints | `/health`, `/run-pilot/from-api` |
| Auth | `X-API-Key` header + env var `API_SECRET_KEY` ב-Cloud Run |
| GitHub repo | `niravivi-dotcom/Hasheket_Shelcha` |
| CI/CD | GitHub Actions → Artifact Registry → Cloud Run (auto-deploy on push to main) |
| Artifact Registry | `me-west1-docker.pkg.dev` — repo `hasheket` |
| Service Account | `gmail-writer@hspension-automation.iam.gserviceaccount.com` |
| Gmail Delegation | Domain-Wide Delegation, scope: `gmail.compose` |
| TEST_GMAIL_IMPERSONATE | `avigail@hspension.co.il` (env var on Cloud Run — drafts go to Avigail's inbox in test) |
| Error mapping file | Google Drive file ID: `1dkrqlHosnM-ehExGk9qo4ajjK8JAmh-F` |
| Gmail credential | "Gmail account 2" (n8n credential ID: zCSo1FRBdBnZb7y8) |
| Drive credential | "Google Drive account" (n8n credential ID: BgjXpVKQYS9oadSp) |

---

## Key Files
| File | Purpose |
|------|---------|
| `record_classifier.py` | סיווג רשומות: אחריות, PreMailCondition, escalation routing |
| `email_builder.py` | בניית טיוטות מייל לפי פורמט (מוסדי / מעסיק / סוכן / רו"ח) |
| `report_builder.py` | דשבורד Excel (run report) + build_case_manager_reports() |
| `pilot_runner_server_v2.py` | Flask על Cloud Run — /run-pilot/from-api |
| `Feedback Email Automation.json` | n8n workflow פעיל (16 nodes) |
| `error_code_mapping_v2.xlsx` | קובץ מיפוי (84 קודים + PreMailConditionField) — source of truth |
| `api_sample.json` | 10 sample records from API |

---

## Current Status (2026-03-23)

### Done
- ✅ Python engine: record_classifier + email_builder + report_builder עובד end-to-end
- ✅ Cloud Run (me-west1) — CI/CD דרך GitHub Actions
- ✅ Token rotation: DataTable `Feedback_email_token` + sub-workflow
- ✅ n8n workflow (16 nodes): Token → Fetch → Merge → Runner → Split CM Reports → Gmail CM → SetFeedbackStatus
- ✅ Gmail drafts: טיוטות נוצרות בתיבה של אביגיל (TEST_GMAIL_IMPERSONATE)
- ✅ PreMailCondition: 18 קודי שגיאה בודקים LastPositive_CHODESH_MASKORET לפני סיווג
- ✅ קודי שגיאה 1 ו-2 מוחרגים לחלוטין (לא ממופים)
- ✅ Escalation routing: counter >= 3 → case_manager (override על כל אחריות אחרת)
- ✅ build_case_manager_reports(): Excel נפרד לכל מנהלת תיק
- ✅ n8n: Code: Split CM Reports + Gmail: Send CM Reports — אומת, מייל התקבל אצל אביגיל
- ✅ דשבורד Excel: טבלת הסלמה לפי שבועות (חיצוני / פנימי)

### Open Issues Before Production
1. **SetFeedbackStatus bulk endpoint:** ממתין לדוד לספק endpoint שמקבל מערך (נשאל 2026-03-19)
2. **Scheduled trigger:** להחליף Webhook ב-Cron
3. **StartDate:** דוד יגביל ל-90 יום אחורה בפרודקשן
4. **אימות תוכן מיילים חיצוניים:** לוודא מעסיקים/מוסדיים — תוכן ופילטור נכונים

### ממתין לתשובה חיצונית
- **דוד** — bulk endpoint ל-SetFeedbackStatus (נשאל 2026-03-19)
