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
- **Counter:** calculated by David's system, arrives ready in the API response (`OnlyOnStatusChange_DatesDiffInWeeks`)
- **Employee name fields:** `EmployeeFirstName` + `EmployeeLastName` (added by David, Apr 2026)
- **Excluded error codes (no processing):** 1, 2, 6, 7, 16, 17, 23, 40, 41, 71, 72
- **Responsibility classification:** לפי קובץ המיפוי (עמודת אחריות שעידו ממלא)
- **PreMailCondition:** 18 קודי שגיאה בודקים `LastPositive_CHODESH_MASKORET` — אם ריק או פער > 6 חודשים → אחריות עוברת ל-Override path
- **Aggregation by responsibility:**
  - גוף מוסדי: קיבוץ לפי FundInstitutionIdentityNumber + CustomerNumber + פורמט שגיאה (1 או 2)
  - מעסיק/רו"ח/סוכן: קיבוץ לפי CustomerNumber → טיוטת מייל עם טבלה + Excel מצורף
  - מנהלת תיק: מייל עם Excel נפרד לכל מנהלת תיק (מפוצל לפי CustomerAccountManagerEmail)
- **Email formats:**
  - גוף מוסדי פורמט 1: קודי שגיאה 4, 5, 11, 15, 24, 25, 39, 42 — רשימת שמות קבצים + ת.ז. חד-ערכיים
  - גוף מוסדי פורמט 2: קוד שגיאה 26 — פורמט נפרד
  - מעסיק/רו"ח/סוכן: טבלה עם עמודות ת.ז., שם מלא, שם קופה, סוג קופה, תיאור שגיאה, טיפול נדרש, חודש שכר
  - מנהלת תיק: Excel עם כל שדות ה-API
- **Multi-manager fetch:** קריאה נפרדת ל-API לכל מנהלת תיק, מאוחד לפי MISPAR_MEZAHE_RESHUMA (dedup)
- **Gmail impersonation:** טיוטות נוצרות בתיבת מנהלת התיק הרלוונטית לפי `CustomerAccountManagerEmail` (ללא TEST_GMAIL_IMPERSONATE בפרודקשן)

### Step 2.5 — Employer Max-Counter Routing
**לוגיקה עסקית: מעסיק = לקוח, לא להציף אותו**

עבור אותו **עובד + קופה + קוד שגיאה** שמופיע בחודשי שכר שונים (counters שונים) עם אחריות מעסיק:
- מחושב ה-counter המקסימלי בין כל הרשומות של אותו צמד
- כל הרשומות מנותבות לפי ה-counter המקסימלי
- אם max >= 3 → הכל עובר למנהלת תיק (במקום להציף המעסיק שוב על אותה בעיה)
- מופעל ב-`apply_employer_max_counter_routing()` אחרי `classify_all()`, לפני `group_records()`
- חל רק על רשומות עם `email_format = FORMAT_EMPLOYER` (לא מוסדי, לא מנהלת תיק)

### Step 3 — Escalation Policy (Counter-based)
| Counter | Action |
|---------|--------|
| 0 | New error (<1 week) — no action |
| 1 | First email (external or internal based on responsibility) |
| 2 | Reminder |
| 3+ | Internal email to case manager (override on all responsibilities) |

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

### SetFeedbackStatus (בודד)
- `POST .../SetFeedbackStatus` — נשאר פעיל, לשימוש חד-פעמי בלבד

### SetFeedbackStatusBatch ✅ (פעיל מ-2026-04-01)
- `POST https://portal.hspension.co.il/usilenceApi/api/services/AutomationFeedback/SetFeedbackStatusBatch`
- **Headers:** `Authorization: Bearer <token>`, `api_version: 1.0`
- **Body:** מערך JSON של רשומות:
```json
[
  {
    "MISPAR_MEZAHE_RESHUMA": "FEE3C94B-...",
    "TreatmentStatus": "נשלח מייל למעסיק שבוע 1",
    "Counter": 1,
    "Responsibility": "מעסיק",
    "EmailFormat": "employer",
    "RoutingReason": "ברירת מחדל",
    "EmailDraftId": "draft-id-from-gmail",
    "SkippedReason": null
  }
]
```
- **שדות חובה:** `MISPAR_MEZAHE_RESHUMA`, `TreatmentStatus`, `Counter`
- **שדות אופציונליים:** `Responsibility` (max 50), `EmailFormat` (max 50), `RoutingReason` (max 100), `EmailDraftId` (max 100), `SkippedReason` (max 100)
- **Response הצלחה:** `[]` (מערך ריק)
- **Response כשלון חלקי:** `[{"MISPAR_MEZAHE_RESHUMA":"...", "success":false, "message":"..."}]` — רק הכשלונות
- **נפח:** chunks של עד 1000 רשומות לבקשה
- **שדות חדשים חוזרים גם ב-GetFeedbackData** לכל רשומה

---

## Infrastructure
| Component | Details |
|-----------|---------|
| Cloud Run server | `https://hasheket-runner-977634044307.me-west1.run.app` |
| Cloud Run region | `me-west1` |
| Cloud Run endpoints | `/health`, `/run-pilot/from-api-v2` |
| Auth | `X-API-Key` header + env var `API_SECRET_KEY` ב-Cloud Run |
| GitHub repo | `niravivi-dotcom/Hasheket_Shelcha` |
| CI/CD | GitHub Actions → Artifact Registry → Cloud Run (auto-deploy on push to main) |
| Artifact Registry | `me-west1-docker.pkg.dev` — repo `hasheket` |
| Service Account | `gmail-writer@hspension-automation.iam.gserviceaccount.com` |
| Gmail Delegation | Domain-Wide Delegation, scope: `gmail.compose` |
| TEST_GMAIL_IMPERSONATE | **הוסר מ-Cloud Run (Apr 2026)** — בפרודקשן טיוטות הולכות לתיבת מנהלת התיק הנכונה לפי CustomerAccountManagerEmail |
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

## Current Status (2026-05-26)

### Done
- ✅ Python engine: record_classifier + email_builder + report_builder עובד end-to-end
- ✅ Cloud Run (me-west1) — CI/CD דרך GitHub Actions, min-instances=1, max-instances=2
- ✅ Token rotation: DataTable `Feedback_email_token` + sub-workflow
- ✅ n8n workflow: Token → Fetch → Merge → Runner → Split CM Reports → Gmail CM → SetFeedbackStatusBatch
- ✅ Trigger: Webhook מדוד (לא Cron ידני)
- ✅ PreMailCondition: 18 קודי שגיאה בודקים LastPositive_CHODESH_MASKORET + חלון 6 חודשים
- ✅ קודי שגיאה 1 ו-2 מוחרגים לחלוטין
- ✅ Escalation routing: counter >= 3 → case_manager override
- ✅ Multi-manager: אביגיל שפיגלמן + מרים נידאם + פייגי צלניקר
- ✅ Gmail impersonation per-manager (TEST_GMAIL_IMPERSONATE הוסר)
- ✅ build_case_manager_reports(): Excel נפרד לכל מנהלת תיק
- ✅ דשבורד Excel: פירוט לפי מנהלת תיק + counter
- ✅ SetFeedbackStatusBatch — פעיל, n8n מעודכן
- ✅ Employee name fields: EmployeeFirstName + EmployeeLastName
- ✅ Employer max-counter routing: מניעת הצפת מעסיק על אותו עובד+קופה+קוד בחודשים שונים
- ✅ מוסדי-3: קרן פנסיה ברירת מחדל לפי ספרת ביקורת (IncomeTaxAuthorizationNumber + FundInstitutionIdentityNumber)
- ✅ תוכן מייל מעסיק: הוסרה עמודת קוד שגיאה, נוספה עמודת חודש שכר
- ✅ טיוטה כפולה: טופל
- ✅ Structured logging + timing per step + failure alert email → niravivi@gmail.com
- ✅ פרודקשן רץ: 11,303 רשומות, 830 מסווגות, 16 drafts, 10,531 ב-payload (May 26 2026)
