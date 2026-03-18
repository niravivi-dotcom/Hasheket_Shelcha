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
- **Override Logic:** for error codes 4, 5, 15, 93 — if LastPositive_CHODESH_MASKORET exists and is relevant, responsibility shifts from employer to institutional body
- **Aggregation:** group records per CustomerNumber + Responsibility → one email draft per group

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
- Body: `{"MISPAR_MEZAHE_RESHUMA": string, "TreatmentStatus": string, "Counter": int}`
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
| `pilot_engine.py` | Core logic: process_from_api_records(), build_email_drafts() |
| `pilot_runner_server.py` | Flask on Cloud Run — /run-pilot/from-api endpoint |
| `n8n_workflow_api_to_gmail_draft_v2.json` | n8n workflow (current, active) |
| `n8n_workflow_cloud_pilot_runner_to_gmail_draft.json` | גרסה קודמת (archive) |
| `System_Specification_v0.4.md` | Full technical spec |
| `error_code_mapping_final.xlsx` | קובץ מיפוי קודי שגיאה (84 קודים) — source of truth |
| `error_code_mapping_logic_draft.xlsx` | קובץ לוגיקה לשיתוף עם עידו (4 sheets, עברית) |
| `api_sample.json` | 10 sample records from API |

---

## Current Status (2026-03-18)

### Done
- ✅ Python engine: process_from_api_records() + build_email_drafts() עובד
- ✅ מיגרציה מ-Render ל-Cloud Run (me-west1) — CI/CD דרך GitHub Actions
- ✅ API Key authentication (X-API-Key header)
- ✅ Token rotation: נפתר עם DataTable `Feedback_email_token` + sub-workflow
- ✅ n8n workflow מוכן end-to-end (nodes 1-15): Token → Fetch → Merge → Runner → SetFeedbackStatus
- ✅ SetFeedbackStatus: batches של 50 + delay 1 שנייה
- ✅ Gmail drafts: Domain-Wide Delegation, TEST_GMAIL_IMPERSONATE=avigail@hspension.co.il
- ✅ קובץ לוגיקה (error_code_mapping_logic_draft.xlsx) מוכן לשיתוף עם עידו

### Open Issues Before Production
1. **Gmail draft verification:** לא אומת שטיוטות אכן נוצרות בתיבה של אביגיל
2. **Rate limiting SetFeedbackStatus:** connection aborted נראה בריצות — לבדוק עם דוד
3. **Retry on Fail:** ל-HTTP: POST Update to David — On Error צריך להיות "Continue" (לא "Stop Workflow")
4. **Scheduled trigger:** להחליף Webhook ב-Cron לפרודקשן
5. **StartDate:** דוד יגביל ל-90 יום אחורה בפרודקשן
6. **node-05 חסר בworkflow הנוכחי:** ה-GetFeedbackData נשלח מתוך ה-Python engine (לא מ-n8n ישירות)
