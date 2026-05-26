# Memory — השקט שלך (Pension Feedback Automation)

## Project Directory
`C:\Users\nirav\Avivi-Solutions\השקט שלך\`

## What It Does
Automates pension feedback error handling. Fetches errors from David's API, processes them (counter-based escalation, override logic), creates Gmail drafts to employers/institutions, and reports back to the source system per record.

## Architecture
```
David's API (GetFeedbackData) → n8n → Render (Python engine) → Gmail Drafts
                                                              → SetFeedbackStatus (per record)
```

## API Endpoints (Test Environment)
- **Base:** `https://portalstage.hspension.co.il/usilenceApi/api`
- **Fetch:** POST `.../GetFeedbackData` | Body: `{"StartDate":"2022-01-01"}`
- **Update:** POST `.../SetFeedbackStatus` | Body: `{"MISPAR_MEZAHE_RESHUMA": str, "TreatmentStatus": str, "Counter": int}` | Response: 204
- **Auth refresh:** POST `.../auth/token/refresh` | Both tokens rotate on refresh

## Token Status (as of 2026-03-04 email from David)
- access_token: 24h expiry | refresh_token: valid until 2027-03-01
- New tokens sent by David on 2026-03-04 (in email "Re: API בטסט")
- **⚠️ Open issue:** n8n node-04 only saves new access_token after refresh — new refresh_token is discarded. Needs fix before production.

## n8n Workflow Status (2026-03-11)
- Nodes 1-12: working
- Node-13: FIXED — now uses correct SetFeedbackStatus endpoint + per-record body (MISPAR_MEZAHE_RESHUMA, TreatmentStatus, Counter)
- Node-12: FIXED — now splits update_payload array into individual items
- **Not yet tested end-to-end** (waiting for real data from David)

## Current Blockers
1. Real data from David — expected ~2026-03-15
2. Token rotation persistence (node-03/04) — needs solution before production
3. Manual Trigger → Cron (before production)

## Key Contacts
- David Spectorman (davids@go-net.co.il) — CTO Go-net, API owner
- Ido Viner (vinnerido@hspension.co.il) — Deputy CEO hspension
