"""
pilot_runner_server_v2.py
--------------------------
Flask server — engine v2.

Endpoints:
  GET  /health             — בריאות השרת
  POST /run-pilot/from-api-v2 — pipeline מלא: fetch → classify → group → build → send → payload

Auth: X-API-Key header (env var API_SECRET_KEY)
"""

import os
import io
import base64
import json
import sys
import time
import traceback
import logging
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, jsonify, request

# =============================================================================
# Logging — stdout עם timestamps (נקרא ב-Cloud Run Logs)
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
log = logging.getLogger(__name__)

APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_DIR))

# engine v2 modules
from mapping_loader    import load_mapping
from record_classifier import classify_all, apply_employer_max_counter_routing
from record_grouper    import group_records, summarize_groups
from email_builder     import build_all_emails
from gmail_sender      import send_all_groups, summarize_results
from payload_builder   import build_payload, summarize_payload
from report_builder    import build_run_report, build_case_manager_reports

app = Flask(__name__)


# =============================================================================
# Helpers
# =============================================================================

def _load_service_account():
    """טוען service account JSON מ-env var GMAIL_SERVICE_ACCOUNT_B64 (base64)."""
    raw = os.environ.get("GMAIL_SERVICE_ACCOUNT_B64")
    if not raw:
        return None
    try:
        return json.loads(base64.b64decode(raw))
    except Exception as e:
        log.warning(f"לא הצלחתי לטעון GMAIL_SERVICE_ACCOUNT_B64: {e}")
        return None


def _send_failure_alert(step: str, error: str, service_account_info: dict,
                        sender: str = None, recipient: str = "niravivi@gmail.com"):
    """שולח מייל התראה כשה-pipeline נופל."""
    if not service_account_info or not sender:
        log.warning("Alert email דולג — אין service account או sender")
        return
    try:
        import email.message
        from googleapiclient.discovery import build
        from google.oauth2 import service_account as sa

        creds = sa.Credentials.from_service_account_info(
            service_account_info,
            scopes=[
                "https://www.googleapis.com/auth/gmail.send",
                "https://www.googleapis.com/auth/gmail.compose",
            ],
        ).with_subject(sender)
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)

        msg = email.message.EmailMessage()
        msg["Subject"] = f"[hspension] Pipeline כשל — {step}"
        msg["From"]    = sender
        msg["To"]      = recipient
        msg.set_content(
            f"הפייפליין נפל בשלב: {step}\n\n"
            f"שגיאה:\n{error}\n\n"
            f"זמן (UTC): {datetime.utcnow().isoformat()}Z"
        )
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId=sender, body={"raw": raw}).execute()
        log.info(f"Alert email נשלח ל-{recipient}")
    except Exception as e:
        log.warning(f"שליחת alert email נכשלה: {e}")


def _check_api_key():
    """מוודא X-API-Key. מחזיר None אם תקין, response אם לא."""
    secret = os.environ.get("API_SECRET_KEY")
    if secret and request.headers.get("X-API-Key") != secret:
        return jsonify({"ok": False, "message": "Unauthorized"}), 401
    return None


def _fetch_david_records(api_base, access_token, start_date, top, acct_mgr,
                         max_retries=3, retry_delay=15):
    """קורא GetFeedbackData מ-API של דוד. מחזיר list. מנסה עד max_retries פעמים."""
    body = {"StartDate": start_date, "top": int(top)}
    if acct_mgr:
        body["AccountManagerEmail"] = acct_mgr

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                log.warning(f"  retry {attempt}/{max_retries} עבור {acct_mgr or 'all'} (ממתין {retry_delay}s)")
                time.sleep(retry_delay)

            resp = requests.post(
                f"{api_base}/services/AutomationFeedback/GetFeedbackData",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type":  "application/json",
                },
                json=body,
                timeout=300,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError("תגובת API של דוד אינה JSON array")
            if attempt > 1:
                log.info(f"  הצליח בניסיון {attempt}")
            return data

        except Exception as e:
            last_exc = e
            log.warning(f"  ניסיון {attempt}/{max_retries} נכשל: {e}")

    raise last_exc


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/health")
def health():
    return jsonify({"ok": True, "version": "v2", "time": datetime.utcnow().isoformat() + "Z"})


@app.post("/run-pilot/from-api-v2")
def run_pilot_from_api_v2():
    """
    Pipeline מלא — engine v2.

    קלט (multipart/form-data):
      access_token          : Bearer token ל-API דוד
      api_base              : base URL של API דוד
      mapping               : קובץ XLSX מיפוי (error_code_mapping_v2.xlsx)
      start_date            : (optional) ברירת מחדל 2022-01-01
      top                   : (optional) מקסימום רשומות, ברירת מחדל 10000
      account_manager_email : (optional) פילטר + כתובת מנהלת תיק

    פלט (JSON):
    {
        "ok":             bool,
        "message":        str,
        "stats": {
            "fetched":    int,
            "classified": int,
            "skipped":    int,
            "groups":     int,
            "emails_ok":  int,
            "emails_fail":int,
            "payload_total": int,
            "payload_chunks": int,
        },
        "send_results":   [ SendResult, ... ],
        "update_payload": [ {MISPAR_MEZAHE_RESHUMA, Responsibility, EmailDraftId}, ... ],
        "update_chunks":  [ [chunk], ... ],
    }
    """
    run_start = time.time()
    log.info("=== pipeline v2 התחיל ===")

    # --- auth ---
    err = _check_api_key()
    if err:
        return err

    # --- קלט ---
    access_token = request.form.get("access_token", "").strip().lstrip("=")
    api_base      = request.form.get("api_base", "").strip().lstrip("=")
    if not access_token or not api_base:
        return jsonify({"ok": False, "message": "חסרים שדות access_token ו/או api_base"}), 400

    start_date    = request.form.get("start_date", "2022-01-01").strip().lstrip("=")
    top           = request.form.get("top", "10000").strip().lstrip("=")
    acct_mgr_raw  = request.form.get("account_manager_email", "").strip().lstrip("=")
    acct_mgr_list = [m.strip() for m in acct_mgr_raw.split(",") if m.strip()]
    dry_run       = request.form.get("dry_run", "false").strip().lower() == "true"
    DEV_RECIPIENT = "niravivi@spring-ai.co.il"

    mapping_file = request.files.get("mapping")
    if mapping_file is None:
        return jsonify({"ok": False, "message": "חסר קובץ mapping בבקשה"}), 400

    service_account_info = _load_service_account()
    alert_sender = acct_mgr_list[0] if acct_mgr_list else None

    def _alert(step, err_msg):
        log.error(f"[FAIL] {step}: {err_msg}")
        _send_failure_alert(step, err_msg, service_account_info, sender=alert_sender)

    # --- שלב 1: fetch ---
    log.info(f"שלב 1: fetch — managers={acct_mgr_list}, start_date={start_date}, top={top}")
    t0 = time.time()
    try:
        if acct_mgr_list:
            merged = {}
            for mgr in acct_mgr_list:
                log.info(f"  קורא GetFeedbackData עבור {mgr}")
                recs = _fetch_david_records(api_base, access_token, start_date, top, mgr)
                log.info(f"  {mgr} → {len(recs)} רשומות")
                for r in recs:
                    rid = r.get("MISPAR_MEZAHE_RESHUMA")
                    if rid:
                        merged[rid] = r
            records_list = list(merged.values())
        else:
            records_list = _fetch_david_records(api_base, access_token, start_date, top, "")
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("fetch מ-API של דוד", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה בקריאת API של דוד: {e}"}), 502

    fetched = len(records_list)
    log.info(f"שלב 1 הסתיים: fetched={fetched} ({time.time()-t0:.1f}s)")

    # --- שלב 2: load mapping ---
    log.info("שלב 2: טעינת mapping")
    t0 = time.time()
    try:
        mapping = load_mapping(io.BytesIO(mapping_file.read()))
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("טעינת mapping", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה בטעינת mapping: {e}"}), 400
    log.info(f"שלב 2 הסתיים ({time.time()-t0:.1f}s)")

    # --- שלב 3: classify ---
    log.info("שלב 3: classify")
    t0 = time.time()
    try:
        classified, skipped_list = classify_all(records_list, mapping)
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("סיווג רשומות", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה בסיווג רשומות: {e}"}), 500
    log.info(f"שלב 3 הסתיים: classified={len(classified)} skipped={len(skipped_list)} ({time.time()-t0:.1f}s)")

    # --- שלב 3.5: employer max-counter routing ---
    try:
        classified = apply_employer_max_counter_routing(classified)
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("employer max-counter routing", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה ב-employer routing override: {e}"}), 500

    # --- שלב 4: group ---
    log.info("שלב 4: group")
    t0 = time.time()
    try:
        groups = group_records(classified)
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("קיבוץ רשומות", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה בקיבוץ: {e}"}), 500
    log.info(f"שלב 4 הסתיים: groups={len(groups)} ({time.time()-t0:.1f}s)")

    # --- שלב 5: build emails ---
    log.info("שלב 5: build emails")
    t0 = time.time()
    try:
        email_results, build_skipped = build_all_emails(groups, mapping)
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("בניית מיילים", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה בבניית מיילים: {e}"}), 500
    log.info(f"שלב 5 הסתיים ({time.time()-t0:.1f}s)")

    # --- DEV mode: prefix subjects with [DEV] ---
    if dry_run:
        for _, content in email_results:
            if content:
                content["subject"] = f"[DEV] {content['subject']}"
        log.info(f"[DEV] subject prefix הוחל על {sum(1 for _, c in email_results if c)} מיילים")

    # --- שלב 6: send / create drafts ---
    log.info("שלב 6: יצירת drafts ב-Gmail")
    t0 = time.time()
    default_impersonate = os.environ.get("TEST_GMAIL_IMPERSONATE", acct_mgr_list[0] if acct_mgr_list else "")
    try:
        send_results, send_skipped = send_all_groups(
            email_results,
            service_account_info,
            default_impersonate,
            dry_run_recipient=DEV_RECIPIENT if dry_run else None,
        )
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("יצירת Gmail drafts", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה ביצירת drafts: {e}"}), 500

    gmail_summary = summarize_results(send_results)
    log.info(f"שלב 6 הסתיים: {gmail_summary} ({time.time()-t0:.1f}s)")

    # --- DEV mode: סיום מוקדם — לא מעדכנים SetFeedbackStatus ---
    if dry_run:
        log.info(f"=== [DEV] pipeline הסתיים — {gmail_summary['ok']} מיילים נשלחו ל-{DEV_RECIPIENT}. SetFeedbackStatus לא עודכן. ===")
        return jsonify({
            "ok":      True,
            "message": f"[DEV] pipeline הסתיים — {gmail_summary['ok']} מיילים נשלחו ל-{DEV_RECIPIENT}. SetFeedbackStatus לא עודכן.",
            "dry_run": True,
            "stats": {
                "fetched":       fetched,
                "classified":    len(classified),
                "skipped":       len(skipped_list),
                "groups":        len(groups),
                "emails_ok":     gmail_summary["ok"],
                "emails_fail":   gmail_summary["failed"],
                "total_seconds": round(time.time() - run_start, 1),
            },
        })

    # --- שלב 7: build payload ---
    log.info("שלב 7: build payload")
    t0 = time.time()
    try:
        payload_result = build_payload(send_results, classified, skipped_records=skipped_list)
    except Exception as e:
        err_msg = f"{e}\n{traceback.format_exc()}"
        _alert("בניית payload", err_msg)
        return jsonify({"ok": False, "message": f"שגיאה בבניית payload: {e}"}), 500
    log.info(f"שלב 7 הסתיים: {summarize_payload(payload_result)} ({time.time()-t0:.1f}s)")

    # --- דו"ח סיכום ---
    import base64 as _b64
    run_dt = datetime.utcnow()
    try:
        report_bytes = build_run_report(groups, send_results, skipped_records=skipped_list, raw_records=records_list, run_date=run_dt)
        report_b64 = _b64.b64encode(report_bytes).decode("utf-8")
    except Exception as e:
        log.warning(f"report build failed: {e}")
        report_b64 = None

    # --- דוחות למנהלות תיק ---
    try:
        cm_reports = build_case_manager_reports(groups, send_results, skipped_records=skipped_list, run_date=run_dt)
    except Exception as e:
        log.warning(f"case manager reports failed: {e}")
        cm_reports = []

    total_time = time.time() - run_start
    log.info(f"=== pipeline v2 הסתיים בהצלחה — {total_time:.1f}s כולל ===")

    return jsonify({
        "ok":      True,
        "message": "pipeline v2 הסתיים בהצלחה",
        "stats": {
            "fetched":        fetched,
            "classified":     len(classified),
            "skipped":        len(skipped_list),
            "groups":         len(groups),
            "emails_ok":      gmail_summary["ok"],
            "emails_fail":    gmail_summary["failed"],
            "payload_total":  payload_result["total"],
            "payload_chunks": len(payload_result["chunks"]),
            "total_seconds":  round(total_time, 1),
        },
        "send_results":       send_results,
        "update_payload":     payload_result["payload"],
        "update_chunks":      payload_result["chunks"],
        "report_xlsx_b64":    report_b64,
        "cm_reports":         cm_reports,
    })


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    host = os.environ.get("PILOT_RUNNER_HOST", "127.0.0.1")
    port = int(os.environ.get("PILOT_RUNNER_PORT", "8788"))   # 8788 כדי לא להתנגש עם v1
    app.run(host=host, port=port, debug=False)
