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
import traceback
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, jsonify, request

APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_DIR))

# engine v2 modules
from mapping_loader    import load_mapping
from record_classifier import classify_all
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
        print(f"[WARN] לא הצלחתי לטעון GMAIL_SERVICE_ACCOUNT_B64: {e}")
        return None


def _check_api_key():
    """מוודא X-API-Key. מחזיר None אם תקין, response אם לא."""
    secret = os.environ.get("API_SECRET_KEY")
    if secret and request.headers.get("X-API-Key") != secret:
        return jsonify({"ok": False, "message": "Unauthorized"}), 401
    return None


def _fetch_david_records(api_base, access_token, start_date, top, acct_mgr):
    """קורא GetFeedbackData מ-API של דוד. מחזיר list."""
    body = {"StartDate": start_date, "top": int(top)}
    if acct_mgr:
        body["AccountManagerEmail"] = acct_mgr

    resp = requests.post(
        f"{api_base}/services/AutomationFeedback/GetFeedbackData",
        headers={
            "Authorization":  f"Bearer {access_token}",
            "Content-Type":   "application/json",
        },
        json=body,
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("תגובת API של דוד אינה JSON array")
    return data


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
    # --- auth ---
    err = _check_api_key()
    if err:
        return err

    # --- קלט ---
    access_token = request.form.get("access_token", "").strip().lstrip("=")
    api_base      = request.form.get("api_base", "").strip().lstrip("=")
    if not access_token or not api_base:
        return jsonify({"ok": False, "message": "חסרים שדות access_token ו/או api_base"}), 400

    start_date = request.form.get("start_date", "2022-01-01").strip().lstrip("=")
    top        = request.form.get("top", "10000").strip().lstrip("=")
    acct_mgr   = request.form.get("account_manager_email", "").strip().lstrip("=")

    mapping_file = request.files.get("mapping")
    if mapping_file is None:
        return jsonify({"ok": False, "message": "חסר קובץ mapping בבקשה"}), 400

    # --- שלב 1: fetch ---
    try:
        records_list = _fetch_david_records(api_base, access_token, start_date, top, acct_mgr)
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בקריאת API של דוד: {e}"}), 502

    fetched = len(records_list)
    print(f"[v2] fetched={fetched} records")

    # --- שלב 2: load mapping ---
    try:
        mapping = load_mapping(io.BytesIO(mapping_file.read()))
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בטעינת mapping: {e}"}), 400

    # --- שלב 3: classify ---
    try:
        classified, skipped_list = classify_all(records_list, mapping)
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בסיווג רשומות: {e}"}), 500

    print(f"[v2] classified={len(classified)} skipped={len(skipped_list)}")

    # --- שלב 4: group ---
    try:
        groups = group_records(classified)
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בקיבוץ: {e}"}), 500

    print(f"[v2] groups={len(groups)} — {summarize_groups(groups)}")

    # --- שלב 5: build emails ---
    try:
        email_results, build_skipped = build_all_emails(groups, mapping)
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בבניית מיילים: {e}"}), 500

    # --- שלב 6: send / create drafts ---
    service_account_info = _load_service_account()
    default_impersonate  = os.environ.get("TEST_GMAIL_IMPERSONATE", acct_mgr or "")

    try:
        send_results, send_skipped = send_all_groups(
            email_results,
            service_account_info,
            default_impersonate,
        )
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה ביצירת drafts: {e}"}), 500

    gmail_summary = summarize_results(send_results)
    print(f"[v2] gmail: {gmail_summary}")

    # --- שלב 7: build payload ---
    try:
        payload_result = build_payload(send_results, classified, skipped_records=skipped_list)
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בבניית payload: {e}"}), 500

    print(f"[v2] payload: {summarize_payload(payload_result)}")

    # --- דו"ח סיכום ---
    import base64 as _b64
    run_dt = datetime.utcnow()
    try:
        report_bytes = build_run_report(groups, send_results, skipped_records=skipped_list, raw_records=records_list, run_date=run_dt)
        report_b64 = _b64.b64encode(report_bytes).decode("utf-8")
    except Exception as e:
        print(f"[v2] report build failed: {e}")
        report_b64 = None

    # --- דוחות למנהלות תיק ---
    try:
        cm_reports = build_case_manager_reports(groups, send_results, skipped_records=skipped_list, run_date=run_dt)
    except Exception as e:
        print(f"[v2] case manager reports failed: {e}")
        cm_reports = []

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
