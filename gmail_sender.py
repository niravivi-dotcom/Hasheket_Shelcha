"""
gmail_sender.py
---------------
יוצר Gmail drafts (או שולח) לכל EmailContent.

כללים:
  מוסדי-1 / מוסדי-2 / מעסיק : draft בלבד (gmail.compose scope)
  מנהלת תיק                  : stub כ-draft עד הוספת gmail.send scope

מחזיר רשימת SendResult dicts:
{
    "group_key":    str,
    "email_format": str,
    "ok":           bool,
    "draft_id":     str | None,   # → payload_builder.py
    "impersonate":  str,          # תיבה שבה נוצר ה-draft
    "record_ids":   [str, ...],   # מזהי רשומות לעדכון SetFeedbackStatus
    "error":        str | None,
}
"""

import os
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from concurrent.futures import ThreadPoolExecutor, as_completed

from mapping_loader import FORMAT_CASE_MGR

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.compose"]


# =============================================================================
# Public API
# =============================================================================

def send_all_groups(email_results, service_account_info, default_impersonate, max_workers=20):
    """
    מעבד את כל הקבוצות ויוצר drafts ב-Gmail.

    email_results       : רשימת (group, email_content) tuples מ-email_builder
    service_account_info: dict של service account (מ-env var GMAIL_SERVICE_ACCOUNT_B64)
    default_impersonate : כתובת מייל ברירת מחדל לחיקוי (override ע"י TEST_GMAIL_IMPERSONATE)
    max_workers         : מקבילות (ברירת מחדל 20)

    מחזיר (send_results, skipped_count)
    """
    # לשלב טסט: TEST_GMAIL_IMPERSONATE דורס את כל תיבות ה-to
    test_override = os.environ.get("TEST_GMAIL_IMPERSONATE", "").strip()

    tasks = []
    for group, email_content in email_results:
        impersonate = test_override or _resolve_impersonate(email_content, default_impersonate)
        tasks.append((group, email_content, impersonate))

    results = [None] * len(tasks)
    skipped = 0

    if not service_account_info:
        # מצב פיתוח / בדיקה בלי service account — מחזיר stub
        return [_stub_result(g, ec) for g, ec, _ in tasks], 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_process_one, group, email_content, impersonate, service_account_info): idx
            for idx, (group, email_content, impersonate) in enumerate(tasks)
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                group = tasks[idx][0]
                results[idx] = _error_result(group, str(e))
                skipped += 1

    return results, skipped


# =============================================================================
# Internal
# =============================================================================

def _process_one(group, email_content, impersonate, service_account_info):
    """יוצר draft אחד ב-Gmail. מחזיר SendResult."""
    to_email = email_content.get("to_email")

    if not impersonate:
        return _error_result(group, "impersonate_email חסר — לא ניתן ליצור draft")

    # אם to_email חסר או לא תקין (לא כתובת מייל) — fallback ל-impersonate
    if not to_email or "@" not in str(to_email):
        to_email = impersonate

    try:
        service = _get_gmail_service(service_account_info, impersonate)
        raw     = _build_mime(email_content)
        created = service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw}},
        ).execute()

        return {
            "group_key":    group["group_key"],
            "email_format": group["email_format"],
            "ok":           True,
            "draft_id":     created.get("id"),
            "impersonate":  impersonate,
            "record_ids":   [r["record_id"] for r in group.get("records", [])],
            "error":        None,
        }

    except Exception as e:
        return _error_result(group, str(e), impersonate=impersonate)


def _get_gmail_service(service_account_info, impersonate_email):
    from google.oauth2 import service_account as sa_module
    from googleapiclient.discovery import build

    creds = sa_module.Credentials.from_service_account_info(
        service_account_info, scopes=GMAIL_SCOPES
    ).with_subject(impersonate_email)
    return build("gmail", "v1", credentials=creds)


def _build_mime(email_content):
    """בונה MIME message מ-EmailContent ומחזיר base64url string."""
    msg = MIMEMultipart()
    msg["to"]      = email_content.get("to_email", "")
    msg["subject"] = email_content.get("subject", "")

    cc = email_content.get("cc_email")
    if cc:
        msg["cc"] = cc

    msg.attach(MIMEText(email_content.get("body_html", ""), "html", "utf-8"))

    for att in email_content.get("attachments", []):
        part = MIMEBase(*att["mimetype"].split("/", 1))
        part.set_payload(att["data"])
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            "attachment",
            filename=att["filename"],
        )
        msg.attach(part)

    return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")


def _resolve_impersonate(email_content, default_impersonate):
    """
    קובע את תיבת המייל שמתוכה ייצא ה-draft.
    כרגיל: default_impersonate (מנהלת תיק / שולח מורשה).
    """
    return default_impersonate or email_content.get("to_email")


def _stub_result(group, email_content):
    """מצב פיתוח — מחזיר תוצאה עם draft_id=None."""
    return {
        "group_key":    group["group_key"],
        "email_format": group["email_format"],
        "ok":           True,
        "draft_id":     None,
        "impersonate":  "STUB",
        "record_ids":   [r["record_id"] for r in group.get("records", [])],
        "error":        None,
    }


def _error_result(group, error_msg, impersonate=None):
    return {
        "group_key":    group.get("group_key"),
        "email_format": group.get("email_format"),
        "ok":           False,
        "draft_id":     None,
        "impersonate":  impersonate,
        "record_ids":   [r["record_id"] for r in group.get("records", [])],
        "error":        error_msg,
    }


# =============================================================================
# Helpers for runner
# =============================================================================

def summarize_results(send_results):
    """מחזיר סיכום קצר לצורכי logging."""
    ok    = sum(1 for r in send_results if r and r.get("ok"))
    fail  = sum(1 for r in send_results if r and not r.get("ok"))
    total = len(send_results)
    return {"total": total, "ok": ok, "failed": fail}
