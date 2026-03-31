"""
payload_builder.py
------------------
בונה את ה-update payload ל-SetFeedbackStatusBatch מתוצאות ה-drafts.

מבנה לכל רשומה:
{
    "MISPAR_MEZAHE_RESHUMA": str,
    "Responsibility":         str | null,   # עברית
    "EmailFormat":            str | null,   # עברית
    "RoutingReason":          str | null,   # עברית
    "EscalationLevel":        int | null,   # counter_weeks
    "EmailDraftId":           str | null,
    "SkippedReason":          str | null,   # null אם טופלה
}

פלט הפונקציה הראשית:
{
    "payload":  [ {record}, ... ],   # כל הרשומות כרשימה שטוחה (טופלו + דולגו)
    "chunks":   [ [{record}, ...], ... ],  # מחולק ל-batches של chunk_size
    "total":    int,
    "skipped":  int,
}
"""

from mapping_loader import RESP_CASE_MANAGER


DEFAULT_CHUNK_SIZE = 1000

# המרת אחריות מאנגלית לעברית
RESPONSIBILITY_HE = {
    "institutional": "מוסדי",
    "employer":      "מעסיק",
    "case_manager":  "מנהלת תיק",
    "accountant":    'רו"ח',
    "agent":         "סוכן",
}

# המרת routing_path לעברית
ROUTING_REASON_HE = {
    "default":           "ברירת מחדל",
    "pre_condition_true":  "תנאי מוקדם חיובי",
    "pre_condition_false": "תנאי מוקדם שלילי",
    "override_1":        "עקיפה ראשית",
    "override_2":        "עקיפה משנית",
    "unknown_code":      "קוד לא ממופה",
    "קוד שגיאה חסר":    "קוד שגיאה חסר",
}


def _routing_reason_he(path):
    """ממיר routing_path לעברית. מטפל גם ב-escalation_cN."""
    if path and path.startswith("escalation_c"):
        return "הסלמה"
    return ROUTING_REASON_HE.get(path, path)


# =============================================================================
# Public API
# =============================================================================

def build_payload(send_results, classified_records, skipped_records=None, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    בונה את payload ל-SetFeedbackStatusBatch.

    send_results       : רשימת SendResult מ-gmail_sender.send_all_groups()
    classified_records : רשימת ClassifiedRecord מ-record_classifier.classify_all()
    skipped_records    : רשימת (record, reason) מ-classify_all() — לדיווח לדוד
    chunk_size         : גודל batch לשליחה (ברירת מחדל 1000)
    """
    # lookup מהיר: record_id → classified data
    classified_lookup = {
        r["record_id"]: r
        for r in classified_records
    }

    payload = []
    skipped_count = 0

    # --- רשומות שטופלו ---
    for result in send_results:
        if not result:
            skipped_count += 1
            continue

        draft_id = result.get("draft_id")

        if not result.get("ok"):
            skipped_count += len(result.get("record_ids", []))
            continue

        for record_id in result.get("record_ids", []):
            rec = classified_lookup.get(record_id, {})
            path = rec.get("routing_path")
            payload.append({
                "MISPAR_MEZAHE_RESHUMA": record_id,
                "Responsibility":        RESPONSIBILITY_HE.get(rec.get("responsibility"), rec.get("responsibility")),
                "EmailFormat":           rec.get("email_format"),
                "RoutingReason":         _routing_reason_he(path),
                "EscalationLevel":       rec.get("counter_weeks"),
                "EmailDraftId":          draft_id,
                "SkippedReason":         None,
            })

    # --- רשומות שדולגו ---
    for raw_rec, reason in (skipped_records or []):
        record_id = raw_rec.get("MISPAR_MEZAHE_RESHUMA") or raw_rec.get("mispar_mezahe_reshuma", "")
        payload.append({
            "MISPAR_MEZAHE_RESHUMA": record_id,
            "Responsibility":        None,
            "EmailFormat":           None,
            "RoutingReason":         None,
            "EscalationLevel":       None,
            "EmailDraftId":          None,
            "SkippedReason":         reason,
        })

    chunks = _chunk(payload, chunk_size)

    return {
        "payload": payload,
        "chunks":  chunks,
        "total":   len(payload),
        "skipped": skipped_count,
    }


# =============================================================================
# Helpers
# =============================================================================

def _chunk(lst, size):
    """מחלק רשימה ל-batches של עד size איברים."""
    return [lst[i: i + size] for i in range(0, len(lst), size)]


def summarize_payload(payload_result):
    """סיכום קצר לצורכי logging."""
    return {
        "total":   payload_result["total"],
        "chunks":  len(payload_result["chunks"]),
        "skipped": payload_result["skipped"],
    }
