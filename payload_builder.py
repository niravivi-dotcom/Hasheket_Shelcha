"""
payload_builder.py
------------------
בונה את ה-update payload ל-SetFeedbackStatusBatch מתוצאות ה-drafts.

מבנה לכל רשומה (schema מאושר מדוד 2026-04-01):
{
    "MISPAR_MEZAHE_RESHUMA": str,          # required
    "TreatmentStatus":        str,          # required
    "Counter":                int,          # required
    "Responsibility":         str | null,   # optional, max 50
    "EmailFormat":            str | null,   # optional, max 50
    "RoutingReason":          str | null,   # optional, max 100
    "EmailDraftId":           str | null,   # optional, max 100
    "SkippedReason":          str | null,   # optional, max 100
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

# גודלי שדות מקסימליים (מאושר מדוד 2026-04-01)
_MAX_LEN = {
    "Responsibility": 50,
    "EmailFormat":    50,
    "RoutingReason":  100,
    "EmailDraftId":   100,
    "SkippedReason":  100,
}

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
    "default":             "ברירת מחדל",
    "pre_condition_true":  "תנאי מוקדם חיובי",
    "pre_condition_false": "תנאי מוקדם שלילי",
    "override_1":          "עקיפה ראשית",
    "override_2":          "עקיפה משנית",
    "unknown_code":        "קוד לא ממופה",
    "קוד שגיאה חסר":      "קוד שגיאה חסר",
}


def _routing_reason_he(path):
    """ממיר routing_path לעברית. מטפל גם ב-escalation_cN."""
    if path and path.startswith("escalation_c"):
        return "הסלמה"
    return ROUTING_REASON_HE.get(path, path)


def _trunc(value, field_name):
    """חותך מחרוזת לגודל מקסימלי של השדה. מחזיר None אם הקלט ריק."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    max_len = _MAX_LEN.get(field_name, 255)
    return s[:max_len]


def _treatment_status(responsibility, counter):
    """מייצר מחרוזת TreatmentStatus לפי אחריות ומונה שבועות."""
    try:
        c = int(counter) if counter is not None else 0
    except (ValueError, TypeError):
        c = 0

    if c == 0:
        return "שגיאה חדשה - המתנה לשבוע הבא"

    prefix_map = {
        "institutional": "גוף מוסדי",
        "employer":      "מעסיק",
        "case_manager":  "מנהלת תיק",
        "accountant":    'רו"ח',
        "agent":         "סוכן",
    }
    prefix = prefix_map.get(responsibility, responsibility or "גורם לא ידוע")

    if c == 1:
        return f"נשלח מייל ל{prefix} שבוע 1"
    if c == 2:
        return f"נשלח מייל תזכורת ל{prefix} שבוע 2"
    if c == 3:
        return f"הסלמה למנהלת תיק (שבוע 3)"
    if c == 4:
        return f"הסלמה למנהלת תיק + מנהלת ראשית (שבוע 4)"
    return f"הסלמה להנהלה בכירה (שבוע {c})"


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
            rec          = classified_lookup.get(record_id, {})
            resp         = rec.get("responsibility")
            counter      = rec.get("counter_weeks")
            path         = rec.get("routing_path")
            payload.append({
                "MISPAR_MEZAHE_RESHUMA": record_id,
                "TreatmentStatus":       _treatment_status(resp, counter),
                "Counter":               int(counter) if counter is not None else 0,
                "Responsibility":        _trunc(RESPONSIBILITY_HE.get(resp, resp), "Responsibility"),
                "EmailFormat":           _trunc(rec.get("email_format"), "EmailFormat"),
                "RoutingReason":         _trunc(_routing_reason_he(path), "RoutingReason"),
                "EmailDraftId":          _trunc(draft_id, "EmailDraftId"),
                "SkippedReason":         None,
            })

    # --- רשומות שדולגו ---
    for raw_rec, reason in (skipped_records or []):
        record_id = raw_rec.get("MISPAR_MEZAHE_RESHUMA") or raw_rec.get("mispar_mezahe_reshuma", "")
        payload.append({
            "MISPAR_MEZAHE_RESHUMA": record_id,
            "TreatmentStatus":       "דולג",
            "Counter":               0,
            "Responsibility":        None,
            "EmailFormat":           None,
            "RoutingReason":         None,
            "EmailDraftId":          None,
            "SkippedReason":         _trunc(reason, "SkippedReason"),
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
