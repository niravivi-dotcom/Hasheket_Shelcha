"""
payload_builder.py
------------------
בונה את ה-update payload ל-SetFeedbackStatus מתוצאות ה-drafts.

מבנה לכל רשומה (הוסכם עם דוד):
{
    "MISPAR_MEZAHE_RESHUMA": str,
    "Responsibility":         str,   # employer / institutional / case_manager / accountant / agent
    "EmailDraftId":           str | None,
}

פלט הפונקציה הראשית:
{
    "payload":  [ {record}, ... ],   # כל הרשומות כרשימה שטוחה
    "chunks":   [ [{record}, ...], ... ],  # מחולק ל-batches של chunk_size
    "total":    int,
    "skipped":  int,   # רשומות שנדחו (draft נכשל)
}
"""

from mapping_loader import RESP_CASE_MANAGER


DEFAULT_CHUNK_SIZE = 1000


# =============================================================================
# Public API
# =============================================================================

def build_payload(send_results, classified_records, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    בונה את payload ל-SetFeedbackStatus.

    send_results       : רשימת SendResult מ-gmail_sender.send_all_groups()
    classified_records : רשימת ClassifiedRecord מ-record_classifier.classify_all()
    chunk_size         : גודל batch לשליחה (ברירת מחדל 1000)

    מחזיר dict עם payload, chunks, total, skipped.
    """
    # lookup מהיר: record_id → responsibility
    resp_lookup = {
        r["record_id"]: r.get("responsibility", RESP_CASE_MANAGER)
        for r in classified_records
    }

    payload = []
    skipped = 0

    for result in send_results:
        if not result:
            skipped += 1
            continue

        draft_id = result.get("draft_id")   # None אם נכשל או Counter=0

        # אם ה-draft נכשל — דלג (אל תדווח לדוד)
        if not result.get("ok"):
            skipped += len(result.get("record_ids", []))
            continue

        for record_id in result.get("record_ids", []):
            responsibility = resp_lookup.get(record_id, RESP_CASE_MANAGER)
            payload.append({
                "MISPAR_MEZAHE_RESHUMA": record_id,
                "Responsibility":        responsibility,
                "EmailDraftId":          draft_id,
            })

    chunks = _chunk(payload, chunk_size)

    return {
        "payload": payload,
        "chunks":  chunks,
        "total":   len(payload),
        "skipped": skipped,
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
