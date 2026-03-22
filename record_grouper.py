"""
record_grouper.py
-----------------
מקבץ רשומות מסווגות לקבוצות מייל.

כללי קיבוץ:
  מוסדי-1 / מוסדי-2 : FundInstitutionIdentityNumber + CustomerNumber + email_format
  מעסיק / רו"ח / סוכן: CustomerNumber + to_role
  מנהלת תיק          : קבוצה אחת כוללת (מייל אחד עם Excel של הכל)

כל EmailGroup מכיל:
  group_key   – מפתח ייחודי לקיבוץ
  email_format
  records     – רשימת ClassifiedRecord
  meta        – מידע עזר (שם מעסיק, שם גוף מוסדי וכו')
"""

from collections import defaultdict
from mapping_loader import (
    FORMAT_MOSADI_1, FORMAT_MOSADI_2,
    FORMAT_EMPLOYER, FORMAT_CASE_MGR,
    RESP_CASE_MANAGER,
)


def group_records(classified_records):
    """
    מקבץ רשימת ClassifiedRecord לרשימת EmailGroup.

    מחזיר רשימה של dicts:
    {
        "group_key":    str,
        "email_format": str,
        "records":      [ClassifiedRecord, ...],
        "meta":         { ... },
    }
    """
    buckets_mosadi  = defaultdict(list)   # מוסדי-1 / מוסדי-2
    buckets_employer = defaultdict(list)  # מעסיק / רו"ח / סוכן
    case_mgr_records = []                 # מנהלת תיק — הכל יחד

    for rec in classified_records:
        fmt = rec.get("email_format", "")

        if fmt in (FORMAT_MOSADI_1, FORMAT_MOSADI_2):
            fund_id  = rec.get("fund_institution_id") or "UNKNOWN_FUND"
            customer = rec.get("customer_number") or "UNKNOWN_CUSTOMER"
            key = f"{fmt}|{fund_id}|{customer}"
            buckets_mosadi[key].append(rec)

        elif fmt == FORMAT_EMPLOYER or rec.get("responsibility") in ("employer", "accountant", "agent"):
            customer = rec.get("customer_number") or "UNKNOWN_CUSTOMER"
            to_role  = rec.get("to_role") or "default"
            key = f"{FORMAT_EMPLOYER}|{customer}|{to_role}"
            buckets_employer[key].append(rec)

        elif fmt == FORMAT_CASE_MGR or rec.get("responsibility") == RESP_CASE_MANAGER:
            case_mgr_records.append(rec)

        else:
            # fallback — מנהלת תיק
            case_mgr_records.append(rec)

    groups = []

    # --- מוסדי ---
    for key, records in buckets_mosadi.items():
        parts = key.split("|", 2)
        email_format = parts[0]
        fund_id      = parts[1] if len(parts) > 1 else ""
        customer     = parts[2] if len(parts) > 2 else ""
        sample = records[0]
        groups.append({
            "group_key":    key,
            "email_format": email_format,
            "records":      records,
            "meta": {
                "fund_institution_id":   fund_id,
                "fund_institution_name": sample.get("fund_institution_name"),
                "customer_number":       customer,
                "employer_name":         sample.get("employer_name"),
                "to_role":               sample.get("to_role"),
                "cc_role":               sample.get("cc_role"),
                "mail_subject_template": sample.get("mail_subject_template"),
            },
        })

    # --- מעסיק / רו"ח / סוכן ---
    for key, records in buckets_employer.items():
        parts = key.split("|", 2)
        customer = parts[1] if len(parts) > 1 else ""
        to_role  = parts[2] if len(parts) > 2 else ""
        sample = records[0]
        groups.append({
            "group_key":    key,
            "email_format": FORMAT_EMPLOYER,
            "records":      records,
            "meta": {
                "customer_number": customer,
                "employer_name":   sample.get("employer_name"),
                "to_role":         to_role,
                "cc_role":         sample.get("cc_role"),
                # כתובות מייל (stub — יגיעו מדוד)
                "to_email":        _resolve_email(sample, to_role),
                "cc_email":        _resolve_email(sample, sample.get("cc_role")),
            },
        })

    # --- מנהלת תיק ---
    if case_mgr_records:
        groups.append({
            "group_key":    FORMAT_CASE_MGR,
            "email_format": FORMAT_CASE_MGR,
            "records":      case_mgr_records,
            "meta": {
                "to_role": "מנהלת תיק",
                "cc_role": None,
                # TODO: to_email יגיע מעידו (חלוקת To/CC)
                "to_email": None,
                "cc_email": None,
            },
        })

    return groups


def _resolve_email(record, role):
    """
    ממיר תפקיד לכתובת מייל מהרשומה.
    stub — כל השדות יגיעו מדוד.
    """
    if not role:
        return None
    role_map = {
        "סוכן":                record.get("email_agent"),
        'רו"ח':                record.get("email_accountant"),
        "איש קשר 1 מעסיק":    record.get("email_contact1"),
        "איש קשר 2 מעסיק":    record.get("email_contact2"),
        "מנהלת תיק":           None,   # נקבע ברמת ה-runner
    }
    return role_map.get(role, record.get("contact_email"))


def summarize_groups(groups):
    """מחזיר סיכום קצר לצורכי logging."""
    return [
        {
            "group_key":    g["group_key"],
            "email_format": g["email_format"],
            "record_count": len(g["records"]),
        }
        for g in groups
    ]
