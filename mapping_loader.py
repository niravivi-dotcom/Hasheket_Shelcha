"""
mapping_loader.py
-----------------
קורא את error_code_mapping_v2.xlsx ומחזיר lookup structures לשאר המודולים.
"""

import io
import pandas as pd


# שמות גיליונות
SHEET_ERRORS       = "קודי שגיאה"
SHEET_ESCALATION   = "מדיניות הסלמה"
SHEET_STATUSES     = "סטטוסים לעיבוד"
SHEET_TEMPLATES    = "תבניות מייל"

# ערכי פורמט מייל (מפתח לגיליון 4)
FORMAT_MOSADI_1    = "מוסדי-1"
FORMAT_MOSADI_2    = "מוסדי-2"
FORMAT_EMPLOYER    = "מעסיק"
FORMAT_CASE_MGR    = "מנהלת תיק"
FORMAT_EXCLUDED    = "מוחרג"

# ערכי אחריות (לשימוש ב-SetFeedbackStatus)
RESP_INSTITUTIONAL = "institutional"
RESP_EMPLOYER      = "employer"
RESP_CASE_MANAGER  = "case_manager"
RESP_ACCOUNTANT    = "accountant"
RESP_AGENT         = "agent"

# מיפוי: ערך עברי מהקובץ → ערך אנגלי ל-API
RESPONSIBILITY_MAP = {
    "מוסדי":        RESP_INSTITUTIONAL,
    "מעסיק":        RESP_EMPLOYER,
    "מנהלת תיק":   RESP_CASE_MANAGER,
    'רו"ח':         RESP_ACCOUNTANT,
    "סוכן":         RESP_AGENT,
}

# TODO: כאשר שדה סוג מוצר יגיע מדוד — להוסיף כאן את קוד קרן פנסיה
PENSION_FUND_PRODUCT_CODE = None  # stub


def _clean(val):
    """מחזיר string נקי, או None אם ריק/NaN."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None


def load_mapping(mapping_source):
    """
    טוען את קובץ המיפוי ומחזיר dict עם ארבעת המבנים הדרושים.

    mapping_source: נתיב לקובץ (str/Path) או bytes/BytesIO (כשמגיע מ-n8n).

    מחזיר:
    {
        "error_codes": { int: {...} },         # lookup לפי קוד שגיאה
        "email_templates": { str: {...} },     # lookup לפי פורמט מייל
        "statuses_to_process": [str, ...],     # סטטוסים לעיבוד
        "escalation_policy": { int: {...} },   # מדיניות הסלמה לפי Counter
    }
    """
    if isinstance(mapping_source, (bytes, bytearray)):
        mapping_source = io.BytesIO(mapping_source)

    xl = pd.ExcelFile(mapping_source)

    error_codes     = _load_error_codes(xl)
    email_templates = _load_email_templates(xl)
    statuses        = _load_statuses(xl)
    escalation      = _load_escalation(xl)

    return {
        "error_codes":      error_codes,
        "email_templates":  email_templates,
        "statuses_to_process": statuses,
        "escalation_policy": escalation,
    }


def _load_error_codes(xl):
    df = xl.parse(SHEET_ERRORS, dtype=str)
    df.columns = df.columns.str.strip()

    # מיפוי עמודות לפי שם (גמיש — קובץ עלול להתעדכן)
    col_map = {
        "קוד שגיאה":              "code",
        "תיאור שגיאה":            "description",
        "מוחרג":                  "excluded",
        "פורמט מייל":             "email_format",
        "אחריות ברירת מחדל":      "responsibility",
        "CC ברירת מחדל":          "cc_responsibility",
        "OverrideMailRecipients":  "override_recipients",
        "CC Override 1":           "cc_override_1",
        "OverrideMailRecipients 2":"override_recipients_2",
        "CC Override 2":           "cc_override_2",
        "נושא מייל":              "mail_subject",
        "הסבר למעסיק":            "explanation_employer",
        "הסבר מנהלת תיק":         "explanation_case_manager",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    result = {}
    for _, row in df.iterrows():
        raw_code = _clean(row.get("code"))
        if raw_code is None:
            continue
        try:
            code = int(float(raw_code))
        except (ValueError, TypeError):
            continue

        responsibility_he = _clean(row.get("responsibility")) or ""
        result[code] = {
            "description":            _clean(row.get("description")),
            "excluded":               str(row.get("excluded", "לא")).strip() == "כן",
            "email_format":           _clean(row.get("email_format")) or FORMAT_EXCLUDED,
            "responsibility":         RESPONSIBILITY_MAP.get(responsibility_he, responsibility_he),
            "responsibility_he":      responsibility_he,
            "cc_responsibility":      _clean(row.get("cc_responsibility")),
            "override_recipients":    _clean(row.get("override_recipients")),
            "cc_override_1":          _clean(row.get("cc_override_1")),
            "override_recipients_2":  _clean(row.get("override_recipients_2")),
            "cc_override_2":          _clean(row.get("cc_override_2")),
            "mail_subject":           _clean(row.get("mail_subject")),
            "explanation_employer":   _clean(row.get("explanation_employer")),
            "explanation_case_manager": _clean(row.get("explanation_case_manager")),
        }
    return result


def _load_email_templates(xl):
    df = xl.parse(SHEET_TEMPLATES, dtype=str)
    df.columns = df.columns.str.strip()

    col_map = {
        "סוג נמען":        "recipient_type",
        "פורמט":           "format",
        "קודים רלוונטיים": "relevant_codes",
        "נושא":            "subject",
        "גוף מייל":        "body",
        "קבצים מצורפים":   "attachments",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    result = {}
    for _, row in df.iterrows():
        fmt = _clean(row.get("format")) or _clean(row.get("recipient_type"))
        if fmt is None:
            continue
        result[fmt] = {
            "recipient_type": _clean(row.get("recipient_type")),
            "relevant_codes": _clean(row.get("relevant_codes")),
            "subject":        _clean(row.get("subject")),
            "body":           _clean(row.get("body")),
            "attachments":    _clean(row.get("attachments")),
        }
    return result


def _load_statuses(xl):
    df = xl.parse(SHEET_STATUSES, dtype=str)
    df.columns = df.columns.str.strip()

    status_col   = next((c for c in df.columns if "סטטוס" in c), None)
    process_col  = next((c for c in df.columns if "לעיבוד" in c), None)

    if status_col is None or process_col is None:
        return []

    return [
        str(row[status_col]).strip()
        for _, row in df.iterrows()
        if _clean(row.get(status_col)) and str(row.get(process_col, "")).strip() == "כן"
    ]


def _load_escalation(xl):
    df = xl.parse(SHEET_ESCALATION, dtype=str)
    df.columns = df.columns.str.strip()

    counter_col = next((c for c in df.columns if "counter" in c.lower() or "מונה" in c), None)
    action_col  = next((c for c in df.columns if "פעולה" in c), None)
    to_col      = next((c for c in df.columns if "נמען" in c), None)

    if counter_col is None:
        return {}

    result = {}
    for _, row in df.iterrows():
        raw = _clean(row.get(counter_col))
        if raw is None:
            continue
        # תמיכה ב-"5+" → שומר כ-5
        raw_key = raw.replace("+", "").strip()
        try:
            key = int(float(raw_key))
        except (ValueError, TypeError):
            continue
        result[key] = {
            "counter_raw": raw,
            "action":      _clean(row.get(action_col)) if action_col else None,
            "recipient":   _clean(row.get(to_col)) if to_col else None,
            "is_max":      "+" in raw,  # True עבור "5+" = כל counter >= 5
        }
    return result


# --- Helper: quick lookup ---

def get_error_rule(mapping, error_code):
    """מחזיר את חוקי הקוד מה-mapping, או None אם לא קיים."""
    try:
        code = int(error_code)
    except (ValueError, TypeError):
        return None
    return mapping["error_codes"].get(code)


def is_excluded(mapping, error_code):
    """True אם הקוד מוחרג מהתהליך."""
    rule = get_error_rule(mapping, error_code)
    return rule is None or rule.get("excluded", False)


def get_email_format(mapping, error_code):
    """מחזיר את פורמט המייל לפי קוד שגיאה."""
    rule = get_error_rule(mapping, error_code)
    if rule is None or rule.get("excluded"):
        return FORMAT_EXCLUDED
    return rule.get("email_format", FORMAT_EXCLUDED)


def get_responsibility(mapping, error_code):
    """מחזיר ערך אחריות באנגלית לפי קוד שגיאה."""
    rule = get_error_rule(mapping, error_code)
    if rule is None:
        return None
    return rule.get("responsibility")
