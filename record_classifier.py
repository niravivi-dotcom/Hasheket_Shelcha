"""
record_classifier.py
--------------------
מסווג רשומה בודדת מה-API של דוד לפי קובץ המיפוי.
מחזיר: אחריות, פורמט מייל, נמענים (תפקידים), האם לעבד.

לוגיקת ניתוב:
  has_previous_positive = LastPositive_CHODESH_MASKORET IS NOT NULL
  ┌─ TRUE  → DefaultResponsibility path (מוסדי)
  └─ FALSE → Override path:
       1. OverrideMailRecipients (אם יש כתובת מייל לתפקיד הזה ברשומה)
       2. OverrideMailRecipients 2 (fallback)
       3. Default (fallback אחרון)
"""

import pandas as pd
from mapping_loader import (
    FORMAT_EXCLUDED, FORMAT_CASE_MGR,
    RESP_CASE_MANAGER, RESPONSIBILITY_MAP,
    PENSION_FUND_PRODUCT_CODE,  # stub
)

# שדות API מדוד — קיימים עכשיו
FIELD_RECORD_ID        = "MISPAR_MEZAHE_RESHUMA"
FIELD_CUSTOMER         = "CustomerNumber"
FIELD_ERROR_CODE       = "ErrorCodeV4Id"
FIELD_COUNTER          = "OnlyOnStatusChange_DatesDiffInWeeks"
FIELD_FEEDBACK_STATUS  = "FeedbackStatus"
FIELD_LAST_POSITIVE    = "LastPositive_CHODESH_MASKORET"
FIELD_CHODESH          = "CHODESH_MASKORET"
FIELD_CONTACT_EMAIL    = "CustomerContactEmail"
FIELD_TIK_MISLAKA      = "TikMislaka"
FIELD_ORIGINAL_FILE    = "OriginalFileName"
FIELD_FUND_NAME        = "FundInstitutionName"
FIELD_FUND_ID          = "FundInstitutionIdentityNumber"
FIELD_FUND_TYPE        = "FundInstitutionType"
FIELD_STATUS_DESC      = "StatusDescription"

# TODO: שדות שיגיעו מדוד בעתיד
FIELD_FULL_NAME        = "FullName"           # שם מלא עובד — ממתין לדוד
FIELD_AGENT_EMAIL      = "AgentEmail"         # מייל סוכן — ממתין לדוד
FIELD_ACCOUNTANT_EMAIL = "AccountantEmail"    # מייל רו"ח — ממתין לדוד
FIELD_CONTACT1_EMAIL   = "Contact1Email"      # איש קשר 1 מעסיק — ממתין לדוד
FIELD_CONTACT2_EMAIL   = "Contact2Email"      # איש קשר 2 מעסיק — ממתין לדוד
FIELD_PRODUCT_TYPE     = "ProductTypeCode"    # קוד סוג מוצר — ממתין לדוד
FIELD_EMPLOYER_NAME    = "EmployerName"       # שם מעסיק — ממתין לדוד
FIELD_EMPLOYEE_ID      = "MISPAR_MEZAHE_OVED" # מ.ז. עובד

# מיפוי: ערך תפקיד בקובץ → שדה API המכיל את הכתובת
ROLE_TO_FIELD = {
    "סוכן":                FIELD_AGENT_EMAIL,
    'רו"ח':                FIELD_ACCOUNTANT_EMAIL,
    "איש קשר 1 מעסיק":    FIELD_CONTACT1_EMAIL,
    "איש קשר 2 מעסיק":    FIELD_CONTACT2_EMAIL,
    "מנהלת תיק":           None,   # נפתר ברמת ה-runner לפי מנהלת התיק האמיתית
}


def _get(record, field, default=None):
    val = record.get(field, default)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return val


def _has_value(record, field):
    """True אם השדה קיים ברשומה עם ערך לא-ריק."""
    val = _get(record, field)
    return val is not None and str(val).strip() not in ("", "nan", "None")


def _check_pre_mail_condition(record, rule):
    """
    בודק את תנאי PreMailCondition לרשומה.
    מחזיר True אם התנאי מתקיים (→ DefaultResponsibility),
             False אם לא מתקיים (→ Override),
             None אם אין תנאי לקוד הזה.
    """
    condition_field = rule.get("pre_mail_condition_field")
    if not condition_field:
        return None  # אין תנאי לקוד הזה

    return _has_value(record, condition_field)


def _resolve_recipients(record, rule):
    """
    קובע נמענים (תפקידים, לא כתובות סופיות) לפי לוגיקת Override.
    מחזיר: { "to_role": str, "cc_role": str, "path": str }
    """
    override1 = rule.get("override_recipients")
    cc1       = rule.get("cc_override_1")
    override2 = rule.get("override_recipients_2")
    cc2       = rule.get("cc_override_2")

    # Override 1: תפקיד שקיים ברשומה
    if override1 and override1 in ROLE_TO_FIELD:
        field = ROLE_TO_FIELD[override1]
        if field and _has_value(record, field):
            return {"to_role": override1, "cc_role": cc1, "path": "override_1"}

    # Override 2: fallback
    if override2 and override2 in ROLE_TO_FIELD:
        field = ROLE_TO_FIELD[override2]
        if field and _has_value(record, field):
            return {"to_role": override2, "cc_role": cc2, "path": "override_2"}

    # Default: DefaultResponsibility
    return {
        "to_role": rule.get("responsibility_he"),
        "cc_role": rule.get("cc_responsibility"),
        "path":    "default",
    }


def classify_record(record, mapping):
    """
    מסווג רשומה בודדת.

    record  : dict (שורה מ-API של דוד)
    mapping : תוצאת load_mapping()

    מחזיר ClassifiedRecord dict, או None אם הרשומה מוחרגת / לא לעיבוד.
    """
    record_id = _get(record, FIELD_RECORD_ID, f"UNKNOWN_{id(record)}")
    customer  = _get(record, FIELD_CUSTOMER)
    counter   = _get(record, FIELD_COUNTER)

    # רשומה מבוטלת — לא לעבד
    status_desc = _get(record, FIELD_STATUS_DESC, "")
    if status_desc and "מבוטלת" in str(status_desc):
        return None, "רשומה מבוטלת"

    # counter < 1 (כולל null) — שגיאה חדשה או לא רלוונטית, אין פעולה
    try:
        c_val = int(float(counter)) if counter is not None else 0
        if c_val < 1:
            return None, f"Counter={c_val} (פחות מ-1)"
    except (ValueError, TypeError):
        c_val = 0

    # קוד שגיאה
    raw_code = _get(record, FIELD_ERROR_CODE)
    if raw_code is None:
        return _build_result(record, record_id, customer, error_code=None, counter=counter, rule=None,
                              responsibility=RESP_CASE_MANAGER,
                              email_format=FORMAT_CASE_MGR,
                              recipients={"to_role": "מנהלת תיק", "cc_role": None, "path": "קוד שגיאה חסר"},
                              escalation_level=c_val), None
    try:
        error_code = int(float(raw_code))
    except (ValueError, TypeError):
        return _build_result(record, record_id, customer, error_code=None, counter=counter, rule=None,
                              responsibility=RESP_CASE_MANAGER,
                              email_format=FORMAT_CASE_MGR,
                              recipients={"to_role": "מנהלת תיק", "cc_role": None, "path": "קוד שגיאה חסר"},
                              escalation_level=c_val), None

    # קודים מוחרגים במפורש
    if error_code in (1, 2):
        return None, f"קוד שגיאה {error_code} — מוחרג"

    # חיפוש בקובץ מיפוי
    rule = mapping["error_codes"].get(error_code)
    if rule is None:
        # קוד לא מוכר — מנהלת תיק כ-fallback
        return _build_result(record, record_id, customer, error_code, counter, rule=None,
                              responsibility=RESP_CASE_MANAGER,
                              email_format=FORMAT_CASE_MGR,
                              recipients={"to_role": "מנהלת תיק", "cc_role": None, "path": "unknown_code"},
                              escalation_level=c_val), None

    # קוד מוחרג
    if rule.get("excluded", False):
        return None, f"קוד שגיאה {error_code} — מוחרג בקובץ מיפוי"

    email_format   = rule.get("email_format", FORMAT_EXCLUDED)
    responsibility = rule.get("responsibility", RESP_CASE_MANAGER)

    # לוגיקת PreMailCondition
    condition_result = _check_pre_mail_condition(record, rule)
    condition_field  = rule.get("pre_mail_condition_field")

    if condition_result is True:
        # תנאי מתקיים → DefaultResponsibility
        recipients = {
            "to_role": rule.get("responsibility_he"),
            "cc_role": rule.get("cc_responsibility"),
            "path":    "pre_condition_true",
        }
    elif condition_result is False:
        # תנאי נכשל → ישירות לOverride ללא בדיקת זמינות כתובת מייל
        to_role = rule.get("override_recipients") or rule.get("responsibility_he")
        cc_role = rule.get("cc_override_1")
        responsibility = RESPONSIBILITY_MAP.get(to_role, responsibility)
        email_format   = _infer_format_from_role(to_role, email_format)
        recipients = {
            "to_role": to_role,
            "cc_role": cc_role,
            "path":    "pre_condition_false",
        }
    elif rule.get("override_recipients"):
        # אין תנאי אך יש override — _resolve_recipients לפי זמינות כתובת
        recipients = _resolve_recipients(record, rule)
        if recipients["path"] != "default":
            to_role = recipients["to_role"]
            responsibility = RESPONSIBILITY_MAP.get(to_role, responsibility)
            email_format   = _infer_format_from_role(to_role, email_format)
    else:
        # אין תנאי, אין override → DefaultResponsibility ישירות
        recipients = {
            "to_role": rule.get("responsibility_he"),
            "cc_role": rule.get("cc_responsibility"),
            "path":    "default",
        }

    # מדיניות הסלמה: counter >= 3 → override למנהלת תיק
    if c_val >= 3:
        responsibility = RESP_CASE_MANAGER
        email_format   = FORMAT_CASE_MGR
        recipients     = {"to_role": "מנהלת תיק", "cc_role": None, "path": f"escalation_c{c_val}"}

    return _build_result(record, record_id, customer, error_code, counter, rule,
                         responsibility, email_format, recipients,
                         condition_result=condition_result,
                         condition_field=condition_field,
                         escalation_level=c_val), None


def _infer_format_from_role(role, default_format):
    """מסיק פורמט מייל מתפקיד הנמען."""
    if role in ('רו"ח', "סוכן", "מעסיק", "איש קשר 1 מעסיק"):
        from mapping_loader import FORMAT_EMPLOYER
        return FORMAT_EMPLOYER
    if role == "מנהלת תיק":
        return FORMAT_CASE_MGR
    return default_format


def _build_result(record, record_id, customer, error_code, counter,
                  rule, responsibility, email_format, recipients,
                  condition_result=None, condition_field=None, escalation_level=None):
    """בונה את ה-ClassifiedRecord המלא."""
    rule = rule or {}
    return {
        # מזהים
        "record_id":       record_id,
        "customer_number": customer,
        "error_code":      error_code,
        "customer_name":   _get(record, "CustomerName"),

        # סיווג
        "responsibility":  responsibility,
        "email_format":    email_format,
        "excluded":        False,

        # נמענים (תפקידים — יתורגמו לכתובות ב-email_builder)
        "to_role":         recipients.get("to_role"),
        "cc_role":         recipients.get("cc_role"),
        "routing_path":    recipients.get("path"),

        # תוכן מייל
        "mail_subject_template":      rule.get("mail_subject"),
        "explanation_employer":       rule.get("explanation_employer"),
        "explanation_case_manager":   rule.get("explanation_case_manager"),
        "error_description":          rule.get("description"),

        # שדות API מקוריים הדרושים לקיבוץ ולבניית מייל
        "fund_institution_id":   _get(record, FIELD_FUND_ID),
        "fund_institution_name": _get(record, FIELD_FUND_NAME),
        "fund_institution_type": _get(record, FIELD_FUND_TYPE),
        "original_file_name":    _get(record, FIELD_ORIGINAL_FILE),
        "tik_mislaka":           _get(record, FIELD_TIK_MISLAKA),
        "employee_id":           _get(record, FIELD_EMPLOYEE_ID),
        "full_name":             _get(record, FIELD_FULL_NAME),      # stub
        "contact_email":         _get(record, FIELD_CONTACT_EMAIL),
        "employer_name":         _get(record, FIELD_EMPLOYER_NAME),  # stub

        # כתובות מייל לפי תפקיד (stub — יגיעו מדוד)
        "email_agent":      _get(record, FIELD_AGENT_EMAIL),
        "email_accountant": _get(record, FIELD_ACCOUNTANT_EMAIL),
        "email_contact1":   _get(record, FIELD_CONTACT1_EMAIL),
        "email_contact2":   _get(record, FIELD_CONTACT2_EMAIL),

        # PreMailCondition
        "pre_mail_condition_result": condition_result,
        "pre_mail_condition_field":  condition_field,
        "pre_mail_condition_value":  _get(record, condition_field) if condition_field else None,

        # הסלמה
        "counter_weeks":    escalation_level,

        # raw לשימוש מנהלת תיק (Excel עם כל השדות)
        "_raw": dict(record),
    }


def classify_all(records, mapping):
    """
    מסווג רשימת רשומות.
    מחזיר (classified_list, skipped_list) כאשר skipped_list = [(record, reason), ...]
    """
    classified = []
    skipped = []
    for rec in records:
        result, reason = classify_record(rec, mapping)
        if result is None:
            skipped.append((rec, reason or "סונן"))
        else:
            classified.append(result)
    return classified, skipped
