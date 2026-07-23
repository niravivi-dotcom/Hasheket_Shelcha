"""
record_classifier.py
--------------------
מסווג רשומה בודדת מה-API של דוד לפי קובץ המיפוי.
מחזיר: אחריות, פורמט מייל, נמענים (תפקידים), האם לעבד.

לוגיקת ניתוב:
  has_previous_positive = LastPositive_CHODESH_MASKORET IS NOT NULL
  TRUE  -> DefaultResponsibility path (מוסדי)
  FALSE -> Override path:
       1. OverrideMailRecipients (אם יש כתובת מייל לתפקיד הזה ברשומה)
       2. OverrideMailRecipients 2 (fallback)
       3. Default (fallback אחרון)
"""

import pandas as pd
from mapping_loader import (
    FORMAT_EXCLUDED, FORMAT_CASE_MGR, FORMAT_MOSADI_3,
    RESP_CASE_MANAGER, RESPONSIBILITY_MAP,
    DEFAULT_FUND_MAP,
)

# שדות API מדוד
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

# שדות API שנוספו לאחרונה
FIELD_INCOME_TAX_AUTH_NUMBER = "FundInstitutionTaxNumber"  # שם השדה בפועל ב-API (IncomeTaxAuthorizationNumber לא קיים)

# שדות שיגיעו מדוד בעתיד
FIELD_FIRST_NAME       = "EmployeeFirstName"
FIELD_LAST_NAME        = "EmployeeLastName"
FIELD_AGENT_EMAIL      = "AgentEmail"
FIELD_ACCOUNTANT_EMAIL = "AccountantEmail"
FIELD_CONTACT1_EMAIL   = "Contact1Email"
FIELD_CONTACT2_EMAIL   = "Contact2Email"
FIELD_PRODUCT_TYPE     = "ProductTypeCode"
FIELD_EMPLOYER_NAME    = "EmployerName"
FIELD_EMPLOYEE_ID      = "MISPAR_MEZAHE_OVED"

ROLE_TO_FIELD = {
    "סוכן":                FIELD_AGENT_EMAIL,
    'רו"ח':                FIELD_ACCOUNTANT_EMAIL,
    "איש קשר 1 מעסיק":    FIELD_CONTACT1_EMAIL,
    "איש קשר 2 מעסיק":    FIELD_CONTACT2_EMAIL,
    "מנהלת תיק":           None,
}


def _get(record, field, default=None):
    val = record.get(field, default)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return val


def _has_value(record, field):
    val = _get(record, field)
    return val is not None and str(val).strip() not in ("", "nan", "None")


def _months_diff(chodesh_current, chodesh_last_positive):
    try:
        c = int(str(chodesh_current).strip())
        l = int(str(chodesh_last_positive).strip())
        return (c // 100) * 12 + (c % 100) - ((l // 100) * 12 + (l % 100))
    except (ValueError, TypeError):
        return None


def _check_default_fund_condition(record, rule):
    # רק עבור קודים עם PreMailCondition
    if not rule.get("pre_mail_condition_field"):
        return None

    # חייב FundInstitutionType == קרן פנסיה
    fund_type = str(_get(record, FIELD_FUND_TYPE) or "").strip()
    if fund_type != "קרן פנסיה":
        return False

    # חייב ת.ז. עובד
    emp_id = str(_get(record, FIELD_EMPLOYEE_ID) or "").strip()
    if not emp_id or len(emp_id) < 2:
        return None

    # ספרת ביקורת
    try:
        last_digit = int(emp_id[-1])
    except (ValueError, IndexError):
        return None

    expected = DEFAULT_FUND_MAP.get(last_digit)
    if expected is None:
        return None

    # בדיקת IncomeTaxAuthorizationNumber
    income_tax = str(_get(record, FIELD_INCOME_TAX_AUTH_NUMBER) or "").strip()
    if income_tax != expected["income_tax_auth"]:
        return False

    # בדיקת FundInstitutionIdentityNumber
    fund_id = str(_get(record, FIELD_FUND_ID) or "").strip()
    if fund_id != expected["fund_id"]:
        return False

    return True


def _check_pre_mail_condition(record, rule):
    condition_field = rule.get("pre_mail_condition_field")
    if not condition_field:
        return None

    if not _has_value(record, condition_field):
        return False

    last_positive   = _get(record, condition_field)
    current_chodesh = _get(record, FIELD_CHODESH)

    if current_chodesh is not None:
        diff = _months_diff(current_chodesh, last_positive)
        if diff is None or diff > 6:
            return False

    return True


def _resolve_recipients(record, rule):
    override1 = rule.get("override_recipients")
    cc1       = rule.get("cc_override_1")
    override2 = rule.get("override_recipients_2")
    cc2       = rule.get("cc_override_2")

    if override1 and override1 in ROLE_TO_FIELD:
        field = ROLE_TO_FIELD[override1]
        if field and _has_value(record, field):
            return {"to_role": override1, "cc_role": cc1, "path": "override_1"}

    if override2 and override2 in ROLE_TO_FIELD:
        field = ROLE_TO_FIELD[override2]
        if field and _has_value(record, field):
            return {"to_role": override2, "cc_role": cc2, "path": "override_2"}

    return {
        "to_role": rule.get("responsibility_he"),
        "cc_role": rule.get("cc_responsibility"),
        "path":    "default",
    }


def classify_record(record, mapping):
    record_id = _get(record, FIELD_RECORD_ID, f"UNKNOWN_{id(record)}")
    customer  = _get(record, FIELD_CUSTOMER)
    counter   = _get(record, FIELD_COUNTER)

    # רשומה מבוטלת
    status_desc = _get(record, FIELD_STATUS_DESC, "")
    if status_desc and "מבוטלת" in str(status_desc):
        return None, "רשומה מבוטלת"

    # סטטוס 6: "רשומה לא נקלטה — הטיפול הסתיים" → מנהלת תיק
    # שימו לב: שונה מ"מבוטלת" — הרשומה לא נדלגת אלא מנותבת לטיפול מנהלת תיק
    # הבדיקה לפני counter<1 כי רשומות אלו עשויות להגיע עם counter=0
    feedback_status_raw = _get(record, FIELD_FEEDBACK_STATUS)
    try:
        feedback_status_id = int(float(str(feedback_status_raw).strip())) if feedback_status_raw is not None else None
    except (ValueError, TypeError):
        feedback_status_id = None

    if feedback_status_id == 6:
        try:
            c_val_fs = int(float(counter)) if counter is not None else 0
        except (ValueError, TypeError):
            c_val_fs = 0
        return _build_result(
            record, record_id, customer,
            error_code=_get(record, FIELD_ERROR_CODE),
            counter=counter, rule=None,
            responsibility=RESP_CASE_MANAGER,
            email_format=FORMAT_CASE_MGR,
            recipients={"to_role": "מנהלת תיק", "cc_role": None, "path": "status_6_ended"},
            escalation_level=c_val_fs,
        ), None

    # counter < 1
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
        return None, f"קוד שגיאה {error_code} מוחרג"

    # חיפוש בקובץ מיפוי
    rule = mapping["error_codes"].get(error_code)
    if rule is None:
        return _build_result(record, record_id, customer, error_code, counter, rule=None,
                              responsibility=RESP_CASE_MANAGER,
                              email_format=FORMAT_CASE_MGR,
                              recipients={"to_role": "מנהלת תיק", "cc_role": None, "path": "unknown_code"},
                              escalation_level=c_val), None

    if rule.get("excluded", False):
        return None, f"קוד שגיאה {error_code} מוחרג בקובץ מיפוי"

    email_format   = rule.get("email_format", FORMAT_EXCLUDED)
    responsibility = rule.get("responsibility", RESP_CASE_MANAGER)

    # שלב 1: בדיקת קרן ברירת מחדל (מוסדי-3)
    default_fund_match = _check_default_fund_condition(record, rule)
    if default_fund_match is True:
        recipients = {
            "to_role": rule.get("responsibility_he"),
            "cc_role": rule.get("cc_responsibility"),
            "path":    "default_fund_match",
        }
        if c_val >= 3:
            responsibility = RESP_CASE_MANAGER
            email_format   = FORMAT_CASE_MGR
            recipients     = {"to_role": "מנהלת תיק", "cc_role": None, "path": f"escalation_c{c_val}"}
        else:
            email_format = FORMAT_MOSADI_3
        return _build_result(record, record_id, customer, error_code, counter, rule,
                             responsibility, email_format, recipients,
                             condition_result=True,
                             condition_field=rule.get("pre_mail_condition_field"),
                             escalation_level=c_val), None

    # שלב 2: PreMailCondition
    condition_result = _check_pre_mail_condition(record, rule)
    condition_field  = rule.get("pre_mail_condition_field")

    if condition_result is True:
        # שינוי 2: אם יש פעולה מפורשת ל-condition=True — החל אותה
        true_action = rule.get("pre_mail_condition_true_action")
        true_value  = rule.get("pre_mail_condition_true_value")
        if true_action == "change_format" and true_value:
            email_format = true_value
        elif true_action == "change_recipient" and true_value:
            to_role_true = true_value
            responsibility = RESPONSIBILITY_MAP.get(to_role_true, responsibility)
            email_format   = _infer_format_from_role(to_role_true, email_format)
        recipients = {
            "to_role": rule.get("responsibility_he"),
            "cc_role": rule.get("cc_responsibility"),
            "path":    "pre_condition_true",
        }
    elif condition_result is False:
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
        recipients = _resolve_recipients(record, rule)
        if recipients["path"] != "default":
            to_role = recipients["to_role"]
            responsibility = RESPONSIBILITY_MAP.get(to_role, responsibility)
            email_format   = _infer_format_from_role(to_role, email_format)
    else:
        recipients = {
            "to_role": rule.get("responsibility_he"),
            "cc_role": rule.get("cc_responsibility"),
            "path":    "default",
        }

    # שמור אחריות מקורית לפני escalation (נדרש עבור apply_cross_error_inheritance)
    base_responsibility = responsibility

    # הסלמה: counter >= 3
    if c_val >= 3:
        responsibility = RESP_CASE_MANAGER
        email_format   = FORMAT_CASE_MGR
        recipients     = {"to_role": "מנהלת תיק", "cc_role": None, "path": f"escalation_c{c_val}"}

    return _build_result(record, record_id, customer, error_code, counter, rule,
                         responsibility, email_format, recipients,
                         condition_result=condition_result,
                         condition_field=condition_field,
                         escalation_level=c_val,
                         base_responsibility=base_responsibility), None



def apply_cross_error_inheritance(classified_records):
    """
    שינוי 1: ירושת גיל שגיאות cross-error-code תחת אותו גוף אחראי.
    קיבוץ: (employee_id, customer_number, fund_institution_id, responsibility)
    אם ישנה רשומה בקבוצה עם counter >= 3 → כל הרשומות בקבוצה עוברות למנהלת תיק.
    אחרת → counter_weeks של הרשומות הצעירות מתעדכן למקסימום הקבוצה.
    """
    groups: dict = {}
    for i, rec in enumerate(classified_records):
        emp_id       = str(rec.get("employee_id") or "")
        customer     = str(rec.get("customer_number") or "")
        fund_id      = str(rec.get("fund_institution_id") or "")
        # השתמש ב-base_responsibility (לפני escalation) כדי לקבץ נכון
        base_resp    = str(rec.get("base_responsibility") or rec.get("responsibility") or "")
        if not emp_id or not fund_id:
            continue
        key = (emp_id, customer, fund_id, base_resp)
        groups.setdefault(key, []).append(i)

    for key, indices in groups.items():
        if len(indices) < 2:
            continue  # רק קבוצות עם יותר מרשומה אחת

        max_counter = max(
            classified_records[i].get("counter_weeks") or 0
            for i in indices
        )

        if max_counter < 1:
            continue

        # עדכן counter_weeks לרשומות צעירות יותר
        for i in indices:
            if (classified_records[i].get("counter_weeks") or 0) < max_counter:
                classified_records[i]["counter_weeks"] = max_counter

        # הסלמה: אם counter >= 3 → כולן למנהלת תיק
        if max_counter >= 3:
            for i in indices:
                if classified_records[i].get("email_format") != FORMAT_CASE_MGR:
                    classified_records[i]["email_format"]  = FORMAT_CASE_MGR
                    classified_records[i]["responsibility"] = RESP_CASE_MANAGER
                    classified_records[i]["to_role"]        = "מנהלת תיק"
                    classified_records[i]["cc_role"]        = None
                    classified_records[i]["routing_path"]   = f"cross_error_inheritance_c{max_counter}"

    return classified_records

def _infer_format_from_role(role, default_format):
    if role in ('רו"ח', "סוכן", "מעסיק", "איש קשר 1 מעסיק"):
        from mapping_loader import FORMAT_EMPLOYER
        return FORMAT_EMPLOYER
    if role == "מנהלת תיק":
        return FORMAT_CASE_MGR
    return default_format


def _build_result(record, record_id, customer, error_code, counter,
                  rule, responsibility, email_format, recipients,
                  condition_result=None, condition_field=None, escalation_level=None,
                  base_responsibility=None):
    rule = rule or {}
    return {
        "record_id":       record_id,
        "customer_number": customer,
        "error_code":      error_code,
        "customer_name":   _get(record, "CustomerName"),
        "responsibility":  responsibility,
        "base_responsibility": base_responsibility if base_responsibility is not None else responsibility,
        "email_format":    email_format,
        "excluded":        False,
        "to_role":         recipients.get("to_role"),
        "cc_role":         recipients.get("cc_role"),
        "routing_path":    recipients.get("path"),
        "mail_subject_template":      rule.get("mail_subject"),
        "explanation_employer":       rule.get("explanation_employer"),
        "explanation_case_manager":   rule.get("explanation_case_manager"),
        "error_description":          rule.get("description"),
        "fund_institution_id":   _get(record, FIELD_FUND_ID),
        "fund_institution_name": _get(record, FIELD_FUND_NAME),
        "fund_institution_type": _get(record, FIELD_FUND_TYPE),
        "original_file_name":    _get(record, FIELD_ORIGINAL_FILE),
        "tik_mislaka":           _get(record, FIELD_TIK_MISLAKA),
        "account_manager_email":  _get(record, "CustomerAccountManagerEmail"),
        "employee_id":           _get(record, FIELD_EMPLOYEE_ID),
        "full_name":             " ".join(filter(None, [
                                     _get(record, FIELD_FIRST_NAME),
                                     _get(record, FIELD_LAST_NAME)
                                 ])) or None,
        "contact_email":         _get(record, FIELD_CONTACT_EMAIL),
        "employer_name":         _get(record, FIELD_EMPLOYER_NAME),
        "email_agent":      _get(record, FIELD_AGENT_EMAIL),
        "email_accountant": _get(record, FIELD_ACCOUNTANT_EMAIL),
        "email_contact1":   _get(record, FIELD_CONTACT1_EMAIL),
        "email_contact2":   _get(record, FIELD_CONTACT2_EMAIL),
        "pre_mail_condition_result": condition_result,
        "pre_mail_condition_field":  condition_field,
        "pre_mail_condition_value":  _get(record, condition_field) if condition_field else None,
        "counter_weeks":    escalation_level,
        "_raw": dict(record),
    }


def classify_all(records, mapping):
    classified = []
    skipped = []
    for rec in records:
        result, reason = classify_record(rec, mapping)
        if result is None:
            skipped.append((rec, reason or "סונן"))
        else:
            classified.append(result)
    return classified, skipped


def apply_employer_max_counter_routing(classified_records):
    from mapping_loader import FORMAT_EMPLOYER, FORMAT_CASE_MGR

    employer_keys = set()
    employer_indices = {}
    for i, rec in enumerate(classified_records):
        if rec.get("email_format") != FORMAT_EMPLOYER:
            continue
        emp_id  = str(rec.get("employee_id") or "")
        fund_id = str(rec.get("fund_institution_id") or "")
        ec      = str(rec.get("error_code") or "")
        if not emp_id or not fund_id or not ec:
            continue
        key = (emp_id, fund_id, ec)
        employer_keys.add(key)
        employer_indices.setdefault(key, []).append(i)

    if not employer_keys:
        return classified_records

    max_counters = {key: 0 for key in employer_keys}
    for rec in classified_records:
        emp_id  = str(rec.get("employee_id") or "")
        fund_id = str(rec.get("fund_institution_id") or "")
        ec      = str(rec.get("error_code") or "")
        key = (emp_id, fund_id, ec)
        if key in employer_keys:
            c = rec.get("counter_weeks") or 0
            if c > max_counters[key]:
                max_counters[key] = c

    for key, indices in employer_indices.items():
        max_counter = max_counters[key]
        if max_counter >= 3:
            for i in indices:
                classified_records[i]["email_format"]  = FORMAT_CASE_MGR
                classified_records[i]["responsibility"] = RESP_CASE_MANAGER
                classified_records[i]["to_role"]        = "מנהלת תיק"
                classified_records[i]["cc_role"]        = None
                classified_records[i]["routing_path"]   = f"employer_max_counter_{max_counter}"

    return classified_records
