"""
email_builder.py
----------------
בונה תוכן מייל לכל EmailGroup לפי פורמט.

מחזיר EmailContent dict:
{
    "subject":     str,
    "body_html":   str,
    "to_email":    str | None,
    "cc_email":    str | None,
    "attachments": [{"filename": str, "data": bytes, "mimetype": str}, ...]
}

פורמטים נתמכים:
  מוסדי-1  — רשימת שמות קבצים + ת.ז. חד-ערכיים
  מוסדי-2  — פורמט לקוד שגיאה 26
  מעסיק    — HTML table + Excel מצורף
  מנהלת תיק — Excel עם כל שדות API
"""

import io
from datetime import date
import pandas as pd

from mapping_loader import (
    FORMAT_MOSADI_1, FORMAT_MOSADI_2,
    FORMAT_EMPLOYER, FORMAT_CASE_MGR,
)


# ---- תבנית CSS בסיסית ל-HTML מיילים ----
_HTML_STYLE = """
<style>
  body { font-family: Arial, sans-serif; direction: rtl; text-align: right; font-size: 14px; color: #333; }
  table { border-collapse: collapse; width: 100%; margin: 16px 0; }
  th, td { border: 1px solid #ccc; padding: 8px 12px; text-align: right; }
  th { background-color: #f0f4f8; font-weight: bold; }
  tr:nth-child(even) { background-color: #fafafa; }
  .footer { margin-top: 24px; font-size: 12px; color: #888; }
</style>
"""

_MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


# =============================================================================
# Public API
# =============================================================================

def build_email(group, mapping, case_manager_email=None):
    """
    מקבל EmailGroup dict (מ-record_grouper) ומחזיר EmailContent dict.

    group            : dict מ-group_records()
    mapping          : dict מ-load_mapping()
    case_manager_email : כתובת מייל מנהלת תיק (נמסרת מה-runner)
    """
    fmt = group.get("email_format", "")

    if fmt == FORMAT_MOSADI_1:
        return _build_mosadi_1(group, mapping)
    elif fmt == FORMAT_MOSADI_2:
        return _build_mosadi_2(group, mapping)
    elif fmt == FORMAT_EMPLOYER:
        return _build_employer(group, mapping)
    elif fmt == FORMAT_CASE_MGR:
        return _build_case_mgr(group, mapping, case_manager_email)
    else:
        # fallback — מנהלת תיק
        return _build_case_mgr(group, mapping, case_manager_email)


def build_all_emails(groups, mapping, case_manager_email=None):
    """
    בונה EmailContent לכל קבוצה.
    מחזיר רשימת (group, email_content) tuples.
    skips = מספר קבוצות שנכשלו.
    """
    results = []
    skipped = 0
    for g in groups:
        try:
            content = build_email(g, mapping, case_manager_email)
            results.append((g, content))
        except Exception as e:
            print(f"[WARN] email_builder: skip group {g.get('group_key')} — {e}")
            skipped += 1
    return results, skipped


# =============================================================================
# מוסדי-1
# =============================================================================

def _build_mosadi_1(group, mapping):
    records = group["records"]
    meta    = group["meta"]

    customer_number = meta.get("customer_number", "")
    customer_name   = meta.get("customer_name") or ""
    fund_name       = meta.get("fund_institution_name") or ""

    subject = f"[מוסדי] {customer_number} {customer_name}".strip()

    # שמות קבצים חד-ערכיים
    file_names = sorted({
        r.get("original_file_name") for r in records
        if r.get("original_file_name")
    })
    # ת.ז. עובדים חד-ערכיים
    employee_ids = sorted({
        str(r.get("employee_id")) for r in records
        if r.get("employee_id")
    })

    files_html = _ul_list(file_names, label="שמות הקבצים שדווחו:")
    ids_html   = _ul_list(employee_ids, label="מספרי זהות עובדים:")

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>שלום,</p>
  <p>התקבל היזון חוזר מ<strong>{fund_name}</strong> עבור המעסיק <strong>{customer_number}</strong>
  בגין העובדים הבאים אשר לא נקלטו באופן תקין למרות שחודשי שכר קודמים עם נתונים זהים נקלטו תקין
  על פי ההיזון החוזר שהתקבל מכם. האם ניתן לבדוק שוב ולשייך?</p>
  {files_html}
  {ids_html}
  {_signature()}
</body>
"""
    return {
        "subject":     subject,
        "body_html":   body_html,
        "to_email":    None,
        "cc_email":    None,
        "attachments": [],
    }


# =============================================================================
# מוסדי-2
# =============================================================================

def _build_mosadi_2(group, mapping):
    records = group["records"]
    meta    = group["meta"]

    customer_number = meta.get("customer_number", "")
    customer_name   = meta.get("customer_name") or ""
    fund_name       = meta.get("fund_institution_name") or ""

    subject = f"[מוסדי] {customer_number} {customer_name}".strip()

    # לכל עובד ייחודי: שם מלא + ת.ז + שם קובץ
    employee_rows = ""
    file_names = sorted({r.get("original_file_name") for r in records if r.get("original_file_name")})
    seen_ids = set()
    for r in records:
        emp_id = str(r.get("employee_id") or "")
        if not emp_id or emp_id in seen_ids:
            continue
        seen_ids.add(emp_id)
        name = r.get("full_name") or "—"
        employee_rows += f"<li>{name} — ת.ז. {emp_id}</li>"

    files_html = _ul_list(file_names, label="שמות הקבצים:")

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>שלום,</p>
  <p>התקבל היזון חוזר מ<strong>{fund_name}</strong> בגין העובדים הבאים כי אין קרן פנסיה לעובד
  תחת המעסיק. ע"פ הנחיות אגף שוק ההון ביטוח וחיסכון במשרד האוצר לא נדרש ביצוע קבלת בעלות
  בקרן פנסיה. כל הפרטים לקבלת בעלות נמצאים בממשק שדווח אליכם.</p>
  <ul>{employee_rows}</ul>
  {files_html}
  {_signature()}
</body>
"""
    return {
        "subject":     subject,
        "body_html":   body_html,
        "to_email":    None,
        "cc_email":    None,
        "attachments": [],
    }


# =============================================================================
# מעסיק / רו"ח / סוכן
# =============================================================================

def _build_employer(group, mapping):
    records = group["records"]
    meta    = group["meta"]

    customer = meta.get("customer_number", "")
    employer = meta.get("customer_name") or meta.get("employer_name") or ""
    subject  = f"[מעסיק] ח.פ {customer} {employer}".strip()

    intro  = "שלום,\n\nמצורפים למייל זה תשובות הקופות לגבי קליטת הכספים לקופות העובדים. האם ידוע ובטיפול?"
    footer = "בכל שאלה נשמח לסייע."

    # HTML table
    table_html = _employer_table(records)

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>{_nl2br(intro)}</p>
  {table_html}
  <p>{footer}</p>
  {_signature()}
</body>
"""

    # Excel מצורף
    xlsx_data     = _employer_excel(records)
    xlsx_filename = f"שגיאות_פנסיה_{customer}.xlsx"

    return {
        "subject":   subject,
        "body_html": body_html,
        "to_email":  meta.get("to_email"),
        "cc_email":  meta.get("cc_email"),
        "attachments": [
            {
                "filename": xlsx_filename,
                "data":     xlsx_data,
                "mimetype": _MIME_XLSX,
            }
        ],
    }


def _dedup_records(records):
    """מחזיר רשומות ייחודיות לפי (ת.ז., קוד שגיאה) — שומר סדר הופעה ראשון."""
    seen = set()
    result = []
    for r in records:
        key = (str(r.get("employee_id") or ""), r.get("error_code"))
        if key not in seen:
            seen.add(key)
            result.append(r)
    return result


def _employer_table(records):
    """HTML table: ת.ז., שם מלא, קוד שגיאה, תיאור שגיאה, טיפול נדרש."""
    rows_html = ""
    for r in _dedup_records(records):
        emp_id   = r.get("employee_id") or ""
        name     = r.get("full_name") or "—"   # stub
        code     = r.get("error_code") or ""
        desc     = r.get("error_description") or ""
        action   = r.get("explanation_employer") or ""
        rows_html += f"""
        <tr>
          <td>{emp_id}</td>
          <td>{name}</td>
          <td>{code}</td>
          <td>{desc}</td>
          <td>{action}</td>
        </tr>"""

    return f"""
<table>
  <thead>
    <tr>
      <th>מ.ז. עובד</th>
      <th>שם מלא</th>
      <th>קוד שגיאה</th>
      <th>תיאור שגיאה</th>
      <th>טיפול נדרש</th>
    </tr>
  </thead>
  <tbody>{rows_html}
  </tbody>
</table>"""


def _employer_excel(records):
    """בונה Excel (bytes) לקבוצת מעסיק."""
    rows = []
    for r in _dedup_records(records):
        rows.append({
            "מ.ז. עובד":     r.get("employee_id"),
            "שם מלא":        r.get("full_name"),      # stub
            "קוד שגיאה":     r.get("error_code"),
            "תיאור שגיאה":   r.get("error_description"),
            "טיפול נדרש":    r.get("explanation_employer"),
            "מס' לקוח":      r.get("customer_number"),
            "שם קובץ מקור":  r.get("original_file_name"),
            "תיק מסלקה":     r.get("tik_mislaka"),
        })

    df  = pd.DataFrame(rows)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="שגיאות")
    return bio.getvalue()


# =============================================================================
# מנהלת תיק
# =============================================================================

def _build_case_mgr(group, mapping, case_manager_email=None):
    records  = group["records"]
    today    = date.today().strftime("%d/%m/%Y")
    subject  = f"[מנהלת תיק] דו\"ח היזון חוזר לתאריך {today}"
    body_tmpl = "מצורף דוח רשומות הדורשות טיפול ידני."

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>{_nl2br(body_tmpl)}</p>
  <p>סה"כ רשומות: {len(records)}</p>
  {_signature()}
</body>
"""

    xlsx_data = _case_mgr_excel(records)

    return {
        "subject":   subject,
        "body_html": body_html,
        "to_email":  case_manager_email,     # נמסר מה-runner
        "cc_email":  None,                   # TODO: חלוקת To/CC מעידו
        "attachments": [
            {
                "filename": "רשומות_לטיפול_מנהלת_תיק.xlsx",
                "data":     xlsx_data,
                "mimetype": _MIME_XLSX,
            }
        ],
    }


def _case_mgr_excel(records):
    """בונה Excel עם כל שדות ה-API (_raw) לכל רשומה."""
    rows = []
    for r in records:
        raw = r.get("_raw", {})
        row = {
            "record_id":               r.get("record_id"),
            "customer_number":         r.get("customer_number"),
            "customer_name":           r.get("customer_name"),
            "error_code":              r.get("error_code"),
            "error_description":       r.get("error_description"),
            "explanation_case_manager": r.get("explanation_case_manager"),
            "counter_weeks":           r.get("counter"),
            "responsibility":          r.get("responsibility"),
            "routing_path":            r.get("routing_path"),
        }
        # כל שדות ה-raw (ללא כפילויות)
        for k, v in raw.items():
            if k not in row:
                row[k] = v
        rows.append(row)

    df  = pd.DataFrame(rows)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="רשומות")
    return bio.getvalue()


# =============================================================================
# Helpers
# =============================================================================

def _render_subject(template_str, meta):
    """מחליף placeholders בנושא המייל לפי meta."""
    if not template_str:
        return None
    s = template_str
    s = s.replace("{customer_number}",       str(meta.get("customer_number") or ""))
    s = s.replace("{employer_name}",         str(meta.get("employer_name") or ""))
    s = s.replace("{fund_institution_name}", str(meta.get("fund_institution_name") or ""))
    s = s.replace("{fund_institution_id}",   str(meta.get("fund_institution_id") or ""))
    return s.strip() or None


def _default_subject_mosadi(meta):
    name = meta.get("fund_institution_name") or meta.get("fund_institution_id") or ""
    return f"פידבק שגיאות פנסיה — {name}".strip()


def _ul_list(items, label=""):
    if not items:
        return ""
    lis = "".join(f"<li>{i}</li>" for i in items)
    header = f"<strong>{label}</strong>" if label else ""
    return f"<div>{header}<ul>{lis}</ul></div>"


def _nl2br(text):
    """ממיר ירידת שורה ל-<br>."""
    return (text or "").replace("\n", "<br>\n")


def _signature():
    return """
<div class="footer">
  <hr>
  <p>מערכת היזון חוזר פנסיוני | hspension</p>
</div>
"""
