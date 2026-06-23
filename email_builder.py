"""
email_builder.py
----------------
בונה תוכן מייל לכל EmailGroup לפי פורמט.

פורמטים נתמכים:
  מוסדי-1  -- רשימת שמות קבצים + ת.ז. חד-ערכיים
  מוסדי-2  -- פורמט לקוד שגיאה 26
  מוסדי-3  -- קרן פנסיה ברירת מחדל (ספרת ביקורת)
  מעסיק    -- HTML table + Excel מצורף
  מנהלת תיק -- Excel עם כל שדות API
"""

import io
from datetime import date
import pandas as pd

from mapping_loader import (
    FORMAT_MOSADI_1, FORMAT_MOSADI_2, FORMAT_MOSADI_3,
    FORMAT_EMPLOYER, FORMAT_CASE_MGR,
)


_HTML_STYLE = """
<style>
  body { font-family: Arial, sans-serif; direction: rtl; text-align: right; font-size: 14px; color: #333; }
  table { border-collapse: collapse; width: 100%; margin: 16px 0; table-layout: fixed; }
  th, td { border: 1px solid #ccc; padding: 8px 12px; text-align: right; word-wrap: break-word; overflow-wrap: break-word; vertical-align: top; }
  th { background-color: #dce8f5; font-weight: bold; }
  tr:nth-child(even) { background-color: #f0f4f8; }
  tr:nth-child(odd)  { background-color: #ffffff; }
  tr:hover { background-color: #e8f0fe; }
  .col-id      { width: 9%; }
  .col-name    { width: 14%; }
  .col-fund    { width: 22%; }
  .col-type    { width: 10%; }
  .col-desc    { width: 18%; }
  .col-action  { width: 20%; }
  .col-chodesh { width: 7%; }
  .footer { margin-top: 24px; font-size: 12px; color: #888; }
</style>
"""

_MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def build_email(group, mapping):
    fmt = group.get("email_format", "")

    if fmt == FORMAT_MOSADI_1:
        content = _build_mosadi_1(group, mapping)
    elif fmt == FORMAT_MOSADI_2:
        content = _build_mosadi_2(group, mapping)
    elif fmt == FORMAT_MOSADI_3:
        content = _build_mosadi_3(group, mapping)
    elif fmt == FORMAT_EMPLOYER:
        content = _build_employer(group, mapping)
    else:
        return None  # מנהלת תיק -- מטופל ב-report_builder

    if content is not None:
        content["account_manager_email"] = group.get("meta", {}).get("account_manager_email")
    return content


def build_all_emails(groups, mapping):
    results = []
    skipped = 0
    for g in groups:
        try:
            content = build_email(g, mapping)
            results.append((g, content))
        except Exception as e:
            print(f"[WARN] email_builder: skip group {g.get('group_key')} -- {e}")
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

    file_names = sorted({
        r.get("original_file_name") for r in records
        if r.get("original_file_name")
    })
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

    employee_rows = ""
    file_names = sorted({r.get("original_file_name") for r in records if r.get("original_file_name")})
    seen_ids = set()
    for r in records:
        emp_id = str(r.get("employee_id") or "")
        if not emp_id or emp_id in seen_ids:
            continue
        seen_ids.add(emp_id)
        name = r.get("full_name") or "---"
        employee_rows += f"<li>{name} -- ת.ז. {emp_id}</li>"

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
# מוסדי-3 -- קרן פנסיה ברירת מחדל
# =============================================================================

def _build_mosadi_3(group, mapping):
    records = group["records"]
    meta    = group["meta"]

    customer_number = meta.get("customer_number", "")
    customer_name   = meta.get("customer_name") or ""
    fund_name       = meta.get("fund_institution_name") or ""

    subject = f"[מוסדי] {customer_number} {customer_name}".strip()

    employee_ids = sorted({
        str(r.get("employee_id")) for r in records
        if r.get("employee_id")
    })
    file_names = sorted({
        r.get("original_file_name") for r in records
        if r.get("original_file_name")
    })

    ids_html   = _ul_list(employee_ids, label="מספרי זהות עובדים:")
    files_html = _ul_list(file_names, label="שמות הקבצים שדווחו:")

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>שלום,</p>
  <p>על פי נהלי קרן ברירת מחדל, העובדים המפורטים להלן משויכים לקרן
  <strong>{fund_name}</strong> כקרן ברירת המחדל עבור המעסיק <strong>{customer_number}</strong>.
  התקבל היזון חוזר המעיד כי הכספים טרם נקלטו בקרן. נבקשכם לבדוק את הנושא ולטפל בהתאם.</p>
  {ids_html}
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
    seen = set()
    result = []
    for r in records:
        key = (str(r.get("employee_id") or ""), r.get("error_code"))
        if key not in seen:
            seen.add(key)
            result.append(r)
    return result


def _employer_table(records):
    rows_html = ""
    for r in _dedup_records(records):
        emp_id    = r.get("employee_id") or ""
        name      = r.get("full_name") or "---"
        fund_name = r.get("fund_institution_name") or "---"
        fund_type = r.get("fund_institution_type") or "---"
        desc      = r.get("error_description") or ""
        action    = r.get("explanation_employer") or ""
        chodesh   = r.get("_raw", {}).get("CHODESH_MASKORET") or ""
        rows_html += f"""
        <tr>
          <td>{emp_id}</td>
          <td>{name}</td>
          <td>{fund_name}</td>
          <td>{fund_type}</td>
          <td>{desc}</td>
          <td>{action}</td>
          <td>{chodesh}</td>
        </tr>"""

    return f"""
<table>
  <colgroup>
    <col class="col-id">
    <col class="col-name">
    <col class="col-fund">
    <col class="col-type">
    <col class="col-desc">
    <col class="col-action">
    <col class="col-chodesh">
  </colgroup>
  <thead>
    <tr>
      <th class="col-id">מ.ז. עובד</th>
      <th class="col-name">שם מלא</th>
      <th class="col-fund">שם קופה</th>
      <th class="col-type">סוג קופה</th>
      <th class="col-desc">תיאור שגיאה</th>
      <th class="col-action">טיפול נדרש</th>
      <th class="col-chodesh">חודש שכר</th>
    </tr>
  </thead>
  <tbody>{rows_html}
  </tbody>
</table>"""


def _employer_excel(records):
    rows = []
    for r in _dedup_records(records):
        rows.append({
            "מ.ז. עובד":     r.get("employee_id"),
            "שם מלא":        r.get("full_name"),
            "שם קופה":       r.get("fund_institution_name"),
            "סוג קופה":      r.get("fund_institution_type"),
            "תיאור שגיאה":   r.get("error_description"),
            "טיפול נדרש":    r.get("explanation_employer"),
            "חודש שכר":      r.get("_raw", {}).get("CHODESH_MASKORET"),
            "מס לקוח":       r.get("customer_number"),
            "שם קובץ מקור":  r.get("original_file_name"),
            "תיק מסלקה":     r.get("tik_mislaka"),
        })

    df  = pd.DataFrame(rows)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="שגיאות")
    return bio.getvalue()


# =============================================================================
# Helpers
# =============================================================================

def _render_subject(template_str, meta):
    if not template_str:
        return None
    s = template_str
    s = s.replace("{customer_number}",       str(meta.get("customer_number") or ""))
    s = s.replace("{employer_name}",         str(meta.get("employer_name") or ""))
    s = s.replace("{fund_institution_name}", str(meta.get("fund_institution_name") or ""))
    s = s.replace("{fund_institution_id}",   str(meta.get("fund_institution_id") or ""))
    return s.strip() or None


def _ul_list(items, label=""):
    if not items:
        return ""
    lis = "".join(f"<li>{i}</li>" for i in items)
    header = f"<strong>{label}</strong>" if label else ""
    return f"<div>{header}<ul>{lis}</ul></div>"


def _nl2br(text):
    return (text or "").replace("\n", "<br>\n")


def _signature():
    return """
<div class="footer">
  <hr>
  <p>מערכת היזון חוזר פנסיוני | hspension</p>
</div>
"""
