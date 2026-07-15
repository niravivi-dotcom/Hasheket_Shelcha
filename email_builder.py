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
  .footer { margin-top: 24px; font-size: 12px; color: #888; }
</style>
"""

# Inline styles — Gmail strips <style> blocks, so all table styling must be inline
_TH    = "background-color:#dce8f5;color:#1F4E79;font-weight:bold;border:1px solid #4472C4;padding:8px 12px;text-align:right;"
_TD    = "border:1px solid #4472C4;padding:8px 12px;text-align:right;vertical-align:top;word-wrap:break-word;overflow-wrap:break-word;"
_TABLE = "border-collapse:collapse;width:100%;margin:16px 0;font-family:Arial,sans-serif;font-size:14px;direction:rtl;"
_TR_EVEN = "background-color:#EBF3FB;"
_TR_ODD  = "background-color:#ffffff;"

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

    fund_part = f" — {fund_name}" if fund_name else ""
    subject = f"[מוסדי] {customer_number} {customer_name}{fund_part}".strip()

    employees  = _collect_employees(records, include_chodesh=True)
    emp_table  = _employees_table(employees, include_chodesh=True)
    file_names = sorted({r.get("original_file_name") for r in records if r.get("original_file_name")})
    files_html = _ul_list(file_names, label="שמות הקבצים שדווחו:")

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>שלום,</p>
  <p>התקבל היזון חוזר מ<strong>{fund_name}</strong> עבור המעסיק <strong>{customer_number}</strong>
  בגין העובדים הבאים אשר לא נקלטו באופן תקין למרות שחודשי שכר קודמים עם נתונים זהים נקלטו תקין
  על פי ההיזון החוזר שהתקבל מכם. האם ניתן לבדוק שוב ולשייך?</p>
  {emp_table}
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
# מוסדי-2
# =============================================================================

def _build_mosadi_2(group, mapping):
    records = group["records"]
    meta    = group["meta"]

    customer_number = meta.get("customer_number", "")
    customer_name   = meta.get("customer_name") or ""
    fund_name       = meta.get("fund_institution_name") or ""

    fund_part = f" — {fund_name}" if fund_name else ""
    subject = f"[מוסדי] {customer_number} {customer_name}{fund_part}".strip()

    employees  = _collect_employees(records, include_chodesh=False)
    emp_table  = _employees_table(employees, include_chodesh=False)
    file_names = sorted({r.get("original_file_name") for r in records if r.get("original_file_name")})
    files_html = _ul_list(file_names, label="שמות הקבצים:")

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>שלום,</p>
  <p>התקבל היזון חוזר מ<strong>{fund_name}</strong> בגין העובדים הבאים כי אין קרן פנסיה לעובד
  תחת המעסיק. ע"פ הנחיות אגף שוק ההון ביטוח וחיסכון במשרד האוצר לא נדרש ביצוע קבלת בעלות
  בקרן פנסיה. כל הפרטים לקבלת בעלות נמצאים בממשק שדווח אליכם.</p>
  {emp_table}
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

    fund_part = f" — {fund_name}" if fund_name else ""
    subject = f"[מוסדי] {customer_number} {customer_name}{fund_part}".strip()

    employees  = _collect_employees(records, include_chodesh=True)
    emp_table  = _employees_table(employees, include_chodesh=True)
    file_names = sorted({r.get("original_file_name") for r in records if r.get("original_file_name")})
    files_html = _ul_list(file_names, label="שמות הקבצים שדווחו:")

    body_html = f"""
{_HTML_STYLE}
<body dir="rtl">
  <p>שלום,</p>
  <p>על פי נהלי קרן ברירת מחדל, העובדים המפורטים להלן משויכים לקרן
  <strong>{fund_name}</strong> כקרן ברירת המחדל עבור המעסיק <strong>{customer_number}</strong>.
  התקבל היזון חוזר המעיד כי הכספים טרם נקלטו בקרן. נבקשכם לבדוק את הנושא ולטפל בהתאם.</p>
  {emp_table}
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

    intro  = "שלום,\n\nמצורפים למייל זה תשובות הקופות לגבי אי קליטת הכספים לקופות העובדים. האם ידוע ובטיפול?"
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
    for i, r in enumerate(_dedup_records(records)):
        tr_bg  = _TR_EVEN if i % 2 else _TR_ODD
        emp_id    = r.get("employee_id") or ""
        name      = r.get("full_name") or "---"
        fund_name = r.get("fund_institution_name") or "---"
        fund_type = r.get("fund_institution_type") or "---"
        desc      = r.get("error_description") or ""
        action    = r.get("explanation_employer") or ""
        chodesh   = r.get("_raw", {}).get("CHODESH_MASKORET") or ""
        rows_html += f"""
        <tr style="{tr_bg}">
          <td style="{_TD}">{emp_id}</td>
          <td style="{_TD}">{name}</td>
          <td style="{_TD}">{fund_name}</td>
          <td style="{_TD}">{fund_type}</td>
          <td style="{_TD}">{desc}</td>
          <td style="{_TD}">{action}</td>
          <td style="{_TD}">{chodesh}</td>
        </tr>"""

    return f"""
<table style="{_TABLE}">
  <thead>
    <tr>
      <th style="{_TH}">מ.ז. עובד</th>
      <th style="{_TH}">שם מלא</th>
      <th style="{_TH}">שם קופה</th>
      <th style="{_TH}">סוג קופה</th>
      <th style="{_TH}">תיאור שגיאה</th>
      <th style="{_TH}">טיפול נדרש</th>
      <th style="{_TH}">חודש שכר</th>
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

def _collect_employees(records, include_chodesh=False):
    """אוסף עובדים ייחודיים עם שם ות.ז (ואופציונלית חודש שכר)."""
    seen = set()
    result = []
    for r in records:
        emp_id = str(r.get("employee_id") or "").strip()
        if not emp_id or emp_id in seen:
            continue
        seen.add(emp_id)
        entry = {
            "id":   emp_id,
            "name": r.get("full_name") or "---",
            "desc": r.get("error_description") or "",
        }
        if include_chodesh:
            entry["chodesh"] = str(r.get("_raw", {}).get("CHODESH_MASKORET") or "")
        result.append(entry)
    return result


def _employees_table(employees, include_chodesh=False):
    """מחזיר טבלת HTML של עובדים — ת.ז + שם + תיאור + חודש שכר אם רלוונטי."""
    if not employees:
        return ""
    rows = ""
    for i, e in enumerate(employees):
        tr_bg = _TR_EVEN if i % 2 else _TR_ODD
        chodesh_td = f"<td style=\"{_TD}{tr_bg}\">{e.get('chodesh', '')}</td>" if include_chodesh else ""
        rows += (
            f"<tr style=\"{tr_bg}\">"
            f"<td style=\"{_TD}\">{e['id']}</td>"
            f"<td style=\"{_TD}\">{e['name']}</td>"
            f"<td style=\"{_TD}\">{e.get('desc','')}</td>"
            f"{chodesh_td}</tr>"
        )

    chodesh_th = f"<th style=\"{_TH}\">חודש שכר</th>" if include_chodesh else ""

    return f"""
<table style="{_TABLE}">
  <thead>
    <tr>
      <th style="{_TH}">ת.ז. עובד</th>
      <th style="{_TH}">שם עובד</th>
      <th style="{_TH}">תיאור הבעיה</th>
      {chodesh_th}
    </tr>
  </thead>
  <tbody>{rows}</tbody>
</table>"""


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
