"""
report_builder.py
-----------------
מייצר Excel סיכום לאחר כל ריצה — לצורך בדיקה ואימות.

גיליונות:
  1. סיכום     — שורה לכל קבוצת מייל (draft_id, נמען, מס' רשומות, טווח שבועות)
  2. פירוט     — שורה לכל רשומה (ת.ז., קוד שגיאה, שבועות, לאיזה מייל שויכה)
  3. מוחרגות   — רשומות שסוננו עם סיבה

שימוש:
  from report_builder import build_run_report
  xlsx_bytes = build_run_report(groups, send_results, skipped_records, run_date)
"""

import io
from datetime import datetime

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter


def build_run_report(groups, send_results, skipped_records=None, run_date=None):
    """
    groups          : מ-group_records()
    send_results    : מ-send_all_groups()  [רשימת SendResult dicts]
    skipped_records : רשימת (record, reason) שנסוננו ב-classify_all (optional)
    run_date        : datetime או None → now()

    מחזיר bytes של Excel.
    """
    run_date = run_date or datetime.now()

    # --- draft_id lookup ---
    draft_map = {r["group_key"]: r for r in (send_results or [])}

    # === גיליון 1: סיכום ===
    summary_rows = []
    for g in groups:
        key    = g["group_key"]
        fmt    = g["email_format"]
        recs   = g["records"]
        meta   = g.get("meta", {})
        sr     = draft_map.get(key, {})

        counters = [r.get("counter") for r in recs if r.get("counter") is not None]
        c_min = min(counters) if counters else None
        c_max = max(counters) if counters else None

        summary_rows.append({
            "פורמט":           fmt,
            "מפתח קבוצה":      key,
            "גוף מוסדי / מעסיק": (
                meta.get("fund_institution_name") or
                meta.get("customer_name") or
                meta.get("customer_number") or ""
            ),
            "ח.פ / מזהה":      meta.get("customer_number") or meta.get("fund_institution_id") or "",
            "מס' רשומות":      len(recs),
            "שבועות (מינ)":    c_min,
            "שבועות (מקס)":    c_max,
            "draft_id":        sr.get("draft_id"),
            "נשלח בהצלחה":     "כן" if sr.get("ok") else ("לא" if sr else "—"),
            "שגיאה":           sr.get("error") or "",
        })

    # === גיליון 2: פירוט ===
    detail_rows = []
    for g in groups:
        key = g["group_key"]
        fmt = g["email_format"]
        sr  = draft_map.get(key, {})
        for r in g["records"]:
            detail_rows.append({
                "פורמט":           fmt,
                "מפתח קבוצה":      key,
                "draft_id":        sr.get("draft_id"),
                "record_id":       r.get("record_id"),
                "ח.פ מעסיק":       r.get("customer_number"),
                "שם מעסיק":        r.get("customer_name"),
                "מ.ז. עובד":       r.get("employee_id"),
                "שם עובד":         r.get("full_name"),
                "קוד שגיאה":       r.get("error_code"),
                "תיאור שגיאה":     r.get("error_description"),
                "שבועות":          r.get("counter"),
                "אחריות":          r.get("responsibility"),
                "נתיב ניתוב":      r.get("routing_path"),
                "גוף מוסדי":       r.get("fund_institution_name"),
                "שם קובץ מקור":    r.get("original_file_name"),
                "חודש שכר":        r.get("_raw", {}).get("CHODESH_MASKORET"),
            })

    # === גיליון 3: מוחרגות ===
    skipped_rows = []
    for item in (skipped_records or []):
        if isinstance(item, tuple) and len(item) == 2:
            rec, reason = item
        else:
            rec, reason = item, "סונן"
        skipped_rows.append({
            "record_id":   rec.get("MISPAR_MEZAHE_RESHUMA") or rec.get("record_id"),
            "ח.פ מעסיק":  rec.get("CustomerNumber") or rec.get("customer_number"),
            "קוד שגיאה":  rec.get("ErrorCodeV4Id") or rec.get("error_code"),
            "שבועות":     rec.get("OnlyOnStatusChange_DatesDiffInWeeks") or rec.get("counter"),
            "סיבה":       reason,
        })

    # === בניית Excel ===
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="סיכום",   index=False)
        pd.DataFrame(detail_rows).to_excel(writer,  sheet_name="פירוט",   index=False)
        pd.DataFrame(skipped_rows).to_excel(writer, sheet_name="מוחרגות", index=False)

    bio.seek(0)
    wb = load_workbook(bio)
    _style_workbook(wb, run_date)

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def _style_workbook(wb, run_date):
    header_fill = PatternFill("solid", start_color="1F4E79", end_color="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    data_font   = Font(name="Arial", size=10)
    center      = Alignment(horizontal="center", vertical="center", wrap_text=False)
    right       = Alignment(horizontal="right",  vertical="center", wrap_text=False)

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        # כותרת גיליון
        ws.sheet_view.rightToLeft = True

        # עיצוב header
        for cell in ws[1]:
            cell.font      = header_font
            cell.fill      = header_fill
            cell.alignment = center

        # עיצוב data
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.font      = data_font
                cell.alignment = right

        # רוחב עמודות אוטומטי
        for col_cells in ws.columns:
            max_len = max((len(str(c.value or "")) for c in col_cells), default=10)
            ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max_len + 4, 50)

        ws.freeze_panes = "A2"
