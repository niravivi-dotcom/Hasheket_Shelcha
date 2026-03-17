import os
import pandas as pd
import datetime
import re
import json
import io
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.compose']

# --- Constants ---
FILE_WEEKLY = 'sample data.2026.02.24.xlsx'
FILE_MAPPING = 'error_code_mapping_final.xlsx'
ENABLE_EXCEL_PIVOT_TABLE = False

STATUS_TO_PROCESS = [
    'רשומה הועברה לטיפול מעסיק',
    'רשומה לא נקלטה על ידי יצרן - נדחה על ידי יצרן',
    'רשומה לא נקלטה על ידי יצרן - הועבר להמשך טיפול אצל יצרן',
    'רשומה נקלטה - נמצא חוסר בא.כ.ע'
]

OVERRIDE_CODES = [4, 5, 15, 93]

COLUMN_KEYWORDS = {
    'MISPAR_MEZAHE_RESHUMA': [
        'misparmezahereshuma', 'mispar_mezahe_reshuma', 'recordid', 'id', 'מזההרשומה', 'מספרמזההרשומה'
    ],
    'CustomerNumber': [
        'customernumber', 'customer', 'customerid', 'employer', 'employerid',
        'מספרלקוח', 'מספרמעסיק', 'מספרארגון', 'מספרח.פ', 'מספרהעסקה',
        'מזההמעסיק'
    ],
    'KodKupa_IdentityNumber': [
        'kodkupa_identitynumber', 'kodkupaidentity', 'kodkupa', 'codefund',
        'חפחברה', 'ח.פ.חברה', 'חפחברהמנהל', 'מספרחברה', 'מספרח.פ.חברה',
        'חפמייצג', 'fundinstitutionidentitynumber'
    ],
    'KodKupa_IncomeTax': [
        'kodkupa_incometax', 'kodkupa_incomtax', 'kupa_tax', 'קודקופהבאוצר',
        'מספרקופהבאוצר', 'מספרקופה', 'מספרמשלם', 'קודקופה', 'fundinstitutionid'
    ],
    'MISPAR_MEZAHE_OVED': [
        'misparmezaheoved', 'mispar_mezahe_oved', 'misparmezahe', 'מספרזההעובד',
        'מספרעובד', 'תז', 'תעודתזהות', 'מזההעובד', 'ת.ז.', 'employeeid', 'mispar_mezahe_oved'
    ],
    'FeedbackStatus': [
        'feedbackstatus', 'feedback_status', 'סטטוס', 'סטטוסרומה', 'סטטוסרשומה',
        'סטטוסהדיווח', 'status', 'statuscode', 'statusdescription', 'feedbackstatusid'
    ],
    'ErrorCodeV4Id': [
        'errorcodev4id', 'errorcode', 'errorcodev4', 'shgiah', 'קודשגיאה',
        'מספרשגיאה', 'kodshgia', 'errorcodeid', 'feedbackerrorcodeid'
    ],
    'ErrorCodeV4Description': [
        'errorcodev4description', 'errorcodedescription', 'description', 'תיאור',
        'תיאורשגיאה', 'תיאורתקלה', 'שגיאה', 'errorcodedesc', 'feedbackerrorcodedescription', 'statusdescription'
    ],
    'UpdateDate': [
        'updatedate', 'statuslastupdatedate', 'laststatusupdate', 'תאריךעדכון',
        'תאריך', 'תאריךסטטוס', 'תאריךעדכוןסטטוס', 'statusupdatedate',
        'תאריךדיווח', 'תאריךדיווח1', 'תאריךדיווח2', 'תאריךדיווח21', 'updatedate'
    ],
    'StatusLastUpdateDate': [
        'statuslastupdatedate', 'statuslastupdate', 'dateofstatusupdate',
        'תאריךעדכוןסטטוס', 'תאריךסטטוס', 'updatedate'
    ],
    'LastSuccessfulChodesh': [
        'lastsuccessfulchodesh', 'lastsuccess', 'successchodesh', 'חודשתקיןאחרון', 'lastpositive_chodesh_maskoret'
    ],
    'CHODESH_MASKORET': [
        'chodesh_maskoret', 'chodeshmaskoret', 'salarymonth', 'חודששכר', 'חודש', 'chodesh_maskoret'
    ],
    'ContactName': [
        'contactname', 'name', 'person', 'אישקשר', 'שםאישקשר', 'customercontactname'
    ],
    'ContactEmail': [
        'contactemail', 'email', 'mail', 'מייל', 'דואל', 'customercontactemail'
    ],
    'TikMislaka': [
        'tikmislaka', 'tik_mislaka', 'tikmislaka', 'תיקמסלקה', 'מסלקה'
    ],
    'OriginalFileName': [
        'originalfilename', 'original_file_name', 'filename', 'שםקובץ', 'שםהקובץ'
    ],
    'Counter': [
        'counter', 'כמות', 'מונה'
    ],
    'WeeksInStatus': [
        'onlyonstatuschange_datesdiffinweeks', 'datesdiffinweeks', 'weeksinstatuscalc'
    ],
    'AccountManagerEmail': [
        'customeraccountmanageremail', 'accountmanageremail', 'manageremail'
    ],
    'AccountManagerName': [
        'customeraccountmanagername', 'accountmanagername', 'managername'
    ]
}

# --- Utility Functions ---

def normalize_status_text(value):
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()

def normalize_column(col_name):
    if pd.isna(col_name):
        return ""
    return re.sub(r'[^0-9a-zא-ת]', '', str(col_name).lower())

def contains_keyword(col_name, keywords):
    normalized = normalize_column(col_name)
    return any(keyword in normalized for keyword in keywords)

def resolve_columns(df, required_keys=None, optional_keys=None, source_name="DataFrame"):
    resolved = {}
    if required_keys is None: required_keys = []
    if optional_keys is None: optional_keys = []
    normalized_map = {normalize_column(col): col for col in df.columns}
    def find_column(key):
        normalized_key = normalize_column(key)
        if normalized_key in normalized_map: return normalized_map[normalized_key]
        keywords = COLUMN_KEYWORDS.get(key, [])
        for norm_col, original_col in normalized_map.items():
            if any(keyword in norm_col for keyword in keywords): return original_col
        return None
    for key in required_keys:
        match = find_column(key)
        if match: resolved[key] = match
        else:
            available = ", ".join(f"{orig}" for norm, orig in normalized_map.items())
            raise KeyError(f"Missing column '{key}' in {source_name}. Available: {available}")
    for key in optional_keys:
        match = find_column(key)
        if match and key not in resolved: resolved[key] = match
    return resolved

def ensure_update_date_key(columns_map):
    if 'UpdateDate' not in columns_map and 'StatusLastUpdateDate' in columns_map:
        columns_map['UpdateDate'] = columns_map['StatusLastUpdateDate']

def get_value(row, col_key, columns_map, default=None):
    col = columns_map.get(col_key)
    if col is None: return default
    return row.get(col, default)

# --- Core Logic Functions ---

def calculate_duration_weeks_simple(update_date):
    if pd.isna(update_date): return 0
    try:
        updated = pd.to_datetime(update_date)
        today = pd.Timestamp.now()
        diff_days = (today - updated).days
        return max(0, diff_days // 7)
    except: return 0

def check_override_with_last_success(current_chodesh, last_success_chodesh):
    if pd.isna(last_success_chodesh) or pd.isna(current_chodesh): return False
    try:
        curr = int(str(current_chodesh).replace("/", "").replace("-", ""))
        last = int(str(last_success_chodesh).replace("/", "").replace("-", ""))
        return last < curr
    except: return False

def get_treatment_status(responsibility, counter):
    if counter == 0: return "שגיאה חדשה - המתנה לשבוע הבא"
    prefix = "מעסיק" if responsibility == 'Employer' else "גוף מוסדי" if responsibility == 'InstitutionalBody' else responsibility
    if counter == 1: return f"נשלח מייל ל{prefix} שבוע 1"
    if counter == 2: return f"נשלח מייל תזכורת ל{prefix} שבוע 2"
    if counter == 3: return f"הסלמה למנהלת תיק (שבוע 3)"
    if counter == 4: return f"הסלמה למנהלת תיק + מנהלת ראשית (שבוע 4)"
    return f"הסלמה להנהלה בכירה (שבוע {counter})"

def find_data_sheet(file_path):
    xl = pd.ExcelFile(file_path)
    for sheet in xl.sheet_names:
        try: df = pd.read_excel(file_path, sheet_name=sheet, nrows=2)
        except: continue
        cols = df.columns.tolist()
        if any(contains_keyword(c, COLUMN_KEYWORDS['FeedbackStatus']) for c in cols) and \
           any(contains_keyword(c, COLUMN_KEYWORDS['ErrorCodeV4Id']) for c in cols):
            return pd.read_excel(file_path, sheet_name=sheet)
    return pd.read_excel(file_path, sheet_name=xl.sheet_names[0])

def process_records(records_df, mapping_dict):
    results = []
    update_payload = []
    issues = []
    cols = resolve_columns(
        records_df,
        required_keys=['CustomerNumber', 'KodKupa_IdentityNumber', 'KodKupa_IncomeTax', 'MISPAR_MEZAHE_OVED', 'ErrorCodeV4Id', 'UpdateDate'],
        optional_keys=['MISPAR_MEZAHE_RESHUMA', 'ErrorCodeV4Description', 'LastSuccessfulChodesh', 'CHODESH_MASKORET', 'ContactName', 'ContactEmail', 'FeedbackStatus', 'TikMislaka', 'OriginalFileName', 'Counter', 'WeeksInStatus', 'AccountManagerEmail', 'AccountManagerName'],
        source_name="InputData"
    )
    for idx, row in records_df.iterrows():
        err_code = get_value(row, 'ErrorCodeV4Id', cols)
        res_id = get_value(row, 'MISPAR_MEZAHE_RESHUMA', cols, f"TEMP_{idx}")
        if pd.isna(err_code): continue
        try: err_code = int(err_code)
        except: pass
        if err_code == 1: continue
        map_rule = mapping_dict.get(err_code)
        if map_rule is None:
            issues.append({'IssueType': 'MissingErrorMapping', 'CustomerNumber': get_value(row, 'CustomerNumber', cols, ''), 'ErrorCode': err_code})
            map_rule = {'DefaultResponsibility': 'Unknown', 'HasOverrideCondition': False}
        update_date = get_value(row, 'UpdateDate', cols)
        counter_raw = get_value(row, 'WeeksInStatus', cols)
        if pd.notna(counter_raw):
            try:
                counter = int(counter_raw)
            except:
                counter = calculate_duration_weeks_simple(update_date)
        else:
            counter = calculate_duration_weeks_simple(update_date)
        responsibility = map_rule.get('DefaultResponsibility', 'Unknown')
        last_success = get_value(row, 'LastSuccessfulChodesh', cols)
        current_chodesh = get_value(row, 'CHODESH_MASKORET', cols)
        if err_code in OVERRIDE_CODES and pd.notna(last_success):
            if check_override_with_last_success(current_chodesh, last_success):
                if map_rule.get('HasOverrideCondition'):
                    responsibility = map_rule.get('OverrideResponsibility', 'InstitutionalBody')
        status = get_treatment_status(responsibility, counter)
        res_entry = {
            'MISPAR_MEZAHE_RESHUMA': res_id,
            'CustomerNumber': get_value(row, 'CustomerNumber', cols, ''),
            'EmployeeID': get_value(row, 'MISPAR_MEZAHE_OVED', cols, ''),
            'KupaID': f"{get_value(row, 'KodKupa_IdentityNumber', cols,'')}-{get_value(row, 'KodKupa_IncomeTax', cols,'')}",
            'ErrorCode': err_code,
            'ErrorDescription': get_value(row, 'ErrorCodeV4Description', cols, ''),
            'UpdateDate': update_date,
            'DurationWeeks': counter,
            'Responsibility': responsibility,
            'TreatmentStatus': status,
            'ContactName': get_value(row, 'ContactName', cols, ''),
            'ContactEmail': get_value(row, 'ContactEmail', cols, ''),
            'AccountManagerEmail': get_value(row, 'AccountManagerEmail', cols, ''),
            'AccountManagerName': get_value(row, 'AccountManagerName', cols, ''),
            'CHODESH_MASKORET': current_chodesh,
            'TikMislaka': get_value(row, 'TikMislaka', cols, ''),
            'OriginalFileName': get_value(row, 'OriginalFileName', cols, '')
        }
        results.append(res_entry)
        update_payload.append({'MISPAR_MEZAHE_RESHUMA': res_id, 'TreatmentStatus': status, 'Counter': counter})
    return pd.DataFrame(results), update_payload, pd.DataFrame(issues)

# --- Excel Table Helper ---

def try_create_excel_pivot_table(outfile, source_sheet="PIVOT_SOURCE", target_sheet="PIVOT"):
    try:
        import win32com.client as win32
        excel = win32.DispatchEx("Excel.Application")
        excel.Visible = False
        wb = excel.Workbooks.Open(str(pd.io.common.path_or_buf(outfile)))
        ws_source = wb.Worksheets(source_sheet)
        try: ws_target = wb.Worksheets(target_sheet)
        except: ws_target = wb.Worksheets.Add(); ws_target.Name = target_sheet
        ws_target.Cells.Clear()
        pc = wb.PivotCaches().Create(SourceType=1, SourceData=ws_source.UsedRange)
        pt = pc.CreatePivotTable(TableDestination=ws_target.Range("A3"), TableName="PivotTable1")
        for i, f in enumerate(["CustomerNumber", "KupaID", "ErrorCode", "Responsibility"], 1):
            pf = pt.PivotFields(f); pf.Orientation = 1; pf.Position = i
        cf = pt.PivotFields("DurationBucket"); cf.Orientation = 2; cf.Position = 1
        pt.AddDataField(pt.PivotFields("UniqueEmployees"), "UniqueEmployees", -4157)
        wb.Save(); wb.Close(); excel.Quit()
        print("[PIVOT] Excel PivotTable created.")
    except Exception as e: print(f"[PIVOT] Skipped real PivotTable: {e}")

# --- Main Entry ---

def main():
    print("--- Starting Pilot Engine ---")
    df_map = pd.read_excel(FILE_MAPPING)
    mapping_dict = df_map.set_index('ErrorCodeV4Id').to_dict('index')
    try: df_input = find_data_sheet(FILE_WEEKLY)
    except Exception as e: print(f"Error loading input: {e}"); return
    output_df, update_payload, issues_df = process_records(df_input, mapping_dict)
    if output_df.empty: print("No records found."); return
    with open('update_payload.json', 'w', encoding='utf-8') as f:
        json.dump(update_payload, f, ensure_ascii=False, indent=4)
    duration_buckets = {1: "W1", 2: "W2", 3: "W3", 4: "W4", 5: ">W5"}
    pivot = output_df.assign(DurationBucket=output_df['DurationWeeks'].fillna(1).astype(int).clip(upper=5)) \
        .groupby(['CustomerNumber', 'KupaID', 'ErrorCode', 'Responsibility', 'DurationBucket']) \
        .agg({'EmployeeID': 'nunique'}).reset_index().rename(columns={'EmployeeID': 'UniqueEmployees'})
    pivot_source_df = pivot.copy()
    pivot_source_df["DurationBucket"] = pivot_source_df["DurationBucket"].map(duration_buckets)
    summary_df = pivot.pivot_table(index=['CustomerNumber', 'KupaID', 'ErrorCode', 'Responsibility'], 
                                   columns='DurationBucket', values='UniqueEmployees', fill_value=0) \
        .rename(columns=duration_buckets).reset_index()
    outfile = 'Pilot_Results_v2.xlsx'
    with pd.ExcelWriter(outfile, engine='openpyxl') as writer:
        output_df.to_excel(writer, sheet_name='Drafts', index=False)
        pivot_source_df.to_excel(writer, sheet_name='PIVOT_SOURCE', index=False)
        summary_df.to_excel(writer, sheet_name='PIVOT', index=False)
        if not issues_df.empty: issues_df.to_excel(writer, sheet_name='Issues', index=False)
    if ENABLE_EXCEL_PIVOT_TABLE: try_create_excel_pivot_table(outfile)
    print(f"Success! Generated Pilot_Results_v2.xlsx and update_payload.json")

def _build_table_html(df, title):
    """Build an HTML table from a DataFrame subset."""
    if df.empty:
        return ""
    rows_html = ""
    for _, row in df.iterrows():
        rows_html += (
            f"<tr>"
            f"<td>{row.get('EmployeeID','')}</td>"
            f"<td>{row.get('KupaID','')}</td>"
            f"<td>{row.get('ErrorCode','')}</td>"
            f"<td>{row.get('ErrorDescription','')}</td>"
            f"</tr>"
        )
    return (
        f"<h3 style='margin-top:20px'>{title}</h3>"
        f"<table border='1' cellpadding='6' cellspacing='0' "
        f"style='border-collapse:collapse;direction:rtl;font-size:13px'>"
        f"<tr style='background:#e8e8e8;font-weight:bold'>"
        f"<th>מ.ז. עובד</th><th>קוד קופה</th><th>קוד שגיאה</th><th>תיאור שגיאה</th>"
        f"</tr>"
        f"{rows_html}"
        f"</table>"
    )


def _build_excel_attachment(group_df):
    """Return base64-encoded Excel with full record details."""
    col_map = {
        'EmployeeID':      'מ.ז. עובד',
        'KupaID':          'קוד קופה',
        'ErrorCode':       'קוד שגיאה',
        'ErrorDescription':'תיאור שגיאה',
        'CHODESH_MASKORET':'חודש שכר',
        'TikMislaka':      'תיק מסלקה',
        'OriginalFileName':'שם קובץ מקור',
        'DurationWeeks':   'שבועות פתוח',
        'Responsibility':  'אחריות',
        'TreatmentStatus': 'סטטוס טיפול',
        'MISPAR_MEZAHE_RESHUMA': 'מזהה רשומה',
    }
    available = {k: v for k, v in col_map.items() if k in group_df.columns}
    export_df = group_df[list(available.keys())].copy()
    export_df.columns = list(available.values())

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='פרטים')
    return base64.b64encode(buf.getvalue()).decode('utf-8')


def build_email_drafts(output_df):
    """
    One email draft per employer (CustomerNumber).
    Body: HTML with two tables — counter=1 (new) and counter>=2 (recurring).
    Attachment: Excel with full details including TikMislaka + OriginalFileName.
    Skips counter=0 records entirely.
    """
    if output_df.empty:
        return []

    drafts = []

    for customer_number, group in output_df.groupby('CustomerNumber'):
        # Skip counter=0 — no action needed yet
        group = group[group['DurationWeeks'] >= 1]
        if group.empty:
            continue

        table1 = group[group['DurationWeeks'] == 1]   # new issues
        table2 = group[group['DurationWeeks'] >= 2]   # recurring issues

        sample = group.iloc[0]
        contact_email         = str(sample.get('ContactEmail',        '') or '')
        contact_name          = str(sample.get('ContactName',         '') or '')
        account_manager_email = str(sample.get('AccountManagerEmail', '') or '')
        account_manager_name  = str(sample.get('AccountManagerName',  '') or '')

        # בסביבת טסט — מנתב את כל המיילים לתיבה אחת
        test_override = os.environ.get('TEST_EMAIL_OVERRIDE', '')
        effective_to  = test_override if test_override else contact_email

        total = len(group)
        subject = f"היזון חוזר פנסיוני — מעסיק {customer_number} | {total} רשומות לטיפול"

        greeting = f"שלום {contact_name}," if contact_name else "שלום,"
        table1_html = _build_table_html(table1, f"שגיאות חדשות ({len(table1)} רשומות)")
        table2_html = _build_table_html(table2, f"שגיאות חוזרות — דווחו בעבר ({len(table2)} רשומות)")

        body = (
            f"<div dir='rtl' style='font-family:Arial,sans-serif;direction:rtl'>"
            f"<p>{greeting}</p>"
            f"<p>מצורפות רשומות היזון חוזר הדורשות טיפולך עבור מעסיק {customer_number}.</p>"
            f"{table1_html}"
            f"{table2_html}"
            f"<p style='margin-top:20px'>לפרטים מלאים ראה קובץ מצורף.</p>"
            f"<p>בברכה,<br>צוות השקט שלך</p>"
            f"</div>"
        )

        excel_b64 = _build_excel_attachment(group)

        drafts.append({
            "customer_number":      str(customer_number),
            "contact_email":        effective_to,
            "contact_name":         contact_name,
            "account_manager_email": account_manager_email,
            "account_manager_name":  account_manager_name,
            "subject":              subject,
            "body":                 body,
            "total_records":        total,
            "new_records":          len(table1),
            "recurring_records":    len(table2),
            "excel_attachment":     excel_b64,
        })

    return drafts


def _get_gmail_service(service_account_info, impersonate_email):
    from google.oauth2 import service_account as sa_module
    from googleapiclient.discovery import build
    creds = sa_module.Credentials.from_service_account_info(
        service_account_info, scopes=GMAIL_SCOPES
    ).with_subject(impersonate_email)
    return build('gmail', 'v1', credentials=creds)


def _build_mime_message(draft_dict):
    msg = MIMEMultipart()
    msg['to']      = draft_dict['contact_email']
    msg['subject'] = draft_dict['subject']
    msg.attach(MIMEText(draft_dict['body'], 'html', 'utf-8'))

    excel_bytes = base64.b64decode(draft_dict['excel_attachment'])
    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    part.set_payload(excel_bytes)
    encoders.encode_base64(part)
    filename = f"hizon_hazor_{draft_dict['customer_number']}.xlsx"
    part.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(part)

    return base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')


def _create_single_draft(draft, service_account_info):
    account_manager_email = draft.get('account_manager_email', '')
    if not account_manager_email:
        return {
            'customer_number': draft.get('customer_number'),
            'ok': False,
            'error': 'account_manager_email חסר — דרפט לא נוצר'
        }
    try:
        service = _get_gmail_service(service_account_info, account_manager_email)
        raw = _build_mime_message(draft)
        created = service.users().drafts().create(
            userId='me', body={'message': {'raw': raw}}
        ).execute()
        return {
            'customer_number':       draft.get('customer_number'),
            'ok':                    True,
            'draft_id':              created.get('id'),
            'account_manager_email': account_manager_email,
            'contact_email':         draft.get('contact_email'),
            'total_records':         draft.get('total_records'),
        }
    except Exception as e:
        return {
            'customer_number':       draft.get('customer_number'),
            'account_manager_email': account_manager_email,
            'ok':                    False,
            'error':                 str(e)
        }


def create_drafts_via_gmail(drafts, service_account_info, max_workers=20):
    results = [None] * len(drafts)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(_create_single_draft, draft, service_account_info): idx
            for idx, draft in enumerate(drafts)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = {
                    'customer_number': drafts[idx].get('customer_number'),
                    'ok': False,
                    'error': str(e)
                }
    return results


def process_from_api_records(records_list, mapping_dict, service_account_info=None):
    """
    כניסה ראשית לעיבוד מ-API של דוד.
    מקבל רשימת records (JSON list), mapping_dict, ואופציונלית service_account_info ליצירת דרפטים ישירות.
    מחזיר dict עם drafts (תוצאות יצירה או תוכן) + update_payload.
    """
    if not records_list:
        return {"ok": False, "message": "לא התקבלו רשומות", "drafts": [], "update_payload": []}

    df_input = pd.DataFrame(records_list)
    output_df, update_payload, issues_df = process_records(df_input, mapping_dict)

    drafts = build_email_drafts(output_df)

    if service_account_info:
        draft_results = create_drafts_via_gmail(drafts, service_account_info)
    else:
        draft_results = drafts  # fallback: מחזיר תוכן (לבדיקה מקומית)

    return {
        "ok":              True,
        "total_input":     len(records_list),
        "total_processed": len(output_df),
        "total_drafts":    len(drafts),
        "drafts":          draft_results,
        "update_payload":  update_payload,
        "issues":          issues_df.to_dict("records") if not issues_df.empty else [],
    }


if __name__ == "__main__":
    main()
