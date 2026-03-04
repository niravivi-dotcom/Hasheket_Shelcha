import pandas as pd
import datetime
import re
import json

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
        optional_keys=['MISPAR_MEZAHE_RESHUMA', 'ErrorCodeV4Description', 'LastSuccessfulChodesh', 'CHODESH_MASKORET', 'ContactName', 'ContactEmail', 'FeedbackStatus'],
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
            'CHODESH_MASKORET': current_chodesh
        }
        results.append(res_entry)
        update_payload.append({'MISPAR_MEZAHE_RESHUMA': res_id, 'CalculatedCounter': counter, 'TreatmentStatus': status})
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

def build_email_drafts(output_df):
    """
    מקבל DataFrame עם תוצאות process_records.
    מחזיר רשימת טיוטות מייל — אחת לכל קבוצת (מעסיק + קופה + קוד שגיאה + אחריות).
    """
    if output_df.empty:
        return []

    drafts = []
    group_cols = ['CustomerNumber', 'KupaID', 'ErrorCode', 'Responsibility']
    available = [c for c in group_cols if c in output_df.columns]

    for keys, group in output_df.groupby(available):
        if not isinstance(keys, tuple):
            keys = (keys,)
        key_dict = dict(zip(available, keys))

        sample = group.iloc[0]
        customer_number = str(key_dict.get('CustomerNumber', ''))
        kupa_id         = str(key_dict.get('KupaID', ''))
        error_code      = str(key_dict.get('ErrorCode', ''))
        responsibility  = str(key_dict.get('Responsibility', ''))

        contact_email   = str(sample.get('ContactEmail', '') or '')
        contact_name    = str(sample.get('ContactName',  '') or '')
        error_desc      = str(sample.get('ErrorDescription', '') or '')
        treatment       = str(sample.get('TreatmentStatus', '') or '')
        duration_weeks  = int(sample['DurationWeeks']) if pd.notna(sample.get('DurationWeeks')) else 0

        unique_employees = group['EmployeeID'].dropna().unique().tolist() if 'EmployeeID' in group.columns else []

        subject = (
            f"היזון חוזר פנסיוני — "
            f"מעסיק {customer_number} | קופה {kupa_id} | קוד {error_code}"
        )

        greeting = f"שלום {contact_name}," if contact_name else "שלום,"
        body = "\n".join(filter(None, [
            greeting,
            "",
            f"קוד שגיאה: {error_code}" + (f" — {error_desc}" if error_desc else ""),
            f"קופה: {kupa_id}",
            f"אחריות: {responsibility}",
            f"מספר עובדים מושפעים: {len(unique_employees)}",
            f"סטטוס טיפול: {treatment}",
            "",
            "בברכה,",
            "צוות השקט שלך",
        ]))

        drafts.append({
            "customer_number":       customer_number,
            "kupa_id":               kupa_id,
            "error_code":            error_code,
            "error_description":     error_desc,
            "responsibility":        responsibility,
            "contact_email":         contact_email,
            "contact_name":          contact_name,
            "treatment_status":      treatment,
            "duration_weeks":        duration_weeks,
            "unique_employees_count": len(unique_employees),
            "subject":               subject,
            "body":                  body,
        })

    return drafts


def process_from_api_records(records_list, mapping_dict):
    """
    כניסה ראשית לעיבוד מ-API של דוד.
    מקבל רשימת records (JSON list) ו-mapping_dict.
    מחזיר dict עם drafts + update_payload.
    """
    if not records_list:
        return {"ok": False, "message": "לא התקבלו רשומות", "drafts": [], "update_payload": []}

    df_input = pd.DataFrame(records_list)
    output_df, update_payload, issues_df = process_records(df_input, mapping_dict)

    drafts = build_email_drafts(output_df)

    return {
        "ok": True,
        "total_input":     len(records_list),
        "total_processed": len(output_df),
        "total_drafts":    len(drafts),
        "drafts":          drafts,
        "update_payload":  update_payload,
        "issues":          issues_df.to_dict("records") if not issues_df.empty else [],
    }


if __name__ == "__main__":
    main()
