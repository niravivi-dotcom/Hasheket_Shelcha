import pandas as pd
import datetime
import re

# --- Constants ---
FILE_WEEKLY = 'feedback report 122025.xlsx'
FILE_HISTORY = 'full feedback report 25112025.xlsx'
FILE_MAPPING = 'error_code_mapping_final.xlsx'

# אם Excel מותקן על המכונה שמריצה את הסקריפט – ניתן ליצור PivotTable אמיתי (אובייקט של אקסל)
# במקום “טבלת פיבוט” רגילה שנכתבת ע"י pandas.
ENABLE_EXCEL_PIVOT_TABLE = True

STATUS_TO_PROCESS = [
    'רשומה הועברה לטיפול מעסיק',
    'רשומה לא נקלטה על ידי יצרן - נדחה על ידי יצרן',
    'רשומה לא נקלטה על ידי יצרן - הועבר להמשך טיפול אצל יצרן',
    'רשומה נקלטה - נמצא חוסר בא.כ.ע'
]


def normalize_status_text(value):
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


NORMALIZED_STATUS_TO_PROCESS = {normalize_status_text(status) for status in STATUS_TO_PROCESS}

OVERRIDE_CODES = [4, 5, 15, 93]


COLUMN_KEYWORDS = {
    'CustomerNumber': [
        'customernumber', 'customer', 'customerid', 'employer', 'employerid',
        'מספרלקוח', 'מספרמעסיק', 'מספרארגון', 'מספרח.פ', 'מספרהעסקה',
        'מזההמעסיק'
    ],
    'KodKupa_IdentityNumber': [
        'kodkupa_identitynumber', 'kodkupaidentity', 'kodkupa', 'codefund',
        'חפחברה', 'ח.פ.חברה', 'חפחברהמנהל', 'מספרחברה', 'מספרח.פ.חברה',
        'חפמייצג'
    ],
    'KodKupa_IncomeTax': [
        'kodkupa_incometax', 'kodkupa_incomtax', 'kupa_tax', 'קודקופהבאוצר',
        'מספרקופהבאוצר', 'מספרקופה', 'מספרמשלם', 'קודקופה'
    ],
    'MISPAR_MEZAHE_OVED': [
        'misparmezaheoved', 'mispar_mezahe_oved', 'misparmezahe', 'מספרזההעובד',
        'מספרעובד', 'תז', 'תעודתזהות', 'מזההעובד', 'ת.ז.'
    ],
    'FeedbackStatus': [
        'feedbackstatus', 'feedback_status', 'סטטוס', 'סטטוסרומה', 'סטטוסרשומה',
        'סטטוסהדיווח', 'status', 'statuscode'
    ],
    'ErrorCodeV4Id': [
        'errorcodev4id', 'errorcode', 'errorcodev4', 'shgiah', 'קודשגיאה',
        'מספרשגיאה', 'kodshgia', 'errorcodeid'
    ],
    'ErrorCodeV4Description': [
        'errorcodev4description', 'errorcodedescription', 'description', 'תיאור',
        'תיאורשגיאה', 'תיאורתקלה', 'שגיאה', 'errorcodedesc'
    ],
    'UpdateDate': [
        'updatedate', 'statuslastupdatedate', 'laststatusupdate', 'תאריךעדכון',
        'תאריך', 'תאריךסטטוס', 'תאריךעדכוןסטטוס', 'statusupdatedate',
        'תאריךדיווח', 'תאריךדיווח1', 'תאריךדיווח2', 'תאריךדיווח21'
    ],
    'StatusLastUpdateDate': [
        'statuslastupdatedate', 'statuslastupdate', 'dateofstatusupdate',
        'תאריךעדכוןסטטוס', 'תאריךסטטוס'
    ]
}

def normalize_column(col_name):
    if pd.isna(col_name):
        return ""
    return re.sub(r'[^0-9a-zא-ת]', '', str(col_name).lower())


def contains_keyword(col_name, keywords):
    normalized = normalize_column(col_name)
    return any(keyword in normalized for keyword in keywords)


def find_data_sheet(file_path):
    """
    Finds the sheet containing the actual data by looking for known column keywords.
    """
    print(f"Scanning sheets in {file_path}")
    xl = pd.ExcelFile(file_path)
    feedback_keywords = ['feedbackstatus', 'סטטוס', 'סטטוסרומה', 'feedback_status', 'status']
    error_keywords = ['errorcodev4id', 'errorcode', 'errorcodev4', 'קודשגיאה', 'שגיאה', 'kodshgia']
    customer_keywords = ['customernumber', 'מספרלקוח', 'מספרמעסיק', 'customerid', 'employerid']

    for sheet in xl.sheet_names:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet, nrows=2)
        except Exception as e:
            print(f"Unable to open sheet {sheet}: {e}")
            continue

        cols = df.columns.tolist()
        has_feedback = any(contains_keyword(col, feedback_keywords) for col in cols)
        has_error = any(contains_keyword(col, error_keywords) for col in cols)
        has_customer = any(contains_keyword(col, customer_keywords) for col in cols)

        if has_feedback and has_error and has_customer:
            print(f"Found data sheet: {sheet}")
            return pd.read_excel(file_path, sheet_name=sheet)

    # fallback to sheet named data (case insensitive)
    for sheet in xl.sheet_names:
        if 'data' == sheet.strip().lower():
            print(f"Falling back to sheet explicitly named 'data': {sheet}")
            return pd.read_excel(file_path, sheet_name=sheet)

    raise ValueError(f"Could not find a valid data sheet in {file_path}")



def resolve_columns(df, required_keys=None, optional_keys=None, source_name="DataFrame"):
    resolved = {}
    if required_keys is None:
        required_keys = []
    if optional_keys is None:
        optional_keys = []

    normalized_map = {normalize_column(col): col for col in df.columns}

    def find_column(key):
        normalized_key = normalize_column(key)
        if normalized_key in normalized_map:
            return normalized_map[normalized_key]

        keywords = COLUMN_KEYWORDS.get(key, [])
        for norm_col, original_col in normalized_map.items():
            if any(keyword in norm_col for keyword in keywords):
                return original_col
        return None

    for key in required_keys:
        match = find_column(key)
        if match:
            resolved[key] = match
        else:
            available = ", ".join(f"{orig} ({norm})" for norm, orig in normalized_map.items())
            raise KeyError(
                f"Missing column '{key}' in {source_name}. "
                f"Available columns: {available}"
            )

    for key in optional_keys:
        match = find_column(key)
        if match and key not in resolved:
            resolved[key] = match

    print(f"{source_name}: resolved columns ->")
    for key, col in resolved.items():
        print(f"  {key}: {col}")

    return resolved


def ensure_update_date_key(columns_map):
    if 'UpdateDate' not in columns_map and 'StatusLastUpdateDate' in columns_map:
        columns_map['UpdateDate'] = columns_map['StatusLastUpdateDate']


def get_value(row, col_key, columns_map, default=None):
    col = columns_map.get(col_key)
    if col is None:
        return default
    return row.get(col, default)

def get_week_start(date):
    """Returns the Monday of the week for the given date."""
    return date - pd.Timedelta(days=date.weekday())

def calculate_duration_weeks(row, df_hist, week_cols, hist_cols):
    """
    Calculates the number of consecutive weeks the record has been in the same status/error.
    """
    mask = (
        (df_hist[hist_cols['CustomerNumber']] == get_value(row, 'CustomerNumber', week_cols)) &
        (df_hist[hist_cols['KodKupa_IdentityNumber']] == get_value(row, 'KodKupa_IdentityNumber', week_cols)) &
        (df_hist[hist_cols['KodKupa_IncomeTax']] == get_value(row, 'KodKupa_IncomeTax', week_cols)) &
        (df_hist[hist_cols['MISPAR_MEZAHE_OVED']] == get_value(row, 'MISPAR_MEZAHE_OVED', week_cols)) &
        (df_hist[hist_cols['ErrorCodeV4Id']] == get_value(row, 'ErrorCodeV4Id', week_cols))
    )

    history_matches = df_hist[mask]
    dates = set()

    row_date = get_value(row, 'UpdateDate', week_cols)
    if pd.notna(row_date):
        parsed = pd.to_datetime(row_date, errors='coerce')
        if not pd.isna(parsed):
            dates.add(parsed)

    hist_update_col = hist_cols.get('UpdateDate')
    if hist_update_col and not history_matches.empty:
        hist_dates = pd.to_datetime(history_matches[hist_update_col], errors='coerce').dropna()
        dates.update(hist_dates)

    if not dates:
        return 1

    unique_weeks = sorted(set(get_week_start(d) for d in dates), reverse=True)
    if not unique_weeks:
        return 1

    streak = 1
    current_week = unique_weeks[0]

    for prev_week in unique_weeks[1:]:
        diff = (current_week - prev_week).days
        if diff == 7:
            streak += 1
            current_week = prev_week
        else:
            break

    return streak

def check_override_condition(row, df_hist, week_cols, hist_cols):
    """
    Checks if there was a successful ingestion in the past for this employee-fund.
    """
    mask = (
        (df_hist[hist_cols['CustomerNumber']] == get_value(row, 'CustomerNumber', week_cols)) &
        (df_hist[hist_cols['KodKupa_IdentityNumber']] == get_value(row, 'KodKupa_IdentityNumber', week_cols)) &
        (df_hist[hist_cols['KodKupa_IncomeTax']] == get_value(row, 'KodKupa_IncomeTax', week_cols)) &
        (df_hist[hist_cols['MISPAR_MEZAHE_OVED']] == get_value(row, 'MISPAR_MEZAHE_OVED', week_cols)) &
        (df_hist[hist_cols['FeedbackStatus']].astype(str).str.contains('נקלט', na=False)) &
        ((df_hist[hist_cols['ErrorCodeV4Id']].isna()) | (df_hist[hist_cols['ErrorCodeV4Id']] == 0))
    )
    return not df_hist[mask].empty


def try_create_excel_pivot_table(
    outfile: str,
    source_sheet: str = "PIVOT_SOURCE",
    target_sheet: str = "PIVOT",
    table_name: str = "PivotTable1",
):
    """
    יוצר PivotTable אמיתי באמצעות Excel COM (win32com).
    אם Excel/pywin32 לא זמינים – מדלג ומשאיר את הפלט כטבלה רגילה.
    הפיבוט נבנה על בסיס טבלת מקור "מנורמלת" שמכילה UniqueEmployees לכל bucket,
    כך שהערכים בפיבוט הם SUM(UniqueEmployees) (שקול ל- DISTINCT COUNT בתוצאה).
    """
    try:
        import win32com.client as win32  # type: ignore
    except Exception as e:
        print(f"[PIVOT] לא ניתן לייבא win32com (pywin32). מדלג על יצירת PivotTable אמיתי. ({e})")
        return

    excel = None
    wb = None
    try:
        excel = win32.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False

        wb = excel.Workbooks.Open(str(outfile))

        # בדיקת גיליונות
        try:
            ws_source = wb.Worksheets(source_sheet)
        except Exception:
            print(f"[PIVOT] גיליון מקור '{source_sheet}' לא נמצא. מדלג על PivotTable.")
            return

        try:
            ws_target = wb.Worksheets(target_sheet)
        except Exception:
            ws_target = wb.Worksheets.Add()
            ws_target.Name = target_sheet

        # נקה יעד
        ws_target.Cells.Clear()

        # קבע טווח מקור
        used = ws_source.UsedRange
        if used is None:
            print(f"[PIVOT] אין UsedRange ב-'{source_sheet}'. מדלג.")
            return

        source_range = used
        dest_cell = ws_target.Range("A3")

        # יצירת PivotCache + PivotTable
        xlDatabase = 1
        pivot_cache = wb.PivotCaches().Create(SourceType=xlDatabase, SourceData=source_range)
        pivot_table = pivot_cache.CreatePivotTable(TableDestination=dest_cell, TableName=table_name)

        # הגדרת שדות: Rows / Columns / Values
        row_fields = [
            "CustomerNumber",
            "KodKupa_IdentityNumber",
            "KodKupa_IncomeTax",
            "ErrorCode",
            "Responsibility",
        ]
        for i, f in enumerate(row_fields, start=1):
            pf = pivot_table.PivotFields(f)
            pf.Orientation = 1  # xlRowField
            pf.Position = i

        col_field = pivot_table.PivotFields("DurationBucket")
        col_field.Orientation = 2  # xlColumnField
        col_field.Position = 1

        data_field = pivot_table.PivotFields("UniqueEmployees")
        pivot_table.AddDataField(data_field, "UniqueEmployees", -4157)  # xlSum

        # רענון ועיצוב בסיסי
        pivot_table.RefreshTable()
        try:
            ws_source.Visible = 0  # xlSheetHidden
        except Exception:
            pass

        wb.Save()
        print("[PIVOT] נוצר PivotTable אמיתי בהצלחה.")
    except Exception as e:
        print(f"[PIVOT] כשל ביצירת PivotTable אמיתי: {e}")
        try:
            if wb is not None:
                wb.Save()
        except Exception:
            pass
    finally:
        try:
            if wb is not None:
                wb.Close(SaveChanges=True)
        except Exception:
            pass
        try:
            if excel is not None:
                excel.Quit()
        except Exception:
            pass

def main():
    print("--- Starting Pilot Engine ---")
    
    # 1. Load Data
    print(f"Loading Mapping: {FILE_MAPPING}")
    df_map = pd.read_excel(FILE_MAPPING)
    mapping_dict = df_map.set_index('ErrorCodeV4Id').to_dict('index')
    
    print(f"Loading History: {FILE_HISTORY}")
    try:
        df_hist = find_data_sheet(FILE_HISTORY)
    except Exception as e:
        print(f"Error loading history: {e}")
        return

    print(f"Loading Weekly: {FILE_WEEKLY}")
    try:
        df_weekly = find_data_sheet(FILE_WEEKLY)
    except Exception as e:
        print(f"Error loading weekly: {e}")
        return
        
    week_required = [
        'CustomerNumber',
        'KodKupa_IdentityNumber',
        'KodKupa_IncomeTax',
        'MISPAR_MEZAHE_OVED',
        'FeedbackStatus',
        'ErrorCodeV4Id',
        'UpdateDate'
    ]
    week_optional = ['StatusLastUpdateDate', 'ErrorCodeV4Description']
    hist_required = week_required.copy()
    hist_optional = ['StatusLastUpdateDate']

    week_cols = resolve_columns(
        df_weekly,
        required_keys=week_required,
        optional_keys=week_optional,
        source_name="Weekly"
    )
    ensure_update_date_key(week_cols)

    hist_cols = resolve_columns(
        df_hist,
        required_keys=hist_required,
        optional_keys=hist_optional,
        source_name="History"
    )
    ensure_update_date_key(hist_cols)

    # 2. Filter Weekly Data
    print("Filtering weekly data...")
    status_col = week_cols['FeedbackStatus']
    normalized_status_col = df_weekly[status_col].apply(normalize_status_text)
    filtered_df = df_weekly[normalized_status_col.isin(NORMALIZED_STATUS_TO_PROCESS)].copy()
    
    # Filter by Date (Last Week)
    # Assuming 'UpdateDate' exists and is datetime compatible
    update_col = week_cols.get('UpdateDate')
    if update_col:
        filtered_df[update_col] = pd.to_datetime(filtered_df[update_col], errors='coerce')
        max_date = filtered_df[update_col].max()
        if pd.notna(max_date):
            # Define "Last Week" as 7 days ending at max_date
            start_date = max_date - pd.Timedelta(days=7)
            original_count = len(filtered_df)
            filtered_df = filtered_df[filtered_df[update_col] >= start_date]
            print(f"Filtered by date: {original_count} -> {len(filtered_df)} records (Max Date: {max_date.date()})")
    
    results = []
    issues = []
    
    print(f"Processing {len(filtered_df)} records...")
    
    for idx, row in filtered_df.iterrows():
        err_code = get_value(row, 'ErrorCodeV4Id', week_cols)
        
        # Validate Error Code
        if pd.isna(err_code):
            continue
        try:
            err_code = int(err_code)
        except:
            pass # Keep as is if not int

        if err_code == 1:
            continue
            
        map_rule = mapping_dict.get(err_code)
        if map_rule is None:
            issues.append({
                'IssueType': 'MissingErrorMapping',
                'CustomerNumber': get_value(row, 'CustomerNumber', week_cols, ''),
                'KodKupa_IdentityNumber': get_value(row, 'KodKupa_IdentityNumber', week_cols, ''),
                'KodKupa_IncomeTax': get_value(row, 'KodKupa_IncomeTax', week_cols, ''),
                'ErrorCode': err_code,
                'FeedbackStatus': normalize_status_text(get_value(row, 'FeedbackStatus', week_cols, '')),
            })
            map_rule = {
                'DefaultResponsibility': 'Unknown',
                'HasOverrideCondition': False
            }
        
        # --- Logic Step 1: Calculate Duration/Counter ---
        counter = calculate_duration_weeks(row, df_hist, week_cols, hist_cols)
        
        # --- Logic Step 2: Determine Responsibility & Override ---
        responsibility = map_rule.get('DefaultResponsibility', 'Unknown')
        override_triggered = False
        
        if err_code in OVERRIDE_CODES:
            if check_override_condition(row, df_hist, week_cols, hist_cols):
                override_triggered = True
                if map_rule.get('HasOverrideCondition'):
                    responsibility = map_rule.get('OverrideResponsibility')
        
        results.append({
            'CustomerNumber': get_value(row, 'CustomerNumber', week_cols, ''),
            'EmployeeID': get_value(row, 'MISPAR_MEZAHE_OVED', week_cols, ''),
            'KupaID': f"{get_value(row, 'KodKupa_IdentityNumber', week_cols,'')}-{get_value(row, 'KodKupa_IncomeTax', week_cols,'')}",
            'KodKupa_IdentityNumber': get_value(row, 'KodKupa_IdentityNumber', week_cols, ''),
            'KodKupa_IncomeTax': get_value(row, 'KodKupa_IncomeTax', week_cols, ''),
            'ErrorCode': err_code,
            'ErrorDescription': get_value(row, 'ErrorCodeV4Description', week_cols, ''),
            'UpdateDate': get_value(row, 'UpdateDate', week_cols, ''),
            'DurationWeeks': counter,
            'Responsibility': responsibility,
            'OverrideTriggered': override_triggered,
            'OriginalStatus': get_value(row, 'FeedbackStatus', week_cols, '')
        })
        
    # 3. Output
    if not results:
        print("No records require handling after filtering.")
        return

    output_df = pd.DataFrame(results)

    # Build a validation summary pivot for grouped email handling
    duration_buckets = {
        1: "DurationWeeks = 1",
        2: "DurationWeeks = 2",
        3: "DurationWeeks = 3",
        4: "DurationWeeks = 4",
        5: "DurationWeeks = >5"
    }

    summary_fields = [
        'CustomerNumber',
        'KodKupa_IdentityNumber',
        'KodKupa_IncomeTax',
        'ErrorCode',
        'Responsibility'
    ]
    pivot = (
        output_df
        .assign(
            DurationBucket=output_df['DurationWeeks'].fillna(1).astype(int).clip(upper=5),
            EmployeeID=output_df['EmployeeID']
        )
        .groupby(summary_fields + ['DurationBucket'])
        .agg({'EmployeeID': pd.Series.nunique})
        .reset_index()
        .rename(columns={'EmployeeID': 'UniqueEmployees'})
    )

    # PivotTable אמיתי באקסל יעבוד על טבלת מקור "ארוכה" (long) ולא על wide.
    # בנוסף, כדי לשמור על שמות עמודות ידידותיים, נחליף את DurationBucket לערכים טקסטואליים.
    pivot_source_df = pivot.copy()
    pivot_source_df["DurationBucket"] = pivot_source_df["DurationBucket"].map(duration_buckets).fillna(
        pivot_source_df["DurationBucket"].astype(str)
    )

    summary_df = (
        pivot
        .pivot_table(
            index=summary_fields,
            columns='DurationBucket',
            values='UniqueEmployees',
            fill_value=0
        )
        .rename(columns=duration_buckets)
        .reset_index()
    )

    duration_columns = [duration_buckets[i] for i in sorted(duration_buckets)]
    # Ensure every duration column exists even if pivot skipped it
    for column in duration_columns:
        if column not in summary_df:
            summary_df[column] = 0

    summary_df = summary_df[summary_fields + duration_columns]

    issues_df = pd.DataFrame(issues) if issues else pd.DataFrame(columns=[
        'IssueType', 'CustomerNumber', 'KodKupa_IdentityNumber', 'KodKupa_IncomeTax', 'ErrorCode', 'FeedbackStatus'
    ])

    outfile = 'Pilot_Results_v2.xlsx'
    with pd.ExcelWriter(outfile, engine='openpyxl') as writer:
        output_df.to_excel(writer, sheet_name='Drafts', index=False)
        # טבלת מקור לפיבוט (ליצירת PivotTable אמיתי באקסל)
        pivot_source_df.to_excel(writer, sheet_name='PIVOT_SOURCE', index=False)
        # טבלת תוצאה "רחבה" – שימושית גם בלי Excel Pivot אמיתי
        summary_df.to_excel(writer, sheet_name='PIVOT', index=False)
        if not issues_df.empty:
            issues_df.to_excel(writer, sheet_name='Processing Issues', index=False)

    # ניסיון ליצור PivotTable אמיתי (אם אפשר)
    if ENABLE_EXCEL_PIVOT_TABLE:
        try_create_excel_pivot_table(outfile)

    print(f"Success! Generated {len(output_df)} draft tasks and PIVOT sheet in {outfile}")

if __name__ == "__main__":
    main()
