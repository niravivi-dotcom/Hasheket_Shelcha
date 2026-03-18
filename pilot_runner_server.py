import os
import io
import base64
import subprocess
from datetime import datetime
from pathlib import Path
import tempfile

from flask import Flask, jsonify, send_file, request


APP_DIR = Path(__file__).resolve().parent
OUTFILE = APP_DIR / "Pilot_Results_v2.xlsx"
PILOT_ENGINE = APP_DIR / "pilot_engine.py"

app = Flask(__name__)


def run_pilot() -> tuple[bool, str]:
    """
    מריץ את pilot_engine.py בתיקיית הפרויקט ומחזיר (success, message).
    """
    cmd = ["py", "-3", str(APP_DIR / "pilot_engine.py")]
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(APP_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return False, f"pilot_engine.py נכשל. rc={completed.returncode}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        return True, completed.stdout.strip() or "OK"
    except Exception as e:
        return False, f"שגיאה בהרצת הפיילוט: {e}"


def run_pilot_in_dir(work_dir: Path) -> tuple[bool, str, Path | None]:
    """
    מריץ את pilot_engine.py בתוך work_dir (שבו נמצאים קבצי הקלט),
    ומחזיר (ok, message, output_path).
    """
    cmd = ["python", str(PILOT_ENGINE)]
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(work_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            return (
                False,
                f"pilot_engine.py נכשל. rc={completed.returncode}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}",
                None,
            )
        out = work_dir / "Pilot_Results_v2.xlsx"
        if not out.exists():
            return False, "הפיילוט רץ אבל לא נוצר Pilot_Results_v2.xlsx", None
        return True, (completed.stdout.strip() or "OK"), out
    except Exception as e:
        return False, f"שגיאה בהרצת הפיילוט: {e}", None


@app.get("/health")
def health():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat() + "Z"})


@app.post("/run-pilot")
def run_pilot_endpoint():
    """
    מפעיל את הפיילוט ומחזיר JSON עם סטטוס ומטא-דאטה.
    """
    ok, msg = run_pilot()
    if not ok:
        return jsonify({"ok": False, "message": msg}), 500

    if not OUTFILE.exists():
        return jsonify({"ok": False, "message": f"הפיילוט רץ אבל לא נוצר קובץ: {OUTFILE}"}), 500

    st = OUTFILE.stat()
    return jsonify(
        {
            "ok": True,
            "message": msg,
            "outfile": str(OUTFILE),
            "size_bytes": st.st_size,
            "modified_utc": datetime.utcfromtimestamp(st.st_mtime).isoformat() + "Z",
        }
    )


@app.post("/run-pilot/from-files")
def run_pilot_from_files():
    """
    מתאים ל-n8n Cloud:
    - שולחים multipart/form-data עם 3 קבצים:
      - weekly: feedback report 122025.xlsx
      - history: full feedback report 25112025.xlsx
      - mapping: error_code_mapping_final.xlsx
    - השרת מריץ את הפיילוט בתוך תיקייה זמנית ומחזיר את Pilot_Results_v2.xlsx כ-binary.
    """
    weekly = request.files.get("weekly")
    history = request.files.get("history")
    mapping = request.files.get("mapping")

    missing = [name for name, f in [("weekly", weekly), ("history", history), ("mapping", mapping)] if f is None]
    if missing:
        return jsonify({"ok": False, "message": f"חסרים קבצים בבקשה: {', '.join(missing)}"}), 400

    with tempfile.TemporaryDirectory(prefix="pilot_run_") as td:
        work_dir = Path(td)

        # שמירה בשמות שהפיילוט מצפה להם (קבצים יחסיים)
        (work_dir / "feedback report 122025.xlsx").write_bytes(weekly.read())
        (work_dir / "full feedback report 25112025.xlsx").write_bytes(history.read())
        (work_dir / "error_code_mapping_final.xlsx").write_bytes(mapping.read())

        ok, msg, out_path = run_pilot_in_dir(work_dir)
        if not ok or out_path is None:
            return jsonify({"ok": False, "message": msg}), 500

        data = out_path.read_bytes()
        bio = io.BytesIO(data)
        response = send_file(
            bio,
            as_attachment=True,
            download_name="Pilot_Results_v2.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            max_age=0,
        )
        response.headers["Content-Length"] = str(len(data))
        response.headers["Cache-Control"] = "no-store"
        response.headers["Connection"] = "close"
        response.headers["X-Pilot-Runner"] = "ok"
        response.headers["X-Pilot-Message"] = msg[:5000]
        return response


def _load_service_account():
    """טוען service account JSON מ-env var GMAIL_SERVICE_ACCOUNT_B64 (base64 מקודד)."""
    import json as _json
    raw = os.environ.get('GMAIL_SERVICE_ACCOUNT_B64')
    if not raw:
        return None
    try:
        return _json.loads(base64.b64decode(raw))
    except Exception as e:
        print(f"[WARN] לא הצלחתי לטעון GMAIL_SERVICE_ACCOUNT_B64: {e}")
        return None


def _check_api_key():
    """מוודא שה-header X-API-Key תואם ל-API_SECRET_KEY. מחזיר None אם תקין, response אם לא."""
    secret = os.environ.get("API_SECRET_KEY")
    if secret and request.headers.get("X-API-Key") != secret:
        return jsonify({"ok": False, "message": "Unauthorized"}), 401
    return None


@app.post("/run-pilot/from-api")
def run_pilot_from_api():
    """
    Endpoint לעיבוד מ-API של דוד.
    קלט (multipart/form-data):
      - access_token: Bearer token לקריאת API דוד
      - api_base: base URL של API דוד
      - start_date: (optional) תאריך התחלה, ברירת מחדל 2022-01-01
      - top: (optional) מקסימום רשומות, ברירת מחדל 10000
      - account_manager_email: (optional) פילטר לפי מנהל תיק
      - mapping: קובץ XLSX של מיפוי קודי שגיאה (מ-Google Drive)
    פלט: JSON עם drafts (תוצאות יצירה ב-Gmail) + update_payload
    """
    err = _check_api_key()
    if err:
        return err

    import json as _json
    import sys as _sys
    import pandas as _pd
    import requests as _requests

    # --- קריאה ישירה ל-API של דוד ---
    access_token = request.form.get("access_token", "").strip().lstrip("=")
    api_base = request.form.get("api_base", "").strip().lstrip("=")
    if not access_token or not api_base:
        return jsonify({"ok": False, "message": "חסרים שדות access_token ו/או api_base בבקשה"}), 400

    start_date = request.form.get("start_date", "2022-01-01").strip().lstrip("=")
    top = request.form.get("top", "10000").strip().lstrip("=")
    acct_mgr = request.form.get("account_manager_email", "").strip().lstrip("=")

    body = {"StartDate": start_date, "top": int(top)}
    if acct_mgr:
        body["AccountManagerEmail"] = acct_mgr

    try:
        resp = _requests.post(
            f"{api_base}/services/AutomationFeedback/GetFeedbackData",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=body,
            timeout=120,
        )
        resp.raise_for_status()
        records_list = resp.json()
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בקריאת API של דוד: {e}"}), 502

    if not isinstance(records_list, list):
        return jsonify({"ok": False, "message": "תגובת API של דוד אינה JSON array"}), 502

    # --- mapping ---
    mapping_file = request.files.get("mapping")
    if mapping_file is None:
        return jsonify({"ok": False, "message": "חסר קובץ mapping בבקשה"}), 400
    try:
        df_map = _pd.read_excel(io.BytesIO(mapping_file.read()))
        mapping_dict = df_map.set_index("ErrorCodeV4Id").to_dict("index")
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בקריאת mapping: {e}"}), 400

    # --- service account ---
    service_account_info = _load_service_account()

    # --- עיבוד + יצירת דרפטים ---
    try:
        _sys.path.insert(0, str(APP_DIR))
        from pilot_engine import process_from_api_records
        result = process_from_api_records(records_list, mapping_dict, service_account_info=service_account_info)
    except Exception as e:
        return jsonify({"ok": False, "message": f"שגיאה בעיבוד: {e}"}), 500

    return jsonify(result)


@app.route("/run-pilot/file", methods=["GET", "POST"])
def run_pilot_file_endpoint():
    """
    מפעיל את הפיילוט ומחזיר את Pilot_Results_v2.xlsx כקובץ להורדה.
    n8n Cloud יכול למשוך את התגובה כ-binary.
    """
    ok, msg = run_pilot()
    if not ok:
        return jsonify({"ok": False, "message": msg}), 500

    if not OUTFILE.exists():
        return jsonify({"ok": False, "message": f"הפיילוט רץ אבל לא נוצר קובץ: {OUTFILE}"}), 500

    # לקריאה יציבה מ-n8n Cloud דרך ngrok: נטען לזיכרון ונחזיר כתשובה "פשוטה"
    # (בלי ETag/conditional/range) + Connection: close כדי למנוע בעיות TLS/keep-alive.
    data = OUTFILE.read_bytes()
    bio = io.BytesIO(data)
    response = send_file(
        bio,
        as_attachment=True,
        download_name="Pilot_Results_v2.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        max_age=0,
    )
    response.headers["Content-Length"] = str(len(data))
    response.headers["Cache-Control"] = "no-store"
    response.headers["Connection"] = "close"
    response.headers["X-Pilot-Runner"] = "ok"
    response.headers["X-Pilot-Message"] = msg[:5000]  # מגבלה סבירה
    return response


@app.get("/latest")
def latest_info():
    """
    מחזיר מידע על הקובץ האחרון (בלי להריץ את הפיילוט).
    """
    if not OUTFILE.exists():
        return jsonify({"ok": False, "message": "Pilot_Results_v2.xlsx לא קיים עדיין"}), 404
    st = OUTFILE.stat()
    return jsonify(
        {
            "ok": True,
            "outfile": str(OUTFILE),
            "size_bytes": st.st_size,
            "modified_utc": datetime.utcfromtimestamp(st.st_mtime).isoformat() + "Z",
        }
    )


@app.get("/latest/file")
def latest_file():
    """
    מחזיר את הקובץ האחרון להורדה (בלי להריץ את הפיילוט).
    זה נתיב יציב יותר ל-n8n כי אין “זמן המתנה” בתוך אותה בקשה.
    """
    if not OUTFILE.exists():
        return jsonify({"ok": False, "message": "Pilot_Results_v2.xlsx לא קיים עדיין"}), 404

    data = OUTFILE.read_bytes()
    bio = io.BytesIO(data)
    response = send_file(
        bio,
        as_attachment=True,
        download_name="Pilot_Results_v2.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        max_age=0,
    )
    response.headers["Content-Length"] = str(len(data))
    response.headers["Cache-Control"] = "no-store"
    response.headers["Connection"] = "close"
    response.headers["X-Pilot-Runner"] = "ok"
    return response


if __name__ == "__main__":
    # ברירת מחדל: שרת מקומי. לחשיפה ל-n8n Cloud השתמש ב-ngrok/Cloudflare Tunnel.
    host = os.environ.get("PILOT_RUNNER_HOST", "127.0.0.1")
    port = int(os.environ.get("PILOT_RUNNER_PORT", "8787"))
    app.run(host=host, port=port, debug=False)

