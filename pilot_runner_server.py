import os
import io
import sys
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
    # cross-platform: נריץ עם ה-python שמריץ את השרת (גם עובד ב-Render/Linux)
    cmd = [sys.executable, str(PILOT_ENGINE)]
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
*** End Patch: Remove stray FastAPI code (keeps Flask-only server) ***
