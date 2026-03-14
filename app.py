import random
import smtplib
import mysql.connector
from flask import Flask, render_template, request, redirect, session, flash, jsonify, make_response, url_for
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

import os
import uuid
import json
import requests
import pandas as pd
from werkzeug.utils import secure_filename
from functools import wraps
import sys

app = Flask(__name__)
app.secret_key = "super_secret_key_change_this"

# Global config dict — populated at startup from config.json
config = {}

@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON for any unhandled exception on API routes."""
    from flask import request as freq
    if freq.path.startswith("/project") and freq.method == "POST":
        return jsonify({"success": False, "error": str(e)}), 500
    raise e

# =========================
# CONFIG
# =========================
UPLOAD_FOLDER = "files"
ALLOWED_EXTENSIONS = {"csv", "xlsx"}
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

KRUTRIM_API_KEY = "PQSKNigTtacTPl4tFFpdJ555FtQoHYs"
AI_MODEL = "gpt-oss-120b"
AI_API_URL = "https://cloud.olakrutrim.com/v1/chat/completions"


# =========================
# DATABASE CONNECTION
# =========================
def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="MyNewPass",
        database="dvt_platform"
    )


# =========================
# GENERATE 6 DIGIT OTP
# =========================
def generate_otp():
    return str(random.randint(100000, 999999))


# =========================
# SEND EMAIL FUNCTION
# =========================
def send_email(receiver_email, otp):
    sender_email = config.get("sender_email", "")
    app_password = config.get("email_pass", "")

    subject = "Your DVT Verification Code"

    # ── Plain-text fallback ──
    plain = f"""DVT — Verification Code

Your one-time code: {otp}

This code expires in 5 minutes.
If you did not request this, you can safely ignore this email.

— DVT Platform"""

    # ── HTML email matching app black & white theme ──
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>DVT Verification Code</title>
</head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f5f5f5;padding:48px 0;">
  <tr><td align="center">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:480px;">

      <!-- HEADER -->
      <tr>
        <td style="background:#0a0a0a;border-radius:16px 16px 0 0;padding:22px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td>
                <table cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="background:#ffffff;border-radius:7px;width:28px;height:28px;text-align:center;vertical-align:middle;">
                      <span style="font-size:13px;font-weight:900;color:#0a0a0a;letter-spacing:-0.5px;">D</span>
                    </td>
                    <td style="padding-left:9px;">
                      <span style="font-size:15px;font-weight:700;color:#ffffff;letter-spacing:-0.3px;">DVT</span>
                    </td>
                  </tr>
                </table>
              </td>
              <td align="right">
                <span style="font-size:10px;font-weight:600;color:rgba(255,255,255,0.35);text-transform:uppercase;letter-spacing:0.1em;">Sign-in Code</span>
              </td>
            </tr>
          </table>
        </td>
      </tr>

      <!-- BODY -->
      <tr>
        <td style="background:#ffffff;padding:40px 32px 32px;border-left:1px solid #e8e8e8;border-right:1px solid #e8e8e8;">

          <h1 style="margin:0 0 8px;font-size:22px;font-weight:700;color:#0a0a0a;letter-spacing:-0.3px;text-align:center;">
            Verify your identity
          </h1>
          <p style="margin:0 0 32px;font-size:14px;color:#737373;text-align:center;line-height:1.6;">
            Use the code below to sign in to DVT.<br/>
            It expires in <strong style="color:#0a0a0a;">5 minutes</strong>.
          </p>

          <!-- OTP -->
          <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:32px;">
            <tr>
              <td align="center">
                <table cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="background:#0a0a0a;border-radius:14px;padding:20px 48px;text-align:center;">
                      <span style="font-size:38px;font-weight:700;color:#ffffff;letter-spacing:14px;font-family:'Courier New',Courier,monospace;">{otp}</span>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>

          <!-- Divider -->
          <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:24px;">
            <tr><td style="border-top:1px solid #f0f0f0;font-size:0;">&nbsp;</td></tr>
          </table>

          <!-- Security note -->
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="background:#f5f5f5;border:1px solid #ebebeb;border-radius:10px;padding:14px 16px;">
                <p style="margin:0;font-size:12px;color:#737373;line-height:1.6;">
                  <strong style="color:#0a0a0a;">Didn't request this?</strong><br/>
                  If you didn't try to sign in, you can safely ignore this email.
                  Your account remains secure.
                </p>
              </td>
            </tr>
          </table>

        </td>
      </tr>

      <!-- FOOTER -->
      <tr>
        <td style="background:#fafafa;border:1px solid #e8e8e8;border-top:none;border-radius:0 0 16px 16px;padding:18px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td><p style="margin:0;font-size:11px;color:#b8b8b8;">© 2026 DVT Platform</p></td>
              <td align="right"><p style="margin:0;font-size:11px;color:#b8b8b8;">v1.0.0</p></td>
            </tr>
          </table>
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject

    # Plain text first (fallback), HTML second (preferred by email clients)
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, app_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print("Email Error:", e)
        return False


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "email" not in session:
            return redirect(url_for("auth_page"))
        return f(*args, **kwargs)
    return decorated_function


# =========================
# AUTH PAGE
# =========================
@app.route("/auth")
def auth_page():
    if "email" in session:
        return redirect(url_for("home_page"))
    response = make_response(render_template("auth.html"))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# =========================
# SEND OTP
# =========================
@app.route("/send-otp", methods=["POST"])
def send_otp():
    email = request.form.get("email")

    if not email:
        flash("Email is required", "error")
        return redirect(url_for("auth_page"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
    user = cursor.fetchone()

    if not user:
        cursor.execute("INSERT INTO users (email) VALUES (%s)", (email,))
        db.commit()

    otp = generate_otp()

    cursor.execute("DELETE FROM otp_veri WHERE email=%s", (email,))
    db.commit()

    cursor.execute("INSERT INTO otp_veri (email, otp) VALUES (%s, %s)", (email, otp))
    db.commit()

    if send_email(email, otp):
        session["temp_email"] = email
        flash("Verification code sent successfully.", "success")
    else:
        flash("Failed to send OTP. Try again.", "error")

    cursor.close()
    db.close()

    return redirect(url_for("auth_page"))


# =========================
# VERIFY OTP
# =========================
@app.route("/verify-otp", methods=["POST"])
def verify_otp():
    otp_entered = request.form.get("otp")
    email = session.get("temp_email")

    if not email:
        flash("Session expired. Please try again.", "error")
        return redirect(url_for("auth_page"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute("SELECT * FROM otp_veri WHERE email=%s AND otp=%s", (email, otp_entered))
    result = cursor.fetchone()

    if result:
        cursor.execute("DELETE FROM otp_veri WHERE email=%s", (email,))
        db.commit()
        session.pop("temp_email", None)
        session["email"] = email
        cursor.close()
        db.close()
        return redirect(url_for("home_page"))
    else:
        flash("Invalid OTP. Please try again.", "error")
        cursor.close()
        db.close()
        return redirect(url_for("auth_page"))


# =========================
# RESEND OTP (AJAX)
# =========================
@app.route("/resend-otp", methods=["POST"])
def resend_otp():
    data = request.get_json()
    email = data.get("email")

    if not email:
        return jsonify({"success": False})

    db = get_db()
    cursor = db.cursor()

    otp = generate_otp()

    cursor.execute("DELETE FROM otp_veri WHERE email=%s", (email,))
    db.commit()

    cursor.execute("INSERT INTO otp_veri (email, otp) VALUES (%s, %s)", (email, otp))
    db.commit()

    success = send_email(email, otp)

    cursor.close()
    db.close()

    return jsonify({"success": success})


@app.route("/check-session")
def check_session():
    return jsonify({"logged_in": "email" in session})


# =========================
# HOME
# =========================
@app.route("/")
@app.route("/")
@login_required
def home_page():
    user_email = session["email"]

    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT project_id, project_name, project_category,
               project_date, project_time, file_name
        FROM projects
        WHERE email = %s
        ORDER BY created_at DESC
    """, (user_email,))

    projects = cursor.fetchall()
    cursor.close()
    db.close()

    return render_template("home.html", projects=projects, user_email=user_email)


# =========================
# LOGOUT
# =========================
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth_page"))


# =========================
# CREATE PAGE
# =========================
@app.route("/create")
@login_required
def create_page():
    return render_template("create.html", user_email=session["email"])


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/create-project", methods=["POST"])
@login_required
def create_project():
    user_email = session["email"]

    project_name = request.form.get("project_name")
    project_category = request.form.get("project_category")
    file = request.files.get("file")

    if not project_name or len(project_name.strip()) < 3:
        flash("Project name must be at least 3 characters.", "error")
        return redirect(url_for("create_page"))

    if not file or file.filename == "":
        flash("Please upload a file.", "error")
        return redirect(url_for("create_page"))

    if not allowed_file(file.filename):
        flash("Only CSV and XLSX files are allowed.", "error")
        return redirect(url_for("create_page"))

    original_filename = secure_filename(file.filename)
    extension = original_filename.rsplit(".", 1)[1].lower()
    unique_filename = f"{uuid.uuid4().hex}.{extension}"
    save_path = os.path.join(app.config["UPLOAD_FOLDER"], unique_filename)
    file.save(save_path)

    now = datetime.now()
    project_date = now.date()
    project_time = now.strftime("%H:%M:%S")

    db = get_db()
    cursor = db.cursor()

    cursor.execute("""
        INSERT INTO projects
        (email, project_name, project_category, project_date, project_time, file_name)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (user_email, project_name.strip(), project_category, project_date, project_time, unique_filename))

    db.commit()
    cursor.close()
    db.close()

    flash("Project created successfully!", "success")
    return redirect(url_for("create_page", created="1"))


# =========================
# PROJECT PAGE
# =========================
@app.route("/project/<int:project_id>")
@login_required
def project_page(project_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT * FROM projects
        WHERE project_id = %s AND email = %s
    """, (project_id, session["email"]))

    project = cursor.fetchone()
    cursor.close()
    db.close()

    if not project:
        flash("Project not found.", "error")
        return redirect(url_for("home_page"))

    file_path = os.path.join("files", project["file_name"])

    if not os.path.exists(file_path):
        flash("File not found.", "error")
        return redirect(url_for("home_page"))

    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        columns = df.columns.tolist()

        scores_exist = False
        existing_scores = {}
        if len(df) > 0:
            try:
                last_vals = df.iloc[-1].apply(pd.to_numeric, errors="coerce")
                if last_vals.notna().all() and last_vals.between(0, 100).all():
                    scores_exist = True
                    existing_scores = {k: int(v) for k, v in last_vals.items()}
            except Exception:
                pass

        data_df = df.iloc[:-1] if scores_exist else df

        total_records = len(data_df)
        invalid_records = int(data_df.isnull().any(axis=1).sum())
        valid_records = total_records - invalid_records
        warning_records = int(data_df.duplicated().sum())

        preview_data = data_df.head(20).to_dict(orient="records")

    except Exception as e:
        print("File Read Error:", e)
        flash("Error reading file.", "error")
        return redirect(url_for("home_page"))

    return render_template(
        "project.html",
        project=project,
        total_records=total_records,
        valid_records=valid_records,
        invalid_records=invalid_records,
        warning_records=warning_records,
        columns=columns,
        preview_data=preview_data,
        scores_exist=scores_exist,
        existing_scores=existing_scores,
        user_email=session["email"]
    )


# =========================
# AI COLUMN SCORING ROUTE
# =========================
@app.route("/project/<int:project_id>/score-columns", methods=["POST"])
@login_required
def score_columns(project_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT * FROM projects
        WHERE project_id = %s AND email = %s
    """, (project_id, session["email"]))

    project = cursor.fetchone()
    cursor.close()
    db.close()

    if not project:
        return jsonify({"success": False, "error": "Project not found."}), 404

    file_path = os.path.join("files", project["file_name"])

    if not os.path.exists(file_path):
        return jsonify({"success": False, "error": "File not found."}), 404

    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read file: {str(e)}"}), 500

    columns = df.columns.tolist()
    category = project.get("project_category", "general data")

    column_list_str = ", ".join(columns)

    prompt = f"""You are a data quality expert. You are given a list of column names from a {category} dataset.

Your task is to assign a relevance score (an integer from 0 to 100) to each column based on how important and meaningful that column name is for a {category} dataset.

Rules:
- Score 75-100: Core, critical columns that are essential for this type of dataset
- Score 50-74: Important but secondary columns
- Score 25-49: Marginally relevant or generic columns
- Score 0-24: Irrelevant, redundant, or unclear columns

Column names: {column_list_str}

Respond ONLY with a valid JSON object where keys are the exact column names and values are integer scores. No explanation, no markdown, no extra text. Example format:
{{"column_name_1": 85, "column_name_2": 42}}"""

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + KRUTRIM_API_KEY
        }
        payload = {
            "model": AI_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 1024
        }
        response = requests.post(AI_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        ai_text = result["choices"][0]["message"]["content"].strip()

        if ai_text.startswith("```"):
            ai_text = ai_text.split("```")[1]
            if ai_text.startswith("json"):
                ai_text = ai_text[4:]

        scores = json.loads(ai_text)

        validated_scores = {}
        for col in columns:
            raw = scores.get(col, scores.get(str(col), 50))
            validated_scores[col] = max(0, min(100, int(raw)))

    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "AI request timed out. Please try again."}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"AI API error: {str(e)}"}), 502
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return jsonify({"success": False, "error": f"Could not parse AI response: {str(e)}"}), 500

    try:
        if file_path.endswith(".csv"):
            df_fresh = pd.read_csv(file_path)
        else:
            df_fresh = pd.read_excel(file_path)

        if len(df_fresh) > 0:
            try:
                last_vals = df_fresh.iloc[-1].apply(pd.to_numeric, errors="coerce")
                if last_vals.notna().all() and last_vals.between(0, 100).all():
                    df_fresh = df_fresh.iloc[:-1]
            except Exception:
                pass

        score_dict = {col: int(validated_scores.get(col, 0)) for col in df_fresh.columns}
        score_row = pd.DataFrame([score_dict])

        updated_df = pd.concat([df_fresh, score_row], ignore_index=True)

        if file_path.endswith(".csv"):
            updated_df.to_csv(file_path, index=False)
        else:
            updated_df.to_excel(file_path, index=False)

        print(f"Score row saved to {file_path}, total rows now: {len(updated_df)}")

    except Exception as e:
        print("File save error:", e)
        import traceback; traceback.print_exc()

    return jsonify({
        "success": True,
        "scores": validated_scores,
        "columns": columns
    })


# =========================
# GENERATE AI PROMPTS FOR COLUMNS
# =========================
@app.route("/project/<int:project_id>/generate-prompts", methods=["POST"])
@login_required
def generate_prompts(project_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM projects WHERE project_id = %s AND email = %s",
                   (project_id, session["email"]))
    project = cursor.fetchone()
    cursor.close()
    db.close()

    if not project:
        return jsonify({"success": False, "error": "Project not found."}), 404

    data = request.get_json()
    columns = data.get("columns", [])

    if not columns:
        return jsonify({"success": False, "error": "No columns provided."}), 400

    category = project.get("project_category", "general data")
    col_list = ", ".join(columns)

    prompt = f"""You are a data validation expert. For each column listed below from a '{category}' dataset, write a concise validation prompt describing what rules, formats, or conditions the data in that column should satisfy.

Rules for your response:
- Each prompt must be max 80 words
- Use comma-separated conditions within each prompt
- Be specific and practical for data validation
- Return ONLY a valid JSON object, no markdown, no explanation

Columns: {col_list}

Response format example:
{{"column_name": "must be non-empty, must be a valid email format, no special characters except @ and ., max 100 characters"}}

Now generate for these columns: {col_list}"""

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + KRUTRIM_API_KEY
        }
        payload = {
            "model": AI_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 2048
        }
        response = requests.post(AI_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        ai_text = result["choices"][0]["message"]["content"].strip()

        if ai_text.startswith("```"):
            lines = ai_text.splitlines()
            ai_text = "\n".join(lines[1:])
            ai_text = ai_text.rsplit("```", 1)[0].strip()

        prompts = json.loads(ai_text)

        cleaned = {}
        for col in columns:
            raw = prompts.get(col, "Must be non-empty, must have valid format, no null values allowed.")
            words = raw.split()
            if len(words) > 80:
                raw = " ".join(words[:80])
            cleaned[col] = raw

        return jsonify({"success": True, "prompts": cleaned})

    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "Request timed out."}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"API error: {str(e)}"}), 502
    except (json.JSONDecodeError, KeyError) as e:
        return jsonify({"success": False, "error": f"Could not parse AI response: {str(e)}"}), 500


# =========================
# START PROCESSING
# =========================
@app.route("/project/<int:project_id>/start-processing", methods=["POST"])
@login_required
def start_processing(project_id):
    import threading

    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM projects WHERE project_id = %s AND email = %s",
        (project_id, session["email"])
    )
    project = cursor.fetchone()
    cursor.close()
    db.close()

    if not project:
        return jsonify({"success": False, "error": "Project not found."}), 404

    data = request.get_json()
    col_config = data.get("config", [])

    if not col_config:
        return jsonify({"success": False, "error": "No column config provided."}), 400

    file_name = project["file_name"]
    file_path = os.path.join("files", file_name)
    base_name = os.path.splitext(file_name)[0]

    runner_dir = "runner"
    os.makedirs(runner_dir, exist_ok=True)
    script_path  = os.path.join(runner_dir, base_name + ".py")
    status_path  = os.path.join(runner_dir, base_name + "_status.json")
    output_path  = os.path.join("files", base_name + ".xlsx")

    rule_lines = []
    for c in col_config:
        rule_lines.append('Column "' + c["column"] + '": ' + c["prompt"])
    col_rules_str = "\n".join(rule_lines)

    ai_prompt = (
        "You are an expert Python developer. Write a complete standalone Python script.\n\n"
        "INPUT FILE  : " + file_path + "\n"
        "OUTPUT FILE : " + output_path + " (same file as input, overwrite it)\n"
        "STATUS FILE : " + status_path + "\n\n"
        "VALIDATION RULES PER COLUMN:\n"
        + col_rules_str + "\n\n"
        "EXACT CODE STRUCTURE TO FOLLOW:\n"
        "\n"
        "import pandas as pd\n"
        "import json, os, re\n"
        "from openpyxl import Workbook\n"
        "from openpyxl.styles import PatternFill\n"
        "\n"
        "INPUT_FILE  = r'" + file_path  + "'\n"
        "OUTPUT_FILE = r'" + output_path + "'  # overwrite the original file\n"
        "STATUS_FILE = r'" + status_path + "'\n"
        "\n"
        "RED_FILL = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')\n"
        "\n"
        "def write_status(status, progress, total, processed, invalid_rows_count, logs, error=None):\n"
        "    tmp_path = STATUS_FILE + '.tmp'\n"
        "    with open(tmp_path, 'w') as f:\n"
        "        json.dump({'status': status, 'progress': progress, 'total_rows': total,\n"
        "                   'processed_rows': processed, 'invalid_rows': invalid_rows_count,\n"
        "                   'logs': logs, 'error': error}, f)\n"
        "    import shutil; shutil.move(tmp_path, STATUS_FILE)\n"
        "\n"
        "# --- ONE FUNCTION PER COLUMN ---\n"
        "# Returns (True, '') if valid, (False, 'reason') if invalid\n"
        "\n"
        "# --- MAIN EXECUTION ---\n"
        "# 1. Read file, detect/save score row, copy to plain lists, del df\n"
        "# 2. Build col_to_func and col_index_map\n"
        "# 3. Create new Workbook, write headers + Efficiency_Score column\n"
        "# 4. Loop rows: write cells, validate, apply RED fill, compute efficiency\n"
        "# 5. Write score row back as last row\n"
        "# 6. wb.save(OUTPUT_FILE)\n"
        "# 7. write_status('complete', 100, ...)\n"
        "\n"
        "OUTPUT ONLY THE COMPLETE PYTHON SCRIPT. NO MARKDOWN. NO EXPLANATION.\n"
    )

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + KRUTRIM_API_KEY
        }
        payload = {
            "model": AI_MODEL,
            "messages": [{"role": "user", "content": ai_prompt}],
            "temperature": 0.1,
            "max_tokens": 8192
        }
        response = requests.post(AI_API_URL, headers=headers, json=payload, timeout=90)
        response.raise_for_status()
        result = response.json()
        script_code = result["choices"][0]["message"]["content"].strip()

        if script_code.startswith("```"):
            script_lines = script_code.splitlines()
            script_code = "\n".join(script_lines[1:])
            script_code = script_code.rsplit("```", 1)[0].strip()

    except Exception as e:
        return jsonify({"success": False, "error": "AI generation failed: " + str(e)}), 500

    with open(script_path, "w", encoding="utf-8") as f:
        f.write(script_code)

    import shutil as _shutil
    _tmp = status_path + ".tmp"
    with open(_tmp, "w", encoding="utf-8") as f:
        json.dump({
            "status": "running",
            "progress": 0,
            "total_rows": 0,
            "processed_rows": 0,
            "invalid_rows": 0,
            "logs": ["Script generated. Execution starting..."],
            "error": None
        }, f)
    _shutil.move(_tmp, status_path)

    def run_script():
        import subprocess as sp
        try:
            sp.run(["python", script_path], timeout=600, check=False)
        except Exception as e:
            import shutil as _sh
            _etmp = status_path + ".tmp"
            with open(_etmp, "w", encoding="utf-8") as sf:
                json.dump({
                    "status": "error",
                    "progress": 0,
                    "total_rows": 0,
                    "processed_rows": 0,
                    "invalid_rows": 0,
                    "logs": ["Execution error: " + str(e)],
                    "error": str(e)
                }, sf)
            _sh.move(_etmp, status_path)

    threading.Thread(target=run_script, daemon=True).start()

    return jsonify({
        "success": True,
        "script_path": script_path,
        "job_id": base_name
    })


# =========================
# PROCESSING STATUS POLL
# =========================
@app.route("/project/<int:project_id>/processing-status/<job_id>")
@login_required
def processing_status(project_id, job_id):
    safe_id = secure_filename(job_id)
    status_path = os.path.join("runner", safe_id + "_status.json")

    if not os.path.exists(status_path):
        return jsonify({
            "status": "pending", "progress": 0,
            "logs": ["Waiting to start..."], "invalid_rows": 0,
            "total_rows": 0, "processed_rows": 0
        })

    import time
    for attempt in range(5):
        try:
            with open(status_path, "r", encoding="utf-8") as f:
                data = f.read().strip()
            if data:
                return jsonify(json.loads(data))
        except Exception:
            pass
        time.sleep(0.1)

    return jsonify({"status": "running", "progress": 0,
                    "logs": ["Processing..."], "invalid_rows": 0,
                    "total_rows": 0, "processed_rows": 0})


# =========================
# VIEW GENERATED SCRIPT
# =========================
@app.route("/project/<int:project_id>/get-script/<job_id>")
@login_required
def get_script(project_id, job_id):
    safe_id = secure_filename(job_id)
    script_path = os.path.join("runner", safe_id + ".py")
    if not os.path.exists(script_path):
        return jsonify({"success": False, "error": "Script not found."}), 404
    with open(script_path, "r", encoding="utf-8") as f:
        return jsonify({"success": True, "code": f.read()})


# =========================
# FULL FILE DATA FOR TABLE
# =========================
@app.route("/project/<int:project_id>/full-data")
@login_required
def full_data(project_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM projects WHERE project_id = %s AND email = %s",
                   (project_id, session["email"]))
    project = cursor.fetchone()
    cursor.close()
    db.close()

    if not project:
        return jsonify({"success": False, "error": "Project not found"}), 404

    base_name = os.path.splitext(project["file_name"])[0]
    xlsx_path = os.path.join("files", base_name + ".xlsx")
    csv_path  = os.path.join("files", project["file_name"])
    file_path = xlsx_path if os.path.exists(xlsx_path) else csv_path

    if not os.path.exists(file_path):
        return jsonify({"success": False, "error": "File not found"}), 404

    try:
        if file_path.endswith(".xlsx"):
            from openpyxl import load_workbook
            wb = load_workbook(file_path)
            ws = wb.active
            headers = []
            rows = []
            red_cells = []

            for r_idx, row in enumerate(ws.iter_rows()):
                if r_idx == 0:
                    headers = [str(c.value) if c.value is not None else "" for c in row]
                    continue
                row_data = []
                for c_idx, cell in enumerate(row):
                    val = cell.value
                    row_data.append("" if val is None else val)
                    fill = cell.fill
                    if fill and fill.fill_type == "solid" and fill.fgColor:
                        rgb = fill.fgColor.rgb if hasattr(fill.fgColor, "rgb") else ""
                        if rgb in ("FFFF0000", "FF0000"):
                            red_cells.append({"row": r_idx - 1, "col": c_idx})
                rows.append(row_data)
        else:
            df = pd.read_csv(file_path)
            headers = list(df.columns)
            rows = df.values.tolist()
            rows = [["" if (isinstance(v, float) and pd.isna(v)) else v for v in r] for r in rows]
            red_cells = []

        return jsonify({
            "success": True,
            "headers": headers,
            "rows": rows,
            "red_cells": red_cells,
            "total": len(rows)
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


# =========================
# DOWNLOAD FILTERED DATA
# =========================
@app.route("/project/<int:project_id>/download-filtered", methods=["POST"])
@login_required
def download_filtered(project_id):
    from io import BytesIO
    from openpyxl import Workbook as XLWorkbook
    from flask import send_file

    data = request.get_json()
    headers = data.get("headers", [])
    rows    = data.get("rows", [])

    if not headers or not rows:
        return jsonify({"success": False, "error": "No data"}), 400

    wb = XLWorkbook()
    ws = wb.active
    ws.title = "Filtered Data"

    for c_idx, h in enumerate(headers, start=1):
        ws.cell(row=1, column=c_idx, value=str(h))

    for r_idx, row in enumerate(rows, start=2):
        for c_idx, val in enumerate(row, start=1):
            ws.cell(row=r_idx, column=c_idx, value=val)

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)

    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="filtered_data.xlsx"
    )


# =========================
# STOP PROCESSING
# =========================
@app.route("/project/<int:project_id>/stop-processing/<job_id>", methods=["POST"])
@login_required
def stop_processing(project_id, job_id):
    import shutil
    base_name = job_id
    status_path = os.path.join("runner", base_name + "_status.json")
    tmp_path = status_path + ".tmp"
    try:
        current = {}
        if os.path.exists(status_path):
            with open(status_path, "r") as f:
                current = json.load(f)

        current["status"] = "stopped"
        current["error"] = "Stopped by user"

        with open(tmp_path, "w") as f:
            json.dump(current, f)
        shutil.move(tmp_path, status_path)

        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================
# DELETE PROJECT
# =========================
@app.route("/delete-project/<int:project_id>", methods=["POST"])
@login_required
def delete_project(project_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    cursor.execute(
        "SELECT * FROM projects WHERE project_id = %s AND email = %s",
        (project_id, session["email"])
    )
    project = cursor.fetchone()

    if not project:
        cursor.close()
        db.close()
        return jsonify({"success": False, "error": "Project not found."}), 404

    base_name = os.path.splitext(project["file_name"])[0]

    # Delete uploaded files (original + processed xlsx)
    for fname in [project["file_name"], base_name + ".xlsx"]:
        path = os.path.join("files", fname)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                print(f"Could not delete file {path}: {e}")

    # Delete runner script + status file
    for suffix in [".py", "_status.json"]:
        path = os.path.join("runner", base_name + suffix)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                print(f"Could not delete runner file {path}: {e}")

    # Delete from database
    cursor.execute(
        "DELETE FROM projects WHERE project_id = %s AND email = %s",
        (project_id, session["email"])
    )
    db.commit()
    cursor.close()
    db.close()

    return jsonify({"success": True})


# =========================
# SETUP & RUN
# =========================

CONFIG_FILE = "config.json"
SQL_FILE    = "dvt_platform_backup.sql"


def _print_banner():
    print()
    print("  ╔══════════════════════════════════════╗")
    print("  ║          DVT Platform Setup          ║")
    print("  ╚══════════════════════════════════════╝")
    print()


def _ok(msg):   print(f"  ✔  {msg}")
def _err(msg):  print(f"  ✘  {msg}")
def _info(msg): print(f"  ·  {msg}")
def _ask(prompt, secret=False):
    label = f"  ▸  {prompt}: "
    return input(label).strip()


# ── Step 1: MySQL ──────────────────────────────────────────────────────────
def setup_mysql():
    print("  ── MySQL Database ─────────────────────────────")
    while True:
        host = _ask("Host [localhost]") or "localhost"
        user = _ask("User")
        password = _ask("Password", secret=True)

        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password
            )
            cursor = conn.cursor()

            # Create database if missing
            cursor.execute("CREATE DATABASE IF NOT EXISTS dvt_platform")
            conn.commit()
            _ok("Connected to MySQL and ensured database exists.")

            # Check if tables already exist
            cursor.execute("USE dvt_platform")
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

            if "projects" in tables and "users" in tables:
                _ok("Tables already exist — skipping SQL import.")
            else:
                # Import SQL backup
                if not os.path.exists(SQL_FILE):
                    _err(f"SQL backup file '{SQL_FILE}' not found. "
                         "Please place it in the same folder as app.py.")
                    sys.exit(1)

                _info(f"Importing '{SQL_FILE}'…")
                with open(SQL_FILE, "r", encoding="utf-8") as f:
                    sql_content = f.read()

                result = subprocess.run(
                    ["mysql", "-h", host, "-u", user,
                     f"-p{password}", "dvt_platform"],
                    input=sql_content,
                    text=True,
                    capture_output=True
                )
                if result.returncode != 0:
                    _err(f"SQL import failed: {result.stderr.strip()}")
                    sys.exit(1)
                _ok("Database imported successfully.")

            return host, user, password

        except mysql.connector.Error as e:
            _err(f"MySQL connection failed: {e}")
            _info("Please check your credentials and try again.")
            print()


# ── Step 2: SMTP / Gmail ───────────────────────────────────────────────────
def setup_smtp():
    print()
    print("  ── Gmail SMTP ─────────────────────────────────")
    _info("You need a Gmail App Password (not your regular password).")
    _info("Generate one at: myaccount.google.com/apppasswords")
    print()
    while True:
        sender = _ask("Sender Gmail address")
        if not sender or "@" not in sender:
            _err("Please enter a valid Gmail address.")
            continue
        app_pass = _ask("Gmail App Password", secret=True)
        if not app_pass:
            _err("App password cannot be empty.")
            continue
        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(sender, app_pass)
            server.quit()
            _ok(f"SMTP authenticated as {sender}.")
            return sender, app_pass
        except smtplib.SMTPAuthenticationError:
            _err("Authentication failed — check your email and app password. Try again.")
        except Exception as e:
            _err(f"SMTP error: {e}")
            _info("Retrying…")
        print()


# ── Step 3: Krutrim API Key ────────────────────────────────────────────────
def setup_krutrim():
    print()
    print("  ── Krutrim AI API ─────────────────────────────")
    _info("Get your API key from: cloud.olakrutrim.com")
    print()
    while True:
        api_key = _ask("Krutrim API Key", secret=True)
        if not api_key:
            _err("API key cannot be empty.")
            continue

        # Quick sanity-check against the API
        _info("Verifying API key…")
        try:
            resp = requests.post(
                "https://cloud.olakrutrim.com/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                },
                json={
                    "model": "gpt-oss-120b",
                    "messages": [{"role": "user", "content": "ping"}],
                    "max_tokens": 5
                },
                timeout=15
            )
            if resp.status_code in (200, 201):
                _ok("API key verified.")
                return api_key
            elif resp.status_code == 401:
                _err("Invalid API key — unauthorised. Try again.")
            else:
                # Non-auth error (quota, model, etc.) — key is likely fine
                _ok(f"API reachable (status {resp.status_code}) — accepting key.")
                return api_key
        except requests.exceptions.Timeout:
            _info("Verification timed out — accepting key and continuing.")
            return api_key
        except Exception as e:
            _info(f"Could not verify ({e}) — accepting key and continuing.")
            return api_key
        print()


# ── Main entrypoint ────────────────────────────────────────────────────────
def load_or_setup():
    global KRUTRIM_API_KEY, config

    if os.path.exists(CONFIG_FILE):
        # Config exists — load and validate DB connection only
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)

            # Quick DB ping to make sure it still works
            conn = mysql.connector.connect(
                host=cfg["db_host"],
                user=cfg["db_user"],
                password=cfg["db_pass"],
                database="dvt_platform"
            )
            conn.close()

            config.update(cfg)
            KRUTRIM_API_KEY = cfg.get("krutrim_api_key", "")
            # Ensure sender_email exists (backward compat with old configs)
            if not cfg.get("sender_email"):
                print()
                print("  ⚠  Sender email not found in config — please re-run setup.")
                print()
                raise KeyError("sender_email missing")
            return  # All good — start normally

        except (mysql.connector.Error, KeyError, json.JSONDecodeError):
            print()
            print("  ⚠  Existing config.json is invalid or DB is unreachable.")
            print("     Running setup again…")
            print()

    # ── First run (or broken config) ──
    _print_banner()

    db_host, db_user, db_pass   = setup_mysql()
    sender_email, email_pass    = setup_smtp()
    api_key                     = setup_krutrim()

    cfg = {
        "db_host":         db_host,
        "db_user":         db_user,
        "db_pass":         db_pass,
        "sender_email":    sender_email,
        "email_pass":      email_pass,
        "krutrim_api_key": api_key
    }

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

    config.update(cfg)
    KRUTRIM_API_KEY = api_key

    print()
    print("  ╔══════════════════════════════════════╗")
    print("  ║   Setup complete — starting DVT…     ║")
    print("  ╚══════════════════════════════════════╝")
    print()


if __name__ == "__main__":
    # Ensure required dirs exist
    os.makedirs("files",  exist_ok=True)
    os.makedirs("runner", exist_ok=True)

    load_or_setup()
    app.run(debug=True)