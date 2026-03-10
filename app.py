# app.py (full updated backend)
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import sqlite3
from fpdf import FPDF
from datetime import datetime, timedelta
import os
import csv
from werkzeug.utils import secure_filename
import math
import re
import traceback

app = Flask(__name__)
CORS(app)

DATABASE = "database.db"
PDF_FOLDER = "pdfs"
UPLOAD_FOLDER = "uploads"
ALLOWED_EXT = {"csv", "pdf", "png", "jpg", "jpeg"}

os.makedirs(PDF_FOLDER, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ---------- DB helpers ----------
def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS bills(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            month TEXT NOT NULL,
            year INTEGER NOT NULL,
            units INTEGER NOT NULL DEFAULT 0,
            amount REAL NOT NULL DEFAULT 0.0,
            receipt_path TEXT,
            status TEXT DEFAULT 'Unpaid',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS reminders(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            bill_id INTEGER,
            reminder_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()

init_db()

# ---------- Utilities ----------
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT

def clean_number_str(v):
    """Remove commas, currency symbols and whitespace from numeric strings."""
    if v is None:
        return ""
    s = str(v).strip()
    s = re.sub(r'[^\d\.\-]', '', s)
    return s

def safe_int(v, default=0):
    try:
        s = clean_number_str(v)
        if s == "" or s == "." or s == "-":
            return default
        return int(float(s))
    except Exception:
        return default

def safe_float(v, default=0.0):
    try:
        s = clean_number_str(v)
        if s == "" or s == "." or s == "-":
            return default
        f = float(s)
        if abs(f) > 1e14:
            return default
        return f
    except Exception:
        return default

@app.errorhandler(Exception)
def handle_exception(e):
    traceback.print_exc()
    return jsonify({"error": "Internal server error", "details": str(e)}), 500

@app.route("/")
def home():
    return jsonify({"message": "Smart Water Backend Running"})

@app.route("/register", methods=["POST"])
def register():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data received"}), 400
    name = data.get("name")
    email = (data.get("email") or "").strip().lower()
    password = data.get("password")
    if not (name and email and password):
        return jsonify({"error": "Missing fields"}), 400
    try:
        conn = get_db()
        conn.execute("INSERT INTO users (name,email,password) VALUES (?,?,?)", (name.strip(), email, password))
        conn.commit()
        conn.close()
        return jsonify({"message": "User Registered Successfully"})
    except sqlite3.IntegrityError:
        return jsonify({"error": "Email Already Exists"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data received"}), 400
    email = (data.get("email") or "").strip().lower()
    password = data.get("password")
    if not (email and password):
        return jsonify({"error": "Missing fields"}), 400
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=? AND password=?", (email, password)).fetchone()
    conn.close()
    if user:
        return jsonify({"message": "Login Success", "user_id": user["id"], "name": user["name"], "is_admin": bool(user["is_admin"])})
    return jsonify({"error": "Invalid Credentials"}), 401

@app.route("/add_bill", methods=["POST"])
def add_bill():
    # Accept both JSON and multipart/form-data (with optional receipt)
    if request.content_type and "multipart/form-data" in request.content_type:
        form = request.form
        user_id = safe_int(form.get("user_id"))
        month = (form.get("month") or "").strip()
        year = safe_int(form.get("year"))
        units = safe_int(form.get("units"))
        amount = safe_float(form.get("amount"))
        receipt_path = None
        if 'receipt' in request.files:
            f = request.files['receipt']
            if f and allowed_file(f.filename):
                filename = secure_filename(f.filename)
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"{ts}_{filename}"
                path = os.path.join(UPLOAD_FOLDER, filename)
                f.save(path)
                receipt_path = filename
    else:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data received"}), 400
        user_id = safe_int(data.get("user_id"))
        month = (data.get("month") or "").strip()
        year = safe_int(data.get("year"))
        units = safe_int(data.get("units"))
        amount = safe_float(data.get("amount"))
        receipt_path = None

    if not user_id or not month or not year:
        return jsonify({"error": "Missing required fields (user_id, month, year)"}), 400

    conn = get_db()
    cursor = conn.cursor()

    existing = cursor.execute("SELECT id FROM bills WHERE user_id=? AND LOWER(month)=LOWER(?) AND year=?", (user_id, month.strip(), year)).fetchone()
    if existing:
        conn.close()
        return jsonify({"error": f"Bill for {month} {year} already exists"}), 400

    cursor.execute("INSERT INTO bills (user_id,month,year,units,amount,receipt_path,status) VALUES (?,?,?,?,?,?,?)",
                   (user_id, month, year, units, amount, receipt_path, 'Unpaid'))

    bill_id = cursor.lastrowid
    reminder_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    cursor.execute("INSERT INTO reminders (user_id,bill_id,reminder_date) VALUES (?,?,?)", (user_id, bill_id, reminder_date))

    conn.commit()
    conn.close()
    return jsonify({"message": "Bill Added Successfully", "bill_id": bill_id})

@app.route("/delete_bill/<int:bill_id>", methods=["POST"])
def delete_bill(bill_id):
    conn = get_db()
    conn.execute("DELETE FROM reminders WHERE bill_id=?", (bill_id,))
    conn.execute("DELETE FROM bills WHERE id=?", (bill_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "Bill deleted"})

@app.route("/upload_bills", methods=["POST"])
def upload_bills():
    # CSV with columns: email or user_id, month, year, units, amount
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else ''
    if ext != 'csv':
        return jsonify({"error": "Invalid file type, must be CSV"}), 400

    filename = secure_filename(file.filename)
    path = os.path.join(UPLOAD_FOLDER, f"csv_{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}")
    file.save(path)

    inserted = 0
    errors = []
    conn = get_db()
    cur = conn.cursor()
    with open(path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for idx, row in enumerate(reader, start=1):
            try:
                # Normalize keys to lower case
                lookup = { (k.strip().lower() if isinstance(k,str) else k): (v.strip() if isinstance(v, str) else v) for k, v in row.items() }
                user_id = None
                if lookup.get("email"):
                    email = (lookup.get("email") or "").strip().lower()
                    if email:
                        u = cur.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
                        if u:
                            user_id = u["id"]
                if not user_id and lookup.get("user_id"):
                    user_id = safe_int(lookup.get("user_id"))

                if not user_id:
                    errors.append(f"Row {idx}: user not found (email/user_id missing or not registered)")
                    continue

                month = (lookup.get("month") or "").strip()
                if not month:
                    errors.append(f"Row {idx}: missing month")
                    continue

                year = safe_int(lookup.get("year"))
                units = safe_int(lookup.get("units"))
                amount = safe_float(lookup.get("amount"))

                exist = cur.execute("SELECT id FROM bills WHERE user_id=? AND LOWER(month)=LOWER(?) AND year=?", (user_id, month.strip(), year)).fetchone()
                if exist:
                    errors.append(f"Row {idx}: duplicate for {month} {year}")
                    continue

                cur.execute("INSERT INTO bills (user_id,month,year,units,amount,status) VALUES (?,?,?,?,?)",
                            (user_id, month, year, units, amount, 'Unpaid'))

                bill_id = cur.lastrowid
                reminder_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                cur.execute("INSERT INTO reminders (user_id,bill_id,reminder_date) VALUES (?,?,?)", (user_id, bill_id, reminder_date))
                inserted += 1
            except Exception as e:
                errors.append(f"Row {idx}: {str(e)}")
    conn.commit()
    conn.close()
    return jsonify({"inserted": inserted, "errors": errors})

@app.route("/get_bills/<int:user_id>", methods=["GET"])
def get_bills(user_id):
    conn = get_db()
    month = request.args.get("month")
    year = request.args.get("year")
    status = request.args.get("status")
    where_clauses = ["user_id=?"]
    params = [user_id]
    if month:
        if month.strip() != "":
            where_clauses.append("LOWER(month)=LOWER(?)")
            params.append(month.strip())
    if year:
        try:
            y = int(year)
            where_clauses.append("year=?")
            params.append(y)
        except Exception:
            pass
    if status:
        if status.strip().lower() in ("paid", "unpaid"):
            where_clauses.append("status=?")
            params.append(status.strip())
    q = f"SELECT id,month,year,units,amount,receipt_path,status,created_at FROM bills WHERE {' AND '.join(where_clauses)} ORDER BY year DESC, id DESC"
    bills = conn.execute(q, tuple(params)).fetchall()
    conn.close()
    out = []
    for b in bills:
        out.append({"id": b["id"], "month": b["month"], "year": b["year"], "units": int(b["units"] or 0), "amount": float(b["amount"] or 0.0), "receipt_path": b["receipt_path"] or "", "status": b["status"], "created_at": b["created_at"]})
    return jsonify(out)

@app.route("/mark_paid/<int:bill_id>", methods=["POST"])
def mark_paid(bill_id):
    conn = get_db()
    conn.execute("UPDATE bills SET status='Paid' WHERE id=?", (bill_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "Marked as Paid"})

@app.route("/download_bill/<int:bill_id>", methods=["GET"])
def download_bill(bill_id):
    conn = get_db()
    bill = conn.execute("SELECT users.name,users.email, bills.month,bills.year, bills.units,bills.amount,bills.status FROM bills JOIN users ON bills.user_id=users.id WHERE bills.id=?", (bill_id,)).fetchone()
    conn.close()
    if not bill:
        return jsonify({"error": "Bill not found"}), 404
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial","B",16)
    pdf.cell(0,10,"SMART WATER BILL",ln=True,align="C")
    pdf.ln(10)
    pdf.set_font("Arial","",12)
    pdf.cell(0,8,f"Name: {bill['name']}",ln=True)
    pdf.cell(0,8,f"Email: {bill['email']}",ln=True)
    pdf.cell(0,8,f"Month: {bill['month']} {bill['year']}",ln=True)
    pdf.cell(0,8,f"Units: {bill['units']}",ln=True)
    pdf.cell(0,8,f"Amount: Rs {bill['amount']}",ln=True)
    pdf.cell(0,8,f"Status: {bill['status']}",ln=True)
    filename = f"{PDF_FOLDER}/bill_{bill_id}.pdf"
    pdf.output(filename)
    return send_file(filename, as_attachment=True)

@app.route("/receipt/<path:filename>", methods=["GET"])
def serve_receipt(filename):
    path = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=False)

@app.route("/get_reminders/<int:user_id>", methods=["GET"])
def get_reminders(user_id):
    conn = get_db()
    reminders = conn.execute("SELECT r.id, r.reminder_date, b.month, b.year, b.amount, b.status FROM reminders r JOIN bills b ON r.bill_id=b.id WHERE r.user_id=? ORDER BY r.reminder_date ASC", (user_id,)).fetchall()
    conn.close()
    results = []
    for r in reminders:
        results.append({"id": r["id"], "month": r["month"], "year": r["year"], "amount": float(r["amount"] or 0.0), "status": r["status"], "reminder_date": r["reminder_date"]})
    return jsonify(results)

@app.route("/analysis/<int:user_id>", methods=["GET"])
def get_analysis(user_id):
    conn = get_db()
    rows = conn.execute("SELECT units,amount,month,year,status FROM bills WHERE user_id=? ORDER BY year, rowid", (user_id,)).fetchall()
    bills = [dict(r) for r in rows]
    total_units = sum(int(r.get("units",0) or 0) for r in bills) if bills else 0
    total_amount = sum(float(r.get("amount",0.0) or 0.0) for r in bills) if bills else 0.0
    paid_count = sum(1 for r in bills if (r.get("status") or "").lower() == "paid")
    unpaid_count = sum(1 for r in bills if (r.get("status") or "").lower() != "paid")
    timeseries = []
    for r in bills:
        timeseries.append({"month": r.get("month"), "year": r.get("year"), "units": int(r.get("units",0) or 0), "amount": float(r.get("amount",0.0) or 0.0)})
    conn.close()
    return jsonify({"total_units": total_units, "total_amount": total_amount, "paid_count": paid_count, "unpaid_count": unpaid_count, "timeseries": timeseries})

@app.route("/anomalies/<int:user_id>", methods=["GET"])
def get_anomalies(user_id):
    conn = get_db()
    rows = conn.execute("SELECT id, month,year,units,amount,created_at FROM bills WHERE user_id=? ORDER BY year, rowid", (user_id,)).fetchall()
    bills = [dict(r) for r in rows]
    conn.close()
    if not bills:
        return jsonify({"anomalies": [], "mean": 0, "std": 0})
    units_list = [int(b.get("units",0) or 0) for b in bills]
    mean = sum(units_list)/len(units_list)
    variance = sum((u-mean)**2 for u in units_list)/len(units_list)
    std = math.sqrt(variance)
    threshold = mean + 2*std
    anomalies = []
    for b in bills:
        if int(b.get("units",0) or 0) > threshold:
            anomalies.append({**b, "mean": mean, "std": std})
    return jsonify({"anomalies": anomalies, "mean": mean, "std": std})

@app.route("/predict/<int:user_id>", methods=["GET"])
def predict_bill(user_id):
    conn = get_db()
    rows = conn.execute("SELECT id,month,year,units,amount FROM bills WHERE user_id=? ORDER BY year, rowid", (user_id,)).fetchall()
    bills = [dict(r) for r in rows]
    conn.close()
    if not bills or len(bills) < 2:
        return jsonify({"error": "Not enough data to predict", "required": 2}), 400
    xs = list(range(len(bills)))
    ys = [int(b.get("units",0) or 0) for b in bills]
    n = len(xs)
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_x2 = sum(x*x for x in xs)
    sum_xy = sum(x*y for x,y in zip(xs,ys))
    denom = (n*sum_x2 - sum_x*sum_x)
    slope = 0.0
    if denom != 0:
        slope = (n*sum_xy - sum_x*sum_y)/denom
    intercept = (sum_y - slope*sum_x)/n
    next_x = n
    predicted_units = max(0, round(intercept + slope*next_x))
    total_units = sum(ys)
    avg_price_per_unit = (sum(float(b.get("amount",0.0) or 0.0) for b in bills)/total_units) if total_units > 0 else 0.0
    predicted_amount = round(predicted_units * avg_price_per_unit, 2)
    return jsonify({"predicted_units": int(predicted_units), "predicted_amount": float(predicted_amount), "slope": slope, "intercept": intercept})

@app.route("/admin/users", methods=["GET"])
def admin_users():
    conn = get_db()
    users = conn.execute("SELECT id,name,email,is_admin FROM users ORDER BY id").fetchall()
    conn.close()
    return jsonify([dict(u) for u in users])

@app.route("/admin/delete_user/<int:user_id>", methods=["POST"])
def admin_delete_user(user_id):
    conn = get_db()
    conn.execute("DELETE FROM bills WHERE user_id=?", (user_id,))
    conn.execute("DELETE FROM reminders WHERE user_id=?", (user_id,))
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "Deleted user and associated data"})

@app.route("/admin/set_admin/<int:user_id>", methods=["POST"])
def admin_set_admin(user_id):
    data = request.get_json() or {}
    make_admin = bool(data.get("is_admin", False))
    conn = get_db()
    conn.execute("UPDATE users SET is_admin=? WHERE id=?", (1 if make_admin else 0, user_id))
    conn.commit()
    conn.close()
    return jsonify({"message": "Updated admin status", "is_admin": make_admin})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)