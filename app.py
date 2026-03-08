# app.py
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import sqlite3
from fpdf import FPDF
from datetime import datetime, timedelta
import os
import csv
from werkzeug.utils import secure_filename
import math

app = Flask(__name__)
CORS(app)

DATABASE = "database.db"
PDF_FOLDER = "pdfs"
UPLOAD_FOLDER = "uploads"
ALLOWED_EXT = {"csv", "pdf"}

if not os.path.exists(PDF_FOLDER):
    os.makedirs(PDF_FOLDER)
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# ========== DB helpers ==========
def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    # users: added is_admin flag
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
            units INTEGER NOT NULL,
            amount REAL NOT NULL,
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
            reminder_date TEXT
        )
    """)

    conn.commit()
    conn.close()

init_db()

# ========== Utilities ==========
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT

def safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

# ========== Root ==========
@app.route("/")
def home():
    return "Smart Water Backend Running"

# ========== Auth / Users ==========
@app.route("/register", methods=["POST"])
def register():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data received"}), 400

    name = data.get("name")
    email = data.get("email")
    password = data.get("password")
    if not (name and email and password):
        return jsonify({"error": "Missing fields"}), 400

    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO users (name,email,password) VALUES (?,?,?)",
            (name, email, password)
        )
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
    email = data.get("email")
    password = data.get("password")
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE email=? AND password=?",
        (email, password)
    ).fetchone()
    conn.close()
    if user:
        return jsonify({
            "message": "Login Success",
            "user_id": user["id"],
            "name": user["name"],
            "is_admin": bool(user["is_admin"])
        })
    return jsonify({"error": "Invalid Credentials"}), 401

# ========== Add single bill ==========
@app.route("/add_bill", methods=["POST"])
def add_bill():
    data = request.get_json()
    try:
        user_id = int(data.get("user_id"))
        month = str(data.get("month")).strip()
        year = int(data.get("year"))
        units = int(data.get("units"))
        amount = float(data.get("amount"))
    except Exception:
        return jsonify({"error": "Invalid input"}), 400

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO bills (user_id,month,year,units,amount,status)
        VALUES (?,?,?,?,?,'Unpaid')
    """, (user_id, month, year, units, amount))

    bill_id = cursor.lastrowid
    reminder_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    cursor.execute("""
        INSERT INTO reminders (user_id,bill_id,reminder_date)
        VALUES (?,?,?)
    """, (user_id, bill_id, reminder_date))

    conn.commit()
    conn.close()
    return jsonify({"message": "Bill Added Successfully"})

# ========== Bulk CSV upload ==========
# CSV expected columns: email or user_id,month,year,units,amount
@app.route("/upload_bills", methods=["POST"])
def upload_bills():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(path)
        inserted = 0
        errors = []
        conn = get_db()
        cur = conn.cursor()
        with open(path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for idx, row in enumerate(reader, start=1):
                try:
                    # normalize keys and strip values
                    lookup = {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in (row.items())}
                    user_id = None
                    if lookup.get("email"):
                        u = cur.execute("SELECT id FROM users WHERE email=?", (lookup["email"],)).fetchone()
                        if u:
                            user_id = u["id"]
                    elif lookup.get("user_id"):
                        user_id = safe_int(lookup.get("user_id"))
                    if not user_id:
                        errors.append(f"Row {idx}: user not found ({row})")
                        continue
                    month = lookup.get("month", "")
                    year = safe_int(lookup.get("year"))
                    units = safe_int(lookup.get("units"))
                    amount = safe_float(lookup.get("amount"))
                    cur.execute("""
                        INSERT INTO bills (user_id,month,year,units,amount,status)
                        VALUES (?,?,?,?,?,'Unpaid')
                    """, (user_id, month, year, units, amount))
                    bill_id = cur.lastrowid
                    reminder_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                    cur.execute("INSERT INTO reminders (user_id,bill_id,reminder_date) VALUES (?,?,?)",
                                (user_id, bill_id, reminder_date))
                    inserted += 1
                except Exception as e:
                    errors.append(f"Row {idx}: {str(e)}")
        conn.commit()
        conn.close()
        return jsonify({"inserted": inserted, "errors": errors})
    return jsonify({"error": "Invalid file type"}), 400

# ========== Get bills (supports filters via query params) ==========
@app.route("/get_bills/<int:user_id>", methods=["GET"])
def get_bills(user_id):
    conn = get_db()
    month = request.args.get("month")
    year = request.args.get("year")
    status = request.args.get("status")

    where_clauses = ["user_id=?"]
    params = [user_id]

    if month:
        where_clauses.append("LOWER(month)=LOWER(?)")
        params.append(month.strip())
    if year:
        where_clauses.append("year=?")
        params.append(safe_int(year))
    if status:
        where_clauses.append("status=?")
        params.append(status.strip())

    q = f"""
        SELECT id,month,year,units,amount,status,created_at
        FROM bills
        WHERE {' AND '.join(where_clauses)}
        ORDER BY year DESC, id DESC
    """
    bills = conn.execute(q, tuple(params)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in bills])

# ========== Mark paid ==========
@app.route("/mark_paid/<int:bill_id>", methods=["POST"])
def mark_paid(bill_id):
    conn = get_db()
    conn.execute("UPDATE bills SET status='Paid' WHERE id=?", (bill_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "Marked as Paid"})

# ========== Download bill PDF ==========
@app.route("/download_bill/<int:bill_id>", methods=["GET"])
def download_bill(bill_id):
    conn = get_db()
    bill = conn.execute("""
        SELECT users.name,users.email,
               bills.month,bills.year,
               bills.units,bills.amount,bills.status
        FROM bills
        JOIN users ON bills.user_id=users.id
        WHERE bills.id=?
    """, (bill_id,)).fetchone()
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

# ========== Reminders ==========
@app.route("/get_reminders/<int:user_id>", methods=["GET"])
def get_reminders(user_id):
    conn = get_db()
    reminders = conn.execute("""
        SELECT r.id, r.reminder_date, b.month, b.year, b.amount, b.status
        FROM reminders r
        JOIN bills b ON r.bill_id=b.id
        WHERE r.user_id=?
        ORDER BY r.reminder_date ASC
    """, (user_id,)).fetchall()
    conn.close()
    # convert sqlite rows to dict with friendly keys
    results = []
    for r in reminders:
        results.append({
            "id": r["id"],
            "month": r["month"],
            "year": r["year"],
            "amount": r["amount"],
            "status": r["status"],
            "reminder_date": r["reminder_date"]
        })
    return jsonify(results)

# ========== Analysis ==========
@app.route("/analysis/<int:user_id>", methods=["GET"])
def get_analysis(user_id):
    conn = get_db()
    rows = conn.execute("SELECT units,amount,month,year,status FROM bills WHERE user_id=? ORDER BY year, rowid", (user_id,)).fetchall()
    bills = [dict(r) for r in rows]
    total_units = sum(r["units"] for r in bills) if bills else 0
    total_amount = sum(r["amount"] for r in bills) if bills else 0
    paid_count = sum(1 for r in bills if r.get("status") == "Paid")
    unpaid_count = sum(1 for r in bills if r.get("status") != "Paid")
    # time series for charts (month-year -> units)
    timeseries = []
    for r in bills:
        timeseries.append({"month": r["month"], "year": r["year"], "units": r["units"], "amount": r["amount"]})
    conn.close()
    return jsonify({
        "total_units": total_units,
        "total_amount": total_amount,
        "paid_count": paid_count,
        "unpaid_count": unpaid_count,
        "timeseries": timeseries
    })

# ========== Anomaly detection ==========
@app.route("/anomalies/<int:user_id>", methods=["GET"])
def get_anomalies(user_id):
    conn = get_db()
    rows = conn.execute("SELECT id, month,year,units,amount,created_at FROM bills WHERE user_id=? ORDER BY year, rowid", (user_id,)).fetchall()
    bills = [dict(r) for r in rows]
    conn.close()
    if not bills:
        return jsonify({"anomalies": [], "mean": 0, "std": 0})
    units_list = [b["units"] for b in bills]
    mean = sum(units_list)/len(units_list)
    # compute std
    variance = sum((u-mean)**2 for u in units_list)/len(units_list)
    std = math.sqrt(variance)
    threshold = mean + 2*std  # simple threshold
    anomalies = []
    for b in bills:
        if b["units"] > threshold:
            anomalies.append({**b, "mean": mean, "std": std})
    return jsonify({"anomalies": anomalies, "mean": mean, "std": std})

# ========== Prediction (very simple linear regression on time index) ==========
@app.route("/predict/<int:user_id>", methods=["GET"])
def predict_bill(user_id):
    conn = get_db()
    rows = conn.execute("SELECT id,month,year,units,amount FROM bills WHERE user_id=? ORDER BY year, rowid", (user_id,)).fetchall()
    bills = [dict(r) for r in rows]
    conn.close()
    if not bills or len(bills) < 2:
        return jsonify({"error": "Not enough data to predict", "required": 2}), 400
    # prepare x = 0,1,2... y = units
    xs = list(range(len(bills)))
    ys = [b["units"] for b in bills]
    n = len(xs)
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_x2 = sum(x*x for x in xs)
    sum_xy = sum(x*y for x,y in zip(xs,ys))
    denom = (n*sum_x2 - sum_x*sum_x)
    if denom == 0:
        slope = 0
    else:
        slope = (n*sum_xy - sum_x*sum_y)/denom
    intercept = (sum_y - slope*sum_x)/n
    next_x = n
    predicted_units = max(0, round(intercept + slope*next_x))
    # estimate price per unit as average
    total_units = sum(b["units"] for b in bills)
    avg_price_per_unit = (sum(b["amount"] for b in bills)/total_units) if total_units > 0 else 0
    predicted_amount = round(predicted_units * avg_price_per_unit, 2)
    return jsonify({"predicted_units": predicted_units, "predicted_amount": predicted_amount, "slope": slope, "intercept": intercept})

# ========== Admin endpoints ==========
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
    data = request.get_json()
    make_admin = bool(data.get("is_admin", False))
    conn = get_db()
    conn.execute("UPDATE users SET is_admin=? WHERE id=?", (1 if make_admin else 0, user_id))
    conn.commit()
    conn.close()
    return jsonify({"message": "Updated admin status", "is_admin": make_admin})

# ========== Run ==========
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)