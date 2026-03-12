from flask import Flask, request, jsonify, send_file, g
from flask_cors import CORS
import sqlite3
from fpdf import FPDF
from datetime import datetime, timedelta
import os
import traceback
import threading
import statistics

app = Flask(__name__)
CORS(app)

DATABASE = "database.db"
PDF_FOLDER = "pdfs"

os.makedirs(PDF_FOLDER, exist_ok=True)

db_lock = threading.Lock()

# ================= DB =================
def _connect_db():
    conn = sqlite3.connect(DATABASE, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except:
        pass
    return conn


def get_db():
    if "db" not in g:
        g.db = _connect_db()
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    conn = _connect_db()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT UNIQUE,
        password TEXT,
        is_admin INTEGER DEFAULT 0
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS bills(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        month TEXT,
        year INTEGER,
        units INTEGER,
        amount REAL,
        status TEXT DEFAULT 'Unpaid',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

# ================= HOME =================
@app.route("/")
def home():
    return jsonify({"message": "Smart Water Backend Running"})


# ================= REGISTER =================
@app.route("/register", methods=["POST"])
def register():
    try:
        data = request.get_json()

        name = data.get("name")
        email = data.get("email")
        password = data.get("password")

        if not name or not email or not password:
            return jsonify({"error": "Missing fields"}), 400

        with db_lock:
            conn = get_db()

            conn.execute(
                "INSERT INTO users(name,email,password) VALUES(?,?,?)",
                (name, email, password)
            )

            conn.commit()

        return jsonify({"message": "User Registered"})

    except sqlite3.IntegrityError:
        return jsonify({"error": "Email already exists"}), 400

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ================= LOGIN =================
@app.route("/login", methods=["POST"])
def login():

    data = request.get_json()

    email = data.get("email")
    password = data.get("password")

    conn = get_db()

    user = conn.execute(
        "SELECT * FROM users WHERE email=? AND password=?",
        (email, password)
    ).fetchone()

    if user:
        return jsonify({
            "message": "Login Success",
            "user_id": user["id"],
            "name": user["name"],
            "is_admin": user["is_admin"]
        })

    return jsonify({"error": "Invalid credentials"}), 401


# ================= ADD BILL =================
@app.route("/add_bill", methods=["POST"])
def add_bill():

    data = request.get_json()

    user_id = data.get("user_id")
    month = data.get("month")
    year = data.get("year")
    units = data.get("units")
    amount = data.get("amount")

    with db_lock:
        conn = get_db()

        cursor = conn.execute(
            "INSERT INTO bills(user_id,month,year,units,amount,status) VALUES(?,?,?,?,?,?)",
            (user_id, month, year, units, amount, "Unpaid")
        )

        bill_id = cursor.lastrowid

        reminder_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

        conn.execute(
            "INSERT INTO reminders(user_id,bill_id,reminder_date) VALUES(?,?,?)",
            (user_id, bill_id, reminder_date)
        )

        conn.commit()

    return jsonify({"message": "Bill Added Successfully"})


# ================= GET BILLS =================
@app.route("/get_bills/<int:user_id>")
def get_bills(user_id):

    conn = get_db()

    bills = conn.execute(
        "SELECT * FROM bills WHERE user_id=? ORDER BY created_at DESC",
        (user_id,)
    ).fetchall()

    return jsonify([dict(b) for b in bills])


# ================= MARK PAID =================
@app.route("/mark_paid/<int:bill_id>", methods=["POST"])
def mark_paid(bill_id):

    with db_lock:
        conn = get_db()

        conn.execute(
            "UPDATE bills SET status='Paid' WHERE id=?",
            (bill_id,)
        )

        conn.commit()

    return jsonify({"message": "Marked Paid"})


# ================= REMINDERS =================
@app.route("/get_reminders/<int:user_id>")
def reminders(user_id):

    conn = get_db()

    rows = conn.execute("""
        SELECT r.id,r.reminder_date,b.month,b.year,b.amount,b.status
        FROM reminders r
        JOIN bills b ON r.bill_id=b.id
        WHERE r.user_id=?
    """, (user_id,)).fetchall()

    return jsonify([dict(r) for r in rows])


# ================= ANALYSIS =================
@app.route("/analysis/<int:user_id>")
def analysis(user_id):

    conn = get_db()

    rows = conn.execute(
        "SELECT units,amount,status FROM bills WHERE user_id=?",
        (user_id,)
    ).fetchall()

    total_units = sum(r["units"] for r in rows)
    total_amount = sum(r["amount"] for r in rows)

    paid = sum(1 for r in rows if r["status"] == "Paid")
    unpaid = sum(1 for r in rows if r["status"] != "Paid")

    return jsonify({
        "total_units": total_units,
        "total_amount": total_amount,
        "paid_count": paid,
        "unpaid_count": unpaid
    })


# ================= ANOMALIES =================
@app.route("/anomalies/<int:user_id>")
def anomalies(user_id):

    conn = get_db()

    rows = conn.execute(
        "SELECT month,year,units,amount FROM bills WHERE user_id=?",
        (user_id,)
    ).fetchall()

    rows = [dict(r) for r in rows]

    if len(rows) < 3:
        return jsonify({"anomalies": [], "mean": 0, "std": 0})

    units = [r["units"] for r in rows]

    mean = statistics.mean(units)
    std = statistics.stdev(units)

    anomalies = []

    for r in rows:
        if abs(r["units"] - mean) > 1.5 * std:   # FIXED THRESHOLD
            anomalies.append(r)

    return jsonify({
        "anomalies": anomalies,
        "mean": mean,
        "std": std
    })


# ================= PREDICTION =================
@app.route("/predict/<int:user_id>")
def predict(user_id):

    conn = get_db()

    rows = conn.execute(
        "SELECT units,amount FROM bills WHERE user_id=?",
        (user_id,)
    ).fetchall()

    if len(rows) < 2:
        return jsonify({"error": "Not enough data"}), 400

    units = [r["units"] for r in rows]
    amounts = [r["amount"] for r in rows]

    predicted_units = round(sum(units) / len(units))

    avg_amount = sum(amounts) / len(amounts)

    predicted_amount = round(avg_amount, 2)

    return jsonify({
        "predicted_units": predicted_units,
        "predicted_amount": predicted_amount
    })


# ================= PDF =================
@app.route("/download_bill/<int:bill_id>")
def download_bill(bill_id):

    conn = get_db()

    bill = conn.execute("""
        SELECT users.name,bills.month,bills.year,bills.units,bills.amount
        FROM bills
        JOIN users ON users.id=bills.user_id
        WHERE bills.id=?
    """, (bill_id,)).fetchone()

    if not bill:
        return jsonify({"error": "Bill not found"}), 404

    pdf = FPDF()

    pdf.add_page()

    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "SMART WATER BILL", ln=True)

    pdf.set_font("Arial", "", 12)
    pdf.cell(0, 10, f"Name: {bill['name']}", ln=True)
    pdf.cell(0, 10, f"Month: {bill['month']} {bill['year']}", ln=True)
    pdf.cell(0, 10, f"Units: {bill['units']}", ln=True)
    pdf.cell(0, 10, f"Amount: ₹{bill['amount']}", ln=True)

    path = f"{PDF_FOLDER}/bill_{bill_id}.pdf"

    pdf.output(path)

    return send_file(path, as_attachment=True)


if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(host="0.0.0.0", port=port)