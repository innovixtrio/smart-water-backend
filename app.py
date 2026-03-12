# app.py
from flask import Flask, request, jsonify, send_file, g
from flask_cors import CORS
import sqlite3
from fpdf import FPDF
from datetime import datetime, timedelta
import os
from werkzeug.utils import secure_filename
import traceback
import threading

app = Flask(__name__)
CORS(app)

DATABASE = "database.db"
PDF_FOLDER = "pdfs"
UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")

ALLOWED_EXT = {"csv", "pdf", "png", "jpg", "jpeg"}

os.makedirs(PDF_FOLDER, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global lock to serialize writes to SQLite (reduces "database is locked")
db_lock = threading.Lock()

# ================= DB =================
def _connect_db():
    conn = sqlite3.connect(DATABASE, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
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
        receipt_path TEXT,
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

# ================= Helpers =================
def allowed_file(filename):
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_EXT

def safe_int(val, default=0):
    try:
        return int(val)
    except Exception:
        return default

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

# JSON error handlers (prevent HTML error pages)
@app.errorhandler(400)
def bad_request(e):
    message = getattr(e, 'description', 'Bad Request')
    return jsonify({"error": str(message)}), 400

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(415)
def unsupported_media_type(e):
    message = getattr(e, 'description', 'Unsupported Media Type')
    return jsonify({"error": str(message)}), 415

# ================= HOME =================
@app.route("/")
def home():
    return jsonify({"message": "Smart Water Backend Running"})

# ================= REGISTER =================
@app.route("/register", methods=["POST"])
def register():
    try:
        data = request.get_json(silent=True) or {}
        name = data.get("name")
        email = data.get("email")
        password = data.get("password")

        if not name or not email or not password:
            return jsonify({"error": "Missing fields"}), 400

        with db_lock:
            conn = get_db()
            try:
                conn.execute("INSERT INTO users(name,email,password) VALUES(?,?,?)", (name, email, password))
                conn.commit()
            except sqlite3.IntegrityError:
                return jsonify({"error": "Email already exists"}), 400

        return jsonify({"message": "User Registered"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= LOGIN =================
@app.route("/login", methods=["POST"])
def login():
    try:
        data = request.get_json(silent=True) or {}
        email = data.get("email")
        password = data.get("password")

        if not email or not password:
            return jsonify({"error": "Missing fields"}), 400

        conn = get_db()
        user = conn.execute("SELECT * FROM users WHERE email=? AND password=?", (email, password)).fetchone()

        if user:
            return jsonify({"message": "Login Success", "user_id": user["id"], "name": user["name"], "is_admin": user["is_admin"]})

        return jsonify({"error": "Invalid credentials"}), 401
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= ADD BILL =================
@app.route("/add_bill", methods=["POST"])
def add_bill():
    try:
        user_id = 0
        month = None
        year = 0
        units = 0
        amount = 0.0
        receipt_filename = None

        content_type = request.content_type or ""
        print(f"ADD_BILL content_type: {content_type}")

        if request.mimetype and request.mimetype.startswith('multipart'):
            form = request.form
            user_id = safe_int(form.get("user_id"), 0)
            month = form.get("month")
            year = safe_int(form.get("year"), 0)
            units = safe_int(form.get("units"), 0)
            amount = safe_float(form.get("amount"), 0.0)

            if "receipt" in request.files:
                f = request.files["receipt"]
                if f and f.filename:
                    if not allowed_file(f.filename):
                        return jsonify({"error": "Invalid file type"}), 400
                    ts = datetime.now().strftime("%Y%m%d%H%M%S")
                    filename = f"{ts}_{secure_filename(f.filename)}"
                    path = os.path.join(UPLOAD_FOLDER, filename)
                    f.save(path)
                    receipt_filename = filename
        else:
            data = request.get_json(silent=True) or {}
            user_id = safe_int(data.get("user_id"), 0)
            month = data.get("month")
            year = safe_int(data.get("year"), 0)
            units = safe_int(data.get("units"), 0)
            amount = safe_float(data.get("amount"), 0.0)
            receipt_filename = None

        if not user_id or not month or not year:
            return jsonify({"error": "Missing fields"}), 400

        with db_lock:
            conn = get_db()
            cursor = conn.cursor()
            try:
                cursor.execute("INSERT INTO bills(user_id,month,year,units,amount,receipt_path,status) VALUES(?,?,?,?,?,?,?)",
                               (user_id, month, year, units, amount, receipt_filename, 'Unpaid'))
                bill_id = cursor.lastrowid
                reminder_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                cursor.execute("INSERT INTO reminders(user_id,bill_id,reminder_date) VALUES(?,?,?)", (user_id, bill_id, reminder_date))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return jsonify({"message": "Bill Added Successfully", "bill_id": bill_id})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= GET BILLS =================
@app.route("/get_bills/<int:user_id>")
def get_bills(user_id):
    try:
        month_q = request.args.get("month", None)
        year_q = request.args.get("year", None)
        status_q = request.args.get("status", None)

        conn = get_db()
        sql = "SELECT * FROM bills WHERE user_id=?"
        params = [user_id]

        if month_q:
            sql += " AND month=?"
            params.append(month_q)
        if year_q:
            sql += " AND year=?"
            params.append(int(year_q))
        if status_q and status_q != "All":
            sql += " AND status=?"
            params.append(status_q)

        sql += " ORDER BY year DESC, created_at DESC"

        bills = conn.execute(sql, tuple(params)).fetchall()
        result = [dict(b) for b in bills]
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= DELETE BILL =================
@app.route("/delete_bill/<int:bill_id>", methods=["POST"])
def delete_bill(bill_id):
    try:
        with db_lock:
            conn = get_db()
            row = conn.execute("SELECT receipt_path FROM bills WHERE id=?", (bill_id,)).fetchone()
            if row and row["receipt_path"]:
                try:
                    os.remove(os.path.join(UPLOAD_FOLDER, row["receipt_path"]))
                except Exception:
                    pass
            conn.execute("DELETE FROM bills WHERE id=?", (bill_id,))
            conn.execute("DELETE FROM reminders WHERE bill_id=?", (bill_id,))
            conn.commit()
        return jsonify({"message": "Bill deleted"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= MARK PAID =================
@app.route("/mark_paid/<int:bill_id>", methods=["POST"])
def mark_paid(bill_id):
    try:
        with db_lock:
            conn = get_db()
            conn.execute("UPDATE bills SET status='Paid' WHERE id=?", (bill_id,))
            conn.commit()
        return jsonify({"message": "Marked Paid"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= REMINDERS =================
@app.route("/get_reminders/<int:user_id>")
def reminders(user_id):
    try:
        conn = get_db()
        rows = conn.execute("""
        SELECT r.id,r.reminder_date,b.month,b.year,b.amount,b.status
        FROM reminders r
        JOIN bills b ON r.bill_id=b.id
        WHERE r.user_id=?
        """, (user_id,)).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= ANALYSIS =================
@app.route("/analysis/<int:user_id>")
def analysis(user_id):
    try:
        conn = get_db()
        rows = conn.execute("SELECT units,amount,status,month,year FROM bills WHERE user_id=?", (user_id,)).fetchall()

        total_units = sum(r["units"] for r in rows)
        total_amount = sum(r["amount"] for r in rows)

        paid = sum(1 for r in rows if r["status"] == "Paid")
        unpaid = sum(1 for r in rows if r["status"] != "Paid")

        # Build simple timeseries sorted by year,month (for UI plotting)
        # We aggregate by "year-month" label to feed front-end
        times = {}
        for r in rows:
            key = f"{r['month']} {r['year']}"
            times.setdefault(key, {"units": 0, "amount": 0.0})
            times[key]["units"] += r["units"]
            times[key]["amount"] += r["amount"]
        # convert to list ordered by insertion (not guaranteed) - sort by year then month name if needed
        # For simplicity, we keep DB order by created_at (not available here) — convert to list:
        timeseries = []
        for k, v in times.items():
            timeseries.append({"label": k, "units": v["units"], "amount": v["amount"]})

        return jsonify({
            "total_units": total_units,
            "total_amount": total_amount,
            "paid_count": paid,
            "unpaid_count": unpaid,
            "timeseries": timeseries
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= PREDICTION =================
@app.route("/predict/<int:user_id>")
def predict(user_id):
    try:
        conn = get_db()
        rows = conn.execute("SELECT units,amount FROM bills WHERE user_id=?", (user_id,)).fetchall()

        if len(rows) < 2:
            return jsonify({"error": "Not enough data"}), 400

        units = [r["units"] for r in rows]
        avg = sum(units) / len(units) if len(units) > 0 else 0
        predicted_units = round(avg)
        total_units_sum = sum(units)
        if total_units_sum == 0:
            predicted_amount = 0.0
        else:
            avg_price = sum(r["amount"] for r in rows) / total_units_sum
            predicted_amount = predicted_units * avg_price

        return jsonify({
            "predicted_units": predicted_units,
            "predicted_amount": predicted_amount
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ================= PDF =================
@app.route("/download_bill/<int:bill_id>")
def download_bill(bill_id):
    try:
        conn = get_db()
        bill = conn.execute("""
        SELECT users.name,users.email,bills.month,bills.year,bills.units,bills.amount
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
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)