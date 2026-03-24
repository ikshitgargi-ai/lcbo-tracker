import os
import io
import csv
import sqlite3
import json
from datetime import datetime
from flask import Flask, render_template, request, jsonify, g, Response, send_file

app = Flask(__name__)
DB_DIR = os.environ.get('DB_DIR', os.path.dirname(__file__))
DB_PATH = os.path.join(DB_DIR, 'lcbo_tracker.db')

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript('''
        CREATE TABLE IF NOT EXISTS stores (
            id INTEGER PRIMARY KEY,
            store_number INTEGER UNIQUE NOT NULL,
            account TEXT,
            address TEXT,
            city TEXT,
            postal TEXT,
            phone TEXT DEFAULT '',
            email TEXT DEFAULT '',
            contacts TEXT DEFAULT '',
            priority TEXT DEFAULT 'Standard',
            status TEXT DEFAULT '',
            rep TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS reps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id INTEGER NOT NULL,
            rep_id INTEGER NOT NULL,
            activity_type TEXT NOT NULL CHECK(activity_type IN ('tasting','site_visit','listing','email','call')),
            notes TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (store_id) REFERENCES stores(id),
            FOREIGN KEY (rep_id) REFERENCES reps(id)
        );
        CREATE INDEX IF NOT EXISTS idx_activities_store ON activities(store_id);
        CREATE INDEX IF NOT EXISTS idx_activities_rep ON activities(rep_id);
        CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(activity_type);
    ''')
    db.commit()
    db.close()

def seed_data():
    db = sqlite3.connect(DB_PATH)
    count = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    if count > 0:
        db.close()
        return

    import openpyxl
    xlsx_path = os.path.join(os.path.dirname(__file__), 'data', 'All LCBO stores.xlsx')
    if not os.path.exists(xlsx_path):
        xlsx_path = '/Users/ikshitsharma/Downloads/All LCBO stores.xlsx'

    import pandas as pd
    df = pd.read_excel(xlsx_path)

    for _, row in df.iterrows():
        store_num = int(row['License #']) if pd.notna(row.get('License #')) else 0
        account = str(row.get('Account', f'LCBO #{store_num}')) if pd.notna(row.get('Account')) else f'LCBO #{store_num}'
        address = str(row.get('Address', '')) if pd.notna(row.get('Address')) else ''
        city = str(row.get('City', '')) if pd.notna(row.get('City')) else ''
        postal = str(row.get('Postal', '')) if pd.notna(row.get('Postal')) else ''
        contacts = str(row.get('Contacts', '')) if pd.notna(row.get('Contacts')) else ''
        emails = str(row.get('Emails', '')) if pd.notna(row.get('Emails')) else ''
        # Clean up comma-only emails
        if emails and all(c == ',' for c in emails):
            emails = ''
        priority = str(row.get('Priority', 'Standard')) if pd.notna(row.get('Priority')) else 'Standard'
        status = str(row.get('Status', '')) if pd.notna(row.get('Status')) else ''
        rep = str(row.get('Rep', '')) if pd.notna(row.get('Rep')) else ''

        try:
            db.execute(
                "INSERT OR IGNORE INTO stores (store_number, account, address, city, postal, phone, email, contacts, priority, status, rep) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (store_num, account, address, city, postal, '', emails, contacts, priority, status, rep)
            )
        except Exception:
            pass

    for rep_name in ['Ikshit Sharma', 'Namit']:
        db.execute("INSERT OR IGNORE INTO reps (name) VALUES (?)", (rep_name,))

    db.commit()
    db.close()

# --- Routes ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stores')
def api_stores():
    db = get_db()
    search = request.args.get('search', '').strip()
    city = request.args.get('city', '').strip()
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))

    query = "SELECT * FROM stores WHERE 1=1"
    params = []
    if search:
        query += " AND (CAST(store_number AS TEXT) LIKE ? OR account LIKE ? OR address LIKE ? OR city LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if city:
        query += " AND city LIKE ?"
        params.append(f"%{city}%")

    total = db.execute(query.replace("SELECT *", "SELECT COUNT(*)"), params).fetchone()[0]
    query += " ORDER BY store_number ASC LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])
    rows = db.execute(query, params).fetchall()

    return jsonify({
        'stores': [dict(r) for r in rows],
        'total': total,
        'page': page,
        'pages': (total + per_page - 1) // per_page
    })

@app.route('/api/stores/<int:store_id>', methods=['GET'])
def api_store_detail(store_id):
    db = get_db()
    store = db.execute("SELECT * FROM stores WHERE id=?", (store_id,)).fetchone()
    if not store:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(dict(store))

@app.route('/api/stores/<int:store_id>', methods=['PUT'])
def api_store_update(store_id):
    db = get_db()
    data = request.json
    fields = ['account', 'address', 'city', 'postal', 'phone', 'email', 'contacts', 'priority', 'status', 'rep']
    sets = []
    params = []
    for f in fields:
        if f in data:
            sets.append(f"{f}=?")
            params.append(data[f])
    if not sets:
        return jsonify({'error': 'No fields to update'}), 400
    params.append(store_id)
    db.execute(f"UPDATE stores SET {','.join(sets)} WHERE id=?", params)
    db.commit()
    return jsonify({'success': True})

@app.route('/api/reps')
def api_reps():
    db = get_db()
    rows = db.execute("SELECT * FROM reps ORDER BY name").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/activities', methods=['POST'])
def api_activity_create():
    db = get_db()
    data = request.json
    store_id = data.get('store_id')
    rep_id = data.get('rep_id')
    activity_type = data.get('activity_type')
    notes = data.get('notes', '')

    if not all([store_id, rep_id, activity_type]):
        return jsonify({'error': 'Missing required fields'}), 400

    cursor = db.execute(
        "INSERT INTO activities (store_id, rep_id, activity_type, notes) VALUES (?,?,?,?)",
        (store_id, rep_id, activity_type, notes)
    )
    db.commit()
    return jsonify({'id': cursor.lastrowid, 'success': True})

@app.route('/api/activities/<int:store_id>')
def api_activities_for_store(store_id):
    db = get_db()
    activity_type = request.args.get('type', '')
    query = """
        SELECT a.*, r.name as rep_name
        FROM activities a JOIN reps r ON a.rep_id=r.id
        WHERE a.store_id=?
    """
    params = [store_id]
    if activity_type:
        query += " AND a.activity_type=?"
        params.append(activity_type)
    query += " ORDER BY a.created_at DESC"
    rows = db.execute(query, params).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/activities/summary/<int:store_id>')
def api_activity_summary(store_id):
    db = get_db()
    rows = db.execute("""
        SELECT activity_type, COUNT(*) as count,
               MAX(created_at) as last_date
        FROM activities WHERE store_id=?
        GROUP BY activity_type
    """, (store_id,)).fetchall()
    return jsonify({r['activity_type']: {'count': r['count'], 'last_date': r['last_date']} for r in rows})

@app.route('/api/dashboard')
def api_dashboard():
    db = get_db()
    total_stores = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    total_activities = db.execute("SELECT COUNT(*) FROM activities").fetchone()[0]
    by_type = db.execute("SELECT activity_type, COUNT(*) as c FROM activities GROUP BY activity_type").fetchall()
    recent = db.execute("""
        SELECT a.*, s.store_number, s.account, r.name as rep_name
        FROM activities a
        JOIN stores s ON a.store_id=s.id
        JOIN reps r ON a.rep_id=r.id
        ORDER BY a.created_at DESC LIMIT 15
    """).fetchall()
    by_rep = db.execute("""
        SELECT r.name, COUNT(a.id) as count
        FROM reps r LEFT JOIN activities a ON r.id=a.rep_id
        GROUP BY r.id
    """).fetchall()

    return jsonify({
        'total_stores': total_stores,
        'total_activities': total_activities,
        'by_type': {r['activity_type']: r['c'] for r in by_type},
        'recent': [dict(r) for r in recent],
        'by_rep': {r['name']: r['count'] for r in by_rep}
    })

@app.route('/api/cities')
def api_cities():
    db = get_db()
    rows = db.execute("SELECT DISTINCT city FROM stores WHERE city != '' ORDER BY city").fetchall()
    return jsonify([r['city'] for r in rows])

@app.route('/api/export/stores')
def export_stores_csv():
    db = get_db()
    rows = db.execute("SELECT * FROM stores ORDER BY store_number").fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Store #', 'Account', 'Address', 'City', 'Postal', 'Phone', 'Email', 'Contacts', 'Priority', 'Status', 'Rep'])
    for r in rows:
        writer.writerow([r['store_number'], r['account'], r['address'], r['city'], r['postal'], r['phone'], r['email'], r['contacts'], r['priority'], r['status'], r['rep']])
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=lcbo_stores_{datetime.now().strftime("%Y%m%d")}.csv'})

@app.route('/api/export/activities')
def export_activities_csv():
    db = get_db()
    rows = db.execute("""
        SELECT s.store_number, s.account, s.city, r.name as rep_name,
               a.activity_type, a.notes, a.created_at
        FROM activities a
        JOIN stores s ON a.store_id=s.id
        JOIN reps r ON a.rep_id=r.id
        ORDER BY a.created_at DESC
    """).fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Store #', 'Account', 'City', 'Rep', 'Activity Type', 'Notes', 'Date'])
    for r in rows:
        writer.writerow([r['store_number'], r['account'], r['city'], r['rep_name'], r['activity_type'], r['notes'], r['created_at']])
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=lcbo_activities_{datetime.now().strftime("%Y%m%d")}.csv'})

@app.route('/api/export/pipeline')
def export_pipeline_csv():
    db = get_db()
    rows = db.execute("""
        SELECT s.store_number, s.account, s.address, s.city, s.postal,
               s.phone, s.email, s.contacts, s.priority, s.status, s.rep,
               COUNT(a.id) as total_activities,
               SUM(CASE WHEN a.activity_type='tasting' THEN 1 ELSE 0 END) as tastings,
               SUM(CASE WHEN a.activity_type='site_visit' THEN 1 ELSE 0 END) as site_visits,
               SUM(CASE WHEN a.activity_type='listing' THEN 1 ELSE 0 END) as listings,
               SUM(CASE WHEN a.activity_type='email' THEN 1 ELSE 0 END) as emails,
               SUM(CASE WHEN a.activity_type='call' THEN 1 ELSE 0 END) as calls,
               MAX(a.created_at) as last_activity
        FROM stores s
        LEFT JOIN activities a ON s.id=a.store_id
        GROUP BY s.id
        ORDER BY s.store_number
    """).fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Store #', 'Account', 'Address', 'City', 'Postal', 'Phone', 'Email', 'Contacts',
                     'Priority', 'Status', 'Rep', 'Total Activities', 'Tastings', 'Site Visits',
                     'Listings', 'Emails', 'Calls', 'Last Activity'])
    for r in rows:
        writer.writerow([r['store_number'], r['account'], r['address'], r['city'], r['postal'],
                         r['phone'], r['email'], r['contacts'], r['priority'], r['status'], r['rep'],
                         r['total_activities'], r['tastings'], r['site_visits'], r['listings'],
                         r['emails'], r['calls'], r['last_activity']])
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=lcbo_pipeline_{datetime.now().strftime("%Y%m%d")}.csv'})

@app.route('/api/export/backup')
def export_backup():
    """Download the entire SQLite database file"""
    if os.path.exists(DB_PATH):
        return send_file(DB_PATH, as_attachment=True,
                         download_name=f'lcbo_tracker_backup_{datetime.now().strftime("%Y%m%d_%H%M")}.db')
    return jsonify({'error': 'Database not found'}), 404

init_db()
seed_data()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    app.run(debug=os.environ.get('FLASK_DEBUG', 'true').lower() == 'true', host='0.0.0.0', port=port)
