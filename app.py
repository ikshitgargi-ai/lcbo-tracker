import os
import io
import csv
import json
import math
import re
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, g, Response, send_file

# Database imports - PostgreSQL for production, SQLite for local dev
DATABASE_URL = os.environ.get('DATABASE_URL', '')
# Strip channel_binding param — psycopg2-binary doesn't support it on all platforms
if DATABASE_URL and 'channel_binding' in DATABASE_URL:
    import re as _re
    DATABASE_URL = _re.sub(r'[&?]channel_binding=[^&]*', '', DATABASE_URL)
USE_POSTGRES = bool(DATABASE_URL)

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras
else:
    import sqlite3

try:
    import requests as http_requests
except ImportError:
    http_requests = None

from decimal import Decimal as _Decimal
from flask.json.provider import DefaultJSONProvider

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class PgJSONProvider(DefaultJSONProvider):
    """Handle PostgreSQL types (Decimal, datetime) in JSON responses"""
    def default(self, o):
        if isinstance(o, _Decimal):
            return float(o)
        if isinstance(o, datetime):
            return o.isoformat()
        if hasattr(o, 'isoformat'):
            return o.isoformat()
        return super().default(o)


app = Flask(__name__, template_folder=os.path.join(BASE_DIR, 'templates'),
            static_folder=os.path.join(BASE_DIR, 'static'))
app.json_provider_class = PgJSONProvider
app.json = PgJSONProvider(app)

DB_DIR = os.environ.get('DB_DIR', BASE_DIR)
DB_PATH = os.path.join(DB_DIR, 'lcbo_tracker.db')

# Rep home base for route planning
REP_HOME = {'lat': 43.6558, 'lng': -79.3628, 'address': '181 Dundas St E, Toronto, ON'}

# Our tracked products on LCBO.com
TRACKED_PRODUCTS = [
    ('NB Distillers', 'Red Admiral Vodka', '20187', 'https://www.lcbo.com/en/red-admiral-vodka-20187', '', 'Spirits'),
    ('NB Distillers', 'Chak De Canadian Whisky', '22246', 'https://www.lcbo.com/en/chak-de-canadian-whisky-22246', '', 'Spirits'),
    ('Anu Portfolio', 'Goenchi Cashew Feni', '46340', 'https://www.lcbo.com/en/goenchi-cashew-feni-46340', '$93.95', 'Spirits'),
    ('Anu Portfolio', 'Goenchi Coconut Feni', '46343', 'https://www.lcbo.com/en/goenchi-coconut-feni-46343', '$93.95', 'Spirits'),
    ('Anu Portfolio', 'Fratelli Classic Shiraz', '46282', 'https://www.lcbo.com/en/fratelli-classic-shiraz-46282', '', 'Wine'),
    ('Anu Portfolio', 'Fratelli Cabernet Sauvignon', '46287', 'https://www.lcbo.com/en/fratelli-cabernet-sauvignon-46287', '$28.95', 'Wine'),
    ('Anu Portfolio', 'Rutland Square Chai Spiced Gin', '', '', '', 'Spirits'),
]

# Ontario city coordinates for route planning
CITY_COORDS = {
    'Toronto': (43.6532, -79.3832), 'Mississauga': (43.5890, -79.6441),
    'Brampton': (43.7315, -79.7624), 'Hamilton': (43.2557, -79.8711),
    'Ottawa': (45.4215, -75.6972), 'London': (42.9849, -81.2453),
    'Markham': (43.8561, -79.3370), 'Vaughan': (43.8361, -79.4983),
    'Kitchener': (43.4516, -80.4925), 'Windsor': (42.3149, -83.0364),
    'Richmond Hill': (43.8828, -79.4403), 'Oakville': (43.4675, -79.6877),
    'Burlington': (43.3255, -79.7990), 'Sudbury': (46.4917, -80.9930),
    'Oshawa': (43.8971, -78.8658), 'Barrie': (44.3894, -79.6903),
    'St. Catharines': (43.1594, -79.2469), 'Guelph': (43.5448, -80.2482),
    'Cambridge': (43.3616, -80.3144), 'Whitby': (43.8975, -78.9429),
    'Ajax': (43.8509, -79.0204), 'Milton': (43.5183, -79.8774),
    'Niagara Falls': (43.0896, -79.0849), 'Thunder Bay': (48.3809, -89.2477),
    'Waterloo': (43.4643, -80.5204), 'Chatham': (42.4048, -82.1910),
    'Brantford': (43.1394, -80.2644), 'Peterborough': (44.3091, -78.3197),
    'Newmarket': (44.0592, -79.4613), 'Kawartha Lakes': (44.3500, -78.7500),
    'Sault Ste. Marie': (46.5219, -84.3461), 'Sarnia': (42.9745, -82.4066),
    'North Bay': (46.3091, -79.4608), 'Belleville': (44.1628, -77.3832),
    'Welland': (42.9923, -79.2487), 'Cornwall': (45.0181, -74.7291),
    'Stouffville': (43.9701, -79.2441), 'Georgetown': (43.6526, -79.9169),
    'Orangeville': (43.9197, -80.0943), 'Orillia': (44.6082, -79.4197),
    'Stratford': (43.3700, -80.9822), 'Timmins': (48.4758, -81.3305),
    'Bowmanville': (43.9126, -78.6871), 'Cobourg': (43.9594, -78.1677),
    'Port Hope': (43.9510, -78.2919), 'Innisfil': (44.3000, -79.5833),
    'Collingwood': (44.5001, -80.2170), 'Woodstock': (43.1315, -80.7564),
    'Pickering': (43.8354, -79.0890), 'Scarborough': (43.7731, -79.2577),
    'Etobicoke': (43.6205, -79.5132), 'North York': (43.7615, -79.4111),
    'East York': (43.6910, -79.3380), 'York': (43.6960, -79.4510),
    'Thornhill': (43.8156, -79.4240), 'Aurora': (44.0065, -79.4504),
    'Keswick': (44.2260, -79.4688), 'Grimsby': (43.1935, -79.5612),
    'Stoney Creek': (43.2176, -79.7441), 'Ancaster': (43.2184, -79.9870),
    'Dundas': (43.2663, -79.9543), 'Fergus': (43.8695, -80.3749),
    'Alliston': (44.1536, -79.8666), 'Bradford': (44.1145, -79.5611),
    'Midland': (44.7496, -79.8877), 'Penetanguishene': (44.7676, -79.9373),
    'Gravenhurst': (44.9190, -79.3731), 'Bracebridge': (44.9989, -79.3113),
    'Huntsville': (45.3287, -79.2164), 'Parry Sound': (45.3433, -80.1847),
    'Kingston': (44.2312, -76.4860), 'Lindsay': (44.3500, -78.7500),
}


# ======== DATABASE ABSTRACTION ========

def get_db():
    if 'db' not in g:
        if USE_POSTGRES:
            g.db = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
            g.db.autocommit = False
        else:
            g.db = sqlite3.connect(DB_PATH)
            g.db.row_factory = sqlite3.Row
            g.db.execute("PRAGMA journal_mode=WAL")
            g.db.execute("PRAGMA foreign_keys=ON")
    return g.db


def db_execute(query, params=None):
    """Execute a query with automatic placeholder conversion for PostgreSQL"""
    db = get_db()
    if USE_POSTGRES:
        query = query.replace('?', '%s')
        query = query.replace('CURRENT_TIMESTAMP', 'NOW()')
        cur = db.cursor()
        cur.execute(query, params or ())
        return cur
    else:
        return db.execute(query, params or [])


def db_fetchone(query, params=None):
    db = get_db()
    if USE_POSTGRES:
        query = query.replace('?', '%s')
        cur = db.cursor()
        cur.execute(query, params or ())
        row = cur.fetchone()
        cur.close()
        return row
    else:
        return db.execute(query, params or []).fetchone()


def db_fetchall(query, params=None):
    db = get_db()
    if USE_POSTGRES:
        query = query.replace('?', '%s')
        cur = db.cursor()
        cur.execute(query, params or ())
        rows = cur.fetchall()
        cur.close()
        return rows
    else:
        return db.execute(query, params or []).fetchall()


def db_commit():
    db = get_db()
    db.commit()


def row_to_dict(row):
    if row is None:
        return None
    if USE_POSTGRES:
        return dict(row)
    else:
        return dict(row)


@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def init_db():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS stores (
                id SERIAL PRIMARY KEY,
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
                rep TEXT DEFAULT '',
                manager_name TEXT DEFAULT '',
                asst_manager_name TEXT DEFAULT '',
                manager_phone TEXT DEFAULT '',
                store_email TEXT DEFAULT '',
                producer TEXT DEFAULT '',
                lat REAL DEFAULT 0,
                lng REAL DEFAULT 0,
                first_seen TIMESTAMP DEFAULT NULL
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS reps (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS activities (
                id SERIAL PRIMARY KEY,
                store_id INTEGER NOT NULL REFERENCES stores(id),
                rep_id INTEGER NOT NULL REFERENCES reps(id),
                activity_type TEXT NOT NULL,
                producer TEXT DEFAULT '',
                venue_type TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                follow_up_date TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                brand TEXT NOT NULL,
                name TEXT NOT NULL,
                lcbo_sku TEXT DEFAULT '',
                lcbo_url TEXT DEFAULT '',
                price TEXT DEFAULT '',
                category TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS inventory_cache (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id),
                store_number INTEGER,
                store_name TEXT DEFAULT '',
                store_city TEXT DEFAULT '',
                quantity INTEGER DEFAULT 0,
                checked_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS followups (
                id SERIAL PRIMARY KEY,
                store_id INTEGER NOT NULL REFERENCES stores(id),
                rep_id INTEGER NOT NULL REFERENCES reps(id),
                activity_id INTEGER REFERENCES activities(id),
                followup_type TEXT DEFAULT '',
                due_date DATE NOT NULL,
                status TEXT DEFAULT 'pending',
                notes TEXT DEFAULT '',
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # Create indexes
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_activities_store ON activities(store_id)",
            "CREATE INDEX IF NOT EXISTS idx_activities_rep ON activities(rep_id)",
            "CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(activity_type)",
            "CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory_cache(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_stores_city ON stores(city)",
            "CREATE INDEX IF NOT EXISTS idx_followups_store ON followups(store_id)",
            "CREATE INDEX IF NOT EXISTS idx_followups_status ON followups(status)",
            "CREATE INDEX IF NOT EXISTS idx_followups_due ON followups(due_date)",
        ]:
            cur.execute(idx)
        # Add columns if upgrading
        migrate_cols = [
            ('stores', 'manager_name', 'TEXT DEFAULT \'\''), ('stores', 'asst_manager_name', 'TEXT DEFAULT \'\''),
            ('stores', 'manager_phone', 'TEXT DEFAULT \'\''), ('stores', 'store_email', 'TEXT DEFAULT \'\''),
            ('stores', 'producer', 'TEXT DEFAULT \'\''), ('stores', 'lat', 'REAL DEFAULT 0'),
            ('stores', 'lng', 'REAL DEFAULT 0'), ('stores', 'first_seen', 'TIMESTAMP DEFAULT NULL'),
            ('activities', 'producer', 'TEXT DEFAULT \'\''), ('activities', 'venue_type', 'TEXT DEFAULT \'\''),
            ('activities', 'follow_up_date', 'TEXT DEFAULT \'\''),
        ]
        for table, col, coltype in migrate_cols:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            except Exception:
                pass  # Column already exists, safe to ignore with autocommit=True
        cur.close()
        conn.close()
        print("[DB] PostgreSQL tables initialized successfully")
    else:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript('''
            CREATE TABLE IF NOT EXISTS stores (
                id INTEGER PRIMARY KEY,
                store_number INTEGER UNIQUE NOT NULL,
                account TEXT, address TEXT, city TEXT, postal TEXT,
                phone TEXT DEFAULT '', email TEXT DEFAULT '', contacts TEXT DEFAULT '',
                priority TEXT DEFAULT 'Standard', status TEXT DEFAULT '', rep TEXT DEFAULT '',
                manager_name TEXT DEFAULT '', asst_manager_name TEXT DEFAULT '',
                manager_phone TEXT DEFAULT '', store_email TEXT DEFAULT '',
                producer TEXT DEFAULT '', lat REAL DEFAULT 0, lng REAL DEFAULT 0,
                first_seen TIMESTAMP DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS reps (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER NOT NULL, rep_id INTEGER NOT NULL,
                activity_type TEXT NOT NULL, producer TEXT DEFAULT '',
                venue_type TEXT DEFAULT '', notes TEXT DEFAULT '',
                follow_up_date TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (store_id) REFERENCES stores(id),
                FOREIGN KEY (rep_id) REFERENCES reps(id)
            );
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand TEXT NOT NULL, name TEXT NOT NULL,
                lcbo_sku TEXT DEFAULT '', lcbo_url TEXT DEFAULT '',
                price TEXT DEFAULT '', category TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS inventory_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL, store_number INTEGER,
                store_name TEXT DEFAULT '', store_city TEXT DEFAULT '',
                quantity INTEGER DEFAULT 0, checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            );
            CREATE INDEX IF NOT EXISTS idx_activities_store ON activities(store_id);
            CREATE INDEX IF NOT EXISTS idx_activities_rep ON activities(rep_id);
            CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(activity_type);
            CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(created_at);
            CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory_cache(product_id);
            CREATE INDEX IF NOT EXISTS idx_stores_city ON stores(city);
            CREATE TABLE IF NOT EXISTS followups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER NOT NULL,
                rep_id INTEGER NOT NULL,
                activity_id INTEGER,
                followup_type TEXT DEFAULT '',
                due_date TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                notes TEXT DEFAULT '',
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (store_id) REFERENCES stores(id),
                FOREIGN KEY (rep_id) REFERENCES reps(id),
                FOREIGN KEY (activity_id) REFERENCES activities(id)
            );
            CREATE INDEX IF NOT EXISTS idx_followups_store ON followups(store_id);
            CREATE INDEX IF NOT EXISTS idx_followups_status ON followups(status);
            CREATE INDEX IF NOT EXISTS idx_followups_due ON followups(due_date);
        ''')
        migrate_cols = [
            ('stores', 'manager_name', "TEXT DEFAULT ''"), ('stores', 'asst_manager_name', "TEXT DEFAULT ''"),
            ('stores', 'manager_phone', "TEXT DEFAULT ''"), ('stores', 'store_email', "TEXT DEFAULT ''"),
            ('stores', 'producer', "TEXT DEFAULT ''"), ('stores', 'lat', "REAL DEFAULT 0"),
            ('stores', 'lng', "REAL DEFAULT 0"), ('stores', 'first_seen', "TIMESTAMP DEFAULT NULL"),
            ('activities', 'producer', "TEXT DEFAULT ''"), ('activities', 'venue_type', "TEXT DEFAULT ''"),
            ('activities', 'follow_up_date', "TEXT DEFAULT ''"),
        ]
        for table, col, coltype in migrate_cols:
            try:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            except Exception:
                pass
        db.commit()
        db.close()


def seed_data():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stores")
        count = cur.fetchone()[0]
        if count == 0:
            xlsx_path = os.path.join(BASE_DIR, 'data', 'All LCBO stores.xlsx')
            if os.path.exists(xlsx_path):
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
                    if emails and all(c == ',' for c in emails):
                        emails = ''
                    priority = str(row.get('Priority', 'Standard')) if pd.notna(row.get('Priority')) else 'Standard'
                    status = str(row.get('Status', '')) if pd.notna(row.get('Status')) else ''
                    rep = str(row.get('Rep', '')) if pd.notna(row.get('Rep')) else ''
                    lat, lng = CITY_COORDS.get(city, (0, 0))
                    try:
                        cur.execute(
                            "INSERT INTO stores (store_number, account, address, city, postal, phone, email, contacts, priority, status, rep, lat, lng) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (store_number) DO NOTHING",
                            (store_num, account, address, city, postal, '', emails, contacts, priority, status, rep, lat, lng)
                        )
                    except Exception:
                        pass
        # Seed reps
        for rep_name in ['Ikshit Sharma', 'Namit']:
            try:
                cur.execute("INSERT INTO reps (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (rep_name,))
            except Exception:
                pass
        # Seed products
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] == 0:
            for brand, name, sku, url, price, cat in TRACKED_PRODUCTS:
                cur.execute("INSERT INTO products (brand, name, lcbo_sku, lcbo_url, price, category) VALUES (%s,%s,%s,%s,%s,%s)",
                            (brand, name, sku, url, price, cat))
        # Set coords for stores missing them
        cur.execute("SELECT id, city FROM stores WHERE (lat = 0 OR lat IS NULL) AND city != ''")
        for s in cur.fetchall():
            city = s[1]
            if city in CITY_COORDS:
                lat, lng = CITY_COORDS[city]
                cur.execute("UPDATE stores SET lat=%s, lng=%s WHERE id=%s", (lat, lng, s[0]))
        cur.close()
        conn.close()
    else:
        db = sqlite3.connect(DB_PATH)
        count = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
        if count == 0:
            xlsx_path = os.path.join(BASE_DIR, 'data', 'All LCBO stores.xlsx')
            if not os.path.exists(xlsx_path):
                xlsx_path = '/Users/ikshitsharma/Downloads/All LCBO stores.xlsx'
            if os.path.exists(xlsx_path):
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
                    if emails and all(c == ',' for c in emails):
                        emails = ''
                    priority = str(row.get('Priority', 'Standard')) if pd.notna(row.get('Priority')) else 'Standard'
                    status = str(row.get('Status', '')) if pd.notna(row.get('Status')) else ''
                    rep = str(row.get('Rep', '')) if pd.notna(row.get('Rep')) else ''
                    lat, lng = CITY_COORDS.get(city, (0, 0))
                    try:
                        db.execute(
                            "INSERT OR IGNORE INTO stores (store_number, account, address, city, postal, phone, email, contacts, priority, status, rep, lat, lng) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (store_num, account, address, city, postal, '', emails, contacts, priority, status, rep, lat, lng)
                        )
                    except Exception:
                        pass
        for rep_name in ['Ikshit Sharma', 'Namit']:
            db.execute("INSERT OR IGNORE INTO reps (name) VALUES (?)", (rep_name,))
        prod_count = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        if prod_count == 0:
            for brand, name, sku, url, price, cat in TRACKED_PRODUCTS:
                db.execute("INSERT INTO products (brand, name, lcbo_sku, lcbo_url, price, category) VALUES (?,?,?,?,?,?)",
                           (brand, name, sku, url, price, cat))
        stores = db.execute("SELECT id, city FROM stores WHERE (lat = 0 OR lat IS NULL) AND city != ''").fetchall()
        for s in stores:
            city = s[1]
            if city in CITY_COORDS:
                lat, lng = CITY_COORDS[city]
                db.execute("UPDATE stores SET lat=?, lng=? WHERE id=?", (lat, lng, s[0]))
        db.commit()
        db.close()


# ======== ROUTES ========

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/stores')
def api_stores():
    search = request.args.get('search', '').strip()
    city = request.args.get('city', '').strip()
    producer = request.args.get('producer', '').strip()
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))

    query = "SELECT * FROM stores WHERE 1=1"
    params = []
    if search:
        query += " AND (CAST(store_number AS TEXT) LIKE ? OR account LIKE ? OR address LIKE ? OR city LIKE ? OR manager_name LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s, s, s])
    if city:
        query += " AND city LIKE ?"
        params.append(f"%{city}%")
    if producer:
        query += " AND producer LIKE ?"
        params.append(f"%{producer}%")

    count_query = query.replace("SELECT *", "SELECT COUNT(*)", 1)
    total = db_fetchone(count_query, params)
    total = total[0] if isinstance(total, tuple) else (total.get('count', 0) if isinstance(total, dict) else list(total.values())[0])

    query += " ORDER BY store_number ASC LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])
    rows = db_fetchall(query, params)

    return jsonify({
        'stores': [dict(r) for r in rows],
        'total': total, 'page': page,
        'pages': max(1, (total + per_page - 1) // per_page)
    })


@app.route('/api/stores/<int:store_id>', methods=['GET'])
def api_store_detail(store_id):
    store = db_fetchone("SELECT * FROM stores WHERE id=?", [store_id])
    if not store:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(dict(store))


@app.route('/api/stores/<int:store_id>', methods=['PUT'])
def api_store_update(store_id):
    data = request.json
    fields = ['account', 'address', 'city', 'postal', 'phone', 'email', 'contacts',
              'priority', 'status', 'rep', 'manager_name', 'asst_manager_name',
              'manager_phone', 'store_email', 'producer']
    sets, params = [], []
    for f in fields:
        if f in data:
            sets.append(f"{f}=?")
            params.append(data[f])
    if not sets:
        return jsonify({'error': 'No fields'}), 400
    params.append(store_id)
    db_execute(f"UPDATE stores SET {','.join(sets)} WHERE id=?", params)
    db_commit()
    return jsonify({'success': True})


@app.route('/api/stores/<int:store_id>/snapshot')
def api_store_snapshot(store_id):
    store = db_fetchone("SELECT * FROM stores WHERE id=?", [store_id])
    if not store:
        return jsonify({'error': 'Not found'}), 404

    activities = db_fetchall("""
        SELECT a.*, r.name as rep_name FROM activities a
        JOIN reps r ON a.rep_id=r.id WHERE a.store_id=?
        ORDER BY a.created_at DESC
    """, [store_id])

    summary = db_fetchall("""
        SELECT activity_type, COUNT(*) as count, MAX(created_at) as last_date
        FROM activities WHERE store_id=? GROUP BY activity_type
    """, [store_id])

    followups = db_fetchall("""
        SELECT a.*, r.name as rep_name FROM activities a
        JOIN reps r ON a.rep_id=r.id
        WHERE a.store_id=? AND a.follow_up_date != '' AND a.follow_up_date IS NOT NULL
        ORDER BY a.follow_up_date DESC
    """, [store_id])

    last_note = db_fetchone("""
        SELECT a.notes, a.created_at, r.name as rep_name, a.activity_type
        FROM activities a JOIN reps r ON a.rep_id=r.id
        WHERE a.store_id=? AND a.notes != ''
        ORDER BY a.created_at DESC LIMIT 1
    """, [store_id])

    return jsonify({
        'store': dict(store),
        'activities': [dict(a) for a in activities],
        'summary': {r['activity_type']: {'count': r['count'], 'last_date': str(r['last_date']) if r['last_date'] else None} for r in summary},
        'followups': [dict(f) for f in followups],
        'last_note': dict(last_note) if last_note else None,
        'total_activities': len(activities),
        'first_contact': dict(activities[-1]) if activities else None,
    })


@app.route('/api/reps')
def api_reps():
    rows = db_fetchall("SELECT * FROM reps ORDER BY name")
    return jsonify([dict(r) for r in rows])


@app.route('/api/activities', methods=['POST'])
def api_activity_create():
    data = request.json
    store_id = data.get('store_id')
    rep_id = data.get('rep_id')
    activity_type = data.get('activity_type')
    producer = data.get('producer', '')
    venue_type = data.get('venue_type', '')
    notes = data.get('notes', '')
    follow_up_date = data.get('follow_up_date', '')

    if not all([store_id, rep_id, activity_type]):
        return jsonify({'error': 'Missing required fields'}), 400

    if USE_POSTGRES:
        row = db_fetchone(
            "INSERT INTO activities (store_id, rep_id, activity_type, producer, venue_type, notes, follow_up_date) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (store_id, rep_id, activity_type, producer, venue_type, notes, follow_up_date)
        )
        db_commit()
        new_id = row['id']
    else:
        db_execute(
            "INSERT INTO activities (store_id, rep_id, activity_type, producer, venue_type, notes, follow_up_date) VALUES (?,?,?,?,?,?,?)",
            (store_id, rep_id, activity_type, producer, venue_type, notes, follow_up_date)
        )
        db_commit()
        last = db_fetchone("SELECT last_insert_rowid() as id")
        new_id = last['id'] if isinstance(last, dict) else last[0]

    # Create persistent follow-up record if date set
    if follow_up_date:
        followup_type = data.get('followup_type', activity_type)
        if USE_POSTGRES:
            db_fetchone(
                "INSERT INTO followups (store_id, rep_id, activity_id, followup_type, due_date, notes) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
                (store_id, rep_id, new_id, followup_type, follow_up_date, notes)
            )
        else:
            db_execute(
                "INSERT INTO followups (store_id, rep_id, activity_id, followup_type, due_date, notes) VALUES (?,?,?,?,?,?)",
                (store_id, rep_id, new_id, followup_type, follow_up_date, notes)
            )
        db_commit()

    act = db_fetchone("""
        SELECT a.*, r.name as rep_name, s.store_number, s.account
        FROM activities a JOIN reps r ON a.rep_id=r.id JOIN stores s ON a.store_id=s.id
        WHERE a.id=?
    """, [new_id])

    result = dict(act) if act else {}
    for k, v in result.items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()

    return jsonify({'id': new_id, 'success': True, 'activity': result})


@app.route('/api/activities/<int:store_id>')
def api_activities_for_store(store_id):
    activity_type = request.args.get('type', '')
    producer = request.args.get('producer', '')
    query = "SELECT a.*, r.name as rep_name FROM activities a JOIN reps r ON a.rep_id=r.id WHERE a.store_id=?"
    params = [store_id]
    if activity_type:
        query += " AND a.activity_type=?"
        params.append(activity_type)
    if producer:
        query += " AND a.producer LIKE ?"
        params.append(f"%{producer}%")
    query += " ORDER BY a.created_at DESC"
    rows = db_fetchall(query, params)
    result = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        result.append(d)
    return jsonify(result)


@app.route('/api/activities/summary/<int:store_id>')
def api_activity_summary(store_id):
    rows = db_fetchall("""
        SELECT activity_type, COUNT(*) as count, MAX(created_at) as last_date
        FROM activities WHERE store_id=? GROUP BY activity_type
    """, [store_id])
    return jsonify({r['activity_type']: {'count': r['count'], 'last_date': str(r['last_date']) if r['last_date'] else None} for r in rows})


@app.route('/api/dashboard')
def api_dashboard():
    total_stores = db_fetchone("SELECT COUNT(*) as c FROM stores")
    total_stores = total_stores['c'] if isinstance(total_stores, dict) else total_stores[0]

    total_activities = db_fetchone("SELECT COUNT(*) as c FROM activities")
    total_activities = total_activities['c'] if isinstance(total_activities, dict) else total_activities[0]

    by_type = db_fetchall("SELECT activity_type, COUNT(*) as c FROM activities GROUP BY activity_type")
    by_producer = db_fetchall("SELECT producer, COUNT(*) as c FROM activities WHERE producer != '' GROUP BY producer")

    recent = db_fetchall("""
        SELECT a.*, s.store_number, s.account, s.city, r.name as rep_name
        FROM activities a JOIN stores s ON a.store_id=s.id JOIN reps r ON a.rep_id=r.id
        ORDER BY a.created_at DESC LIMIT 20
    """)

    by_rep = db_fetchall("""
        SELECT r.name, COUNT(a.id) as count FROM reps r
        LEFT JOIN activities a ON r.id=a.rep_id GROUP BY r.id, r.name
    """)

    active_stores = db_fetchone("SELECT COUNT(DISTINCT store_id) as c FROM activities")
    active_stores = active_stores['c'] if isinstance(active_stores, dict) else active_stores[0]

    if USE_POSTGRES:
        week_activities = db_fetchone("SELECT COUNT(*) as c FROM activities WHERE created_at >= NOW() - INTERVAL '7 days'")
    else:
        week_activities = db_fetchone("SELECT COUNT(*) as c FROM activities WHERE created_at >= datetime('now', '-7 days')")
    week_activities = week_activities['c'] if isinstance(week_activities, dict) else week_activities[0]

    today = datetime.now().strftime('%Y-%m-%d')
    try:
        overdue = db_fetchone("SELECT COUNT(*) as c FROM followups WHERE status='pending' AND due_date < ?", [today])
    except Exception:
        overdue = db_fetchone("SELECT COUNT(*) as c FROM activities WHERE follow_up_date != '' AND follow_up_date < ? AND follow_up_date IS NOT NULL", [today])
    overdue = overdue['c'] if isinstance(overdue, dict) else overdue[0]

    # New stores discovered in last 30 days via sync
    try:
        since_30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        new_stores = db_fetchone("SELECT COUNT(*) as c FROM stores WHERE first_seen >= ?", [since_30])
        new_stores = new_stores['c'] if isinstance(new_stores, dict) else new_stores[0]
    except Exception:
        new_stores = 0

    # Last sync time
    try:
        last_synced = db_fetchone("SELECT MAX(first_seen) as t FROM stores WHERE first_seen IS NOT NULL")
        last_synced = last_synced['t'] if isinstance(last_synced, dict) else last_synced[0]
        if isinstance(last_synced, datetime):
            last_synced = last_synced.isoformat()
        else:
            last_synced = str(last_synced) if last_synced else None
    except Exception:
        last_synced = None

    # Serialize datetimes
    recent_list = []
    for r in recent:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        recent_list.append(d)

    return jsonify({
        'total_stores': total_stores, 'total_activities': total_activities,
        'active_stores': active_stores, 'week_activities': week_activities,
        'overdue_followups': overdue, 'new_stores_30d': new_stores,
        'last_synced': last_synced,
        'by_type': {r['activity_type']: r['c'] for r in by_type},
        'by_producer': {r['producer']: r['c'] for r in by_producer},
        'recent': recent_list,
        'by_rep': {r['name']: r['count'] for r in by_rep}
    })


@app.route('/api/cities')
def api_cities():
    rows = db_fetchall("SELECT DISTINCT city FROM stores WHERE city != '' ORDER BY city")
    return jsonify([r['city'] for r in rows])


@app.route('/api/followups')
def api_followups():
    status_filter = request.args.get('status', '')  # pending, completed, all
    # Try new followups table first, fall back to activities
    try:
        query = """
            SELECT f.*, s.store_number, s.account, s.city, s.address, r.name as rep_name,
                   a.activity_type, a.producer, a.venue_type, a.notes as activity_notes
            FROM followups f
            JOIN stores s ON f.store_id=s.id
            JOIN reps r ON f.rep_id=r.id
            LEFT JOIN activities a ON f.activity_id=a.id
        """
        params = []
        if status_filter == 'completed':
            query += " WHERE f.status = 'completed'"
        elif status_filter != 'all':
            query += " WHERE f.status = 'pending'"
        query += " ORDER BY f.due_date ASC"
        rows = db_fetchall(query, params)
        result = []
        for r in rows:
            d = dict(r)
            # Map to frontend-expected fields
            d['follow_up_date'] = str(d.get('due_date', ''))
            if not d.get('notes') and d.get('activity_notes'):
                d['notes'] = d['activity_notes']
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
                elif hasattr(v, 'isoformat'):
                    d[k] = v.isoformat()
            result.append(d)
        # If followups table is empty, migrate from activities
        if not result:
            old_rows = db_fetchall("""
                SELECT a.*, s.store_number, s.account, s.city, s.address, r.name as rep_name
                FROM activities a JOIN stores s ON a.store_id=s.id JOIN reps r ON a.rep_id=r.id
                WHERE a.follow_up_date != '' AND a.follow_up_date IS NOT NULL
                ORDER BY a.follow_up_date ASC
            """)
            for r in old_rows:
                d = dict(r)
                # Migrate to followups table
                try:
                    if USE_POSTGRES:
                        db_fetchone(
                            "INSERT INTO followups (store_id, rep_id, activity_id, followup_type, due_date, notes) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
                            (d['store_id'], d['rep_id'], d['id'], d['activity_type'], d['follow_up_date'], d.get('notes', ''))
                        )
                    else:
                        db_execute(
                            "INSERT INTO followups (store_id, rep_id, activity_id, followup_type, due_date, notes) VALUES (?,?,?,?,?,?)",
                            (d['store_id'], d['rep_id'], d['id'], d['activity_type'], d['follow_up_date'], d.get('notes', ''))
                        )
                except Exception:
                    pass
                for k, v in d.items():
                    if isinstance(v, datetime):
                        d[k] = v.isoformat()
                result.append(d)
            if result:
                db_commit()
        return jsonify(result)
    except Exception:
        # Fallback to old activities-based followups
        rows = db_fetchall("""
            SELECT a.*, s.store_number, s.account, s.city, s.address, r.name as rep_name
            FROM activities a JOIN stores s ON a.store_id=s.id JOIN reps r ON a.rep_id=r.id
            WHERE a.follow_up_date != '' AND a.follow_up_date IS NOT NULL
            ORDER BY a.follow_up_date ASC
        """)
        result = []
        for r in rows:
            d = dict(r)
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
            result.append(d)
        return jsonify(result)


@app.route('/api/followups/<int:followup_id>/complete', methods=['POST'])
def api_followup_complete(followup_id):
    """Mark a follow-up as completed — data is NEVER deleted, only status changes"""
    db_execute("UPDATE followups SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id=?", [followup_id])
    db_commit()
    return jsonify({'success': True, 'message': 'Follow-up marked as completed'})


@app.route('/api/followups/<int:followup_id>/reschedule', methods=['POST'])
def api_followup_reschedule(followup_id):
    """Reschedule a follow-up — never delete, only update date"""
    data = request.json
    new_date = data.get('due_date')
    if not new_date:
        return jsonify({'error': 'due_date required'}), 400
    db_execute("UPDATE followups SET due_date=?, status='pending' WHERE id=?", [new_date, followup_id])
    db_commit()
    return jsonify({'success': True, 'message': f'Follow-up rescheduled to {new_date}'})


# === PRODUCTS & INVENTORY ===

@app.route('/api/products')
def api_products():
    rows = db_fetchall("SELECT * FROM products ORDER BY brand, name")
    products = []
    for r in rows:
        p = dict(r)
        for k, v in p.items():
            if isinstance(v, datetime):
                p[k] = v.isoformat()
        inv = db_fetchone("""
            SELECT COUNT(*) as store_count, COALESCE(SUM(quantity), 0) as total_qty, MAX(checked_at) as last_check
            FROM inventory_cache WHERE product_id=? AND quantity > 0
        """, [r['id']])
        p['stores_stocked'] = inv['store_count'] if inv else 0
        p['total_inventory'] = inv['total_qty'] if inv and inv['total_qty'] else 0
        lc = inv['last_check'] if inv else None
        p['last_checked'] = str(lc) if lc else None
        products.append(p)
    return jsonify(products)


@app.route('/api/products', methods=['POST'])
def api_product_create():
    data = request.json
    if USE_POSTGRES:
        row = db_fetchone(
            "INSERT INTO products (brand, name, lcbo_sku, lcbo_url, price, category) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
            (data.get('brand', ''), data.get('name', ''), data.get('lcbo_sku', ''),
             data.get('lcbo_url', ''), data.get('price', ''), data.get('category', ''))
        )
        db_commit()
        return jsonify({'id': row['id'], 'success': True})
    else:
        db_execute(
            "INSERT INTO products (brand, name, lcbo_sku, lcbo_url, price, category) VALUES (?,?,?,?,?,?)",
            (data.get('brand', ''), data.get('name', ''), data.get('lcbo_sku', ''),
             data.get('lcbo_url', ''), data.get('price', ''), data.get('category', ''))
        )
        db_commit()
        last = db_fetchone("SELECT last_insert_rowid() as id")
        return jsonify({'id': last['id'] if isinstance(last, dict) else last[0], 'success': True})


def _parse_occ_stores(data):
    """Parse SAP Commerce Cloud OCC API response for store inventory"""
    stores = []
    store_list = (data.get('stores') or data.get('pointOfServices') or
                  data.get('storeInventory') or data.get('results') or [])
    if isinstance(store_list, dict):
        store_list = store_list.get('entry', [])
    for s in store_list:
        qty = 0
        for stock_key in ['stockInfo', 'stock', 'stockLevel']:
            stock = s.get(stock_key)
            if isinstance(stock, dict):
                qty = int(stock.get('stockLevel', 0) or 0)
                break
            elif isinstance(stock, (int, float)):
                qty = int(stock)
                break
        addr = s.get('address') or {}
        city = addr.get('town') or addr.get('city') or ''
        store_num = re.sub(r'^[A-Za-z_-]+', '', str(s.get('name', '')).strip()).strip()
        stores.append({
            'store_name': s.get('displayName') or s.get('description') or f'Store #{store_num}',
            'store_number': store_num,
            'city': city,
            'quantity': qty
        })
    return [s for s in stores if s['quantity'] > 0]


def _parse_inventory_html(html):
    """Try multiple patterns to extract store inventory from LCBO HTML page"""
    stores = []

    # Pattern 1: var storeData = [...] (legacy format)
    m = re.search(r'var\s+storeData\s*=\s*(\[.*?\]);', html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            for s in data:
                stores.append({'store_name': s.get('name', ''), 'store_number': str(s.get('store_id', '')),
                               'city': s.get('city', ''), 'quantity': int(s.get('quantity', 0) or 0)})
            if stores:
                return stores
        except Exception:
            pass

    # Pattern 2: JSON array with store/inventory keys in script tags
    for m in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL):
        script = m.group(1)
        if not any(k in script for k in ('"stores"', 'storeInventory', 'storeList', 'availableStore')):
            continue
        for pat in [r'"stores"\s*:\s*(\[.*?\])', r'"storeList"\s*:\s*(\[.*?\])',
                    r'"storeInventory"\s*:\s*(\[.*?\])', r'"availableStores"\s*:\s*(\[.*?\])']:
            m2 = re.search(pat, script, re.DOTALL)
            if m2:
                try:
                    data = json.loads(m2.group(1))
                    for s in data:
                        stores.append({
                            'store_name': s.get('name') or s.get('storeName') or s.get('displayName', ''),
                            'store_number': str(s.get('id') or s.get('storeId') or s.get('store_id') or ''),
                            'city': s.get('city') or (s.get('address') or {}).get('city', ''),
                            'quantity': int(s.get('quantity') or s.get('stockLevel') or 0)
                        })
                    if stores:
                        return stores
                except Exception:
                    pass

    # Pattern 3: window.__NEXT_DATA__ (Next.js SSR)
    m = re.search(r'window\.__NEXT_DATA__\s*=\s*(\{.*\})', html)
    if m:
        try:
            def _find_stores(obj, depth=0):
                if depth > 8:
                    return []
                if isinstance(obj, list) and obj and isinstance(obj[0], dict):
                    if any(k in obj[0] for k in ('storeId', 'store_id', 'quantity', 'stockLevel')):
                        return obj
                if isinstance(obj, dict):
                    for key in ('stores', 'storeList', 'storeInventory', 'availableStores'):
                        if key in obj:
                            r = _find_stores(obj[key], depth + 1)
                            if r:
                                return r
                    for v in obj.values():
                        r = _find_stores(v, depth + 1)
                        if r:
                            return r
                return []
            found = _find_stores(json.loads(m.group(1)))
            for s in found:
                stores.append({'store_name': s.get('name') or s.get('storeName', ''),
                               'store_number': str(s.get('storeId') or s.get('store_id') or ''),
                               'city': s.get('city', ''),
                               'quantity': int(s.get('quantity') or s.get('stockLevel') or 0)})
            if stores:
                return stores
        except Exception:
            pass

    # Pattern 4: HTML class-based fallback
    store_blocks = re.findall(r'class="store-name[^"]*"[^>]*>([^<]+)<', html)
    qty_blocks = re.findall(r'class="store-stock[^"]*"[^>]*>([^<]+)<', html)
    for i, name in enumerate(store_blocks):
        qty = int(re.sub(r'[^0-9]', '', qty_blocks[i].strip() if i < len(qty_blocks) else '') or '0')
        stores.append({'store_name': name.strip(), 'city': '', 'quantity': qty, 'store_number': ''})

    return stores


def fetch_lcbo_inventory_live(sku):
    """
    Try multiple methods to fetch live LCBO store inventory.
    Returns (stores_list, source_string, error_or_None)
    """
    if not http_requests:
        return [], 'unavailable', 'requests module not installed'

    base_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept-Language': 'en-CA,en;q=0.9',
        'Referer': 'https://www.lcbo.com/',
    }

    # Method 1: SAP Commerce Cloud OCC REST API (current LCBO backend as of 2023+)
    occ_paths = [
        f'/lcboocc/v2/lcbo/stores?productCode={sku}&pageSize=200&fields=FULL',
        f'/lcboocc/v2/lcbo/products/{sku}/stock?pageSize=200&fields=DEFAULT',
        f'/api/2.0/products/{sku}/stores?per_page=200',
    ]
    for path in occ_paths:
        try:
            resp = http_requests.get(f'https://www.lcbo.com{path}',
                                     headers={**base_headers, 'Accept': 'application/json'}, timeout=15)
            if resp.status_code == 200 and 'json' in resp.headers.get('Content-Type', ''):
                stores = _parse_occ_stores(resp.json())
                if stores:
                    return stores, 'lcbo_api', None
        except Exception:
            pass

    # Method 2: Store inventory page (multiple HTML/JSON patterns)
    try:
        resp = http_requests.get(f'https://www.lcbo.com/en/storeinventory/?sku={sku}',
                                 headers={**base_headers, 'Accept': 'text/html,*/*'}, timeout=20)
        if resp.status_code == 200:
            stores = _parse_inventory_html(resp.text)
            if stores:
                return stores, 'lcbo_inventory_page', None
    except Exception:
        pass

    # Method 3: Product detail page embedded JSON
    try:
        resp = http_requests.get(f'https://www.lcbo.com/en/p/product-{sku}',
                                 headers={**base_headers, 'Accept': 'text/html,*/*'}, timeout=20)
        if resp.status_code == 200:
            stores = _parse_inventory_html(resp.text)
            if stores:
                return stores, 'lcbo_product_page', None
    except Exception:
        pass

    return [], 'failed', 'All live check methods failed — LCBO.com may be unavailable or layout changed'


@app.route('/api/inventory/check/<sku>')
def api_inventory_check(sku):
    product = db_fetchone("SELECT * FROM products WHERE lcbo_sku=?", [sku])
    if not product:
        return jsonify({'error': 'Product not found', 'stores': []})
    product = dict(product)
    for k, v in product.items():
        if isinstance(v, datetime):
            product[k] = v.isoformat()

    stores, source, err = fetch_lcbo_inventory_live(sku)

    if stores:
        db_execute("DELETE FROM inventory_cache WHERE product_id=?", [product['id']])
        for s in stores:
            db_execute(
                "INSERT INTO inventory_cache (product_id, store_number, store_name, store_city, quantity) VALUES (?,?,?,?,?)",
                [product['id'], s.get('store_number', 0), s.get('store_name', ''), s.get('city', ''), s.get('quantity', 0)]
            )
        db_commit()
        return jsonify({'product': product, 'stores': stores, 'checked_at': datetime.now().isoformat(),
                        'source': source, 'store_count': len(stores)})

    # Fall back to cache
    cached = db_fetchall("SELECT * FROM inventory_cache WHERE product_id=? ORDER BY store_city", [product['id']])
    cached_list = [dict(c) for c in cached]
    return jsonify({'product': product, 'stores': cached_list, 'checked_at': None,
                    'source': 'cache', 'error': err or 'Live check failed',
                    'store_count': len(cached_list)})


# === ROUTE PLANNING ===

@app.route('/api/routes')
def api_routes():
    city = request.args.get('city', '').strip()
    max_distance = request.args.get('max_km', '').strip()
    limit = int(request.args.get('limit', 50))
    sort_by = request.args.get('sort', 'distance')  # distance, priority, activity
    district = request.args.get('district', '').strip()  # GTA, Eastern, Northern, etc.

    # District regions mapping
    DISTRICTS = {
        'GTA': ['Toronto', 'Scarborough', 'Etobicoke', 'North York', 'East York', 'York',
                'Mississauga', 'Brampton', 'Vaughan', 'Markham', 'Richmond Hill', 'Thornhill',
                'Pickering', 'Ajax', 'Whitby', 'Oshawa', 'Oakville', 'Burlington', 'Milton',
                'Newmarket', 'Aurora', 'Stouffville', 'Keswick', 'Innisfil'],
        'Golden Horseshoe': ['Hamilton', 'St. Catharines', 'Niagara Falls', 'Welland', 'Grimsby',
                            'Stoney Creek', 'Ancaster', 'Dundas', 'Burlington', 'Oakville', 'Brantford'],
        'Southwestern': ['London', 'Windsor', 'Kitchener', 'Waterloo', 'Cambridge', 'Guelph',
                        'Woodstock', 'Stratford', 'Chatham', 'Sarnia'],
        'Eastern': ['Ottawa', 'Kingston', 'Belleville', 'Cornwall', 'Peterborough', 'Cobourg',
                   'Port Hope', 'Bowmanville', 'Lindsay', 'Kawartha Lakes'],
        'Northern': ['Sudbury', 'Thunder Bay', 'Sault Ste. Marie', 'North Bay', 'Timmins',
                    'Barrie', 'Orillia', 'Collingwood', 'Midland', 'Penetanguishene',
                    'Gravenhurst', 'Bracebridge', 'Huntsville', 'Parry Sound'],
    }

    # Single optimized query with activity counts and last activity
    query = """SELECT s.*, COUNT(a.id) as activity_count,
               MAX(a.created_at) as last_activity_date,
               (SELECT a2.activity_type FROM activities a2 WHERE a2.store_id=s.id ORDER BY a2.created_at DESC LIMIT 1) as last_activity_type
               FROM stores s LEFT JOIN activities a ON s.id=a.store_id
               WHERE s.lat != 0 AND s.lng != 0"""
    params = []
    if city:
        query += " AND s.city LIKE ?"
        params.append(f"%{city}%")
    elif district and district in DISTRICTS:
        placeholders = ','.join(['?' for _ in DISTRICTS[district]])
        query += f" AND s.city IN ({placeholders})"
        params.extend(DISTRICTS[district])

    if USE_POSTGRES:
        query += " GROUP BY s.id"
    else:
        query += " GROUP BY s.id"

    stores = db_fetchall(query, params)
    results = []
    for s in stores:
        s = dict(s)
        dist = haversine(REP_HOME['lat'], REP_HOME['lng'], float(s['lat'] or 0), float(s['lng'] or 0))
        if max_distance and dist > float(max_distance):
            continue
        s['distance_km'] = round(dist, 1)
        s['activity_count'] = int(s.get('activity_count') or 0)

        last_date = s.pop('last_activity_date', None)
        last_type = s.pop('last_activity_type', None)
        if last_date:
            if isinstance(last_date, datetime):
                last_date_str = last_date.isoformat()
            else:
                last_date_str = str(last_date)
            s['last_activity'] = {'activity_type': last_type, 'created_at': last_date_str}
        else:
            s['last_activity'] = None

        # Priority score: lower = higher priority (needs visit)
        days_since = 999
        if last_date:
            try:
                if isinstance(last_date, datetime):
                    days_since = (datetime.now() - last_date.replace(tzinfo=None)).days
                else:
                    last_dt = datetime.fromisoformat(str(last_date).replace('Z', '+00:00'))
                    days_since = (datetime.now() - last_dt.replace(tzinfo=None)).days
            except Exception:
                pass
        priority_score = s['distance_km'] * 0.3 - days_since * 0.5 - (10 - min(s['activity_count'], 10)) * 2
        s['priority_score'] = round(priority_score, 1)
        s['days_since_visit'] = days_since if days_since < 999 else None
        s['full_address'] = f"{s.get('address', '') or ''}, {s.get('city', '') or ''}, ON {s.get('postal', '') or ''}".strip(', ')
        results.append(s)

    if sort_by == 'priority':
        results.sort(key=lambda x: x['priority_score'])
    elif sort_by == 'activity':
        results.sort(key=lambda x: -(x.get('days_since_visit') or 999))
    else:
        results.sort(key=lambda x: x['distance_km'])
    results = results[:limit]

    # Build Google Maps multi-stop route URL using real addresses
    route_url = f"https://www.google.com/maps/dir/{REP_HOME['lat']},{REP_HOME['lng']}"
    for s in results[:9]:
        addr = s['full_address'].replace(' ', '+')
        route_url += f"/{addr}" if addr.strip(', ') else f"/{s['lat']},{s['lng']}"

    # Group by city for district summary
    city_groups = {}
    for s in results:
        c = s.get('city', 'Unknown')
        if c not in city_groups:
            city_groups[c] = {'city': c, 'count': 0, 'avg_dist': 0, 'stores': []}
        city_groups[c]['count'] += 1
        city_groups[c]['avg_dist'] += s['distance_km']
        city_groups[c]['stores'].append(s['id'])
    for cg in city_groups.values():
        cg['avg_dist'] = round(cg['avg_dist'] / cg['count'], 1)
    district_summary = sorted(city_groups.values(), key=lambda x: x['avg_dist'])

    return jsonify({
        'stores': results, 'rep_home': REP_HOME, 'route_url': route_url,
        'total': len(results), 'districts': list(DISTRICTS.keys()),
        'district_summary': district_summary
    })


@app.route('/api/routes/cities')
def api_route_cities():
    rows = db_fetchall("""
        SELECT city, COUNT(*) as store_count, AVG(lat) as avg_lat, AVG(lng) as avg_lng
        FROM stores WHERE city != '' AND lat != 0
        GROUP BY city ORDER BY city
    """)
    # Get activity counts per city
    act_rows = db_fetchall("""
        SELECT s.city, COUNT(a.id) as act_count
        FROM stores s LEFT JOIN activities a ON s.id=a.store_id
        WHERE s.city != '' GROUP BY s.city
    """)
    city_acts = {dict(r)['city']: dict(r)['act_count'] for r in act_rows}

    results = []
    for r in rows:
        r = dict(r)
        avg_lat = float(r['avg_lat'] or 0)
        avg_lng = float(r['avg_lng'] or 0)
        dist = haversine(REP_HOME['lat'], REP_HOME['lng'], avg_lat, avg_lng)
        results.append({
            'city': r['city'], 'store_count': int(r['store_count']),
            'distance_km': round(dist, 1), 'lat': avg_lat, 'lng': avg_lng,
            'activity_count': int(city_acts.get(r['city'], 0)),
            'coverage': round(int(city_acts.get(r['city'], 0)) / max(int(r['store_count']), 1) * 100)
        })
    results.sort(key=lambda x: x['distance_km'])
    return jsonify(results)


@app.route('/api/routes/district/<district_name>')
def api_route_district(district_name):
    """Get optimized route for an entire district with city-by-city breakdown"""
    DISTRICTS = {
        'GTA': ['Toronto', 'Scarborough', 'Etobicoke', 'North York', 'East York', 'York',
                'Mississauga', 'Brampton', 'Vaughan', 'Markham', 'Richmond Hill', 'Thornhill',
                'Pickering', 'Ajax', 'Whitby', 'Oshawa', 'Oakville', 'Burlington', 'Milton',
                'Newmarket', 'Aurora', 'Stouffville', 'Keswick', 'Innisfil'],
        'Golden Horseshoe': ['Hamilton', 'St. Catharines', 'Niagara Falls', 'Welland', 'Grimsby',
                            'Stoney Creek', 'Ancaster', 'Dundas', 'Burlington', 'Oakville', 'Brantford'],
        'Southwestern': ['London', 'Windsor', 'Kitchener', 'Waterloo', 'Cambridge', 'Guelph',
                        'Woodstock', 'Stratford', 'Chatham', 'Sarnia'],
        'Eastern': ['Ottawa', 'Kingston', 'Belleville', 'Cornwall', 'Peterborough', 'Cobourg',
                   'Port Hope', 'Bowmanville', 'Lindsay', 'Kawartha Lakes'],
        'Northern': ['Sudbury', 'Thunder Bay', 'Sault Ste. Marie', 'North Bay', 'Timmins',
                    'Barrie', 'Orillia', 'Collingwood', 'Midland', 'Penetanguishene',
                    'Gravenhurst', 'Bracebridge', 'Huntsville', 'Parry Sound'],
    }
    if district_name not in DISTRICTS:
        return jsonify({'error': 'Unknown district'}), 404

    cities = DISTRICTS[district_name]
    placeholders = ','.join(['?' for _ in cities])
    stores = db_fetchall(f"SELECT * FROM stores WHERE city IN ({placeholders}) AND lat != 0", cities)

    city_breakdown = {}
    for s in stores:
        s = dict(s)
        c = s['city']
        if c not in city_breakdown:
            dist = haversine(REP_HOME['lat'], REP_HOME['lng'], s['lat'], s['lng'])
            city_breakdown[c] = {'city': c, 'distance_km': round(dist, 1), 'total_stores': 0, 'visited': 0, 'stores': []}
        city_breakdown[c]['total_stores'] += 1
        act_count = db_fetchone("SELECT COUNT(*) as c FROM activities WHERE store_id=?", [s['id']])
        cnt = act_count['c'] if isinstance(act_count, dict) else act_count[0]
        if cnt > 0:
            city_breakdown[c]['visited'] += 1
        city_breakdown[c]['stores'].append({
            'id': s['id'], 'store_number': s['store_number'], 'account': s['account'],
            'address': s['address'], 'activity_count': cnt
        })

    breakdown = sorted(city_breakdown.values(), key=lambda x: x['distance_km'])
    for cb in breakdown:
        cb['coverage'] = round(cb['visited'] / max(cb['total_stores'], 1) * 100)
        # Generate Google Maps route for this city (top 9 unvisited stores)
        unvisited = [st for st in cb['stores'] if st['activity_count'] == 0][:9]
        if unvisited:
            route = f"https://www.google.com/maps/dir/{REP_HOME['lat']},{REP_HOME['lng']}"
            for st in unvisited:
                addr = (st.get('address', '') or '').replace(' ', '+')
                route += f"/{addr}" if addr else ''
            cb['route_url'] = route

    return jsonify({
        'district': district_name, 'cities': [c for c in cities if c in city_breakdown],
        'total_stores': sum(cb['total_stores'] for cb in breakdown),
        'total_visited': sum(cb['visited'] for cb in breakdown),
        'breakdown': breakdown
    })


# === EXPORT ===

@app.route('/api/export/stores')
def export_stores_csv():
    rows = db_fetchall("SELECT * FROM stores ORDER BY store_number")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Store #', 'Account', 'Address', 'City', 'Postal', 'Phone', 'Email', 'Contacts',
                     'Priority', 'Status', 'Rep', 'Manager', 'Asst Manager', 'Manager Phone', 'Store Email', 'Producer'])
    for r in rows:
        r = dict(r)
        writer.writerow([r['store_number'], r['account'], r['address'], r['city'], r['postal'], r['phone'],
                         r['email'], r['contacts'], r['priority'], r['status'], r['rep'],
                         r['manager_name'], r['asst_manager_name'], r['manager_phone'], r['store_email'], r['producer']])
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=lcbo_stores_{datetime.now().strftime("%Y%m%d")}.csv'})


@app.route('/api/export/activities')
def export_activities_csv():
    rows = db_fetchall("""
        SELECT s.store_number, s.account, s.city, r.name as rep_name,
               a.activity_type, a.producer, a.venue_type, a.notes, a.follow_up_date, a.created_at
        FROM activities a JOIN stores s ON a.store_id=s.id JOIN reps r ON a.rep_id=r.id
        ORDER BY a.created_at DESC
    """)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Store #', 'Account', 'City', 'Rep', 'Activity', 'Producer', 'Venue', 'Notes', 'Follow-Up', 'Date/Time'])
    for r in rows:
        r = dict(r)
        ca = r['created_at']
        if isinstance(ca, datetime):
            ca = ca.isoformat()
        writer.writerow([r['store_number'], r['account'], r['city'], r['rep_name'],
                         r['activity_type'], r['producer'], r['venue_type'], r['notes'], r['follow_up_date'], ca])
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=lcbo_activities_{datetime.now().strftime("%Y%m%d")}.csv'})


@app.route('/api/export/pipeline')
def export_pipeline_csv():
    rows = db_fetchall("""
        SELECT s.store_number, s.account, s.address, s.city, s.postal,
               s.phone, s.email, s.contacts, s.priority, s.status, s.rep,
               s.manager_name, s.asst_manager_name, s.manager_phone, s.store_email, s.producer,
               COUNT(a.id) as total_activities,
               SUM(CASE WHEN a.activity_type='tasting' THEN 1 ELSE 0 END) as tastings,
               SUM(CASE WHEN a.activity_type='site_visit' THEN 1 ELSE 0 END) as site_visits,
               SUM(CASE WHEN a.activity_type='listing' THEN 1 ELSE 0 END) as listings,
               SUM(CASE WHEN a.activity_type='email' THEN 1 ELSE 0 END) as emails,
               SUM(CASE WHEN a.activity_type='call' THEN 1 ELSE 0 END) as calls,
               MAX(a.created_at) as last_activity
        FROM stores s LEFT JOIN activities a ON s.id=a.store_id
        GROUP BY s.id, s.store_number, s.account, s.address, s.city, s.postal,
                 s.phone, s.email, s.contacts, s.priority, s.status, s.rep,
                 s.manager_name, s.asst_manager_name, s.manager_phone, s.store_email, s.producer
        ORDER BY s.store_number
    """)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Store #', 'Account', 'Address', 'City', 'Postal', 'Phone', 'Email', 'Contacts',
                     'Priority', 'Status', 'Rep', 'Manager', 'Asst Manager', 'Manager Phone', 'Store Email',
                     'Producer', 'Total Activities', 'Tastings', 'Site Visits', 'Listings', 'Emails', 'Calls', 'Last Activity'])
    for r in rows:
        r = dict(r)
        la = r['last_activity']
        if isinstance(la, datetime):
            la = la.isoformat()
        writer.writerow([r['store_number'], r['account'], r['address'], r['city'], r['postal'],
                         r['phone'], r['email'], r['contacts'], r['priority'], r['status'], r['rep'],
                         r['manager_name'], r['asst_manager_name'], r['manager_phone'], r['store_email'], r['producer'],
                         r['total_activities'], r['tastings'], r['site_visits'], r['listings'],
                         r['emails'], r['calls'], la])
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=lcbo_pipeline_{datetime.now().strftime("%Y%m%d")}.csv'})


@app.route('/api/export/backup')
def export_backup():
    if not USE_POSTGRES and os.path.exists(DB_PATH):
        return send_file(DB_PATH, as_attachment=True,
                         download_name=f'lcbo_tracker_backup_{datetime.now().strftime("%Y%m%d_%H%M")}.db')
    # For PostgreSQL, export all data as JSON
    stores = db_fetchall("SELECT * FROM stores ORDER BY store_number")
    activities = db_fetchall("""
        SELECT a.*, r.name as rep_name, s.store_number, s.account
        FROM activities a JOIN reps r ON a.rep_id=r.id JOIN stores s ON a.store_id=s.id
        ORDER BY a.created_at DESC
    """)
    backup = {
        'exported_at': datetime.now().isoformat(),
        'stores': [dict(r) for r in stores],
        'activities': [],
    }
    for a in activities:
        d = dict(a)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        backup['activities'].append(d)
    output = json.dumps(backup, indent=2, default=str)
    return Response(output, mimetype='application/json',
                    headers={'Content-Disposition': f'attachment; filename=lcbo_tracker_backup_{datetime.now().strftime("%Y%m%d_%H%M")}.json'})


@app.route('/api/geocode', methods=['POST'])
def api_geocode_stores():
    """Geocode stores using Nominatim (OpenStreetMap) - batch process"""
    if not http_requests:
        return jsonify({'error': 'requests library not available'}), 500
    batch_size = int(request.args.get('batch', 50))
    # Find stores still using city-center coords (multiple stores share same lat/lng)
    rows = db_fetchall("""
        SELECT id, address, city, postal, lat, lng FROM stores
        WHERE address != '' AND city != ''
        ORDER BY id
    """)
    # Group by lat/lng to find stores sharing coordinates
    coord_groups = {}
    for r in rows:
        r = dict(r)
        key = (round(float(r['lat'] or 0), 4), round(float(r['lng'] or 0), 4))
        if key not in coord_groups:
            coord_groups[key] = []
        coord_groups[key].append(r)
    # Only geocode stores that share coords with 2+ other stores (city-center defaults)
    needs_geocoding = []
    for key, group in coord_groups.items():
        if len(group) > 1:
            needs_geocoding.extend(group)
    needs_geocoding = needs_geocoding[:batch_size]
    if not needs_geocoding:
        return jsonify({'message': 'All stores already have unique coordinates', 'geocoded': 0})
    geocoded = 0
    errors = 0
    for store in needs_geocoding:
        addr = f"{store['address']}, {store['city']}, ON {store['postal']}, Canada"
        try:
            resp = http_requests.get(
                'https://nominatim.openstreetmap.org/search',
                params={'q': addr, 'format': 'json', 'limit': 1},
                headers={'User-Agent': 'LCBOTracker/1.0 (anu-spirits-crm)'},
                timeout=10
            )
            if resp.status_code == 200 and resp.json():
                result = resp.json()[0]
                new_lat = float(result['lat'])
                new_lng = float(result['lon'])
                db_execute("UPDATE stores SET lat=?, lng=? WHERE id=?", [new_lat, new_lng, store['id']])
                geocoded += 1
            else:
                errors += 1
        except Exception:
            errors += 1
        import time as _time
        _time.sleep(1.1)  # Nominatim rate limit: 1 req/sec
    db_commit()
    remaining = len([s for g in coord_groups.values() if len(g) > 1 for s in g]) - batch_size
    return jsonify({
        'geocoded': geocoded, 'errors': errors,
        'remaining': max(0, remaining),
        'message': f'Geocoded {geocoded} stores. {max(0, remaining)} remaining. Run again to continue.'
    })


@app.route('/api/opportunities/nb-distillers')
def api_nb_opportunities():
    """Show stores with 0 or 1 NB Distillers products stocked — key sales opportunities"""
    # Get inventory cache for NB products (Red Admiral 20187, Chak De 22246)
    nb_products = db_fetchall("SELECT id, name, lcbo_sku FROM products WHERE brand='NB Distillers'")
    nb_product_ids = [p['id'] for p in nb_products]

    # Get stores with their NB Distillers stock counts
    stores = db_fetchall("""
        SELECT s.id, s.store_number, s.account, s.city, s.address, s.postal,
               s.manager_name, s.phone, s.priority, s.lat, s.lng,
               COUNT(DISTINCT ic.product_id) as nb_products_stocked,
               COALESCE(SUM(ic.quantity), 0) as total_nb_inventory,
               COUNT(a.id) as activity_count,
               MAX(a.created_at) as last_visit
        FROM stores s
        LEFT JOIN inventory_cache ic ON s.store_number = CAST(ic.store_number AS INTEGER)
            AND ic.product_id IN (SELECT id FROM products WHERE brand='NB Distillers')
            AND ic.quantity > 0
        LEFT JOIN activities a ON s.id = a.store_id
        GROUP BY s.id
        ORDER BY nb_products_stocked ASC, s.city, s.store_number
    """)

    zero_stock = []
    one_product = []
    fully_stocked = []
    for s in stores:
        s = dict(s)
        stocked = int(s.get('nb_products_stocked') or 0)
        s['nb_products_stocked'] = stocked
        s['total_nb_inventory'] = int(s.get('total_nb_inventory') or 0)
        s['activity_count'] = int(s.get('activity_count') or 0)
        last = s.pop('last_visit', None)
        s['last_visit'] = last.isoformat() if isinstance(last, datetime) else str(last) if last else None
        s['full_address'] = f"{s.get('address', '')}, {s.get('city', '')}, ON {s.get('postal', '')}".strip(', ')
        if stocked == 0:
            zero_stock.append(s)
        elif stocked == 1:
            one_product.append(s)
        else:
            fully_stocked.append(s)

    return jsonify({
        'zero_stock': zero_stock,
        'one_product': one_product,
        'fully_stocked': fully_stocked,
        'summary': {
            'total_stores': len(stores),
            'zero_nb': len(zero_stock),
            'one_nb': len(one_product),
            'both_nb': len(fully_stocked),
            'nb_products': [dict(p) for p in nb_products]
        }
    })


@app.route('/api/routes/daily-plan')
def api_daily_plan():
    """Generate optimized daily route plan for a rep — 8-10 stores per day"""
    rep_id = request.args.get('rep_id', '1')
    district = request.args.get('district', 'GTA')
    days = int(request.args.get('days', 5))  # Mon-Fri
    stores_per_day = int(request.args.get('stores_per_day', 8))

    DISTRICTS = {
        'GTA': ['Toronto', 'Scarborough', 'Etobicoke', 'North York', 'East York', 'York',
                'Mississauga', 'Brampton', 'Vaughan', 'Markham', 'Richmond Hill', 'Thornhill',
                'Pickering', 'Ajax', 'Whitby', 'Oshawa', 'Oakville', 'Burlington', 'Milton',
                'Newmarket', 'Aurora', 'Stouffville', 'Keswick', 'Innisfil'],
        'Golden Horseshoe': ['Hamilton', 'St. Catharines', 'Niagara Falls', 'Welland', 'Grimsby',
                            'Stoney Creek', 'Ancaster', 'Dundas', 'Burlington', 'Oakville', 'Brantford'],
        'Southwestern': ['London', 'Windsor', 'Kitchener', 'Waterloo', 'Cambridge', 'Guelph',
                        'Woodstock', 'Stratford', 'Chatham', 'Sarnia'],
        'Eastern': ['Ottawa', 'Kingston', 'Belleville', 'Cornwall', 'Peterborough', 'Cobourg',
                   'Port Hope', 'Bowmanville', 'Lindsay', 'Kawartha Lakes'],
        'Northern': ['Sudbury', 'Thunder Bay', 'Sault Ste. Marie', 'North Bay', 'Timmins',
                    'Barrie', 'Orillia', 'Collingwood', 'Midland', 'Penetanguishene',
                    'Gravenhurst', 'Bracebridge', 'Huntsville', 'Parry Sound'],
    }

    cities = DISTRICTS.get(district, DISTRICTS['GTA'])
    placeholders = ','.join(['?' for _ in cities])

    # Get unvisited/priority stores in district
    stores = db_fetchall(f"""
        SELECT s.*, COUNT(a.id) as activity_count,
               MAX(a.created_at) as last_activity_date
        FROM stores s LEFT JOIN activities a ON s.id=a.store_id
        WHERE s.city IN ({placeholders})
        GROUP BY s.id
        ORDER BY COUNT(a.id) ASC, s.city
    """, cities)

    store_list = []
    for s in stores:
        s = dict(s)
        s['activity_count'] = int(s.get('activity_count') or 0)
        last = s.pop('last_activity_date', None)
        days_since = 999
        if last:
            try:
                if isinstance(last, datetime):
                    days_since = (datetime.now() - last.replace(tzinfo=None)).days
                else:
                    days_since = (datetime.now() - datetime.fromisoformat(str(last).replace('Z', '+00:00')).replace(tzinfo=None)).days
            except Exception:
                pass
        s['days_since_visit'] = days_since if days_since < 999 else None
        s['full_address'] = f"{s.get('address', '')}, {s.get('city', '')}, ON {s.get('postal', '')}".strip(', ')
        s['lat'] = float(s.get('lat') or 0)
        s['lng'] = float(s.get('lng') or 0)
        store_list.append(s)

    # Sort by priority: unvisited first, then longest since visit
    store_list.sort(key=lambda x: (x['activity_count'], -(x['days_since_visit'] or 999)))

    # Group into daily plans by city proximity
    daily_plans = []
    assigned = set()
    today = datetime.now()
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']

    for day_idx in range(days):
        day_stores = []
        # Pick a seed city that has unassigned stores
        seed_store = None
        for s in store_list:
            if s['id'] not in assigned:
                seed_store = s
                break
        if not seed_store:
            break

        day_stores.append(seed_store)
        assigned.add(seed_store['id'])
        seed_city = seed_store.get('city', '')

        # Fill rest of day with nearby stores (same city first, then nearby)
        for s in store_list:
            if len(day_stores) >= stores_per_day:
                break
            if s['id'] in assigned:
                continue
            # Same city or within 15km
            if s.get('city') == seed_city:
                day_stores.append(s)
                assigned.add(s['id'])
            elif seed_store['lat'] and s.get('lat'):
                dist = haversine(seed_store['lat'], seed_store['lng'], s['lat'], s['lng'])
                if dist < 15:
                    day_stores.append(s)
                    assigned.add(s['id'])

        # Build Google Maps route for the day
        route_url = f"https://www.google.com/maps/dir/{REP_HOME['lat']},{REP_HOME['lng']}"
        for s in day_stores[:9]:
            addr = s['full_address'].replace(' ', '+')
            route_url += f"/{addr}" if addr.strip(', ') else f"/{s['lat']},{s['lng']}"

        plan_date = today + timedelta(days=(day_idx - today.weekday()) % 7 + (7 if day_idx >= 5 else 0))
        if day_idx < 5:
            plan_date = today + timedelta(days=day_idx)

        daily_plans.append({
            'day': day_names[day_idx % 5],
            'date': plan_date.strftime('%Y-%m-%d'),
            'stores': day_stores,
            'store_count': len(day_stores),
            'cities': list(set(s.get('city', '') for s in day_stores)),
            'route_url': route_url
        })

    return jsonify({
        'plans': daily_plans,
        'district': district,
        'total_stores_planned': len(assigned),
        'total_stores_in_district': len(store_list)
    })


@app.route('/api/stores/sync', methods=['POST'])
def api_sync_stores():
    """Sync LCBO store list from Ontario Open Data (data.ontario.ca) to catch new openings."""
    if not http_requests:
        return jsonify({'error': 'requests module not available', 'new': 0, 'updated': 0})
    try:
        # Discover LCBO store locations dataset on Ontario Open Data (CKAN API)
        pkg_resp = http_requests.get(
            'https://data.ontario.ca/api/3/action/package_show',
            params={'id': 'lcbo-store-locations'},
            timeout=30
        )
        if pkg_resp.status_code != 200:
            return jsonify({'error': f'Ontario Open Data unreachable (HTTP {pkg_resp.status_code})', 'new': 0})
        pkg_data = pkg_resp.json()
        if not pkg_data.get('success'):
            return jsonify({'error': 'LCBO dataset not found on data.ontario.ca', 'new': 0})

        resources = pkg_data['result'].get('resources', [])
        resource_id = None
        csv_url = None
        for r in resources:
            if r.get('datastore_active'):
                resource_id = r['id']
                break
            if not csv_url and r.get('format', '').upper() == 'CSV':
                csv_url = r.get('url')

        new_count = 0
        updated_count = 0

        if resource_id:
            # Fetch via CKAN datastore API
            offset = 0
            limit = 1000
            total = None
            while True:
                resp = http_requests.get(
                    'https://data.ontario.ca/api/3/action/datastore_search',
                    params={'resource_id': resource_id, 'limit': limit, 'offset': offset},
                    timeout=30
                )
                if resp.status_code != 200:
                    break
                result = resp.json().get('result', {})
                records = result.get('records', [])
                if total is None:
                    total = result.get('total', 0)
                if not records:
                    break
                for record in records:
                    n, u = _upsert_store_record(record)
                    new_count += n
                    updated_count += u
                db_commit()
                offset += len(records)
                if total and offset >= total:
                    break
        elif csv_url:
            # Download CSV directly
            import io as _io
            import csv as _csv
            r = http_requests.get(csv_url, timeout=60)
            if r.status_code == 200:
                reader = _csv.DictReader(_io.StringIO(r.text))
                for row in reader:
                    n, u = _upsert_store_record(row)
                    new_count += n
                    updated_count += u
                db_commit()
        else:
            return jsonify({'error': 'No accessible data resource found in LCBO dataset', 'new': 0})

        return jsonify({
            'success': True,
            'new_stores': new_count,
            'existing_stores': updated_count,
            'message': f'Sync complete: {new_count} new stores added, {updated_count} existing verified'
        })
    except Exception as e:
        return jsonify({'error': str(e), 'new': 0, 'updated': 0})


def _upsert_store_record(record):
    """Insert or no-op a store record from Ontario Open Data. Returns (new_count, updated_count)."""
    store_num = None
    for k in ('Store #', 'STORE_NO', 'Store Number', 'License #', 'store_number'):
        val = record.get(k)
        if val:
            try:
                store_num = int(str(val).strip().replace(',', ''))
                break
            except Exception:
                pass
    if not store_num or store_num <= 0:
        return 0, 0

    account = (record.get('Store Name') or record.get('Account') or record.get('NAME') or f'LCBO #{store_num}')
    address = (record.get('Address') or record.get('Street Address') or record.get('ADDRESS') or '')
    city = (record.get('City') or record.get('CITY') or '')
    postal = (record.get('Postal Code') or record.get('Postal') or record.get('POSTAL_CODE') or '')
    phone = (record.get('Phone') or record.get('PHONE') or '')
    lat, lng = CITY_COORDS.get(city, (0, 0))

    existing = db_fetchone("SELECT id FROM stores WHERE store_number=?", [store_num])
    if existing:
        return 0, 1
    if USE_POSTGRES:
        db_execute(
            "INSERT INTO stores (store_number, account, address, city, postal, phone, lat, lng, first_seen) VALUES (?,?,?,?,?,?,?,?,NOW()) ON CONFLICT (store_number) DO NOTHING",
            [store_num, account, address, city, postal, phone, lat, lng]
        )
    else:
        db_execute(
            "INSERT OR IGNORE INTO stores (store_number, account, address, city, postal, phone, lat, lng, first_seen) VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
            [store_num, account, address, city, postal, phone, lat, lng]
        )
    return 1, 0


@app.route('/api/stores/new')
def api_new_stores():
    """Return stores added via sync (have a first_seen date), newest first."""
    days = int(request.args.get('days', 90))
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    if USE_POSTGRES:
        rows = db_fetchall("""
            SELECT s.*, COUNT(a.id) as activity_count
            FROM stores s LEFT JOIN activities a ON s.id=a.store_id
            WHERE s.first_seen >= %s::date
            GROUP BY s.id ORDER BY s.first_seen DESC LIMIT 100
        """, [since])
    else:
        rows = db_fetchall("""
            SELECT s.*, COUNT(a.id) as activity_count
            FROM stores s LEFT JOIN activities a ON s.id=a.store_id
            WHERE s.first_seen >= ?
            GROUP BY s.id ORDER BY s.first_seen DESC LIMIT 100
        """, [since])
    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get('first_seen'), datetime):
            d['first_seen'] = d['first_seen'].isoformat()
        result.append(d)
    return jsonify(result)


@app.route('/api/analytics/opportunity')
def api_opportunity():
    city = request.args.get('city', '').strip()
    query = """
        SELECT s.*, COUNT(a.id) as act_count, MAX(a.created_at) as last_activity
        FROM stores s LEFT JOIN activities a ON s.id=a.store_id
    """
    params = []
    if city:
        query += " WHERE s.city LIKE ?"
        params.append(f"%{city}%")
    if USE_POSTGRES:
        query += " GROUP BY s.id HAVING COUNT(a.id) < 3 ORDER BY COUNT(a.id) ASC, s.city LIMIT 100"
    else:
        query += " GROUP BY s.id HAVING act_count < 3 ORDER BY act_count ASC, s.city LIMIT 100"
    rows = db_fetchall(query, params)
    return jsonify([dict(r) for r in rows])


# ======== INIT ========

init_db()
seed_data()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    app.run(debug=os.environ.get('FLASK_DEBUG', 'true').lower() == 'true', host='0.0.0.0', port=port)
