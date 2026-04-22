import os
import io
import csv
import gc
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

# Our tracked products on LCBO.com — verified SKUs (April 2026)
# Source: live LCBO.com pages + lcbo.dev GraphQL API introspection
TRACKED_PRODUCTS = [
    # NB Distillers (Anu-owned brand)
    ('NB Distillers', 'Red Admiral Vodka', '20187', 'https://www.lcbo.com/en/red-admiral-vodka-20187', '$29.75', 'Spirits'),
    ('NB Distillers', 'Chak De Canadian Whisky', '22246', 'https://www.lcbo.com/en/chak-de-canadian-whisky-22246', '$34.95', 'Spirits'),
    # Anu portfolio — Goenchi Feni (India)
    ('Goenchi', 'Goenchi Cashew Feni', '46340', 'https://www.lcbo.com/en/goenchi-cashew-feni-46340', '$93.95', 'Spirits'),
    ('Goenchi', 'Goenchi Coconut Feni', '46343', 'https://www.lcbo.com/en/goenchi-coconut-feni-46343', '$93.95', 'Spirits'),
    # Fratelli wines (India)
    ('Fratelli', 'Fratelli Classic Shiraz', '46282', 'https://www.lcbo.com/en/fratelli-classic-shiraz-46282', '$22.95', 'Wine'),
    ('Fratelli', 'Fratelli Sauvignon Blanc', '46286', 'https://www.lcbo.com/en/fratelli-sauvignon-blanc-46286', '$24.95', 'Wine'),
    ('Fratelli', 'Fratelli Chenin Blanc', '46285', 'https://www.lcbo.com/en/fratelli-chenin-blanc-46285', '$25.95', 'Wine'),
    ('Fratelli', 'Fratelli Cabernet Sauvignon', '46287', 'https://www.lcbo.com/en/fratelli-cabernet-sauvignon-46287', '$28.95', 'Wine'),
    # Rutland Square (Scotland) — pending LCBO listing
    ('Rutland Square', 'Rutland Square Chai Spiced Gin', '', 'https://rutlandsquare.com/products/chai-spiced-scottish-gin', '', 'Spirits'),
]

# Brand-level grouping for gap/opportunity reports
ANU_BRANDS = {'NB Distillers', 'Goenchi', 'Fratelli', 'Rutland Square', 'Anu Portfolio'}

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
                lng REAL DEFAULT 0
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
            ('stores', 'lng', 'REAL DEFAULT 0'),
            ('activities', 'producer', 'TEXT DEFAULT \'\''), ('activities', 'venue_type', 'TEXT DEFAULT \'\''),
            ('activities', 'follow_up_date', 'TEXT DEFAULT \'\''),
        ]
        migrate_cols.extend([
            ('products', 'listing_status', "INTEGER DEFAULT 2"),
            ('products', 'listing_date', "TEXT DEFAULT ''"),
            ('products', 'delisting_date', "TEXT DEFAULT ''"),
            ('activities', 'status_code', "INTEGER DEFAULT 0"),
            ('stores', 'lcbo_store_id', "TEXT DEFAULT ''"),
        ])
        for table, col, coltype in migrate_cols:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            except Exception:
                pass  # Column already exists, safe to ignore with autocommit=True

        # Create weekly_reports table for persistent report storage
        cur.execute('''
            CREATE TABLE IF NOT EXISTS weekly_reports (
                id SERIAL PRIMARY KEY,
                week_start DATE NOT NULL,
                week_end DATE NOT NULL,
                report_data JSONB,
                generated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(week_start)
            )
        ''')
        # Create inventory_history table for tracking stock changes over time
        cur.execute('''
            CREATE TABLE IF NOT EXISTS inventory_history (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id),
                store_number TEXT,
                store_name TEXT,
                store_city TEXT,
                quantity INTEGER DEFAULT 0,
                recorded_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_inv_history_product ON inventory_history(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_inv_history_date ON inventory_history(recorded_at)",
            "CREATE INDEX IF NOT EXISTS idx_weekly_reports_week ON weekly_reports(week_start)",
        ]:
            try:
                cur.execute(idx)
            except Exception:
                pass

        # ======== SOD (Sale of Data) tables — daily inventory feed ========
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sod_sync_runs (
                id SERIAL PRIMARY KEY,
                run_at TIMESTAMP DEFAULT NOW(),
                source TEXT NOT NULL,
                file_name TEXT,
                snapshot_date DATE,
                status TEXT DEFAULT 'running',
                total_rows INTEGER DEFAULT 0,
                anu_rows INTEGER DEFAULT 0,
                new_listings INTEGER DEFAULT 0,
                new_delistings INTEGER DEFAULT 0,
                error TEXT,
                duration_seconds REAL DEFAULT 0
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sod_inventory (
                id BIGSERIAL PRIMARY KEY,
                sku TEXT NOT NULL,
                store_number INTEGER NOT NULL,
                snapshot_date DATE NOT NULL,
                status TEXT,
                on_hand INTEGER DEFAULT 0,
                product_name TEXT DEFAULT '',
                source TEXT DEFAULT 'daily_a',
                ingested_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(sku, store_number, snapshot_date)
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sod_products (
                sku TEXT PRIMARY KEY,
                product_name TEXT DEFAULT '',
                first_seen DATE,
                last_seen DATE,
                current_status TEXT DEFAULT 'L',
                store_count INTEGER DEFAULT 0,
                total_on_hand INTEGER DEFAULT 0,
                is_tracked BOOLEAN DEFAULT FALSE,
                brand TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sod_listing_changes (
                id BIGSERIAL PRIMARY KEY,
                sku TEXT NOT NULL,
                store_number INTEGER,
                change_date DATE NOT NULL,
                old_status TEXT,
                new_status TEXT,
                change_type TEXT,
                detected_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_sod_inv_sku ON sod_inventory(sku)",
            "CREATE INDEX IF NOT EXISTS idx_sod_inv_date ON sod_inventory(snapshot_date)",
            "CREATE INDEX IF NOT EXISTS idx_sod_inv_sku_date ON sod_inventory(sku, snapshot_date)",
            "CREATE INDEX IF NOT EXISTS idx_sod_inv_store ON sod_inventory(store_number)",
            "CREATE INDEX IF NOT EXISTS idx_sod_runs_at ON sod_sync_runs(run_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_sod_changes_sku ON sod_listing_changes(sku)",
            "CREATE INDEX IF NOT EXISTS idx_sod_changes_date ON sod_listing_changes(change_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_sod_products_tracked ON sod_products(is_tracked)",
        ]:
            try:
                cur.execute(idx)
            except Exception:
                pass

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
                producer TEXT DEFAULT '', lat REAL DEFAULT 0, lng REAL DEFAULT 0
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
            CREATE TABLE IF NOT EXISTS weekly_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT NOT NULL UNIQUE,
                week_end TEXT NOT NULL,
                report_data TEXT,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS inventory_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                store_number TEXT,
                store_name TEXT,
                store_city TEXT,
                quantity INTEGER DEFAULT 0,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            );
            CREATE INDEX IF NOT EXISTS idx_inv_history_product ON inventory_history(product_id);
            CREATE INDEX IF NOT EXISTS idx_inv_history_date ON inventory_history(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_weekly_reports_week ON weekly_reports(week_start);

            -- ======== SOD (Sale of Data) tables ========
            CREATE TABLE IF NOT EXISTS sod_sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT NOT NULL,
                file_name TEXT,
                snapshot_date TEXT,
                status TEXT DEFAULT 'running',
                total_rows INTEGER DEFAULT 0,
                anu_rows INTEGER DEFAULT 0,
                new_listings INTEGER DEFAULT 0,
                new_delistings INTEGER DEFAULT 0,
                error TEXT,
                duration_seconds REAL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS sod_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                store_number INTEGER NOT NULL,
                snapshot_date TEXT NOT NULL,
                status TEXT,
                on_hand INTEGER DEFAULT 0,
                product_name TEXT DEFAULT '',
                source TEXT DEFAULT 'daily_a',
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sku, store_number, snapshot_date)
            );
            CREATE TABLE IF NOT EXISTS sod_products (
                sku TEXT PRIMARY KEY,
                product_name TEXT DEFAULT '',
                first_seen TEXT,
                last_seen TEXT,
                current_status TEXT DEFAULT 'L',
                store_count INTEGER DEFAULT 0,
                total_on_hand INTEGER DEFAULT 0,
                is_tracked INTEGER DEFAULT 0,
                brand TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sod_listing_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                store_number INTEGER,
                change_date TEXT NOT NULL,
                old_status TEXT,
                new_status TEXT,
                change_type TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_sod_inv_sku ON sod_inventory(sku);
            CREATE INDEX IF NOT EXISTS idx_sod_inv_date ON sod_inventory(snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_sod_inv_sku_date ON sod_inventory(sku, snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_sod_inv_store ON sod_inventory(store_number);
            CREATE INDEX IF NOT EXISTS idx_sod_runs_at ON sod_sync_runs(run_at DESC);
            CREATE INDEX IF NOT EXISTS idx_sod_changes_sku ON sod_listing_changes(sku);
            CREATE INDEX IF NOT EXISTS idx_sod_changes_date ON sod_listing_changes(change_date DESC);
            CREATE INDEX IF NOT EXISTS idx_sod_products_tracked ON sod_products(is_tracked);
        ''')
        migrate_cols = [
            ('stores', 'manager_name', "TEXT DEFAULT ''"), ('stores', 'asst_manager_name', "TEXT DEFAULT ''"),
            ('stores', 'manager_phone', "TEXT DEFAULT ''"), ('stores', 'store_email', "TEXT DEFAULT ''"),
            ('stores', 'producer', "TEXT DEFAULT ''"), ('stores', 'lat', "REAL DEFAULT 0"),
            ('stores', 'lng', "REAL DEFAULT 0"),
            ('stores', 'lcbo_store_id', "TEXT DEFAULT ''"),
            ('activities', 'producer', "TEXT DEFAULT ''"), ('activities', 'venue_type', "TEXT DEFAULT ''"),
            ('activities', 'follow_up_date', "TEXT DEFAULT ''"),
            ('activities', 'status_code', "INTEGER DEFAULT 0"),
            ('products', 'listing_status', "INTEGER DEFAULT 2"),
            ('products', 'listing_date', "TEXT DEFAULT ''"),
            ('products', 'delisting_date', "TEXT DEFAULT ''"),
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
        'overdue_followups': overdue,
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


LCBO_GRAPHQL = 'https://api.lcbo.dev/graphql'
LCBO_STORE_INVENTORY_URL = 'https://www.lcbo.com/en/storeinventory/'
LCBO_PRODUCT_URL = 'https://www.lcbo.com/en/product/'
INVENTORY_QUERY = """
query GetProductInventory($sku: String!) {
  product(sku: $sku) {
    sku name priceInCents producerName isBuyable updatedAt
    alcoholPercent unitVolumeMl sellRankMonthly sellRankYearly
    inventories {
      totalCount
      edges {
        node {
          quantity updatedAt
          store {
            externalId name city address latitude longitude
          }
        }
      }
    }
  }
}
"""

_STORELIST_RE = re.compile(r'"storeList"\s*:\s*(\[\[.*?\]\])\s*[,}]', re.DOTALL)
_PRICE_RE = re.compile(r'"price"\s*:\s*"?([\d.]+)"?')
_BUYABLE_RE = re.compile(r'"is[_ ]?buyable"\s*:\s*(true|false)', re.IGNORECASE)


def scrape_lcbo_inventory(sku):
    """Scrape LIVE store-level inventory from LCBO.com storeinventory page.
    Returns list of dicts with store_number, city, intersection, address, phone, qty.
    Works for EVERY listed SKU on LCBO.com (including products missing from lcbo.dev)."""
    if not http_requests or not sku:
        return [], 'no http/sku'
    try:
        resp = http_requests.get(
            f'{LCBO_STORE_INVENTORY_URL}?sku={sku}',
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-CA,en;q=0.9',
            },
            timeout=25,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return [], f'http {resp.status_code}'
        html = resp.text
        m = _STORELIST_RE.search(html)
        if not m:
            return [], 'storeList not found — product may be delisted or not stocked'
        raw = m.group(1)
        try:
            rows = json.loads(raw)
        except Exception:
            # storeList is in [["a","b",...],...] shape — should parse as JSON
            return [], 'storeList parse error'
        stores = []
        # first row is header
        for row in rows[1:]:
            if not isinstance(row, list) or len(row) < 7:
                continue
            city, intersection, addr1, addr2, phone, store_num, qty = row[:7]
            try:
                qty_int = int(qty)
            except (TypeError, ValueError):
                qty_int = 0
            stores.append({
                'store_number': str(store_num).strip(),
                'city': (city or '').strip(),
                'intersection': (intersection or '').strip(),
                'address': (addr1 or '').strip() + ((' ' + addr2) if addr2 else ''),
                'phone': (phone or '').strip(),
                'quantity': qty_int,
                'store_name': f"{(city or '').strip()} — {(intersection or '').strip()}".strip(' —'),
            })
        return stores, None
    except Exception as e:
        return [], f'scrape error: {e}'


def fetch_lcbo_graphql_inventory(sku):
    """Fetch live inventory + metadata from LCBO.dev GraphQL — used for price/listing-status enrichment.
    Does NOT cover all SKUs (e.g. 20187 Red Admiral is missing) so always combine with scrape_lcbo_inventory."""
    if not http_requests or not sku:
        return None, 'no http/sku'
    try:
        resp = http_requests.post(
            LCBO_GRAPHQL,
            json={'query': INVENTORY_QUERY, 'variables': {'sku': str(sku)}},
            headers={'Content-Type': 'application/json', 'User-Agent': 'AnuSpirits-CRM/2.0'},
            timeout=20
        )
        if resp.status_code != 200:
            return None, f'API returned {resp.status_code}'
        data = resp.json()
        if 'errors' in data:
            return None, str(data['errors'])
        product_data = data.get('data', {}).get('product')
        return product_data, None
    except Exception as e:
        return None, str(e)


def live_inventory_for_sku(sku):
    """Combine LCBO.com scrape (comprehensive) with lcbo.dev GraphQL (metadata).
    Returns (stores_list, meta_dict) where meta has price_cents, is_buyable, updated_at etc."""
    scraped, scrape_err = scrape_lcbo_inventory(sku)
    meta = {
        'source': 'lcbo.com',
        'scrape_error': scrape_err,
        'price_cents': None,
        'is_buyable': None,
        'updated_at': None,
        'sell_rank_yearly': None,
        'alcohol_percent': None,
        'unit_volume_ml': None,
    }
    gql, _gql_err = fetch_lcbo_graphql_inventory(sku)
    if gql:
        meta['price_cents'] = gql.get('priceInCents')
        meta['is_buyable'] = gql.get('isBuyable')
        meta['updated_at'] = gql.get('updatedAt')
        meta['sell_rank_yearly'] = gql.get('sellRankYearly')
        meta['alcohol_percent'] = gql.get('alcoholPercent')
        meta['unit_volume_ml'] = gql.get('unitVolumeMl')
    # Listing status heuristic:
    # - scraped has stores + is_buyable true  => Active (2)
    # - scraped has stores + is_buyable false => Delisting (3)
    # - no scraped stores + is_buyable false  => Delisted/warehouse-only (4-5)
    # - no scraped stores + gql returns null  => Not in LCBO.dev index (but may be on LCBO.com)
    if scraped:
        if meta['is_buyable'] is False:
            meta['listing_status'] = 3  # to be delisted
        else:
            meta['listing_status'] = 2  # active
    else:
        if meta['is_buyable'] is False:
            meta['listing_status'] = 5  # fully delisted
        else:
            meta['listing_status'] = 4  # warehouse only / no retail stock
    return scraped, meta


def _persist_live_inventory(product_id, sku, stores, meta):
    """Write scraped inventory to inventory_cache + inventory_history, update product listing status."""
    # Replace inventory cache for this product
    db_execute("DELETE FROM inventory_cache WHERE product_id=?", [product_id])
    for s in stores:
        db_execute(
            "INSERT INTO inventory_cache (product_id, store_number, store_name, store_city, quantity) VALUES (?,?,?,?,?)",
            [product_id, s['store_number'], s['store_name'], s['city'], s['quantity']]
        )
    # Append aggregate snapshot row to inventory_history for trend reporting
    try:
        db_execute(
            "INSERT INTO inventory_history (product_id, store_number, store_name, store_city, quantity) VALUES (?,?,?,?,?)",
            [product_id, 'SUMMARY', f'{len(stores)} stores', 'TOTAL', sum(s['quantity'] for s in stores)]
        )
    except Exception:
        pass
    # Update price and listing status
    if meta.get('price_cents'):
        db_execute("UPDATE products SET price=? WHERE id=?", [f"${meta['price_cents']/100:.2f}", product_id])
    if meta.get('listing_status'):
        db_execute("UPDATE products SET listing_status=? WHERE id=?", [meta['listing_status'], product_id])
    db_commit()


@app.route('/api/inventory/check/<sku>')
def api_inventory_check(sku):
    """LIVE inventory check — scrapes LCBO.com storeList + enriches via lcbo.dev.
    Works for ALL listed SKUs. Persists to inventory_cache and inventory_history."""
    product = db_fetchone("SELECT * FROM products WHERE lcbo_sku=?", [sku])
    if not product:
        return jsonify({'error': 'Product not found in CRM', 'stores': []})
    product = dict(product)
    for k, v in product.items():
        if isinstance(v, datetime):
            product[k] = v.isoformat()

    stores, meta = live_inventory_for_sku(sku)
    if stores:
        _persist_live_inventory(product['id'], sku, stores, meta)
        product['listing_status'] = meta.get('listing_status', 2)
        if meta.get('price_cents'):
            product['price'] = f"${meta['price_cents']/100:.2f}"
        return jsonify({
            'product': product,
            'stores': stores,
            'total_units': sum(s['quantity'] for s in stores),
            'store_count': len(stores),
            'meta': meta,
            'checked_at': datetime.now().isoformat(),
            'source': 'lcbo.com (live scrape) + lcbo.dev enrichment'
        })
    else:
        # Fall back to cached data
        cached = db_fetchall("SELECT * FROM inventory_cache WHERE product_id=? ORDER BY store_city", [product['id']])
        return jsonify({
            'product': product,
            'stores': [dict(c) for c in cached],
            'total_units': sum(c['quantity'] for c in cached),
            'store_count': len(cached),
            'meta': meta,
            'checked_at': None,
            'source': 'cache',
            'error': meta.get('scrape_error') or 'No live stock available'
        })


@app.route('/api/inventory/refresh-all', methods=['POST'])
def api_inventory_refresh_all():
    """Refresh inventories for ALL tracked products from live LCBO.com — run daily via cron."""
    if not http_requests:
        return jsonify({'error': 'requests library not available'})
    products = db_fetchall("SELECT * FROM products WHERE lcbo_sku != ''")
    results = []
    total_refreshed = 0
    for p in products:
        p = dict(p)
        sku = p.get('lcbo_sku', '')
        if not sku:
            continue
        stores, meta = live_inventory_for_sku(sku)
        if stores:
            _persist_live_inventory(p['id'], sku, stores, meta)
            total_refreshed += 1
            results.append({
                'sku': sku, 'name': p['name'],
                'stores': len(stores),
                'total_units': sum(s['quantity'] for s in stores),
                'price': (f"${meta['price_cents']/100:.2f}" if meta.get('price_cents') else p.get('price', '')),
                'listing_status': meta.get('listing_status'),
                'status': 'refreshed'
            })
        else:
            results.append({'sku': sku, 'name': p['name'], 'stores': 0, 'status': 'no_stock_or_delisted', 'error': meta.get('scrape_error')})
    return jsonify({
        'refreshed': total_refreshed,
        'total_tracked': len(results),
        'products': results,
        'timestamp': datetime.now().isoformat(),
        'source': 'lcbo.com live + lcbo.dev'
    })


@app.route('/api/inventory/gap-report')
def api_gap_report():
    """GAP REPORT: for each tracked product, list LCBO stores NOT carrying it.
    These are the rep's top listing-opportunity stores."""
    sku_filter = request.args.get('sku', '').strip()
    city_filter = request.args.get('city', '').strip()

    # Get all CRM stores
    all_stores = db_fetchall("SELECT id, store_number, account, city, address, postal, manager_name, phone, priority, lat, lng FROM stores")
    all_stores = [dict(s) for s in all_stores]
    stores_by_num = {str(s['store_number']): s for s in all_stores}
    total_crm_stores = len(all_stores)

    # Products to scan
    if sku_filter:
        products = db_fetchall("SELECT * FROM products WHERE lcbo_sku=?", [sku_filter])
    else:
        products = db_fetchall("SELECT * FROM products WHERE lcbo_sku != ''")

    gap_results = []
    for p in products:
        p = dict(p)
        sku = p.get('lcbo_sku', '')
        if not sku:
            continue
        # Get stores carrying this product (from cache, most recent refresh)
        carrying = db_fetchall(
            "SELECT store_number, quantity FROM inventory_cache WHERE product_id=?",
            [p['id']]
        )
        carrying_nums = {str(c['store_number']): int(c['quantity'] or 0) for c in carrying}

        # Gap stores = CRM stores NOT in carrying
        gap_stores = []
        for s in all_stores:
            num = str(s['store_number'])
            if num not in carrying_nums:
                if not city_filter or city_filter.lower() in (s.get('city') or '').lower():
                    gap_stores.append({
                        **s,
                        'full_address': f"{s.get('address','')}, {s.get('city','')}, ON {s.get('postal','')}".strip(', '),
                    })
        gap_results.append({
            'product': {'id': p['id'], 'name': p['name'], 'sku': sku, 'brand': p.get('brand'), 'price': p.get('price')},
            'carrying_count': len(carrying_nums),
            'gap_count': len(gap_stores),
            'gap_rate_pct': round(100.0 * len(gap_stores) / max(total_crm_stores, 1), 1),
            'gap_stores': gap_stores[:500],  # cap for payload size
        })

    return jsonify({
        'generated_at': datetime.now().isoformat(),
        'total_crm_stores': total_crm_stores,
        'products': gap_results,
    })


@app.route('/api/inventory/reorder-needed')
def api_reorder_needed():
    """REORDER NEEDED: stores with low stock (below threshold) on any tracked product.
    Query params: threshold (default 5), sku (optional), city (optional)."""
    threshold = int(request.args.get('threshold', 5))
    sku_filter = request.args.get('sku', '').strip()
    city_filter = request.args.get('city', '').strip()

    # Find low-stock inventory entries
    query = """
        SELECT ic.store_number, ic.store_name, ic.store_city, ic.quantity,
               p.id as product_id, p.lcbo_sku, p.name as product_name, p.brand, p.price,
               s.id as store_id, s.address, s.postal, s.manager_name, s.phone, s.lat, s.lng
        FROM inventory_cache ic
        JOIN products p ON ic.product_id = p.id
        LEFT JOIN stores s ON CAST(s.store_number AS TEXT) = ic.store_number
        WHERE ic.quantity < ?
    """
    params = [threshold]
    if sku_filter:
        query += " AND p.lcbo_sku=?"
        params.append(sku_filter)
    if city_filter:
        query += " AND LOWER(ic.store_city) LIKE ?"
        params.append(f"%{city_filter.lower()}%")
    query += " ORDER BY ic.quantity ASC, ic.store_city"

    rows = db_fetchall(query, params)
    results = []
    for r in rows:
        r = dict(r)
        results.append({
            'store_number': r.get('store_number'),
            'store_name': r.get('store_name'),
            'city': r.get('store_city'),
            'address': r.get('address'),
            'phone': r.get('phone'),
            'manager': r.get('manager_name'),
            'quantity': int(r.get('quantity') or 0),
            'product': {'id': r.get('product_id'), 'sku': r.get('lcbo_sku'), 'name': r.get('product_name'), 'brand': r.get('brand'), 'price': r.get('price')},
            'urgency': 'critical' if (r.get('quantity') or 0) == 0 else ('high' if (r.get('quantity') or 0) <= 2 else 'medium'),
            'in_crm': r.get('store_id') is not None,
        })

    return jsonify({
        'threshold': threshold,
        'total_reorder_alerts': len(results),
        'critical_count': sum(1 for x in results if x['urgency'] == 'critical'),
        'high_count': sum(1 for x in results if x['urgency'] == 'high'),
        'medium_count': sum(1 for x in results if x['urgency'] == 'medium'),
        'alerts': results,
        'generated_at': datetime.now().isoformat(),
    })


@app.route('/api/inventory/listing-status')
def api_listing_status():
    """LISTING STATUS report: current live status + total distribution for every tracked product."""
    products = db_fetchall("SELECT * FROM products WHERE lcbo_sku != '' ORDER BY brand, name")
    status_map = {
        1: 'New Listing',
        2: 'Active',
        3: 'Delisting (to be removed)',
        4: 'Warehouse Only',
        5: 'Fully Delisted',
    }
    out = []
    for p in products:
        p = dict(p)
        cache = db_fetchone(
            "SELECT COUNT(*) as store_count, COALESCE(SUM(quantity),0) as total_units FROM inventory_cache WHERE product_id=?",
            [p['id']]
        )
        cache = dict(cache) if cache else {'store_count': 0, 'total_units': 0}
        # Last 14 aggregate snapshots (summary rows only)
        history = db_fetchall(
            "SELECT recorded_at, store_name, quantity as total_units FROM inventory_history WHERE product_id=? AND store_number='SUMMARY' ORDER BY recorded_at DESC LIMIT 14",
            [p['id']]
        )
        history = []
        for h in db_fetchall(
            "SELECT recorded_at, store_name, quantity FROM inventory_history WHERE product_id=? AND store_number=? ORDER BY recorded_at DESC LIMIT 14",
            [p['id'], 'SUMMARY']
        ):
            h = dict(h)
            rec = h.get('recorded_at')
            if isinstance(rec, datetime):
                rec = rec.isoformat()
            history.append({
                'date': rec,
                'store_count_text': h.get('store_name'),
                'total_units': int(h.get('quantity') or 0),
            })
        status_code = int(p.get('listing_status') or 2)
        out.append({
            'sku': p.get('lcbo_sku'),
            'name': p.get('name'),
            'brand': p.get('brand'),
            'price': p.get('price'),
            'listing_status_code': status_code,
            'listing_status_label': status_map.get(status_code, 'Unknown'),
            'store_count': int(cache.get('store_count') or 0),
            'total_units': int(cache.get('total_units') or 0),
            'trend_14d': history,
            'lcbo_url': f"https://www.lcbo.com/en/product/{p.get('lcbo_sku')}" if p.get('lcbo_sku') else None,
        })
    return jsonify({'products': out, 'generated_at': datetime.now().isoformat()})


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


# =================================================================
# ============= LCBO SALE-OF-DATA (SOD) INTEGRATION ===============
# =================================================================
# Source:   https://sod.lcbo.com  (authenticated subscriber portal)
# Options:  12 = Daily Inventory A (all SKUs, ~75 MB/day, every store)
#           13 = Daily Inventory B (agent-specific, smaller)
# Format:   Fixed-width .dat, 47 chars per row, latin-1 encoding
#   [0:8]   date YYYYMMDD
#   [8:15]  SKU (7 digits, zero-padded)
#   [15:32] product name (17 chars, space-padded)
#   [32:36] store number (4 digits)
#   [36:37] status (L=listed / D=to-be-delisted / F=fully-delisted)
#   [37:38] qty sign (space=+, '-'=negative)
#   [38:47] qty (9 digits)
# Daily rotation: files named by weekday (MON/TUE/WED/...); overwritten each week.
# =================================================================

import threading
import zipfile
import tempfile
import traceback
from urllib.parse import urljoin

SOD_BASE = 'https://sod.lcbo.com'
SOD_USER = os.environ.get('SOD_USER', '').strip()
SOD_PASSWORD = os.environ.get('SOD_PASSWORD', '').strip()
SOD_AGENT_ID = os.environ.get('SOD_AGENT_ID', '1113').strip()  # default: VINETER/XTVTR

# ------- SKU → brand mapping (only Anu/NB Distillers products tracked in reports) -------
# Keys are 7-char zero-padded SKUs (matches what SOD emits)
SOD_TRACKED_SKUS = {
    # NB Distillers (Anu-owned)
    '0020187': ('NB Distillers', 'Red Admiral Vodka'),
    '0022246': ('NB Distillers', 'Chak De Canadian Whisky'),
    # Goenchi (Anu portfolio)
    '0046340': ('Goenchi', 'Goenchi Cashew Feni'),
    '0046343': ('Goenchi', 'Goenchi Coconut Feni'),
    # Fratelli (Anu portfolio)
    '0046282': ('Fratelli', 'Fratelli Classic Shiraz'),
    '0046285': ('Fratelli', 'Fratelli Chenin Blanc'),
    '0046286': ('Fratelli', 'Fratelli Sauvignon Blanc'),
    '0046287': ('Fratelli', 'Fratelli Cabernet Sauvignon'),
}

_SOD_CSRF_RE = re.compile(
    r'<input[^>]*name="csrf_token"[^>]*value="([^"]+)"', re.IGNORECASE
)


class SODClient:
    """Authenticated client for https://sod.lcbo.com/."""

    def __init__(self, user=None, password=None, agent_id=None, timeout=60):
        self.user = user or SOD_USER
        self.password = password or SOD_PASSWORD
        self.agent_id = agent_id or SOD_AGENT_ID
        self.timeout = timeout
        if http_requests is None:
            raise RuntimeError("'requests' library not available")
        self.session = http_requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36',
        })
        self._logged_in = False

    def _extract_csrf(self, html):
        m = _SOD_CSRF_RE.search(html)
        return m.group(1) if m else None

    def login(self):
        if not self.user or not self.password:
            raise RuntimeError("SOD credentials not configured (SOD_USER / SOD_PASSWORD env vars)")
        # 1) GET sign-in page to obtain CSRF token
        r = self.session.get(f'{SOD_BASE}/user/sign-in', timeout=self.timeout)
        r.raise_for_status()
        csrf = self._extract_csrf(r.text)
        if not csrf:
            raise RuntimeError("Could not extract csrf_token from /user/sign-in")
        # 2) POST credentials
        r = self.session.post(
            f'{SOD_BASE}/user/sign-in',
            data={
                'csrf_token': csrf,
                'next': '/',
                'reg_next': '/',
                'username': self.user,
                'password': self.password,
                'remember_me': 'y',
            },
            headers={'Referer': f'{SOD_BASE}/user/sign-in'},
            allow_redirects=False,
            timeout=self.timeout,
        )
        if r.status_code not in (302, 303):
            raise RuntimeError(f"SOD login failed (HTTP {r.status_code}) — check credentials")
        if 'remember_token' not in self.session.cookies.get_dict():
            # Session cookie alone might suffice; verify by fetching subscribers page
            test = self.session.get(f'{SOD_BASE}/subscribers', timeout=self.timeout)
            if 'Sign out' not in test.text:
                raise RuntimeError("SOD login failed: no remember_token and /subscribers missing Sign-out link")
        self._logged_in = True
        return True

    def _ensure_logged_in(self):
        if not self._logged_in:
            self.login()

    def latest_filename(self, source):
        """Infer today's filename from weekday.

        LCBO uploads nightly (~02:00 ET). If the current time is before ~02:30 ET,
        today's file may not yet be present and we fall back to yesterday's.
        """
        import calendar
        # Use today's weekday; caller can fall back to yesterday on 404
        today = datetime.utcnow()
        # SOD timezone is America/Toronto; naive handling: use UTC day-of-week
        wd = today.strftime('%a').upper()  # 'TUE'
        if source == 'daily_a':
            return f'alldlyinventory{wd}.zip'
        elif source == 'daily_b':
            return f'Edlyinventory{self.agent_id}{wd}.zip'
        raise ValueError(f'Unknown source {source!r}')

    def download_option(self, source, filename=None):
        """Download a specific SOD file. Returns (bytes, filename)."""
        self._ensure_logged_in()
        fn = filename or self.latest_filename(source)
        if source == 'daily_a':
            url = f'{SOD_BASE}/downloads/general/12/{fn}'
        elif source == 'daily_b':
            url = f'{SOD_BASE}/downloads/agent/{self.agent_id}/13/{fn}'
        else:
            raise ValueError(f'Unknown source {source!r}')
        r = self.session.get(url, timeout=self.timeout, stream=True)
        if r.status_code == 404:
            # Try yesterday's file as fallback
            yd = (datetime.utcnow() - timedelta(days=1)).strftime('%a').upper()
            if source == 'daily_a':
                fn = f'alldlyinventory{yd}.zip'
                url = f'{SOD_BASE}/downloads/general/12/{fn}'
            else:
                fn = f'Edlyinventory{self.agent_id}{yd}.zip'
                url = f'{SOD_BASE}/downloads/agent/{self.agent_id}/13/{fn}'
            r = self.session.get(url, timeout=self.timeout, stream=True)
        r.raise_for_status()
        content = r.content
        if not content or not content.startswith(b'PK'):
            raise RuntimeError(f"Did not receive zip data from {url} (content-type={r.headers.get('Content-Type')})")
        return content, fn

    def download_zip_bytes(self, source, filename=None):
        """Download and return the raw zip bytes + final filename.

        Kept small in memory (~9MB for Daily A). The .dat inside is ~75MB
        uncompressed — we NEVER materialize that blob; callers must stream
        the member via open_dat_stream() instead.
        """
        data, fn = self.download_option(source, filename=filename)
        return data, fn


def _parse_sod_line(line):
    """Parse one SOD fixed-width row string. Returns dict or None if invalid."""
    if len(line) < 47:
        return None
    try:
        date_raw = line[0:8]
        sku = line[8:15]
        name = line[15:32].strip()
        store = line[32:36]
        status = line[36:37].strip() or 'L'
        sign = line[37:38]
        qty_digits = line[38:47].strip()
        if not date_raw.isdigit() or not sku.isdigit() or not store.isdigit():
            return None
        qty = int(qty_digits) if qty_digits.isdigit() else 0
        if sign == '-':
            qty = -qty
        snapshot_date = f'{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}'
        return {
            'snapshot_date': snapshot_date,
            'sku': sku,
            'product_name': name,
            'store_number': int(store),
            'status': status,
            'on_hand': qty,
        }
    except (ValueError, IndexError):
        return None


def parse_sod_dat(raw_bytes):
    """Backward-compatible generator. Prefer stream_parse_sod_zip for large files.

    Each row is 47 chars (plus newline), latin-1 encoded. See format notes at top.
    """
    text = raw_bytes.decode('latin-1', errors='replace')
    for line in text.splitlines():
        row = _parse_sod_line(line)
        if row is not None:
            yield row


def stream_parse_sod_zip(zip_bytes, tracked_skus, keep_all_rows=False):
    """Streaming parser + aggregator for a SOD .zip download.

    Iterates the .dat member line-by-line via zipfile.open() + TextIOWrapper
    so the 75MB .dat text is NEVER held in RAM. Retains only:
      - per-sku aggregates keyed by (snapshot_date, sku)  (small: ~700 SKUs)
      - rows for tracked SKUs (~155 rows for Daily A), or all rows when
        keep_all_rows is True (~1,400 rows for Daily B)

    Returns dict with: dat_name, total, per_sku_by_date, rows_to_persist,
    dates_seen, tracked_row_count.
    """
    per_sku_by_date = {}   # {date: {sku: {'name', 'status_counts', 'store_count', 'total_on_hand'}}}
    rows_to_persist = []   # only tracked rows (or all, for Daily B)
    dates_seen = set()
    total = 0
    tracked_row_count = 0

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        members = zf.namelist()
        if not members:
            raise RuntimeError("Zip is empty")
        dat_name = members[0]
        with zf.open(dat_name) as raw_stream:
            text_stream = io.TextIOWrapper(raw_stream, encoding='latin-1', errors='replace', newline='')
            for line in text_stream:
                # TextIOWrapper keeps the newline; strip trailing CR/LF only
                if line.endswith('\n'):
                    line = line[:-1]
                if line.endswith('\r'):
                    line = line[:-1]
                row = _parse_sod_line(line)
                if row is None:
                    continue
                total += 1
                d = row['snapshot_date']
                dates_seen.add(d)
                date_bucket = per_sku_by_date.setdefault(d, {})
                agg = date_bucket.get(row['sku'])
                if agg is None:
                    agg = {
                        'name': row['product_name'],
                        'status_counts': {},
                        'store_count': 0,
                        'total_on_hand': 0,
                    }
                    date_bucket[row['sku']] = agg
                agg['status_counts'][row['status']] = agg['status_counts'].get(row['status'], 0) + 1
                agg['store_count'] += 1
                agg['total_on_hand'] += row['on_hand']

                is_tracked = row['sku'] in tracked_skus
                if is_tracked:
                    tracked_row_count += 1
                if keep_all_rows or is_tracked:
                    rows_to_persist.append(row)

    return {
        'dat_name': dat_name,
        'total': total,
        'per_sku_by_date': per_sku_by_date,
        'rows_to_persist': rows_to_persist,
        'dates_seen': dates_seen,
        'tracked_row_count': tracked_row_count,
    }


# ------- DB helpers scoped to the sync pipeline (use a dedicated connection) -------
def _sod_get_conn():
    """Dedicated connection for the sync (not the request-scoped `g.db`).

    Syncs run in background threads and from schedulers, so must not reuse Flask's g.
    """
    if USE_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    else:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn


def _sod_ph():
    """Return the placeholder token for the current DB."""
    return '%s' if USE_POSTGRES else '?'


def run_sod_sync(source='daily_a', filename=None, client=None):
    """Download + parse + ingest one SOD file. Idempotent per (sku, store, date).

    Returns a dict summary of the run.
    """
    start = datetime.utcnow()
    ph = _sod_ph()
    conn = _sod_get_conn()
    try:
        cur = conn.cursor()
        # Record the run as 'running' up front for observability
        if USE_POSTGRES:
            cur.execute(
                "INSERT INTO sod_sync_runs (source, status) VALUES (%s, 'running') RETURNING id",
                (source,),
            )
            run_id = cur.fetchone()[0]
        else:
            cur.execute(
                "INSERT INTO sod_sync_runs (source, status) VALUES (?, 'running')",
                (source,),
            )
            run_id = cur.lastrowid
        conn.commit()

        # 1) Download zip bytes (small — ~9MB for Daily A, ~8KB for Daily B)
        client = client or SODClient()
        zip_bytes, zip_name = client.download_zip_bytes(source, filename=filename)

        # 2) Stream-parse directly from the zip. NEVER materializes the 75MB .dat text
        #    or the 1.5M-row list. Only keeps small aggregates + tracked rows.
        keep_all = (source != 'daily_a')  # Daily B is already agent-filtered (~1,400 rows)
        parsed = stream_parse_sod_zip(zip_bytes, SOD_TRACKED_SKUS, keep_all_rows=keep_all)
        # Free the zip bytes ASAP
        del zip_bytes
        gc.collect()

        dat_name = parsed['dat_name']
        total = parsed['total']
        if not parsed['dates_seen']:
            raise RuntimeError("No rows parsed from .dat file")
        snapshot_date = max(parsed['dates_seen'])  # use the most recent date in the feed

        # 3) per_sku aggregates for the newest snapshot (already computed during streaming)
        per_sku = parsed['per_sku_by_date'].get(snapshot_date, {})
        # rows_to_persist holds tracked-SKU rows (Daily A) or all rows (Daily B) across
        # every date in the file — filter to the newest snapshot only.
        latest_rows = [r for r in parsed['rows_to_persist'] if r['snapshot_date'] == snapshot_date]
        anu_count = sum(1 for r in latest_rows if r['sku'] in SOD_TRACKED_SKUS)
        # Release the parsed buffers we no longer need
        parsed = None
        gc.collect()

        # 4) Pull prior sod_products state to compute listing changes
        cur.execute("SELECT sku, current_status FROM sod_products")
        prior = {row[0]: row[1] for row in cur.fetchall()}
        new_listings = 0
        new_delistings = 0
        change_inserts = []
        is_cold_start = len(prior) == 0
        for sku, agg in per_sku.items():
            # Majority status wins for product-level status
            status = max(agg['status_counts'].items(), key=lambda x: x[1])[0]
            old = prior.get(sku)
            # On cold start, only record NEW_LISTING events for tracked SKUs to avoid noise
            # (the full catalog doesn't belong in the change-log on first ingest).
            if old is None:
                if sku in SOD_TRACKED_SKUS and not is_cold_start:
                    change_inserts.append((sku, None, snapshot_date, None, status, 'NEW_LISTING'))
                    if status == 'L':
                        new_listings += 1
                elif sku in SOD_TRACKED_SKUS and is_cold_start:
                    # Record a BASELINE event so the timeline has a starting point
                    change_inserts.append((sku, None, snapshot_date, None, status, 'BASELINE'))
            elif old != status:
                # Status flips are always interesting, not only for tracked SKUs
                if status in ('D', 'F') and old == 'L':
                    change_inserts.append((sku, None, snapshot_date, old, status, 'DELISTED'))
                    if sku in SOD_TRACKED_SKUS:
                        new_delistings += 1
                elif status == 'L' and old in ('D', 'F'):
                    change_inserts.append((sku, None, snapshot_date, old, status, 'RELISTED'))
                    if sku in SOD_TRACKED_SKUS:
                        new_listings += 1
                else:
                    change_inserts.append((sku, None, snapshot_date, old, status, 'STATUS_FLIP'))

        # 5) Upsert sod_inventory
        # latest_rows is already filtered correctly by the streaming parser:
        #   - Daily A: only tracked-SKU rows (~155)
        #   - Daily B: all rows (~1,400, already agent-filtered server-side)
        rows_to_persist = latest_rows

        if rows_to_persist:
            if USE_POSTGRES:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO sod_inventory
                        (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                    VALUES %s
                    ON CONFLICT (sku, store_number, snapshot_date) DO UPDATE SET
                        status = EXCLUDED.status,
                        on_hand = EXCLUDED.on_hand,
                        product_name = EXCLUDED.product_name,
                        source = EXCLUDED.source,
                        ingested_at = NOW()
                    """,
                    [(r['sku'], r['store_number'], r['snapshot_date'], r['status'],
                      r['on_hand'], r['product_name'], source) for r in rows_to_persist],
                    page_size=1000,
                )
            else:
                cur.executemany(
                    """INSERT INTO sod_inventory
                       (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                       VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT(sku, store_number, snapshot_date) DO UPDATE SET
                         status=excluded.status, on_hand=excluded.on_hand,
                         product_name=excluded.product_name, source=excluded.source,
                         ingested_at=CURRENT_TIMESTAMP""",
                    [(r['sku'], r['store_number'], r['snapshot_date'], r['status'],
                      r['on_hand'], r['product_name'], source) for r in rows_to_persist],
                )

        # 6) Upsert sod_products rollup
        for sku, agg in per_sku.items():
            brand, display_name = SOD_TRACKED_SKUS.get(sku, ('', agg['name']))
            is_tracked = sku in SOD_TRACKED_SKUS
            status = max(agg['status_counts'].items(), key=lambda x: x[1])[0]
            if USE_POSTGRES:
                cur.execute(
                    """INSERT INTO sod_products
                        (sku, product_name, first_seen, last_seen, current_status,
                         store_count, total_on_hand, is_tracked, brand, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (sku) DO UPDATE SET
                         product_name = EXCLUDED.product_name,
                         last_seen = EXCLUDED.last_seen,
                         current_status = EXCLUDED.current_status,
                         store_count = EXCLUDED.store_count,
                         total_on_hand = EXCLUDED.total_on_hand,
                         is_tracked = EXCLUDED.is_tracked,
                         brand = EXCLUDED.brand,
                         updated_at = NOW()""",
                    (sku, display_name or agg['name'], snapshot_date, snapshot_date, status,
                     agg['store_count'], agg['total_on_hand'], is_tracked, brand),
                )
            else:
                cur.execute(
                    """INSERT INTO sod_products
                        (sku, product_name, first_seen, last_seen, current_status,
                         store_count, total_on_hand, is_tracked, brand, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP)
                       ON CONFLICT(sku) DO UPDATE SET
                         product_name=excluded.product_name,
                         last_seen=excluded.last_seen,
                         current_status=excluded.current_status,
                         store_count=excluded.store_count,
                         total_on_hand=excluded.total_on_hand,
                         is_tracked=excluded.is_tracked,
                         brand=excluded.brand,
                         updated_at=CURRENT_TIMESTAMP""",
                    (sku, display_name or agg['name'], snapshot_date, snapshot_date, status,
                     agg['store_count'], agg['total_on_hand'], 1 if is_tracked else 0, brand),
                )

        # 7) Insert detected listing changes
        if change_inserts:
            if USE_POSTGRES:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO sod_listing_changes
                       (sku, store_number, change_date, old_status, new_status, change_type)
                       VALUES %s""",
                    change_inserts,
                )
            else:
                cur.executemany(
                    """INSERT INTO sod_listing_changes
                       (sku, store_number, change_date, old_status, new_status, change_type)
                       VALUES (?,?,?,?,?,?)""",
                    change_inserts,
                )

        # 8) Also stamp a summary inventory_history row per tracked SKU (for legacy views)
        for sku, (brand, pname) in SOD_TRACKED_SKUS.items():
            agg = per_sku.get(sku)
            if not agg:
                continue
            # find product_id from products table
            if USE_POSTGRES:
                cur.execute("SELECT id FROM products WHERE lcbo_sku = %s LIMIT 1", (sku.lstrip('0'),))
            else:
                cur.execute("SELECT id FROM products WHERE lcbo_sku = ? LIMIT 1", (sku.lstrip('0'),))
            prow = cur.fetchone()
            if prow:
                pid = prow[0]
                if USE_POSTGRES:
                    cur.execute(
                        """INSERT INTO inventory_history
                           (product_id, store_number, store_name, store_city, quantity, recorded_at)
                           VALUES (%s, 'SUMMARY', %s, 'SOD', %s, %s)""",
                        (pid, f"{agg['store_count']} stores (SOD)", agg['total_on_hand'], snapshot_date),
                    )
                else:
                    cur.execute(
                        """INSERT INTO inventory_history
                           (product_id, store_number, store_name, store_city, quantity, recorded_at)
                           VALUES (?, 'SUMMARY', ?, 'SOD', ?, ?)""",
                        (pid, f"{agg['store_count']} stores (SOD)", agg['total_on_hand'], snapshot_date),
                    )

        duration = (datetime.utcnow() - start).total_seconds()
        if USE_POSTGRES:
            cur.execute(
                """UPDATE sod_sync_runs SET
                    status='success', file_name=%s, snapshot_date=%s,
                    total_rows=%s, anu_rows=%s, new_listings=%s, new_delistings=%s,
                    duration_seconds=%s
                   WHERE id=%s""",
                (zip_name, snapshot_date, total, anu_count, new_listings, new_delistings, duration, run_id),
            )
        else:
            cur.execute(
                """UPDATE sod_sync_runs SET
                    status='success', file_name=?, snapshot_date=?,
                    total_rows=?, anu_rows=?, new_listings=?, new_delistings=?,
                    duration_seconds=?
                   WHERE id=?""",
                (zip_name, snapshot_date, total, anu_count, new_listings, new_delistings, duration, run_id),
            )
        conn.commit()
        cur.close()
        return {
            'status': 'success',
            'run_id': run_id,
            'source': source,
            'file_name': zip_name,
            'snapshot_date': snapshot_date,
            'total_rows': total,
            'anu_rows': anu_count,
            'new_listings': new_listings,
            'new_delistings': new_delistings,
            'duration_seconds': round(duration, 1),
        }
    except Exception as e:
        err = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}"
        print(f"[SOD] sync failed: {err}")
        try:
            duration = (datetime.utcnow() - start).total_seconds()
            if USE_POSTGRES:
                conn.rollback()
                cur2 = conn.cursor()
                cur2.execute(
                    "UPDATE sod_sync_runs SET status='failed', error=%s, duration_seconds=%s WHERE id=%s",
                    (err[:2000], duration, run_id),
                )
                conn.commit()
                cur2.close()
            else:
                conn.rollback()
                conn.execute(
                    "UPDATE sod_sync_runs SET status='failed', error=?, duration_seconds=? WHERE id=?",
                    (err[:2000], duration, run_id),
                )
                conn.commit()
        except Exception:
            pass
        return {'status': 'failed', 'source': source, 'error': err}
    finally:
        try:
            conn.close()
        except Exception:
            pass


# --------- Async trigger + scheduler ---------

_sod_sync_lock = threading.Lock()
_sod_last_result = {'daily_a': None, 'daily_b': None}


def _sod_sync_worker(sources):
    """Run the sync in a background thread (used for manual trigger)."""
    if not _sod_sync_lock.acquire(blocking=False):
        print('[SOD] sync already running, skipping')
        return
    try:
        client = SODClient()
        try:
            client.login()
        except Exception as e:
            print(f'[SOD] login failed: {e}')
            for s in sources:
                _sod_last_result[s] = {'status': 'failed', 'error': str(e), 'source': s}
            return
        for src in sources:
            result = run_sod_sync(src, client=client)
            _sod_last_result[src] = result
            print(f'[SOD] {src}: {result.get("status")} '
                  f'rows={result.get("total_rows",0)} '
                  f'anu={result.get("anu_rows",0)} '
                  f'new_listings={result.get("new_listings",0)} '
                  f'new_delistings={result.get("new_delistings",0)}')
    finally:
        _sod_sync_lock.release()


def start_sod_sync_async(sources=None):
    sources = sources or ['daily_a', 'daily_b']
    t = threading.Thread(target=_sod_sync_worker, args=(sources,), daemon=True)
    t.start()
    return t


# --------- Endpoints ---------

@app.route('/api/sod/status', methods=['GET'])
def api_sod_status():
    """Last sync runs + counts of ingested data + configuration check."""
    configured = bool(SOD_USER and SOD_PASSWORD)
    rows = db_fetchall(
        "SELECT id, run_at, source, file_name, snapshot_date, status, total_rows, "
        "anu_rows, new_listings, new_delistings, duration_seconds, error "
        "FROM sod_sync_runs ORDER BY run_at DESC LIMIT 20"
    )
    last_by_source = {}
    for r in rows:
        rd = row_to_dict(r) if not isinstance(r, dict) else r
        src = rd['source']
        if src not in last_by_source or rd.get('status') == 'success':
            last_by_source.setdefault(src, rd)
    # Snapshot stats
    stats = row_to_dict(db_fetchone(
        "SELECT COUNT(*) AS inv_rows, COUNT(DISTINCT sku) AS sku_count, "
        "COUNT(DISTINCT snapshot_date) AS snapshot_days, "
        "MAX(snapshot_date) AS latest_snapshot "
        "FROM sod_inventory"
    ) or {})
    tracked_count = (row_to_dict(db_fetchone(
        "SELECT COUNT(*) AS c FROM sod_products WHERE is_tracked = " + ("TRUE" if USE_POSTGRES else "1")
    )) or {}).get('c', 0)
    return jsonify({
        'configured': configured,
        'agent_id': SOD_AGENT_ID if configured else None,
        'recent_runs': [row_to_dict(r) for r in rows],
        'last_by_source': last_by_source,
        'stats': {
            **stats,
            'tracked_products': tracked_count,
        },
        'scheduler_running': _sod_scheduler_running(),
    })


@app.route('/api/sod/sync', methods=['POST'])
def api_sod_sync():
    """Kick off an async sync for one or both sources."""
    if not SOD_USER or not SOD_PASSWORD:
        return jsonify({'error': 'SOD_USER / SOD_PASSWORD env vars not configured'}), 400
    body = request.get_json(silent=True) or {}
    sources = body.get('sources') or ['daily_a', 'daily_b']
    sources = [s for s in sources if s in ('daily_a', 'daily_b')]
    if not sources:
        return jsonify({'error': 'no valid sources provided'}), 400
    if _sod_sync_lock.locked():
        return jsonify({'status': 'already_running', 'sources': sources}), 202
    start_sod_sync_async(sources)
    return jsonify({'status': 'started', 'sources': sources}), 202


@app.route('/api/sod/inventory', methods=['GET'])
def api_sod_inventory():
    """Per-store inventory from SOD. Filter by sku, store, or brand.

    Defaults to the latest snapshot across all tracked SKUs.
    """
    sku = request.args.get('sku', '').strip()
    brand = request.args.get('brand', '').strip()
    snapshot_date = request.args.get('date', '').strip()
    tracked_only = request.args.get('tracked_only', '1') == '1'

    if not snapshot_date:
        latest = db_fetchone("SELECT MAX(snapshot_date) AS d FROM sod_inventory")
        if latest:
            snapshot_date = (latest['d'] if isinstance(latest, dict) else latest[0])
        if not snapshot_date:
            return jsonify({'rows': [], 'snapshot_date': None, 'message': 'no SOD data ingested yet'})
    snapshot_date = str(snapshot_date)

    query = (
        "SELECT i.sku, i.store_number, i.status, i.on_hand, i.product_name, "
        "i.snapshot_date, p.brand AS brand "
        "FROM sod_inventory i "
        "LEFT JOIN sod_products p ON p.sku = i.sku "
        "WHERE i.snapshot_date = ?"
    )
    params = [snapshot_date]
    if sku:
        # Zero-pad user input to match stored format
        padded = sku.zfill(7)
        query += " AND i.sku = ?"
        params.append(padded)
    if brand:
        query += " AND p.brand = ?"
        params.append(brand)
    if tracked_only and not sku:
        query += " AND (p.is_tracked = " + ("TRUE" if USE_POSTGRES else "1") + ")"
    query += " ORDER BY i.sku, i.store_number"
    rows = db_fetchall(query, params)
    return jsonify({
        'snapshot_date': snapshot_date,
        'count': len(rows),
        'rows': [row_to_dict(r) for r in rows],
    })


@app.route('/api/sod/products', methods=['GET'])
def api_sod_products():
    """Product-level rollup. Shows every Anu/NB SKU with store count + total on-hand + status."""
    tracked_only = request.args.get('tracked_only', '1') == '1'
    query = (
        "SELECT sku, product_name, brand, current_status, store_count, total_on_hand, "
        "first_seen, last_seen, is_tracked, updated_at "
        "FROM sod_products"
    )
    if tracked_only:
        query += " WHERE is_tracked = " + ("TRUE" if USE_POSTGRES else "1")
    query += " ORDER BY is_tracked DESC, brand, product_name"
    rows = db_fetchall(query)
    return jsonify({'count': len(rows), 'rows': [row_to_dict(r) for r in rows]})


@app.route('/api/sod/listing-changes', methods=['GET'])
def api_sod_listing_changes():
    """Listing status changes detected by the sync. Default window: 90 days.

    Filters: ?days=30, ?type=DELISTED|NEW_LISTING|RELISTED|STATUS_FLIP, ?tracked_only=1
    """
    try:
        days = int(request.args.get('days', '90'))
    except ValueError:
        days = 90
    change_type = request.args.get('type', '').strip().upper()
    tracked_only = request.args.get('tracked_only', '1') == '1'
    cutoff = (datetime.utcnow().date() - timedelta(days=days)).isoformat()
    query = (
        "SELECT c.id, c.sku, c.store_number, c.change_date, c.old_status, c.new_status, "
        "c.change_type, c.detected_at, p.product_name AS product_name, p.brand AS brand, "
        "p.is_tracked AS is_tracked "
        "FROM sod_listing_changes c "
        "LEFT JOIN sod_products p ON p.sku = c.sku "
        "WHERE c.change_date >= ?"
    )
    params = [cutoff]
    if change_type:
        query += " AND c.change_type = ?"
        params.append(change_type)
    if tracked_only:
        query += " AND p.is_tracked = " + ("TRUE" if USE_POSTGRES else "1")
    query += " ORDER BY c.change_date DESC, c.id DESC LIMIT 500"
    rows = db_fetchall(query, params)
    return jsonify({
        'window_days': days,
        'count': len(rows),
        'rows': [row_to_dict(r) for r in rows],
    })


@app.route('/api/sod/gap-report', methods=['GET'])
def api_sod_gap_report():
    """For each tracked SKU, list stores in the master store table that are NOT carrying it
    according to the most recent SOD snapshot. Source of truth: SOD Daily Inventory A.
    """
    latest = db_fetchone("SELECT MAX(snapshot_date) AS d FROM sod_inventory")
    snapshot_date = (latest['d'] if isinstance(latest, dict) else latest[0]) if latest else None
    if not snapshot_date:
        return jsonify({'snapshot_date': None, 'products': [], 'message': 'no SOD data yet — run /api/sod/sync first'})
    snapshot_date = str(snapshot_date)

    # All active LCBO stores
    store_rows = db_fetchall(
        "SELECT store_number, account, address, city, rep FROM stores WHERE store_number > 0"
    )
    all_stores = [row_to_dict(r) for r in store_rows]
    store_by_num = {int(s['store_number']): s for s in all_stores}

    # For each tracked SKU, pull carrying stores from SOD
    report = []
    for sku, (brand, pname) in SOD_TRACKED_SKUS.items():
        carrying = db_fetchall(
            "SELECT store_number, status, on_hand FROM sod_inventory "
            "WHERE sku = ? AND snapshot_date = ?",
            [sku, snapshot_date],
        )
        carrying_map = {int(row_to_dict(r)['store_number']): row_to_dict(r) for r in carrying}
        gap_stores = []
        for s in all_stores:
            if int(s['store_number']) not in carrying_map:
                gap_stores.append(s)
        # Segment carrying by status
        listed = [c for c in carrying_map.values() if c.get('status') == 'L']
        delisting = [c for c in carrying_map.values() if c.get('status') == 'D']
        fully_delisted = [c for c in carrying_map.values() if c.get('status') == 'F']
        report.append({
            'sku': sku,
            'brand': brand,
            'product_name': pname,
            'total_stores_in_system': len(all_stores),
            'carrying_count': len(carrying_map),
            'listed_count': len(listed),
            'delisting_count': len(delisting),
            'fully_delisted_count': len(fully_delisted),
            'gap_count': len(gap_stores),
            'coverage_pct': round(100 * len(carrying_map) / max(1, len(all_stores)), 1),
            'gap_stores': gap_stores[:200],  # cap for payload size
        })
    return jsonify({
        'snapshot_date': snapshot_date,
        'total_stores': len(all_stores),
        'products': report,
    })


@app.route('/api/sod/reorder', methods=['GET'])
def api_sod_reorder():
    """Stores carrying each tracked SKU with low on-hand. Bucketed by urgency."""
    try:
        threshold = int(request.args.get('threshold', '6'))
    except ValueError:
        threshold = 6
    latest = db_fetchone("SELECT MAX(snapshot_date) AS d FROM sod_inventory")
    snapshot_date = (latest['d'] if isinstance(latest, dict) else latest[0]) if latest else None
    if not snapshot_date:
        return jsonify({'snapshot_date': None, 'rows': [], 'message': 'no SOD data yet'})
    snapshot_date = str(snapshot_date)

    # Map store_number → store info
    store_rows = db_fetchall("SELECT store_number, account, address, city, rep FROM stores")
    store_map = {int(row_to_dict(r)['store_number']): row_to_dict(r) for r in store_rows}

    query = (
        "SELECT i.sku, i.store_number, i.status, i.on_hand, i.product_name, p.brand AS brand "
        "FROM sod_inventory i LEFT JOIN sod_products p ON p.sku = i.sku "
        "WHERE i.snapshot_date = ? AND p.is_tracked = " + ("TRUE" if USE_POSTGRES else "1") +
        " AND i.status = 'L' AND i.on_hand <= ? "
        "ORDER BY i.on_hand ASC, i.sku"
    )
    rows = db_fetchall(query, [snapshot_date, threshold])
    output = []
    for r in rows:
        d = row_to_dict(r)
        snum = int(d['store_number'])
        s = store_map.get(snum, {})
        oh = d.get('on_hand') or 0
        if oh <= 1:
            urgency = 'critical'
        elif oh <= 3:
            urgency = 'high'
        else:
            urgency = 'medium'
        output.append({
            **d,
            'store_account': s.get('account', f'LCBO #{snum}'),
            'store_city': s.get('city', ''),
            'store_rep': s.get('rep', ''),
            'urgency': urgency,
        })
    counts = {'critical': 0, 'high': 0, 'medium': 0}
    for r in output:
        counts[r['urgency']] += 1
    return jsonify({
        'snapshot_date': snapshot_date,
        'threshold': threshold,
        'count': len(output),
        'urgency_counts': counts,
        'rows': output,
    })


@app.route('/api/sod/trend/<sku>', methods=['GET'])
def api_sod_trend(sku):
    """Daily history of store_count + total_on_hand for a SKU (line chart)."""
    padded = sku.zfill(7)
    try:
        days = int(request.args.get('days', '60'))
    except ValueError:
        days = 60
    cutoff = (datetime.utcnow().date() - timedelta(days=days)).isoformat()
    rows = db_fetchall(
        "SELECT snapshot_date, COUNT(*) AS store_count, "
        "SUM(on_hand) AS total_on_hand, "
        "SUM(CASE WHEN status='L' THEN 1 ELSE 0 END) AS listed_stores, "
        "SUM(CASE WHEN status='D' THEN 1 ELSE 0 END) AS delisting_stores "
        "FROM sod_inventory WHERE sku = ? AND snapshot_date >= ? "
        "GROUP BY snapshot_date ORDER BY snapshot_date",
        [padded, cutoff],
    )
    return jsonify({'sku': padded, 'days': days, 'rows': [row_to_dict(r) for r in rows]})


# --------- Daily / Weekly / Monthly summary reports ---------

def _sod_summary_for_range(start_date, end_date):
    """Return a dict summarising SOD data for a [start, end] date range (inclusive).

    If the requested window has no data, automatically shifts the window to end on
    the latest available snapshot (preserves the window length). This keeps the
    reports useful on weekends / when today's file hasn't been uploaded yet.
    """
    ph = _sod_ph()
    start = start_date.isoformat() if isinstance(start_date, (datetime,)) else str(start_date)
    end = end_date.isoformat() if isinstance(end_date, (datetime,)) else str(end_date)

    # Empty-range fallback: anchor to latest snapshot we actually have
    probe = db_fetchone(
        "SELECT MAX(snapshot_date) AS d FROM sod_inventory WHERE snapshot_date BETWEEN ? AND ?",
        [start, end],
    )
    probe_d = (probe['d'] if isinstance(probe, dict) else probe[0]) if probe else None
    if probe_d is None:
        latest_any = db_fetchone("SELECT MAX(snapshot_date) AS d FROM sod_inventory")
        latest_d = (latest_any['d'] if isinstance(latest_any, dict) else latest_any[0]) if latest_any else None
        if latest_d:
            try:
                s_d = datetime.strptime(start, '%Y-%m-%d').date()
                e_d = datetime.strptime(end, '%Y-%m-%d').date()
                window_len = (e_d - s_d).days
                new_end = datetime.strptime(str(latest_d), '%Y-%m-%d').date()
                new_start = new_end - timedelta(days=window_len)
                start = new_start.isoformat()
                end = new_end.isoformat()
            except Exception:
                pass

    # Per-SKU totals over the window
    per_sku = db_fetchall(
        "SELECT i.sku, p.product_name AS product_name, p.brand AS brand, "
        "COUNT(DISTINCT i.snapshot_date) AS day_count, "
        "AVG(i.on_hand * 1.0) AS avg_on_hand, "
        "MAX(i.snapshot_date) AS latest_date, "
        "SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END) AS listed_store_days, "
        "SUM(CASE WHEN i.status='D' THEN 1 ELSE 0 END) AS delisting_store_days "
        "FROM sod_inventory i LEFT JOIN sod_products p ON p.sku = i.sku "
        "WHERE p.is_tracked = " + ("TRUE" if USE_POSTGRES else "1") +
        " AND i.snapshot_date BETWEEN ? AND ? "
        "GROUP BY i.sku, p.product_name, p.brand "
        "ORDER BY p.brand, p.product_name",
        [start, end],
    )
    # Listing changes in window
    changes = db_fetchall(
        "SELECT c.sku, p.product_name AS product_name, p.brand AS brand, "
        "c.change_type, c.change_date, c.old_status, c.new_status "
        "FROM sod_listing_changes c LEFT JOIN sod_products p ON p.sku = c.sku "
        "WHERE p.is_tracked = " + ("TRUE" if USE_POSTGRES else "1") +
        " AND c.change_date BETWEEN ? AND ? "
        "ORDER BY c.change_date DESC, c.id DESC",
        [start, end],
    )
    # Latest snapshot metrics
    latest_date_row = db_fetchone(
        "SELECT MAX(snapshot_date) AS d FROM sod_inventory WHERE snapshot_date BETWEEN ? AND ?",
        [start, end],
    )
    latest_date = (latest_date_row['d'] if isinstance(latest_date_row, dict) else latest_date_row[0]) if latest_date_row else None

    snapshot_metrics = []
    if latest_date:
        snapshot_metrics_rows = db_fetchall(
            "SELECT i.sku, p.product_name AS product_name, p.brand AS brand, "
            "COUNT(*) AS store_count, "
            "SUM(i.on_hand) AS total_on_hand, "
            "SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END) AS listed_stores, "
            "SUM(CASE WHEN i.status='D' THEN 1 ELSE 0 END) AS delisting_stores, "
            "SUM(CASE WHEN i.status='F' THEN 1 ELSE 0 END) AS fully_delisted_stores "
            "FROM sod_inventory i LEFT JOIN sod_products p ON p.sku = i.sku "
            "WHERE p.is_tracked = " + ("TRUE" if USE_POSTGRES else "1") +
            " AND i.snapshot_date = ? "
            "GROUP BY i.sku, p.product_name, p.brand "
            "ORDER BY p.brand, p.product_name",
            [str(latest_date)],
        )
        snapshot_metrics = [row_to_dict(r) for r in snapshot_metrics_rows]

    return {
        'window': {'start': start, 'end': end, 'latest_snapshot': str(latest_date) if latest_date else None},
        'per_sku': [row_to_dict(r) for r in per_sku],
        'snapshot_metrics': snapshot_metrics,
        'listing_changes': [row_to_dict(r) for r in changes],
        'totals': {
            'products_tracked': len(per_sku),
            'changes_in_window': len(changes),
            'new_listings': sum(1 for r in changes if row_to_dict(r)['change_type'] == 'NEW_LISTING'),
            'delistings': sum(1 for r in changes if row_to_dict(r)['change_type'] == 'DELISTED'),
            'relistings': sum(1 for r in changes if row_to_dict(r)['change_type'] == 'RELISTED'),
        },
    }


@app.route('/api/reports/daily', methods=['GET'])
def api_report_daily():
    day_str = request.args.get('date')
    try:
        day = datetime.strptime(day_str, '%Y-%m-%d').date() if day_str else datetime.utcnow().date()
    except ValueError:
        day = datetime.utcnow().date()
    return jsonify(_sod_summary_for_range(day, day))


@app.route('/api/reports/weekly', methods=['GET'])
def api_report_weekly():
    end_str = request.args.get('end')
    try:
        end = datetime.strptime(end_str, '%Y-%m-%d').date() if end_str else datetime.utcnow().date()
    except ValueError:
        end = datetime.utcnow().date()
    start = end - timedelta(days=6)
    return jsonify(_sod_summary_for_range(start, end))


@app.route('/api/reports/monthly', methods=['GET'])
def api_report_monthly():
    end_str = request.args.get('end')
    try:
        end = datetime.strptime(end_str, '%Y-%m-%d').date() if end_str else datetime.utcnow().date()
    except ValueError:
        end = datetime.utcnow().date()
    start = end.replace(day=1)
    return jsonify(_sod_summary_for_range(start, end))


@app.route('/api/reports/rep', methods=['GET'])
def api_report_rep():
    """Per-rep performance: stores assigned, products carried, gap count, delisting risk."""
    latest = db_fetchone("SELECT MAX(snapshot_date) AS d FROM sod_inventory")
    snapshot_date = (latest['d'] if isinstance(latest, dict) else latest[0]) if latest else None

    # All reps (from stores table)
    rep_rows = db_fetchall(
        "SELECT rep, COUNT(*) AS store_count FROM stores WHERE rep IS NOT NULL AND rep != '' "
        "GROUP BY rep ORDER BY store_count DESC"
    )
    out = []
    for rr in rep_rows:
        rd = row_to_dict(rr)
        rep_name = rd['rep']
        # Per-rep: how many of his stores are carrying each tracked SKU
        per_sku = []
        if snapshot_date:
            for sku, (brand, pname) in SOD_TRACKED_SKUS.items():
                carrying = db_fetchone(
                    "SELECT COUNT(*) AS c FROM sod_inventory i "
                    "JOIN stores s ON s.store_number = i.store_number "
                    "WHERE s.rep = ? AND i.sku = ? AND i.snapshot_date = ?",
                    [rep_name, sku, str(snapshot_date)],
                )
                carrying_cnt = (row_to_dict(carrying) or {}).get('c', 0)
                delisting = db_fetchone(
                    "SELECT COUNT(*) AS c FROM sod_inventory i "
                    "JOIN stores s ON s.store_number = i.store_number "
                    "WHERE s.rep = ? AND i.sku = ? AND i.snapshot_date = ? AND i.status = 'D'",
                    [rep_name, sku, str(snapshot_date)],
                )
                delisting_cnt = (row_to_dict(delisting) or {}).get('c', 0)
                per_sku.append({
                    'sku': sku,
                    'brand': brand,
                    'product_name': pname,
                    'stores_carrying': carrying_cnt,
                    'stores_delisting': delisting_cnt,
                    'gap_count': rd['store_count'] - carrying_cnt,
                })
        out.append({
            'rep': rep_name,
            'total_stores': rd['store_count'],
            'per_product': per_sku,
        })
    return jsonify({'snapshot_date': str(snapshot_date) if snapshot_date else None, 'reps': out})


# --------- Scheduler ---------

_sod_scheduler = None


def _sod_scheduler_running():
    try:
        return _sod_scheduler is not None and _sod_scheduler.running
    except Exception:
        return False


def _sod_last_successful_sync_age_hours():
    """Return hours since last successful sync, or None if never synced."""
    row = db_fetchone(
        "SELECT run_at FROM sod_sync_runs WHERE status='success' ORDER BY run_at DESC LIMIT 1"
    )
    if not row:
        return None
    d = row_to_dict(row) if not isinstance(row, dict) else row
    val = d.get('run_at')
    if not val:
        return None
    if isinstance(val, str):
        try:
            val = datetime.fromisoformat(val)
        except Exception:
            try:
                val = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            except Exception:
                return None
    return (datetime.utcnow() - val).total_seconds() / 3600.0


def start_sod_scheduler():
    """Start an APScheduler BackgroundScheduler that runs the sync daily at 03:00 America/Toronto.

    Schedule rationale: LCBO uploads the daily file between ~01:30 and ~02:30 ET. We run at
    03:00 ET to guarantee the file is present.

    Also kicks off a catch-up sync at startup if the last successful sync is > 24h old.

    Safe to call multiple times — only one scheduler is ever started per process.
    """
    global _sod_scheduler
    if _sod_scheduler is not None:
        return
    if not (SOD_USER and SOD_PASSWORD):
        print('[SOD] scheduler NOT started — SOD_USER/SOD_PASSWORD not configured')
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print('[SOD] apscheduler not installed — skipping scheduler. pip install apscheduler')
        return
    try:
        # America/Toronto (EST/EDT) — fall back to UTC if tzdata missing
        try:
            sched = BackgroundScheduler(timezone='America/Toronto')
        except Exception:
            sched = BackgroundScheduler()
        sched.add_job(
            lambda: _sod_sync_worker(['daily_a', 'daily_b']),
            CronTrigger(hour=3, minute=0),  # 03:00 ET — after LCBO finishes uploading
            id='sod_daily_sync',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 6,  # tolerate up to 6h delay (e.g. Render cold boot)
        )
        sched.start()
        _sod_scheduler = sched
        print(f'[SOD] Daily scheduler started — next run: {sched.get_job("sod_daily_sync").next_run_time}')

        # --- Startup catch-up: if last successful sync is > 24h old, fire immediately (delayed) ---
        def _catchup_if_stale():
            try:
                # Defer briefly so the DB is ready and the app is serving
                import time as _t
                _t.sleep(30)
                with app.app_context():
                    age = _sod_last_successful_sync_age_hours()
                if age is None:
                    print('[SOD] no prior successful sync — running initial catch-up')
                    _sod_sync_worker(['daily_a', 'daily_b'])
                elif age > 24:
                    print(f'[SOD] last sync was {age:.1f}h ago — running catch-up')
                    _sod_sync_worker(['daily_a', 'daily_b'])
                else:
                    print(f'[SOD] last sync {age:.1f}h ago — no catch-up needed')
            except Exception as e:
                print(f'[SOD] catch-up check failed: {e}')
        threading.Thread(target=_catchup_if_stale, daemon=True).start()
    except Exception as e:
        print(f'[SOD] scheduler failed to start: {e}')


# --------- External cron trigger (for Render Cron Job as a redundant trigger) ---------

SOD_CRON_TOKEN = os.environ.get('SOD_CRON_TOKEN', '').strip()


@app.route('/api/sod/cron', methods=['POST', 'GET'])
def api_sod_cron():
    """Endpoint for Render Cron Job to hit daily.

    Protected by SOD_CRON_TOKEN env var: requests must pass ?token=... or
    Authorization: Bearer <token>. Set SOD_CRON_TOKEN on Render and configure the
    cron service to curl this URL daily.

    This runs alongside the in-process APScheduler as belt-and-suspenders: if the
    Render web worker is sleeping or has crashed, the cron call wakes it up and
    triggers the sync.
    """
    provided = request.args.get('token', '').strip()
    if not provided:
        auth = request.headers.get('Authorization', '')
        if auth.startswith('Bearer '):
            provided = auth[7:].strip()
    if not SOD_CRON_TOKEN:
        return jsonify({'error': 'SOD_CRON_TOKEN env var not set — cron endpoint disabled'}), 503
    if provided != SOD_CRON_TOKEN:
        return jsonify({'error': 'invalid token'}), 401
    # Only fire if last sync is > 6h old — avoids duplicate work if scheduler already ran
    age = _sod_last_successful_sync_age_hours()
    if age is not None and age < 6:
        return jsonify({'status': 'skipped', 'reason': f'last sync {age:.1f}h ago (< 6h)'}), 200
    if _sod_sync_lock.locked():
        return jsonify({'status': 'already_running'}), 202
    start_sod_sync_async(['daily_a', 'daily_b'])
    return jsonify({'status': 'started', 'reason': f'last sync {age}h ago' if age is not None else 'no prior sync'}), 202


@app.route('/api/sod/health', methods=['GET'])
def api_sod_health():
    """Lightweight health check: is the sync fresh? For monitoring."""
    age = _sod_last_successful_sync_age_hours()
    if age is None:
        return jsonify({'status': 'never_synced', 'configured': bool(SOD_USER and SOD_PASSWORD)}), 503
    fresh = age < 36  # 36h window = one missed day
    return jsonify({
        'status': 'healthy' if fresh else 'stale',
        'last_sync_age_hours': round(age, 2),
        'scheduler_running': _sod_scheduler_running(),
        'configured': bool(SOD_USER and SOD_PASSWORD),
    }), 200 if fresh else 503


# ======== INIT ========

init_db()
seed_data()
start_sod_scheduler()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    app.run(debug=os.environ.get('FLASK_DEBUG', 'true').lower() == 'true', host='0.0.0.0', port=port)
