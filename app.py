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


# ============================================================================
# In-process response cache — saves Render free-tier credits AND speeds up
# the heavy SKU-aggregation endpoints (velocity, sku-trend, sod-trend) from
# 16-24s down to <100ms on cache hits. TTL = 5 minutes (data only changes
# once a day from SOD ingest, but we want fresh-on-demand for new visits).
# Pure-stdlib so no new dependency = no extra build credits.
# ============================================================================
import threading as _threading
_cache_lock = _threading.RLock()
_cache_store: dict = {}  # key -> (expires_epoch, value, status_code)
_CACHE_MAX_ENTRIES = 500


def _cache_get(key):
    with _cache_lock:
        v = _cache_store.get(key)
        if v is None:
            return None
        expires, val, code = v
        if expires < datetime.utcnow().timestamp():
            _cache_store.pop(key, None)
            return None
        return (val, code)


def _cache_put(key, val, code, ttl_seconds):
    with _cache_lock:
        if len(_cache_store) > _CACHE_MAX_ENTRIES:
            # LRU-ish: drop oldest 20%
            now = datetime.utcnow().timestamp()
            expired = [k for k, v in _cache_store.items() if v[0] < now]
            for k in expired:
                _cache_store.pop(k, None)
            if len(_cache_store) > _CACHE_MAX_ENTRIES:
                # still full — drop arbitrary 20%
                drop = list(_cache_store.keys())[: _CACHE_MAX_ENTRIES // 5]
                for k in drop:
                    _cache_store.pop(k, None)
        _cache_store[key] = (datetime.utcnow().timestamp() + ttl_seconds, val, code)


def cached_response(ttl_seconds: int = 300, key_args: tuple = ()):
    """Decorator: cache the JSON response of a Flask handler.

    key_args: extra request.args names to include in the cache key.
    Cache headers are added so clients also benefit from browser caching.
    """
    def decorator(fn):
        from functools import wraps

        @wraps(fn)
        def wrapped(*args, **kwargs):
            # Skip cache when explicitly bypassed (?nocache=1 or X-Bypass-Cache header)
            if request.args.get('nocache') or request.headers.get('X-Bypass-Cache'):
                return fn(*args, **kwargs)
            arg_part = '|'.join(f"{k}={request.args.get(k, '')}" for k in key_args)
            cache_key = f"{fn.__name__}:{request.path}?{arg_part}:{json.dumps(kwargs, sort_keys=True, default=str)}"
            hit = _cache_get(cache_key)
            if hit is not None:
                val, code = hit
                resp = Response(val, mimetype='application/json', status=code)
                resp.headers['X-Cache'] = 'HIT'
                resp.headers['Cache-Control'] = f'public, max-age={ttl_seconds}'
                return resp
            result = fn(*args, **kwargs)
            # Flask handlers can return jsonify(...) (Response) or (Response, code) tuple
            code = 200
            if isinstance(result, tuple):
                resp_obj, code = result[0], result[1]
            else:
                resp_obj = result
            try:
                body = resp_obj.get_data(as_text=True)
                _cache_put(cache_key, body, code, ttl_seconds)
                resp_obj.headers['X-Cache'] = 'MISS'
                resp_obj.headers['Cache-Control'] = f'public, max-age={ttl_seconds}'
            except Exception:
                pass
            return result if isinstance(result, tuple) else resp_obj

        return wrapped
    return decorator


# ============================================================================
# Lightweight rate limiter — protects the worker from accidental 200-parallel
# bursts (which is what killed Render last week). Per-IP bucket; default 50
# req/sec. No new dependency, pure stdlib.
# ============================================================================
from collections import deque as _deque
_rate_buckets: dict = {}
_rate_lock = _threading.RLock()

def _client_ip():
    fwd = request.headers.get('X-Forwarded-For', '')
    if fwd:
        return fwd.split(',')[0].strip()
    return request.remote_addr or 'unknown'

@app.before_request
def _rate_limit_global():
    # Skip rate limiting for healthz / root — those are uptime probes
    if request.path in ('/healthz', '/'):
        return
    ip = _client_ip()
    now = datetime.utcnow().timestamp()
    with _rate_lock:
        bucket = _rate_buckets.setdefault(ip, _deque())
        while bucket and bucket[0] < now - 1.0:
            bucket.popleft()
        if len(bucket) >= 50:  # 50 req/sec per IP
            return jsonify({'error': 'rate limit — slow down (max 50/sec/IP)'}), 429
        bucket.append(now)
        if len(_rate_buckets) > 1000:
            for k in list(_rate_buckets.keys())[:200]:
                if not _rate_buckets[k] or _rate_buckets[k][-1] < now - 60:
                    _rate_buckets.pop(k, None)


@app.route('/api/admin/cache-stats', methods=['GET'])
def api_admin_cache_stats():
    with _cache_lock:
        now = datetime.utcnow().timestamp()
        live = sum(1 for v in _cache_store.values() if v[0] >= now)
        expired = len(_cache_store) - live
    return jsonify({'entries_live': live, 'entries_expired': expired, 'total': len(_cache_store)})


@app.route('/api/admin/cache-clear', methods=['POST'])
def api_admin_cache_clear():
    if not _admin_token_ok():
        return jsonify({'error': 'forbidden — set X-Admin-Token header'}), 403
    with _cache_lock:
        n = len(_cache_store)
        _cache_store.clear()
    return jsonify({'cleared': n})

# CORS — allow the Vercel-hosted Next.js frontend to call this backend.
# Default origins: localhost dev + lcbo-tracker-web.vercel.app + Anu domain.
# Override via env var CORS_ORIGINS (comma-separated).
try:
    from flask_cors import CORS
    _cors_origins = os.environ.get(
        'CORS_ORIGINS',
        'http://localhost:3000,http://localhost:3001,'
        'https://lcbo-tracker-web.vercel.app,'
        'https://lcbo.anu-spirits.com'
    ).split(',')
    CORS(app, resources={r'/api/*': {'origins': _cors_origins},
                         r'/healthz': {'origins': _cors_origins}}, supports_credentials=False)
    print(f'[CORS] enabled for: {_cors_origins}')
except ImportError:
    print('[CORS] flask-cors not installed — frontend on different domain will be blocked')

# Sentry (optional, no-op if SENTRY_DSN unset)
_sentry_dsn = os.environ.get('SENTRY_DSN', '').strip()
if _sentry_dsn:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.flask import FlaskIntegration
        sentry_sdk.init(
            dsn=_sentry_dsn,
            integrations=[FlaskIntegration()],
            traces_sample_rate=0.1,  # 10% of requests
            profiles_sample_rate=0.0,
            environment=os.environ.get('SENTRY_ENV', 'production'),
            release=os.environ.get('GIT_COMMIT_SHA', 'unknown')[:7],
        )
        print(f'[Sentry] initialized (env={os.environ.get("SENTRY_ENV", "production")})')
    except ImportError:
        print('[Sentry] sentry-sdk not installed; skipping')
    except Exception as e:
        print(f'[Sentry] init failed: {e}')

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
            # DictCursor (not RealDictCursor): rows support BOTH positional (r[0])
            # and key-based (r['col']) access, AND `dict(r)` still works.
            # Previously used RealDictCursor which broke the CRM endpoints that
            # relied on positional row access → 500s in production.
            g.db = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
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
        # Per-(store, sku) change tracking for our tracked SKUs:
        # answers "which stores added/dropped Red Admiral last week?"
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sod_store_sku_changes (
                id BIGSERIAL PRIMARY KEY,
                sku TEXT NOT NULL,
                store_number INTEGER NOT NULL,
                change_date DATE NOT NULL,
                old_status TEXT,
                new_status TEXT,
                change_type TEXT NOT NULL,
                detected_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(sku, store_number, change_date, change_type)
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
            "CREATE INDEX IF NOT EXISTS idx_sssc_date ON sod_store_sku_changes(change_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_sssc_sku ON sod_store_sku_changes(sku)",
            "CREATE INDEX IF NOT EXISTS idx_sssc_type ON sod_store_sku_changes(change_type)",
        ]:
            try:
                cur.execute(idx)
            except Exception:
                pass

        # ======== CRM tables: territories, goals, HORECA accounts ========
        cur.execute('''
            CREATE TABLE IF NOT EXISTS territories (
                id SERIAL PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                region TEXT DEFAULT '',
                rep_name TEXT DEFAULT '',
                color TEXT DEFAULT '#b22222',
                fsa_prefixes TEXT DEFAULT '',
                city_prefixes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS sales_goals (
                id SERIAL PRIMARY KEY,
                scope TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                target_units INTEGER DEFAULT 0,
                target_revenue NUMERIC(12,2) DEFAULT 0,
                target_listings INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(scope, scope_key, period_start, period_end)
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS horeca_accounts (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                account_type TEXT DEFAULT 'restaurant',
                address TEXT DEFAULT '',
                city TEXT DEFAULT '',
                postal TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                email TEXT DEFAULT '',
                contact_name TEXT DEFAULT '',
                contact_title TEXT DEFAULT '',
                territory_id INTEGER REFERENCES territories(id),
                rep_name TEXT DEFAULT '',
                status TEXT DEFAULT 'prospect',
                priority TEXT DEFAULT 'Standard',
                lat REAL DEFAULT 0,
                lng REAL DEFAULT 0,
                last_visit DATE,
                next_visit DATE,
                products_carried TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_terr_code ON territories(code)",
            "CREATE INDEX IF NOT EXISTS idx_goals_scope ON sales_goals(scope, scope_key)",
            "CREATE INDEX IF NOT EXISTS idx_goals_period ON sales_goals(period_start, period_end)",
            "CREATE INDEX IF NOT EXISTS idx_horeca_territory ON horeca_accounts(territory_id)",
            "CREATE INDEX IF NOT EXISTS idx_horeca_status ON horeca_accounts(status)",
            "CREATE INDEX IF NOT EXISTS idx_horeca_city ON horeca_accounts(city)",
        ]:
            try:
                cur.execute(idx)
            except Exception:
                pass

        # ======== SPRINT 3: CRM SYSTEM-OF-ACTION TABLES ========
        # deals: pipeline tracking per (store, sku) — the heart of the CRM
        cur.execute('''
            CREATE TABLE IF NOT EXISTS deals (
                id SERIAL PRIMARY KEY,
                store_number INTEGER,
                horeca_account_id INTEGER REFERENCES horeca_accounts(id),
                sku TEXT NOT NULL,
                stage TEXT NOT NULL DEFAULT 'prospecting',
                probability INTEGER DEFAULT 10,
                expected_close_date DATE,
                expected_units INTEGER DEFAULT 0,
                expected_revenue NUMERIC(12,2) DEFAULT 0,
                owner_rep TEXT DEFAULT '',
                next_action TEXT DEFAULT '',
                next_action_date DATE,
                notes TEXT DEFAULT '',
                source TEXT DEFAULT 'manual',
                closed_at TIMESTAMP,
                closed_reason TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # rep_quotas: quarterly targets per rep
        cur.execute('''
            CREATE TABLE IF NOT EXISTS rep_quotas (
                id SERIAL PRIMARY KEY,
                rep TEXT NOT NULL,
                quarter TEXT NOT NULL,
                target_activities INTEGER DEFAULT 0,
                target_visits INTEGER DEFAULT 0,
                target_new_listings INTEGER DEFAULT 0,
                target_units INTEGER DEFAULT 0,
                target_revenue NUMERIC(12,2) DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(rep, quarter)
            )
        ''')
        # visit_photos: photo URLs linked to activities (storage in R2/S3 later)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS visit_photos (
                id SERIAL PRIMARY KEY,
                activity_id INTEGER REFERENCES activities(id) ON DELETE CASCADE,
                photo_url TEXT NOT NULL,
                caption TEXT DEFAULT '',
                photo_type TEXT DEFAULT 'shelf',
                uploaded_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # activity_sku_outcomes: per-SKU outcome for a visit (listed/discussed/sampled/declined)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS activity_sku_outcomes (
                id SERIAL PRIMARY KEY,
                activity_id INTEGER REFERENCES activities(id) ON DELETE CASCADE,
                sku TEXT NOT NULL,
                outcome TEXT NOT NULL,
                facings INTEGER DEFAULT 0,
                competitor_notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # daily_plan_cache: materialized rep daily plan (for mobile instant load)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS daily_plan_cache (
                id SERIAL PRIMARY KEY,
                rep TEXT NOT NULL,
                plan_date DATE NOT NULL,
                stops_json TEXT NOT NULL,
                generated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(rep, plan_date)
            )
        ''')
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_deals_store ON deals(store_number)",
            "CREATE INDEX IF NOT EXISTS idx_deals_sku ON deals(sku)",
            "CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage)",
            "CREATE INDEX IF NOT EXISTS idx_deals_owner ON deals(owner_rep)",
            "CREATE INDEX IF NOT EXISTS idx_deals_next_action ON deals(next_action_date)",
            "CREATE INDEX IF NOT EXISTS idx_quotas_rep ON rep_quotas(rep, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_vp_activity ON visit_photos(activity_id)",
            "CREATE INDEX IF NOT EXISTS idx_aso_activity ON activity_sku_outcomes(activity_id)",
            "CREATE INDEX IF NOT EXISTS idx_aso_sku ON activity_sku_outcomes(sku)",
            "CREATE INDEX IF NOT EXISTS idx_dp_rep_date ON daily_plan_cache(rep, plan_date)",
        ]:
            try:
                cur.execute(idx)
            except Exception:
                pass

        # Add new columns on existing tables (upgrade-safe)
        crm_migrate_cols = [
            ('stores', 'territory_id', 'INTEGER'),
            ('sod_products', 'category', "TEXT DEFAULT ''"),
            ('sod_products', 'category_group', "TEXT DEFAULT ''"),
            # Activity enrichment for Sprint 3
            ('activities', 'outcome', "TEXT DEFAULT ''"),
            ('activities', 'duration_minutes', "INTEGER DEFAULT 0"),
            ('activities', 'rating', "INTEGER DEFAULT 0"),  # 1-5 store visit rating
            ('activities', 'next_action', "TEXT DEFAULT ''"),
            ('activities', 'next_action_date', "DATE"),
            ('activities', 'rep', "TEXT DEFAULT ''"),  # denormalized for fast queries
            ('activities', 'horeca_account_id', "INTEGER"),
            ('activities', 'lat', "REAL DEFAULT 0"),  # GPS where visit was logged
            ('activities', 'lng', "REAL DEFAULT 0"),
            # Sprint 6: storage backbone — backdating + soft-delete + provenance
            ('activities', 'visit_date', "DATE"),  # When visit ACTUALLY happened (vs when logged)
            ('activities', 'deleted_at', "TIMESTAMP"),  # Soft-delete timestamp; NEVER hard-delete
            ('activities', 'updated_at', "TIMESTAMP DEFAULT NOW()"),
        ]
        for table, col, coltype in crm_migrate_cols:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            except Exception:
                pass

        # Sprint 6: append-only audit log of EVERY mutation
        cur.execute('''
            CREATE TABLE IF NOT EXISTS event_log (
                id BIGSERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                actor TEXT DEFAULT '',
                payload_json TEXT DEFAULT '',
                ip_address TEXT DEFAULT '',
                user_agent TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type)",
            "CREATE INDEX IF NOT EXISTS idx_event_log_entity ON event_log(entity_type, entity_id)",
            "CREATE INDEX IF NOT EXISTS idx_event_log_at ON event_log(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_activities_visit_date ON activities(visit_date)",
            "CREATE INDEX IF NOT EXISTS idx_activities_deleted_at ON activities(deleted_at)",
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
            CREATE TABLE IF NOT EXISTS sod_store_sku_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                store_number INTEGER NOT NULL,
                change_date TEXT NOT NULL,
                old_status TEXT,
                new_status TEXT,
                change_type TEXT NOT NULL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sku, store_number, change_date, change_type)
            );
            CREATE INDEX IF NOT EXISTS idx_sssc_date ON sod_store_sku_changes(change_date DESC);
            CREATE INDEX IF NOT EXISTS idx_sssc_sku ON sod_store_sku_changes(sku);
            CREATE INDEX IF NOT EXISTS idx_sssc_type ON sod_store_sku_changes(change_type);
            CREATE INDEX IF NOT EXISTS idx_sod_inv_sku ON sod_inventory(sku);
            CREATE INDEX IF NOT EXISTS idx_sod_inv_date ON sod_inventory(snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_sod_inv_sku_date ON sod_inventory(sku, snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_sod_inv_store ON sod_inventory(store_number);
            CREATE INDEX IF NOT EXISTS idx_sod_runs_at ON sod_sync_runs(run_at DESC);
            CREATE INDEX IF NOT EXISTS idx_sod_changes_sku ON sod_listing_changes(sku);
            CREATE INDEX IF NOT EXISTS idx_sod_changes_date ON sod_listing_changes(change_date DESC);
            CREATE INDEX IF NOT EXISTS idx_sod_products_tracked ON sod_products(is_tracked);

            -- ======== CRM tables: territories, goals, HORECA accounts ========
            CREATE TABLE IF NOT EXISTS territories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                region TEXT DEFAULT '',
                rep_name TEXT DEFAULT '',
                color TEXT DEFAULT '#b22222',
                fsa_prefixes TEXT DEFAULT '',
                city_prefixes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sales_goals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                target_units INTEGER DEFAULT 0,
                target_revenue REAL DEFAULT 0,
                target_listings INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(scope, scope_key, period_start, period_end)
            );
            CREATE TABLE IF NOT EXISTS horeca_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                account_type TEXT DEFAULT 'restaurant',
                address TEXT DEFAULT '',
                city TEXT DEFAULT '',
                postal TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                email TEXT DEFAULT '',
                contact_name TEXT DEFAULT '',
                contact_title TEXT DEFAULT '',
                territory_id INTEGER,
                rep_name TEXT DEFAULT '',
                status TEXT DEFAULT 'prospect',
                priority TEXT DEFAULT 'Standard',
                lat REAL DEFAULT 0,
                lng REAL DEFAULT 0,
                last_visit TEXT,
                next_visit TEXT,
                products_carried TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (territory_id) REFERENCES territories(id)
            );
            CREATE INDEX IF NOT EXISTS idx_terr_code ON territories(code);
            CREATE INDEX IF NOT EXISTS idx_goals_scope ON sales_goals(scope, scope_key);
            CREATE INDEX IF NOT EXISTS idx_goals_period ON sales_goals(period_start, period_end);
            CREATE INDEX IF NOT EXISTS idx_horeca_territory ON horeca_accounts(territory_id);
            CREATE INDEX IF NOT EXISTS idx_horeca_status ON horeca_accounts(status);
            CREATE INDEX IF NOT EXISTS idx_horeca_city ON horeca_accounts(city);

            -- ======== SPRINT 3: CRM SYSTEM-OF-ACTION TABLES ========
            CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_number INTEGER,
                horeca_account_id INTEGER,
                sku TEXT NOT NULL,
                stage TEXT NOT NULL DEFAULT 'prospecting',
                probability INTEGER DEFAULT 10,
                expected_close_date TEXT,
                expected_units INTEGER DEFAULT 0,
                expected_revenue REAL DEFAULT 0,
                owner_rep TEXT DEFAULT '',
                next_action TEXT DEFAULT '',
                next_action_date TEXT,
                notes TEXT DEFAULT '',
                source TEXT DEFAULT 'manual',
                closed_at TIMESTAMP,
                closed_reason TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS rep_quotas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rep TEXT NOT NULL,
                quarter TEXT NOT NULL,
                target_activities INTEGER DEFAULT 0,
                target_visits INTEGER DEFAULT 0,
                target_new_listings INTEGER DEFAULT 0,
                target_units INTEGER DEFAULT 0,
                target_revenue REAL DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(rep, quarter)
            );
            CREATE TABLE IF NOT EXISTS visit_photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_id INTEGER,
                photo_url TEXT NOT NULL,
                caption TEXT DEFAULT '',
                photo_type TEXT DEFAULT 'shelf',
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (activity_id) REFERENCES activities(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS activity_sku_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_id INTEGER,
                sku TEXT NOT NULL,
                outcome TEXT NOT NULL,
                facings INTEGER DEFAULT 0,
                competitor_notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (activity_id) REFERENCES activities(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS daily_plan_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rep TEXT NOT NULL,
                plan_date TEXT NOT NULL,
                stops_json TEXT NOT NULL,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(rep, plan_date)
            );
            CREATE INDEX IF NOT EXISTS idx_deals_store ON deals(store_number);
            CREATE INDEX IF NOT EXISTS idx_deals_sku ON deals(sku);
            CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage);
            CREATE INDEX IF NOT EXISTS idx_deals_owner ON deals(owner_rep);
            CREATE INDEX IF NOT EXISTS idx_deals_next_action ON deals(next_action_date);
            CREATE INDEX IF NOT EXISTS idx_quotas_rep ON rep_quotas(rep, quarter);
            CREATE INDEX IF NOT EXISTS idx_vp_activity ON visit_photos(activity_id);
            CREATE INDEX IF NOT EXISTS idx_aso_activity ON activity_sku_outcomes(activity_id);
            CREATE INDEX IF NOT EXISTS idx_aso_sku ON activity_sku_outcomes(sku);
            CREATE INDEX IF NOT EXISTS idx_dp_rep_date ON daily_plan_cache(rep, plan_date);
        ''')
        migrate_cols = [
            ('stores', 'manager_name', "TEXT DEFAULT ''"), ('stores', 'asst_manager_name', "TEXT DEFAULT ''"),
            ('stores', 'manager_phone', "TEXT DEFAULT ''"), ('stores', 'store_email', "TEXT DEFAULT ''"),
            ('stores', 'producer', "TEXT DEFAULT ''"), ('stores', 'lat', "REAL DEFAULT 0"),
            ('stores', 'lng', "REAL DEFAULT 0"),
            ('stores', 'lcbo_store_id', "TEXT DEFAULT ''"),
            ('stores', 'territory_id', "INTEGER"),
            ('activities', 'producer', "TEXT DEFAULT ''"), ('activities', 'venue_type', "TEXT DEFAULT ''"),
            ('activities', 'follow_up_date', "TEXT DEFAULT ''"),
            ('activities', 'status_code', "INTEGER DEFAULT 0"),
            ('products', 'listing_status', "INTEGER DEFAULT 2"),
            ('products', 'listing_date', "TEXT DEFAULT ''"),
            ('products', 'delisting_date', "TEXT DEFAULT ''"),
            ('sod_products', 'category', "TEXT DEFAULT ''"),
            ('sod_products', 'category_group', "TEXT DEFAULT ''"),
            # Sprint 3 activity enrichment
            ('activities', 'outcome', "TEXT DEFAULT ''"),
            ('activities', 'duration_minutes', "INTEGER DEFAULT 0"),
            ('activities', 'rating', "INTEGER DEFAULT 0"),
            ('activities', 'next_action', "TEXT DEFAULT ''"),
            ('activities', 'next_action_date', "TEXT"),
            ('activities', 'rep', "TEXT DEFAULT ''"),
            ('activities', 'horeca_account_id', "INTEGER"),
            ('activities', 'lat', "REAL DEFAULT 0"),
            ('activities', 'lng', "REAL DEFAULT 0"),
            # Sprint 6: storage backbone
            ('activities', 'visit_date', "TEXT"),
            ('activities', 'deleted_at', "TIMESTAMP"),
            ('activities', 'updated_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]
        for table, col, coltype in migrate_cols:
            try:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            except Exception:
                pass
        # Append-only audit log
        db.execute('''
            CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                actor TEXT DEFAULT '',
                payload_json TEXT DEFAULT '',
                ip_address TEXT DEFAULT '',
                user_agent TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type)",
            "CREATE INDEX IF NOT EXISTS idx_event_log_entity ON event_log(entity_type, entity_id)",
            "CREATE INDEX IF NOT EXISTS idx_event_log_at ON event_log(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_activities_visit_date ON activities(visit_date)",
            "CREATE INDEX IF NOT EXISTS idx_activities_deleted_at ON activities(deleted_at)",
        ]:
            try:
                db.execute(idx)
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
    Query params: threshold (default 5), sku (optional), city (optional).

    Note: legacy endpoint backed by inventory_cache (SQLite scrape table). On
    Postgres production we surface SOD-driven reorder via /api/sod/reorder. We
    return an empty list (with explicit redirect note) instead of 500ing if the
    legacy table doesn't exist on this DB.
    """
    threshold = int(request.args.get('threshold', 5))
    sku_filter = request.args.get('sku', '').strip()
    city_filter = request.args.get('city', '').strip()

    if USE_POSTGRES:
        # Legacy table doesn't exist in Postgres production; redirect callers to /api/sod/reorder
        return jsonify({
            'threshold': threshold,
            'total_reorder_alerts': 0,
            'critical_count': 0,
            'high_count': 0,
            'medium_count': 0,
            'alerts': [],
            'note': 'Legacy endpoint (inventory_cache). Use /api/sod/reorder for SOD-driven low-stock alerts.',
            'generated_at': datetime.now().isoformat(),
        })

    # Find low-stock inventory entries (SQLite legacy path only)
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
        # Pick a seed: prefer one with valid coords so haversine clustering works.
        # Sprint 0 fix: previously could seed on lat=0 store, breaking distance calc.
        seed_store = None
        for s in store_list:
            if s['id'] not in assigned and s.get('lat') and s.get('lng'):
                seed_store = s
                break
        if seed_store is None:
            # Fall back to any unassigned store (city-grouping only)
            for s in store_list:
                if s['id'] not in assigned:
                    seed_store = s
                    break
        if not seed_store:
            break

        day_stores.append(seed_store)
        assigned.add(seed_store['id'])
        seed_city = seed_store.get('city', '')
        seed_has_coords = bool(seed_store.get('lat') and seed_store.get('lng'))

        # Fill rest of day with nearby stores (same city first, then within 15km).
        for s in store_list:
            if len(day_stores) >= stores_per_day:
                break
            if s['id'] in assigned:
                continue
            # Same city is the strongest signal regardless of coords
            if s.get('city') == seed_city:
                day_stores.append(s)
                assigned.add(s['id'])
            elif seed_has_coords and s.get('lat') and s.get('lng'):
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

# ------- SKU → brand mapping -------
# Keys are 7-char zero-padded SKUs (matches what SOD emits).
# NB Distillers is the PRIMARY paying client. Goenchi + Fratelli are
# Anu's secondary import portfolio — tracked separately in /anu-import.
SOD_TRACKED_SKUS = {
    # NB Distillers (PRIMARY paying client)
    '0020187': ('NB Distillers', 'Red Admiral Vodka'),
    '0022246': ('NB Distillers', 'Chak De Canadian Whisky'),
    # Anu Import portfolio (SECONDARY)
    '0046340': ('Goenchi', 'Goenchi Cashew Feni'),
    '0046343': ('Goenchi', 'Goenchi Coconut Feni'),
    '0046282': ('Fratelli', 'Fratelli Classic Shiraz'),
    '0046285': ('Fratelli', 'Fratelli Chenin Blanc'),
    '0046286': ('Fratelli', 'Fratelli Sauvignon Blanc'),
    '0046287': ('Fratelli', 'Fratelli Cabernet Sauvignon'),
}

# Client classification — drives whether a SKU appears in NB-primary views vs the
# /anu-import secondary section.
SOD_PRIMARY_BRAND = 'NB Distillers'
SOD_ANU_IMPORT_BRANDS = {'Goenchi', 'Fratelli'}


def sku_client_class(sku):
    """Return 'nb_primary' for NB Distillers SKUs, 'anu_import' for the rest."""
    brand, _ = SOD_TRACKED_SKUS.get(sku, ('', ''))
    return 'nb_primary' if brand == SOD_PRIMARY_BRAND else 'anu_import'


def primary_skus():
    return [s for s, (b, _) in SOD_TRACKED_SKUS.items() if b == SOD_PRIMARY_BRAND]


def anu_import_skus():
    return [s for s, (b, _) in SOD_TRACKED_SKUS.items() if b in SOD_ANU_IMPORT_BRANDS]

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

    def _toronto_now(self):
        """Return current time in America/Toronto. Falls back to UTC-5 if zoneinfo unavailable."""
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo('America/Toronto'))
        except Exception:
            # Conservative fallback: UTC-5 (EST). Off by 1h during DST but weekday usually still right.
            return datetime.utcnow() - timedelta(hours=5)

    def _filename_for_weekday(self, source, weekday_abbrev):
        """Build the SOD filename for a given source and weekday abbrev (MON/TUE/...)."""
        wd = weekday_abbrev.upper()
        if source == 'daily_a':
            return f'alldlyinventory{wd}.zip'
        elif source == 'daily_b':
            return f'Edlyinventory{self.agent_id}{wd}.zip'
        raise ValueError(f'Unknown source {source!r}')

    def latest_filename(self, source):
        """Today's filename based on America/Toronto weekday.

        LCBO uploads nightly (~02:00 ET). If we're before that, today's file
        may not be present yet — `download_option` walks back day-by-day to find
        the freshest available file.
        """
        wd = self._toronto_now().strftime('%a').upper()
        return self._filename_for_weekday(source, wd)

    def _url_for(self, source, fn):
        if source == 'daily_a':
            return f'{SOD_BASE}/downloads/general/12/{fn}'
        elif source == 'daily_b':
            return f'{SOD_BASE}/downloads/agent/{self.agent_id}/13/{fn}'
        raise ValueError(f'Unknown source {source!r}')

    def _peek_snapshot_date(self, zip_bytes):
        """Read the FIRST data row of the .dat inside a zip and return its YYYY-MM-DD.

        Used to validate freshness before accepting a candidate file.
        Returns '' on any error (caller treats as "unknown / accept reluctantly").
        """
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                members = zf.namelist()
                if not members:
                    return ''
                with zf.open(members[0]) as raw:
                    head = raw.read(64).decode('latin-1', errors='replace')
                if len(head) < 8:
                    return ''
                d = head[:8]
                if not d.isdigit():
                    return ''
                return f'{d[0:4]}-{d[4:6]}-{d[6:8]}'
        except Exception:
            return ''

    def download_option(self, source, filename=None, max_days_back=7, max_age_days=3):
        """Download a SOD file with multi-day walkback + freshness validation.

        Walks back up to `max_days_back` days from today (Toronto time). For each
        candidate filename: HTTPs it; if 200 with PK zip bytes, peeks at the first
        row's date; accepts if (today - snapshot) <= `max_age_days`. If we exhaust
        the walkback without finding a fresh file, returns the most recent file we
        DID find (even if older than max_age_days) so the app keeps working — but
        the caller should surface staleness via /api/sod/status freshness flags.

        Returns (zip_bytes, filename, snapshot_date_str | '').
        """
        self._ensure_logged_in()
        toronto_today = self._toronto_now().date()

        # If caller supplied an explicit filename, honor it (single attempt, no walkback).
        if filename:
            url = self._url_for(source, filename)
            r = self.session.get(url, timeout=self.timeout, stream=True)
            r.raise_for_status()
            content = r.content
            if not content or not content.startswith(b'PK'):
                raise RuntimeError(f"Did not receive zip data from {url}")
            snap = self._peek_snapshot_date(content)
            return content, filename, snap

        # Walk back day-by-day, prefer fresher snapshots.
        last_resort = None  # (content, fn, snap) — kept in case nothing fresh found
        tried = []
        for offset in range(0, max_days_back):
            d = toronto_today - timedelta(days=offset)
            wd = d.strftime('%a').upper()
            fn = self._filename_for_weekday(source, wd)
            # Avoid duplicate attempts when the same weekday appears twice in 7 days
            # (it doesn't in 7-day window, but defense in depth).
            if fn in [t[0] for t in tried]:
                continue
            url = self._url_for(source, fn)
            try:
                r = self.session.get(url, timeout=self.timeout, stream=True)
            except Exception as e:
                tried.append((fn, f'request_error:{type(e).__name__}'))
                continue
            if r.status_code == 404:
                tried.append((fn, '404'))
                continue
            try:
                r.raise_for_status()
            except Exception:
                tried.append((fn, f'http_{r.status_code}'))
                continue
            content = r.content
            if not content or not content.startswith(b'PK'):
                tried.append((fn, 'not_zip'))
                continue
            snap = self._peek_snapshot_date(content)
            tried.append((fn, f'200,snap={snap}'))
            # Accept if snapshot is fresh
            if snap:
                try:
                    snap_d = datetime.strptime(snap, '%Y-%m-%d').date()
                    age = (toronto_today - snap_d).days
                    if age <= max_age_days:
                        return content, fn, snap
                except Exception:
                    pass
            # Keep as last_resort if we don't find anything fresh
            if last_resort is None:
                last_resort = (content, fn, snap)

        if last_resort is not None:
            print(f'[SOD] WARNING: no fresh file found in {max_days_back}-day walkback. '
                  f'Returning stale file. Tried: {tried}')
            return last_resort
        raise RuntimeError(f'[SOD] No SOD file found in {max_days_back}-day walkback. Tried: {tried}')

    def download_zip_bytes(self, source, filename=None):
        """Download and return the raw zip bytes + final filename + snapshot date.

        Kept small in memory (~9MB for Daily A). The .dat inside is ~75MB
        uncompressed — we NEVER materialize that blob; callers must stream
        the member via stream_parse_sod_zip() instead.

        Backward-compatible: returns (bytes, filename) if 2 values are unpacked,
        but new code can unpack (bytes, filename, snapshot_date).
        """
        return self.download_option(source, filename=filename)


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


def stream_parse_sod_zip(zip_bytes, tracked_skus, keep_all_rows=False, progress_every=200_000):
    """Streaming parser + aggregator for a SOD .zip download.

    OPTIMIZED for low-memory hosts (Render Starter 512MB):
      - Only builds per-SKU aggregates for TRACKED SKUs (was: all 21k SKUs).
        Untracked SKUs only count toward the global total + dates_seen.
      - Logs progress every `progress_every` rows so we can see where ingest
        stalls (was: silent until completion).
      - Caps `rows_to_persist` to tracked-only when keep_all_rows=False.

    Returns dict with: dat_name, total, per_sku_by_date, rows_to_persist,
    dates_seen, tracked_row_count, untracked_row_count, untracked_sku_count.
    """
    per_sku_by_date = {}   # {date: {sku: {'name', 'status_counts', 'store_count', 'total_on_hand'}}}
    rows_to_persist = []   # only tracked rows (or all, for Daily B)
    dates_seen = set()
    untracked_skus_seen = set()  # for stats only
    total = 0
    tracked_row_count = 0
    untracked_row_count = 0
    last_logged = 0
    started = datetime.utcnow()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        members = zf.namelist()
        if not members:
            raise RuntimeError("Zip is empty")
        dat_name = members[0]
        print(f"[SOD-parse] streaming {dat_name} ({len(zip_bytes):,}B compressed)…")
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
                is_tracked = row['sku'] in tracked_skus

                if is_tracked or keep_all_rows:
                    # Build per-SKU aggregates for tracked SKUs (and all SKUs in keep-all mode)
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
                else:
                    # Untracked SKU on a daily_a run — just count, don't aggregate
                    # (saves ~95% of dict memory on the global file)
                    untracked_row_count += 1
                    untracked_skus_seen.add(row['sku'])

                if is_tracked:
                    tracked_row_count += 1
                if keep_all_rows or is_tracked:
                    rows_to_persist.append(row)

                # Progress every N rows (about every 1-2s on Render Starter)
                if total - last_logged >= progress_every:
                    elapsed = (datetime.utcnow() - started).total_seconds()
                    rate = total / max(elapsed, 0.001)
                    print(f"[SOD-parse] {total:>9,} rows ({elapsed:.1f}s, {rate:,.0f}/s, "
                          f"tracked={tracked_row_count}, persist_buf={len(rows_to_persist)})")
                    last_logged = total

    elapsed = (datetime.utcnow() - started).total_seconds()
    print(f"[SOD-parse] DONE: {total:,} rows in {elapsed:.1f}s "
          f"(tracked={tracked_row_count}, untracked={untracked_row_count}, "
          f"untracked_skus={len(untracked_skus_seen)})")

    return {
        'dat_name': dat_name,
        'total': total,
        'per_sku_by_date': per_sku_by_date,
        'rows_to_persist': rows_to_persist,
        'dates_seen': dates_seen,
        'tracked_row_count': tracked_row_count,
        'untracked_row_count': untracked_row_count,
        'untracked_sku_count': len(untracked_skus_seen),
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
        # download_zip_bytes now walks back up to 7 days and validates freshness.
        print(f"[SOD-{source}] step 1/8: downloading zip…")
        client = client or SODClient()
        download_result = client.download_zip_bytes(source, filename=filename)
        # Tolerate both old (2-tuple) and new (3-tuple) signatures
        if len(download_result) == 3:
            zip_bytes, zip_name, peeked_snapshot = download_result
        else:
            zip_bytes, zip_name = download_result
            peeked_snapshot = ''
        print(f"[SOD-{source}] step 1/8 done: {zip_name} ({len(zip_bytes):,}B)")

        # 2) Stream-parse directly from the zip. NEVER materializes the 75MB .dat text
        #    or the 1.5M-row list. Only keeps small aggregates + tracked rows.
        print(f"[SOD-{source}] step 2/8: parsing rows (will log progress)…")
        keep_all = (source != 'daily_a')  # Daily B is already agent-filtered (~1,400 rows)
        parsed = stream_parse_sod_zip(zip_bytes, SOD_TRACKED_SKUS, keep_all_rows=keep_all)
        # Free the zip bytes ASAP
        del zip_bytes
        gc.collect()
        print(f"[SOD-{source}] step 2/8 done: {parsed['total']:,} rows parsed, "
              f"{parsed['tracked_row_count']} tracked")

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

        # 4) Pull prior sod_products state to compute SKU-level listing changes.
        # OPTIMIZATION: only fetch the SKUs that appear in this snapshot (was: full
        # 21k-row table scan every sync). Cuts memory + query time on Render Starter.
        print(f"[SOD-{source}] step 3/8: loading prior status for {len(per_sku)} SKUs…")
        prior = {}
        if per_sku:
            sku_list = list(per_sku.keys())
            if USE_POSTGRES:
                cur.execute(
                    "SELECT sku, current_status FROM sod_products WHERE sku = ANY(%s)",
                    (sku_list,),
                )
            else:
                ph_list = ','.join(['?'] * len(sku_list))
                cur.execute(
                    f"SELECT sku, current_status FROM sod_products WHERE sku IN ({ph_list})",
                    sku_list,
                )
            prior = {row[0]: row[1] for row in cur.fetchall()}
        print(f"[SOD-{source}] step 4/8: computing SKU-level changes…")
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

        print(f"[SOD-{source}] step 4/8 done: {len(change_inserts)} SKU-level changes")
        # 4b) PER-STORE per-SKU change detection for our tracked SKUs.
        # Answers "which stores added Red Admiral last week" — the rep workflow.
        # Compare current snapshot per-(sku,store) status to the most-recent PRIOR
        # snapshot for that SKU. Insert into sod_store_sku_changes (idempotent via
        # UNIQUE constraint).
        store_change_inserts = []  # (sku, store_number, change_date, old_status, new_status, change_type)
        tracked_in_snapshot = {sku: agg for sku, agg in per_sku.items() if sku in SOD_TRACKED_SKUS}
        for tracked_sku in tracked_in_snapshot:
            # Find the previous snapshot date for this SKU (the one BEFORE today's)
            if USE_POSTGRES:
                cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku = %s AND snapshot_date < %s",
                    (tracked_sku, snapshot_date),
                )
                prior_snap = cur.fetchone()[0]
            else:
                cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku = ? AND snapshot_date < ?",
                    (tracked_sku, snapshot_date),
                )
                prior_snap = cur.fetchone()[0]
            # Build prior {store -> status}
            prior_per_store = {}
            if prior_snap:
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number, status FROM sod_inventory "
                        "WHERE sku = %s AND snapshot_date = %s",
                        (tracked_sku, prior_snap),
                    )
                else:
                    cur.execute(
                        "SELECT store_number, status FROM sod_inventory "
                        "WHERE sku = ? AND snapshot_date = ?",
                        (tracked_sku, prior_snap),
                    )
                prior_per_store = {r[0]: r[1] for r in cur.fetchall()}
            # Build current {store -> status} from the rows we just streamed
            current_per_store = {
                r['store_number']: r['status']
                for r in latest_rows if r['sku'] == tracked_sku
            }
            # Diff: what's new, what changed, what disappeared
            for store, new_st in current_per_store.items():
                old_st = prior_per_store.get(store)
                if old_st is None:
                    # Store newly carrying this SKU
                    store_change_inserts.append(
                        (tracked_sku, store, snapshot_date, None, new_st, 'NEW_LISTING'),
                    )
                elif old_st != new_st:
                    if new_st == 'L' and old_st in ('D', 'F'):
                        store_change_inserts.append(
                            (tracked_sku, store, snapshot_date, old_st, new_st, 'RELISTED'),
                        )
                    elif new_st in ('D', 'F') and old_st == 'L':
                        store_change_inserts.append(
                            (tracked_sku, store, snapshot_date, old_st, new_st, 'DELISTED'),
                        )
                    else:
                        store_change_inserts.append(
                            (tracked_sku, store, snapshot_date, old_st, new_st, 'STATUS_FLIP'),
                        )
            for store, old_st in prior_per_store.items():
                if store not in current_per_store:
                    # Store dropped this SKU entirely (no row at all)
                    store_change_inserts.append(
                        (tracked_sku, store, snapshot_date, old_st, None, 'DROPPED'),
                    )

        # 5) Upsert sod_inventory
        # latest_rows is already filtered correctly by the streaming parser:
        #   - Daily A: only tracked-SKU rows (~155)
        #   - Daily B: all rows (~1,400, already agent-filtered server-side)
        print(f"[SOD-{source}] step 5/8: upserting {len(latest_rows)} sod_inventory rows…")
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

        print(f"[SOD-{source}] step 5/8 done")
        # 6) Upsert sod_products rollup
        print(f"[SOD-{source}] step 6/8: upserting {len(per_sku)} sod_products rollups…")
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

        print(f"[SOD-{source}] step 6/8 done")
        # 7) Insert detected listing changes (SKU-level)
        print(f"[SOD-{source}] step 7/8: writing {len(change_inserts)} SKU + "
              f"{len(store_change_inserts)} per-store change events…")
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

        # 7b) Insert per-(store,sku) changes — idempotent UPSERT on natural key.
        if store_change_inserts:
            if USE_POSTGRES:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO sod_store_sku_changes
                       (sku, store_number, change_date, old_status, new_status, change_type)
                       VALUES %s
                       ON CONFLICT (sku, store_number, change_date, change_type) DO NOTHING""",
                    store_change_inserts,
                )
            else:
                cur.executemany(
                    """INSERT INTO sod_store_sku_changes
                       (sku, store_number, change_date, old_status, new_status, change_type)
                       VALUES (?,?,?,?,?,?)
                       ON CONFLICT(sku, store_number, change_date, change_type) DO NOTHING""",
                    store_change_inserts,
                )

        print(f"[SOD-{source}] step 7/8 done")
        # 8) Also stamp a summary inventory_history row per tracked SKU (for legacy views)
        print(f"[SOD-{source}] step 8/8: stamping inventory_history summaries…")
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
        # Email the user — SOD ingest failures are critical, the rep workflow depends on this data.
        try:
            send_alert(
                subject=f"SOD sync FAILED: {source}",
                body=(
                    f"SOD ingest from source '{source}' failed at {datetime.utcnow().isoformat()}Z.\n\n"
                    f"Error:\n{err[:1500]}\n\n"
                    f"This means the rep dashboard, route planner, and gap reports may show stale data\n"
                    f"until the next successful sync. The daily health check at 06:00 / 14:00 ET will\n"
                    f"attempt auto-recovery; you can also POST /api/sod/sync to retry manually."
                ),
                level='critical',
            )
        except Exception as _:
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


def _sod_run_if_stale(sources, max_age_hours=6):
    """Fire a sync if the last successful sync is older than max_age_hours
    OR the data itself is at least 1 day old (fixed: was '> 1' which missed
    the common case of pulling yesterday's file at 03 ET because LCBO hadn't
    published today's yet, then never re-trying).

    Catch-up jobs at 07/12/18 ET use this so we don't re-run unnecessarily
    when the 03 ET main sync got TODAY's file. But if 03 ET only got
    yesterday's file (LCBO publishes ~01-02 ET, so race is real), we WILL
    re-try at 07 ET, which is when LCBO is reliably published.
    """
    run_age = _last_successful_run_age_hours_safe()
    data_age = _sod_data_age_days()
    # Fire if:
    #   - no successful run yet
    #   - last run was longer ago than threshold
    #   - data is at least 1 day old (we want today's data)
    #   - AND we haven't tried in the last hour (avoid hammering)
    has_recent_attempt = run_age is not None and run_age < 1.0
    stale_data = data_age is None or data_age >= 1
    needs_sync = (
        run_age is None
        or run_age > max_age_hours
        or (stale_data and not has_recent_attempt)
    )
    if needs_sync:
        print(f'[SOD] catch-up firing (run_age={run_age}h, data_age={data_age}d, threshold={max_age_hours}h)')
        _sod_sync_worker(sources)
    else:
        print(f'[SOD] catch-up skipped (run {run_age:.1f}h ago, data {data_age}d old — fresh enough)')


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
    """Last sync runs + counts of ingested data + configuration check + freshness."""
    configured = bool(SOD_USER and SOD_PASSWORD)
    # Filter out orphaned 'running' rows older than 6h — they're crashes.
    if USE_POSTGRES:
        rows_query = (
            "SELECT id, run_at, source, file_name, snapshot_date, status, total_rows, "
            "anu_rows, new_listings, new_delistings, duration_seconds, error "
            "FROM sod_sync_runs "
            "WHERE NOT (status='running' AND run_at < NOW() - INTERVAL '6 hours') "
            "ORDER BY run_at DESC LIMIT 20"
        )
    else:
        rows_query = (
            "SELECT id, run_at, source, file_name, snapshot_date, status, total_rows, "
            "anu_rows, new_listings, new_delistings, duration_seconds, error "
            "FROM sod_sync_runs "
            "WHERE NOT (status='running' AND datetime(run_at) < datetime('now','-6 hours')) "
            "ORDER BY run_at DESC LIMIT 20"
        )
    rows = db_fetchall(rows_query)
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
        'freshness': _sod_freshness(),
        'scheduler_running': _sod_scheduler_running(),
    })


@app.route('/api/sod/refresh-snapshot', methods=['POST'])
def api_sod_refresh_snapshot():
    """Force a SOD sync RIGHT NOW with multi-day walkback enabled.

    Use this when you suspect the data is stale. Synchronously triggers a
    sync (in a thread) and returns 202; check /api/sod/status after ~30s.
    """
    if not SOD_USER or not SOD_PASSWORD:
        return jsonify({'error': 'SOD_USER / SOD_PASSWORD env vars not configured'}), 400
    if _sod_sync_lock.locked():
        return jsonify({'status': 'already_running'}), 202
    sources = ['daily_a', 'daily_b']
    body = request.get_json(silent=True) or {}
    if body.get('sources'):
        sources = [s for s in body['sources'] if s in ('daily_a', 'daily_b')]
    # Cleanup orphans first
    _cleanup_orphaned_sod_runs(max_age_hours=1)
    start_sod_sync_async(sources)
    return jsonify({
        'status': 'started',
        'sources': sources,
        'note': 'walkback up to 7 days; check /api/sod/status in 60-90s for freshness',
    }), 202


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
@cached_response(ttl_seconds=300, key_args=())
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
    the latest available snapshot (preserves the window length) AND surfaces this
    in the response via `window_shifted: true`, with `requested_window` echoing
    what the caller asked for. This keeps the API honest — the user knows the
    data is from a different window than they requested.
    """
    ph = _sod_ph()
    requested_start = start_date.isoformat() if isinstance(start_date, (datetime,)) else str(start_date)
    requested_end = end_date.isoformat() if isinstance(end_date, (datetime,)) else str(end_date)
    start = requested_start
    end = requested_end
    window_shifted = False

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
                window_shifted = True
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
        'window': {
            'start': start,
            'end': end,
            'latest_snapshot': str(latest_date) if latest_date else None,
            'window_shifted': window_shifted,
            'requested_window': {'start': requested_start, 'end': requested_end},
        },
        'freshness': _sod_freshness(),
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


def _toronto_today():
    """Today's date in America/Toronto."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo('America/Toronto')).date()
    except Exception:
        return (datetime.utcnow() - timedelta(hours=5)).date()


@app.route('/api/reports/daily', methods=['GET'])
def api_report_daily():
    day_str = request.args.get('date')
    try:
        day = datetime.strptime(day_str, '%Y-%m-%d').date() if day_str else _toronto_today()
    except ValueError:
        day = _toronto_today()
    return jsonify(_sod_summary_for_range(day, day))


@app.route('/api/reports/weekly', methods=['GET'])
def api_report_weekly():
    """Mon-Sun week. ?end=YYYY-MM-DD (any day in target week) or omit for current week.

    Mode toggle: ?mode=rolling7 returns the legacy rolling-7-day window for
    callers that depend on it.
    """
    end_str = request.args.get('end')
    mode = request.args.get('mode', 'mon-sun').lower()
    try:
        anchor = datetime.strptime(end_str, '%Y-%m-%d').date() if end_str else _toronto_today()
    except ValueError:
        anchor = _toronto_today()
    if mode == 'rolling7':
        end = anchor
        start = end - timedelta(days=6)
    else:
        # Mon-Sun aligned week containing the anchor date
        # weekday(): Mon=0 .. Sun=6
        start = anchor - timedelta(days=anchor.weekday())  # this Mon
        end = start + timedelta(days=6)                    # this Sun
    return jsonify(_sod_summary_for_range(start, end))


@app.route('/api/reports/monthly', methods=['GET'])
def api_report_monthly():
    end_str = request.args.get('end')
    try:
        end = datetime.strptime(end_str, '%Y-%m-%d').date() if end_str else _toronto_today()
    except ValueError:
        end = _toronto_today()
    start = end.replace(day=1)
    return jsonify(_sod_summary_for_range(start, end))


@app.route('/api/reports/rep', methods=['GET'])
def api_report_rep():
    """Per-rep performance: stores assigned, products carried, gap count, delisting risk."""
    latest = db_fetchone("SELECT MAX(snapshot_date) AS d FROM sod_inventory")
    snapshot_date = (latest['d'] if isinstance(latest, dict) else latest[0]) if latest else None

    # All reps (from stores table) — TRIM + LOWER de-dupe variants of the same name.
    # Sprint 0 fix: previously case-sensitive → "John Smith" / "JOHN SMITH" / " john smith "
    # were 3 separate "reps" each with 1 store. Now collapsed.
    # Display name is "first variant we see" via MIN(rep) per group.
    rep_rows = db_fetchall(
        "SELECT MIN(TRIM(rep)) AS rep, COUNT(*) AS store_count FROM stores "
        "WHERE rep IS NOT NULL AND TRIM(rep) <> '' "
        "GROUP BY LOWER(TRIM(rep)) ORDER BY store_count DESC"
    )
    out = []
    for rr in rep_rows:
        rd = row_to_dict(rr)
        rep_name = rd['rep']
        # Per-rep: how many of his stores are carrying each tracked SKU.
        # FIXED Sprint 0: filter status='L' so delisting/delisted stores are NOT
        # counted as "carrying" — that bug silently understated gap counts.
        # Also case/whitespace-insensitive rep match.
        per_sku = []
        if snapshot_date:
            for sku, (brand, pname) in SOD_TRACKED_SKUS.items():
                carrying = db_fetchone(
                    "SELECT COUNT(*) AS c FROM sod_inventory i "
                    "JOIN stores s ON s.store_number = i.store_number "
                    "WHERE LOWER(TRIM(s.rep)) = LOWER(TRIM(?)) AND i.sku = ? "
                    "AND i.snapshot_date = ? AND i.status = 'L'",
                    [rep_name, sku, str(snapshot_date)],
                )
                carrying_cnt = (row_to_dict(carrying) or {}).get('c', 0)
                delisting = db_fetchone(
                    "SELECT COUNT(*) AS c FROM sod_inventory i "
                    "JOIN stores s ON s.store_number = i.store_number "
                    "WHERE LOWER(TRIM(s.rep)) = LOWER(TRIM(?)) AND i.sku = ? "
                    "AND i.snapshot_date = ? AND i.status IN ('D','F')",
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
    """Return hours since last successful sync RUN, or None if never synced.

    NOTE: This is the age of the last sync ATTEMPT, NOT the age of the data
    inside that sync. A sync that successfully ingested 7-day-old data still
    reports "0 hours" here. For data freshness, use _sod_data_age_days().
    """
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


def _max_snapshot_date():
    """Get MAX(snapshot_date) from sod_inventory using a dedicated connection.

    Safe to call outside Flask request context (e.g. from startup/scheduler).
    Returns a date or None.
    """
    try:
        conn = _sod_get_conn()
        cur = conn.cursor()
        cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory")
        r = cur.fetchone()
        cur.close()
        conn.close()
        if not r:
            return None
        snap = r[0]
        if snap is None:
            return None
        if isinstance(snap, str):
            try:
                return datetime.strptime(snap, '%Y-%m-%d').date()
            except Exception:
                return None
        if hasattr(snap, 'date'):
            return snap.date()
        return snap
    except Exception:
        return None


def _sod_data_age_days():
    """Return days between today (Toronto) and the freshest snapshot in sod_inventory.

    This is the TRUE freshness — what the user actually cares about.
    Returns None if no data ingested yet.
    """
    snap = _max_snapshot_date()
    if snap is None:
        return None
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo('America/Toronto')).date()
    except Exception:
        today = (datetime.utcnow() - timedelta(hours=5)).date()
    return (today - snap).days


def _last_successful_run_age_hours_safe():
    """Same as _sod_last_successful_sync_age_hours but Flask-context-free."""
    try:
        conn = _sod_get_conn()
        cur = conn.cursor()
        cur.execute("SELECT run_at FROM sod_sync_runs WHERE status='success' ORDER BY run_at DESC LIMIT 1")
        r = cur.fetchone()
        cur.close()
        conn.close()
        if not r:
            return None
        val = r[0]
        if isinstance(val, str):
            try:
                val = datetime.fromisoformat(val)
            except Exception:
                try:
                    val = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    return None
        # Strip tz if present so subtraction works
        if hasattr(val, 'tzinfo') and val.tzinfo is not None:
            val = val.replace(tzinfo=None)
        return (datetime.utcnow() - val).total_seconds() / 3600.0
    except Exception:
        return None


def _sod_freshness():
    """Return a freshness summary dict used by every report response.

    Keys:
      latest_snapshot: 'YYYY-MM-DD' or None
      snapshot_age_days: int or None
      is_stale: bool (True if age > 1 day — emails an alert at the next health check)
      last_run_age_hours: float or None
    """
    snap = _max_snapshot_date()
    age_days = None
    if snap is not None:
        try:
            from zoneinfo import ZoneInfo
            today = datetime.now(ZoneInfo('America/Toronto')).date()
        except Exception:
            today = (datetime.utcnow() - timedelta(hours=5)).date()
        age_days = (today - snap).days
    last_run = _last_successful_run_age_hours_safe()
    return {
        'latest_snapshot': snap.isoformat() if snap else None,
        'snapshot_age_days': age_days,
        'is_stale': (age_days is not None and age_days > 1),
        'last_run_age_hours': round(last_run, 2) if last_run is not None else None,
    }


def _cleanup_orphaned_sod_runs(max_age_hours=6):
    """Mark orphaned 'running' rows as failed if they've been sitting > max_age_hours.

    These are crashes — the process died mid-sync and never updated the row.
    Without this cleanup, /api/sod/status shows misleading 'running' rows forever.
    `max_age_hours` is an int we control (no SQL-injection risk via f-string).
    """
    try:
        h = int(max_age_hours)  # defend against type confusion
        if USE_POSTGRES:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute(
                f"UPDATE sod_sync_runs SET status='failed', "
                f"error=COALESCE(error,'') || ' [auto-cleaned: orphaned > {h}h]' "
                f"WHERE status='running' AND run_at < NOW() - INTERVAL '{h} hours'"
            )
            n = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
        else:
            db = sqlite3.connect(DB_PATH)
            cur = db.execute(
                f"UPDATE sod_sync_runs SET status='failed', "
                f"error=COALESCE(error,'') || ' [auto-cleaned: orphaned > {h}h]' "
                f"WHERE status='running' AND datetime(run_at) < datetime('now', '-{h} hours')"
            )
            n = cur.rowcount
            db.commit()
            db.close()
        if n:
            print(f'[SOD] cleaned up {n} orphaned running rows (> {h}h old)')
        return n
    except Exception as e:
        print(f'[SOD] orphan cleanup failed: {e}')
        return 0


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
        # MAIN: 03:00 ET — primary run, after LCBO's ~01:30-02:30 ET upload window
        sched.add_job(
            lambda: _sod_sync_worker(['daily_a', 'daily_b']),
            CronTrigger(hour=3, minute=0),
            id='sod_main_sync',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 6,
        )
        # CATCH-UP #1: 07:00 ET — if 03:00 missed (Render cold boot, LCBO late upload).
        # Uses the same _sod_sync_worker which is idempotent via ON CONFLICT upserts.
        sched.add_job(
            lambda: _sod_run_if_stale(['daily_a', 'daily_b'], max_age_hours=6),
            CronTrigger(hour=7, minute=0),
            id='sod_catchup_morning',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 4,
        )
        # CATCH-UP #2: 12:00 ET — midday check. Only fires if we still don't have
        # today's data.
        sched.add_job(
            lambda: _sod_run_if_stale(['daily_a', 'daily_b'], max_age_hours=12),
            CronTrigger(hour=12, minute=0),
            id='sod_catchup_noon',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 4,
        )
        # CATCH-UP #3: 18:00 ET — evening check. Last chance to pull today's file.
        sched.add_job(
            lambda: _sod_run_if_stale(['daily_a', 'daily_b'], max_age_hours=18),
            CronTrigger(hour=18, minute=0),
            id='sod_catchup_evening',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 4,
        )
        sched.start()
        _sod_scheduler = sched
        next_run = sched.get_job("sod_main_sync").next_run_time
        print(f'[SOD] Scheduler started — main @ 03:00 ET, catch-ups @ 07:00/12:00/18:00 ET '
              f'(next: {next_run})')

        # --- Startup catch-up: if no sync in > 6h OR snapshot > 1 day old, fire immediately ---
        def _catchup_if_stale():
            try:
                import time as _t
                _t.sleep(30)  # let DB + app finish warming
                run_age = _last_successful_run_age_hours_safe()
                data_age = _sod_data_age_days()
                should_catchup = (
                    run_age is None
                    or run_age > 6
                    or data_age is None
                    or data_age > 1
                )
                if should_catchup:
                    print(
                        f'[SOD] startup catch-up firing '
                        f'(run_age={run_age}h, data_age={data_age}d)'
                    )
                    _sod_sync_worker(['daily_a', 'daily_b'])
                else:
                    print(f'[SOD] fresh (run {run_age:.1f}h ago, data {data_age}d old) — no catch-up')
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
    """Lightweight health check: is the DATA fresh? For monitoring.

    Returns:
      200 + status='healthy' if snapshot is <= 1 day old.
      503 + status='stale' if snapshot is > 1 day old.
      503 + status='never_synced' if no data ingested yet.
    """
    fresh_info = _sod_freshness()
    age_days = fresh_info['snapshot_age_days']
    age_hours = _sod_last_successful_sync_age_hours()
    if age_days is None:
        return jsonify({
            'status': 'never_synced',
            'configured': bool(SOD_USER and SOD_PASSWORD),
            'last_run_age_hours': round(age_hours, 2) if age_hours is not None else None,
        }), 503
    is_fresh = age_days <= 1
    return jsonify({
        'status': 'healthy' if is_fresh else 'stale',
        'snapshot_date': fresh_info['latest_snapshot'],
        'snapshot_age_days': age_days,
        'last_run_age_hours': round(age_hours, 2) if age_hours is not None else None,
        'scheduler_running': _sod_scheduler_running(),
        'configured': bool(SOD_USER and SOD_PASSWORD),
    }), 200 if is_fresh else 503


@app.route('/healthz', methods=['GET'])
def api_healthz():
    """Standard health probe used by Render / uptime monitors. 503 if data > 1d stale."""
    fresh = _sod_freshness()
    age_days = fresh['snapshot_age_days']
    healthy = age_days is not None and age_days <= 1
    return jsonify({
        'status': 'healthy' if healthy else 'unhealthy',
        'build': 'finder-stale-1d-v3',
        **fresh,
    }), 200 if healthy else 503


# ============================================================================
# DAILY AGENT — automated health check + auto-recovery, runs every morning
# ============================================================================

def _daily_health_check(auto_recover=True):
    """Run a comprehensive system health check + (optionally) auto-recover.

    Checks:
      1. SOD snapshot freshness (age <= 2 days)
      2. Last successful sync per source
      3. Tracked-SKU data integrity (each of our 8 SKUs has data in latest snapshot)
      4. No stuck-running rows
      5. Per-store change tracking (sod_store_sku_changes has data)
      6. Brand endpoints return non-zero counts

    Auto-recovery actions when checks fail:
      - SOD stale → trigger _sod_sync_worker(['daily_a','daily_b'])
      - Stuck running → cleanup orphaned rows
      - Missing per-store changes → run _backfill_store_sku_changes()

    Returns dict: {checks, recovered, healthy, summary}.
    Used by scheduler + GET /api/admin/daily-health-check.
    """
    import time
    started = datetime.utcnow()
    checks = []
    recovered = []

    def check(name, ok, detail=''):
        checks.append({'name': name, 'ok': bool(ok), 'detail': detail})

    # 1. SOD freshness
    fresh = _sod_freshness()
    age = fresh.get('snapshot_age_days')
    sod_fresh = age is not None and age <= 2
    check('sod_snapshot_fresh', sod_fresh,
          f'snapshot_date={fresh.get("latest_snapshot")} age_days={age}')
    if not sod_fresh and auto_recover and SOD_USER and SOD_PASSWORD:
        try:
            _cleanup_orphaned_sod_runs(max_age_hours=1)
            if not _sod_sync_lock.locked():
                start_sod_sync_async(['daily_a', 'daily_b'])
                recovered.append('triggered_sod_refresh')
        except Exception as e:
            recovered.append(f'sod_refresh_failed:{e}')

    # 2. Last successful sync per source within 24h (lowered from 36h — user wants
    #    to know if data goes stale > 1 day)
    for source in ('daily_a', 'daily_b'):
        row = db_fetchone(
            "SELECT MAX(run_at) FROM sod_sync_runs "
            "WHERE source = ? AND status = 'success'",
            [source],
        )
        v = (row_to_dict(row) if row and not isinstance(row, dict) else row) if row else None
        # Get the timestamp value
        last_at = None
        if row:
            try:
                last_at = list(row.values())[0] if isinstance(row, dict) else row[0]
            except Exception:
                last_at = None
        if isinstance(last_at, str):
            try:
                last_at = datetime.fromisoformat(last_at)
            except Exception:
                last_at = None
        if last_at and hasattr(last_at, 'tzinfo') and last_at.tzinfo:
            last_at = last_at.replace(tzinfo=None)
        hours = ((datetime.utcnow() - last_at).total_seconds() / 3600) if last_at else None
        check(f'last_success_{source}_within_24h',
              hours is not None and hours <= 24,
              f'hours_since_last_success={round(hours,1) if hours is not None else None}')

    # 3. Each tracked SKU has data in its OWN latest snapshot (within last 3 days).
    # Why per-SKU: daily_b is agent-only (VINETER) and doesn't contain Anu SKUs.
    # Globally MAX(snapshot_date) might be daily_b's date with 0 Anu rows. We need
    # to ask "does this SKU have a recent snapshot of its own?"
    today = _toronto_today()
    for sku, (brand, name) in SOD_TRACKED_SKUS.items():
        # Find the latest snapshot that has THIS SKU
        row = db_fetchone(
            "SELECT MAX(snapshot_date), COUNT(*) FROM sod_inventory WHERE sku = ?",
            [sku],
        )
        if row:
            vals = list(row.values()) if isinstance(row, dict) else row
            sku_latest, total_rows = vals[0], vals[1]
        else:
            sku_latest, total_rows = None, 0
        # Convert sku_latest to date for age calc
        sku_latest_date = None
        if sku_latest:
            try:
                if isinstance(sku_latest, str):
                    sku_latest_date = datetime.strptime(sku_latest, '%Y-%m-%d').date()
                elif hasattr(sku_latest, 'date'):
                    sku_latest_date = sku_latest.date()
                else:
                    sku_latest_date = sku_latest
            except Exception:
                pass
        sku_age = (today - sku_latest_date).days if sku_latest_date else None
        # Healthy if SKU has data within last 1 day (lowered from 3 — user wants
        # tighter alerting on stale data)
        ok = total_rows > 0 and sku_age is not None and sku_age <= 1
        check(f'tracked_sku_data_{sku}', ok,
              f'{brand} {name}: latest_snapshot={sku_latest} age={sku_age}d total_rows={total_rows}')

    # 4. No stuck-running rows older than 6h
    row = db_fetchone(
        "SELECT COUNT(*) FROM sod_sync_runs WHERE status = 'running' "
        "AND run_at < " + ("NOW() - INTERVAL '6 hours'" if USE_POSTGRES else "datetime('now', '-6 hours')")
    )
    stuck = (list(row.values())[0] if isinstance(row, dict) else row[0]) if row else 0
    check('no_stuck_running_rows', stuck == 0, f'{stuck} stuck')
    if stuck > 0 and auto_recover:
        n = _cleanup_orphaned_sod_runs(max_age_hours=1)
        recovered.append(f'cleaned_{n}_stuck_runs')

    # 5. Per-store changes table has data
    row = db_fetchone("SELECT COUNT(*) FROM sod_store_sku_changes")
    sssc_count = (list(row.values())[0] if isinstance(row, dict) else row[0]) if row else 0
    has_store_changes = sssc_count > 0
    check('per_store_changes_present', has_store_changes,
          f'{sssc_count} change events in DB')
    if not has_store_changes and auto_recover:
        try:
            n = _backfill_store_sku_changes()
            recovered.append(f'backfilled_{n}_store_changes')
        except Exception as e:
            recovered.append(f'backfill_failed:{e}')

    # 6. Brand endpoints return non-zero
    tracked_count = sum(1 for _ in SOD_TRACKED_SKUS.items())
    check('tracked_sku_count', tracked_count >= 1, f'{tracked_count} tracked SKUs configured')

    duration = (datetime.utcnow() - started).total_seconds()
    healthy = all(c['ok'] for c in checks)
    return {
        'started_at': started.isoformat() + 'Z',
        'duration_seconds': round(duration, 2),
        'healthy': healthy,
        'checks': checks,
        'recovered': recovered,
        'auto_recover_enabled': auto_recover,
        'summary': f"{sum(1 for c in checks if c['ok'])}/{len(checks)} checks passed"
                   + (f"; recovered: {len(recovered)} action(s)" if recovered else ""),
        'freshness': fresh,
    }


@app.route('/api/admin/daily-health-check', methods=['GET', 'POST'])
def api_daily_health_check():
    """Run the daily health check + auto-recovery on demand.

    GET = check only (don't fix), POST = check AND auto-recover.
    Always returns 200 with full report (use 'healthy' field to alert).
    """
    auto_recover = request.method == 'POST'
    report = _daily_health_check(auto_recover=auto_recover)
    return jsonify(report)


_health_scheduler = None


def start_health_scheduler():
    """Start an APScheduler job that runs the daily health check at 06:00 ET.

    This is BELT-AND-SUSPENDERS: catches if the 03:00 sync failed silently,
    runs auto-recovery, and logs a summary so we can audit.
    """
    global _health_scheduler
    if _health_scheduler is not None:
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print('[health] apscheduler not installed — skipping')
        return
    try:
        try:
            sched = BackgroundScheduler(timezone='America/Toronto')
        except Exception:
            sched = BackgroundScheduler()
        def _run():
            try:
                report = _daily_health_check(auto_recover=True)
                print(f"[health] {report['summary']} (took {report['duration_seconds']}s)")
                if not report['healthy']:
                    failed = [c for c in report['checks'] if not c['ok']]
                    print(f"[health] FAILED CHECKS: {failed}")
                    body_lines = [
                        f"Health check failed at {datetime.utcnow().isoformat()}Z",
                        f"Summary: {report['summary']}",
                        '',
                        'Failed checks:',
                    ]
                    for c in failed:
                        body_lines.append(f"  • {c['name']}: {c['detail']}")
                    if report.get('recovered'):
                        body_lines.append('')
                        body_lines.append(f"Auto-recovered: {', '.join(report['recovered'])}")
                    fresh = report.get('freshness', {})
                    if fresh:
                        body_lines.append('')
                        body_lines.append(f"Latest SOD snapshot: {fresh.get('latest_snapshot')} ({fresh.get('snapshot_age_days')}d old)")
                        body_lines.append(f"Last successful run: {fresh.get('last_run_age_hours')}h ago")
                    send_alert(
                        subject=f"Health check FAILED: {len(failed)} issue(s)",
                        body='\n'.join(body_lines),
                        level='critical' if any(c['name'].startswith('sod_') for c in failed) else 'warning',
                    )
            except Exception as e:
                print(f"[health] check failed: {e}")
                try:
                    send_alert(
                        subject="Health check raised an exception",
                        body=f"_daily_health_check exception: {e}",
                        level='critical',
                    )
                except Exception:
                    pass
        sched.add_job(
            _run,
            CronTrigger(hour=6, minute=0),  # 06:00 ET — after SOD sync at 03:00
            id='daily_health_check',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 4,
        )
        # Also run mid-day sanity check at 14:00 ET
        sched.add_job(
            _run,
            CronTrigger(hour=14, minute=0),
            id='midday_health_check',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 2,
        )

        # Proactive stale-data watcher — fires HOURLY, alerts (with 6h cooldown
        # so we don't spam) the moment data goes stale > 24h. Doesn't run any
        # auto-recovery — that's the daily job's job.
        _last_stale_alert_at = {'ts': None}

        def _stale_watch():
            try:
                from datetime import datetime as _dt
                fresh = _sod_freshness()
                age_days = fresh.get('snapshot_age_days')
                last_hours = fresh.get('last_run_age_hours')
                stale_by_snapshot = age_days is not None and age_days > 1
                stale_by_run = last_hours is not None and last_hours > 24
                if stale_by_snapshot or stale_by_run:
                    last = _last_stale_alert_at.get('ts')
                    now = _dt.utcnow()
                    cooldown_hours = 6
                    if last and (now - last).total_seconds() < cooldown_hours * 3600:
                        return  # still in cooldown
                    body_lines = [
                        f"Proactive stale-data alert at {now.isoformat()}Z",
                        '',
                        f"Latest SOD snapshot: {fresh.get('latest_snapshot')} "
                        f"({age_days}d old)" if age_days is not None else "Latest snapshot: never",
                        f"Last successful sync: {last_hours}h ago"
                        if last_hours is not None else "Last successful sync: never",
                        '',
                        f"Threshold: snapshot must be ≤1d old AND last sync ≤24h ago.",
                        '',
                        f"Auto-recovery will attempt at the next 06:00 / 14:00 ET health check.",
                        f"Manual trigger: POST /api/sod/sync",
                    ]
                    send_alert(
                        subject=f"⚠️ SOD data stale: snapshot {age_days}d old, last sync {last_hours}h ago",
                        body='\n'.join(body_lines),
                        level='warning',
                    )
                    _last_stale_alert_at['ts'] = now
            except Exception as e:
                print(f"[stale-watch] error: {e}")

        sched.add_job(
            _stale_watch,
            CronTrigger(minute=15),  # every hour at :15
            id='hourly_stale_watch',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )

        sched.start()
        _health_scheduler = sched
        print('[health] Daily health check scheduled at 06:00 + 14:00 ET; hourly stale-watch at :15')
    except Exception as e:
        print(f'[health] scheduler failed: {e}')


# ======================================================================================
# ============================== EMAIL + WEBHOOK ALERTS ================================
#
# Email alerts so the user knows when:
#   - SOD ingest fails or the latest report doesn't arrive
#   - The daily health check detects stale data, missing tracked-SKU rows,
#     or stuck rows
#   - The backend can't sync (DB unreachable, scheduler failed)
#   - Subscription / billing issues bubble up via the alert webhook
#
# Two delivery paths, configured via env vars (set EITHER, BOTH, or NEITHER):
#   1) Resend API   — RESEND_API_KEY + ALERT_EMAIL_TO + (optional) ALERT_EMAIL_FROM
#   2) Plain SMTP   — SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_EMAIL_TO
#                     + (optional) ALERT_EMAIL_FROM
#
# Plus the existing ALERT_WEBHOOK_URL keeps working for Slack/Discord/Make/Zapier.
# Failures are logged and never crash the caller.

def send_alert(subject: str, body: str, level: str = 'warning'):
    """Best-effort send: email (Resend then SMTP) + webhook. Logs every attempt.

    level: 'info' | 'warning' | 'critical' — colors / prefixes the subject.
    """
    prefix = {'info': '✓', 'warning': '⚠', 'critical': '🔴'}.get(level, '⚠')
    full_subject = f"[Anu LCBO] {prefix} {subject}"
    print(f"[alert/{level}] {subject}")

    # ---- 1. Resend API (preferred — clean HTTP) ----
    resend_key = os.environ.get('RESEND_API_KEY', '').strip()
    to_addr = os.environ.get('ALERT_EMAIL_TO', '').strip()
    from_addr = os.environ.get('ALERT_EMAIL_FROM', 'alerts@anu-lcbo.local').strip()
    if resend_key and to_addr and http_requests:
        try:
            r = http_requests.post(
                'https://api.resend.com/emails',
                headers={
                    'Authorization': f'Bearer {resend_key}',
                    'Content-Type': 'application/json',
                },
                json={
                    'from': from_addr,
                    'to': [a.strip() for a in to_addr.split(',') if a.strip()],
                    'subject': full_subject,
                    'text': body,
                },
                timeout=10,
            )
            if r.status_code in (200, 201, 202):
                print(f"[alert] resend ok ({r.status_code})")
            else:
                print(f"[alert] resend failed {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[alert] resend exception: {e}")
    elif to_addr:
        # ---- 2. SMTP fallback ----
        smtp_host = os.environ.get('SMTP_HOST', '').strip()
        if smtp_host:
            try:
                import smtplib
                from email.mime.text import MIMEText
                msg = MIMEText(body, 'plain', 'utf-8')
                msg['Subject'] = full_subject
                msg['From'] = from_addr
                msg['To'] = to_addr
                port = int(os.environ.get('SMTP_PORT', '587'))
                user = os.environ.get('SMTP_USER', '')
                pw = os.environ.get('SMTP_PASS', '')
                with smtplib.SMTP(smtp_host, port, timeout=15) as s:
                    s.ehlo()
                    if port == 587:
                        s.starttls()
                        s.ehlo()
                    if user and pw:
                        s.login(user, pw)
                    s.sendmail(from_addr, [a.strip() for a in to_addr.split(',') if a.strip()], msg.as_string())
                print("[alert] smtp ok")
            except Exception as e:
                print(f"[alert] smtp exception: {e}")

    # ---- 3. Webhook (Slack/Discord/Make/Zapier) ----
    webhook = os.environ.get('ALERT_WEBHOOK_URL', '').strip()
    if webhook and http_requests:
        try:
            color = {'info': 'good', 'warning': 'warning', 'critical': 'danger'}.get(level, 'warning')
            http_requests.post(
                webhook,
                json={
                    'text': full_subject,
                    'attachments': [{'color': color, 'text': body[:3000]}],
                },
                timeout=10,
            )
            print("[alert] webhook ok")
        except Exception as e:
            print(f"[alert] webhook exception: {e}")


@app.route('/api/admin/test-alert', methods=['POST', 'GET'])
def api_test_alert():
    """Fire a test alert through every configured channel. Use after configuring
    RESEND_API_KEY / SMTP / ALERT_WEBHOOK_URL to verify the wiring works.
    """
    subject = request.args.get('subject', 'Test alert')
    body = request.args.get('body', 'This is a test alert from the LCBO Tracker. '
                             'If you received this email or webhook ping, alerts are wired correctly.')
    level = request.args.get('level', 'info')
    send_alert(subject, body, level=level)
    return jsonify({
        'status': 'sent',
        'channels_configured': {
            'resend': bool(os.environ.get('RESEND_API_KEY')) and bool(os.environ.get('ALERT_EMAIL_TO')),
            'smtp': bool(os.environ.get('SMTP_HOST')) and bool(os.environ.get('ALERT_EMAIL_TO')),
            'webhook': bool(os.environ.get('ALERT_WEBHOOK_URL')),
        },
    })


# ======================================================================================
# ================================= CRM LAYER ==========================================
#
# Commercial-grade CRM features built on top of SOD + LCBO.com data:
#   - Territory model (Ontario FSA postal-code prefixes → territories → reps)
#   - Store-level classification (fast FSA lookup)
#   - Category inference (product-name pattern matching, enrich-on-demand from LCBO.com)
#   - Brink-of-OOS detection (stores with tracked SKU listed but on_hand <= threshold)
#   - Gap analysis grouped by territory
#   - Opportunity finder (slow-mover replacement candidates per store)
#   - Sales goals (rep/SKU/territory-scoped, period-bounded, with progress from SOD)
#   - HORECA accounts (bar/restaurant/hotel/catering CRM)
#   - Unified store detail (SOD + live LCBO.com inventory side-by-side)
#   - Listing-change digest (last N days, grouped)
#   - Full-DB JSON backup endpoint
#
# All data is idempotent-upserted; init_db() is a no-op on existing tables; Neon Postgres
# is external so Render worker restarts can NEVER lose persisted data.
# ======================================================================================

# ------- Ontario FSA (postal-code) → Territory map -------
# Ontario postal codes start with K/L/M/N/P. Grouping below follows common LCBO/wholesale
# territorial splits. Editable via the /api/crm/territories endpoint.
ONTARIO_TERRITORIES = [
    {
        'code': 'TOR_CORE',
        'name': 'Toronto Core',
        'region': 'GTA',
        'color': '#b22222',
        'fsa_prefixes': 'M4,M5,M6',
        'city_prefixes': 'toronto',
    },
    {
        'code': 'TOR_EAST',
        'name': 'Toronto East + Scarborough',
        'region': 'GTA',
        'color': '#d4a574',
        'fsa_prefixes': 'M1,M3,M4L,M4M',
        'city_prefixes': 'scarborough,east york,north york',
    },
    {
        'code': 'TOR_WEST',
        'name': 'Toronto West + Etobicoke',
        'region': 'GTA',
        'color': '#e07a5f',
        'fsa_prefixes': 'M8,M9',
        'city_prefixes': 'etobicoke,york',
    },
    {
        'code': 'GTA_WEST',
        'name': 'Mississauga / Oakville / Brampton',
        'region': 'GTA',
        'color': '#f2cc8f',
        'fsa_prefixes': 'L4,L5,L6,L7',
        'city_prefixes': 'mississauga,oakville,brampton,milton,burlington',
    },
    {
        'code': 'GTA_NORTH',
        'name': 'York Region / Vaughan / Markham',
        'region': 'GTA',
        'color': '#81b29a',
        'fsa_prefixes': 'L3,L4',
        'city_prefixes': 'vaughan,markham,richmond hill,aurora,newmarket',
    },
    {
        'code': 'HAMILTON_NIAGARA',
        'name': 'Hamilton / Niagara',
        'region': 'Southwest',
        'color': '#3d5a80',
        'fsa_prefixes': 'L8,L9,L0R,L2,L3',
        'city_prefixes': 'hamilton,niagara,st catharines,st. catharines,welland',
    },
    {
        'code': 'SW_ONT',
        'name': 'Southwest Ontario (London/Windsor)',
        'region': 'Southwest',
        'color': '#6a994e',
        'fsa_prefixes': 'N',
        'city_prefixes': 'london,windsor,kitchener,waterloo,cambridge,guelph',
    },
    {
        'code': 'OTTAWA',
        'name': 'Ottawa + Eastern Ontario',
        'region': 'East',
        'color': '#457b9d',
        'fsa_prefixes': 'K1,K2,K4,K6,K7',
        'city_prefixes': 'ottawa,kingston,kanata,nepean,gloucester,orleans',
    },
    {
        'code': 'CENTRAL_ONT',
        'name': 'Central Ontario (Barrie/Muskoka)',
        'region': 'Central',
        'color': '#a98467',
        'fsa_prefixes': 'L0,L4M,L4N,L9,P0',
        'city_prefixes': 'barrie,orillia,gravenhurst,bracebridge,huntsville,peterborough',
    },
    {
        'code': 'NORTHERN_ONT',
        'name': 'Northern Ontario',
        'region': 'North',
        'color': '#264653',
        'fsa_prefixes': 'P',
        'city_prefixes': 'sudbury,thunder bay,sault ste marie,sault ste. marie,north bay,timmins',
    },
]


def _fsa_from_postal(postal):
    """Return the 3-char FSA from a Canadian postal code string."""
    if not postal:
        return ''
    p = str(postal).upper().replace(' ', '').replace('-', '')
    if len(p) < 3:
        return ''
    return p[:3]


def classify_territory(postal, city):
    """Best-match territory_code for a store given postal + city.

    Strategy: FSA prefix match first (most specific wins), then city name contains.
    Returns the territory_code string, or 'UNASSIGNED' if nothing matches.
    """
    fsa = _fsa_from_postal(postal)
    city_l = (city or '').strip().lower()

    # Score each territory: longer FSA-prefix match wins; city substring match is fallback.
    best_code = None
    best_score = 0
    for t in ONTARIO_TERRITORIES:
        score = 0
        for prefix in t['fsa_prefixes'].split(','):
            prefix = prefix.strip().upper()
            if prefix and fsa.startswith(prefix):
                # Longer prefix = more specific match
                score = max(score, 10 + len(prefix))
        if score == 0 and city_l:
            for cp in t['city_prefixes'].split(','):
                cp = cp.strip().lower()
                if cp and cp in city_l:
                    score = max(score, 5 + len(cp) // 2)
        if score > best_score:
            best_score = score
            best_code = t['code']
    return best_code or 'UNASSIGNED'


def seed_territories():
    """Idempotently upsert ONTARIO_TERRITORIES into the territories table."""
    ph = _sod_ph()
    conn = _sod_get_conn()
    try:
        cur = conn.cursor()
        for t in ONTARIO_TERRITORIES:
            if USE_POSTGRES:
                cur.execute(
                    """INSERT INTO territories (code, name, region, color, fsa_prefixes, city_prefixes)
                       VALUES (%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (code) DO UPDATE SET
                           name=EXCLUDED.name, region=EXCLUDED.region, color=EXCLUDED.color,
                           fsa_prefixes=EXCLUDED.fsa_prefixes, city_prefixes=EXCLUDED.city_prefixes""",
                    (t['code'], t['name'], t['region'], t['color'], t['fsa_prefixes'], t['city_prefixes']),
                )
            else:
                cur.execute(
                    """INSERT INTO territories (code, name, region, color, fsa_prefixes, city_prefixes)
                       VALUES (?,?,?,?,?,?)
                       ON CONFLICT(code) DO UPDATE SET
                           name=excluded.name, region=excluded.region, color=excluded.color,
                           fsa_prefixes=excluded.fsa_prefixes, city_prefixes=excluded.city_prefixes""",
                    (t['code'], t['name'], t['region'], t['color'], t['fsa_prefixes'], t['city_prefixes']),
                )
        # Also add an UNASSIGNED catch-all
        if USE_POSTGRES:
            cur.execute(
                """INSERT INTO territories (code, name, region, color)
                   VALUES ('UNASSIGNED','Unassigned','','#888888')
                   ON CONFLICT (code) DO NOTHING""",
            )
        else:
            cur.execute(
                """INSERT OR IGNORE INTO territories (code, name, region, color)
                   VALUES ('UNASSIGNED','Unassigned','','#888888')""",
            )
        conn.commit()
        # Now auto-assign stores that don't yet have a territory_id
        cur.execute("SELECT id, code FROM territories")
        code_to_id = {row[1]: row[0] for row in cur.fetchall()}
        cur.execute("SELECT id, postal, city FROM stores WHERE territory_id IS NULL OR territory_id = 0")
        unassigned = cur.fetchall()
        assigned_count = 0
        for sid, postal, city in unassigned:
            tcode = classify_territory(postal, city)
            tid = code_to_id.get(tcode) or code_to_id.get('UNASSIGNED')
            if tid:
                if USE_POSTGRES:
                    cur.execute("UPDATE stores SET territory_id=%s WHERE id=%s", (tid, sid))
                else:
                    cur.execute("UPDATE stores SET territory_id=? WHERE id=?", (tid, sid))
                assigned_count += 1
        conn.commit()
        cur.close()
        print(f"[CRM] Territories seeded: {len(ONTARIO_TERRITORIES)} + UNASSIGNED. "
              f"Auto-assigned territory to {assigned_count} stores.")
    except Exception as e:
        print(f"[CRM] seed_territories failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ------- SKU Category classifier -------
# SOD rows don't include category. We infer from product_name using keyword patterns,
# good-enough for the opportunity finder. Can be enriched later from LCBO.com GraphQL.
CATEGORY_PATTERNS = [
    # (category_group, category, keywords-lowercase)
    ('SPIRITS', 'Vodka',     ['vodka']),
    ('SPIRITS', 'Whisky',    ['whisky', 'whiskey', 'bourbon', 'rye', 'scotch']),
    ('SPIRITS', 'Gin',       ['gin']),
    ('SPIRITS', 'Rum',       ['rum']),
    ('SPIRITS', 'Tequila',   ['tequila', 'mezcal']),
    ('SPIRITS', 'Brandy',    ['brandy', 'cognac', 'armagnac']),
    ('SPIRITS', 'Liqueur',   ['liqueur', 'amaretto', 'sambuca', 'schnapps']),
    ('SPIRITS', 'Feni',      ['feni']),
    ('SPIRITS', 'Other Spirits', ['cachaca', 'cachaça', 'grappa', 'pisco', 'arrack', 'aquavit', 'soju', 'baijiu']),
    ('WINE',    'Red Wine',  ['shiraz', 'cabernet', 'merlot', 'pinot noir', 'malbec', 'tempranillo', 'syrah', 'zinfandel', 'sangiovese']),
    ('WINE',    'White Wine',['chardonnay', 'sauvignon', 'pinot grigio', 'riesling', 'chenin', 'gewurz', 'viognier', 'semillon']),
    ('WINE',    'Rose',      ['rose', 'rosé']),
    ('WINE',    'Sparkling', ['champagne', 'prosecco', 'cava', 'sparkling']),
    ('WINE',    'Fortified', ['port', 'sherry', 'madeira', 'vermouth']),
    ('BEER',    'Beer',      [' beer', 'lager', 'pilsner', 'pilsener', ' ale', ' ipa', 'stout', 'porter', 'weiss', 'hefeweiss']),
    ('BEER',    'Cider',     [' cider']),
    ('RTD',     'Cooler/RTD',['cooler', 'seltzer', ' rtd', 'ready to drink', 'mixed drink']),
]


def classify_sku_category(name):
    """Return (category_group, category) for a product name. Defaults to ('', '')."""
    if not name:
        return '', ''
    nl = ' ' + name.lower() + ' '
    for group, cat, keywords in CATEGORY_PATTERNS:
        for kw in keywords:
            if kw in nl:
                return group, cat
    return '', ''


def refresh_sod_product_categories():
    """Backfill sod_products.category / category_group for rows that lack them.

    Called once on startup. Fast — only touches rows with NULL/empty category.
    """
    ph = _sod_ph()
    conn = _sod_get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT sku, product_name FROM sod_products WHERE COALESCE(category, '') = ''")
        rows = cur.fetchall()
        updated = 0
        for sku, name in rows:
            grp, cat = classify_sku_category(name)
            if cat:
                if USE_POSTGRES:
                    cur.execute(
                        "UPDATE sod_products SET category=%s, category_group=%s WHERE sku=%s",
                        (cat, grp, sku),
                    )
                else:
                    cur.execute(
                        "UPDATE sod_products SET category=?, category_group=? WHERE sku=?",
                        (cat, grp, sku),
                    )
                updated += 1
        conn.commit()
        cur.close()
        if updated:
            print(f"[CRM] Classified category for {updated} SOD products.")
    except Exception as e:
        print(f"[CRM] refresh_sod_product_categories failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ======== CRM API endpoints ========

@app.route('/api/crm/territories', methods=['GET'])
def api_crm_territories():
    """List all territories with store counts."""
    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db
    if USE_POSTGRES:
        cur.execute("""
            SELECT t.id, t.code, t.name, t.region, t.rep_name, t.color,
                   t.fsa_prefixes, t.city_prefixes,
                   COALESCE(sc.store_count, 0) AS store_count,
                   COALESCE(hc.horeca_count, 0) AS horeca_count
            FROM territories t
            LEFT JOIN (SELECT territory_id, COUNT(*) AS store_count FROM stores WHERE territory_id IS NOT NULL GROUP BY territory_id) sc
                ON sc.territory_id = t.id
            LEFT JOIN (SELECT territory_id, COUNT(*) AS horeca_count FROM horeca_accounts WHERE territory_id IS NOT NULL GROUP BY territory_id) hc
                ON hc.territory_id = t.id
            ORDER BY t.region, t.name
        """)
        rows = cur.fetchall()
        result = [{'id': r[0], 'code': r[1], 'name': r[2], 'region': r[3], 'rep_name': r[4],
                   'color': r[5], 'fsa_prefixes': r[6], 'city_prefixes': r[7],
                   'store_count': r[8], 'horeca_count': r[9]} for r in rows]
    else:
        rows = db.execute("""
            SELECT t.id, t.code, t.name, t.region, t.rep_name, t.color,
                   t.fsa_prefixes, t.city_prefixes,
                   COALESCE(sc.store_count, 0),
                   COALESCE(hc.horeca_count, 0)
            FROM territories t
            LEFT JOIN (SELECT territory_id, COUNT(*) AS store_count FROM stores WHERE territory_id IS NOT NULL GROUP BY territory_id) sc
                ON sc.territory_id = t.id
            LEFT JOIN (SELECT territory_id, COUNT(*) AS horeca_count FROM horeca_accounts WHERE territory_id IS NOT NULL GROUP BY territory_id) hc
                ON hc.territory_id = t.id
            ORDER BY t.region, t.name
        """).fetchall()
        result = [{'id': r[0], 'code': r[1], 'name': r[2], 'region': r[3], 'rep_name': r[4],
                   'color': r[5], 'fsa_prefixes': r[6], 'city_prefixes': r[7],
                   'store_count': r[8], 'horeca_count': r[9]} for r in rows]
    return jsonify(result)


@app.route('/api/crm/territories/<int:territory_id>', methods=['PUT'])
def api_crm_territory_update(territory_id):
    """Update a territory — typically to assign a rep_name."""
    data = request.get_json() or {}
    fields = []
    params = []
    for col in ('name', 'region', 'rep_name', 'color', 'fsa_prefixes', 'city_prefixes'):
        if col in data:
            fields.append(f"{col}=%s" if USE_POSTGRES else f"{col}=?")
            params.append(data[col])
    if not fields:
        return jsonify({'error': 'no updatable fields provided'}), 400
    params.append(territory_id)
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(f"UPDATE territories SET {', '.join(fields)} WHERE id=%s", params)
        db.commit()
        cur.close()
    else:
        db.execute(f"UPDATE territories SET {', '.join(fields)} WHERE id=?", params)
        db.commit()
    return jsonify({'status': 'ok'})


@app.route('/api/crm/territories/reassign', methods=['POST'])
def api_crm_territories_reassign():
    """Re-run the FSA-based classifier for ALL stores (force reassignment)."""
    seed_territories()
    # Force re-classify even for stores that already have a territory_id
    conn = _sod_get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, code FROM territories")
        code_to_id = {row[1]: row[0] for row in cur.fetchall()}
        cur.execute("SELECT id, postal, city FROM stores")
        rows = cur.fetchall()
        n = 0
        for sid, postal, city in rows:
            tcode = classify_territory(postal, city)
            tid = code_to_id.get(tcode) or code_to_id.get('UNASSIGNED')
            if tid:
                if USE_POSTGRES:
                    cur.execute("UPDATE stores SET territory_id=%s WHERE id=%s", (tid, sid))
                else:
                    cur.execute("UPDATE stores SET territory_id=? WHERE id=?", (tid, sid))
                n += 1
        conn.commit()
        cur.close()
        return jsonify({'status': 'ok', 'reassigned': n})
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.route('/api/crm/stores', methods=['GET'])
def api_crm_stores():
    """Stores list with territory join — for the Map view.

    Query params:
      territory_id: filter by one territory
      with_coords_only=1: only stores with lat/lng
    """
    territory_id = request.args.get('territory_id', type=int)
    coords_only = request.args.get('with_coords_only', '').lower() in ('1', 'true', 'yes')
    db = get_db()
    where = ['1=1']
    params = []
    if territory_id:
        where.append('s.territory_id=' + ('%s' if USE_POSTGRES else '?'))
        params.append(territory_id)
    if coords_only:
        where.append('s.lat IS NOT NULL AND s.lng IS NOT NULL AND s.lat <> 0 AND s.lng <> 0')
    where_sql = ' AND '.join(where)
    q = f"""
        SELECT s.id, s.store_number, s.account, s.address, s.city, s.postal,
               s.priority, s.rep, s.lat, s.lng, s.territory_id,
               COALESCE(t.code, ''), COALESCE(t.name, ''), COALESCE(t.color, '#888')
        FROM stores s
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE {where_sql}
        ORDER BY s.city, s.store_number
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()
    return jsonify([{
        'id': r[0], 'store_number': r[1], 'account': r[2], 'address': r[3],
        'city': r[4], 'postal': r[5], 'priority': r[6], 'rep': r[7],
        'lat': r[8] or 0, 'lng': r[9] or 0, 'territory_id': r[10],
        'territory_code': r[11], 'territory_name': r[12], 'territory_color': r[13],
    } for r in rows])


@app.route('/api/crm/stores/<int:store_id>/territory', methods=['PUT'])
def api_crm_store_set_territory(store_id):
    """Manually override a store's territory."""
    data = request.get_json() or {}
    tid = data.get('territory_id')
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("UPDATE stores SET territory_id=%s WHERE id=%s", (tid, store_id))
        db.commit()
        cur.close()
    else:
        db.execute("UPDATE stores SET territory_id=? WHERE id=?", (tid, store_id))
        db.commit()
    return jsonify({'status': 'ok'})


# ------- Brink-of-OOS detection -------
@app.route('/api/crm/oos-risk', methods=['GET'])
def api_crm_oos_risk():
    """Stores carrying a tracked SKU but on_hand is dangerously low.

    Query params:
      threshold: max on_hand (default 2 = brink)
      sku: limit to one SKU
      territory_id: limit to one territory

    Returns list sorted by on_hand asc then store_number.
    """
    threshold = request.args.get('threshold', default=2, type=int)
    sku = request.args.get('sku', '').strip()
    territory_id = request.args.get('territory_id', type=int)
    tracked_skus = list(SOD_TRACKED_SKUS.keys())
    if not tracked_skus:
        return jsonify([])
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    # Use the latest snapshot per SKU
    params = []
    sku_filter = ''
    if sku:
        skus = [sku.zfill(7)]
    else:
        skus = tracked_skus
    placeholders = ','.join([ph] * len(skus))
    q = f"""
        WITH latest AS (
            SELECT sku, MAX(snapshot_date) AS d
            FROM sod_inventory WHERE sku IN ({placeholders})
            GROUP BY sku
        )
        SELECT i.sku, i.product_name, i.store_number, i.status, i.on_hand, i.snapshot_date,
               s.id AS store_id, s.account, s.city, s.postal, s.rep,
               t.id AS territory_id, t.code AS territory_code, t.name AS territory_name, t.color
        FROM sod_inventory i
        JOIN latest l ON l.sku = i.sku AND l.d = i.snapshot_date
        LEFT JOIN stores s ON s.store_number = i.store_number
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE i.status = 'L'
          AND i.on_hand <= {ph}
    """
    params = list(skus) + [threshold]
    if territory_id:
        q += f" AND s.territory_id = {ph}"
        params.append(territory_id)
    q += " ORDER BY i.on_hand ASC, i.sku, i.store_number"
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()
    return jsonify([{
        'sku': r[0], 'product_name': r[1], 'store_number': r[2], 'status': r[3],
        'on_hand': r[4], 'snapshot_date': str(r[5]),
        'store_id': r[6], 'account': r[7], 'city': r[8], 'postal': r[9], 'rep': r[10],
        'territory_id': r[11], 'territory_code': r[12], 'territory_name': r[13],
        'territory_color': r[14],
        'severity': 'critical' if (r[4] or 0) == 0 else ('high' if (r[4] or 0) <= 1 else 'medium'),
    } for r in rows])


# ------- Gap analysis by territory -------
@app.route('/api/crm/gap-by-territory', methods=['GET'])
def api_crm_gap_by_territory():
    """For each tracked SKU, list stores NOT currently listing it, grouped by territory.

    Query params:
      sku: limit to one SKU (else all tracked)
      territory_id: limit to one territory
    """
    sku = request.args.get('sku', '').strip()
    territory_id = request.args.get('territory_id', type=int)
    tracked_skus = list(SOD_TRACKED_SKUS.keys())
    if not tracked_skus:
        return jsonify([])
    if sku:
        target_skus = [sku.zfill(7)]
    else:
        target_skus = tracked_skus
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    results = []
    for tsku in target_skus:
        # latest snapshot date for this sku
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s", (tsku,))
            r = cur.fetchone()
        else:
            r = db.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=?", (tsku,)).fetchone()
        latest_d = r[0] if r else None
        # stores currently listing it
        listing_stores = set()
        if latest_d:
            if USE_POSTGRES:
                cur.execute(
                    "SELECT store_number FROM sod_inventory WHERE sku=%s AND snapshot_date=%s AND status='L'",
                    (tsku, latest_d),
                )
                listing_stores = {row[0] for row in cur.fetchall()}
                cur.close()
            else:
                listing_stores = {row[0] for row in db.execute(
                    "SELECT store_number FROM sod_inventory WHERE sku=? AND snapshot_date=? AND status='L'",
                    (tsku, latest_d),
                ).fetchall()}
        # all stores (optionally filtered by territory)
        sw = [' 1=1 ']
        sp = []
        if territory_id:
            sw.append(' s.territory_id=' + ph)
            sp.append(territory_id)
        sq = f"""
            SELECT s.id, s.store_number, s.account, s.city, s.postal, s.rep, s.priority,
                   s.territory_id, COALESCE(t.code,''), COALESCE(t.name,''), COALESCE(t.color,'#888')
            FROM stores s
            LEFT JOIN territories t ON t.id = s.territory_id
            WHERE {' AND '.join(sw)}
            ORDER BY COALESCE(t.name,'zzz'), s.city, s.store_number
        """
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(sq, sp)
            stores_rows = cur.fetchall()
            cur.close()
        else:
            stores_rows = db.execute(sq, sp).fetchall()
        brand, pname = SOD_TRACKED_SKUS.get(tsku, ('', tsku))
        for sr in stores_rows:
            if sr[1] in listing_stores:
                continue
            results.append({
                'sku': tsku, 'brand': brand, 'product_name': pname,
                'store_id': sr[0], 'store_number': sr[1], 'account': sr[2],
                'city': sr[3], 'postal': sr[4], 'rep': sr[5], 'priority': sr[6],
                'territory_id': sr[7], 'territory_code': sr[8],
                'territory_name': sr[9] or 'Unassigned', 'territory_color': sr[10],
                'latest_snapshot': str(latest_d) if latest_d else None,
            })
    return jsonify(results)


# ------- Opportunity finder: slow-mover replacement candidates -------
@app.route('/api/crm/opportunities', methods=['GET'])
def api_crm_opportunities():
    """For each store where our tracked SKU is NOT listed, find competitor SKUs in the same
    category that are:
      - Currently listed at that store (status='L')
      - Underperforming: on_hand <= slow_threshold (default 3) OR status in ('D','F')
    These are the best replacement pitches — "delist this slow-mover, list our SKU instead."

    Query params:
      sku: target tracked SKU (required to pitch a specific product)
      slow_threshold: max on_hand considered slow (default 3)
      territory_id: limit to one territory
      limit: max results per SKU (default 200)
    """
    sku = request.args.get('sku', '').strip()
    slow_threshold = request.args.get('slow_threshold', default=3, type=int)
    territory_id = request.args.get('territory_id', type=int)
    limit = request.args.get('limit', default=200, type=int)

    tracked_skus = list(SOD_TRACKED_SKUS.keys())
    if not tracked_skus:
        return jsonify([])
    target_skus = [sku.zfill(7)] if sku else tracked_skus

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    out = []

    for tsku in target_skus:
        brand, pname = SOD_TRACKED_SKUS.get(tsku, ('', tsku))
        # Determine this SKU's category (from sod_products, else classify its name)
        grp, cat = '', ''
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute("SELECT category_group, category, product_name FROM sod_products WHERE sku=%s", (tsku,))
            pr = cur.fetchone()
        else:
            pr = db.execute("SELECT category_group, category, product_name FROM sod_products WHERE sku=?", (tsku,)).fetchone()
        if pr:
            grp, cat = pr[0] or '', pr[1] or ''
            if not cat:
                grp, cat = classify_sku_category(pr[2] or pname)
        else:
            grp, cat = classify_sku_category(pname)
        if not cat:
            # Unknown category — skip, can't build opportunities
            continue

        # Latest snapshot globally — simplest assumption
        if USE_POSTGRES:
            cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory")
            latest = cur.fetchone()[0]
        else:
            latest = db.execute("SELECT MAX(snapshot_date) FROM sod_inventory").fetchone()[0]
        if not latest:
            continue

        # Find stores already listing OUR SKU at latest snapshot (exclude them from opportunities)
        if USE_POSTGRES:
            cur.execute(
                "SELECT store_number FROM sod_inventory WHERE sku=%s AND snapshot_date=%s AND status='L'",
                (tsku, latest),
            )
            our_listed = {row[0] for row in cur.fetchall()}
        else:
            our_listed = {row[0] for row in db.execute(
                "SELECT store_number FROM sod_inventory WHERE sku=? AND snapshot_date=? AND status='L'",
                (tsku, latest),
            ).fetchall()}

        # Find slow/delisting rows in the same category at the latest snapshot
        params = [latest, slow_threshold]
        terr_join = ""
        terr_where = ""
        if territory_id:
            terr_where = f" AND s.territory_id = {ph}"
            params.append(territory_id)
        q = f"""
            SELECT i.sku, i.product_name, i.store_number, i.status, i.on_hand,
                   p.category_group, p.category,
                   s.id, s.account, s.city, s.postal, s.rep,
                   s.territory_id, COALESCE(t.code,''), COALESCE(t.name,''), COALESCE(t.color,'#888')
            FROM sod_inventory i
            JOIN sod_products p ON p.sku = i.sku
            LEFT JOIN stores s ON s.store_number = i.store_number
            LEFT JOIN territories t ON t.id = s.territory_id
            WHERE i.snapshot_date = {ph}
              AND p.category = {ph}
              AND (
                    (i.status = 'L' AND i.on_hand <= {ph})
                 OR i.status IN ('D', 'F')
              )
              AND i.sku <> {ph}
              {terr_where}
            ORDER BY i.status DESC, i.on_hand ASC, i.store_number
            LIMIT {ph}
        """
        params = [latest, cat, slow_threshold, tsku] + ([territory_id] if territory_id else []) + [limit * 4]  # over-fetch, we filter more below
        # Rebuild params in correct order (SQL above uses: latest, cat, slow_threshold, tsku, [territory_id], limit)
        params_final = [latest, cat, slow_threshold, tsku]
        if territory_id:
            params_final.append(territory_id)
        params_final.append(limit * 4)
        if USE_POSTGRES:
            cur.execute(q, params_final)
            rows = cur.fetchall()
        else:
            rows = db.execute(q, params_final).fetchall()

        # Filter: skip stores that already carry our SKU
        kept = 0
        for r in rows:
            if r[2] in our_listed:
                continue
            severity = 'delisting' if r[3] in ('D', 'F') else ('critical_slow' if (r[4] or 0) == 0 else 'slow')
            score = 0
            if r[3] == 'D':
                score += 50
            elif r[3] == 'F':
                score += 30
            if (r[4] or 0) == 0:
                score += 40
            elif (r[4] or 0) <= 1:
                score += 25
            elif (r[4] or 0) <= 3:
                score += 10
            out.append({
                'our_sku': tsku, 'our_brand': brand, 'our_product': pname,
                'category': cat, 'category_group': grp,
                'competitor_sku': r[0], 'competitor_name': r[1],
                'competitor_status': r[3], 'competitor_on_hand': r[4] or 0,
                'store_id': r[7], 'store_number': r[2], 'account': r[8],
                'city': r[9], 'postal': r[10], 'rep': r[11],
                'territory_id': r[12], 'territory_code': r[13],
                'territory_name': r[14] or 'Unassigned', 'territory_color': r[15],
                'severity': severity, 'opportunity_score': score,
            })
            kept += 1
            if kept >= limit:
                break
        if USE_POSTGRES:
            cur.close()

    # Sort by opportunity score descending
    out.sort(key=lambda x: (-x['opportunity_score'], x['our_sku'], x['store_number']))
    return jsonify(out[:limit * len(target_skus)])


# ------- Listing / delisting digest for dashboard -------
@app.route('/api/crm/listing-digest', methods=['GET'])
def api_crm_listing_digest():
    """Aggregate sod_listing_changes over the last N days (default 7).

    Returns: counts by change_type + top movements + tracked-SKU highlights.
    """
    days = request.args.get('days', default=7, type=int)
    tracked_only = request.args.get('tracked_only', '').lower() in ('1', 'true', 'yes')
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    since = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')

    # counts by change_type
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "SELECT change_type, COUNT(*) FROM sod_listing_changes WHERE change_date >= %s GROUP BY change_type ORDER BY COUNT(*) DESC",
            (since,),
        )
        counts = [{'change_type': r[0], 'count': r[1]} for r in cur.fetchall()]
    else:
        counts = [{'change_type': r[0], 'count': r[1]} for r in db.execute(
            "SELECT change_type, COUNT(*) FROM sod_listing_changes WHERE change_date >= ? GROUP BY change_type ORDER BY COUNT(*) DESC",
            (since,),
        ).fetchall()]

    # recent rows (latest 100, optional tracked-only)
    where = "WHERE change_date >= " + ph
    params = [since]
    if tracked_only:
        tracked_list = list(SOD_TRACKED_SKUS.keys())
        if tracked_list:
            phs = ','.join([ph] * len(tracked_list))
            where += f" AND c.sku IN ({phs})"
            params.extend(tracked_list)
    q = f"""
        SELECT c.sku, COALESCE(p.product_name, ''), c.change_date,
               c.old_status, c.new_status, c.change_type,
               COALESCE(p.brand,''), COALESCE(p.is_tracked, {'FALSE' if USE_POSTGRES else '0'})
        FROM sod_listing_changes c
        LEFT JOIN sod_products p ON p.sku = c.sku
        {where}
        ORDER BY c.change_date DESC, c.id DESC
        LIMIT 200
    """
    if USE_POSTGRES:
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()
    changes = [{
        'sku': r[0], 'product_name': r[1], 'change_date': str(r[2]),
        'old_status': r[3], 'new_status': r[4], 'change_type': r[5],
        'brand': r[6], 'is_tracked': bool(r[7]),
    } for r in rows]

    return jsonify({
        'window_days': days,
        'since': since,
        'counts': counts,
        'changes': changes,
    })


# ------- Sales goals -------
@app.route('/api/crm/goals', methods=['GET'])
def api_crm_goals_list():
    """List all goals, optionally filtered by scope/period."""
    scope = request.args.get('scope', '').strip()
    active_on = request.args.get('active_on', '').strip()  # YYYY-MM-DD
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = ['1=1']
    params = []
    if scope:
        where.append(f'scope={ph}')
        params.append(scope)
    if active_on:
        where.append(f'period_start <= {ph} AND period_end >= {ph}')
        params.extend([active_on, active_on])
    q = f"""
        SELECT id, scope, scope_key, period_start, period_end,
               target_units, target_revenue, target_listings, notes
        FROM sales_goals WHERE {' AND '.join(where)}
        ORDER BY period_end DESC, scope, scope_key
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()
    return jsonify([{
        'id': r[0], 'scope': r[1], 'scope_key': r[2],
        'period_start': str(r[3]), 'period_end': str(r[4]),
        'target_units': r[5], 'target_revenue': float(r[6] or 0),
        'target_listings': r[7], 'notes': r[8],
    } for r in rows])


@app.route('/api/crm/goals', methods=['POST'])
def api_crm_goals_create():
    data = request.get_json() or {}
    required = ['scope', 'scope_key', 'period_start', 'period_end']
    missing = [k for k in required if not data.get(k)]
    if missing:
        return jsonify({'error': f'missing fields: {missing}'}), 400
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    q = f"""
        INSERT INTO sales_goals
            (scope, scope_key, period_start, period_end,
             target_units, target_revenue, target_listings, notes, updated_at)
        VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{'NOW()' if USE_POSTGRES else 'CURRENT_TIMESTAMP'})
        ON CONFLICT (scope, scope_key, period_start, period_end) DO UPDATE SET
            target_units=EXCLUDED.target_units,
            target_revenue=EXCLUDED.target_revenue,
            target_listings=EXCLUDED.target_listings,
            notes=EXCLUDED.notes,
            updated_at={'NOW()' if USE_POSTGRES else 'CURRENT_TIMESTAMP'}
    """ if USE_POSTGRES else f"""
        INSERT INTO sales_goals
            (scope, scope_key, period_start, period_end,
             target_units, target_revenue, target_listings, notes, updated_at)
        VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(scope, scope_key, period_start, period_end) DO UPDATE SET
            target_units=excluded.target_units,
            target_revenue=excluded.target_revenue,
            target_listings=excluded.target_listings,
            notes=excluded.notes,
            updated_at=CURRENT_TIMESTAMP
    """
    params = (data['scope'], data['scope_key'], data['period_start'], data['period_end'],
              int(data.get('target_units') or 0), float(data.get('target_revenue') or 0),
              int(data.get('target_listings') or 0), data.get('notes', ''))
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        db.commit()
        cur.close()
    else:
        db.execute(q, params)
        db.commit()
    return jsonify({'status': 'ok'})


@app.route('/api/crm/goals/<int:goal_id>', methods=['DELETE'])
def api_crm_goals_delete(goal_id):
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(f"DELETE FROM sales_goals WHERE id={ph}", (goal_id,))
        db.commit()
        cur.close()
    else:
        db.execute(f"DELETE FROM sales_goals WHERE id={ph}", (goal_id,))
        db.commit()
    return jsonify({'status': 'ok'})


@app.route('/api/crm/goals/progress', methods=['GET'])
def api_crm_goals_progress():
    """Compute progress per active goal.

    Progress metrics (best-effort from SOD):
      - listings: # of stores currently listing this SKU (for scope='sku')
      - units: cumulative on_hand across stores at latest snapshot
      - revenue: not tracked (requires price × units; stubbed 0)
    """
    today = datetime.utcnow().strftime('%Y-%m-%d')
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "SELECT id, scope, scope_key, period_start, period_end, target_units, target_revenue, target_listings, notes "
            "FROM sales_goals WHERE period_start <= %s AND period_end >= %s",
            (today, today),
        )
        goals = cur.fetchall()
    else:
        goals = db.execute(
            "SELECT id, scope, scope_key, period_start, period_end, target_units, target_revenue, target_listings, notes "
            "FROM sales_goals WHERE period_start <= ? AND period_end >= ?",
            (today, today),
        ).fetchall()

    out = []
    for g in goals:
        gid, scope, key, pstart, pend, tunits, trev, tlist, notes = g
        achieved_units = 0
        achieved_listings = 0
        if scope == 'sku':
            sku = str(key).zfill(7)
            if USE_POSTGRES:
                cur.execute(
                    "SELECT COALESCE(SUM(on_hand),0), SUM(CASE WHEN status='L' THEN 1 ELSE 0 END) "
                    "FROM sod_inventory WHERE sku=%s AND snapshot_date=(SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s)",
                    (sku, sku),
                )
                r = cur.fetchone()
            else:
                r = db.execute(
                    "SELECT COALESCE(SUM(on_hand),0), SUM(CASE WHEN status='L' THEN 1 ELSE 0 END) "
                    "FROM sod_inventory WHERE sku=? AND snapshot_date=(SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=?)",
                    (sku, sku),
                ).fetchone()
            achieved_units = int(r[0] or 0)
            achieved_listings = int(r[1] or 0)
        elif scope == 'territory':
            # aggregate across all tracked SKUs for stores in this territory
            tracked = list(SOD_TRACKED_SKUS.keys())
            if tracked:
                phs = ','.join([ph] * len(tracked))
                if USE_POSTGRES:
                    cur.execute(
                        f"""SELECT COALESCE(SUM(i.on_hand),0),
                                   SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END)
                            FROM sod_inventory i
                            JOIN stores s ON s.store_number = i.store_number
                            WHERE i.sku IN ({phs})
                              AND s.territory_id = (SELECT id FROM territories WHERE code=%s OR CAST(id AS TEXT)=%s LIMIT 1)
                              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)""",
                        tracked + [key, key],
                    )
                    r = cur.fetchone()
                else:
                    r = db.execute(
                        f"""SELECT COALESCE(SUM(i.on_hand),0),
                                   SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END)
                            FROM sod_inventory i
                            JOIN stores s ON s.store_number = i.store_number
                            WHERE i.sku IN ({phs})
                              AND s.territory_id = (SELECT id FROM territories WHERE code=? OR CAST(id AS TEXT)=? LIMIT 1)
                              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)""",
                        tracked + [key, key],
                    ).fetchone()
                achieved_units = int(r[0] or 0)
                achieved_listings = int(r[1] or 0)
        # rep scope — roll up across all tracked SKUs in stores assigned to this rep
        elif scope == 'rep':
            tracked = list(SOD_TRACKED_SKUS.keys())
            if tracked:
                phs = ','.join([ph] * len(tracked))
                if USE_POSTGRES:
                    cur.execute(
                        f"""SELECT COALESCE(SUM(i.on_hand),0),
                                   SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END)
                            FROM sod_inventory i
                            JOIN stores s ON s.store_number = i.store_number
                            WHERE i.sku IN ({phs}) AND LOWER(s.rep) = LOWER(%s)
                              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)""",
                        tracked + [key],
                    )
                    r = cur.fetchone()
                else:
                    r = db.execute(
                        f"""SELECT COALESCE(SUM(i.on_hand),0),
                                   SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END)
                            FROM sod_inventory i
                            JOIN stores s ON s.store_number = i.store_number
                            WHERE i.sku IN ({phs}) AND LOWER(s.rep) = LOWER(?)
                              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)""",
                        tracked + [key],
                    ).fetchone()
                achieved_units = int(r[0] or 0)
                achieved_listings = int(r[1] or 0)
        out.append({
            'id': gid, 'scope': scope, 'scope_key': key,
            'period_start': str(pstart), 'period_end': str(pend),
            'target_units': tunits, 'target_revenue': float(trev or 0), 'target_listings': tlist,
            'achieved_units': achieved_units, 'achieved_listings': achieved_listings,
            'pct_units': round(100 * achieved_units / tunits, 1) if tunits else None,
            'pct_listings': round(100 * achieved_listings / tlist, 1) if tlist else None,
            'notes': notes,
        })
    if USE_POSTGRES:
        cur.close()
    return jsonify(out)


# ------- HORECA accounts CRUD -------
@app.route('/api/crm/horeca', methods=['GET'])
def api_crm_horeca_list():
    """List HORECA accounts with territory join."""
    territory_id = request.args.get('territory_id', type=int)
    status = request.args.get('status', '').strip()
    account_type = request.args.get('type', '').strip()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = ['1=1']
    params = []
    if territory_id:
        where.append(f'h.territory_id={ph}')
        params.append(territory_id)
    if status:
        where.append(f'h.status={ph}')
        params.append(status)
    if account_type:
        where.append(f'h.account_type={ph}')
        params.append(account_type)
    q = f"""
        SELECT h.id, h.name, h.account_type, h.address, h.city, h.postal,
               h.phone, h.email, h.contact_name, h.contact_title,
               h.territory_id, COALESCE(t.name,'') AS territory_name,
               COALESCE(t.color,'#888'),
               h.rep_name, h.status, h.priority, h.lat, h.lng,
               h.last_visit, h.next_visit, h.products_carried, h.notes,
               h.created_at, h.updated_at
        FROM horeca_accounts h
        LEFT JOIN territories t ON t.id = h.territory_id
        WHERE {' AND '.join(where)}
        ORDER BY h.priority DESC, h.name
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()
    return jsonify([{
        'id': r[0], 'name': r[1], 'account_type': r[2], 'address': r[3],
        'city': r[4], 'postal': r[5], 'phone': r[6], 'email': r[7],
        'contact_name': r[8], 'contact_title': r[9],
        'territory_id': r[10], 'territory_name': r[11], 'territory_color': r[12],
        'rep_name': r[13], 'status': r[14], 'priority': r[15],
        'lat': r[16] or 0, 'lng': r[17] or 0,
        'last_visit': str(r[18]) if r[18] else '',
        'next_visit': str(r[19]) if r[19] else '',
        'products_carried': r[20] or '',
        'notes': r[21] or '',
        'created_at': str(r[22]) if r[22] else '',
        'updated_at': str(r[23]) if r[23] else '',
    } for r in rows])


@app.route('/api/crm/horeca', methods=['POST'])
def api_crm_horeca_create():
    data = request.get_json() or {}
    if not data.get('name'):
        return jsonify({'error': 'name is required'}), 400
    # Auto-assign territory from postal+city if not provided
    tid = data.get('territory_id')
    if not tid:
        tcode = classify_territory(data.get('postal', ''), data.get('city', ''))
        db0 = get_db()
        ph0 = '%s' if USE_POSTGRES else '?'
        if USE_POSTGRES:
            cur0 = db0.cursor()
            cur0.execute(f"SELECT id FROM territories WHERE code={ph0}", (tcode,))
            row = cur0.fetchone()
            cur0.close()
        else:
            row = db0.execute(f"SELECT id FROM territories WHERE code={ph0}", (tcode,)).fetchone()
        tid = row[0] if row else None
    cols = ['name', 'account_type', 'address', 'city', 'postal', 'phone', 'email',
            'contact_name', 'contact_title', 'territory_id', 'rep_name', 'status',
            'priority', 'lat', 'lng', 'last_visit', 'next_visit', 'products_carried', 'notes']
    vals = (
        data.get('name', ''), data.get('account_type', 'restaurant'),
        data.get('address', ''), data.get('city', ''), data.get('postal', ''),
        data.get('phone', ''), data.get('email', ''),
        data.get('contact_name', ''), data.get('contact_title', ''),
        tid, data.get('rep_name', ''), data.get('status', 'prospect'),
        data.get('priority', 'Standard'),
        float(data.get('lat') or 0), float(data.get('lng') or 0),
        data.get('last_visit') or None, data.get('next_visit') or None,
        data.get('products_carried', ''), data.get('notes', ''),
    )
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"INSERT INTO horeca_accounts ({', '.join(cols)}) VALUES ({','.join(['%s']*len(cols))}) RETURNING id",
            vals,
        )
        new_id = cur.fetchone()[0]
        db.commit()
        cur.close()
    else:
        c = db.execute(
            f"INSERT INTO horeca_accounts ({', '.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
            vals,
        )
        new_id = c.lastrowid
        db.commit()
    return jsonify({'status': 'ok', 'id': new_id})


@app.route('/api/crm/horeca/<int:hid>', methods=['PUT'])
def api_crm_horeca_update(hid):
    data = request.get_json() or {}
    allowed = ('name', 'account_type', 'address', 'city', 'postal', 'phone', 'email',
               'contact_name', 'contact_title', 'territory_id', 'rep_name', 'status',
               'priority', 'lat', 'lng', 'last_visit', 'next_visit', 'products_carried', 'notes')
    fields = []
    params = []
    ph = '%s' if USE_POSTGRES else '?'
    for col in allowed:
        if col in data:
            fields.append(f"{col}={ph}")
            params.append(data[col] if data[col] != '' else None if col in ('last_visit', 'next_visit', 'territory_id') else data[col])
    if not fields:
        return jsonify({'error': 'no updatable fields'}), 400
    fields.append(f"updated_at={'NOW()' if USE_POSTGRES else 'CURRENT_TIMESTAMP'}")
    params.append(hid)
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(f"UPDATE horeca_accounts SET {', '.join(fields)} WHERE id={ph}", params)
        db.commit()
        cur.close()
    else:
        db.execute(f"UPDATE horeca_accounts SET {', '.join(fields)} WHERE id={ph}", params)
        db.commit()
    return jsonify({'status': 'ok'})


@app.route('/api/crm/horeca/<int:hid>', methods=['DELETE'])
def api_crm_horeca_delete(hid):
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(f"DELETE FROM horeca_accounts WHERE id={ph}", (hid,))
        db.commit()
        cur.close()
    else:
        db.execute(f"DELETE FROM horeca_accounts WHERE id={ph}", (hid,))
        db.commit()
    return jsonify({'status': 'ok'})


# ------- Unified store detail (SOD + live LCBO.com) -------
@app.route('/api/crm/store/<int:store_number>/inventory', methods=['GET'])
def api_crm_store_inventory(store_number):
    """Return current tracked-SKU status at this store from BOTH sources:
      - SOD (last snapshot): status, on_hand
      - LCBO.com live (optional, set live=1): on_hand right now
    """
    include_live = request.args.get('live', '').lower() in ('1', 'true', 'yes')
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    tracked = list(SOD_TRACKED_SKUS.keys())
    if not tracked:
        return jsonify({'store_number': store_number, 'sod': [], 'live': []})
    phs = ','.join([ph] * len(tracked))
    q = f"""
        WITH latest AS (
            SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
            WHERE sku IN ({phs}) GROUP BY sku
        )
        SELECT i.sku, i.product_name, i.status, i.on_hand, i.snapshot_date
        FROM sod_inventory i
        JOIN latest l ON l.sku = i.sku AND l.d = i.snapshot_date
        WHERE i.store_number = {ph}
        ORDER BY i.sku
    """
    params = tracked + [store_number]
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        sod_rows = cur.fetchall()
        cur.close()
    else:
        sod_rows = db.execute(q, params).fetchall()
    sod = [{
        'sku': r[0], 'product_name': r[1], 'status': r[2],
        'on_hand': r[3], 'snapshot_date': str(r[4]),
        'brand': SOD_TRACKED_SKUS.get(r[0], ('', ''))[0],
    } for r in sod_rows]

    live = []
    if include_live:
        # Use existing scrape_lcbo_inventory for each tracked SKU, filter for this store
        try:
            for sku in tracked:
                try:
                    results = scrape_lcbo_inventory(sku.lstrip('0'))  # type: ignore[name-defined]
                except Exception:
                    results = []
                for row in results or []:
                    if str(row.get('store_number', '')) == str(store_number):
                        live.append({
                            'sku': sku,
                            'brand': SOD_TRACKED_SKUS.get(sku, ('', ''))[0],
                            'product_name': SOD_TRACKED_SKUS.get(sku, ('', ''))[1],
                            'quantity': row.get('quantity', 0),
                            'store_name': row.get('store_name', ''),
                            'city': row.get('store_city', ''),
                            'source': 'lcbo.com_live',
                        })
                        break
        except Exception as e:
            live = [{'error': f'live fetch failed: {e}'}]
    return jsonify({'store_number': store_number, 'sod': sod, 'live': live})


# ------- Full-DB backup (JSON) -------
@app.route('/api/crm/backup', methods=['GET'])
def api_crm_backup():
    """One-shot JSON dump of all CRM + SOD tables. Use for offline backup / disaster recovery.

    Pipe to a file:  curl https://.../api/crm/backup > backup-$(date +%F).json

    For a complete plug-and-play export including audit + inventory tables, use
    /api/admin/export instead.
    """
    db = get_db()
    tables = [
        'territories', 'stores', 'horeca_accounts', 'sales_goals',
        'sod_products', 'sod_listing_changes',
        # intentionally excluded (too large for a quick backup): sod_inventory, inventory_history
        'reps', 'products', 'followups',
    ]
    out = {'generated_at': datetime.utcnow().isoformat() + 'Z', 'tables': {}}
    for t in tables:
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(f"SELECT * FROM {t}")
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, [_json_safe(v) for v in row])) for row in cur.fetchall()]
                cur.close()
            else:
                rows_raw = db.execute(f"SELECT * FROM {t}").fetchall()
                cols = [d[0] for d in db.execute(f"SELECT * FROM {t} LIMIT 0").description]
                rows = [dict(zip(cols, [_json_safe(v) for v in row])) for row in rows_raw]
            out['tables'][t] = {'row_count': len(rows), 'rows': rows}
        except Exception as e:
            out['tables'][t] = {'error': str(e)}
    return jsonify(out)


# ============================================================================
# PLUG-AND-PLAY EXPORT/IMPORT — one-button host migration safety net.
#
# Scenario: app dies on host X. You want to spin up the same code on host Y
# pointed at the same Neon DB — no migration needed (data is in Neon, not on
# the host). But if you want to MOVE the data too (different DB, different
# Postgres provider), use:
#
#   1. Pull full snapshot from running app:
#        curl -H "X-Admin-Token: $TOKEN" https://OLD/api/admin/export?include=core \
#          > anu-tracker-backup.json
#
#   2. Stand up a fresh app on the new host (pointed at a fresh Postgres). The
#      schema bootstraps automatically on first request.
#
#   3. Push the snapshot in:
#        curl -X POST -H "X-Admin-Token: $TOKEN" -H "Content-Type: application/json" \
#             --data-binary @anu-tracker-backup.json \
#             https://NEW/api/admin/import?mode=merge
#
# All tables are upserted by primary key — safe to run twice, idempotent.
# ============================================================================

# Tables in dependency order — parents before children (for clean restore).
# Audit tables (event_log, sod_store_sku_changes) included so you keep history.
_EXPORT_TABLES = [
    # Core CRM (parents first)
    ('territories',                'id'),
    ('stores',                     'id'),
    ('reps',                       'id'),
    # Reference data
    ('products',                   'id'),
    ('sod_products',               'sku'),
    # Activity / pipeline (depends on stores)
    ('activities',                 'id'),
    ('deals',                      'id'),
    ('followups',                  'id'),
    ('rep_quotas',                 'id'),
    ('sales_goals',                'id'),
    ('horeca_accounts',            'id'),
    # Audit + history
    ('event_log',                  'id'),
    ('sod_listing_changes',        'id'),
    ('sod_store_sku_changes',      'id'),
    ('sod_sync_runs',              'id'),
    # Optional (large)
    ('sod_inventory',              None),  # 1M+ rows, only included with ?include=all
    ('inventory_history',          None),
]


def _admin_token_ok() -> bool:
    """Verify X-Admin-Token header against ADMIN_TOKEN env var.

    If ADMIN_TOKEN is not set, only allow from localhost (dev convenience).
    Constant-time comparison to defeat timing attacks.
    """
    expected = os.environ.get('ADMIN_TOKEN', '').strip()
    if not expected:
        # Dev-only: allow if request looks local
        return request.remote_addr in ('127.0.0.1', '::1', 'localhost')
    got = request.headers.get('X-Admin-Token', '').strip()
    if not got:
        # Fall back to query param ?admin_token= for browser UX (still validated)
        got = (request.args.get('admin_token') or '').strip()
    if not got or len(got) != len(expected):
        return False
    # Constant-time compare
    import hmac
    return hmac.compare_digest(got, expected)


def require_admin_token(fn):
    """Decorator for endpoints that mutate data or expose secrets. Returns 403
    if the X-Admin-Token header (or ?admin_token= query param) doesn't match
    ADMIN_TOKEN. All ADMIN_TOKEN usage here is constant-time compared.
    """
    from functools import wraps

    @wraps(fn)
    def wrapped(*args, **kwargs):
        if not _admin_token_ok():
            return jsonify({
                'error': 'forbidden',
                'detail': 'Provide a valid X-Admin-Token header (or ?admin_token= query param). '
                          'Set ADMIN_TOKEN env var on the host to enable.',
            }), 403
        return fn(*args, **kwargs)

    return wrapped


@app.route('/api/admin/export', methods=['GET'])
def api_admin_export():
    """Full JSON export — plug-and-play backup. Auth: X-Admin-Token header.

    Query params:
      include = 'core' (default — every table EXCEPT sod_inventory + inventory_history)
              | 'all'  (everything, can be 100MB+)
              | 'essential' (stores + territories + activities + deals + quotas)

    Returns: { generated_at, schema_version, include, tables: {name: {row_count, rows}} }
    """
    if not _admin_token_ok():
        return jsonify({'error': 'forbidden — set X-Admin-Token header'}), 403

    include = request.args.get('include', 'core')
    if include == 'essential':
        wanted = {'territories', 'stores', 'activities', 'deals', 'quotas', 'reps'}
    elif include == 'all':
        wanted = {t for t, _ in _EXPORT_TABLES}
    else:
        # core: everything except the two largest tables
        wanted = {t for t, _ in _EXPORT_TABLES if t not in ('sod_inventory', 'inventory_history')}

    db = get_db()
    out = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'schema_version': 1,
        'include': include,
        'tables': {},
    }
    for tname, _pk in _EXPORT_TABLES:
        if tname not in wanted:
            continue
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(f"SELECT * FROM {tname}")
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, [_json_safe(v) for v in row])) for row in cur.fetchall()]
                cur.close()
            else:
                rows_raw = db.execute(f"SELECT * FROM {tname}").fetchall()
                cols = [d[0] for d in db.execute(f"SELECT * FROM {tname} LIMIT 0").description]
                rows = [dict(zip(cols, [_json_safe(v) for v in row])) for row in rows_raw]
            out['tables'][tname] = {'row_count': len(rows), 'columns': cols, 'rows': rows}
        except Exception as e:
            # Postgres: roll back the aborted transaction so subsequent tables work
            if USE_POSTGRES:
                try:
                    db.rollback()
                except Exception:
                    pass
            out['tables'][tname] = {'error': str(e)}
    return jsonify(out)


@app.route('/api/admin/import', methods=['POST'])
def api_admin_import():
    """Restore from /api/admin/export JSON. Auth: X-Admin-Token header.

    Modes (?mode=...):
      merge   = upsert every row by PK; rows that exist are updated, new rows added (default, safest)
      append  = INSERT only; skip rows whose PK already exists (no updates)
      replace = TRUNCATE table then INSERT (DESTRUCTIVE — confirmation required via ?confirm=YES)

    Returns per-table results with insert/update/skip counts.
    """
    if not _admin_token_ok():
        return jsonify({'error': 'forbidden — set X-Admin-Token header'}), 403

    mode = (request.args.get('mode') or 'merge').lower()
    if mode == 'replace' and request.args.get('confirm') != 'YES':
        return jsonify({'error': 'replace mode is destructive — pass ?confirm=YES to proceed'}), 400

    payload = request.get_json(silent=True) or {}
    tables = payload.get('tables') or {}
    if not isinstance(tables, dict) or not tables:
        return jsonify({'error': 'invalid payload — expected {tables: {name: {rows: [...]}}}'}), 400

    db = get_db()
    results = {}
    for tname, pk in _EXPORT_TABLES:
        td = tables.get(tname)
        if not td or 'rows' not in td:
            continue
        rows = td.get('rows') or []
        cols = td.get('columns') or (list(rows[0].keys()) if rows else [])
        if not cols:
            results[tname] = {'inserted': 0, 'updated': 0, 'skipped': 0, 'error': 'no columns'}
            continue
        ins = upd = skip = 0
        err = None
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                if mode == 'replace':
                    cur.execute(f"TRUNCATE TABLE {tname} RESTART IDENTITY CASCADE")
                placeholders = ','.join(['%s'] * len(cols))
                col_list = ','.join(cols)
                if mode == 'merge' and pk:
                    update_set = ','.join(f"{c}=EXCLUDED.{c}" for c in cols if c != pk)
                    sql = (f"INSERT INTO {tname} ({col_list}) VALUES ({placeholders}) "
                           f"ON CONFLICT ({pk}) DO UPDATE SET {update_set}")
                elif mode == 'append' and pk:
                    sql = f"INSERT INTO {tname} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO NOTHING"
                else:
                    sql = f"INSERT INTO {tname} ({col_list}) VALUES ({placeholders})"
                for r in rows:
                    vals = tuple(r.get(c) for c in cols)
                    try:
                        cur.execute(sql, vals)
                        if cur.rowcount == 1:
                            ins += 1
                        else:
                            upd += 1
                    except Exception as e:
                        # roll back this row's transaction so subsequent rows still work
                        db.rollback()
                        skip += 1
                        if err is None:
                            err = f"{type(e).__name__}: {str(e)[:200]}"
                        cur = db.cursor()
                db.commit()
                cur.close()
            else:
                # SQLite path (dev only)
                if mode == 'replace':
                    db.execute(f"DELETE FROM {tname}")
                placeholders = ','.join(['?'] * len(cols))
                col_list = ','.join(cols)
                verb = 'INSERT OR REPLACE' if mode == 'merge' else ('INSERT OR IGNORE' if mode == 'append' else 'INSERT')
                sql = f"{verb} INTO {tname} ({col_list}) VALUES ({placeholders})"
                for r in rows:
                    vals = tuple(r.get(c) for c in cols)
                    try:
                        c = db.execute(sql, vals)
                        if c.rowcount > 0:
                            ins += 1
                        else:
                            skip += 1
                    except Exception as e:
                        skip += 1
                        if err is None:
                            err = f"{type(e).__name__}: {str(e)[:200]}"
                db.commit()
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:300]}"
        results[tname] = {'inserted': ins, 'updated': upd, 'skipped': skip, 'error': err}

    return jsonify({
        'status': 'completed',
        'mode': mode,
        'tables': results,
        'summary': {
            'tables_imported': sum(1 for r in results.values() if r.get('inserted', 0) + r.get('updated', 0) > 0),
            'rows_inserted': sum(r.get('inserted', 0) for r in results.values()),
            'rows_updated': sum(r.get('updated', 0) for r in results.values()),
            'rows_skipped': sum(r.get('skipped', 0) for r in results.values()),
        },
    })


@app.route('/api/admin/data-integrity', methods=['GET'])
def api_admin_data_integrity():
    """Comprehensive data-accuracy audit. Read-only, no auth (just diagnostic).

    Returns per-tracked-SKU report:
      - latest_snapshot_date + age_days
      - SOD store counts: total / listed (L) / will-delist (D) / fully-delisted (F)
      - lcbo.com store_count (live scrape — most recent inventory_history)
      - drift: stores where SOD says listed but lcbo.com shows 0, or vice versa
      - LCBO_LIVE_ONLY discoveries (lcbo.com live, SOD missing/F) in last 30d
      - listing-flip events in last 30d (D→L, L→D, F→L, etc.)
      - duplicate detection (same sku/store/date with multiple rows)
      - integrity_grade: A (no drift), B (≤5% drift), C (≤15%), D (>15%), F (data missing)
    Plus global sanity checks (orphaned stores, stuck sync runs, scheduler status).
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    today = _toronto_today()
    since_30d = (today - timedelta(days=30)).isoformat()

    report = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'today_toronto': today.isoformat(),
        'global': {},
        'per_sku': [],
        'global_grade': 'A',
    }

    def _safe_count(sql, params=()):
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(sql, params)
                v = cur.fetchone()[0]
                cur.close()
                return int(v or 0)
            return int((db.execute(sql, params).fetchone() or [0])[0])
        except Exception as e:
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass
            return f'ERR: {str(e)[:120]}'

    # ---- Global ----
    # Stuck running rows (sync started but never finished)
    if USE_POSTGRES:
        stuck_sql = "SELECT COUNT(*) FROM sod_sync_runs WHERE status='running' AND run_at < NOW() - INTERVAL '6 hours'"
        failed_sql = "SELECT COUNT(*) FROM sod_sync_runs WHERE status='failed' AND run_at >= NOW() - INTERVAL '7 days'"
    else:
        stuck_sql = "SELECT COUNT(*) FROM sod_sync_runs WHERE status='running' AND run_at < datetime('now', '-6 hours')"
        failed_sql = "SELECT COUNT(*) FROM sod_sync_runs WHERE status='failed' AND run_at >= datetime('now', '-7 days')"
    report['global']['stuck_sync_runs'] = _safe_count(stuck_sql)
    report['global']['failed_runs_7d'] = _safe_count(failed_sql)
    # Orphaned inventory rows (sku/store_number with no matching tracked SKU)
    if USE_POSTGRES:
        cur = db.cursor()
        try:
            cur.execute(
                "SELECT COUNT(DISTINCT sku) FROM sod_inventory "
                "WHERE sku NOT IN %s",
                (tuple(SOD_TRACKED_SKUS.keys()) or ('',),)
            )
            report['global']['untracked_skus_in_inventory'] = int(cur.fetchone()[0] or 0)
        except Exception as e:
            db.rollback()
            report['global']['untracked_skus_in_inventory'] = f'ERR: {str(e)[:80]}'
        cur.close()
    # Scheduler status
    report['global']['sod_scheduler_running'] = _sod_scheduler_running()
    # Latest run by source
    runs_by_src = {}
    try:
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT source, MAX(run_at), MAX(CASE WHEN status='success' THEN run_at END) "
                "FROM sod_sync_runs GROUP BY source"
            )
            for r in cur.fetchall():
                runs_by_src[r[0]] = {
                    'last_attempt': r[1].isoformat() if r[1] else None,
                    'last_success': r[2].isoformat() if r[2] else None,
                }
            cur.close()
        else:
            for r in db.execute(
                "SELECT source, MAX(run_at), MAX(CASE WHEN status='success' THEN run_at END) "
                "FROM sod_sync_runs GROUP BY source"
            ).fetchall():
                runs_by_src[r[0]] = {'last_attempt': r[1], 'last_success': r[2]}
    except Exception as e:
        if USE_POSTGRES:
            try: db.rollback()
            except Exception: pass
    report['global']['runs_by_source'] = runs_by_src

    # ---- Per-tracked-SKU integrity ----
    drift_total = 0
    listed_total = 0
    for sku, (brand, name) in SOD_TRACKED_SKUS.items():
        s = {'sku': sku, 'brand': brand, 'product_name': name}
        # Latest snapshot for THIS sku
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s", (sku,))
                latest = cur.fetchone()[0]
                cur.close()
            else:
                latest = db.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=?", (sku,)).fetchone()[0]
        except Exception:
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass
            latest = None
        s['latest_snapshot'] = latest.isoformat() if hasattr(latest, 'isoformat') else (str(latest) if latest else None)
        try:
            if isinstance(latest, str):
                latest_d = datetime.strptime(latest, '%Y-%m-%d').date()
            elif hasattr(latest, 'isoformat'):
                latest_d = latest if hasattr(latest, 'days') is False and not isinstance(latest, datetime) else (latest.date() if isinstance(latest, datetime) else latest)
            else:
                latest_d = latest
            s['snapshot_age_days'] = (today - latest_d).days if latest_d else None
        except Exception:
            s['snapshot_age_days'] = None

        # Status counts at latest snapshot
        if latest:
            try:
                if USE_POSTGRES:
                    cur = db.cursor()
                    cur.execute(
                        "SELECT status, COUNT(*), COALESCE(SUM(on_hand),0) FROM sod_inventory "
                        "WHERE sku=%s AND snapshot_date=%s GROUP BY status",
                        (sku, latest),
                    )
                    rows = cur.fetchall()
                    cur.close()
                else:
                    rows = db.execute(
                        "SELECT status, COUNT(*), COALESCE(SUM(on_hand),0) FROM sod_inventory "
                        "WHERE sku=? AND snapshot_date=?",
                        (sku, latest),
                    ).fetchall()
                status_counts = {r[0]: int(r[1]) for r in rows}
                onhand = sum(int(r[2] or 0) for r in rows if r[0] == 'L')
                s['status_counts'] = {
                    'L': status_counts.get('L', 0),
                    'D': status_counts.get('D', 0),
                    'F': status_counts.get('F', 0),
                    'total': sum(status_counts.values()),
                }
                s['on_hand_units'] = onhand
            except Exception as e:
                if USE_POSTGRES:
                    try: db.rollback()
                    except Exception: pass
                s['status_counts'] = {'error': str(e)[:120]}
                s['on_hand_units'] = 0
        else:
            s['status_counts'] = {'L': 0, 'D': 0, 'F': 0, 'total': 0}
            s['on_hand_units'] = 0

        # LCBO.com live store count (latest inventory_history per sku)
        try:
            sku_clean = sku.lstrip('0')
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute("SELECT id FROM products WHERE lcbo_sku=%s LIMIT 1", (sku_clean,))
                prow = cur.fetchone()
                cur.close()
            else:
                prow = db.execute("SELECT id FROM products WHERE lcbo_sku=? LIMIT 1", (sku_clean,)).fetchone()
            pid = prow[0] if prow else None
        except Exception:
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass
            pid = None
        if pid:
            try:
                if USE_POSTGRES:
                    cur = db.cursor()
                    cur.execute(
                        "SELECT COUNT(DISTINCT store_number), COALESCE(SUM(quantity),0) "
                        "FROM inventory_history WHERE product_id=%s AND quantity > 0 "
                        "AND recorded_at >= NOW() - INTERVAL '24 hours'",
                        (pid,),
                    )
                    row = cur.fetchone()
                    cur.close()
                else:
                    row = db.execute(
                        "SELECT COUNT(DISTINCT store_number), COALESCE(SUM(quantity),0) "
                        "FROM inventory_history WHERE product_id=? AND quantity > 0 "
                        "AND recorded_at >= datetime('now','-24 hours')",
                        (pid,),
                    ).fetchone()
                s['lcbo_live_24h'] = {
                    'stores_with_inventory': int(row[0] or 0),
                    'total_units': int(row[1] or 0),
                }
            except Exception:
                if USE_POSTGRES:
                    try: db.rollback()
                    except Exception: pass
                s['lcbo_live_24h'] = None
        else:
            s['lcbo_live_24h'] = None

        # LCBO_LIVE_ONLY discoveries last 30d
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM sod_store_sku_changes "
                    "WHERE sku=%s AND change_type='LCBO_LIVE_ONLY' AND change_date >= %s",
                    (sku, since_30d),
                )
                s['lcbo_only_discoveries_30d'] = int(cur.fetchone()[0] or 0)
                cur.close()
            else:
                s['lcbo_only_discoveries_30d'] = int((db.execute(
                    "SELECT COUNT(*) FROM sod_store_sku_changes "
                    "WHERE sku=? AND change_type='LCBO_LIVE_ONLY' AND change_date >= ?",
                    (sku, since_30d),
                ).fetchone() or [0])[0])
        except Exception:
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass
            s['lcbo_only_discoveries_30d'] = None

        # Listing-flip events last 30d (NEW_LISTING, DELISTING_NOW, etc.)
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(
                    "SELECT change_type, COUNT(*) FROM sod_store_sku_changes "
                    "WHERE sku=%s AND change_date >= %s GROUP BY change_type",
                    (sku, since_30d),
                )
                s['flips_30d'] = {r[0]: int(r[1]) for r in cur.fetchall()}
                cur.close()
            else:
                s['flips_30d'] = dict((r[0], int(r[1])) for r in db.execute(
                    "SELECT change_type, COUNT(*) FROM sod_store_sku_changes "
                    "WHERE sku=? AND change_date >= ? GROUP BY change_type",
                    (sku, since_30d),
                ).fetchall())
        except Exception:
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass
            s['flips_30d'] = None

        # Duplicate row detection (sku/store/date with > 1 row)
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT sku, store_number, snapshot_date, COUNT(*) AS c "
                    "  FROM sod_inventory WHERE sku=%s "
                    "  GROUP BY sku, store_number, snapshot_date HAVING COUNT(*) > 1"
                    ") d",
                    (sku,),
                )
                s['duplicate_rows'] = int(cur.fetchone()[0] or 0)
                cur.close()
            else:
                s['duplicate_rows'] = int((db.execute(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT sku, store_number, snapshot_date, COUNT(*) AS c "
                    "  FROM sod_inventory WHERE sku=? "
                    "  GROUP BY sku, store_number, snapshot_date HAVING c > 1"
                    ")",
                    (sku,),
                ).fetchone() or [0])[0])
        except Exception:
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass
            s['duplicate_rows'] = None

        # Drift: SOD says listed but lcbo.com 0; or SOD missing/F but lcbo.com > 0.
        # ONLY compute drift when BOTH sources have recent data — if lcbo.com hasn't
        # been scraped recently, skip drift comparison (would otherwise show a
        # false 100% drift just because lcbo_live_24h is empty).
        sod_listed = s.get('status_counts', {}).get('L', 0) if isinstance(s.get('status_counts'), dict) else 0
        lcbo_live_data = s.get('lcbo_live_24h')
        if isinstance(lcbo_live_data, dict) and lcbo_live_data.get('stores_with_inventory') is not None:
            lcbo_live = lcbo_live_data.get('stores_with_inventory', 0)
            # Only compute drift if lcbo.com has SOME data (otherwise scrape may not have run)
            if lcbo_live > 0 or sod_listed == 0:
                drift = abs(sod_listed - lcbo_live)
                drift_pct = drift / max(sod_listed, lcbo_live, 1) * 100
                s['drift'] = {
                    'sod_listed_count': sod_listed,
                    'lcbo_live_24h_count': lcbo_live,
                    'difference': sod_listed - lcbo_live,
                    'drift_pct': round(drift_pct, 1),
                }
                drift_total += drift
                listed_total += sod_listed
            else:
                # lcbo.com data unavailable — skip drift, don't penalize
                s['drift'] = {'note': 'lcbo.com scrape data unavailable in last 24h — drift not computed'}
        else:
            s['drift'] = None

        # Per-SKU grade
        age = s.get('snapshot_age_days')
        dups = s.get('duplicate_rows') or 0
        drift_obj = s.get('drift') or {}
        # Only penalize for drift if it was actually computed (not "skipped because no lcbo data")
        drift_pct = drift_obj.get('drift_pct') if 'drift_pct' in drift_obj else None
        if not s.get('latest_snapshot'):
            s['integrity_grade'] = 'F'
            s['grade_reason'] = 'no_snapshot'
        elif age is not None and age > 2:
            s['integrity_grade'] = 'D'
            s['grade_reason'] = f'snapshot_{age}d_old'
        elif dups > 0:
            s['integrity_grade'] = 'C'
            s['grade_reason'] = f'{dups}_duplicate_rows'
        elif drift_pct is not None and drift_pct > 25:
            s['integrity_grade'] = 'C'
            s['grade_reason'] = f'drift_{drift_pct}pct'
        elif drift_pct is not None and drift_pct > 10:
            s['integrity_grade'] = 'B'
            s['grade_reason'] = f'drift_{drift_pct}pct'
        elif age is not None and age > 1:
            s['integrity_grade'] = 'B'
            s['grade_reason'] = f'snapshot_{age}d_old'
        else:
            s['integrity_grade'] = 'A'
            s['grade_reason'] = 'all_checks_pass'

        report['per_sku'].append(s)

    # Global grade (worst per-SKU grade caps the global)
    grades = [s.get('integrity_grade', 'F') for s in report['per_sku']]
    if 'F' in grades:
        report['global_grade'] = 'F'
    elif 'D' in grades:
        report['global_grade'] = 'D'
    elif 'C' in grades:
        report['global_grade'] = 'C'
    elif 'B' in grades:
        report['global_grade'] = 'B'
    else:
        report['global_grade'] = 'A'

    # Aggregate drift
    if listed_total > 0:
        report['global']['aggregate_drift_pct'] = round(drift_total / listed_total * 100, 1)
    else:
        report['global']['aggregate_drift_pct'] = None

    return jsonify(report)


@app.route('/api/admin/db-stats', methods=['GET'])
def api_admin_db_stats():
    """Quick DB sanity stats — row counts per table + size hints. Public (read-only)."""
    db = get_db()
    stats = {}
    for tname, _pk in _EXPORT_TABLES:
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(f"SELECT COUNT(*) FROM {tname}")
                stats[tname] = int(cur.fetchone()[0])
                cur.close()
            else:
                row = db.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()
                stats[tname] = int(row[0])
        except Exception as e:
            # Postgres: roll back so subsequent table queries don't fail in cascade
            if USE_POSTGRES:
                try:
                    db.rollback()
                except Exception:
                    pass
            stats[tname] = f"ERR: {str(e)[:100]}"
    return jsonify({
        'tables': stats,
        'total_rows': sum(v for v in stats.values() if isinstance(v, int)),
        'snapshot_freshness': _sod_freshness(),
        'admin_token_required': bool(os.environ.get('ADMIN_TOKEN')),
        'data_host': 'Neon Postgres (separate from app host) — data persists across host migrations',
    })


def _build_essential_backup():
    """Build the essential backup payload (small enough to email): every CRM
    table EXCEPT the giant SOD inventory snapshots. Returns a dict ready to
    be JSON-serialized.
    """
    db = get_db()
    out = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'schema_version': 1,
        'include': 'core',
        'tables': {},
    }
    excluded = {'sod_inventory', 'inventory_history'}
    for tname, _pk in _EXPORT_TABLES:
        if tname in excluded:
            continue
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(f"SELECT * FROM {tname}")
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, [_json_safe(v) for v in row])) for row in cur.fetchall()]
                cur.close()
            else:
                rows_raw = db.execute(f"SELECT * FROM {tname}").fetchall()
                cols = [d[0] for d in db.execute(f"SELECT * FROM {tname} LIMIT 0").description]
                rows = [dict(zip(cols, [_json_safe(v) for v in row])) for row in rows_raw]
            out['tables'][tname] = {'row_count': len(rows), 'columns': cols, 'rows': rows}
        except Exception as e:
            # Postgres: must rollback after error or subsequent queries fail
            if USE_POSTGRES:
                try:
                    db.rollback()
                except Exception:
                    pass
            out['tables'][tname] = {'error': str(e)}
    return out


def _send_backup_email(payload: dict):
    """Email the backup JSON as an attachment via Resend. Best-effort — logs
    failure but never raises (we don't want backup failure to crash the cron).
    """
    resend_key = os.environ.get('RESEND_API_KEY', '').strip()
    to_addr = os.environ.get('ALERT_EMAIL_TO', '').strip()
    from_addr = os.environ.get('ALERT_EMAIL_FROM', 'alerts@anu-lcbo.local').strip()
    if not (resend_key and to_addr and http_requests):
        print("[backup] skipped — RESEND_API_KEY or ALERT_EMAIL_TO not configured")
        return False
    import json as _json, base64
    body_json = _json.dumps(payload, separators=(',', ':'))
    b64 = base64.b64encode(body_json.encode('utf-8')).decode('ascii')
    today = datetime.utcnow().strftime('%Y-%m-%d')
    total_rows = sum(t.get('row_count', 0) for t in payload.get('tables', {}).values() if isinstance(t, dict))
    summary_lines = ['Daily backup — Anu LCBO Tracker', f"Date: {today}",
                     f"Total rows: {total_rows}", '', 'Per-table row counts:']
    for tname, td in sorted(payload.get('tables', {}).items()):
        if isinstance(td, dict):
            n = td.get('row_count', 'ERR' if 'error' in td else '?')
            summary_lines.append(f"  • {tname:30s} {n}")
    summary_lines += ['', 'To restore on a new host:',
                      '  1. Stand up the app on a new host pointed at a fresh Postgres DB',
                      '  2. Download this attachment',
                      '  3. POST it to /api/admin/import?mode=merge with X-Admin-Token header',
                      '',
                      'Note: SOD inventory rows (1M+) are NOT in this backup — those rebuild',
                      'automatically on the next SOD sync. CRM data, audit log, and pipeline',
                      'state ARE in this backup.']
    try:
        r = http_requests.post(
            'https://api.resend.com/emails',
            headers={'Authorization': f'Bearer {resend_key}', 'Content-Type': 'application/json'},
            json={
                'from': from_addr,
                'to': [a.strip() for a in to_addr.split(',') if a.strip()],
                'subject': f'[Anu LCBO] Daily backup — {today} — {total_rows} rows',
                'text': '\n'.join(summary_lines),
                'attachments': [{
                    'filename': f'anu-lcbo-backup-{today}.json',
                    'content': b64,
                }],
            },
            timeout=30,
        )
        if r.status_code in (200, 201, 202):
            print(f"[backup] daily backup emailed ({total_rows} rows, {len(body_json)} bytes)")
            return True
        else:
            print(f"[backup] resend failed {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[backup] email exception: {e}")
    return False


_backup_scheduler = None


def start_backup_scheduler():
    """Run a daily backup at 02:00 ET (before SOD sync at 03:00) — emails the
    full CRM backup as a JSON attachment to ALERT_EMAIL_TO. Belt-and-suspenders
    for data loss: if Render runs out of credits AND Neon goes offline AND we
    lose the latest hot data, the user still has yesterday's snapshot in email.
    """
    global _backup_scheduler
    if _backup_scheduler is not None:
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print('[backup] apscheduler not installed — skipping')
        return
    try:
        try:
            sched = BackgroundScheduler(timezone='America/Toronto')
        except Exception:
            sched = BackgroundScheduler()

        def _run_backup():
            try:
                payload = _build_essential_backup()
                ok = _send_backup_email(payload)
                if not ok:
                    send_alert(
                        subject="Daily backup failed to send",
                        body="The daily backup couldn't be emailed. Verify RESEND_API_KEY + "
                             "ALERT_EMAIL_TO are set. Hit /api/admin/export manually as a fallback.",
                        level='warning',
                    )
            except Exception as e:
                print(f"[backup] daily run failed: {e}")
                try:
                    send_alert(
                        subject="Daily backup raised an exception",
                        body=f"_run_backup exception: {e}",
                        level='warning',
                    )
                except Exception:
                    pass

        sched.add_job(
            _run_backup,
            CronTrigger(hour=2, minute=0),  # 02:00 ET
            id='daily_backup',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 6,
        )
        sched.start()
        _backup_scheduler = sched
        print('[backup] Daily backup scheduled at 02:00 ET (emails to ALERT_EMAIL_TO)')
    except Exception as e:
        print(f'[backup] scheduler failed: {e}')


@app.route('/api/admin/run-backup-now', methods=['POST'])
def api_admin_run_backup_now():
    """Trigger an immediate backup-to-email. Auth: X-Admin-Token."""
    if not _admin_token_ok():
        return jsonify({'error': 'forbidden — set X-Admin-Token header'}), 403
    payload = _build_essential_backup()
    ok = _send_backup_email(payload)
    return jsonify({
        'status': 'sent' if ok else 'skipped',
        'rows': sum(t.get('row_count', 0) for t in payload.get('tables', {}).values() if isinstance(t, dict)),
        'email_to': os.environ.get('ALERT_EMAIL_TO', '(not set)'),
    })


# ============================================================================
# TASTING BOOKINGS — book future tastings, see upcoming, .ics calendar export,
#                   daily digest email of tomorrow's tastings.
#
# Data model: backed by the existing `deals` table with stage='tasting_scheduled'
# and next_action_date=<future date>. Plus the corresponding store_number and
# rep ownership. This keeps tastings inside the existing pipeline so manager
# dashboards naturally count them.
# ============================================================================

@app.route('/api/crm/tasting-booking', methods=['POST'])
def api_crm_book_tasting():
    """Book a future tasting. Body:
      { store_number: int, rep: str, scheduled_date: 'YYYY-MM-DD',
        sku?: str, notes?: str, expected_units?: int }

    Creates a deal with stage='tasting_scheduled' so it shows up in the pipeline
    AND in the daily morning digest email of tomorrow's tastings. Idempotent on
    (store_number, rep, scheduled_date, sku) — booking the same slot twice is
    a no-op (returns the existing deal id).
    """
    body = request.get_json(silent=True) or {}
    store_number = body.get('store_number')
    rep = (body.get('rep') or '').strip()
    sched = (body.get('scheduled_date') or '').strip()
    sku = (body.get('sku') or '').strip()
    notes = (body.get('notes') or '').strip()
    expected_units = int(body.get('expected_units') or 0)

    if not store_number or not rep or not sched:
        return jsonify({'error': 'store_number, rep, scheduled_date are required'}), 400

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # Find existing booking (idempotent)
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"SELECT id FROM deals WHERE store_number={ph} AND owner_rep={ph} AND "
            f"next_action_date={ph} AND stage='tasting_scheduled' "
            f"AND COALESCE(sku,'')={ph} LIMIT 1",
            (store_number, rep, sched, sku),
        )
        row = cur.fetchone()
        if row:
            cur.close()
            return jsonify({'status': 'exists', 'deal_id': row[0]})
        cur.execute(
            f"INSERT INTO deals (store_number, sku, stage, probability, "
            f"next_action_date, expected_units, owner_rep, next_action, notes, source) "
            f"VALUES ({ph},{ph},'tasting_scheduled',40,{ph},{ph},{ph},'Tasting',{ph},'manual') RETURNING id",
            (store_number, sku, sched, expected_units, rep, notes),
        )
        new_id = cur.fetchone()[0]
        db.commit()
        cur.close()
    else:
        row = db.execute(
            f"SELECT id FROM deals WHERE store_number={ph} AND owner_rep={ph} AND "
            f"next_action_date={ph} AND stage='tasting_scheduled' AND COALESCE(sku,'')={ph} LIMIT 1",
            (store_number, rep, sched, sku),
        ).fetchone()
        if row:
            return jsonify({'status': 'exists', 'deal_id': row[0]})
        c = db.execute(
            f"INSERT INTO deals (store_number, sku, stage, probability, "
            f"next_action_date, expected_units, owner_rep, next_action, notes, source) "
            f"VALUES ({ph},{ph},'tasting_scheduled',40,{ph},{ph},{ph},'Tasting',{ph},'manual')",
            (store_number, sku, sched, expected_units, rep, notes),
        )
        new_id = c.lastrowid
        db.commit()

    try:
        _log_event('tasting_booked', rep, store_number, sku,
                   {'scheduled_date': sched, 'expected_units': expected_units, 'notes': notes[:200]})
    except Exception:
        pass

    return jsonify({'status': 'booked', 'deal_id': new_id, 'scheduled_date': sched, 'rep': rep})


@app.route('/api/crm/tastings/upcoming', methods=['GET'])
def api_crm_tastings_upcoming():
    """List upcoming tasting bookings. Query: ?days=14&rep=Ikshit (rep optional).

    Joins to stores for full store info so the rep card shows where + when + with whom.
    """
    days = int(request.args.get('days', 14))
    rep = (request.args.get('rep') or '').strip()
    today = _toronto_today().isoformat()
    until = (_toronto_today() + timedelta(days=days)).isoformat()

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = [
        "d.stage='tasting_scheduled'",
        f"d.next_action_date >= {ph}",
        f"d.next_action_date <= {ph}",
        "(d.closed_at IS NULL)",
    ]
    params = [today, until]
    if rep:
        where.append(f"d.owner_rep = {ph}")
        params.append(rep)

    sql = f"""
        SELECT d.id, d.store_number, d.sku, d.next_action_date, d.expected_units,
               d.owner_rep, d.notes, d.created_at,
               COALESCE(s.account,'') AS account,
               COALESCE(s.address,'') AS address,
               COALESCE(s.city,'') AS city,
               COALESCE(s.postal,'') AS postal,
               COALESCE(s.manager_name,'') AS manager_name,
               COALESCE(s.manager_phone, s.phone, '') AS phone,
               COALESCE(t.name,'') AS territory_name,
               COALESCE(t.color,'#888') AS territory_color
        FROM deals d
        LEFT JOIN stores s ON s.store_number = d.store_number
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE {' AND '.join(where)}
        ORDER BY d.next_action_date ASC, d.owner_rep
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(sql, params).fetchall()

    bookings = [
        {
            'deal_id': r[0],
            'store_number': r[1],
            'sku': r[2] or '',
            'scheduled_date': str(r[3]) if r[3] else None,
            'expected_units': int(r[4] or 0),
            'rep': r[5] or '',
            'notes': r[6] or '',
            'booked_at': str(r[7]) if r[7] else None,
            'account': r[8],
            'address': r[9],
            'city': r[10],
            'postal': r[11],
            'manager_name': r[12],
            'phone': r[13],
            'territory_name': r[14],
            'territory_color': r[15],
        }
        for r in rows
    ]
    return jsonify({
        'window': {'from': today, 'to': until, 'days': days},
        'rep': rep or '(all)',
        'count': len(bookings),
        'bookings': bookings,
    })


@app.route('/api/crm/calendar/<rep_name>.ics', methods=['GET'])
def api_crm_calendar_ics(rep_name):
    """iCal export of upcoming tastings for one rep. Subscribe to this URL in
    Google Calendar / Apple Calendar to see all bookings on the rep's phone.
    """
    days = int(request.args.get('days', 60))
    today = _toronto_today().isoformat()
    until = (_toronto_today() + timedelta(days=days)).isoformat()

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    sql = f"""
        SELECT d.id, d.store_number, d.sku, d.next_action_date, d.notes,
               COALESCE(s.account,''), COALESCE(s.address,''), COALESCE(s.city,''),
               COALESCE(s.manager_name,''), COALESCE(s.manager_phone, s.phone, '')
        FROM deals d
        LEFT JOIN stores s ON s.store_number = d.store_number
        WHERE d.stage='tasting_scheduled'
          AND d.closed_at IS NULL
          AND d.owner_rep = {ph}
          AND d.next_action_date >= {ph}
          AND d.next_action_date <= {ph}
        ORDER BY d.next_action_date ASC
    """
    params = (rep_name, today, until)
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(sql, params).fetchall()

    # Build .ics
    lines = [
        'BEGIN:VCALENDAR',
        'VERSION:2.0',
        'PRODID:-//Anu Spirits//LCBO Tracker//EN',
        'CALSCALE:GREGORIAN',
        f'X-WR-CALNAME:Anu Tastings — {rep_name}',
        'X-WR-TIMEZONE:America/Toronto',
    ]
    for r in rows:
        d_id, store_no, sku, sched, notes, account, address, city, manager, phone = r
        date_str = str(sched).replace('-', '')
        summary = f"Tasting #{store_no} {account or ''}".strip()
        descr_parts = []
        if sku:
            descr_parts.append(f"SKU: {sku}")
        if manager:
            descr_parts.append(f"Manager: {manager}")
        if phone:
            descr_parts.append(f"Phone: {phone}")
        if notes:
            descr_parts.append(f"Notes: {notes}")
        descr = '\\n'.join(descr_parts).replace(',', '\\,').replace(';', '\\;')
        loc = f"{address}, {city}".strip(', ').replace(',', '\\,').replace(';', '\\;')
        lines += [
            'BEGIN:VEVENT',
            f'UID:tasting-{d_id}@anu-lcbo',
            f'DTSTAMP:{datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")}',
            f'DTSTART;VALUE=DATE:{date_str}',
            f'DTEND;VALUE=DATE:{date_str}',
            f'SUMMARY:{summary}',
            f'DESCRIPTION:{descr}',
            f'LOCATION:{loc}',
            'END:VEVENT',
        ]
    lines.append('END:VCALENDAR')
    return '\r\n'.join(lines), 200, {
        'Content-Type': 'text/calendar; charset=utf-8',
        'Content-Disposition': f'attachment; filename="anu-tastings-{rep_name}.ics"',
    }


def _send_tasting_digest_email(when='tomorrow'):
    """Email tomorrow's tasting schedule to TASTING_DIGEST_TO. Falls back to
    ALERT_EMAIL_TO if TASTING_DIGEST_TO is not set. Best-effort.
    """
    if when == 'tomorrow':
        target_date = (_toronto_today() + timedelta(days=1)).isoformat()
        when_label = 'Tomorrow'
    elif when == 'today':
        target_date = _toronto_today().isoformat()
        when_label = "Today"
    else:
        target_date = when
        when_label = when

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    sql = f"""
        SELECT d.store_number, d.sku, d.expected_units, d.owner_rep, d.notes,
               COALESCE(s.account,''), COALESCE(s.address,''), COALESCE(s.city,''),
               COALESCE(s.manager_name,''), COALESCE(s.manager_phone, s.phone, '')
        FROM deals d
        LEFT JOIN stores s ON s.store_number = d.store_number
        WHERE d.stage='tasting_scheduled'
          AND d.closed_at IS NULL
          AND d.next_action_date = {ph}
        ORDER BY d.owner_rep, d.store_number
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql, (target_date,))
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(sql, (target_date,)).fetchall()

    # Email config
    to_addr = (
        os.environ.get('TASTING_DIGEST_TO', '').strip()
        or os.environ.get('ALERT_EMAIL_TO', '').strip()
    )
    resend_key = os.environ.get('RESEND_API_KEY', '').strip()
    from_addr = os.environ.get('ALERT_EMAIL_FROM', 'alerts@anu-lcbo.local').strip()

    if not (resend_key and to_addr and http_requests):
        print("[tasting-digest] skipped — RESEND_API_KEY or TASTING_DIGEST_TO not configured")
        return False

    if not rows:
        # Still send a "no tastings" email so user knows the digest is alive.
        body_text = f"No tastings booked for {when_label} ({target_date}).\n\nUse /api/crm/tasting-booking to book new tastings."
        subject = f"[Anu LCBO] {when_label}'s tastings — none booked ({target_date})"
    else:
        # Group by rep
        by_rep = {}
        for r in rows:
            store_no, sku, units, rep, notes, account, address, city, manager, phone = r
            by_rep.setdefault(rep, []).append({
                'store_number': store_no, 'sku': sku, 'units': units,
                'account': account, 'address': address, 'city': city,
                'manager': manager, 'phone': phone, 'notes': notes,
            })
        lines = [f"{when_label}'s tastings — {target_date}", '=' * 50, '']
        for rep, items in sorted(by_rep.items()):
            lines.append(f"📋 {rep} ({len(items)} tasting{'s' if len(items)!=1 else ''}):")
            for i, it in enumerate(items, 1):
                lines.append(f"  {i}. #{it['store_number']} {it['account']}")
                if it['address']:
                    lines.append(f"     📍 {it['address']}, {it['city']}")
                if it['manager']:
                    lines.append(f"     👤 {it['manager']}{' · ' + it['phone'] if it['phone'] else ''}")
                if it['sku']:
                    lines.append(f"     🍷 SKU {it['sku']}{' (' + str(it['units']) + ' units)' if it['units'] else ''}")
                if it['notes']:
                    lines.append(f"     📝 {it['notes']}")
                lines.append('')
            lines.append('')
        body_text = '\n'.join(lines)
        subject = f"[Anu LCBO] {when_label}'s tastings — {len(rows)} booked ({target_date})"

    try:
        r = http_requests.post(
            'https://api.resend.com/emails',
            headers={'Authorization': f'Bearer {resend_key}', 'Content-Type': 'application/json'},
            json={
                'from': from_addr,
                'to': [a.strip() for a in to_addr.split(',') if a.strip()],
                'subject': subject,
                'text': body_text,
            },
            timeout=20,
        )
        if r.status_code in (200, 201, 202):
            print(f"[tasting-digest] sent {len(rows)} tastings for {target_date}")
            return True
        else:
            print(f"[tasting-digest] resend failed {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[tasting-digest] exception: {e}")
    return False


@app.route('/api/admin/send-tasting-digest', methods=['POST', 'GET'])
def api_admin_send_tasting_digest():
    """Manually trigger the tomorrow's-tastings digest email.
    Useful for testing or if you missed the 06:30 ET cron.
    """
    when = request.args.get('when', 'tomorrow')
    ok = _send_tasting_digest_email(when=when)
    return jsonify({
        'status': 'sent' if ok else 'skipped',
        'when': when,
        'to': os.environ.get('TASTING_DIGEST_TO', os.environ.get('ALERT_EMAIL_TO', '(not set)')),
    })


_tasting_digest_scheduler = None


def start_tasting_digest_scheduler():
    """Daily 06:30 ET email of tomorrow's tasting schedule. Recipients via
    TASTING_DIGEST_TO env var (comma-separated). Falls back to ALERT_EMAIL_TO.
    """
    global _tasting_digest_scheduler
    if _tasting_digest_scheduler is not None:
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print('[tasting-digest] apscheduler not installed — skipping')
        return
    try:
        try:
            sched = BackgroundScheduler(timezone='America/Toronto')
        except Exception:
            sched = BackgroundScheduler()

        sched.add_job(
            lambda: _send_tasting_digest_email('tomorrow'),
            CronTrigger(hour=6, minute=30),  # 06:30 ET — after morning health check
            id='daily_tasting_digest',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600 * 6,
        )
        sched.start()
        _tasting_digest_scheduler = sched
        print('[tasting-digest] Daily tasting digest scheduled at 06:30 ET (emails to TASTING_DIGEST_TO)')
    except Exception as e:
        print(f'[tasting-digest] scheduler failed: {e}')


def _json_safe(v):
    """Make a DB value JSON-friendly."""
    if v is None:
        return None
    if isinstance(v, (int, float, bool, str)):
        return v
    try:
        # datetime/date
        return v.isoformat()
    except Exception:
        return str(v)


# ------- CRM dashboard rollup — one-shot for the homepage -------
@app.route('/api/crm/dashboard', methods=['GET'])
@cached_response(ttl_seconds=60, key_args=())
def api_crm_dashboard():
    """Everything the main CRM dashboard needs in one call.

    Returns: summary KPIs, OOS-risk count, gap count, recent listings/delistings,
             territory breakdown, tracked-SKU rollup.
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # SOD latest snapshot
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory")
        latest = cur.fetchone()[0]
    else:
        latest = db.execute("SELECT MAX(snapshot_date) FROM sod_inventory").fetchone()[0]

    # Tracked SKU rollup from sod_products
    tracked = list(SOD_TRACKED_SKUS.keys())
    sku_rollup = []
    if tracked:
        phs = ','.join([ph] * len(tracked))
        if USE_POSTGRES:
            cur.execute(
                f"SELECT sku, product_name, current_status, store_count, total_on_hand, brand "
                f"FROM sod_products WHERE sku IN ({phs}) ORDER BY sku",
                tracked,
            )
            rr = cur.fetchall()
        else:
            rr = db.execute(
                f"SELECT sku, product_name, current_status, store_count, total_on_hand, brand "
                f"FROM sod_products WHERE sku IN ({phs}) ORDER BY sku",
                tracked,
            ).fetchall()
        for r in rr:
            brand, pname = SOD_TRACKED_SKUS.get(r[0], (r[5] or '', r[1]))
            sku_rollup.append({
                'sku': r[0], 'brand': brand, 'product_name': pname or r[1],
                'current_status': r[2], 'store_count': r[3], 'total_on_hand': r[4],
            })

    # OOS brink count (on_hand <= 2, tracked, L).
    # Use an explicit CTE so both Postgres and SQLite parse the correlated sub-lookup
    # consistently. The previous inline correlated subquery was ambiguous in Postgres
    # (no alias on outer table) → 500.
    oos_brink_count = 0
    if tracked and latest:
        phs = ','.join([ph] * len(tracked))
        q = f"""
            WITH latest_per_sku AS (
                SELECT sku, MAX(snapshot_date) AS d
                FROM sod_inventory WHERE sku IN ({phs})
                GROUP BY sku
            )
            SELECT COUNT(*) FROM sod_inventory i
            JOIN latest_per_sku l ON l.sku = i.sku AND l.d = i.snapshot_date
            WHERE i.status='L' AND i.on_hand <= 2
        """
        if USE_POSTGRES:
            cur.execute(q, tracked)
            oos_brink_count = cur.fetchone()[0] or 0
        else:
            oos_brink_count = db.execute(q, tracked).fetchone()[0] or 0

    # Listings/delistings last 7 days
    since = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')
    if USE_POSTGRES:
        cur.execute(
            "SELECT change_type, COUNT(*) FROM sod_listing_changes WHERE change_date >= %s GROUP BY change_type",
            (since,),
        )
        digest_counts = {r[0]: r[1] for r in cur.fetchall()}
    else:
        digest_counts = {r[0]: r[1] for r in db.execute(
            "SELECT change_type, COUNT(*) FROM sod_listing_changes WHERE change_date >= ?",
            (since,),
        ).fetchall()}

    # Territory store counts
    if USE_POSTGRES:
        cur.execute("""
            SELECT t.code, t.name, t.color, COUNT(s.id)
            FROM territories t LEFT JOIN stores s ON s.territory_id = t.id
            GROUP BY t.code, t.name, t.color ORDER BY t.name
        """)
        terr = [{'code': r[0], 'name': r[1], 'color': r[2], 'store_count': r[3]} for r in cur.fetchall()]
        cur.close()
    else:
        terr = [{'code': r[0], 'name': r[1], 'color': r[2], 'store_count': r[3]} for r in db.execute("""
            SELECT t.code, t.name, t.color, COUNT(s.id)
            FROM territories t LEFT JOIN stores s ON s.territory_id = t.id
            GROUP BY t.code, t.name, t.color ORDER BY t.name
        """).fetchall()]

    return jsonify({
        'latest_snapshot': str(latest) if latest else None,
        'tracked_sku_rollup': sku_rollup,
        'oos_brink_count': oos_brink_count,
        'digest_last_7_days': digest_counts,
        'territories': terr,
    })


# ======================================================================================
# ============================== SPRINT 2 BACKEND ======================================
#
# Endpoints supporting commercial-grade UX features:
#   - /api/crm/sku-trend/<sku> — daily store_count + on_hand for time-series chart
#   - /api/crm/store-trend/<store_number> — daily history for one store
#   - /api/crm/wow-deltas — WoW / MoM / YoY comparison KPIs
#   - /api/crm/nearby — stores within radius of (lat,lng), sorted by distance
#                       (with opportunity score for the rep)
#   - /api/ai/ask — Claude-powered natural-language assistant (NL → SQL → answer)
# ======================================================================================


@app.route('/api/crm/sku-trend/<sku>', methods=['GET'])
@cached_response(ttl_seconds=300, key_args=())
def api_crm_sku_trend(sku):
    """Daily aggregates for a SKU over the last N days (default 90).

    Returns one row per snapshot_date with:
      store_count (where status='L'), delisting_count, total_on_hand, avg_on_hand
    """
    days = int(request.args.get('days', 90))
    sku_norm = sku.zfill(7)
    since = (_toronto_today() - timedelta(days=days)).isoformat()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    q = f"""
        SELECT snapshot_date,
               SUM(CASE WHEN status='L' THEN 1 ELSE 0 END) AS listed_count,
               SUM(CASE WHEN status='D' THEN 1 ELSE 0 END) AS delisting_count,
               SUM(CASE WHEN status='F' THEN 1 ELSE 0 END) AS fully_delisted_count,
               COALESCE(SUM(on_hand), 0) AS total_on_hand,
               COUNT(*) AS row_count
        FROM sod_inventory
        WHERE sku = {ph} AND snapshot_date >= {ph}
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, (sku_norm, since))
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, (sku_norm, since)).fetchall()

    series = [
        {
            'date': str(r[0]),
            'listed': int(r[1] or 0),
            'delisting': int(r[2] or 0),
            'fully_delisted': int(r[3] or 0),
            'total_on_hand': int(r[4] or 0),
            'avg_on_hand': round(float(r[4] or 0) / max(int(r[5] or 1), 1), 1),
        }
        for r in rows
    ]
    brand, name = SOD_TRACKED_SKUS.get(sku_norm, ('', ''))
    return jsonify({
        'sku': sku_norm,
        'brand': brand,
        'product_name': name,
        'days': days,
        'since': since,
        'series': series,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/store-trend/<int:store_number>', methods=['GET'])
@cached_response(ttl_seconds=300, key_args=())
def api_crm_store_trend(store_number):
    """Daily snapshot for one store across all tracked SKUs."""
    days = int(request.args.get('days', 90))
    since = (_toronto_today() - timedelta(days=days)).isoformat()
    tracked = list(SOD_TRACKED_SKUS.keys())
    if not tracked:
        return jsonify({'store_number': store_number, 'series': []})
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    phs = ','.join([ph] * len(tracked))
    q = f"""
        SELECT snapshot_date, sku, status, on_hand
        FROM sod_inventory
        WHERE store_number = {ph} AND sku IN ({phs}) AND snapshot_date >= {ph}
        ORDER BY snapshot_date, sku
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, [store_number] + tracked + [since])
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, [store_number] + tracked + [since]).fetchall()
    out = []
    for r in rows:
        brand, name = SOD_TRACKED_SKUS.get(r[1], ('', ''))
        out.append({
            'date': str(r[0]),
            'sku': r[1],
            'brand': brand,
            'product_name': name,
            'status': r[2],
            'on_hand': int(r[3] or 0),
        })
    return jsonify({
        'store_number': store_number,
        'days': days,
        'since': since,
        'series': out,
        'freshness': _sod_freshness(),
    })


def _backfill_store_sku_changes():
    """One-time backfill: walk historical sod_inventory snapshots in date order,
    diff per-(store,sku) status between consecutive dates for tracked SKUs, and
    populate sod_store_sku_changes. Idempotent (UNIQUE constraint).

    Safe to call repeatedly. Skips work if no new snapshots since last run.
    """
    try:
        ph = '%s' if USE_POSTGRES else '?'
        conn = _sod_get_conn()
        cur = conn.cursor()
        tracked = list(SOD_TRACKED_SKUS.keys())
        if not tracked:
            cur.close()
            conn.close()
            return 0
        phs = ','.join([ph] * len(tracked))
        # Get all distinct snapshot dates that contain any tracked SKU, ordered
        cur.execute(
            f"SELECT DISTINCT snapshot_date FROM sod_inventory "
            f"WHERE sku IN ({phs}) ORDER BY snapshot_date ASC",
            tracked,
        )
        dates = [str(r[0]) for r in cur.fetchall()]
        if len(dates) < 2:
            cur.close(); conn.close()
            return 0

        total_inserts = 0
        # For each consecutive pair of dates (per SKU), compute diffs and upsert
        for sku in tracked:
            # Get all snapshot dates that have this specific SKU
            cur.execute(
                f"SELECT DISTINCT snapshot_date FROM sod_inventory "
                f"WHERE sku = {ph} ORDER BY snapshot_date ASC",
                (sku,),
            )
            sku_dates = [str(r[0]) for r in cur.fetchall()]
            if len(sku_dates) < 2:
                continue
            # Walk pairwise
            inserts = []
            for i in range(1, len(sku_dates)):
                prev_date = sku_dates[i-1]
                curr_date = sku_dates[i]
                # prev per-store
                cur.execute(
                    f"SELECT store_number, status FROM sod_inventory "
                    f"WHERE sku = {ph} AND snapshot_date = {ph}",
                    (sku, prev_date),
                )
                prev_per_store = {r[0]: r[1] for r in cur.fetchall()}
                # curr per-store
                cur.execute(
                    f"SELECT store_number, status FROM sod_inventory "
                    f"WHERE sku = {ph} AND snapshot_date = {ph}",
                    (sku, curr_date),
                )
                curr_per_store = {r[0]: r[1] for r in cur.fetchall()}
                # Diffs
                for store, new_st in curr_per_store.items():
                    old_st = prev_per_store.get(store)
                    if old_st is None:
                        inserts.append((sku, store, curr_date, None, new_st, 'NEW_LISTING'))
                    elif old_st != new_st:
                        if new_st == 'L' and old_st in ('D', 'F'):
                            inserts.append((sku, store, curr_date, old_st, new_st, 'RELISTED'))
                        elif new_st in ('D', 'F') and old_st == 'L':
                            inserts.append((sku, store, curr_date, old_st, new_st, 'DELISTED'))
                        else:
                            inserts.append((sku, store, curr_date, old_st, new_st, 'STATUS_FLIP'))
                for store, old_st in prev_per_store.items():
                    if store not in curr_per_store:
                        inserts.append((sku, store, curr_date, old_st, None, 'DROPPED'))
            if inserts:
                if USE_POSTGRES:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO sod_store_sku_changes
                           (sku, store_number, change_date, old_status, new_status, change_type)
                           VALUES %s
                           ON CONFLICT (sku, store_number, change_date, change_type) DO NOTHING""",
                        inserts,
                    )
                else:
                    cur.executemany(
                        """INSERT INTO sod_store_sku_changes
                           (sku, store_number, change_date, old_status, new_status, change_type)
                           VALUES (?,?,?,?,?,?)
                           ON CONFLICT(sku, store_number, change_date, change_type) DO NOTHING""",
                        inserts,
                    )
                total_inserts += len(inserts)
        conn.commit()
        cur.close()
        conn.close()
        if total_inserts:
            print(f'[backfill] inserted {total_inserts} per-store change events')
        return total_inserts
    except Exception as e:
        print(f'[backfill] failed: {e}')
        return 0


@app.route('/api/crm/backfill-store-changes', methods=['POST'])
def api_crm_backfill_store_changes():
    """Manually trigger backfill of per-store changes from historical snapshots."""
    n = _backfill_store_sku_changes()
    return jsonify({'inserted': n, 'status': 'ok'})


@app.route('/api/crm/log-listing', methods=['POST'])
def api_crm_log_listing():
    """Manual listing event — rep marks a NEW_LISTING they know about before SOD detects it.

    Body: {sku, store_number, change_date (optional, defaults today), source (optional)}
    Inserts directly into sod_store_sku_changes with change_type='NEW_LISTING'.
    Idempotent via UNIQUE constraint.
    """
    body = request.get_json(silent=True) or {}
    sku = (body.get('sku') or '').strip()
    store_number = body.get('store_number')
    change_date = body.get('change_date') or _toronto_today().isoformat()
    if not sku or not store_number:
        return jsonify({'error': 'sku + store_number required'}), 400
    sku_norm = sku.zfill(7)
    if sku_norm not in SOD_TRACKED_SKUS:
        return jsonify({'error': f'sku {sku_norm} is not tracked'}), 400

    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            """INSERT INTO sod_store_sku_changes
               (sku, store_number, change_date, old_status, new_status, change_type)
               VALUES (%s, %s, %s, NULL, 'L', 'NEW_LISTING')
               ON CONFLICT (sku, store_number, change_date, change_type) DO NOTHING
               RETURNING id""",
            (sku_norm, int(store_number), change_date),
        )
        r = cur.fetchone()
        db.commit()
        cur.close()
        new_id = r[0] if r else None
    else:
        c = db.execute(
            """INSERT INTO sod_store_sku_changes
               (sku, store_number, change_date, old_status, new_status, change_type)
               VALUES (?, ?, ?, NULL, 'L', 'NEW_LISTING')
               ON CONFLICT(sku, store_number, change_date, change_type) DO NOTHING""",
            (sku_norm, int(store_number), change_date),
        )
        db.commit()
        new_id = c.lastrowid
    brand, name = SOD_TRACKED_SKUS[sku_norm]
    return jsonify({
        'status': 'ok' if new_id else 'duplicate_ignored',
        'id': new_id,
        'sku': sku_norm,
        'brand': brand,
        'product_name': name,
        'store_number': int(store_number),
        'change_date': change_date,
    })


@app.route('/api/crm/inventory-adds', methods=['GET'])
def api_crm_inventory_adds():
    """Stores where on_hand jumped from 0 (or no row) to >0 in last N days for tracked SKUs.

    Detects new shipments — distinct from NEW_LISTING (which is a status change).
    Cross-references sod_inventory snapshots: for each (sku, store), find dates where
    on_hand transitioned from 0/missing to a positive number.

    Query params: days (default 60, max 120)
    """
    days = min(int(request.args.get('days', 60)), 120)
    since = (_toronto_today() - timedelta(days=days)).isoformat()
    sku_filter = (request.args.get('sku') or '').strip()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    tracked = [sku_filter.zfill(7)] if sku_filter else list(SOD_TRACKED_SKUS.keys())
    phs = ','.join([ph] * len(tracked))

    # For each (sku, store) pair, walk snapshots in date order and find 0→positive transitions.
    # Use a window function to compare each row to its previous snapshot.
    if USE_POSTGRES:
        q = f"""
            WITH ranked AS (
                SELECT sku, store_number, snapshot_date, on_hand,
                       LAG(on_hand) OVER (PARTITION BY sku, store_number ORDER BY snapshot_date) AS prev_oh,
                       LAG(snapshot_date) OVER (PARTITION BY sku, store_number ORDER BY snapshot_date) AS prev_date
                FROM sod_inventory
                WHERE sku IN ({phs})
            )
            SELECT r.sku, r.store_number, r.snapshot_date, r.on_hand, r.prev_oh, r.prev_date,
                   s.account, s.city, s.postal, s.rep, t.name, COALESCE(t.color, '#888')
            FROM ranked r
            LEFT JOIN stores s ON s.store_number = r.store_number
            LEFT JOIN territories t ON t.id = s.territory_id
            WHERE r.snapshot_date >= {ph}
              AND r.on_hand > 0
              AND (r.prev_oh = 0 OR r.prev_oh IS NULL)
              AND r.prev_date IS NOT NULL
            ORDER BY r.snapshot_date DESC, r.on_hand DESC
            LIMIT 500
        """
        cur = db.cursor()
        cur.execute(q, tracked + [since])
        rows = cur.fetchall()
        cur.close()
    else:
        # SQLite supports LAG window function in 3.25+
        q = f"""
            WITH ranked AS (
                SELECT sku, store_number, snapshot_date, on_hand,
                       LAG(on_hand) OVER (PARTITION BY sku, store_number ORDER BY snapshot_date) AS prev_oh,
                       LAG(snapshot_date) OVER (PARTITION BY sku, store_number ORDER BY snapshot_date) AS prev_date
                FROM sod_inventory
                WHERE sku IN ({phs})
            )
            SELECT r.sku, r.store_number, r.snapshot_date, r.on_hand, r.prev_oh, r.prev_date,
                   s.account, s.city, s.postal, s.rep, t.name, COALESCE(t.color, '#888')
            FROM ranked r
            LEFT JOIN stores s ON s.store_number = r.store_number
            LEFT JOIN territories t ON t.id = s.territory_id
            WHERE r.snapshot_date >= ?
              AND r.on_hand > 0
              AND (r.prev_oh = 0 OR r.prev_oh IS NULL)
              AND r.prev_date IS NOT NULL
            ORDER BY r.snapshot_date DESC, r.on_hand DESC
            LIMIT 500
        """
        rows = db.execute(q, tracked + [since]).fetchall()

    out = []
    for r in rows:
        sku, store, snap, oh, prev_oh, prev_date, account, city, postal, rep, terr_name, terr_color = r
        brand, pname = SOD_TRACKED_SKUS.get(sku, ('', ''))
        out.append({
            'sku': sku,
            'brand': brand,
            'product_name': pname,
            'store_number': store,
            'snapshot_date': str(snap),
            'on_hand': int(oh or 0),
            'prev_on_hand': int(prev_oh or 0),
            'prev_date': str(prev_date) if prev_date else None,
            'jump': int(oh or 0) - int(prev_oh or 0),
            'account': account,
            'city': city,
            'postal': postal,
            'rep': rep,
            'territory_name': terr_name or 'Unassigned',
            'territory_color': terr_color or '#888',
        })

    # Per-SKU rollup
    by_sku = {}
    for o in out:
        k = o['sku']
        agg = by_sku.setdefault(k, {
            'sku': k, 'brand': o['brand'], 'product_name': o['product_name'],
            'event_count': 0, 'unique_stores': set(), 'total_units_added': 0,
        })
        agg['event_count'] += 1
        agg['unique_stores'].add(o['store_number'])
        agg['total_units_added'] += o['jump']
    per_sku = [
        {**v, 'unique_stores': len(v['unique_stores'])}
        for v in by_sku.values()
    ]

    # Available data window
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("SELECT MIN(snapshot_date), MAX(snapshot_date), COUNT(DISTINCT snapshot_date) FROM sod_inventory")
        r = cur.fetchone()
        cur.close()
    else:
        r = db.execute(
            "SELECT MIN(snapshot_date), MAX(snapshot_date), COUNT(DISTINCT snapshot_date) FROM sod_inventory"
        ).fetchone()
    earliest, latest, days_available = (str(r[0]) if r[0] else None, str(r[1]) if r[1] else None, int(r[2] or 0))

    return jsonify({
        'days_requested': days,
        'days_of_history_available': days_available,
        'earliest_snapshot': earliest,
        'latest_snapshot': latest,
        'since': since,
        'total': len(out),
        'per_sku': per_sku,
        'events': out,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/distribution-additions', methods=['GET'])
def api_crm_distribution_additions():
    """Stores that ADDED our tracked SKUs to distribution in the last N days.

    Query params:
      days (default 60)
      sku (optional) — filter to one tracked SKU
      brand (optional) — filter to brand
    """
    days = int(request.args.get('days', 60))
    sku_filter = request.args.get('sku', '').strip()
    brand_filter = request.args.get('brand', '').strip().lower()
    since = (_toronto_today() - timedelta(days=days)).isoformat()

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = [f"c.change_type IN ('NEW_LISTING','RELISTED')",
             f"c.change_date >= {ph}"]
    params = [since]
    if sku_filter:
        where.append(f"c.sku = {ph}")
        params.append(sku_filter.zfill(7))

    q = f"""
        SELECT c.sku, c.store_number, c.change_date, c.old_status, c.new_status, c.change_type,
               s.account, s.city, s.postal, s.rep, s.priority,
               t.name AS territory_name, COALESCE(t.color, '#888') AS territory_color,
               -- current on_hand from latest snapshot
               (SELECT i2.on_hand FROM sod_inventory i2
                  WHERE i2.sku = c.sku AND i2.store_number = c.store_number
                  ORDER BY i2.snapshot_date DESC LIMIT 1) AS current_on_hand,
               (SELECT i2.status FROM sod_inventory i2
                  WHERE i2.sku = c.sku AND i2.store_number = c.store_number
                  ORDER BY i2.snapshot_date DESC LIMIT 1) AS current_status
        FROM sod_store_sku_changes c
        LEFT JOIN stores s ON s.store_number = c.store_number
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE {' AND '.join(where)}
        ORDER BY c.change_date DESC, c.sku, c.store_number
        LIMIT 1000
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()

    out = []
    for r in rows:
        sku = r[0]
        brand, pname = SOD_TRACKED_SKUS.get(sku, ('', ''))
        if brand_filter and brand_filter not in brand.lower():
            continue
        out.append({
            'sku': sku,
            'brand': brand,
            'product_name': pname,
            'store_number': r[1],
            'change_date': str(r[2]),
            'old_status': r[3],
            'new_status': r[4],
            'change_type': r[5],
            'account': r[6],
            'city': r[7],
            'postal': r[8],
            'rep': r[9],
            'priority': r[10],
            'territory_name': r[11] or 'Unassigned',
            'territory_color': r[12],
            'current_on_hand': r[13] or 0,
            'current_status': r[14],
        })
    # Per-SKU summary
    by_sku: dict = {}
    for o in out:
        k = o['sku']
        by_sku.setdefault(k, {
            'sku': k,
            'brand': o['brand'],
            'product_name': o['product_name'],
            'count': 0,
            'still_listed': 0,
            'lost_again': 0,
        })
        by_sku[k]['count'] += 1
        if o['current_status'] == 'L':
            by_sku[k]['still_listed'] += 1
        else:
            by_sku[k]['lost_again'] += 1
    # Available data window
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("SELECT MIN(snapshot_date), MAX(snapshot_date), COUNT(DISTINCT snapshot_date) FROM sod_inventory")
        rr = cur.fetchone()
        cur.close()
    else:
        rr = db.execute(
            "SELECT MIN(snapshot_date), MAX(snapshot_date), COUNT(DISTINCT snapshot_date) FROM sod_inventory"
        ).fetchone()
    earliest = str(rr[0]) if rr[0] else None
    latest = str(rr[1]) if rr[1] else None
    days_available = int(rr[2] or 0)

    return jsonify({
        'days_requested': days,
        'days_of_history_available': days_available,
        'earliest_snapshot': earliest,
        'latest_snapshot': latest,
        'since': since,
        'total': len(out),
        'per_sku': list(by_sku.values()),
        'additions': out,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/nb-tracker', methods=['GET'])
@cached_response(ttl_seconds=120, key_args=())
def api_crm_nb_tracker():
    """Dedicated NB Distillers tracker — premium client view.

    NB Distillers is the paying client. This endpoint is shaped specifically for
    their executive view — Red Admiral Vodka (SKU 20187) + Chak De Canadian
    Whisky (SKU 22246) combined, with everything they care about in one payload.

    Returns:
      - per_sku: rollup with listed/delisting/fully_delisted/on_hand
      - velocity: week-rate per SKU, days-to-OOS for stores at risk
      - top_stores: stores carrying NB products by on-hand
      - additions_60d: every store that added an NB SKU in last 60 days
      - delistings_60d: every store where an NB SKU got delisted
      - oos_risk: stores listed but on_hand <= 2
      - tasting_followups: stores where tasting happened, NB SKU not currently listed
      - lcbo_live_discoveries: stores where lcbo.com shows NB live but SOD shows blank/F
      - territory_coverage: NB store count per territory
      - 30-day trend series for the dashboard chart
    """
    NB_SKUS = [s for s, (b, _) in SOD_TRACKED_SKUS.items() if b == 'NB Distillers']
    if not NB_SKUS:
        return jsonify({'error': 'No NB Distillers SKUs configured'}), 500

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    phs = ','.join([ph] * len(NB_SKUS))
    today_d = _toronto_today()
    since60 = (today_d - timedelta(days=60)).isoformat()

    # 1) Per-SKU rollup
    per_sku = []
    for sku in NB_SKUS:
        brand, pname = SOD_TRACKED_SKUS[sku]
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s", (sku,))
            latest = cur.fetchone()[0]
            cur.close()
        else:
            latest = db.execute(
                "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = ?", (sku,)
            ).fetchone()[0]
        if not latest:
            per_sku.append({
                'sku': sku, 'brand': brand, 'product_name': pname,
                'snapshot_date': None,
                'listed': 0, 'delisting': 0, 'fully_delisted': 0,
                'total_on_hand': 0, 'avg_on_hand': 0,
            })
            continue
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='D' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='F' THEN 1 ELSE 0 END), "
                "COALESCE(SUM(on_hand), 0), "
                "COALESCE(AVG(CASE WHEN status='L' AND on_hand > 0 THEN on_hand END), 0) "
                "FROM sod_inventory WHERE sku=%s AND snapshot_date=%s",
                (sku, latest),
            )
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(
                "SELECT SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='D' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='F' THEN 1 ELSE 0 END), "
                "COALESCE(SUM(on_hand), 0), "
                "COALESCE(AVG(CASE WHEN status='L' AND on_hand > 0 THEN on_hand END), 0) "
                "FROM sod_inventory WHERE sku=? AND snapshot_date=?",
                (sku, latest),
            ).fetchone()
        per_sku.append({
            'sku': sku, 'brand': brand, 'product_name': pname,
            'lcbo_url': f'https://www.lcbo.com/en/product-{int(sku)}',
            'snapshot_date': str(latest),
            'listed': int(r[0] or 0),
            'delisting': int(r[1] or 0),
            'fully_delisted': int(r[2] or 0),
            'total_on_hand': int(r[3] or 0),
            'avg_on_hand_at_listed': round(float(r[4] or 0), 1),
        })

    # 2) Top stores carrying NB products
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""WITH latest AS (
                    SELECT sku, MAX(snapshot_date) d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT i.store_number, i.sku, i.status, i.on_hand,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_inventory i
                JOIN latest l ON l.sku=i.sku AND l.d=i.snapshot_date
                LEFT JOIN stores s ON s.store_number = i.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE i.sku IN ({phs}) AND i.status = 'L'
                ORDER BY i.on_hand DESC LIMIT 25""",
            NB_SKUS + NB_SKUS,
        )
        top_rows = cur.fetchall()
        cur.close()
    else:
        top_rows = db.execute(
            f"""WITH latest AS (
                    SELECT sku, MAX(snapshot_date) d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT i.store_number, i.sku, i.status, i.on_hand,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_inventory i
                JOIN latest l ON l.sku=i.sku AND l.d=i.snapshot_date
                LEFT JOIN stores s ON s.store_number = i.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE i.sku IN ({phs}) AND i.status = 'L'
                ORDER BY i.on_hand DESC LIMIT 25""",
            NB_SKUS + NB_SKUS,
        ).fetchall()
    top_stores = [{
        'store_number': r[0], 'sku': r[1],
        'product_name': SOD_TRACKED_SKUS.get(r[1], ('', ''))[1],
        'status': r[2], 'on_hand': int(r[3] or 0),
        'account': r[4], 'city': r[5],
        'territory_name': r[6] or 'Unassigned',
        'territory_color': r[7],
    } for r in top_rows]

    # 3) Additions in last 60 days
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       s.account, s.city, t.name, COALESCE(t.color, '#888'),
                       (SELECT i.on_hand FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1) AS current_on_hand,
                       (SELECT i.status FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1) AS current_status
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE c.sku IN ({phs})
                  AND c.change_type IN ('NEW_LISTING', 'RELISTED')
                  AND c.change_date >= %s
                ORDER BY c.change_date DESC LIMIT 100""",
            NB_SKUS + [since60],
        )
        add_rows = cur.fetchall()
        cur.close()
    else:
        add_rows = db.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       s.account, s.city, t.name, COALESCE(t.color, '#888'),
                       (SELECT i.on_hand FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1) AS current_on_hand,
                       (SELECT i.status FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1) AS current_status
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE c.sku IN ({phs})
                  AND c.change_type IN ('NEW_LISTING', 'RELISTED')
                  AND c.change_date >= ?
                ORDER BY c.change_date DESC LIMIT 100""",
            NB_SKUS + [since60],
        ).fetchall()
    additions_60d = [{
        'sku': r[0], 'product_name': SOD_TRACKED_SKUS.get(r[0], ('', ''))[1],
        'store_number': r[1], 'change_date': str(r[2]), 'change_type': r[3],
        'account': r[4], 'city': r[5],
        'territory_name': r[6] or 'Unassigned', 'territory_color': r[7],
        'current_on_hand': int(r[8] or 0), 'current_status': r[9],
    } for r in add_rows]

    # 4) Delistings in last 60 days
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       c.old_status, c.new_status,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE c.sku IN ({phs})
                  AND c.change_type IN ('DELISTED','DROPPED','STATUS_FLIP')
                  AND c.change_date >= %s
                ORDER BY c.change_date DESC LIMIT 100""",
            NB_SKUS + [since60],
        )
        del_rows = cur.fetchall()
        cur.close()
    else:
        del_rows = db.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       c.old_status, c.new_status,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE c.sku IN ({phs})
                  AND c.change_type IN ('DELISTED','DROPPED','STATUS_FLIP')
                  AND c.change_date >= ?
                ORDER BY c.change_date DESC LIMIT 100""",
            NB_SKUS + [since60],
        ).fetchall()
    delistings_60d = [{
        'sku': r[0], 'product_name': SOD_TRACKED_SKUS.get(r[0], ('', ''))[1],
        'store_number': r[1], 'change_date': str(r[2]), 'change_type': r[3],
        'old_status': r[4], 'new_status': r[5],
        'account': r[6], 'city': r[7],
        'territory_name': r[8] or 'Unassigned', 'territory_color': r[9],
    } for r in del_rows]

    # 5) OOS risk for NB SKUs
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""WITH latest AS (
                    SELECT sku, MAX(snapshot_date) d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT i.sku, i.store_number, i.on_hand,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_inventory i
                JOIN latest l ON l.sku=i.sku AND l.d=i.snapshot_date
                LEFT JOIN stores s ON s.store_number=i.store_number
                LEFT JOIN territories t ON t.id=s.territory_id
                WHERE i.sku IN ({phs}) AND i.status='L' AND i.on_hand <= 2
                ORDER BY i.on_hand ASC LIMIT 50""",
            NB_SKUS + NB_SKUS,
        )
        oos_rows = cur.fetchall()
        cur.close()
    else:
        oos_rows = db.execute(
            f"""WITH latest AS (
                    SELECT sku, MAX(snapshot_date) d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT i.sku, i.store_number, i.on_hand,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_inventory i
                JOIN latest l ON l.sku=i.sku AND l.d=i.snapshot_date
                LEFT JOIN stores s ON s.store_number=i.store_number
                LEFT JOIN territories t ON t.id=s.territory_id
                WHERE i.sku IN ({phs}) AND i.status='L' AND i.on_hand <= 2
                ORDER BY i.on_hand ASC LIMIT 50""",
            NB_SKUS + NB_SKUS,
        ).fetchall()
    oos_risk = [{
        'sku': r[0], 'product_name': SOD_TRACKED_SKUS.get(r[0], ('', ''))[1],
        'store_number': r[1], 'on_hand': int(r[2] or 0),
        'severity': 'critical' if (r[2] or 0) == 0 else ('high' if (r[2] or 0) <= 1 else 'medium'),
        'account': r[3], 'city': r[4],
        'territory_name': r[5] or 'Unassigned', 'territory_color': r[6],
    } for r in oos_rows]

    # 6) Territory coverage (count of stores carrying any NB SKU at status='L', per territory)
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""WITH latest AS (
                    SELECT sku, MAX(snapshot_date) d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT t.code, t.name, COALESCE(t.color, '#888'),
                       COUNT(DISTINCT i.store_number) AS nb_stores,
                       COUNT(DISTINCT s.id) AS total_stores
                FROM territories t
                LEFT JOIN stores s ON s.territory_id = t.id
                LEFT JOIN sod_inventory i ON i.store_number = s.store_number AND i.status='L'
                  AND i.sku IN ({phs})
                  AND i.snapshot_date = (SELECT d FROM latest WHERE latest.sku=i.sku)
                GROUP BY t.code, t.name, t.color
                ORDER BY nb_stores DESC""",
            NB_SKUS + NB_SKUS,
        )
        terr_rows = cur.fetchall()
        cur.close()
    else:
        terr_rows = db.execute(
            f"""WITH latest AS (
                    SELECT sku, MAX(snapshot_date) d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT t.code, t.name, COALESCE(t.color, '#888'),
                       COUNT(DISTINCT i.store_number) AS nb_stores,
                       COUNT(DISTINCT s.id) AS total_stores
                FROM territories t
                LEFT JOIN stores s ON s.territory_id = t.id
                LEFT JOIN sod_inventory i ON i.store_number = s.store_number AND i.status='L'
                  AND i.sku IN ({phs})
                  AND i.snapshot_date = (SELECT d FROM latest WHERE latest.sku=i.sku)
                GROUP BY t.code, t.name, t.color
                ORDER BY nb_stores DESC""",
            NB_SKUS + NB_SKUS,
        ).fetchall()
    territory_coverage = [{
        'code': r[0], 'name': r[1], 'color': r[2],
        'nb_stores': int(r[3] or 0),
        'total_stores': int(r[4] or 0),
        'coverage_pct': round(100 * (r[3] or 0) / (r[4] or 1), 1) if r[4] else 0,
    } for r in terr_rows]

    # 7) 30-day trend series (combined NB)
    since30 = (today_d - timedelta(days=30)).isoformat()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""SELECT snapshot_date,
                       SUM(CASE WHEN status='L' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN status='D' THEN 1 ELSE 0 END),
                       COALESCE(SUM(on_hand), 0)
                FROM sod_inventory
                WHERE sku IN ({phs}) AND snapshot_date >= %s
                GROUP BY snapshot_date
                ORDER BY snapshot_date""",
            NB_SKUS + [since30],
        )
        trend_rows = cur.fetchall()
        cur.close()
    else:
        trend_rows = db.execute(
            f"""SELECT snapshot_date,
                       SUM(CASE WHEN status='L' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN status='D' THEN 1 ELSE 0 END),
                       COALESCE(SUM(on_hand), 0)
                FROM sod_inventory
                WHERE sku IN ({phs}) AND snapshot_date >= ?
                GROUP BY snapshot_date
                ORDER BY snapshot_date""",
            NB_SKUS + [since30],
        ).fetchall()
    trend = [{
        'date': str(r[0]),
        'listed': int(r[1] or 0),
        'delisting': int(r[2] or 0),
        'total_on_hand': int(r[3] or 0),
    } for r in trend_rows]

    # 8) Tasting follow-ups specifically for NB
    nb_followups = []
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""SELECT
                    aso.sku, aso.outcome,
                    a.id, a.activity_type, a.notes, a.rep,
                    COALESCE(a.visit_date, a.created_at::date) AS visit_when,
                    s.store_number, s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM activity_sku_outcomes aso
                JOIN activities a ON a.id = aso.activity_id
                LEFT JOIN stores s ON s.id = a.store_id
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE a.deleted_at IS NULL
                  AND aso.sku IN ({phs})
                  AND (LOWER(aso.outcome) IN ('tasting','sampled','samples_left','sample_drop')
                       OR LOWER(a.activity_type) IN ('tasting','sample_drop'))""",
            NB_SKUS,
        )
        f_rows = cur.fetchall()
        cur.close()
    else:
        f_rows = db.execute(
            f"""SELECT
                    aso.sku, aso.outcome,
                    a.id, a.activity_type, a.notes, a.rep,
                    COALESCE(a.visit_date, DATE(a.created_at)) AS visit_when,
                    s.store_number, s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM activity_sku_outcomes aso
                JOIN activities a ON a.id = aso.activity_id
                LEFT JOIN stores s ON s.id = a.store_id
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE a.deleted_at IS NULL
                  AND aso.sku IN ({phs})
                  AND (LOWER(aso.outcome) IN ('tasting','sampled','samples_left','sample_drop')
                       OR LOWER(a.activity_type) IN ('tasting','sample_drop'))""",
            NB_SKUS,
        ).fetchall()
    seen = set()
    for r in f_rows:
        sku = r[0]
        store_number = r[7]
        if not store_number or (store_number, sku) in seen:
            continue
        seen.add((store_number, sku))
        # Current status
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT status FROM sod_inventory WHERE sku=%s AND store_number=%s "
                "ORDER BY snapshot_date DESC LIMIT 1", (sku, store_number),
            )
            sr = cur.fetchone()
            cur.close()
        else:
            sr = db.execute(
                "SELECT status FROM sod_inventory WHERE sku=? AND store_number=? "
                "ORDER BY snapshot_date DESC LIMIT 1", (sku, store_number),
            ).fetchone()
        current_status = sr[0] if sr else None
        if current_status == 'L':
            continue
        try:
            visit_iso = r[6].isoformat() if hasattr(r[6], 'isoformat') else str(r[6])
            days_since = (datetime.now() - datetime.fromisoformat(visit_iso.split('T')[0])).days
        except Exception:
            visit_iso = str(r[6])
            days_since = None
        nb_followups.append({
            'sku': sku,
            'product_name': SOD_TRACKED_SKUS.get(sku, ('', ''))[1],
            'store_number': store_number,
            'tasting_date': visit_iso,
            'days_since_tasting': days_since,
            'tasting_outcome': r[1],
            'rep': r[5],
            'account': r[8], 'city': r[9],
            'territory_name': r[10] or 'Unassigned',
            'territory_color': r[11],
            'current_sod_status': current_status,
        })

    # Aggregated brand totals
    totals = {
        'total_skus': len(NB_SKUS),
        'total_listed_stores': sum(p['listed'] for p in per_sku),
        'total_delisting_stores': sum(p['delisting'] for p in per_sku),
        'total_on_hand_units': sum(p['total_on_hand'] for p in per_sku),
        'additions_60d': len(additions_60d),
        'delistings_60d': len(delistings_60d),
        'oos_risk_count': len(oos_risk),
        'tasting_followups_count': len(nb_followups),
    }

    return jsonify({
        'brand': 'NB Distillers',
        'tagline': 'Premium Anu Spirits client tracker',
        'skus': NB_SKUS,
        'per_sku': per_sku,
        'totals': totals,
        'top_stores': top_stores,
        'additions_60d': additions_60d,
        'delistings_60d': delistings_60d,
        'oos_risk': oos_risk,
        'tasting_followups': nb_followups,
        'territory_coverage': territory_coverage,
        'trend_30d': trend,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/brand/<brand>', methods=['GET'])
def api_crm_brand(brand):
    """Combined distribution health for one brand (e.g. 'NB Distillers' covers
    Red Admiral + Chak De together).

    Returns per-SKU rollup + combined-brand metrics + recent additions/losses
    (last 60 days) + per-store matrix (which stores carry which of our SKUs).
    """
    brand_clean = brand.replace('-', ' ').strip().lower()
    # Match SKUs by brand
    matched = [(sku, b, n) for sku, (b, n) in SOD_TRACKED_SKUS.items()
               if b.lower() == brand_clean or brand_clean in b.lower()]
    if not matched:
        return jsonify({'error': f'No tracked SKUs found for brand {brand}'}), 404
    skus = [s for s, _, _ in matched]
    brand_canonical = matched[0][1]

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    phs = ','.join([ph] * len(skus))

    # Per-SKU rollup at latest snapshot per-SKU
    per_sku_summary = []
    for sku, b, n in matched:
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s",
                (sku,),
            )
            latest = cur.fetchone()[0]
            cur.close()
        else:
            latest = db.execute(
                "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = ?",
                (sku,),
            ).fetchone()[0]
        if not latest:
            per_sku_summary.append({
                'sku': sku, 'brand': b, 'product_name': n,
                'snapshot_date': None, 'listed': 0, 'delisting': 0,
                'fully_delisted': 0, 'total_on_hand': 0,
            })
            continue
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='D' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='F' THEN 1 ELSE 0 END), "
                "COALESCE(SUM(on_hand), 0) "
                "FROM sod_inventory WHERE sku = %s AND snapshot_date = %s",
                (sku, latest),
            )
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(
                "SELECT SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='D' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='F' THEN 1 ELSE 0 END), "
                "COALESCE(SUM(on_hand), 0) "
                "FROM sod_inventory WHERE sku = ? AND snapshot_date = ?",
                (sku, latest),
            ).fetchone()
        per_sku_summary.append({
            'sku': sku, 'brand': b, 'product_name': n,
            'snapshot_date': str(latest),
            'listed': int(r[0] or 0),
            'delisting': int(r[1] or 0),
            'fully_delisted': int(r[2] or 0),
            'total_on_hand': int(r[3] or 0),
        })

    # Per-store matrix: which of our SKUs does each store carry, at status='L'?
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""WITH latest_per_sku AS (
                    SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT i.store_number, i.sku, i.status, i.on_hand,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_inventory i
                JOIN latest_per_sku l ON l.sku = i.sku AND l.d = i.snapshot_date
                LEFT JOIN stores s ON s.store_number = i.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE i.sku IN ({phs})""",
            skus + skus,
        )
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(
            f"""WITH latest_per_sku AS (
                    SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
                    WHERE sku IN ({phs}) GROUP BY sku
                )
                SELECT i.store_number, i.sku, i.status, i.on_hand,
                       s.account, s.city, t.name, COALESCE(t.color, '#888')
                FROM sod_inventory i
                JOIN latest_per_sku l ON l.sku = i.sku AND l.d = i.snapshot_date
                LEFT JOIN stores s ON s.store_number = i.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE i.sku IN ({phs})""",
            skus + skus,
        ).fetchall()

    matrix: dict = {}
    for r in rows:
        store, sku, st, oh, account, city, terr_name, terr_color = r
        if store not in matrix:
            matrix[store] = {
                'store_number': store, 'account': account, 'city': city,
                'territory_name': terr_name, 'territory_color': terr_color,
                'skus': {},
            }
        matrix[store]['skus'][sku] = {'status': st, 'on_hand': oh or 0}

    # Compute brand-level metrics from matrix
    stores_with_any_listed = sum(
        1 for s in matrix.values()
        if any(v['status'] == 'L' for v in s['skus'].values())
    )
    stores_with_all_listed = sum(
        1 for s in matrix.values()
        if all(s['skus'].get(sku, {}).get('status') == 'L' for sku in skus)
    )
    stores_with_any_delisting = sum(
        1 for s in matrix.values()
        if any(v['status'] in ('D', 'F') for v in s['skus'].values())
    )

    # Recent additions/losses (last 60 days) for these SKUs
    since = (_toronto_today() - timedelta(days=60)).isoformat()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""SELECT change_type, COUNT(*) FROM sod_store_sku_changes
                WHERE sku IN ({phs}) AND change_date >= {ph}
                GROUP BY change_type""",
            skus + [since],
        )
        type_counts = {r[0]: int(r[1]) for r in cur.fetchall()}
        cur.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       c.old_status, c.new_status, s.account, s.city
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                WHERE c.sku IN ({phs}) AND c.change_date >= {ph}
                ORDER BY c.change_date DESC LIMIT 50""",
            skus + [since],
        )
        recent_changes = cur.fetchall()
        cur.close()
    else:
        type_counts = {r[0]: int(r[1]) for r in db.execute(
            f"""SELECT change_type, COUNT(*) FROM sod_store_sku_changes
                WHERE sku IN ({phs}) AND change_date >= ?
                GROUP BY change_type""",
            skus + [since],
        ).fetchall()}
        recent_changes = db.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       c.old_status, c.new_status, s.account, s.city
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                WHERE c.sku IN ({phs}) AND c.change_date >= ?
                ORDER BY c.change_date DESC LIMIT 50""",
            skus + [since],
        ).fetchall()

    return jsonify({
        'brand': brand_canonical,
        'skus': skus,
        'per_sku': per_sku_summary,
        'totals': {
            'total_stores_with_any_listed': stores_with_any_listed,
            'total_stores_with_all_listed': stores_with_all_listed,
            'total_stores_with_any_delisting': stores_with_any_delisting,
            'total_stores_in_matrix': len(matrix),
        },
        'matrix': list(matrix.values()),
        'recent_changes_60d': {
            'counts': type_counts,
            'recent': [{
                'sku': r[0], 'store_number': r[1], 'change_date': str(r[2]),
                'change_type': r[3], 'old_status': r[4], 'new_status': r[5],
                'account': r[6], 'city': r[7],
            } for r in recent_changes],
        },
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/brands', methods=['GET'])
@cached_response(ttl_seconds=300, key_args=())
def api_crm_brands_list():
    """List all brands we track + KPIs per brand."""
    brand_skus: dict = {}
    for sku, (b, n) in SOD_TRACKED_SKUS.items():
        brand_skus.setdefault(b, []).append({'sku': sku, 'product_name': n})
    out = []
    for brand, skus in brand_skus.items():
        sku_list = [s['sku'] for s in skus]
        # Per-brand: total listed stores at latest per-SKU snapshot
        db = get_db()
        ph = '%s' if USE_POSTGRES else '?'
        phs = ','.join([ph] * len(sku_list))
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"""WITH latest_per_sku AS (
                        SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
                        WHERE sku IN ({phs}) GROUP BY sku
                    )
                    SELECT
                        SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN i.status='D' THEN 1 ELSE 0 END),
                        COALESCE(SUM(i.on_hand), 0),
                        COUNT(DISTINCT i.store_number)
                    FROM sod_inventory i
                    JOIN latest_per_sku l ON l.sku = i.sku AND l.d = i.snapshot_date""",
                sku_list,
            )
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(
                f"""WITH latest_per_sku AS (
                        SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
                        WHERE sku IN ({phs}) GROUP BY sku
                    )
                    SELECT
                        SUM(CASE WHEN i.status='L' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN i.status='D' THEN 1 ELSE 0 END),
                        COALESCE(SUM(i.on_hand), 0),
                        COUNT(DISTINCT i.store_number)
                    FROM sod_inventory i
                    JOIN latest_per_sku l ON l.sku = i.sku AND l.d = i.snapshot_date""",
                sku_list,
            ).fetchone()
        # Recent additions in last 60d
        since = (_toronto_today() - timedelta(days=60)).isoformat()
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"""SELECT COUNT(*) FROM sod_store_sku_changes
                    WHERE sku IN ({phs})
                    AND change_type IN ('NEW_LISTING','RELISTED')
                    AND change_date >= %s""",
                sku_list + [since],
            )
            additions = int(cur.fetchone()[0] or 0)
            cur.close()
        else:
            additions = int(db.execute(
                f"""SELECT COUNT(*) FROM sod_store_sku_changes
                    WHERE sku IN ({phs})
                    AND change_type IN ('NEW_LISTING','RELISTED')
                    AND change_date >= ?""",
                sku_list + [since],
            ).fetchone()[0] or 0)
        out.append({
            'brand': brand,
            'slug': brand.lower().replace(' ', '-'),
            'sku_count': len(sku_list),
            'skus': skus,
            'total_listed': int(r[0] or 0),
            'total_delisting': int(r[1] or 0),
            'total_on_hand': int(r[2] or 0),
            'total_stores': int(r[3] or 0),
            'additions_60d': additions,
        })
    return jsonify({'brands': out})


@app.route('/api/crm/portfolio-trend', methods=['GET'])
@cached_response(ttl_seconds=300, key_args=('days',))
def api_crm_portfolio_trend():
    """One time-series across ALL tracked SKUs, per snapshot_date.

    Powers the dashboard hero chart. Returns one row per date with:
      total_listed (sum of stores carrying any tracked SKU at status='L'),
      total_delisting, total_fully_delisted, total_on_hand,
      tracked_skus_with_data (how many of our 8 SKUs had data that day).
    """
    days = int(request.args.get('days', 30))
    since = (_toronto_today() - timedelta(days=days)).isoformat()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    tracked = list(SOD_TRACKED_SKUS.keys())
    if not tracked:
        return jsonify({'series': []})
    phs = ','.join([ph] * len(tracked))
    q = f"""
        SELECT snapshot_date,
               SUM(CASE WHEN status='L' THEN 1 ELSE 0 END) AS listed,
               SUM(CASE WHEN status='D' THEN 1 ELSE 0 END) AS delisting,
               SUM(CASE WHEN status='F' THEN 1 ELSE 0 END) AS fully_delisted,
               COALESCE(SUM(on_hand), 0) AS total_on_hand,
               COUNT(DISTINCT sku) AS skus_with_data
        FROM sod_inventory
        WHERE sku IN ({phs}) AND snapshot_date >= {ph}
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, tracked + [since])
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, tracked + [since]).fetchall()
    return jsonify({
        'days': days,
        'since': since,
        'series': [{
            'date': str(r[0]),
            'listed': int(r[1] or 0),
            'delisting': int(r[2] or 0),
            'fully_delisted': int(r[3] or 0),
            'total_on_hand': int(r[4] or 0),
            'skus_with_data': int(r[5] or 0),
        } for r in rows],
        'freshness': _sod_freshness(),
    })


@app.route('/api/sod/ingest-calendar', methods=['GET'])
def api_sod_ingest_calendar():
    """Last N days: which days have a SOD snapshot ingested? For the SOD page strip.

    Each day: { date, snapshot_date_present, latest_run_at, success_count, fail_count, sources }
    """
    days = int(request.args.get('days', 14))
    db = get_db()
    today = _toronto_today()
    out = []
    for i in range(days):
        d = today - timedelta(days=i)
        d_str = d.isoformat()
        # Did we ingest a snapshot for this date in either source?
        ph = '%s' if USE_POSTGRES else '?'
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM sod_inventory WHERE snapshot_date = %s LIMIT 1",
                (d_str,),
            )
            has_data = cur.fetchone()[0] > 0
            cur.execute(
                "SELECT MAX(run_at), COUNT(CASE WHEN status='success' THEN 1 END), "
                "COUNT(CASE WHEN status='failed' THEN 1 END), "
                "STRING_AGG(DISTINCT source, ',') "
                "FROM sod_sync_runs WHERE DATE(run_at) = %s",
                (d_str,),
            )
            r = cur.fetchone()
            cur.close()
            latest_run, success, fails, sources = (r[0], r[1] or 0, r[2] or 0, r[3] or '')
        else:
            r0 = db.execute(
                "SELECT COUNT(*) FROM sod_inventory WHERE snapshot_date = ? LIMIT 1",
                (d_str,),
            ).fetchone()
            has_data = r0[0] > 0
            r = db.execute(
                "SELECT MAX(run_at), COUNT(CASE WHEN status='success' THEN 1 END), "
                "COUNT(CASE WHEN status='failed' THEN 1 END), "
                "GROUP_CONCAT(DISTINCT source) "
                "FROM sod_sync_runs WHERE DATE(run_at) = ?",
                (d_str,),
            ).fetchone()
            latest_run, success, fails, sources = (r[0], r[1] or 0, r[2] or 0, r[3] or '')
        out.append({
            'date': d_str,
            'weekday': d.strftime('%a'),
            'has_snapshot': has_data,
            'latest_run_at': str(latest_run) if latest_run else None,
            'success_runs': success,
            'failed_runs': fails,
            'sources': sources or '',
            'is_today': i == 0,
        })
    return jsonify({'days': days, 'calendar': out})


@app.route('/api/crm/wow-deltas', methods=['GET'])
def api_crm_wow_deltas():
    """Per-tracked-SKU comparison: latest vs 7d ago vs 30d ago vs 365d ago.

    Fixed: closest_snapshot now scoped PER-SKU — the latest snapshot <= target
    that actually CONTAINS the SKU. Previously used global max snapshot, which
    picked daily_b agent-only snapshots where Anu SKUs don't exist → false
    -100% deltas.
    """
    today = _toronto_today()
    tracked = list(SOD_TRACKED_SKUS.keys())
    if not tracked:
        return jsonify({})
    target_days = {'today': 0, 'wow': 7, 'mom': 30, 'yoy': 365}
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    def closest_snapshot_for_sku(sku, target_date_str):
        q = (f"SELECT MAX(snapshot_date) FROM sod_inventory "
             f"WHERE sku = {ph} AND snapshot_date <= {ph}")
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(q, (sku, target_date_str))
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(q, (sku, target_date_str)).fetchone()
        return str(r[0]) if r and r[0] else None

    def metrics_for(sku, snap_date):
        if not snap_date:
            return {'listed': 0, 'on_hand': 0}
        q = (f"SELECT SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
             f"COALESCE(SUM(on_hand), 0) "
             f"FROM sod_inventory WHERE sku = {ph} AND snapshot_date = {ph}")
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(q, (sku, snap_date))
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(q, (sku, snap_date)).fetchone()
        return {'listed': int(r[0] or 0), 'on_hand': int(r[1] or 0)}

    def delta(now, then):
        if then == 0:
            return {'abs': now, 'pct': None}
        return {'abs': now - then, 'pct': round(100 * (now - then) / then, 1)}

    snapshot_tally = {}
    out = []
    for sku in tracked:
        brand, pname = SOD_TRACKED_SKUS[sku]
        snaps = {
            k: closest_snapshot_for_sku(sku, (today - timedelta(days=d)).isoformat())
            for k, d in target_days.items()
        }
        mets = {k: metrics_for(sku, snaps[k]) for k in target_days}
        if snaps['today']:
            snapshot_tally[snaps['today']] = snapshot_tally.get(snaps['today'], 0) + 1
        latest = mets['today']
        out.append({
            'sku': sku, 'brand': brand, 'product_name': pname,
            'now': latest, 'now_snapshot': snaps['today'],
            'wow': {
                'listed_delta': delta(latest['listed'], mets['wow']['listed']),
                'on_hand_delta': delta(latest['on_hand'], mets['wow']['on_hand']),
                'baseline_snapshot': snaps['wow'],
            },
            'mom': {
                'listed_delta': delta(latest['listed'], mets['mom']['listed']),
                'on_hand_delta': delta(latest['on_hand'], mets['mom']['on_hand']),
                'baseline_snapshot': snaps['mom'],
            },
            'yoy': {
                'listed_delta': delta(latest['listed'], mets['yoy']['listed']),
                'on_hand_delta': delta(latest['on_hand'], mets['yoy']['on_hand']),
                'baseline_snapshot': snaps['yoy'],
            },
        })
    top_today = (
        max(snapshot_tally.items(), key=lambda x: x[1])[0] if snapshot_tally else None
    )
    return jsonify({
        'snapshots': {
            'today': top_today,
            'wow': (today - timedelta(days=7)).isoformat(),
            'mom': (today - timedelta(days=30)).isoformat(),
            'yoy': (today - timedelta(days=365)).isoformat(),
        },
        'tracked': out,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/route-planner', methods=['GET'])
def api_crm_route_planner():
    """Build an optimized rep route for a day.

    Filter: stores in a city/district with AT MOST `max_skus_listed` of our
    tracked SKUs currently listed (default 1) — i.e. priority targets that
    have zero or one of our products and need attention.

    Optimization: nearest-neighbor TSP starting from start_lat/start_lng (rep's
    current location) or the first store if no start coords.

    Query params:
      city           — exact city match (e.g. 'Toronto')
      district       — territory name match (e.g. 'GTA West') — alternative to city
      max_skus_listed — stores with ≤ this many of our SKUs at status='L' (default 1)
      brand          — 'NB Distillers' or 'Anu Import' to scope by brand
      max_stops      — cap (default 10)
      start_lat, start_lng — optimize from this location
      include_no_lcbo — include stores not yet in our CRM (default false)
    """
    city = request.args.get('city', '').strip()
    district = request.args.get('district', '').strip()
    max_skus_listed = int(request.args.get('max_skus_listed', 1))
    brand_filter = request.args.get('brand', '').strip()
    max_stops = int(request.args.get('max_stops', 10))
    try:
        start_lat = float(request.args.get('start_lat', 0))
        start_lng = float(request.args.get('start_lng', 0))
    except ValueError:
        start_lat = start_lng = 0.0

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # Determine which SKUs count toward "listed at this store"
    if brand_filter.lower().startswith('anu'):
        scoped_skus = anu_import_skus()
    elif brand_filter == 'NB Distillers' or brand_filter.lower() == 'nb':
        scoped_skus = primary_skus()
    else:
        scoped_skus = list(SOD_TRACKED_SKUS.keys())
    if not scoped_skus:
        return jsonify({'route': [], 'totals': {}})
    sku_phs = ','.join([ph] * len(scoped_skus))

    # Pull stores in the city/district with valid coords
    where = ['s.lat <> 0', 's.lng <> 0']
    params = []
    if city:
        where.append(f'LOWER(s.city) = LOWER({ph})')
        params.append(city)
    elif district:
        where.append(f'LOWER(t.name) LIKE LOWER({ph})')
        params.append(f'%{district}%')
    sql_where = ' AND '.join(where)

    # For each candidate store, count how many of our SKUs are listed at status='L'
    # at the latest snapshot per SKU.
    q = f"""
        WITH latest_per_sku AS (
            SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
            WHERE sku IN ({sku_phs}) GROUP BY sku
        ),
        store_listed AS (
            SELECT i.store_number, COUNT(DISTINCT i.sku) AS listed_count
            FROM sod_inventory i
            JOIN latest_per_sku l ON l.sku = i.sku AND l.d = i.snapshot_date
            WHERE i.sku IN ({sku_phs}) AND i.status = 'L'
            GROUP BY i.store_number
        )
        SELECT s.id, s.store_number, s.account, s.address, s.city, s.postal,
               s.priority, s.rep, s.lat, s.lng, s.manager_name, s.manager_phone,
               COALESCE(t.id, 0), COALESCE(t.name, ''), COALESCE(t.color, '#888'),
               COALESCE(sl.listed_count, 0) AS listed_count
        FROM stores s
        LEFT JOIN territories t ON t.id = s.territory_id
        LEFT JOIN store_listed sl ON sl.store_number = s.store_number
        WHERE {sql_where}
        AND COALESCE(sl.listed_count, 0) <= {ph}
    """
    final_params = scoped_skus + scoped_skus + params + [max_skus_listed]
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, final_params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, final_params).fetchall()

    candidates = [{
        'store_id': r[0], 'store_number': r[1], 'account': r[2],
        'address': r[3], 'city': r[4], 'postal': r[5], 'priority': r[6],
        'rep': r[7] or '', 'lat': float(r[8] or 0), 'lng': float(r[9] or 0),
        'manager_name': r[10] or '', 'manager_phone': r[11] or '',
        'territory_id': r[12] or None, 'territory_name': r[13],
        'territory_color': r[14], 'skus_listed': int(r[15] or 0),
    } for r in rows if r[8] and r[9]]

    if not candidates:
        return jsonify({
            'route': [], 'total_stops': 0, 'total_distance_km': 0,
            'city': city, 'district': district, 'max_skus_listed': max_skus_listed,
        })

    # Score: prefer 0-listed > 1-listed > priority='High' > recently-not-visited
    candidates.sort(key=lambda c: (
        c['skus_listed'],  # zero first
        0 if (c['priority'] or '').lower() in ('top', 'high') else 1,
        c['city'],
    ))

    # Nearest-neighbor TSP
    if start_lat and start_lng:
        seed = {'lat': start_lat, 'lng': start_lng}
    else:
        seed = candidates[0]
    route = []
    pool = list(candidates)
    while pool and len(route) < max_stops:
        nxt = min(pool, key=lambda c: haversine(seed['lat'], seed['lng'], c['lat'], c['lng']))
        nxt['leg_distance_km'] = round(
            haversine(seed['lat'], seed['lng'], nxt['lat'], nxt['lng']), 2,
        )
        route.append(nxt)
        pool.remove(nxt)
        seed = nxt

    total_dist = round(sum(s['leg_distance_km'] for s in route), 1)
    return jsonify({
        'city': city or None,
        'district': district or None,
        'brand_filter': brand_filter or 'all',
        'max_skus_listed': max_skus_listed,
        'total_stops': len(route),
        'total_distance_km': total_dist,
        'total_candidates': len(candidates),
        'route': route,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/anu-import', methods=['GET'])
@cached_response(ttl_seconds=120, key_args=())
def api_crm_anu_import():
    """Anu Import portfolio tracker (Goenchi + Fratelli) — secondary to NB.

    Same shape as /nb-tracker but scoped to Anu Import SKUs.
    """
    skus = anu_import_skus()
    if not skus:
        return jsonify({'brand': 'Anu Import', 'skus': [], 'totals': {}})

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    phs = ','.join([ph] * len(skus))
    today_d = _toronto_today()
    since60 = (today_d - timedelta(days=60)).isoformat()

    per_sku = []
    for sku in skus:
        brand, pname = SOD_TRACKED_SKUS[sku]
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s", (sku,))
            latest = cur.fetchone()[0]
            cur.close()
        else:
            latest = db.execute(
                "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = ?", (sku,)
            ).fetchone()[0]
        if not latest:
            per_sku.append({'sku': sku, 'brand': brand, 'product_name': pname,
                            'snapshot_date': None, 'listed': 0, 'delisting': 0,
                            'fully_delisted': 0, 'total_on_hand': 0,
                            'lcbo_url': f'https://www.lcbo.com/en/product-{int(sku)}'})
            continue
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='D' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='F' THEN 1 ELSE 0 END), "
                "COALESCE(SUM(on_hand), 0) FROM sod_inventory "
                "WHERE sku=%s AND snapshot_date=%s",
                (sku, latest),
            )
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(
                "SELECT SUM(CASE WHEN status='L' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='D' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='F' THEN 1 ELSE 0 END), "
                "COALESCE(SUM(on_hand), 0) FROM sod_inventory "
                "WHERE sku=? AND snapshot_date=?",
                (sku, latest),
            ).fetchone()
        per_sku.append({
            'sku': sku, 'brand': brand, 'product_name': pname,
            'lcbo_url': f'https://www.lcbo.com/en/product-{int(sku)}',
            'snapshot_date': str(latest),
            'listed': int(r[0] or 0), 'delisting': int(r[1] or 0),
            'fully_delisted': int(r[2] or 0), 'total_on_hand': int(r[3] or 0),
        })

    # Recent additions
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       s.account, s.city, t.name, COALESCE(t.color, '#888'),
                       (SELECT i.on_hand FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1) AS current_on_hand,
                       (SELECT i.status FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1) AS current_status
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE c.sku IN ({phs})
                  AND c.change_type IN ('NEW_LISTING','RELISTED')
                  AND c.change_date >= %s
                ORDER BY c.change_date DESC LIMIT 100""",
            skus + [since60],
        )
        add_rows = cur.fetchall()
        cur.close()
    else:
        add_rows = db.execute(
            f"""SELECT c.sku, c.store_number, c.change_date, c.change_type,
                       s.account, s.city, t.name, COALESCE(t.color, '#888'),
                       (SELECT i.on_hand FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1),
                       (SELECT i.status FROM sod_inventory i
                          WHERE i.sku=c.sku AND i.store_number=c.store_number
                          ORDER BY i.snapshot_date DESC LIMIT 1)
                FROM sod_store_sku_changes c
                LEFT JOIN stores s ON s.store_number = c.store_number
                LEFT JOIN territories t ON t.id = s.territory_id
                WHERE c.sku IN ({phs})
                  AND c.change_type IN ('NEW_LISTING','RELISTED')
                  AND c.change_date >= ?
                ORDER BY c.change_date DESC LIMIT 100""",
            skus + [since60],
        ).fetchall()
    additions_60d = [{
        'sku': r[0], 'product_name': SOD_TRACKED_SKUS.get(r[0], ('', ''))[1],
        'store_number': r[1], 'change_date': str(r[2]), 'change_type': r[3],
        'account': r[4], 'city': r[5],
        'territory_name': r[6] or 'Unassigned', 'territory_color': r[7],
        'current_on_hand': int(r[8] or 0), 'current_status': r[9],
    } for r in add_rows]

    totals = {
        'total_skus': len(skus),
        'total_listed_stores': sum(p['listed'] for p in per_sku),
        'total_delisting_stores': sum(p['delisting'] for p in per_sku),
        'total_on_hand_units': sum(p['total_on_hand'] for p in per_sku),
        'additions_60d': len(additions_60d),
    }

    return jsonify({
        'brand': 'Anu Import',
        'tagline': 'Goenchi + Fratelli — secondary import portfolio',
        'skus': skus,
        'per_sku': per_sku,
        'totals': totals,
        'additions_60d': additions_60d,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/cities', methods=['GET'])
@cached_response(ttl_seconds=3600, key_args=())
def api_crm_cities():
    """List all distinct cities with store counts (for route-planner picker)."""
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "SELECT city, COUNT(*) FROM stores WHERE city IS NOT NULL "
            "AND TRIM(city) <> '' AND lat <> 0 AND lng <> 0 "
            "GROUP BY city ORDER BY COUNT(*) DESC, city ASC",
        )
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(
            "SELECT city, COUNT(*) FROM stores WHERE city IS NOT NULL "
            "AND TRIM(city) <> '' AND lat <> 0 AND lng <> 0 "
            "GROUP BY city ORDER BY COUNT(*) DESC, city ASC",
        ).fetchall()
    return jsonify([{'city': r[0], 'store_count': int(r[1])} for r in rows])


@app.route('/api/crm/store-search', methods=['GET'])
def api_crm_store_search():
    """Typeahead lookup: match by store_number, account name, address, OR city.

    Each match includes the LAST conversation/activity (rep + date + notes)
    so the rep sees context immediately. Powers the rep "Quick Log" search bar.
    """
    q = (request.args.get('q') or '').strip()
    if len(q) < 2:
        return jsonify({'matches': [], 'query': q})
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    like = f'%{q}%'
    sql = f"""
        SELECT s.id, s.store_number, COALESCE(s.account, ''), COALESCE(s.address, ''),
               COALESCE(s.city, ''), COALESCE(s.postal, ''),
               COALESCE(s.phone, ''), COALESCE(s.manager_phone, ''),
               COALESCE(s.manager_name, ''), COALESCE(s.rep, ''),
               COALESCE(s.lat, 0), COALESCE(s.lng, 0),
               la.last_activity_at, la.last_activity_type,
               la.last_activity_rep, la.last_activity_notes
        FROM stores s
        LEFT JOIN LATERAL (
            SELECT a.created_at AS last_activity_at,
                   a.activity_type AS last_activity_type,
                   COALESCE(a.rep, r.name, '') AS last_activity_rep,
                   COALESCE(a.notes, '') AS last_activity_notes
            FROM activities a
            LEFT JOIN reps r ON r.id = a.rep_id
            WHERE a.store_id = s.id AND a.deleted_at IS NULL
            ORDER BY a.created_at DESC
            LIMIT 1
        ) la ON TRUE
        WHERE (
              CAST(s.store_number AS TEXT) LIKE {ph}
           OR LOWER(s.account) LIKE LOWER({ph})
           OR LOWER(s.address) LIKE LOWER({ph})
           OR LOWER(s.city) LIKE LOWER({ph})
           OR LOWER(s.postal) LIKE LOWER({ph})
        )
        ORDER BY
          CASE WHEN CAST(s.store_number AS TEXT) = {ph} THEN 0
               WHEN CAST(s.store_number AS TEXT) LIKE {ph} THEN 1
               ELSE 2 END,
          la.last_activity_at DESC NULLS LAST,
          s.store_number
        LIMIT 10
    """
    params = (like, like, like, like, like, q, f'{q}%')
    rows = []
    try:
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
        else:
            # SQLite doesn't support LATERAL JOIN — fall back to a simpler subquery
            sql_sqlite = f"""
                SELECT s.id, s.store_number, COALESCE(s.account, ''), COALESCE(s.address, ''),
                       COALESCE(s.city, ''), COALESCE(s.postal, ''),
                       COALESCE(s.phone, ''), COALESCE(s.manager_phone, ''),
                       COALESCE(s.manager_name, ''), COALESCE(s.rep, ''),
                       COALESCE(s.lat, 0), COALESCE(s.lng, 0),
                       (SELECT created_at FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1),
                       (SELECT activity_type FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1),
                       (SELECT COALESCE(rep,'') FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1),
                       (SELECT COALESCE(notes,'') FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1)
                FROM stores s
                WHERE (
                      CAST(s.store_number AS TEXT) LIKE ?
                   OR LOWER(s.account) LIKE LOWER(?)
                   OR LOWER(s.address) LIKE LOWER(?)
                   OR LOWER(s.city) LIKE LOWER(?)
                   OR LOWER(s.postal) LIKE LOWER(?)
                )
                ORDER BY
                  CASE WHEN CAST(s.store_number AS TEXT) = ? THEN 0
                       WHEN CAST(s.store_number AS TEXT) LIKE ? THEN 1
                       ELSE 2 END,
                  s.store_number
                LIMIT 10
            """
            rows = db.execute(sql_sqlite, params).fetchall()
    except Exception as e:
        # Schema-evolution safety: if the activities table or its columns aren't
        # quite right yet (bootstrap), gracefully fall back to base store match.
        print(f"[store-search] LATERAL join failed, falling back: {e}")
        sql_fallback = f"""
            SELECT s.id, s.store_number, COALESCE(s.account, ''), COALESCE(s.address, ''),
                   COALESCE(s.city, ''), COALESCE(s.postal, ''),
                   COALESCE(s.phone, ''), COALESCE(s.manager_phone, ''),
                   COALESCE(s.manager_name, ''), COALESCE(s.rep, ''),
                   COALESCE(s.lat, 0), COALESCE(s.lng, 0),
                   NULL, NULL, '', ''
            FROM stores s
            WHERE (
                  CAST(s.store_number AS TEXT) LIKE {ph}
               OR LOWER(s.account) LIKE LOWER({ph})
               OR LOWER(s.address) LIKE LOWER({ph})
               OR LOWER(s.city) LIKE LOWER({ph})
               OR LOWER(s.postal) LIKE LOWER({ph})
            )
            ORDER BY s.store_number
            LIMIT 10
        """
        try:
            if USE_POSTGRES:
                db.rollback()
                cur = db.cursor()
                cur.execute(sql_fallback, (like, like, like, like, like))
                rows = cur.fetchall()
                cur.close()
            else:
                rows = db.execute(sql_fallback, (like, like, like, like, like)).fetchall()
        except Exception as e2:
            print(f"[store-search] fallback also failed: {e2}")
            return jsonify({'matches': [], 'query': q, 'error': str(e2)[:200]})

    matches = [
        {
            'id': r[0],
            'store_number': r[1],
            'account': r[2],
            'address': r[3],
            'city': r[4],
            'postal': r[5],
            'phone': r[6],
            'manager_phone': r[7],
            'manager_name': r[8],
            'rep': r[9],
            'lat': float(r[10]) if r[10] else 0,
            'lng': float(r[11]) if r[11] else 0,
            'last_activity_at': str(r[12]) if r[12] else None,
            'last_activity_type': r[13] or None,
            'last_activity_rep': r[14] or '',
            'last_activity_notes': (r[15] or '')[:200],  # truncate for typeahead
        }
        for r in rows
    ]
    return jsonify({'matches': matches, 'query': q})


@app.route('/api/crm/stores-finder', methods=['GET'])
@cached_response(ttl_seconds=120, key_args=('city', 'rep', 'territory_id', 'priority'))
def api_crm_stores_finder():
    """Full directory of all stores with their address, manager, phone, territory,
    and last-interaction summary. Powers the /finder page — one-shot load of all
    766 stores so the client can do snappy local search/filter.

    Query params (all optional):
      city, rep, territory_id, priority — filter
      include_inactive=1 — include soft-deleted stores (default: hide)

    Response:
      { count, stores: [{store_number, account, address, city, postal,
        phone, manager_name, manager_phone, store_email, rep, priority,
        territory_id, territory_name, territory_color, lat, lng,
        last_activity_at, last_activity_type, last_activity_rep,
        last_activity_notes, total_activities, total_deals, open_deals }, ...]
        freshness: {...} }
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = ['1=1']
    params = []
    city = (request.args.get('city') or '').strip()
    rep = (request.args.get('rep') or '').strip()
    territory_id = request.args.get('territory_id', type=int)
    priority = (request.args.get('priority') or '').strip()
    if city:
        where.append(f"LOWER(s.city) = LOWER({ph})")
        params.append(city)
    if rep:
        where.append(f"s.rep = {ph}")
        params.append(rep)
    if territory_id:
        where.append(f"s.territory_id = {ph}")
        params.append(territory_id)
    if priority:
        where.append(f"s.priority = {ph}")
        params.append(priority)

    if USE_POSTGRES:
        sql = f"""
            SELECT s.id, s.store_number, COALESCE(s.account,''), COALESCE(s.address,''),
                   COALESCE(s.city,''), COALESCE(s.postal,''),
                   COALESCE(s.phone,''), COALESCE(s.manager_phone,''),
                   COALESCE(s.manager_name,''), COALESCE(s.asst_manager_name,''),
                   COALESCE(s.store_email,''), COALESCE(s.rep,''),
                   COALESCE(s.priority,''), s.territory_id,
                   COALESCE(t.name,'Unassigned'), COALESCE(t.color,'#888'),
                   COALESCE(s.lat,0), COALESCE(s.lng,0),
                   la.last_activity_at, la.last_activity_type,
                   la.last_activity_rep, la.last_activity_notes,
                   ac.total_activities, dc.total_deals, dc.open_deals
            FROM stores s
            LEFT JOIN territories t ON t.id = s.territory_id
            LEFT JOIN LATERAL (
                SELECT a.created_at AS last_activity_at,
                       a.activity_type AS last_activity_type,
                       COALESCE(a.rep, r.name, '') AS last_activity_rep,
                       COALESCE(a.notes, '') AS last_activity_notes
                FROM activities a
                LEFT JOIN reps r ON r.id = a.rep_id
                WHERE a.store_id = s.id AND a.deleted_at IS NULL
                ORDER BY a.created_at DESC
                LIMIT 1
            ) la ON TRUE
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS total_activities
                FROM activities a2
                WHERE a2.store_id = s.id AND a2.deleted_at IS NULL
            ) ac ON TRUE
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS total_deals,
                       SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) AS open_deals
                FROM deals d
                WHERE d.store_number = s.store_number
            ) dc ON TRUE
            WHERE {' AND '.join(where)}
            ORDER BY
              CASE WHEN la.last_activity_at IS NOT NULL THEN 0 ELSE 1 END,
              la.last_activity_at DESC NULLS LAST,
              s.store_number
        """
        cur = db.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        except Exception as e:
            db.rollback()
            cur.close()
            return jsonify({'error': str(e)[:300]}), 500
        cur.close()
    else:
        # SQLite fallback (LATERAL JOIN unsupported)
        sql = f"""
            SELECT s.id, s.store_number, COALESCE(s.account,''), COALESCE(s.address,''),
                   COALESCE(s.city,''), COALESCE(s.postal,''),
                   COALESCE(s.phone,''), COALESCE(s.manager_phone,''),
                   COALESCE(s.manager_name,''), COALESCE(s.asst_manager_name,''),
                   COALESCE(s.store_email,''), COALESCE(s.rep,''),
                   COALESCE(s.priority,''), s.territory_id,
                   COALESCE(t.name,'Unassigned'), COALESCE(t.color,'#888'),
                   COALESCE(s.lat,0), COALESCE(s.lng,0),
                   (SELECT created_at FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1),
                   (SELECT activity_type FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1),
                   (SELECT COALESCE(rep,'') FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1),
                   (SELECT COALESCE(notes,'') FROM activities WHERE store_id=s.id AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1),
                   (SELECT COUNT(*) FROM activities WHERE store_id=s.id AND deleted_at IS NULL),
                   (SELECT COUNT(*) FROM deals WHERE store_number=s.store_number),
                   (SELECT COUNT(*) FROM deals WHERE store_number=s.store_number AND closed_at IS NULL)
            FROM stores s
            LEFT JOIN territories t ON t.id = s.territory_id
            WHERE {' AND '.join(where)}
            ORDER BY s.store_number
        """
        rows = db.execute(sql, params).fetchall()

    stores = []
    for r in rows:
        stores.append({
            'id': r[0], 'store_number': r[1], 'account': r[2], 'address': r[3],
            'city': r[4], 'postal': r[5], 'phone': r[6], 'manager_phone': r[7],
            'manager_name': r[8], 'asst_manager_name': r[9],
            'store_email': r[10], 'rep': r[11], 'priority': r[12],
            'territory_id': r[13], 'territory_name': r[14], 'territory_color': r[15],
            'lat': float(r[16]) if r[16] else 0, 'lng': float(r[17]) if r[17] else 0,
            'last_activity_at': str(r[18]) if r[18] else None,
            'last_activity_type': r[19] or None,
            'last_activity_rep': r[20] or '',
            'last_activity_notes': (r[21] or '')[:300],
            'total_activities': int(r[22] or 0),
            'total_deals': int(r[23] or 0),
            'open_deals': int(r[24] or 0),
        })
    return jsonify({
        'count': len(stores),
        'stores': stores,
        'filters': {'city': city or None, 'rep': rep or None,
                    'territory_id': territory_id, 'priority': priority or None},
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/nearby', methods=['GET'])
def api_crm_nearby():
    """Stores near a given lat/lng, sorted by distance.

    Query params:
      lat, lng (required, floats)
      radius_km (default 15)
      limit (default 25)
      sku (optional) — filters to stores carrying this tracked SKU; result includes
        on_hand and status; non-listed stores get an opportunity_score for pitches
    """
    try:
        lat = float(request.args.get('lat', 0))
        lng = float(request.args.get('lng', 0))
    except ValueError:
        return jsonify({'error': 'lat/lng must be floats'}), 400
    if lat == 0 or lng == 0:
        return jsonify({'error': 'lat/lng required and must be non-zero'}), 400
    radius_km = float(request.args.get('radius_km', 15))
    limit = int(request.args.get('limit', 25))
    sku = request.args.get('sku', '').strip()

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    # Pull all stores with valid coords; haversine in Python is fine at 766 rows.
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("""
            SELECT s.id, s.store_number, s.account, s.address, s.city, s.postal,
                   s.priority, s.rep, s.lat, s.lng, s.territory_id,
                   COALESCE(t.code, ''), COALESCE(t.name, ''), COALESCE(t.color, '#888')
            FROM stores s LEFT JOIN territories t ON t.id = s.territory_id
            WHERE s.lat <> 0 AND s.lng <> 0
        """)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute("""
            SELECT s.id, s.store_number, s.account, s.address, s.city, s.postal,
                   s.priority, s.rep, s.lat, s.lng, s.territory_id,
                   COALESCE(t.code, ''), COALESCE(t.name, ''), COALESCE(t.color, '#888')
            FROM stores s LEFT JOIN territories t ON t.id = s.territory_id
            WHERE s.lat <> 0 AND s.lng <> 0
        """).fetchall()

    # Lookup status for SKU (if requested)
    sku_status = {}
    if sku:
        sku_norm = sku.zfill(7)
        # Find latest snapshot for this SKU
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s", (sku_norm,)
            )
            latest = cur.fetchone()[0]
            if latest:
                cur.execute(
                    "SELECT store_number, status, on_hand FROM sod_inventory "
                    "WHERE sku = %s AND snapshot_date = %s",
                    (sku_norm, latest),
                )
                sku_status = {r[0]: {'status': r[1], 'on_hand': r[2] or 0} for r in cur.fetchall()}
            cur.close()
        else:
            latest = db.execute(
                "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = ?", (sku_norm,)
            ).fetchone()[0]
            if latest:
                sku_status = {r[0]: {'status': r[1], 'on_hand': r[2] or 0} for r in db.execute(
                    "SELECT store_number, status, on_hand FROM sod_inventory "
                    "WHERE sku = ? AND snapshot_date = ?",
                    (sku_norm, latest),
                ).fetchall()}

    out = []
    for r in rows:
        d = haversine(lat, lng, float(r[8] or 0), float(r[9] or 0))
        if d > radius_km:
            continue
        item = {
            'id': r[0], 'store_number': r[1], 'account': r[2],
            'address': r[3], 'city': r[4], 'postal': r[5],
            'priority': r[6], 'rep': r[7],
            'lat': r[8], 'lng': r[9],
            'territory_id': r[10], 'territory_code': r[11],
            'territory_name': r[12], 'territory_color': r[13],
            'distance_km': round(d, 2),
        }
        if sku:
            stat = sku_status.get(item['store_number'])
            if stat:
                item['sku_status'] = stat['status']
                item['sku_on_hand'] = stat['on_hand']
                # Pitch score: prioritize stores where SKU is delisting or low stock
                if stat['status'] in ('D', 'F'):
                    item['opportunity_score'] = 60
                elif stat['on_hand'] <= 1:
                    item['opportunity_score'] = 50
                elif stat['on_hand'] <= 3:
                    item['opportunity_score'] = 25
                else:
                    item['opportunity_score'] = 5
            else:
                item['sku_status'] = None
                item['sku_on_hand'] = 0
                item['opportunity_score'] = 35  # NOT listed = listing opportunity
        out.append(item)

    out.sort(key=lambda x: x['distance_km'])
    return jsonify({
        'origin': {'lat': lat, 'lng': lng},
        'radius_km': radius_km,
        'sku': sku.zfill(7) if sku else None,
        'results': out[:limit],
        'total_within_radius': len(out),
    })


# ------- AI assistant: Claude-powered NL → SQL/data → narrative -------
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '').strip()
AI_MODEL = os.environ.get('ANTHROPIC_MODEL', 'claude-sonnet-4-5-20250929')


@app.route('/api/ai/ask', methods=['POST'])
def api_ai_ask():
    """Claude-powered natural-language query over CRM data.

    Strategy: we ship Claude a SCHEMA + sample data and the user's question; Claude
    generates a single read-only SQL query (SELECT only); we run it; we ship the
    result back to Claude for natural-language summarization. Returns:
      { question, sql, rows, answer, model }

    Safety:
      - Only SELECT queries allowed (regex-checked + we run on a separate connection
        with autocommit+timeout)
      - Only whitelisted tables exposed in the schema prompt
      - Hard row limit of 1000
    """
    if not ANTHROPIC_API_KEY:
        return jsonify({'error': 'ANTHROPIC_API_KEY env var not set'}), 503
    body = request.get_json(silent=True) or {}
    question = (body.get('question') or '').strip()
    if not question or len(question) > 500:
        return jsonify({'error': 'question must be 1-500 chars'}), 400

    schema = """
    Tables (PostgreSQL, all read-only):

    sod_inventory(sku TEXT, store_number INT, snapshot_date DATE,
                  status TEXT 'L'=Listed/'D'=Delisting/'F'=Fully Delisted,
                  on_hand INT, product_name TEXT)
    sod_products(sku TEXT PK, product_name TEXT, current_status TEXT,
                 store_count INT, total_on_hand INT, is_tracked BOOL,
                 brand TEXT, category TEXT, category_group TEXT)
    sod_listing_changes(sku TEXT, store_number INT, change_date DATE,
                        old_status TEXT, new_status TEXT,
                        change_type TEXT 'NEW_LISTING'/'DELISTED'/'RELISTED'/'STATUS_FLIP'/'BASELINE')
    stores(id INT, store_number INT UNIQUE, account TEXT, address TEXT, city TEXT,
           postal TEXT, rep TEXT, priority TEXT,
           lat REAL, lng REAL, territory_id INT)
    territories(id INT PK, code TEXT, name TEXT, region TEXT, color TEXT)
    horeca_accounts(id INT, name TEXT, account_type TEXT,
                    city TEXT, status TEXT, priority TEXT, territory_id INT)
    sales_goals(id INT, scope TEXT, scope_key TEXT, period_start DATE,
                period_end DATE, target_units INT, target_listings INT)

    Tracked SKUs (Anu portfolio): """ + ', '.join(
        f"'{s}' ({b} {n})" for s, (b, n) in SOD_TRACKED_SKUS.items()
    ) + """

    Latest snapshot date is in `(SELECT MAX(snapshot_date) FROM sod_inventory)`.
    """

    system_prompt = (
        "You are a SQL analyst for the Anu Spirits LCBO CRM. The user asks a question; "
        "you respond with EXACTLY one read-only SQL SELECT statement that answers it. "
        "Use Postgres syntax. Do NOT use INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE. "
        "Keep the result <= 50 rows (use LIMIT). Output ONLY the SQL — no commentary, "
        "no markdown, no code fences. The schema is:\n\n" + schema
    )

    # Step 1: generate SQL
    try:
        r = http_requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': AI_MODEL,
                'max_tokens': 1024,
                'system': system_prompt,
                'messages': [{'role': 'user', 'content': question}],
            },
            timeout=30,
        )
        r.raise_for_status()
        sql = r.json()['content'][0]['text'].strip()
        # Strip code-fence if Claude added one
        if sql.startswith('```'):
            sql = re.sub(r'^```\w*\n?', '', sql)
            sql = re.sub(r'\n?```$', '', sql).strip()
    except Exception as e:
        return jsonify({'error': f'AI generation failed: {e}'}), 502

    # Safety: SELECT-only
    sql_lower = sql.lower().lstrip()
    if not sql_lower.startswith('select') and not sql_lower.startswith('with'):
        return jsonify({'error': 'AI returned non-SELECT', 'sql': sql}), 422
    forbidden = ['insert ', 'update ', 'delete ', 'drop ', 'alter ', 'truncate ', 'create ', 'grant ', 'revoke ', '; ', ';\n']
    if any(f in sql_lower for f in forbidden):
        return jsonify({'error': 'AI returned dangerous SQL', 'sql': sql}), 422

    # Run on a fresh autocommit connection so we can't poison the request transaction.
    rows: list = []
    cols: list = []
    try:
        if USE_POSTGRES:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute('SET statement_timeout = 5000')
                cur.execute(sql)
                rows = cur.fetchmany(1000)
                cols = [d[0] for d in cur.description] if cur.description else []
            conn.close()
        else:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.execute(sql)
            rows = cur.fetchmany(1000)
            cols = [d[0] for d in cur.description] if cur.description else []
            conn.close()
        rows_dict = [
            {cols[i]: _json_safe(v) for i, v in enumerate(row)} for row in rows
        ]
    except Exception as e:
        return jsonify({'error': f'SQL execution failed: {e}', 'sql': sql}), 422

    # Step 2: ask Claude to summarize
    try:
        summary_msg = (
            f"User asked: {question}\n\n"
            f"SQL run: {sql}\n\n"
            f"Result ({len(rows_dict)} rows): {json.dumps(rows_dict[:30], default=str)}\n\n"
            f"Write a 2-3 sentence answer in plain English. Quote specific numbers. "
            f"Don't recommend actions unless asked."
        )
        r2 = http_requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': AI_MODEL,
                'max_tokens': 512,
                'messages': [{'role': 'user', 'content': summary_msg}],
            },
            timeout=30,
        )
        r2.raise_for_status()
        answer = r2.json()['content'][0]['text'].strip()
    except Exception as e:
        answer = f"(AI summarization failed: {e})"

    return jsonify({
        'question': question,
        'sql': sql,
        'rows': rows_dict,
        'columns': cols,
        'row_count': len(rows_dict),
        'answer': answer,
        'model': AI_MODEL,
    })


# ======================================================================================
# =========================== SPRINT 3: SYSTEM-OF-ACTION CRM ===========================
#
# The heart of a real commercial CRM. Everything above was analytics; now we let reps
# DO things — log visits, move deals through pipeline, hit quotas, get told what to
# do next. This is what separates LCBO Tracker from a dashboard.
# ======================================================================================

# Pipeline stages (ordered). Moving forward = deal progressing. "listed" and "lost"
# are terminal.
DEAL_STAGES = [
    ('prospecting', 'Prospecting', 10),
    ('pitched', 'Pitched', 25),
    ('tasting_scheduled', 'Tasting Scheduled', 40),
    ('tasting_done', 'Tasting Done', 55),
    ('samples_left', 'Samples Left', 65),
    ('in_review', 'In Review (LCBO)', 80),
    ('listed', 'Listed (Won)', 100),
    ('lost', 'Lost', 0),
]


def _current_quarter(date_obj=None):
    """Return a quarter label like '2026-Q2' for the given date (defaults to today)."""
    d = date_obj or _toronto_today()
    q = (d.month - 1) // 3 + 1
    return f'{d.year}-Q{q}'


def _sod_velocity_for(sku, store_number=None, days=30):
    """Compute units-per-week velocity for a SKU (optionally one store).

    We use the CURRENT snapshot vs the snapshot N days ago at the same store.
    on_hand DECREASES = units sold ≈ velocity. If on_hand increases, that's a
    restock — we floor at 0 (conservative).
    Returns dict: {week_velocity, days_to_oos, current_on_hand, prior_on_hand, prior_date}.
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    sku_norm = str(sku).zfill(7)
    params = [sku_norm]
    where_store = ''
    if store_number is not None:
        where_store = f' AND store_number = {ph}'
        params.append(store_number)

    # Latest snapshot
    q_latest = (
        f"SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = {ph}{where_store}"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q_latest, params)
        latest = cur.fetchone()[0]
        cur.close()
    else:
        latest = db.execute(q_latest, params).fetchone()[0]
    if not latest:
        return {'week_velocity': None, 'days_to_oos': None, 'current_on_hand': 0, 'prior_on_hand': 0, 'prior_date': None}

    # Prior snapshot (~days ago)
    target_prior = (latest if isinstance(latest, str) else latest.isoformat())
    try:
        ld = datetime.strptime(str(latest), '%Y-%m-%d').date() if isinstance(latest, str) else latest
    except Exception:
        ld = _toronto_today()
    prior_target = (ld - timedelta(days=days)).isoformat()
    q_prior = (
        f"SELECT MAX(snapshot_date) FROM sod_inventory "
        f"WHERE sku = {ph}{where_store} AND snapshot_date <= {ph}"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q_prior, params + [prior_target])
        prior = cur.fetchone()[0]
        cur.close()
    else:
        prior = db.execute(q_prior, params + [prior_target]).fetchone()[0]

    # Sum on_hand at latest + prior
    def _sum_oh(snap):
        if not snap:
            return 0
        q = (
            f"SELECT COALESCE(SUM(on_hand), 0) FROM sod_inventory "
            f"WHERE sku = {ph}{where_store} AND snapshot_date = {ph}"
        )
        if USE_POSTGRES:
            c = db.cursor()
            c.execute(q, params + [snap])
            r = c.fetchone()[0]
            c.close()
        else:
            r = db.execute(q, params + [snap]).fetchone()[0]
        return int(r or 0)

    current_oh = _sum_oh(latest)
    prior_oh = _sum_oh(prior)

    # Days elapsed between snapshots
    if not prior or prior == latest:
        return {
            'week_velocity': None, 'days_to_oos': None,
            'current_on_hand': current_oh, 'prior_on_hand': prior_oh,
            'prior_date': str(prior) if prior else None,
        }
    try:
        latest_d = datetime.strptime(str(latest), '%Y-%m-%d').date()
        prior_d = datetime.strptime(str(prior), '%Y-%m-%d').date()
        elapsed = (latest_d - prior_d).days
    except Exception:
        elapsed = days

    sold = max(prior_oh - current_oh, 0)
    week_velocity = round(sold * 7 / max(elapsed, 1), 1) if elapsed > 0 else 0
    days_to_oos = round(current_oh / (week_velocity / 7), 1) if week_velocity > 0 else None

    return {
        'week_velocity': week_velocity,
        'days_to_oos': days_to_oos,
        'current_on_hand': current_oh,
        'prior_on_hand': prior_oh,
        'prior_date': str(prior),
    }


@app.route('/api/crm/velocity/<sku>', methods=['GET'])
@cached_response(ttl_seconds=300, key_args=())
def api_crm_velocity(sku):
    """Units-per-week velocity for a SKU (aggregated across all stores).

    Also returns per-store velocity for the top-N highest-velocity stores.
    """
    days = int(request.args.get('days', 30))
    top = int(request.args.get('top', 20))

    overall = _sod_velocity_for(sku, days=days)

    # Per-store velocity — only at stores currently listing (status='L')
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    sku_norm = str(sku).zfill(7)

    # Latest snapshot overall for this sku
    latest_q = f"SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = {ph}"
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(latest_q, (sku_norm,))
        latest = cur.fetchone()[0]
        cur.close()
    else:
        latest = db.execute(latest_q, (sku_norm,)).fetchone()[0]

    by_store = []
    if latest:
        # Find all stores listing this SKU
        stores_q = (
            f"SELECT store_number FROM sod_inventory "
            f"WHERE sku = {ph} AND snapshot_date = {ph} AND status = 'L'"
        )
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(stores_q, (sku_norm, latest))
            store_nums = [r[0] for r in cur.fetchall()]
            cur.close()
        else:
            store_nums = [r[0] for r in db.execute(stores_q, (sku_norm, latest)).fetchall()]

        for sn in store_nums:
            v = _sod_velocity_for(sku_norm, store_number=sn, days=days)
            if v['week_velocity'] is None:
                continue
            by_store.append({'store_number': sn, **v})

    by_store.sort(key=lambda x: -(x.get('week_velocity') or 0))
    brand, pname = SOD_TRACKED_SKUS.get(sku_norm, ('', ''))
    return jsonify({
        'sku': sku_norm,
        'brand': brand,
        'product_name': pname,
        'window_days': days,
        'overall': overall,
        'per_store_top': by_store[:top],
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/store/<int:store_number>/replace-targets', methods=['GET'])
def api_crm_replace_targets(store_number):
    """For ONE store, return worst-performing competitor SKUs in EACH category that
    our tracked SKUs compete in. The killer rep-workflow feature: walk into a store,
    instantly see the pitch list ranked by replacement opportunity score.

    Per category, we return up to `per_cat` SKUs (default 5) sorted by:
      - status: D > F > L (delisting first, then fully delisted, then slow listed)
      - on_hand: ascending (lowest stock first)

    Each row includes the recommended OUR_SKU to pitch as replacement (the first
    tracked SKU we have in that category).
    """
    per_cat = int(request.args.get('per_cat', 5))
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # Latest snapshot at this store
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE store_number = %s", (store_number,))
        latest = cur.fetchone()[0]
        cur.close()
    else:
        latest = db.execute(
            "SELECT MAX(snapshot_date) FROM sod_inventory WHERE store_number = ?",
            (store_number,),
        ).fetchone()[0]

    if not latest:
        return jsonify({'store_number': store_number, 'snapshot_date': None, 'categories': []})

    # Determine which categories our tracked SKUs are in + map cat -> our pitch SKU
    tracked = list(SOD_TRACKED_SKUS.keys())
    pitch_for_cat = {}
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"SELECT sku, COALESCE(category, ''), COALESCE(brand, ''), COALESCE(product_name, '') "
            f"FROM sod_products WHERE sku IN ({','.join(['%s'] * len(tracked))})",
            tracked,
        )
        for r in cur.fetchall():
            sku, cat, brand, name = r[0], r[1], r[2], r[3]
            if cat and cat not in pitch_for_cat:
                # Use our hardcoded SOD_TRACKED_SKUS for canonical brand/name
                tb, tn = SOD_TRACKED_SKUS.get(sku, (brand, name))
                pitch_for_cat[cat] = {'sku': sku, 'brand': tb, 'product_name': tn}
        cur.close()
    else:
        rows = db.execute(
            f"SELECT sku, COALESCE(category, ''), COALESCE(brand, ''), COALESCE(product_name, '') "
            f"FROM sod_products WHERE sku IN ({','.join(['?'] * len(tracked))})",
            tracked,
        ).fetchall()
        for r in rows:
            sku, cat, brand, name = r[0], r[1], r[2], r[3]
            if cat and cat not in pitch_for_cat:
                tb, tn = SOD_TRACKED_SKUS.get(sku, (brand, name))
                pitch_for_cat[cat] = {'sku': sku, 'brand': tb, 'product_name': tn}

    if not pitch_for_cat:
        return jsonify({'store_number': store_number, 'snapshot_date': str(latest), 'categories': []})

    # For each category at this store, find competitor SKUs ranked by underperformance
    out = []
    for cat, pitch in pitch_for_cat.items():
        # competitor candidates: same category, at this store, in latest snapshot,
        # NOT one of our tracked SKUs, sorted by status then on_hand
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"""SELECT i.sku, i.product_name, i.status, i.on_hand,
                           p.brand, p.category
                    FROM sod_inventory i
                    JOIN sod_products p ON p.sku = i.sku
                    WHERE i.store_number = %s
                      AND i.snapshot_date = %s
                      AND p.category = %s
                      AND i.sku NOT IN ({','.join(['%s'] * len(tracked))})
                    ORDER BY
                      CASE i.status WHEN 'D' THEN 1 WHEN 'F' THEN 2 WHEN 'L' THEN 3 ELSE 4 END,
                      i.on_hand ASC
                    LIMIT %s""",
                [store_number, latest, cat] + tracked + [per_cat],
            )
            rows = cur.fetchall()
            cur.close()
        else:
            rows = db.execute(
                f"""SELECT i.sku, i.product_name, i.status, i.on_hand,
                           p.brand, p.category
                    FROM sod_inventory i
                    JOIN sod_products p ON p.sku = i.sku
                    WHERE i.store_number = ?
                      AND i.snapshot_date = ?
                      AND p.category = ?
                      AND i.sku NOT IN ({','.join(['?'] * len(tracked))})
                    ORDER BY
                      CASE i.status WHEN 'D' THEN 1 WHEN 'F' THEN 2 WHEN 'L' THEN 3 ELSE 4 END,
                      i.on_hand ASC
                    LIMIT ?""",
                [store_number, latest, cat] + tracked + [per_cat],
            ).fetchall()

        targets = []
        for r in rows:
            sku, name, status, on_hand, brand, _ = r
            score = 0
            if status == 'D':
                score += 50
            elif status == 'F':
                score += 30
            if (on_hand or 0) == 0:
                score += 40
            elif (on_hand or 0) <= 1:
                score += 25
            elif (on_hand or 0) <= 3:
                score += 10
            targets.append({
                'competitor_sku': sku,
                'competitor_name': name,
                'competitor_brand': brand,
                'competitor_status': status,
                'competitor_on_hand': int(on_hand or 0),
                'opportunity_score': score,
            })
        out.append({
            'category': cat,
            'pitch_our_sku': pitch['sku'],
            'pitch_our_brand': pitch['brand'],
            'pitch_our_product': pitch['product_name'],
            'targets': targets,
        })

    # Sort categories by total opportunity score (descending)
    out.sort(key=lambda c: -sum(t['opportunity_score'] for t in c['targets']))
    return jsonify({
        'store_number': store_number,
        'snapshot_date': str(latest),
        'categories': out,
    })


@app.route('/api/crm/store/<int:store_number>/full', methods=['GET'])
def api_crm_store_full(store_number):
    """One-call store profile: store info + tracked SKU status + recent activities +
    open deals + replace-targets summary. Powers the upgraded /stores/[id] page."""
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    # Store info
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            """SELECT s.id, s.store_number, s.account, s.address, s.city, s.postal,
                      s.phone, s.email, s.priority, s.rep, s.lat, s.lng,
                      s.manager_name, s.asst_manager_name, s.manager_phone, s.store_email,
                      COALESCE(t.id, 0), COALESCE(t.code, ''), COALESCE(t.name, ''), COALESCE(t.color, '#888')
               FROM stores s LEFT JOIN territories t ON t.id = s.territory_id
               WHERE s.store_number = %s LIMIT 1""",
            (store_number,),
        )
        s = cur.fetchone()
        cur.close()
    else:
        s = db.execute(
            """SELECT s.id, s.store_number, s.account, s.address, s.city, s.postal,
                      s.phone, s.email, s.priority, s.rep, s.lat, s.lng,
                      s.manager_name, s.asst_manager_name, s.manager_phone, s.store_email,
                      COALESCE(t.id, 0), COALESCE(t.code, ''), COALESCE(t.name, ''), COALESCE(t.color, '#888')
               FROM stores s LEFT JOIN territories t ON t.id = s.territory_id
               WHERE s.store_number = ? LIMIT 1""",
            (store_number,),
        ).fetchone()
    if not s:
        return jsonify({'error': 'store not found'}), 404
    store = {
        'id': s[0], 'store_number': s[1], 'account': s[2], 'address': s[3],
        'city': s[4], 'postal': s[5], 'phone': s[6], 'email': s[7],
        'priority': s[8], 'rep': s[9], 'lat': s[10], 'lng': s[11],
        'manager_name': s[12], 'asst_manager_name': s[13],
        'manager_phone': s[14], 'store_email': s[15],
        'territory_id': s[16] or None, 'territory_code': s[17],
        'territory_name': s[18], 'territory_color': s[19],
    }
    return jsonify({'store': store, 'snapshot_date': str(_max_snapshot_date()) if _max_snapshot_date() else None})


@app.route('/api/crm/shelf-share/<int:store_number>', methods=['GET'])
def api_crm_shelf_share(store_number):
    """For one store: our share of each tracked SKU's category at the latest snapshot.

    Example output per category:
      { category: 'Vodka', our_facings: 12, total_facings: 180,
        our_on_hand: 240, total_on_hand: 5600,
        share_by_facings_pct: 6.7, share_by_on_hand_pct: 4.3 }
    Where "facings" is approximated by store_count for each SKU in this store's
    category at the latest snapshot.
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    latest_q = f"SELECT MAX(snapshot_date) FROM sod_inventory WHERE store_number = {ph}"
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(latest_q, (store_number,))
        latest = cur.fetchone()[0]
        cur.close()
    else:
        latest = db.execute(latest_q, (store_number,)).fetchone()[0]

    if not latest:
        return jsonify({
            'store_number': store_number,
            'snapshot_date': None,
            'categories': [],
        })

    # What categories are our tracked SKUs in?
    tracked = list(SOD_TRACKED_SKUS.keys())
    our_cats = set()
    if USE_POSTGRES:
        phs = ','.join(['%s'] * len(tracked))
        cur = db.cursor()
        cur.execute(
            f"SELECT DISTINCT category FROM sod_products "
            f"WHERE sku IN ({phs}) AND COALESCE(category,'') <> ''",
            tracked,
        )
        our_cats = {r[0] for r in cur.fetchall()}
        cur.close()
    else:
        phs = ','.join(['?'] * len(tracked))
        our_cats = {r[0] for r in db.execute(
            f"SELECT DISTINCT category FROM sod_products "
            f"WHERE sku IN ({phs}) AND COALESCE(category,'') <> ''",
            tracked,
        ).fetchall()}

    out = []
    for cat in sorted(our_cats):
        # Total: every SKU in this category at this store+snapshot
        cat_q = (
            f"SELECT COUNT(*) AS facings, COALESCE(SUM(i.on_hand), 0) AS oh "
            f"FROM sod_inventory i JOIN sod_products p ON p.sku = i.sku "
            f"WHERE i.store_number = {ph} AND i.snapshot_date = {ph} "
            f"AND p.category = {ph} AND i.status IN ('L','D')"
        )
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(cat_q, (store_number, latest, cat))
            r = cur.fetchone()
            total_facings, total_oh = int(r[0] or 0), int(r[1] or 0)
            # Ours
            cur.execute(cat_q + f" AND i.sku IN ({phs})",
                        (store_number, latest, cat) + tuple(tracked))
            r = cur.fetchone()
            our_facings, our_oh = int(r[0] or 0), int(r[1] or 0)
            cur.close()
        else:
            r = db.execute(cat_q, (store_number, latest, cat)).fetchone()
            total_facings, total_oh = int(r[0] or 0), int(r[1] or 0)
            r = db.execute(cat_q + f" AND i.sku IN ({phs})",
                           (store_number, latest, cat) + tuple(tracked)).fetchone()
            our_facings, our_oh = int(r[0] or 0), int(r[1] or 0)

        out.append({
            'category': cat,
            'our_facings': our_facings,
            'total_facings': total_facings,
            'our_on_hand': our_oh,
            'total_on_hand': total_oh,
            'share_by_facings_pct': round(100 * our_facings / total_facings, 1) if total_facings else 0,
            'share_by_on_hand_pct': round(100 * our_oh / total_oh, 1) if total_oh else 0,
        })

    return jsonify({
        'store_number': store_number,
        'snapshot_date': str(latest),
        'categories': out,
    })


# =========================== Deal pipeline CRUD ===========================

@app.route('/api/crm/deals', methods=['GET'])
def api_crm_deals_list():
    """List all deals, with optional filters."""
    rep = request.args.get('rep', '').strip()
    stage = request.args.get('stage', '').strip()
    sku = request.args.get('sku', '').strip()
    store = request.args.get('store_number', type=int)
    include_closed = request.args.get('include_closed', '').lower() in ('1', 'true')

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = ['1=1']
    params = []
    if rep:
        where.append(f'LOWER(TRIM(d.owner_rep)) = LOWER(TRIM({ph}))')
        params.append(rep)
    if stage:
        where.append(f'd.stage = {ph}')
        params.append(stage)
    if sku:
        where.append(f'd.sku = {ph}')
        params.append(sku.zfill(7))
    if store:
        where.append(f'd.store_number = {ph}')
        params.append(store)
    if not include_closed:
        where.append("d.stage NOT IN ('listed','lost')")

    q = f"""
        SELECT d.id, d.store_number, d.horeca_account_id, d.sku, d.stage,
               d.probability, d.expected_close_date, d.expected_units, d.expected_revenue,
               d.owner_rep, d.next_action, d.next_action_date, d.notes,
               d.source, d.closed_at, d.closed_reason, d.created_at, d.updated_at,
               s.account, s.city, s.territory_id, COALESCE(t.name, ''), COALESCE(t.color, '#888'),
               h.name AS horeca_name
        FROM deals d
        LEFT JOIN stores s ON s.store_number = d.store_number
        LEFT JOIN territories t ON t.id = s.territory_id
        LEFT JOIN horeca_accounts h ON h.id = d.horeca_account_id
        WHERE {' AND '.join(where)}
        ORDER BY
            CASE d.stage
              WHEN 'prospecting' THEN 1 WHEN 'pitched' THEN 2
              WHEN 'tasting_scheduled' THEN 3 WHEN 'tasting_done' THEN 4
              WHEN 'samples_left' THEN 5 WHEN 'in_review' THEN 6
              WHEN 'listed' THEN 7 WHEN 'lost' THEN 8 ELSE 9
            END,
            d.next_action_date NULLS LAST, d.updated_at DESC
    """ if USE_POSTGRES else f"""
        SELECT d.id, d.store_number, d.horeca_account_id, d.sku, d.stage,
               d.probability, d.expected_close_date, d.expected_units, d.expected_revenue,
               d.owner_rep, d.next_action, d.next_action_date, d.notes,
               d.source, d.closed_at, d.closed_reason, d.created_at, d.updated_at,
               s.account, s.city, s.territory_id, COALESCE(t.name, ''), COALESCE(t.color, '#888'),
               h.name AS horeca_name
        FROM deals d
        LEFT JOIN stores s ON s.store_number = d.store_number
        LEFT JOIN territories t ON t.id = s.territory_id
        LEFT JOIN horeca_accounts h ON h.id = d.horeca_account_id
        WHERE {' AND '.join(where)}
        ORDER BY d.stage, d.next_action_date, d.updated_at DESC
    """

    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()

    out = []
    for r in rows:
        sku_norm = r[3]
        brand, pname = SOD_TRACKED_SKUS.get(sku_norm, ('', sku_norm))
        out.append({
            'id': r[0], 'store_number': r[1], 'horeca_account_id': r[2], 'sku': sku_norm,
            'brand': brand, 'product_name': pname,
            'stage': r[4], 'probability': r[5],
            'expected_close_date': str(r[6]) if r[6] else None,
            'expected_units': r[7], 'expected_revenue': float(r[8] or 0),
            'owner_rep': r[9], 'next_action': r[10],
            'next_action_date': str(r[11]) if r[11] else None,
            'notes': r[12], 'source': r[13],
            'closed_at': str(r[14]) if r[14] else None, 'closed_reason': r[15],
            'created_at': str(r[16]) if r[16] else None,
            'updated_at': str(r[17]) if r[17] else None,
            'account': r[18], 'city': r[19], 'territory_id': r[20],
            'territory_name': r[21], 'territory_color': r[22],
            'horeca_name': r[23],
        })
    # Pipeline summary — deals per stage
    summary = {}
    for d in out:
        summary[d['stage']] = summary.get(d['stage'], 0) + 1
    return jsonify({'deals': out, 'stage_counts': summary, 'stages': [
        {'key': k, 'label': l, 'probability': p} for k, l, p in DEAL_STAGES
    ]})


@app.route('/api/crm/deals', methods=['POST'])
def api_crm_deals_create():
    d = request.get_json() or {}
    if not d.get('sku'):
        return jsonify({'error': 'sku required'}), 400
    if not (d.get('store_number') or d.get('horeca_account_id')):
        return jsonify({'error': 'store_number or horeca_account_id required'}), 400
    sku_norm = str(d['sku']).zfill(7)
    stage = d.get('stage', 'prospecting')
    db = get_db()
    cols = ['store_number', 'horeca_account_id', 'sku', 'stage', 'probability',
            'expected_close_date', 'expected_units', 'expected_revenue',
            'owner_rep', 'next_action', 'next_action_date', 'notes', 'source']
    vals = (
        d.get('store_number'), d.get('horeca_account_id'), sku_norm, stage,
        int(d.get('probability') or next((p for k, _, p in DEAL_STAGES if k == stage), 10)),
        d.get('expected_close_date') or None,
        int(d.get('expected_units') or 0), float(d.get('expected_revenue') or 0),
        d.get('owner_rep', ''), d.get('next_action', ''),
        d.get('next_action_date') or None,
        d.get('notes', ''), d.get('source', 'manual'),
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"INSERT INTO deals ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))}) RETURNING id",
            vals,
        )
        new_id = cur.fetchone()[0]
        # Auto-stamp closed_at if created in a terminal stage so manager dashboard
        # listings_won_60d includes deals created directly as 'listed' (e.g. bulk
        # imports). Without this, listings_won_60d would always show 0 for
        # imports until someone PATCHed the deal.
        if stage in ('listed', 'lost'):
            cur.execute(
                "UPDATE deals SET closed_at = NOW() WHERE id = %s AND closed_at IS NULL",
                (new_id,),
            )
        db.commit()
        cur.close()
    else:
        c = db.execute(
            f"INSERT INTO deals ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
            vals,
        )
        new_id = c.lastrowid
        if stage in ('listed', 'lost'):
            db.execute(
                "UPDATE deals SET closed_at = CURRENT_TIMESTAMP WHERE id = ? AND closed_at IS NULL",
                (new_id,),
            )
        db.commit()
    return jsonify({'status': 'ok', 'id': new_id})


@app.route('/api/crm/admin/backfill-closed-deals', methods=['POST'])
@require_admin_token
def api_crm_backfill_closed_deals():
    """One-shot fix: set closed_at on any 'listed' or 'lost' deal where it's NULL.

    Useful right after a bulk import where the create endpoint was older.
    """
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "UPDATE deals SET closed_at = NOW() "
            "WHERE stage IN ('listed','lost') AND closed_at IS NULL"
        )
        n = cur.rowcount
        db.commit()
        cur.close()
    else:
        c = db.execute(
            "UPDATE deals SET closed_at = CURRENT_TIMESTAMP "
            "WHERE stage IN ('listed','lost') AND closed_at IS NULL"
        )
        n = c.rowcount
        db.commit()
    return jsonify({'status': 'ok', 'updated': n})


@app.route('/api/crm/deals/<int:deal_id>', methods=['PUT', 'PATCH'])
def api_crm_deals_update(deal_id):
    d = request.get_json() or {}
    allowed = ('stage', 'probability', 'expected_close_date', 'expected_units',
               'expected_revenue', 'owner_rep', 'next_action', 'next_action_date',
               'notes', 'closed_reason')
    sets = []
    params = []
    ph = '%s' if USE_POSTGRES else '?'
    for c in allowed:
        if c in d:
            sets.append(f'{c}={ph}')
            v = d[c]
            if c in ('expected_close_date', 'next_action_date') and v == '':
                v = None
            params.append(v)
    # Auto-update probability from stage if stage changed but prob didn't
    if 'stage' in d and 'probability' not in d:
        st = d['stage']
        prob = next((p for k, _, p in DEAL_STAGES if k == st), None)
        if prob is not None:
            sets.append(f'probability={ph}')
            params.append(prob)
    # Auto-stamp closed_at if moving to a terminal stage
    if d.get('stage') in ('listed', 'lost'):
        sets.append(f'closed_at={"NOW()" if USE_POSTGRES else "CURRENT_TIMESTAMP"}')
    if not sets:
        return jsonify({'error': 'no updatable fields'}), 400
    sets.append(f'updated_at={"NOW()" if USE_POSTGRES else "CURRENT_TIMESTAMP"}')
    params.append(deal_id)
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(f"UPDATE deals SET {', '.join(sets)} WHERE id={ph}", params)
        db.commit()
        cur.close()
    else:
        db.execute(f"UPDATE deals SET {', '.join(sets)} WHERE id={ph}", params)
        db.commit()
    return jsonify({'status': 'ok'})


@app.route('/api/crm/deals/<int:deal_id>', methods=['DELETE'])
def api_crm_deals_delete(deal_id):
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(f"DELETE FROM deals WHERE id={ph}", (deal_id,))
        db.commit()
        cur.close()
    else:
        db.execute(f"DELETE FROM deals WHERE id={ph}", (deal_id,))
        db.commit()
    return jsonify({'status': 'ok'})


# =========================== Activity logging ===========================

@app.route('/api/crm/activities', methods=['GET'])
def api_crm_activities_list():
    """Recent activity feed."""
    days = int(request.args.get('days', 30))
    rep = request.args.get('rep', '').strip()
    store = request.args.get('store_number', type=int)
    horeca_id = request.args.get('horeca_account_id', type=int)
    limit = int(request.args.get('limit', 200))

    since = (_toronto_today() - timedelta(days=days)).isoformat()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = [f'a.created_at >= {ph}']
    params = [since]
    if rep:
        where.append(f'LOWER(TRIM(a.rep)) = LOWER(TRIM({ph}))')
        params.append(rep)
    if store:
        where.append(f's.store_number = {ph}')
        params.append(store)
    if horeca_id:
        where.append(f'a.horeca_account_id = {ph}')
        params.append(horeca_id)

    q = f"""
        SELECT a.id, a.created_at, a.activity_type, a.rep, a.outcome, a.notes,
               a.rating, a.duration_minutes, a.next_action, a.next_action_date,
               a.store_id, s.store_number, s.account, s.city,
               a.horeca_account_id, h.name AS horeca_name
        FROM activities a
        LEFT JOIN stores s ON s.id = a.store_id
        LEFT JOIN horeca_accounts h ON h.id = a.horeca_account_id
        WHERE {' AND '.join(where)}
        ORDER BY a.created_at DESC
        LIMIT {ph}
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params + [limit])
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params + [limit]).fetchall()

    out = [{
        'id': r[0], 'created_at': str(r[1]) if r[1] else None,
        'activity_type': r[2], 'rep': r[3], 'outcome': r[4], 'notes': r[5],
        'rating': r[6], 'duration_minutes': r[7],
        'next_action': r[8], 'next_action_date': str(r[9]) if r[9] else None,
        'store_id': r[10], 'store_number': r[11], 'account': r[12], 'city': r[13],
        'horeca_account_id': r[14], 'horeca_name': r[15],
    } for r in rows]
    return jsonify({'activities': out, 'window_days': days, 'total': len(out)})


@app.route('/api/crm/activities', methods=['POST'])
def api_crm_activities_create():
    """Log an activity (visit, call, email, tasting, sample-drop, POSM).

    Accepts optional sku_outcomes array [{sku, outcome, facings, competitor_notes}]
    which creates activity_sku_outcomes rows + optionally advances the deal pipeline.
    """
    d = request.get_json() or {}
    activity_type = (d.get('activity_type') or '').strip()
    if not activity_type:
        return jsonify({'error': 'activity_type required'}), 400

    # Resolve store_id (if store_number provided)
    store_id = d.get('store_id')
    store_number = d.get('store_number')
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    if not store_id and store_number:
        q = f"SELECT id FROM stores WHERE store_number = {ph} LIMIT 1"
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(q, (store_number,))
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(q, (store_number,)).fetchone()
        if r:
            store_id = r[0]

    # Resolve rep_id (we require a non-null rep_id due to FK in the original schema)
    rep_name = (d.get('rep') or '').strip()
    rep_id = None
    if rep_name:
        q = f"SELECT id FROM reps WHERE LOWER(TRIM(name)) = LOWER(TRIM({ph})) LIMIT 1"
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(q, (rep_name,))
            r = cur.fetchone()
            cur.close()
        else:
            r = db.execute(q, (rep_name,)).fetchone()
        if r:
            rep_id = r[0]
        else:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute("INSERT INTO reps (name) VALUES (%s) RETURNING id", (rep_name,))
                rep_id = cur.fetchone()[0]
                db.commit()
                cur.close()
            else:
                c = db.execute("INSERT INTO reps (name) VALUES (?)", (rep_name,))
                rep_id = c.lastrowid
                db.commit()

    if not (store_id or d.get('horeca_account_id')):
        return jsonify({'error': 'store_number or horeca_account_id required'}), 400
    if not rep_id:
        return jsonify({'error': 'rep required'}), 400

    # Sprint 6: visit_date allows backdating — rep can log a visit that already
    # happened. Defaults to today if not provided.
    visit_date = d.get('visit_date') or _toronto_today().isoformat()

    insert_cols = ['store_id', 'rep_id', 'activity_type', 'notes', 'rep',
                   'outcome', 'duration_minutes', 'rating',
                   'next_action', 'next_action_date', 'horeca_account_id',
                   'lat', 'lng', 'visit_date']
    vals = (
        store_id, rep_id, activity_type, d.get('notes', ''), rep_name,
        d.get('outcome', ''), int(d.get('duration_minutes') or 0), int(d.get('rating') or 0),
        d.get('next_action', ''), d.get('next_action_date') or None,
        d.get('horeca_account_id'),
        float(d.get('lat') or 0), float(d.get('lng') or 0),
        visit_date,
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"INSERT INTO activities ({','.join(insert_cols)}) VALUES ({','.join(['%s']*len(insert_cols))}) RETURNING id",
            vals,
        )
        activity_id = cur.fetchone()[0]
        db.commit()
        cur.close()
    else:
        c = db.execute(
            f"INSERT INTO activities ({','.join(insert_cols)}) VALUES ({','.join(['?']*len(insert_cols))})",
            vals,
        )
        activity_id = c.lastrowid
        db.commit()
    # Audit log
    try:
        _log_event('activity_created', 'activity', str(activity_id), rep_name,
                   {'activity_type': activity_type, 'store_number': store_number,
                    'visit_date': visit_date, 'sku_outcomes_count': len(d.get('sku_outcomes') or [])})
    except Exception:
        pass

    # Per-SKU outcomes → activity_sku_outcomes rows
    sku_outcomes = d.get('sku_outcomes') or []
    for so in sku_outcomes:
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(
                    "INSERT INTO activity_sku_outcomes "
                    "(activity_id, sku, outcome, facings, competitor_notes) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (activity_id, str(so['sku']).zfill(7), so.get('outcome', ''),
                     int(so.get('facings') or 0), so.get('competitor_notes', '')),
                )
                db.commit()
                cur.close()
            else:
                db.execute(
                    "INSERT INTO activity_sku_outcomes "
                    "(activity_id, sku, outcome, facings, competitor_notes) "
                    "VALUES (?,?,?,?,?)",
                    (activity_id, str(so['sku']).zfill(7), so.get('outcome', ''),
                     int(so.get('facings') or 0), so.get('competitor_notes', '')),
                )
                db.commit()
        except Exception as e:
            print(f'[activity-log] sku outcome insert failed: {e}')

    # Optional: if outcome suggests pipeline advancement, upsert a deal row
    # (e.g., "samples_left" → create/update deal with stage='samples_left')
    suggested_stage = d.get('advance_pipeline_stage')
    if suggested_stage and store_number and sku_outcomes:
        for so in sku_outcomes:
            sku_z = str(so['sku']).zfill(7)
            try:
                # Upsert style for deals — find existing open deal or create
                if USE_POSTGRES:
                    cur = db.cursor()
                    cur.execute(
                        "SELECT id FROM deals WHERE store_number=%s AND sku=%s "
                        "AND stage NOT IN ('listed','lost') LIMIT 1",
                        (store_number, sku_z),
                    )
                    r = cur.fetchone()
                    if r:
                        cur.execute(
                            "UPDATE deals SET stage=%s, owner_rep=%s, updated_at=NOW() WHERE id=%s",
                            (suggested_stage, rep_name, r[0]),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO deals (store_number, sku, stage, owner_rep, source) "
                            "VALUES (%s,%s,%s,%s,'activity_log')",
                            (store_number, sku_z, suggested_stage, rep_name),
                        )
                    db.commit()
                    cur.close()
                else:
                    r = db.execute(
                        "SELECT id FROM deals WHERE store_number=? AND sku=? "
                        "AND stage NOT IN ('listed','lost') LIMIT 1",
                        (store_number, sku_z),
                    ).fetchone()
                    if r:
                        db.execute(
                            "UPDATE deals SET stage=?, owner_rep=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                            (suggested_stage, rep_name, r[0]),
                        )
                    else:
                        db.execute(
                            "INSERT INTO deals (store_number, sku, stage, owner_rep, source) "
                            "VALUES (?,?,?,?,'activity_log')",
                            (store_number, sku_z, suggested_stage, rep_name),
                        )
                    db.commit()
            except Exception as e:
                print(f'[activity-log] pipeline advance failed: {e}')

    return jsonify({'status': 'ok', 'id': activity_id})


# =========================== Rep quotas ===========================

@app.route('/api/crm/priority-targets', methods=['GET'])
@cached_response(ttl_seconds=120, key_args=('rep', 'max_skus', 'days'))
def api_crm_priority_targets():
    """Priority targets for a rep — stores in their territory with ≤max_skus
    of our tracked SKUs currently listed. Server-side SQL filtering is fast.

    ?rep=Namit|Surya|Ikshit|Virat|Neeraj  &max_skus=1  &days=14  &max_per_day=10
    """
    rep = (request.args.get('rep') or '').strip()
    max_skus = int(request.args.get('max_skus', 1))
    days = max(1, min(int(request.args.get('days', 14)), 21))
    max_per_day = max(5, min(int(request.args.get('max_per_day', 10)), 14))

    # Same territory map as territory-plan — keep in sync.
    TERR = {
        'Namit': {'prefixes': ['M'],
                  'cities': ['Woodbridge','Vaughan','Maple','Markham','Stouffville',
                             'Newmarket','Aurora','Richmond Hill','Thornhill','Concord','Kleinburg']},
        'Ikshit': {'prefixes': ['L5','L6','L7'],
                   'cities': ['Burlington','Oakville','Milton','Georgetown','Mississauga','Brampton']},
        'Virat': {'prefixes': ['L1'],
                  'cities': ['Pickering','Ajax','Whitby','Oshawa','Bowmanville','Courtice',
                             'Clarington','Port Perry','Uxbridge']},
        'Surya': {'prefixes': ['K'],
                  'cities': ['Kingston','Brockville','Cornwall','Belleville','Trenton','Picton','Napanee']},
        'Neeraj': {'prefixes': ['N'],
                   'cities': ['Hamilton','Burlington','Niagara Falls','St. Catharines','Welland',
                              'Kitchener','Waterloo','Cambridge','Guelph','London','Brantford',
                              'Woodstock','Stratford','Sarnia','Windsor','Chatham']},
    }
    if rep not in TERR:
        return jsonify({'error': f'Unknown rep {rep!r}. Valid: {list(TERR.keys())}'}), 400
    cfg = TERR[rep]

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    scoped_skus = list(SOD_TRACKED_SKUS.keys())
    sku_phs = ','.join([ph] * len(scoped_skus))

    # Build territory WHERE clause
    where_parts = []
    params = []
    for pfx in cfg['prefixes']:
        where_parts.append(f"REPLACE(UPPER(s.postal),' ','') LIKE {ph}")
        params.append(f"{pfx}%")
    for city in cfg['cities']:
        where_parts.append(f"LOWER(TRIM(s.city)) = LOWER({ph})")
        params.append(city)
    territory_sql = "(" + " OR ".join(where_parts) + ")"

    # SQL: for each store in territory, count how many of our SKUs are listed
    # at status='L' at the latest snapshot. Filter to ≤max_skus.
    q = f"""
        WITH latest_per_sku AS (
            SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
            WHERE sku IN ({sku_phs}) GROUP BY sku
        ),
        store_listed AS (
            SELECT i.store_number, COUNT(DISTINCT i.sku) AS listed_count
            FROM sod_inventory i
            JOIN latest_per_sku l ON l.sku = i.sku AND l.d = i.snapshot_date
            WHERE i.sku IN ({sku_phs}) AND i.status = 'L'
            GROUP BY i.store_number
        )
        SELECT s.id, s.store_number, COALESCE(s.account,''),
               COALESCE(s.address,''), COALESCE(s.city,''), COALESCE(s.postal,''),
               COALESCE(s.priority,''), COALESCE(s.rep,''),
               COALESCE(s.lat,0), COALESCE(s.lng,0),
               COALESCE(s.manager_name,''), COALESCE(s.manager_phone, s.phone, ''),
               COALESCE(t.name,''), COALESCE(t.color,'#888'),
               COALESCE(sl.listed_count, 0) AS listed_count
        FROM stores s
        LEFT JOIN territories t ON t.id = s.territory_id
        LEFT JOIN store_listed sl ON sl.store_number = s.store_number
        WHERE {territory_sql}
          AND s.lat <> 0 AND s.lng <> 0
          AND COALESCE(sl.listed_count, 0) <= {ph}
        ORDER BY COALESCE(sl.listed_count, 0), s.city, s.postal
    """
    final_params = scoped_skus + scoped_skus + params + [max_skus]
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, final_params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, final_params).fetchall()

    stores = []
    for r in rows:
        stores.append({
            'id': r[0], 'store_number': r[1], 'account': r[2], 'address': r[3],
            'city': r[4], 'postal': (r[5] or '').strip().upper().replace(' ',''),
            'priority': r[6], 'rep_assigned': r[7],
            'lat': float(r[8]) if r[8] else 0,
            'lng': float(r[9]) if r[9] else 0,
            'manager_name': r[10], 'phone': r[11],
            'territory_name': r[12], 'territory_color': r[13],
            'skus_listed_count': int(r[14]),
        })

    # Cluster into days using FSA buckets + nearest-neighbor TSP
    from collections import defaultdict
    from math import radians, sin, cos, asin, sqrt
    by_fsa = defaultdict(list)
    for s in stores:
        fsa = s['postal'][:3] if s['postal'] else 'UNK'
        by_fsa[fsa].append(s)
    sorted_fsas = sorted(by_fsa.keys(), key=lambda k: (-len(by_fsa[k]), k))
    day_buckets = []
    cur_day = []
    for fsa in sorted_fsas:
        for s in by_fsa[fsa]:
            cur_day.append(s)
            if len(cur_day) >= max_per_day:
                day_buckets.append(cur_day)
                cur_day = []
    if cur_day:
        day_buckets.append(cur_day)

    def hv(a, b):
        if not (a.get('lat') and a.get('lng') and b.get('lat') and b.get('lng')):
            return 999
        lat1, lng1, lat2, lng2 = map(radians, (a['lat'], a['lng'], b['lat'], b['lng']))
        dlat = lat2 - lat1; dlng = lng2 - lng1
        h = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlng/2)**2
        return 2 * 6371 * asin(sqrt(h))

    today = _toronto_today()
    plan = []
    for d_idx, bucket in enumerate(day_buckets[:days]):
        if not bucket: continue
        with_gps = [s for s in bucket if s['lat'] and s['lng']]
        if with_gps:
            cx = sum(s['lat'] for s in with_gps) / len(with_gps)
            cy = sum(s['lng'] for s in with_gps) / len(with_gps)
            start = {'lat': cx, 'lng': cy}
        else:
            start = {'lat': 0, 'lng': 0}
        remaining = list(bucket)
        ordered = []
        cur_pt = start
        total_km = 0.0
        while remaining:
            nxt = min(remaining, key=lambda s: hv(cur_pt, s))
            d = hv(cur_pt, nxt)
            if ordered and d < 999:
                nxt['leg_km'] = round(d, 1)
                total_km += d
            else:
                nxt['leg_km'] = 0
            ordered.append(nxt)
            remaining.remove(nxt)
            cur_pt = nxt
        plan.append({
            'day': d_idx + 1,
            'date': (today + timedelta(days=d_idx)).isoformat(),
            'stops': len(ordered),
            'total_km_est': round(total_km, 1),
            'cluster_label': bucket[0].get('city',''),
            'stores': ordered,
        })

    return jsonify({
        'rep': rep,
        'territory': cfg,
        'max_skus_filter': max_skus,
        'total_priority_stores': len(stores),
        'zero_skus_listed': sum(1 for s in stores if s['skus_listed_count'] == 0),
        'one_sku_listed': sum(1 for s in stores if s['skus_listed_count'] == 1),
        'days_in_plan': len(plan),
        'stores_in_plan': sum(d['stops'] for d in plan),
        'plan': plan,
    })


@app.route('/api/crm/territory-plan', methods=['GET'])
@cached_response(ttl_seconds=300, key_args=('rep', 'days'))
def api_crm_territory_plan():
    """Build a 14-day route plan for a rep's territory.

    Two predefined territories:
      - rep=Namit  → GTA core (postal M*) — Toronto downtown + nearby
      - rep=Surya  → Ottawa region (postal K1, K2, K6, K7) + Kingston

    Algorithm:
      1. Pull all stores in territory (filtered by postal prefix)
      2. Cluster by city + postal FSA (first 3 chars) for fuel-efficient day groups
      3. Within each day cluster, run nearest-neighbor TSP from cluster centroid
      4. Cap each day at `max_per_day` stops (default 9)
      5. Return 14-day plan starting from today

    Query: ?rep=Namit&days=14&max_per_day=9
    """
    rep = (request.args.get('rep') or '').strip()
    days = max(1, min(int(request.args.get('days', 14)), 21))
    max_per_day = max(5, min(int(request.args.get('max_per_day', 9)), 14))

    # 5-REP TERRITORY MAP — covers all of Ontario, postal-prefix clustered
    # for fuel-efficient driving. Each rep's territory targets ~7-day cycle
    # at 8-10 stops/day (56-70 stores/week).
    TERRITORY = {
        'Namit': {
            # Toronto downtown/core/mid + North York + Etobicoke + Scarborough (all M*)
            # PLUS Woodbridge / Vaughan / Markham / Newmarket
            'name': 'Toronto + Vaughan + Markham + Newmarket',
            'postal_prefixes': ['M'],  # All Toronto (112 stores)
            'fallback_cities': ['Woodbridge', 'Vaughan', 'Maple', 'Markham',
                                'Stouffville', 'Newmarket', 'Aurora',
                                'Richmond Hill', 'Thornhill', 'Concord', 'Kleinburg'],
            'target_min': 120, 'target_max': 160,
        },
        'Ikshit': {
            'name': 'GTA West (Mississauga/Brampton/Halton)',
            'postal_prefixes': ['L5', 'L6', 'L7'],
            'fallback_cities': ['Burlington', 'Oakville', 'Milton', 'Georgetown',
                                'Mississauga', 'Brampton'],
            'target_min': 50, 'target_max': 80,
        },
        'Virat': {
            # GTA East + Durham (excluding Markham which is Namit's now)
            'name': 'GTA East + Durham (Pickering/Ajax/Whitby/Oshawa)',
            'postal_prefixes': ['L1'],
            'fallback_cities': ['Pickering', 'Ajax', 'Whitby', 'Oshawa',
                                'Bowmanville', 'Courtice', 'Clarington',
                                'Port Perry', 'Uxbridge'],
            'target_min': 30, 'target_max': 50,
        },
        'Surya': {
            # ALL stores in and around Ottawa — every K* postal code (city + rural)
            'name': 'Ottawa region + ALL K* (Eastern Ontario)',
            'postal_prefixes': ['K'],  # All K — Ottawa region + eastern rural
            'fallback_cities': ['Kingston', 'Brockville', 'Cornwall', 'Stittsville',
                                'Carleton Place', 'Gananoque', 'Rockland', 'Embrun',
                                'Kanata', 'Nepean', 'Orleans', 'Manotick',
                                'Almonte', 'Smiths Falls', 'Perth', 'Renfrew',
                                'Pembroke', 'Petawawa', 'Arnprior', 'Belleville',
                                'Trenton', 'Picton', 'Napanee'],
            'target_min': 80, 'target_max': 150,
        },
        'Neeraj': {
            'name': 'South-Western Ontario (Hamilton/Niagara/Kitchener/London)',
            'postal_prefixes': ['N'],  # 148 stores in N* postal
            'fallback_cities': ['Hamilton', 'Burlington', 'Niagara Falls',
                                'St. Catharines', 'Welland', 'Kitchener',
                                'Waterloo', 'Cambridge', 'Guelph', 'London',
                                'Brantford', 'Woodstock', 'Stratford', 'Sarnia',
                                'Windsor', 'Chatham'],
            'target_min': 100, 'target_max': 150,
        },
    }
    if rep not in TERRITORY:
        return jsonify({'error': f'No predefined territory for rep {rep!r}. '
                                  f'Defined: {list(TERRITORY.keys())}'}), 400

    cfg = TERRITORY[rep]
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # Pull all stores in territory
    prefixes = cfg['postal_prefixes']
    where_parts = []
    params = []
    for pfx in prefixes:
        where_parts.append(f"REPLACE(UPPER(s.postal),' ','') LIKE {ph}")
        params.append(f"{pfx}%")
    where_sql = "(" + " OR ".join(where_parts) + ")"

    # Optional fallback cities for tighter targeting
    fb_cities = cfg.get('fallback_cities', [])
    if fb_cities:
        city_phs = ','.join([ph] * len(fb_cities))
        where_sql = f"({where_sql} OR LOWER(TRIM(s.city)) IN ({city_phs}))"
        params.extend(c.lower().strip() for c in fb_cities)

    sql = (
        f"SELECT s.id, s.store_number, COALESCE(s.account,''), COALESCE(s.address,''), "
        f"COALESCE(s.city,''), COALESCE(s.postal,''), COALESCE(s.priority,''), "
        f"COALESCE(s.lat,0), COALESCE(s.lng,0), "
        f"COALESCE(s.manager_name,''), COALESCE(s.manager_phone, s.phone, ''), "
        f"COALESCE(s.rep,''), COALESCE(t.name,''), COALESCE(t.color,'#888'), "
        f"la.last_visit_at "
        f"FROM stores s "
        f"LEFT JOIN territories t ON t.id = s.territory_id "
        f"LEFT JOIN ("
        f"  SELECT store_id, MAX(created_at) AS last_visit_at "
        f"  FROM activities WHERE deleted_at IS NULL GROUP BY store_id"
        f") la ON la.store_id = s.id "
        f"WHERE {where_sql} "
        f"ORDER BY s.city, REPLACE(UPPER(s.postal),' ','')"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(sql, params).fetchall()

    stores_raw = [{
        'id': r[0], 'store_number': r[1], 'account': r[2], 'address': r[3],
        'city': r[4], 'postal': (r[5] or '').strip().upper().replace(' ',''),
        'priority': r[6],
        'lat': float(r[7]) if r[7] else 0, 'lng': float(r[8]) if r[8] else 0,
        'manager_name': r[9], 'phone': r[10],
        'rep_assigned': r[11], 'territory_name': r[12], 'territory_color': r[13],
        'last_visit_at': str(r[14]) if r[14] else None,
    } for r in rows]

    # Cluster by FSA (first 3 chars of postal) — fuel-efficient day groups
    from collections import defaultdict
    by_fsa = defaultdict(list)
    for s in stores_raw:
        fsa = s['postal'][:3] if s['postal'] else 'UNK'
        by_fsa[fsa].append(s)

    # Order FSAs by store count desc (visit busiest neighborhoods first)
    sorted_fsas = sorted(by_fsa.keys(), key=lambda k: (-len(by_fsa[k]), k))

    # Pack into days: max_per_day per day, prefer keeping FSAs together
    day_buckets = []
    current_day = []
    for fsa in sorted_fsas:
        for s in by_fsa[fsa]:
            current_day.append(s)
            if len(current_day) >= max_per_day:
                day_buckets.append(current_day)
                current_day = []
    if current_day:
        day_buckets.append(current_day)

    # Within each day bucket, run nearest-neighbor TSP from cluster centroid
    # to minimize travel. Use haversine distance.
    def hv(a, b):
        from math import radians, sin, cos, asin, sqrt
        if not (a['lat'] and a['lng'] and b['lat'] and b['lng']):
            return 999  # missing GPS — push to end
        lat1, lng1, lat2, lng2 = map(radians, (a['lat'], a['lng'], b['lat'], b['lng']))
        dlat = lat2 - lat1; dlng = lng2 - lng1
        h = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlng/2)**2
        return 2 * 6371 * asin(sqrt(h))

    optimized_days = []
    today = _toronto_today()
    for day_idx, bucket in enumerate(day_buckets[:days]):
        if not bucket:
            continue
        # Centroid
        with_gps = [s for s in bucket if s['lat'] and s['lng']]
        if with_gps:
            cx = sum(s['lat'] for s in with_gps) / len(with_gps)
            cy = sum(s['lng'] for s in with_gps) / len(with_gps)
            start = {'lat': cx, 'lng': cy}
        else:
            start = {'lat': 0, 'lng': 0}
        # Nearest-neighbor from start
        remaining = list(bucket)
        ordered = []
        cur = start
        while remaining:
            nxt = min(remaining, key=lambda s: hv(cur, s))
            ordered.append(nxt)
            remaining.remove(nxt)
            cur = nxt
        # Compute leg distances + total
        total_km = 0.0
        for i in range(1, len(ordered)):
            d = hv(ordered[i-1], ordered[i])
            ordered[i]['leg_km'] = round(d, 1) if d < 999 else None
            if d < 999:
                total_km += d
        ordered[0]['leg_km'] = 0
        plan_date = (today + timedelta(days=day_idx)).isoformat()
        optimized_days.append({
            'day': day_idx + 1,
            'date': plan_date,
            'stops': len(ordered),
            'total_km_est': round(total_km, 1),
            'cluster_label': bucket[0].get('city', ''),
            'stores': ordered,
        })

    return jsonify({
        'rep': rep,
        'territory_name': cfg['name'],
        'days_in_plan': len(optimized_days),
        'total_stores_in_territory': len(stores_raw),
        'stores_in_plan': sum(d['stops'] for d in optimized_days),
        'max_per_day': max_per_day,
        'plan': optimized_days,
    })


@app.route('/api/crm/rep-performance', methods=['GET'])
@cached_response(ttl_seconds=60, key_args=('days',))
def api_crm_rep_performance():
    """Per-rep KPI scoreboard. ?days=7|30|90 (default 30).

    Returns for each rep in the official roster:
      - activities: total + breakdown by type (visit/call/tasting/sample/email)
      - stores_covered: distinct stores visited in window
      - days_active: distinct days with at least 1 activity
      - deals: total / open / won (listed) / lost
      - listings_won: deals reached 'listed' stage in window
      - tasting_to_listing_rate: %
      - last_activity_at + last_activity_store
    Plus the 4 official-roster reps even if they have 0 activity (so manager
    sees the full team, not just the active ones).
    """
    days = int(request.args.get('days', 30))
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    today = _toronto_today()
    since = (today - timedelta(days=days)).isoformat()

    # Always include the official roster, even with 0 activity
    OFFICIAL_REPS = ['Ikshit', 'Virat', 'Namit', 'Surya', 'Neeraj']

    out = {rep: {
        'rep': rep,
        'activities_total': 0,
        'activities_by_type': {},
        'stores_covered': 0,
        'days_active': 0,
        'deals_open': 0,
        'deals_listed': 0,
        'deals_lost': 0,
        'listings_won_in_window': 0,
        'last_activity_at': None,
        'last_activity_store': None,
        'last_activity_type': None,
    } for rep in OFFICIAL_REPS}

    # Activity totals + breakdown
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "SELECT COALESCE(rep, '') AS rep, COALESCE(activity_type,'') AS at, COUNT(*) "
            "FROM activities WHERE deleted_at IS NULL AND COALESCE(visit_date, created_at::date) >= %s "
            "GROUP BY rep, at",
            (since,),
        )
        for r in cur.fetchall():
            rep = (r[0] or '').strip()
            at = r[1] or 'unknown'
            cnt = int(r[2])
            if not rep:
                continue
            entry = out.setdefault(rep, dict(out.get(OFFICIAL_REPS[0], {}), rep=rep))
            entry['activities_total'] = entry.get('activities_total', 0) + cnt
            entry['activities_by_type'] = dict(entry.get('activities_by_type', {}))
            entry['activities_by_type'][at] = entry['activities_by_type'].get(at, 0) + cnt
        cur.close()
    else:
        for r in db.execute(
            "SELECT COALESCE(rep,'') AS rep, COALESCE(activity_type,'') AS at, COUNT(*) "
            "FROM activities WHERE deleted_at IS NULL AND COALESCE(visit_date, DATE(created_at)) >= ? "
            "GROUP BY rep, at",
            (since,),
        ).fetchall():
            rep = (r[0] or '').strip()
            at = r[1] or 'unknown'
            cnt = int(r[2])
            if not rep:
                continue
            entry = out.setdefault(rep, {})
            entry.setdefault('rep', rep)
            entry['activities_total'] = entry.get('activities_total', 0) + cnt
            entry['activities_by_type'] = entry.get('activities_by_type') or {}
            entry['activities_by_type'][at] = entry['activities_by_type'].get(at, 0) + cnt

    # Distinct stores visited + days active + last activity
    date_expr = "created_at::date" if USE_POSTGRES else "DATE(created_at)"
    sql = (
        f"SELECT COALESCE(rep,'') AS rep, COUNT(DISTINCT store_id), "
        f"COUNT(DISTINCT COALESCE(visit_date, {date_expr})), MAX(created_at) "
        f"FROM activities WHERE deleted_at IS NULL "
        f"AND COALESCE(visit_date, {date_expr}) >= {ph} GROUP BY rep"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql, (since,))
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(sql, (since,)).fetchall()
    for r in rows:
        rep = (r[0] or '').strip()
        if not rep or rep not in out:
            continue
        out[rep]['stores_covered'] = int(r[1] or 0)
        out[rep]['days_active'] = int(r[2] or 0)
        out[rep]['last_activity_at'] = str(r[3]) if r[3] else None

    # Last activity store/type
    for rep in list(out.keys()):
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(
                    "SELECT s.store_number, a.activity_type FROM activities a "
                    "LEFT JOIN stores s ON s.id = a.store_id "
                    "WHERE COALESCE(a.rep,'') = %s AND a.deleted_at IS NULL "
                    "ORDER BY a.created_at DESC LIMIT 1",
                    (rep,),
                )
                lr = cur.fetchone()
                cur.close()
            else:
                lr = db.execute(
                    "SELECT s.store_number, a.activity_type FROM activities a "
                    "LEFT JOIN stores s ON s.id = a.store_id "
                    "WHERE COALESCE(a.rep,'') = ? AND a.deleted_at IS NULL "
                    "ORDER BY a.created_at DESC LIMIT 1",
                    (rep,),
                ).fetchone()
            if lr:
                out[rep]['last_activity_store'] = lr[0]
                out[rep]['last_activity_type'] = lr[1]
        except Exception:
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass

    # Deal counts
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "SELECT COALESCE(owner_rep,'') AS rep, "
            "  SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN stage='listed' THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN stage='lost' THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN stage='listed' AND closed_at IS NOT NULL AND closed_at >= %s THEN 1 ELSE 0 END) "
            "FROM deals GROUP BY owner_rep",
            (since,),
        )
        for r in cur.fetchall():
            rep = (r[0] or '').strip()
            if not rep or rep not in out:
                continue
            out[rep]['deals_open'] = int(r[1] or 0)
            out[rep]['deals_listed'] = int(r[2] or 0)
            out[rep]['deals_lost'] = int(r[3] or 0)
            out[rep]['listings_won_in_window'] = int(r[4] or 0)
        cur.close()
    else:
        for r in db.execute(
            "SELECT COALESCE(owner_rep,'') AS rep, "
            "  SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN stage='listed' THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN stage='lost' THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN stage='listed' AND closed_at IS NOT NULL AND closed_at >= ? THEN 1 ELSE 0 END) "
            "FROM deals GROUP BY owner_rep",
            (since,),
        ).fetchall():
            rep = (r[0] or '').strip()
            if not rep or rep not in out:
                continue
            out[rep]['deals_open'] = int(r[1] or 0)
            out[rep]['deals_listed'] = int(r[2] or 0)
            out[rep]['deals_lost'] = int(r[3] or 0)
            out[rep]['listings_won_in_window'] = int(r[4] or 0)

    # Tasting → listing conversion rate per rep
    for rep, e in out.items():
        tastings = (e.get('activities_by_type') or {}).get('tasting', 0) + \
                   (e.get('activities_by_type') or {}).get('sample_drop', 0)
        wins = e.get('listings_won_in_window', 0)
        e['tasting_to_listing_rate_pct'] = round(wins * 100 / tastings, 1) if tastings > 0 else None

    return jsonify({
        'window_days': days,
        'since': since,
        'reps': list(out.values()),
        'totals': {
            'activities': sum(e.get('activities_total', 0) for e in out.values()),
            'stores_covered': sum(e.get('stores_covered', 0) for e in out.values()),
            'listings_won': sum(e.get('listings_won_in_window', 0) for e in out.values()),
            'open_deals': sum(e.get('deals_open', 0) for e in out.values()),
        },
    })


@app.route('/api/crm/daily-log', methods=['GET'])
@cached_response(ttl_seconds=30, key_args=('date', 'days'))
def api_crm_daily_log():
    """Daily activity log — every activity logged on a given date.

    ?date=YYYY-MM-DD (default today)  ?days=1 (default 1, max 14)

    Used by the /daily-log page so a manager can see exactly what happened
    each day, who did it, and where. Sourced from activities + event_log
    (audit trail of state changes), unioned and sorted by timestamp DESC.
    """
    target_date = (request.args.get('date') or _toronto_today().isoformat()).strip()
    days = max(1, min(int(request.args.get('days', 1)), 14))
    try:
        end = datetime.strptime(target_date, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'date must be YYYY-MM-DD'}), 400
    start = (end - timedelta(days=days - 1)).isoformat()
    end_iso = end.isoformat()

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    date_expr = "a.created_at::date" if USE_POSTGRES else "DATE(a.created_at)"
    sql = (
        f"SELECT a.id, a.created_at, COALESCE(a.visit_date, {date_expr}), "
        f"COALESCE(a.rep,''), COALESCE(a.activity_type,''), "
        f"COALESCE(a.notes,''), COALESCE(a.outcome,''), "
        f"COALESCE(a.duration_minutes,0), COALESCE(a.rating,0), "
        f"s.store_number, COALESCE(s.account,''), "
        f"COALESCE(s.city,''), COALESCE(s.address,''), "
        f"COALESCE(t.name,'Unassigned'), COALESCE(t.color,'#888') "
        f"FROM activities a "
        f"LEFT JOIN stores s ON s.id = a.store_id "
        f"LEFT JOIN territories t ON t.id = s.territory_id "
        f"WHERE a.deleted_at IS NULL "
        f"AND COALESCE(a.visit_date, {date_expr}) BETWEEN {ph} AND {ph} "
        f"ORDER BY a.created_at DESC LIMIT 500"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql, (start, end_iso))
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(sql, (start, end_iso)).fetchall()

    activities = [{
        'id': r[0],
        'created_at': str(r[1]) if r[1] else None,
        'visit_date': str(r[2]) if r[2] else None,
        'rep': r[3],
        'activity_type': r[4],
        'notes': r[5],
        'outcome': r[6],
        'duration_minutes': int(r[7]),
        'rating': int(r[8]),
        'store_number': r[9],
        'account': r[10],
        'city': r[11],
        'address': r[12],
        'territory_name': r[13],
        'territory_color': r[14],
    } for r in rows]

    # Per-rep summary
    by_rep = {}
    for a in activities:
        rep = a['rep'] or '(unassigned)'
        b = by_rep.setdefault(rep, {'rep': rep, 'count': 0, 'by_type': {}, 'stores': set()})
        b['count'] += 1
        b['by_type'][a['activity_type']] = b['by_type'].get(a['activity_type'], 0) + 1
        if a['store_number']:
            b['stores'].add(a['store_number'])
    by_rep_list = [{
        'rep': v['rep'], 'count': v['count'],
        'by_type': v['by_type'], 'stores_visited': len(v['stores']),
    } for v in by_rep.values()]
    by_rep_list.sort(key=lambda x: -x['count'])

    return jsonify({
        'window': {'start': start, 'end': end_iso, 'days': days},
        'count': len(activities),
        'activities': activities,
        'by_rep': by_rep_list,
    })


@app.route('/api/crm/manager-dashboard', methods=['GET'])
@cached_response(ttl_seconds=60, key_args=())
def api_crm_manager_dashboard():
    """Single-call aggregate for the /manager page.

    Returns per-rep:
      - stores_assigned (territory or stores.rep)
      - activities logged (last 30d) — by type
      - new listings won (deals stage='listed' OR new SOD listings in their stores last 60d)
      - delistings in their stores (last 30d)
      - new stores added in their territory (last 60d)
      - gap_count (stores in their territory NOT carrying any tracked SKU)

    Plus territories summary + global KPIs.
    """
    days_activity = int(request.args.get('days_activity', 30))
    days_listings = int(request.args.get('days_listings', 60))
    today_d = _toronto_today()
    activity_since = (today_d - timedelta(days=days_activity)).isoformat()
    listings_since = (today_d - timedelta(days=days_listings)).isoformat()

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    tracked = list(SOD_TRACKED_SKUS.keys())
    if not tracked:
        return jsonify({'reps': [], 'territories': [], 'totals': {}})
    phs = ','.join([ph] * len(tracked))

    # 1) Reps from stores table (deduped + trimmed)
    # Union: any rep that has stores OR activities OR deals shows up. This
    # ensures reps like Namit show up in the manager dashboard even before
    # they've been assigned to stores via the territory builder.
    reps_query = (
        "SELECT MIN(rep) AS rep, MAX(store_count) AS store_count FROM ("
        "  SELECT MIN(TRIM(rep)) AS rep, COUNT(*) AS store_count FROM stores "
        "    WHERE rep IS NOT NULL AND TRIM(rep) <> '' GROUP BY LOWER(TRIM(rep))"
        "  UNION ALL"
        "  SELECT MIN(TRIM(rep)) AS rep, 0 AS store_count FROM activities "
        "    WHERE rep IS NOT NULL AND TRIM(rep) <> '' "
        "    AND deleted_at IS NULL GROUP BY LOWER(TRIM(rep))"
        "  UNION ALL"
        "  SELECT MIN(TRIM(owner_rep)) AS rep, 0 AS store_count FROM deals "
        "    WHERE owner_rep IS NOT NULL AND TRIM(owner_rep) <> '' "
        "    GROUP BY LOWER(TRIM(owner_rep))"
        ") u "
        "GROUP BY LOWER(TRIM(rep)) "
        "ORDER BY MAX(store_count) DESC, MIN(rep) ASC"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(reps_query)
        rep_rows = cur.fetchall()
        cur.close()
    else:
        rep_rows = db.execute(reps_query).fetchall()

    out_reps = []
    for rr in rep_rows:
        rep_name, store_count = rr[0], int(rr[1] or 0)

        # 2) Activities logged in the last N days
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT COUNT(*), "
                "SUM(CASE WHEN LOWER(activity_type) LIKE '%%visit%%' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN LOWER(activity_type) IN ('tasting','sample_drop') THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN LOWER(activity_type) IN ('call','email') THEN 1 ELSE 0 END) "
                "FROM activities WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s)) "
                "AND created_at >= %s "
                "AND deleted_at IS NULL",
                (rep_name, activity_since),
            )
            ar = cur.fetchone()
            cur.close()
        else:
            ar = db.execute(
                "SELECT COUNT(*), "
                "SUM(CASE WHEN LOWER(activity_type) LIKE '%visit%' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN LOWER(activity_type) IN ('tasting','sample_drop') THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN LOWER(activity_type) IN ('call','email') THEN 1 ELSE 0 END) "
                "FROM activities WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?)) "
                "AND datetime(created_at) >= ? "
                "AND deleted_at IS NULL",
                (rep_name, activity_since),
            ).fetchone()
        activities_total = int((ar[0] or 0) if ar else 0)
        visits = int((ar[1] or 0) if ar else 0)
        tastings = int((ar[2] or 0) if ar else 0)
        outreach = int((ar[3] or 0) if ar else 0)

        # 3) New listings won — deals closed-as-listed by this rep
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM deals WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM(%s)) "
                "AND stage='listed' AND closed_at >= %s",
                (rep_name, listings_since),
            )
            listings_won = int(cur.fetchone()[0] or 0)
            cur.close()
        else:
            listings_won = int(db.execute(
                "SELECT COUNT(*) FROM deals WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM(?)) "
                "AND stage='listed' AND datetime(closed_at) >= ?",
                (rep_name, listings_since),
            ).fetchone()[0] or 0)

        # 4) New stores added in this rep's territory in last N days
        # (stores covered by this rep where one of our SKUs got NEW_LISTING/RELISTED)
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"""SELECT COUNT(DISTINCT (c.sku, c.store_number))
                    FROM sod_store_sku_changes c
                    JOIN stores s ON s.store_number = c.store_number
                    WHERE c.sku IN ({phs})
                      AND c.change_type IN ('NEW_LISTING','RELISTED')
                      AND c.change_date >= %s
                      AND LOWER(TRIM(s.rep)) = LOWER(TRIM(%s))""",
                tracked + [listings_since, rep_name],
            )
            new_stores = int(cur.fetchone()[0] or 0)
            cur.close()
        else:
            new_stores = int(db.execute(
                f"""SELECT COUNT(*) FROM (
                      SELECT DISTINCT c.sku, c.store_number
                      FROM sod_store_sku_changes c
                      JOIN stores s ON s.store_number = c.store_number
                      WHERE c.sku IN ({phs})
                        AND c.change_type IN ('NEW_LISTING','RELISTED')
                        AND c.change_date >= ?
                        AND LOWER(TRIM(s.rep)) = LOWER(TRIM(?)))""",
                tracked + [listings_since, rep_name],
            ).fetchone()[0] or 0)

        # 5) Delistings in their stores last N days
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"""SELECT COUNT(DISTINCT (c.sku, c.store_number))
                    FROM sod_store_sku_changes c
                    JOIN stores s ON s.store_number = c.store_number
                    WHERE c.sku IN ({phs})
                      AND c.change_type IN ('DELISTED','DROPPED')
                      AND c.change_date >= %s
                      AND LOWER(TRIM(s.rep)) = LOWER(TRIM(%s))""",
                tracked + [listings_since, rep_name],
            )
            delistings = int(cur.fetchone()[0] or 0)
            cur.close()
        else:
            delistings = int(db.execute(
                f"""SELECT COUNT(*) FROM (
                      SELECT DISTINCT c.sku, c.store_number
                      FROM sod_store_sku_changes c
                      JOIN stores s ON s.store_number = c.store_number
                      WHERE c.sku IN ({phs})
                        AND c.change_type IN ('DELISTED','DROPPED')
                        AND c.change_date >= ?
                        AND LOWER(TRIM(s.rep)) = LOWER(TRIM(?)))""",
                tracked + [listings_since, rep_name],
            ).fetchone()[0] or 0)

        # 6) Gap count — stores in their book NOT carrying ANY tracked SKU at status='L'
        # latest snapshot per sku, count distinct stores assigned to this rep that
        # have ZERO listed tracked SKUs.
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"""WITH rep_stores AS (
                        SELECT id, store_number FROM stores
                        WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s))
                    ),
                    listed AS (
                        SELECT DISTINCT i.store_number FROM sod_inventory i
                        WHERE i.sku IN ({phs}) AND i.status='L'
                          AND i.snapshot_date = (SELECT MAX(snapshot_date)
                                                 FROM sod_inventory i2
                                                 WHERE i2.sku = i.sku)
                    )
                    SELECT COUNT(*) FROM rep_stores rs
                    WHERE rs.store_number NOT IN (SELECT store_number FROM listed)""",
                [rep_name] + tracked,
            )
            gap_count = int(cur.fetchone()[0] or 0)
            cur.close()
        else:
            gap_count = int(db.execute(
                f"""WITH rep_stores AS (
                        SELECT id, store_number FROM stores
                        WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?))
                    ),
                    listed AS (
                        SELECT DISTINCT i.store_number FROM sod_inventory i
                        WHERE i.sku IN ({phs}) AND i.status='L'
                          AND i.snapshot_date = (SELECT MAX(snapshot_date)
                                                 FROM sod_inventory i2
                                                 WHERE i2.sku = i.sku)
                    )
                    SELECT COUNT(*) FROM rep_stores rs
                    WHERE rs.store_number NOT IN (SELECT store_number FROM listed)""",
                [rep_name] + tracked,
            ).fetchone()[0] or 0)

        # 7) Quota lookup for current quarter
        cq = _current_quarter()
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT target_activities, target_visits, target_new_listings "
                "FROM rep_quotas WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s)) AND quarter=%s LIMIT 1",
                (rep_name, cq),
            )
            qrow = cur.fetchone()
            cur.close()
        else:
            qrow = db.execute(
                "SELECT target_activities, target_visits, target_new_listings "
                "FROM rep_quotas WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?)) AND quarter=? LIMIT 1",
                (rep_name, cq),
            ).fetchone()
        quota_acts = int((qrow[0] if qrow else 0) or 0)
        quota_visits = int((qrow[1] if qrow else 0) or 0)
        quota_listings = int((qrow[2] if qrow else 0) or 0)

        out_reps.append({
            'rep': rep_name,
            'store_count': store_count,
            'gap_count': gap_count,
            'activities_30d': activities_total,
            'visits_30d': visits,
            'tastings_30d': tastings,
            'outreach_30d': outreach,
            'listings_won_60d': listings_won,
            'new_stores_60d': new_stores,
            'delistings_60d': delistings,
            'quota_activities': quota_acts,
            'quota_visits': quota_visits,
            'quota_new_listings': quota_listings,
            'pct_quota_activities': round(100 * activities_total / quota_acts, 1) if quota_acts > 0 else None,
            'pct_quota_visits': round(100 * visits / quota_visits, 1) if quota_visits > 0 else None,
            'pct_quota_listings': round(100 * listings_won / quota_listings, 1) if quota_listings > 0 else None,
            'gap_pct': round(100 * gap_count / store_count, 1) if store_count > 0 else None,
        })

    # 8) Territory rollup — store + rep + listed-SKU coverage per territory
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            """SELECT t.id, t.code, t.name, t.region, t.color, t.rep_name,
                      COUNT(s.id) AS store_count
               FROM territories t
               LEFT JOIN stores s ON s.territory_id = t.id
               GROUP BY t.id, t.code, t.name, t.region, t.color, t.rep_name
               ORDER BY t.region, t.name""")
        terr_rows = cur.fetchall()
        cur.close()
    else:
        terr_rows = db.execute(
            """SELECT t.id, t.code, t.name, t.region, t.color, t.rep_name,
                      COUNT(s.id) AS store_count
               FROM territories t
               LEFT JOIN stores s ON s.territory_id = t.id
               GROUP BY t.id, t.code, t.name, t.region, t.color, t.rep_name
               ORDER BY t.region, t.name""").fetchall()
    out_terr = [{
        'id': r[0], 'code': r[1], 'name': r[2], 'region': r[3] or '',
        'color': r[4] or '#888', 'rep_name': r[5] or '',
        'store_count': int(r[6] or 0),
    } for r in terr_rows]

    # 9) Global totals
    totals = {
        'reps': len(out_reps),
        'territories': len(out_terr),
        'total_stores': sum(r['store_count'] for r in out_reps),
        'total_listings_won_60d': sum(r['listings_won_60d'] for r in out_reps),
        'total_new_stores_60d': sum(r['new_stores_60d'] for r in out_reps),
        'total_delistings_60d': sum(r['delistings_60d'] for r in out_reps),
        'total_activities_30d': sum(r['activities_30d'] for r in out_reps),
        'total_gap': sum(r['gap_count'] for r in out_reps),
    }

    return jsonify({
        'days_activity': days_activity,
        'days_listings': days_listings,
        'reps': out_reps,
        'territories': out_terr,
        'totals': totals,
        'freshness': _sod_freshness(),
    })


REP_ROSTER_DEFAULT = ['Ikshit', 'Virat', 'Namit', 'Surya', 'Neeraj']


@app.route('/api/crm/admin/roster', methods=['GET'])
def api_crm_admin_roster_get():
    """Return the configured rep roster (4 official reps for Anu)."""
    return jsonify({
        'roster': REP_ROSTER_DEFAULT,
        'placeholder_for_unassigned': 'New Rep 1',
    })


@app.route('/api/crm/admin/set-roster', methods=['POST'])
@require_admin_token
def api_crm_admin_set_roster():
    """Normalize the rep roster across stores + territories.

    Sets stores.rep = NULL for any rep NOT in the official roster.
    Sets territories.rep_name = 'New Rep 1' for any territory that has no rep
    after normalization (so the manager UI shows a placeholder).

    Body (optional): {roster: ['Neeraj', 'Virat', ...]} to override defaults.
    """
    body = request.get_json(silent=True) or {}
    roster = body.get('roster') or REP_ROSTER_DEFAULT
    placeholder = body.get('placeholder', 'New Rep 1')
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # 1) Clear stores.rep for any name not matching the roster (case-insensitive trim).
    if USE_POSTGRES:
        cur = db.cursor()
        roster_lower = [r.lower().strip() for r in roster]
        ph_list = ','.join([ph] * len(roster_lower))
        cur.execute(
            f"UPDATE stores SET rep = '' "
            f"WHERE rep IS NOT NULL AND TRIM(rep) <> '' "
            f"AND LOWER(TRIM(rep)) NOT IN ({ph_list})",
            roster_lower,
        )
        cleared_count = cur.rowcount

        # 2) Reset territories.rep_name → 'New Rep 1' if not in roster
        cur.execute(
            f"UPDATE territories SET rep_name = %s "
            f"WHERE LOWER(TRIM(COALESCE(rep_name,''))) NOT IN ({ph_list}) OR rep_name IS NULL",
            [placeholder] + roster_lower,
        )
        terr_reset = cur.rowcount
        db.commit()
        cur.close()
    else:
        roster_lower = [r.lower().strip() for r in roster]
        ph_list = ','.join(['?'] * len(roster_lower))
        c1 = db.execute(
            f"UPDATE stores SET rep = '' "
            f"WHERE rep IS NOT NULL AND TRIM(rep) <> '' "
            f"AND LOWER(TRIM(rep)) NOT IN ({ph_list})",
            roster_lower,
        )
        cleared_count = c1.rowcount
        c2 = db.execute(
            f"UPDATE territories SET rep_name = ? "
            f"WHERE LOWER(TRIM(COALESCE(rep_name,''))) NOT IN ({ph_list}) OR rep_name IS NULL",
            [placeholder] + roster_lower,
        )
        terr_reset = c2.rowcount
        db.commit()

    try:
        _log_event('roster_set', 'admin', None, '',
                   {'roster': roster, 'cleared_stores_count': cleared_count,
                    'territories_reset': terr_reset, 'placeholder': placeholder})
    except Exception:
        pass

    return jsonify({
        'status': 'ok',
        'roster': roster,
        'cleared_stores_count': cleared_count,
        'territories_reset_to_placeholder': terr_reset,
        'placeholder': placeholder,
    })


@app.route('/api/crm/admin/bulk-reassign-rep', methods=['POST'])
@require_admin_token
def api_crm_admin_bulk_reassign_rep():
    """Move every store from one rep to another in one shot.

    Body: {from_rep: 'Tyler Weber', to_rep: 'Neeraj'}
    Useful for rolling new hires into the system.
    """
    body = request.get_json(silent=True) or {}
    from_rep = (body.get('from_rep') or '').strip()
    to_rep = (body.get('to_rep') or '').strip()
    if not to_rep:
        return jsonify({'error': 'to_rep required'}), 400
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    if from_rep:
        # Move stores from one rep to another
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"UPDATE stores SET rep = %s WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s))",
                (to_rep, from_rep),
            )
            n = cur.rowcount
            db.commit()
            cur.close()
        else:
            c = db.execute(
                f"UPDATE stores SET rep = ? WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?))",
                (to_rep, from_rep),
            )
            n = c.rowcount
            db.commit()
    else:
        # Move all stores with no rep to the target rep
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                f"UPDATE stores SET rep = %s WHERE rep IS NULL OR TRIM(rep) = ''",
                (to_rep,),
            )
            n = cur.rowcount
            db.commit()
            cur.close()
        else:
            c = db.execute(
                f"UPDATE stores SET rep = ? WHERE rep IS NULL OR TRIM(rep) = ''",
                (to_rep,),
            )
            n = c.rowcount
            db.commit()
    try:
        _log_event('rep_bulk_reassign', 'admin', None, '',
                   {'from_rep': from_rep or '(empty)', 'to_rep': to_rep, 'count': n})
    except Exception:
        pass
    return jsonify({'status': 'ok', 'reassigned': n, 'to_rep': to_rep})


@app.route('/api/crm/territories/<int:territory_id>/assign-stores', methods=['POST'])
def api_crm_territory_assign_stores(territory_id):
    """Bulk-assign stores to a territory + optionally set the rep_name on the territory.

    Body: {store_numbers: [1, 2, 3, ...], rep_name?: 'Ikshit Sharma'}
    """
    body = request.get_json(silent=True) or {}
    store_numbers = body.get('store_numbers') or []
    rep_name = (body.get('rep_name') or '').strip()
    if not store_numbers:
        return jsonify({'error': 'store_numbers required'}), 400

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    phs = ','.join([ph] * len(store_numbers))

    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            f"UPDATE stores SET territory_id = %s WHERE store_number IN ({phs})",
            [territory_id] + list(store_numbers),
        )
        # Optionally also set the rep
        if rep_name:
            cur.execute(
                f"UPDATE stores SET rep = %s WHERE store_number IN ({phs})",
                [rep_name] + list(store_numbers),
            )
            cur.execute(
                "UPDATE territories SET rep_name = %s WHERE id = %s",
                (rep_name, territory_id),
            )
        db.commit()
        cur.close()
    else:
        db.execute(
            f"UPDATE stores SET territory_id = ? WHERE store_number IN ({phs})",
            [territory_id] + list(store_numbers),
        )
        if rep_name:
            db.execute(
                f"UPDATE stores SET rep = ? WHERE store_number IN ({phs})",
                [rep_name] + list(store_numbers),
            )
            db.execute(
                "UPDATE territories SET rep_name = ? WHERE id = ?",
                (rep_name, territory_id),
            )
        db.commit()

    try:
        _log_event('territory_assigned', 'territory', str(territory_id), rep_name,
                   {'store_count': len(store_numbers), 'rep_name': rep_name})
    except Exception:
        pass
    return jsonify({'status': 'ok', 'assigned': len(store_numbers), 'territory_id': territory_id, 'rep': rep_name or None})


@app.route('/api/crm/quotas', methods=['GET'])
def api_crm_quotas_list():
    """List all quotas with live progress."""
    rep = request.args.get('rep', '').strip()
    quarter = request.args.get('quarter', '').strip() or _current_quarter()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = [f'quarter = {ph}']
    params = [quarter]
    if rep:
        where.append(f'LOWER(TRIM(rep)) = LOWER(TRIM({ph}))')
        params.append(rep)
    q = (f"SELECT id, rep, quarter, target_activities, target_visits, "
         f"target_new_listings, target_units, target_revenue, notes "
         f"FROM rep_quotas WHERE {' AND '.join(where)}")
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()

    # Parse quarter -> date range
    try:
        y, qn = quarter.split('-Q')
        qstart_month = (int(qn) - 1) * 3 + 1
        qstart = datetime(int(y), qstart_month, 1).date()
        if qstart_month + 3 > 12:
            qend = datetime(int(y) + 1, 1, 1).date() - timedelta(days=1)
        else:
            qend = datetime(int(y), qstart_month + 3, 1).date() - timedelta(days=1)
    except Exception:
        qstart = _toronto_today().replace(day=1)
        qend = _toronto_today()

    out = []
    for r in rows:
        rep_name = r[1]
        # Activities count
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT COUNT(*), COUNT(CASE WHEN LOWER(activity_type) LIKE '%%visit%%' THEN 1 END) "
                "FROM activities WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s)) "
                "AND created_at BETWEEN %s AND %s",
                (rep_name, qstart, qend),
            )
            ar = cur.fetchone()
            cur.close()
        else:
            ar = db.execute(
                "SELECT COUNT(*), COUNT(CASE WHEN LOWER(activity_type) LIKE '%visit%' THEN 1 END) "
                "FROM activities WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?)) "
                "AND datetime(created_at) BETWEEN ? AND ?",
                (rep_name, qstart.isoformat(), qend.isoformat() + ' 23:59:59'),
            ).fetchone()
        activities_done = int(ar[0] or 0) if ar else 0
        visits_done = int(ar[1] or 0) if ar else 0

        # New listings = deals that moved to 'listed' this quarter
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM deals WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM(%s)) "
                "AND stage='listed' AND closed_at BETWEEN %s AND %s",
                (rep_name, qstart, qend),
            )
            new_listings = int(cur.fetchone()[0] or 0)
            cur.close()
        else:
            new_listings = int(db.execute(
                "SELECT COUNT(*) FROM deals WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM(?)) "
                "AND stage='listed' AND datetime(closed_at) BETWEEN ? AND ?",
                (rep_name, qstart.isoformat(), qend.isoformat() + ' 23:59:59'),
            ).fetchone()[0] or 0)

        def pct(done, target):
            return round(100 * done / target, 1) if target > 0 else None

        out.append({
            'id': r[0], 'rep': rep_name, 'quarter': r[2],
            'period_start': qstart.isoformat(), 'period_end': qend.isoformat(),
            'targets': {
                'activities': r[3], 'visits': r[4],
                'new_listings': r[5], 'units': r[6],
                'revenue': float(r[7] or 0),
            },
            'achieved': {
                'activities': activities_done, 'visits': visits_done,
                'new_listings': new_listings,
                'units': 0, 'revenue': 0,  # require sales data integration
            },
            'pct': {
                'activities': pct(activities_done, r[3]),
                'visits': pct(visits_done, r[4]),
                'new_listings': pct(new_listings, r[5]),
            },
            'notes': r[8],
        })
    return jsonify({'quarter': quarter, 'quotas': out})


@app.route('/api/crm/quotas', methods=['POST'])
def api_crm_quotas_upsert():
    d = request.get_json() or {}
    rep = (d.get('rep') or '').strip()
    quarter = (d.get('quarter') or _current_quarter()).strip()
    if not rep:
        return jsonify({'error': 'rep required'}), 400
    db = get_db()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            """INSERT INTO rep_quotas
               (rep, quarter, target_activities, target_visits, target_new_listings,
                target_units, target_revenue, notes)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (rep, quarter) DO UPDATE SET
                   target_activities=EXCLUDED.target_activities,
                   target_visits=EXCLUDED.target_visits,
                   target_new_listings=EXCLUDED.target_new_listings,
                   target_units=EXCLUDED.target_units,
                   target_revenue=EXCLUDED.target_revenue,
                   notes=EXCLUDED.notes""",
            (rep, quarter,
             int(d.get('target_activities') or 0),
             int(d.get('target_visits') or 0),
             int(d.get('target_new_listings') or 0),
             int(d.get('target_units') or 0),
             float(d.get('target_revenue') or 0),
             d.get('notes', '')),
        )
        db.commit()
        cur.close()
    else:
        db.execute(
            """INSERT INTO rep_quotas
               (rep, quarter, target_activities, target_visits, target_new_listings,
                target_units, target_revenue, notes)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(rep, quarter) DO UPDATE SET
                   target_activities=excluded.target_activities,
                   target_visits=excluded.target_visits,
                   target_new_listings=excluded.target_new_listings,
                   target_units=excluded.target_units,
                   target_revenue=excluded.target_revenue,
                   notes=excluded.notes""",
            (rep, quarter,
             int(d.get('target_activities') or 0),
             int(d.get('target_visits') or 0),
             int(d.get('target_new_listings') or 0),
             int(d.get('target_units') or 0),
             float(d.get('target_revenue') or 0),
             d.get('notes', '')),
        )
        db.commit()
    return jsonify({'status': 'ok'})


# =========================== Daily plan: today's stops ===========================

@app.route('/api/crm/today/<path:rep>', methods=['GET'])
def api_crm_today(rep):
    """Compute today's plan for a rep.

    Algorithm:
      1. Pull stores in rep's territory (or assigned to them via stores.rep).
      2. Score each by: days_since_last_visit (weighted x2), OOS-risk count,
         open-deal-stage priority, store priority (High/Top = +2).
      3. Also pull deals with next_action_date <= today (overdue/due actions).
      4. Order by score desc; return top 8 stops with short-TSP ordering by proximity.
    """
    rep_trimmed = rep.strip()
    limit = int(request.args.get('limit', 8))
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # Stores assigned to this rep (match LOWER(TRIM))
    q = f"""
        SELECT s.id, s.store_number, s.account, s.address, s.city, s.postal,
               s.priority, s.lat, s.lng, s.territory_id,
               COALESCE(t.name, ''), COALESCE(t.color, '#888'),
               (SELECT MAX(a.created_at) FROM activities a WHERE a.store_id = s.id) AS last_visit,
               (SELECT COUNT(*) FROM activities a WHERE a.store_id = s.id) AS visit_count
        FROM stores s LEFT JOIN territories t ON t.id = s.territory_id
        WHERE LOWER(TRIM(s.rep)) = LOWER(TRIM({ph}))
        AND s.lat <> 0 AND s.lng <> 0
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, (rep_trimmed,))
        stores = cur.fetchall()
        cur.close()
    else:
        stores = db.execute(q, (rep_trimmed,)).fetchall()

    # Deals with next_action_date <= today
    today_str = _toronto_today().isoformat()
    dq = f"""
        SELECT store_number, sku, stage, next_action, next_action_date
        FROM deals
        WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM({ph}))
        AND stage NOT IN ('listed','lost')
        AND (next_action_date IS NULL OR next_action_date <= {ph})
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(dq, (rep_trimmed, today_str))
        deal_rows = cur.fetchall()
        cur.close()
    else:
        deal_rows = db.execute(dq, (rep_trimmed, today_str)).fetchall()

    # Build deal lookup per store
    deals_by_store = {}
    for dr in deal_rows:
        deals_by_store.setdefault(dr[0], []).append({
            'sku': dr[1], 'stage': dr[2],
            'next_action': dr[3], 'next_action_date': str(dr[4]) if dr[4] else None,
        })

    # OOS brink lookup per store
    tracked = list(SOD_TRACKED_SKUS.keys())
    oos_by_store = {}
    if tracked:
        phs = ','.join([ph] * len(tracked))
        oosq = f"""
            WITH latest AS (
                SELECT sku, MAX(snapshot_date) AS d FROM sod_inventory
                WHERE sku IN ({phs}) GROUP BY sku
            )
            SELECT i.store_number, COUNT(*)
            FROM sod_inventory i JOIN latest l ON l.sku = i.sku AND l.d = i.snapshot_date
            WHERE i.status = 'L' AND i.on_hand <= 2
            GROUP BY i.store_number
        """
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(oosq, tracked)
            for r in cur.fetchall():
                oos_by_store[r[0]] = int(r[1])
            cur.close()
        else:
            for r in db.execute(oosq, tracked).fetchall():
                oos_by_store[r[0]] = int(r[1])

    # Score each store
    scored = []
    for s in stores:
        sid, sn, account, address, city, postal, priority, lat, lng, tid, tname, tcolor, last_visit, visit_count = s
        days_since = 999
        if last_visit:
            try:
                lv = last_visit if isinstance(last_visit, datetime) else datetime.fromisoformat(str(last_visit).split('+')[0])
                days_since = (datetime.now() - lv.replace(tzinfo=None)).days
            except Exception:
                pass

        score = 0
        score += min(days_since, 60) * 2  # recency weight
        score += (oos_by_store.get(sn, 0)) * 15  # OOS urgency
        score += len(deals_by_store.get(sn, [])) * 10  # open actions
        if (priority or '').lower() in ('top', 'high'):
            score += 20
        if visit_count == 0:
            score += 25  # never-visited bonus

        scored.append({
            'store_id': sid, 'store_number': sn, 'account': account,
            'address': address, 'city': city, 'postal': postal,
            'priority': priority, 'lat': lat, 'lng': lng,
            'territory_id': tid, 'territory_name': tname, 'territory_color': tcolor,
            'days_since_visit': days_since if days_since < 999 else None,
            'visit_count': visit_count,
            'oos_count': oos_by_store.get(sn, 0),
            'deals': deals_by_store.get(sn, []),
            'score': score,
        })
    scored.sort(key=lambda x: -x['score'])

    # Take top `limit*2` and nearest-neighbor TSP them starting from the highest-scored
    pool = scored[: max(limit * 2, 4)]
    if len(pool) <= 1:
        ordered = pool
    else:
        ordered = [pool.pop(0)]
        while pool and len(ordered) < limit:
            last = ordered[-1]
            def d(a, b):
                try:
                    return haversine(a['lat'], a['lng'], b['lat'], b['lng'])
                except Exception:
                    return 1e6
            next_idx = min(range(len(pool)), key=lambda i: d(last, pool[i]))
            ordered.append(pool.pop(next_idx))

    # Total driving distance
    total_km = 0.0
    for i in range(1, len(ordered)):
        try:
            total_km += haversine(
                ordered[i-1]['lat'], ordered[i-1]['lng'],
                ordered[i]['lat'], ordered[i]['lng'],
            )
        except Exception:
            pass

    return jsonify({
        'rep': rep_trimmed,
        'plan_date': today_str,
        'stops': ordered[:limit],
        'total_distance_km': round(total_km, 1),
        'total_stops': len(ordered[:limit]),
        'overdue_deal_actions': sum(len(v) for v in deals_by_store.values()),
        'total_candidate_stores': len(stores),
    })


@app.route('/api/crm/reps-with-stores', methods=['GET'])
def api_crm_reps_with_stores():
    """List reps from stores table + how many stores they cover."""
    db = get_db()
    q = (
        "SELECT MIN(TRIM(rep)) AS rep, COUNT(*) AS store_count "
        "FROM stores WHERE rep IS NOT NULL AND TRIM(rep) <> '' "
        "GROUP BY LOWER(TRIM(rep)) ORDER BY store_count DESC"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q).fetchall()
    return jsonify([{'rep': r[0], 'store_count': r[1]} for r in rows])


# ------- Optional daily lcbo.com scrape (dual-source ingest) -------
_lcbo_scheduler = None


def _lcbo_daily_scrape_worker():
    """Scrape live LCBO.com inventory + RECONCILE with SOD.

    Two outputs:
      1. inventory_history: append-only trend log (existing behavior).
      2. sod_store_sku_changes with change_type='LCBO_LIVE_ONLY' for stores where
         lcbo.com shows on_hand > 0 BUT SOD has no row OR status='F' (delisted).
         This is the killer signal: 'lcbo.com shows live inventory at this store
         but SOD says it's delisted/missing — investigate.'

    Idempotent on the reconciliation side via UNIQUE(sku, store_number, change_date,
    change_type). Runs every 2 hours via the scheduler.
    """
    try:
        scrape = globals().get('scrape_lcbo_inventory')
        if not callable(scrape):
            print('[LCBO-live] scrape_lcbo_inventory not available')
            return
        conn = _sod_get_conn()
        cur = conn.cursor()
        total_rows = 0
        discoveries = 0  # NEW: stores found via lcbo.com but missing from SOD
        today_str = _toronto_today().isoformat()
        for sku, (brand, pname) in SOD_TRACKED_SKUS.items():
            sku_clean = sku.lstrip('0')
            sku_padded = sku  # already 7-char zero-padded in SOD_TRACKED_SKUS
            try:
                rows = scrape(sku_clean) or []
            except Exception as e:
                print(f'[LCBO-live] scrape failed for {sku}: {e}')
                continue

            # Find product row for inventory_history
            if USE_POSTGRES:
                cur.execute("SELECT id FROM products WHERE lcbo_sku=%s LIMIT 1", (sku_clean,))
                prow = cur.fetchone()
            else:
                prow = cur.execute("SELECT id FROM products WHERE lcbo_sku=? LIMIT 1", (sku_clean,)).fetchone()
            pid = prow[0] if prow else None

            # Pull SOD's current view of THIS sku (latest snapshot)
            if USE_POSTGRES:
                cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s",
                    (sku_padded,),
                )
                latest = cur.fetchone()[0]
            else:
                latest = cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=?",
                    (sku_padded,),
                ).fetchone()[0]
            sod_per_store = {}
            if latest:
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number, status, on_hand FROM sod_inventory "
                        "WHERE sku=%s AND snapshot_date=%s",
                        (sku_padded, latest),
                    )
                else:
                    cur.execute(
                        "SELECT store_number, status, on_hand FROM sod_inventory "
                        "WHERE sku=? AND snapshot_date=?",
                        (sku_padded, latest),
                    )
                sod_per_store = {int(r[0]): {'status': r[1], 'on_hand': r[2]} for r in cur.fetchall()}

            # Walk lcbo.com rows
            sku_discoveries = []
            for r in rows:
                store_num_raw = r.get('store_number', '')
                qty = int(r.get('quantity', 0) or 0)
                # Append to inventory_history
                if pid is not None:
                    if USE_POSTGRES:
                        cur.execute(
                            """INSERT INTO inventory_history
                               (product_id, store_number, store_name, store_city, quantity, recorded_at)
                               VALUES (%s,%s,%s,%s,%s,NOW())""",
                            (pid, str(store_num_raw), r.get('store_name', ''),
                             r.get('store_city', ''), qty),
                        )
                    else:
                        cur.execute(
                            """INSERT INTO inventory_history
                               (product_id, store_number, store_name, store_city, quantity, recorded_at)
                               VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)""",
                            (pid, str(store_num_raw), r.get('store_name', ''),
                             r.get('store_city', ''), qty),
                        )
                    total_rows += 1

                # RECONCILE: lcbo.com shows in-stock but SOD missing or status=F?
                try:
                    store_num = int(store_num_raw)
                except (ValueError, TypeError):
                    continue
                if qty <= 0:
                    continue
                sod = sod_per_store.get(store_num)
                if sod is None:
                    sku_discoveries.append((sku_padded, store_num, today_str, None, 'L', 'LCBO_LIVE_ONLY'))
                elif sod.get('status') == 'F':
                    sku_discoveries.append((sku_padded, store_num, today_str, 'F', 'L', 'LCBO_LIVE_ONLY'))

            # Bulk insert discoveries (idempotent)
            if sku_discoveries:
                if USE_POSTGRES:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO sod_store_sku_changes
                           (sku, store_number, change_date, old_status, new_status, change_type)
                           VALUES %s
                           ON CONFLICT (sku, store_number, change_date, change_type) DO NOTHING""",
                        sku_discoveries,
                    )
                else:
                    cur.executemany(
                        """INSERT INTO sod_store_sku_changes
                           (sku, store_number, change_date, old_status, new_status, change_type)
                           VALUES (?,?,?,?,?,?)
                           ON CONFLICT(sku, store_number, change_date, change_type) DO NOTHING""",
                        sku_discoveries,
                    )
                discoveries += len(sku_discoveries)
        conn.commit()
        cur.close()
        conn.close()
        print(f'[LCBO-live] scraped {total_rows} store-rows; '
              f'found {discoveries} discoveries (lcbo.com live but SOD blank/F)')
    except Exception as e:
        print(f'[LCBO-live] scrape failed: {e}')


def _log_event(event_type, entity_type, entity_id, actor, payload=None):
    """Append-only audit log. Never throws — failures shouldn't block writes.

    event_type: e.g. 'activity_created', 'deal_advanced', 'listing_marked'
    entity_type: 'activity' / 'deal' / 'store' / 'sku' etc.
    entity_id:   the ID or natural key
    actor:       who did it (rep name)
    payload:     dict, will be JSON-encoded
    """
    try:
        conn = _sod_get_conn()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute(
                "INSERT INTO event_log (event_type, entity_type, entity_id, actor, "
                "payload_json, ip_address, user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (event_type, entity_type, str(entity_id) if entity_id is not None else None,
                 actor or '', json.dumps(payload) if payload else '',
                 request.headers.get('X-Forwarded-For', request.remote_addr or '')[:50] if request else '',
                 (request.headers.get('User-Agent', '') if request else '')[:200]),
            )
        else:
            cur.execute(
                "INSERT INTO event_log (event_type, entity_type, entity_id, actor, "
                "payload_json, ip_address, user_agent) VALUES (?,?,?,?,?,?,?)",
                (event_type, entity_type, str(entity_id) if entity_id is not None else None,
                 actor or '', json.dumps(payload) if payload else '',
                 request.headers.get('X-Forwarded-For', request.remote_addr or '')[:50] if request else '',
                 (request.headers.get('User-Agent', '') if request else '')[:200]),
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        # NEVER let logging block the user. Just print and move on.
        print(f'[event_log] failed to write: {e}')


@app.route('/api/crm/event-log', methods=['GET'])
def api_crm_event_log():
    """Append-only audit trail of every mutation."""
    days = int(request.args.get('days', 30))
    entity_type = request.args.get('entity_type', '').strip()
    actor = request.args.get('actor', '').strip()
    limit = int(request.args.get('limit', 500))
    since = (_toronto_today() - timedelta(days=days)).isoformat()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    where = [f'created_at >= {ph}']
    params = [since]
    if entity_type:
        where.append(f'entity_type = {ph}')
        params.append(entity_type)
    if actor:
        where.append(f'LOWER(actor) = LOWER({ph})')
        params.append(actor)
    q = (f"SELECT id, event_type, entity_type, entity_id, actor, payload_json, "
         f"ip_address, user_agent, created_at FROM event_log "
         f"WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT {ph}")
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params + [limit])
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params + [limit]).fetchall()
    return jsonify({
        'events': [{
            'id': r[0], 'event_type': r[1], 'entity_type': r[2], 'entity_id': r[3],
            'actor': r[4], 'payload_json': r[5], 'ip_address': r[6],
            'user_agent': r[7], 'created_at': str(r[8]),
        } for r in rows],
        'days': days,
        'total': len(rows),
    })


@app.route('/api/crm/tasting-followups', methods=['GET'])
def api_crm_tasting_followups():
    """Stores where a TASTING / SAMPLE was logged for a tracked SKU but the SKU
    is NOT currently listed at that store. The follow-up opportunity list.

    Joins activity_sku_outcomes (with outcomes ∈ tasting/sampled/samples_left)
    against the latest SOD snapshot per SKU. Returns rows where current SOD
    status is missing, 'D', or 'F'.
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    days = int(request.args.get('days', 365))
    since = (_toronto_today() - timedelta(days=days)).isoformat()

    # Find activities (visits/tastings) within window, with sku_outcomes that are tasting-y
    tasting_outcomes = ('tasting', 'sampled', 'samples_left', 'sample_drop')
    phs = ','.join([ph] * len(tasting_outcomes))
    q = f"""
        SELECT
            aso.sku, aso.outcome, aso.facings,
            a.id AS activity_id, a.activity_type, a.notes, a.outcome AS activity_outcome,
            a.rep, COALESCE(a.visit_date, a.created_at) AS visit_when,
            s.id AS store_id, s.store_number, s.account, s.city, s.postal,
            t.name AS territory_name, COALESCE(t.color, '#888') AS territory_color
        FROM activity_sku_outcomes aso
        JOIN activities a ON a.id = aso.activity_id
        LEFT JOIN stores s ON s.id = a.store_id
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE a.deleted_at IS NULL
          AND COALESCE(a.visit_date, a.created_at::date) >= {ph}
          AND (
            LOWER(aso.outcome) IN ({phs})
            OR LOWER(a.activity_type) IN ('tasting', 'sample_drop')
          )
    """ if USE_POSTGRES else f"""
        SELECT
            aso.sku, aso.outcome, aso.facings,
            a.id AS activity_id, a.activity_type, a.notes, a.outcome AS activity_outcome,
            a.rep, COALESCE(a.visit_date, DATE(a.created_at)) AS visit_when,
            s.id AS store_id, s.store_number, s.account, s.city, s.postal,
            t.name AS territory_name, COALESCE(t.color, '#888') AS territory_color
        FROM activity_sku_outcomes aso
        JOIN activities a ON a.id = aso.activity_id
        LEFT JOIN stores s ON s.id = a.store_id
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE a.deleted_at IS NULL
          AND COALESCE(a.visit_date, DATE(a.created_at)) >= {ph}
          AND (
            LOWER(aso.outcome) IN ({phs})
            OR LOWER(a.activity_type) IN ('tasting', 'sample_drop')
          )
    """
    params = [since] + [o for o in tasting_outcomes]
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, params).fetchall()

    # For each tasting row, look up CURRENT SOD status of that SKU at that store
    out = []
    seen = set()
    for r in rows:
        sku, sku_outcome, facings, activity_id, atype, anotes, aoutcome, rep, visit_when, \
            store_id, store_number, account, city, postal, terr_name, terr_color = r
        if not store_number:
            continue
        # Latest SOD status for this (sku, store)
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(
                "SELECT status, on_hand, snapshot_date FROM sod_inventory "
                "WHERE sku = %s AND store_number = %s "
                "ORDER BY snapshot_date DESC LIMIT 1",
                (sku, store_number),
            )
            sod_row = cur.fetchone()
            cur.close()
        else:
            sod_row = db.execute(
                "SELECT status, on_hand, snapshot_date FROM sod_inventory "
                "WHERE sku = ? AND store_number = ? "
                "ORDER BY snapshot_date DESC LIMIT 1",
                (sku, store_number),
            ).fetchone()
        current_status = sod_row[0] if sod_row else None
        current_on_hand = (sod_row[1] or 0) if sod_row else 0
        # Only flag if NOT currently listed (missing, D, or F)
        if current_status == 'L':
            continue
        # De-dup: one entry per (store, sku) — pick the most recent tasting
        key = (store_number, sku)
        if key in seen:
            continue
        seen.add(key)
        try:
            visit_date = visit_when.isoformat() if hasattr(visit_when, 'isoformat') else str(visit_when)
        except Exception:
            visit_date = str(visit_when)
        try:
            days_since = (datetime.now() - datetime.fromisoformat(visit_date.split('T')[0])).days
        except Exception:
            days_since = None
        brand, pname = SOD_TRACKED_SKUS.get(sku, ('', ''))
        out.append({
            'sku': sku,
            'brand': brand,
            'product_name': pname,
            'store_number': store_number,
            'store_id': store_id,
            'account': account,
            'city': city,
            'postal': postal,
            'territory_name': terr_name or 'Unassigned',
            'territory_color': terr_color,
            'tasting_date': visit_date,
            'days_since_tasting': days_since,
            'tasting_outcome': sku_outcome,
            'tasting_facings': facings,
            'activity_id': activity_id,
            'activity_type': atype,
            'activity_outcome': aoutcome or '',
            'activity_notes': (anotes or '')[:200],
            'rep': rep,
            'current_sod_status': current_status,  # None / D / F
            'current_sod_on_hand': current_on_hand,
            'priority_score': (
                30 + (60 - min(days_since or 60, 60))  # newer = higher score
                + (15 if current_status == 'D' else 0)  # delisting nudge
                + (10 if facings and facings > 0 else 0)
            ),
        })
    out.sort(key=lambda x: -(x.get('priority_score') or 0))
    return jsonify({
        'days': days,
        'since': since,
        'total': len(out),
        'followups': out,
    })


@app.route('/api/crm/lcbo-live-discoveries', methods=['GET'])
def api_crm_lcbo_live_discoveries():
    """Stores where lcbo.com shows our SKU live but SOD has it as missing/delisted.

    These are highest-value discoveries — LCBO ordered the product, it's on shelf
    selling, but SOD hasn't reflected it yet (SOD lag can be days/weeks for some
    listings). Surfacing them lets the rep act before the next SOD cycle.
    """
    days = int(request.args.get('days', 30))
    since = (_toronto_today() - timedelta(days=days)).isoformat()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    q = f"""
        SELECT c.sku, c.store_number, c.change_date, c.old_status,
               s.account, s.city, s.postal, s.rep,
               t.name AS territory_name, COALESCE(t.color, '#888') AS territory_color,
               (SELECT i.status FROM sod_inventory i
                  WHERE i.sku = c.sku AND i.store_number = c.store_number
                  ORDER BY i.snapshot_date DESC LIMIT 1) AS current_sod_status,
               (SELECT i.on_hand FROM sod_inventory i
                  WHERE i.sku = c.sku AND i.store_number = c.store_number
                  ORDER BY i.snapshot_date DESC LIMIT 1) AS current_sod_on_hand,
               (SELECT MAX(ih.recorded_at) FROM inventory_history ih
                  JOIN products p ON p.id = ih.product_id
                  WHERE p.lcbo_sku = TRIM(LEADING '0' FROM c.sku)
                  AND ih.store_number = CAST(c.store_number AS TEXT)) AS last_lcbo_seen
        FROM sod_store_sku_changes c
        LEFT JOIN stores s ON s.store_number = c.store_number
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE c.change_type = 'LCBO_LIVE_ONLY' AND c.change_date >= {ph}
        ORDER BY c.change_date DESC, c.sku, c.store_number
        LIMIT 500
    """ if USE_POSTGRES else f"""
        SELECT c.sku, c.store_number, c.change_date, c.old_status,
               s.account, s.city, s.postal, s.rep,
               t.name AS territory_name, COALESCE(t.color, '#888') AS territory_color,
               (SELECT i.status FROM sod_inventory i
                  WHERE i.sku = c.sku AND i.store_number = c.store_number
                  ORDER BY i.snapshot_date DESC LIMIT 1) AS current_sod_status,
               (SELECT i.on_hand FROM sod_inventory i
                  WHERE i.sku = c.sku AND i.store_number = c.store_number
                  ORDER BY i.snapshot_date DESC LIMIT 1) AS current_sod_on_hand,
               (SELECT MAX(ih.recorded_at) FROM inventory_history ih
                  JOIN products p ON p.id = ih.product_id
                  WHERE p.lcbo_sku = LTRIM(c.sku, '0')
                  AND ih.store_number = CAST(c.store_number AS TEXT)) AS last_lcbo_seen
        FROM sod_store_sku_changes c
        LEFT JOIN stores s ON s.store_number = c.store_number
        LEFT JOIN territories t ON t.id = s.territory_id
        WHERE c.change_type = 'LCBO_LIVE_ONLY' AND c.change_date >= ?
        ORDER BY c.change_date DESC, c.sku, c.store_number
        LIMIT 500
    """
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(q, [since])
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(q, [since]).fetchall()
    out = []
    for r in rows:
        sku = r[0]
        brand, pname = SOD_TRACKED_SKUS.get(sku, ('', ''))
        out.append({
            'sku': sku, 'brand': brand, 'product_name': pname,
            'store_number': r[1],
            'change_date': str(r[2]),
            'old_sod_status': r[3],
            'account': r[4], 'city': r[5], 'postal': r[6], 'rep': r[7],
            'territory_name': r[8] or 'Unassigned',
            'territory_color': r[9],
            'current_sod_status': r[10],
            'current_sod_on_hand': r[11] or 0,
            'last_lcbo_seen': str(r[12]) if r[12] else None,
        })
    return jsonify({
        'days': days, 'since': since, 'total': len(out), 'discoveries': out,
        'freshness': _sod_freshness(),
    })


@app.route('/api/crm/lcbo-rescan', methods=['POST'])
def api_crm_lcbo_rescan():
    """Manually trigger an immediate lcbo.com scrape + reconcile."""
    if _sod_sync_lock.locked():
        return jsonify({'status': 'busy', 'note': 'a sync is already running'}), 202
    threading.Thread(target=_lcbo_daily_scrape_worker, daemon=True).start()
    return jsonify({'status': 'started', 'note': 'scraping lcbo.com for tracked SKUs'}), 202


def start_lcbo_scheduler():
    """Start the lcbo.com scraper every 2 hours.

    More aggressive than the SOD scheduler because lcbo.com updates in real-time
    when stores get new shipments, while SOD only refreshes once a day.
    """
    global _lcbo_scheduler
    if _lcbo_scheduler is not None:
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print('[LCBO-live] apscheduler not installed — skipping')
        return
    try:
        try:
            sched = BackgroundScheduler(timezone='America/Toronto')
        except Exception:
            sched = BackgroundScheduler()
        # Every 30 minutes from 06:00 to 23:00 ET (35 runs/day) — near-realtime.
        # User asked for "by-the-second" — this is the closest we can get without
        # rate-limiting LCBO.com. Each run reconciles tracked SKUs against SOD.
        sched.add_job(
            _lcbo_daily_scrape_worker,
            CronTrigger(hour='6-23', minute='0,30'),
            id='lcbo_30min_scrape',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800 * 2,
        )
        sched.start()
        _lcbo_scheduler = sched
        print(f'[LCBO-live] Daily scraper scheduled for 04:00 ET')
    except Exception as e:
        print(f'[LCBO-live] scheduler failed: {e}')


# ======== INIT ========

init_db()
seed_data()
seed_territories()
refresh_sod_product_categories()
# Ensure all 5 official reps exist in the reps table on every boot.
# Without this, /api/reps returns only reps that have already logged activity
# (chicken-and-egg) and the dropdown is missing names. Idempotent.
try:
    _conn = _sod_get_conn()
    _cur = _conn.cursor()
    for _r in REP_ROSTER_DEFAULT:
        if USE_POSTGRES:
            _cur.execute(
                "INSERT INTO reps (name) SELECT %s WHERE NOT EXISTS "
                "(SELECT 1 FROM reps WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s)))",
                (_r, _r),
            )
        else:
            _cur.execute(
                "INSERT INTO reps (name) SELECT ? WHERE NOT EXISTS "
                "(SELECT 1 FROM reps WHERE LOWER(TRIM(name)) = LOWER(TRIM(?)))",
                (_r, _r),
            )
    _conn.commit()
    _cur.close()
    _conn.close()
    print(f"[startup] Ensured {len(REP_ROSTER_DEFAULT)} official reps in DB: {REP_ROSTER_DEFAULT}")
except Exception as _e:
    print(f"[startup] rep seeding skipped: {_e}")
# Sprint 0: cleanup orphaned 'running' SOD runs from prior crashes (e.g. OOM kills).
# Lowered to 1h (was 6h) so we surface hangs faster — gunicorn has --timeout=1800
# (30 min) so anything still 'running' after 1h is definitely a crash, not progress.
_cleanup_orphaned_sod_runs(max_age_hours=1)
# Sprint 4: backfill per-store SKU change events from historical snapshots so the
# /distribution-additions + brand drill-downs have data immediately on first deploy.
# Idempotent: re-running is safe (UNIQUE constraint).
try:
    _backfill_store_sku_changes()
except Exception as _e:
    print(f'[backfill] error during startup backfill: {_e}')
start_sod_scheduler()
start_lcbo_scheduler()
# Sprint 5: daily automated health check + auto-recovery (06:00 + 14:00 ET).
start_health_scheduler()
# Sprint 6: daily backup-to-email at 02:00 ET so data is recoverable even if
# the host AND the DB die (worst-case disaster recovery via email attachment).
start_backup_scheduler()
# Sprint 7: daily tasting digest at 06:30 ET — emails tomorrow's bookings
# to ikshit@anuspirits.com / sales@anuspirits.com (via TASTING_DIGEST_TO).
start_tasting_digest_scheduler()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    app.run(debug=os.environ.get('FLASK_DEBUG', 'true').lower() == 'true', host='0.0.0.0', port=port)
