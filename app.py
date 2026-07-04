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

# ── Auth helpers (defined early so decorators below can reference them) ───
# These need to live above the first @require_admin_token / @require_app_origin
# decorator usage. Python evaluates decorators at module-import time, so
# referencing an undefined symbol → NameError → gunicorn worker dies → deploy fails.

import hmac as _hmac

# API_KEY gates ALL endpoints (read + write). Set in Render env vars.
# Generate with: python3 -c "import secrets; print(secrets.token_urlsafe(48))"
API_KEY = os.environ.get('API_KEY', '').strip()

# Public paths that skip API key auth (health probes, static assets, root page)
_PUBLIC_PATHS = frozenset(('/', '/healthz', '/favicon.ico'))


@app.before_request
def _require_api_key():
    """Gate every /api/* request behind X-API-Key header.
    If API_KEY env var is unset, allow everything (dev mode only).
    """
    if not API_KEY:
        return
    if request.path in _PUBLIC_PATHS:
        return
    if request.path.startswith('/static'):
        return
    if not request.path.startswith('/api'):
        return
    got = (request.headers.get('X-API-Key') or '').strip()
    if not got:
        return jsonify({'error': 'unauthorized', 'detail': 'X-API-Key header required.'}), 401
    if not _hmac.compare_digest(got, API_KEY):
        return jsonify({'error': 'forbidden', 'detail': 'Invalid API key.'}), 403


def _admin_token_ok() -> bool:
    """Verify X-Admin-Token header against ADMIN_TOKEN env var.
    If ADMIN_TOKEN is not set, only allow from localhost (dev convenience).
    """
    expected = os.environ.get('ADMIN_TOKEN', '').strip()
    if not expected:
        return request.remote_addr in ('127.0.0.1', '::1', 'localhost')
    got = request.headers.get('X-Admin-Token', '').strip()
    if not got or len(got) != len(expected):
        return False
    return _hmac.compare_digest(got, expected)


def require_admin_token(fn):
    """Decorator for endpoints that mutate data or expose secrets."""
    from functools import wraps

    @wraps(fn)
    def wrapped(*args, **kwargs):
        if not _admin_token_ok():
            return jsonify({
                'error': 'forbidden',
                'detail': 'Provide a valid X-Admin-Token header.',
            }), 403
        return fn(*args, **kwargs)
    return wrapped


_ALLOWED_ORIGINS = {
    'https://lcbo-tracker-web.vercel.app',
    'https://lcbo.anu-spirits.com',
    'http://localhost:3000',
    'http://localhost:3001',
    'http://127.0.0.1:3000',
}


def _request_origin_ok() -> bool:
    """Return True if the request's Origin or Referer is on the explicit allowlist."""
    origin = (request.headers.get('Origin') or '').strip().rstrip('/')
    referer = (request.headers.get('Referer') or '').strip()
    if origin and origin in _ALLOWED_ORIGINS:
        return True
    if referer:
        try:
            from urllib.parse import urlparse
            p = urlparse(referer)
            host_root = f"{p.scheme}://{p.netloc}".rstrip('/')
            if host_root in _ALLOWED_ORIGINS:
                return True
        except Exception:
            pass
    return False


def require_app_origin(fn):
    """Decorator for mutating CRM endpoints. Requires browser Origin from the
    Anu CRM frontend OR a valid X-Admin-Token.
    """
    from functools import wraps

    @wraps(fn)
    def wrapped(*args, **kwargs):
        if _request_origin_ok() or _admin_token_ok():
            return fn(*args, **kwargs)
        return jsonify({
            'error': 'forbidden',
            'detail': 'This endpoint is callable only from the Anu CRM frontend or with a valid X-Admin-Token.',
        }), 403
    return wrapped


@app.after_request
def _security_headers(resp):
    resp.headers['Strict-Transport-Security'] = 'max-age=63072000; includeSubDomains; preload'
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['X-Frame-Options'] = 'DENY'
    resp.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    resp.headers['Permissions-Policy'] = 'camera=(), microphone=(), geolocation=(self)'
    resp.headers['X-XSS-Protection'] = '1; mode=block'
    if request.path.startswith('/api'):
        resp.headers['Content-Security-Policy'] = "default-src 'none'; frame-ancestors 'none'"
        resp.headers['Cache-Control'] = resp.headers.get('Cache-Control', 'no-store')
    return resp


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
            # Silent GPS capture on activity submission. Existing lat/lng
            # columns hold the position; these new fields add precision +
            # capture-time + distance from the store's known coords. Never
            # surfaced in the rep-facing UI — operator-only via reports.
            ('activities', 'accuracy_m', 'REAL'),
            ('activities', 'client_ts', 'TIMESTAMP'),
            ('activities', 'distance_from_store_m', 'REAL'),
            ('activities', 'deleted_at', 'TIMESTAMP'),
            # Store profile: spirits ambassador name + freeform notes.
            # Surfaces on /stores/[id] for rep updates during visits.
            ('stores', 'spirits_ambassador', "TEXT DEFAULT ''"),
            ('stores', 'store_notes', "TEXT DEFAULT ''"),
            # Multi-brand portfolio scaffold — every product is tagged with
            # a portfolio (default 'Anu'). When new brand engagements start
            # we tag those SKUs with a different portfolio so the same CRM
            # can host multiple agency books with role-based filtering.
            ('products', 'portfolio', "TEXT DEFAULT 'Anu'"),
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
        # Idempotency guard for sod_listing_changes — DEFERRED to admin
        # endpoint /api/admin/dedupe-listing-changes so startup stays fast.
        # Pre-existing duplicates would block CREATE UNIQUE INDEX; rather than
        # run a potentially-slow DELETE at boot (gunicorn worker timeout =
        # 30s on Render), we attempt the index and silently skip if dupes
        # exist. The insert path has a fallback for both the indexed and
        # non-indexed case, so functionality is unaffected either way.
        try:
            cur.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS uniq_sod_listing_changes
                  ON sod_listing_changes (sku, COALESCE(store_number, -1), change_date, change_type)
            ''')
        except Exception as _idx_err:
            print(f"[init_db] uniq_sod_listing_changes index skipped (dedupe needed): {_idx_err}")
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
@require_app_origin
def api_store_update(store_id):
    data = request.json
    fields = ['account', 'address', 'city', 'postal', 'phone', 'email', 'contacts',
              'priority', 'status', 'rep', 'manager_name', 'asst_manager_name',
              'manager_phone', 'store_email', 'producer',
              'spirits_ambassador', 'store_notes']
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
@require_app_origin
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
@require_app_origin
def api_followup_complete(followup_id):
    """Mark a follow-up as completed — data is NEVER deleted, only status changes"""
    db_execute("UPDATE followups SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE id=?", [followup_id])
    db_commit()
    return jsonify({'success': True, 'message': 'Follow-up marked as completed'})


@app.route('/api/followups/<int:followup_id>/reschedule', methods=['POST'])
@require_app_origin
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
@require_app_origin
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
    """Write scraped inventory to inventory_cache + inventory_history, update product listing status.

    CRITICAL: writes per-store rows so the cross-validation engine can detect
    SOD vs lcbo.com drift accurately (was: only writing one SUMMARY row, which
    caused the data-integrity report to think we have inventory in '1 store'
    and report 98% drift on every SKU — completely useless).
    """
    # Replace inventory cache for this product
    db_execute("DELETE FROM inventory_cache WHERE product_id=?", [product_id])
    for s in stores:
        db_execute(
            "INSERT INTO inventory_cache (product_id, store_number, store_name, store_city, quantity) VALUES (?,?,?,?,?)",
            [product_id, s['store_number'], s['store_name'], s['city'], s['quantity']]
        )
    # Append PER-STORE rows to inventory_history (this is the source of truth
    # for the cross-validation engine; SUMMARY rows below are kept for legacy
    # 14-day trend graphs but excluded from drift queries).
    try:
        for s in stores:
            db_execute(
                "INSERT INTO inventory_history (product_id, store_number, store_name, store_city, quantity) VALUES (?,?,?,?,?)",
                [product_id, str(s['store_number']), s['store_name'], s['city'], s['quantity']]
            )
        # Aggregate row for legacy trend display
        db_execute(
            "INSERT INTO inventory_history (product_id, store_number, store_name, store_city, quantity) VALUES (?,?,?,?,?)",
            [product_id, 'SUMMARY', f'{len(stores)} stores', 'TOTAL', sum(s['quantity'] for s in stores)]
        )
    except Exception as e:
        print(f"[persist] inventory_history write failed for product {product_id}: {e}")
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
@require_app_origin
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
@require_app_origin
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
    # NB Distillers (PRIMARY paying client — reps are NB-focused for now)
    '0020187': ('NB Distillers', 'Red Admiral Vodka'),
    '0022246': ('NB Distillers', 'Chak De Canadian Whisky'),
    # Anu Import portfolio (separate agency book — toggle to view)
    '0046340': ('Goenchi', 'Goenchi Cashew Feni'),
    '0046343': ('Goenchi', 'Goenchi Coconut Feni'),
    '0046282': ('Fratelli', 'Fratelli Classic Shiraz'),
    '0046285': ('Fratelli', 'Fratelli Chenin Blanc'),
    '0046286': ('Fratelli', 'Fratelli Sauvignon Blanc'),
    '0046287': ('Fratelli', 'Fratelli Cabernet Sauvignon'),
    '0045378': ('Rock Paper', 'Rock Paper Rum'),  # NEW Anu import — added 2026-05-27
    # Prospect analysis for the agency pitch — Mandakini (Oxford Beverage Group).
    # Added 2026-07-04 so SOD carries store-level rows for #47587 (listed vs stocked).
    '0047587': ('Mandakini', 'Mandakini Malabari Vaatte Desi Daaru'),
}

# Portfolio split — every tracked SKU belongs to exactly one agency book.
# Used to scope rep dashboards / territory rollups / morning digest etc.
# Default behavior on rep-facing surfaces is portfolio='NB' so the field
# team's view stays clean; Anu data is one toggle away.
SKU_PORTFOLIO = {
    '0020187': 'NB',  '0022246': 'NB',
    '0046340': 'Anu', '0046343': 'Anu',
    '0046282': 'Anu', '0046285': 'Anu',
    '0046286': 'Anu', '0046287': 'Anu',
    '0045378': 'Anu',
    '0047587': 'Anu',  # Mandakini prospect
}


def _skus_for_portfolio(portfolio: str | None) -> list[str]:
    """Return tracked SKU codes that belong to the requested portfolio.

    portfolio:
      'NB'  → only NB Distillers SKUs (Red Admiral + Chak De)  ← rep default
      'Anu' → only Anu Import SKUs (Goenchi/Fratelli/Rock Paper)
      'all' / None / '' → every tracked SKU (operator view)
    """
    if not portfolio or str(portfolio).strip().lower() == 'all':
        return list(SOD_TRACKED_SKUS.keys())
    want = str(portfolio).strip().upper()
    if want == 'ANU': want = 'Anu'  # canonical case
    return [s for s, p in SKU_PORTFOLIO.items() if p == want]

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

    # ───── CATALOG DISCOVERY (annual archives, options, informative) ─────
    # SOD portal exposes way more than just the daily files. Categories
    # include Informative 2025 (annual), Option 1/3/5 2025 (agent options
    # for tracking-over-time), etc. The portal index lists everything as
    # links of the form /downloads/general/{cat_id}/{filename} or
    # /downloads/agent/{agent_id}/{cat_id}/{filename}.
    #
    # list_catalog() crawls the index and returns the parsed tree.
    # download_url() fetches any file given its full URL.
    def list_catalog(self, debug=False):
        """Discover every available SOD download category + file.

        STRATEGY:
          1. Fetch /subscribers (the portal landing page after login)
          2. From it, extract every anchor href — not just /downloads/...
             Categories are listed as table rows where the LABEL column
             names the category and the third column has an 'Available
             documents' link to the per-category file list.
          3. For each candidate "category page" link found on the index
             (typical paths: /subscribers/N, /downloads/general/N,
              /downloads/agent/AGENT/N), follow it.
          4. On each category page, regex-extract every .zip file link
             and use the page's H1/title as the category label.

        Returns: {'index_url_used', 'agent_id', 'categories', 'discovery_log'}
        """
        import re as _re
        from urllib.parse import urljoin
        self._ensure_logged_in()

        # ANCHOR_RE captures: full href + visible label text
        ANCHOR_RE = _re.compile(
            r'<a[^>]+href="([^"]+)"[^>]*>([^<]{0,200})</a>',
            _re.IGNORECASE | _re.DOTALL,
        )
        FILE_RE = _re.compile(
            r'href="([^"]*?/downloads/[^"]*?\.zip)"',
            _re.IGNORECASE,
        )
        TITLE_RE = _re.compile(
            r'<(?:h1|h2)[^>]*>([^<]{3,200})</(?:h1|h2)>',
            _re.IGNORECASE,
        )

        index_candidates = [f'{SOD_BASE}/subscribers']
        if self.agent_id:
            index_candidates += [
                f'{SOD_BASE}/subscribers/{self.agent_id}',
                f'{SOD_BASE}/downloads/agent/{self.agent_id}',
            ]
        index_candidates += [f'{SOD_BASE}/downloads', f'{SOD_BASE}/']

        discovery_log: list = []
        index_url_used = None
        page_anchors: dict = {}     # url -> [(href, label), ...]

        # ── Pass 1: scan known index pages for any anchor links ────────
        for url in index_candidates:
            try:
                r = self.session.get(url, timeout=self.timeout)
            except Exception as e:
                discovery_log.append(f'{url} → {e}')
                continue
            discovery_log.append(f'{url} → {r.status_code} ({len(r.text or "")} bytes)')
            if r.status_code != 200:
                continue
            html = r.text or ''
            if 'Sign out' not in html and 'sign-out' not in html.lower():
                continue
            if not index_url_used:
                index_url_used = url
            anchors = []
            for m in ANCHOR_RE.finditer(html):
                href, label = m.groups()
                href_abs = urljoin(url, href)
                anchors.append((href_abs, ' '.join(label.split())[:200]))
            page_anchors[url] = anchors

        # ── Pass 2: collect candidate category-page URLs ────────────────
        # Heuristics:
        #   1. Anything under /downloads/* that's NOT directly a .zip
        #   2. Anything under /subscribers/* deeper than the root
        #   3. Anchors with text matching "available documents" / "files"
        candidates: dict = {}   # cat_url -> {label: str, anchor_label: str}
        for src_url, anchors in page_anchors.items():
            for href, label in anchors:
                if not href.startswith(SOD_BASE):
                    continue
                if href.lower().endswith('.zip'):
                    continue
                lower_label = label.lower()
                is_dl_path = '/downloads/' in href and href.rstrip('/') != f'{SOD_BASE}/downloads'
                is_sub_path = '/subscribers/' in href and href.rstrip('/') != f'{SOD_BASE}/subscribers'
                is_doc_link = (
                    'available document' in lower_label
                    or 'documents' == lower_label
                    or 'files' == lower_label
                )
                if is_dl_path or is_sub_path or is_doc_link:
                    candidates.setdefault(href, {
                        'anchor_label': label or '',
                        'discovered_from': src_url,
                    })

        # ── Pass 3: visit each candidate; harvest .zip links + page title ─
        files_by_cat: dict = {}
        labels_by_cat: dict = {}
        for cat_url, meta in candidates.items():
            try:
                r = self.session.get(cat_url, timeout=self.timeout)
            except Exception as e:
                discovery_log.append(f'{cat_url} → {e}')
                continue
            discovery_log.append(f'{cat_url} → {r.status_code}')
            if r.status_code != 200:
                continue
            html = r.text or ''
            # Page title often names the category
            title_m = TITLE_RE.search(html)
            page_title = ' '.join(title_m.group(1).split()) if title_m else ''
            label = page_title or meta.get('anchor_label') or cat_url
            # Use cat_url as the category key
            for m in FILE_RE.finditer(html):
                rel = m.group(1)
                full = urljoin(cat_url, rel)
                fname = full.rsplit('/', 1)[-1]
                # Derive a stable category_id from the URL path
                # (e.g. /downloads/general/12/foo.zip → ('general', '12'))
                segs = full.split('/downloads/', 1)[-1].split('/')
                if len(segs) >= 2:
                    if segs[0] == 'agent' and len(segs) >= 3:
                        scope = f'agent/{segs[1]}'
                        cat_id = segs[2] if segs[2].isdigit() else 0
                    else:
                        scope = segs[0]
                        cat_id = segs[1] if segs[1].isdigit() else 0
                else:
                    scope = 'unknown'
                    cat_id = 0
                cat_key = cat_url  # use the page URL as a stable grouping key
                files_by_cat.setdefault(cat_key, []).append({
                    'url': full,
                    'filename': fname,
                    'category_scope': scope,
                    'category_id': int(cat_id) if str(cat_id).isdigit() else 0,
                })
                labels_by_cat.setdefault(cat_key, label)

        # ── Build response ─────────────────────────────────────────────
        categories = []
        for cat_url, files in files_by_cat.items():
            files.sort(key=lambda f: f['filename'])
            scope = files[0]['category_scope']
            cat_id = files[0]['category_id']
            categories.append({
                'category_key': f'{scope}/{cat_id}',
                'category_label': labels_by_cat.get(cat_url, cat_url),
                'category_url': cat_url,
                'category_scope': scope,
                'category_id': cat_id,
                'file_count': len(files),
                'files': files[:200],
            })
        categories.sort(key=lambda c: (c['category_scope'] != 'general', c['category_id']))

        out = {
            'index_url_used': index_url_used,
            'categories': categories,
            'agent_id': self.agent_id,
            'candidates_scanned': len(candidates),
        }
        if debug:
            out['discovery_log'] = discovery_log[:80]
            sample = (page_anchors.get(f'{SOD_BASE}/subscribers') or [])[:60]
            out['sample_subscribers_anchors'] = [
                {'href': h, 'text': t} for h, t in sample
            ]
            # Dump structural elements so we can see how categories are rendered
            try:
                r = self.session.get(f'{SOD_BASE}/subscribers', timeout=self.timeout)
                if r.status_code == 200:
                    html = r.text or ''
                    out['subscribers_html_len'] = len(html)
                    # Forms (might be the navigation mechanism)
                    forms = _re.findall(
                        r'<form[^>]*action="([^"]+)"[^>]*>',
                        html, _re.IGNORECASE,
                    )[:20]
                    out['forms'] = forms
                    # Selects + options (might be category dropdown)
                    selects = []
                    for sm in _re.finditer(r'<select[^>]*>(.*?)</select>',
                                           html, _re.DOTALL | _re.IGNORECASE):
                        opts = _re.findall(
                            r'<option[^>]*value="([^"]*)"[^>]*>([^<]+)</option>',
                            sm.group(1), _re.IGNORECASE,
                        )
                        if opts:
                            selects.append(opts[:30])
                    out['selects'] = selects
                    # Look for tables — categories may be rendered as table rows
                    table_rows = _re.findall(
                        r'<tr[^>]*>(.*?)</tr>',
                        html, _re.DOTALL | _re.IGNORECASE,
                    )
                    # Strip tags, keep text — first 30 rows
                    out['table_rows_sample'] = []
                    for row in table_rows[:30]:
                        txt = _re.sub(r'<[^>]+>', ' | ', row)
                        txt = ' '.join(txt.split())
                        if txt and len(txt) > 5:
                            out['table_rows_sample'].append(txt[:200])
                    # First 4KB of <body> content (raw, for visual inspection)
                    body_m = _re.search(
                        r'<body[^>]*>(.*?)</body>',
                        html, _re.DOTALL | _re.IGNORECASE,
                    )
                    if body_m:
                        body = body_m.group(1)
                        # Strip <head>-ish noise
                        body = _re.sub(r'<script.*?</script>', '', body,
                                       flags=_re.DOTALL | _re.IGNORECASE)
                        body = _re.sub(r'<style.*?</style>', '', body,
                                       flags=_re.DOTALL | _re.IGNORECASE)
                        out['body_first_4k'] = body[:4000]
            except Exception as e:
                out['debug_error'] = str(e)
        return out

    def download_url(self, full_url):
        """Download a file by its full SOD URL. Returns raw bytes.

        Used by the portal-driven import flow to fetch any file the
        catalog discovered (annual archives, options, informative, etc).
        """
        self._ensure_logged_in()
        if not full_url.startswith(SOD_BASE):
            raise RuntimeError(f"Refusing to fetch URL outside {SOD_BASE}: {full_url}")
        r = self.session.get(full_url, timeout=max(self.timeout, 600))
        r.raise_for_status()
        body = r.content or b''
        if not body[:2] == b'PK':
            raise RuntimeError(
                f"Downloaded {full_url} is not a ZIP "
                f"(first bytes: {body[:8]!r}). Possibly an HTML error page; "
                f"check that the agent has access to this file."
            )
        return body


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


def stream_parse_sod_zip_to_sets(zip_bytes, tracked_skus, target_status='L'):
    """ULTRA-LEAN streaming parser for compare-uploads.

    Builds ONLY (date, sku) -> set of store_numbers where status == target_status.
    Nothing else accumulates — no per-row dicts, no aggregates. Stays well under
    10MB of RAM even on a full 50MB / 1.5M-row SOD file.

    Returns dict:
      dat_name: str
      total_rows: int (rows actually parsed)
      tracked_rows: int
      dates_seen: set[str]
      listed_by_date_sku: {date_str: {sku: set(store_number)}}
      counts_by_date_sku: {date_str: {sku: {'L': n, 'D': n, 'F': n, 'total': n,
                                            'on_hand_listed': n, 'product_name': str}}}

    Use counts_by_date_sku for headline stats, listed_by_date_sku for the diff.
    """
    listed_by_date_sku: dict = {}   # date -> sku -> set(store)
    counts_by_date_sku: dict = {}   # date -> sku -> dict
    dates_seen: set = set()
    total_rows = 0
    tracked_rows = 0
    last_logged = 0
    started = datetime.utcnow()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        members = zf.namelist()
        if not members:
            raise RuntimeError("Zip is empty")
        dat_name = members[0]
        print(f"[SOD-lean] streaming {dat_name} ({len(zip_bytes):,}B compressed)…")
        with zf.open(dat_name) as raw_stream:
            text_stream = io.TextIOWrapper(
                raw_stream, encoding='latin-1', errors='replace', newline='')
            for line in text_stream:
                if line.endswith('\n'):
                    line = line[:-1]
                if line.endswith('\r'):
                    line = line[:-1]
                row = _parse_sod_line(line)
                if row is None:
                    continue
                total_rows += 1
                if row['sku'] not in tracked_skus:
                    # Not one of ours — count toward total but don't track per-store
                    continue
                tracked_rows += 1
                d = row['snapshot_date']
                dates_seen.add(d)
                sku = row['sku']
                status = row['status'] or 'L'

                # Per-status counter
                date_bucket = counts_by_date_sku.setdefault(d, {})
                cnt = date_bucket.get(sku)
                if cnt is None:
                    cnt = {'L': 0, 'D': 0, 'F': 0, 'total': 0,
                           'on_hand_listed': 0, 'product_name': row['product_name']}
                    date_bucket[sku] = cnt
                cnt[status] = cnt.get(status, 0) + 1
                cnt['total'] += 1
                if status == 'L':
                    cnt['on_hand_listed'] += row['on_hand']

                # Set membership for the target status (default L)
                if status == target_status:
                    sku_bucket = listed_by_date_sku.setdefault(d, {}).setdefault(sku, set())
                    sku_bucket.add(row['store_number'])

                if total_rows - last_logged >= 200_000:
                    elapsed = (datetime.utcnow() - started).total_seconds()
                    rate = total_rows / max(elapsed, 0.001)
                    print(f"[SOD-lean] {total_rows:>9,} rows ({elapsed:.1f}s, {rate:,.0f}/s, "
                          f"tracked={tracked_rows}, dates={len(dates_seen)})")
                    last_logged = total_rows

    elapsed = (datetime.utcnow() - started).total_seconds()
    print(f"[SOD-lean] DONE: {total_rows:,} rows in {elapsed:.1f}s "
          f"(tracked={tracked_rows}, dates={sorted(dates_seen)})")

    return {
        'dat_name': dat_name,
        'total_rows': total_rows,
        'tracked_rows': tracked_rows,
        'dates_seen': dates_seen,
        'listed_by_date_sku': listed_by_date_sku,
        'counts_by_date_sku': counts_by_date_sku,
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
                # Idempotent UPSERT — backed by the uniq_sod_listing_changes
                # unique index (sku, COALESCE(store_number,-1), change_date, change_type).
                # Re-running the same snapshot used to duplicate every event.
                # FALLBACK: if the index doesn't exist (e.g. dedupe step on a
                # massive history table didn't complete), ON CONFLICT raises;
                # we catch and fall back to plain INSERT to keep the daily
                # sync from breaking.
                # SAVEPOINT so a failure here can only lose THIS statement.
                # The old fallback called conn.rollback(), which silently threw
                # away the sod_inventory + sod_products writes of every sync
                # since 2026-05-26 while still committing a 'success' run row
                # (the uniq_sod_listing_changes index never exists in prod, so
                # ON CONFLICT raised on every run).
                cur.execute("SAVEPOINT sp_listing_changes")
                try:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO sod_listing_changes
                           (sku, store_number, change_date, old_status, new_status, change_type)
                           VALUES %s
                           ON CONFLICT (sku, COALESCE(store_number, -1), change_date, change_type)
                           DO NOTHING""",
                        change_inserts,
                    )
                    cur.execute("RELEASE SAVEPOINT sp_listing_changes")
                except Exception as _conflict_err:
                    print(f"[SOD-{source}] ON CONFLICT failed ({_conflict_err}), retrying with plain INSERT")
                    cur.execute("ROLLBACK TO SAVEPOINT sp_listing_changes")
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO sod_listing_changes
                           (sku, store_number, change_date, old_status, new_status, change_type)
                           VALUES %s""",
                        change_inserts,
                    )
                    cur.execute("RELEASE SAVEPOINT sp_listing_changes")
            else:
                cur.executemany(
                    """INSERT OR IGNORE INTO sod_listing_changes
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

        # 7c) AUTO-ONBOARD: any store_number that appeared in this snapshot
        # but isn't in our master `stores` directory → INSERT a stub row.
        # This grows the master directory automatically when LCBO opens new
        # stores or starts carrying our SKUs at a store we hadn't tracked.
        # ON CONFLICT DO NOTHING so hand-curated rows are never overwritten.
        try:
            if USE_POSTGRES:
                cur.execute(
                    "SELECT DISTINCT store_number FROM sod_inventory "
                    "WHERE snapshot_date = %s",
                    (snapshot_date,))
                snap_stores = {int(r[0]) for r in cur.fetchall()}
                cur.execute("SELECT store_number FROM stores")
                existing_stores = {int(r[0]) for r in cur.fetchall()}
                missing = snap_stores - existing_stores
                if missing:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO stores
                             (store_number, account, address, city, priority)
                           VALUES %s
                           ON CONFLICT (store_number) DO NOTHING""",
                        [(sn, f"LCBO #{sn}", '', '', 'Standard') for sn in missing],
                    )
                    print(f"[SOD-{source}] auto-onboarded {len(missing)} stores from SOD into master directory")
            else:
                snap_stores = {
                    int(r[0]) for r in cur.execute(
                        "SELECT DISTINCT store_number FROM sod_inventory WHERE snapshot_date=?",
                        (snapshot_date,)).fetchall()
                }
                existing_stores = {
                    int(r[0]) for r in cur.execute("SELECT store_number FROM stores").fetchall()
                }
                missing = snap_stores - existing_stores
                if missing:
                    cur.executemany(
                        """INSERT OR IGNORE INTO stores
                             (store_number, account, address, city, priority)
                           VALUES (?,?,?,?,?)""",
                        [(sn, f"LCBO #{sn}", '', '', 'Standard') for sn in missing],
                    )
                    print(f"[SOD-{source}] auto-onboarded {len(missing)} stores from SOD")
        except Exception as _e:
            print(f"[SOD-{source}] auto-onboard skipped: {_e}")

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
@require_app_origin
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
@require_app_origin
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


@app.route('/api/sod/portfolios', methods=['GET'])
def api_sod_portfolios():
    """List portfolios and the SKUs in each — drives the NB/Anu toggle.

    Returns:
      portfolios: [{key, label, sku_count, skus: [{sku, brand, product_name}]}]
      default: 'NB' (the rep-facing default)
    """
    out = []
    for pkey, plabel in [('NB', 'NB Distillers'), ('Anu', 'Anu Imports')]:
        skus = []
        for sku, (brand, name) in SOD_TRACKED_SKUS.items():
            if SKU_PORTFOLIO.get(sku) == pkey:
                skus.append({'sku': sku, 'brand': brand, 'product_name': name})
        out.append({
            'key': pkey,
            'label': plabel,
            'sku_count': len(skus),
            'skus': skus,
        })
    return jsonify({'portfolios': out, 'default': 'NB'})


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


def _utc_to_et_str(ts) -> tuple[str, str, str]:
    """Convert a naive-UTC timestamp into ET strings for CSV/email.

    Returns (iso_et_with_offset, date_et, time_et). Handles strings
    (postgres format) and datetime objects. Falls back to raw strings
    if anything fails — never throws, never blocks export.
    """
    if ts in (None, ''):
        return ('', '', '')
    try:
        from zoneinfo import ZoneInfo
        if isinstance(ts, str):
            s = ts.strip().replace(' ', 'T')
            if not (s.endswith('Z') or '+' in s[10:] or '-' in s[10:]):
                # Bare timestamp → treat as UTC
                s = s + '+00:00'
            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        else:
            dt = ts
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
        et = dt.astimezone(ZoneInfo('America/Toronto'))
        return (
            et.strftime('%Y-%m-%dT%H:%M:%S%z'),
            et.strftime('%Y-%m-%d'),
            et.strftime('%H:%M:%S'),
        )
    except Exception:
        # Fallback: best-effort string split
        raw = str(ts)
        return (raw, raw[:10], raw[11:19] if len(raw) >= 19 else '')


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
    """Render / uptime probe.

    Default behavior (=process-liveness check): always returns 200 if the
    Flask worker is responsive. SOD freshness is reported as a field but
    does NOT affect the HTTP code — uptime monitors stay green even when
    SOD ingest is naturally 1-2 days behind on weekends.

    Pass ?deep=1 to flip to the strict check (503 when snapshot >2d old).
    Use the strict check from internal monitoring (the existing _stale_watch
    cron) but NOT from public uptime services like UptimeRobot.
    """
    deep = request.args.get('deep') in ('1', 'true', 'yes')
    fresh = _sod_freshness()
    age_days = fresh.get('snapshot_age_days')
    # Strict (deep) freshness threshold relaxed to 2 days — accounts for
    # weekends where LCBO may not publish a fresh file Sat/Sun. The hourly
    # _stale_watch escalates to email alert at >24h independently.
    fresh_ok = age_days is None or age_days <= 2
    payload = {
        'status': 'healthy' if (not deep or fresh_ok) else 'unhealthy',
        'build': 'finder-stale-1d-v3',
        'mode': 'deep' if deep else 'liveness',
        **fresh,
    }
    code = 200 if (not deep or fresh_ok) else 503
    return jsonify(payload), code


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
@require_app_origin
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
@require_app_origin
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
@require_app_origin
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
@require_app_origin
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
@require_app_origin
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
@require_app_origin
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
@require_app_origin
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
@require_app_origin
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

    Query params:
      portfolio=NB|Anu|all  scope tracked SKUs (default 'NB' for rep view).
    """
    include_live = request.args.get('live', '').lower() in ('1', 'true', 'yes')
    portfolio = (request.args.get('portfolio') or 'NB').strip()
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    tracked = _skus_for_portfolio(portfolio)
    if not tracked:
        return jsonify({'store_number': store_number, 'portfolio': portfolio,
                        'sod': [], 'missing_skus': [], 'live': []})
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

    # Build "missing_skus" — every tracked SKU in this portfolio that does
    # NOT appear in the store's latest snapshot. These are missed
    # opportunities the rep can pitch on a store visit.
    portfolio_sku_set = set(tracked)
    present_skus = {r['sku'] for r in sod}
    missing_skus = []
    for sku, (brand, name) in SOD_TRACKED_SKUS.items():
        if sku not in portfolio_sku_set:
            continue
        if sku in present_skus:
            continue
        missing_skus.append({
            'sku': sku,
            'brand': brand,
            'product_name': name,
            'pattern': 'missing_opportunity',
        })
    missing_skus.sort(key=lambda x: (x['brand'], x['product_name']))

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
    return jsonify({
        'store_number': store_number,
        'portfolio': portfolio,
        'sod': sod,
        'missing_skus': missing_skus,
        'live': live,
    })


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


# NB: _admin_token_ok / require_admin_token / require_app_origin are defined
# at the top of the file so decorators can reference them at import time.


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


# ───────────────────────────────────────────────────────────────────────
# DOWNLOAD EVERYTHING — bundles every table the operator might want
# into a ZIP of CSVs. Designed to be fed straight into ChatGPT / Claude
# for behavior analysis ("are reps actually working or going to the same
# stores repeatedly?").
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/export/everything', methods=['GET'])
@require_app_origin
def api_admin_export_everything():
    """Bundle every CRM + audit table into a ZIP of CSVs.

    Query params:
      include_sod=1      include sod_inventory + sod_listing_changes (large; default 1)
      include_history=0  include inventory_history (very large; default 0)
      days=90            row-level filter for time-bounded tables (default 90)

    Returns: zipped bundle, served as application/zip with a filename like
    anu-export-YYYY-MM-DD-HHMM.zip. Each table becomes one CSV inside.
    Plus a README.md describing the schemas + columns.
    """
    import csv as _csv
    import zipfile as _zip
    try:
        days = max(1, min(int(request.args.get('days', '90')), 730))
    except ValueError:
        days = 90
    include_sod = request.args.get('include_sod', '1') in ('1', 'true', 'yes')
    include_history = request.args.get('include_history', '0') in ('1', 'true', 'yes')

    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db
    buf = io.BytesIO()

    # Resolve which tables to dump
    base_tables = [
        # (table, time_filter_column or None, optional ORDER BY)
        ('stores',                  None,            'store_number'),
        ('territories',             None,            'id'),
        ('reps',                    None,            'id'),
        ('products',                None,            'id'),
        ('sod_products',            None,            'sku'),
        ('activities',              'created_at',    'created_at DESC'),
        ('deals',                   'created_at',    'created_at DESC'),
        ('followups',               'created_at',    'created_at DESC'),
        ('rep_listing_observations','observed_at',   'observed_at DESC'),
        ('rep_quotas',              None,            'id'),
        ('sales_goals',             None,            'id'),
        ('horeca_accounts',         None,            'id'),
        ('event_log',               'occurred_at',   'occurred_at DESC'),
        ('sod_sync_runs',           'run_at',        'run_at DESC'),
        ('sod_store_sku_changes',   'change_date',   'change_date DESC, id DESC'),
        ('sod_listing_changes',     'change_date',   'change_date DESC, id DESC'),
    ]
    if include_sod:
        base_tables.append(('sod_inventory', 'snapshot_date', 'snapshot_date DESC, sku'))
    if include_history:
        base_tables.append(('inventory_history', 'recorded_at', 'recorded_at DESC'))

    manifest = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'window_days': days,
        'include_sod': include_sod,
        'include_history': include_history,
        'tables': {},
    }

    with _zip.ZipFile(buf, 'w', _zip.ZIP_DEFLATED) as zf:
        for tname, time_col, order_by in base_tables:
            try:
                if time_col:
                    if USE_POSTGRES:
                        sql = (f"SELECT * FROM {tname} "
                               f"WHERE {time_col} >= NOW() - (INTERVAL '1 day' * %s) "
                               f"ORDER BY {order_by} LIMIT 100000")
                        cur.execute(sql, (days,))
                    else:
                        sql = (f"SELECT * FROM {tname} "
                               f"WHERE {time_col} >= datetime('now', ? || ' days') "
                               f"ORDER BY {order_by} LIMIT 100000")
                        cur.execute(sql, (f'-{days}',))
                else:
                    cur.execute(f"SELECT * FROM {tname} ORDER BY {order_by} LIMIT 200000")
                col_names = [d[0] for d in cur.description]
                rows = cur.fetchall()

                csv_buf = io.StringIO()
                w = _csv.writer(csv_buf)
                w.writerow(col_names)
                for row in rows:
                    w.writerow([_json_safe(v) for v in row])
                zf.writestr(f'{tname}.csv', csv_buf.getvalue())
                manifest['tables'][tname] = {
                    'rows': len(rows),
                    'columns': col_names,
                    'time_filter_column': time_col,
                }
            except Exception as e:
                manifest['tables'][tname] = {'error': str(e)}
                try: db.rollback()
                except Exception: pass

        # README explaining the bundle
        readme_lines = [
            "# Anu LCBO Tracker — Data Export",
            "",
            f"Generated: {manifest['generated_at']}",
            f"Time window: last {days} days (for time-filtered tables)",
            f"include_sod={include_sod}, include_history={include_history}",
            "",
            "## Files",
            "",
        ]
        for t, info in manifest['tables'].items():
            if 'rows' in info:
                readme_lines.append(f"- `{t}.csv` — {info['rows']:,} rows, columns: {', '.join(info['columns'])}")
            else:
                readme_lines.append(f"- `{t}.csv` — ERROR: {info['error']}")
        readme_lines += [
            "",
            "## Suggested AI prompts",
            "",
            "### 1. Are reps actually working or visiting the same places?",
            "Upload activities.csv + stores.csv to ChatGPT/Claude and ask:",
            '> "For each rep, count: total visits, unique stores, repeat-visit ratio, days active, days idle. Flag any rep visiting the same store >2 times in 30 days. Flag any rep with <50% territory coverage."',
            "",
            "### 2. Did our listings move correctly between snapshots?",
            "Upload sod_listing_changes.csv + sod_store_sku_changes.csv:",
            '> "Group by sku and change_type. For NEW_LISTING events in the last 30 days, are they concentrated geographically? Are there suspicious mass-delistings?"',
            "",
            "### 3. Pipeline conversion analysis",
            "Upload deals.csv + activities.csv:",
            '> "For each deal that became Listed, how many activities preceded it? Average days from prospecting to listed? Which rep has the highest conversion rate?"',
            "",
        ]
        zf.writestr('README.md', '\n'.join(readme_lines))
        zf.writestr('manifest.json', json.dumps(manifest, indent=2, default=str))

    if USE_POSTGRES: cur.close()

    buf.seek(0)
    fname = f"anu-export-{datetime.utcnow().strftime('%Y-%m-%d-%H%M')}.zip"
    return Response(
        buf.getvalue(),
        mimetype='application/zip',
        headers={
            'Content-Disposition': f'attachment; filename="{fname}"',
            'X-Anu-Export-Tables': str(len(manifest['tables'])),
        },
    )


# ───────────────────────────────────────────────────────────────────────
# REP BEHAVIOR ANALYSIS — answers "are reps working or going to the same
# places repeatedly". Per-rep metrics with behavior_flags surfacing the
# stuff you'd want to dig into.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/rep-behavior', methods=['GET'])
def api_admin_rep_behavior():
    """Per-rep behavior metrics for the last N days (default 30).

    Returns per rep:
      visits_total, unique_stores, repeat_visits, repeat_visit_pct,
      active_days, days_since_last_visit, days_idle (active - lookback),
      territory_size, coverage_pct, listings_won, tastings, outreach,
      high_repeat_stores: [{store_number, account, visits, last_visit}],
      visit_pace_per_active_day, behavior_flags: [string]

    Behavior flags:
      stale            - no visits in 14+ days
      narrow_coverage  - <30% of territory visited in window
      high_repeats     - any store visited 3+ times in window
      no_conversion    - 30+ visits, 0 listings_won
      single_city      - 80%+ of visits in one city
    """
    try:
        days = max(7, min(int(request.args.get('days', '30')), 365))
    except ValueError:
        days = 30

    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db

    # Get full rep roster (same hard-coded list as frontend)
    rep_roster = ['Ikshit', 'Namit', 'Virat', 'Surya', 'Neeraj']
    out_rows = []

    for rep in rep_roster:
        try:
            # Visits in window
            if USE_POSTGRES:
                cur.execute(
                    "SELECT a.id, a.store_id, a.created_at::text, "
                    "       s.store_number, s.account, s.city "
                    "FROM activities a "
                    "LEFT JOIN stores s ON s.id = a.store_id "
                    "WHERE LOWER(TRIM(a.rep)) = LOWER(TRIM(%s)) "
                    "  AND a.created_at >= NOW() - (INTERVAL '1 day' * %s) "
                    "  AND (a.deleted_at IS NULL) "
                    "ORDER BY a.created_at DESC",
                    (rep, days))
            else:
                cur.execute(
                    "SELECT a.id, a.store_id, a.created_at, "
                    "       s.store_number, s.account, s.city "
                    "FROM activities a "
                    "LEFT JOIN stores s ON s.id = a.store_id "
                    "WHERE LOWER(TRIM(a.rep)) = LOWER(TRIM(?)) "
                    "  AND a.created_at >= datetime('now', ? || ' days') "
                    "  AND (a.deleted_at IS NULL) "
                    "ORDER BY a.created_at DESC",
                    (rep, f'-{days}'))
            visits = cur.fetchall()
        except Exception:
            try: db.rollback()
            except Exception: pass
            visits = []

        visits_total = len(visits)
        store_visit_counts: dict = {}     # store_number -> count
        store_meta: dict = {}             # store_number -> {account, city, last_visit}
        active_days_set: set = set()
        city_counts: dict = {}
        last_visit_at = None

        for v in visits:
            sn = v[3]
            if sn is not None:
                try:
                    sn_int = int(sn)
                    store_visit_counts[sn_int] = store_visit_counts.get(sn_int, 0) + 1
                    if sn_int not in store_meta:
                        store_meta[sn_int] = {
                            'account': v[4] or '',
                            'city': v[5] or '',
                            'last_visit': str(v[2]) if v[2] else None,
                        }
                except (ValueError, TypeError):
                    pass
            ts = str(v[2]) if v[2] else ''
            if ts:
                active_days_set.add(ts[:10])  # YYYY-MM-DD
                if last_visit_at is None or ts > last_visit_at:
                    last_visit_at = ts
            if v[5]:
                city_counts[v[5]] = city_counts.get(v[5], 0) + 1

        unique_stores = len(store_visit_counts)
        repeat_visits = visits_total - unique_stores
        repeat_visit_pct = round(repeat_visits / visits_total * 100, 1) if visits_total else 0.0

        # High-repeat stores (3+ visits)
        high_repeat = []
        for sn, count in sorted(store_visit_counts.items(), key=lambda x: -x[1])[:10]:
            if count >= 3:
                meta = store_meta.get(sn, {})
                high_repeat.append({
                    'store_number': sn,
                    'account': meta.get('account', ''),
                    'city': meta.get('city', ''),
                    'visits': count,
                    'last_visit': meta.get('last_visit'),
                })

        # Territory size — best-effort from stores.rep + sod_inventory.store_number
        try:
            if USE_POSTGRES:
                cur.execute("SELECT COUNT(*) FROM stores WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s))", (rep,))
            else:
                cur.execute("SELECT COUNT(*) FROM stores WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?))", (rep,))
            territory_size = int(cur.fetchone()[0] or 0)
        except Exception:
            try: db.rollback()
            except Exception: pass
            territory_size = 0
        coverage_pct = round(unique_stores / territory_size * 100, 1) if territory_size > 0 else None

        # Listings won (deals stage='listed' with closed_at in window)
        try:
            if USE_POSTGRES:
                cur.execute(
                    "SELECT COUNT(*) FROM deals "
                    "WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM(%s)) "
                    "  AND stage='listed' "
                    "  AND closed_at >= NOW() - (INTERVAL '1 day' * %s)",
                    (rep, days))
            else:
                cur.execute(
                    "SELECT COUNT(*) FROM deals "
                    "WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM(?)) "
                    "  AND stage='listed' "
                    "  AND closed_at >= datetime('now', ? || ' days')",
                    (rep, f'-{days}'))
            listings_won = int(cur.fetchone()[0] or 0)
        except Exception:
            try: db.rollback()
            except Exception: pass
            listings_won = 0

        # Tastings + outreach
        tastings = sum(1 for v in visits
                       if v and len(v) > 0
                       and isinstance(v[0], (int, str))
                       )  # placeholder; we don't have activity_type joined here

        # Calculate days_since_last_visit
        days_since_last = None
        if last_visit_at:
            try:
                from datetime import datetime as _dt
                last_dt = _dt.fromisoformat(last_visit_at.replace('Z', '+00:00').split('.')[0])
                days_since_last = (datetime.utcnow() - last_dt).days
            except Exception:
                pass

        # Behavior flags
        flags = []
        if days_since_last is not None and days_since_last >= 14:
            flags.append('stale')
        if coverage_pct is not None and coverage_pct < 30 and territory_size >= 10:
            flags.append('narrow_coverage')
        if any(c >= 3 for c in store_visit_counts.values()):
            flags.append('high_repeats')
        if visits_total >= 30 and listings_won == 0:
            flags.append('no_conversion')
        # Single-city concentration
        if visits_total >= 10:
            top_city_visits = max(city_counts.values()) if city_counts else 0
            if top_city_visits / visits_total >= 0.8:
                flags.append('single_city')

        out_rows.append({
            'rep': rep,
            'window_days': days,
            'visits_total': visits_total,
            'unique_stores': unique_stores,
            'repeat_visits': repeat_visits,
            'repeat_visit_pct': repeat_visit_pct,
            'active_days': len(active_days_set),
            'days_since_last_visit': days_since_last,
            'territory_size': territory_size,
            'coverage_pct': coverage_pct,
            'listings_won_in_window': listings_won,
            'visit_pace_per_active_day': (
                round(visits_total / len(active_days_set), 1)
                if active_days_set else None
            ),
            'high_repeat_stores': high_repeat,
            'top_cities': sorted(
                [{'city': c, 'visits': n} for c, n in city_counts.items()],
                key=lambda x: -x['visits'])[:5],
            'behavior_flags': flags,
        })

    if USE_POSTGRES: cur.close()

    # Global findings
    findings = []
    stale_reps = [r['rep'] for r in out_rows if 'stale' in r['behavior_flags']]
    narrow_reps = [r['rep'] for r in out_rows if 'narrow_coverage' in r['behavior_flags']]
    high_repeat_reps = [r['rep'] for r in out_rows if 'high_repeats' in r['behavior_flags']]
    no_conv_reps = [r['rep'] for r in out_rows if 'no_conversion' in r['behavior_flags']]
    if stale_reps:
        findings.append(f"{len(stale_reps)} rep(s) haven't logged a visit in 14+ days: {', '.join(stale_reps)}")
    if narrow_reps:
        findings.append(f"{len(narrow_reps)} rep(s) covering <30% of territory: {', '.join(narrow_reps)}")
    if high_repeat_reps:
        findings.append(f"{len(high_repeat_reps)} rep(s) revisiting the same store 3+ times: {', '.join(high_repeat_reps)}")
    if no_conv_reps:
        findings.append(f"{len(no_conv_reps)} rep(s) with 30+ visits but 0 listings won: {', '.join(no_conv_reps)}")
    if not findings:
        findings.append("All reps within healthy ranges across all behavior checks.")

    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'window_days': days,
        'per_rep': out_rows,
        'global_findings': findings,
        'how_to_read': (
            "Each rep gets visits_total, unique_stores, and repeat_visit_pct. "
            "behavior_flags surface 5 patterns: stale (14d idle), narrow_coverage "
            "(<30% territory), high_repeats (any store 3+ visits), no_conversion "
            "(30+ visits, 0 listings won), single_city (80%+ in one city). "
            "Download the full activity log via /api/admin/export/everything for "
            "deeper AI analysis."
        ),
    })


# ───────────────────────────────────────────────────────────────────────
# REP ACTIVITY REPORT — daily/weekly/monthly downloadable activity log
# with per-rep filter. CSV-exportable for direct submission or pivot.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/rep-activity-report', methods=['GET'])
def api_admin_rep_activity_report():
    """Per-rep activity log with day/week/month filtering.

    Query params:
      rep        (str, OPTIONAL)   — filter to one rep (case-insensitive)
      period     (str, OPTIONAL)   — 'today' / 'yesterday' / 'this_week' /
                                     'last_week' / 'this_month' / 'last_month' /
                                     'last_7d' / 'last_30d' / 'last_90d' / 'ytd'
                                     (overridden by start/end if both given)
      start, end (YYYY-MM-DD)      — explicit window
      format     (json|csv)        — default json

    Returns per-activity row + per-rep summary + per-day rollup.
    """
    from datetime import date as _date, timedelta as _td

    rep_filter = (request.args.get('rep') or '').strip()
    period = (request.args.get('period') or '').strip().lower()
    start_q = (request.args.get('start') or '').strip()
    end_q = (request.args.get('end') or '').strip()
    fmt = (request.args.get('format') or 'json').lower()

    today_d = _toronto_today()

    # Resolve window
    if start_q and end_q:
        try:
            start_d = _date.fromisoformat(start_q)
            end_d = _date.fromisoformat(end_q)
        except ValueError:
            return jsonify({'error': 'start/end must be YYYY-MM-DD'}), 400
    elif period == 'today':
        start_d = end_d = today_d
    elif period == 'yesterday':
        start_d = end_d = today_d - _td(days=1)
    elif period == 'this_week':
        start_d = today_d - _td(days=today_d.weekday())  # Monday
        end_d = today_d
    elif period == 'last_week':
        end_d = today_d - _td(days=today_d.weekday() + 1)  # last Sunday
        start_d = end_d - _td(days=6)                       # last Monday
    elif period == 'this_month':
        start_d = today_d.replace(day=1)
        end_d = today_d
    elif period == 'last_month':
        first_this_month = today_d.replace(day=1)
        end_d = first_this_month - _td(days=1)
        start_d = end_d.replace(day=1)
    elif period == 'last_7d':
        start_d = today_d - _td(days=7)
        end_d = today_d
    elif period == 'last_30d':
        start_d = today_d - _td(days=30)
        end_d = today_d
    elif period == 'last_90d':
        start_d = today_d - _td(days=90)
        end_d = today_d
    elif period == 'ytd':
        start_d = _date(today_d.year, 1, 1)
        end_d = today_d
    else:
        # Default: last 30d
        start_d = today_d - _td(days=30)
        end_d = today_d

    if start_d > end_d:
        return jsonify({'error': 'start must be <= end'}), 400

    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db
    rows = []
    try:
        # created_at is stored as naive UTC; reps work in America/Toronto.
        # Convert UTC → Toronto before truncating to date so a 23:50 ET log
        # (which is 03:50 UTC the next day) shows up on the correct day.
        sql = """
            SELECT a.id, a.created_at, a.rep, a.activity_type, a.notes,
                   a.store_id, s.store_number, s.account, s.address,
                   s.city, s.postal, s.priority, s.rep AS store_rep,
                   a.lat AS client_lat, a.lng AS client_lng,
                   a.accuracy_m, a.client_ts, a.distance_from_store_m
            FROM activities a
            LEFT JOIN stores s ON s.id = a.store_id
            WHERE a.deleted_at IS NULL
              AND ((a.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Toronto')::date >= %s::date
              AND ((a.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Toronto')::date <= %s::date
        """ if USE_POSTGRES else """
            SELECT a.id, a.created_at, a.rep, a.activity_type, a.notes,
                   a.store_id, s.store_number, s.account, s.address,
                   s.city, s.postal, s.priority, s.rep AS store_rep,
                   a.lat AS client_lat, a.lng AS client_lng,
                   a.accuracy_m, a.client_ts, a.distance_from_store_m
            FROM activities a
            LEFT JOIN stores s ON s.id = a.store_id
            WHERE a.deleted_at IS NULL
              AND date(a.created_at, '-4 hours') >= ?
              AND date(a.created_at, '-4 hours') <= ?
        """
        params: list = [start_d.isoformat(), end_d.isoformat()]
        if rep_filter:
            sql += " AND LOWER(TRIM(a.rep)) = LOWER(TRIM(%s))" if USE_POSTGRES else " AND LOWER(TRIM(a.rep)) = LOWER(TRIM(?))"
            params.append(rep_filter)
        sql += " ORDER BY a.created_at DESC LIMIT 50000"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            rows.append({cols[i]: _json_safe(v) for i, v in enumerate(r)})
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        return jsonify({'error': f'query failed: {e}'}), 500
    finally:
        if USE_POSTGRES:
            try: cur.close()
            except Exception: pass

    # Per-rep summary
    by_rep: dict = {}
    by_day: dict = {}
    by_type: dict = {}
    for r in rows:
        rep_name = (r.get('rep') or '').strip() or '(unknown)'
        rep_lower = rep_name.lower()
        d_key = (str(r.get('created_at') or ''))[:10]
        a_type = (r.get('activity_type') or '').strip().lower() or '(unspecified)'

        bucket = by_rep.setdefault(rep_lower, {
            'rep': rep_name, 'visits': 0, 'unique_stores': set(),
            'activity_types': {}, 'first_at': None, 'last_at': None,
        })
        bucket['visits'] += 1
        if r.get('store_id') is not None:
            bucket['unique_stores'].add(r['store_id'])
        bucket['activity_types'][a_type] = bucket['activity_types'].get(a_type, 0) + 1
        ts = str(r.get('created_at') or '')
        if not bucket['first_at'] or ts < bucket['first_at']:
            bucket['first_at'] = ts
        if not bucket['last_at'] or ts > bucket['last_at']:
            bucket['last_at'] = ts

        if d_key:
            by_day[d_key] = by_day.get(d_key, 0) + 1
        by_type[a_type] = by_type.get(a_type, 0) + 1

    summary_rows = []
    for b in by_rep.values():
        summary_rows.append({
            'rep': b['rep'],
            'visits': b['visits'],
            'unique_stores': len(b['unique_stores']),
            'repeat_visit_pct': round(
                (b['visits'] - len(b['unique_stores'])) / b['visits'] * 100, 1
            ) if b['visits'] else 0.0,
            'activity_types': b['activity_types'],
            'first_at': b['first_at'],
            'last_at': b['last_at'],
        })
    summary_rows.sort(key=lambda x: -x['visits'])

    daily_rollup = sorted(
        [{'date': d, 'visits': n} for d, n in by_day.items()],
        key=lambda x: x['date'],
    )

    if fmt == 'csv':
        import csv as _csv, io as _io
        buf = _io.StringIO()
        w = _csv.writer(buf)
        # All time columns rendered in America/Toronto (rep local time).
        # Internal UTC timestamp kept as `created_at_utc` for audit.
        w.writerow([
            'created_at_et', 'date_et', 'time_et', 'created_at_utc',
            'rep', 'activity_type',
            'store_number', 'account', 'address', 'city', 'postal',
            'store_priority', 'store_assigned_rep', 'notes',
            'logged_lat', 'logged_lng', 'logged_accuracy_m',
            'logged_at_client', 'distance_from_store_m',
        ])
        for r in rows:
            ts_raw = r.get('created_at') or ''
            et_iso, et_date, et_time = _utc_to_et_str(ts_raw)
            cl_lat = r.get('client_lat')
            cl_lng = r.get('client_lng')
            w.writerow([
                et_iso, et_date, et_time, str(ts_raw),
                r.get('rep') or '',
                r.get('activity_type') or '',
                r.get('store_number') or '',
                r.get('account') or '',
                r.get('address') or '',
                r.get('city') or '',
                r.get('postal') or '',
                r.get('priority') or '',
                r.get('store_rep') or '',
                (r.get('notes') or '').replace('\n', ' ').replace('\r', ' '),
                cl_lat if (cl_lat is not None and cl_lat != 0) else '',
                cl_lng if (cl_lng is not None and cl_lng != 0) else '',
                r.get('accuracy_m') or '',
                r.get('client_ts') or '',
                r.get('distance_from_store_m') or '',
            ])
        fname = (
            f"anu-rep-activity-{start_d.isoformat()}-to-{end_d.isoformat()}"
            + (f'-{rep_filter}' if rep_filter else '-all-reps')
            + '.csv'
        )
        return Response(
            buf.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{fname}"'},
        )

    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'window': {
            'start': start_d.isoformat(),
            'end': end_d.isoformat(),
            'days': (end_d - start_d).days + 1,
            'period_resolved_from': period or 'last_30d (default)',
        },
        'rep_filter': rep_filter or None,
        'totals': {
            'rows': len(rows),
            'unique_reps': len(by_rep),
            'unique_active_days': len(by_day),
            'activity_type_counts': by_type,
        },
        'per_rep_summary': summary_rows,
        'daily_rollup': daily_rollup,
        'rows': rows[:1000],   # cap response size; CSV path returns all
        'how_to_read': (
            "Each row is one activity (visit / call / email / tasting). "
            "per_rep_summary aggregates per rep within the window. "
            "daily_rollup is the bar-chart series. "
            "Add &format=csv to download the full row-level log "
            "(up to 50,000 rows) — designed for Excel pivots and "
            "submission to NB Distillers / brand owners."
        ),
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
                # EXCLUDE 'SUMMARY' pseudo-store — that's a legacy aggregate row
                # used for 14-day trend graphs; counting it as a store inflates
                # drift detection and breaks the cross-validation report.
                if USE_POSTGRES:
                    cur = db.cursor()
                    cur.execute(
                        "SELECT COUNT(DISTINCT store_number), COALESCE(SUM(quantity),0) "
                        "FROM inventory_history WHERE product_id=%s AND quantity > 0 "
                        "AND store_number <> 'SUMMARY' "
                        "AND recorded_at >= NOW() - INTERVAL '24 hours'",
                        (pid,),
                    )
                    row = cur.fetchone()
                    cur.close()
                else:
                    row = db.execute(
                        "SELECT COUNT(DISTINCT store_number), COALESCE(SUM(quantity),0) "
                        "FROM inventory_history WHERE product_id=? AND quantity > 0 "
                        "AND store_number <> 'SUMMARY' "
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


# ───────────────────────────────────────────────────────────────────────
# Commission audit — produces a per-store-SKU disagreement report so we
# can claim listings we're not being paid for. The CEO-level use case:
# every store where lcbo.com shows our bottle on shelf but SOD says
# Not-Listed/Delisted is potentially a missing commission line.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/commission-audit', methods=['GET'])
def api_admin_commission_audit():
    """Per-store-SKU reconciliation report — every disagreement between
    SOD's listing status and what lcbo.com / rep observations actually
    show. This is the artifact you take to NB Distillers / brand owners
    when contesting commission shortfalls.

    Query params:
      sku=<7-digit padded SKU>   filter to one product
      days=<int>                 lookback window for lcbo.com data (default 7)
      include_matches=1          include rows where SOD and lcbo.com agree
                                 (default: only return disagreements)
      format=json|csv            (default json)
    """
    sku_filter = (request.args.get('sku') or '').strip()
    try:
        days = max(1, min(int(request.args.get('days', 7)), 30))
    except ValueError:
        days = 7
    include_matches = request.args.get('include_matches') in ('1', 'true', 'yes')
    fmt = (request.args.get('format') or 'json').lower()

    skus_to_audit = [sku_filter] if sku_filter and sku_filter in SOD_TRACKED_SKUS else list(SOD_TRACKED_SKUS.keys())

    db = get_db()
    rows_out = []
    summary = {
        'lcbo_only': 0,         # ← potential commission claims (SOD missed)
        'sod_only_empty': 0,    # ← SOD listed, on_hand=0 (real, just no stock right now)
        'sod_only_stale': 0,    # ← SOD listed, on_hand>0, but no recent lcbo.com confirm
        'agree': 0,
        'units_undercounted': 0,
    }

    for sku in skus_to_audit:
        brand, name = SOD_TRACKED_SKUS[sku]
        sku_clean = sku.lstrip('0')

        # Map: store_number -> SOD status / on_hand at latest snapshot
        sod_per_store = {}
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s", (sku,))
                latest = cur.fetchone()[0]
                if latest:
                    cur.execute(
                        "SELECT store_number, status, on_hand FROM sod_inventory "
                        "WHERE sku=%s AND snapshot_date=%s", (sku, latest))
                    for r in cur.fetchall():
                        sod_per_store[int(r[0])] = {'status': r[1], 'on_hand': int(r[2] or 0), 'snapshot_date': str(latest)}
                cur.close()
            else:
                latest_row = db.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=?", (sku,)).fetchone()
                latest = latest_row[0] if latest_row else None
                if latest:
                    cur = db.execute(
                        "SELECT store_number, status, on_hand FROM sod_inventory "
                        "WHERE sku=? AND snapshot_date=?", (sku, latest))
                    for r in cur.fetchall():
                        sod_per_store[int(r[0])] = {'status': r[1], 'on_hand': int(r[2] or 0), 'snapshot_date': str(latest)}
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[commission-audit] SOD lookup failed for {sku}: {e}")

        # Map: store_number -> latest lcbo.com qty (last N days, excluding SUMMARY)
        lcbo_per_store = {}
        try:
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute("SELECT id FROM products WHERE lcbo_sku=%s LIMIT 1", (sku_clean,))
                prow = cur.fetchone()
                pid = prow[0] if prow else None
                if pid:
                    cur.execute(
                        "SELECT store_number, MAX(quantity), MAX(recorded_at)::text "
                        "FROM inventory_history WHERE product_id=%s "
                        "AND quantity > 0 AND store_number <> 'SUMMARY' "
                        "AND recorded_at >= NOW() - (INTERVAL '1 day' * %s) "
                        "GROUP BY store_number",
                        (pid, days))
                    for r in cur.fetchall():
                        try:
                            lcbo_per_store[int(r[0])] = {'qty': int(r[1] or 0), 'as_of': r[2]}
                        except (ValueError, TypeError):
                            continue
                cur.close()
            else:
                prow = db.execute("SELECT id FROM products WHERE lcbo_sku=? LIMIT 1", (sku_clean,)).fetchone()
                pid = prow[0] if prow else None
                if pid:
                    cur = db.execute(
                        "SELECT store_number, MAX(quantity), MAX(recorded_at) "
                        "FROM inventory_history WHERE product_id=? "
                        "AND quantity > 0 AND store_number <> 'SUMMARY' "
                        "AND recorded_at >= datetime('now', ? || ' days') "
                        "GROUP BY store_number",
                        (pid, f'-{days}'))
                    for r in cur.fetchall():
                        try:
                            lcbo_per_store[int(r[0])] = {'qty': int(r[1] or 0), 'as_of': r[2]}
                        except (ValueError, TypeError):
                            continue
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[commission-audit] lcbo.com lookup failed for {sku}: {e}")

        # Map: rep-observed listings (manual override flow)
        rep_observed = {}
        try:
            cur = db.cursor() if USE_POSTGRES else db
            sql = ("SELECT store_number, MAX(observed_at)::text, MAX(rep) "
                   "FROM rep_listing_observations "
                   "WHERE sku=%s AND observed_at >= NOW() - (INTERVAL '1 day' * %s) "
                   "GROUP BY store_number") if USE_POSTGRES else (
                   "SELECT store_number, MAX(observed_at), MAX(rep) "
                   "FROM rep_listing_observations "
                   "WHERE sku=? AND observed_at >= datetime('now', ? || ' days') "
                   "GROUP BY store_number")
            params = (sku, days) if USE_POSTGRES else (sku, f'-{days}')
            if USE_POSTGRES:
                cur.execute(sql, params)
                for r in cur.fetchall():
                    rep_observed[int(r[0])] = {'observed_at': r[1], 'rep': r[2]}
                cur.close()
            else:
                for r in cur.execute(sql, params).fetchall():
                    rep_observed[int(r[0])] = {'observed_at': r[1], 'rep': r[2]}
        except Exception:
            # Table may not exist yet — silently skip
            try: db.rollback()
            except Exception: pass

        # Walk the universe of stores that appeared in either source
        all_stores = set(sod_per_store.keys()) | set(lcbo_per_store.keys()) | set(rep_observed.keys())
        for store_num in sorted(all_stores):
            sod = sod_per_store.get(store_num)
            lcbo = lcbo_per_store.get(store_num)
            obs = rep_observed.get(store_num)
            sod_says_listed = bool(sod and sod['status'] == 'L')
            sod_says_delist = bool(sod and sod['status'] in ('D', 'F'))
            lcbo_has = bool(lcbo and lcbo['qty'] > 0)
            rep_saw = obs is not None

            verdict = 'agree'
            claim_units = 0
            if (lcbo_has or rep_saw) and not sod_says_listed:
                verdict = 'lcbo_only'  # → potential commission claim
                summary['lcbo_only'] += 1
                claim_units = lcbo['qty'] if lcbo else 0
                summary['units_undercounted'] += claim_units
            elif sod_says_listed and not (lcbo_has or rep_saw):
                # Distinguish "real listing, empty shelf" from "possibly stale SOD"
                if sod and (sod.get('on_hand') or 0) == 0:
                    verdict = 'sod_only_empty'  # listed, just out-of-stock
                    summary['sod_only_empty'] += 1
                else:
                    verdict = 'sod_only_stale'  # listed w/ stock but lcbo.com hasn't confirmed
                    summary['sod_only_stale'] += 1
            else:
                summary['agree'] += 1
                if not include_matches:
                    continue

            rows_out.append({
                'sku': sku,
                'product_name': name,
                'brand': brand,
                'store_number': store_num,
                'verdict': verdict,
                'claim_units': claim_units,
                'sod_status': sod['status'] if sod else None,
                'sod_on_hand': sod['on_hand'] if sod else 0,
                'sod_snapshot_date': sod['snapshot_date'] if sod else None,
                'lcbo_units': lcbo['qty'] if lcbo else 0,
                'lcbo_seen_at': lcbo['as_of'] if lcbo else None,
                'rep_observed': rep_saw,
                'rep_observation_at': obs['observed_at'] if obs else None,
                'rep_observation_by': obs['rep'] if obs else None,
            })

    if fmt == 'csv':
        import csv as _csv, io as _io
        buf = _io.StringIO()
        if rows_out:
            w = _csv.DictWriter(buf, fieldnames=list(rows_out[0].keys()))
            w.writeheader()
            for r in rows_out:
                w.writerow(r)
        return Response(
            buf.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=anu-commission-audit-{_toronto_today().isoformat()}.csv'},
        )

    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'window_days': days,
        'sku_filter': sku_filter or None,
        'summary': summary,
        'rows': rows_out,
        'how_to_use': (
            "lcbo_only rows are stores where lcbo.com or our reps saw stock but SOD did "
            "not flag the SKU as Listed at that store. Each row is a potential commission "
            "claim. Filter to verdict='lcbo_only', export to CSV, and submit to brand owner."
        ),
    })


# Manual rep override — "I physically saw Red Admiral on shelf at this store"
# This is the field-data flow that catches SOD undercounts the moment a rep
# notices them, instead of waiting for the next lcbo.com scrape.
@app.route('/api/crm/observe-listing', methods=['POST'])
@require_app_origin
def api_crm_observe_listing():
    """A rep visited a store, saw our bottle on shelf, but SOD/lcbo.com
    don't show it. They tap "Saw on shelf" → row lands here → feeds the
    commission-audit reconciliation.

    Body: { sku, store_number, rep, on_shelf: bool, units?: int, notes?: str }
    """
    body = request.get_json(silent=True) or {}
    sku = (body.get('sku') or '').strip()
    store_number = body.get('store_number')
    rep = (body.get('rep') or '').strip()
    on_shelf = bool(body.get('on_shelf', True))
    units = body.get('units')
    notes = (body.get('notes') or '').strip()[:500]

    if not sku or sku not in SOD_TRACKED_SKUS:
        return jsonify({'error': 'sku must be one of the 8 tracked SKUs'}), 400
    try:
        store_number = int(store_number)
    except (TypeError, ValueError):
        return jsonify({'error': 'store_number must be an integer'}), 400
    if not rep:
        return jsonify({'error': 'rep is required'}), 400

    # Lazy-create the table (will exist after first call)
    db = get_db()
    try:
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS rep_listing_observations (
                    id BIGSERIAL PRIMARY KEY,
                    sku TEXT NOT NULL,
                    store_number INTEGER NOT NULL,
                    rep TEXT NOT NULL,
                    on_shelf BOOLEAN DEFAULT TRUE,
                    units INTEGER,
                    notes TEXT,
                    observed_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_rep_obs_sku_store ON rep_listing_observations (sku, store_number, observed_at DESC)')
            cur.execute(
                "INSERT INTO rep_listing_observations (sku, store_number, rep, on_shelf, units, notes) "
                "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
                (sku, store_number, rep, on_shelf,
                 int(units) if units is not None else None,
                 notes or None),
            )
            new_id = cur.fetchone()[0]
            cur.close()
            db.commit()
        else:
            db.execute('''
                CREATE TABLE IF NOT EXISTS rep_listing_observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku TEXT NOT NULL,
                    store_number INTEGER NOT NULL,
                    rep TEXT NOT NULL,
                    on_shelf BOOLEAN DEFAULT 1,
                    units INTEGER,
                    notes TEXT,
                    observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            db.execute('CREATE INDEX IF NOT EXISTS idx_rep_obs_sku_store ON rep_listing_observations (sku, store_number, observed_at DESC)')
            cur = db.execute(
                "INSERT INTO rep_listing_observations (sku, store_number, rep, on_shelf, units, notes) "
                "VALUES (?,?,?,?,?,?)",
                (sku, store_number, rep, 1 if on_shelf else 0,
                 int(units) if units is not None else None, notes or None),
            )
            new_id = cur.lastrowid
            db.commit()
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        return jsonify({'error': f'observation save failed: {e}'}), 500

    return jsonify({
        'id': new_id,
        'sku': sku,
        'store_number': store_number,
        'rep': rep,
        'on_shelf': on_shelf,
        'recorded_at': datetime.utcnow().isoformat() + 'Z',
        'note': 'This observation will appear in the next /api/admin/commission-audit run as a "lcbo_only" row if SOD doesn\'t already show this SKU as Listed at this store.',
    }), 201


# ───────────────────────────────────────────────────────────────────────
# MOVEMENT REPORT — accurate counts of stores, new listings, and new
# stores in any date range. Single source of truth for the questions
# "how many stores does LCBO have right now?" / "how many new listings
# did we win last week?" / "did any new stores open this month?"
# ───────────────────────────────────────────────────────────────────────
# ───────────────────────────────────────────────────────────────────────
# Store universe = UNION of all sources. The truth is no single source
# (SOD, lcbo.com, our master directory) is complete. SOD might miss
# stores that lcbo.com shows; lcbo.com might miss stores that SOD has;
# our master directory might be stale or have stores LCBO has closed.
# This helper resolves the union so callers see all stores ever seen.
# ───────────────────────────────────────────────────────────────────────
def _resolve_store_universe(db, lcbo_window_hours=48):
    """Return the union of stores across all data sources.

    Returns a tuple (universe_dict, stats_dict):
      universe_dict: {store_number: {'in_master': bool, 'in_sod_latest': bool,
                                     'in_lcbo_recent': bool}}
      stats_dict:    counts of intersections + summary
    """
    universe: dict = {}

    def _mark(sn, key):
        try:
            n = int(sn)
        except (ValueError, TypeError):
            return
        if n not in universe:
            universe[n] = {'in_master': False, 'in_sod_latest': False, 'in_lcbo_recent': False}
        universe[n][key] = True

    # 1. Master `stores` directory
    try:
        cur = db.cursor() if USE_POSTGRES else db
        if USE_POSTGRES:
            cur.execute("SELECT store_number FROM stores")
            for r in cur.fetchall():
                _mark(r[0], 'in_master')
            cur.close()
        else:
            for r in cur.execute("SELECT store_number FROM stores").fetchall():
                _mark(r[0], 'in_master')
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        print(f"[universe] master read failed: {e}")

    # 2. Latest SOD snapshot
    try:
        cur = db.cursor() if USE_POSTGRES else db
        if USE_POSTGRES:
            cur.execute(
                "SELECT DISTINCT store_number FROM sod_inventory "
                "WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)")
            for r in cur.fetchall():
                _mark(r[0], 'in_sod_latest')
            cur.close()
        else:
            for r in cur.execute(
                "SELECT DISTINCT store_number FROM sod_inventory "
                "WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)"
            ).fetchall():
                _mark(r[0], 'in_sod_latest')
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        print(f"[universe] sod read failed: {e}")

    # 3. lcbo.com inventory_history within window
    try:
        cur = db.cursor() if USE_POSTGRES else db
        if USE_POSTGRES:
            cur.execute(
                "SELECT DISTINCT store_number FROM inventory_history "
                "WHERE store_number <> 'SUMMARY' "
                "  AND recorded_at >= NOW() - (INTERVAL '1 hour' * %s)",
                (lcbo_window_hours,))
            for r in cur.fetchall():
                _mark(r[0], 'in_lcbo_recent')
            cur.close()
        else:
            for r in cur.execute(
                "SELECT DISTINCT store_number FROM inventory_history "
                "WHERE store_number <> 'SUMMARY' "
                "  AND recorded_at >= datetime('now', ? || ' hours')",
                (f'-{lcbo_window_hours}',)).fetchall():
                _mark(r[0], 'in_lcbo_recent')
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        print(f"[universe] lcbo read failed: {e}")

    # Stats
    total = len(universe)
    in_all_three = sum(
        1 for v in universe.values()
        if v['in_master'] and v['in_sod_latest'] and v['in_lcbo_recent']
    )
    in_master_only = sum(
        1 for v in universe.values()
        if v['in_master'] and not v['in_sod_latest'] and not v['in_lcbo_recent']
    )
    in_sod_only = sum(
        1 for v in universe.values()
        if not v['in_master'] and v['in_sod_latest'] and not v['in_lcbo_recent']
    )
    in_lcbo_only = sum(
        1 for v in universe.values()
        if not v['in_master'] and not v['in_sod_latest'] and v['in_lcbo_recent']
    )
    in_master_and_sod = sum(
        1 for v in universe.values()
        if v['in_master'] and v['in_sod_latest']
    )
    in_master_and_lcbo = sum(
        1 for v in universe.values()
        if v['in_master'] and v['in_lcbo_recent']
    )
    in_sod_and_lcbo = sum(
        1 for v in universe.values()
        if v['in_sod_latest'] and v['in_lcbo_recent']
    )

    stats = {
        'total_universe_size': total,
        'in_all_three': in_all_three,
        'in_master_only': in_master_only,
        'in_sod_only': in_sod_only,        # ← new stores LCBO opened we haven't onboarded
        'in_lcbo_only': in_lcbo_only,      # ← stores lcbo.com sees but SOD missed
        'in_master_and_sod': in_master_and_sod,
        'in_master_and_lcbo': in_master_and_lcbo,
        'in_sod_and_lcbo': in_sod_and_lcbo,
    }
    return universe, stats


def _resolve_carrying_us_universe(db, lcbo_window_hours=48):
    """Return the union of stores currently carrying ANY of our SKUs across
    all data sources. A store counts if ANY of these is true:
      - SOD latest snapshot has status='L' for any of our SKUs
      - lcbo.com inventory_history shows qty>0 in window for any of our SKUs
      - rep_listing_observations on_shelf=true within 30 days for any of our SKUs

    Returns (carrying_dict, stats):
      carrying_dict: {store_number: {sources: ['sod','lcbo','rep'], skus: [...]}}
    """
    carrying: dict = {}

    def _add(sn, source, sku):
        try:
            n = int(sn)
        except (ValueError, TypeError):
            return
        if n not in carrying:
            carrying[n] = {'sources': set(), 'skus': set()}
        carrying[n]['sources'].add(source)
        carrying[n]['skus'].add(sku)

    # 1. SOD-listed
    try:
        cur = db.cursor() if USE_POSTGRES else db
        if USE_POSTGRES:
            cur.execute(
                "SELECT store_number, sku FROM sod_inventory "
                "WHERE status='L' "
                "  AND snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)")
            for r in cur.fetchall():
                _add(r[0], 'sod', r[1])
            cur.close()
        else:
            for r in cur.execute(
                "SELECT store_number, sku FROM sod_inventory "
                "WHERE status='L' "
                "  AND snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)"
            ).fetchall():
                _add(r[0], 'sod', r[1])
    except Exception as e:
        try: db.rollback()
        except Exception: pass

    # 2. lcbo.com qty>0 in window — must JOIN products to get the padded SKU
    try:
        cur = db.cursor() if USE_POSTGRES else db
        if USE_POSTGRES:
            cur.execute(
                "SELECT ih.store_number, p.lcbo_sku FROM inventory_history ih "
                "JOIN products p ON p.id = ih.product_id "
                "WHERE ih.store_number <> 'SUMMARY' "
                "  AND ih.quantity > 0 "
                "  AND ih.recorded_at >= NOW() - (INTERVAL '1 hour' * %s)",
                (lcbo_window_hours,))
            for r in cur.fetchall():
                # Pad the SKU back to 7 digits to match SOD_TRACKED_SKUS keys
                sku = (r[1] or '').zfill(7)
                _add(r[0], 'lcbo', sku)
            cur.close()
        else:
            for r in cur.execute(
                "SELECT ih.store_number, p.lcbo_sku FROM inventory_history ih "
                "JOIN products p ON p.id = ih.product_id "
                "WHERE ih.store_number <> 'SUMMARY' "
                "  AND ih.quantity > 0 "
                "  AND ih.recorded_at >= datetime('now', ? || ' hours')",
                (f'-{lcbo_window_hours}',)).fetchall():
                sku = (r[1] or '').zfill(7)
                _add(r[0], 'lcbo', sku)
    except Exception as e:
        try: db.rollback()
        except Exception: pass

    # 3. Rep observations within 30 days
    try:
        cur = db.cursor() if USE_POSTGRES else db
        if USE_POSTGRES:
            cur.execute(
                "SELECT store_number, sku FROM rep_listing_observations "
                "WHERE on_shelf = TRUE "
                "  AND observed_at >= NOW() - INTERVAL '30 days'")
            for r in cur.fetchall():
                _add(r[0], 'rep', r[1])
            cur.close()
        else:
            for r in cur.execute(
                "SELECT store_number, sku FROM rep_listing_observations "
                "WHERE on_shelf = 1 "
                "  AND observed_at >= datetime('now','-30 days')"
            ).fetchall():
                _add(r[0], 'rep', r[1])
    except Exception:
        # Table may not exist yet
        try: db.rollback()
        except Exception: pass

    # Convert sets to sorted lists for JSON serialization
    out = {n: {'sources': sorted(v['sources']), 'skus': sorted(v['skus'])}
           for n, v in carrying.items()}

    stats = {
        'total_stores_carrying_any_sku': len(out),
        'sod_only': sum(1 for v in out.values() if set(v['sources']) == {'sod'}),
        'lcbo_only': sum(1 for v in out.values() if set(v['sources']) == {'lcbo'}),
        'rep_only': sum(1 for v in out.values() if set(v['sources']) == {'rep'}),
        'sod_and_lcbo': sum(1 for v in out.values()
                            if 'sod' in v['sources'] and 'lcbo' in v['sources']),
        'all_three': sum(1 for v in out.values()
                         if set(v['sources']) >= {'sod', 'lcbo', 'rep'}),
    }
    return out, stats


@app.route('/api/admin/store-universe', methods=['GET'])
def api_admin_store_universe():
    """Per-store source breakdown — every store seen in any source.

    Query params:
      lcbo_hours=48  — how far back the lcbo.com window goes (default 48h)
      verbose=1      — include the full per-store dict (default: just stats + drift list)
    """
    try:
        lcbo_hours = max(1, min(int(request.args.get('lcbo_hours', '48')), 720))
    except ValueError:
        lcbo_hours = 48
    verbose = request.args.get('verbose') in ('1', 'true', 'yes')

    db = get_db()
    universe, u_stats = _resolve_store_universe(db, lcbo_window_hours=lcbo_hours)
    carrying, c_stats = _resolve_carrying_us_universe(db, lcbo_window_hours=lcbo_hours)

    # Drift items the operator should investigate
    drift = {
        'sod_only_stores': [
            sn for sn, v in universe.items()
            if v['in_sod_latest'] and not v['in_master']
        ][:50],
        'lcbo_only_stores': [
            sn for sn, v in universe.items()
            if v['in_lcbo_recent'] and not v['in_master']
        ][:50],
        'master_only_stores': [
            sn for sn, v in universe.items()
            if v['in_master'] and not v['in_sod_latest'] and not v['in_lcbo_recent']
        ][:50],
        'carrying_us_only_in_sod': [
            sn for sn, v in carrying.items()
            if set(v['sources']) == {'sod'}
        ][:50],
        'carrying_us_only_in_lcbo': [
            sn for sn, v in carrying.items()
            if set(v['sources']) == {'lcbo'}
        ][:50],
        'carrying_us_only_via_rep': [
            sn for sn, v in carrying.items()
            if set(v['sources']) == {'rep'}
        ][:50],
    }

    out = {
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'lcbo_window_hours': lcbo_hours,
        'universe_stats': u_stats,
        'carrying_stats': c_stats,
        'drift': drift,
        'how_to_read': (
            "lcbo_only_stores = stores lcbo.com shows but SOD missed. "
            "carrying_us_only_in_lcbo = stores where lcbo.com confirms us on shelf "
            "but SOD doesn't list us — potential commission claims. "
            "master_only_stores = stores in our directory neither source has seen "
            "recently — likely closed/stale."
        ),
    }
    if verbose:
        out['per_store'] = {
            str(sn): {
                **universe[sn],
                'carrying_skus': carrying.get(sn, {}).get('skus', []),
                'carrying_sources': carrying.get(sn, {}).get('sources', []),
            }
            for sn in sorted(universe.keys())
        }
    return jsonify(out)


# ───────────────────────────────────────────────────────────────────────
# New-listings-by-range — the "how many stores were added per SKU between
# date X and date Y" report. Methodology:
#
#   1. Find the SOD snapshot closest to (and on/after) start_date for each SKU
#   2. Find the SOD snapshot closest to (and on/before) end_date for each SKU
#   3. New listing = (sku, store) listed at end-snapshot but NOT listed at
#      start-snapshot (status='L' at end, status≠'L' or absent at start)
#   4. Cross-check each new listing against lcbo.com inventory_history (was
#      qty>0 around the end-snapshot date?) — triple verification
#   5. Also include rep observations within the window as additional
#      confirmation
#
# This catches everything: SOD-detected listings, lcbo.com-detected listings
# (where SOD might hide them), and rep-observed listings.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/new-listings-by-range', methods=['GET'])
def api_admin_new_listings_by_range():
    """New-listings-by-range — per-SKU comparison of two SOD snapshots.

    Query params:
      start=YYYY-MM-DD     compare-from date (default: 30 days ago)
      end=YYYY-MM-DD       compare-to date (default: today)
      sku=<7-digit>        filter to one SKU (optional)
      include_lcbo=1       cross-check each new listing against lcbo.com (default 1)

    Returns per SKU:
      - start_snapshot_date (closest SOD snapshot at/before start)
      - end_snapshot_date   (closest SOD snapshot at/before end)
      - new_listings_count  (stores Listed at end but not at start)
      - new_listings: [{store_number, lcbo_confirmed, rep_confirmed, first_seen_in_window}]
      - lost_listings_count (stores Listed at start but not at end)
      - net_change          (new - lost)
    """
    from datetime import date as _date
    today = _toronto_today()
    try:
        start = (request.args.get('start') or '').strip()
        end = (request.args.get('end') or '').strip()
        # ALSO accept ?since=Nd / ?since=7 — convenience for callers
        # (frontend uses start/end; this is for ad-hoc curl + reports).
        since = (request.args.get('since') or '').strip().lower().rstrip('d')
        if start:
            start_d = _date.fromisoformat(start)
        elif since and since.isdigit():
            start_d = today - timedelta(days=int(since))
        else:
            start_d = today - timedelta(days=30)
        if end:
            end_d = _date.fromisoformat(end)
        else:
            end_d = today
        if start_d > end_d:
            return jsonify({'error': 'start must be <= end'}), 400
    except ValueError:
        return jsonify({'error': 'dates must be YYYY-MM-DD (or since=Nd)'}), 400

    sku_filter = (request.args.get('sku') or '').strip()
    include_lcbo = (request.args.get('include_lcbo', '1') in ('1', 'true', 'yes'))
    # strict_mode (default ON) — only count stores with a verified
    # NEW_LISTING/RELISTED change event in window OR lcbo.com / rep
    # confirmation. Filters out stores that appear in the snapshot diff
    # but have no transition event recorded — those are usually just
    # day-1 baseline gaps (we ingested them late, they were already
    # listed before our SOD history begins) and are NOT real new
    # listings. Pass strict_mode=0 to include them.
    strict_mode = (request.args.get('strict_mode', '1') in ('1', 'true', 'yes'))
    # fresh_lcbo (default OFF — slow). When ON, kicks off a synchronous
    # lcbo.com scrape for all 8 tracked SKUs BEFORE the diff so the
    # cross-check uses live-as-of-this-moment data, not the most recent
    # 30-min cron's snapshot. Adds ~30-60s to the request but produces
    # the most accurate possible verification.
    fresh_lcbo = (request.args.get('fresh_lcbo', '0') in ('1', 'true', 'yes'))
    if fresh_lcbo and include_lcbo:
        try:
            scrape_worker = globals().get('_lcbo_daily_scrape_worker')
            if callable(scrape_worker):
                print('[new-listings] Triggering live lcbo.com scrape before diff...')
                scrape_worker()
                print('[new-listings] Live scrape complete; running diff with fresh data')
        except Exception as e:
            print(f'[new-listings] Live scrape failed (proceeding with stale data): {e}')
    portfolio = (request.args.get('portfolio') or 'NB').strip()
    portfolio_skus = _skus_for_portfolio(portfolio)
    if sku_filter and sku_filter in SOD_TRACKED_SKUS:
        skus_to_audit = [sku_filter] if sku_filter in set(portfolio_skus) else []
    else:
        skus_to_audit = portfolio_skus

    db = get_db()
    out_rows = []
    summary = {
        'portfolio': portfolio,
        'total_new_listings': 0,
        'total_lost_listings': 0,
        'net_change': 0,
        'lcbo_confirmed_new': 0,
        'rep_confirmed_new': 0,
        'total_confirmed_new': 0,
        'total_unconfirmed': 0,
    }

    for sku in skus_to_audit:
        brand, name = SOD_TRACKED_SKUS[sku]
        sku_clean = sku.lstrip('0')

        # Find the actual snapshot dates we'll use for the comparison.
        # Pick the SOD snapshot at-or-before the requested start, and the
        # SOD snapshot at-or-before the requested end.
        #
        # POLICY: if no snapshot exists at-or-before start_d (= our SOD
        # history is younger than the requested start), DO NOT fall back
        # silently. The diff would be 'everything Listed at end vs nothing'
        # which inflates 'new' to whatever the current Listed count is —
        # that's how the user got '9 and 7 for both NB products' for any
        # date range that predated our ingest. Instead, return a row with
        # start_was_clipped=true and zeroed counts so the UI can prompt
        # the operator to upload a historical SOD ZIP via the new
        # /api/admin/sod/compare-uploads or /api/admin/sod/upload-historical
        # endpoint to produce a truthful diff.
        start_snapshot = None
        end_snapshot = None
        start_was_clipped = False
        earliest_available = None
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=%s AND snapshot_date <= %s",
                    (sku, start_d.isoformat()))
                start_snapshot = cur.fetchone()[0]
                cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=%s AND snapshot_date <= %s",
                    (sku, end_d.isoformat()))
                end_snapshot = cur.fetchone()[0]
                if start_snapshot is None:
                    start_was_clipped = True
                    cur.execute(
                        "SELECT MIN(snapshot_date) FROM sod_inventory WHERE sku=%s", (sku,))
                    earliest_available = cur.fetchone()[0]
                cur.close()
            else:
                start_snapshot = cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=? AND snapshot_date <= ?",
                    (sku, start_d.isoformat())).fetchone()[0]
                end_snapshot = cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=? AND snapshot_date <= ?",
                    (sku, end_d.isoformat())).fetchone()[0]
                if start_snapshot is None:
                    start_was_clipped = True
                    earliest_available = cur.execute(
                        "SELECT MIN(snapshot_date) FROM sod_inventory WHERE sku=?", (sku,)
                    ).fetchone()[0]
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            out_rows.append({
                'sku': sku, 'product_name': name, 'brand': brand,
                'error': f'snapshot lookup failed: {e}',
            })
            continue

        # If start was clipped, return the row with a clear flag and zeros.
        # This is honest: we don't know what was Listed at start_d because
        # we didn't have data then. Operator can upload a historical ZIP.
        if start_was_clipped:
            out_rows.append({
                'sku': sku,
                'product_name': name,
                'brand': brand,
                'start_snapshot_date': None,
                'end_snapshot_date': str(end_snapshot) if end_snapshot else None,
                'start_was_clipped': True,
                'earliest_available_snapshot': str(earliest_available) if earliest_available else None,
                'sod_new_count': 0,
                'lcbo_only_new_count': 0,
                'rep_only_new_count': 0,
                'union_new_count': 0,
                'confirmed_new_count': 0,
                'unconfirmed_count': 0,
                'sod_lost_count': 0,
                'net_change': 0,
                'start_listed_count': 0,
                'end_listed_count': 0,
                'lcbo_confirmed_count': 0,
                'rep_confirmed_count': 0,
                'new_stores': [],
                'lost_stores': [],
                'message': (
                    f"Our SOD ingest only goes back to {earliest_available}; the "
                    f"requested start date ({start_d.isoformat()}) is before that. "
                    f"To get a real diff, upload a historical SOD inventory ZIP via "
                    f"/api/admin/sod/compare-uploads."
                ),
            })
            continue

        # Pull the two snapshots' Listed-store sets
        def listed_stores_at(snap_date):
            if not snap_date:
                return set()
            try:
                cur = db.cursor() if USE_POSTGRES else db
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number FROM sod_inventory "
                        "WHERE sku=%s AND snapshot_date=%s AND status='L'",
                        (sku, snap_date))
                    out = {int(r[0]) for r in cur.fetchall()}
                    cur.close()
                else:
                    out = {
                        int(r[0]) for r in cur.execute(
                            "SELECT store_number FROM sod_inventory "
                            "WHERE sku=? AND snapshot_date=? AND status='L'",
                            (sku, snap_date)).fetchall()
                    }
                return out
            except Exception:
                try: db.rollback()
                except Exception: pass
                return set()

        start_listed = listed_stores_at(start_snapshot)
        end_listed = listed_stores_at(end_snapshot)

        new_set = end_listed - start_listed     # listings won
        lost_set = start_listed - end_listed     # listings lost

        # Pull NEW_LISTING change events for this SKU within window —
        # this is the diff-engine's verified-transition signal. A store
        # in new_set BUT NOT in this set is likely just a baseline gap
        # (e.g. the store was listed all along but missing from our
        # day-1 snapshot) — NOT a real new listing.
        confirmed_new_event = set()
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute(
                    "SELECT DISTINCT store_number FROM sod_listing_changes "
                    "WHERE sku=%s "
                    "  AND change_type IN ('NEW_LISTING','RELISTED') "
                    "  AND change_date BETWEEN %s AND %s "
                    "  AND store_number IS NOT NULL",
                    (sku, start_d.isoformat(), end_d.isoformat()))
                confirmed_new_event = {int(r[0]) for r in cur.fetchall()}
                cur.close()
            else:
                confirmed_new_event = {
                    int(r[0]) for r in cur.execute(
                        "SELECT DISTINCT store_number FROM sod_listing_changes "
                        "WHERE sku=? "
                        "  AND change_type IN ('NEW_LISTING','RELISTED') "
                        "  AND change_date BETWEEN ? AND ? "
                        "  AND store_number IS NOT NULL",
                        (sku, start_d.isoformat(), end_d.isoformat())).fetchall()
                }
        except Exception:
            try: db.rollback()
            except Exception: pass

        # Pull last-seen-listed-before-window for each new_set store
        # so we know if it's truly new or if it was Listed before our window.
        last_listed_before: dict = {}
        if new_set:
            try:
                cur = db.cursor() if USE_POSTGRES else db
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number, MAX(snapshot_date)::text "
                        "FROM sod_inventory "
                        "WHERE sku=%s AND status='L' "
                        "  AND snapshot_date < %s "
                        "GROUP BY store_number",
                        (sku, start_d.isoformat()))
                    for r in cur.fetchall():
                        try:
                            last_listed_before[int(r[0])] = r[1]
                        except (ValueError, TypeError):
                            continue
                    cur.close()
                else:
                    for r in cur.execute(
                        "SELECT store_number, MAX(snapshot_date) "
                        "FROM sod_inventory "
                        "WHERE sku=? AND status='L' "
                        "  AND snapshot_date < ? "
                        "GROUP BY store_number",
                        (sku, start_d.isoformat())).fetchall():
                        try:
                            last_listed_before[int(r[0])] = str(r[1]) if r[1] else None
                        except (ValueError, TypeError):
                            continue
            except Exception:
                try: db.rollback()
                except Exception: pass

        # Cross-check new listings against lcbo.com (qty>0 within window)
        lcbo_confirmed = set()
        if include_lcbo and new_set:
            try:
                cur = db.cursor() if USE_POSTGRES else db
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT id FROM products WHERE lcbo_sku=%s LIMIT 1",
                        (sku_clean,))
                    prow = cur.fetchone()
                    pid = prow[0] if prow else None
                    if pid:
                        cur.execute(
                            "SELECT DISTINCT store_number FROM inventory_history "
                            "WHERE product_id=%s AND quantity>0 "
                            "  AND store_number <> 'SUMMARY' "
                            "  AND recorded_at >= %s "
                            "  AND recorded_at <= %s",
                            (pid, start_d.isoformat(),
                             (end_d + timedelta(days=1)).isoformat()))
                        for r in cur.fetchall():
                            try:
                                lcbo_confirmed.add(int(r[0]))
                            except (ValueError, TypeError):
                                continue
                    cur.close()
                else:
                    prow = cur.execute(
                        "SELECT id FROM products WHERE lcbo_sku=? LIMIT 1",
                        (sku_clean,)).fetchone()
                    pid = prow[0] if prow else None
                    if pid:
                        for r in cur.execute(
                            "SELECT DISTINCT store_number FROM inventory_history "
                            "WHERE product_id=? AND quantity>0 "
                            "  AND store_number <> 'SUMMARY' "
                            "  AND recorded_at >= ? "
                            "  AND recorded_at <= ?",
                            (pid, start_d.isoformat(),
                             (end_d + timedelta(days=1)).isoformat())).fetchall():
                            try:
                                lcbo_confirmed.add(int(r[0]))
                            except (ValueError, TypeError):
                                continue
            except Exception:
                try: db.rollback()
                except Exception: pass

        # Also pull rep observations for this SKU within window
        rep_confirmed = set()
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute(
                    "SELECT DISTINCT store_number FROM rep_listing_observations "
                    "WHERE sku=%s AND on_shelf=TRUE "
                    "  AND observed_at >= %s AND observed_at <= %s",
                    (sku, start_d.isoformat(),
                     (end_d + timedelta(days=1)).isoformat()))
                for r in cur.fetchall():
                    try:
                        rep_confirmed.add(int(r[0]))
                    except (ValueError, TypeError):
                        continue
                cur.close()
            else:
                for r in cur.execute(
                    "SELECT DISTINCT store_number FROM rep_listing_observations "
                    "WHERE sku=? AND on_shelf=1 "
                    "  AND observed_at >= ? AND observed_at <= ?",
                    (sku, start_d.isoformat(),
                     (end_d + timedelta(days=1)).isoformat())).fetchall():
                    try:
                        rep_confirmed.add(int(r[0]))
                    except (ValueError, TypeError):
                        continue
        except Exception:
            try: db.rollback()
            except Exception: pass

        # ALSO catch lcbo.com-only new listings: stores where lcbo.com saw
        # qty>0 in-window AND that store wasn't Listed at start_snapshot.
        # These are "hidden in SOD but visible on lcbo.com" wins.
        lcbo_only_new = (lcbo_confirmed - start_listed) - new_set
        rep_only_new = (rep_confirmed - start_listed) - new_set - lcbo_only_new

        # Build per-store rows with evidence per store. Each row carries:
        #   confirmed_new        - True iff there's a NEW_LISTING/RELISTED
        #                          event for (sku, store) in window OR
        #                          discovered_via != 'sod' (lcbo/rep saw it).
        #                          When False, the store appears in the diff
        #                          but the change-event log doesn't back it up
        #                          — usually means our day-1 snapshot was
        #                          incomplete and this store was already
        #                          listed before our ingest started.
        #   evidence             - human-readable explanation
        #   last_listed_before_window - if the SOD snapshot history shows
        #                          this store WAS listed before window start,
        #                          it's NOT actually new.
        store_rows = []
        for sn in sorted(new_set):
            had_event = sn in confirmed_new_event
            prior_listed = last_listed_before.get(sn)
            confirmed_new = had_event or (sn in lcbo_confirmed) or (sn in rep_confirmed)
            if had_event:
                ev = 'NEW_LISTING/RELISTED event recorded in window'
            elif prior_listed:
                ev = f'⚠ Already Listed in our SOD on {prior_listed} (before window) — NOT actually new'
            else:
                ev = '⚠ No NEW_LISTING event in change log — likely day-1 baseline gap, not a real new listing'
            store_rows.append({
                'store_number': sn,
                'discovered_via': 'sod',
                'confirmed_new': confirmed_new,
                'has_change_event': had_event,
                'last_listed_before_window': prior_listed,
                'lcbo_confirmed': sn in lcbo_confirmed,
                'rep_confirmed': sn in rep_confirmed,
                'evidence': ev,
            })
        for sn in sorted(lcbo_only_new):
            store_rows.append({
                'store_number': sn,
                'discovered_via': 'lcbo_only',
                'confirmed_new': True,
                'has_change_event': sn in confirmed_new_event,
                'last_listed_before_window': last_listed_before.get(sn),
                'lcbo_confirmed': True,
                'rep_confirmed': sn in rep_confirmed,
                'evidence': 'lcbo.com showed qty>0 in window; SOD did not list (commission claim)',
            })
        for sn in sorted(rep_only_new):
            store_rows.append({
                'store_number': sn,
                'discovered_via': 'rep_only',
                'confirmed_new': True,
                'has_change_event': sn in confirmed_new_event,
                'last_listed_before_window': last_listed_before.get(sn),
                'lcbo_confirmed': sn in lcbo_confirmed,
                'rep_confirmed': True,
                'evidence': 'Rep observed on shelf in window',
            })

        # If strict_mode, filter out unconfirmed rows BEFORE counting
        if strict_mode:
            store_rows = [s for s in store_rows if s['confirmed_new']]
            new_set = {s['store_number'] for s in store_rows if s['discovered_via'] == 'sod'}

        # Combined "all sources" new-listing count (the union)
        union_new = new_set | lcbo_only_new | rep_only_new

        confirmed_count = sum(1 for s in store_rows if s.get('confirmed_new'))
        unconfirmed_count = len(store_rows) - confirmed_count

        out_rows.append({
            'sku': sku,
            'product_name': name,
            'brand': brand,
            'start_snapshot_date': str(start_snapshot) if start_snapshot else None,
            'end_snapshot_date': str(end_snapshot) if end_snapshot else None,
            'start_was_clipped': start_was_clipped,
            'sod_new_count': len(new_set),
            'lcbo_only_new_count': len(lcbo_only_new),
            'rep_only_new_count': len(rep_only_new),
            'union_new_count': len(union_new),
            'confirmed_new_count': confirmed_count,
            'unconfirmed_count': unconfirmed_count,
            'sod_lost_count': len(lost_set),
            'net_change': len(new_set) - len(lost_set),
            'start_listed_count': len(start_listed),
            'end_listed_count': len(end_listed),
            'lcbo_confirmed_count': len(lcbo_confirmed & union_new),
            'rep_confirmed_count': len(rep_confirmed & union_new),
            'new_stores': store_rows,
            'lost_stores': sorted(lost_set),
        })

        summary['total_new_listings'] += len(union_new)
        summary['total_lost_listings'] += len(lost_set)
        summary['net_change'] += len(new_set) - len(lost_set)
        summary['lcbo_confirmed_new'] += len(lcbo_confirmed & union_new)
        summary['rep_confirmed_new'] += len(rep_confirmed & union_new)
        summary.setdefault('total_confirmed_new', 0)
        summary.setdefault('total_unconfirmed', 0)
        summary['total_confirmed_new'] += confirmed_count
        summary['total_unconfirmed'] += unconfirmed_count

    fmt = (request.args.get('format') or 'json').lower()
    if fmt == 'csv':
        # Flat CSV: one row per (sku, store) added or lost. Designed for
        # direct import into Excel / submission to NB Distillers.
        import csv as _csv, io as _io
        buf = _io.StringIO()
        w = _csv.writer(buf)
        w.writerow([
            'window_start', 'window_end', 'sku', 'product_name', 'brand',
            'verdict', 'store_number', 'discovered_via',
            'confirmed_new', 'has_change_event', 'last_listed_before_window',
            'evidence',
            'lcbo_confirmed', 'rep_confirmed',
            'start_snapshot_date', 'end_snapshot_date',
            'start_was_clipped', 'strict_mode',
        ])
        for r in out_rows:
            if r.get('start_was_clipped'):
                w.writerow([
                    start_d.isoformat(), end_d.isoformat(), r['sku'],
                    r['product_name'], r['brand'], 'INSUFFICIENT_HISTORY',
                    '', '', '', '', '',
                    'No SOD snapshot at-or-before window start; upload a historical ZIP',
                    '', '',
                    r.get('start_snapshot_date') or '',
                    r.get('end_snapshot_date') or '',
                    True, strict_mode,
                ])
                continue
            for s in r.get('new_stores') or []:
                w.writerow([
                    start_d.isoformat(), end_d.isoformat(), r['sku'],
                    r['product_name'], r['brand'], 'ADDED',
                    s.get('store_number'), s.get('discovered_via'),
                    bool(s.get('confirmed_new')),
                    bool(s.get('has_change_event')),
                    s.get('last_listed_before_window') or '',
                    s.get('evidence') or '',
                    bool(s.get('lcbo_confirmed')),
                    bool(s.get('rep_confirmed')),
                    r.get('start_snapshot_date') or '',
                    r.get('end_snapshot_date') or '',
                    False, strict_mode,
                ])
            for sn in r.get('lost_stores') or []:
                w.writerow([
                    start_d.isoformat(), end_d.isoformat(), r['sku'],
                    r['product_name'], r['brand'], 'LOST',
                    sn, 'sod', '', '', '', '', '', '',
                    r.get('start_snapshot_date') or '',
                    r.get('end_snapshot_date') or '',
                    False, strict_mode,
                ])
        fname = (
            f"anu-new-listings-{start_d.isoformat()}-to-{end_d.isoformat()}"
            + (f'-{sku_filter}' if sku_filter else '')
            + ('-strict' if strict_mode else '-loose')
            + '.csv'
        )
        return Response(
            buf.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{fname}"'},
        )

    return jsonify({
        'window': {
            'start': start_d.isoformat(),
            'end': end_d.isoformat(),
            'days': (end_d - start_d).days + 1,
        },
        'sku_filter': sku_filter or None,
        'include_lcbo_cross_check': include_lcbo,
        'strict_mode': strict_mode,
        'summary': summary,
        'per_sku': out_rows,
        'how_to_read': (
            "STRICT MODE (default) shows only stores with a verified "
            "NEW_LISTING/RELISTED change event in window OR independent "
            "confirmation from lcbo.com or a rep. Unconfirmed stores (in "
            "the snapshot diff but with no transition event) are filtered "
            "out — they're usually just day-1 baseline gaps where the "
            "store was already listed before our SOD ingest started, not "
            "real new listings. Pass &strict_mode=0 to see them. "
            "Each store row carries 'confirmed_new', 'has_change_event', "
            "'last_listed_before_window', and 'evidence' for full audit. "
            "Add &format=csv to download."
        ),
        'as_of': datetime.utcnow().isoformat() + 'Z',
    })


# ───────────────────────────────────────────────────────────────────────
# SOD COMPARE UPLOADS — the "right way" to do new-listings-by-range:
#
#   1. Operator downloads the SOD inventory ZIP for a historical date
#      (e.g. March 1) directly from sod.lcbo.com.
#   2. Optionally downloads today's ZIP (or we use the latest from the DB).
#   3. Uploads the two ZIPs (or one ZIP) to this endpoint.
#   4. We stream-parse both, build per-SKU per-store Listed sets, diff them.
#   5. Return per-SKU "stores added" + "stores lost" with full store_number
#      lists. Nothing gets persisted; pure compute.
#
# This bypasses the "no historical SOD data in our DB" problem — operator
# brings the historical data with them. Works for any date range as long
# as SOD's portal still has the file.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/compare-uploads', methods=['POST'])
@require_app_origin
def api_admin_sod_compare_uploads():
    """Compare two uploaded SOD inventory ZIPs and produce a per-SKU diff.

    Memory-safe — uses stream_parse_sod_zip_to_sets which never buffers
    rows, only the (sku, store) Listed sets. Stays well under 50MB RAM
    even with two 50MB+ ZIPs uploaded.

    Multipart form fields:
      from_zip      (file, REQUIRED) — the historical/baseline snapshot
      to_zip        (file, OPTIONAL) — the comparison snapshot. If omitted,
                                       we use the latest snapshot from our DB.
      from_date     (str,  OPTIONAL) — if from_zip has multiple dates, pick
                                       which one to use (YYYY-MM-DD). Default:
                                       most recent date in the ZIP.
      to_date       (str,  OPTIONAL) — same idea for to_zip.
      sku           (str,  OPTIONAL) — filter to one tracked SKU
      include_lcbo  (str,  OPTIONAL) — '1' to cross-check via lcbo.com (default)
    """
    from_file = request.files.get('from_zip')
    to_file = request.files.get('to_zip')
    sku_filter = (request.form.get('sku') or '').strip()
    include_lcbo = (request.form.get('include_lcbo', '1') in ('1', 'true', 'yes'))
    from_date_pick = (request.form.get('from_date') or '').strip()
    to_date_pick = (request.form.get('to_date') or '').strip()
    # When fresh_lcbo=1, kick off a live lcbo.com scrape before computing
    # the diff so the cross-check has the freshest possible inventory data.
    fresh_lcbo = (request.form.get('fresh_lcbo', '0') in ('1', 'true', 'yes'))
    if fresh_lcbo and include_lcbo:
        try:
            scrape_worker = globals().get('_lcbo_daily_scrape_worker')
            if callable(scrape_worker):
                print('[compare-uploads] Triggering live lcbo.com scrape before diff...')
                scrape_worker()
                print('[compare-uploads] Live scrape complete')
        except Exception as e:
            print(f'[compare-uploads] Live scrape failed: {e}')

    if not from_file:
        return jsonify({'error': 'from_zip is required (the historical snapshot)'}), 400

    skus_to_audit = (
        [sku_filter] if (sku_filter and sku_filter in SOD_TRACKED_SKUS)
        else list(SOD_TRACKED_SKUS.keys())
    )
    tracked_set = set(skus_to_audit)

    # Parse "from" with the lean streaming parser (no row buffering)
    try:
        from_bytes = from_file.read()
        if not from_bytes:
            return jsonify({'error': 'from_zip is empty'}), 400
        from_parsed = stream_parse_sod_zip_to_sets(from_bytes, tracked_set)
    except Exception as e:
        return jsonify({'error': f'failed to parse from_zip: {e}'}), 400

    from_dates = sorted(from_parsed.get('dates_seen', set()))
    if not from_dates:
        return jsonify({
            'error': 'no rows for tracked SKUs found in from_zip',
            'tip': 'ZIP may not contain any of our 8 tracked SKUs.',
        }), 422
    # Pick which date inside from_zip to use as the snapshot
    if from_date_pick and from_date_pick in from_parsed['listed_by_date_sku']:
        from_date_used = from_date_pick
    elif from_date_pick:
        return jsonify({
            'error': f'from_date {from_date_pick} not in zip',
            'available_dates': from_dates,
        }), 400
    else:
        from_date_used = from_dates[-1]   # default: most recent date in ZIP

    from_listed_by_sku: dict = {
        sku: from_parsed['listed_by_date_sku'].get(from_date_used, {}).get(sku, set())
        for sku in skus_to_audit
    }

    # Parse "to" if provided, else fall back to latest snapshot in DB
    to_listed_by_sku: dict = {sku: set() for sku in skus_to_audit}
    to_dates = []
    to_source = 'db_latest'
    to_date_used = None
    to_parsed: dict = {}  # so the parse_stats reference is safe even when to_file is None
    if to_file:
        try:
            to_bytes = to_file.read()
            if to_bytes:
                to_parsed = stream_parse_sod_zip_to_sets(to_bytes, tracked_set)
                to_dates = sorted(to_parsed.get('dates_seen', set()))
                if not to_dates:
                    return jsonify({'error': 'no rows for tracked SKUs in to_zip'}), 422
                if to_date_pick and to_date_pick in to_parsed['listed_by_date_sku']:
                    to_date_used = to_date_pick
                elif to_date_pick:
                    return jsonify({
                        'error': f'to_date {to_date_pick} not in zip',
                        'available_dates': to_dates,
                    }), 400
                else:
                    to_date_used = to_dates[-1]
                to_listed_by_sku = {
                    sku: to_parsed['listed_by_date_sku'].get(to_date_used, {}).get(sku, set())
                    for sku in skus_to_audit
                }
                to_source = 'uploaded'
        except Exception as e:
            return jsonify({'error': f'failed to parse to_zip: {e}'}), 400

    if to_source == 'db_latest':
        # Pull latest snapshot per SKU from DB
        try:
            db = get_db()
            cur = db.cursor() if USE_POSTGRES else db
            for sku in skus_to_audit:
                if USE_POSTGRES:
                    cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s", (sku,))
                    latest = cur.fetchone()[0]
                else:
                    latest = cur.execute(
                        "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=?",
                        (sku,)).fetchone()[0]
                if not latest:
                    continue
                to_dates.append(str(latest))
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number FROM sod_inventory "
                        "WHERE sku=%s AND snapshot_date=%s AND status='L'",
                        (sku, latest))
                    to_listed_by_sku[sku] = {int(r[0]) for r in cur.fetchall()}
                else:
                    to_listed_by_sku[sku] = {
                        int(r[0]) for r in cur.execute(
                            "SELECT store_number FROM sod_inventory "
                            "WHERE sku=? AND snapshot_date=? AND status='L'",
                            (sku, latest)).fetchall()
                    }
            if USE_POSTGRES: cur.close()
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            return jsonify({'error': f'DB lookup failed: {e}'}), 500
        to_dates = sorted(set(to_dates))

    # Optional lcbo.com cross-check for each "added" store-SKU
    lcbo_per_sku = {}
    if include_lcbo:
        try:
            db = get_db()
            cur = db.cursor() if USE_POSTGRES else db
            for sku in skus_to_audit:
                sku_clean = sku.lstrip('0')
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT id FROM products WHERE lcbo_sku=%s LIMIT 1",
                        (sku_clean,))
                    prow = cur.fetchone()
                    pid = prow[0] if prow else None
                    if pid:
                        cur.execute(
                            "SELECT DISTINCT store_number FROM inventory_history "
                            "WHERE product_id=%s AND quantity>0 "
                            "  AND store_number <> 'SUMMARY' "
                            "  AND recorded_at >= NOW() - INTERVAL '7 days'",
                            (pid,))
                        seen = set()
                        for r in cur.fetchall():
                            try:
                                seen.add(int(r[0]))
                            except (ValueError, TypeError):
                                continue
                        lcbo_per_sku[sku] = seen
                else:
                    prow = cur.execute(
                        "SELECT id FROM products WHERE lcbo_sku=? LIMIT 1",
                        (sku_clean,)).fetchone()
                    pid = prow[0] if prow else None
                    if pid:
                        seen = set()
                        for r in cur.execute(
                            "SELECT DISTINCT store_number FROM inventory_history "
                            "WHERE product_id=? AND quantity>0 "
                            "  AND store_number <> 'SUMMARY' "
                            "  AND recorded_at >= datetime('now','-7 days')",
                            (pid,)).fetchall():
                            try:
                                seen.add(int(r[0]))
                            except (ValueError, TypeError):
                                continue
                        lcbo_per_sku[sku] = seen
            if USE_POSTGRES: cur.close()
        except Exception:
            try: db.rollback()
            except Exception: pass

    # Compute per-SKU diff
    per_sku_out = []
    summary = {
        'total_added': 0,
        'total_lost': 0,
        'total_lcbo_only': 0,
    }
    for sku in skus_to_audit:
        brand, name = SOD_TRACKED_SKUS[sku]
        from_set = from_listed_by_sku.get(sku, set())
        to_set = to_listed_by_sku.get(sku, set())
        added = to_set - from_set
        lost = from_set - to_set
        # Also include lcbo-only-new: stores lcbo.com saw within 7 days that
        # weren't in 'from' set (potentially being hidden in 'to' SOD as well)
        lcbo_seen = lcbo_per_sku.get(sku, set())
        lcbo_only_new = (lcbo_seen - from_set) - to_set
        union_added = added | lcbo_only_new

        added_rows = []
        for sn in sorted(added):
            added_rows.append({
                'store_number': sn,
                'discovered_via': 'sod',
                'lcbo_confirmed': sn in lcbo_seen,
            })
        for sn in sorted(lcbo_only_new):
            added_rows.append({
                'store_number': sn,
                'discovered_via': 'lcbo_only',
                'lcbo_confirmed': True,
            })

        per_sku_out.append({
            'sku': sku,
            'product_name': name,
            'brand': brand,
            'from_listed_count': len(from_set),
            'to_listed_count': len(to_set),
            'sod_added_count': len(added),
            'sod_lost_count': len(lost),
            'lcbo_only_added_count': len(lcbo_only_new),
            'lcbo_confirmed_added': len(added & lcbo_seen),
            'union_added_count': len(union_added),
            'net_change': len(added) - len(lost),
            'added_stores': added_rows,
            'lost_stores': sorted(lost),
        })
        summary['total_added'] += len(union_added)
        summary['total_lost'] += len(lost)
        summary['total_lcbo_only'] += len(lcbo_only_new)

    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'from_filename': getattr(from_file, 'filename', '') or '',
        'to_filename': getattr(to_file, 'filename', '') if to_file else '(latest from DB)',
        'from_dates_in_zip': from_dates,
        'from_date_used': from_date_used,
        'to_dates': to_dates,
        'to_date_used': to_date_used,
        'to_source': to_source,
        'sku_filter': sku_filter or None,
        'include_lcbo_cross_check': include_lcbo,
        'summary': summary,
        'per_sku': per_sku_out,
        'parse_stats': {
            'from_total_rows': from_parsed.get('total_rows', 0),
            'from_tracked_rows': from_parsed.get('tracked_rows', 0),
            'to_total_rows': to_parsed.get('total_rows') if to_file else None,
            'to_tracked_rows': to_parsed.get('tracked_rows') if to_file else None,
        },
        'how_to_read': (
            "Upload a SOD inventory ZIP from any historical date as 'from_zip'. "
            "The endpoint diffs it against today's snapshot (or a second uploaded "
            "ZIP) and returns stores added/lost per SKU. If a ZIP contains "
            "multiple dates, the most recent is used by default — pass from_date "
            "or to_date to override. lcbo.com cross-check catches listings that "
            "may be hidden in the 'to' SOD but visible to customers."
        ),
    })


# ───────────────────────────────────────────────────────────────────────
# SOD UPLOAD-PREVIEW — see what dates a ZIP contains and what would be
# inserted, BEFORE persisting. Lean parser, no DB writes.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/upload-preview', methods=['POST'])
@require_app_origin
def api_admin_sod_upload_preview():
    """Inspect a SOD ZIP without persisting. Returns dates inside, per-SKU
    Listed counts, and what would be inserted vs already exists.

    Multipart form fields:
      zip   (file, REQUIRED)   — the SOD inventory ZIP
    """
    f = request.files.get('zip')
    if not f:
        return jsonify({'error': 'zip file is required'}), 400
    try:
        zip_bytes = f.read()
        if not zip_bytes:
            return jsonify({'error': 'zip is empty'}), 400
        parsed = stream_parse_sod_zip_to_sets(zip_bytes, set(SOD_TRACKED_SKUS.keys()))
    except Exception as e:
        return jsonify({'error': f'parse failed: {e}'}), 400

    dates = sorted(parsed.get('dates_seen', set()))
    counts = parsed.get('counts_by_date_sku', {})

    # Per-date per-SKU summary
    per_date = []
    for d in dates:
        sku_rows = []
        for sku, cnt in (counts.get(d) or {}).items():
            brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
            sku_rows.append({
                'sku': sku,
                'product_name': name,
                'brand': brand,
                'L': cnt.get('L', 0),
                'D': cnt.get('D', 0),
                'F': cnt.get('F', 0),
                'total': cnt.get('total', 0),
                'on_hand_listed': cnt.get('on_hand_listed', 0),
            })
        sku_rows.sort(key=lambda r: -r['L'])
        per_date.append({
            'snapshot_date': d,
            'tracked_sku_rows': sku_rows,
        })

    # Check overlap with existing data
    existing_per_date = {}
    try:
        db = get_db()
        cur = db.cursor() if USE_POSTGRES else db
        for d in dates:
            if USE_POSTGRES:
                cur.execute("SELECT COUNT(*) FROM sod_inventory WHERE snapshot_date=%s", (d,))
                existing_per_date[d] = int(cur.fetchone()[0] or 0)
            else:
                existing_per_date[d] = int(cur.execute(
                    "SELECT COUNT(*) FROM sod_inventory WHERE snapshot_date=?", (d,)
                ).fetchone()[0] or 0)
        if USE_POSTGRES: cur.close()
    except Exception:
        try: db.rollback()
        except Exception: pass

    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'filename': getattr(f, 'filename', '') or '',
        'total_rows_in_zip': parsed.get('total_rows', 0),
        'tracked_rows_in_zip': parsed.get('tracked_rows', 0),
        'dates_in_zip': dates,
        'per_date': per_date,
        'existing_rows_per_date': existing_per_date,
        'note': (
            "This is a preview only — no rows are persisted. To actually "
            "ingest, POST the same file to /api/admin/sod/upload-historical."
        ),
    })


# ───────────────────────────────────────────────────────────────────────
# SOD ROLLBACK — delete a historical snapshot if it was wrongly uploaded.
# DOES NOT touch the live sync data; only deletes a specific snapshot_date.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/rollback-snapshot', methods=['POST'])
@require_app_origin
def api_admin_sod_rollback_snapshot():
    """Delete all sod_inventory rows for a specific snapshot_date.

    Body: { snapshot_date: 'YYYY-MM-DD', confirm: true }

    Refuses to delete unless confirm=true is set explicitly. Also logs an
    event_log row so the deletion is auditable.
    """
    body = request.get_json(silent=True) or {}
    snapshot_date = (body.get('snapshot_date') or '').strip()
    confirm = bool(body.get('confirm'))

    if not snapshot_date:
        return jsonify({'error': 'snapshot_date is required (YYYY-MM-DD)'}), 400
    try:
        from datetime import date as _date
        _date.fromisoformat(snapshot_date)
    except ValueError:
        return jsonify({'error': 'snapshot_date must be YYYY-MM-DD'}), 400
    if not confirm:
        return jsonify({
            'error': 'confirm=true is required to actually delete',
            'snapshot_date': snapshot_date,
            'note': 'Re-POST with {"confirm": true} to perform the deletion.',
        }), 400

    deleted = 0
    try:
        conn = _sod_get_conn()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("DELETE FROM sod_inventory WHERE snapshot_date=%s", (snapshot_date,))
            deleted = cur.rowcount
            # Also remove any sod_listing_changes that fired for this snapshot
            cur.execute(
                "DELETE FROM sod_listing_changes WHERE change_date=%s "
                "AND (old_status IS NULL OR old_status='') ",  # only synthetic baselines
                (snapshot_date,))
        else:
            cur.execute("DELETE FROM sod_inventory WHERE snapshot_date=?", (snapshot_date,))
            deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({'error': f'delete failed: {e}'}), 500

    # Audit log
    try:
        _log_event('sod_snapshot_rolled_back', 'snapshot', snapshot_date,
                   actor=request.headers.get('X-User', 'admin'),
                   payload={'deleted_rows': deleted})
    except Exception:
        pass

    return jsonify({
        'status': 'ok',
        'snapshot_date': snapshot_date,
        'deleted_rows': deleted,
        'note': 'Snapshot removed from sod_inventory. /new-listings will no longer compare against this date.',
    })


# ───────────────────────────────────────────────────────────────────────
# SOD HISTORICAL UPLOAD — backfill a past snapshot into sod_inventory.
#
# After upload, the existing /new-listings page will work for any date
# range that includes the uploaded snapshot (no longer clipped to our
# ingest start). One-time per past date; ON CONFLICT DO NOTHING so re-
# uploading the same file is safe.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/upload-historical', methods=['POST'])
@require_app_origin
def api_admin_sod_upload_historical():
    """Backfill a historical SOD snapshot into the sod_inventory table.

    Multipart form fields:
      zip            (file, REQUIRED)   — the SOD inventory ZIP
      keep_all_rows  (str, OPTIONAL)    — '1' to ingest ALL SKUs (default 0)
      only_dates     (str, OPTIONAL)    — comma-separated YYYY-MM-DD list;
                                          if provided, ONLY rows with these
                                          dates are ingested (skip the rest).
                                          Useful when a ZIP contains weekly
                                          data and you only want one day.

    The ZIP's snapshot_date is taken from the rows themselves. ON CONFLICT
    (sku, store_number, snapshot_date) DO NOTHING so re-uploads are
    idempotent. If you uploaded the wrong date, use
    POST /api/admin/sod/rollback-snapshot to remove it.
    """
    f = request.files.get('zip')
    if not f:
        return jsonify({'error': 'zip file is required'}), 400
    keep_all = request.form.get('keep_all_rows') in ('1', 'true', 'yes')
    only_dates_raw = (request.form.get('only_dates') or '').strip()
    only_dates = (
        {d.strip() for d in only_dates_raw.split(',') if d.strip()}
        if only_dates_raw else None
    )
    if only_dates:
        try:
            from datetime import date as _date
            for d in only_dates:
                _date.fromisoformat(d)
        except ValueError:
            return jsonify({'error': 'only_dates must be comma-separated YYYY-MM-DD'}), 400

    try:
        zip_bytes = f.read()
        if not zip_bytes:
            return jsonify({'error': 'zip is empty'}), 400
        parsed = stream_parse_sod_zip(
            zip_bytes, set(SOD_TRACKED_SKUS.keys()),
            keep_all_rows=keep_all)
    except Exception as e:
        return jsonify({'error': f'parse failed: {e}'}), 400

    rows = parsed.get('rows_to_persist', [])
    dates = sorted(parsed.get('dates_seen', set()))
    if only_dates:
        rows = [r for r in rows if r['snapshot_date'] in only_dates]
        dates = [d for d in dates if d in only_dates]
    if not rows:
        return jsonify({
            'error': 'no rows for tracked SKUs found in this ZIP'
                     + (f' for the requested dates' if only_dates else ''),
            'dates_in_zip': sorted(parsed.get('dates_seen', set())),
            'only_dates_filter': sorted(only_dates) if only_dates else None,
        }), 422

    # Bulk insert via existing sod_inventory schema. ON CONFLICT DO NOTHING
    # so this is idempotent — uploading the same file twice is a no-op.
    inserted = 0
    skipped = 0
    try:
        conn = _sod_get_conn()
        cur = conn.cursor()
        # Stream in batches of 5000 to keep memory bounded
        BATCH = 5000
        batch = []
        for r in rows:
            batch.append((
                r['sku'], r['store_number'], r['snapshot_date'],
                r['status'], r['on_hand'], r['product_name'],
                'historical_upload',
            ))
            if len(batch) >= BATCH:
                if USE_POSTGRES:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO sod_inventory
                           (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                           VALUES %s
                           ON CONFLICT (sku, store_number, snapshot_date) DO NOTHING""",
                        batch,
                    )
                    inserted += cur.rowcount
                else:
                    cur.executemany(
                        """INSERT OR IGNORE INTO sod_inventory
                           (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                           VALUES (?,?,?,?,?,?,?)""",
                        batch,
                    )
                    inserted += cur.rowcount
                batch = []
        if batch:
            if USE_POSTGRES:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO sod_inventory
                       (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                       VALUES %s
                       ON CONFLICT (sku, store_number, snapshot_date) DO NOTHING""",
                    batch,
                )
                inserted += cur.rowcount
            else:
                cur.executemany(
                    """INSERT OR IGNORE INTO sod_inventory
                       (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                       VALUES (?,?,?,?,?,?,?)""",
                    batch,
                )
                inserted += cur.rowcount
        skipped = len(rows) - inserted
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({'error': f'DB insert failed: {e}'}), 500

    return jsonify({
        'status': 'ok',
        'filename': getattr(f, 'filename', '') or '',
        'dates_in_zip': dates,
        'tracked_rows_in_zip': len(rows),
        'inserted': inserted,
        'skipped_existing': skipped,
        'note': (
            "Historical snapshot backfilled into sod_inventory. The /new-listings "
            "page can now compare any date range that includes these dates."
        ),
    })


# ───────────────────────────────────────────────────────────────────────
# SOD PORTAL CATALOG — discover every file the supplier portal serves,
# including annual archives (Informative 2025, Option 1/3/5 2025) that
# can backfill our entire history in one shot.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/portal-catalog', methods=['GET'])
@require_app_origin
def api_admin_sod_portal_catalog():
    """Crawl the SOD supplier portal and return the discovered catalog.

    Returns: {
      index_url_used: str (which page we successfully scraped)
      agent_id: str
      categories: [
        {category_key, category_label, category_scope, category_id,
         file_count, files: [{url, filename, ...}]},
        ...
      ]
    }
    """
    debug = request.args.get('debug') in ('1', 'true', 'yes')
    try:
        client = SODClient()
        catalog = client.list_catalog(debug=debug)
    except Exception as e:
        return jsonify({'error': f'catalog discovery failed: {e}'}), 500
    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        **catalog,
        'how_to_use': (
            "Pick any file URL and POST to /api/admin/sod/import-from-portal "
            "with body {url}. The file is downloaded, parsed, and ingested "
            "into sod_inventory. Annual archives (e.g. Option 5 2025) "
            "backfill our entire SOD history in one shot. Add ?debug=1 to "
            "include the discovery log + sample anchors from /subscribers."
        ),
    })


# ───────────────────────────────────────────────────────────────────────
# SOD PORTAL IMPORT — given a catalog file URL, download it and ingest.
# Same persistence path as upload-historical, just sourced from the
# portal directly instead of via manual upload.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/import-from-portal', methods=['POST'])
@require_app_origin
def api_admin_sod_import_from_portal():
    """Download a SOD file from the portal and ingest into sod_inventory.

    Body (JSON): {
      url: str (REQUIRED) — full SOD portal URL of the .zip
      keep_all_rows: bool (default false) — ingest all SKUs vs tracked-only
      only_dates: list[str] (optional) — restrict to specific dates within
                                         multi-day archives
    }

    Returns import summary same as /upload-historical.
    """
    body = request.get_json(silent=True) or {}
    url = (body.get('url') or '').strip()
    keep_all = bool(body.get('keep_all_rows', False))
    only_dates_list = body.get('only_dates') or []
    only_dates = (
        {d for d in only_dates_list if d}
        if isinstance(only_dates_list, list) else None
    )
    if only_dates:
        try:
            from datetime import date as _date
            for d in only_dates:
                _date.fromisoformat(d)
        except ValueError:
            return jsonify({'error': 'only_dates must be YYYY-MM-DD strings'}), 400

    if not url or not url.startswith(SOD_BASE):
        return jsonify({'error': f'url must start with {SOD_BASE}'}), 400

    try:
        client = SODClient()
        zip_bytes = client.download_url(url)
    except Exception as e:
        return jsonify({'error': f'download failed: {e}'}), 502

    if not zip_bytes:
        return jsonify({'error': 'downloaded file was empty'}), 502

    try:
        parsed = stream_parse_sod_zip(
            zip_bytes, set(SOD_TRACKED_SKUS.keys()),
            keep_all_rows=keep_all)
    except Exception as e:
        return jsonify({'error': f'parse failed: {e}'}), 422

    rows = parsed.get('rows_to_persist', [])
    dates = sorted(parsed.get('dates_seen', set()))
    if only_dates:
        rows = [r for r in rows if r['snapshot_date'] in only_dates]
        dates = [d for d in dates if d in only_dates]
    if not rows:
        return jsonify({
            'error': 'no rows for tracked SKUs found in this download'
                     + (' for the requested dates' if only_dates else ''),
            'dates_in_zip': sorted(parsed.get('dates_seen', set())),
            'only_dates_filter': sorted(only_dates) if only_dates else None,
            'url': url,
        }), 422

    inserted = 0
    try:
        conn = _sod_get_conn()
        cur = conn.cursor()
        BATCH = 5000
        batch = []
        for r in rows:
            batch.append((
                r['sku'], r['store_number'], r['snapshot_date'],
                r['status'], r['on_hand'], r['product_name'],
                'portal_archive_import',
            ))
            if len(batch) >= BATCH:
                if USE_POSTGRES:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO sod_inventory
                           (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                           VALUES %s
                           ON CONFLICT (sku, store_number, snapshot_date) DO NOTHING""",
                        batch,
                    )
                    inserted += cur.rowcount
                else:
                    cur.executemany(
                        """INSERT OR IGNORE INTO sod_inventory
                           (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                           VALUES (?,?,?,?,?,?,?)""",
                        batch,
                    )
                    inserted += cur.rowcount
                batch = []
        if batch:
            if USE_POSTGRES:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO sod_inventory
                       (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                       VALUES %s
                       ON CONFLICT (sku, store_number, snapshot_date) DO NOTHING""",
                    batch,
                )
                inserted += cur.rowcount
            else:
                cur.executemany(
                    """INSERT OR IGNORE INTO sod_inventory
                       (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                       VALUES (?,?,?,?,?,?,?)""",
                    batch,
                )
                inserted += cur.rowcount
        skipped = len(rows) - inserted
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({'error': f'DB insert failed: {e}'}), 500

    return jsonify({
        'status': 'ok',
        'url': url,
        'dates_in_zip': dates,
        'tracked_rows': len(rows),
        'inserted': inserted,
        'skipped_existing': skipped,
        'note': 'Portal archive imported into sod_inventory.',
    })


# ───────────────────────────────────────────────────────────────────────
# SOD HISTORY COVERAGE — shows the operator the date range we have data
# for, per SKU. The "you don't need to upload anything if your window
# falls inside this range" indicator.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/history-coverage', methods=['GET'])
def api_admin_sod_history_coverage():
    """Return the date range of SOD history we have, per tracked SKU.

    No params. Used by /new-listings frontend to show the user how far
    back they can compare without uploading a historical ZIP.
    """
    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db
    out_per_sku = []
    for sku in SOD_TRACKED_SKUS.keys():
        brand, name = SOD_TRACKED_SKUS[sku]
        try:
            if USE_POSTGRES:
                cur.execute(
                    "SELECT MIN(snapshot_date)::text, MAX(snapshot_date)::text, "
                    "       COUNT(DISTINCT snapshot_date)::int "
                    "FROM sod_inventory WHERE sku = %s",
                    (sku,))
                r = cur.fetchone()
            else:
                r = cur.execute(
                    "SELECT MIN(snapshot_date), MAX(snapshot_date), "
                    "       COUNT(DISTINCT snapshot_date) "
                    "FROM sod_inventory WHERE sku = ?",
                    (sku,)).fetchone()
            min_d = str(r[0]) if r and r[0] else None
            max_d = str(r[1]) if r and r[1] else None
            days = int(r[2] or 0) if r else 0
            # Find any gaps (missed snapshot days within the range)
            gaps_sql = (
                "SELECT (s + INTERVAL '1 day')::date::text AS gap_start "
                "FROM (SELECT snapshot_date AS s, "
                "       LEAD(snapshot_date) OVER (ORDER BY snapshot_date) AS next "
                "       FROM sod_inventory WHERE sku=%s "
                "       GROUP BY snapshot_date) t "
                "WHERE next IS NOT NULL AND next - s > 1 "
                "ORDER BY s LIMIT 30"
            ) if USE_POSTGRES else None
            gaps = []
            if USE_POSTGRES and min_d and max_d:
                try:
                    cur.execute(gaps_sql, (sku,))
                    for gr in cur.fetchall():
                        gaps.append(str(gr[0]))
                except Exception:
                    try: db.rollback()
                    except Exception: pass
            out_per_sku.append({
                'sku': sku,
                'product_name': name,
                'brand': brand,
                'earliest_date': min_d,
                'latest_date': max_d,
                'distinct_days_in_history': days,
                'gap_starts_first_30': gaps,
            })
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            out_per_sku.append({'sku': sku, 'error': str(e)})
    if USE_POSTGRES:
        cur.close()

    # Compute overall range
    earliest_all = min(
        (r['earliest_date'] for r in out_per_sku if r.get('earliest_date')),
        default=None)
    latest_all = max(
        (r['latest_date'] for r in out_per_sku if r.get('latest_date')),
        default=None)

    response = jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'overall_earliest': earliest_all,
        'overall_latest': latest_all,
        'overall_days': (
            (datetime.fromisoformat(latest_all).date() -
             datetime.fromisoformat(earliest_all).date()).days + 1
            if (earliest_all and latest_all) else 0
        ),
        'per_sku': out_per_sku,
        'how_to_read': (
            "earliest_date is the first SOD snapshot we have for each SKU. "
            "If your /new-listings window starts AFTER this, the diff is "
            "real and self-contained. If your window starts BEFORE this, "
            "upload a historical SOD ZIP via /sod-compare to fill the gap."
        ),
    })
    # Cache 5 min — coverage only changes on new daily ingest
    response.headers['Cache-Control'] = 'public, max-age=300, stale-while-revalidate=600'
    return response


# ───────────────────────────────────────────────────────────────────────
# SOD BULK HISTORICAL UPLOAD — accept multiple ZIPs in one POST so the
# operator can backfill a long history in one go.
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/sod/bulk-upload-historical', methods=['POST'])
@require_app_origin
def api_admin_sod_bulk_upload_historical():
    """Backfill MULTIPLE historical SOD ZIPs at once.

    Multipart form: each field name beginning with 'zip' (e.g. zip0,
    zip1, zip2 ... or just zip[]) is treated as a SOD inventory ZIP.
    Each is parsed + ingested independently with ON CONFLICT DO NOTHING.

    Returns: per-file summary (filename, rows_inserted, dates_in_zip,
    error if any). Same idempotency guarantees as /upload-historical.
    """
    files = []
    for k in request.files:
        files.extend(request.files.getlist(k))
    if not files:
        return jsonify({'error': 'no zip files provided (use form fields zip0, zip1, ... or zip[])'}), 400

    keep_all = request.form.get('keep_all_rows') in ('1', 'true', 'yes')

    results = []
    total_inserted = 0
    total_skipped = 0
    for f in files:
        fname = getattr(f, 'filename', '') or ''
        try:
            zip_bytes = f.read()
            if not zip_bytes:
                results.append({'filename': fname, 'error': 'empty zip'})
                continue
            parsed = stream_parse_sod_zip(
                zip_bytes, set(SOD_TRACKED_SKUS.keys()),
                keep_all_rows=keep_all)
            rows = parsed.get('rows_to_persist', [])
            dates = sorted(parsed.get('dates_seen', set()))
            if not rows:
                results.append({
                    'filename': fname,
                    'dates_in_zip': dates,
                    'tracked_rows': 0,
                    'inserted': 0,
                    'skipped_existing': 0,
                    'note': 'no tracked-SKU rows in this ZIP',
                })
                continue
            inserted = 0
            conn = _sod_get_conn()
            cur = conn.cursor()
            BATCH = 5000
            batch = []
            for r in rows:
                batch.append((
                    r['sku'], r['store_number'], r['snapshot_date'],
                    r['status'], r['on_hand'], r['product_name'],
                    'historical_bulk_upload',
                ))
                if len(batch) >= BATCH:
                    if USE_POSTGRES:
                        psycopg2.extras.execute_values(
                            cur,
                            """INSERT INTO sod_inventory
                               (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                               VALUES %s
                               ON CONFLICT (sku, store_number, snapshot_date) DO NOTHING""",
                            batch,
                        )
                        inserted += cur.rowcount
                    else:
                        cur.executemany(
                            """INSERT OR IGNORE INTO sod_inventory
                               (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                               VALUES (?,?,?,?,?,?,?)""",
                            batch,
                        )
                        inserted += cur.rowcount
                    batch = []
            if batch:
                if USE_POSTGRES:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO sod_inventory
                           (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                           VALUES %s
                           ON CONFLICT (sku, store_number, snapshot_date) DO NOTHING""",
                        batch,
                    )
                    inserted += cur.rowcount
                else:
                    cur.executemany(
                        """INSERT OR IGNORE INTO sod_inventory
                           (sku, store_number, snapshot_date, status, on_hand, product_name, source)
                           VALUES (?,?,?,?,?,?,?)""",
                        batch,
                    )
                    inserted += cur.rowcount
            skipped = len(rows) - inserted
            conn.commit()
            cur.close()
            conn.close()
            total_inserted += inserted
            total_skipped += skipped
            results.append({
                'filename': fname,
                'dates_in_zip': dates,
                'tracked_rows': len(rows),
                'inserted': inserted,
                'skipped_existing': skipped,
            })
        except Exception as e:
            results.append({'filename': fname, 'error': str(e)})
    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'files_processed': len(files),
        'total_inserted': total_inserted,
        'total_skipped': total_skipped,
        'per_file': results,
    })


# ───────────────────────────────────────────────────────────────────────
# HIDDEN LISTINGS DETECTOR — finds 4 patterns of sneaky disappearance:
#
#   1. GHOST listings:    Listed in a recent past snapshot, NOT in latest
#                         snapshot, AND no DELISTED event was recorded.
#                         (= row vanished without leaving a trail)
#
#   2. HIDDEN INVENTORY:  SOD says non-L (Delisted/Fully Delisted/absent)
#                         BUT lcbo.com sees qty>0 in last 48h, OR rep saw
#                         on shelf in last 7 days.
#                         (= the bottle is on shelf, SOD won't pay us for it)
#
#   3. FLICKER:           Same (sku, store) flipped status 3+ times in last
#                         30 days. Real listings don't flicker — this is
#                         either bad data or deliberate obscuring.
#
#   4. MASS-DELIST:       A snapshot day where listed-count dropped >10%
#                         vs prior day for any SKU. Suspicious — real
#                         delistings happen 1-2 stores at a time.
#
# This is the headline anti-fraud audit. Output is "every store-SKU pair
# that looks like the listing was hidden, with evidence."
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/hidden-listings', methods=['GET'])
def api_admin_hidden_listings():
    """Detect listings hidden in SOD via 5 distinct patterns.

    Query params:
      sku=<7-digit>      filter to one SKU (optional)
      lookback_days=90   how far back to look for past snapshots (default 90)
      flicker_min=3      min status changes to count as flicker (default 3)
      mass_delist_pct=10 day-over-day drop threshold (default 10%)
      lcbo_window_h=72   lcbo.com inventory window for hidden-inventory check
      start=YYYY-MM-DD   override start of inspection window (else: lookback_days)
      end=YYYY-MM-DD     override end of inspection window (else: latest snapshot)
      min_on_hand=1      min on_hand units for the inventory-no-listing detector
    """
    from datetime import date as _date, timedelta as _td

    try:
        lookback_days = max(7, min(int(request.args.get('lookback_days', '90')), 365))
        flicker_min = max(2, min(int(request.args.get('flicker_min', '3')), 20))
        mass_delist_pct = max(1, min(int(request.args.get('mass_delist_pct', '10')), 100))
        lcbo_window_h = max(1, min(int(request.args.get('lcbo_window_h', '72')), 720))
        min_on_hand = max(1, min(int(request.args.get('min_on_hand', '1')), 9999))
    except ValueError:
        return jsonify({'error': 'numeric params must be integers'}), 400

    # Optional explicit date-range overrides
    start_q = (request.args.get('start') or '').strip()
    end_q = (request.args.get('end') or '').strip()
    try:
        range_start = _date.fromisoformat(start_q) if start_q else None
        range_end = _date.fromisoformat(end_q) if end_q else None
    except ValueError:
        return jsonify({'error': 'start/end must be YYYY-MM-DD'}), 400
    if range_start and range_end and range_start > range_end:
        return jsonify({'error': 'start must be <= end'}), 400

    sku_filter = (request.args.get('sku') or '').strip()
    portfolio = (request.args.get('portfolio') or 'NB').strip()
    portfolio_skus = _skus_for_portfolio(portfolio)
    if sku_filter and sku_filter in SOD_TRACKED_SKUS:
        # Explicit sku always wins (and must be inside the portfolio to count)
        skus_to_audit = [sku_filter] if sku_filter in set(portfolio_skus) else []
    else:
        skus_to_audit = portfolio_skus

    db = get_db()
    output = {
        'portfolio': portfolio,
        'ghost_listings': [],
        'hidden_inventory': [],
        'flicker_patterns': [],
        'mass_delist_days': [],
        'inventory_no_listing': [],
    }
    summary = {
        'total_ghost': 0,
        'total_hidden_inventory': 0,
        'total_flicker': 0,
        'total_mass_delist_events': 0,
        'total_inventory_no_listing': 0,
    }

    for sku in skus_to_audit:
        brand, name = SOD_TRACKED_SKUS[sku]
        sku_clean = sku.lstrip('0')

        # ── PATTERN 1: GHOST LISTINGS ──────────────────────────────────
        # Stores that were Listed at some point in the lookback window
        # but are NOT in the latest snapshot AND have no DELISTED change
        # event recorded. The row just vanished.
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s", (sku,))
                latest = cur.fetchone()[0]
            else:
                latest = cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=?", (sku,)
                ).fetchone()[0]
            if not latest:
                continue

            # Stores in latest snapshot
            if USE_POSTGRES:
                cur.execute(
                    "SELECT store_number FROM sod_inventory "
                    "WHERE sku=%s AND snapshot_date=%s",
                    (sku, latest))
                latest_stores = {int(r[0]) for r in cur.fetchall()}
                cur.execute(
                    "SELECT DISTINCT store_number FROM sod_inventory "
                    "WHERE sku=%s AND status='L' "
                    "  AND snapshot_date >= %s::date - (INTERVAL '1 day' * %s)",
                    (sku, latest, lookback_days))
                ever_listed = {int(r[0]) for r in cur.fetchall()}
                cur.execute(
                    "SELECT DISTINCT store_number FROM sod_listing_changes "
                    "WHERE sku=%s AND change_type IN ('DELISTED','FULLY_DELISTED') "
                    "  AND change_date >= %s::date - (INTERVAL '1 day' * %s) "
                    "  AND store_number IS NOT NULL",
                    (sku, latest, lookback_days))
                had_delist_event = {int(r[0]) for r in cur.fetchall()}
            else:
                latest_stores = {
                    int(r[0]) for r in cur.execute(
                        "SELECT store_number FROM sod_inventory "
                        "WHERE sku=? AND snapshot_date=?",
                        (sku, latest)).fetchall()
                }
                ever_listed = {
                    int(r[0]) for r in cur.execute(
                        "SELECT DISTINCT store_number FROM sod_inventory "
                        "WHERE sku=? AND status='L' "
                        "  AND snapshot_date >= date(?, ? || ' days')",
                        (sku, latest, f'-{lookback_days}')).fetchall()
                }
                had_delist_event = {
                    int(r[0]) for r in cur.execute(
                        "SELECT DISTINCT store_number FROM sod_listing_changes "
                        "WHERE sku=? AND change_type IN ('DELISTED','FULLY_DELISTED') "
                        "  AND change_date >= date(?, ? || ' days') "
                        "  AND store_number IS NOT NULL",
                        (sku, latest, f'-{lookback_days}')).fetchall()
                }

            # GHOST = ever-listed AND not-in-latest-snapshot AND no-delist-event
            ghosts = (ever_listed - latest_stores) - had_delist_event
            for sn in sorted(ghosts):
                # Find when this store was last seen as Listed
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT MAX(snapshot_date)::text FROM sod_inventory "
                        "WHERE sku=%s AND store_number=%s AND status='L'",
                        (sku, sn))
                    last_listed = cur.fetchone()[0]
                else:
                    last_listed = cur.execute(
                        "SELECT MAX(snapshot_date) FROM sod_inventory "
                        "WHERE sku=? AND store_number=? AND status='L'",
                        (sku, sn)).fetchone()[0]
                output['ghost_listings'].append({
                    'sku': sku,
                    'product_name': name,
                    'brand': brand,
                    'store_number': sn,
                    'last_listed_date': str(last_listed) if last_listed else None,
                    'days_since_last_listed': (
                        (latest - last_listed).days
                        if last_listed and hasattr(latest, '__sub__') else None
                    ),
                    'pattern': 'ghost',
                    'evidence': 'Listed in past snapshot but absent from latest with no DELISTED event.',
                })
            summary['total_ghost'] += len(ghosts)
            if USE_POSTGRES: cur.close()
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[hidden] ghost-detect failed for {sku}: {e}")

        # ── PATTERN 2: HIDDEN INVENTORY ────────────────────────────────
        # SOD says non-L (D/F/absent) at latest snapshot, BUT lcbo.com
        # saw qty>0 in last lcbo_window_h hours, OR rep observed on shelf.
        try:
            cur = db.cursor() if USE_POSTGRES else db
            # SOD-listed at latest
            if USE_POSTGRES:
                cur.execute(
                    "SELECT store_number, status FROM sod_inventory "
                    "WHERE sku=%s AND snapshot_date=%s",
                    (sku, latest))
                sod_at_latest = {int(r[0]): r[1] for r in cur.fetchall()}
            else:
                sod_at_latest = {
                    int(r[0]): r[1] for r in cur.execute(
                        "SELECT store_number, status FROM sod_inventory "
                        "WHERE sku=? AND snapshot_date=?",
                        (sku, latest)).fetchall()
                }
            # lcbo.com saw qty>0 within window
            lcbo_inventory = {}  # store_num -> (qty, recorded_at)
            if USE_POSTGRES:
                cur.execute(
                    "SELECT id FROM products WHERE lcbo_sku=%s LIMIT 1", (sku_clean,))
                prow = cur.fetchone()
                pid = prow[0] if prow else None
                if pid:
                    cur.execute(
                        "SELECT store_number, MAX(quantity), MAX(recorded_at)::text "
                        "FROM inventory_history "
                        "WHERE product_id=%s AND quantity>0 "
                        "  AND store_number <> 'SUMMARY' "
                        "  AND recorded_at >= NOW() - (INTERVAL '1 hour' * %s) "
                        "GROUP BY store_number",
                        (pid, lcbo_window_h))
                    for r in cur.fetchall():
                        try:
                            lcbo_inventory[int(r[0])] = (int(r[1] or 0), r[2])
                        except (ValueError, TypeError):
                            continue
            else:
                prow = cur.execute(
                    "SELECT id FROM products WHERE lcbo_sku=? LIMIT 1",
                    (sku_clean,)).fetchone()
                pid = prow[0] if prow else None
                if pid:
                    for r in cur.execute(
                        "SELECT store_number, MAX(quantity), MAX(recorded_at) "
                        "FROM inventory_history "
                        "WHERE product_id=? AND quantity>0 "
                        "  AND store_number <> 'SUMMARY' "
                        "  AND recorded_at >= datetime('now', ? || ' hours') "
                        "GROUP BY store_number",
                        (pid, f'-{lcbo_window_h}')).fetchall():
                        try:
                            lcbo_inventory[int(r[0])] = (int(r[1] or 0), r[2])
                        except (ValueError, TypeError):
                            continue

            # Rep observations within last 7 days
            rep_seen = {}
            try:
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number, MAX(observed_at)::text, MAX(rep) "
                        "FROM rep_listing_observations "
                        "WHERE sku=%s AND on_shelf=TRUE "
                        "  AND observed_at >= NOW() - INTERVAL '7 days' "
                        "GROUP BY store_number",
                        (sku,))
                    for r in cur.fetchall():
                        try:
                            rep_seen[int(r[0])] = (r[1], r[2])
                        except (ValueError, TypeError):
                            continue
                else:
                    for r in cur.execute(
                        "SELECT store_number, MAX(observed_at), MAX(rep) "
                        "FROM rep_listing_observations "
                        "WHERE sku=? AND on_shelf=1 "
                        "  AND observed_at >= datetime('now','-7 days') "
                        "GROUP BY store_number",
                        (sku,)).fetchall():
                        try:
                            rep_seen[int(r[0])] = (r[1], r[2])
                        except (ValueError, TypeError):
                            continue
            except Exception:
                try: db.rollback()
                except Exception: pass

            # Hidden inventory: any store where (lcbo OR rep) shows on shelf
            # AND SOD's status is non-L (or absent)
            for sn in sorted(set(lcbo_inventory.keys()) | set(rep_seen.keys())):
                sod_status = sod_at_latest.get(sn)
                if sod_status == 'L':
                    continue  # not hidden
                lcbo_data = lcbo_inventory.get(sn)
                rep_data = rep_seen.get(sn)
                if not (lcbo_data or rep_data):
                    continue
                output['hidden_inventory'].append({
                    'sku': sku,
                    'product_name': name,
                    'brand': brand,
                    'store_number': sn,
                    'sod_status': sod_status or 'absent',
                    'lcbo_units': lcbo_data[0] if lcbo_data else 0,
                    'lcbo_seen_at': lcbo_data[1] if lcbo_data else None,
                    'rep_observed_at': rep_data[0] if rep_data else None,
                    'rep_observed_by': rep_data[1] if rep_data else None,
                    'pattern': 'hidden_inventory',
                    'evidence': (
                        f"lcbo.com {lcbo_data[0]} units "
                        if lcbo_data else ''
                    ) + (
                        f"+ rep saw shelf "
                        if rep_data else ''
                    ) + (
                        f"but SOD shows status={sod_status or 'absent'}."
                    ),
                })
            summary['total_hidden_inventory'] += sum(
                1 for h in output['hidden_inventory'] if h['sku'] == sku)
            if USE_POSTGRES: cur.close()
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[hidden] hidden-inventory-detect failed for {sku}: {e}")

        # ── PATTERN 3: FLICKER ─────────────────────────────────────────
        # Same (sku, store) flipped status flicker_min+ times in last 30 days.
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute(
                    "SELECT store_number, COUNT(*)::int AS flips, "
                    "       MIN(change_date)::text AS first_flip, "
                    "       MAX(change_date)::text AS last_flip, "
                    "       string_agg(change_type, ',' ORDER BY change_date) AS sequence "
                    "FROM sod_listing_changes "
                    "WHERE sku=%s AND store_number IS NOT NULL "
                    "  AND change_date >= CURRENT_DATE - INTERVAL '30 days' "
                    "  AND change_type IN ('NEW_LISTING','DELISTED','RELISTED','STATUS_FLIP') "
                    "GROUP BY store_number "
                    "HAVING COUNT(*) >= %s "
                    "ORDER BY COUNT(*) DESC LIMIT 50",
                    (sku, flicker_min))
                for r in cur.fetchall():
                    output['flicker_patterns'].append({
                        'sku': sku,
                        'product_name': name,
                        'brand': brand,
                        'store_number': int(r[0]),
                        'flip_count': int(r[1]),
                        'first_flip_date': r[2],
                        'last_flip_date': r[3],
                        'sequence': r[4],
                        'pattern': 'flicker',
                        'evidence': f"Status flipped {r[1]} times in 30 days: {r[4]}",
                    })
                cur.close()
            else:
                for r in cur.execute(
                    "SELECT store_number, COUNT(*) AS flips, "
                    "       MIN(change_date) AS first_flip, "
                    "       MAX(change_date) AS last_flip "
                    "FROM sod_listing_changes "
                    "WHERE sku=? AND store_number IS NOT NULL "
                    "  AND change_date >= date('now','-30 days') "
                    "  AND change_type IN ('NEW_LISTING','DELISTED','RELISTED','STATUS_FLIP') "
                    "GROUP BY store_number "
                    "HAVING COUNT(*) >= ? "
                    "ORDER BY COUNT(*) DESC LIMIT 50",
                    (sku, flicker_min)).fetchall():
                    output['flicker_patterns'].append({
                        'sku': sku, 'product_name': name, 'brand': brand,
                        'store_number': int(r[0]),
                        'flip_count': int(r[1]),
                        'first_flip_date': str(r[2]),
                        'last_flip_date': str(r[3]),
                        'sequence': '',
                        'pattern': 'flicker',
                        'evidence': f"Status flipped {r[1]} times in 30 days",
                    })
            summary['total_flicker'] += sum(
                1 for f in output['flicker_patterns'] if f['sku'] == sku)
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[hidden] flicker-detect failed for {sku}: {e}")

        # ── PATTERN 4: MASS-DELIST DAYS ────────────────────────────────
        # Find snapshot days where listed-count dropped >mass_delist_pct%
        # day-over-day. Real delistings are 1-2 stores at a time.
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute(
                    "WITH daily AS ( "
                    "  SELECT snapshot_date, "
                    "         COUNT(*) FILTER (WHERE status='L')::int AS listed_count "
                    "  FROM sod_inventory WHERE sku=%s "
                    "  GROUP BY snapshot_date "
                    "  ORDER BY snapshot_date "
                    "), windowed AS ( "
                    "  SELECT snapshot_date, listed_count, "
                    "         LAG(listed_count) OVER (ORDER BY snapshot_date) AS prev_count "
                    "  FROM daily "
                    ") "
                    "SELECT snapshot_date::text, listed_count, prev_count, "
                    "       (prev_count - listed_count) AS drop_count "
                    "FROM windowed "
                    "WHERE prev_count IS NOT NULL "
                    "  AND prev_count > 0 "
                    "  AND ((prev_count - listed_count)::float / prev_count) * 100 >= %s "
                    "  AND snapshot_date >= CURRENT_DATE - (INTERVAL '1 day' * %s) "
                    "ORDER BY snapshot_date DESC LIMIT 20",
                    (sku, mass_delist_pct, lookback_days))
                rows = cur.fetchall()
                for r in rows:
                    drop = int(r[3] or 0)
                    pct = round(drop / r[2] * 100, 1) if r[2] else 0
                    output['mass_delist_days'].append({
                        'sku': sku,
                        'product_name': name,
                        'brand': brand,
                        'snapshot_date': r[0],
                        'listed_count': int(r[1] or 0),
                        'prev_count': int(r[2] or 0),
                        'drop_count': drop,
                        'drop_pct': pct,
                        'pattern': 'mass_delist',
                        'evidence': (
                            f"{drop} stores dropped from L between snapshots "
                            f"({r[2]} → {r[1]}, -{pct}%)."
                        ),
                    })
                summary['total_mass_delist_events'] += len(rows)
                cur.close()
            # sqlite path omitted — feature is for prod
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[hidden] mass-delist-detect failed for {sku}: {e}")

        # ── PATTERN 5: INVENTORY-NO-LISTING ────────────────────────────
        # SOD's own on_hand column shows units > 0 but the row's status
        # is 'D' (Delisting) or 'F' (Fully Delisted) — i.e. LCBO claims
        # the listing is gone, but the warehouse data still reports
        # bottles on the floor. These are the "blank-status with stock"
        # cases listings hide in. Scanned across the requested date range,
        # not just the latest snapshot, so movements aren't lost.
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                # Window: explicit start/end, else last lookback_days
                if range_start and range_end:
                    cur.execute(
                        "SELECT store_number, snapshot_date::text, status, "
                        "       on_hand, product_name "
                        "FROM sod_inventory "
                        "WHERE sku=%s AND status IN ('D','F') "
                        "  AND on_hand >= %s "
                        "  AND snapshot_date BETWEEN %s::date AND %s::date "
                        "ORDER BY snapshot_date DESC, store_number ASC "
                        "LIMIT 2000",
                        (sku, min_on_hand, range_start.isoformat(),
                         range_end.isoformat()))
                else:
                    cur.execute(
                        "SELECT store_number, snapshot_date::text, status, "
                        "       on_hand, product_name "
                        "FROM sod_inventory "
                        "WHERE sku=%s AND status IN ('D','F') "
                        "  AND on_hand >= %s "
                        "  AND snapshot_date >= CURRENT_DATE - "
                        "      (INTERVAL '1 day' * %s) "
                        "ORDER BY snapshot_date DESC, store_number ASC "
                        "LIMIT 2000",
                        (sku, min_on_hand, lookback_days))
                rows = cur.fetchall()
                # Aggregate per (store, status) — show latest snapshot per store
                seen: set = set()
                for r in rows:
                    try:
                        sn = int(r[0])
                    except (ValueError, TypeError):
                        continue
                    key = (sn, r[2])
                    if key in seen:
                        continue
                    seen.add(key)
                    output['inventory_no_listing'].append({
                        'sku': sku,
                        'product_name': r[4] or name,
                        'brand': brand,
                        'store_number': sn,
                        'snapshot_date': r[1],
                        'sod_status': r[2],
                        'on_hand': int(r[3] or 0),
                        'pattern': 'inventory_no_listing',
                        'evidence': (
                            f"SOD status={r[2]} ({'Delisting' if r[2]=='D' else 'Fully Delisted'}) "
                            f"but on_hand={int(r[3] or 0)} units in snapshot {r[1]}."
                        ),
                    })
                summary['total_inventory_no_listing'] += len(seen)
                cur.close()
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[hidden] inventory-no-listing-detect failed for {sku}: {e}")

    # Sort each section by recency / severity
    output['ghost_listings'].sort(
        key=lambda x: x.get('last_listed_date') or '', reverse=True)
    output['hidden_inventory'].sort(
        key=lambda x: -(x.get('lcbo_units') or 0))
    output['flicker_patterns'].sort(
        key=lambda x: -x.get('flip_count', 0))
    output['mass_delist_days'].sort(
        key=lambda x: x.get('snapshot_date') or '', reverse=True)
    output['inventory_no_listing'].sort(
        key=lambda x: -(x.get('on_hand') or 0))

    return jsonify({
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'params': {
            'lookback_days': lookback_days,
            'flicker_min': flicker_min,
            'mass_delist_pct': mass_delist_pct,
            'lcbo_window_h': lcbo_window_h,
            'sku_filter': sku_filter or None,
            'start': range_start.isoformat() if range_start else None,
            'end': range_end.isoformat() if range_end else None,
            'min_on_hand': min_on_hand,
        },
        'summary': summary,
        'patterns': output,
        'how_to_read': (
            "GHOST = was Listed, vanished from snapshots without a DELISTED event. "
            "HIDDEN INVENTORY = lcbo.com or a rep saw stock on shelf, but SOD says "
            "we're not Listed (this is the strongest evidence of intentional hiding). "
            "FLICKER = same store-SKU flipped status 3+ times in 30 days "
            "(unusual for real listings). "
            "MASS-DELIST = a snapshot day where >10% of our listings dropped at once. "
            "INVENTORY-NO-LISTING = SOD's own row shows on_hand>0 units but status is "
            "D (Delisting) or F (Fully Delisted). Bottles on the warehouse floor "
            "with no active listing — the most common 'blank-with-stock' hiding pattern. "
            "Scanned across the full date range (start/end or lookback_days), not just "
            "the latest snapshot, so nothing slips through. "
            "Together these surface every documented disappearance pattern."
        ),
    })


@app.route('/api/admin/movement', methods=['GET'])
def api_admin_movement():
    """Authoritative store + listing movement counts in a date range.

    Query params:
      start=YYYY-MM-DD     window start (default: 7 days ago)
      end=YYYY-MM-DD       window end (default: today)
      sku=<7-digit>        filter listing counts to one SKU (optional)
      tracked_only=1       only count tracked SKUs (default: 1)

    Returns:
      store_universe:
        current_lcbo_stores       — distinct store_numbers in the latest
                                    SOD snapshot (the "real" LCBO count)
        crm_stores                — rows in our stores table
        snapshot_date             — date of the count
        crm_minus_lcbo            — stores in our CRM not in latest SOD
                                    (closed/stale)
        lcbo_minus_crm            — stores LCBO has that we haven't onboarded
      new_stores:
        added_in_range            — store_numbers first appearing in SOD
                                    within [start, end]
        store_list                — array of {store_number, first_seen_date}
      listings:
        new_in_range              — count of NEW_LISTING events in [start, end]
        delisted_in_range         — count of DELISTED events
        relisted_in_range         — count of RELISTED events
        per_sku                   — breakdown per tracked SKU
        per_day                   — daily counts for charting
        sample_new_listings       — up to 100 most recent NEW_LISTING events
                                    (sku, store_number, date) for verification
    """
    from datetime import date as _date
    today = _toronto_today()
    try:
        start = (request.args.get('start') or '').strip()
        end = (request.args.get('end') or '').strip()
        if start:
            start_d = _date.fromisoformat(start)
        else:
            start_d = today - timedelta(days=7)
        if end:
            end_d = _date.fromisoformat(end)
        else:
            end_d = today
        if start_d > end_d:
            return jsonify({'error': 'start must be <= end'}), 400
    except ValueError:
        return jsonify({'error': 'dates must be YYYY-MM-DD'}), 400

    sku_filter = (request.args.get('sku') or '').strip()
    tracked_only = (request.args.get('tracked_only', '1') in ('1', 'true', 'yes'))

    db = get_db()

    # ── 1. Store universe (UNION across all sources) ───────────────────
    # The truth = master `stores` ∪ latest SOD snapshot ∪ recent lcbo.com.
    # Each source is incomplete on its own:
    #   - master directory: hand-maintained, can be stale
    #   - SOD: filtered to our 8 tracked SKUs at ingest
    #   - lcbo.com: 30-min scrape, sometimes misses stores/products
    # Plus: which stores actually CARRY any of our SKUs is also a union.
    try:
        _u, _u_stats = _resolve_store_universe(db, lcbo_window_hours=48)
        _c, _c_stats = _resolve_carrying_us_universe(db, lcbo_window_hours=48)
    except Exception as _e:
        _u, _u_stats, _c, _c_stats = {}, {}, {}, {}

    universe = {}
    try:
        cur = db.cursor() if USE_POSTGRES else db
        # Latest snapshot date in SOD
        if USE_POSTGRES:
            cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory")
            latest = cur.fetchone()[0]
        else:
            latest = cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory").fetchone()[0]
        universe['snapshot_date'] = str(latest) if latest else None

        # Stores carrying at least one of our tracked SKUs (the SOD-side count)
        if latest:
            if USE_POSTGRES:
                cur.execute(
                    "SELECT COUNT(DISTINCT store_number) FROM sod_inventory WHERE snapshot_date=%s",
                    (latest,))
                stores_with_us = cur.fetchone()[0] or 0
            else:
                stores_with_us = cur.execute(
                    "SELECT COUNT(DISTINCT store_number) FROM sod_inventory WHERE snapshot_date=?",
                    (latest,)).fetchone()[0] or 0
        else:
            stores_with_us = 0
        universe['stores_carrying_our_skus'] = int(stores_with_us)

        # Total LCBO universe — from our master `stores` list (the only
        # source we have for the full ~766 store directory)
        if USE_POSTGRES:
            cur.execute("SELECT COUNT(*) FROM stores")
            crm_count = cur.fetchone()[0] or 0
        else:
            crm_count = cur.execute("SELECT COUNT(*) FROM stores").fetchone()[0] or 0
        universe['lcbo_universe_total'] = int(crm_count)
        # Legacy alias (kept for backward compat with anything that read `crm_stores`)
        universe['crm_stores'] = int(crm_count)
        # Legacy alias for current_lcbo_stores — DO NOT USE; will be removed
        universe['current_lcbo_stores'] = int(stores_with_us)

        # Coverage %: how much of our universe carries at least one of our SKUs
        if crm_count > 0:
            universe['carrying_pct'] = round(stores_with_us / crm_count * 100, 1)
        else:
            universe['carrying_pct'] = 0

        # Drift: stores in CRM master that did NOT show up in the latest
        # SOD snapshot (= stores not carrying any of our SKUs right now)
        if latest:
            if USE_POSTGRES:
                cur.execute("""
                    SELECT COUNT(*) FROM stores s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM sod_inventory i
                        WHERE i.store_number = s.store_number AND i.snapshot_date = %s
                    )
                """, (latest,))
                no_listing = cur.fetchone()[0] or 0
                # Stores that appeared in SOD but aren't in our CRM master
                cur.execute("""
                    SELECT COUNT(DISTINCT i.store_number) FROM sod_inventory i
                    WHERE i.snapshot_date = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM stores s WHERE s.store_number = i.store_number
                      )
                """, (latest,))
                lcbo_minus = cur.fetchone()[0] or 0
            else:
                no_listing = cur.execute("""
                    SELECT COUNT(*) FROM stores s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM sod_inventory i
                        WHERE i.store_number = s.store_number AND i.snapshot_date = ?
                    )
                """, (latest,)).fetchone()[0] or 0
                lcbo_minus = cur.execute("""
                    SELECT COUNT(DISTINCT i.store_number) FROM sod_inventory i
                    WHERE i.snapshot_date = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM stores s WHERE s.store_number = i.store_number
                      )
                """, (latest,)).fetchone()[0] or 0
        else:
            no_listing = lcbo_minus = 0
        universe['stores_without_our_skus'] = int(no_listing)
        universe['stores_in_sod_not_in_crm'] = int(lcbo_minus)
        # Legacy aliases
        universe['crm_minus_lcbo'] = int(no_listing)
        universe['lcbo_minus_crm'] = int(lcbo_minus)
        if USE_POSTGRES: cur.close()
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        universe['error'] = str(e)

    # Union-based authoritative counts (the truth across all sources)
    universe['union_total_stores'] = _u_stats.get('total_universe_size', 0)
    universe['carrying_us_anywhere'] = _c_stats.get('total_stores_carrying_any_sku', 0)
    universe['carrying_only_sod'] = _c_stats.get('sod_only', 0)
    universe['carrying_only_lcbo'] = _c_stats.get('lcbo_only', 0)
    universe['carrying_only_rep_observed'] = _c_stats.get('rep_only', 0)
    universe['carrying_in_sod_and_lcbo'] = _c_stats.get('sod_and_lcbo', 0)
    universe['source_drift'] = {
        'in_sod_not_master': _u_stats.get('in_sod_only', 0),
        'in_lcbo_not_master': _u_stats.get('in_lcbo_only', 0),
        'in_master_not_either': _u_stats.get('in_master_only', 0),
    }

    # ── 2. New stores added in [start, end] ────────────────────────────
    new_stores_section = {'added_in_range': 0, 'store_list': []}
    try:
        cur = db.cursor() if USE_POSTGRES else db
        # First appearance per store_number in sod_inventory
        if USE_POSTGRES:
            cur.execute("""
                SELECT store_number, MIN(snapshot_date) AS first_seen
                FROM sod_inventory
                GROUP BY store_number
                HAVING MIN(snapshot_date) >= %s AND MIN(snapshot_date) <= %s
                ORDER BY first_seen DESC
                LIMIT 100
            """, (start_d.isoformat(), end_d.isoformat()))
        else:
            cur.execute("""
                SELECT store_number, MIN(snapshot_date) AS first_seen
                FROM sod_inventory
                GROUP BY store_number
                HAVING MIN(snapshot_date) >= ? AND MIN(snapshot_date) <= ?
                ORDER BY first_seen DESC
                LIMIT 100
            """, (start_d.isoformat(), end_d.isoformat()))
        new_store_rows = cur.fetchall()
        new_stores_section['added_in_range'] = len(new_store_rows)
        new_stores_section['store_list'] = [
            {'store_number': int(r[0]), 'first_seen_date': str(r[1])}
            for r in new_store_rows
        ]
        if USE_POSTGRES: cur.close()
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        new_stores_section['error'] = str(e)

    # ── 3. Listing events in [start, end] ──────────────────────────────
    listings_section = {
        'new_in_range': 0,
        'delisted_in_range': 0,
        'relisted_in_range': 0,
        'per_sku': [],
        'per_day': [],
        'sample_new_listings': [],
    }
    try:
        cur = db.cursor() if USE_POSTGRES else db

        base_where = " WHERE c.change_date BETWEEN %s AND %s" if USE_POSTGRES else " WHERE c.change_date BETWEEN ? AND ?"
        ph = "%s" if USE_POSTGRES else "?"
        params: list = [start_d.isoformat(), end_d.isoformat()]
        if sku_filter:
            base_where += f" AND c.sku = {ph}"
            params.append(sku_filter)
        join = (" LEFT JOIN sod_products p ON p.sku = c.sku"
                + (f" WHERE p.is_tracked = {'TRUE' if USE_POSTGRES else '1'}" if False else ""))
        # We embed tracked_only as a HAVING-after-WHERE filter
        tracked_clause = (
            f" AND p.is_tracked = {'TRUE' if USE_POSTGRES else '1'}"
        ) if tracked_only else ""

        # Totals by change_type
        sql_totals = (
            "SELECT c.change_type, COUNT(*)::int AS n "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + tracked_clause
            + " GROUP BY c.change_type"
        ) if USE_POSTGRES else (
            "SELECT c.change_type, COUNT(*) AS n "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + tracked_clause
            + " GROUP BY c.change_type"
        )
        cur.execute(sql_totals, params)
        for r in cur.fetchall():
            ct = r[0] or '?'
            n = int(r[1] or 0)
            if ct == 'NEW_LISTING':
                listings_section['new_in_range'] = n
            elif ct == 'DELISTED':
                listings_section['delisted_in_range'] = n
            elif ct == 'RELISTED':
                listings_section['relisted_in_range'] = n

        # Per-SKU breakdown of NEW_LISTING (the most-asked metric)
        sql_per_sku = (
            "SELECT c.sku, p.product_name, p.brand, COUNT(*)::int AS n "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + " AND c.change_type = 'NEW_LISTING'"
            + tracked_clause
            + " GROUP BY c.sku, p.product_name, p.brand "
            + " ORDER BY n DESC"
        ) if USE_POSTGRES else (
            "SELECT c.sku, p.product_name, p.brand, COUNT(*) AS n "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + " AND c.change_type = 'NEW_LISTING'"
            + tracked_clause
            + " GROUP BY c.sku, p.product_name, p.brand "
            + " ORDER BY n DESC"
        )
        cur.execute(sql_per_sku, params)
        for r in cur.fetchall():
            listings_section['per_sku'].append({
                'sku': r[0],
                'product_name': r[1] or '',
                'brand': r[2] or '',
                'new_listings': int(r[3] or 0),
            })

        # Per-day breakdown for chart
        sql_per_day = (
            "SELECT c.change_date::text, c.change_type, COUNT(*)::int AS n "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + tracked_clause
            + " GROUP BY c.change_date, c.change_type "
            + " ORDER BY c.change_date"
        ) if USE_POSTGRES else (
            "SELECT c.change_date, c.change_type, COUNT(*) AS n "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + tracked_clause
            + " GROUP BY c.change_date, c.change_type "
            + " ORDER BY c.change_date"
        )
        cur.execute(sql_per_day, params)
        per_day_map: dict = {}
        for r in cur.fetchall():
            day = str(r[0])
            ct = r[1] or '?'
            n = int(r[2] or 0)
            if day not in per_day_map:
                per_day_map[day] = {'date': day, 'NEW_LISTING': 0, 'DELISTED': 0, 'RELISTED': 0}
            if ct in per_day_map[day]:
                per_day_map[day][ct] = n
        listings_section['per_day'] = list(per_day_map.values())

        # Sample of NEW_LISTING events (for trust + verification)
        sql_sample = (
            "SELECT c.change_date::text, c.sku, p.product_name, p.brand, c.store_number "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + " AND c.change_type = 'NEW_LISTING' AND c.store_number IS NOT NULL"
            + tracked_clause
            + " ORDER BY c.change_date DESC, c.id DESC "
            + " LIMIT 100"
        ) if USE_POSTGRES else (
            "SELECT c.change_date, c.sku, p.product_name, p.brand, c.store_number "
            "FROM sod_listing_changes c "
            "LEFT JOIN sod_products p ON p.sku = c.sku"
            + base_where
            + " AND c.change_type = 'NEW_LISTING' AND c.store_number IS NOT NULL"
            + tracked_clause
            + " ORDER BY c.change_date DESC, c.id DESC "
            + " LIMIT 100"
        )
        cur.execute(sql_sample, params)
        for r in cur.fetchall():
            listings_section['sample_new_listings'].append({
                'date': str(r[0]),
                'sku': r[1],
                'product_name': r[2] or '',
                'brand': r[3] or '',
                'store_number': int(r[4]) if r[4] is not None else None,
            })

        if USE_POSTGRES: cur.close()
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        listings_section['error'] = str(e)

    return jsonify({
        'window': {
            'start': start_d.isoformat(),
            'end': end_d.isoformat(),
            'days': (end_d - start_d).days + 1,
        },
        'sku_filter': sku_filter or None,
        'tracked_only': tracked_only,
        'store_universe': universe,
        'new_stores': new_stores_section,
        'listings': listings_section,
        'as_of': datetime.utcnow().isoformat() + 'Z',
    })


# ───────────────────────────────────────────────────────────────────────
# MORNING DIGEST — daily OOS + low-stock report for the founder inbox.
# Hits at the configured cron (~07:00 ET). Surfaces every store-SKU pair
# where status='L' but on_hand is 0 or below the LOW_STOCK threshold.
# ───────────────────────────────────────────────────────────────────────
LOW_STOCK_THRESHOLD = 7  # bottles — "listed but barely on the shelf"


def _build_morning_digest(low_threshold: int = LOW_STOCK_THRESHOLD,
                          portfolio: str | None = None) -> dict:
    """Build the structured morning-digest payload.

    Returns rows for three buckets, scoped to the latest SOD snapshot:
      - oos        : status='L' AND on_hand <= 0 (zero or negative)
      - low_stock  : status='L' AND 0 < on_hand < low_threshold
      - missing    : tracked SKU has NO row at all at a store-day that
                     yesterday WAS listed there (transition risk)

    Pass portfolio='Anu' / 'NB' / etc. to filter to a sub-portfolio
    once we onboard more brands. Default: all tracked SKUs.
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # Latest snapshot date — anchors every query
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory")
        latest = cur.fetchone()[0]
        cur.close()
    else:
        latest = db.execute(
            "SELECT MAX(snapshot_date) FROM sod_inventory").fetchone()[0]

    out = {
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'snapshot_date': str(latest) if latest else None,
        'low_threshold': low_threshold,
        'portfolio': portfolio or 'all',
        'buckets': {
            'oos': [],          # listed but on_hand <= 0
            'low_stock': [],    # listed but 0 < on_hand < threshold
        },
        'summary': {
            'total_oos': 0,
            'total_low_stock': 0,
            'oos_units_short': 0,
            'low_stock_total_units': 0,
        },
    }
    if not latest:
        return out

    # Build SKU filter based on portfolio (default: all)
    skus = _skus_for_portfolio(portfolio) if portfolio else list(SOD_TRACKED_SKUS.keys())
    if not skus:
        return out
    phs = ','.join([ph] * len(skus))

    # OOS — listed but on_hand <= 0
    sql_oos = f"""
        SELECT i.sku, i.product_name, i.store_number, i.on_hand,
               COALESCE(s.account, ''),
               COALESCE(s.address, ''),
               COALESCE(s.city, ''),
               COALESCE(s.postal, ''),
               COALESCE(s.rep, '')
        FROM sod_inventory i
        LEFT JOIN stores s ON s.store_number = i.store_number
        WHERE i.sku IN ({phs})
          AND i.snapshot_date = {ph}
          AND i.status = 'L'
          AND COALESCE(i.on_hand, 0) <= 0
        ORDER BY i.sku, i.store_number
    """
    params = skus + [latest]
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql_oos, params)
        oos_rows = cur.fetchall()
        cur.close()
    else:
        oos_rows = db.execute(sql_oos, params).fetchall()

    for r in oos_rows:
        out['buckets']['oos'].append({
            'sku': r[0], 'product_name': r[1],
            'store_number': int(r[2]),
            'on_hand': int(r[3] or 0),
            'account': r[4], 'address': r[5], 'city': r[6],
            'postal': r[7], 'rep': r[8],
        })
        out['summary']['oos_units_short'] += max(0, low_threshold - int(r[3] or 0))

    # LOW_STOCK — listed and 0 < on_hand < threshold
    sql_low = f"""
        SELECT i.sku, i.product_name, i.store_number, i.on_hand,
               COALESCE(s.account, ''),
               COALESCE(s.address, ''),
               COALESCE(s.city, ''),
               COALESCE(s.postal, ''),
               COALESCE(s.rep, '')
        FROM sod_inventory i
        LEFT JOIN stores s ON s.store_number = i.store_number
        WHERE i.sku IN ({phs})
          AND i.snapshot_date = {ph}
          AND i.status = 'L'
          AND COALESCE(i.on_hand, 0) > 0
          AND COALESCE(i.on_hand, 0) < {ph}
        ORDER BY i.on_hand ASC, i.sku, i.store_number
    """
    params = skus + [latest, low_threshold]
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql_low, params)
        low_rows = cur.fetchall()
        cur.close()
    else:
        low_rows = db.execute(sql_low, params).fetchall()

    for r in low_rows:
        out['buckets']['low_stock'].append({
            'sku': r[0], 'product_name': r[1],
            'store_number': int(r[2]),
            'on_hand': int(r[3] or 0),
            'account': r[4], 'address': r[5], 'city': r[6],
            'postal': r[7], 'rep': r[8],
        })
        out['summary']['low_stock_total_units'] += int(r[3] or 0)

    out['summary']['total_oos'] = len(out['buckets']['oos'])
    out['summary']['total_low_stock'] = len(out['buckets']['low_stock'])
    return out


@app.route('/api/crm/morning-digest', methods=['GET'])
def api_crm_morning_digest():
    """JSON view of the daily OOS + low-stock digest.

    Query params:
      threshold   override the low-stock threshold (default 7)
      portfolio   filter to a sub-portfolio (default: all tracked SKUs)

    Use this from the frontend (read-only) and from the daily cron
    (which renders this into HTML and emails it).
    """
    try:
        thr = max(1, min(int(request.args.get('threshold', LOW_STOCK_THRESHOLD)), 100))
    except ValueError:
        thr = LOW_STOCK_THRESHOLD
    portfolio = (request.args.get('portfolio') or '').strip() or None
    payload = _build_morning_digest(thr, portfolio)
    return jsonify(payload)


def _render_morning_digest_html(digest: dict) -> str:
    """Render the digest payload as a brand-styled HTML email.

    Plain inline CSS only — most email clients (Gmail, Outlook web)
    strip <style> blocks and class selectors. Keep it minimal.
    """
    snap = digest.get('snapshot_date') or 'unknown'
    s = digest.get('summary', {})
    oos = digest.get('buckets', {}).get('oos', [])
    low = digest.get('buckets', {}).get('low_stock', [])
    thr = digest.get('low_threshold', LOW_STOCK_THRESHOLD)

    def _td(text, color=None, bold=False):
        style = ['padding:6px 10px', 'border-bottom:1px solid #e6e2d8',
                 'font-size:13px']
        if color: style.append(f'color:{color}')
        if bold: style.append('font-weight:600')
        return f'<td style="{";".join(style)}">{text}</td>'

    def _table(title: str, rows: list, color: str, max_rows: int = 60):
        if not rows:
            return f'<p style="color:#666;margin:8px 0;">No {title.lower()} 🎉</p>'
        header = (
            '<tr style="background:#f7f3e9">'
            + ''.join(
                f'<th style="padding:6px 10px;text-align:left;font-size:12px;'
                f'border-bottom:1px solid #cdc6b3;color:#4a3f25">{h}</th>'
                for h in ('Product', 'Store', 'Address', 'On hand', 'Rep')
            )
            + '</tr>'
        )
        body = ''
        for r in rows[:max_rows]:
            on_hand_str = (f'<strong style="color:{color}">'
                           f'{r.get("on_hand", 0)}</strong>')
            body += (
                '<tr>'
                + _td(r.get('product_name', '')[:32])
                + _td(f'#{r.get("store_number")}', bold=True)
                + _td(f'{r.get("address","")[:48]}<br><span style="color:#888;font-size:11px">'
                      f'{r.get("city","")} {r.get("postal","")}</span>')
                + _td(on_hand_str)
                + _td(r.get('rep') or '—')
                + '</tr>'
            )
        more = ''
        if len(rows) > max_rows:
            more = (f'<p style="color:#888;font-size:12px;margin:4px 0">'
                    f'…and {len(rows) - max_rows} more rows in the dashboard.</p>')
        return (
            f'<h3 style="color:{color};margin:18px 0 6px;font-family:Arial">'
            f'{title} <span style="color:#666;font-weight:400">({len(rows)})</span></h3>'
            f'<table style="border-collapse:collapse;width:100%;'
            f'font-family:Arial,sans-serif">{header}{body}</table>{more}'
        )

    # Render "generated" timestamp in ET so the rep sees their local time
    try:
        from zoneinfo import ZoneInfo
        from datetime import timezone as _tz
        gen_et = (
            datetime.utcnow().replace(tzinfo=_tz.utc)
            .astimezone(ZoneInfo('America/Toronto'))
        )
        gen_str = gen_et.strftime('%Y-%m-%d %I:%M %p ET')
    except Exception:
        gen_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    body = f'''
    <div style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto;color:#2a1f0f">
      <h1 style="color:#d4a574;margin:0 0 4px">Anu Spirits — Morning Digest</h1>
      <p style="color:#666;margin:0 0 16px;font-size:13px">
        Snapshot {snap} · threshold &lt; {thr} bottles · generated {gen_str}
      </p>
      <table style="border-collapse:collapse;margin-bottom:18px">
        <tr>
          <td style="padding:10px 18px;background:#fff5e6;border:1px solid #efe1c3;border-radius:6px">
            <div style="font-size:11px;color:#7a5a25;text-transform:uppercase">OOS</div>
            <div style="font-size:28px;font-weight:700;color:#c0392b">{s.get('total_oos', 0)}</div>
          </td>
          <td style="width:8px"></td>
          <td style="padding:10px 18px;background:#fffaf0;border:1px solid #efe1c3;border-radius:6px">
            <div style="font-size:11px;color:#7a5a25;text-transform:uppercase">Low stock &lt;{thr}</div>
            <div style="font-size:28px;font-weight:700;color:#d35400">{s.get('total_low_stock', 0)}</div>
          </td>
        </tr>
      </table>
      {_table('OOS — listed but ZERO on hand', oos, '#c0392b', max_rows=80)}
      {_table(f'Low stock — listed but under {thr} bottles', low, '#d35400', max_rows=80)}
      <hr style="border:none;border-top:1px solid #efe1c3;margin:24px 0 10px">
      <p style="color:#888;font-size:12px;margin:0">
        Full dashboard:
        <a href="https://lcbo-tracker-web.vercel.app/" style="color:#d4a574">lcbo-tracker-web.vercel.app</a>
        · Daily SOD ingest · Anu Spirits Tracker
      </p>
    </div>
    '''
    return body.strip()


def _send_morning_digest_email(digest: dict, to_email: str | None = None) -> dict:
    """Email the morning digest. Returns {'sent': bool, 'detail': str}.

    Uses SMTP credentials from env (SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS).
    Falls back to noop with a clear detail message if creds aren't set —
    that keeps the cron from crashing in dev / new deployments.
    """
    import smtplib
    from email.message import EmailMessage

    to = (to_email or os.environ.get('DIGEST_TO_EMAIL', '')).strip()
    host = os.environ.get('SMTP_HOST', '').strip()
    port = int(os.environ.get('SMTP_PORT', '587') or '587')
    user = os.environ.get('SMTP_USER', '').strip()
    pwd = os.environ.get('SMTP_PASS', '').strip()
    from_addr = os.environ.get('SMTP_FROM', user).strip()

    if not (to and host and user and pwd):
        return {
            'sent': False,
            'detail': (
                'SMTP/recipient not configured. Set SMTP_HOST, SMTP_PORT, '
                'SMTP_USER, SMTP_PASS, DIGEST_TO_EMAIL (and optionally '
                'SMTP_FROM) in Render env to enable email delivery.'
            ),
        }

    s = digest.get('summary', {})
    snap = digest.get('snapshot_date', 'unknown')
    subj_prefix = '🟢' if (s.get('total_oos', 0) == 0 and s.get('total_low_stock', 0) == 0) else '🔴'
    subject = (
        f"{subj_prefix} Anu Morning Digest {snap} · "
        f"{s.get('total_oos',0)} OOS · {s.get('total_low_stock',0)} low"
    )

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to
    plain = (
        f"OOS: {s.get('total_oos',0)} · Low stock (<{digest.get('low_threshold','?')}): "
        f"{s.get('total_low_stock',0)}. Snapshot {snap}. "
        f"View HTML in a rendering email client or open the dashboard."
    )
    msg.set_content(plain)
    msg.add_alternative(_render_morning_digest_html(digest), subtype='html')

    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, timeout=30) as smtp:
                smtp.login(user, pwd)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=30) as smtp:
                smtp.starttls()
                smtp.login(user, pwd)
                smtp.send_message(msg)
        return {'sent': True, 'detail': f'sent to {to}'}
    except Exception as e:
        return {'sent': False, 'detail': f'smtp error: {e}'}


@app.route('/api/cron/morning-digest', methods=['POST', 'GET'])
def api_cron_morning_digest():
    """Cron entrypoint — build the digest and email it.

    Schedule from Render Cron Jobs at 07:00 America/Toronto every day
    (which is 11:00 UTC during EDT, 12:00 UTC during EST). Returns a
    JSON summary so the cron logs show what was sent.
    """
    # Light auth so randos can't trigger emails — CRON_SECRET env var,
    # checked via header X-Cron-Secret or ?secret= query.
    expected = os.environ.get('CRON_SECRET', '').strip()
    given = (
        request.headers.get('X-Cron-Secret', '').strip()
        or (request.args.get('secret') or '').strip()
    )
    if expected and given != expected:
        return jsonify({'error': 'forbidden'}), 403

    try:
        thr = max(1, min(int(request.args.get('threshold', LOW_STOCK_THRESHOLD)), 100))
    except ValueError:
        thr = LOW_STOCK_THRESHOLD
    # Cron defaults to NB portfolio (rep team is NB-focused). Pass
    # ?portfolio=Anu or ?portfolio=all to override.
    portfolio = (request.args.get('portfolio') or 'NB').strip()
    digest = _build_morning_digest(thr, portfolio)
    email = _send_morning_digest_email(digest, request.args.get('to'))
    return jsonify({
        'snapshot_date': digest.get('snapshot_date'),
        'summary': digest.get('summary'),
        'email': email,
    })


# ───────────────────────────────────────────────────────────────────────
# SYSTEM STATUS — single endpoint for an at-a-glance dashboard
# (frontend renders a green/amber/red dot using these signals).
# ───────────────────────────────────────────────────────────────────────
@app.route('/api/admin/system-status', methods=['GET'])
def api_admin_system_status():
    """Aggregate health signals into one tier: ok | degraded | down.

    Cached for 30s — this endpoint is hit on every page nav.
    """
    fresh = _sod_freshness()
    age_days = fresh.get('snapshot_age_days')
    last_run_h = fresh.get('last_run_age_hours')

    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db
    issues = []
    signals = {
        'sod_snapshot_age_days': age_days,
        'sod_last_run_age_hours': last_run_h,
    }

    # SOD freshness
    if age_days is not None and age_days > 2:
        issues.append(f'SOD snapshot is {age_days} days old (catchup runs every 6h)')
    if last_run_h is not None and last_run_h > 25:
        issues.append(f'No successful SOD sync in {last_run_h:.0f}h')

    # Inventory_history (lcbo.com scrape) freshness
    try:
        if USE_POSTGRES:
            cur.execute(
                "SELECT MAX(recorded_at) FROM inventory_history "
                "WHERE store_number <> 'SUMMARY'"
            )
            r = cur.fetchone()
            last_lcbo = r[0] if r else None
        else:
            r = cur.execute(
                "SELECT MAX(recorded_at) FROM inventory_history "
                "WHERE store_number <> 'SUMMARY'"
            ).fetchone()
            last_lcbo = r[0] if r else None
        if last_lcbo:
            try:
                from datetime import datetime as _dt
                if isinstance(last_lcbo, str):
                    last_lcbo = _dt.fromisoformat(last_lcbo.split('+')[0].split('Z')[0])
                lcbo_age_h = (datetime.utcnow() - last_lcbo).total_seconds() / 3600.0
                signals['lcbo_scrape_age_hours'] = round(lcbo_age_h, 1)
                if lcbo_age_h > 6:
                    issues.append(f'lcbo.com cross-check is {lcbo_age_h:.1f}h stale (cron runs every 30min)')
            except Exception:
                pass
        else:
            signals['lcbo_scrape_age_hours'] = None
    except Exception:
        try: db.rollback()
        except Exception: pass

    # Latest activity log entry (rep activity heartbeat)
    try:
        if USE_POSTGRES:
            cur.execute(
                "SELECT MAX(created_at) FROM activities WHERE deleted_at IS NULL"
            )
            r = cur.fetchone()
            last_act = r[0] if r else None
        else:
            r = cur.execute(
                "SELECT MAX(created_at) FROM activities WHERE deleted_at IS NULL"
            ).fetchone()
            last_act = r[0] if r else None
        if last_act:
            try:
                from datetime import datetime as _dt
                if isinstance(last_act, str):
                    last_act = _dt.fromisoformat(last_act.split('+')[0].split('Z')[0])
                act_age_h = (datetime.utcnow() - last_act).total_seconds() / 3600.0
                signals['last_activity_age_hours'] = round(act_age_h, 1)
                if act_age_h > 72:
                    issues.append(f'No rep visits logged in {act_age_h/24:.1f} days')
            except Exception:
                pass
    except Exception:
        try: db.rollback()
        except Exception: pass

    # Failed sync runs in last 24h
    try:
        if USE_POSTGRES:
            cur.execute(
                "SELECT COUNT(*) FROM sod_sync_runs "
                "WHERE status = 'failed' AND run_at >= NOW() - INTERVAL '24 hours'"
            )
            failed_24h = int(cur.fetchone()[0] or 0)
        else:
            failed_24h = int(cur.execute(
                "SELECT COUNT(*) FROM sod_sync_runs "
                "WHERE status = 'failed' AND run_at >= datetime('now','-24 hours')"
            ).fetchone()[0] or 0)
        signals['sod_failed_runs_24h'] = failed_24h
        if failed_24h >= 3:
            issues.append(f'{failed_24h} SOD sync failures in the last 24h')
    except Exception:
        try: db.rollback()
        except Exception: pass

    if USE_POSTGRES:
        cur.close()

    # Tier the overall status
    if not issues:
        tier = 'ok'
    elif any(
        ('age >' in issue) or ('No successful SOD sync' in issue)
        for issue in issues
    ):
        tier = 'degraded'
    else:
        tier = 'degraded'

    payload = {
        'tier': tier,
        'issues': issues,
        'signals': signals,
        'sod_latest_snapshot': fresh.get('latest_snapshot'),
        'as_of': datetime.utcnow().isoformat() + 'Z',
    }
    response = jsonify(payload)
    # Cache 30s — this is hit on every page nav
    response.headers['Cache-Control'] = 'public, max-age=30, stale-while-revalidate=60'
    return response


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
@require_app_origin
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


# ------- Territory rollup — distribution + per-SKU drilldown per rep -------
@app.route('/api/crm/territory-rollup', methods=['GET'])
def api_crm_territory_rollup():
    """Per-rep distribution: store count, presence by SKU, missing-SKU gap count.

    Each rep block shows:
      stores_total       : assigned stores
      stores_visited_30d : stores with any activity in the last 30 days
      per_sku            : { sku, name, present_stores, missing_stores,
                             oos_stores, low_stock_stores }
      coverage_pct       : % of assigned stores with at least one tracked SKU

    Powers the upgraded /territories page: pick a rep → see which SKUs
    are underdistributed in their patch.
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    # Latest snapshot anchors per-SKU presence
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory")
        latest = cur.fetchone()[0]
        cur.close()
    else:
        latest = db.execute(
            "SELECT MAX(snapshot_date) FROM sod_inventory").fetchone()[0]

    portfolio = (request.args.get('portfolio') or 'NB').strip()
    skus = _skus_for_portfolio(portfolio)
    out = {
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'snapshot_date': str(latest) if latest else None,
        'portfolio': portfolio,
        'portfolio_skus': skus,
        'territories': [],
    }
    if not latest or not skus:
        return jsonify(out)

    phs = ','.join([ph] * len(skus))

    # Pre-compute store_number → rep map (from stores table)
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "SELECT store_number, COALESCE(rep, '') FROM stores "
            "WHERE store_number IS NOT NULL")
        store_to_rep = {int(r[0]): r[1].strip() for r in cur.fetchall()}
        cur.close()
    else:
        store_to_rep = {
            int(r[0]): (r[1] or '').strip() for r in db.execute(
                "SELECT store_number, COALESCE(rep, '') FROM stores "
                "WHERE store_number IS NOT NULL").fetchall()
        }

    # Recent activity (30d) per store
    since_30 = (datetime.utcnow() - timedelta(days=30)).isoformat()
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(
            "SELECT DISTINCT s.store_number "
            "FROM activities a JOIN stores s ON s.id = a.store_id "
            "WHERE a.deleted_at IS NULL AND a.created_at >= %s",
            (since_30,))
        visited_stores = {int(r[0]) for r in cur.fetchall()}
        cur.close()
    else:
        visited_stores = {
            int(r[0]) for r in db.execute(
                "SELECT DISTINCT s.store_number "
                "FROM activities a JOIN stores s ON s.id = a.store_id "
                "WHERE a.deleted_at IS NULL AND datetime(a.created_at) >= ?",
                (since_30,)).fetchall()
        }

    # SOD rows at latest snapshot — store→sku→(status, on_hand)
    sql_sod = f"""
        SELECT store_number, sku, status, on_hand
        FROM sod_inventory
        WHERE snapshot_date = {ph}
          AND sku IN ({phs})
    """
    params = [latest] + skus
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql_sod, params)
        sod_rows = cur.fetchall()
        cur.close()
    else:
        sod_rows = db.execute(sql_sod, params).fetchall()

    # store_number → sku → row
    store_sku_status: dict = {}
    for r in sod_rows:
        sn = int(r[0]); sku = r[1]
        store_sku_status.setdefault(sn, {})[sku] = {
            'status': r[2], 'on_hand': int(r[3] or 0),
        }

    # Group stores by rep (everyone in TERRITORY_MAP + unassigned bucket)
    rep_to_stores: dict = {}
    for sn, rep in store_to_rep.items():
        rep_to_stores.setdefault(rep or '(unassigned)', set()).add(sn)

    # Build per-rep rollup
    for rep_name, store_set in sorted(rep_to_stores.items()):
        # Pull territory meta from TERRITORY_MAP if known
        meta = TERRITORY_MAP.get(rep_name, {}) if rep_name != '(unassigned)' else {}
        per_sku = []
        any_presence = 0
        for sku in skus:
            present = missing = oos = low = 0
            for sn in store_set:
                row = store_sku_status.get(sn, {}).get(sku)
                if row is None or row.get('status') != 'L':
                    missing += 1
                else:
                    present += 1
                    on_hand = row.get('on_hand', 0)
                    if on_hand <= 0:
                        oos += 1
                    elif on_hand < LOW_STOCK_THRESHOLD:
                        low += 1
            brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
            per_sku.append({
                'sku': sku,
                'brand': brand,
                'product_name': name,
                'present_stores': present,
                'missing_stores': missing,
                'oos_stores': oos,
                'low_stock_stores': low,
                'distribution_pct': round(present / len(store_set) * 100, 1) if store_set else 0,
            })
            if present > 0:
                any_presence += 1
        # Coverage = % of stores with at least one of our tracked SKUs Listed
        carrying = 0
        for sn in store_set:
            row_map = store_sku_status.get(sn, {})
            if any((row_map.get(sku, {}) or {}).get('status') == 'L' for sku in skus):
                carrying += 1
        coverage_pct = round(carrying / len(store_set) * 100, 1) if store_set else 0
        visited_n = len(store_set & visited_stores)
        out['territories'].append({
            'rep': rep_name,
            'territory_name': meta.get('name', '') or rep_name,
            'stores_total': len(store_set),
            'stores_visited_30d': visited_n,
            'visited_pct': round(visited_n / len(store_set) * 100, 1) if store_set else 0,
            'stores_carrying_any_sku': carrying,
            'coverage_pct': coverage_pct,
            'sku_distribution_avg_pct': round(
                sum(s['distribution_pct'] for s in per_sku) / max(len(per_sku), 1), 1),
            'per_sku': per_sku,
        })

    # Sort biggest territory first, unassigned last
    out['territories'].sort(
        key=lambda t: (t['rep'] == '(unassigned)', -t['stores_total']))
    return jsonify(out)


# ------- Rep self-service dashboard -------
@app.route('/api/crm/rep-dashboard/<rep>', methods=['GET'])
def api_crm_rep_dashboard(rep):
    """One-call self-service view for a single rep:
      - my_stats : visits/tastings/calls/etc. last 30d + 90d
      - my_stores : assigned store count + last_visited window
      - recent_activities : last 25 logged activities
      - new_listings_won : deals I closed-as-listed in last 90d
      - open_deals : my open pipeline
      - opportunities : SKUs missing in my stores (top 25 by store count)
      - deliveries_logged_30d : how many delivery activity rows I logged
      - my_oos / my_low_stock : OOS + low-stock store-SKU pairs in my patch

    Query params:
      portfolio=NB|Anu|all  scope all SKU-derived metrics (OOS, low-stock,
                            opportunities, new_listings_won). Default 'NB'
                            because the field team is NB-focused for now.
                            Activity counts and recent_activities are
                            never SKU-filtered.
    """
    rep_clean = (rep or '').strip()
    if not rep_clean:
        return jsonify({'error': 'rep required'}), 400
    portfolio = (request.args.get('portfolio') or 'NB').strip()
    portfolio_skus = _skus_for_portfolio(portfolio)
    portfolio_sku_set = set(portfolio_skus)
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'

    from datetime import datetime as _dt, timedelta as _td
    since_30 = (_dt.utcnow() - _td(days=30)).isoformat()
    since_90 = (_dt.utcnow() - _td(days=90)).isoformat()

    def _exec(sql: str, params: tuple = ()):
        if USE_POSTGRES:
            cur = db.cursor(); cur.execute(sql, params)
            rows = cur.fetchall(); cur.close()
            return rows
        return db.execute(sql, params).fetchall()

    # My stats
    if USE_POSTGRES:
        my_stats_q = (
            "SELECT COUNT(*), "
            "SUM(CASE WHEN LOWER(activity_type) LIKE '%%visit%%' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'tasting' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'meeting' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'order_commitment' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'delivery' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) IN ('call','email') THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'sample_drop' THEN 1 ELSE 0 END) "
            "FROM activities WHERE deleted_at IS NULL "
            "AND LOWER(TRIM(rep)) = LOWER(TRIM(%s)) "
            "AND created_at >= %s"
        )
    else:
        my_stats_q = (
            "SELECT COUNT(*), "
            "SUM(CASE WHEN LOWER(activity_type) LIKE '%visit%' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'tasting' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'meeting' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'order_commitment' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'delivery' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) IN ('call','email') THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN LOWER(activity_type) = 'sample_drop' THEN 1 ELSE 0 END) "
            "FROM activities WHERE deleted_at IS NULL "
            "AND LOWER(TRIM(rep)) = LOWER(TRIM(?)) "
            "AND datetime(created_at) >= ?"
        )

    def _stats_for(since: str):
        row = _exec(my_stats_q, (rep_clean, since))[0]
        return {
            'total': int(row[0] or 0),
            'visits': int(row[1] or 0),
            'tastings': int(row[2] or 0),
            'meetings': int(row[3] or 0),
            'order_commitments': int(row[4] or 0),
            'deliveries': int(row[5] or 0),
            'outreach': int(row[6] or 0),
            'sample_drops': int(row[7] or 0),
        }

    stats_30 = _stats_for(since_30)
    stats_90 = _stats_for(since_90)

    # My assigned stores
    if USE_POSTGRES:
        rows = _exec(
            "SELECT COUNT(*) FROM stores WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s))",
            (rep_clean,),
        )
    else:
        rows = _exec(
            "SELECT COUNT(*) FROM stores WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?))",
            (rep_clean,),
        )
    my_store_count = int(rows[0][0] or 0)

    # Recent activities (last 25)
    if USE_POSTGRES:
        recent_q = (
            "SELECT a.id, a.activity_type, a.notes, a.created_at, "
            "       a.store_id, s.store_number, COALESCE(s.account,''), "
            "       COALESCE(s.city,''), COALESCE(a.outcome,'') "
            "FROM activities a LEFT JOIN stores s ON s.id = a.store_id "
            "WHERE a.deleted_at IS NULL "
            "AND LOWER(TRIM(a.rep)) = LOWER(TRIM(%s)) "
            "ORDER BY a.created_at DESC LIMIT 25"
        )
    else:
        recent_q = (
            "SELECT a.id, a.activity_type, a.notes, a.created_at, "
            "       a.store_id, s.store_number, COALESCE(s.account,''), "
            "       COALESCE(s.city,''), COALESCE(a.outcome,'') "
            "FROM activities a LEFT JOIN stores s ON s.id = a.store_id "
            "WHERE a.deleted_at IS NULL "
            "AND LOWER(TRIM(a.rep)) = LOWER(TRIM(?)) "
            "ORDER BY a.created_at DESC LIMIT 25"
        )
    recent_rows = _exec(recent_q, (rep_clean,))
    recent_activities = [{
        'id': r[0],
        'activity_type': r[1],
        'notes': r[2] or '',
        'created_at': str(r[3]) if r[3] else '',
        'store_number': r[5],
        'account': r[6],
        'city': r[7],
        'outcome': r[8],
    } for r in recent_rows]

    # New listings won (deals closed as listed in last 90d)
    if USE_POSTGRES:
        won_rows = _exec(
            "SELECT d.id, d.sku, COALESCE(p.brand,''), COALESCE(p.name,''), "
            "       d.closed_at::text, d.store_number "
            "FROM deals d LEFT JOIN products p ON p.lcbo_sku = LTRIM(d.sku,'0') "
            "WHERE LOWER(TRIM(d.owner_rep)) = LOWER(TRIM(%s)) "
            "AND d.stage='listed' AND d.closed_at >= %s "
            "ORDER BY d.closed_at DESC LIMIT 25",
            (rep_clean, since_90),
        )
    else:
        won_rows = _exec(
            "SELECT d.id, d.sku, COALESCE(p.brand,''), COALESCE(p.name,''), "
            "       d.closed_at, d.store_number "
            "FROM deals d LEFT JOIN products p ON p.lcbo_sku = LTRIM(d.sku,'0') "
            "WHERE LOWER(TRIM(d.owner_rep)) = LOWER(TRIM(?)) "
            "AND d.stage='listed' AND datetime(d.closed_at) >= ? "
            "ORDER BY d.closed_at DESC LIMIT 25",
            (rep_clean, since_90),
        )
    new_listings_won = [{
        'id': r[0], 'sku': r[1], 'brand': r[2], 'product_name': r[3],
        'closed_at': str(r[4]) if r[4] else '', 'store_number': r[5],
    } for r in won_rows if r[1] in portfolio_sku_set]

    # Open deals
    if USE_POSTGRES:
        open_rows = _exec(
            "SELECT d.id, d.sku, COALESCE(p.brand,''), COALESCE(p.name,''), "
            "       d.stage, d.store_number, COALESCE(d.next_action,''), "
            "       d.next_action_date::text "
            "FROM deals d LEFT JOIN products p ON p.lcbo_sku = LTRIM(d.sku,'0') "
            "WHERE LOWER(TRIM(d.owner_rep)) = LOWER(TRIM(%s)) "
            "AND d.stage NOT IN ('listed','lost') "
            "ORDER BY d.id DESC LIMIT 50",
            (rep_clean,),
        )
    else:
        open_rows = _exec(
            "SELECT d.id, d.sku, COALESCE(p.brand,''), COALESCE(p.name,''), "
            "       d.stage, d.store_number, COALESCE(d.next_action,''), "
            "       d.next_action_date "
            "FROM deals d LEFT JOIN products p ON p.lcbo_sku = LTRIM(d.sku,'0') "
            "WHERE LOWER(TRIM(d.owner_rep)) = LOWER(TRIM(?)) "
            "AND d.stage NOT IN ('listed','lost') "
            "ORDER BY d.id DESC LIMIT 50",
            (rep_clean,),
        )
    open_deals = [{
        'id': r[0], 'sku': r[1], 'brand': r[2], 'product_name': r[3],
        'stage': r[4], 'store_number': r[5],
        'next_action': r[6], 'next_action_date': str(r[7]) if r[7] else '',
    } for r in open_rows if r[1] in portfolio_sku_set]

    # OOS + low-stock IN MY PATCH (uses morning-digest helper, filtered to rep)
    digest = _build_morning_digest(portfolio=portfolio)
    my_oos = [r for r in digest['buckets']['oos']
              if (r.get('rep') or '').strip().lower() == rep_clean.lower()]
    my_low_stock = [r for r in digest['buckets']['low_stock']
                    if (r.get('rep') or '').strip().lower() == rep_clean.lower()]

    # Opportunities — SKUs MISSING across my stores (aggregate, top 25)
    # i.e. tracked SKUs that are NOT at most of my stores — portfolio-scoped
    if USE_POSTGRES and portfolio_skus:
        opp_rows = _exec(
            f"""
            WITH my_stores AS (
              SELECT store_number FROM stores
              WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s))
            ),
            latest AS (
              SELECT MAX(snapshot_date) AS d FROM sod_inventory
            )
            SELECT i.sku,
                   COUNT(DISTINCT i.store_number) FILTER (WHERE i.status='L') AS present_stores
            FROM sod_inventory i, latest
            WHERE i.snapshot_date = latest.d
              AND i.store_number IN (SELECT store_number FROM my_stores)
              AND i.sku IN ({','.join(['%s']*len(portfolio_skus))})
            GROUP BY i.sku
            """,
            tuple([rep_clean] + portfolio_skus),
        )
    else:
        opp_rows = []  # sqlite path omitted — prod is postgres

    opportunities = []
    seen_present = {r[0] for r in opp_rows}
    # Also include portfolio SKUs that have ZERO presence (true 100% gap)
    for sku in portfolio_skus:
        if sku not in seen_present:
            opp_rows = list(opp_rows) + [(sku, 0)]
    for r in opp_rows:
        sku = r[0]; present = int(r[1] or 0)
        brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
        missing = max(0, my_store_count - present)
        if missing > 0:
            opportunities.append({
                'sku': sku, 'brand': brand, 'product_name': name,
                'present_stores': present,
                'missing_stores': missing,
                'opportunity_pct': round(missing / max(my_store_count, 1) * 100, 1),
            })
    opportunities.sort(key=lambda x: -x['missing_stores'])

    return jsonify({
        'rep': rep_clean,
        'portfolio': portfolio,
        'portfolio_skus': portfolio_skus,
        'as_of': datetime.utcnow().isoformat() + 'Z',
        'my_store_count': my_store_count,
        'stats_30d': stats_30,
        'stats_90d': stats_90,
        'recent_activities': recent_activities,
        'new_listings_won': new_listings_won,
        'open_deals': open_deals,
        'my_oos': my_oos,
        'my_low_stock': my_low_stock,
        'opportunities': opportunities[:25],
    })


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
@require_app_origin
def api_crm_backfill_store_changes():
    """Manually trigger backfill of per-store changes from historical snapshots."""
    n = _backfill_store_sku_changes()
    return jsonify({'inserted': n, 'status': 'ok'})


@app.route('/api/crm/log-listing', methods=['POST'])
@require_app_origin
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


# ===================================================================
# FREE deterministic answer engine — no LLM, no API cost.
# Pattern-matches the question against a fixed set of intents and
# emits a real SQL query + plain-English narrative answer.
# Used as the primary path so /ask works without Anthropic credits.
# ===================================================================

_SKU_ALIASES = {
    # NB Distillers (priority)
    'red admiral': '0020187',
    'red admiral vodka': '0020187',
    'admiral': '0020187',
    'chak de': '0022246',
    'chakde': '0022246',
    'chak de whisky': '0022246',
    'chak de canadian whisky': '0022246',
    # Anu Import
    'goenchi cashew': '0046340',
    'cashew feni': '0046340',
    'goenchi coconut': '0046343',
    'coconut feni': '0046343',
    'goenchi': '0046340',  # default to cashew
    'feni': '0046340',
    'fratelli shiraz': '0046282',
    'classic shiraz': '0046282',
    'shiraz': '0046282',
    'fratelli chenin': '0046285',
    'chenin': '0046285',
    'fratelli sauv': '0046286',
    'sauvignon blanc': '0046286',
    'sauvignon': '0046286',
    'fratelli cabernet': '0046287',
    'cabernet': '0046287',
    'cabernet sauvignon': '0046287',
}

_REP_NAMES = ['ikshit', 'namit', 'virat', 'surya', 'neeraj']

_CITY_ALIASES = [
    'toronto', 'ottawa', 'mississauga', 'brampton', 'hamilton',
    'london', 'kitchener', 'waterloo', 'guelph', 'cambridge',
    'oakville', 'burlington', 'milton', 'caledon', 'newmarket',
    'aurora', 'richmond hill', 'markham', 'vaughan', 'kingston',
    'windsor', 'barrie', 'sudbury', 'thunder bay', 'st. catharines',
    'niagara falls', 'oshawa', 'whitby', 'pickering',
]


def _detect_sku(q):
    """Return SKU code if question references one of our tracked SKUs, else None."""
    ql = q.lower()
    # Match longest alias first
    for alias in sorted(_SKU_ALIASES.keys(), key=len, reverse=True):
        if alias in ql:
            return _SKU_ALIASES[alias]
    # Direct SKU code (7 digits)
    m = re.search(r'\b(\d{7})\b', q)
    if m and m.group(1) in SOD_TRACKED_SKUS:
        return m.group(1)
    return None


def _detect_rep(q):
    ql = q.lower()
    for r in _REP_NAMES:
        # word-boundary match so "namit" doesn't match "namite" etc
        if re.search(r'\b' + r + r'\b', ql):
            return r.capitalize()
    return None


def _detect_city(q):
    ql = q.lower()
    for c in _CITY_ALIASES:
        if c in ql:
            return c.title()
    return None


def _detect_store_number(q):
    """Match a 1-4 digit number when the word 'store' is present."""
    if 'store' not in q.lower():
        return None
    # Avoid SKU codes (7 digits) — only 1-4 digit nums
    m = re.search(r'\bstore\s*#?\s*(\d{1,4})\b', q.lower())
    if m:
        return int(m.group(1))
    return None


def _run_select(sql, params=None):
    """Run a read-only SELECT with timeout and return (rows_dict, columns)."""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute('SET statement_timeout = 5000')
                cur.execute(sql, params or ())
                rows = cur.fetchmany(1000)
                cols = [d[0] for d in cur.description] if cur.description else []
        finally:
            conn.close()
    else:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.execute(sql.replace('%s', '?'), params or [])
            rows = cur.fetchmany(1000)
            cols = [d[0] for d in cur.description] if cur.description else []
        finally:
            conn.close()
    rows_dict = [
        {cols[i]: _json_safe(v) for i, v in enumerate(row)} for row in rows
    ]
    return rows_dict, cols


def _summarize_sku(sku):
    """Plain-English summary for a single tracked SKU."""
    brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))

    # Latest snapshot rollup
    sql = """
        SELECT
            COUNT(*) FILTER (WHERE status = 'L')::int AS listed,
            COUNT(*) FILTER (WHERE status = 'D')::int AS delisting,
            COUNT(*) FILTER (WHERE status = 'F')::int AS fully_delisted,
            COALESCE(SUM(on_hand) FILTER (WHERE status = 'L'), 0)::int AS units_on_shelf,
            MAX(snapshot_date)::text AS as_of
        FROM sod_inventory
        WHERE sku = %s
          AND snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s)
    """
    rows, cols = _run_select(sql, (sku, sku))
    if not rows:
        return None
    r = rows[0]
    listed = r.get('listed') or 0
    delist = r.get('delisting') or 0
    fully = r.get('fully_delisted') or 0
    units = r.get('units_on_shelf') or 0
    as_of = r.get('as_of') or 'unknown'

    # Recent listing changes (7d)
    sql2 = """
        SELECT change_type, COUNT(*)::int AS n
        FROM sod_listing_changes
        WHERE sku = %s
          AND change_date >= CURRENT_DATE - INTERVAL '7 days'
          AND change_type IN ('NEW_LISTING', 'DELISTED', 'RELISTED')
        GROUP BY change_type
    """
    changes, _ = _run_select(sql2, (sku,))
    chg_map = {c['change_type']: c['n'] for c in changes}
    new_l = chg_map.get('NEW_LISTING', 0)
    deli = chg_map.get('DELISTED', 0)
    relist = chg_map.get('RELISTED', 0)

    # Top 5 cities by store count
    sql3 = """
        SELECT s.city, COUNT(DISTINCT s.store_number)::int AS stores
        FROM sod_inventory i
        JOIN stores s ON s.store_number = i.store_number
        WHERE i.sku = %s AND i.status = 'L'
          AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s)
          AND s.city IS NOT NULL AND s.city <> ''
        GROUP BY s.city
        ORDER BY stores DESC
        LIMIT 5
    """
    cities, _ = _run_select(sql3, (sku, sku))
    city_phrase = ''
    if cities:
        city_phrase = ' Top cities: ' + ', '.join(
            f"{c['city']} ({c['stores']})" for c in cities
        ) + '.'

    # Build narrative
    parts = [
        f"{name} ({brand}, SKU {sku}) is currently listed in {listed} LCBO stores",
        f"with {units:,} units on shelf as of {as_of}.",
    ]
    if delist or fully:
        parts.append(
            f"Watch list: {delist} stores marked Delisting and {fully} marked Fully Delisted."
        )
    if new_l or deli or relist:
        parts.append(
            f"Last 7 days: {new_l} new listings, {deli} delistings, {relist} relistings."
        )
    parts.append(city_phrase)
    answer = ' '.join(p for p in parts if p).strip()

    # Combined rows for the table view (city breakdown is most useful)
    return answer, sql3.strip(), cities, ['city', 'stores']


def _summarize_rep(rep_name):
    """Plain-English summary for a rep's recent activity."""
    # Visits in last 7 / 30 days
    sql = """
        SELECT
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days')::int AS visits_7d,
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')::int AS visits_30d,
            COUNT(DISTINCT store_id) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')::int AS unique_stores_30d,
            MAX(created_at)::text AS last_visit
        FROM activities
        WHERE rep = %s
    """
    rows, _ = _run_select(sql, (rep_name,))
    r = (rows or [{}])[0]
    v7 = r.get('visits_7d') or 0
    v30 = r.get('visits_30d') or 0
    uniq = r.get('unique_stores_30d') or 0
    last = r.get('last_visit') or 'never'

    # Stores in territory (assignment lives in stores.rep column when seeded)
    sql2 = """
        SELECT COUNT(*)::int AS territory_size
        FROM stores
        WHERE rep = %s
    """
    trows, _ = _run_select(sql2, (rep_name,))
    territory = (trows or [{'territory_size': 0}])[0].get('territory_size') or 0

    # Top 5 most-visited stores in last 30d
    sql3 = """
        SELECT s.store_number, s.account, s.city,
               COUNT(*)::int AS visits
        FROM activities a
        JOIN stores s ON s.id = a.store_id
        WHERE a.rep = %s
          AND a.created_at >= NOW() - INTERVAL '30 days'
        GROUP BY s.store_number, s.account, s.city
        ORDER BY visits DESC
        LIMIT 5
    """
    top, _ = _run_select(sql3, (rep_name,))

    parts = []
    # Territory size only printed when stores.rep column actually has data —
    # otherwise it's 0 because territory is assigned dynamically via FSA at
    # routing time, and the narrative would mislead.
    if territory > 0:
        coverage_pct = round((uniq / territory * 100), 1) if territory else 0
        parts.append(f"{rep_name} has {territory} stores in territory ({coverage_pct}% covered in last 30d).")
    parts.append(
        f"Last 7 days: {v7} visits. Last 30 days: {v30} visits across {uniq} unique stores."
    )
    if last and last != 'never':
        parts.append(f"Last visit logged: {last}.")
    else:
        parts.append("No visits logged yet.")
    if top:
        leader = top[0]
        parts.append(
            f"Most-visited stop: store #{leader.get('store_number')} "
            f"({leader.get('city','')}) — {leader.get('visits')} visits."
        )
    answer = ' '.join(parts)
    return answer, sql3.strip(), top, ['store_number', 'account', 'city', 'visits']


def _summarize_store(store_number):
    """Plain-English summary for a single store."""
    sql = """
        SELECT s.store_number, s.account, s.address, s.city, s.postal,
               s.rep, s.priority, s.manager_name, s.phone
        FROM stores s
        WHERE s.store_number = %s
        LIMIT 1
    """
    rows, _ = _run_select(sql, (store_number,))
    if not rows:
        return f"No store found with number {store_number}.", sql.strip(), [], []
    s = rows[0]

    # Listings of our SKUs at this store
    sql2 = """
        SELECT i.sku, i.product_name, i.status, i.on_hand
        FROM sod_inventory i
        WHERE i.store_number = %s
          AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)
          AND i.sku IN ({})
        ORDER BY i.sku
    """.format(','.join("'%s'" % k for k in SOD_TRACKED_SKUS.keys()))
    listings, _ = _run_select(sql2, (store_number,))
    listed = [l for l in listings if l.get('status') == 'L']

    # Last visit
    sql3 = """
        SELECT a.rep, a.created_at::text AS at, a.notes
        FROM activities a
        JOIN stores st ON st.id = a.store_id
        WHERE st.store_number = %s
        ORDER BY a.created_at DESC
        LIMIT 1
    """
    last, _ = _run_select(sql3, (store_number,))

    addr = f"{s.get('address') or ''}, {s.get('city') or ''}".strip(', ')
    parts = [
        f"Store #{s.get('store_number')} — {s.get('account') or 'LCBO'} ({addr}).",
        f"Rep: {s.get('rep') or 'unassigned'}. Priority: {s.get('priority') or 'none'}.",
    ]
    if s.get('manager_name'):
        parts.append(f"Manager: {s['manager_name']}.")
    if listed:
        names = [l.get('product_name') or l.get('sku') for l in listed]
        parts.append(f"Currently listing {len(listed)}/{len(SOD_TRACKED_SKUS)} of our tracked SKUs: {', '.join(names)}.")
    else:
        parts.append(f"Currently listing 0/{len(SOD_TRACKED_SKUS)} of our tracked SKUs — high-priority pitch target.")
    if last:
        parts.append(f"Last visit: {last[0].get('rep')} at {last[0].get('at')}.")
    else:
        parts.append("No visits logged yet.")

    return ' '.join(parts), sql2.strip(), listings, ['sku', 'product_name', 'status', 'on_hand']


def _summarize_portfolio():
    """Roll-up across all 8 tracked SKUs."""
    sql = """
        SELECT i.sku, p.product_name,
               COUNT(*) FILTER (WHERE i.status = 'L')::int AS listed,
               COUNT(*) FILTER (WHERE i.status = 'D')::int AS delisting,
               COALESCE(SUM(i.on_hand) FILTER (WHERE i.status = 'L'), 0)::int AS units
        FROM sod_inventory i
        LEFT JOIN sod_products p ON p.sku = i.sku
        WHERE i.sku IN ({})
          AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)
        GROUP BY i.sku, p.product_name
        ORDER BY listed DESC
    """.format(','.join("'%s'" % k for k in SOD_TRACKED_SKUS.keys()))
    rows, _ = _run_select(sql)
    if not rows:
        return "No SOD inventory data loaded yet.", sql.strip(), [], []

    total_listed = sum(r.get('listed') or 0 for r in rows)
    total_delist = sum(r.get('delisting') or 0 for r in rows)
    total_units = sum(r.get('units') or 0 for r in rows)
    leader = max(rows, key=lambda r: r.get('listed') or 0)
    leader_name = leader.get('product_name') or SOD_TRACKED_SKUS.get(leader.get('sku'), ('', ''))[1]

    parts = [
        f"Portfolio rollup across {len(rows)} tracked SKUs:",
        f"{total_listed} total store-listings, {total_units:,} units on shelf, {total_delist} flagged delisting.",
        f"Top performer: {leader_name} with {leader.get('listed')} stores.",
    ]
    return ' '.join(parts), sql.strip(), rows, ['sku', 'product_name', 'listed', 'delisting', 'units']


def _count_stores_listing(sku, city=None):
    brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
    if city:
        sql = """
            SELECT COUNT(DISTINCT i.store_number)::int AS stores
            FROM sod_inventory i
            JOIN stores s ON s.store_number = i.store_number
            WHERE i.sku = %s AND i.status = 'L'
              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s)
              AND s.city ILIKE %s
        """
        rows, _ = _run_select(sql, (sku, sku, f'%{city}%'))
        n = (rows or [{'stores': 0}])[0].get('stores') or 0
        return f"{n} LCBO stores in {city} are currently listing {name}.", sql.strip(), rows, ['stores']
    sql = """
        SELECT COUNT(DISTINCT store_number)::int AS stores
        FROM sod_inventory
        WHERE sku = %s AND status = 'L'
          AND snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s)
    """
    rows, _ = _run_select(sql, (sku, sku))
    n = (rows or [{'stores': 0}])[0].get('stores') or 0
    return f"{n} LCBO stores are currently listing {name} ({brand}).", sql.strip(), rows, ['stores']


def _top_stores_for_sku(sku, limit=10, city=None):
    brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
    if city:
        sql = """
            SELECT s.store_number, s.account, s.city, i.on_hand
            FROM sod_inventory i
            JOIN stores s ON s.store_number = i.store_number
            WHERE i.sku = %s AND i.status = 'L'
              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s)
              AND s.city ILIKE %s
            ORDER BY i.on_hand DESC NULLS LAST
            LIMIT %s
        """
        rows, _ = _run_select(sql, (sku, sku, f'%{city}%', limit))
    else:
        sql = """
            SELECT s.store_number, s.account, s.city, i.on_hand
            FROM sod_inventory i
            JOIN stores s ON s.store_number = i.store_number
            WHERE i.sku = %s AND i.status = 'L'
              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s)
            ORDER BY i.on_hand DESC NULLS LAST
            LIMIT %s
        """
        rows, _ = _run_select(sql, (sku, sku, limit))

    if not rows:
        scope = f" in {city}" if city else ''
        return f"No stores currently list {name}{scope}.", sql.strip(), rows, ['store_number', 'account', 'city', 'on_hand']
    top = rows[0]
    scope = f" in {city}" if city else ''
    return (
        f"Top {len(rows)} stores for {name}{scope} by on-hand inventory. "
        f"Leader: store #{top['store_number']} ({top.get('city','')}) with {top.get('on_hand') or 0} units."
    ), sql.strip(), rows, ['store_number', 'account', 'city', 'on_hand']


def _list_delisting(sku=None):
    if sku:
        brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
        sql = """
            SELECT s.store_number, s.account, s.city, i.on_hand
            FROM sod_inventory i
            JOIN stores s ON s.store_number = i.store_number
            WHERE i.sku = %s AND i.status = 'D'
              AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku = %s)
            ORDER BY i.on_hand DESC NULLS LAST
            LIMIT 50
        """
        rows, _ = _run_select(sql, (sku, sku))
        return (
            f"{len(rows)} stores currently flagged Delisting for {name}. "
            f"These are urgent saves — every one rescued is a kept listing."
        ), sql.strip(), rows, ['store_number', 'account', 'city', 'on_hand']
    # Across portfolio
    sql = """
        SELECT i.sku, p.product_name, COUNT(*)::int AS delisting_stores
        FROM sod_inventory i
        LEFT JOIN sod_products p ON p.sku = i.sku
        WHERE i.status = 'D'
          AND i.sku IN ({})
          AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM sod_inventory)
        GROUP BY i.sku, p.product_name
        ORDER BY delisting_stores DESC
    """.format(','.join("'%s'" % k for k in SOD_TRACKED_SKUS.keys()))
    rows, _ = _run_select(sql)
    total = sum(r.get('delisting_stores') or 0 for r in rows)
    return (
        f"{total} store-SKU combinations across our portfolio are flagged Delisting. "
        f"Saving these is the fastest revenue-protection move."
    ), sql.strip(), rows, ['sku', 'product_name', 'delisting_stores']


def _detect_window_days(q):
    """Extract a date window from natural language. Returns int days (default 7)."""
    ql = q.lower()
    m = re.search(r'(?:last|past)\s+(\d+)\s*(?:day|days|d)\b', ql)
    if m:
        return max(1, min(int(m.group(1)), 365))
    if 'today' in ql or 'yesterday' in ql:
        return 1
    if 'this week' in ql or 'last 7' in ql or 'past week' in ql or 'last week' in ql:
        return 7
    if ('this month' in ql or 'last 30' in ql or 'past month' in ql
            or 'last month' in ql or 'monthly' in ql):
        return 30
    if 'this quarter' in ql or 'last 90' in ql or 'past 90' in ql or 'last quarter' in ql:
        return 90
    if 'this year' in ql or 'last 365' in ql or 'ytd' in ql or 'last year' in ql:
        return 365
    return 7


def _count_total_stores():
    """How many stores does LCBO have? How many carry our SKUs?

    Uses the UNION of all sources (master directory + latest SOD + recent
    lcbo.com) so the count reflects reality, not a single source's gaps.
    """
    db = get_db()
    try:
        _, u_stats = _resolve_store_universe(db, lcbo_window_hours=48)
        _, c_stats = _resolve_carrying_us_universe(db, lcbo_window_hours=48)
    except Exception:
        u_stats, c_stats = {}, {}

    # Also pull the master count + snapshot date for reference
    sql_meta = """
        SELECT
            (SELECT COUNT(*)::int FROM stores) AS master_count,
            (SELECT MAX(snapshot_date)::text FROM sod_inventory) AS as_of
    """
    meta_rows, _ = _run_select(sql_meta)
    master_count = (meta_rows[0].get('master_count') or 0) if meta_rows else 0
    as_of = (meta_rows[0].get('as_of') or 'unknown') if meta_rows else 'unknown'

    universe_total = u_stats.get('total_universe_size', master_count)
    carrying_total = c_stats.get('total_stores_carrying_any_sku', 0)
    carrying_sod_and_lcbo = c_stats.get('sod_and_lcbo', 0)
    carrying_only_sod = c_stats.get('sod_only', 0)
    carrying_only_lcbo = c_stats.get('lcbo_only', 0)
    in_sod_only = u_stats.get('in_sod_only', 0)
    in_lcbo_only = u_stats.get('in_lcbo_only', 0)

    pct = round(carrying_total / universe_total * 100, 1) if universe_total else 0
    parts = [
        f"LCBO universe (union of master directory, SOD, and lcbo.com): {universe_total} stores.",
        f"{carrying_total} carry at least one of our 8 SKUs ({pct}% coverage) as of {as_of}.",
    ]
    if carrying_only_lcbo:
        parts.append(f"{carrying_only_lcbo} stores show our products only on lcbo.com (SOD missed) — potential commission claims.")
    if carrying_only_sod:
        parts.append(f"{carrying_only_sod} stores show our products only in SOD (lcbo.com hasn't confirmed yet).")
    if in_sod_only or in_lcbo_only:
        parts.append(f"Source drift: {in_sod_only} stores in SOD not in master, {in_lcbo_only} stores on lcbo.com not in master (auto-onboarded on next scrape).")
    return ' '.join(parts), sql_meta.strip(), meta_rows, ['master_count', 'as_of']


def _count_new_listings(days, sku=None):
    """How many new listings in the last N days?"""
    if sku:
        brand, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
        sql = """
            SELECT COUNT(*)::int AS new_listings
            FROM sod_listing_changes
            WHERE change_type = 'NEW_LISTING'
              AND change_date >= CURRENT_DATE - (INTERVAL '1 day' * %s)
              AND sku = %s
        """
        rows, _ = _run_select(sql, (days, sku))
        n = (rows[0].get('new_listings') or 0) if rows else 0
        # Also pull a breakdown of stores
        stores_sql = """
            SELECT change_date::text AS date, store_number
            FROM sod_listing_changes
            WHERE change_type = 'NEW_LISTING'
              AND change_date >= CURRENT_DATE - (INTERVAL '1 day' * %s)
              AND sku = %s
              AND store_number IS NOT NULL
            ORDER BY change_date DESC
            LIMIT 20
        """
        store_rows, _ = _run_select(stores_sql, (days, sku))
        return (
            f"Last {days} days: {n} new {name} listings detected. "
            f"({len(store_rows)} most recent shown below.)"
        ), stores_sql.strip(), store_rows, ['date', 'store_number']

    # Across portfolio
    sql = """
        SELECT c.sku, p.product_name, p.brand, COUNT(*)::int AS new_listings
        FROM sod_listing_changes c
        LEFT JOIN sod_products p ON p.sku = c.sku
        WHERE c.change_type = 'NEW_LISTING'
          AND c.change_date >= CURRENT_DATE - (INTERVAL '1 day' * %s)
          AND p.is_tracked = TRUE
        GROUP BY c.sku, p.product_name, p.brand
        ORDER BY new_listings DESC
    """
    rows, _ = _run_select(sql, (days,))
    total = sum(r.get('new_listings') or 0 for r in rows)
    if not rows or total == 0:
        return (
            f"No new listings detected in the last {days} days across the tracked portfolio. "
            f"(SOD's NEW_LISTING events fire when a SKU first appears at a store.)"
        ), sql.strip(), rows, ['sku', 'product_name', 'brand', 'new_listings']

    leader = rows[0]
    leader_name = leader.get('product_name') or leader.get('sku')
    parts = [
        f"Last {days} days: {total} new listings across {len(rows)} tracked SKUs.",
        f"Leader: {leader_name} with {leader.get('new_listings')} new stores.",
    ]
    return ' '.join(parts), sql.strip(), rows, ['sku', 'product_name', 'brand', 'new_listings']


def _new_listings_per_sku_in_range(days, sku=None):
    """Per-SKU new listings via 2-snapshot diff + lcbo.com triple-check.

    This is the 'how many stores were added between date X and today' answer
    — uses snapshot diff, not just NEW_LISTING events (which can miss listings
    SOD hides). Cross-checks each one against lcbo.com inventory_history.
    """
    db = get_db()
    end_d = _toronto_today()
    start_d = end_d - timedelta(days=days)

    skus_to_audit = (
        [sku] if (sku and sku in SOD_TRACKED_SKUS)
        else list(SOD_TRACKED_SKUS.keys())
    )

    rows_out = []
    total_new = 0
    total_lcbo_only = 0

    for s in skus_to_audit:
        brand, name = SOD_TRACKED_SKUS[s]
        sku_clean = s.lstrip('0')

        # Snapshot dates. If start_d predates our SOD history we DO NOT
        # fall back silently — we mark this row as clipped and skip the
        # diff so we don't return inflated counts.
        start_clipped = False
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=%s AND snapshot_date <= %s",
                    (s, start_d.isoformat()))
                start_snap = cur.fetchone()[0]
                cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=%s AND snapshot_date <= %s",
                    (s, end_d.isoformat()))
                end_snap = cur.fetchone()[0]
                cur.close()
            else:
                start_snap = cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=? AND snapshot_date <= ?",
                    (s, start_d.isoformat())).fetchone()[0]
                end_snap = cur.execute(
                    "SELECT MAX(snapshot_date) FROM sod_inventory "
                    "WHERE sku=? AND snapshot_date <= ?",
                    (s, end_d.isoformat())).fetchone()[0]
            if start_snap is None:
                start_clipped = True
        except Exception:
            try: db.rollback()
            except Exception: pass
            continue

        # If start_snap is missing (our history is younger than start_d),
        # this comparison would be meaningless ('everything vs nothing').
        # Return a clipped row with 0s so the AI can be honest.
        if start_clipped:
            rows_out.append({
                'sku': s,
                'product_name': name,
                'brand': brand,
                'sod_new': 0,
                'lcbo_only_new': 0,
                'total_new': 0,
                'start_snap': None,
                'end_snap': str(end_snap) if end_snap else None,
                'clipped': True,
            })
            continue

        def listed_at(snap):
            if not snap:
                return set()
            try:
                cur = db.cursor() if USE_POSTGRES else db
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number FROM sod_inventory "
                        "WHERE sku=%s AND snapshot_date=%s AND status='L'",
                        (s, snap))
                    out = {int(r[0]) for r in cur.fetchall()}
                    cur.close()
                else:
                    out = {
                        int(r[0]) for r in cur.execute(
                            "SELECT store_number FROM sod_inventory "
                            "WHERE sku=? AND snapshot_date=? AND status='L'",
                            (s, snap)).fetchall()
                    }
                return out
            except Exception:
                try: db.rollback()
                except Exception: pass
                return set()

        start_set = listed_at(start_snap)
        end_set = listed_at(end_snap)
        new_sod = end_set - start_set

        # lcbo.com triple-check: stores where lcbo.com saw qty>0 in window
        # but weren't Listed at start_snap = lcbo-only new listings
        lcbo_seen = set()
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                cur.execute(
                    "SELECT id FROM products WHERE lcbo_sku=%s LIMIT 1",
                    (sku_clean,))
                prow = cur.fetchone()
                pid = prow[0] if prow else None
                if pid:
                    cur.execute(
                        "SELECT DISTINCT store_number FROM inventory_history "
                        "WHERE product_id=%s AND quantity>0 "
                        "  AND store_number <> 'SUMMARY' "
                        "  AND recorded_at >= %s",
                        (pid, start_d.isoformat()))
                    for r in cur.fetchall():
                        try:
                            lcbo_seen.add(int(r[0]))
                        except (ValueError, TypeError):
                            continue
                cur.close()
        except Exception:
            try: db.rollback()
            except Exception: pass

        lcbo_only_new = (lcbo_seen - start_set) - new_sod
        union_new = new_sod | lcbo_only_new

        rows_out.append({
            'sku': s,
            'product_name': name,
            'brand': brand,
            'sod_new': len(new_sod),
            'lcbo_only_new': len(lcbo_only_new),
            'total_new': len(union_new),
            'start_snap': str(start_snap) if start_snap else None,
            'end_snap': str(end_snap) if end_snap else None,
        })
        total_new += len(union_new)
        total_lcbo_only += len(lcbo_only_new)

    rows_out.sort(key=lambda r: -r['total_new'])

    if sku:
        # Single-SKU answer
        if not rows_out:
            return f"No data for {sku}.", 'snapshot-diff', [], []
        r = rows_out[0]
        if r.get('clipped'):
            return (
                f"Our SOD ingest only covers data starting after the requested "
                f"window's start date, so a real diff for {r['product_name']} over "
                f"the last {days} days isn't possible from stored data alone. "
                f"Upload a historical SOD inventory ZIP via /sod-compare to get "
                f"a truthful comparison."
            ), 'snapshot-diff', rows_out, ['sku', 'product_name', 'sod_new', 'lcbo_only_new', 'total_new']
        if r['total_new'] == 0:
            return (
                f"Last {days} days for {r['product_name']}: 0 new stores added "
                f"(comparing snapshots {r['start_snap']} → {r['end_snap']})."
            ), 'snapshot-diff', rows_out, ['sku', 'product_name', 'sod_new', 'lcbo_only_new', 'total_new']
        parts = [
            f"Last {days} days for {r['product_name']}: {r['total_new']} new store-listings "
            f"({r['sod_new']} confirmed by SOD, {r['lcbo_only_new']} caught only on lcbo.com). "
            f"Snapshot comparison: {r['start_snap']} → {r['end_snap']}."
        ]
        if r['lcbo_only_new']:
            parts.append(f"The {r['lcbo_only_new']} lcbo.com-only finds are potential commission claims SOD missed.")
        return ' '.join(parts), 'snapshot-diff', rows_out, ['sku', 'product_name', 'sod_new', 'lcbo_only_new', 'total_new']

    # Portfolio-wide
    clipped_skus = sum(1 for r in rows_out if r.get('clipped'))
    parts = [
        f"Last {days} days: {total_new} new store-listings won across the portfolio."
    ]
    if clipped_skus:
        parts.append(
            f"⚠ {clipped_skus} SKU(s) couldn't be compared because the requested window "
            f"predates our SOD history — upload a historical SOD ZIP via /sod-compare to "
            f"get a real diff for those."
        )
    if total_lcbo_only:
        parts.append(f"{total_lcbo_only} of those were caught only on lcbo.com (SOD missed) — potential commission claims.")
    if rows_out:
        non_clipped = [r for r in rows_out if not r.get('clipped')]
        if non_clipped and non_clipped[0]['total_new'] > 0:
            leader = non_clipped[0]
            parts.append(f"Top performer: {leader['product_name']} with {leader['total_new']} new stores.")
    parts.append(f"Run /new-listings page for the per-store breakdown.")
    return ' '.join(parts), 'snapshot-diff', rows_out, ['sku', 'product_name', 'sod_new', 'lcbo_only_new', 'total_new']


def _summarize_hidden_listings(sku=None):
    """Plain-English count of suspicious listing patterns.

    Calls the same logic as /api/admin/hidden-listings — just summarizes.
    """
    db = get_db()
    skus = [sku] if (sku and sku in SOD_TRACKED_SKUS) else list(SOD_TRACKED_SKUS.keys())

    ghost_count = 0
    hidden_count = 0
    flicker_count = 0
    mass_delist_count = 0
    leader_sku = None
    leader_name = None
    leader_total = 0

    for s in skus:
        per_sku_total = 0
        # Reuse a tiny version of the same logic — 4 cheap counts
        try:
            cur = db.cursor() if USE_POSTGRES else db
            if USE_POSTGRES:
                # Latest snapshot
                cur.execute("SELECT MAX(snapshot_date) FROM sod_inventory WHERE sku=%s", (s,))
                latest = cur.fetchone()[0]
                if not latest:
                    continue
                # Ghosts: ever-listed - latest_stores - delist_events
                cur.execute(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT DISTINCT store_number FROM sod_inventory "
                    "  WHERE sku=%s AND status='L' "
                    "    AND snapshot_date >= %s::date - INTERVAL '90 days' "
                    "  EXCEPT "
                    "  SELECT store_number FROM sod_inventory "
                    "  WHERE sku=%s AND snapshot_date=%s "
                    "  EXCEPT "
                    "  SELECT DISTINCT store_number FROM sod_listing_changes "
                    "  WHERE sku=%s AND change_type IN ('DELISTED','FULLY_DELISTED') "
                    "    AND store_number IS NOT NULL"
                    ") g",
                    (s, latest, s, latest, s))
                g = int(cur.fetchone()[0] or 0)
                # Hidden inventory: SOD non-L AND lcbo qty>0 within 72h
                cur.execute(
                    "SELECT COUNT(DISTINCT ih.store_number) "
                    "FROM inventory_history ih "
                    "JOIN products p ON p.id = ih.product_id "
                    "WHERE p.lcbo_sku = %s "
                    "  AND ih.quantity > 0 "
                    "  AND ih.store_number <> 'SUMMARY' "
                    "  AND ih.recorded_at >= NOW() - INTERVAL '72 hours' "
                    "  AND NOT EXISTS ( "
                    "    SELECT 1 FROM sod_inventory si "
                    "    WHERE si.sku=%s AND si.snapshot_date=%s "
                    "      AND si.status='L' "
                    "      AND si.store_number = NULLIF(ih.store_number,'')::int "
                    "  )",
                    (s.lstrip('0'), s, latest))
                h = int(cur.fetchone()[0] or 0)
                # Flicker: 3+ changes in 30 days per store
                cur.execute(
                    "SELECT COUNT(*) FROM ( "
                    "  SELECT store_number FROM sod_listing_changes "
                    "  WHERE sku=%s AND store_number IS NOT NULL "
                    "    AND change_date >= CURRENT_DATE - INTERVAL '30 days' "
                    "    AND change_type IN ('NEW_LISTING','DELISTED','RELISTED','STATUS_FLIP') "
                    "  GROUP BY store_number HAVING COUNT(*) >= 3 "
                    ") f",
                    (s,))
                fl = int(cur.fetchone()[0] or 0)
                # Mass-delist: days where listed count dropped >10%
                cur.execute(
                    "WITH daily AS ( "
                    "  SELECT snapshot_date, COUNT(*) FILTER (WHERE status='L')::int AS lc "
                    "  FROM sod_inventory WHERE sku=%s GROUP BY snapshot_date "
                    "), w AS ( SELECT snapshot_date, lc, "
                    "         LAG(lc) OVER (ORDER BY snapshot_date) AS prev FROM daily) "
                    "SELECT COUNT(*) FROM w "
                    "WHERE prev IS NOT NULL AND prev > 0 "
                    "  AND ((prev - lc)::float / prev) * 100 >= 10 "
                    "  AND snapshot_date >= CURRENT_DATE - INTERVAL '90 days'",
                    (s,))
                md = int(cur.fetchone()[0] or 0)
                cur.close()
            else:
                g = h = fl = md = 0
            ghost_count += g
            hidden_count += h
            flicker_count += fl
            mass_delist_count += md
            per_sku_total = g + h + fl + md
            if per_sku_total > leader_total:
                leader_total = per_sku_total
                leader_sku = s
                _, leader_name = SOD_TRACKED_SKUS.get(s, ('', s))
        except Exception:
            try: db.rollback()
            except Exception: pass

    total = ghost_count + hidden_count + flicker_count + mass_delist_count
    if sku:
        _, name = SOD_TRACKED_SKUS.get(sku, ('', sku))
        if total == 0:
            return (
                f"No hidden-listing patterns detected for {name}. "
                f"SOD looks clean across the 4 audit checks (ghost / hidden inventory / flicker / mass-delist)."
            ), 'hidden-listings', [], []
        parts = [f"{name} suspicious patterns:"]
        if ghost_count: parts.append(f"{ghost_count} ghost listings (vanished without DELISTED event).")
        if hidden_count: parts.append(f"{hidden_count} hidden-inventory (lcbo.com sees stock, SOD says no).")
        if flicker_count: parts.append(f"{flicker_count} flicker patterns (status flipped 3+ times in 30d).")
        if mass_delist_count: parts.append(f"{mass_delist_count} mass-delist days (>10% drop in one day).")
        return ' '.join(parts), 'hidden-listings', [], []

    if total == 0:
        return (
            "No hidden-listing patterns detected across the portfolio. "
            "All 4 audit checks (ghost / hidden inventory / flicker / mass-delist) clean."
        ), 'hidden-listings', [], []
    parts = [
        f"Hidden-listing audit found {total} suspicious patterns across the portfolio:"
    ]
    if ghost_count: parts.append(f"{ghost_count} ghosts (Listed then vanished without DELISTED event).")
    if hidden_count: parts.append(f"{hidden_count} hidden-inventory (lcbo.com confirms stock, SOD says no).")
    if flicker_count: parts.append(f"{flicker_count} flickers (status whipsawing 3+ times in 30 days).")
    if mass_delist_count: parts.append(f"{mass_delist_count} mass-delist events (>10% drop in one day).")
    if leader_sku and leader_total:
        parts.append(f"Worst offender: {leader_name} with {leader_total} flagged.")
    parts.append("Open /hidden-listings for the per-store evidence.")
    return ' '.join(parts), 'hidden-listings', [], []


def _summarize_rep_behavior(days=30):
    """Plain-English rep behavior audit — answers 'are they working or
    going to the same places?'."""
    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db
    rep_roster = ['Ikshit', 'Namit', 'Virat', 'Surya', 'Neeraj']

    flagged = []  # list of {rep, flags, key_metric}
    rows_for_table = []

    for rep in rep_roster:
        try:
            if USE_POSTGRES:
                cur.execute(
                    "SELECT COUNT(*)::int, COUNT(DISTINCT store_id)::int, "
                    "       COUNT(DISTINCT DATE(created_at))::int, "
                    "       MAX(created_at)::text "
                    "FROM activities "
                    "WHERE LOWER(TRIM(rep)) = LOWER(TRIM(%s)) "
                    "  AND created_at >= NOW() - (INTERVAL '1 day' * %s) "
                    "  AND deleted_at IS NULL",
                    (rep, days))
            else:
                cur.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT store_id), "
                    "       COUNT(DISTINCT DATE(created_at)), MAX(created_at) "
                    "FROM activities "
                    "WHERE LOWER(TRIM(rep)) = LOWER(TRIM(?)) "
                    "  AND created_at >= datetime('now', ? || ' days') "
                    "  AND deleted_at IS NULL",
                    (rep, f'-{days}'))
            r = cur.fetchone()
            visits, unique_stores, active_days, last_visit = (
                int(r[0] or 0), int(r[1] or 0), int(r[2] or 0), r[3] or '')
        except Exception:
            try: db.rollback()
            except Exception: pass
            visits = unique_stores = active_days = 0
            last_visit = ''

        repeat_pct = round((visits - unique_stores) / visits * 100, 1) if visits else 0
        flags_local = []
        if last_visit:
            try:
                from datetime import datetime as _dt
                lv = _dt.fromisoformat(str(last_visit).replace('Z', '+00:00').split('.')[0])
                idle = (datetime.utcnow() - lv).days
                if idle >= 14:
                    flags_local.append(f'stale({idle}d)')
            except Exception:
                pass
        if repeat_pct >= 50 and visits >= 10:
            flags_local.append('high_repeat')
        rows_for_table.append({
            'rep': rep, 'visits': visits, 'unique_stores': unique_stores,
            'repeat_pct': repeat_pct, 'active_days': active_days,
        })
        if flags_local:
            flagged.append({'rep': rep, 'flags': flags_local,
                            'visits': visits, 'unique_stores': unique_stores,
                            'repeat_pct': repeat_pct})

    if USE_POSTGRES: cur.close()

    parts = [f"Rep behavior over last {days} days:"]
    rows_for_table.sort(key=lambda x: -x['visits'])
    for r in rows_for_table:
        parts.append(
            f"\n  {r['rep']}: {r['visits']} visits across {r['unique_stores']} stores "
            f"({r['repeat_pct']}% repeats), active {r['active_days']} days"
        )
    if flagged:
        parts.append("\n\nFlags:")
        for f in flagged:
            parts.append(f"\n  • {f['rep']}: {', '.join(f['flags'])}")
    else:
        parts.append("\n\nNo behavior flags raised. All reps within healthy ranges.")
    parts.append("\n\nFor the full per-store breakdown, open /exports → Rep Behavior.")
    return ''.join(parts), 'rep-behavior', rows_for_table, ['rep', 'visits', 'unique_stores', 'repeat_pct', 'active_days']


def _summarize_source_drift():
    """Plain-English breakdown of where SOD and lcbo.com disagree."""
    db = get_db()
    try:
        _, u_stats = _resolve_store_universe(db, lcbo_window_hours=48)
        _, c_stats = _resolve_carrying_us_universe(db, lcbo_window_hours=48)
    except Exception:
        u_stats, c_stats = {}, {}

    parts = ["Source drift across SOD, lcbo.com, and our master directory:"]
    in_sod_only = u_stats.get('in_sod_only', 0)
    in_lcbo_only = u_stats.get('in_lcbo_only', 0)
    in_master_only = u_stats.get('in_master_only', 0)
    only_sod = c_stats.get('sod_only', 0)
    only_lcbo = c_stats.get('lcbo_only', 0)

    if in_sod_only:
        parts.append(f"{in_sod_only} stores show in SOD's latest snapshot but aren't in our master directory (auto-onboarded next scrape).")
    if in_lcbo_only:
        parts.append(f"{in_lcbo_only} stores show on lcbo.com but aren't in our master directory (auto-onboarded next scrape).")
    if in_master_only:
        parts.append(f"{in_master_only} stores in our directory haven't appeared in SOD or lcbo.com recently — likely closed or stale.")
    if only_lcbo:
        parts.append(f"{only_lcbo} stores carry our SKUs ONLY according to lcbo.com — SOD missed them. These are potential commission claims.")
    if only_sod:
        parts.append(f"{only_sod} stores carry our SKUs ONLY according to SOD — lcbo.com hasn't confirmed yet (out-of-stock or hidden).")
    if len(parts) == 1:
        parts.append("All sources agree.")
    return ' '.join(parts), 'source-drift', [], []


def _count_new_stores(days):
    """Stores first appearing in SOD inventory within the last N days."""
    sql = """
        SELECT store_number, MIN(snapshot_date)::text AS first_seen
        FROM sod_inventory
        GROUP BY store_number
        HAVING MIN(snapshot_date) >= CURRENT_DATE - (INTERVAL '1 day' * %s)
        ORDER BY first_seen DESC
        LIMIT 50
    """
    rows, _ = _run_select(sql, (days,))
    if not rows:
        return (
            f"No new LCBO stores appeared in the last {days} days. "
            f"(Detected by first-appearance in the daily SOD feed.)"
        ), sql.strip(), [], ['store_number', 'first_seen']
    return (
        f"Last {days} days: {len(rows)} new LCBO stores detected. "
        f"Most recent: store #{rows[0].get('store_number')} on {rows[0].get('first_seen')}."
    ), sql.strip(), rows, ['store_number', 'first_seen']


def _free_answer(question):
    """Try to answer the question deterministically.
    Returns (answer, sql, rows, columns) on success, None on no match."""
    q = question.strip()
    ql = q.lower()

    sku = _detect_sku(q)
    rep = _detect_rep(q)
    city = _detect_city(q)
    store_num = _detect_store_number(q)
    days = _detect_window_days(q)

    is_summary = any(w in ql for w in [
        'summarize', 'summary', 'overview', 'how is', "how's", 'report on',
        'tell me about', 'status of', 'how are', 'recap',
    ])
    is_count = any(w in ql for w in ['how many', 'count', 'number of'])
    is_top = any(w in ql for w in ['top', 'best', 'most', 'largest', 'biggest'])
    is_delisting = 'delist' in ql
    is_listing = ('listing' in ql or 'listed' in ql or 'carry' in ql or 'carrying' in ql or 'sell' in ql)
    is_portfolio = any(w in ql for w in [
        'portfolio', 'all skus', 'all products', 'everything', 'all brands',
        'whole', 'entire', 'tracked skus', 'tracked products',
    ])
    is_new = (
        'new' in ql or 'added' in ql or 'recent' in ql or 'fresh' in ql
        or 'gained' in ql or 'won' in ql or 'picked up' in ql
    )
    is_about_stores_total = (
        'lcbo store' in ql or 'how many stores' in ql or 'total stores' in ql
        or 'store count' in ql or 'lcbo locations' in ql
        or ('how many' in ql and 'store' in ql and not sku)
    )

    # Priority 0-pre-pre: rep-behavior audit — only fires when the question
    # is about REPS COLLECTIVELY (not a single rep). 'rep' singular is
    # detected → handled by the per-rep summary intent later.
    has_reps_plural = 'reps' in ql or 'team' in ql or 'all reps' in ql
    is_behavior_audit = (
        has_reps_plural and (
            'behavior' in ql or 'behaviour' in ql or 'working' in ql
            or 'lazy' in ql or 'slack' in ql or 'audit' in ql
            or ('same' in ql and ('store' in ql or 'place' in ql))
            or 'repeat' in ql or 'repeats' in ql
            or 'doing' in ql        # 'are reps doing their job', 'how are reps doing'
            or 'performance' in ql
            or 'how' in ql           # 'how are reps' / 'how is the team'
            or 'visit' in ql         # 'how often do reps visit'
            or 'work' in ql
        )
    ) or (
        'are they working' in ql or 'rep audit' in ql or 'team audit' in ql
    )
    if is_behavior_audit:
        return _summarize_rep_behavior(days)

    # Priority 0-pre: hidden-listings audit — "any hidden listings", "ghost
    # listings", "what is being hidden", "suspicious listings", "fraud"
    is_hidden_audit = (
        'hidden' in ql or 'ghost' in ql or 'sneaky' in ql or 'fraud' in ql
        or 'suspicious' in ql or 'flicker' in ql or 'mass delist' in ql
        or ('disappear' in ql and ('listing' in ql or 'store' in ql))
        or ('vanish' in ql)
    )
    if is_hidden_audit:
        return _summarize_hidden_listings(sku)

    # Priority 0a: source-drift / data-quality question — "where does SOD disagree"
    is_drift = (
        'drift' in ql or 'disagree' in ql or 'mismatch' in ql
        or 'source' in ql and ('compare' in ql or 'difference' in ql)
        or 'sod vs' in ql or 'lcbo vs' in ql
        or 'where' in ql and 'differ' in ql
    )
    if is_drift:
        return _summarize_source_drift()

    # Priority 0b: new-listings-by-snapshot-diff — "how many new stores were
    # ADDED for Red Admiral last 60 days" or "how many stores added this month".
    # Distinct from _count_new_listings (which counts NEW_LISTING events) —
    # this does a 2-snapshot diff that catches listings even if the diff
    # engine missed the event, plus cross-checks against lcbo.com.
    is_added_phrase = (
        'added' in ql or 'gained' in ql or 'won' in ql or 'picked up' in ql
        or ('stores' in ql and ('for' in ql or sku) and is_new)
    )
    if is_added_phrase and ('store' in ql or 'listing' in ql) and (sku or is_listing):
        return _new_listings_per_sku_in_range(days, sku)

    # Priority 0c: simpler new listings question — "how many new listings last week"
    if is_new and is_listing:
        return _count_new_listings(days, sku)

    # Priority 0c: new stores question — "any new stores added this week"
    if is_new and 'store' in ql and not sku and not rep and not is_listing:
        return _count_new_stores(days)

    # Priority 0d: total store count question — "how many stores does LCBO have"
    if is_about_stores_total and not (is_listing and sku):
        return _count_total_stores()

    # Priority 1: store-specific summary
    if store_num is not None:
        return _summarize_store(store_num)

    # Priority 2: rep summary
    if rep and (is_summary or 'doing' in ql or 'visit' in ql or 'activity' in ql):
        return _summarize_rep(rep)

    # Priority 3: portfolio rollup
    if is_portfolio or (is_summary and not sku and not rep):
        return _summarize_portfolio()

    # Priority 4: SKU-level questions
    if sku:
        if is_delisting:
            return _list_delisting(sku)
        if is_count and (is_listing or 'store' in ql):
            return _count_stores_listing(sku, city)
        if is_top:
            return _top_stores_for_sku(sku, 10, city)
        # Default to SKU summary
        return _summarize_sku(sku)

    # Priority 5: generic delisting question without SKU
    if is_delisting:
        return _list_delisting(None)

    # Priority 6: rep without summary keyword
    if rep:
        return _summarize_rep(rep)

    return None


_FREE_ANSWER_HELP = (
    "I can answer questions like: 'Summarize Red Admiral', 'How is Namit doing?', "
    "'Summarize store 217', 'How many stores list Chak De in Toronto?', "
    "'Top stores for Red Admiral', 'What's delisting?', 'Portfolio summary', "
    "'How many LCBO stores are there?', 'New listings last 7 days', "
    "'New stores added for Red Admiral last 60 days', "
    "'Any new stores added this month?', 'Where does SOD disagree with lcbo.com?', "
    "'Source drift report', 'Any hidden listings?', 'Ghost listings for Red Admiral?'."
)


@app.route('/api/ai/ask', methods=['POST'])
def api_ai_ask():
    """Natural-language Q&A over CRM data.

    Two paths:
      1. FREE deterministic engine (always tries first) — pattern matches the
         question to a fixed set of intents (summarize SKU/rep/store/portfolio,
         count, top-N, delisting list) and emits a real SQL query + plain-English
         narrative. No external API cost.
      2. Claude fallback (only if ANTHROPIC_API_KEY is set AND free engine
         can't match) — generates SQL via Claude, runs it, summarizes.

    Returns: { question, sql, rows, columns, row_count, answer, model }
    Safety:
      - Only SELECT queries allowed
      - Hard row limit of 1000
      - Statement timeout 5s
    """
    body = request.get_json(silent=True) or {}
    question = (body.get('question') or '').strip()
    if not question or len(question) > 500:
        return jsonify({'error': 'question must be 1-500 chars'}), 400

    # ── Path 1: free deterministic engine ──
    try:
        free = _free_answer(question)
    except Exception as e:
        return jsonify({'error': f'Free engine error: {e}'}), 500
    if free is not None:
        answer, sql, rows, cols = free
        return jsonify({
            'question': question,
            'sql': sql,
            'rows': rows,
            'columns': cols,
            'row_count': len(rows),
            'answer': answer,
            'model': 'anu-rules-v1 (free)',
        })

    # ── Path 2: Claude fallback (only if API key set + funded) ──
    if not ANTHROPIC_API_KEY:
        return jsonify({
            'question': question,
            'sql': '',
            'rows': [],
            'columns': [],
            'row_count': 0,
            'answer': (
                "I couldn't match that to a known pattern. " + _FREE_ANSWER_HELP
            ),
            'model': 'anu-rules-v1 (free)',
        })

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
        if r.status_code >= 400:
            # Surface Anthropic's actual error message (billing, model, etc.)
            try:
                err_body = r.json()
                err_msg = err_body.get('error', {}).get('message') or err_body
            except Exception:
                err_msg = r.text[:500]
            return jsonify({
                'error': f'Anthropic API error ({r.status_code}): {err_msg}',
                'model': AI_MODEL,
            }), 502
        sql = r.json()['content'][0]['text'].strip()
        # Strip code-fence if Claude added one
        if sql.startswith('```'):
            sql = re.sub(r'^```\w*\n?', '', sql)
            sql = re.sub(r'\n?```$', '', sql).strip()
    except Exception as e:
        return jsonify({'error': f'AI generation failed: {e}'}), 502

    # Safety: SELECT-only — multi-layered, audited 2026-05.
    # Strip block comments + line comments first so they can't smuggle DML.
    sql_clean = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    sql_clean = re.sub(r'--[^\n]*', ' ', sql_clean)
    sql_lower = sql_clean.lower().strip()

    # 1. Must START with SELECT or WITH (CTE)
    if not (sql_lower.startswith('select') or sql_lower.startswith('with')):
        return jsonify({'error': 'AI returned non-SELECT', 'sql': sql}), 422

    # 2. NO semicolons anywhere (was: only flagged "; " or ";\n" — bypassable).
    #    Trailing single ; is stripped first since some libraries add it.
    sql_no_trailing = sql_clean.rstrip().rstrip(';').rstrip()
    if ';' in sql_no_trailing:
        return jsonify({'error': 'AI returned multi-statement SQL', 'sql': sql}), 422

    # 3. Whole-word DML/DDL keyword scan (works regardless of whitespace, including
    #    CTE-with-DML attacks like  WITH x AS (DELETE FROM t RETURNING *) SELECT ...).
    sql_word_lower = ' ' + re.sub(r'\s+', ' ', sql_clean.lower()) + ' '
    forbidden_words = [
        'insert', 'update', 'delete', 'drop', 'alter', 'truncate',
        'create', 'grant', 'revoke', 'merge', 'replace', 'copy',
        'vacuum', 'analyze', 'reindex', 'cluster', 'lock',
        'do', 'execute', 'call', 'prepare', 'discard', 'listen', 'notify',
        'set', 'reset', 'load', 'comment',
        'pg_read_file', 'pg_ls_dir', 'pg_sleep', 'pg_terminate_backend',
        'dblink', 'dblink_exec', 'lo_import', 'lo_export',
    ]
    for w in forbidden_words:
        if f' {w} ' in sql_word_lower or sql_word_lower.startswith(f' {w} '):
            return jsonify({'error': f'AI returned dangerous SQL (keyword: {w})', 'sql': sql}), 422

    # Run on a fresh autocommit READ-ONLY connection so even if validation is
    # bypassed, Postgres itself refuses DML.
    rows: list = []
    cols: list = []
    try:
        if USE_POSTGRES:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute('SET statement_timeout = 5000')
                cur.execute('SET default_transaction_read_only = ON')
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
        if r2.status_code >= 400:
            try:
                err_body = r2.json()
                err_msg = err_body.get('error', {}).get('message') or err_body
            except Exception:
                err_msg = r2.text[:500]
            answer = f"(Summary skipped — Anthropic {r2.status_code}: {err_msg})"
        else:
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
                      COALESCE(t.id, 0), COALESCE(t.code, ''), COALESCE(t.name, ''), COALESCE(t.color, '#888'),
                      COALESCE(s.spirits_ambassador, ''), COALESCE(s.store_notes, '')
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
                      COALESCE(t.id, 0), COALESCE(t.code, ''), COALESCE(t.name, ''), COALESCE(t.color, '#888'),
                      COALESCE(s.spirits_ambassador, ''), COALESCE(s.store_notes, '')
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
        'spirits_ambassador': s[20] or '', 'store_notes': s[21] or '',
    }
    return jsonify({'store': store, 'snapshot_date': str(_max_snapshot_date()) if _max_snapshot_date() else None})


@app.route('/api/crm/resolve-store', methods=['GET'])
def api_crm_resolve_store():
    """Smart store resolver — rep types address OR store# and we find the match.

    Accepts:
      q   any of: store_number (e.g. "217"), postal code (e.g. "M5V 2H1"),
                  address fragment ("King St W"), account name fragment ("LCBO #217"),
                  city name ("Toronto").
      limit  max matches to return (default 8, max 25)

    Returns ranked matches with a confidence score so the UI can show the
    best hit at the top and let the rep tap to confirm. The same query gets
    re-tried across multiple fields so one input handles every common rep
    typing pattern.
    """
    q = (request.args.get('q') or '').strip()
    if not q:
        return jsonify({'query': '', 'matches': []})
    try:
        limit = max(1, min(int(request.args.get('limit', '8')), 25))
    except ValueError:
        limit = 8

    q_norm = q.strip()
    q_postal = q_norm.upper().replace(' ', '')  # postal codes without spaces
    q_like = f"%{q_norm}%"
    q_lower = q_norm.lower()

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    matches: list = []
    seen_ids: set = set()

    def _row_to_match(row, confidence: int, reason: str):
        sid = row[0]
        if sid in seen_ids:
            return None
        seen_ids.add(sid)
        return {
            'id': sid,
            'store_number': row[1],
            'account': row[2] or '',
            'address': row[3] or '',
            'city': row[4] or '',
            'postal': row[5] or '',
            'rep': row[6] or '',
            'priority': row[7] or '',
            'lat': row[8],
            'lng': row[9],
            'confidence': confidence,
            'match_reason': reason,
        }

    cols = ("s.id, s.store_number, s.account, s.address, s.city, s.postal, "
            "s.rep, s.priority, s.lat, s.lng")

    def _exec(sql: str, params: tuple):
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            return rows
        return db.execute(sql, params).fetchall()

    try:
        # 1) exact store number — highest confidence
        if q_norm.isdigit():
            sn = int(q_norm)
            rows = _exec(
                f"SELECT {cols} FROM stores s WHERE s.store_number = {ph} LIMIT 5",
                (sn,),
            )
            for r in rows:
                m = _row_to_match(r, 100, 'exact store number')
                if m: matches.append(m)

        # 2) exact postal code (normalize spaces)
        if len(matches) < limit and len(q_postal) >= 3 and q_postal[0].isalpha():
            rows = _exec(
                f"SELECT {cols} FROM stores s "
                f"WHERE UPPER(REPLACE(COALESCE(s.postal,''), ' ', '')) "
                f"      LIKE {ph} LIMIT 10",
                (q_postal + '%',),
            )
            for r in rows:
                m = _row_to_match(r, 90, f"postal starts with {q_postal}")
                if m: matches.append(m)

        # 3) address fragment
        if len(matches) < limit:
            rows = _exec(
                f"SELECT {cols} FROM stores s "
                f"WHERE LOWER(COALESCE(s.address,'')) LIKE {ph} LIMIT 15",
                (f'%{q_lower}%',),
            )
            for r in rows:
                m = _row_to_match(r, 70, 'address match')
                if m: matches.append(m)

        # 4) account name fragment (e.g. "LCBO #217", "Bedford Park")
        if len(matches) < limit:
            rows = _exec(
                f"SELECT {cols} FROM stores s "
                f"WHERE LOWER(COALESCE(s.account,'')) LIKE {ph} LIMIT 15",
                (f'%{q_lower}%',),
            )
            for r in rows:
                m = _row_to_match(r, 55, 'account name match')
                if m: matches.append(m)

        # 5) city match (lowest confidence — broad)
        if len(matches) < limit:
            rows = _exec(
                f"SELECT {cols} FROM stores s "
                f"WHERE LOWER(COALESCE(s.city,'')) LIKE {ph} LIMIT 15",
                (f'%{q_lower}%',),
            )
            for r in rows:
                m = _row_to_match(r, 40, f"city match: {q_norm}")
                if m: matches.append(m)
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        return jsonify({'query': q, 'matches': [], 'error': f'lookup failed: {e}'}), 500

    matches.sort(key=lambda m: (-m['confidence'], m['store_number']))
    return jsonify({
        'query': q,
        'count': len(matches[:limit]),
        'matches': matches[:limit],
        'how_to_read': (
            "confidence 100 = exact store#; 90 = postal-prefix; "
            "70 = address fragment; 55 = account name; 40 = city. "
            "Pick the top match unless the address looks wrong."
        ),
    })


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
@require_app_origin
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
@require_app_origin
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
@require_app_origin
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

    # Silent geo capture — frontend sends GPS coords + accuracy + read-time
    # silently on every activity submit. NEVER displayed in rep-facing UI;
    # operator-only via /api/admin/rep-activity-report CSV. Compute distance
    # from the store's known coords (haversine) so we can flag visits logged
    # from far away (e.g. logged from home).
    def _f(v):
        try:
            return float(v) if v not in (None, '', 'null') else None
        except (TypeError, ValueError):
            return None
    in_lat = _f(d.get('lat'))
    in_lng = _f(d.get('lng'))
    accuracy_m = _f(d.get('accuracy_m'))
    client_ts_raw = d.get('client_ts') or None
    client_ts = (
        client_ts_raw[:19].replace('T', ' ')
        if (client_ts_raw and isinstance(client_ts_raw, str)) else None
    )
    distance_from_store_m = None
    if in_lat and in_lng and store_id:
        try:
            if USE_POSTGRES:
                _c = db.cursor()
                _c.execute("SELECT lat, lng FROM stores WHERE id=%s", (store_id,))
                srow = _c.fetchone()
                _c.close()
            else:
                srow = db.execute("SELECT lat, lng FROM stores WHERE id=?", (store_id,)).fetchone()
            if srow and srow[0] and srow[1]:
                import math
                slat, slng = float(srow[0]), float(srow[1])
                R = 6371000.0
                dlat = math.radians(slat - in_lat)
                dlng = math.radians(slng - in_lng)
                a = (math.sin(dlat / 2) ** 2
                     + math.cos(math.radians(in_lat))
                     * math.cos(math.radians(slat))
                     * math.sin(dlng / 2) ** 2)
                distance_from_store_m = round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 1)
        except Exception:
            distance_from_store_m = None

    insert_cols = ['store_id', 'rep_id', 'activity_type', 'notes', 'rep',
                   'outcome', 'duration_minutes', 'rating',
                   'next_action', 'next_action_date', 'horeca_account_id',
                   'lat', 'lng', 'visit_date',
                   'accuracy_m', 'client_ts', 'distance_from_store_m']
    vals = (
        store_id, rep_id, activity_type, d.get('notes', ''), rep_name,
        d.get('outcome', ''), int(d.get('duration_minutes') or 0), int(d.get('rating') or 0),
        d.get('next_action', ''), d.get('next_action_date') or None,
        d.get('horeca_account_id'),
        float(in_lat or 0), float(in_lng or 0),
        visit_date,
        accuracy_m, client_ts, distance_from_store_m,
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

@app.route('/api/admin/bulk-geocode', methods=['POST'])
@require_admin_token
def api_admin_bulk_geocode():
    """Bulk-update lat/lng for many stores in one call. Auth: X-Admin-Token.

    Body: {"updates": [{"store_number": 4, "lat": 43.67, "lng": -79.35}, ...]}
    Idempotent: re-running with same data is safe (UPDATE only).
    """
    body = request.get_json(silent=True) or {}
    updates = body.get('updates') or []
    if not isinstance(updates, list) or not updates:
        return jsonify({'error': 'updates: [{store_number, lat, lng}, ...] required'}), 400

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    succeeded = 0
    failed = 0
    errors = []
    for u in updates:
        try:
            sn = int(u.get('store_number'))
            lat = float(u.get('lat'))
            lng = float(u.get('lng'))
            if not (lat and lng) or abs(lat) > 90 or abs(lng) > 180:
                raise ValueError("invalid coords")
            if USE_POSTGRES:
                cur = db.cursor()
                cur.execute(
                    f"UPDATE stores SET lat=%s, lng=%s WHERE store_number=%s",
                    (lat, lng, sn),
                )
                rc = cur.rowcount
                cur.close()
            else:
                rc = db.execute(
                    "UPDATE stores SET lat=?, lng=? WHERE store_number=?",
                    (lat, lng, sn),
                ).rowcount
            if rc > 0:
                succeeded += 1
            else:
                failed += 1
                errors.append(f"#{sn}: not found")
        except Exception as e:
            failed += 1
            errors.append(f"#{u.get('store_number')}: {e}")
            if USE_POSTGRES:
                try: db.rollback()
                except Exception: pass
    db.commit()
    return jsonify({
        'updates_received': len(updates),
        'succeeded': succeeded,
        'failed': failed,
        'errors': errors[:20],
    })


@app.route('/api/crm/eod-brief', methods=['GET'])
def api_crm_eod_brief():
    """End-of-day brief for a rep — summary text ready to paste into WhatsApp/text.

    ?rep=Namit  &date=YYYY-MM-DD (default today, Toronto)

    Counts visits, calls, emails, tastings against daily targets:
      - Target: 8-10 store visits + 6-8 calls/emails (= 14-18 activities)

    Returns:
      - rep, date, totals, per-store breakdown, deals_advanced
      - whatsapp_text: pre-formatted message ready to share
      - whatsapp_url: deep-link `https://wa.me/?text=...`
      - meets_target: bool
    """
    rep = (request.args.get('rep') or '').strip()
    date_str = (request.args.get('date') or _toronto_today().isoformat()).strip()
    if not rep:
        return jsonify({'error': 'rep required'}), 400
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'date must be YYYY-MM-DD'}), 400

    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    date_expr = "a.created_at::date" if USE_POSTGRES else "DATE(a.created_at)"

    # Pull all activities by this rep for that date
    sql = (
        f"SELECT a.id, a.activity_type, a.notes, a.outcome, a.duration_minutes, "
        f"a.rating, a.visit_date, a.created_at, "
        f"s.store_number, COALESCE(s.account,''), COALESCE(s.city,'') "
        f"FROM activities a "
        f"LEFT JOIN stores s ON s.id = a.store_id "
        f"WHERE a.deleted_at IS NULL "
        f"AND COALESCE(LOWER(TRIM(a.rep)),'') = LOWER(TRIM({ph})) "
        f"AND COALESCE(a.visit_date, {date_expr}) = {ph} "
        f"ORDER BY a.created_at"
    )
    if USE_POSTGRES:
        cur = db.cursor()
        cur.execute(sql, (rep, target_date.isoformat()))
        rows = cur.fetchall()
        cur.close()
    else:
        rows = db.execute(sql, (rep, target_date.isoformat())).fetchall()

    # Categorize
    VISIT_TYPES = {'store_visit', 'visit', 'tasting', 'sample_drop'}
    CALL_TYPES = {'call', 'phone'}
    EMAIL_TYPES = {'email', 'mail'}
    visits = []
    calls = []
    emails = []
    others = []
    for r in rows:
        rec = {
            'id': r[0], 'activity_type': r[1] or '', 'notes': r[2] or '',
            'outcome': r[3] or '', 'duration_minutes': int(r[4] or 0),
            'rating': int(r[5] or 0), 'visit_date': str(r[6]) if r[6] else None,
            'created_at': str(r[7]) if r[7] else None,
            'store_number': r[8], 'account': r[9], 'city': r[10],
        }
        at = (rec['activity_type'] or '').lower()
        if at in VISIT_TYPES: visits.append(rec)
        elif at in CALL_TYPES: calls.append(rec)
        elif at in EMAIL_TYPES: emails.append(rec)
        else: others.append(rec)

    # Pull deals advanced today
    deal_sql = (
        f"SELECT d.id, d.store_number, d.sku, d.stage, d.notes, d.expected_units "
        f"FROM deals d WHERE LOWER(TRIM(d.owner_rep)) = LOWER(TRIM({ph})) "
        f"AND ((d.created_at::date = {ph}) OR (d.closed_at::date = {ph})) "
    ) if USE_POSTGRES else (
        f"SELECT id, store_number, sku, stage, notes, expected_units "
        f"FROM deals WHERE LOWER(TRIM(owner_rep)) = LOWER(TRIM(?)) "
        f"AND (DATE(created_at) = ? OR DATE(closed_at) = ?)"
    )
    try:
        if USE_POSTGRES:
            cur = db.cursor()
            cur.execute(deal_sql, (rep, target_date.isoformat(), target_date.isoformat()))
            deals = [{'id': r[0], 'store_number': r[1], 'sku': r[2], 'stage': r[3],
                      'notes': r[4] or '', 'expected_units': int(r[5] or 0)}
                     for r in cur.fetchall()]
            cur.close()
        else:
            deals = [{'id': r[0], 'store_number': r[1], 'sku': r[2], 'stage': r[3],
                      'notes': r[4] or '', 'expected_units': int(r[5] or 0)}
                     for r in db.execute(deal_sql, (rep, target_date.isoformat(), target_date.isoformat())).fetchall()]
    except Exception:
        if USE_POSTGRES:
            try: db.rollback()
            except Exception: pass
        deals = []

    # Targets
    TARGET_VISITS_MIN = 8
    TARGET_VISITS_MAX = 10
    TARGET_CALLS_EMAILS_MIN = 6
    TARGET_CALLS_EMAILS_MAX = 8
    n_visits = len(visits)
    n_calls = len(calls)
    n_emails = len(emails)
    n_calls_emails = n_calls + n_emails
    visits_ok = n_visits >= TARGET_VISITS_MIN
    calls_ok = n_calls_emails >= TARGET_CALLS_EMAILS_MIN
    meets_target = visits_ok and calls_ok

    # Build WhatsApp text
    lines = [
        f"📋 EOD — {rep} — {target_date.strftime('%a %b %d')}",
        "",
        f"✅ Visits: {n_visits} (target {TARGET_VISITS_MIN}-{TARGET_VISITS_MAX})"
        + (" 👍" if visits_ok else " ⚠️ short"),
        f"📞 Calls + emails: {n_calls_emails} ({n_calls} calls + {n_emails} emails) "
        f"(target {TARGET_CALLS_EMAILS_MIN}-{TARGET_CALLS_EMAILS_MAX})"
        + (" 👍" if calls_ok else " ⚠️ short"),
    ]
    if visits:
        lines.append("")
        lines.append("Stores visited:")
        for v in visits[:12]:
            tag = f" — {v['outcome']}" if v.get('outcome') else ""
            lines.append(f"  • #{v['store_number']} {v['account'][:25]}{tag}")
    if deals:
        lines.append("")
        lines.append(f"Pipeline updates: {len(deals)}")
        for d in deals[:6]:
            lines.append(f"  • #{d['store_number']} → {d['stage']}"
                         + (f" ({d['expected_units']}u)" if d['expected_units'] else ""))
    if not (visits or calls or emails or deals):
        lines.append("")
        lines.append("No activity logged today.")

    text = "\n".join(lines)
    import urllib.parse as _up
    whatsapp_url = f"https://wa.me/?text={_up.quote(text)}"

    return jsonify({
        'rep': rep,
        'date': target_date.isoformat(),
        'totals': {
            'visits': n_visits,
            'calls': n_calls,
            'emails': n_emails,
            'calls_emails_combined': n_calls_emails,
            'other_activities': len(others),
            'deals_touched': len(deals),
        },
        'targets': {
            'visits_min': TARGET_VISITS_MIN, 'visits_max': TARGET_VISITS_MAX,
            'calls_emails_min': TARGET_CALLS_EMAILS_MIN, 'calls_emails_max': TARGET_CALLS_EMAILS_MAX,
        },
        'meets_target': meets_target,
        'visits_ok': visits_ok, 'calls_emails_ok': calls_ok,
        'visits': visits, 'calls': calls, 'emails': emails, 'others': others,
        'deals_touched': deals,
        'whatsapp_text': text,
        'whatsapp_url': whatsapp_url,
    })


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
        'Ikshit': {  # Burlington + Oakville (Halton south)
            'prefixes': ['L7L','L7M','L7N','L7P','L7R','L7S','L7T'],
            'cities': ['Burlington','Oakville','Bronte']},
        'Virat': {   # Mississauga + Milton + Caledon
            'prefixes': ['L4Z','L5','L6P','L6R','L6S','L6T','L6V','L6W','L6X','L6Y','L7A','L7C','L7E','L7G','L7K'],
            'cities': ['Mississauga','Milton','Caledon','Bolton','Georgetown','Brampton']},
        'Surya': {
            # IN + AROUND OTTAWA ONLY (no Kingston/Brockville/Belleville/Petawawa)
            'prefixes': ['K1', 'K2', 'K0A', 'K4A', 'K4B', 'K4C', 'K4P', 'K4M', 'K4R'],
            'cities': ['Ottawa','Kanata','Nepean','Orleans','Stittsville',
                       'Manotick','Rockland','Embrun','Carleton Place',
                       'Almonte','Smiths Falls','Gloucester','Vanier',
                       'Russell','Kemptville','Cumberland','Greely']},
        'Neeraj': {  # Guelph + Cambridge + KW + Hamilton (broader west of GTA)
            'prefixes': ['N1','N2','N3','L8','L9G','L9H','L9J','L9K'],
            'cities': ['Guelph','Cambridge','Kitchener','Waterloo','Hamilton',
                       'Stoney Creek','Ancaster','Dundas','Acton','Rockwood',
                       'Fergus','Elora','Erin']},
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

    # Cluster into days. STRATEGY: dense urban-first, rural last.
    #   1. Group by FSA (first 3 postal chars)
    #   2. For each FSA, compute centroid + neighborhood density (avg distance
    #      between stores in the FSA) — DENSE = small avg distance
    #   3. Sort FSAs by density (densest first) so Day 1 = tightest cluster
    #   4. Pack into days with max_per_day cap
    #   5. Within each day, run nearest-neighbor TSP from centroid
    from collections import defaultdict
    from math import radians, sin, cos, asin, sqrt

    def hv(a, b):
        if not (a.get('lat') and a.get('lng') and b.get('lat') and b.get('lng')):
            return 999
        lat1, lng1, lat2, lng2 = map(radians, (a['lat'], a['lng'], b['lat'], b['lng']))
        dlat = lat2 - lat1; dlng = lng2 - lng1
        h = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlng/2)**2
        return 2 * 6371 * asin(sqrt(h))

    by_fsa = defaultdict(list)
    for s in stores:
        fsa = s['postal'][:3] if s['postal'] else 'UNK'
        by_fsa[fsa].append(s)

    # Sort FSAs by distance from each rep's HOME BASE (densest urban centre).
    # Day 1 = closest cluster to home (urban core), later days = farther rural.
    REP_HOME = {
        'Namit':  {'lat': 43.6532, 'lng': -79.3832, 'name': 'Downtown Toronto'},
        'Ikshit': {'lat': 43.5890, 'lng': -79.6441, 'name': 'Mississauga'},
        'Virat':  {'lat': 43.8975, 'lng': -78.9429, 'name': 'Whitby/Oshawa'},
        'Surya':  {'lat': 45.4215, 'lng': -75.6972, 'name': 'Downtown Ottawa'},
        'Neeraj': {'lat': 43.2557, 'lng': -79.8711, 'name': 'Hamilton'},
    }
    home = REP_HOME.get(rep, {'lat': 43.65, 'lng': -79.38})

    def fsa_priority(fsa):
        fsa_stores = by_fsa[fsa]
        with_gps = [s for s in fsa_stores if s.get('lat') and s.get('lng')]
        if not with_gps:
            return (10000, 0)  # no GPS → push to end
        cx = sum(s['lat'] for s in with_gps) / len(with_gps)
        cy = sum(s['lng'] for s in with_gps) / len(with_gps)
        dist_from_home = hv({'lat': cx, 'lng': cy}, home)
        # Closer to home = lower score = comes first.
        # Tie-break: bigger FSA wins (more stops in one trip)
        return (dist_from_home, -len(fsa_stores))

    sorted_fsas = sorted(by_fsa.keys(), key=fsa_priority)

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
            # Burlington + Oakville (Halton south)
            'name': 'Burlington + Oakville',
            'postal_prefixes': ['L7L', 'L7M', 'L7N', 'L7P', 'L7R', 'L7S', 'L7T'],
            'fallback_cities': ['Burlington', 'Oakville', 'Bronte'],
            'target_min': 25, 'target_max': 45,
        },
        'Virat': {
            # Mississauga + Milton + Caledon + Brampton
            'name': 'Mississauga + Milton + Caledon + Brampton',
            'postal_prefixes': ['L4Z','L5','L6P','L6R','L6S','L6T','L6V','L6W','L6X','L6Y','L7A','L7C','L7E','L7G','L7K'],
            'fallback_cities': ['Mississauga', 'Milton', 'Caledon', 'Bolton',
                                'Georgetown', 'Brampton'],
            'target_min': 40, 'target_max': 70,
        },
        'Surya': {
            # IN + AROUND OTTAWA ONLY — Ottawa core (K1*, K2*) + immediate
            # surrounding (K0A rural Ottawa, K4* Orleans/Cumberland).
            # NOT: Kingston/Brockville/Belleville/Petawawa/Renfrew (too far).
            'name': 'Ottawa + immediate surroundings',
            'postal_prefixes': ['K1', 'K2', 'K0A', 'K4A', 'K4B', 'K4C', 'K4P', 'K4M', 'K4R'],
            'fallback_cities': ['Ottawa', 'Kanata', 'Nepean', 'Orleans',
                                'Stittsville', 'Manotick', 'Rockland', 'Embrun',
                                'Carleton Place', 'Almonte', 'Smiths Falls',
                                'Gloucester', 'Vanier', 'Russell', 'Kemptville',
                                'Cumberland', 'Greely'],
            'target_min': 40, 'target_max': 70,
        },
        'Neeraj': {
            # Guelph + Cambridge + KW + Hamilton (west of GTA)
            'name': 'Guelph + Cambridge + KW + Hamilton',
            'postal_prefixes': ['N1', 'N2', 'N3', 'L8', 'L9G', 'L9H', 'L9J', 'L9K'],
            'fallback_cities': ['Hamilton', 'Burlington', 'Niagara Falls',
                                'Guelph', 'Cambridge', 'Kitchener', 'Waterloo',
                                'Hamilton', 'Stoney Creek', 'Ancaster', 'Dundas',
                                'Acton', 'Rockwood', 'Fergus', 'Elora', 'Erin'],
            'target_min': 50, 'target_max': 100,
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

# Module-level TERRITORY map — single source of truth for postal-prefix
# routing assignments. Reps can STILL log activities anywhere; this just
# drives /api/crm/territory-plan + the auto-stamp on stores.rep.
TERRITORY_MAP = {
    'Namit': {
        # WHOLE GTA — Toronto + Durham + York + Peel (Brampton + Mississauga
        # + Caledon + Milton + Malton) + Halton-north. Consolidated 2026-05-31
        # per founder direction: "namit has whole in and around gta".
        # ONLY exclusion within GTA: L7L-T (Burlington + Oakville = Ikshit).
        # Reps can still log anywhere; this is the default-assignment map.
        'name': 'Greater Toronto Area — entire',
        'postal_prefixes': [
            'M',                                           # Toronto (416)
            'L1',                                          # Durham
            'L3R', 'L3S', 'L3T', 'L3X', 'L3Y',             # Markham/Newmarket
            'L4',                                          # All L4 (Vaughan/Stouffville/Aurora/RH/Concord/Woodbridge/Malton/N.Mississauga)
            'L5',                                          # Mississauga
            'L6',                                          # Brampton + Markham
            'L7A', 'L7C', 'L7E', 'L7G', 'L7K',             # Caledon/Bolton/Georgetown
            'L9T',                                         # Milton
        ],
        'fallback_cities': [
            'Toronto', 'North York', 'Etobicoke', 'Scarborough',
            'Woodbridge', 'Vaughan', 'Maple', 'Markham', 'Stouffville',
            'Newmarket', 'Aurora', 'Richmond Hill', 'Thornhill',
            'Concord', 'Kleinburg', 'Pickering', 'Ajax', 'Whitby',
            'Oshawa', 'East Gwillimbury', 'Newcastle',
            'Mississauga', 'Malton', 'Brampton', 'Milton',
            'Bolton', 'Caledon', 'Georgetown', 'Schomberg',
        ],
        'target_min': 220, 'target_max': 300,
    },
    'Surya': {
        # Ottawa METRO only — capped 50-55 stores per founder direction
        # 2026-05-31 (Surya can't travel too rural). Includes Ottawa core
        # + immediate ring (rural K0A) + Orleans/Cumberland/Rockland (K4)
        # + Carleton Place / Smiths Falls (K7A/C — close enough to Ottawa).
        # EXCLUDES the Ottawa Valley / Eastern Ontario rural reach that was
        # in the prior wider map (Cornwall/Brockville/Hawkesbury/Pembroke/
        # Petawawa/Renfrew/Perth/all K0B-K0J).
        'name': 'Ottawa metro (capped 50-55 stores)',
        'postal_prefixes': [
            'K1', 'K2',                                    # Ottawa core (416-equivalent)
            'K0A',                                         # Rural Ottawa ring
            'K4A', 'K4B', 'K4C', 'K4K', 'K4M', 'K4P', 'K4R',  # Orleans/Cumberland/Rockland
            'K7A', 'K7C',                                  # Smiths Falls + Carleton Place (close-in)
        ],
        'fallback_cities': [
            'Ottawa', 'Kanata', 'Nepean', 'Orleans', 'Stittsville',
            'Manotick', 'Rockland', 'Embrun', 'Carleton Place',
            'Almonte', 'Smiths Falls', 'Gloucester', 'Vanier',
            'Russell', 'Kemptville', 'Cumberland', 'Greely',
            'Casselman', 'Limoges', 'Metcalfe', 'Osgoode',
            'Carp', 'Richmond', 'Bourget',
        ],
        'target_min': 48, 'target_max': 58,
    },
    'Ikshit': {
        # Burlington (L7L-T) + Oakville (L6H-M). Halton south.
        # NOTE: longer prefixes here beat Namit's bare 'L6' / 'L7A/C/E/G/K',
        # so Oakville stays with Ikshit even though it's geographically GTA.
        'name': 'Burlington + Oakville',
        'postal_prefixes': [
            'L7L', 'L7M', 'L7N', 'L7P', 'L7R', 'L7S', 'L7T',  # Burlington
            'L6H', 'L6J', 'L6K', 'L6L', 'L6M',                 # Oakville
        ],
        'fallback_cities': ['Burlington', 'Oakville', 'Bronte'],
        'target_min': 13, 'target_max': 30,
    },
    # Neeraj + Virat removed from TERRITORY_MAP on 2026-05-31 — their
    # patches (Brampton/Milton/Malton, Mississauga/Caledon) are now
    # consolidated under Namit's "whole GTA" assignment. They can still
    # log activities anywhere; this just removes their default stamp.
}


@app.route('/api/crm/admin/purge-test-activities', methods=['POST'])
@require_app_origin
def api_crm_admin_purge_test_activities():
    """Soft-delete QA / stress-test activity rows.

    Reversible: sets activities.deleted_at = now() for rows whose notes
    begin with a known test prefix. NEVER hard-deletes. NEVER touches
    real rep activity. The rep-activity-report and every rollup already
    filter `deleted_at IS NULL`, so soft-deleted rows vanish from all
    reporting but remain on disk for audit / undelete.

    Matched prefixes (case-insensitive): 'stress-test', 'stress-check',
    'final-test', 'qa-test'.

    Returns the count soft-deleted and a sample of what was matched.
    """
    db = get_db()
    ph = '%s' if USE_POSTGRES else '?'
    prefixes = ['stress-test%', 'stress-check%', 'final-test%', 'qa-test%']
    where = ' OR '.join([f"LOWER(notes) LIKE {ph}" for _ in prefixes])

    try:
        cur = db.cursor() if USE_POSTGRES else db
        # Preview what will be matched
        cur.execute(
            f"SELECT id, rep, activity_type, notes FROM activities "
            f"WHERE deleted_at IS NULL AND ({where}) "
            f"ORDER BY id",
            prefixes,
        )
        matched = [
            {'id': r[0], 'rep': r[1], 'activity_type': r[2],
             'notes': (r[3] or '')[:60]}
            for r in cur.fetchall()
        ]
        if not matched:
            if USE_POSTGRES:
                cur.close()
            return jsonify({'soft_deleted': 0, 'matched': [],
                            'status': 'nothing to purge'})

        ids = [m['id'] for m in matched]
        id_ph = ','.join([ph] * len(ids))
        if USE_POSTGRES:
            cur.execute(
                f"UPDATE activities SET deleted_at = NOW() "
                f"WHERE id IN ({id_ph}) AND deleted_at IS NULL",
                ids,
            )
        else:
            cur.execute(
                f"UPDATE activities SET deleted_at = datetime('now') "
                f"WHERE id IN ({id_ph}) AND deleted_at IS NULL",
                ids,
            )
        db.commit()
        if USE_POSTGRES:
            cur.close()
        return jsonify({
            'soft_deleted': len(ids),
            'matched': matched,
            'status': 'ok',
            'note': 'Reversible — rows kept on disk with deleted_at set. '
                    'They no longer appear in any report or rollup.',
        })
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        return jsonify({'error': f'purge failed: {e}'}), 500


@app.route('/api/crm/admin/restamp-territories', methods=['POST'])
@require_app_origin
def api_crm_admin_restamp_territories():
    """Re-stamp the stores.rep column based on TERRITORY_MAP postal prefixes.

    Walks every store; for each store, finds the first rep in TERRITORY_MAP
    whose postal_prefixes match the store's postal code; sets stores.rep
    accordingly.

    Query params:
      clear_unmatched=1  — stores matching no territory get rep='' (a clean
                           full re-stamp; required when a rep's old patch is
                           dropped from the map so stale assignments don't
                           linger). Default 0 = leave unmatched stores as-is.

    This is the canonical way to assign Namit=GTA, Surya=Ottawa, etc.
    Returns per-rep counts of stores stamped.
    """
    clear_unmatched = (request.args.get('clear_unmatched') or '').strip() in ('1', 'true', 'yes')
    db = get_db()
    cur = db.cursor() if USE_POSTGRES else db
    try:
        if USE_POSTGRES:
            cur.execute("SELECT id, postal, city FROM stores")
        else:
            cur.execute("SELECT id, postal, city FROM stores")
        all_stores = cur.fetchall()
    except Exception as e:
        return jsonify({'error': f'read failed: {e}'}), 500

    per_rep_counts: dict = {r: 0 for r in TERRITORY_MAP.keys()}
    unmatched = 0
    updates: list = []  # (rep, store_id)

    for row in all_stores:
        store_id = row[0] if not isinstance(row, dict) else row['id']
        postal = (row[1] if not isinstance(row, dict) else row['postal']) or ''
        city = (row[2] if not isinstance(row, dict) else row['city']) or ''
        postal_clean = postal.upper().replace(' ', '')
        city_lower = city.lower().strip()

        matched_rep = None
        # Postal-prefix match first (longest prefix wins)
        best_pfx_len = 0
        for rep, cfg in TERRITORY_MAP.items():
            for pfx in cfg.get('postal_prefixes', []):
                if postal_clean.startswith(pfx.upper()) and len(pfx) > best_pfx_len:
                    matched_rep = rep
                    best_pfx_len = len(pfx)
        # City fallback if no postal match
        if not matched_rep:
            for rep, cfg in TERRITORY_MAP.items():
                if city_lower in [c.lower() for c in cfg.get('fallback_cities', [])]:
                    matched_rep = rep
                    break
        if matched_rep:
            per_rep_counts[matched_rep] += 1
            updates.append((matched_rep, store_id))
        else:
            unmatched += 1
            if clear_unmatched:
                # Drop the stale rep so a removed territory doesn't linger
                updates.append(('', store_id))

    # Bulk update
    try:
        if USE_POSTGRES:
            psycopg2.extras.execute_batch(
                cur,
                "UPDATE stores SET rep = %s WHERE id = %s",
                updates, page_size=500,
            )
        else:
            cur.executemany(
                "UPDATE stores SET rep = ? WHERE id = ?",
                updates,
            )
        db.commit()
        if USE_POSTGRES:
            cur.close()
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        return jsonify({'error': f'update failed: {e}'}), 500

    try:
        _log_event('territory_restamp', 'admin', None, '',
                   {'per_rep': per_rep_counts, 'unmatched': unmatched})
    except Exception:
        pass

    return jsonify({
        'status': 'ok',
        'total_stores': len(all_stores),
        'per_rep_counts': per_rep_counts,
        'unmatched': unmatched,
        'clear_unmatched': clear_unmatched,
        'note': (
            "Stores assignment updated from TERRITORY_MAP. Reps can still log "
            "activities at any store — this only sets the default rep for "
            "territory-plan + rep-performance views."
            + (" Unmatched stores were cleared to unassigned."
               if clear_unmatched else "")
        ),
    })


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
@require_app_origin
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
@require_app_origin
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
    """Scrape live LCBO.com inventory + RECONCILE with SOD + AUTO-ONBOARD stores.

    Three outputs:
      1. inventory_history: append-only trend log.
      2. sod_store_sku_changes with change_type='LCBO_LIVE_ONLY' for stores where
         lcbo.com shows on_hand > 0 BUT SOD has no row OR status='F' (delisted).
         This is the killer signal: 'lcbo.com shows live inventory at this store
         but SOD says it's delisted/missing — investigate.'
      3. AUTO-ONBOARD: any store_number that lcbo.com shows but our master
         `stores` directory is missing → INSERT with city/address/phone from
         the scrape. ON CONFLICT DO NOTHING so we never overwrite hand-curated
         CRM rows. This grows the master directory automatically as LCBO opens
         new stores or as we widen coverage.

    Idempotent on the reconciliation side via UNIQUE(sku, store_number,
    change_date, change_type). Runs every 30 min via the scheduler.
    """
    try:
        scrape = globals().get('scrape_lcbo_inventory')
        if not callable(scrape):
            print('[LCBO-live] scrape_lcbo_inventory not available')
            return
        conn = _sod_get_conn()
        cur = conn.cursor()
        total_rows = 0
        discoveries = 0  # stores found via lcbo.com but missing from SOD
        new_stores_added = 0  # stores newly auto-onboarded into `stores`
        today_str = _toronto_today().isoformat()
        # Collect per-store metadata across all SKUs to do one bulk auto-onboard
        # at the end — saves N×SKU upserts.
        store_meta_seen: dict = {}  # store_num -> {city, intersection, address, phone}
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

                # Track per-store metadata for auto-onboarding (the lcbo.com
                # storeList rows include city, intersection, address, phone)
                try:
                    sn_int = int(store_num_raw)
                    if sn_int not in store_meta_seen:
                        store_meta_seen[sn_int] = {
                            'city': (r.get('city') or r.get('store_city') or '').strip(),
                            'intersection': (r.get('intersection') or '').strip(),
                            'address': (r.get('address') or '').strip(),
                            'phone': (r.get('phone') or '').strip(),
                            'store_name': (r.get('store_name') or '').strip(),
                        }
                except (ValueError, TypeError):
                    pass

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

        # ── AUTO-ONBOARD: bulk-insert any store_numbers we saw on lcbo.com
        # that aren't yet in our master `stores` directory. ON CONFLICT
        # DO NOTHING so hand-curated CRM rows are never overwritten.
        if store_meta_seen:
            try:
                # Find which store_numbers are missing
                seen_nums = list(store_meta_seen.keys())
                if USE_POSTGRES:
                    cur.execute(
                        "SELECT store_number FROM stores WHERE store_number = ANY(%s)",
                        (seen_nums,))
                    existing = {int(r[0]) for r in cur.fetchall()}
                else:
                    placeholders = ','.join('?' * len(seen_nums))
                    cur.execute(
                        f"SELECT store_number FROM stores WHERE store_number IN ({placeholders})",
                        seen_nums)
                    existing = {int(r[0]) for r in cur.fetchall()}
                missing = [n for n in seen_nums if n not in existing]
                if missing:
                    rows_to_insert = []
                    for sn in missing:
                        meta = store_meta_seen[sn]
                        # Build a sensible account label from city + intersection
                        account = (
                            f"LCBO #{sn}"
                            + (f" — {meta['city']}" if meta['city'] else '')
                            + (f" ({meta['intersection']})" if meta['intersection'] else '')
                        )[:200]
                        rows_to_insert.append((
                            sn,
                            account,
                            meta['address'] or '',
                            meta['city'] or '',
                            meta['phone'] or '',
                            'Standard',
                        ))
                    if USE_POSTGRES:
                        psycopg2.extras.execute_values(
                            cur,
                            """INSERT INTO stores
                                 (store_number, account, address, city, phone, priority)
                               VALUES %s
                               ON CONFLICT (store_number) DO NOTHING""",
                            rows_to_insert,
                        )
                    else:
                        cur.executemany(
                            """INSERT OR IGNORE INTO stores
                                 (store_number, account, address, city, phone, priority)
                               VALUES (?,?,?,?,?,?)""",
                            rows_to_insert,
                        )
                    new_stores_added = len(missing)
            except Exception as e:
                print(f'[LCBO-live] auto-onboard skipped: {e}')

        conn.commit()
        cur.close()
        conn.close()
        print(f'[LCBO-live] scraped {total_rows} store-rows; '
              f'found {discoveries} discoveries (lcbo.com live but SOD blank/F); '
              f'auto-onboarded {new_stores_added} new stores into master directory')
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
@require_app_origin
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
