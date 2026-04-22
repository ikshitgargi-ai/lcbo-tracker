"""Sprint 0 regression tests — the bugs that explain "everything is off."

Each test corresponds to a HIGH-severity bug found in the audit. If any of
these fail, deploy must be blocked.

Run with: pytest tests/ -v
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

# Force SQLite for tests so we don't touch production Postgres.
os.environ.pop('DATABASE_URL', None)
TEST_DB = '/tmp/lcbo_tracker_sprint0_test.db'
os.environ['DB_PATH'] = TEST_DB

# Import the app fresh (and isolated) for each test session
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.fixture(scope='module', autouse=True)
def _fresh_db():
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    # Force re-import in case prior tests cached app
    for mod in list(sys.modules):
        if mod == 'app' or mod.startswith('app.'):
            del sys.modules[mod]
    yield
    # Don't delete — keeps inspection possible after a failure


@pytest.fixture(scope='module')
def app_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location('app', os.path.join(os.path.dirname(__file__), '..', 'app.py'))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def client(app_module):
    return app_module.app.test_client()


# ========================================================================
# Bug #1: latest_filename uses UTC weekday → wrong file requested at night
# ========================================================================

class TestTorontoTimezoneFilename:
    def test_filename_uses_toronto_weekday_not_utc(self, app_module):
        """At 23:00 UTC on a Monday (= 19:00 ET, still MON in Toronto),
        latest_filename should return MON file, not TUE.
        Pre-fix: returned TUE because UTC-weekday."""
        client = app_module.SODClient.__new__(app_module.SODClient)
        client.agent_id = '1113'

        # Mock: 23:00 UTC Mon Jan 6, 2025 = 18:00 ET Mon
        fixed_utc = datetime(2025, 1, 6, 23, 0, 0, tzinfo=timezone.utc)
        try:
            from zoneinfo import ZoneInfo
            fixed_toronto = fixed_utc.astimezone(ZoneInfo('America/Toronto'))
            assert fixed_toronto.strftime('%a').upper() == 'MON', \
                f"Test setup wrong: Toronto weekday should be MON, got {fixed_toronto.strftime('%a')}"
        except ImportError:
            pytest.skip('zoneinfo not available')

        with patch.object(app_module.SODClient, '_toronto_now', return_value=fixed_toronto):
            assert client.latest_filename('daily_a') == 'alldlyinventoryMON.zip', \
                "BUG: latest_filename returned wrong weekday (was using UTC weekday)"
            assert client.latest_filename('daily_b') == 'Edlyinventory1113MON.zip'


# ========================================================================
# Bug #7 (smoking gun): rep "carrying" count includes status D and F
# ========================================================================

class TestRepCarryingFiltersStatusL:
    def test_rep_carrying_only_counts_listed_stores(self, app_module, client):
        """Rep with 10 stores: 2 status='L', 3 status='D', 5 not in inventory.
        Pre-fix: carrying_cnt = 5 (counts D as carrying), gap = 5.
        Post-fix: carrying_cnt = 2, gap = 8."""
        # Seed: rep + 10 stores + sod_inventory with mix of statuses
        m = app_module
        ph = m._sod_ph()
        conn = m._sod_get_conn()
        cur = conn.cursor()
        # Insert rep into stores table
        cur.execute("DELETE FROM stores")
        cur.execute("DELETE FROM sod_inventory")
        cur.execute("DELETE FROM sod_products")

        for i in range(10):
            cur.execute(
                "INSERT INTO stores (store_number, account, city, postal, rep) VALUES (?,?,?,?,?)",
                (1000 + i, f'Store {1000+i}', 'Toronto', 'M5V 1J1', 'Test Rep'),
            )

        # First tracked SKU
        first_sku = list(m.SOD_TRACKED_SKUS.keys())[0]  # e.g. '0020187'
        snapshot_date = '2026-04-21'

        # Mark sod_products row as tracked
        cur.execute(
            "INSERT INTO sod_products (sku, product_name, current_status, is_tracked, brand) "
            "VALUES (?,?,?,?,?)",
            (first_sku, m.SOD_TRACKED_SKUS[first_sku][1], 'L', 1, m.SOD_TRACKED_SKUS[first_sku][0]),
        )

        # 2 stores status='L'
        for i in range(2):
            cur.execute(
                "INSERT INTO sod_inventory (sku, store_number, snapshot_date, status, on_hand, source) "
                "VALUES (?,?,?,?,?,?)",
                (first_sku, 1000 + i, snapshot_date, 'L', 12, 'daily_a'),
            )
        # 3 stores status='D'
        for i in range(2, 5):
            cur.execute(
                "INSERT INTO sod_inventory (sku, store_number, snapshot_date, status, on_hand, source) "
                "VALUES (?,?,?,?,?,?)",
                (first_sku, 1000 + i, snapshot_date, 'D', 4, 'daily_a'),
            )
        # Stores 5-9: NO sod_inventory row (= true gap)
        conn.commit()
        conn.close()

        # Hit rep report
        r = client.get('/api/reports/rep')
        assert r.status_code == 200
        body = r.json
        # Accept either list or {reps: [...]}
        rows = body if isinstance(body, list) else body.get('reps', [])
        # Find Test Rep
        rep_row = next((x for x in rows if x['rep'].strip().lower() == 'test rep'), None)
        assert rep_row is not None, "Test Rep not in /api/reports/rep response"
        assert rep_row['total_stores'] == 10
        # Find this SKU's entry in per_product
        sku_entry = next((p for p in rep_row['per_product'] if p['sku'] == first_sku), None)
        assert sku_entry is not None, f"SKU {first_sku} not in rep's per_product"
        assert sku_entry['stores_carrying'] == 2, \
            f"BUG: stores_carrying should be 2 (only L=Listed), got {sku_entry['stores_carrying']}"
        assert sku_entry['gap_count'] == 8, \
            f"BUG: gap_count should be 8 (10 - 2 listed), got {sku_entry['gap_count']}"
        assert sku_entry['stores_delisting'] == 3, \
            f"stores_delisting should be 3 (D status), got {sku_entry['stores_delisting']}"

    def test_rep_matching_is_case_insensitive(self, app_module, client):
        """Rep with whitespace + mixed case should still match."""
        m = app_module
        conn = m._sod_get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM stores")
        # Same rep with varying whitespace/case — should be normalized to one
        cur.execute("INSERT INTO stores (store_number, rep) VALUES (?,?)", (2001, '  john smith '))
        cur.execute("INSERT INTO stores (store_number, rep) VALUES (?,?)", (2002, 'John Smith'))
        cur.execute("INSERT INTO stores (store_number, rep) VALUES (?,?)", (2003, 'JOHN SMITH'))
        conn.commit()
        conn.close()

        r = client.get('/api/reports/rep')
        body = r.json
        rows = body if isinstance(body, list) else body.get('reps', [])
        # Should appear as ONE rep with 3 stores after TRIM dedupe
        smiths = [x for x in rows if x['rep'].strip().lower() == 'john smith']
        assert len(smiths) == 1, f"BUG: rep variants not deduped, got {[x['rep'] for x in smiths]}"
        assert smiths[0]['total_stores'] == 3


# ========================================================================
# Bug #8: weekly is rolling-7-day, should be Mon-Sun
# ========================================================================

class TestWeeklyIsMonSun:
    def test_weekly_returns_mon_sun_window(self, app_module, client):
        """?end=2026-04-23 (a Thursday) should return Mon 2026-04-20 to Sun 2026-04-26."""
        r = client.get('/api/reports/weekly?end=2026-04-23')
        assert r.status_code == 200
        w = r.json['window']
        # Mon-Sun for the week containing Thu 4/23: 4/20 .. 4/26
        assert w['start'] == '2026-04-20', \
            f"BUG: weekly start should be Mon 2026-04-20, got {w['start']}"
        assert w['end'] == '2026-04-26', \
            f"BUG: weekly end should be Sun 2026-04-26, got {w['end']}"

    def test_weekly_rolling_mode_still_works(self, app_module, client):
        """?mode=rolling7 preserves legacy rolling-7 behavior."""
        r = client.get('/api/reports/weekly?end=2026-04-23&mode=rolling7')
        w = r.json['window']
        assert w['start'] == '2026-04-17'  # 23 - 6
        assert w['end'] == '2026-04-23'


# ========================================================================
# Bug #9: window_shifted is silent
# ========================================================================

class TestWindowShiftedFlag:
    def test_future_window_response_flags_shift(self, app_module, client):
        """A request for a future date with no data should auto-shift AND flag it."""
        # No SOD data for the year 2099
        r = client.get('/api/reports/daily?date=2099-01-01')
        assert r.status_code == 200
        w = r.json['window']
        # If there's any SOD data at all, the window will shift
        if w.get('latest_snapshot'):
            assert w['window_shifted'] is True, \
                "BUG: window_shifted should be True when auto-shift occurred"
            assert w['requested_window']['start'] == '2099-01-01', \
                "BUG: requested_window should echo what user asked for"
            assert w['requested_window']['end'] == '2099-01-01'

    def test_in_range_request_does_not_flag_shift(self, app_module, client):
        """If we have data in the requested window, no shift, flag is False."""
        # Seed a snapshot for today
        m = app_module
        conn = m._sod_get_conn()
        cur = conn.cursor()
        first_sku = list(m.SOD_TRACKED_SKUS.keys())[0]
        today = m._toronto_today().isoformat()
        cur.execute(
            "INSERT OR REPLACE INTO sod_inventory (sku, store_number, snapshot_date, status, on_hand, source) "
            "VALUES (?,?,?,?,?,?)",
            (first_sku, 9999, today, 'L', 5, 'daily_a'),
        )
        conn.commit()
        conn.close()

        r = client.get(f'/api/reports/daily?date={today}')
        w = r.json['window']
        assert w['window_shifted'] is False
        assert w['start'] == today and w['end'] == today


# ========================================================================
# Bug #4: orphaned 'running' rows pollute /api/sod/status
# ========================================================================

class TestOrphanedRunningCleanup:
    def test_orphan_cleanup_marks_old_running_as_failed(self, app_module):
        """A running row > 6h old should be marked failed by _cleanup_orphaned_sod_runs."""
        m = app_module
        conn = m._sod_get_conn()
        cur = conn.cursor()
        # Insert a fake stuck row
        cur.execute(
            "INSERT INTO sod_sync_runs (source, status, run_at) VALUES (?,?, datetime('now', '-7 hours'))",
            ('daily_a', 'running'),
        )
        # And a fresh one that should NOT be touched
        cur.execute(
            "INSERT INTO sod_sync_runs (source, status, run_at) VALUES (?,?, datetime('now', '-1 hour'))",
            ('daily_a', 'running'),
        )
        conn.commit()
        conn.close()

        n = m._cleanup_orphaned_sod_runs(max_age_hours=6)
        assert n >= 1, f"BUG: orphan cleanup should mark at least 1 row, marked {n}"

        # Verify status
        conn = m._sod_get_conn()
        cur = conn.cursor()
        cur.execute("SELECT status FROM sod_sync_runs WHERE run_at < datetime('now', '-6 hours') AND source='daily_a'")
        rows = cur.fetchall()
        for row in rows:
            assert row[0] == 'failed', f"Old running row should be failed, got {row[0]}"
        cur.execute("SELECT status FROM sod_sync_runs WHERE run_at > datetime('now', '-3 hours') AND source='daily_a'")
        rows = cur.fetchall()
        for row in rows:
            assert row[0] == 'running', f"Recent running row should still be running, got {row[0]}"
        conn.close()


# ========================================================================
# Bug #3: freshness should be from snapshot_date, not run_at
# ========================================================================

class TestFreshnessFromSnapshotDate:
    def test_freshness_age_days_uses_snapshot(self, app_module):
        """_sod_data_age_days should compare today (Toronto) to MAX(snapshot_date)."""
        m = app_module
        conn = m._sod_get_conn()
        cur = conn.cursor()
        # 5-day-old snapshot
        old_date = (m._toronto_today() - timedelta(days=5)).isoformat()
        first_sku = list(m.SOD_TRACKED_SKUS.keys())[0]
        cur.execute("DELETE FROM sod_inventory")
        cur.execute(
            "INSERT INTO sod_inventory (sku, store_number, snapshot_date, status, on_hand, source) "
            "VALUES (?,?,?,?,?,?)",
            (first_sku, 8000, old_date, 'L', 1, 'daily_a'),
        )
        conn.commit()
        conn.close()

        age = m._sod_data_age_days()
        assert age == 5, f"BUG: data age should be 5 days, got {age}"

        fresh = m._sod_freshness()
        assert fresh['snapshot_age_days'] == 5
        assert fresh['is_stale'] is True, "BUG: is_stale should be True when age > 2 days"

    def test_health_endpoint_503_when_stale(self, app_module, client):
        """/api/sod/health returns 503 when snapshot > 2 days old."""
        # Continues from previous test — 5-day-old snapshot in DB
        r = client.get('/api/sod/health')
        assert r.status_code == 503
        assert r.json['status'] == 'stale'
        assert r.json['snapshot_age_days'] == 5

    def test_healthz_endpoint_consistent(self, app_module, client):
        """/healthz uses the same freshness logic as /api/sod/health."""
        r = client.get('/healthz')
        assert r.status_code == 503
        assert r.json['snapshot_age_days'] == 5


# ========================================================================
# Bug #2: download_option walks back multi-day, validates freshness
# ========================================================================

class TestDownloadWalkback:
    def test_download_walkback_skips_404s_and_picks_fresh(self, app_module, monkeypatch):
        """Mock the LCBO server: today's file 404, yesterday's is fresh.
        Should return yesterday's file, not error out."""
        m = app_module
        client = app_module.SODClient.__new__(app_module.SODClient)
        client.agent_id = '1113'
        client.session = type('S', (), {'get': None})()  # placeholder
        client._logged_in = True
        client.timeout = 10

        from zoneinfo import ZoneInfo
        fixed_today = datetime(2025, 1, 8, 12, 0, 0, tzinfo=ZoneInfo('America/Toronto'))  # WED
        monkeypatch.setattr(client, '_toronto_now', lambda: fixed_today)
        monkeypatch.setattr(client, '_ensure_logged_in', lambda: None)

        # Build a fake fresh zip for yesterday (TUE = 2025-01-07)
        import io, zipfile
        # First 8 bytes of any row in the .dat = YYYYMMDD = "20250107"
        fake_dat = b'20250107' + b'X' * 39 + b'\n'
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('alldlyinventoryTUE.dat', fake_dat)
        fresh_zip = buf.getvalue()

        # Mock session.get: WED -> 404, TUE -> 200 with fresh zip
        def mock_get(url, timeout=None, stream=None):
            class R:
                pass
            r = R()
            r.headers = {'Content-Type': 'application/zip'}
            if 'WED' in url:
                r.status_code = 404
                r.content = b''
                r.raise_for_status = lambda: None
            elif 'TUE' in url:
                r.status_code = 200
                r.content = fresh_zip
                r.raise_for_status = lambda: None
            else:
                r.status_code = 404
                r.content = b''
                r.raise_for_status = lambda: None
            return r
        client.session.get = mock_get

        result = client.download_option('daily_a', max_age_days=3)
        assert len(result) == 3
        zip_bytes, fn, snap = result
        assert fn == 'alldlyinventoryTUE.zip', f"BUG: should fall back to TUE, got {fn}"
        assert snap == '2025-01-07', f"BUG: should peek snapshot date, got {snap}"
