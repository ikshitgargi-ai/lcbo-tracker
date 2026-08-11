"""SOD DURABILITY — the guarantee that no day is ever silently lost.

Ground truth for the filename mapping comes from real production sync runs on
the NB tracker:
    alldlyinventoryMON.zip  (published Mon 2026-08-10) -> snapshot 2026-08-09
    alldlyinventorySAT.zip  (published Sat 2026-08-08) -> snapshot 2026-08-07
    Edlyinventory1113MON.zip                           -> snapshot 2026-08-09

Run: python3 -m pytest tests/test_sod_durability.py -v
"""
import datetime
import os
import sqlite3
import sys
import zlib

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import sod_durability as sd  # noqa: E402

TODAY = datetime.date(2026, 8, 10)   # a Monday


@pytest.fixture
def conn():
    c = sqlite3.connect(':memory:')
    # Mirrors the production columns replay_archive writes, so a schema
    # mismatch shows up here instead of silently restoring nothing.
    c.execute("CREATE TABLE sod_inventory (sku TEXT, store_number INT, "
              "snapshot_date TEXT, status TEXT, on_hand INT, "
              "product_name TEXT, source TEXT, "
              "UNIQUE (sku, store_number, snapshot_date))")
    c.commit()
    sd.ensure_tables(c, use_postgres=False)
    return c


def _seed(conn, dates, sku='0000163'):
    for d in dates:
        conn.execute("INSERT INTO sod_inventory (sku, store_number, snapshot_date, "
                     "status, on_hand) VALUES (?, 100, ?, 'L', 5)", (sku, d))
    conn.commit()


class TestFilenameMapping:
    """The mapping is derived from real files, not guessed."""

    @pytest.mark.parametrize('snapshot,expected', [
        ('2026-08-09', 'alldlyinventoryMON.zip'),
        ('2026-08-07', 'alldlyinventorySAT.zip'),
    ])
    def test_matches_production(self, snapshot, expected):
        assert sd.filename_for('daily_a', snapshot) == expected

    def test_agent_file_matches_production(self):
        assert sd.filename_for('daily_b', '2026-08-09', agent_id='1113') == \
            'Edlyinventory1113MON.zip'

    def test_snapshot_is_published_the_next_day(self):
        assert sd.publish_date_for('2026-08-09') == datetime.date(2026, 8, 10)

    def test_unknown_source_is_refused(self):
        with pytest.raises(ValueError):
            sd.filename_for('weekly_z', '2026-08-09')


class TestTheSevenDayTrap:
    """The whole point of the module. Weekday filenames repeat every 7 days, so
    the SAME name holds DIFFERENT data depending on when you ask. Backfilling an
    out-of-window date would write recent numbers under an old date and corrupt
    billing. The window check is the guard."""

    def test_aug8_is_still_recoverable(self):
        assert sd.is_recoverable('2026-08-08', TODAY)
        assert sd.filename_for('daily_a', '2026-08-08') == 'alldlyinventorySUN.zip'

    def test_aug1_is_gone_even_though_the_filename_still_exists(self):
        # Aug 1 and Aug 8 both map to SUN. That file now holds Aug 8 data.
        # Fetching it for Aug 1 would write Aug 8's numbers under Aug 1.
        assert sd.filename_for('daily_a', '2026-08-01') == \
            sd.filename_for('daily_a', '2026-08-08')
        assert not sd.is_recoverable('2026-08-01', TODAY), \
            'an out-of-window date must never be treated as recoverable'

    def test_the_real_nb_holes_are_classified_correctly(self):
        # The four days NB actually lost, verified live on 2026-08-10.
        assert sd.is_recoverable('2026-08-08', TODAY)
        for gone in ('2026-08-01', '2026-07-24', '2026-07-23'):
            assert not sd.is_recoverable(gone, TODAY)

    def test_window_boundary(self):
        assert sd.is_recoverable(TODAY - datetime.timedelta(days=6), TODAY)
        assert not sd.is_recoverable(TODAY - datetime.timedelta(days=7), TODAY)

    def test_a_future_date_is_never_recoverable(self):
        assert not sd.is_recoverable(TODAY + datetime.timedelta(days=1), TODAY)


class TestGapAnalysis:
    def test_finds_the_hole_and_says_it_is_recoverable(self, conn):
        _seed(conn, ['2026-08-06', '2026-08-07', '2026-08-09'])
        g = sd.analyze_gaps(conn, False, TODAY)
        assert g['recoverable'] == ['2026-08-08']
        assert g['permanent'] == []

    def test_old_hole_is_permanent_not_recoverable(self, conn):
        _seed(conn, ['2026-07-22', '2026-07-25'])
        g = sd.analyze_gaps(conn, False, TODAY)
        # Jul 23/24 are long gone; everything after Jul 25 is also missing but
        # only the last 6 days of it can still be fetched.
        assert '2026-07-23' in g['permanent']
        assert '2026-07-24' in g['permanent']
        assert '2026-08-08' in g['recoverable']
        assert not set(g['recoverable']) & set(g['permanent'])

    def test_today_is_never_a_gap(self, conn):
        # LCBO publishes a day's data the following morning, so "today missing"
        # is normal, not a loss. Reporting it would cry wolf every single day.
        _seed(conn, ['2026-08-08', '2026-08-09'])
        g = sd.analyze_gaps(conn, False, TODAY)
        assert TODAY.isoformat() not in g['recoverable']
        assert TODAY.isoformat() not in g['permanent']
        assert g['last_day'] == '2026-08-09'

    def test_no_data_is_not_a_thousand_gaps(self, conn):
        g = sd.analyze_gaps(conn, False, TODAY)
        assert g == {'held': [], 'recoverable': [], 'permanent': [],
                     'first_day': None, 'last_day': None}

    def test_complete_history_reports_no_gaps(self, conn):
        _seed(conn, [(TODAY - datetime.timedelta(days=i)).isoformat()
                     for i in range(1, 10)])
        g = sd.analyze_gaps(conn, False, TODAY)
        assert g['recoverable'] == [] and g['permanent'] == []


class TestArchiveIsWriteOnce:
    def test_day_is_archived_with_its_rows(self, conn):
        rows = [{'sku': '0000163', 'store_number': 100, 'on_hand': 5, 'status': 'L'}]
        sd.record_day(conn, False, 'daily_a', '2026-08-09',
                      file_name='alldlyinventoryMON.zip', file_bytes=9000,
                      total_rows=1602673, tracked_rows_data=rows)
        r = conn.execute("SELECT file_name, total_rows, tracked_rows, rows_gz "
                         "FROM sod_day_archive WHERE snapshot_date='2026-08-09'"
                         ).fetchone()
        assert r[0] == 'alldlyinventoryMON.zip'
        assert r[1] == 1602673 and r[2] == 1
        import json
        import zlib
        assert json.loads(zlib.decompress(r[3]).decode()) == rows

    def test_a_later_feed_correction_cannot_rewrite_a_billed_day(self, conn):
        """The first capture of a day is the one we keep. If LCBO reissues a
        file with different numbers, we must not silently restate history we
        may already have invoiced against."""
        sd.record_day(conn, False, 'daily_a', '2026-08-09',
                      file_name='original.zip', total_rows=100,
                      tracked_rows_data=[{'sku': 'A', 'on_hand': 5}])
        sd.record_day(conn, False, 'daily_a', '2026-08-09',
                      file_name='CORRECTED.zip', total_rows=999,
                      tracked_rows_data=[{'sku': 'A', 'on_hand': 0}])
        rows = conn.execute("SELECT file_name, total_rows FROM sod_day_archive "
                            "WHERE snapshot_date='2026-08-09'").fetchall()
        assert len(rows) == 1, 'a day must be archived exactly once'
        assert rows[0] == ('original.zip', 100)

    def test_sources_are_archived_independently(self, conn):
        sd.record_day(conn, False, 'daily_a', '2026-08-09', file_name='a.zip')
        sd.record_day(conn, False, 'daily_b', '2026-08-09', file_name='b.zip')
        n = conn.execute("SELECT COUNT(*) FROM sod_day_archive").fetchone()[0]
        assert n == 2

    def test_ensure_tables_is_idempotent(self, conn):
        sd.ensure_tables(conn, use_postgres=False)
        sd.ensure_tables(conn, use_postgres=False)


class TestGapsAreRemembered:
    def test_permanent_gap_recorded_once_and_kept(self, conn):
        sd.record_permanent_gap(conn, False, 'daily_a', '2026-07-23',
                                note='free-tier spin-down')
        sd.record_permanent_gap(conn, False, 'daily_a', '2026-07-23', note='again')
        rows = conn.execute("SELECT snapshot_date, note FROM sod_permanent_gaps"
                            ).fetchall()
        assert rows == [('2026-07-23', 'free-tier spin-down')]

    def test_every_backfill_attempt_is_logged_even_on_failure(self, conn):
        sd.record_backfill_attempt(conn, False, 'daily_a', '2026-08-08',
                                   'alldlyinventorySUN.zip', 'ok', 'ingested')
        sd.record_backfill_attempt(conn, False, 'daily_a', '2026-08-08',
                                   'alldlyinventorySUN.zip', 'date_mismatch',
                                   'file carried 2026-08-15')
        rows = conn.execute("SELECT outcome FROM sod_backfill_attempts "
                            "ORDER BY id").fetchall()
        assert [r[0] for r in rows] == ['ok', 'date_mismatch']


class TestRetentionPromise:
    """The client trackers must answer "day by day, any date to today" for at
    least a year at any moment. These tests exist so that promise cannot be
    quietly broken by a future retention or cleanup change."""

    def test_retention_window_is_at_least_a_year(self):
        assert sd.RETENTION_MIN_DAYS >= 365

    def test_no_code_path_prunes_tracked_sod_rows(self):
        """A DELETE against the SOD tables is how a year of history disappears.
        The client apps must have none. If one is ever added deliberately, it
        has to archive first and this test has to be updated on purpose."""
        import glob
        import os
        here = os.path.dirname(__file__)
        app_py = os.path.join(here, '..', 'app.py')
        src = open(app_py).read().lower()
        for table in ('sod_day_archive', 'sod_permanent_gaps', 'listing_ledger'):
            assert f'delete from {table}' not in src, \
                f'{table} must never be deleted from'
        # sod_inventory may only be deleted by the audited manual rollback,
        # which archives the day first. Anything else is a retention leak.
        assert src.count('delete from sod_inventory') <= 2
        if 'delete from sod_inventory' in src:
            assert 'refusing to delete: could not archive' in src, \
                'a sod_inventory delete exists with no archive-first guard'
        assert glob.glob(os.path.join(here, '..', 'sod_durability.py'))

    def test_a_year_of_days_is_reported_day_by_day(self, conn):
        base = datetime.date(2026, 8, 10)
        for i in range(0, 370):
            _seed(conn, [(base - datetime.timedelta(days=i)).isoformat()])
        out = sd.daily_series(conn, False, ['0000163'],
                              base - datetime.timedelta(days=364), base)
        assert len(out['days']) == 365
        assert all(d['present'] for d in out['days'])
        assert out['days'][0]['date'] == (base - datetime.timedelta(days=364)).isoformat()
        assert out['days'][-1]['date'] == base.isoformat()

    def test_a_hole_stays_visible_in_the_series(self, conn):
        _seed(conn, ['2026-08-06', '2026-08-07', '2026-08-09'])
        out = sd.daily_series(conn, False, ['0000163'], '2026-08-06', '2026-08-09')
        got = {d['date']: d['present'] for d in out['days']}
        assert got == {'2026-08-06': True, '2026-08-07': True,
                       '2026-08-08': False, '2026-08-09': True}, \
            'a missing day must not silently close up'

    def test_series_rolls_up_stores_listed_and_units(self, conn):
        conn.execute("INSERT INTO sod_inventory (sku, store_number, snapshot_date,"
                     " status, on_hand) VALUES ('0000163', 10, '2026-08-09', 'L', 7)")
        conn.execute("INSERT INTO sod_inventory (sku, store_number, snapshot_date,"
                     " status, on_hand) VALUES ('0000163', 11, '2026-08-09', 'D', 2)")
        conn.commit()
        day = sd.daily_series(conn, False, ['0000163'],
                              '2026-08-09', '2026-08-09')['days'][0]
        assert day['per_sku']['0000163'] == {'stores': 2, 'listed_stores': 1, 'units': 9}
        assert day['listed_stores'] == 1 and day['units'] == 9

    def test_untracked_skus_never_enter_the_series(self, conn):
        _seed(conn, ['2026-08-09'], sku='0000163')
        _seed(conn, ['2026-08-09'], sku='9999999')
        day = sd.daily_series(conn, False, ['0000163'],
                              '2026-08-09', '2026-08-09')['days'][0]
        assert list(day['per_sku']) == ['0000163']


class TestTheBackupActuallyRestores:
    """A backup that cannot be restored is not a backup. The emailed payload
    carries the compressed archive rather than the raw inventory table, so the
    whole round trip has to work: blob out, base64 through JSON, blob back in,
    rows replayed."""

    def test_a_day_survives_the_full_backup_round_trip(self, conn):
        rows = [{'sku': '0000163', 'store_number': 217, 'snapshot_date': '2026-08-09',
                 'status': 'L', 'on_hand': 11, 'product_name': 'RED ADMIRAL VODKA'}]
        sd.record_day(conn, False, 'daily_a', '2026-08-09',
                      file_name='alldlyinventoryMON.zip', tracked_rows_data=rows)

        # what the backup writes out
        blob = conn.execute("SELECT rows_gz FROM sod_day_archive").fetchone()[0]
        import base64
        import json as _json
        wire = {'__b64__': base64.b64encode(bytes(blob)).decode('ascii')}
        shipped = _json.loads(_json.dumps(wire))       # survives JSON transport
        restored_blob = base64.b64decode(shipped['__b64__'])
        assert _json.loads(zlib.decompress(restored_blob).decode()) == rows

    def test_replay_puts_the_rows_back(self, conn):
        rows = [{'sku': '0000163', 'store_number': 217, 'snapshot_date': '2026-08-09',
                 'status': 'L', 'on_hand': 11, 'product_name': 'RED ADMIRAL'},
                {'sku': '0000163', 'store_number': 218, 'snapshot_date': '2026-08-09',
                 'status': 'D', 'on_hand': 0, 'product_name': 'RED ADMIRAL'}]
        sd.record_day(conn, False, 'daily_a', '2026-08-09', tracked_rows_data=rows)
        # simulate the day being absent from a freshly restored database
        conn.execute("DELETE FROM sod_inventory WHERE snapshot_date='2026-08-09'")
        conn.commit()

        out = sd.replay_archive(conn, False)
        assert out['rows_restored'] == 2
        assert out['days_restored'] == ['2026-08-09']
        got = conn.execute("SELECT store_number, status, on_hand FROM sod_inventory "
                           "WHERE snapshot_date='2026-08-09' ORDER BY store_number"
                           ).fetchall()
        assert got == [(217, 'L', 11), (218, 'D', 0)]

    def test_replay_never_overwrites_a_day_we_still_hold(self, conn):
        """Replaying must be safe to run at any time. A day already in the
        database is the live record and must win over the archive copy."""
        sd.record_day(conn, False, 'daily_a', '2026-08-09', tracked_rows_data=[
            {'sku': '0000163', 'store_number': 217, 'snapshot_date': '2026-08-09',
             'status': 'L', 'on_hand': 11, 'product_name': 'X'}])
        conn.execute("INSERT INTO sod_inventory (sku, store_number, snapshot_date, "
                     "status, on_hand) VALUES ('0000163', 217, '2026-08-09', 'L', 99)")
        conn.commit()
        sd.replay_archive(conn, False)
        rows = conn.execute("SELECT on_hand FROM sod_inventory WHERE store_number=217 "
                            "AND snapshot_date='2026-08-09'").fetchall()
        assert rows == [(99,)], 'the live row must not be duplicated or overwritten'

    def test_replay_is_idempotent(self, conn):
        sd.record_day(conn, False, 'daily_a', '2026-08-09', tracked_rows_data=[
            {'sku': '0000163', 'store_number': 217, 'snapshot_date': '2026-08-09',
             'status': 'L', 'on_hand': 5, 'product_name': 'X'}])
        conn.execute("DELETE FROM sod_inventory WHERE snapshot_date='2026-08-09'")
        conn.commit()
        first = sd.replay_archive(conn, False)['rows_restored']
        second = sd.replay_archive(conn, False)['rows_restored']
        assert first == 1 and second == 0
        n = conn.execute("SELECT COUNT(*) FROM sod_inventory "
                         "WHERE snapshot_date='2026-08-09'").fetchone()[0]
        assert n == 1

    def test_replay_can_target_one_day(self, conn):
        for d in ('2026-08-08', '2026-08-09'):
            sd.record_day(conn, False, 'daily_a', d, tracked_rows_data=[
                {'sku': '0000163', 'store_number': 1, 'snapshot_date': d,
                 'status': 'L', 'on_hand': 1, 'product_name': 'X'}])
        conn.execute("DELETE FROM sod_inventory")
        conn.commit()
        out = sd.replay_archive(conn, False, snapshot_dates=['2026-08-08'])
        assert out['days_restored'] == ['2026-08-08']

    def test_a_corrupt_blob_does_not_abort_the_whole_replay(self, conn):
        sd.record_day(conn, False, 'daily_a', '2026-08-08', tracked_rows_data=[
            {'sku': '0000163', 'store_number': 1, 'snapshot_date': '2026-08-08',
             'status': 'L', 'on_hand': 1, 'product_name': 'X'}])
        conn.execute("INSERT INTO sod_day_archive (source, snapshot_date, rows_gz) "
                     "VALUES ('daily_a', '2026-08-07', ?)", (b'not-compressed',))
        conn.execute("DELETE FROM sod_inventory")
        conn.commit()
        out = sd.replay_archive(conn, False)
        assert out['days_restored'] == ['2026-08-08'], \
            'one unreadable day must not cost us the readable ones'


class TestChecksum:
    def test_sha256_detects_a_changed_file(self):
        assert sd.sha256_of(b'PK\x03\x04aaa') != sd.sha256_of(b'PK\x03\x04aab')
        assert sd.sha256_of(b'') == ''
