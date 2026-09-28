from contextlib import closing
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock
from functools import partial

import pandas as pd
import psycopg2
from psycopg2.extensions import make_dsn
from psycopg2.extras import Json

from scripts._postgres import temporary_database
from src.runner import run_job
from src.processing import calc_model_ret, expand_positions
from src.analysis import prepare_positions
from src.position_reader import get_position_range, get_position_start


class AnalyzerPostgresTests(TestCase):
    @classmethod
    def setUpClass(cls):
        env = cls.enterClassContext(temporary_database())
        cls.database = env['PGDATABASE']
        cls.admin = psycopg2.connect()
        cls.admin.autocommit = True
        cls.addClassCleanup(cls.admin.close)
        cls.dsn = make_dsn(dbname=cls.database)
        with closing(psycopg2.connect(cls.dsn)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(Path(__file__).with_name('0001_initial.sql').read_text())

    def setUp(self):
        self.conn = psycopg2.connect(self.dsn)
        self.addCleanup(self.conn.close)
        with self.conn.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE positions RESTART IDENTITY')
        self.conn.commit()
        self.timestamp = int(pd.Timestamp.now(tz='UTC').floor('5min').timestamp())
        self.now = pd.to_datetime(self.timestamp, unit='s', utc=True)

    def submit(self, timestamp, model, positions=None, weights=None):
        with self.conn.cursor() as cursor:
            cursor.execute(
                'INSERT INTO positions (timestamp, model_id, positions, weights) VALUES (%s, %s, %s, %s)',
                (timestamp, model, Json(positions or {}), Json(weights or {})),
            )

    def read_positions(self):
        return get_position_range(self.dsn, min_timestamp=self.timestamp,
                                  max_timestamp=self.timestamp + 300, seed_min_timestamp=self.timestamp)

    def assert_reader_closed(self):
        with self.admin.cursor() as cursor:
            cursor.execute(
                'SELECT count(*) FROM pg_stat_activity WHERE datname = %s AND pid <> %s',
                (self.database, self.conn.get_backend_pid()),
            )
            self.assertEqual(cursor.fetchone()[0], 0)

    def test_native_json_positions_weights_and_timestamp_filter(self):
        self.submit(self.timestamp - 1, 'old', positions={'BTC': 1.0})
        self.submit(self.timestamp, 'model', positions={'BTC': 0.5})
        self.submit(self.timestamp, 'pf-model', weights={'model': 0.7})
        self.conn.commit()

        result = expand_positions(self.read_positions())

        self.assertEqual(set(result.index.get_level_values('model_id')), {'model', 'pf-model'})
        self.assertEqual(result.loc[('model', self.now), 'p.BTC'], 0.5)
        self.assertEqual(result.loc[('pf-model', self.now), 'w.model'], 0.7)
        self.assert_reader_closed()

    def test_empty_database_remains_empty_after_preprocessing(self):
        result = prepare_positions(self.read_positions(), self.now, self.now + pd.Timedelta(minutes=5))
        self.assertTrue(result.empty)
        self.assert_reader_closed()

    def test_reader_only_sees_committed_positions(self):
        self.submit(self.timestamp, 'model', positions={'BTC': 0.5})
        self.assertTrue(self.read_positions().empty)
        self.conn.commit()
        self.assertEqual(len(self.read_positions()), 1)
        self.assert_reader_closed()

    def test_query_failure_closes_connection(self):
        with self.conn.cursor() as cursor:
            cursor.execute('ALTER TABLE positions RENAME TO hidden_positions')
        self.conn.commit()
        try:
            with self.assertRaises(psycopg2.errors.UndefinedTable):
                self.read_positions()
            self.assert_reader_closed()
        finally:
            with self.conn.cursor() as cursor:
                cursor.execute('ALTER TABLE hidden_positions RENAME TO positions')
            self.conn.commit()

    def test_missing_xrp_price_does_not_hide_other_models(self):
        self.submit(self.timestamp, 'valid', positions={'BTC': 0.5})
        self.submit(self.timestamp, 'missing', positions={'XRP': 1.0})
        self.submit(self.timestamp, 'flat', positions={'XRP': 0.0})
        self.conn.commit()
        market = pd.DataFrame({'ret.BTC': [0.1]}, index=pd.DatetimeIndex([self.now], name='timestamp'))

        for stock in (False, True):
            with self.subTest(stock=stock):
                writer = Mock()
                run_job(
                    start_time=self.now - pd.Timedelta(days=1), end_time=self.now + pd.Timedelta(minutes=5),
                    execution_time=self.now,
                    read_positions=partial(get_position_range, self.dsn),
                    read_market=Mock(return_value=market), write_results=writer,
                    logger=Mock(), stock=stock,
                )
                rows = writer.call_args.args[1]
                current = rows.loc[rows['timestamp'] == self.now].set_index('model_id')['ret'].to_dict()
                self.assertAlmostEqual(current['valid'], 0.05)
                self.assertEqual(current['flat'], 0)
                self.assertNotIn('missing', current)
                self.assert_reader_closed()

    def test_initial_origin_and_bounded_read_include_each_models_predecessor(self):
        self.assertIsNone(get_position_start(self.dsn))
        for seconds, model, value in ((-900, 'first', 0.1), (-600, 'first', 0.2),
                                       (-300, 'second', 0.3), (0, 'first', 0.4),
                                       (300, 'first', 0.5)):
            self.submit(self.timestamp + seconds, model, positions={'BTC': value})
        self.conn.commit()
        self.assertEqual(get_position_start(self.dsn), self.now - pd.Timedelta(minutes=15))
        rows = get_position_range(self.dsn, min_timestamp=self.timestamp,
                                  max_timestamp=self.timestamp + 300)
        self.assertEqual(sorted(rows['timestamp'].tolist()), [
            self.now - pd.Timedelta(minutes=10), self.now - pd.Timedelta(minutes=5), self.now,
        ])
        self.assertEqual(rows.loc[rows['timestamp'] == self.now, 'positions'].iloc[0], {'BTC': 0.4})
        bounded = get_position_range(self.dsn, min_timestamp=self.timestamp,
                                     max_timestamp=self.timestamp + 300,
                                     seed_min_timestamp=self.timestamp - 300)
        self.assertEqual(sorted(bounded['timestamp'].tolist()), [
            self.now - pd.Timedelta(minutes=5), self.now,
        ])
        self.assert_reader_closed()

    def test_invalid_numeric_json_does_not_hide_valid_model(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                'INSERT INTO positions (timestamp, model_id, positions, weights) '
                'VALUES (%s, %s, %s, %s)',
                (self.timestamp, 'invalid', '{"BTC": "invalid"}', '{}'),
            )
        self.submit(self.timestamp, 'valid', positions={'BTC': 0.5})
        self.conn.commit()

        frame = prepare_positions(self.read_positions(), self.now, self.now + pd.Timedelta(minutes=5))
        frame['ret.BTC'] = 0.1
        result = calc_model_ret(frame, frame.filter(regex=r'^ret\.').groupby(level='timestamp').first())
        self.assertAlmostEqual(result.loc[self.now, 'valid'], 0.05)
        self.assertTrue(pd.isna(result.loc[self.now, 'invalid']))
        self.assert_reader_closed()
