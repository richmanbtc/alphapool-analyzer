import unittest
from unittest.mock import MagicMock, patch, sentinel

import pandas as pd

from src.processing import expand_positions
from src.position_reader import get_position_range, get_position_start


class GetPositionsTests(unittest.TestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.cursor = self.conn.cursor.return_value.__enter__.return_value
        connect = patch('src.position_reader.psycopg2.connect', return_value=self.conn)
        self.connect = connect.start()
        self.addCleanup(connect.stop)

    def read(self):
        return get_position_range(sentinel.database_url, min_timestamp=1700000000,
                                  max_timestamp=1700000300, seed_min_timestamp=1699913600)

    def test_native_mappings_remain_compatible_with_processing(self):
        timestamp = 1700000000
        self.cursor.fetchall.return_value = [
            (timestamp, 'model1', {'BTC': 0.5}, {}),
            (timestamp, 'pf-example', {}, {'model1': 0.7}),
        ]
        result = expand_positions(self.read())
        query, params = self.cursor.execute.call_args.args
        self.assertIn('timestamp < %s', query)
        self.assertEqual(params, (timestamp, timestamp + 300, timestamp, 1699913600))
        t = pd.to_datetime(timestamp, unit='s', utc=True)
        self.assertEqual(result.loc[('model1', t), 'p.BTC'], 0.5)
        self.assertEqual(result.loc[('pf-example', t), 'w.model1'], 0.7)
        self.conn.close.assert_called_once()

    def test_empty_result(self):
        self.cursor.fetchall.return_value = []
        result = expand_positions(self.read())
        self.assertTrue(result.empty)
        self.assertEqual(result.index.names, ['model_id', 'timestamp'])
        self.conn.close.assert_called_once()

    def test_empty_position_or_weight_mapping(self):
        for positions, weights, columns in (({'BTC': 0.5}, {}, ['p.BTC']),
                                             ({}, {'model1': 0.7}, ['w.model1']), ({}, {}, [])):
            with self.subTest(positions=positions, weights=weights):
                self.cursor.fetchall.return_value = [(1700000000, 'example', positions, weights)]
                result = expand_positions(self.read())
                self.assertEqual(result.columns.tolist(), columns)
                self.assertEqual(len(result), 1)

    def test_failures_release_both_reader_connections(self):
        self.cursor.execute.side_effect = RuntimeError('query failed')
        for read in (self.read, lambda: get_position_start(sentinel.database_url)):
            with self.subTest(read=read.__name__):
                self.conn.reset_mock()
                with self.assertRaisesRegex(RuntimeError, 'query failed'):
                    read()
                self.conn.close.assert_called_once()
                self.conn.commit.assert_not_called()


if __name__ == '__main__':
    unittest.main()
