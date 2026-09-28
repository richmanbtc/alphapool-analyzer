import io
import unittest

import numpy as np
import pandas as pd
import pyarrow.parquet as parquet
from google.cloud.bigquery import _pandas_helpers
from pandas.testing import assert_frame_equal

from src.analysis import calc_position_frame, calc_return_frame
from src.result_writer import SCHEMAS
from src.processing import calc_portfolio_positions


class OutputFrameTests(unittest.TestCase):
    def setUp(self):
        self.now = pd.Timestamp('2025-01-03', tz='UTC')
        self.before = self.now - pd.Timedelta(minutes=5)

    def test_positions_preserve_values_order_and_invalid_differences(self):
        index = pd.MultiIndex.from_tuples([
            ('first', self.before), ('first', self.now), ('second', self.now),
        ], names=['model_id', 'timestamp'])
        frame = pd.DataFrame({'p.A': [np.inf, 0.5, np.nan],
                              'w.first': [0.25, 0.75, 0.0], 'p.zero': [0., 0., 0.]}, index=index)
        original = frame.copy()
        result = calc_position_frame(frame, self.now)
        self.assertEqual(result['model_id'].tolist(), ['first', 'first', 'second', 'first', 'second'])
        self.assertEqual(result['symbol'].tolist(), ['A', 'first', 'first', 'zero', 'zero'])
        self.assertEqual(result['position'].tolist(), [0.5, 0.75, 0.0, 0.0, 0.0])
        self.assertTrue(pd.isna(result['position_diff'].iloc[0]))
        self.assertEqual(result['position_diff'].iloc[1:].tolist(), [0.5, 0.0, 0.0, 0.0])
        self.assertTrue(result['timestamp'].eq(self.now).all())
        assert_frame_equal(frame, original)

    def test_duplicate_symbol_labels_preserve_rows(self):
        index = pd.MultiIndex.from_tuples([('model', self.now)], names=['model_id', 'timestamp'])
        result = calc_position_frame(pd.DataFrame({'p.A': [0.5], 'w.A': [0.25]}, index=index), self.now)
        self.assertEqual(result['symbol'].tolist(), ['A', 'A'])
        self.assertEqual(result['position'].tolist(), [0.5, 0.25])

    def test_identifiers_preserve_embedded_prefixes(self):
        index = pd.MultiIndex.from_tuples([('model.w.ret.name', self.now), ('portfolio', self.now)],
                                         names=['model_id', 'timestamp'])
        frame = pd.DataFrame({'p.asset.p.w.name': [0.5, 0.0], 'w.model.w.ret.name': [0.0, 0.4]}, index=index)
        expanded = calc_portfolio_positions(frame)
        self.assertAlmostEqual(expanded.loc[('portfolio', self.now), 'p.asset.p.w.name'], 0.2)
        positions = calc_position_frame(expanded, self.now)
        self.assertEqual(set(positions.symbol), {'asset.p.w.name', 'model.w.ret.name'})
        returns = calc_return_frame(pd.DataFrame({'ret.model.ret.name': [0.1]},
                                    index=pd.DatetimeIndex([self.now], name='timestamp')), self.now)
        self.assertEqual(returns.model_id.tolist(), ['ret.model.ret.name'])

    def test_outputs_round_trip_through_bigquery_parquet(self):
        index = pd.MultiIndex.from_tuples([('model', self.now)], names=['model_id', 'timestamp'])
        positions = calc_position_frame(pd.DataFrame({'p.A': [0.5]}, index=index), self.now)
        returns = calc_return_frame(pd.DataFrame({'model': [0.1]},
                                    index=pd.DatetimeIndex([self.now], name='timestamp')), self.now)
        for name, frame in (('analyzer_positions', positions), ('analyzer_rets', returns)):
            with self.subTest(name=name):
                output = io.BytesIO()
                _pandas_helpers.dataframe_to_parquet(frame, SCHEMAS[name], output)
                table = parquet.read_table(io.BytesIO(output.getvalue()))
                actual = table.to_pandas()[frame.columns]
                expected = frame.copy()
                for column in ('model_id', 'symbol'):
                    if column in expected:
                        self.assertEqual(str(table.schema.field(column).type), 'string')
                assert_frame_equal(actual, expected, check_dtype=False)


if __name__ == '__main__':
    unittest.main()
