import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from src.analysis import prepare_positions, calc_position_frame
from src.processing import calc_model_ret


class ExpiryTests(unittest.TestCase):
    def setUp(self):
        self.start = pd.Timestamp('2025-01-01', tz='UTC')
        self.expiry = self.start + pd.Timedelta(days=1, minutes=5)

    def raw(self, rows):
        return pd.DataFrame([
            dict(model_id=model, timestamp=self.start + pd.Timedelta(minutes=minutes),
                 positions=positions, weights=weights)
            for model, minutes, positions, weights in rows
        ])

    def output(self, raw, start, end):
        positions = prepare_positions(raw, start, end)
        return calc_position_frame(positions, start, reset_after_gap=True)

    def test_expiry_emits_one_zero_and_restart_diff_is_from_zero(self):
        raw = self.raw([('model', 0, {'A': 0.4}, {}), ('model', 4320, {'A': 0.7}, {})])
        end = self.start + pd.Timedelta(days=3, minutes=5)
        result = self.output(raw, self.start, end).set_index('timestamp')
        self.assertEqual(result.loc[self.start + pd.Timedelta(days=1), 'position'], 0.4)
        self.assertEqual(result.loc[self.expiry, 'position'], 0)
        self.assertAlmostEqual(result.loc[self.expiry, 'position_diff'], -0.4)
        self.assertFalse(((result.index > self.expiry) & (result.index < end - pd.Timedelta(minutes=5))).any())
        self.assertAlmostEqual(result.iloc[-1]['position_diff'], 0.7)

    def test_expired_component_preserves_other_portfolio_contributions(self):
        raw = self.raw([
            ('old', 0, {'A': 1.0}, {}), ('live', 1440, {'A': 0.4}, {}),
            ('portfolio', 1440, {}, {'old': 0.5, 'live': 0.5}),
        ])
        positions = prepare_positions(raw, self.start + pd.Timedelta(days=1), self.expiry + pd.Timedelta(minutes=10))
        for minutes in (0, 5):
            t = self.expiry + pd.Timedelta(minutes=minutes)
            self.assertAlmostEqual(positions.loc[('portfolio', t), 'p.A'], 0.2)
        market = pd.DataFrame({'ret.A': [0.1]}, index=[self.expiry])
        returns = calc_model_ret(positions, market)
        self.assertAlmostEqual(returns.loc[self.expiry, 'portfolio'], 0.02)
        self.assertEqual(returns.loc[self.expiry, 'old'], 0)

    def test_portfolio_remains_zero_after_all_component_seeds_age_out(self):
        raw = self.raw([('portfolio', 1440, {}, {'expired': 1.0})])
        positions = prepare_positions(raw, self.expiry, self.expiry + pd.Timedelta(minutes=5))
        returns = calc_model_ret(positions, pd.DataFrame())
        self.assertEqual(returns.loc[self.expiry, 'portfolio'], 0)

    def test_weight_expiry_zeroes_portfolio_and_weight(self):
        raw = self.raw([
            ('live', 1440, {'A': 1.0}, {}), ('portfolio', 0, {}, {'live': 0.5}),
        ])
        result = self.output(raw, self.start + pd.Timedelta(days=1), self.expiry + pd.Timedelta(minutes=10))
        rows = result.loc[(result.model_id == 'portfolio') & (result.timestamp == self.expiry)].set_index('symbol')
        self.assertEqual(rows.loc['A', 'position'], 0)
        self.assertEqual(rows.loc['live', 'position'], 0)
        self.assertEqual(rows.loc['A', 'position_diff'], -0.5)
        self.assertEqual(rows.loc['live', 'position_diff'], -0.5)
        self.assertFalse(((result.model_id == 'portfolio') & (result.timestamp > self.expiry)).any())

    def test_non_grid_timestamp_expires_at_first_sample_after_24_hours(self):
        raw = self.raw([('model', 1, {'A': 0.4}, {})])
        result = self.output(raw, self.start, self.expiry + pd.Timedelta(minutes=10))
        self.assertEqual(result.iloc[-1]['timestamp'], self.expiry)
        self.assertEqual(result.iloc[-1]['position'], 0)

    def test_split_at_expiry_and_restart_matches_single_interval(self):
        raw = self.raw([('model', 0, {'A': 0.4}, {}), ('model', 4320, {'A': 0.7}, {})])
        end = self.start + pd.Timedelta(days=3, minutes=10)
        expected = self.output(raw, self.start, end)
        for split in (self.expiry, self.expiry + pd.Timedelta(minutes=5), end - pd.Timedelta(minutes=10)):
            actual = pd.concat([self.output(raw.loc[raw.timestamp < split], self.start, split),
                                self.output(raw, split, end)], ignore_index=True)
            assert_frame_equal(actual, expected)


if __name__ == '__main__':
    unittest.main()
