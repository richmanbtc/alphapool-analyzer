import unittest
from unittest.mock import Mock

import numpy as np
import pandas as pd

from src.market import calc_crypto_returns, calc_stock_returns, fetch_market_returns
from src.runner import run_job
from src.analysis import prepare_positions
from src.processing import (
    calc_model_ret, calc_portfolio_positions, concat_market_returns,
    expand_positions,
)


class MissingDataTests(unittest.TestCase):
    def setUp(self):
        self.now = pd.Timestamp('2025-01-03', tz='UTC')

    def positions(self, rows):
        return pd.DataFrame([
            dict(model_id=model, timestamp=self.now, delay=0,
                 positions=positions, weights=weights)
            for model, positions, weights in rows
        ], columns=['model_id', 'timestamp', 'positions', 'weights'])

    def test_missing_prices_only_affect_exposed_models(self):
        frame = expand_positions(self.positions([
            ('valid', {'A': 1.0}, {}),
            ('missing', {'B': 1.0}, {}),
            ('flat', {'A': 0.0, 'B': 0.0}, {}),
        ])).fillna(0)
        frame['ret.A'] = 0.1
        result = calc_model_ret(frame, frame.filter(regex=r'^ret\.').groupby(level='timestamp').first())
        self.assertAlmostEqual(result.loc[self.now, 'valid'], 0.1)
        self.assertTrue(pd.isna(result.loc[self.now, 'missing']))
        self.assertEqual(result.loc[self.now, 'flat'], 0)

    def test_missing_price_only_removes_the_affected_timestamp(self):
        positions = self.positions([('model', {'A': 1.0}, {})])
        later = positions.copy()
        later['timestamp'] += pd.Timedelta(minutes=5)
        frame = expand_positions(pd.concat([positions, later], ignore_index=True))
        frame['ret.A'] = [np.nan, 0.1]
        result = calc_model_ret(frame, frame.filter(regex=r'^ret\.').groupby(level='timestamp').first())
        self.assertTrue(pd.isna(result.loc[self.now, 'model']))
        self.assertAlmostEqual(result.iloc[1]['model'], 0.1)

    def test_invalid_returns_and_positions_are_missing(self):
        for invalid in (np.nan, np.inf, -np.inf, 'invalid'):
            for field in ('p.A', 'ret.A'):
                with self.subTest(invalid=invalid, field=field):
                    frame = expand_positions(self.positions([('model', {'A': 1.0}, {})]))
                    frame['ret.A'] = 0.1
                    frame[field] = invalid
                    self.assertTrue(calc_model_ret(frame, frame.filter(regex=r'^ret\.').groupby(level='timestamp').first()).isna().all().all())

    def test_missing_portfolio_reference_does_not_affect_other_models(self):
        frame = expand_positions(self.positions([
            ('valid', {'A': 1.0}, {}),
            ('portfolio', {}, {'absent': 1.0}),
        ])).fillna(0)
        frame = calc_portfolio_positions(frame)
        frame['ret.A'] = 0.1
        result = calc_model_ret(frame, frame.filter(regex=r'^ret\.').groupby(level='timestamp').first())
        self.assertAlmostEqual(result.loc[self.now, 'valid'], 0.1)
        self.assertEqual(result.loc[self.now, 'portfolio'], 0)

    def test_zero_weight_does_not_propagate_invalid_positions(self):
        frame = expand_positions(self.positions([
            ('valid', {'A': 1.0}, {}),
            ('invalid', {'A': 0.0}, {}),
            ('portfolio', {}, {'invalid': 1.0}),
        ])).fillna(0)
        frame.loc[('invalid', self.now), 'p.A'] = np.nan
        result = calc_portfolio_positions(frame)
        self.assertEqual(result.loc[('valid', self.now), 'p.A'], 1)
        self.assertTrue(pd.isna(result.loc[('portfolio', self.now), 'p.A']))

    def test_malformed_position_does_not_hide_valid_models(self):
        frame = prepare_positions(self.positions([
            ('invalid', {'A': 'invalid'}, {}),
            ('valid', {'A': 1.0}, {}),
        ]), self.now, self.now + pd.Timedelta(minutes=5))
        frame['ret.A'] = 0.1
        result = calc_model_ret(frame, frame.filter(regex=r'^ret\.').groupby(level='timestamp').first())
        self.assertAlmostEqual(result.loc[self.now, 'valid'], 0.1)
        self.assertTrue(pd.isna(result.loc[self.now, 'invalid']))

    def test_invalid_identifiers_and_json_shapes_do_not_hide_valid_models(self):
        for stock in (False, True):
            for field in ('positions', 'weights'):
                for value in ([{'A': 1.0}], 'invalid', 123):
                    with self.subTest(stock=stock, field=field, value=value):
                        raw = self.positions([
                            ('valid', {'A': 1.0}, {}), ('invalid', {'A': 1.0}, {}),
                            (None, {'A': 1.0}, {}), ('', {'A': 1.0}, {}),
                        ])
                        raw.at[1, field] = value
                        market = pd.DataFrame({'ret.A': [0.1]}, index=[self.now])
                        writer = self.run_job(stock, raw, market)
                        positions, returns = writer.write.call_args.args[:2]
                        self.assertNotIn('invalid', positions.model_id.to_list())
                        current = returns.loc[returns.timestamp == self.now].set_index('model_id')
                        self.assertAlmostEqual(current.loc['valid', 'ret'], 0.1)
                        self.assertNotIn('invalid', current.index)

    def test_malformed_snapshot_stops_carry_forward_until_valid_update(self):
        raw = pd.DataFrame([
            dict(model_id='model', timestamp=self.now + pd.Timedelta(minutes=minutes),
                 positions=positions, weights={})
            for minutes, positions in [(0, {'A': 1.0}), (5, ['invalid']), (15, {'A': 0.5})]
        ])
        frame = prepare_positions(raw, self.now, self.now + pd.Timedelta(minutes=20))
        times = frame.index.get_level_values('timestamp')
        self.assertEqual(times.tolist(), [self.now, self.now + pd.Timedelta(minutes=15)])
        self.assertEqual(frame['p.A'].to_list(), [1.0, 0.5])

    def test_all_malformed_inputs_produce_empty_outputs_and_advance(self):
        for stock in (False, True):
            raw = self.positions([(None, {'A': 1.0}, {}), ('invalid', ['invalid'], {})])
            writer = self.run_job(stock, raw, pd.DataFrame())
            writer.write.assert_called_once()
            self.assertTrue(writer.write.call_args.args[0].empty)
            self.assertTrue(writer.write.call_args.args[1].empty)

    def test_empty_positions_and_absent_position_columns(self):
        frame = prepare_positions(self.positions([]), self.now, self.now + pd.Timedelta(minutes=5))
        self.assertTrue(frame.empty)
        frame = expand_positions(self.positions([('model', {}, {})]))
        self.assertTrue(calc_model_ret(frame, frame.filter(regex=r'^ret\.').groupby(level='timestamp').first()).eq(0).all().all())

    def test_empty_market_and_empty_symbol_list(self):
        for stock in (False, True):
            with self.subTest(stock=stock):
                fetcher = Mock()
                self.assertTrue(fetch_market_returns([], 0, stock=stock, fetcher=fetcher).empty)
                fetcher.fetch.assert_not_called()
                fetcher.fetch.return_value = [pd.DataFrame()]
                result = fetch_market_returns(['A'], 0, stock=stock, fetcher=fetcher)
                self.assertTrue(result.empty)
                self.assertEqual(result.index.name, 'timestamp')

    def test_market_missing_values_are_not_zero_filled(self):
        market = pd.DataFrame([
            dict(symbol=symbol, timestamp=self.now + pd.Timedelta(days=day),
                 cl=price, op=price, mo_cl=price, af_op=price, adj_factor=1.,
                 unused=None)
            for symbol, price in [('A', 100.), ('B', np.nan)]
            for day in (0, 1)
        ])
        for calculate in (calc_crypto_returns, calc_stock_returns):
            with self.subTest(calculate=calculate.__name__):
                result = calculate(market)
                self.assertEqual(result.loc[self.now, 'ret.A'], 0)
                self.assertTrue(result['ret.B'].isna().all())

    def test_malformed_market_price_does_not_crash_other_symbols(self):
        market = pd.DataFrame([
            dict(symbol=symbol, timestamp=self.now + pd.Timedelta(days=day),
                 cl=price, op=price, mo_cl=price, af_op=price, adj_factor=1.)
            for symbol, price in [('A', 100.), ('B', 'invalid')]
            for day in (0, 1)
        ])
        for calculate in (calc_crypto_returns, calc_stock_returns):
            with self.subTest(calculate=calculate.__name__):
                result = calculate(market)
                self.assertEqual(result.loc[self.now, 'ret.A'], 0)
                self.assertTrue(result['ret.B'].isna().all())

    def run_job(self, stock, positions, market):
        writer = Mock()
        run_job(
            start_time=self.now - pd.Timedelta(days=1), end_time=self.now + pd.Timedelta(minutes=5),
            execution_time=self.now, read_positions=Mock(return_value=positions),
            read_market=Mock(return_value=market), write_results=writer.write,
            logger=Mock(), stock=stock,
        )
        return writer

    def test_empty_intervals_still_commit_progress(self):
        stale = self.positions([('model', {'A': 1.0}, {})])
        stale['timestamp'] -= pd.Timedelta(days=60)
        for stock in (False, True):
            for positions in (self.positions([]), self.positions([('model', {}, {})]), stale):
                with self.subTest(stock=stock, rows=len(positions)):
                    writer = self.run_job(stock, positions, concat_market_returns([]))
                    writer.write.assert_called_once()
                    self.assertTrue(writer.write.call_args.args[0].empty)
                    self.assertTrue(writer.write.call_args.args[1].empty)

    def test_jobs_save_valid_returns_and_omit_invalid_returns(self):
        positions = self.positions([
            ('valid', {'A': 1.0}, {}),
            ('missing', {'B': 1.0}, {}),
        ])
        market = pd.DataFrame({'ret.A': [0.1]}, index=pd.DatetimeIndex([self.now], name='timestamp'))
        for stock in (False, True):
            with self.subTest(stock=stock):
                writer = self.run_job(stock, positions, market)
                rows = writer.write.call_args.args[1]
                current = rows.loc[rows['timestamp'] == self.now].set_index('model_id')['ret'].to_dict()
                self.assertAlmostEqual(current['valid'], 0.1)
                self.assertNotIn('missing', current)

    def test_jobs_with_missing_market_save_no_exposed_returns(self):
        positions = self.positions([('model', {'A': 1.0}, {})])
        for stock in (False, True):
            with self.subTest(stock=stock):
                writer = self.run_job(stock, positions, concat_market_returns([]))
                rows = writer.write.call_args.args[1]
                self.assertFalse(rows['timestamp'].eq(self.now).any())

    def test_flat_models_have_zero_returns_without_prices(self):
        positions = self.positions([('flat', {'A': 0.0}, {})])
        for stock in (False, True):
            with self.subTest(stock=stock):
                writer = self.run_job(stock, positions, concat_market_returns([]))
                rows = writer.write.call_args.args[1]
                self.assertFalse(rows.empty)
                self.assertTrue(rows['ret'].eq(0).all())


if __name__ == '__main__':
    unittest.main()
