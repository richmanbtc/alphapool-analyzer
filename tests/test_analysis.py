import unittest
from unittest.mock import Mock, patch, sentinel

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from src import main, main_stock
from src.analysis import (
    add_equal_weighted_portfolio, normalize_stock_positions, calc_position_frame,
    prepare_positions, calc_required_symbols, calc_return_frame,
)
from src.market import calc_crypto_returns, calc_stock_returns, fetch_market_returns
from src.runner import run_job
from src.processing import calc_model_ret


class AnalysisTests(unittest.TestCase):
    def setUp(self):
        self.now = pd.Timestamp('2025-01-03', tz='UTC')

    def frame(self, records):
        return pd.DataFrame(records).set_index(['model_id', 'timestamp']).sort_index()

    def raw_positions(self, timestamp=None):
        return pd.DataFrame([dict(model_id='model', timestamp=self.now if timestamp is None else timestamp,
                                  positions={'A': 0.5}, weights={})])

    def test_crypto_prices_are_sorted_and_deduplicated_without_mutation(self):
        times = pd.date_range(self.now, periods=3, freq='5min')
        market = pd.DataFrame([
            dict(symbol='AUSDT', timestamp=times[2], cl=121.),
            dict(symbol='AUSDT', timestamp=times[1], cl=105.),
            dict(symbol='AUSDT', timestamp=times[0], cl=100.),
            dict(symbol='AUSDT', timestamp=times[1], cl=110.),
        ])
        original = market.copy(deep=True)
        result = calc_crypto_returns(market)
        self.assertEqual(result.columns.tolist(), ['ret.A'])
        self.assertAlmostEqual(result.loc[times[0], 'ret.A'], 0.1)
        self.assertAlmostEqual(result.loc[times[1], 'ret.A'], 0.1)
        self.assertTrue(pd.isna(result.loc[times[2], 'ret.A']))
        assert_frame_equal(market, original)

    def test_stock_session_returns_and_split_adjusted_overnight_return(self):
        market = pd.DataFrame([
            dict(symbol='A', timestamp=self.now + pd.Timedelta(days=1),
                 op=66.55, mo_cl=66.55, af_op=66.55, cl=66.55, adj_factor=0.5),
            dict(symbol='A', timestamp=self.now,
                 op=100., mo_cl=110., af_op=121., cl=133.1, adj_factor=1.),
        ])
        original = market.copy(deep=True)
        result = calc_stock_returns(market)
        for minutes in (0, 150, 210):
            self.assertAlmostEqual(result.loc[self.now + pd.Timedelta(minutes=minutes), 'ret.A'], 0.1)
        self.assertAlmostEqual(result.loc[self.now + pd.Timedelta(hours=6), 'ret.A'], 0)
        self.assertTrue(pd.isna(result.iloc[-1]['ret.A']))
        assert_frame_equal(market, original)

    def test_stock_duplicates_are_removed_before_split_adjustment(self):
        tomorrow = self.now + pd.Timedelta(days=1)
        market = pd.DataFrame([
            dict(symbol='A', timestamp=tomorrow, op=40., mo_cl=40., af_op=40., cl=40., adj_factor=0.5),
            dict(symbol='A', timestamp=self.now, op=100., mo_cl=100., af_op=100., cl=100., adj_factor=1.),
            dict(symbol='A', timestamp=tomorrow, op=50., mo_cl=50., af_op=50., cl=50., adj_factor=0.5),
        ])
        original = market.copy(deep=True)
        result = calc_stock_returns(market)
        self.assertAlmostEqual(result.loc[self.now + pd.Timedelta(hours=6), 'ret.A'], 0)
        assert_frame_equal(result, calc_stock_returns(market.iloc[1:]))
        assert_frame_equal(market, original)

    def test_crypto_removes_only_quote_currency_suffix(self):
        market = pd.DataFrame({'symbol': ['USDTXUSDT', 'USDTXUSDT'],
                               'timestamp': [self.now, self.now + pd.Timedelta(minutes=5)],
                               'cl': [100., 110.]})
        result = calc_crypto_returns(market)
        self.assertEqual(result.columns.tolist(), ['ret.USDTX'])
        self.assertAlmostEqual(result.iloc[0, 0], 0.1)

    def test_market_adapter_selects_source_and_symbols(self):
        for stock in (False, True):
            with self.subTest(stock=stock):
                fetcher = Mock()
                fetcher.fetch.return_value = [pd.DataFrame()]
                fetch_market_returns(['A'], sentinel.timestamp, stock=stock, fetcher=fetcher)
                args = fetcher.fetch.call_args.kwargs
                options = args['provider_configs'][0]['options']
                self.assertEqual(options['table'], 'jq_ohlcv' if stock else 'binance_ohlcv_5m')
                self.assertEqual(options['symbols'], ['A'] if stock else ['AUSDT'])
                self.assertIs(args['min_timestamp'], sentinel.timestamp)

    def test_stock_time_fill_preserves_existing_positions(self):
        frame = self.frame([
            dict(model_id='model', timestamp=self.now, **{'p.A': 1.}),
            dict(model_id='model', timestamp=self.now + pd.Timedelta(minutes=150), **{'p.A': 9.}),
        ])
        original = frame.copy(deep=True)
        result = normalize_stock_positions(frame)
        self.assertEqual(len(result), 3)
        self.assertEqual(result.loc[('model', self.now + pd.Timedelta(minutes=150)), 'p.A'], 9.)
        self.assertEqual(result.loc[('model', self.now + pd.Timedelta(minutes=210)), 'p.A'], 9.)
        assert_frame_equal(frame, original)

    def test_stock_time_fill_adds_both_sessions(self):
        frame = self.frame([dict(model_id='model', timestamp=self.now, **{'p.A': 0.5})])
        result = normalize_stock_positions(frame)
        self.assertEqual(result.index.get_level_values('timestamp').tolist(),
                         [self.now + pd.Timedelta(minutes=m) for m in (0, 150, 210)])
        self.assertTrue(result['p.A'].eq(0.5).all())

    def test_equal_weighted_portfolio_averages_positions_only(self):
        frame = self.frame([
            dict(model_id='first', timestamp=self.now, **{'p.A': 1., 'w.first': 0.5}),
            dict(model_id='second', timestamp=self.now, **{'p.A': -0.5, 'w.first': 0.0}),
        ])
        original = frame.copy(deep=True)
        result = add_equal_weighted_portfolio(frame)
        self.assertEqual(result.loc[('pf-equal', self.now), 'p.A'], 0.25)
        self.assertTrue(pd.isna(result.loc[('pf-equal', self.now), 'w.first']))
        assert_frame_equal(frame, original)

    def test_preparation_evaluates_crypto_activity_at_historical_times(self):
        first = self.now - pd.Timedelta(days=3)
        raw = self.raw_positions(first)
        crypto = prepare_positions(raw, first, self.now)
        times = crypto.index.get_level_values('timestamp')
        self.assertEqual(times.min(), first)
        self.assertEqual(times.max(), first + pd.Timedelta(days=1, minutes=5))
        self.assertEqual(crypto.iloc[-1]['p.A'], 0)
        stocks = prepare_positions(raw, first, self.now, stock=True)
        self.assertEqual(set(stocks.index.get_level_values('model_id')), {'model', 'pf-equal'})

    def test_position_frame_use_history_before_window_for_difference(self):
        frame = self.frame([
            dict(model_id='model', timestamp=self.now - pd.Timedelta(minutes=5), **{'p.A': 0.2}),
            dict(model_id='model', timestamp=self.now, **{'p.A': 0.5}),
            dict(model_id='other', timestamp=self.now, **{'p.A': 0.8}),
            dict(model_id='invalid', timestamp=self.now, **{'p.A': np.nan}),
        ])
        rows = calc_position_frame(frame, self.now)
        self.assertEqual(len(rows), 2)
        rows = rows.set_index('model_id').to_dict('index')
        self.assertAlmostEqual(rows['model']['position_diff'], 0.3)
        self.assertEqual(rows['other']['position_diff'], 0)
        self.assertEqual(rows['model']['position'], 0.5)
        self.assertEqual(rows['model']['symbol'], 'A')
        self.assertEqual(rows['model']['timestamp'], self.now)

    def test_required_symbols_exclude_weights_and_zero_exposure(self):
        frame = self.frame([dict(model_id='model', timestamp=self.now,
                                **{'p.A': 0., 'p.B': -0.5, 'p.missing': np.nan, 'w.other': 1.})])
        self.assertEqual(calc_required_symbols(frame), ['B'])

    def test_return_frame_drop_missing_values_per_model_and_filter_window(self):
        returns = pd.DataFrame({'first': [0.1, 0.2], 'second': [0.3, np.nan]},
                               index=pd.DatetimeIndex([self.now - pd.Timedelta(minutes=5), self.now], name='timestamp'))
        self.assertEqual(calc_return_frame(returns, self.now).to_dict('records'),
                         [dict(timestamp=self.now, ret=0.2, model_id='first')])

    def test_runner_reads_and_writes_the_explicit_interval(self):
        market = pd.DataFrame({'ret.A': [0.1]}, index=pd.DatetimeIndex([self.now], name='timestamp'))
        for stock, days in ((False, 7), (True, 56)):
            with self.subTest(stock=stock):
                read_positions = Mock(return_value=self.raw_positions())
                read_market = Mock(return_value=market)
                write = Mock()
                run_job(start_time=self.now - pd.Timedelta(days=1), end_time=self.now + pd.Timedelta(minutes=5),
                        execution_time=self.now,
                        read_positions=read_positions, read_market=read_market,
                        write_results=write, logger=Mock(), stock=stock)
                earliest = (self.now - pd.Timedelta(days=1, minutes=0 if stock else 5)).timestamp()
                latest = (self.now + pd.Timedelta(minutes=5)).timestamp()
                seed_options = {} if stock else {'seed_min_timestamp': earliest - pd.Timedelta(days=1).total_seconds()}
                read_positions.assert_called_once_with(min_timestamp=earliest, max_timestamp=latest, **seed_options)
                read_market.assert_called_once_with(symbols=['A'], min_timestamp=earliest, max_timestamp=latest)
                self.assertEqual(write.call_args.args[2], self.now - pd.Timedelta(days=1))
                self.assertEqual(write.call_args.kwargs['mode'], 'stock' if stock else 'crypto')
                rows = write.call_args.args[1]
                row = rows.loc[(rows['model_id'] == 'model') & (rows['timestamp'] == self.now)].iloc[0]
                self.assertAlmostEqual(row['ret'], 0.05)

    def test_empty_interval_advances_without_fetching_market(self):
        read_market, write = Mock(), Mock()
        run_job(start_time=self.now, end_time=self.now + pd.Timedelta(minutes=5), execution_time=self.now, read_positions=Mock(return_value=self.raw_positions().iloc[:0]),
                read_market=read_market, write_results=write, logger=Mock())
        read_market.assert_not_called()
        write.assert_called_once()
        self.assertTrue(write.call_args.args[0].empty)
        self.assertTrue(write.call_args.args[1].empty)

    def test_returns_align_market_times_for_each_model_without_mutation(self):
        later = self.now + pd.Timedelta(minutes=5)
        missing = later + pd.Timedelta(minutes=5)
        positions = self.frame([
            dict(model_id='first', timestamp=self.now, **{'p.A': 2., 'p.B': 0.}),
            dict(model_id='first', timestamp=later, **{'p.A': 1., 'p.B': 1.}),
            dict(model_id='second', timestamp=self.now, **{'p.A': 0., 'p.B': 1.}),
            dict(model_id='second', timestamp=later, **{'p.A': 0., 'p.B': 1.}),
            dict(model_id='second', timestamp=missing, **{'p.A': 0., 'p.B': 0.}),
        ])
        market = pd.DataFrame({'ret.A': [0.2, 0.1], 'ret.B': [-0.1, np.nan]},
                              index=pd.DatetimeIndex([later, self.now], name='timestamp'))
        original_positions, original_market = positions.copy(), market.copy()
        result = calc_model_ret(positions, market)
        self.assertAlmostEqual(result.loc[self.now, 'first'], 0.2)
        self.assertAlmostEqual(result.loc[later, 'first'], 0.1)
        self.assertTrue(pd.isna(result.loc[self.now, 'second']))
        self.assertAlmostEqual(result.loc[later, 'second'], -0.1)
        self.assertEqual(result.loc[missing, 'second'], 0)
        assert_frame_equal(positions, original_positions)
        assert_frame_equal(market, original_market)

    def test_empty_output_frames_keep_their_columns(self):
        positions = self.frame([dict(model_id='model', timestamp=self.now, **{'p.A': 0.})])
        output = calc_position_frame(positions.iloc[:0], self.now)
        self.assertTrue(output.empty)
        self.assertEqual(set(output.columns), {'model_id', 'timestamp', 'symbol', 'position', 'position_diff'})
        returns = pd.DataFrame(index=pd.DatetimeIndex([], name='timestamp', tz='UTC'))
        output = calc_return_frame(returns, self.now)
        self.assertTrue(output.empty)
        self.assertEqual(set(output.columns), {'model_id', 'timestamp', 'ret'})

    def test_entry_points_select_the_correct_mode(self):
        for module, stock in ((main, False), (main_stock, True)):
            with self.subTest(stock=stock), patch.object(module, 'run') as run:
                module.start()
                run.assert_called_once_with(stock=stock)


if __name__ == '__main__':
    unittest.main()
