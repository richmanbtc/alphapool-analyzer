import unittest
from unittest.mock import Mock, patch
from dataclasses import replace
from src.settings import CRYPTO

import pandas as pd
from pandas.testing import assert_frame_equal

from src.runner import run_incremental, run_job


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.start = pd.Timestamp('2025-01-01', tz='UTC')
        self.now = self.start + pd.Timedelta(days=40)
        self.empty = pd.DataFrame(columns=['timestamp', 'model_id', 'positions', 'weights'])

    def run_once(self, progress, *, origin=None, stock=False, writer=None, available=None):
        reads = Mock(return_value=self.empty)
        write = writer or Mock()
        run_incremental(
            execution_time=self.now, stock=stock, logger=Mock(),
            read_progress=Mock(return_value=progress), read_start=Mock(return_value=origin),
            read_market_end=Mock(return_value=self.now if available is None else available),
            read_positions=reads, read_market=Mock(), write_results=write,
        )
        return reads, write

    def test_initial_run_catches_up_across_empty_intervals(self):
        for stock, days in ((False, 1), (True, 7)):
            reads, write = self.run_once(None, origin=self.start, stock=stock)
            cursor, completed = self.start, None
            self.assertEqual(write.call_count, (40 + days - 1) // days)
            self.assertEqual(reads.call_count, write.call_count)
            for call in write.call_args_list:
                end = min(cursor + pd.Timedelta(days=days), self.now)
                self.assertEqual(call.args[2:4], (cursor, end))
                self.assertEqual(call.kwargs['completed_until'], completed)
                self.assertEqual(call.kwargs['retain_since'], write.call_args.kwargs['retain_since'])
                self.assertTrue(call.args[0].empty)
                cursor = completed = end
            self.assertEqual(cursor, self.now)

    def test_checkpoint_accepts_bigquery_datetime_values(self):
        _, write = self.run_once(self.start.to_pydatetime())
        first = write.call_args_list[0]
        self.assertEqual(first.args[2], self.start - pd.Timedelta(days=1))
        self.assertEqual(first.args[3], self.start + pd.Timedelta(days=1))

    def test_overlap_and_forward_progress_are_independent(self):
        settings = replace(CRYPTO, advance=pd.Timedelta(hours=3), overlap=pd.Timedelta(hours=1))
        with patch('src.runner.get_settings', return_value=settings):
            _, write = self.run_once(self.start, available=self.start + pd.Timedelta(hours=5))
        self.assertEqual(write.call_count, 2)
        self.assertEqual(write.call_args_list[0].args[2:4],
                         (self.start - pd.Timedelta(hours=1), self.start + pd.Timedelta(hours=3)))
        self.assertEqual(write.call_args_list[1].args[2:4],
                         (self.start + pd.Timedelta(hours=3), self.start + pd.Timedelta(hours=5)))

    def test_no_input_does_not_create_progress(self):
        reads, write = self.run_once(None)
        reads.assert_not_called()
        write.assert_not_called()

    def test_existing_progress_catches_up_with_overlap_only_once(self):
        for stock, days in ((False, 1), (True, 7)):
            _, write = self.run_once(self.start, stock=stock)
            cursor = self.start
            for i, call in enumerate(write.call_args_list):
                start = cursor - pd.Timedelta(days=days) if i == 0 else cursor
                end = min(cursor + pd.Timedelta(days=days), self.now)
                self.assertEqual(call.args[2:4], (start, end))
                self.assertEqual(call.kwargs['completed_until'], cursor)
                cursor = end
            self.assertEqual(cursor, self.now)

    def test_failures_do_not_skip_the_failed_interval(self):
        failing = Mock(side_effect=RuntimeError('commit failed'))
        with self.assertRaisesRegex(RuntimeError, 'commit failed'):
            self.run_once(self.start, writer=failing)
        _, retried = self.run_once(self.start)
        self.assertEqual(failing.call_args.args[2:4], retried.call_args_list[0].args[2:4])

    def test_partial_initial_run_resumes_from_last_successful_commit(self):
        for stock, days in ((False, 1), (True, 7)):
            progress = None

            def commit(*args, completed_until, **kwargs):
                nonlocal progress
                self.assertEqual(completed_until, progress)
                if progress is not None:
                    raise RuntimeError('commit failed')
                progress = args[3]

            failing = Mock(side_effect=commit)
            with self.assertRaisesRegex(RuntimeError, 'commit failed'):
                self.run_once(None, origin=self.start, stock=stock, writer=failing)
            self.assertEqual(failing.call_count, 2)
            self.assertEqual(progress, self.start + pd.Timedelta(days=days))
            _, retried = self.run_once(progress, stock=stock)
            first = retried.call_args_list[0]
            self.assertEqual(first.kwargs['completed_until'], progress)
            self.assertEqual(first.args[2], progress - pd.Timedelta(days=days))
            self.assertEqual(first.args[3], failing.call_args.args[3])
            self.assertEqual(retried.call_args.args[3], self.now)

    def test_progress_and_market_boundary_are_read_once_per_run(self):
        progress = Mock(return_value=self.start)
        origin = Mock()
        boundary = Mock(side_effect=[self.now, self.now + pd.Timedelta(days=1)])
        writer = Mock()
        run_incremental(
            execution_time=self.now, read_progress=progress, read_start=origin,
            read_market_end=boundary, read_positions=Mock(return_value=self.empty),
            read_market=Mock(), write_results=writer, logger=Mock(),
        )
        progress.assert_called_once()
        origin.assert_not_called()
        boundary.assert_called_once()
        self.assertEqual(writer.call_args.args[3], self.now)

    def test_no_market_data_does_not_write_progress(self):
        writer = Mock()
        run_incremental(
            execution_time=self.now, read_progress=Mock(return_value=None),
            read_start=Mock(return_value=self.start), read_market_end=Mock(return_value=None),
            read_positions=Mock(), read_market=Mock(), write_results=writer, logger=Mock(),
        )
        writer.assert_not_called()

    def test_initial_run_at_market_boundary_does_not_write(self):
        reads, write = self.run_once(None, origin=self.now)
        reads.assert_not_called()
        write.assert_not_called()

    def test_recent_overlap_stops_at_available_market_boundary(self):
        boundary = self.now - pd.Timedelta(hours=2)
        _, write = self.run_once(boundary, available=boundary)
        write.assert_called_once()
        self.assertEqual(write.call_args.args[3], boundary)
        self.assertEqual(write.call_args.args[2], boundary - pd.Timedelta(days=1))

    def test_read_failure_never_writes_progress(self):
        writer = Mock()
        with self.assertRaisesRegex(RuntimeError, 'read failed'):
            run_job(execution_time=self.now, start_time=self.start, end_time=self.start + pd.Timedelta(days=1),
                    read_positions=Mock(side_effect=RuntimeError('read failed')), read_market=Mock(),
                    write_results=writer, logger=Mock())
        writer.assert_not_called()

    def test_completion_log_reports_saved_interval_and_counts(self):
        for stock in (False, True):
            for empty in (False, True):
                raw = self.empty if empty else pd.DataFrame([
                    dict(timestamp=self.start, model_id='model', positions={'A': 1.0}, weights={})])
                writer, logger = Mock(), Mock()
                end = self.start + pd.Timedelta(minutes=5)
                run_job(execution_time=self.start, start_time=self.start, end_time=end,
                        read_positions=Mock(return_value=raw),
                        read_market=Mock(return_value=pd.DataFrame({'ret.A': [0.1]}, index=[self.start])),
                        write_results=writer, logger=logger, stock=stock)
                fmt, *values = logger.info.call_args.args
                message = fmt % tuple(values)
                positions, returns = writer.call_args.args[:2]
                for expected in ('mode=' + ('stock' if stock else 'crypto'),
                                 'start=' + self.start.isoformat(), 'end=' + end.isoformat(),
                                 f'positions={len(positions)}', f'returns={len(returns)}'):
                    self.assertIn(expected, message)

    def test_failed_save_does_not_log_completion(self):
        logger = Mock()
        with self.assertRaisesRegex(RuntimeError, 'save failed'):
            run_job(execution_time=self.start, start_time=self.start,
                    end_time=self.start + pd.Timedelta(minutes=5),
                    read_positions=Mock(return_value=self.empty), read_market=Mock(),
                    write_results=Mock(side_effect=RuntimeError('save failed')), logger=logger)
        logger.info.assert_not_called()

    def calculate(self, raw, start, end, stock=False):
        def read(min_timestamp, max_timestamp, seed_min_timestamp=None):
            times = raw.timestamp
            before = raw.loc[times < pd.to_datetime(min_timestamp, unit='s', utc=True)]
            if seed_min_timestamp is not None:
                before = before.loc[before.timestamp >= pd.to_datetime(seed_min_timestamp, unit='s', utc=True)]
            seed = before.sort_values('timestamp').groupby('model_id').tail(1)
            current = raw.loc[(times >= pd.to_datetime(min_timestamp, unit='s', utc=True)) &
                              (times < pd.to_datetime(max_timestamp, unit='s', utc=True))]
            return pd.concat([seed, current], ignore_index=True)

        times = pd.date_range(self.start - pd.Timedelta(days=1), self.start + pd.Timedelta(days=10), freq='5min')
        market = pd.DataFrame({'ret.A': 0.1}, index=times.rename('timestamp'))
        writer = Mock()
        run_job(execution_time=self.start + pd.Timedelta(days=4), start_time=start, end_time=end,
                read_positions=read, read_market=Mock(return_value=market), write_results=writer,
                logger=Mock(), stock=stock)
        return writer.call_args.args[:2]

    def test_split_and_single_intervals_have_identical_values_including_differences(self):
        for stock in (False, True):
            with self.subTest(stock=stock):
                times = [self.start + pd.Timedelta(days=d) for d in (0, 1, 3, 4)]
                raw = pd.DataFrame([
                    dict(timestamp=t, model_id=model, positions={'A': value}, weights={})
                    for t, value in zip(times, (0.2, 0.0, 0.7, 0.3)) for model in ('first', 'second')
                ])
                end = self.start + pd.Timedelta(days=5)
                split = self.start + pd.Timedelta(days=3)
                full = self.calculate(raw, self.start, end, stock)
                left = self.calculate(raw, self.start, split, stock)
                right = self.calculate(raw, split, end, stock)
                for i in range(2):
                    keys = ['model_id', 'timestamp'] + (['symbol'] if i == 0 else [])
                    actual = pd.concat([left[i], right[i]], ignore_index=True).sort_values(keys).reset_index(drop=True)
                    expected = full[i].sort_values(keys).reset_index(drop=True)
                    assert_frame_equal(actual, expected)

    def test_backfill_keeps_returns_but_does_not_restore_expired_positions(self):
        raw = pd.DataFrame([dict(timestamp=self.start, model_id='model', delay=0,
                                 positions={'A': 1.0}, weights={})])
        writer = Mock()
        run_job(execution_time=self.now, start_time=self.start, end_time=self.start + pd.Timedelta(minutes=5),
                read_positions=Mock(return_value=raw),
                read_market=Mock(return_value=pd.DataFrame({'ret.A': [0.1]}, index=[self.start])),
                write_results=writer, logger=Mock())
        self.assertTrue(writer.call_args.args[0].empty)
        self.assertEqual(writer.call_args.args[1]['ret'].tolist(), [0.1])


if __name__ == '__main__':
    unittest.main()
