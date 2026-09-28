import os
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.result_writer import ResultWriter
from src.settings import ENV_NAMES


class ResultWriterTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {ENV_NAMES['output_dataset']: 'output'}, clear=True)
        env.start()
        self.addCleanup(env.stop)
        client_patch = patch('src.result_writer.bigquery.Client')
        self.client = client_patch.start().return_value
        self.addCleanup(client_patch.stop)
        self.client.project = 'test-project'
        self.writer = ResultWriter()
        self.timestamp = pd.Timestamp('2025-01-01', tz='UTC')
        self.positions = pd.DataFrame([dict(model_id='model', timestamp=self.timestamp,
                                           symbol='asset', position=0.5, position_diff=0.1)])
        self.rets = pd.DataFrame([dict(model_id='model', timestamp=self.timestamp, ret=0.01)])

    def write(self, positions, returns):
        self.writer.write(positions, returns, self.timestamp,
                          self.timestamp + pd.Timedelta(days=1),
                          retain_since=self.timestamp - pd.Timedelta(days=7), mode='crypto')

    def test_progress_read_and_atomic_checkpoint(self):
        self.client.query.return_value.result.return_value = [{'completed_until': self.timestamp}]
        self.assertEqual(self.writer.read_progress('crypto'), self.timestamp)
        self.write(self.positions, self.rets)
        sql = self.client.query.call_args.args[0]
        self.assertIn('ASSERT', sql)
        self.assertIn('IS NOT DISTINCT FROM @completed_until', sql)
        self.assertIn('GREATEST(@end_time', sql)
        self.assertNotIn('MERGE', sql)
        self.assertLess(sql.index('(mode, completed_until, updated_at)'), sql.index('COMMIT'))

    def test_loads_finish_before_atomic_replacement(self):
        events = []
        self.client.load_table_from_dataframe.return_value.result.side_effect = lambda: events.append('loaded')
        self.client.query.side_effect = lambda *a, **kw: events.append('query') or Mock()

        self.write(self.positions, self.rets)

        self.assertEqual(events, ['loaded', 'loaded', 'query'])
        sql = self.client.query.call_args.args[0]
        self.assertTrue(sql.startswith('BEGIN TRANSACTION;'))
        self.assertTrue(sql.endswith('COMMIT TRANSACTION;'))
        self.assertEqual(sql.count('timestamp >= @min_update_time'), 2)
        self.assertEqual(sql.count('INSERT INTO'), 3)
        config = self.client.query.call_args.kwargs['job_config']
        self.assertEqual(config.query_parameters[0].value, self.timestamp)
        frame = self.client.load_table_from_dataframe.call_args_list[0].args[0]
        self.assertIs(frame, self.positions)
        self.assertIs(self.client.load_table_from_dataframe.call_args_list[1].args[0], self.rets)
        self.assertEqual(str(frame.timestamp.dtype), 'datetime64[ns, UTC]')
        self.assertEqual(frame.position_diff.iloc[0], 0.1)
        self.assertEqual(self.client.delete_table.call_count, 2)
        for call in self.client.create_table.call_args_list:
            table = call.args[0]
            if table.table_id.startswith('_analyzer_stage_'):
                self.assertIsNotNone(table.expires)
            elif table.table_id != 'analyzer_progress':
                self.assertEqual(table.time_partitioning.field, 'timestamp')
                self.assertEqual(table.clustering_fields, ['model_id'])

    def test_empty_results_clear_ranges_without_loading(self):
        self.write(self.positions.iloc[:0], self.rets.iloc[:0])

        sql = self.client.query.call_args.args[0]
        self.assertEqual(sql.count('timestamp < @end_time'), 2)
        self.assertIn('OR timestamp < @retain_since', sql)
        self.assertNotIn('MERGE', sql)
        self.assertEqual(sql.count('INSERT INTO'), 1)
        self.assertIn('(mode, completed_until, updated_at)', sql)
        self.client.load_table_from_dataframe.assert_not_called()

    def test_load_failure_does_not_mutate_results(self):
        self.client.load_table_from_dataframe.return_value.result.side_effect = [None, RuntimeError('load failed')]

        with self.assertRaisesRegex(RuntimeError, 'load failed'):
            self.write(self.positions, self.rets)

        self.client.query.assert_not_called()
        self.assertEqual(self.client.delete_table.call_count, 2)

    def test_query_failure_cleans_staging(self):
        self.client.query.return_value.result.side_effect = RuntimeError('query failed')

        with self.assertRaisesRegex(RuntimeError, 'query failed'):
            self.write(self.positions, self.rets)

        self.assertEqual(self.client.delete_table.call_count, 2)

    def test_cleanup_failure_after_commit_is_only_a_warning(self):
        self.client.delete_table.side_effect = [RuntimeError('cleanup failed'), None]
        with self.assertLogs('src.result_writer', level='WARNING') as logs:
            self.write(self.positions, self.rets)
        self.client.query.return_value.result.assert_called_once()
        self.assertEqual(self.client.delete_table.call_count, 2)
        self.assertEqual(len(logs.output), 1)

    def test_cleanup_failures_preserve_original_save_error(self):
        self.client.query.return_value.result.side_effect = RuntimeError('query failed')
        self.client.delete_table.side_effect = RuntimeError('cleanup failed')
        with self.assertLogs('src.result_writer', level='WARNING') as logs:
            with self.assertRaisesRegex(RuntimeError, 'query failed'):
                self.write(self.positions, self.rets)
        self.assertEqual(self.client.delete_table.call_count, 2)
        self.assertEqual(len(logs.output), 2)

    def test_dataset_falls_back_to_market_dataset(self):
        with patch.dict(os.environ, {ENV_NAMES['market_dataset']: 'market'}, clear=True):
            writer = ResultWriter()
        self.assertEqual(writer.dataset.dataset_id, 'market')

    def test_missing_dataset_fails_before_creating_client(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, 'dataset is required'):
                ResultWriter()


if __name__ == '__main__':
    unittest.main()
