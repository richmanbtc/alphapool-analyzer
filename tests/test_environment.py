import os
import unittest
from unittest.mock import patch

import pandas as pd

from src.settings import ENV_NAMES, get_config_value
from src.result_writer import ResultWriter
from src.data_providers.bigquery import fetch, latest_timestamp


class EnvironmentTests(unittest.TestCase):
    def test_values_are_read_at_call_time(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(get_config_value('project_id'))
            os.environ[ENV_NAMES['project_id']] = 'test-project'
            self.assertEqual(get_config_value('project_id'), 'test-project')

    def test_renamed_variables_reach_both_bigquery_consumers(self):
        names = {key: 'TEST_' + key.upper() for key in ENV_NAMES}
        values = {names['project_id']: 'test-project', names['market_dataset']: 'market'}
        with patch.dict(ENV_NAMES, names), patch.dict(os.environ, values, clear=True), \
                patch('src.data_providers.bigquery.bigquery.Client') as client:
            client.return_value.project = 'test-project'
            client.return_value.query.return_value.result.return_value = [{'timestamp': 100}]
            client.return_value.query.return_value.to_dataframe.return_value = pd.DataFrame({'timestamp': [0]})
            writer = ResultWriter()
            self.assertEqual(writer.dataset.dataset_id, 'market')
            latest_timestamp('prices', 200)
            self.assertIn('`market.prices`', client.return_value.query.call_args.args[0])
            fetch({'columns': ['timestamp', 'symbol', 'cl'], 'table': 'prices'}, 0)
            self.assertIn('`market.prices`', client.return_value.query.call_args.args[0])
            self.assertEqual(client.call_count, 3)
            for call in client.call_args_list:
                self.assertEqual(call.kwargs, {'project': 'test-project'})


if __name__ == '__main__':
    unittest.main()
