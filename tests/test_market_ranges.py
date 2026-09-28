import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.data_providers.bigquery import fetch, latest_timestamp
from src.market import fetch_market_returns, get_market_end


class MarketRangeTests(unittest.TestCase):
    def test_bounded_query_includes_one_successor_per_symbol(self):
        with patch('src.data_providers.bigquery.bigquery.Client') as client:
            client.return_value.query.return_value.to_dataframe.return_value = pd.DataFrame({'timestamp': [0]})
            fetch({'columns': ['timestamp', 'symbol', 'cl'], 'table': 'prices', 'symbols': ['A'], 'max_timestamp': 100,
                   'available_timestamp': 200}, min_timestamp=0)
            query = client.return_value.query.call_args
            sql = query.args[0]
            self.assertIn('timestamp < @end', sql)
            self.assertIn('timestamp >= @end', sql)
            self.assertIn('PARTITION BY symbol ORDER BY timestamp', sql)
            self.assertIn('timestamp <= @available', sql)
            values = {p.name: p.value for p in query.kwargs['job_config'].query_parameters if hasattr(p, 'value')}
            self.assertEqual(values, {'start': 0, 'end': 100, 'available': 200})

    def test_market_queries_project_only_required_columns(self):
        for stock in (False, True):
            for end in (None, 100):
                with self.subTest(stock=stock, end=end), \
                        patch('src.data_providers.bigquery.bigquery.Client') as client, \
                        patch('src.data_providers.bigquery.get_config_value', return_value='synthetic'):
                    client.return_value.query.return_value.to_dataframe.return_value = pd.DataFrame({'timestamp': []})
                    fetch_market_returns(['A'], 0, stock=stock, max_timestamp=end)
                    query = client.return_value.query.call_args.args[0]
                    self.assertNotIn('SELECT *', query)
                    expected = ['timestamp', 'symbol', 'cl']
                    if stock:
                        expected += ['op', 'mo_cl', 'af_op', 'adj_factor']
                    selections = [part.split(' FROM ')[0] for part in query.split('SELECT ')[1:]]
                    self.assertEqual(len(selections), 1 if end is None else 2)
                    for selection in selections:
                        self.assertEqual(selection.split(', '), [f'`{column}`' for column in expected])

    def test_market_watermark_respects_completed_bar_limit(self):
        with patch('src.data_providers.bigquery.bigquery.Client') as client:
            client.return_value.query.return_value.result.return_value = [{'timestamp': 100}]
            self.assertEqual(latest_timestamp('prices', 200), 100)
            self.assertIn('timestamp <= @available', client.return_value.query.call_args.args[0])
        with patch('src.market.latest_timestamp', return_value=None):
            self.assertIsNone(get_market_end(200))

    def test_stock_successor_after_long_holiday_preserves_overnight_return(self):
        first = pd.Timestamp('2025-01-01', tz='UTC')
        market = pd.DataFrame([
            dict(symbol='A', timestamp=first, op=100., mo_cl=100., af_op=100., cl=100., adj_factor=1.),
            dict(symbol='A', timestamp=first + pd.Timedelta(days=14),
                 op=55., mo_cl=55., af_op=55., cl=55., adj_factor=0.5),
        ])
        fetcher = Mock()
        fetcher.fetch.return_value = [market]
        result = fetch_market_returns(['A'], first.timestamp(), stock=True, fetcher=fetcher,
                                      max_timestamp=(first + pd.Timedelta(days=1)).timestamp())
        self.assertAlmostEqual(result.loc[first + pd.Timedelta(hours=6), 'ret.A'], 0.1)


if __name__ == '__main__':
    unittest.main()
