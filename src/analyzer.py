import pandas as pd
from .data_fetcher import DataFetcher


class Analyzer:
    def __init__(self, db):
        self._db = db
        self._positions = db.create_table('positions')
        self._analyzer_rets = db.create_table('analyzer_rets')

        # self._analyzer_positions = db.create_table('analyzer_positions')
        # self._analyzer_weights = db.create_table('analyzer_weights')

    def analyze(self):
        start_time, end_time = self._calc_time_range()
        self._logger.info('start_time {} end_time {}'.format(start_time, end_time))

        df = self._db.get_positions(min_timestamp=start_time)
        df_ohlcv = self._fetch_ohlcv(df)
        df_positions = self._calc_df_positions(df)

        # calc ret
        df_ohlcv['ret'] = (df_ohlcv.groupby('symbol')['cl'].shift(-1) / df_ohlcv['cl'] - 1).fillna(0)
        df_positions = df_positions.join(df_ohlcv[['ret']], how='left')
        df_positions['ret'] = df_positions['ret'].fillna(0)
        df_positions['pos_ret'] = df_positions['pos'] * df_positions['ret']
        df_ret = df_positions.groupby(['timestamp', 'model_id'])['pos_ret'].sum()

        with self._db:
            self._analyzer_rets.delete(timestamp={ 'gte': start_time })
            self._analyzer_rets.insert_many(df_ret.reset_index().to_dict('records'))


        # positions
        # retrieve model positions
        # calc model positions
        # calc portfolio positions
        # upsert

        # ret
        # retrieve model positions
        # retrieve prices (default, exchange specific)
        # calc taker ret
        # calc maker ret (add to taker)
        # calc portfolio ret
        # upsert ret

    def _calc_time_range(self):
        if self._analyzer_rets.count() == 0:
            query = 'SELECT MIN(timestamp) AS min_timestamp FROM positions'
            start_time = self._db.query(query)[0]['min_timestamp']
        else:
            query = 'SELECT MAX(timestamp) AS max_timestamp FROM analyzer_rets'
            start_time = self._db.query(query)[0]['max_timestamp'] - 24 * 60 * 60
        end_time = start_time + 7 * 24 * 60 * 60
        return start_time, end_time

    def _fetch_ohlcv(self, df):
        start_time = df.index.get_level_values('timestamp').min().timestamp()

        symbols = set()
        for row in df.itertuples():
            symbols.add(set(row['positions'].keys()))

        provider_configs = [
            {
                'provider': 'bigquery',
                'options': {
                    'table': 'binance_ohlcv_5m',
                    'symbols': ['{}USDT'.format(x) for x in symbols],
                }
            },
        ]
        dfs = DataFetcher().fetch(provider_configs=provider_configs, min_timestamp=start_time)
        df = dfs[0]
        df['symbol'] = df['symbol'].str.replace('USDT', '')
        df = df.set_index(['timestamp', 'symbol']).sort_index()
        return df

    def _calc_df_positions(self, df):
        rows = []
        for row in df.itertuples():
            idx = row['Index']
            rows.append({
                'timestamp': idx[0],
                'model_id': idx[1],
                **row['positions'],
            })
        df = pd.DataFrame(rows).set_index(['timestamp', 'model_id']).sort_index()
        df = df.ffill().asfreq('5min', method='ffill').fillna(0)
        return df
