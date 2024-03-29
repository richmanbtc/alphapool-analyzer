import math
import os
import time
import pandas as pd
import dataset
from alphapool import Client
from .logger import create_logger
from .processing import (
    preprocess_df, calc_model_ret, calc_portfolio_positions,
    convert_to_old_format
)
from .data_fetcher import DataFetcher
from .utils import vacuum


def fetch_df_market(symbols, min_timestamp):
    provider_configs = [
        {
            'provider': 'bigquery',
            'options': {
                'table': 'jq_ohlcv',
                'symbols': symbols,
            }
        },
    ]
    dfs = DataFetcher().fetch(provider_configs=provider_configs, min_timestamp=min_timestamp)
    df = dfs[0]

    df['adj_cl'] = df['cl'] * df.groupby('symbol')['adj_factor'].transform(lambda x: x.shift(-1).fillna(1).iloc[::-1].cumprod().iloc[::-1])

    dfs = []
    for symbol, df_symbol in df.groupby('symbol'):
        df_symbol = df_symbol.sort_values('timestamp')
        df_symbol = df_symbol.drop_duplicates("timestamp", keep="last")

        def calc_op_mocl(df):
            df = df.copy()
            df["ret." + symbol] = df["mo_cl"] / df["op"] - 1
            df = df.dropna()
            return df.set_index(["timestamp"])

        def calc_mocl_afop(df):
            df = df.copy()
            df['timestamp'] += pd.to_timedelta(2 * 60 + 30, unit='minute')
            df["ret." + symbol] = df["af_op"] / df["mo_cl"] - 1
            df = df.dropna()
            return df.set_index(["timestamp"])

        def calc_afop_cl(df):
            df = df.copy()
            df['timestamp'] += pd.to_timedelta(3 * 60 + 30, unit='minute')
            df["ret." + symbol] = df["cl"] / df["af_op"] - 1
            df = df.dropna()
            return df.set_index(["timestamp"])

        def calc_clop(df):
            df = df.copy()
            df['timestamp'] += pd.to_timedelta(6, unit='H')
            df["adj_op"] = df["op"] * df['adj_cl'] / df['cl']
            df["ret." + symbol] = df["adj_op"].shift(-1) / df["adj_cl"] - 1
            df = df.dropna()
            return df.set_index(["timestamp"])

        dfs += [
            pd.concat([
                calc_op_mocl(df_symbol)[["ret." + symbol]],
                calc_mocl_afop(df_symbol)[["ret." + symbol]],
                calc_afop_cl(df_symbol)[["ret." + symbol]],
                calc_clop(df_symbol)[["ret." + symbol]],
            ])
        ]

    df = pd.concat(dfs, axis=1)
    df = df.sort_index()
    df = df.fillna(0)

    return df


def _normalize_stock_pos(df):
    def fill_time_shift(df, hour, shift_minutes):
        idx = df.index
        idx_src = idx[idx.get_level_values('timestamp').hour == hour]
        idx_dest = idx_src.to_frame()
        idx_dest['timestamp'] += pd.to_timedelta(shift_minutes, unit='minute')
        idx_dest = pd.MultiIndex.from_frame(idx_dest)
        dest_exists = idx_dest.isin(idx)
        return pd.concat([
            df,
            df.loc[idx_src[~dest_exists]].set_index(idx_dest[~dest_exists]),
        ])

    df = fill_time_shift(df, 0, 2 * 60 + 30)
    df = fill_time_shift(df, 2, 60)
    df = df.sort_index()
    return df


def start():
    log_level = os.getenv("ALPHAPOOL_LOG_LEVEL")
    interval = 5 * 60

    logger = create_logger(log_level)

    database_url = os.getenv("ALPHAPOOL_DATABASE_URL")
    db = dataset.connect(database_url)
    client = Client(db)

    analyzer_positions = db.create_table(
        'analyzer_positions',
        primary_type=db.types.bigint,
    )
    analyzer_rets = db.create_table(
        'analyzer_rets',
        primary_type=db.types.bigint,
    )

    def job():
        logger.info("job started")

        logger.info("vacuum")
        vacuum(database_url)

        execution_time = math.floor(time.time() / interval) * interval
        execution_time = pd.to_datetime(execution_time, unit="s", utc=True)
        logger.info("execution_time {}".format(execution_time))

        min_update_time = execution_time - pd.to_timedelta(2 * 28, unit="D")
        min_fetch_time = min_update_time - pd.to_timedelta(1, unit="D")
        logger.info("min_update_time {}".format(min_update_time))
        logger.info("min_fetch_time {}".format(min_fetch_time))
        df = client.get_positions(min_timestamp=min_fetch_time.timestamp())

        df = convert_to_old_format(df)

        df = preprocess_df(df, execution_time, inactive_days=7, disable_asfreq=True)

        df = _normalize_stock_pos(df)

        # df = calc_portfolio_positions(df)
        def calc_equal_weighted(df):
            symbol_cols = [x for x in df.columns if x.startswith("p.")]
            df2 = df.groupby('timestamp')[symbol_cols].mean()
            df2['model_id'] = 'pf-equal'
            df2 = df2.reset_index().set_index(["model_id", "timestamp"])
            return df.append(df2).sort_index()
        df = calc_equal_weighted(df)
        logger.debug(df)

        symbols = df.columns.str.replace("p.", "", regex=False).to_list()
        symbols = [symbol for symbol in symbols if not symbol.startswith('w.')]
        logger.debug(symbols)

        position_rows = []
        for col in df.columns:
            if df[col].abs().sum() == 0:
                continue
            df2 = pd.concat([
                df[col].rename('position'),
                df.groupby('model_id')[col].diff(1).fillna(0).rename('position_diff'),
            ], axis=1)
            df2 = df2.reset_index()
            df2 = df2[df2['timestamp'] >= min_update_time]
            df2['symbol'] = col.replace('p.', '').replace('w.', '')
            position_rows += df2.to_dict('records')

        df_ret = fetch_df_market(symbols=symbols, min_timestamp=min_fetch_time.timestamp())
        df = df.join(df_ret).dropna()
        logger.debug(df_ret)

        df_model_ret = calc_model_ret(df).fillna(0).dropna()
        logger.debug(df_model_ret)

        ret_rows = []
        for col in df_model_ret.columns:
            df2 = pd.concat([
                df_model_ret[col].rename('ret')
            ], axis=1)
            df2 = df2.reset_index()
            df2 = df2[df2['timestamp'] >= min_update_time]
            df2['model_id'] = col.replace('ret.', '')
            ret_rows += df2.to_dict('records')

        with db:
            analyzer_positions.delete(timestamp={ 'gte': min_update_time })
            analyzer_positions.insert_many(position_rows)
            analyzer_rets.delete(timestamp={ 'gte': min_update_time })
            analyzer_rets.insert_many(ret_rows)

        analyzer_positions.create_index(['timestamp', 'model_id'])
        analyzer_rets.create_index(['timestamp', 'model_id'])

        logger.info("job finished")

    job()


start()
