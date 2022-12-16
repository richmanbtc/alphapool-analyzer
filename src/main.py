import math
import os
import time
import pandas as pd
import dataset
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from alphapool import Client
from .logger import create_logger
from .processing import (
    preprocess_df, calc_model_ret, calc_portfolio_positions,
    convert_to_old_format
)
from .data_fetcher import DataFetcher


def vacuum(database_url):
    engine = create_engine(database_url)
    with engine.connect() as conn:
        statement = text('VACUUM')
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(statement)


def fetch_df_market(symbols, min_timestamp):
    provider_configs = [
        {
            'provider': 'bigquery',
            'options': {
                'table': 'binance_ohlcv_5m',
                'symbols': ['{}USDT'.format(x) for x in symbols],
            }
        },
    ]
    dfs = DataFetcher().fetch(provider_configs=provider_configs, min_timestamp=min_timestamp)
    df = dfs[0]
    df['symbol'] = df['symbol'].str.replace('USDT', '')

    dfs = []
    for symbol, df_symbol in df.groupby('symbol'):
        df_symbol = df_symbol.sort_values('timestamp')
        df_symbol = df_symbol.drop_duplicates("timestamp", keep="last")
        df_symbol["ret." + symbol] = df_symbol["cl"].shift(-1) / df_symbol["cl"] - 1
        df_symbol = df_symbol.dropna()

        df_symbol = df_symbol.set_index(["timestamp"])
        dfs.append(df_symbol[["ret." + symbol]])

    df = pd.concat(dfs, axis=1)
    df = df.sort_index()

    return df


def start():
    log_level = os.getenv("ALPHAPOOL_LOG_LEVEL")
    interval = 5 * 60

    logger = create_logger(log_level)

    database_url = os.getenv("ALPHAPOOL_DATABASE_URL")
    db = dataset.connect(database_url)
    client = Client(db)

    analyzer_positions = db.create_table('analyzer_positions')
    analyzer_rets = db.create_table('analyzer_rets')

    def job():
        logger.info("job started")

        logger.info("vacuum")
        vacuum(database_url)

        execution_time = math.floor(time.time() / interval) * interval
        execution_time = pd.to_datetime(execution_time, unit="s", utc=True)
        logger.info("execution_time {}".format(execution_time))

        if analyzer_positions.count() == 0:
            df = client.get_positions()
            min_update_time = df.index.get_level_values('timestamp').min()
            min_fetch_time = min_update_time
        else:
            last_position_time = pd.to_datetime(
                analyzer_positions.find_one(order_by=['-timestamp'])['timestamp'],
                utc=True)
            min_update_time = last_position_time - pd.to_timedelta(1, unit="D")
            min_fetch_time = min_update_time - pd.to_timedelta(1, unit="D")
            df = client.get_positions(min_timestamp=min_fetch_time.timestamp())

        df = convert_to_old_format(df)

        logger.info("min_update_time {}".format(min_update_time))
        logger.info("min_fetch_time {}".format(min_fetch_time))

        df = preprocess_df(df, execution_time)
        df = calc_portfolio_positions(df)
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

        df_model_ret = calc_model_ret(df).dropna()
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
