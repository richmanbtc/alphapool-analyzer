import math
import os
import time
import pandas as pd
import dataset
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from alphapool import Client
from .market_data_store.data_fetcher_builder import DataFetcherBuilder
from .market_data_store.market_data_store import MarketDataStore
from .logger import create_logger
from .processing import preprocess_df, calc_model_ret


def vacuum(database_url):
    engine = create_engine(database_url)
    with engine.connect() as conn:
        statement = text('VACUUM')
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(statement)


def start():
    log_level = os.getenv("ALPHAPOOL_LOG_LEVEL")
    interval = 5 * 60
    tournament = "crypto"

    logger = create_logger(log_level)

    database_url = os.getenv("ALPHAPOOL_DATABASE_URL")
    db = dataset.connect(database_url)
    client = Client(db)

    data_fetcher_builder = DataFetcherBuilder()
    market_data_store = MarketDataStore(
        data_fetcher_builder=data_fetcher_builder,
        start_time=client.get_positions(tournament="crypto").index.get_level_values('timestamp').min().timestamp(),
        logger=logger,
        interval=5 * 60,
    )

    analyzer_positions = db.create_table('analyzer_positions')
    analyzer_rets = db.create_table('analyzer_rets')

    def job():
        logger.info("job started")
        execution_time = math.floor(time.time() / interval) * interval
        execution_time = pd.to_datetime(execution_time, unit="s", utc=True)
        min_update_time = execution_time - pd.to_timedelta(2, unit="D")
        logger.info("execution_time {}".format(execution_time))
        logger.info("min_update_time {}".format(min_update_time))

        logger.info("vacuum")
        vacuum(database_url)

        df = client.get_positions(tournament="crypto", min_timestamp=min_update_time.timestamp())
        df = preprocess_df(df, execution_time)
        logger.debug(df)

        symbols = df.columns.str.replace("p.", "", regex=False).to_list()
        symbols = [symbol for symbol in symbols if not symbol.startswith('w.')]
        logger.debug(symbols)

        rows = []
        for col in df.columns:
            if df[col].abs().sum() == 0:
                continue
            df2 = pd.concat([
                df[col].rename('position'),
                df.groupby('model_id')[col].diff(1).fillna(0).rename('position_diff'),
            ], axis=1)
            df2 = df2.reset_index()
            df2['tournament'] = tournament
            df2['symbol'] = col.replace('p.', '').replace('w.', '')
            rows += df2.to_dict('records')
        with db:
            analyzer_positions.delete(timestamp={ 'gte': min_update_time.timestamp() })
            analyzer_positions.insert_many(rows)

        df_ret = market_data_store.fetch_df_market(symbols=symbols)
        df = df.join(df_ret).dropna()
        logger.debug(df_ret)

        df_model_ret = calc_model_ret(df).dropna()
        logger.debug(df_model_ret)

        rows = []
        for col in df_model_ret.columns:
            df2 = pd.concat([
                df_model_ret[col].rename('ret')
            ], axis=1)
            df2 = df2.reset_index()
            df2['tournament'] = tournament
            df2['model_id'] = col.replace('ret.', '')
            rows += df2.to_dict('records')
        with db:
            analyzer_rets.delete(timestamp={ 'gte': min_update_time.timestamp() })
            analyzer_rets.insert_many(rows)

        analyzer_positions.create_index(['timestamp', 'model_id'])
        analyzer_rets.create_index(['timestamp', 'model_id'])

        logger.info("job finished")

    job()


start()
