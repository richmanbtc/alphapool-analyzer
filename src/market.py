import pandas as pd

from .data_fetcher import DataFetcher
from .data_providers.bigquery import latest_timestamp
from .processing import concat_market_returns
from .settings import get_settings


def fetch_market_returns(symbols, min_timestamp, *, stock=False, fetcher=None,
                         max_timestamp=None, available_timestamp=None):
    if not symbols:
        return concat_market_returns([])
    provider_configs = [{
        'provider': 'bigquery',
        'options': {
            'table': 'jq_ohlcv' if stock else 'binance_ohlcv_5m',
            'symbols': symbols if stock else [symbol + 'USDT' for symbol in symbols],
            'columns': ['timestamp', 'symbol', 'cl'] + (['op', 'mo_cl', 'af_op', 'adj_factor'] if stock else []),
            'max_timestamp': max_timestamp,
            'available_timestamp': available_timestamp,
        },
    }]
    if fetcher is None:
        fetcher = DataFetcher()
    frames = fetcher.fetch(provider_configs=provider_configs, min_timestamp=min_timestamp)
    calculate = calc_stock_returns if stock else calc_crypto_returns
    return calculate(frames[0])


def calc_crypto_returns(df):
    if df.empty:
        return concat_market_returns([])
    df = df.copy()
    df['cl'] = pd.to_numeric(df.get('cl'), errors='coerce')
    df['symbol'] = df['symbol'].str.removesuffix('USDT')

    dfs = []
    for symbol, df_symbol in df.groupby('symbol'):
        df_symbol = df_symbol.sort_values('timestamp', kind='stable')
        df_symbol = df_symbol.drop_duplicates('timestamp', keep='last')
        close = df_symbol.set_index('timestamp')['cl']
        returns = close.shift(-1) / close - 1
        dfs.append(returns.rename('ret.' + symbol).to_frame())

    return concat_market_returns(dfs)


def calc_stock_returns(df):
    if df.empty:
        return concat_market_returns([])
    df = df.copy()
    for col in ('cl', 'op', 'mo_cl', 'af_op', 'adj_factor'):
        df[col] = pd.to_numeric(df.get(col), errors='coerce')
    df = df.sort_values('timestamp', kind='stable').drop_duplicates(['symbol', 'timestamp'], keep='last')

    df['adj_cl'] = df['cl'] * df.groupby('symbol')['adj_factor'].transform(lambda x: x.shift(-1).fillna(1).iloc[::-1].cumprod().iloc[::-1])

    dfs = []
    for symbol, df_symbol in df.groupby('symbol'):
        df_symbol = df_symbol.set_index('timestamp')
        adj_op = df_symbol['op'] * df_symbol['adj_cl'] / df_symbol['cl']
        sessions = [
            (0, df_symbol['mo_cl'] / df_symbol['op'] - 1),
            (150, df_symbol['af_op'] / df_symbol['mo_cl'] - 1),
            (210, df_symbol['cl'] / df_symbol['af_op'] - 1),
            (360, adj_op.shift(-1) / df_symbol['adj_cl'] - 1),
        ]
        returns = pd.concat([
            ret.shift(freq=pd.Timedelta(minutes=minutes))
            for minutes, ret in sessions
        ])
        dfs.append(returns.rename('ret.' + symbol).to_frame())

    return concat_market_returns(dfs)


def get_market_end(available_timestamp, *, stock=False):
    value = latest_timestamp('jq_ohlcv' if stock else 'binance_ohlcv_5m', available_timestamp)
    return None if value is None else pd.to_datetime(value, unit='s', utc=True).floor(get_settings(stock).frequency)
