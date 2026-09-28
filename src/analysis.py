import numpy as np
import pandas as pd

from .processing import calc_portfolio_positions, expand_positions
from .settings import CRYPTO, CRYPTO_INTERVAL, POSITION_TTL


def prepare_positions(raw, start, end, *, stock=False):
    """Keep historical models and seed interval boundaries from prior snapshots."""
    df = expand_positions(raw)
    df = df.loc[~df.index.get_level_values('model_id').str.startswith('portfolio:')]
    invalid = df.pop('_invalid') if '_invalid' in df else pd.Series(False, index=df.index)
    df = df.filter(regex=r'^(p|w)\.').fillna(0)
    df = df.apply(pd.to_numeric, errors='coerce').replace([np.inf, -np.inf], np.nan)
    if df.empty or (stock and not any(col.startswith('p.') for col in df.columns)):
        return df.iloc[:0]
    df.loc[invalid] = np.nan
    df['_emit'] = ~invalid
    if stock:
        df = normalize_stock_positions(df)
        return add_equal_weighted_portfolio(df.loc[df.pop('_emit')])
    times = pd.date_range(start - CRYPTO_INTERVAL, end, freq=CRYPTO.frequency, inclusive='left', name='timestamp')
    frames = []
    for model, frame in df.groupby('model_id'):
        frame = frame.droplevel('model_id').sort_index()
        frame = frame.loc[~frame.index.duplicated(keep='last')]
        updates = pd.Series(frame.index, index=frame.index).reindex(times, method='ffill')
        age = times - updates
        expired = age > POSITION_TTL
        frame = frame.reindex(times, method='ffill')
        frame.loc[expired | updates.isna(), frame.columns != '_emit'] = 0
        # Keep active samples and exactly the first grid point after expiry.
        frame['_emit'] = frame['_emit'].eq(True) & updates.notna() & (age <= (POSITION_TTL + CRYPTO_INTERVAL))
        frame['model_id'] = model
        frames.append(frame.reset_index().set_index(['model_id', 'timestamp']))
    result = calc_portfolio_positions(pd.concat(frames).sort_index())
    return result.loc[result.pop('_emit')]


def calc_required_symbols(df):
    return [col[2:] for col in df.columns if col.startswith('p.') and df[col].dropna().ne(0).any()]


def calc_position_frame(df, min_update_time, *, reset_after_gap=False):
    differences = df.groupby('model_id').diff().fillna(0)
    if reset_after_gap:
        times = pd.Series(df.index.get_level_values('timestamp'), index=df.index)
        gaps = times.groupby('model_id').diff().ne(CRYPTO_INTERVAL)
        differences.loc[gaps] = df.loc[gaps]
    in_window = df.index.get_level_values('timestamp') >= min_update_time
    frames = []
    for col in df.columns:
        valid = in_window & np.isfinite(df[col])
        frame = pd.DataFrame({
            'position': df.loc[valid, col],
            'position_diff': differences.loc[valid, col].replace([np.inf, -np.inf], np.nan),
        }).reset_index()
        frame['symbol'] = col[2:]
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=['model_id', 'timestamp', 'position', 'position_diff', 'symbol'],
    )


def calc_return_frame(returns, min_update_time):
    frames = []
    for col in returns.columns:
        values = returns.loc[returns.index >= min_update_time, col].dropna()
        frame = values.rename('ret').rename_axis('timestamp').reset_index()
        frame['model_id'] = col
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=['timestamp', 'ret', 'model_id'],
    )


def add_equal_weighted_portfolio(df):
    symbol_cols = [col for col in df.columns if col.startswith('p.')]
    portfolio = df.groupby('timestamp')[symbol_cols].mean()
    portfolio['model_id'] = 'pf-equal'
    portfolio = portfolio.reset_index().set_index(['model_id', 'timestamp'])
    return pd.concat([df, portfolio]).sort_index()


def normalize_stock_positions(df):
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
    return df.sort_index()
