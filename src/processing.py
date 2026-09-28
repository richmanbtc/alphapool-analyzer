import numpy as np
import pandas as pd


def calc_portfolio_positions(df):
    df = df.copy()
    symbol_cols = [x for x in df.columns if x.startswith('p.')]
    for col in df.columns:
        if col.startswith('w.'):
            model_id = col[2:]

            df2 = df.index.to_frame()
            df2['model_id'] = model_id
            idx = pd.MultiIndex.from_frame(df2)

            idx_exists = idx.isin(df.index)
            idx_exists &= df[col].ne(0).to_numpy()
            df.loc[df.index[idx_exists], symbol_cols] += df[col].values[idx_exists].reshape(-1, 1) * df.loc[idx[idx_exists], symbol_cols].values
    return df


def concat_market_returns(dfs):
    if not dfs:
        return pd.DataFrame(index=pd.DatetimeIndex([], name='timestamp', tz='UTC'))
    return pd.concat(dfs, axis=1).sort_index().replace([np.inf, -np.inf], np.nan)


def calc_model_ret(df, market):
    symbol_cols = [col for col in df.columns if col.startswith('p.')]
    if df.empty:
        return concat_market_returns([])
    model_ret = pd.Series(0.0, index=df.index)
    timestamps = df.index.get_level_values('timestamp')
    for col in symbol_cols:
        position = pd.to_numeric(df[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
        price_ret = market.get('ret.' + col[2:])
        if price_ret is None:
            price_ret = np.nan
        else:
            price_ret = pd.to_numeric(price_ret, errors='coerce').reindex(timestamps)
            price_ret.index = df.index
        contribution = (position * price_ret).replace([np.inf, -np.inf], np.nan)
        model_ret += contribution.where(position.ne(0), 0.0)
    return model_ret.unstack(level=0)


def expand_positions(df):
    df = df.reset_index()
    df = df.loc[df['model_id'].map(lambda value: isinstance(value, str) and bool(value))
                & df['timestamp'].notna()].reset_index(drop=True)
    invalid = pd.Series(False, index=df.index)
    dfs = [df[['model_id', 'timestamp']]]

    for prefix, col in (('p.', 'positions'), ('w.', 'weights')):
        if col not in df.columns:
            continue
        objects = df[col].map(lambda value: isinstance(value, dict))
        invalid |= ~objects & df[col].notna()
        df_flattened = pd.json_normalize(df[col].where(objects, {}))
        if df_flattened.shape[1] > 0:
            df_flattened.columns = prefix + df_flattened.columns
            dfs.append(df_flattened)

    if invalid.any():
        dfs.append(invalid.rename('_invalid'))
    return pd.concat(dfs, axis=1).set_index(['model_id', 'timestamp']).sort_index()
