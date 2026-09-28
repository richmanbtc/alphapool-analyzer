from functools import partial

import pandas as pd

from .analysis import calc_position_frame, calc_required_symbols, calc_return_frame, prepare_positions
from .logger import create_logger
from .market import fetch_market_returns, get_market_end
from .processing import calc_model_ret, concat_market_returns
from .result_writer import ResultWriter
from .settings import CRYPTO_INTERVAL, POSITION_TTL, get_config_value, get_settings
from .position_reader import get_position_range, get_position_start


def run_job(*, execution_time, start_time, end_time, read_positions, read_market,
            write_results, logger, stock=False, completed_until=None):
    """Calculate exactly one half-open interval; commit results and progress together."""
    settings = get_settings(stock)
    retention = execution_time - settings.retention
    # Include the previous grid point, and its preceding snapshot, for differences.
    fetch_start = start_time.normalize() if stock else start_time - CRYPTO_INTERVAL
    seed_options = {} if stock else {
        'seed_min_timestamp': (fetch_start - POSITION_TTL).timestamp(),
    }
    raw = read_positions(min_timestamp=fetch_start.timestamp(), max_timestamp=end_time.timestamp(), **seed_options)
    positions = prepare_positions(raw, start_time, end_time, stock=stock)
    if positions.empty:
        logger.warning('No analyzable positions in interval; advancing progress')
        returns = concat_market_returns([])
    else:
        market = read_market(
            symbols=calc_required_symbols(positions), min_timestamp=fetch_start.timestamp(),
            max_timestamp=end_time.timestamp(),
        )
        returns = calc_model_ret(positions, market)
        if returns.isna().any().any():
            logger.warning('Skipping returns with missing or invalid inputs')
    position_rows = calc_position_frame(positions, max(start_time, retention),
                                       reset_after_gap=not stock)
    return_rows = calc_return_frame(returns, start_time)
    position_rows = position_rows.loc[position_rows['timestamp'] < end_time]
    return_rows = return_rows.loc[return_rows['timestamp'] < end_time]
    write_results(position_rows, return_rows, start_time, end_time,
                  retain_since=retention, mode=settings.mode,
                  completed_until=completed_until)
    logger.info('Interval finished: mode=%s start=%s end=%s positions=%d returns=%d',
                settings.mode, start_time.isoformat(), end_time.isoformat(), len(position_rows), len(return_rows))


def run_incremental(*, execution_time, read_progress, read_start, read_market_end,
                    read_positions, read_market, write_results, logger, stock=False):
    settings = get_settings(stock)
    completed = read_progress(settings.mode)
    origin = read_start() if completed is None else completed
    if origin is None:
        logger.info('No input positions; skipping job')
        return
    origin = pd.to_datetime(origin, utc=True).floor(settings.frequency)
    start = origin if completed is None else origin - settings.overlap
    start = start.floor(settings.frequency)
    available = read_market_end()
    if available is None:
        logger.info('No completed market data; skipping job')
        return
    end = min(origin + settings.advance, available)
    if end <= start:
        logger.info('No completed interval; skipping job')
        return
    run_job(execution_time=execution_time, start_time=start, end_time=end,
            read_positions=read_positions, read_market=read_market, write_results=write_results,
            logger=logger, stock=stock, completed_until=completed)


def run(*, stock=False):
    logger = create_logger(get_config_value('log_level'))
    database_url = get_config_value('database_url')
    now = pd.Timestamp.now(tz='UTC')
    # Only bars whose full time interval has elapsed are eligible.
    settings = get_settings(stock)
    market_limit = now.floor(settings.frequency) - pd.Timedelta(settings.frequency)
    writer = ResultWriter()
    run_incremental(
        execution_time=now, stock=stock, logger=logger,
        read_progress=writer.read_progress,
        read_start=partial(get_position_start, database_url),
        read_market_end=partial(get_market_end, market_limit.timestamp(), stock=stock),
        read_positions=partial(get_position_range, database_url),
        read_market=partial(fetch_market_returns, stock=stock, available_timestamp=market_limit.timestamp()),
        write_results=writer.write,
    )
