"""Repeatable analyzer benchmark with real PostgreSQL and offline BigQuery loads."""
import argparse
from contextlib import closing
from functools import wraps
import io
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
from tempfile import TemporaryDirectory, TemporaryFile
from threading import Event, Thread
from time import perf_counter
from types import SimpleNamespace
from uuid import uuid4

from ._postgres import temporary_database

ROOT = Path(__file__).resolve().parents[1]


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['crypto', 'stock'], required=True)
    parser.add_argument('--models', type=int)
    parser.add_argument('--symbols', type=int)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--worker', choices=['prepare', 'run'], help=argparse.SUPPRESS)
    parser.add_argument('--data', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.models = args.models if args.models is not None else (256 if args.mode == 'stock' else 24)
    args.symbols = args.symbols if args.symbols is not None else (32 if args.mode == 'stock' else 24)
    if min(args.models, args.symbols, args.repeats) < 1:
        parser.error('models, symbols and repeats must be positive')
    return args


def scenario(args):
    import pandas as pd
    now = pd.Timestamp('2025-03-03', tz='UTC')
    days = 57 if args.mode == 'stock' else 8
    times = pd.date_range(now - pd.Timedelta(days=days), now,
                          freq='B' if args.mode == 'stock' else '5min')
    return now, times


def prepare(args):
    import numpy as np
    import pandas as pd
    import psycopg2

    rng = np.random.default_rng(42)
    now, times = scenario(args)
    symbols = [f'asset{i}' for i in range(args.symbols)]
    portfolio_count = min(3, args.models - 1) if args.mode == 'crypto' else 0
    count = 0
    with closing(psycopg2.connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute((ROOT / 'tests/postgres/0001_initial.sql').read_text())
            buffer = io.StringIO()
            for model in range(args.models + portfolio_count):
                portfolio = model >= args.models
                model_id = f'pf-{model}' if portfolio else f'model{model}'
                held = rng.choice(symbols, size=min(3, len(symbols)), replace=False)
                for step, timestamp in enumerate(times):
                    positions, weights = {}, {}
                    if portfolio:
                        weights = {f'model{i}': 0.5 for i in (0, 1)}
                    else:
                        positions = {str(symbol): round(float(rng.normal(0, 0.2)), 4) for symbol in held}
                        if model == 0 or step % 31 == 0:
                            positions = dict.fromkeys(positions, 0.0)
                    buffer.write(f'{int(timestamp.timestamp())}\t{model_id}\t{json.dumps(positions)}\t{json.dumps(weights)}\n')
                    count += 1
                    if count % 1000 == 0:
                        buffer.seek(0)
                        cursor.copy_expert('COPY positions (timestamp, model_id, positions, weights) FROM STDIN', buffer)
                        buffer = io.StringIO()
            buffer.seek(0)
            cursor.copy_expert('COPY positions (timestamp, model_id, positions, weights) FROM STDIN', buffer)
            cursor.execute('ANALYZE positions')
        conn.commit()

    market_times = times.append(pd.DatetimeIndex([now + pd.Timedelta(days=1 if args.mode == 'stock' else 0, minutes=0 if args.mode == 'stock' else 5)]))
    frames = []
    for i, symbol in enumerate(symbols):
        prices = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, len(market_times))))
        frame = pd.DataFrame({'symbol': symbol if args.mode == 'stock' else symbol + 'USDT',
                              'timestamp': market_times, 'cl': prices})
        if args.mode == 'stock':
            frame['op'] = prices * 0.999
            frame['mo_cl'] = prices * 1.001
            frame['af_op'] = prices * 1.0005
            frame['adj_factor'] = 1.0
        if i == 0:
            frame.loc[frame.index % 17 == 1, 'cl'] = np.nan
        frames.append(frame)
    market = pd.concat(frames, ignore_index=True)
    market.to_parquet(args.data / 'market.parquet', index=False)
    return dict(input_rows=count, market_rows=len(market), timestamps=len(times),
                portfolio_models=portfolio_count, days=57 if args.mode == 'stock' else 8)


def measure(args):
    import numpy as np
    import pandas as pd
    from google.cloud import bigquery
    from google.cloud.bigquery import _pandas_helpers
    from src import runner
    from src.market import fetch_market_returns
    from src.result_writer import ResultWriter
    from src.position_reader import get_position_range

    stages, outputs, memory, pending = {}, {}, {}, {}

    def rss_mib():
        resident_pages = int(Path('/proc/self/statm').read_text().split()[1])
        return resident_pages * os.sysconf('SC_PAGE_SIZE') / (1024 * 1024)

    def lifetime_peak_mib():
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


    def timed(name, function):
        @wraps(function)
        def call(*values, **options):
            samples = [rss_mib()]
            before_peak = lifetime_peak_mib()
            stopped = Event()

            def sample():
                while not stopped.wait(0.005):
                    samples.append(rss_mib())

            sampler = Thread(target=sample, daemon=True)
            sampler.start()
            start = perf_counter()
            try:
                return function(*values, **options)
            finally:
                stages[name] = stages.get(name, 0) + perf_counter() - start
                samples.append(rss_mib())
                stopped.set()
                sampler.join()
                memory[name] = dict(start_rss_mib=samples[0], end_rss_mib=samples[-1],
                                    sampled_peak_rss_mib=max(samples),
                                    lifetime_peak_before_mib=before_peak,
                                    lifetime_peak_after_mib=lifetime_peak_mib())
        return call

    for name in ('prepare_positions', 'calc_model_ret', 'calc_position_frame', 'calc_return_frame'):
        setattr(runner, name, timed(name, getattr(runner, name)))

    def read_market(symbols, min_timestamp, max_timestamp):
        frame = pd.read_parquet(args.data / 'market.parquet')

        def fetch(provider_configs, min_timestamp):
            requested = provider_configs[0]['options']['symbols']
            selected = frame.loc[frame['symbol'].isin(requested) &
                                 (frame['timestamp'] >= pd.to_datetime(min_timestamp, unit='s', utc=True))]
            end = pd.to_datetime(max_timestamp, unit='s', utc=True)
            successor = selected.loc[selected['timestamp'] >= end].sort_values('timestamp').groupby('symbol').head(1)
            return [pd.concat([selected.loc[selected['timestamp'] < end], successor], ignore_index=True)]

        return fetch_market_returns(symbols, min_timestamp, stock=args.mode == 'stock',
                                    fetcher=SimpleNamespace(fetch=fetch), max_timestamp=max_timestamp)

    completed = SimpleNamespace(result=lambda: None)

    def serialize(frame, schema):
        with TemporaryFile() as output:
            _pandas_helpers.dataframe_to_parquet(frame, schema, output)
            return output.tell()

    def load(frame, target, job_config):
        name = 'positions' if 'position' in frame.columns else 'returns'
        size = timed('parquet_' + name, serialize)(frame, job_config.schema)
        pending[name] = (frame, size)
        return completed

    def validate(name, frame, size):
        values = frame['position' if name == 'positions' else 'ret']
        if not np.isfinite(values).all():
            raise ValueError('Non-finite benchmark output')
        if name == 'returns':
            flat = frame.loc[frame['model_id'] == 'model0', 'ret']
            if flat.empty or not flat.eq(0).all():
                raise ValueError('Flat model validation failed')
        outputs[name] = dict(rows=len(frame), parquet_bytes=size,
                             value_sum=float(values.sum()),
                             fingerprint=int(pd.util.hash_pandas_object(frame, index=False).sum()))

    writer = ResultWriter.__new__(ResultWriter)
    writer.dataset = bigquery.DatasetReference(uuid4().hex, uuid4().hex)
    writer.client = SimpleNamespace(
        create_table=lambda *a, **k: None, delete_table=lambda *a, **k: None,
        query=lambda *a, **k: completed, load_table_from_dataframe=load,
    )
    now, _ = scenario(args)
    start = perf_counter()
    runner.run_job(
        execution_time=now, start_time=now - pd.Timedelta(days=14 if args.mode == 'stock' else 2),
        end_time=now, stock=args.mode == 'stock', logger=SimpleNamespace(info=lambda *a: None, warning=lambda *a: None),
        read_positions=timed('postgres_read', lambda **kw: get_position_range('', **kw)),
        read_market=timed('market_read_and_returns', read_market),
        write_results=writer.write,
    )
    elapsed = perf_counter() - start
    pipeline_peak = lifetime_peak_mib()
    validation_start = perf_counter()
    for name, (frame, size) in pending.items():
        timed('validate_' + name, validate)(name, frame, size)
    validation_seconds = perf_counter() - validation_start
    if not outputs.get('returns', {}).get('rows'):
        raise ValueError('No returns produced')
    return dict(pipeline_seconds=elapsed, pipeline_peak_rss_mib=pipeline_peak,
                validation_seconds=validation_seconds, peak_rss_mib=lifetime_peak_mib(),
                stages_seconds=stages, stages_memory_mib=memory, outputs=outputs)


def benchmark(args):
    with temporary_database() as env, TemporaryDirectory() as directory:
        command = [sys.executable, '-m', 'scripts.benchmark', '--mode', args.mode,
                   '--models', str(args.models), '--symbols', str(args.symbols), '--data', directory]

        def worker(kind):
            start = perf_counter()
            result = subprocess.run(command + ['--worker', kind], env=env, cwd=ROOT, capture_output=True, text=True)
            if result.returncode:
                raise RuntimeError('Benchmark worker failed; check dependencies and worker implementation')
            data = json.loads(result.stdout)
            if kind == 'run':
                data['process_seconds'] = perf_counter() - start
            return data

        print('Preparing synthetic data and warming up...', file=sys.stderr, flush=True)
        inputs = worker('prepare')
        warmup = worker('run')
        runs = []
        for index in range(args.repeats):
            print(f'Measuring run {index + 1}/{args.repeats}...', file=sys.stderr, flush=True)
            result = worker('run')
            if result['outputs'] != warmup['outputs']:
                raise RuntimeError('Output changed between benchmark runs')
            runs.append(result)
        summary = {}
        for metric in ('pipeline_seconds', 'process_seconds', 'pipeline_peak_rss_mib',
                       'validation_seconds', 'peak_rss_mib'):
            values = [run[metric] for run in runs]
            summary[metric] = dict(median=statistics.median(values), min=min(values), max=max(values))
        report = dict(measurement_version=3, mode=args.mode, models=args.models, symbols=args.symbols, seed=42,
                      inputs=inputs, summary=summary, runs=runs)
        text = json.dumps(report, indent=2)
        if args.output:
            args.output.write_text(text + '\n', encoding='ascii')
        print(text)


if __name__ == '__main__':
    args = parse_args()
    if args.worker:
        print(json.dumps(prepare(args) if args.worker == 'prepare' else measure(args)))
    else:
        benchmark(args)
