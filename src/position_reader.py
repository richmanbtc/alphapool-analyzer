from contextlib import closing

import pandas as pd
import psycopg2


def get_position_start(database_url):
    with closing(psycopg2.connect(database_url)) as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT MIN(timestamp) FROM positions')
            value = cursor.fetchone()[0]
    return None if value is None else pd.to_datetime(value, unit='s', utc=True)


def get_position_range(database_url, *, min_timestamp, max_timestamp, seed_min_timestamp=None):
    """Read a bounded interval plus the last snapshot per model before it."""
    with closing(psycopg2.connect(database_url)) as conn:
        with conn.cursor() as cursor:
            seed_condition = '' if seed_min_timestamp is None else ' AND timestamp >= %s'
            params = (min_timestamp, max_timestamp, min_timestamp)
            if seed_min_timestamp is not None:
                params += (seed_min_timestamp,)
            cursor.execute(
                'SELECT timestamp, model_id, positions, weights FROM positions '
                'WHERE timestamp >= %s AND timestamp < %s UNION ALL '
                '(SELECT DISTINCT ON (model_id) timestamp, model_id, positions, weights '
                f'FROM positions WHERE timestamp < %s{seed_condition} ORDER BY model_id, timestamp DESC)',
                params,
            )
            frame = pd.DataFrame(cursor.fetchall(), columns=[
                'timestamp', 'model_id', 'positions', 'weights',
            ])
    frame['timestamp'] = pd.to_datetime(frame['timestamp'], unit='s', utc=True)
    return frame
