import pandas as pd
from google.cloud import bigquery

from ..settings import get_config_value


def latest_timestamp(table, available_timestamp):
    client = bigquery.Client(project=get_config_value('project_id'))
    query = f"SELECT MAX(timestamp) AS timestamp FROM `{get_config_value('market_dataset')}.{table}` WHERE timestamp <= @available"
    rows = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('available', 'FLOAT64', available_timestamp),
    ])).result()
    return next(iter(rows))['timestamp']


def fetch(options=None, min_timestamp=None):
    client = bigquery.Client(project=get_config_value('project_id'))
    conditions, parameters = [], []
    if min_timestamp is not None and not options.get('ignore_min_timestamp', False):
        conditions.append('timestamp >= @start')
        parameters.append(bigquery.ScalarQueryParameter('start', 'FLOAT64', min_timestamp))
    if options.get('symbols') is not None:
        conditions.append('symbol IN UNNEST(@symbols)')
        parameters.append(bigquery.ArrayQueryParameter('symbols', 'STRING', options['symbols']))
    if options.get('available_timestamp') is not None:
        conditions.append('timestamp <= @available')
        parameters.append(bigquery.ScalarQueryParameter('available', 'FLOAT64', options['available_timestamp']))
    where = ' AND '.join(conditions) or 'TRUE'
    source = f"`{get_config_value('market_dataset')}.{options['table']}`"
    columns = ', '.join(f'`{column}`' for column in options['columns'])
    if options.get('max_timestamp') is None:
        query = f'SELECT {columns} FROM {source} WHERE {where}'
    else:
        parameters.append(bigquery.ScalarQueryParameter('end', 'FLOAT64', options['max_timestamp']))
        # One successor per symbol handles weekends and long exchange holidays.
        query = (f'SELECT {columns} FROM {source} WHERE {where} AND timestamp < @end UNION ALL '
                 f'(SELECT {columns} FROM {source} WHERE {where} AND timestamp >= @end '
                 'QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp) = 1)')
    df = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=parameters)).to_dataframe()
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    return df
