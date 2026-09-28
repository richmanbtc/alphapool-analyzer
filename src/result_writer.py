from datetime import datetime, timedelta, timezone
from uuid import uuid4
import logging

from google.cloud import bigquery

from .settings import get_config_value


COMMON_SCHEMA = [
    bigquery.SchemaField('model_id', 'STRING'),
    bigquery.SchemaField('timestamp', 'TIMESTAMP'),
]
SCHEMAS = {
    'analyzer_positions': COMMON_SCHEMA + [
        bigquery.SchemaField('symbol', 'STRING'),
        bigquery.SchemaField('position', 'FLOAT64'),
        bigquery.SchemaField('position_diff', 'FLOAT64'),
    ],
    'analyzer_rets': COMMON_SCHEMA + [bigquery.SchemaField('ret', 'FLOAT64')],
}


class ResultWriter:
    def __init__(self):
        dataset_id = get_config_value('output_dataset') or get_config_value('market_dataset')
        if not dataset_id:
            raise ValueError('An output or market dataset is required')
        self.client = bigquery.Client(project=get_config_value('project_id'))
        self.dataset = bigquery.DatasetReference.from_string(
            dataset_id, default_project=self.client.project,
        )

    def _ensure_progress(self):
        table = self.dataset.table('analyzer_progress')
        self.client.create_table(bigquery.Table(table, schema=[
            bigquery.SchemaField('mode', 'STRING'),
            bigquery.SchemaField('completed_until', 'TIMESTAMP'),
            bigquery.SchemaField('updated_at', 'TIMESTAMP'),
        ]), exists_ok=True)
        return table

    def read_progress(self, mode):
        table = self._ensure_progress()
        rows = self.client.query(
            f'SELECT MAX(completed_until) AS completed_until FROM `{table}` WHERE mode = @mode',
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter('mode', 'STRING', mode),
            ]),
        ).result()
        return next(iter(rows))['completed_until']

    def write(self, positions, returns, min_update_time, end_time, *, retain_since,
              mode, completed_until=None):
        """Replace one interval and checkpoint atomically, rejecting stale writers."""
        progress = self._ensure_progress()
        staging = []
        statements = [
            'BEGIN TRANSACTION;',
            f'ASSERT (SELECT MAX(completed_until) FROM `{progress}` WHERE mode = @mode) '
            "IS NOT DISTINCT FROM @completed_until AS 'Progress changed; retry the job';",
        ]
        try:
            for name, frame in (
                ('analyzer_positions', positions), ('analyzer_rets', returns),
            ):
                schema = SCHEMAS[name]
                target = self.dataset.table(name)
                table = bigquery.Table(target, schema=schema)
                table.time_partitioning = bigquery.TimePartitioning(field='timestamp')
                table.clustering_fields = ['model_id']
                self.client.create_table(table, exists_ok=True)
                condition = '(timestamp >= @min_update_time AND timestamp < @end_time)'
                if name == 'analyzer_positions':
                    condition += ' OR timestamp < @retain_since'
                statements.append(f'DELETE FROM `{target}` WHERE {condition};')
                if frame.empty:
                    continue
                source = self.dataset.table('_analyzer_stage_' + uuid4().hex)
                staging.append(source)
                table = bigquery.Table(source, schema=schema)
                table.expires = datetime.now(timezone.utc) + timedelta(days=1)
                self.client.create_table(table)
                self.client.load_table_from_dataframe(
                    frame, source, job_config=bigquery.LoadJobConfig(schema=schema),
                ).result()
                columns = ', '.join(f'`{field.name}`' for field in schema)
                statements.append(
                    f'INSERT INTO `{target}` ({columns}) SELECT {columns} FROM `{source}`;'
                )
            statements.append(
                f'INSERT INTO `{progress}` (mode, completed_until, updated_at) '
                'VALUES (@mode, GREATEST(@end_time, COALESCE(@completed_until, @end_time)), CURRENT_TIMESTAMP());'
            )
            statements.append('COMMIT TRANSACTION;')
            self.client.query(
                '\n'.join(statements),
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter('min_update_time', 'TIMESTAMP', min_update_time),
                    bigquery.ScalarQueryParameter('end_time', 'TIMESTAMP', end_time),
                    bigquery.ScalarQueryParameter('retain_since', 'TIMESTAMP', retain_since),
                    bigquery.ScalarQueryParameter('mode', 'STRING', mode),
                    bigquery.ScalarQueryParameter('completed_until', 'TIMESTAMP', completed_until),
                ]),
            ).result()
        finally:
            for table in staging:
                try:
                    self.client.delete_table(table, not_found_ok=True)
                except Exception:
                    logging.getLogger(__name__).warning('Staging cleanup failed; table will expire automatically')
