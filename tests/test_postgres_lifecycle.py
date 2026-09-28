from unittest import TestCase
from unittest.mock import patch

from scripts._postgres import temporary_database


class TemporaryDatabaseTests(TestCase):
    @patch('scripts._postgres.psycopg2.connect')
    def test_isolated_database_and_cleanup(self, connect):
        admin = connect.return_value
        cursor = admin.cursor.return_value.__enter__.return_value
        with temporary_database() as first:
            with temporary_database() as second:
                self.assertNotEqual(first['PGDATABASE'], second['PGDATABASE'])
                self.assertTrue(first['PGDATABASE'].startswith('test_'))
                self.assertTrue(admin.autocommit)
        queries = [str(call.args[0]) for call in cursor.execute.call_args_list]
        self.assertEqual(len(queries), 4)
        self.assertIn('CREATE DATABASE', queries[0])
        self.assertIn(first['PGDATABASE'], queries[0])
        self.assertIn('DROP DATABASE', queries[-1])
        self.assertIn(first['PGDATABASE'], queries[-1])
        self.assertEqual(admin.close.call_count, 2)

    @patch('scripts._postgres.psycopg2.connect')
    def test_body_failure_still_drops_database(self, connect):
        admin = connect.return_value
        cursor = admin.cursor.return_value.__enter__.return_value
        with self.assertRaisesRegex(RuntimeError, 'worker failed'):
            with temporary_database():
                raise RuntimeError('worker failed')
        self.assertIn('DROP DATABASE', str(cursor.execute.call_args.args[0]))
        admin.close.assert_called_once()

    @patch('scripts._postgres.psycopg2.connect')
    def test_create_failure_does_not_drop_database(self, connect):
        admin = connect.return_value
        cursor = admin.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = RuntimeError('create failed')
        with self.assertRaisesRegex(RuntimeError, 'create failed'):
            with temporary_database():
                self.fail('creation should fail before yielding')
        cursor.execute.assert_called_once()
        admin.close.assert_called_once()
