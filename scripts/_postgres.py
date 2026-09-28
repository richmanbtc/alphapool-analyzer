"""Isolated scratch databases on the configured test PostgreSQL service."""
from contextlib import closing, contextmanager
import os
from uuid import uuid4

import psycopg2
from psycopg2 import sql


@contextmanager
def temporary_database():
    database = 'test_' + uuid4().hex
    with closing(psycopg2.connect(connect_timeout=10)) as admin:
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        try:
            yield dict(os.environ, PGDATABASE=database)
        finally:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
