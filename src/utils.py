from sqlalchemy import create_engine
from sqlalchemy.sql import text


def vacuum(database_url):
    engine = create_engine(database_url)
    with engine.connect() as conn:
        statement = text('VACUUM')
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(statement)


def drop_table(database_url, table_name):
    engine = create_engine(database_url)
    with engine.connect() as conn:
        statement = text(f'DROP TABLE IF EXISTS {table_name}')
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(statement)
