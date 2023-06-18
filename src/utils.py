from sqlalchemy import create_engine
from sqlalchemy.sql import text


def vacuum(database_url):
    engine = create_engine(database_url)
    with engine.connect() as conn:
        statement = text('VACUUM')
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(statement)
