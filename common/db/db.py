from contextlib import contextmanager

import sqlalchemy as sql
import sqlalchemy.orm as orm

def engine(connection_string: str):
    return sql.create_engine(connection_string )

def sessionmaker(engine: sql.engine.Engine):
    return orm.sessionmaker(engine)

@contextmanager
def Session(sm: orm.sessionmaker):
    session = sm()
    try:
        yield session
    finally:
        session.close()