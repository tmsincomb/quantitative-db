from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quantdb.config import auth
from quantdb.utils import dbUri


def get_session(echo: bool = True, test: bool = False) -> Session:
    """
    Get a SQLAlchemy session for the main or test database.

    Parameters
    ----------
    echo : bool, optional
        If True, SQLAlchemy will log all statements.
    test : bool, optional
        If True, connect to the test database (quantdb_test or as configured).

    Returns
    -------
    session : sqlalchemy.orm.Session
        The database session.
    """
    # pull in the db connection info
    dbkwargs = {k: auth.get(f'db-{k}') for k in ('user', 'host', 'port', 'database')}
    dbkwargs['dbuser'] = dbkwargs.pop('user')
    dbkwargs['dbuser'] = 'quantdb-test-admin' if test else dbkwargs['dbuser']
    if test:
        dbkwargs['database'] = auth.get('test-db-database') or 'quantdb_test'
    engine = create_engine(dbUri(**dbkwargs))  # type: ignore
    engine.echo = echo
    session = Session(engine)
    return session
