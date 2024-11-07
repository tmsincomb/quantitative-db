from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quantdb.config import auth
from quantdb.utils import dbUri


def get_session(echo: bool = True) -> Session:
    # pull in the db connection info
    dbkwargs = {
        k: auth.get(f"db-{k}") for k in ("user", "host", "port", "database")
    }  # TODO integrate with cli options
    # custom user variable needed
    dbkwargs["dbuser"] = dbkwargs.pop("user")
    dbkwargs["dbuser"] = "quantdb-test-admin"
    # create connection env with DB
    engine = create_engine(dbUri(**dbkwargs))  # type: ignore
    # bool: echo me
    engine.echo = echo
    # use connection env as unique session
    session = Session(engine)
    return session
