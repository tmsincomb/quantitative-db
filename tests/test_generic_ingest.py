import pytest

from quantdb.client import get_session
from quantdb.generic_ingest import (
    back_populate_tables,
    get_constraint_columns,
    get_or_create,
    object_as_dict,
    query_by_constraints,
)
from quantdb.models import Objects


def setup_test_session():
    """
    Returns a SQLAlchemy session connected to the test PostgreSQL database.
    The test DB is prepared by bin/prepare_test_db.sh and populated with the first 10 rows of each table.
    """
    return get_session(echo=False, test=True)


def test_object_as_dict():
    obj = Objects(id='00000000-0000-0000-0000-000000000001', id_type='dataset', id_file=None, id_internal=None)
    d = object_as_dict(obj)
    assert d['id'] == '00000000-0000-0000-0000-000000000001'
    assert d['id_type'] == 'dataset'


def test_get_or_create_creates_and_gets():
    session = setup_test_session()
    obj = Objects(id='00000000-0000-0000-0000-000000000002', id_type='dataset', id_file=None, id_internal=None)
    instance = get_or_create(session, obj)
    assert getattr(instance, 'id', None) == '00000000-0000-0000-0000-000000000002'
    # Should return the same instance if called again
    instance2 = get_or_create(session, obj)
    assert getattr(instance, 'id', None) == getattr(instance2, 'id', None)


def test_get_constraint_columns():
    cols = get_constraint_columns(Objects)
    assert any('id' in col for col in cols)


def test_query_by_constraints():
    session = setup_test_session()
    obj = Objects(id='00000000-0000-0000-0000-000000000003', id_type='dataset', id_file=None, id_internal=None)
    session.add(obj)
    session.commit()
    # Should find the object by unique constraint
    found = query_by_constraints(
        session, Objects(id='00000000-0000-0000-0000-000000000003', id_type='dataset', id_file=None, id_internal=None)
    )
    assert found is not None
    assert getattr(found, 'id', None) == '00000000-0000-0000-0000-000000000003'
    assert getattr(found, 'id_type', None) == 'dataset'


def test_back_populate_tables_adds_and_merges():
    session = setup_test_session()
    obj = Objects(id='00000000-0000-0000-0000-000000000004', id_type='dataset', id_file=None, id_internal=None)
    # Should add new
    out = back_populate_tables(session, obj)
    assert getattr(out, 'id', None) == '00000000-0000-0000-0000-000000000004'
    # Should merge existing
    obj2 = Objects(id='00000000-0000-0000-0000-000000000004', id_type='dataset', id_file=None, id_internal=None)
    out2 = back_populate_tables(session, obj2)
    assert getattr(out2, 'id', None) == getattr(out, 'id', None)


def test_get_or_create_back_populate():
    session = setup_test_session()
    obj = Objects(id='00000000-0000-0000-0000-000000000005', id_type='dataset', id_file=None, id_internal=None)
    instance = get_or_create(session, obj, back_populate={'id_file': 12345})
    assert getattr(instance, 'id_file', None) == 12345


def test_print_first_row_of_each_entity():
    """
    Prints the first row of each mapped entity in the test database.
    """
    import inspect as pyinspect

    session = setup_test_session()
    from quantdb import models

    printed = False
    for name, cls in vars(models).items():
        if pyinspect.isclass(cls) and hasattr(cls, '__table__') and hasattr(cls, '__mapper__'):
            try:
                row = session.query(cls).first()
                if row:
                    print(f'First row for {name}: {row}')
                    printed = True
            except Exception as e:
                print(f'Could not query {name}: {e}')
    if not printed:
        print('No rows found in any entity.')
