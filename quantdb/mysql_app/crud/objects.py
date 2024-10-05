import logging
from typing import Any, List

from sqlalchemy.orm import Session

from quantdb.client import get_session
from quantdb.ingest_tabular.models import Objects

logger = logging.getLogger(__name__)
session = get_session()


# Create a new Objects record
def create_object(session: Session, **kwargs):
    new_record = Objects(**kwargs)
    session.add(new_record)
    session.commit()
    return new_record


# Read Objects records
def read_objects(session: Session, record_id=None, **filters):
    query = session.query(Objects)
    if record_id:
        return query.get(record_id)
    if filters:
        return query.filter_by(**filters).all()
    return query.all()


# Update an Objects record
def update_object(session: Session, record_id, **kwargs):
    record = session.query(Objects).get(record_id)
    if not record:
        return None
    for key, value in kwargs.items():
        setattr(record, key, value)
    session.commit()
    return record


# Delete an Objects record
def delete_object(session: Session, record_id):
    record = session.query(Objects).get(record_id)
    if not record:
        return None
    session.delete(record)
    session.commit()
    return record


# Example usage
if __name__ == "__main__":
    # Create a new record
    new_record = create_object(
        session,
        id="some-uuid",
        id_type="dataset",
        id_file=123,
        id_internal="another-uuid",
    )
    print(f"Created: {new_record}")

    # Read records
    records = read_objects(session)
    print(f"Read: {records}")

    # Update a record
    updated_record = update_object(session, new_record.id, id_file=456)
    print(f"Updated: {updated_record}")

    # Delete a record
    deleted_record = delete_object(session, new_record.id)
    print(f"Deleted: {deleted_record}")
