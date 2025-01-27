import logging
from typing import Any, List

from sqlalchemy.orm import Session

from quantdb.client import get_session
from quantdb.ingest_tabular.models import DescriptorsQuant

logger = logging.getLogger(__name__)
session = get_session()


# Create a new DescriptorsQuant record
def create_descriptors_quant(db_session: Session, **kwargs: Any) -> DescriptorsQuant:
    """
    Create a new DescriptorsQuant record in the database if it does not already exist.

    Parameters
    ----------
    session : Session
        The SQLAlchemy session to use for database operations.
    **kwargs : dict[str, Any]
        Keyword arguments representing the fields and values for the DescriptorsQuant record.

    Returns
    -------
    DescriptorsQuant
        The newly created DescriptorsQuant record, or the existing record if it already exists.

    Notes
    -----
    This function checks for the existence of a record based on the provided fields and values.
    If a matching record is found, it is returned. Otherwise, a new record is created, added to
    the session, and committed to the database.
    """
    # Check if the record already exists based on unique constraints or identifying fields
    existing_record = db_session.query(DescriptorsQuant).filter_by(**kwargs).first()
    if existing_record:
        logger.info('Record already exists: %s', existing_record)
        return existing_record

    # Create a new record if it doesn't exist
    new_descriptor = DescriptorsQuant(**kwargs)
    db_session.add(new_descriptor)
    db_session.commit()
    return new_descriptor


# Read DescriptorsQuant records
def read_descriptors_quant(
    db_session: Session, record_id: int | None = None, **filters: dict[str, Any]
) -> Any | None | List[DescriptorsQuant]:
    """
    Retrieve descriptors from the database.
    This function queries the DescriptorsQuant table in the database using the provided SQLAlchemy session.
    It can retrieve a specific record by its ID or filter records based on additional criteria.

    Parameters
    ----------
    session : Session
        The SQLAlchemy session to use for the query.
    record_id : int, optional
        The ID of the specific record to retrieve. Defaults to None.
    **filters : dict, optional
        Additional keyword arguments to filter the query.

    Returns
    -------
    DescriptorsQuant or list of DescriptorsQuant
        The retrieved record if `record_id` is provided,
        a list of records if filters are provided, or all records if no filters are applied.
    """
    query = db_session.query(DescriptorsQuant)
    if record_id:
        return query.get(record_id)
    if filters:
        return query.filter_by(**filters).all()
    return query.all()


# Update a DescriptorsQuant record
def update_descriptors_quant(db_session: Session, record_id: int, **kwargs: dict[str, Any]) -> None | DescriptorsQuant:
    """
    Update a DescriptorsQuant record with the given record_id and kwargs.

    Parameters
    ----------
    session : Session
        SQLAlchemy session object.
    record_id : int
        ID of the record to update.
    kwargs : dict
        Fields to update in the record.

    Returns
    -------
    DescriptorsQuant
        The updated record or None if not found.
    """
    record = db_session.query(DescriptorsQuant).get(record_id)
    if not record:
        print(f'Record with id {record_id} not found')
        return None
    for key, value in kwargs.items():
        setattr(record, key, value)
    print(f'Updated record: {record}')
    session.commit()
    return record


# Delete a DescriptorsQuant record
def delete_descriptors_quant(db_session: Session, record_id: int) -> None | Any:
    """
    Deletes a DescriptorsQuant record from the database.

    Parameters
    ----------
    session : Session
        The SQLAlchemy session to use for the database operation.
    record_id : int
        The ID of the DescriptorsQuant record to delete.

    Returns
    -------
    None or Any
        Returns None if the record does not exist, otherwise returns the deleted record.
    """
    record = session.query(DescriptorsQuant).get(record_id)
    if not record:
        return None
    db_session.delete(record)
    db_session.commit()
    return record


# Example usage
if __name__ == '__main__':
    # Create a new record
    new_record = create_descriptors_quant(
        session,
        label='Example Label',
        shape='scalar',
        aggregation_type='instance',
        unit=1,
        aspect=1,
        domain=1,
    )
    print(f'Created: {new_record}')

    # Read records
    records = read_descriptors_quant(session)
    print(f'Read: {records}')

    # Update a record
    updated_record = update_descriptors_quant(session, new_record.id, label='Updated Label')
    print(f'Updated: {updated_record}')

    # # Delete a record
    # deleted_record = delete_descriptors_quant(session, new_record.id)
    # print(f"Deleted: {deleted_record}")
