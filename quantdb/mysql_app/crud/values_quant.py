import decimal
import logging
import uuid
from typing import Any, List

from sqlalchemy import Uuid
from sqlalchemy.orm import Session, class_mapper

from quantdb.client import get_session
from quantdb.ingest_tabular.models import (
    DescriptorsInst,
    DescriptorsQuant,
    ObjDescInst,
    ObjDescQuant,
    Objects,
    ValuesInst,
    ValuesQuant,
)

logger = logging.getLogger(__name__)
session = get_session()


def model_to_dict(instance) -> None | dict[str, Any]:
    """
    Convert a SQLAlchemy model instance to a dictionary.
    """
    if not instance:
        return None

    # Get the class mapper
    mapper = class_mapper(instance.__class__)  # type: ignore

    # Extract column names and their values
    columns = [column.key for column in mapper.columns]
    instance_dict = {column: getattr(instance, column) for column in columns}  # type: ignore

    return instance_dict


def create_value_quant(
    db: Session,
    value: decimal.Decimal,
    object: str | uuid.UUID,
    desc_quant: int | str | DescriptorsQuant,
    desc_inst: int | str | DescriptorsInst,
    value_blob: None | dict[str, Any] = None,
    instance: None | int = None,
    orig_value: None | str = None,
    orig_units: None | str = None,
) -> ValuesQuant:
    """
    Creates a new value record in the database.

    Parameters
    ----------
    db : Session
        The database session.
    value : decimal.Decimal
        The numeric value associated with the entry.
    object : uuid.UUID
        The UUID of the related object.
    desc_inst : int, optional
        The foreign key referencing `descriptors_inst.id` (default is None).
    desc_quant : int
        The foreign key referencing `descriptors_quant.id`.
    value_blob : dict, optional
        Additional value data as a JSONB object (default is None).
    instance : int, optional
        The foreign key referencing `values_inst.id` (default is None).
    orig_value : str, optional
        The original value as a string (default is None).
    orig_units : str, optional
        The original units of the value (default is None).

    Returns
    -------
    ValuesQuant
        The newly created ValuesQuant object.
    """
    # Check if the object exists, if not create it
    logger.debug("Checking if object exists in the database.")
    object_record = db.query(Objects).filter(Objects.id == object).first()
    if not object_record:
        logger.info(f"Object with id {object} not found. Creating new object.")
        object_record = Objects(id=object)
        db.add(object_record)
        db.commit()
        db.refresh(object_record)
        logger.info(f"Created new object with id {object_record.id}")

    if desc_inst is None:
        logger.debug("desc_inst not provided. Attempting to find matching DescriptorsInst.")
        # Propagate DescriptorsInst if desc_inst was not given
        desc_inst_record = db.query(DescriptorsInst).filter(DescriptorsInst.object == object).first()
        if desc_inst_record:
            desc_inst = desc_inst_record.id
            logger.info(f"Found matching DescriptorsInst with id {desc_inst}")
        else:
            logger.error("desc_inst is required if no matching DescriptorsInst found for the given object.")
            raise ValueError("desc_inst is required if no matching DescriptorsInst found for the given object.")

    logger.debug("Creating new ValuesQuant record.")
    new_value_quant = ValuesQuant(
        value=value,
        object=object,
        desc_inst=desc_inst,
        desc_quant=desc_quant,
        value_blob=value_blob,
        instance=instance,
        orig_value=orig_value,
        orig_units=orig_units,
    )
    db.add(new_value_quant)
    db.commit()
    db.refresh(new_value_quant)
    logger.info(f"Created new ValuesQuant record with id {new_value_quant.id}")
    return new_value_quant


def get_value_quant_by_id(db: Session, value_quant_id: int):
    """
    Retrieves a value record by its ID.

    Parameters
    ----------
    db : Session
        The database session.
    value_quant_id : int
        The ID of the value to retrieve.

    Returns
    -------
    ValuesQuant or None
        The ValuesQuant object if found, otherwise None.
    """
    return db.query(ValuesQuant).filter(ValuesQuant.id == value_quant_id).first()


def get_all_value_quants(db: Session):
    """
    Retrieves all value records.

    Parameters
    ----------
    db : Session
        The database session.

    Returns
    -------
    list of ValuesQuant
        A list of all ValuesQuant objects.
    """
    return db.query(ValuesQuant).all()


def update_value_quant(
    db: Session,
    value_quant_id: int,
    value: decimal.Decimal = None,
    value_blob: dict = None,
    instance: int = None,
    orig_value: str = None,
    orig_units: str = None,
):
    """
    Updates an existing value record.

    Parameters
    ----------
    db : Session
        The database session.
    value_quant_id : int
        The ID of the value to update.
    value : decimal.Decimal, optional
        The updated numeric value (default is None).
    value_blob : dict, optional
        The updated additional value data as a JSONB object (default is None).
    instance : int, optional
        The updated foreign key referencing `values_inst.id` (default is None).
    orig_value : str, optional
        The updated original value as a string (default is None).
    orig_units : str, optional
        The updated original units of the value (default is None).

    Returns
    -------
    ValuesQuant or None
        The updated ValuesQuant object if found, otherwise None.
    """
    value_quant = get_value_quant_by_id(db, value_quant_id)
    if value_quant:
        if value is not None:
            value_quant.value = value
        if value_blob is not None:
            value_quant.value_blob = value_blob
        if instance is not None:
            value_quant.instance = instance
        if orig_value is not None:
            value_quant.orig_value = orig_value
        if orig_units is not None:
            value_quant.orig_units = orig_units
        db.commit()
        db.refresh(value_quant)
    return value_quant


def delete_value_quant(db: Session, value_quant_id: int):
    """
    Deletes a value record.

    Parameters
    ----------
    db : Session
        The database session.
    value_quant_id : int
        The ID of the value to delete.

    Returns
    -------
    bool
        True if the value was deleted, False otherwise.
    """
    value_quant = get_value_quant_by_id(db, value_quant_id)
    if value_quant:
        db.delete(value_quant)
        db.commit()
        return True
    return False


if __name__ == "__main__":
    # Create a new value record
    new_value_quant = create_value_quant(
        db=session,
        value=decimal.Decimal("123.45"),
        object=uuid.UUID("38215e04-e8ab-4a55-8291-46839f96ed1b"),
        desc_inst=15,  # Ensure this is a valid foreign key reference
        desc_quant=2,  # Ensure this is a valid foreign key reference
        value_blob={"additional_info": "some extra data"},
        orig_value="123.45",
        orig_units="meters",
    )

    # Get a value record by ID
    # value_quant = get_value_quant_by_id(session, 1)
    # print(model_to_dict(value_quant))

    # Get all value records
    # all_value_quants = get_all_value_quants(db)

    # # Update a value record
    # updated_value_quant = update_value_quant(
    #     db=db,
    #     value_quant_id=1,
    #     value=456.78,
    #     value_blob={"additional_info": "updated extra data"}
    # )

    # # Delete a value record
    # was_deleted = delete_value_quant(db, 1)
