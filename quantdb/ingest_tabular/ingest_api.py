import typing
from dataclasses import asdict, dataclass
from typing import Any, AnyStr, ClassVar, Dict, Protocol

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.sql import text

Base = declarative_base()


class UUID(str):
    pass


class Dataclass(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]


@dataclass
class Unit:
    __table__ = 'units'
    id = Column(Integer, primary_key=True)
    label = Column(String, nullable=True)
    iri = Column(String)


@dataclass
class Objects:
    __table__ = 'objects'
    id: UUID
    id_type: str
    id_file: int | None = None
    id_internal: UUID | None = None


@dataclass
class DescriptorQuant:
    __table__ = 'descriptors_quant'
    label: str
    aggregation_type: str
    unit: str | None = None
    aspect: int | None = None
    domain: str | None = None
    description: str | None = None
    curator_note: str | None = None
    shape: str = 'scalar'


@dataclass
class ValuesQuant:
    __table__ = 'values_quant'
    value: float
    object: UUID
    desc_inst: int
    desc_quant: int
    value_blob: float
    instance: int | None = None
    orig_value: str | None = None
    orig_units: str | None = None


@dataclass
class DescriptorInst:
    __table__ = 'descriptors_inst'
    label: str
    description: str | None = None
    iri: str | None = None


class ObjDescQuant:
    __table__ = 'obj_desc_quant'
    object: UUID
    desc_inst: int
    desc_quant: int


class Ingest:
    def __init__(self, session: Session):
        self.session = session

    def execute_insert(self, query: str, dataclasses: typing.Sequence[Dataclass]) -> None:
        """Execute a query with dataclasses as parameters

        Parameters
        ----------
        query : str
            sql query string
        dataclasses : typing.Sequence[Dataclass]
            a list of SQL table dataclasses to be inserted
        """
        statement = text(query)
        params = [asdict(dataclass) for dataclass in dataclasses]
        try:
            result = self.session.execute(statement=statement, params=params)
            self.session.commit()
            return result
        except Exception as e:
            print(f'Error: {e}')
            self.session.rollback()

    def insert_units(self, units: typing.Sequence[Unit]) -> None:
        query = r'INSERT INTO units (label, iri) VALUES (:label, :iri)'
        self.execute_insert(query=query, dataclasses=units)

    def insert_objects(self, objects: typing.Sequence[Objects]) -> None:
        query = r'INSERT INTO objects (id, id_type) VALUES (:id, :id_type) ON CONFLICT DO NOTHING'
        self.execute_insert(query=query, dataclasses=objects)

    def insert_descriptors_quant(self, descriptor_quants: typing.Sequence[DescriptorQuant]) -> None:
        query = r'INSERT INTO descriptors_quant (label, domain, aspect, unit, aggregation_type) VALUES (:label, :domain, :aspect, :unit, :aggregation_type)'
        self.execute_insert(query=query, dataclasses=descriptor_quants)

    def insert_values_quant(
        self,
        values_quant: typing.Sequence[ValuesQuant],
    ):
        pass


# ingest.insert_units(
#     units=[
#         Unit(
#             label="pixel",
#             iri="http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/pixel",
#         ),
#     ]
# )
