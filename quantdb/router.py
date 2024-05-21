from __future__ import annotations

from functools import lru_cache
from typing import Any, Generator

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session  # type: ignore

from quantdb.config import Settings

from quantdb import mysql_app

app = FastAPI()


@lru_cache()
def get_settings():
    """
    Get settings from mongodb - alternative for test and production credientials

    Returns
    -------
    Settings
        pydantic BaseSettings model with MongoDB credentials
    """
    return Settings()


def get_mysql_db() -> Generator[Session, Any, None]:
    db = mysql_app.database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get(
    "/example",
    status_code=200,
)
def get_example(
    mysql_db: Session = Depends(get_mysql_db),  # type: ignore
):
    query = text(
        """
        select * from <table_name> limit 10
    """
    )
    result = mysql_db.execute(query)
    data = result.mappings().all()
    return data

    def default_flow(endpoint, record_type, query_fun, json_fun, alt_query_fun=None):
        try:
            kwargs = getArgs(request, endpoint, dev=dev)
        except exc.UnknownArg as e:
            return json.dumps({"error": e.args[0], "http_response_status": 422}), 422
        except Exception as e:
            breakpoint()
            raise e

        def gkw(k):
            return k in kwargs and kwargs[k]

        if gkw("include-unused"):
            query_fun = alt_query_fun

        # FIXME record_type is actually determined entirely in query_fun right now
        try:
            query, params = query_fun(endpoint, kwargs)
        except Exception as e:
            breakpoint()
            raise e

        if gkw("return-query"):
            return query

        try:
            res = session.execute(sql_text(query), params)
        except Exception as e:
            breakpoint()
            raise e

        try:
            out = json_fun(record_type, res, prov=("prov" in kwargs and kwargs["prov"]))
            resp = json.dumps(wrap_out(endpoint, kwargs, out), cls=JEncode), 200, {"Content-Type": "application/json"}
        except Exception as e:
            breakpoint()
            raise e

        return resp


# @app.route(f"{bp}/test")
# def route_test_check():
#     "objects with derived values that match all criteria"
#     return "testing-api"

# @app.route(f"{bp}/objects")
# def route_1_objects():
#     "objects with derived values that match all criteria"
#     return default_flow("objects", "object", main_query, to_json)

# @app.route(f"{bp}/desc/inst")
# @app.route(f"{bp}/descriptors/inst")
# @app.route(f"{bp}/classes")
# def route_1_desc_inst():
#     def query(endpoint, kwargs):
#         return (
#             (
#                 "SELECT "
#                 "id.iri, "
#                 "id.label, "
#                 "idpar.label as subclassof"
#                 """
# FROM descriptors_inst AS id
# LEFT OUTER JOIN class_parent AS ip ON ip.id = id.id
# LEFT OUTER JOIN descriptors_inst AS idpar ON idpar.id = ip.parent
# """
#             ),
#             {},
#         )

#     return default_flow("desc/inst", "desc-inst", main_query, to_json, alt_query_fun=query)

# @app.route(f"{bp}/desc/cat")
# @app.route(f"{bp}/descriptors/cat")
# @app.route(f"{bp}/predicates")
# def route_1_desc_cat():
#     def query(endpoint, kwargs):
#         return (
#             "select "
#             "cd.label, "
#             "cdid.label AS domain, "
#             "cd.range, "
#             "cd.description "
#             "from descriptors_cat as cd "
#             "left outer join descriptors_inst as cdid on cdid.id = cd.domain"
#         ), {}

#     return default_flow(
#         "desc/cat", "desc-cat", main_query, to_json, alt_query_fun=query
#     )  # TODO likely need different args e.g. to filter by desc_inst

# @app.route(f"{bp}/desc/quant")
# @app.route(f"{bp}/descriptors/quant")
# def route_1_desc_quant():
#     def query(endpoint, kwargs):
#         return (
#             "select "
#             "qd.label, "
#             "id.label AS domain, "
#             "qd.shape, "
#             "qd.aggregation_type as agg_type, "
#             "a.label AS aspect, "
#             "u.label AS unit, "
#             "qd.description "
#             "from descriptors_quant as qd "
#             "left outer join descriptors_inst as id on id.id = qd.domain "
#             "left outer join units as u on u.id = qd.unit "
#             "join aspects as a on a.id = qd.aspect"
#         ), {}

#     return default_flow(
#         "desc/quant", "desc-quant", main_query, to_json, alt_query_fun=query
#     )  # TODO likely need different args e.g. to filter by desc_inst

# @app.route(f"{bp}/values/inst")
# @app.route(f"{bp}/instances")
# def route_1_val_inst():
#     "instances associated with values that match all critiera"
#     return default_flow("values/inst", "instance", main_query, to_json)

# @app.route(f"{bp}/values")
# @app.route(f"{bp}/values/cat-quant")
# def route_1_val_cat_quant():
#     return default_flow("values/cat-quant", None, main_query, to_json)

# @app.route(f"{bp}/values/cat")
# def route_1_val_cat():
#     return default_flow("values/cat", "value-cat", main_query, to_json)

# @app.route(f"{bp}/values/quant")
# def route_1_val_quant():
#     return default_flow("values/quant", "value-quant", main_query, to_json)

# @app.route(f"{bp}/terms")
# @app.route(f"{bp}/controlled-terms")
# def route_1_cterms():
#     def query(endpoint, kwargs):
#         return ("select " "ct.iri, " "ct.label " "from controlled_terms as ct"), {}

#     return default_flow("terms", "term", main_query, to_json, alt_query_fun=query)

# @app.route(f"{bp}/units")
# def route_1_units():
#     def query(endpoint, kwargs):
#         return ("select " "u.iri, " "u.label " "from units as u"), {}

#     return default_flow("units", "unit", main_query, to_json, alt_query_fun=query)

# @app.route(f"{bp}/aspects")
# def route_1_aspects():
#     def query(endpoint, kwargs):
#         return (
#             (
#                 "SELECT "
#                 "a.iri, "
#                 "a.label, "
#                 "aspar.label AS subclassof "
#                 """
# FROM aspects AS a
# LEFT OUTER JOIN aspect_parent AS ap ON ap.id = a.id
# LEFT OUTER JOIN aspects AS aspar ON aspar.id = ap.parent
# """
#             ),
#             {},
#         )

#     return default_flow("aspects", "aspect", main_query, to_json, alt_query_fun=query)

# return app
