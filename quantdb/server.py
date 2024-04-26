import sys
import json
import uuid
import pprint
from decimal import Decimal
from datetime import datetime
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text as sql_text
import config


# from pyontutils.utils_fast import isoformat
def isoformat(datetime_instance, timespec='auto'):
    kwargs = {}
    if isinstance(datetime_instance, datetime):
        # don't pass timespec if type is not date not datetime
        kwargs['timespec'] = timespec

    return (datetime_instance
            .isoformat(**kwargs)
            .replace('.', ',')
            .replace('+00:00', 'Z'))


# log = log.getChild('server')

def test():
    app = run_server()
    client = app.test_client()
    runner = app.test_cli_runner()

    dataset_uuid = 'aa43eda8-b29a-4c25-9840-ecbd57598afc'
    some_object_id = '414886a9-9ec7-447e-b4d8-3ae42fda93b7'  # XXX FAKE
    actual_package_uuid = '15bcbcd5-b054-40ef-9b5c-6a260d441621'
    base = 'http://localhost:8989/api/1/'
    urls = (
        f'{base}values/inst?dataset={dataset_uuid}',
        f'{base}values/inst?dataset={dataset_uuid}&aspect=distance&aspect=time',
        f'{base}values/inst?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5',


        f'{base}objects?dataset={dataset_uuid}',
        f'{base}objects?dataset={dataset_uuid}&aspect=distance',
        f'{base}objects?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5',  # expect nothing
        f'{base}objects?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5&union-cat-quant=true',

        f'{base}values/quant?dataset={dataset_uuid}&aspect=distance',
        f'{base}values/quant?object={actual_package_uuid}&aspect=distance',

        f'{base}values/cat?object={actual_package_uuid}',
        f'{base}values/cat?object={actual_package_uuid}&union-cat-quant=true',  # shouldn't need it in this case

        f'{base}values/cat-quant?object={actual_package_uuid}',
        f'{base}values/cat-quant?object={actual_package_uuid}&union-cat-quant=true',

        f'{base}values?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5',
        f'{base}values?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5&union-cat-quant=true',

        f'{base}values?object={actual_package_uuid}',
        f'{base}values?object={actual_package_uuid}&union-cat-quant=true',

        f'{base}values/inst?object={actual_package_uuid}',
        f'{base}values/inst?object={actual_package_uuid}&union-cat-quant=true',

        f'{base}desc/inst',
        f'{base}desc/cat',
        f'{base}desc/quant',
    )
    resps = []
    for url in urls:
        resp = client.get(url)
        resp.ok = resp.status_code < 400
        resp.url = resp.request.url
        resp.content = resp.data
        resps.append(json.loads(resp.data.decode()))

    pprint.pprint(resps, width=120)
    breakpoint()


class JENcode(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, datetime):
            return isoformat(obj)
        elif isinstance(obj, Decimal):
            # FIXME TODO precision etc. see comment on quant_descriptors
            return float(obj)

        return json.JSONEncoder.default(self, obj)


url_sql_where = (  # TODO arity spec here

    # dupes overwrite params but that is ok, this way we get the correct table alias for both cases
    ('object', 'object', 'cv.object_id = any(:object)', 'cat'),  # XXX should not use this outside values/ unless we left outer due to intersect ?
    ('object', 'object', 'qv.object_id = any(:object)', 'quant'),  # XXX should not use this outside values/ unless we left outer due to intersect ?

    ('desc-inst', 'desc_inst', 'idin.label = any(:desc_inst)', 'both'),
    ('dataset', 'dataset', 'im.dataset = :dataset', 'both'),
    ('inst', 'inst', 'im.formal_id = any(:inst)', 'both'),
    ('subject', 'subject', 'im.sub_id = any(:subject)', 'both'),
    ('sample', 'sample', 'im.sam_id = any(:sample)', 'both'),

    ('desc-cat', 'desc_cat', 'cd.label = any(:desc_cat)', 'cat'),

    ('value-cat', 'value_cat', 'ct.label = any(:value_cat)', 'cat'),
    ('value-cat-open', 'value_cat_open', 'cv.value_open = any(:value_cat_open)', 'cat'),

    ('unit', 'unit', 'u.label = any(:unit)', 'quant'),
    ('aspect', 'aspect', 'ain.label = any(:aspect)', 'quant'),
    ('agg-type', 'agg_type', 'qd.aggregation_type = :agg_type', 'quant'),
    # TODO shape

    ('value-quant', 'value_quant', 'qv.value = :value_quant', 'quant'),
    ('value-quant-margin', 'value_quant_margin', 'qv.value <= :value_quant + :value_quant_margin AND qv.value >= :value_quant - :value_quant_margin', 'quant'),
    ('value-quant-min', 'value_quant_min', 'qv.value >= :value_quant_min', 'quant'),
    ('value-quant-max', 'value_quant_max', 'qv.value <= :value_quant_min', 'quant'),
)


def get_where(kwargs):
    _where_cat = []
    _where_quant = []
    params = {}
    for u, s, w, t in url_sql_where:
        if u in kwargs and kwargs[u]:
            params[s] = kwargs[u]
            if t == 'cat':
                _where_cat.append(w)
            elif t == 'quant':
                _where_quant.append(w)
            elif t == 'both':
                _where_cat.append(w)
                _where_quant.append(w)
            else:
                raise ValueError('wat')

    where_cat = ' AND '.join(_where_cat)
    where_quant = ' AND '.join(_where_quant)
    print(f'\nwhere-quant\n{where_quant}\nwhere-quant')
    return where_cat, where_quant, params


def main_query(endpoint, kwargs):
    ep_select = {
        #'instances': 'im.dataset, im.formal_id, im.sam_id, im.sub_id, id.label',
        'instances': 'im.dataset, im.formal_id AS inst, im.sam_id AS sample, im.sub_id AS subject, id.label AS inst_desc',
        'objects': 'im.dataset, o.id, o.id_type, oi.updated_transitive',  # TODO probably some path metadata file type, etc. too

        'values/cat':   'im.dataset, im.formal_id AS inst, id.label AS inst_desc, cdid.label AS domain, cd.range, cd.label AS cat_desc, cv.value_open, ct.label AS value_controlled',  # TODO and where did it come from TODO iri
        # TODO will want/need to return the shape of the value for these as well since that will be needed to correctly interpret the contents of the value field in the future
        'values/quant': 'im.dataset, im.formal_id AS inst, id.label AS inst_desc, qd.aggregation_type AS agg_type, a.label AS aspect, u.label AS unit, qv.value',  # TODO and where did it come from
        'values/cat-quant': (
            "'value-cat'   AS type, im.dataset, im.formal_id AS inst, id.label AS inst_desc, cdid.label AS domain,      cd.range,    NULL as agg_type, cd.label AS pred_or_asp,  cv.value_open AS vo_or_unit, ct.label AS value_controlled, NULL AS value"
            ,
            "'value-quant' AS type, im.dataset, im.formal_id AS inst, id.label AS inst_desc,       NULL AS domain, NULL AS range, qd.aggregation_type,       a.label AS aspect,              u.label AS unit,                   NULL AS vc, qv.value"
        )
    }[endpoint]
    # FIXME move extra and select out and pass then in in as arguments ? or retain control here?
    extra_cat = {
        'objects':             '\nJOIN objects AS o ON cv.object_id = o.id LEFT OUTER JOIN objects_internal AS oi ON oi.id = o.id',
    }
    extra_quant = {
        'objects':             '\nJOIN objects AS o ON qv.object_id = o.id LEFT OUTER JOIN objects_internal AS oi ON oi.id = o.id',
    }
    ep_extra_cat = extra_cat[endpoint] if endpoint in extra_cat else ''
    ep_extra_quant = extra_quant[endpoint] if endpoint in extra_quant else ''

    ep_select_cat, ep_select_quant = ep_select if isinstance(ep_select, tuple) else (ep_select, ep_select)
    select_cat, select_quant = f'SELECT {ep_select_cat}', f'SELECT {ep_select_quant}'
    where_cat, where_quant, params = get_where(kwargs)
    q_cat = """FROM cat_values AS cv

JOIN class_measured AS idin
CROSS JOIN LATERAL get_class_children(idin.id) AS idc ON cv.inst_desc = idc.child
JOIN class_measured AS id ON cv.inst_desc = id.id
JOIN instance_measured AS im ON cv.measured_instance = im.id

JOIN cat_descriptors AS cd ON cv.cat_desc = cd.id
LEFT OUTER JOIN class_measured AS cdid ON cd.is_measuring = cdid.id  -- XXX TODO mismach
LEFT OUTER JOIN controlled_terms AS ct ON cv.value_controlled = ct.id"""

    q_quant = """FROM quant_values AS qv

JOIN class_measured AS idin
CROSS JOIN LATERAL get_class_children(idin.id) AS idc ON qv.inst_desc = idc.child
JOIN class_measured AS id ON qv.inst_desc = id.id
JOIN instance_measured AS im ON qv.measured_instance = im.id

JOIN quant_descriptors AS qd ON qv.quant_desc = qd.id
JOIN aspects AS ain
CROSS JOIN LATERAL get_aspect_children(ain.id) AS ac ON qd.aspect = ac.child
JOIN aspects AS a ON ac.child = a.id
LEFT OUTER JOIN units AS u ON qd.unit = u.id"""

    sw_cat = f'{select_cat}\n{q_cat}{ep_extra_cat}\nWHERE {where_cat}'  # XXX yes this can be malformed in some cases
    sw_quant = f'{select_quant}\n{q_quant}{ep_extra_quant}\nWHERE {where_quant}'  # XXX yes this can be malformed in some cases
    if endpoint == 'values/cat':
        query = sw_cat
    elif endpoint == 'values/quant':
        query = sw_quant
    else:
        operator = 'UNION' if 'union-cat-quant' in kwargs and kwargs['union-cat-quant'] else 'INTERSECT'
        query = f'{sw_cat}\n{operator}\n{sw_quant}'

    print(query)
    return query, params


def to_json(record_type, res):
    rows = list(res)
    if rows:
        if record_type == 'object':
            result = [{k: v for k, v in r._asdict().items() if k != 'id'}
                      # do not leak internal ids because the might change and are not meaningful
                      if r.id_type == 'quantdb' else
                      {k: v for k, v in r._asdict().items() if k != 'updated_transitive'}
                      for r in rows]
        elif record_type is None and 'type' in rows[0]._fields:
            rem_cat = 'value', 'agg_type'
            def type_fields_cat(k):
                if k == 'pred_or_asp':
                    return 'cat_desc'
                elif k == 'vo_or_unit':
                    return 'value_open'
                else:
                    return k

            rem_quant = 'domain', 'range', 'value_controlled'
            def type_fields_quant(k):
                if k == 'pred_or_asp':
                    return 'aspect'
                elif k == 'vo_or_unit':
                    return 'unit'
                else:
                    return k

            def prow(r):
                if r.type == 'value-cat':
                    rem, type_fields = rem_cat, type_fields_cat
                elif r.type == 'value-quant':
                    rem, type_fields = rem_quant, type_fields_quant
                else:
                    raise NotImplementedError(f'wat {r.type}')

                return {type_fields(k): v for k, v in r._asdict().items() if k not in rem}

            result = [prow(r) for r in rows]
        else:
            result = [r._asdict() for r in rows]

        if record_type is not None:
            for r in result:
                r['type'] = record_type

        out = result
        #breakpoint()
    else:
        out = []

    return json.dumps(out, cls=JENcode), 200, {'Content-Type': 'application/json'}


def getArgs(request):
    default = {
        'object': [],
        'updated-transitive': None,  # TODO needed to query for some internal

        ## inst
        'desc-inst': [],  # aka class

        # value-inst
        'dataset': None,
        'inst': [],
        'subject': [],
        'sample': [],
        'include-equivalent': False,

        ## cat
        'dsec-cat': [],  # aka predicate

        'value-cat': [],
        'value-cat-open': [],

        ## quant
        # desc-quant
        'unit': [],
        'aspect': [],
        'agg-type': None,
        # TODO shape

        'value-quant': None,
        'value-quant-margin': None,
        'value-quant-min': None,
        'value-quant-max': None,

        'limit': 100,
        #'operator': 'INTERSECT',  # XXX ...
        'union-cat-quant': False,  # by default we intersect but sometimes you want the union instead e.g. if object is passed

        #'cat-value': [],
        #'class': [],
        #'predicate': None,
        #'object': None,
        #'filter': [],

        #'quant-value': None,
        #'quant-margin': None,
        #'quant-min': None,
        #'quant-max': None,
    }
    extras = set(request.args) - set(default)
    if extras:
        # FIXME raise this as a 401, TODO need error types for this
        raise ValueError(f'unknown args {extras}')

    def convert(k, d):
        if k in request.args:
            if k in ('dataset', 'include-equivalent', 'union-cat-quant') or k.startswith('value-quant-'):
                v = request.args[k]
                if k in ('dataset',):
                    v = uuid.UUID(v)
            else:
                v = request.args.getlist(k)
                if k in ('object',):
                    v = [uuid.UUID(_) for _ in v]  # caste to uuid to simplify sqlalchemy type mapping
        else:
            return d

        if k in ('include-equivalent', 'union-cat-quant'):
            if v.lower() == 'true':
                return True
            elif v.lower() == 'false':
                return False
            else:
                raise TypeError(f'Expected a bool, got "{v}" instead.')
        elif k.startswith('value-quant-') or k in ('limit',):
            try:
                return float(v)
            except ValueError as e:
                raise e
        else:
            return v

    out = {k:convert(k, v) for k, v in default.items()}
    return out


def dbUri(dbuser, host, port, database):
    if hasattr(sys, 'pypy_version_info'):
        dialect = 'psycopg2cffi'
    else:
        dialect = 'psycopg2'
    return f'postgresql+{dialect}://{dbuser}@{host}:{port}/{database}'


def make_app(db=None, name='quantdb-server'):
    app = Flask(name)
    kwargs = {k:config.auth.get(f'db-{k}')  # TODO integrate with cli options
              for k in ('user', 'host', 'port', 'database')}
    kwargs['dbuser'] = kwargs.pop('user')
    app.config['SQLALCHEMY_DATABASE_URI'] = dbUri(**kwargs)  # use os.environ.update
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    session = db.session

    bp = '/api/1/'

    def default_flow(endpoint, record_type, query_fun, json_fun):
        try:
            kwargs = getArgs(request)
        except Exception as e:
            breakpoint()
            raise e
        # TODO error handling and stuff
        # FIXME record_type is actually determined entirely in query_fun right now
        try:
            query, params = query_fun(endpoint, kwargs)
        except Exception as e:
            breakpoint()
            raise e

        try:
            res = session.execute(sql_text(query), params)
        except Exception as e:
            breakpoint()
            raise e

        try:
            resp = json_fun(record_type, res)
        except Exception as e:
            breakpoint()
            raise e

        return resp

    @app.route(f'{bp}/objects')
    def route_1_objects():
        "objects with derived values that match all criteria"
        return default_flow('objects', 'object', main_query, to_json)

    @app.route(f'{bp}/desc/inst')
    @app.route(f'{bp}/descriptors/inst')
    @app.route(f'{bp}/classes')
    def route_1_desc_inst():
        def query(endpoint, kwargs):
            return 'select id.iri, id.label from class_measured as id', {}

        return default_flow('desc/inst', 'desc-inst', query, to_json)  # TODO likely need different args

    @app.route(f'{bp}/desc/cat')
    @app.route(f'{bp}/descriptors/cat')
    @app.route(f'{bp}/predicates')
    def route_1_desc_cat():
        def query(endpoint, kwargs):
            return ('select cd.label, cdid.label AS domain, cd.range, cd.description '
                    'from cat_descriptors as cd '
                    'left outer join class_measured as cdid on cdid.id = cd.is_measuring'
                    ), {}

        return default_flow('desc/cat', 'desc-cat', query, to_json)  # TODO likely need different args e.g. to filter by inst_desc

    @app.route(f'{bp}/desc/quant')
    @app.route(f'{bp}/descriptors/quant')
    def route_1_desc_quant():
        def query(endpoint, kwargs):
            return ('select qd.label, id.label AS inst_desc, qd.shape, qd.aggregation_type as agg_type, a.label AS aspect, u.label AS unit, qd.description '
                    'from quant_descriptors as qd '
                    'left outer join class_measured as id on id.id = qd.is_measuring '
                    'left outer join units as u on u.id = qd.unit '
                    'join aspects as a on a.id = qd.aspect'
                    ), {}

        return default_flow('desc/quant', 'desc-quant', query, to_json)  # TODO likely need different args e.g. to filter by inst_desc

    @app.route(f'{bp}/values/inst')
    @app.route(f'{bp}/instances')
    def route_1_val_inst():
        "instances associated with values that match all critiera"
        return default_flow('instances', 'instance', main_query, to_json)

    @app.route(f'{bp}/values')
    @app.route(f'{bp}/values/cat-quant')
    def route_1_val_cat_quant():
        return default_flow('values/cat-quant', None, main_query, to_json)

    @app.route(f'{bp}/values/cat')
    def route_1_val_cat():
        return default_flow('values/cat', 'value-cat', main_query, to_json)

    @app.route(f'{bp}/values/quant')
    def route_1_val_quant():
        return default_flow('values/quant', 'value-quant', main_query, to_json)

    @app.route(f'{bp}/terms')
    def route_1_cterms(): pass

    @app.route(f'{bp}/units')
    def route_1_units(): pass

    @app.route(f'{bp}/aspects')
    def route_1_aspects(): pass

    def route_datasets(id=None):
        # TODO improve this to pull from meta add uris etc.
        table, title = report.size()
        return wrap_tables(table, title=title)

    return app


def run_server():
    return make_app(db=SQLAlchemy())


if __name__ == '__main__':
    test()
