import json
import uuid
from decimal import Decimal
from datetime import datetime
from flask import Flask, request
from sqlalchemy.sql import text as sql_text
from quantdb.utils import log, dbUri, isoformat
from quantdb.config import auth
from quantdb import exceptions as exc


log = log.getChild('api')


class JEncode(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, datetime):
            return isoformat(obj)
        elif isinstance(obj, Decimal):
            # FIXME TODO precision etc. see comment on descriptors_quant
            return float(obj)

        return json.JSONEncoder.default(self, obj)


url_sql_where = (  # TODO arity spec here

    # dupes overwrite params but that is ok, this way we get the correct table alias for both cases
    ('object', 'object', 'cv.object = any(:object)', 'cat'),  # XXX should not use this outside values/ unless we left outer due to intersect ?
    ('object', 'object', 'qv.object = any(:object)', 'quant'),  # XXX should not use this outside values/ unless we left outer due to intersect ?

    ('desc-inst', 'desc_inst', 'idin.label = any(:desc_inst)', 'both'),
    ('dataset', 'dataset', 'im.dataset = :dataset', 'both'),
    ('inst', 'inst', 'im.id_formal = any(:inst)', 'both'),
    ('subject', 'subject', 'im.id_sub = any(:subject)', 'both'),
    ('sample', 'sample', 'im.id_sam = any(:sample)', 'both'),

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
    log.log(9, f'\nwhere-quant\n{where_quant}\nwhere-quant')
    return where_cat, where_quant, params


def main_query(endpoint, kwargs):
    ep_select = {
        #'instances': 'im.dataset, im.id_formal, im.id_sam, im.id_sub, id.label',
        'values/inst': (
            'im.dataset, '
            'im.id_formal AS inst, '
            'im.id_sam AS sample, '
            'im.id_sub AS subject, '
            'id.label AS desc_inst'
        ),
        'objects': (  # TODO probably some path metadata file type, etc. too
            'im.dataset, '
            'o.id, '
            'o.id_type, '
            'oi.updated_transitive'
        ),
        'values/cat': (
            'im.dataset, '
            'im.id_formal AS inst, '
            'id.label AS desc_inst, '
            'cdid.label AS domain, '
            'cd.range, '
            'cd.label AS desc_cat, '
            'cv.value_open, '
            'ct.label AS value_controlled'  # TODO and where did it come from TODO iri
        ),
        # TODO will want/need to return the shape of the value for these as well since that will be needed to correctly interpret the contents of the value field in the future
        'values/quant': (
            'im.dataset, '
            'im.id_formal AS inst, '
            'id.label AS desc_inst, '
            'qd.aggregation_type AS agg_type, '
            'a.label AS aspect, '
            'u.label AS unit, qv.value'  # TODO and where did it come from
        ),
        'values/cat-quant': (
            (
                "'value-cat'   AS type, "
                'im.dataset, '
                'im.id_formal AS inst, '
                'id.label AS desc_inst, '
                'cdid.label AS domain, '
                'cd.range, '
                'NULL::quant_agg_type as agg_type, '  # have to annoate the nulls because distinct causes type inference to fail ???
                'cd.label AS pred_or_asp, '
                'cv.value_open AS vo_or_unit, '
                'ct.label AS value_controlled, '
                'NULL::numeric AS value')
            , (
                "'value-quant' AS type, im.dataset, "
                'im.id_formal AS inst, id.label AS desc_inst, '
                'NULL AS domain, '
                'NULL::cat_range_type AS range, '
                'qd.aggregation_type AS agg_type, '
                'a.label AS aspect, '
                'u.label AS unit, '
                'NULL AS vc, qv.value'
            ))}[endpoint]
    # FIXME move extra and select out and pass then in in as arguments ? or retain control here?

    def gkw(k):
        return k in kwargs and kwargs[k]

    class sn:  # select needs
        objects = endpoint == 'objects'
        desc_inst = endpoint != 'objects'
        desc_cat = endpoint in ('values/cat', 'values/cat-quant')
        value_cat = endpoint in ('values/cat', 'values/cat-quant')
        aspect = endpoint in ('values/quant', 'values/cat-quant')
        unit = endpoint in ('values/quant', 'values/cat-quant')
        agg_type = endpoint in ('values/quant', 'values/cat-quant')

    class kw:  # keywords
        prov = gkw('prov')
        source_only = gkw('source-only')
        desc_inst = gkw('desc-inst')
        desc_cat = gkw('desc-cat')
        value_cat = gkw('value-cat')
        aspect = gkw('aspect')
        unit = gkw('unit')
        agg_type = gkw('agg-type')

    s_prov_objs = """
,
im.dataset as prov_source_dataset, -- FIXME dataset_object maybe? or what?
o.id as prov_source_id,
o.id_type as prov_source_id_type,
oi.updated_transitive as prov_source_updated_transitive,
"""

    s_prov_i = """
adi.addr_type  as prov_inst_addr_type,
adi.addr_field as prov_inst_addr_field,
adi.value_type as prov_inst_type,

add.addr_type  as prov_desc_inst_addr_type,
add.addr_field as prov_desc_inst_addr_field,
add.value_type as prov_desc_inst_type
"""

    s_prov_c = """
adc.addr_type  as prov_value_addr_type,
adc.addr_field as prov_value_addr_field,
adc.value_type as prov_value_type
""" + (""",
NULL::address_type     as prov_unit_addr_type,
NULL                   as prov_unit_addr_field,
NULL::field_value_type as prov_unit_type,

NULL::address_type     as prov_aspect_addr_type,
NULL                   as prov_aspect_addr_field,
NULL::field_value_type as prov_aspect_type
""" if sn.unit or endpoint == 'values/inst' else '')

    s_prov_q = """
adq.addr_type  as prov_value_addr_type,
adq.addr_field as prov_value_addr_field,
adq.value_type as prov_value_type,

adu.addr_type  as prov_unit_addr_type,
adu.addr_field as prov_unit_addr_field,
adu.value_type as prov_unit_type,

ada.addr_type  as prov_aspect_addr_type,
ada.addr_field as prov_aspect_addr_field,
ada.value_type as prov_aspect_type
"""

    q_prov_i = """
JOIN obj_desc_inst AS odi ON odi.object = o.id AND odi.desc_inst = im.desc_inst
JOIN addresses AS adi ON adi.id = odi.addr_field
LEFT OUTER JOIN addresses AS add ON add.id = odi.addr_desc_inst
"""

    q_prov_c = """
JOIN obj_desc_cat AS odc ON odc.object = o.id AND odc.desc_cat = cv.desc_cat
JOIN addresses AS adc ON adc.id = odc.addr_field
"""

    q_prov_q = """
JOIN obj_desc_quant AS odq ON odq.object = o.id AND odq.desc_quant = qv.desc_quant
JOIN addresses as adq on adq.id = odq.addr_field
LEFT OUTER JOIN addresses AS adu ON adu.id = odq.addr_unit
LEFT OUTER JOIN addresses AS ada ON ada.id = odq.addr_aspect
"""

    maybe_distinct = 'DISTINCT ' if (sn.objects or kw.prov) and not kw.source_only else ''
    ep_select_cat, ep_select_quant = ep_select if isinstance(ep_select, tuple) else (ep_select, ep_select)
    select_cat = f'SELECT {maybe_distinct}{ep_select_cat}' + (
        (s_prov_objs + s_prov_i + ((',\n' + s_prov_c) if endpoint != 'values/inst' else '')) if kw.prov else '')
    select_quant = f'SELECT {maybe_distinct}{ep_select_quant}' + (
        (s_prov_objs + s_prov_i + ((',\n' + s_prov_q) if endpoint != 'values/inst' else '')) if kw.prov else '')
    _where_cat, _where_quant, params = get_where(kwargs)
    where_cat = f'WHERE {_where_cat}' if _where_cat else ''
    where_quant = f'WHERE {_where_quant}' if _where_quant else ''

    # FIXME even trying to be smart here about which joins to pull just papers over the underlying perf issue
    # shaves about 140ms off but the underlying issue remains
    q_cat = '\n'.join((
        'FROM values_cat AS cv',
        '\n'.join((
            'JOIN descriptors_inst AS idin',
            'CROSS JOIN LATERAL get_child_closed_desc_inst(idin.id) AS idc ON cv.desc_inst = idc.child -- FIXME',
        )) if kw.desc_inst else '',
        'JOIN descriptors_inst AS id ON cv.desc_inst = id.id' if sn.desc_inst or kw.desc_inst else '',
        'JOIN values_inst AS im ON cv.instance = im.id',
        '\n'.join((
            'JOIN descriptors_cat AS cd ON cv.desc_cat = cd.id',
            'LEFT OUTER JOIN descriptors_inst AS cdid ON cd.domain = cdid.id  -- XXX TODO mismach',
        )) if sn.desc_cat or kw.desc_cat else '',
        'LEFT OUTER JOIN controlled_terms AS ct ON cv.value_controlled = ct.id' if sn.value_cat or kw.value_cat else '',
        (('\n'
          'JOIN objects AS o ON cv.object = o.id\n'
          'LEFT OUTER JOIN objects_internal AS oi\n'
          'ON oi.id = o.id\n')
         if kw.source_only else
         ('\n'  # have to use LEFT OUTER because object might have only one of cat or quant
          'LEFT OUTER JOIN values_quant AS qv ON qv.instance = im.id\n'
          'JOIN objects AS o ON cv.object = o.id OR qv.object = o.id\n'
          'LEFT OUTER JOIN objects_internal AS oi\n'
          'ON oi.id = o.id\n')
         ) if sn.objects or kw.prov else '',
        (q_prov_i + q_prov_c) if kw.prov else '',
    ))

    q_quant = '\n'.join((
        'FROM values_quant AS qv',
        '\n'.join((
            'JOIN descriptors_inst AS idin',
            'CROSS JOIN LATERAL get_child_closed_desc_inst(idin.id) AS idc ON qv.desc_inst = idc.child -- FIXME',
        )) if kw.desc_inst else '',
        'JOIN descriptors_inst AS id ON qv.desc_inst = id.id' if sn.desc_inst or kw.desc_inst else '',
        'JOIN values_inst AS im ON qv.instance = im.id',
        'JOIN descriptors_quant AS qd ON qv.desc_quant = qd.id' if sn.aspect or sn.unit or sn.agg_type or kw.aspect or kw.unit or kw.agg_type else '',
        '\n'.join((
            'JOIN aspects AS ain',
            'CROSS JOIN LATERAL get_child_closed_aspect(ain.id) AS ac ON qd.aspect = ac.child',
            'JOIN aspects AS a ON ac.child = a.id',
        )) if kw.aspect else ('JOIN aspects AS a ON qd.aspect = a.id' if sn.aspect else ''), # TODO we filter these out based on endpoint
        'LEFT OUTER JOIN units AS u ON qd.unit = u.id' if sn.unit or kw.unit else '',
        (('\n'
          'JOIN objects AS o ON qv.object = o.id\n'
          'LEFT OUTER JOIN objects_internal AS oi ON oi.id = o.id\n')
         if kw.source_only else
         ('\n'  # have to use LEFT OUTER because object might have only one of cat or quant
          'LEFT OUTER JOIN values_cat AS cv ON cv.instance = im.id\n'
          'JOIN objects AS o ON qv.object = o.id OR cv.object = o.id\n'
          'LEFT OUTER JOIN objects_internal AS oi ON oi.id = o.id\n')
         ) if sn.objects or kw.prov else '',
        (q_prov_i + q_prov_q) if kw.prov else '',
    ))

    sw_cat = f'{select_cat}\n{q_cat}\n{where_cat}'  # XXX yes this can be malformed in some cases
    sw_quant = f'{select_quant}\n{q_quant}\n{where_quant}'  # XXX yes this can be malformed in some cases
    if endpoint == 'values/cat':
        query = sw_cat
    elif endpoint == 'values/quant':
        query = sw_quant
    else:
        operator = 'UNION' if 'union-cat-quant' in kwargs and kwargs['union-cat-quant'] else 'INTERSECT'
        query = f'{sw_cat}\n{operator}\n{sw_quant}'

    log.log(9, '\n' + query)
    return query, params


def to_json(record_type, res, prov=False):
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
                    return 'desc_cat'
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

        if prov:
            def pop_prefix(d, prefix):
                usc = prefix.count('_')
                return {k.split('_', 1 + usc)[-1]:v for k in list(d) if k.startswith(prefix + '_') and (v := d.pop(k)) is not None}

            for r in result:
                provs = pop_prefix(r, 'prov')
                if 'source_id_type' in provs and provs['source_id_type'] == 'quantdb':
                    provs.pop('source_id', None)  # don't leak internal ids
                else:
                    provs.pop('source_updated_transitive', None)  # always None in this case

                for prefix in ('desc_inst', 'inst', 'value', 'value', 'source'):
                    d = pop_prefix(provs, prefix)
                    if d:
                        d['type'] = 'address' if prefix != 'source' else 'object'
                        provs[prefix] = d

                provs['type'] = 'prov'
                r['prov'] = provs

        out = result
        #breakpoint()
    else:
        out = []

    return json.dumps(out, cls=JEncode), 200, {'Content-Type': 'application/json'}


def getArgs(request, endpoint, dev=False):
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
        'desc-cat': [],  # aka predicate

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
        'source-only': False,
        'prov': False,

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

    if dev:
        default['return-query'] = False

    # modify defaults by endpoint
    if endpoint != 'objects':
        default.pop('source-only')

    if not endpoint.startswith('values/'):
        default.pop('prov')
    elif endpoint == 'values/cat':
        [default.pop(k) for k in list(default) if k.startswith('value-quant') or k in ('unit', 'aspect', 'agg-type')]
    elif endpoint == 'values/quant':
        [default.pop(k) for k in list(default) if k in ('desc-cat', 'value-cat', 'value-cat-open')]

    extras = set(request.args) - set(default)
    if extras:
        # FIXME raise this as a 401, TODO need error types for this
        nl = '\n'
        raise exc.UnknownArg(f'unknown args: {nl.join(extras)}')

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


def make_app(db=None, name='quantdb-api-server', dev=False):
    app = Flask(name)
    kwargs = {k:auth.get(f'db-{k}')  # TODO integrate with cli options
              for k in ('user', 'host', 'port', 'database')}
    kwargs['dbuser'] = kwargs.pop('user')
    app.config['SQLALCHEMY_DATABASE_URI'] = dbUri(**kwargs)  # use os.environ.update
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    session = db.session

    bp = '/api/1/'

    def default_flow(endpoint, record_type, query_fun, json_fun):
        try:
            kwargs = getArgs(request, endpoint, dev=dev)
        except exc.UnknownArg as e:
            return json.dumps({'error': e.args[0], 'http_response_status': 422}), 422
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

        if 'return-query' in kwargs and kwargs['return-query']:
            return query

        try:
            res = session.execute(sql_text(query), params)
        except Exception as e:
            breakpoint()
            raise e

        try:
            resp = json_fun(record_type, res, prov=('prov' in kwargs and kwargs['prov']))
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
            return ('select '

                    'id.iri, '
                    'id.label '

                    'from descriptors_inst as id'), {}

        return default_flow('desc/inst', 'desc-inst', query, to_json)  # TODO likely need different args

    @app.route(f'{bp}/desc/cat')
    @app.route(f'{bp}/descriptors/cat')
    @app.route(f'{bp}/predicates')
    def route_1_desc_cat():
        def query(endpoint, kwargs):
            return ('select '

                    'cd.label, '
                    'cdid.label AS domain, '
                    'cd.range, '
                    'cd.description '

                    'from descriptors_cat as cd '
                    'left outer join descriptors_inst as cdid on cdid.id = cd.domain'
                    ), {}

        return default_flow('desc/cat', 'desc-cat', query, to_json)  # TODO likely need different args e.g. to filter by desc_inst

    @app.route(f'{bp}/desc/quant')
    @app.route(f'{bp}/descriptors/quant')
    def route_1_desc_quant():
        def query(endpoint, kwargs):
            return ('select '

                    'qd.label, '
                    'id.label AS desc_inst, '
                    'qd.shape, '
                    'qd.aggregation_type as agg_type, '
                    'a.label AS aspect, '
                    'u.label AS unit, '
                    'qd.description '

                    'from descriptors_quant as qd '
                    'left outer join descriptors_inst as id on id.id = qd.domain '
                    'left outer join units as u on u.id = qd.unit '
                    'join aspects as a on a.id = qd.aspect'
                    ), {}

        return default_flow('desc/quant', 'desc-quant', query, to_json)  # TODO likely need different args e.g. to filter by desc_inst

    @app.route(f'{bp}/values/inst')
    @app.route(f'{bp}/instances')
    def route_1_val_inst():
        "instances associated with values that match all critiera"
        return default_flow('values/inst', 'instance', main_query, to_json)

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
    @app.route(f'{bp}/controlled-terms')
    def route_1_cterms():
        def query(endpoint, kwargs):
            return ('select '

                    'ct.iri, '
                    'ct.label '

                    'from controlled_terms as ct'), {}

        return default_flow('terms', 'term', query, to_json)  # TODO likely need different args

    @app.route(f'{bp}/units')
    def route_1_units():
        def query(endpoint, kwargs):
            return ('select '

                    'u.iri, '
                    'u.label '

                    'from units as u'), {}

        return default_flow('units', 'unit', query, to_json)  # TODO likely need different args

    @app.route(f'{bp}/aspects')
    def route_1_aspects():
        def query(endpoint, kwargs):
            return ('select '

                    'a.iri, '
                    'a.label '

                    'from aspects as a'), {}

        return default_flow('aspects', 'aspect', query, to_json)  # TODO likely need different args

    return app
