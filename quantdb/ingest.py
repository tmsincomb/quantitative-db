import json
import pathlib
import sys
from collections import defaultdict

import requests
from sparcur import objects as sparcur_objects  # register pathmeta type

# FIXME sparcur dependencies, or keep ingest separate
from sparcur.utils import fromJson
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import text as sql_text

from quantdb.utils import dbUri, isoformat, log

######### start database interaction section


log = log.getChild("ingest")

try:
    if get_ipython().__class__.__name__ == "ZMQInteractiveShell":
        import sys  # FIXME hack that should be in jupyter-repl env or something

        sys.breakpointhook = lambda: None
except NameError:
    pass


# from interlex.core import getName
class getName:
    class MyBool:
        """python is dumb"""

    def __init__(self):
        self.counter = -1
        self.value_to_name = {}

    def valueCheck(self, value):
        if isinstance(value, dict):
            value = hash(
                frozenset(
                    (
                        k,
                        (
                            self.valueCheck(v)
                            if isinstance(v, list) or isinstance(v, dict)
                            else v
                        ),
                    )
                    for k, v in value.items()
                )
            )
        elif isinstance(value, list):
            value = tuple(self.valueCheck(e) for e in value)
        elif isinstance(value, bool):
            value = self.MyBool, value
        else:
            pass

        return value

    def __call__(self, value, type=None):
        value = self.valueCheck(value)
        if type is not None and (value, type) in self.value_to_name:
            return self.value_to_name[value, type]
        elif type is None and value in self.value_to_name:
            return self.value_to_name[value]
        else:
            self.counter += 1
            name = "v" + str(self.counter)

            if type is None:
                self.value_to_name[value] = name
            else:
                self.value_to_name[value, type] = name

            return name


# from interlex.core import makeParamsValues
def makeParamsValues(
    *value_sets, constants=tuple(), types=tuple(), row_types=tuple()
):
    # TODO variable sized records and
    # common value names
    if constants and not all(":" in c for c in constants):
        raise ValueError(
            f"All constants must pass variables in via params {constants}"
        )

    getname = getName()

    params = {}
    if types:
        bindparams = []
        itertypes = (t for ts in types for t in ts)

    elif row_types:
        bindparams = []

    for i, values in enumerate(value_sets):
        # proto_params doesn't need to be a dict
        # values will be reduced when we create params as a dict
        if row_types:
            proto_params = [
                (
                    tuple(
                        getname(value, type=t)
                        for value, t in zip(row, row_types)
                    ),
                    row,
                )
                for row in values
            ]
        else:
            proto_params = [
                (tuple(getname(value) for value in row), row) for row in values
            ]

        values_template = ", ".join(
            "("
            + ", ".join(constants + tuple(":" + name for name in names))
            + ")"
            for names, _ in proto_params
        )
        yield values_template
        if row_types:
            _done = set()
            for names, values in proto_params:
                for name, value, type in zip(names, values, row_types):
                    params[name] = value
                    if type is not None and name not in _done:
                        bindparams.append(bindparam(name, type_=type))
        else:
            for names, values in proto_params:
                for name, value in zip(names, values):
                    params[name] = value
                    if types:
                        maybe_type = next(itertypes)
                        if maybe_type is not None:
                            bindparams.append(bindparam(name, type_=maybe_type))

    yield params
    if types or row_types:
        yield bindparams  # TODO not sure if there are dupes here


"""
l left
r right
c cardiac not left or right but a branch on its own
a abdominal
p posterior

c cervical
t throacic
a abdominal
"""
sam_ordering = {
    "l": 0,  # left
    "r": 0,  # right
    "c": 0,  # cardiac safe to keep at zero since the c index usually come after t
    "a": 1,  # anterior abdominal
    "p": 1,  # posterior abdominal
}
seg_ordering = {
    "c": 0,  # cervical
    "t": 1,  # thoracic
    "a": 2,  # abdominal
}


def anat_index(sample):
    # count the number of distinct values less than a given integer
    # create the map

    sam, sam_id, seg, seg_id = sample.split("-")
    # FIXME bad becase left and right are unstable and we don't care about this, we just want relative to max possible
    # don't do this with sort
    sam_ind = sam_ordering[sam_id]
    for k, v in seg_ordering.items():
        if seg_id.startswith(k):
            prefix = k
            seg_ind = v
            break
    else:
        if sam_id == "c":
            # print('c sample', sample)
            # rest = int(''.join(_ for _ in seg_id if _.isdigit()))
            rest = int(seg_id[:-1])
            suffix = int(seg_id[-1].encode().hex())
            return sam_ind, 0, rest, suffix
        else:
            msg = f"unknown seg {sample}"
            print(msg)  # FIXME TODO logging
            # raise ValueError(msg)
            # return int(f'{sam_ind}000')
            return sam_ind, 0, 0, 0

    rest = int(
        seg_id[len(prefix) :]
    )  # FIXME this convention is not always followed
    comps = sam_ind, seg_ind, rest, 0
    # return int(f'{sam_ind}{seg_ind}{rest:0>2d}')
    return comps


def pps(path_structure):
    if len(path_structure) == 6:
        # FIXME utter hack
        top, subject, sam_1, segment, modality, file = path_structure
        p1 = (
            sam_1,
            subject,
        )  # child, parent to match db convention wasDerivedFrom
        p2 = segment, sam_1
        return {
            "parents": (p1, p2),
            "subject": subject,
            "sample": segment,
            "modality": modality,
            # note that because we do not convert to a single value we cannot include raw_anat_index in the qdb but that's ok
            "raw_anat_index_v1": anat_index(segment),
        }
    else:
        raise NotImplementedError(path_structure)


def ext(j):
    out = {}
    out["dataset"] = j["dataset_id"]
    out["object"] = j["remote_id"]
    out["file_id"] = (
        j["file_id"] if "file_id" in j else int(j["uri_api"].rsplit("/")[-1])
    )  # XXX old pathmeta schema that didn't include file id
    ps = pathlib.Path(j["dataset_relative_path"]).parts
    [p for p in ps if p.startswith("sub-") or p.startswith("sam-")]
    out.update(pps(ps))
    return out


class Queries:
    def __init__(self, session):
        self.session = session

    def address_from_fadd_type_fadd(self, fadd_type, fadd):
        # FIXME multi etc.
        res = [
            i
            for i, in self.session.execute(
                sql_text(
                    "select * from address_from_fadd_type_fadd(:fadd_type, :fadd)"
                ),
                dict(fadd_type=fadd_type, fadd=fadd),
            )
        ]
        if res:
            return res[0]

    def desc_inst_from_label(self, label):
        # FIXME multi etc.
        res = [
            i
            for i, in self.session.execute(
                sql_text("select * from desc_inst_from_label(:label)"),
                dict(label=label),
            )
        ]
        if res:
            return res[0]

    def desc_quant_from_label(self, label):
        # FIXME multi etc.
        res = [
            i
            for i, in self.session.execute(
                sql_text("select * from desc_quant_from_label(:label)"),
                dict(label=label),
            )
        ]
        if res:
            return res[0]

    def desc_cat_from_label_domain_label(self, label, domain_label):
        # FIXME multi etc.
        res = [
            i
            for i, in self.session.execute(
                sql_text(
                    "select * from desc_cat_from_label_domain_label(:label, :domain_label)"
                ),
                dict(label=label, domain_label=domain_label),
            )
        ]
        if res:
            return res[0]

    def cterm_from_label(self, label):
        # FIXME multi etc.
        res = [
            i
            for i, in self.session.execute(
                sql_text("select * from cterm_from_label(:label)"),
                dict(label=label),
            )
        ]
        if res:
            return res[0]

    def insts_from_dataset_ids(self, dataset, ids):
        return list(
            self.session.execute(
                sql_text(
                    "select * from insts_from_dataset_ids(:dataset, :ids)"
                ),
                dict(dataset=dataset, ids=ids),
            )
        )


class InternalIds:
    def set_desc_quant_from_label(self, label: str, desctriptor: str):
        self.__setattr__(label, desctriptor)

    def __init__(self, queries) -> None:

        q = queries
        self._q = queries

        self.addr_suid = q.address_from_fadd_type_fadd(
            "tabular-header", "id_sub"
        )
        self.addr_said = q.address_from_fadd_type_fadd(
            "tabular-header", "id_sam"
        )
        self.addr_spec = q.address_from_fadd_type_fadd(
            "tabular-header", "species"
        )
        self.addr_saty = q.address_from_fadd_type_fadd(
            "tabular-header", "sample_type"
        )
        self.addr_faid = q.address_from_fadd_type_fadd(
            "tabular-header", "fascicle"
        )  # for REVA ft

        self.addr_tmod = q.address_from_fadd_type_fadd(
            "tabular-header", "modality"
        )
        # addr_trai = address_from_fadd_type_fadd('tabular-header', 'raw_anat_index')
        # addr_tnai = address_from_fadd_type_fadd('tabular-header', 'norm_anat_index')
        # addr_context = address_from_fadd_type_fadd('context', '#/path-metadata/{index of match remote_id}/dataset_relative_path')  # XXX this doesn't do what we want, I think what we really would want in these contexts are objects_internal that reference the file system state for a given updated snapshot, that is the real "object" that corresponds to the path-metadata.json that we are working from

        # addr_jpmod = address_from_fadd_type_fadd('json-path-with-types', '#/#int/modality')
        # addr_jprai = address_from_fadd_type_fadd('json-path-with-types', '#/#int/anat_index')
        # addr_jpnai = address_from_fadd_type_fadd('json-path-with-types', '#/#int/norm_anat_index')

        self.addr_jpdrp = q.address_from_fadd_type_fadd(
            "json-path-with-types",
            "#/path-metadata/data/#int/dataset_relative_path",
        )

        # XXX these are more accurate if opaque
        self.addr_jpmod = q.address_from_fadd_type_fadd(
            "json-path-with-types",
            "#/path-metadata/data/#int/dataset_relative_path#derive-modality",
        )
        # addr_jprai = address_from_fadd_type_fadd('json-path-with-types', '#/path-metadata/data/#int/dataset_relative_path#derive-raw-anat-index')
        self.addr_jpnai = q.address_from_fadd_type_fadd(
            "json-path-with-types",
            "#/path-metadata/data/#int/dataset_relative_path#derive-norm-anat-index-v1",
        )
        self.addr_jpnain = q.address_from_fadd_type_fadd(
            "json-path-with-types",
            "#/path-metadata/data/#int/dataset_relative_path#derive-norm-anat-index-v1-min",
        )
        self.addr_jpnaix = q.address_from_fadd_type_fadd(
            "json-path-with-types",
            "#/path-metadata/data/#int/dataset_relative_path#derive-norm-anat-index-v1-max",
        )
        self.addr_jpsuid = q.address_from_fadd_type_fadd(
            "json-path-with-types",
            "#/path-metadata/data/#int/dataset_relative_path#derive-subject-id",
        )
        self.addr_jpsaid = q.address_from_fadd_type_fadd(
            "json-path-with-types",
            "#/path-metadata/data/#int/dataset_relative_path#derive-sample-id",
        )

        self.addr_jpspec = q.address_from_fadd_type_fadd(
            "json-path-with-types", "#/local/tom-made-it-up/species"
        )
        self.addr_jpsaty = q.address_from_fadd_type_fadd(
            "json-path-with-types", "#/local/tom-made-it-up/sample_type"
        )

        # future version when we actually have the metadata files
        # addr_jpmod = address_from_fadd_type_fadd('json-path-with-types', '#/curation-export/manifest/#int/modality')
        # addr_jprai = address_from_fadd_type_fadd('json-path-with-types', '#/curation-export/samples/#int/raw_anat_index')
        # addr_jpnai = address_from_fadd_type_fadd('json-path-with-types', '#/curation-export/samples/#int/norm_anat_index')
        # addr_jpsuid = address_from_fadd_type_fadd('json-path-with-types', '#/curation-export/subjects/#int/id_sub')
        # addr_jpsaid = address_from_fadd_type_fadd('json-path-with-types', '#/curation-export/samples/#int/id_sam')

        self.addr_const_null = q.address_from_fadd_type_fadd("constant", None)

        # qd_rai = desc_quant_from_label('reva ft sample anatomical location distance index raw')
        self.qd_nai = q.desc_quant_from_label(
            "reva ft sample anatomical location distance index normalized v1"
        )
        self.qd_nain = q.desc_quant_from_label(
            "reva ft sample anatomical location distance index normalized v1 min"
        )
        self.qd_naix = q.desc_quant_from_label(
            "reva ft sample anatomical location distance index normalized v1 max"
        )

        self.cd_mod = q.desc_cat_from_label_domain_label(
            "hasDataAboutItModality", None
        )
        self.cd_bot = q.desc_cat_from_label_domain_label(
            "bottom", None
        )  # we just need something we can reference that points to null so we can have a refernce to all the objects

        self.id_human = q.desc_inst_from_label("human")
        self.id_nerve = q.desc_inst_from_label("nerve")
        self.id_nerve_volume = q.desc_inst_from_label("nerve-volume")
        self.luid = {
            "human": self.id_human,
            "nerve": self.id_nerve,
            "nerve-volume": self.id_nerve_volume,
        }

        self.ct_mod = q.cterm_from_label("microct")  # lol ct ct
        self.ct_hack = q.cterm_from_label("hack-associate-some-value")
        self.luct = {
            "ct-hack": self.ct_hack,
            "microct": self.ct_mod,
        }

        # Fascicles
        # F


class Inserts:
    # TODO
    pass


def ingest(
    dataset_uuid,
    extract_fun,
    session,
    commit=False,
    dev=False,
    values_args: list | None = None,
):
    """generic ingest workflow
    this_dataset_updated_uuid might not be needed in future,
    add a kwarg to control it maybe?
    """

    ocdn = " ON CONFLICT DO NOTHING" if dev else ""

    if extract_fun is None and values_args is None:
        raise TypeError("need one of extract_fun or values_args")

    (
        updated_transitive,
        values_objects,
        values_dataset_object,
        make_values_instances,
        make_values_parents,
        make_void,
        make_vocd,
        make_voqd,
        make_values_cat,
        make_values_quant,
    ) = (
        extract_fun(dataset_uuid) if values_args is None else values_args
    )

    q = Queries(session)
    i = InternalIds(q)

    # no dependencies to generate, but insert has to come after the dataset has been inserted (minimally)
    values_instances = make_values_instances(i)

    res0 = session.execute(
        sql_text(
            "INSERT INTO objects (id, id_type) VALUES (:id, :id_type) ON CONFLICT DO NOTHING"
        ),
        dict(id=dataset_uuid, id_type="dataset"),
    )

    # oh dear https://stackoverflow.com/questions/34708509/how-to-use-returning-with-on-conflict-in-postgresql
    res1 = session.execute(
        sql_text(
            "WITH ins AS (INSERT INTO objects_internal (type, dataset, updated_transitive, label) VALUES ('path-metadata', :dataset, :updated_transitive, :label) ON CONFLICT DO NOTHING RETURNING id) SELECT id FROM ins UNION ALL SELECT id FROM objects_internal WHERE type = 'path-metadata' AND dataset = :dataset AND updated_transitive = :updated_transitive"
        ),  # TODO see whether we actually need union all here or whether union by itself is sufficient
        dict(
            dataset=dataset_uuid,
            updated_transitive=updated_transitive,
            label=f"test-load-for-f001 {isoformat(updated_transitive)}",
        ),
    )

    # it is better to use this approach for all top down information
    # just assume that it is coming from some combination of the metadata files and the file system
    # and leave it at that, prov can be chased down later if needed
    this_dataset_updated_uuid = [_ for _, in res1][0]
    void = make_void(this_dataset_updated_uuid, i)
    vocd = make_vocd(this_dataset_updated_uuid, i)
    voqd = make_voqd(this_dataset_updated_uuid, i)

    res1_1 = session.execute(
        sql_text(
            "INSERT INTO objects (id, id_type, id_internal) VALUES (:id, :id_type, :id) ON CONFLICT DO NOTHING"
        ),  # FIXME bad ocdn here
        dict(id=this_dataset_updated_uuid, id_type="quantdb"),
    )

    vt, params = makeParamsValues(values_objects)
    session.execute(
        sql_text(
            f"INSERT INTO objects (id, id_type, id_file) VALUES {vt}{ocdn}"
        ),
        params,
    )

    vt, params = makeParamsValues(values_dataset_object)
    session.execute(
        sql_text(
            f"INSERT INTO dataset_object (dataset, object) VALUES {vt}{ocdn}"
        ),
        params,
    )

    vt, params = makeParamsValues(values_instances)
    session.execute(
        sql_text(
            f"INSERT INTO values_inst (dataset, id_formal, type, desc_inst, id_sub, id_sam) VALUES {vt}{ocdn}"
        ),
        params,
    )

    # inserts that depend on instances having already been inserted
    ilt = q.insts_from_dataset_ids(
        dataset_uuid, [f for d, f, *rest in values_instances]
    )
    luinst = {(str(dataset), id_formal): id for id, dataset, id_formal in ilt}
    values_parents = make_values_parents(luinst)
    values_cv = make_values_cat(this_dataset_updated_uuid, i, luinst)
    values_qv = make_values_quant(this_dataset_updated_uuid, i, luinst)

    vt, params = makeParamsValues(values_parents)
    session.execute(
        sql_text(f"INSERT INTO instance_parent VALUES {vt}{ocdn}"), params
    )

    vt, params = makeParamsValues(void)
    session.execute(
        sql_text(
            f"INSERT INTO obj_desc_inst (object, desc_inst, addr_field, addr_desc_inst) VALUES {vt}{ocdn}"
        ),
        params,
    )

    vt, params = makeParamsValues(vocd)
    session.execute(
        sql_text(
            f"INSERT INTO obj_desc_cat (object, desc_cat, addr_field) VALUES {vt}{ocdn}"
        ),
        params,
    )

    vt, params = makeParamsValues(voqd)
    session.execute(
        sql_text(
            f"INSERT INTO obj_desc_quant (object, desc_quant, addr_field) VALUES {vt}{ocdn}"
        ),
        params,
    )

    vt, params = makeParamsValues(values_cv)
    session.execute(
        sql_text(
            f"INSERT INTO values_cat (value_open, value_controlled, object, desc_inst, desc_cat, instance) VALUES {vt}{ocdn}"
        ),
        params,
    )

    vt, params, bindparams = makeParamsValues(
        # FIXME LOL the types spec here is atrocious ... but it does work ...
        # XXX and barring the unfortunate case, which we have now encountered  where
        # now fixed in the local impl
        values_qv,
        row_types=(None, None, None, None, None, JSONB),
    )

    t = sql_text(
        f"INSERT INTO values_quant (value, object, desc_inst, desc_quant, instance, value_blob) VALUES {vt}{ocdn}"
    )
    tin = t.bindparams(*bindparams)
    session.execute(tin, params)

    if commit:
        session.commit()


def sample_id_from_package_uuid(package_uuid):
    raise NotImplementedError("TODO")
    return sample_id


def sub_id_from_sam_id(sample_id):
    raise NotImplementedError("TODO")
    return subject_id


def rows_from_package_uuid(package_uuid):
    raise NotImplementedError("TODO")
    return rows


def map_addresses(table_header, package_addresses):
    raise NotImplementedError("TODO")
    return defined_columns


def extract_reva_ft_tabular(
    dataset_uuid: str, package_uuid: str, package_addresses: str, sample_id=None
):
    # TODO: Troy
    # worst case derivtive files might need a manual assertion linking to sample
    sample_id = sample_id_from_package_uuid(
        package_uuid
    )  # look a the file hierarchy and find the sample
    subject_id = sub_id_from_sam_id(sample_id)
    rows = rows_from_package_uuid(package_uuid)
    instances = []
    values = []
    defined_columns = map_addresses(rows[0], package_addresses)
    for row in rows[1:]:
        subthing_id = row[1]  # change to "fascicle"
        # might need another factor when coming from microct virtual sections if it is not in the spreadsheet
        formal_id = sample_id + subthing_id
        instances.append(
            {
                "dataset": dataset_uuid,
                "id_formal": formal_id,
                "type": "below",
                "desc_inst": "fascicle-cross-section",
                "id_sub": subject_id,
                "id_sam": sample_id,
            }
        )
        # for column_name, column_index in defined_columns:

    # TODO return the make functions that match those produced by extract_fun in ingest
    # return (
    #     updated_transitive,
    #     values_objects,
    #     values_dataset_object,
    #     make_values_instances,
    #     make_values_parents,
    #     make_void,
    #     make_vocd,
    #     make_voqd,
    #     make_values_cat,
    #     make_values_quant,
    # )


def make_descriptors_etc_reva_ft_tabular():
    # the things we need to insert as part of the specification of the schema for the files we are ingesting

    # aspect
    need_aspects = [
        "area",
        "diameter",
    ]

    # unit
    units = [
        "um",
        "pixel",
    ]

    # quantitative descriptors
    need_qd = [
        "fascicle cross section diameter um",
        "fascicle cross section diameter um min",
        "fascicle cross section diameter um max",
        # TODO: add more here
    ]

    q = Queries(session)
    i = InternalIds(q)

    # TODO properly query to find existing or add new quantitative descriptors
    # query descriptors?

    # OR put them in inserts.sql
    # insert descripters?

    # addresses
    column_name_qd_mapping = [
        # ('fascicle'),  # covered in the values_inst
        ("area", i.area),
        (
            "longest_diameter",
            i.longest_diameter,
        ),  # TODO: fascicle cross section diameter unit max
        (
            "shortest_diameter",
            i.shortest_diameter,
        ),  # fascicle cross section diameter unit min
        ("eff_diam", i.fcsdu),  # TODO: effective diameter
        (
            "c_estimate_nav",
            i.c_estimate_nav,
        ),  # TODO: c estimate nerve area volume
        ("c_estimate_nf", i.c_estimate_nf),  # TODO: c estimate nerve fascicle
        (
            "nfibers_w_c_estimate_nav",
            i.nfibers_w_c_estimate_nav,
        ),  # TODO: number of fibers with c estimate nerve area volume
        (
            "nfibers_w_c_estimate_nf",
            i.nfibers_w_c_estimate_nf,
        ),  # TODO: number of fibers with c estimate nerve fascicle
        ("nfibers_all", i.nfibers_all),  # number of fibers all
        ("n_a_alpha", i.n_a_alpha),  # TODO: TODO: number of fibers alpha
        ("n_a_beta", i.n_a_beta),  # TODO: BUG: number of fibers beta
        ("n_a_gamma", i.n_a_gamma),  # TODO: BUG: number of fibers gamma
        ("n_a_delta", i.n_a_delta),  # TODO: BUG: number of fibers delta
        ("n_b", i.n_b),  # TODO: number of fibers b
        (
            "n_unmyel_nf",
            i.n_unmyel_nf,
        ),  # TODO: number of unmyelinated neral filimant
        ("n_nav", i.n_nav),  # TODO: number of nerve axon volume
        ("n_chat", i.n_chat),  # TODO: number cholen acetal transferase
        (
            "n_myelinated",
            i.n_myelinated,
        ),  # TODO: number of myelinated nerve fibers
        ("area_a_alpha", i.area_a_alpha),  # TODO: area of fibers alpha
        ("area_a_beta", i.area_a_beta),  # TODO: area of fibers beta
        ("area_a_gamma", i.area_a_gamma),  # TODO: area of fibers gamma
        ("area_a_delta", i.area_a_delta),  # TODO: area of fibers delta
        ("area_b", i.area_b),  # TODO: area of fibers b
        (
            "area_unmyel_nf",
            i.area_unmyel_nf,
        ),  # TODO: area of unmyelinated nerve fibers
        ("area_nav", i.area_nav),  # TODO: area of nerve axon volume
        ("area_chat", i.area_chat),  # TODO: TODO: area of chat
        (
            "area_myelinated",
            i.area_myelinated,
        ),  # TODO: area of myelinated nerve fibers
        ("chat_available", i.chat_available),  # TODO: chat available
        ("nav_available", i.nav_available),  # TODO: nerve axon volume available
        ("x_pix", i.x_pix),  # TODO: x pixel; microns
        ("y_pix", i.y_pix),  # TODO: y pixel
        ("x_um", i.x_um),  # TODO: x um
        ("y_um", i.y_um),  # TODO: y um
        ("x_cent", i.x_cent),  # TODO: x center
        ("y_cent", i.y_cent),  # TODO: y center
        ("rho", i.rho),  # TODO: rho; microns
        ("rho_pix", i.rho_pix),  # TODO: rho pixel
        ("phi", i.phi),  # TODO: phi
        ("epi_dist", i.epi_dist),  # TODO: epineurium distance
        ("epi_dist_inv", i.epi_dist_inv),  # TODO: epineurium distance inverse
        ("nerve_based_area", i.nerve_based_area),  # TODO: nerve based area
        (
            "nerve_based_perimeter",
            i.nerve_based_perimeter,
        ),  # TODO: nerve based perimeter
        (
            "nerve_based_eff_diam",
            i.nerve_based_eff_diam,
        ),  # TODO: nerve based effective diameter
        (
            "perinerium_vertices",
            i.perinerium_vertices,
        ),  # TODO: perinerium vertices
        (
            "perinerium_vertices_px",
            i.perinerium_vertices_px,
        ),  # TODO: perinerium vertices pixel
        (
            "nerve_based_shortest_diameter",
            i.nerve_based_shortest_diameter,
        ),  # TODO: nerve based shortest diameter
        ("hull_contrs", i.hull_contrs),  # TODO: hull contours
        ("hull_contr_areas", i.hull_contr_areas),  # TODO: hull contour areas
    ]
    addresses = [
        ("tabular-header", name)
        for name, qd in column_name_qd_mapping
        if qd is not None
    ]
    # TODO do the inserts


def extract_reva_ft(dataset_uuid, source_local=False, visualize=False):"
    if source_local:
        with open(
            pathlib.Path(
                f"~/.local/share/sparcur/export/datasets/{dataset_uuid}/LATEST/path-metadata.json"
            ).expanduser(),
            "rt",
        ) as f:
            blob = json.load(f)

    else:
        resp = requests.get(
            f"https://cassava.ucsd.edu/sparc/datasets/{dataset_uuid}/LATEST/path-metadata.json"
        )

        try:
            blob = resp.json()
        except Exception as e:
            breakpoint()
            raise e

    for j in blob["data"]:
        j["type"] = "pathmeta"

    ir = fromJson(blob)

    updated_transitive = max(
        [i["timestamp_updated"] for i in ir["data"][1:]]
    )  # 1: to skip the dataset object itself

    jpx = [
        r
        for r in ir["data"]
        if "mimetype" in r and r["mimetype"] == "image/jpx"
    ]

    exts = [ext(j) for j in jpx]
    # hrm = sorted(exts, key=lambda j: j['raw_anat_index'])
    # max_rai  = max([e['raw_anat_index'] for e in exts])
    # import math
    # log_max_rai = math.log10(max_rai)

    # normalize the index by mapping distinct values to the integers
    nondist = sorted([e["raw_anat_index_v1"] for e in exts])
    lin_distinct = {
        v: i
        for i, v in enumerate(
            sorted(set([e["raw_anat_index_v1"] for e in exts]))
        )
    }
    max_distinct = len(lin_distinct)
    mdp1 = max_distinct + 0.1  # to simplify adding overlap

    dd = defaultdict(list)
    for e in exts:
        # e['norm_anat_index'] = math.log10(e['raw_anat_index']) / log_max_rai
        pos = lin_distinct[e["raw_anat_index_v1"]]
        e["norm_anat_index_v1"] = (pos + 0.55) / mdp1
        e["norm_anat_index_v1_min"] = pos / mdp1
        e["norm_anat_index_v1_max"] = (
            pos + 1.1
        ) / mdp1  # ensure there is overlap between section for purposes of testing
        # TODO norm_anat_index_min
        # TODO norm_anat_index_max
        dd[e["dataset"], e["sample"]].append(e)
    inst_obj_index = dict(dd)

    max_nai = max([e["norm_anat_index_v1"] for e in exts])
    min_nain = min([e["norm_anat_index_v1_min"] for e in exts])
    max_naix = max([e["norm_anat_index_v1_max"] for e in exts])

    if visualize:
        x = list(range(len(exts)))
        # ry = sorted([e['raw_anat_index'] for e in exts])
        ny = sorted([e["norm_anat_index_v1"] for e in exts])
        nyn = sorted([e["norm_anat_index_v1_min"] for e in exts])
        nyx = sorted([e["norm_anat_index_v1_max"] for e in exts])
        nnx = list(zip(nyn, nyx))
        import pylab as plt
        import seaborn

        # plt.figure()
        # seaborn.scatterplot(x=x, y=ry)
        plt.figure()
        # end = 10
        end = len(x)
        seaborn.scatterplot(x=x[:end], y=ny[:end])
        seaborn.scatterplot(x=x[:end], y=nyn[:end])
        seaborn.scatterplot(x=x[:end], y=nyx[:end])

    datasets = {
        i.uuid: {"id_type": i.type} for e in exts if (i := e["dataset"])
    }

    packages = {
        i.uuid: {
            "id_type": i.type,
            "id_file": e["file_id"],
        }
        for e in exts
        if (i := e["object"])
    }

    objects = {**datasets, **packages}
    dataset_object = list(
        set(
            (d.uuid, o.uuid)
            for e in exts
            if (d := e["dataset"]) and (o := e["object"])
        )
    )

    subjects = {
        k: {
            "type": "subject",
            "desc_inst": "human",
            "id_sub": k[1],
        }
        for k in sorted(set((e["dataset"], e["subject"]) for e in exts))
    }
    segments = {
        k[:2]: {
            "type": "sample",  # FIXME vs below ???
            "desc_inst": "nerve-volume",  # FIXME should this be nerve-segment and then we use nerve-volume for the 1:1 with files?
            "id_sub": k[-1],
            "id_sam": k[1],
        }
        for k in sorted(
            set((e["dataset"], e["sample"], e["subject"]) for e in exts)
        )
    }
    parents = sorted(
        set((e["dataset"],) + p for e in exts for p in e["parents"])
    )
    sam_other = {
        p[:2]: {
            "type": "sample",
            "desc_inst": "nerve",
            "id_sub": p[-1],
            "id_sam": p[1],
        }
        for p in parents
        if p[:2] not in segments
    }
    samples = {**segments, **sam_other}
    instances = {**subjects, **samples}

    values_objects = [
        (i, o["id_type"], o["id_file"] if "id_file" in o else None)
        for i, o in objects.items()
        if o["id_type"] != "dataset"  # already did it above
    ]
    values_dataset_object = dataset_object

    def make_values_instances(i):
        values_instances = [
            (
                d.uuid,
                f,
                inst["type"],
                i.luid[inst["desc_inst"]],
                inst["id_sub"] if "id_sub" in inst else None,
                inst["id_sam"] if "id_sam" in inst else None,
            )
            for (d, f), inst in instances.items()
        ]

        return values_instances

    def make_values_parents(luinst):
        """need the lookup for instances"""
        values_parents = [
            (luinst[d.uuid, child], luinst[d.uuid, parent])
            for d, child, parent in parents
        ]
        return values_parents

    # XXX REMINDER an object descriptor pair can be associated with an arbitrary number of measured instances
    # BUT that mapping only appears when there is _something_ in the qv or cv tables so we need an object
    # desc_inst pair, and an object cat or quant desc pair otherwise our constraints are violated
    # when there is only a single (or zero) records per object then we just create one so that the association
    # to a an instance can proceed, even if the mapping of that instance is from an external source
    # XXX the external source is part of the issue I think
    def make_void(this_dataset_updated_uuid, i):
        void = [  # FIXME this is rather annoying because you have to list all expected types in advance, but I guess that is what we want
            (
                this_dataset_updated_uuid,
                i.id_human,
                i.addr_jpsuid,
                i.addr_jpspec,
            ),
            (
                this_dataset_updated_uuid,
                i.id_nerve,
                i.addr_jpsaid,
                i.addr_jpsaty,
            ),
            (
                this_dataset_updated_uuid,
                i.id_nerve_volume,
                i.addr_jpsaid,
                i.addr_jpsaty,
            ),
            # FIXME what about manifests? those link metadata as an extra hop ... everything meta related needs to come from combined object metadata ???
            # that would certainly make more sense than the nonsense that is going on here, it would simplify the referencing for all the topdown
            # information that we have but it sort of obscures sources, however this _is_ contextual info ... sigh
            # XXX the other option would just be to just put the darned files in the instance measured table :/ because we do have data about them
            # annoying :/
            # + [(i, 'nerve-volume', addr_context) for i in packages]
        ] + [
            (
                o,
                i.id_nerve_volume,
                i.addr_const_null,
                None,
            )  # XXX FIXME this is the only way I can think to do this right now ?
            for o, b in objects.items()
            if b["id_type"] == "package"
        ]

        return void

    def make_vocd(this_dataset_updated_uuid, i):
        vocd = [
            # FIXME this reveals that there are cases where we may not have void for a single file or that the id comes from context and is not embedded
            # figuring out how to turn that around is going to take a bit of thinking
            (this_dataset_updated_uuid, i.cd_mod, i.addr_jpmod),
        ] + [
            (
                o,
                i.cd_bot,
                i.addr_const_null,
            )  # XXX FIXME this is the only way I can think to do this right now ?
            for o, b in objects.items()
            if b["id_type"] == "package"
        ]

        return vocd

    def make_voqd(this_dataset_updated_uuid, i):
        voqd = [  # FIXME this isn't quite right, we should just do it to the segments and pretend it is from the samples file I think?
            # FIXME addr_aspect and addr_unit are ... implicit, there is no conditional dispatch but the choice of the constant quantative descriptor comes from ... tgbugs? or what? I think we just leave it as null because it is a constant across all fields, but maybe we just use constant null? but why bother in that case?
            (this_dataset_updated_uuid, i.qd_nai, i.addr_jpnai),
            (
                this_dataset_updated_uuid,
                i.qd_nain,
                i.addr_jpnain,
            ),  # XXX FIXME is this really an aggregation type in this case? I guess it technically if the sample space is over all the points inside the segment or something
            (this_dataset_updated_uuid, i.qd_naix, i.addr_jpnaix),
        ]
        return voqd

    def make_values_cat(this_dataset_updated_uuid, i, luinst):
        # obj_index = {e['object']: e for e in exts}
        values_cv = [
            # value_open, value_controlled, object, desc_inst, desc_cat
            (
                e[k],
                i.luct[e[k]],
                this_dataset_updated_uuid,
                # e['object'].uuid,  # FIXME still not right this comes from the updated latest
                i.id_nerve_volume,
                cd,  # if we mess this up the fk ok obj_desc_cat will catch it :)
                luinst[e["dataset"].uuid, e["sample"]],  # get us the instance
            )
            for e in exts
            for k, cd in (("modality", i.cd_mod),)
        ] + [
            (
                None,
                i.ct_hack,
                e["object"].uuid,
                i.id_nerve_volume,
                i.cd_bot,  # if we mess this up the fk ok obj_desc_cat will catch it :)
                luinst[e["dataset"].uuid, e["sample"]],  # get us the instance
            )
            for e in exts
        ]
        return values_cv

    def make_values_quant(this_dataset_updated_uuid, i, luinst):
        values_qv = [
            # value, object, desc_inst, desc_quant, inst, value_blob
            (
                e[k],
                # e['object'].uuid,  # FIXME TODO we could fill this here but we choose to use this_dataset_updated_uuid instead I think
                this_dataset_updated_uuid,
                i.id_nerve_volume,
                qd,  # if we mess this up the fk ok obj_desc_cat will catch it :)
                luinst[e["dataset"].uuid, e["sample"]],  # get us the instance
                e[k],
            )
            for e in exts
            for k, qd in (
                # ('raw_anat_index', qd_rai),  # XXX this is a bad place to store object -> field -> qd mappings also risks mismatch on address
                ("norm_anat_index_v1", i.qd_nai),
                ("norm_anat_index_v1_min", i.qd_nain),
                ("norm_anat_index_v1_max", i.qd_naix),
            )
        ]
        return values_qv

    return (
        updated_transitive,
        values_objects,
        values_dataset_object,
        make_values_instances,
        make_values_parents,
        make_void,
        make_vocd,
        make_voqd,
        make_values_cat,
        make_values_quant,
    )
    # this is where things get annoying with needing selects on instance measured


def ingest_reva_ft_all(
    session: Session,  # type: ignore
    source_local: bool = False,
    do_insert: bool = True,
    batch: bool = False,
    commit: bool = False,
    dev: bool = False,
) -> None:
    """
    Ingest all REVA datasets

    Parameters
    ----------
    session : Session
        connection to DB using engine env
    do_insert : bool, optional
        insert the data, by default True
    batch : bool, optional
        batch the inserts, by default False
    commit : bool, optional
       commit the transaction, by default False
    dev : bool, optional
        dev mode, by default False
    """
    dataset_uuids = (
        "aa43eda8-b29a-4c25-9840-ecbd57598afc",  # f001
        # the rest have uuid1 issues :/ all in the undefined folder it seems, might be able to fix with a reupload
        "bc4cc558-727c-4691-ae6d-498b57a10085",  # f002  # XXX has a uuid1 so breaking in prod right now have to push the new pipelines
        "ec6ad74e-7b59-409b-8fc7-a304319b6faf",  # f003  # also uuid1 issue
        "a8b2bdc7-54df-46a3-810e-83cdf33cfc3a",  # f004
        "04a5fed9-7ba6-4292-b1a6-9cab5c38895f",  # f005
    )

    batched = []
    for dataset_uuid in dataset_uuids:
        if do_insert and not batch:
            ingest(
                dataset_uuid=dataset_uuid,
                extract_fun=extract_reva_ft,
                session=session,  # type: ignore
                # source_local=source_local,
                commit=commit,
                dev=dev,
            )
        else:
            # FIXME make it possible to stage everything and then batch the inserts
            values_args = extract_reva_ft(
                dataset_uuid, source_local=source_local
            )
            if batch:
                batched.append((dataset_uuid, values_args))

    if do_insert and batch:
        for duuid, vargs in batched:
            ingest(
                duuid, None, session, commit=commit, dev=dev, values_args=vargs
            )


def main(source_local=False, commit=False, echo=True):
    """Run generic REVA ingest

    WARNING: will be changes to dynamic source other than REVA in future.

    Parameters
    ----------
    source_local : bool, optional
        nothing yet, by default False
    commit : bool, optional
        dry run if false; real ingest if true, by default False
    echo : bool, optional
        TODO: not clear look at alchemy docs, by default True

    Raises
    ------
    e
        _description_
    """
    from quantdb.config import auth

    # pull in the db connection info
    dbkwargs = {
        k: auth.get(f"db-{k}") for k in ("user", "host", "port", "database")
    }  # TODO integrate with cli options
    # custom user variable needed
    dbkwargs["dbuser"] = dbkwargs.pop("user")
    # create connection env with DB
    engine = create_engine(dbUri(**dbkwargs))
    # bool: echo me
    engine.echo = echo
    # use connection env as unique session
    session = Session(engine)

    # try to ingest reva facular tubular all
    try:
        ingest_reva_ft_all(
            session=session,  # type: ignore
            # source_local=source_local=source_local,
            do_insert=True,
            batch=True,
            commit=commit,
            dev=True,
        )
    # failed: undue DB request, close connection, and remove connection env.
    except Exception as e:
        session.rollback()
        session.close()
        engine.dispose()
        raise e

    # rm alloc memory in GIL for connection
    session.close()
    # rm alloc memory in GIL for connection env
    engine.dispose()


if __name__ == "__main__":
    main()
