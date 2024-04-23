-- CONNECT TO quantdb_test USER "quantdb-user";

\set dataset_uuid_1 {8b9e0f61-401f-4b9a-a26c-5d8a1481976f}
\set dataset_uuid_2 {77758d88-b525-4d51-be1c-05428ecb2f62}
\set dataset_uuid_3 {18a29eff-ff2e-446f-892c-393f9055457e}
\set dataset_uuid_4 {c88750d4-ce18-4a96-b00b-6b23ee6717a4}

\set package_uuid_1 {1ff97dbc-05e0-4c53-8c92-31a7b9bf75ab}
\set package_uuid_2 {6b823093-a26a-4930-95c5-3f2bc3cfe516}
\set package_uuid_3 {527e9cfe-7925-414c-bcab-72c96813293c}

\set internal_uuid_1 {2d0267ee-6fb9-449a-b0c3-3a46d1fc5783}
\set max_uuid        {ffffffff-ffff-ffff-ffff-ffffffffffff} -- ignore the limits of 4 and b for now

INSERT INTO objects (id_type, id) VALUES
('dataset', :'dataset_uuid_1'),
('dataset', :'dataset_uuid_2'),
('dataset', :'dataset_uuid_3'),
('dataset', :'dataset_uuid_4'),
;

/* -- TODO test-negative
INSERT INTO objects (id_type, id) VALUES
('package', :'package_uuid_1')
;
*/

INSERT INTO objects (id_type, id, id_file) VALUES
('package', :'package_uuid_1', 0),
('package', :'package_uuid_2', 0),
('package', :'package_uuid_3', 0)
;

INSERT INTO objects (id_type, id, id_internal) VALUES
('internal', :'internal_uuid_1', 'trigger_objects_uuid_zero_to_one')
;

INSERT INTO class_measured (iri, label) VALUES -- NOTE TO SELF class measured is where we handle hierarchical resolution we don't do that for categorical values right now which is why we have the complexity in this table
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/participant', 'participant'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/specimen', 'specimen'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/subject', 'subject'),  -- XXX now dragged up to instance_type
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/sample', 'sample'),  -- XXX now dragged up to instance_type
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/site', 'site'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/performance', 'performance'),

-- FIXME ncbitax obvs, with subject now at type level we can include the species taxononmy here instead of in controlled terms
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/human', 'human'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/rat', 'rat'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/mouse', 'mouse'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/pig', 'pig'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/dog', 'dog'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/cat', 'cat'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/ferret', 'ferret'),

--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual', 'virtual'), -- a data signature of a real thing (don't do it this way)

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve', 'nerve'), -- nerve or branch but really just nerve or part of nerve
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve/volume', 'nerve-volume'), -- aka
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve/cross-section', 'nerve-cross-section'), -- an atomic section wrt the data
--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve-segment', 'nerve-segment'), -- reva ft unit XXX do not use equiv to volume
--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/segment/cross-section', 'segment-cross-section'), -- single z plane, TODO do we need a data perspective name for these, e.g. image-z-stack-plane 3d-volume-plane

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/cell', 'cell'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/neuron', 'neuron'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/soma', 'soma'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/axon', 'axon'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/dendrite', 'dendrite'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/fascicle', 'fascicle'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/fiber', 'fiber'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/tissue', 'tissue'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/fascicle/cross-section', 'fascicle-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/fiber/cross-section', 'fiber-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/tissue/cross-section', 'tissue-cross-section'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/fascicle/volume', 'fascicle-volume'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/fiber/volume', 'fiber-volume'), -- FIXME axon ??
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/tissue/volume', 'tissue-volume'), -- this is the class we probably use for uct

--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/', ''),
--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/', ''),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/thing', 'class thing')
;

INSERT INTO class_parent (id, parent) VALUES
-- TODO populate from subClassOf query from sparc-methods.ttl
-- FIXME also need to map to thing in cases where missing?
(inst_desc_from_label('specimen'),               inst_desc_from_label('participant')),
(inst_desc_from_label('subject'),                inst_desc_from_label('specimen')),
(inst_desc_from_label('sample'),                 inst_desc_from_label('specimen')), -- FIXME TODO do we have multi-parent for samples with types?
(inst_desc_from_label('nerve-volume'),           inst_desc_from_label('nerve')),
(inst_desc_from_label('nerve-cross-section'),    inst_desc_from_label('nerve')),
(inst_desc_from_label('fascicle-volume'),        inst_desc_from_label('fascicle')),
(inst_desc_from_label('fascicle-cross-section'), inst_desc_from_label('fascicle')),
(inst_desc_from_label('fiber-volume'),           inst_desc_from_label('fiber')),
(inst_desc_from_label('fiber-cross-section'),    inst_desc_from_label('fiber'))
--(inst_desc_from_label(''),                       inst_desc_from_label('')),

;

INSERT INTO controlled_terms (iri, label) VALUES
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/fascicleStainType/chat', 'chat'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/fascicleStainType/myelin', 'myelin'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/fascicleStainType/neurofilament', 'neurofilament'),

('http://purl.obolibrary.org/obo/UBERON_0001021', 'nerve'),

--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/', '')

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/thing', 'controlled thing')
;

INSERT INTO cat_descriptors (label, is_measuring, range) VALUES

-- these are examples that probably should not be used on real data because they are too
-- granular to be useful without the ability to traverse in the hierarchy
('fasciclePositiveStainType', inst_desc_from_label('fascicle-volume'),  'controlled'),
('fasciclePositiveStainType', inst_desc_from_label('fascicle-cross-section'),  'controlled'),

('fascicleNegativeStainType', inst_desc_from_label('fascicle-volume'),  'controlled'),
('fascicleNegativeStainType', inst_desc_from_label('fascicle-cross-section'),  'controlled'),

('positiveStainType', NULL, 'controlled'), -- this is a more practical approach
('hasAnatomicalTag', NULL, 'controlled')
;

INSERT INTO aspects (iri, label) VALUES
-- RULING distance is more fundamental, however it is literally definitional
-- unless it is being derived from some other quantity such as c
-- this is becuase you have to pick two specific points in order to define length
-- while distance is abstract and can unitized in countless ways (i.e. based on
-- which two points you pick)

('http://uri.interlex.org/tgbugs/uris/readable/aspect/distance', 'distance'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/length', 'length'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/diameter', 'diameter'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/diameter-orthogonal-to-anterior-posterior-axis', 'diameter-orthogonal-to-anterior-posterior-axis'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/length-parallel-to-anterior-posterior-axis', 'length-parallel-to-anterior-posterior-axis')
-- ('http://uri.interlex.org/tgbugs/uris/readable/aspect/', ''),
;

INSERT INTO aspect_parent (id, parent) VALUES
(aspect_from_label('length'), aspect_from_label('distance')),
(aspect_from_label('diameter'), aspect_from_label('length')),
(aspect_from_label('diameter-orthogonal-to-anterior-posterior-axis'), aspect_from_label('diameter')),
(aspect_from_label('length-parallel-to-anterior-posterior-axis'), aspect_from_label('length'))
--(aspect_from_label(), aspect_from_label()),
;

INSERT INTO units (iri, label) VALUES
-- obvious we need synonyms
-- TODO need to decide on naming convention for units becuase label is likely to be the primary interface to the table
--('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/unitless', 'unitless'), -- FIXME VS NULL

('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/micrometer', 'um'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/meter', 'm')
-- ('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/', ''),
;


INSERT INTO quant_descriptors (label, is_measuring, aspect, unit, aggregation_type) VALUES
-- lack of support for hierarchical queries over aspects is a killer here, and also for classes

('thing object uuid ratio', -- multiple objects means the entity will show up at multiple places, OR pick min or something ...
/*
FIXME technically in order to insert these we need to have an instance AND at least one object
we don't want to create a bunch of file object instances though, the first time that we actually know that an
object id has data about an instance id is when it is entered into quant_values or cat_values
which means that if we don't want the uuid ratio process to create spurious entities then we can only
run this on whole files that map directly subject or sample level, which is already what we assume
for the reva ft vagus segment microct case
another way to frame the issue is that if you have multiple files with data that maps to slightly different
parts of an instance then you probably want/need slightly different instance ids to map because the data model
assumes that the data inside individual records (sometimes records are whole files) correspond to a single instance
cases of multiple reva ft uct images for single sample segment are an example of the issue
i.e. we may need sites, otherwise files "about" the same thing are going to teleport and the location
of an object in uuid space will change over time ... ah well, that is the issue with this coordinate system
which is that objects are located in n dimensional uuid space, not just 1d uuid space, and n changes over
time, so either you take the norm or you take the min across all axes but that has to be done after the fact

quant_values or cat_values ? but that doesn't work because of the circularity, the problem is that we already
instance ids for this to work
*/
NULL, -- inst_desc_from_label('file'), -- FIXME VS inst_desc_from_label('class thing'),
aspect_from_label('distance-object-uuid-ratio'), -- yes this is an abstract distance in UUID space between zero and max int 128
-- curator_note: non-locality of mapping to uuid space is fun
NULL,
'instance'
),

('thing min object uuid ratio',
/*
FIXME TODO practical issue: we assume objects are immutable, so thing object uuid ratio is ok
because we create a obj_quant_descriptor for it for it that references the non-min version of
this and will always be static, however min could change every time ... actually ... we are ok
right now because there are no unique constraints on enabled right now, but as soon as we do
enable them we are going to be in trouble and if we don't enable them then we will have multiple
min values in the database, from a conceptual standpoint this means that internal objects have to
be append only as well, they don't have to be immutable, but any additions to them can't result in
a change to an existing record, this means that when we recompute the min we have to issue a new
internal object id, but we might be able to be smart about and have e.g. one uuid for the 1st
min addition, 2nd, etc. essentially pretending that there is an append only no duplicates file
that tracks the min as things update ... this is ... not an ideal solution, and it also shows that
there will be issue if there are multiple versions of the same measures ... this is already
discussed in the context of aspects when dealing with multiple very similar columns
either you factor out performance, or the analysis is different so the results may share a superclass
but have to be distinct in their aspect name or something
*/
NULL,
aspect_from_label('distance-object-uuid-ratio'),
NULL,
'min'
),

('fascicle cross section diameter um',
inst_desc_from_label('fascicle-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um'),
'instance'),

('fascicle cross section diameter um min',
inst_desc_from_label('fascicle-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um'),
'min'), -- FIXME ... not really an agg type ...

('fascicle cross section diameter um max',
inst_desc_from_label('fascicle-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um'),
'max'), -- FIXME ... not really an agg type ...

('fascicle volume diameter orth-ap-axis um minimum',
inst_desc_from_label('fascicle-volume'),
aspect_from_label('diameter-orthogonal-to-anterior-posterior-axis'), -- and here we see the problem
unit_from_label('um'),
'min'),

('fascicle volume length parallel-to-ap-axis um',
inst_desc_from_label('fascicle-volume'),
aspect_from_label('length-parallel-to-anterior-posterior-axis'),
unit_from_label('um'),
'instance')

;

INSERT INTO addresses (address_type, field_address) VALUES
('tabular-header', 'id'),
('tabular-header', 'stain'),
('tabular-header', 'diameter'),
('tabular-header', 'diameter-min'),
('tabular-header', 'diameter-max'),
--('image-axis-index', 'x'), -- TODO we aren't reading values out here, we are using implicit values to assign an id
--(,)
;

INSERT INTO addresses (address_type, field_address, value_type) VALUES
('tabular-header', 'stain', 'multi'),
('tabular-header', 'stain-negative', 'multi')
--(,,)
;

INSERT INTO obj_inst_descriptors (object_id, inst_desc, field_address) VALUES
(:'package_uuid_1',
inst_desc_from_label('fascicle-cross-section'),
address_from_fadd_type_fadd('tabular-header', 'id'))
;

INSERT INTO obj_cat_descriptors (object_id, cat_desc, field_address) VALUES
(:'package_uuid_1', -- FIXME obviously a bad example because there could be multiple positive strains
cat_desc_from_label_measuring_label('fasciclePositiveStainType', 'fascicle-cross-section'),
address_from_fadd_type_fadd_vtype('tabular-header', 'stain', 'multi')),

(:'package_uuid_1',
cat_desc_from_label_measuring_label('fascicleNegativeStainType', 'fascicle-cross-section'),
address_from_fadd_type_fadd_vtype('tabular-header', 'stain-negative', 'multi'))

;

INSERT INTO obj_quant_descriptors (object_id, quant_desc, field_address) VALUES
(:'package_uuid_1',
quant_desc_from_label('fascicle cross section diameter um'),
address_from_fadd_type_fadd('tabular-header', 'diameter')),

(:'package_uuid_1',
quant_desc_from_label('fascicle cross section diameter um min'),
address_from_fadd_type_fadd('tabular-header', 'diameter-min')),

(:'package_uuid_1',
quant_desc_from_label('fascicle cross section diameter um max'),
address_from_fadd_type_fadd('tabular-header', 'diameter-max'))

;

-- query to get object ids that have not been extracted at all
SELECT objects.id_type, oid.object_id FROM obj_inst_descriptors AS oid
LEFT JOIN quant_values as qv
ON oid.object_id = qv.object_id
LEFT JOIN cat_values as cv
ON oid.object_id = cv.object_id
JOIN objects ON oid.object_id = objects.id
WHERE qv.object_id is NULL AND cv.object_id is NULL;

SELECT * FROM get_all_values_example();
SELECT 'guo-1';
SELECT * FROM get_unextracte_objects();

/*
-- more granular query to get object ids where cat or quant data have not
-- been extracted for the specific descriptors that have been specified
-- this makes it possible to do incremental extraction
SELECT objects.id_type, oqd.object_id FROM obj_quant_descriptors AS oqd
LEFT JOIN quant_values as qv
ON oqd.object_id = qv.object_id
JOIN objects ON oqd.object_id = objects.id AND oqd.quant_desc = qv.quant_desc
WHERE qv.object_id is NULL

UNION

SELECT objects.id_type, ocd.object_id FROM obj_cat_descriptors AS ocd
-- FIXME this can't detect cases where an UPDATE modified a cat_desc address value type or changed an address
-- e.g. if a curator makes a mistake, I guess we can have a force rerun option or add an AFTER UPDATE trigger
-- the obj_x_descriptor tables ??
LEFT JOIN cat_values as cv
ON ocd.object_id = cv.object_id
JOIN objects ON ocd.object_id = objects.id AND ocd.cat_desc = cv.cat_desc
WHERE cv.object_id is NULL;
*/

-- the order of inserts here more or less corresponds to the order needed for the workflow

INSERT INTO instance_measured (type, inst_desc, dataset, formal_id) VALUES
('subject', inst_desc_from_label('human'), :'dataset_uuid_1', 'sub-1'),
('sample', inst_desc_from_label('nerve-volume'), :'dataset_uuid_1', 'sam-l'), -- FIXME and here we see the issue with nerve vs nerve-thing
('sample', inst_desc_from_label('nerve-cross-section'), :'dataset_uuid_1', 'sam-l-seg-c7-A-level-1'),

('subject', inst_desc_from_label('human'), :'dataset_uuid_2', 'sub-1'),
('subject', inst_desc_from_label('human'), :'dataset_uuid_3', 'sub-1'),
('subject', inst_desc_from_label('human'), :'dataset_uuid_4', 'sub-1')
;

SELECT * FROM  instance_measured;

INSERT INTO inst_equiv (left_thing, right_thing) VALUES (1, 2);
SELECT * FROM  inst_equiv ORDER BY left_thing;

INSERT INTO inst_equiv (left_thing, right_thing) VALUES (3, 4);
SELECT * FROM  inst_equiv ORDER BY left_thing;

SELECT * FROM get_all_equivs(2, 3);
INSERT INTO inst_equiv (left_thing, right_thing) VALUES (2, 3);
SELECT * FROM  inst_equiv ORDER BY left_thing;

INSERT INTO instance_subject (id, subject) VALUES
(inst_from_dataset_id(:'dataset_uuid_1', 'sam-l'), inst_from_dataset_id(:'dataset_uuid_1', 'sub-1')),
(inst_from_dataset_id(:'dataset_uuid_1', 'sam-l-seg-c7-A-level-1'), inst_from_dataset_id(:'dataset_uuid_1', 'sub-1'))
;

INSERT INTO instance_measured (type, inst_desc, dataset, formal_id) VALUES
(
'below',
inst_desc_from_label('fascicle-cross-section'),
:'dataset_uuid_1',
'fsccs-1'
)
;

INSERT INTO instance_parent (id, parent) VALUES
(inst_from_dataset_id(:'dataset_uuid_1', 'sam-l'), inst_from_dataset_id(:'dataset_uuid_1', 'sub-1')),
(inst_from_dataset_id(:'dataset_uuid_1', 'sam-l-seg-c7-A-level-1'), inst_from_dataset_id(:'dataset_uuid_1', 'sam-l')),
(inst_from_dataset_id(:'dataset_uuid_1', 'fsccs-1'), inst_from_dataset_id(:'dataset_uuid_1', 'sam-l-seg-c7-A-level-1'))
;

INSERT INTO instance_subject (id, subject) VALUES
(inst_from_dataset_id(:'dataset_uuid_1', 'fsccs-1'), inst_from_dataset_id(:'dataset_uuid_1', 'sub-1'))
;

INSERT INTO instance_sample (id, sample) VALUES
(inst_from_dataset_id(:'dataset_uuid_1', 'fsccs-1'), inst_from_dataset_id(:'dataset_uuid_1', 'sam-l-seg-c7-A-level-1'))
;

INSERT INTO cat_values (object_id, inst_desc, cat_desc, measured_instance, value_open, value_controlled) VALUES
(
:'package_uuid_1',
inst_desc_from_label('fascicle-cross-section'),
cat_desc_from_label_measuring_label('fasciclePositiveStainType', 'fascicle-cross-section'),
inst_from_dataset_id(:'dataset_uuid_1', 'fsccs-1'),
'chat',
cterm_from_label('chat')
)
;

INSERT INTO quant_values (object_id, inst_desc, quant_desc, measured_instance, value_blob, value) VALUES
 -- FIXME TODO ensure that dataset/package combo matches and is present
(
:'package_uuid_1',
inst_desc_from_label('fascicle-cross-section'), -- TODO automate filling from inst itself probably since it is more for convenience here
quant_desc_from_label('fascicle cross section diameter um'),
inst_from_dataset_id(:'dataset_uuid_1', 'fsccs-1'),
to_jsonb(342),
342
)
;

SELECT * FROM get_all_values_example();
SELECT 'guo-2';
SELECT * FROM get_unextracte_objects();

-- hierachy testing
SELECT * FROM get_aspect_parents(aspect_from_label('diameter-orthogonal-to-anterior-posterior-axis'));
SELECT * FROM get_aspect_parent_edges(aspect_from_label('diameter-orthogonal-to-anterior-posterior-axis'));
SELECT * FROM get_all_aspect_parents();

SELECT * FROM get_class_parents(inst_desc_from_label('fascicle-cross-section'));
SELECT * FROM get_class_parent_edges(inst_desc_from_label('fascicle-cross-section'));
SELECT * FROM get_all_class_parents();

SELECT * FROM get_instance_parents(inst_from_dataset_id(:'dataset_uuid_1', 'fsccs-1'));
SELECT * FROM get_instance_parent_edges(inst_from_dataset_id(:'dataset_uuid_1', 'fsccs-1'));
SELECT * FROM get_all_instance_parents();

-- hrm do we need get_x_children? answer seems to be yes?

-- top down hierarchical query
-- FIXME almost certainly faster to call get_aspect_children once and then do where a0.id in aspect_children ...
-- using explain we can see that this version get_aspect_parents in a loop for all aspects (I think) which is dumb
-- but the query planner may be able to figure it out diesn't actually need to do that for the entire sub select
-- but only for a0.id ??? yeah, seems so
/*
select * from quant_values as qv
join quant_descriptors as qd on qv.quant_desc = qd.id
join aspects as a0 on qd.aspect = a0.id
join (select a.id, p.parent from aspects as a cross join get_aspect_parents(a.id) as p) as ap on ap.id = a0.id
where ap.parent = aspect_from_label('distance');
*/

-- FIXME for our join use case we likely want this to include the starting class itself
SELECT * FROM get_aspect_children(aspect_from_label('distance'));
SELECT * FROM get_aspect_child_edges(aspect_from_label('distance'));

SELECT * FROM get_aspect_children(aspect_from_label('diameter'));
SELECT * FROM get_aspect_child_edges(aspect_from_label('diameter'));

select qv from get_aspect_children(aspect_from_label('distance')) as ap
join quant_descriptors as qd on qd.aspect = ap.child
join quant_values as qv on qv.quant_desc = qd.id;

--SELECT * FROM get_class_children(inst_desc_from_label('participant'));

SELECT cm.label FROM get_class_children(inst_desc_from_label('participant')) as c join class_measured as cm on c.child = cm.id ;
SELECT cm.label FROM get_class_children(inst_desc_from_label('nerve')) as c join class_measured as cm on c.child = cm.id ;
