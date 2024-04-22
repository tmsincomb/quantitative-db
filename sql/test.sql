-- CONNECT TO quantdb_test USER "quantdb-user";

\set dataset_uuid_1 {8b9e0f61-401f-4b9a-a26c-5d8a1481976f}
\set dataset_uuid_2 {77758d88-b525-4d51-be1c-05428ecb2f62}
\set dataset_uuid_3 {18a29eff-ff2e-446f-892c-393f9055457e}
\set dataset_uuid_4 {c88750d4-ce18-4a96-b00b-6b23ee6717a4}

\set package_uuid_1 {1ff97dbc-05e0-4c53-8c92-31a7b9bf75ab}
\set package_uuid_2 {6b823093-a26a-4930-95c5-3f2bc3cfe516}
\set package_uuid_3 {527e9cfe-7925-414c-bcab-72c96813293c}

INSERT INTO objects (id_type, id) VALUES
('dataset', :'dataset_uuid_1'),
('dataset', :'dataset_uuid_2'),
('dataset', :'dataset_uuid_3'),
('dataset', :'dataset_uuid_4')
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

INSERT INTO sds_specimen (dataset, specimen_id) VALUES
(:'dataset_uuid_1', 'sub-1'),
(:'dataset_uuid_2', 'sub-1'),
(:'dataset_uuid_3', 'sub-1'),
(:'dataset_uuid_4', 'sub-1')
;

SELECT * FROM  sds_specimen;

INSERT INTO sds_specimen_equiv (left_thing, right_thing) VALUES (1, 2);
SELECT * FROM  sds_specimen_equiv ORDER BY left_thing;

INSERT INTO sds_specimen_equiv (left_thing, right_thing) VALUES (3, 4);
SELECT * FROM  sds_specimen_equiv ORDER BY left_thing;

SELECT * FROM get_all_equivs(2, 3);
INSERT INTO sds_specimen_equiv (left_thing, right_thing) VALUES (2, 3);
SELECT * FROM  sds_specimen_equiv ORDER BY left_thing;

INSERT INTO class_measured (iri, label) VALUES
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/participant', 'participant'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/specimen', 'specimen'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/subject', 'subject'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/sample', 'sample'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/site', 'site'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/sds/performance', 'performance'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/nerve', 'nerve'), -- nerve or branch
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/nerve/cross-section', 'nerve-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/nerve/longitudinal', 'nerve-longitudinal'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/cell', 'cell'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/neuron', 'neuron'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/soma', 'soma'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/axon', 'axon'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/dendrite', 'dendrite'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/nerve-segment', 'nerve-segment'), -- reva ft unit
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fascicle', 'fascicle'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fiber', 'fiber'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/tissue', 'tissue'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/segment/cross-section', 'segment-cross-section'), -- single z plane, TODO do we need a data perspective name for these, e.g. image-z-stack-plane 3d-volume-plane
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fascicle/cross-section', 'fascicle-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fiber/cross-section', 'fiber-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/tissue/cross-section', 'tissue-cross-section'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fascicle/longitudinal', 'fascicle-longitudinal'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fiber/longitudinal', 'fiber-longitudinal'), -- FIXME axon ??
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/tissue/longitudinal', 'tissue-longitudinal'), -- this is the class we probably use for uct

--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/', ''),
--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/', ''),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/thing', 'class thing')
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
('fasciclePositiveStainType', inst_desc_from_label('fascicle-longitudinal'),  'controlled'),
('fasciclePositiveStainType', inst_desc_from_label('fascicle-cross-section'),  'controlled'),

('fascicleNegativeStainType', inst_desc_from_label('fascicle-longitudinal'),  'controlled'),
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
('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/micrometer', 'um'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/meter', 'm')
-- ('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/', ''),
;

INSERT INTO quant_descriptors (label, is_measuring, aspect, unit, aggregation_type) VALUES
-- lack of support for hierarchical queries over aspects is a killer here, and also for classes
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

('fascicle longitudinal diameter orth-ap-axis um minimum',
inst_desc_from_label('fascicle-longitudinal'),
aspect_from_label('diameter-orthogonal-to-anterior-posterior-axis'), -- and here we see the problem
unit_from_label('um'),
'min'),

('fascicle longitudinal length parallel-to-ap-axis um',
inst_desc_from_label('fascicle-longitudinal'),
aspect_from_label('length-parallel-to-anterior-posterior-axis'),
unit_from_label('um'),
'instance')

;

INSERT INTO addresses (address_type, field_address) VALUES
('tabular-header', 'id'),
('tabular-header', 'stain'),
('tabular-header', 'diameter'),
('tabular-header', 'diameter-min'),
('tabular-header', 'diameter-max')
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

INSERT INTO sds_specimen (dataset, specimen_id) VALUES
(:'dataset_uuid_1', 'sam-seg-c7-A-level-1') -- FIXME I think we need a parent column here as well
;

INSERT INTO instance_measured (inst_desc, dataset, formal_id, specimen_id, subject_id) VALUES
(
inst_desc_from_label('fascicle-cross-section'),
:'dataset_uuid_1',
'fsccs-1',
spec_from_dataset_id(:'dataset_uuid_1', 'sam-seg-c7-A-level-1'),
spec_from_dataset_id(:'dataset_uuid_1', 'sub-1') -- FIXME we need a way to validate this, which means we need the transitive parent table
)
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

SELECT * FROM get_instance_parents(inst_from_dataset_id('diameter-orthogonal-to-anterior-posterior-axis'));
SELECT * FROM get_instance_parent_edges(inst_from_dataset_id('diameter-orthogonal-to-anterior-posterior-axis'));
SELECT * FROM get_all_instance_parents();

-- hrm do we need get_x_children? answer seems to be yes?

-- top down hierarchical query
-- FIXME almost certainly faster to call get_aspect_children once and then do where a0.id in aspect_children ...
-- using explain we can see that this version get_aspect_parents in a loop for all aspects (I think) which is dumb
-- but the query planner may be able to figure it out diesn't actually need to do that for the entire sub select
-- but only for a0.id ??? yeah, seems so
select * from quant_values as qv
join quant_descriptors as qd on qv.quant_desc = qd.id
join aspects as a0 on qd.aspect = a0.id
join (select a.id, p.parent from aspects as a cross join get_aspect_parents(a.id) as p) as ap on ap.id = a0.id
where ap.parent = aspect_from_label('distance');
