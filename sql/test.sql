-- CONNECT TO quantdb_test USER "quantdb-user";

\set dataset_uuid_1 {8b9e0f61-401f-4b9a-a26c-5d8a1481976f}
\set dataset_uuid_2 {77758d88-b525-4d51-be1c-05428ecb2f62}
\set dataset_uuid_3 {18a29eff-ff2e-446f-892c-393f9055457e}
\set dataset_uuid_4 {c88750d4-ce18-4a96-b00b-6b23ee6717a4}


INSERT INTO objects (id_type, id) VALUES ('dataset', :'dataset_uuid_1');
INSERT INTO objects (id_type, id) VALUES ('dataset', :'dataset_uuid_2');
INSERT INTO objects (id_type, id) VALUES ('dataset', :'dataset_uuid_3');
INSERT INTO objects (id_type, id) VALUES ('dataset', :'dataset_uuid_4');

INSERT INTO sds_specimen (dataset, specimen_id) VALUES (:'dataset_uuid_1', 'sub-1');
INSERT INTO sds_specimen (dataset, specimen_id) VALUES (:'dataset_uuid_2', 'sub-1');
INSERT INTO sds_specimen (dataset, specimen_id) VALUES (:'dataset_uuid_3', 'sub-1');
INSERT INTO sds_specimen (dataset, specimen_id) VALUES (:'dataset_uuid_4', 'sub-1');

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

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fascicle', 'fascicle'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fiber', 'fiber'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/tissue', 'tissue'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fascicle/cross-section', 'fascicle-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fiber/cross-section', 'fiber-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/tissue/cross-section', 'tissue-cross-section'),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fascicle/longitudinal', 'fascicle-longitudinal'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/fiber/longitudinal', 'fiber-longitudinal'), -- FIXME axon ??
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/virtual/tissue/longitudinal', 'tissue-longitudinal'),

--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/', ''),
--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/', ''),

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/thing', 'class thing')
;

INSERT INTO controlled_terms (iri, label) VALUES
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/fascicleStainType/chat', 'chat'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/fascicleStainType/myelin', 'myelin'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/fascicleStainType/neurofilament', 'neurofilament'),

--('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/', '')

('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/thing', 'controlled thing')
;

INSERT INTO cat_descriptors (label, is_measuring, range) VALUES
('fasciclePositiveStainType', (SELECT id from class_measured WHERE label = 'fascicle-longitudinal'),  'controlled'),
('fasciclePositiveStainType', (SELECT id from class_measured WHERE label = 'fascicle-cross-section'),  'controlled')
;

INSERT INTO aspects (iri, label) VALUES
('http://uri.interlex.org/tgbugs/uris/readable/aspect/diameter', 'diameter'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/diameter-orthogonal-to-anterior-posterior-axis', 'diameter-orthogonal-to-anterior-posterior-axis'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/length-parallel-to-anterior-posterior-axis', 'length-parallel-to-anterior-posterior-axis')
-- ('http://uri.interlex.org/tgbugs/uris/readable/aspect/', ''),
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
(select id from class_measured where label = 'fascicle-cross-section'),
(select id from aspects where label = 'diameter'),
(select id from units where label = 'um'),
'instance'),

('fascicle longitudinal diameter orth-ap-axis um minimum',
(select id from class_measured where label = 'fascicle-longitudinal'),
(select id from aspects where label = 'diameter-orthogonal-to-anterior-posterior-axis'), -- and here we see the problem
(select id from units where label = 'um'),
'min'),

('fascicle longitudinal length parallel-to-ap-axis um',
(select id from class_measured where label = 'fascicle-longitudinal'),
(select id from aspects where label = 'length-parallel-to-anterior-posterior-axis'),
(select id from units where label = 'um'),
'instance')

;
