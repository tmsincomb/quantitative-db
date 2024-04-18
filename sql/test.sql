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
