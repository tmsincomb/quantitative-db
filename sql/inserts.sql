/*
inserts for various curation flows
*/

-- f006
INSERT INTO addresses (addr_type, addr_field) VALUES
('record-index', NULL),
-- fascicles
('tabular-header', 'fascicle'),
('tabular-header', 'area'),
('tabular-header', 'longest_diameter'),
('tabular-header', 'shortest_diameter'),
('tabular-header', 'eff_diam'),
('tabular-header', 'c_estimate_nav'),
('tabular-header', 'c_estimate_nf'),
('tabular-header', 'nfibers_w_c_estimate_nav'),
('tabular-header', 'nfibers_w_c_estimate_nf'),
('tabular-header', 'nfibers_all'),
('tabular-header', 'n_a_alpha'),
('tabular-header', 'n_a_beta'),
('tabular-header', 'n_a_gamma'),
('tabular-header', 'n_a_delta'),
('tabular-header', 'n_b'),
('tabular-header', 'n_unmyel_nf'),
('tabular-header', 'n_nav'),
('tabular-header', 'n_chat'),
('tabular-header', 'n_myelinated'),
('tabular-header', 'area_a_alpha'),
('tabular-header', 'area_a_beta'),
('tabular-header', 'area_a_gamma'),
('tabular-header', 'area_a_delta'),
('tabular-header', 'area_b'),
('tabular-header', 'area_unmyel_nf'),
('tabular-header', 'area_nav'),
('tabular-header', 'area_chat'),
('tabular-header', 'area_myelinated'),
('tabular-header', 'crop_x_start'),
('tabular-header', 'crop_x_stop'),
('tabular-header', 'crop_y_start'),
('tabular-header', 'crop_y_stop'),
('tabular-header', 'chat_available'),
('tabular-header', 'nav_available'),
('tabular-header', 'th_available'),
('tabular-header', 'x_pix'),
('tabular-header', 'y_pix'),
('tabular-header', 'x_um'),
('tabular-header', 'y_um'),
('tabular-header', 'x_cent'),
('tabular-header', 'y_cent'),
('tabular-header', 'rho'),
('tabular-header', 'rho_pix'),
('tabular-header', 'phi'),
('tabular-header', 'epi_dist'),
('tabular-header', 'epi_dist_inv'),
('tabular-header', 'nerve_based_area'),
('tabular-header', 'nerve_based_perimeter'),
('tabular-header', 'nerve_based_eff_diam'),
('tabular-header', 'perinerium_vertices'),
('tabular-header', 'perinerium_vertices_px'),
('tabular-header', 'nerve_based_shortest_diameter'),
('tabular-header', 'hull_contrs'),
('tabular-header', 'hull_contr_areas'),

-- fibers
('tabular-header', 'a_alpha'),
('tabular-header', 'a_beta'),
('tabular-header', 'a_delta'),
('tabular-header', 'a_gamma'),
('tabular-header', 'b'),
('tabular-header', 'c_estimate_nav'),
('tabular-header', 'c_estimate_nav_frac'),
('tabular-header', 'c_estimate_nf'),
('tabular-header', 'c_estimate_nf_frac'),
('tabular-header', 'chat'),
('tabular-header', 'chat_available'),
('tabular-header', 'eff_fib_diam'),
('tabular-header', 'eff_fib_diam_w_myel'),
('tabular-header', 'fascicle'),
('tabular-header', 'fiber_area'),
('tabular-header', 'fiber_area_pix'),
('tabular-header', 'hull_vertices'),
('tabular-header', 'hull_vertices_w_myel'),
('tabular-header', 'longest_diameter'),
('tabular-header', 'longest_diameter_w_myel'),
('tabular-header', 'max_myelin_thickness'),
('tabular-header', 'median_myelin_thickness'),
('tabular-header', 'myelin_area'),
('tabular-header', 'myelin_area_pix'),
('tabular-header', 'myelinated'),
('tabular-header', 'nav'),
('tabular-header', 'nav_available'),
('tabular-header', 'peri_dist'),
('tabular-header', 'perimeter'),
('tabular-header', 'phi'),
('tabular-header', 'rho'),
('tabular-header', 'rho_pix'),
('tabular-header', 'shortest_diameter'),
('tabular-header', 'shortest_diameter_w_myel'),
('tabular-header', 'th_available'),
('tabular-header', 'th_myelin_p'),
('tabular-header', 'th_not_nf'),
('tabular-header', 'th_overlap_p'),
('tabular-header', 'unmyel_nf'),
('tabular-header', 'x'),
('tabular-header', 'x_pix'),
('tabular-header', 'x_pix_lvl'),
('tabular-header', 'y'),
('tabular-header', 'y_pix'),
('tabular-header', 'y_pix_lvl'),

-- curation export paths
('json-path-with-types', '#/curation-export/subjects/#int/subject_id'),
('json-path-with-types', '#/curation-export/subjects/#int/species#translate_species'),
('json-path-with-types', '#/curation-export/samples/#int/sample_id'),
('json-path-with-types', '#/curation-export/samples/#int/sample_type#translate_sample_type'),
('json-path-with-types', '#/curation-export/sites/#int/site_id'),
('json-path-with-types', '#/curation-export/sites/#int/sites_type#translate_sites_type')

;

INSERT INTO units (iri, label) VALUES
('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/um2', 'um2'),
-- XXX FIXME pixels these are count-of-thing that likely need to be handled separately that is frogs, sheep in field etc. to avoid mirroring hierarchy
('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/pixel', 'pixel'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/pixel-9um', 'pixel-9um'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/unit/pixel-11um', 'pixel-11um') -- FIXME might be 11.something
;

INSERT INTO aspects (iri, label) VALUES
-- technically position is a vector of distances from a defined starting point or in a defined coordinate system
('http://uri.interlex.org/tgbugs/uris/readable/aspect/position', 'position'), -- XXX this is always relative to something that needs to be specified
('http://uri.interlex.org/tgbugs/uris/readable/aspect/centroid', 'centroid'),
-- distance
('http://uri.interlex.org/tgbugs/uris/readable/aspect/distance-along-axis', 'distance-along-axis'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/centroid-x', 'centroid-x'), -- FIXME technically this is part of a cenroid aspects but vectors ...
('http://uri.interlex.org/tgbugs/uris/readable/aspect/centroid-y', 'centroid-y'), -- FIXME technically this is part of a cenroid aspects but vectors ...
('http://uri.interlex.org/tgbugs/uris/readable/aspect/radius', 'radius'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/radius/from/parent/centroid', 'radius-from-parent-centroid'), -- from-parent-instance-cendroid but instance is implied
('http://uri.interlex.org/tgbugs/uris/readable/aspect/perimeter', 'perimeter'),

('http://uri.interlex.org/tgbugs/uris/readable/aspect/area', 'area'),

('http://uri.interlex.org/tgbugs/uris/readable/aspect/volume', 'volume'),

('http://uri.interlex.org/tgbugs/uris/readable/aspect/angle', 'angle'),
('http://uri.interlex.org/tgbugs/uris/readable/aspect/angle/from/parent/centroid', 'angle-from-parent-centroid') -- FIXME -from- implies we need context for these

;

INSERT INTO aspect_parent (id, parent) VALUES
(aspect_from_label('radius'), aspect_from_label('distance')),
(aspect_from_label('perimeter'), aspect_from_label('distance')),
(aspect_from_label('angle-from-parent-centroid'), aspect_from_label('angle')),
(aspect_from_label('radius-from-parent-centroid'), aspect_from_label('radius')),

(aspect_from_label('centroid'), aspect_from_label('position')),
(aspect_from_label('distance-along-axis'), aspect_from_label('distance')),
(aspect_from_label('centroid-x'), aspect_from_label('distance-along-axis')), -- technically correct but annoying to find due to needing partOf or similar
(aspect_from_label('centroid-y'), aspect_from_label('distance-along-axis')) -- and due to lacking vectors atm
;

INSERT INTO descriptors_quant (label, domain, aspect, unit, aggregation_type) VALUES

('fascicle cross section area um2',
desc_inst_from_label('fascicle-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um2'),
'instance'),

('fiber cross section area um2',
desc_inst_from_label('fiber-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um2'),
'instance'),

('fiber cross section diameter um',
desc_inst_from_label('fiber-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um'),
'instance'),

('fiber cross section diameter um min',
desc_inst_from_label('fiber-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um'),
'min'),

('fiber cross section diameter um max',
desc_inst_from_label('fiber-cross-section'),
aspect_from_label('diameter'),
unit_from_label('um'),
'max')

;

INSERT INTO descriptors_cat (label, domain, range) VALUES
('hasAxonFiberType', NULL, 'controlled')

;

INSERT INTO descriptors_inst (iri, label) VALUES
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/myelin', 'myelin'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/myelin/cross-section', 'myelin-cross-section'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/extruded-plane', 'extruded-plane') -- for sites, no extra hierarchy yet
;

INSERT INTO controlled_terms (iri, label) VALUES
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/axonFiberType/myelinated', 'myelinated'),
('https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/axonFiberType/unmyelinated', 'unmyelinated')
;
