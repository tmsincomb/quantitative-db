#!/usr/bin/env python3
"""
F006 Dataset Ingestion using Generic Ingest Approach

This script demonstrates a simplified ingestion process using the ORM models
and generic_ingest helper functions instead of raw SQL.

Dataset ID: 2a3d01c0-39d3-464a-8746-54c9d67ebe0f
"""

import json
import pathlib
import uuid
from datetime import datetime

from quantdb.client import get_session
from quantdb.generic_ingest import back_populate_tables, get_or_create
from quantdb.models import (
    Addresses,
    Aspects,
    ControlledTerms,
    DescriptorsCat,
    DescriptorsInst,
    DescriptorsQuant,
    ObjDescCat,
    ObjDescInst,
    ObjDescQuant,
    Objects,
    Units,
    ValuesCat,
    ValuesInst,
    ValuesQuant,
)

# Dataset configuration
DATASET_UUID = '2a3d01c0-39d3-464a-8746-54c9d67ebe0f'
DATA_DIR = pathlib.Path(__file__).parent / 'data'


def load_path_metadata():
    """Load the path metadata from local JSON file."""
    metadata_file = DATA_DIR / 'f006_path_metadata.json'
    with open(metadata_file, 'r') as f:
        return json.load(f)


def parse_path_structure(path_parts):
    """
    Parse the path structure to extract subject, sample, and modality info.
    Example path: sub-f006/sam-l-seg-c1/microct/image001.jpx
    """
    if len(path_parts) >= 4:
        subject_id = path_parts[0]  # sub-f006
        sample_id = path_parts[1]  # sam-l-seg-c1
        modality = path_parts[2]  # microct
        filename = path_parts[3]  # image001.jpx

        return {'subject_id': subject_id, 'sample_id': sample_id, 'modality': modality, 'filename': filename}
    else:
        raise ValueError(f'Unexpected path structure: {path_parts}')


def create_basic_descriptors(session):
    """Create basic descriptors needed for f006 dataset."""

    # === ROOT TABLES ===

    # Create Aspects (ROOT TABLE)
    aspects = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/aspect/volume', 'label': 'volume'},
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/aspect/length', 'label': 'length'},
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/aspect/diameter', 'label': 'diameter'},
    ]

    created_aspects = {}
    for aspect_data in aspects:
        aspect = Aspects(iri=aspect_data['iri'], label=aspect_data['label'])
        created_aspect = get_or_create(session, aspect)
        created_aspects[aspect_data['label']] = created_aspect

    # Create Units (ROOT TABLE)
    units = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/unit/cubic-millimeter', 'label': 'mm3'},
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/unit/millimeter', 'label': 'mm'},
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/unit/micrometer', 'label': 'um'},
    ]

    created_units = {}
    for unit_data in units:
        unit = Units(iri=unit_data['iri'], label=unit_data['label'])
        created_unit = get_or_create(session, unit)
        created_units[unit_data['label']] = created_unit

    # Create DescriptorsInst (ROOT TABLE)
    descriptors = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/human', 'label': 'human'},
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve-volume', 'label': 'nerve-volume'},
        {
            'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve-cross-section',
            'label': 'nerve-cross-section',
        },
    ]

    created_descriptors = {}
    for desc_data in descriptors:
        desc_inst = DescriptorsInst(iri=desc_data['iri'], label=desc_data['label'])
        created_desc = get_or_create(session, desc_inst)
        created_descriptors[desc_data['label']] = created_desc

    # Create ControlledTerms (ROOT TABLE)
    controlled_terms = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/microct', 'label': 'microct'},
    ]

    created_terms = {}
    for term_data in controlled_terms:
        term = ControlledTerms(iri=term_data['iri'], label=term_data['label'])
        created_term = get_or_create(session, term)
        created_terms[term_data['label']] = created_term

    # Create Addresses (ROOT TABLE)
    const_addr = session.query(Addresses).filter_by(addr_type='constant', value_type='single').first()
    if not const_addr:
        const_addr = Addresses(addr_type='constant', addr_field=None, value_type='single')
        session.add(const_addr)
        session.commit()

    # Create tabular address for data fields
    tabular_addr = (
        session.query(Addresses).filter_by(addr_type='tabular-header', addr_field='volume', value_type='single').first()
    )
    if not tabular_addr:
        tabular_addr = Addresses(addr_type='tabular-header', addr_field='volume', value_type='single')
        session.add(tabular_addr)
        session.commit()

    # === INTERMEDIATE TABLES ===

    # Create DescriptorsCat (INTERMEDIATE TABLE - depends on DescriptorsInst)
    modality_desc = DescriptorsCat(
        domain=created_descriptors['nerve-volume'].id, range='controlled', label='hasDataAboutItModality'
    )
    modality_desc.descriptors_inst = created_descriptors['nerve-volume']
    created_modality_desc = get_or_create(session, modality_desc)

    # Create DescriptorsQuant (INTERMEDIATE TABLE - depends on Units, Aspects, DescriptorsInst)
    # Example 1: Nerve volume in mm3
    nerve_volume_desc = DescriptorsQuant(
        shape='scalar',
        label='nerve-volume-mm3',
        aggregation_type='instance',
        unit=created_units['mm3'].id,
        aspect=created_aspects['volume'].id,
        domain=created_descriptors['nerve-volume'].id,
        description='Volume of nerve segment in cubic millimeters',
    )
    nerve_volume_desc.units = created_units['mm3']
    nerve_volume_desc.aspects = created_aspects['volume']
    nerve_volume_desc.descriptors_inst = created_descriptors['nerve-volume']
    created_nerve_volume_desc = get_or_create(session, nerve_volume_desc)

    # Example 2: Nerve cross-section diameter in um
    nerve_diameter_desc = DescriptorsQuant(
        shape='scalar',
        label='nerve-cross-section-diameter-um',
        aggregation_type='instance',
        unit=created_units['um'].id,
        aspect=created_aspects['diameter'].id,
        domain=created_descriptors['nerve-cross-section'].id,
        description='Diameter of nerve cross-section in micrometers',
    )
    nerve_diameter_desc.units = created_units['um']
    nerve_diameter_desc.aspects = created_aspects['diameter']
    nerve_diameter_desc.descriptors_inst = created_descriptors['nerve-cross-section']
    created_nerve_diameter_desc = get_or_create(session, nerve_diameter_desc)

    return {
        'aspects': created_aspects,
        'units': created_units,
        'descriptors': created_descriptors,
        'terms': created_terms,
        'modality_desc': created_modality_desc,
        'nerve_volume_desc': created_nerve_volume_desc,
        'nerve_diameter_desc': created_nerve_diameter_desc,
        'const_addr': const_addr,
        'tabular_addr': tabular_addr,
    }


def ingest_objects_table(session, metadata, components):
    """Ingest objects into the objects table using ORM."""

    print('=== Ingesting Objects Table ===')

    # Create dataset object
    dataset_obj = Objects(id=DATASET_UUID, id_type='dataset', id_file=None, id_internal=None)
    dataset_result = get_or_create(session, dataset_obj)
    print(f'Created/found dataset object: {dataset_result.id}')

    # Create package objects for each file
    created_objects = []
    for item in metadata['data']:
        package_id = str(uuid.uuid4())  # Generate UUID for package

        package_obj = Objects(id=package_id, id_type='package', id_file=item['file_id'], id_internal=None)
        # Set relationship to dataset
        package_obj.objects_ = dataset_result

        package_result = get_or_create(session, package_obj)
        created_objects.append(package_result)
        print(f'Created package object: {package_result.id} for file_id: {package_result.id_file}')

    return dataset_result, created_objects


def ingest_instances_table(session, metadata, components, dataset_obj):
    """Ingest instances into the values_inst table using ORM."""

    print('=== Ingesting Values Instance Table ===')

    created_instances = {}
    processed_subjects = set()
    processed_samples = set()

    for item in metadata['data']:
        path_parts = pathlib.Path(item['dataset_relative_path']).parts
        parsed_path = parse_path_structure(path_parts)

        # Create subject instance (if not already created)
        subject_id = parsed_path['subject_id']
        if subject_id not in processed_subjects:
            subject_inst = ValuesInst(
                type='subject',
                desc_inst=components['descriptors']['human'].id,
                dataset=dataset_obj.id,
                id_formal=subject_id,
                id_sub=subject_id,
                id_sam=None,
            )
            # Set relationships
            subject_inst.objects = dataset_obj
            subject_inst.descriptors_inst = components['descriptors']['human']

            subject_result = get_or_create(session, subject_inst)
            created_instances[subject_id] = subject_result
            processed_subjects.add(subject_id)
            print(f'Created subject instance: {subject_result.id_formal}')

        # Create sample instance (if not already created)
        sample_id = parsed_path['sample_id']
        sample_key = f'{subject_id}_{sample_id}'
        if sample_key not in processed_samples:
            sample_inst = ValuesInst(
                type='sample',
                desc_inst=components['descriptors']['nerve-volume'].id,
                dataset=dataset_obj.id,
                id_formal=sample_id,
                id_sub=subject_id,
                id_sam=sample_id,
            )
            # Set relationships
            sample_inst.objects = dataset_obj
            sample_inst.descriptors_inst = components['descriptors']['nerve-volume']

            sample_result = get_or_create(session, sample_inst)
            created_instances[sample_key] = sample_result
            processed_samples.add(sample_key)
            print(f'Created sample instance: {sample_result.id_formal}')

    return created_instances


def create_obj_desc_mappings(session, components, package_objects):
    """Create ObjDesc* mappings that tell the system where to find data in packages."""

    print('=== Creating ObjDesc Mappings ===')

    created_mappings = {'obj_desc_inst': [], 'obj_desc_cat': [], 'obj_desc_quant': []}

    # For each package, create mappings
    for package in package_objects:
        # ObjDescInst - Maps package to instance descriptors
        # This tells the system that this package contains data about nerve-volume instances
        obj_desc_inst = ObjDescInst(
            object=package.id,
            desc_inst=components['descriptors']['nerve-volume'].id,
            addr_field=components['const_addr'].id,
        )
        obj_desc_inst.objects = package
        obj_desc_inst.descriptors_inst = components['descriptors']['nerve-volume']
        obj_desc_inst.addresses_field = components['const_addr']

        result = get_or_create(session, obj_desc_inst)
        created_mappings['obj_desc_inst'].append(result)

        # ObjDescCat - Maps package to categorical descriptors
        # This tells the system that this package contains modality data
        obj_desc_cat = ObjDescCat(
            object=package.id, desc_cat=components['modality_desc'].id, addr_field=components['const_addr'].id
        )
        obj_desc_cat.objects = package
        obj_desc_cat.descriptors_cat = components['modality_desc']
        obj_desc_cat.addresses_ = components['const_addr']

        result = get_or_create(session, obj_desc_cat)
        created_mappings['obj_desc_cat'].append(result)

        # ObjDescQuant - Maps package to quantitative descriptors
        # This tells the system that this package contains volume measurements
        obj_desc_quant = ObjDescQuant(
            object=package.id, desc_quant=components['nerve_volume_desc'].id, addr_field=components['tabular_addr'].id
        )
        obj_desc_quant.objects = package
        obj_desc_quant.descriptors_quant = components['nerve_volume_desc']
        obj_desc_quant.addresses_field = components['tabular_addr']

        result = get_or_create(session, obj_desc_quant)
        created_mappings['obj_desc_quant'].append(result)

        print(f'Created mappings for package: {package.id}')

    return created_mappings


def create_leaf_values(session, metadata, components, dataset_obj, package_objects, instances, mappings):
    """Create leaf table values using back_populate_tables."""

    print('=== Creating Leaf Table Values ===')

    created_values = {'values_cat': [], 'values_quant': []}

    # Process each file to create values
    for idx, item in enumerate(metadata['data']):
        package = package_objects[idx]
        path_parts = pathlib.Path(item['dataset_relative_path']).parts
        parsed_path = parse_path_structure(path_parts)

        # Get the sample instance for this file
        sample_key = f"{parsed_path['subject_id']}_{parsed_path['sample_id']}"
        sample_instance = instances[sample_key]

        # Create categorical value for modality
        values_cat = ValuesCat(
            value_controlled=components['terms']['microct'].id,
            object=package.id,
            desc_inst=components['descriptors']['nerve-volume'].id,
            desc_cat=components['modality_desc'].id,
            instance=sample_instance.id,
        )

        # Set all relationships for back_populate_tables
        values_cat.controlled_terms = components['terms']['microct']
        values_cat.descriptors_cat = components['modality_desc']
        values_cat.descriptors_inst = components['descriptors']['nerve-volume']
        values_cat.values_inst = sample_instance
        values_cat.obj_desc_cat = next(m for m in mappings['obj_desc_cat'] if m.object == package.id)
        values_cat.obj_desc_inst = next(m for m in mappings['obj_desc_inst'] if m.object == package.id)
        values_cat.objects = package

        # Use back_populate_tables to handle all relationships
        result_cat = back_populate_tables(session, values_cat)
        created_values['values_cat'].append(result_cat)
        print(f"Created categorical value for {parsed_path['sample_id']}: modality={parsed_path['modality']}")

        # Create quantitative value (example: nerve volume)
        # In a real scenario, you would extract this from the actual data
        example_volume = 42.5 + idx  # Placeholder value

        values_quant = ValuesQuant(
            value=example_volume,
            object=package.id,
            desc_inst=components['descriptors']['nerve-volume'].id,
            desc_quant=components['nerve_volume_desc'].id,
            instance=sample_instance.id,
            value_blob={'value': float(example_volume), 'unit': 'mm3'},
        )

        # Set all relationships for back_populate_tables
        values_quant.descriptors_inst = components['descriptors']['nerve-volume']
        values_quant.descriptors_quant = components['nerve_volume_desc']
        values_quant.values_inst = sample_instance
        values_quant.obj_desc_inst = next(m for m in mappings['obj_desc_inst'] if m.object == package.id)
        values_quant.obj_desc_quant = next(m for m in mappings['obj_desc_quant'] if m.object == package.id)
        values_quant.objects = package

        # Use back_populate_tables to handle all relationships
        result_quant = back_populate_tables(session, values_quant)
        created_values['values_quant'].append(result_quant)
        print(f"Created quantitative value for {parsed_path['sample_id']}: volume={example_volume} mm3")

    return created_values


def run_f006_ingestion(session=None, commit=False):
    """
    Main ingestion function for f006 dataset.

    This demonstrates a complete table-to-table ingestion using the ORM approach.
    """

    if session is None:
        session = get_session(echo=False, test=True)

    try:
        print(f'Starting F006 ingestion for dataset: {DATASET_UUID}')

        # Load metadata
        metadata = load_path_metadata()
        print(f"Loaded metadata for {len(metadata['data'])} files")

        # Create necessary descriptors and components
        components = create_basic_descriptors(session)
        print('Created basic descriptors and components')

        # Ingest objects table
        dataset_obj, package_objects = ingest_objects_table(session, metadata, components)

        # Ingest instances table
        instances = ingest_instances_table(session, metadata, components, dataset_obj)

        # Create ObjDesc* mappings (intermediate tables)
        mappings = create_obj_desc_mappings(session, components, package_objects)

        # Create leaf table values using back_populate_tables
        leaf_values = create_leaf_values(
            session, metadata, components, dataset_obj, package_objects, instances, mappings
        )

        if commit:
            session.commit()
            print('✓ Transaction committed successfully')
        else:
            session.rollback()
            print('✓ Transaction rolled back (dry run)')

        print(f'✓ Ingestion completed successfully!')
        print(f'  - Dataset object: 1')
        print(f'  - Package objects: {len(package_objects)}')
        print(f'  - Instances: {len(instances)}')
        print(f"  - ObjDescInst mappings: {len(mappings['obj_desc_inst'])}")
        print(f"  - ObjDescCat mappings: {len(mappings['obj_desc_cat'])}")
        print(f"  - ObjDescQuant mappings: {len(mappings['obj_desc_quant'])}")
        print(f"  - ValuesCat entries: {len(leaf_values['values_cat'])}")
        print(f"  - ValuesQuant entries: {len(leaf_values['values_quant'])}")

        return {
            'dataset_obj': dataset_obj,
            'package_objects': package_objects,
            'instances': instances,
            'mappings': mappings,
            'leaf_values': leaf_values,
        }

    except Exception as e:
        session.rollback()
        print(f'✗ Ingestion failed: {e}')
        raise


if __name__ == '__main__':
    # Run the ingestion with commit=True for actual data insertion
    session = get_session(echo=True, test=True)
    result = run_f006_ingestion(session, commit=True)
    session.close()
    print('F006 ingestion completed!')
