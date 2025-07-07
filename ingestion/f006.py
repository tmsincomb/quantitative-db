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
    Objects,
    Units,
    ValuesInst,
    ValuesQuant,
    ValuesCat,
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
        sample_id = path_parts[1]   # sam-l-seg-c1
        modality = path_parts[2]    # microct
        filename = path_parts[3]    # image001.jpx
        
        return {
            'subject_id': subject_id,
            'sample_id': sample_id,
            'modality': modality,
            'filename': filename
        }
    else:
        raise ValueError(f"Unexpected path structure: {path_parts}")


def create_basic_descriptors(session):
    """Create basic descriptors needed for f006 dataset."""
    
    # Create basic instance descriptors
    descriptors = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/human', 'label': 'human'},
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve-volume', 'label': 'nerve-volume'},
    ]
    
    created_descriptors = {}
    for desc_data in descriptors:
        desc_inst = DescriptorsInst(
            iri=desc_data['iri'],
            label=desc_data['label']
        )
        created_desc = get_or_create(session, desc_inst)
        created_descriptors[desc_data['label']] = created_desc
    
    # Create controlled terms
    controlled_terms = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/microct', 'label': 'microct'},
    ]
    
    created_terms = {}
    for term_data in controlled_terms:
        term = ControlledTerms(
            iri=term_data['iri'],
            label=term_data['label']
        )
        created_term = get_or_create(session, term)
        created_terms[term_data['label']] = created_term
    
    # Create categorical descriptor for modality
    modality_desc = DescriptorsCat(
        domain=created_descriptors['nerve-volume'].id,
        range='controlled',
        label='hasDataAboutItModality'
    )
    modality_desc.descriptors_inst = created_descriptors['nerve-volume']
    created_modality_desc = get_or_create(session, modality_desc)
    
    # Create address for constant values
    const_addr = Addresses(
        addr_type='constant',
        addr_field=None,
        value_type='single'
    )
    created_addr = get_or_create(session, const_addr)
    
    return {
        'descriptors': created_descriptors,
        'terms': created_terms,
        'modality_desc': created_modality_desc,
        'const_addr': created_addr
    }


def ingest_objects_table(session, metadata, components):
    """Ingest objects into the objects table using ORM."""
    
    print("=== Ingesting Objects Table ===")
    
    # Create dataset object
    dataset_obj = Objects(
        id=DATASET_UUID,
        id_type='dataset',
        id_file=None,
        id_internal=None
    )
    dataset_result = get_or_create(session, dataset_obj)
    print(f"Created/found dataset object: {dataset_result.id}")
    
    # Create package objects for each file
    created_objects = []
    for item in metadata['data']:
        package_id = str(uuid.uuid4())  # Generate UUID for package
        
        package_obj = Objects(
            id=package_id,
            id_type='package',
            id_file=item['file_id'],
            id_internal=None
        )
        # Set relationship to dataset
        package_obj.objects_ = dataset_result
        
        package_result = get_or_create(session, package_obj)
        created_objects.append(package_result)
        print(f"Created package object: {package_result.id} for file_id: {package_result.id_file}")
    
    return dataset_result, created_objects


def ingest_instances_table(session, metadata, components, dataset_obj):
    """Ingest instances into the values_inst table using ORM."""
    
    print("=== Ingesting Values Instance Table ===")
    
    created_instances = []
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
                id_sam=None
            )
            # Set relationships
            subject_inst.objects = dataset_obj
            subject_inst.descriptors_inst = components['descriptors']['human']
            
            subject_result = get_or_create(session, subject_inst)
            created_instances.append(subject_result)
            processed_subjects.add(subject_id)
            print(f"Created subject instance: {subject_result.id_formal}")
        
        # Create sample instance (if not already created)
        sample_id = parsed_path['sample_id']
        sample_key = f"{subject_id}_{sample_id}"
        if sample_key not in processed_samples:
            sample_inst = ValuesInst(
                type='sample',
                desc_inst=components['descriptors']['nerve-volume'].id,
                dataset=dataset_obj.id,
                id_formal=sample_id,
                id_sub=subject_id,
                id_sam=sample_id
            )
            # Set relationships
            sample_inst.objects = dataset_obj
            sample_inst.descriptors_inst = components['descriptors']['nerve-volume']
            
            sample_result = get_or_create(session, sample_inst)
            created_instances.append(sample_result)
            processed_samples.add(sample_key)
            print(f"Created sample instance: {sample_result.id_formal}")
    
    return created_instances


def run_f006_ingestion(session=None, commit=False):
    """
    Main ingestion function for f006 dataset.
    
    This demonstrates a complete table-to-table ingestion using the ORM approach.
    """
    
    if session is None:
        session = get_session(echo=False, test=True)
    
    try:
        print(f"Starting F006 ingestion for dataset: {DATASET_UUID}")
        
        # Load metadata
        metadata = load_path_metadata()
        print(f"Loaded metadata for {len(metadata['data'])} files")
        
        # Create necessary descriptors and components
        components = create_basic_descriptors(session)
        print("Created basic descriptors and components")
        
        # Ingest objects table
        dataset_obj, package_objects = ingest_objects_table(session, metadata, components)
        
        # Ingest instances table
        instances = ingest_instances_table(session, metadata, components, dataset_obj)
        
        if commit:
            session.commit()
            print("✓ Transaction committed successfully")
        else:
            session.rollback()
            print("✓ Transaction rolled back (dry run)")
            
        print(f"✓ Ingestion completed successfully!")
        print(f"  - Dataset object: 1")
        print(f"  - Package objects: {len(package_objects)}")
        print(f"  - Instances: {len(instances)}")
        
        return {
            'dataset_obj': dataset_obj,
            'package_objects': package_objects,
            'instances': instances
        }
        
    except Exception as e:
        session.rollback()
        print(f"✗ Ingestion failed: {e}")
        raise


if __name__ == '__main__':
    # Run the ingestion with commit=True for actual data insertion
    session = get_session(echo=True, test=True)
    result = run_f006_ingestion(session, commit=True)
    session.close()
    print("F006 ingestion completed!")