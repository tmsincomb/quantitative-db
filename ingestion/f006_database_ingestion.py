#!/usr/bin/env python3
"""
F006 Dataset Database Ingestion from Organized CSVs

This script reads the organized CSV files created by f006_data_extraction.py
and ingests them into the local PostgreSQL database using the ORM approach.

Dataset ID: 2a3d01c0-39d3-464a-8746-54c9d67ebe0f
"""

import pathlib
import pandas as pd
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional

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
    ValuesCat,
    ValuesInst,
    ValuesQuant,
)

# Dataset configuration
DATASET_UUID = '2a3d01c0-39d3-464a-8746-54c9d67ebe0f'
DATA_DIR = pathlib.Path(__file__).parent / 'data'
CSV_INPUT_DIR = DATA_DIR / 'csv_outputs'


def load_csv_data(csv_dir: pathlib.Path) -> Dict[str, pd.DataFrame]:
    """
    Load all the organized CSV files created by the data extraction process.
    
    Parameters
    ----------
    csv_dir : pathlib.Path
        Directory containing the CSV files
        
    Returns
    -------
    dict
        Dictionary of DataFrames loaded from CSV files
    """
    print(f"Loading CSV files from {csv_dir}...")
    
    expected_files = {
        'datasets': 'f006_datasets_latest.csv',
        'packages': 'f006_packages_latest.csv', 
        'subjects': 'f006_subjects_latest.csv',
        'samples': 'f006_samples_latest.csv',
        'files': 'f006_files_latest.csv'
    }
    
    dataframes = {}
    
    for name, filename in expected_files.items():
        filepath = csv_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"Required CSV file not found: {filepath}")
        
        try:
            df = pd.read_csv(filepath)
            dataframes[name] = df
            print(f"  ✓ Loaded {name}: {len(df)} records from {filename}")
        except Exception as e:
            raise Exception(f"Error loading {filename}: {e}")
    
    return dataframes


def validate_csv_data(dataframes: Dict[str, pd.DataFrame]) -> None:
    """
    Validate that the loaded CSV data has the expected structure.
    
    Parameters
    ----------
    dataframes : dict
        Dictionary of DataFrames to validate
    """
    print("Validating CSV data structure...")
    
    # Check that we have all required DataFrames
    required_dfs = ['datasets', 'packages', 'subjects', 'samples', 'files']
    missing_dfs = set(required_dfs) - set(dataframes.keys())
    if missing_dfs:
        raise ValueError(f"Missing required DataFrames: {missing_dfs}")
    
    # Check for minimum required columns
    required_columns = {
        'datasets': ['dataset_id', 'dataset_type'],
        'packages': ['package_id', 'file_id', 'dataset_id'],
        'subjects': ['subject_id', 'dataset_id', 'formal_id'],
        'samples': ['sample_id', 'subject_id', 'dataset_id', 'formal_id'],
        'files': ['file_id', 'package_id', 'dataset_id', 'subject_id', 'sample_id']
    }
    
    for df_name, required_cols in required_columns.items():
        if df_name in dataframes:
            df = dataframes[df_name]
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"{df_name}: Missing required columns: {missing_cols}")
    
    print("✓ CSV data validation passed")


def create_basic_descriptors(session) -> Dict[str, Any]:
    """
    Create basic descriptors needed for f006 dataset.
    
    Parameters
    ----------
    session : sqlalchemy.orm.Session
        Database session
        
    Returns
    -------
    dict
        Dictionary containing created descriptor components
    """
    print("Creating basic descriptors...")

    # Create basic instance descriptors
    descriptors = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/human', 'label': 'human'},
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/classes/nerve-volume', 'label': 'nerve-volume'},
    ]

    created_descriptors = {}
    for desc_data in descriptors:
        desc_inst = DescriptorsInst(iri=desc_data['iri'], label=desc_data['label'])
        created_desc = get_or_create(session, desc_inst)
        created_descriptors[desc_data['label']] = created_desc

    # Create controlled terms
    controlled_terms = [
        {'iri': 'https://uri.interlex.org/tgbugs/uris/readable/quantdb/controlled/microct', 'label': 'microct'},
    ]

    created_terms = {}
    for term_data in controlled_terms:
        term = ControlledTerms(iri=term_data['iri'], label=term_data['label'])
        created_term = get_or_create(session, term)
        created_terms[term_data['label']] = created_term

    # Create categorical descriptor for modality
    modality_desc = DescriptorsCat(
        domain=created_descriptors['nerve-volume'].id, range='controlled', label='hasDataAboutItModality'
    )
    modality_desc.descriptors_inst = created_descriptors['nerve-volume']
    created_modality_desc = get_or_create(session, modality_desc)

    # Create address for constant values - just use any existing constant address
    const_addr = session.query(Addresses).filter_by(addr_type='constant', value_type='single').first()
    if not const_addr:
        # If no constant address exists, create one
        const_addr = Addresses(addr_type='constant', addr_field=None, value_type='single')
        session.add(const_addr)
        session.commit()

    print(f"✓ Created descriptors: {list(created_descriptors.keys())}")
    print(f"✓ Created controlled terms: {list(created_terms.keys())}")

    return {
        'descriptors': created_descriptors,
        'terms': created_terms,
        'modality_desc': created_modality_desc,
        'const_addr': const_addr,
    }


def ingest_objects_from_csv(session, dataframes: Dict[str, pd.DataFrame], components: Dict[str, Any]) -> Dict[str, Objects]:
    """
    Ingest objects into the objects table from CSV data.
    
    Parameters
    ----------
    session : sqlalchemy.orm.Session
        Database session
    dataframes : dict
        Dictionary containing the loaded CSV DataFrames
    components : dict
        Dictionary containing created descriptor components
        
    Returns
    -------
    dict
        Dictionary containing created objects (dataset and packages)
    """
    print('=== Ingesting Objects from CSV ===')

    created_objects = {}
    
    # Create dataset object from datasets CSV
    datasets_df = dataframes['datasets']
    if len(datasets_df) > 0:
        dataset_row = datasets_df.iloc[0]  # Should only be one dataset
        dataset_obj = Objects(
            id=dataset_row['dataset_id'], 
            id_type='dataset', 
            id_file=None, 
            id_internal=None
        )
        dataset_result = get_or_create(session, dataset_obj)
        created_objects['dataset'] = dataset_result
        print(f'✓ Created/found dataset object: {dataset_result.id}')

    # Create package objects from packages CSV
    packages_df = dataframes['packages']
    package_objects = []
    
    for _, package_row in packages_df.iterrows():
        package_obj = Objects(
            id=package_row['package_id'], 
            id_type='package', 
            id_file=package_row['file_id'], 
            id_internal=None
        )
        # Set relationship to dataset
        package_obj.objects_ = created_objects['dataset']

        package_result = get_or_create(session, package_obj)
        package_objects.append(package_result)
        print(f'✓ Created package object: {package_result.id} for file_id: {package_result.id_file}')

    created_objects['packages'] = package_objects
    print(f'✓ Created {len(package_objects)} package objects')

    return created_objects


def ingest_instances_from_csv(session, dataframes: Dict[str, pd.DataFrame], components: Dict[str, Any], 
                             objects: Dict[str, Objects]) -> List[ValuesInst]:
    """
    Ingest instances into the values_inst table from CSV data.
    
    Parameters
    ----------
    session : sqlalchemy.orm.Session
        Database session
    dataframes : dict
        Dictionary containing the loaded CSV DataFrames
    components : dict
        Dictionary containing created descriptor components
    objects : dict
        Dictionary containing created objects
        
    Returns
    -------
    list
        List of created instance objects
    """
    print('=== Ingesting Instances from CSV ===')

    created_instances = []
    dataset_obj = objects['dataset']

    # Create subject instances from subjects CSV
    subjects_df = dataframes['subjects']
    for _, subject_row in subjects_df.iterrows():
        subject_inst = ValuesInst(
            type='subject',
            desc_inst=components['descriptors']['human'].id,
            dataset=dataset_obj.id,
            id_formal=subject_row['formal_id'],
            id_sub=subject_row['subject_id'],
            id_sam=None,
        )
        # Set relationships
        subject_inst.objects = dataset_obj
        subject_inst.descriptors_inst = components['descriptors']['human']

        subject_result = get_or_create(session, subject_inst)
        created_instances.append(subject_result)
        print(f'✓ Created subject instance: {subject_result.id_formal}')

    # Create sample instances from samples CSV
    samples_df = dataframes['samples']
    for _, sample_row in samples_df.iterrows():
        sample_inst = ValuesInst(
            type='sample',
            desc_inst=components['descriptors']['nerve-volume'].id,
            dataset=dataset_obj.id,
            id_formal=sample_row['formal_id'],
            id_sub=sample_row['subject_id'],
            id_sam=sample_row['sample_id'],
        )
        # Set relationships
        sample_inst.objects = dataset_obj
        sample_inst.descriptors_inst = components['descriptors']['nerve-volume']

        sample_result = get_or_create(session, sample_inst)
        created_instances.append(sample_result)
        print(f'✓ Created sample instance: {sample_result.id_formal}')

    print(f'✓ Created {len(created_instances)} instances total')
    return created_instances


def create_summary_report(objects: Dict[str, Objects], instances: List[ValuesInst], 
                         dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Create a summary report of the ingestion process.
    
    Parameters
    ----------
    objects : dict
        Dictionary containing created objects
    instances : list
        List of created instances
    dataframes : dict
        Dictionary containing the source CSV DataFrames
        
    Returns
    -------
    dict
        Summary report
    """
    # Count instances by type
    instance_counts = {}
    for instance in instances:
        inst_type = instance.type
        instance_counts[inst_type] = instance_counts.get(inst_type, 0) + 1

    # Create summary
    summary = {
        'ingestion_timestamp': datetime.now().isoformat(),
        'dataset_uuid': DATASET_UUID,
        'source_files': {
            name: len(df) for name, df in dataframes.items()
        },
        'ingested_objects': {
            'dataset': 1 if 'dataset' in objects else 0,
            'packages': len(objects.get('packages', []))
        },
        'ingested_instances': instance_counts,
        'total_instances': len(instances),
        'success': True
    }
    
    return summary


def run_f006_database_ingestion(csv_dir: pathlib.Path = CSV_INPUT_DIR, session=None, commit: bool = False) -> Dict[str, Any]:
    """
    Main function to run the complete F006 database ingestion from CSV files.
    
    Parameters
    ----------
    csv_dir : pathlib.Path, optional
        Directory containing the CSV files to ingest
    session : sqlalchemy.orm.Session, optional
        Database session to use (creates new one if None)
    commit : bool, optional
        Whether to commit the transaction (default: False for dry run)
        
    Returns
    -------
    dict
        Summary report of the ingestion process
    """
    print(f"="*60)
    print(f"F006 Database Ingestion Pipeline")
    print(f"Dataset UUID: {DATASET_UUID}")
    print(f"CSV Directory: {csv_dir}")
    print(f"Commit: {'Yes' if commit else 'No (dry run)'}")
    print(f"="*60)

    if session is None:
        session = get_session(echo=False, test=True)

    try:
        # Step 1: Load CSV data
        dataframes = load_csv_data(csv_dir)
        
        # Step 2: Validate CSV data structure
        validate_csv_data(dataframes)
        
        # Step 3: Create necessary descriptors and components
        components = create_basic_descriptors(session)
        
        # Step 4: Ingest objects from CSV
        objects = ingest_objects_from_csv(session, dataframes, components)
        
        # Step 5: Ingest instances from CSV
        instances = ingest_instances_from_csv(session, dataframes, components, objects)
        
        # Step 6: Create summary report
        summary = create_summary_report(objects, instances, dataframes)
        
        if commit:
            session.commit()
            print('\n✓ Transaction committed successfully')
        else:
            session.rollback()
            print('\n✓ Transaction rolled back (dry run)')

        print(f'\n✓ Database ingestion completed successfully!')
        print(f'✓ Processed {summary["total_instances"]} instances from {len(dataframes)} CSV files')
        
        # Print detailed summary
        print(f'\nIngestion Summary:')
        print(f'  Dataset object: {summary["ingested_objects"]["dataset"]}')
        print(f'  Package objects: {summary["ingested_objects"]["packages"]}')
        for inst_type, count in summary["ingested_instances"].items():
            print(f'  {inst_type.capitalize()} instances: {count}')

        return summary

    except Exception as e:
        session.rollback()
        print(f'\n✗ Database ingestion failed: {e}')
        raise


def main():
    """Main function for command-line execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest F006 data from CSV files into database')
    parser.add_argument('--csv-dir', type=pathlib.Path, default=CSV_INPUT_DIR,
                       help='Directory containing CSV files to ingest')
    parser.add_argument('--commit', action='store_true',
                       help='Commit the transaction (default: dry run)')
    parser.add_argument('--echo', action='store_true',
                       help='Enable SQL echo for debugging')
    
    args = parser.parse_args()
    
    # Validate that CSV directory exists
    if not args.csv_dir.exists():
        print(f"Error: CSV directory does not exist: {args.csv_dir}")
        print("Please run f006_data_extraction.py first to create the CSV files.")
        return 1
    
    # Run the ingestion
    try:
        session = get_session(echo=args.echo, test=True)
        summary = run_f006_database_ingestion(
            csv_dir=args.csv_dir,
            session=session,
            commit=args.commit
        )
        session.close()
        
        print(f"\n{'='*60}")
        print(f"Ingestion completed successfully!")
        if not args.commit:
            print(f"Note: This was a dry run. Use --commit to actually save data.")
        print(f"{'='*60}")
        
        return 0
        
    except Exception as e:
        print(f"Ingestion failed: {e}")
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())