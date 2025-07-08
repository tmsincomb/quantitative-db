#!/usr/bin/env python3
"""
F006 Dataset Data Extraction from Cassava APIs

This script pulls data from Cassava APIs, analyzes, cleans, and saves organized CSVs
for the F006 dataset (ID: 2a3d01c0-39d3-464a-8746-54c9d67ebe0f).

The extracted data is saved to CSV files that can then be ingested into the database.
"""

import json
import pathlib
import uuid
import pandas as pd
import requests
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Optional

# Dataset configuration
DATASET_UUID = '2a3d01c0-39d3-464a-8746-54c9d67ebe0f'
DATA_DIR = pathlib.Path(__file__).parent / 'data'
CSV_OUTPUT_DIR = DATA_DIR / 'csv_outputs'

# Ensure output directory exists
CSV_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_cassava_metadata(dataset_uuid: str, source_local: bool = False) -> Dict[str, Any]:
    """
    Fetch path metadata from Cassava API or local source.
    
    Parameters
    ----------
    dataset_uuid : str
        The dataset UUID to fetch metadata for
    source_local : bool, optional
        If True, load from local file, otherwise fetch from Cassava API
        
    Returns
    -------
    dict
        The raw metadata blob from Cassava API
    """
    if source_local:
        # Fallback to local file for testing
        metadata_file = DATA_DIR / 'f006_path_metadata.json'
        print(f"Loading metadata from local file: {metadata_file}")
        with open(metadata_file, 'r') as f:
            blob = json.load(f)
    else:
        # Fetch from Cassava API
        api_url = f'https://cassava.ucsd.edu/sparc/datasets/{dataset_uuid}/LATEST/path-metadata.json'
        print(f"Fetching metadata from Cassava API: {api_url}")
        
        try:
            resp = requests.get(api_url)
            resp.raise_for_status()  # Raise an exception for bad status codes
            blob = resp.json()
            print(f"Successfully fetched metadata from Cassava API")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Cassava API: {e}")
            print("Falling back to local file...")
            return fetch_cassava_metadata(dataset_uuid, source_local=True)
        except Exception as e:
            print(f"Error parsing JSON from Cassava API: {e}")
            raise e

    # Add type information to all data entries
    for j in blob.get('data', []):
        j['type'] = 'pathmeta'
    
    return blob


def parse_path_structure(path_parts: List[str]) -> Dict[str, str]:
    """
    Parse the path structure to extract subject, sample, and modality info.
    Example path: sub-f006/sam-l-seg-c1/microct/image001.jpx
    
    Parameters
    ----------
    path_parts : List[str]
        List of path components
        
    Returns
    -------
    dict
        Parsed components with keys: subject_id, sample_id, modality, filename
    """
    if len(path_parts) >= 4:
        subject_id = path_parts[0]  # sub-f006
        sample_id = path_parts[1]  # sam-l-seg-c1
        modality = path_parts[2]  # microct
        filename = path_parts[3]  # image001.jpx

        return {
            'subject_id': subject_id, 
            'sample_id': sample_id, 
            'modality': modality, 
            'filename': filename
        }
    else:
        raise ValueError(f'Unexpected path structure: {path_parts}')


def analyze_and_clean_metadata(blob: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Analyze and clean the metadata, organizing it into structured DataFrames.
    
    Parameters
    ----------
    blob : dict
        Raw metadata blob from Cassava API
        
    Returns
    -------
    dict
        Dictionary containing organized DataFrames for different entity types
    """
    print("Analyzing and cleaning metadata...")
    
    # Extract relevant files (JPX images for F006)
    jpx_files = [
        record for record in blob.get('data', []) 
        if 'mimetype' in record and record['mimetype'] == 'image/jpx'
    ]
    
    print(f"Found {len(jpx_files)} JPX files to process")
    
    # Initialize collections for different entity types
    datasets_data = []
    packages_data = []
    subjects_data = []
    samples_data = []
    files_data = []
    
    processed_subjects = set()
    processed_samples = set()
    
    # Process each JPX file
    for item in jpx_files:
        try:
            # Parse the dataset relative path
            path_parts = pathlib.Path(item['dataset_relative_path']).parts
            parsed_path = parse_path_structure(path_parts)
            
            # Extract basic information
            dataset_id = item['dataset_id']
            file_id = item.get('file_id', int(item['uri_api'].rsplit('/')[-1]))
            package_id = str(uuid.uuid4())  # Generate UUID for package
            timestamp_updated = item.get('timestamp_updated')
            
            # Dataset information (should be consistent across all files)
            dataset_info = {
                'dataset_id': dataset_id,
                'dataset_type': 'dataset',
                'basename': item.get('basename', ''),
                'timestamp_updated': timestamp_updated
            }
            if dataset_info not in datasets_data:
                datasets_data.append(dataset_info)
            
            # Package information (one per file)
            package_info = {
                'package_id': package_id,
                'package_type': 'package',
                'file_id': file_id,
                'dataset_id': dataset_id,
                'uri_api': item.get('uri_api', ''),
                'mimetype': item.get('mimetype', ''),
                'dataset_relative_path': item['dataset_relative_path'],
                'filename': parsed_path['filename'],
                'modality': parsed_path['modality'],
                'timestamp_updated': timestamp_updated
            }
            packages_data.append(package_info)
            
            # Subject information (unique per subject)
            subject_id = parsed_path['subject_id']
            if subject_id not in processed_subjects:
                subject_info = {
                    'subject_id': subject_id,
                    'dataset_id': dataset_id,
                    'instance_type': 'subject',
                    'descriptor_type': 'human',
                    'formal_id': subject_id
                }
                subjects_data.append(subject_info)
                processed_subjects.add(subject_id)
            
            # Sample information (unique per sample)
            sample_id = parsed_path['sample_id']
            sample_key = f"{subject_id}_{sample_id}"
            if sample_key not in processed_samples:
                sample_info = {
                    'sample_id': sample_id,
                    'subject_id': subject_id,
                    'dataset_id': dataset_id,
                    'instance_type': 'sample',
                    'descriptor_type': 'nerve-volume',
                    'formal_id': sample_id
                }
                samples_data.append(sample_info)
                processed_samples.add(sample_key)
            
            # File-level information for detailed tracking
            file_info = {
                'file_id': file_id,
                'package_id': package_id,
                'dataset_id': dataset_id,
                'subject_id': subject_id,
                'sample_id': sample_id,
                'modality': parsed_path['modality'],
                'filename': parsed_path['filename'],
                'dataset_relative_path': item['dataset_relative_path'],
                'uri_api': item.get('uri_api', ''),
                'mimetype': item.get('mimetype', ''),
                'timestamp_updated': timestamp_updated
            }
            files_data.append(file_info)
            
        except Exception as e:
            print(f"Warning: Error processing item {item.get('dataset_relative_path', 'unknown')}: {e}")
            continue
    
    # Convert to DataFrames
    dataframes = {
        'datasets': pd.DataFrame(datasets_data),
        'packages': pd.DataFrame(packages_data),
        'subjects': pd.DataFrame(subjects_data),
        'samples': pd.DataFrame(samples_data),
        'files': pd.DataFrame(files_data)
    }
    
    # Add summary statistics
    print(f"Processed data summary:")
    for name, df in dataframes.items():
        print(f"  - {name}: {len(df)} records")
    
    return dataframes


def validate_data_quality(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Validate the quality and consistency of the extracted data.
    
    Parameters
    ----------
    dataframes : dict
        Dictionary of DataFrames to validate
        
    Returns
    -------
    dict
        Validation report with any issues found
    """
    print("Validating data quality...")
    
    validation_report = {
        'errors': [],
        'warnings': [],
        'stats': {}
    }
    
    # Check for required columns
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
                validation_report['errors'].append(
                    f"{df_name}: Missing required columns: {missing_cols}"
                )
    
    # Check for null values in critical columns
    for df_name, df in dataframes.items():
        null_counts = df.isnull().sum()
        critical_nulls = null_counts[null_counts > 0]
        if len(critical_nulls) > 0:
            validation_report['warnings'].append(
                f"{df_name}: Found null values in columns: {dict(critical_nulls)}"
            )
    
    # Check referential integrity
    if 'packages' in dataframes and 'datasets' in dataframes:
        package_datasets = set(dataframes['packages']['dataset_id'].unique())
        available_datasets = set(dataframes['datasets']['dataset_id'].unique())
        orphaned_packages = package_datasets - available_datasets
        if orphaned_packages:
            validation_report['errors'].append(
                f"Found packages referencing missing datasets: {orphaned_packages}"
            )
    
    # Add statistics
    validation_report['stats'] = {
        name: {
            'row_count': len(df),
            'null_values': df.isnull().sum().sum()
        }
        for name, df in dataframes.items()
    }
    
    # Print validation results
    if validation_report['errors']:
        print(f"Found {len(validation_report['errors'])} errors:")
        for error in validation_report['errors']:
            print(f"  ERROR: {error}")
    
    if validation_report['warnings']:
        print(f"Found {len(validation_report['warnings'])} warnings:")
        for warning in validation_report['warnings']:
            print(f"  WARNING: {warning}")
    
    if not validation_report['errors'] and not validation_report['warnings']:
        print("✓ Data validation passed - no issues found!")
    
    return validation_report


def save_dataframes_to_csv(dataframes: Dict[str, pd.DataFrame], output_dir: pathlib.Path) -> Dict[str, pathlib.Path]:
    """
    Save all DataFrames to organized CSV files.
    
    Parameters
    ----------
    dataframes : dict
        Dictionary of DataFrames to save
    output_dir : pathlib.Path
        Directory to save CSV files to
        
    Returns
    -------
    dict
        Dictionary mapping DataFrame names to their saved file paths
    """
    print(f"Saving DataFrames to CSV files in {output_dir}...")
    
    saved_files = {}
    
    for name, df in dataframes.items():
        # Create descriptive filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"f006_{name}_{timestamp}.csv"
        filepath = output_dir / filename
        
        # Also create a "latest" version without timestamp for easy access
        latest_filename = f"f006_{name}_latest.csv"
        latest_filepath = output_dir / latest_filename
        
        # Save both versions
        df.to_csv(filepath, index=False)
        df.to_csv(latest_filepath, index=False)
        
        saved_files[name] = latest_filepath
        print(f"  ✓ Saved {name}: {len(df)} records to {latest_filename}")
    
    return saved_files


def create_data_dictionary(dataframes: Dict[str, pd.DataFrame], output_dir: pathlib.Path) -> pathlib.Path:
    """
    Create a data dictionary describing all the CSV files and their columns.
    
    Parameters
    ----------
    dataframes : dict
        Dictionary of DataFrames to document
    output_dir : pathlib.Path
        Directory to save the data dictionary to
        
    Returns
    -------
    pathlib.Path
        Path to the saved data dictionary file
    """
    print("Creating data dictionary...")
    
    data_dict = {
        'overview': {
            'dataset_id': DATASET_UUID,
            'extraction_timestamp': datetime.now().isoformat(),
            'total_files': sum(len(df) for df in dataframes.values()),
            'description': 'F006 dataset extracted from Cassava API and organized into CSV files'
        },
        'files': {}
    }
    
    for name, df in dataframes.items():
        file_info = {
            'filename': f'f006_{name}_latest.csv',
            'description': f'{name.capitalize()} data from F006 dataset',
            'row_count': len(df),
            'columns': {}
        }
        
        for col in df.columns:
            col_info = {
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'unique_count': int(df[col].nunique()) if df[col].dtype in ['object', 'int64', 'float64'] else None
            }
            
            # Add sample values for better understanding
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                if df[col].dtype == 'object':
                    sample_values = non_null_values.head(3).tolist()
                else:
                    sample_values = non_null_values.head(3).tolist()
                col_info['sample_values'] = sample_values
            
            file_info['columns'][col] = col_info
        
        data_dict['files'][name] = file_info
    
    # Save data dictionary
    dict_path = output_dir / 'f006_data_dictionary.json'
    with open(dict_path, 'w') as f:
        json.dump(data_dict, f, indent=2, default=str)
    
    print(f"✓ Data dictionary saved to {dict_path.name}")
    return dict_path


def run_f006_data_extraction(dataset_uuid: str = DATASET_UUID, source_local: bool = False) -> Dict[str, pathlib.Path]:
    """
    Main function to run the complete F006 data extraction process.
    
    Parameters
    ----------
    dataset_uuid : str, optional
        The dataset UUID to process
    source_local : bool, optional
        If True, use local test data instead of Cassava API
        
    Returns
    -------
    dict
        Dictionary mapping DataFrame names to their saved CSV file paths
    """
    print(f"="*60)
    print(f"F006 Data Extraction Pipeline")
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Source: {'Local file' if source_local else 'Cassava API'}")
    print(f"Output directory: {CSV_OUTPUT_DIR}")
    print(f"="*60)
    
    try:
        # Step 1: Fetch data from Cassava API
        blob = fetch_cassava_metadata(dataset_uuid, source_local=source_local)
        
        # Step 2: Analyze and clean the data
        dataframes = analyze_and_clean_metadata(blob)
        
        # Step 3: Validate data quality
        validation_report = validate_data_quality(dataframes)
        
        # Step 4: Save to CSV files
        saved_files = save_dataframes_to_csv(dataframes, CSV_OUTPUT_DIR)
        
        # Step 5: Create data dictionary
        dict_path = create_data_dictionary(dataframes, CSV_OUTPUT_DIR)
        
        print(f"\n✓ F006 data extraction completed successfully!")
        print(f"✓ Saved {len(saved_files)} CSV files to {CSV_OUTPUT_DIR}")
        print(f"✓ Files ready for database ingestion")
        
        return saved_files
        
    except Exception as e:
        print(f"\n✗ F006 data extraction failed: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract F006 data from Cassava API')
    parser.add_argument('--dataset-uuid', default=DATASET_UUID, 
                       help='Dataset UUID to process')
    parser.add_argument('--source-local', action='store_true',
                       help='Use local test data instead of Cassava API')
    
    args = parser.parse_args()
    
    # Run the extraction
    saved_files = run_f006_data_extraction(
        dataset_uuid=args.dataset_uuid,
        source_local=args.source_local
    )
    
    print(f"\nSaved files:")
    for name, filepath in saved_files.items():
        print(f"  {name}: {filepath}")