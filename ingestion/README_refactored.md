# F006 Dataset Refactored Ingestion Implementation

This directory contains the refactored implementation of F006 dataset ingestion, now split into two separate, specialized files that follow a clear data pipeline approach.

## Overview

**Dataset ID**: `2a3d01c0-39d3-464a-8746-54c9d67ebe0f`

The refactored implementation separates concerns into two distinct processes:

1. **Data Extraction** (`f006_data_extraction.py`) - Pulls data from Cassava APIs, analyzes, cleans, and saves organized CSVs
2. **Database Ingestion** (`f006_database_ingestion.py`) - Reads organized CSVs and ingests them into the local PostgreSQL database

## Files Structure

```
ingestion/
├── f006_data_extraction.py      # Cassava API → CSV pipeline
├── f006_database_ingestion.py   # CSV → PostgreSQL pipeline
├── f006.py                      # Original implementation (deprecated)
├── data/
│   ├── f006_path_metadata.json  # Local test data
│   └── csv_outputs/             # Generated CSV files (created automatically)
│       ├── f006_datasets_latest.csv
│       ├── f006_packages_latest.csv
│       ├── f006_subjects_latest.csv
│       ├── f006_samples_latest.csv
│       ├── f006_files_latest.csv
│       └── f006_data_dictionary.json
└── README_refactored.md         # This file
```

## Workflow

### Step 1: Data Extraction from Cassava APIs

```bash
# Extract data from Cassava API and save as organized CSVs
python f006_data_extraction.py

# Or use local test data for development
python f006_data_extraction.py --source-local

# Specify custom dataset UUID (optional)
python f006_data_extraction.py --dataset-uuid 2a3d01c0-39d3-464a-8746-54c9d67ebe0f
```

**What this does:**
- Fetches path metadata from Cassava API
- Analyzes and parses path structures (`sub-f006/sam-l-seg-c1/microct/image001.jpx`)
- Extracts subject, sample, package, and file information
- Validates data quality and referential integrity
- Saves organized CSV files in `data/csv_outputs/`
- Creates a data dictionary documenting all fields

**Output CSV Files:**
- `f006_datasets_latest.csv` - Dataset-level information
- `f006_packages_latest.csv` - Package/file objects with metadata
- `f006_subjects_latest.csv` - Subject instance data
- `f006_samples_latest.csv` - Sample instance data  
- `f006_files_latest.csv` - Detailed file-level tracking
- `f006_data_dictionary.json` - Comprehensive data documentation

### Step 2: Database Ingestion from CSVs

```bash
# Dry run (recommended first) - validates data but doesn't commit
python f006_database_ingestion.py

# Actual ingestion - commits data to database
python f006_database_ingestion.py --commit

# Enable SQL debugging
python f006_database_ingestion.py --commit --echo

# Use custom CSV directory
python f006_database_ingestion.py --csv-dir /path/to/csv/files --commit
```

**What this does:**
- Loads and validates the organized CSV files
- Creates necessary database descriptors and controlled terms
- Ingests objects (dataset and packages) using ORM models
- Ingests instances (subjects and samples) with proper relationships
- Provides detailed progress reporting and validation

## Key Benefits of Refactored Approach

### 1. **Separation of Concerns**
- **Data Extraction**: Focused on API integration, data cleaning, and CSV generation
- **Database Ingestion**: Focused on database operations and ORM relationships

### 2. **Improved Debugging and Development**
- CSV files provide a clear intermediate format for inspection
- Each stage can be run independently
- Easy to validate data before database ingestion
- Can rerun database ingestion without re-fetching API data

### 3. **Data Preservation and Reproducibility**
- CSV files serve as a snapshot of the extracted data
- Timestamped files maintain historical records
- Data dictionary provides comprehensive documentation
- Can easily share or backup the extracted data

### 4. **Enhanced Error Handling**
- Granular error reporting at each stage
- Graceful fallback from API to local data
- Comprehensive validation at multiple levels
- Detailed logging and progress tracking

### 5. **Scalability and Maintenance**
- Easier to modify extraction logic without affecting database code
- CSV format enables integration with other tools (Excel, R, etc.)
- Cleaner codebase with focused responsibilities
- Better testing capabilities for each component

## Data Flow

```
Cassava API → Raw JSON → Analyzed Data → CSV Files → Database
     ↓             ↓           ↓           ↓          ↓
┌─────────────────────────────────────────────────────────────┐
│              f006_data_extraction.py                       │
├─────────────────────────────────────────────────────────────┤
│ • fetch_cassava_metadata()                                 │
│ • analyze_and_clean_metadata()                             │
│ • validate_data_quality()                                  │
│ • save_dataframes_to_csv()                                 │
│ • create_data_dictionary()                                 │
└─────────────────────────────────────────────────────────────┘
                              ↓
              ┌─────────────────────────────────┐
              │        CSV Files                │
              │ • f006_datasets_latest.csv      │
              │ • f006_packages_latest.csv      │
              │ • f006_subjects_latest.csv      │
              │ • f006_samples_latest.csv       │
              │ • f006_files_latest.csv         │
              │ • f006_data_dictionary.json     │
              └─────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│            f006_database_ingestion.py                      │
├─────────────────────────────────────────────────────────────┤
│ • load_csv_data()                                          │
│ • validate_csv_data()                                      │
│ • create_basic_descriptors()                               │
│ • ingest_objects_from_csv()                                │
│ • ingest_instances_from_csv()                              │
└─────────────────────────────────────────────────────────────┘
                              ↓
              ┌─────────────────────────────────┐
              │      PostgreSQL Database        │
              │ • objects table                 │
              │ • values_inst table             │
              │ • descriptors_* tables          │
              │ • controlled_terms table        │
              └─────────────────────────────────┘
```

## Error Handling and Validation

### Data Extraction Stage
- **API Connectivity**: Automatic fallback to local data if Cassava API fails
- **Data Structure**: Validates expected path structures and metadata fields
- **Data Quality**: Checks for null values, referential integrity, and completeness
- **Output Validation**: Ensures all CSV files are created with expected schemas

### Database Ingestion Stage  
- **CSV Validation**: Verifies all required files exist and have correct structure
- **Database Schema**: Validates against ORM models and constraints
- **Transaction Safety**: Uses database transactions with rollback on errors
- **Dry Run Mode**: Allows validation without committing changes

## Testing and Development

### Run with Local Test Data
```bash
# Extract using local JSON file (for development/testing)
python f006_data_extraction.py --source-local

# Then ingest (dry run)
python f006_database_ingestion.py
```

### Validate Generated Data
```bash
# Check the data dictionary
cat data/csv_outputs/f006_data_dictionary.json

# Inspect CSV files
head data/csv_outputs/f006_*.csv
```

### Database Testing
```bash
# Test database connection and schema
python f006_database_ingestion.py --echo

# Commit only after validation
python f006_database_ingestion.py --commit
```

## Integration with Existing Workflow

This refactored approach maintains compatibility with the existing quantdb ecosystem:

- Uses the same ORM models from `quantdb.models`
- Leverages `quantdb.generic_ingest` helper functions
- Connects to the same test database via `quantdb.client`
- Maintains the same data relationships and constraints

## Comparison with Original Implementation

| Aspect | Original `f006.py` | Refactored Approach |
|--------|-------------------|-------------------|
| **Structure** | Single monolithic file | Two specialized files |
| **Data Source** | Local JSON only | Cassava API + fallback |
| **Intermediate Format** | In-memory only | Persistent CSV files |
| **Debugging** | Limited visibility | CSV inspection + detailed logging |
| **Reusability** | Tightly coupled | Modular and reusable |
| **Documentation** | Minimal | Comprehensive data dictionary |
| **Error Recovery** | Start from scratch | Resume from CSV stage |
| **Data Sharing** | Requires code execution | CSV files easily shared |

## Future Enhancements

The refactored structure enables easy extension for:

1. **Additional Data Sources**: New extractors can generate compatible CSVs
2. **Enhanced Analytics**: CSV files can be processed by external tools
3. **Batch Processing**: Multiple datasets can be extracted and queued for ingestion
4. **Data Validation**: More sophisticated quality checks can be added
5. **Monitoring**: Integration with data pipeline monitoring tools
6. **Caching**: Intelligent caching strategies for API data

## Troubleshooting

### Common Issues

1. **Cassava API Timeout**
   ```bash
   # Use local data as fallback
   python f006_data_extraction.py --source-local
   ```

2. **Missing CSV Files**
   ```bash
   # Check if extraction completed successfully
   ls -la data/csv_outputs/
   # Re-run extraction if needed
   python f006_data_extraction.py
   ```

3. **Database Connection Issues**
   ```bash
   # Test database connection
   python -c "from quantdb.client import get_session; print(get_session(test=True))"
   ```

4. **Validation Errors**
   ```bash
   # Check data dictionary for schema details
   cat data/csv_outputs/f006_data_dictionary.json
   # Use dry run mode to identify issues
   python f006_database_ingestion.py
   ```

## Success Validation

After running both scripts successfully, you should see:

1. **CSV Files Created**: 5 CSV files + data dictionary in `data/csv_outputs/`
2. **Database Records**: Objects and instances inserted into PostgreSQL
3. **No Errors**: Both scripts complete without exceptions
4. **Proper Relationships**: Foreign key relationships maintained

```bash
# Verify CSV files
ls -la data/csv_outputs/f006_*_latest.csv

# Check database records (if committed)
psql quantdb_test -c "SELECT COUNT(*) FROM objects WHERE id = '2a3d01c0-39d3-464a-8746-54c9d67ebe0f';"
```

This refactored approach provides a robust, maintainable, and scalable foundation for F006 dataset ingestion while maintaining full compatibility with the existing quantdb infrastructure.