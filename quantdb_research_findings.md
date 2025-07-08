# QuantDB Research Findings

## Project Overview

**QuantDB** is a sophisticated database schema and API system for storing and querying arbitrary quantitative measurements, particularly focused on neuroscience and biomedical research data. The project provides a comprehensive solution for managing complex scientific datasets with both categorical and quantitative measurements.

### Core Purpose
- Store arbitrary quantitative measurements with rich metadata
- Provide a REST API for querying complex scientific data
- Support hierarchical data structures (subjects → samples → measurements)
- Handle both controlled vocabularies and open-text categorical data
- Manage data provenance and relationships between objects

## Technical Architecture

### Technology Stack
- **Backend Framework**: Flask + SQLAlchemy 2.0 with FastAPI components
- **Database**: PostgreSQL with custom schemas and functions
- **Authentication**: Orthauth framework for security
- **Data Processing**: Pandas for data manipulation
- **API**: RESTful endpoints with comprehensive query capabilities
- **Testing**: Pytest with database integration tests

### Key Dependencies
```python
# Core dependencies from pyproject.toml
- sqlalchemy ~= 2.0.29
- fastapi ~= 0.110.1  
- uvicorn ~= 0.29.0
- flask ~= 3.0.3
- orthauth ~= 0.0.18
- psycopg2-binary (PostgreSQL driver)
- pandas==2.2.3
```

## Database Schema Architecture

### Core Entity Model

The database follows a sophisticated entity-attribute-value (EAV) pattern with strong typing:

#### Primary Entities
1. **Objects** - Core data objects with UUID identifiers
2. **Values_Inst** - Instance hierarchy (subjects, samples, below)
3. **Values_Cat** - Categorical measurements
4. **Values_Quant** - Quantitative measurements  
5. **Descriptors** - Type definitions for instances, categories, and quantities

#### Key Design Patterns
- **Hierarchical Instances**: Subject → Sample → Measurements
- **Strong Typing**: Separate descriptor tables for different data types
- **Provenance Tracking**: Address system for data source traceability
- **Controlled Vocabularies**: Integration with ontologies (UBERON, etc.)

### Schema Structure
```sql
-- Core tables (from models.py analysis)
objects                 -- UUID-based object identification
values_inst            -- Subject/sample hierarchy
values_cat             -- Categorical measurements  
values_quant           -- Quantitative measurements
descriptors_inst       -- Instance type definitions
descriptors_cat        -- Categorical property definitions
descriptors_quant      -- Quantitative property definitions
addresses              -- Data provenance addresses
controlled_terms       -- Controlled vocabularies
units                  -- Measurement units
aspects                -- Measurement aspects/dimensions
```

## API Architecture

### REST Endpoints

The API provides comprehensive querying capabilities through a structured endpoint hierarchy:

#### Core Endpoints
- `/api/1/objects` - Query data objects
- `/api/1/values/inst` - Instance queries (subjects, samples)
- `/api/1/values/cat` - Categorical value queries
- `/api/1/values/quant` - Quantitative value queries
- `/api/1/values` - Combined categorical and quantitative queries

#### Descriptor Endpoints
- `/api/1/desc/inst` - Instance type definitions
- `/api/1/desc/cat` - Categorical descriptors
- `/api/1/desc/quant` - Quantitative descriptors

#### Vocabulary Endpoints
- `/api/1/terms` - Controlled terms
- `/api/1/units` - Measurement units
- `/api/1/aspects` - Measurement aspects

### Query Parameters

The API supports sophisticated querying with multiple parameter types:

#### Filtering Parameters
- `object` - Filter by object UUID(s)
- `dataset` - Filter by dataset UUID
- `subject` - Filter by subject ID(s)
- `sample` - Filter by sample ID(s)
- `inst` - Filter by instance ID(s)

#### Value-based Filtering
- `value-quant`, `value-quant-min`, `value-quant-max` - Numeric range queries
- `value-cat`, `value-cat-open` - Categorical value filtering
- `unit`, `aspect`, `agg-type` - Measurement metadata filtering

#### Advanced Options
- `prov` - Include provenance information
- `union-cat-quant` - Union vs. intersect behavior
- `include-equivalent` - Include equivalent instances

## Data Ingestion System

### Ingestion Architecture

The system provides a sophisticated data ingestion pipeline:

#### Core Components
1. **Generic Ingest** (`generic_ingest.py`) - Framework for custom data processors
2. **Specific Extractors** - Domain-specific data extraction functions
3. **Address System** - Tracks data provenance and source locations
4. **Batch Processing** - Support for large-scale data ingestion

#### Extraction Functions
- `extract_reva_ft()` - REVA (vagus nerve) functional testing data
- `extract_demo()` - Demo dataset processing
- `extract_demo_jp2()` - JP2 image dataset processing

#### Key Features
- **Hierarchical Data Processing**: Automatic parent-child relationship creation
- **Metadata Extraction**: Path-based metadata extraction from filenames
- **Anatomical Indexing**: Spatial ordering for anatomical samples
- **Provenance Tracking**: Complete audit trail of data sources

### Example Data Flow
```python
# Typical ingestion workflow
def ingest(dataset_uuid, extract_fun, session):
    # 1. Extract structured data from source
    # 2. Create object hierarchy
    # 3. Generate instance relationships  
    # 4. Insert categorical/quantitative values
    # 5. Maintain provenance addresses
```

## Key Features

### 1. Hierarchical Data Organization
- **Subjects**: Top-level experimental entities
- **Samples**: Physical samples derived from subjects
- **Measurements**: Values associated with samples/subjects

### 2. Flexible Measurement Types
- **Quantitative**: Numeric values with units and aspects
- **Categorical**: Controlled or open-text classifications
- **Aggregation Types**: Instance, function, summary, statistical measures

### 3. Ontology Integration
- Support for IRI-based controlled vocabularies
- Integration with UBERON anatomical ontology
- Extensible term management system

### 4. Advanced Querying
- **Complex Filtering**: Multi-parameter queries with AND/OR logic
- **Hierarchical Resolution**: Automatic parent-child relationship queries
- **Provenance Queries**: Track data sources and transformations
- **Union/Intersect Operations**: Flexible result combination

### 5. Data Provenance
- **Address System**: Track exact source locations in files
- **Multiple Address Types**: Tabular headers, JSON paths, file system extraction
- **Transformation Tracking**: Maintain audit trails

## Testing Infrastructure

### Test Coverage
- **Database Setup Tests**: Automated database creation and verification
- **API Integration Tests**: Endpoint functionality validation
- **Data Structure Tests**: Schema integrity verification
- **Ingestion Tests**: Data processing pipeline validation

### Test Configuration
- **Isolated Test Database**: `quantdb_test` with hardcoded local configuration
- **Automated Setup**: `bin/dbsetup` script integration
- **Schema Verification**: Table, function, and enum validation

## Configuration Management

### Database Configuration
- **Multi-environment Support**: Development, testing, production
- **Credential Management**: Integration with orthauth
- **Connection Management**: SQLAlchemy engine configuration

### Authentication
- **User Roles**: `quantdb-admin`, `quantdb-user`, test users
- **Password Management**: `.pgpass` file integration
- **Security**: Role-based access control

## Development Tools

### Setup Scripts
- `bin/dbsetup` - Database initialization and schema creation
- `bin/prepare_test_db.sh` - Test database preparation
- `bin/vizschema` - Schema visualization tools

### Code Quality
- **Pre-commit Hooks**: Blue (code formatting), flake8 (linting)
- **Type Hints**: Comprehensive type annotations
- **Documentation**: Extensive inline documentation

## Use Cases

### Primary Applications
1. **Neuroscience Research**: Vagus nerve morphometry and functional data
2. **Biomedical Datasets**: Multi-modal experimental data management
3. **Scientific Data Integration**: Combining measurements from multiple sources
4. **Research Data Sharing**: Standardized API for data access

### Example Queries
```bash
# Find all measurements on a specific subject
/api/1/values?subject=sub-001

# Query quantitative values within a range
/api/1/values/quant?aspect=diameter&value-quant-min=10&value-quant-max=50

# Get categorical data for specific samples
/api/1/values/cat?sample=sam-001&desc-cat=species
```

## Project Structure

```
quantitative-db-fork/
├── quantdb/           # Core application code
│   ├── api.py         # REST API implementation
│   ├── models.py      # SQLAlchemy ORM models
│   ├── ingest.py      # Data ingestion pipeline
│   └── config.py      # Configuration management
├── sql/               # Database schema definitions
├── test/              # Test suite
├── ingestion/         # Data processing modules
├── docs/              # API documentation
└── bin/               # Setup and utility scripts
```

## Future Considerations

### Potential Enhancements
1. **Performance Optimization**: Query optimization for large datasets
2. **API Versioning**: Support for multiple API versions
3. **Real-time Data**: Streaming data ingestion capabilities
4. **Visualization**: Built-in data visualization components
5. **Export Capabilities**: Standardized data export formats

### Scalability Considerations
- **Database Partitioning**: For very large datasets
- **Caching Layer**: Redis integration for frequent queries
- **Microservices**: Decomposition for larger deployments
- **Cloud Integration**: AWS/Azure deployment optimization

## Conclusion

QuantDB represents a sophisticated solution for managing complex scientific datasets with rich metadata, hierarchical organization, and flexible querying capabilities. The system demonstrates best practices in database design, API development, and scientific data management, making it particularly well-suited for neuroscience and biomedical research applications.

The project's strength lies in its comprehensive approach to data modeling, strong typing system, and extensive provenance tracking, while maintaining flexibility for diverse measurement types and research domains.