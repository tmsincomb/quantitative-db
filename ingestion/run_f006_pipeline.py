#!/usr/bin/env python3
"""
Complete F006 Pipeline Runner

This script demonstrates how to run the complete F006 data pipeline:
1. Extract data from Cassava API and save as organized CSVs
2. Ingest the CSV data into the PostgreSQL database

Usage:
    python run_f006_pipeline.py                    # Dry run (recommended first)
    python run_f006_pipeline.py --commit           # Actual database commit
    python run_f006_pipeline.py --source-local     # Use local test data
"""

import sys
import pathlib
import argparse
from datetime import datetime

# Add the current directory to the path so we can import our modules
sys.path.insert(0, str(pathlib.Path(__file__).parent))

try:
    from f006_data_extraction import run_f006_data_extraction
    from f006_database_ingestion import run_f006_database_ingestion
    from quantdb.client import get_session
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please ensure you're running this from the ingestion directory and quantdb is properly installed.")
    sys.exit(1)

# Dataset configuration
DATASET_UUID = '2a3d01c0-39d3-464a-8746-54c9d67ebe0f'


def run_complete_f006_pipeline(dataset_uuid: str = DATASET_UUID, 
                              source_local: bool = False, 
                              commit: bool = False,
                              echo_sql: bool = False) -> dict:
    """
    Run the complete F006 data pipeline from Cassava API to PostgreSQL database.
    
    Parameters
    ----------
    dataset_uuid : str, optional
        The dataset UUID to process
    source_local : bool, optional
        If True, use local test data instead of Cassava API
    commit : bool, optional
        Whether to commit the database transaction
    echo_sql : bool, optional
        Whether to enable SQL echo for debugging
        
    Returns
    -------
    dict
        Summary report of the complete pipeline execution
    """
    pipeline_start_time = datetime.now()
    
    print("=" * 80)
    print("F006 COMPLETE DATA PIPELINE")
    print("=" * 80)
    print(f"Dataset UUID: {dataset_uuid}")
    print(f"Data Source: {'Local file' if source_local else 'Cassava API'}")
    print(f"Database Commit: {'Yes' if commit else 'No (dry run)'}")
    print(f"SQL Echo: {'Yes' if echo_sql else 'No'}")
    print(f"Started at: {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    pipeline_report = {
        'pipeline_start_time': pipeline_start_time.isoformat(),
        'dataset_uuid': dataset_uuid,
        'source_local': source_local,
        'commit': commit,
        'stages': {},
        'success': False,
        'error': None
    }
    
    try:
        # Stage 1: Data Extraction from Cassava API
        print("\n" + "=" * 60)
        print("STAGE 1: DATA EXTRACTION FROM CASSAVA API")
        print("=" * 60)
        
        extraction_start = datetime.now()
        saved_files = run_f006_data_extraction(
            dataset_uuid=dataset_uuid,
            source_local=source_local
        )
        extraction_end = datetime.now()
        
        pipeline_report['stages']['extraction'] = {
            'start_time': extraction_start.isoformat(),
            'end_time': extraction_end.isoformat(),
            'duration_seconds': (extraction_end - extraction_start).total_seconds(),
            'files_created': list(saved_files.keys()),
            'success': True
        }
        
        print(f"\n✓ Stage 1 completed successfully in {(extraction_end - extraction_start).total_seconds():.1f} seconds")
        
        # Stage 2: Database Ingestion from CSVs
        print("\n" + "=" * 60)
        print("STAGE 2: DATABASE INGESTION FROM CSVs")
        print("=" * 60)
        
        ingestion_start = datetime.now()
        
        # Get database session
        session = get_session(echo=echo_sql, test=True)
        
        # Run the database ingestion
        ingestion_summary = run_f006_database_ingestion(
            session=session,
            commit=commit
        )
        
        session.close()
        ingestion_end = datetime.now()
        
        pipeline_report['stages']['ingestion'] = {
            'start_time': ingestion_start.isoformat(),
            'end_time': ingestion_end.isoformat(),
            'duration_seconds': (ingestion_end - ingestion_start).total_seconds(),
            'ingestion_summary': ingestion_summary,
            'success': True
        }
        
        print(f"\n✓ Stage 2 completed successfully in {(ingestion_end - ingestion_start).total_seconds():.1f} seconds")
        
        # Pipeline completion
        pipeline_end_time = datetime.now()
        total_duration = pipeline_end_time - pipeline_start_time
        
        pipeline_report['pipeline_end_time'] = pipeline_end_time.isoformat()
        pipeline_report['total_duration_seconds'] = total_duration.total_seconds()
        pipeline_report['success'] = True
        
        # Final summary
        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"Total Duration: {total_duration.total_seconds():.1f} seconds")
        print(f"Stage 1 (Extraction): {pipeline_report['stages']['extraction']['duration_seconds']:.1f}s")
        print(f"Stage 2 (Ingestion): {pipeline_report['stages']['ingestion']['duration_seconds']:.1f}s")
        
        print(f"\nData Processing Summary:")
        if 'ingestion_summary' in pipeline_report['stages']['ingestion']:
            summary = pipeline_report['stages']['ingestion']['ingestion_summary']
            print(f"  • CSV files processed: {len(summary.get('source_files', {}))}")
            print(f"  • Database objects created: {summary.get('ingested_objects', {}).get('packages', 0) + 1}")
            print(f"  • Instance records created: {summary.get('total_instances', 0)}")
        
        if commit:
            print(f"\n✓ All data has been committed to the database!")
        else:
            print(f"\n⚠ This was a dry run - no data was committed to the database.")
            print(f"  Use --commit flag to actually save the data.")
        
        print("=" * 80)
        
        return pipeline_report
        
    except Exception as e:
        pipeline_end_time = datetime.now()
        total_duration = pipeline_end_time - pipeline_start_time
        
        pipeline_report['pipeline_end_time'] = pipeline_end_time.isoformat()
        pipeline_report['total_duration_seconds'] = total_duration.total_seconds()
        pipeline_report['success'] = False
        pipeline_report['error'] = str(e)
        
        print(f"\n" + "=" * 80)
        print("PIPELINE FAILED!")
        print("=" * 80)
        print(f"Error: {e}")
        print(f"Total Duration: {total_duration.total_seconds():.1f} seconds")
        print("=" * 80)
        
        raise


def main():
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(
        description='Run the complete F006 data pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              # Dry run with Cassava API
  %(prog)s --commit                     # Full pipeline with database commit
  %(prog)s --source-local               # Use local test data (dry run)
  %(prog)s --source-local --commit      # Local data with database commit
  %(prog)s --commit --echo              # Full pipeline with SQL debugging
        """
    )
    
    parser.add_argument('--dataset-uuid', default=DATASET_UUID,
                       help=f'Dataset UUID to process (default: {DATASET_UUID})')
    parser.add_argument('--source-local', action='store_true',
                       help='Use local test data instead of Cassava API')
    parser.add_argument('--commit', action='store_true',
                       help='Commit the database transaction (default: dry run)')
    parser.add_argument('--echo', action='store_true',
                       help='Enable SQL echo for debugging')
    
    args = parser.parse_args()
    
    try:
        pipeline_report = run_complete_f006_pipeline(
            dataset_uuid=args.dataset_uuid,
            source_local=args.source_local,
            commit=args.commit,
            echo_sql=args.echo
        )
        
        return 0
        
    except KeyboardInterrupt:
        print(f"\n\nPipeline interrupted by user")
        return 130
        
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())