#!/usr/bin/env python3
"""
Export all tables from the test database to CSV files for debugging.
This shows what data was populated by f006.py.
"""

import os

import pandas as pd
from sqlalchemy import inspect, text

from quantdb.client import get_session
from quantdb.models import Base


def export_tables_to_csv(output_dir='debug_csvs'):
    """Export all tables from the test database to CSV files."""

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get a session to the test database (same as f006.py uses)
    session = get_session(echo=False, test=True)

    try:
        # Get the inspector to examine the database
        inspector = inspect(session.bind)

        # Get all table names
        table_names = inspector.get_table_names()
        print(f'Found {len(table_names)} tables in the database')
        print('=' * 60)

        # Track what we export
        exported_tables = []
        empty_tables = []

        # Export each table
        for table_name in sorted(table_names):
            try:
                # Query the table
                df = pd.read_sql_table(table_name, session.bind)

                if len(df) > 0:
                    # Save to CSV
                    output_file = os.path.join(output_dir, f'{table_name}.csv')
                    df.to_csv(output_file, index=False)
                    exported_tables.append((table_name, len(df)))
                    print(f'✓ Exported {table_name}: {len(df)} rows → {output_file}')
                else:
                    empty_tables.append(table_name)
                    print(f'  Skipped {table_name}: empty table')

            except Exception as e:
                print(f'✗ Error exporting {table_name}: {e}')

        print('=' * 60)

        # Create summary file
        summary_file = os.path.join(output_dir, '_table_summary.csv')
        summary_data = []

        for table_name in sorted(table_names):
            if any(t[0] == table_name for t in exported_tables):
                row_count = next(t[1] for t in exported_tables if t[0] == table_name)
            else:
                row_count = 0
            summary_data.append({'table': table_name, 'row_count': row_count})

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(summary_file, index=False)
        print(f'\nCreated summary file: {summary_file}')

        # Print summary
        print(f'\nTables with data ({len(exported_tables)}):')
        for table_name, row_count in exported_tables:
            print(f'  - {table_name}: {row_count} rows')

        if empty_tables:
            print(f'\nEmpty tables ({len(empty_tables)}):')
            for table_name in empty_tables:
                print(f'  - {table_name}')

    finally:
        session.close()


def export_detailed_views(output_dir='debug_csvs'):
    """Export detailed views of key tables with joined data."""

    print('\n' + '=' * 60)
    print('Exporting detailed views of key tables...')

    session = get_session(echo=False, test=True)

    try:
        # Detailed view of values_cat with all relationships
        query = """
        SELECT
            vc.id,
            vc.value_open,
            ct.label as value_controlled_label,
            vc.object,
            o.id_type as object_type,
            di.label as desc_inst_label,
            dc.label as desc_cat_label,
            vi.id_formal as instance_formal_id
        FROM values_cat vc
        LEFT JOIN controlled_terms ct ON vc.value_controlled = ct.id
        LEFT JOIN objects o ON vc.object = o.id
        LEFT JOIN descriptors_inst di ON vc.desc_inst = di.id
        LEFT JOIN descriptors_cat dc ON vc.desc_cat = dc.id
        LEFT JOIN values_inst vi ON vc.instance = vi.id
        ORDER BY vc.id
        """
        df = pd.read_sql_query(query, session.bind)
        output_file = os.path.join(output_dir, 'values_cat_detailed.csv')
        df.to_csv(output_file, index=False)
        print(f'✓ Exported detailed values_cat: {len(df)} rows → {output_file}')

        # Detailed view of values_quant with all relationships
        query = """
        SELECT
            vq.id,
            vq.value,
            vq.object,
            vq.desc_inst,
            vq.desc_quant,
            vq.instance,
            vq.orig_value,
            vq.orig_units,
            vq.value_blob,
            di.label as desc_inst_label,
            dq.label as desc_quant_label,
            vi.id_formal as instance_formal_id,
            o.id_type as object_type,
            u.label as unit_label,
            a.label as aspect_label
        FROM values_quant vq
        LEFT JOIN descriptors_inst di ON vq.desc_inst = di.id
        LEFT JOIN descriptors_quant dq ON vq.desc_quant = dq.id
        LEFT JOIN values_inst vi ON vq.instance = vi.id
        LEFT JOIN objects o ON vq.object = o.id
        LEFT JOIN units u ON dq.unit = u.id
        LEFT JOIN aspects a ON dq.aspect = a.id
        ORDER BY vq.id
        """
        df = pd.read_sql_query(query, session.bind)
        output_file = os.path.join(output_dir, 'values_quant_detailed.csv')
        df.to_csv(output_file, index=False)
        print(f'✓ Exported detailed values_quant: {len(df)} rows → {output_file}')

        # Detailed view of objects
        query = """
        SELECT
            o.id,
            o.id_type,
            o.id_file,
            o.id_internal,
            parent.id as parent_id,
            parent.id_type as parent_type
        FROM objects o
        LEFT JOIN dataset_object d_obj ON o.id = d_obj.object
        LEFT JOIN objects parent ON d_obj.dataset = parent.id
        ORDER BY o.id_type, o.id
        """
        df = pd.read_sql_query(query, session.bind)
        output_file = os.path.join(output_dir, 'objects_detailed.csv')
        df.to_csv(output_file, index=False)
        print(f'✓ Exported detailed objects: {len(df)} rows → {output_file}')

        # Detailed view of values_inst
        query = """
        SELECT
            vi.id,
            vi.type,
            vi.desc_inst,
            vi.dataset,
            vi.id_formal,
            vi.local_identifier,
            vi.id_sub,
            vi.id_sam
        FROM values_inst vi
        ORDER BY vi.id
        """
        df = pd.read_sql_query(query, session.bind)
        output_file = os.path.join(output_dir, 'values_inst_detailed.csv')
        df.to_csv(output_file, index=False)
        print(f'✓ Exported detailed values_inst: {len(df)} rows → {output_file}')

        # Detailed view of obj_desc_inst
        query = """
        SELECT
            odi.object,
            odi.desc_inst,
            odi.addr_field,
            di.label as desc_inst_label,
            o.id_type as object_type
        FROM obj_desc_inst odi
        LEFT JOIN descriptors_inst di ON odi.desc_inst = di.id
        LEFT JOIN objects o ON odi.object = o.id
        ORDER BY odi.object
        """
        df = pd.read_sql_query(query, session.bind)
        output_file = os.path.join(output_dir, 'obj_desc_inst_detailed.csv')
        df.to_csv(output_file, index=False)
        print(f'✓ Exported detailed obj_desc_inst: {len(df)} rows → {output_file}')

        # Detailed view of obj_desc_cat
        query = """
        SELECT
            odc.object,
            odc.desc_cat,
            odc.addr_field,
            dc.label as desc_cat_label,
            o.id_type as object_type
        FROM obj_desc_cat odc
        LEFT JOIN descriptors_cat dc ON odc.desc_cat = dc.id
        LEFT JOIN objects o ON odc.object = o.id
        ORDER BY odc.object
        """
        df = pd.read_sql_query(query, session.bind)
        output_file = os.path.join(output_dir, 'obj_desc_cat_detailed.csv')
        df.to_csv(output_file, index=False)
        print(f'✓ Exported detailed obj_desc_cat: {len(df)} rows → {output_file}')

        # Detailed view of obj_desc_quant
        query = """
        SELECT
            odq.object,
            odq.desc_quant,
            odq.addr_field,
            dq.label as desc_quant_label,
            o.id_type as object_type
        FROM obj_desc_quant odq
        LEFT JOIN descriptors_quant dq ON odq.desc_quant = dq.id
        LEFT JOIN objects o ON odq.object = o.id
        ORDER BY odq.object
        """
        df = pd.read_sql_query(query, session.bind)
        output_file = os.path.join(output_dir, 'obj_desc_quant_detailed.csv')
        df.to_csv(output_file, index=False)
        print(f'✓ Exported detailed obj_desc_quant: {len(df)} rows → {output_file}')

    finally:
        session.close()


if __name__ == '__main__':
    print('Exporting all tables from test database to CSV files...')
    print('This shows the data populated by f006.py\n')

    export_tables_to_csv()
    export_detailed_views()

    print("\nDone! Check the 'debug_csvs' directory for the exported files.")
