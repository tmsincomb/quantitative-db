#!/usr/bin/env python3
"""
Test that f006.py properly implements the table population guide.

This test verifies:
1. All root tables are created (Addresses, Aspects, Units, ControlledTerms, DescriptorsInst, Objects)
2. All intermediate tables are created (DescriptorsCat, DescriptorsQuant, ValuesInst, ObjDesc*)
3. Leaf tables use back_populate_tables (ValuesCat, ValuesQuant)
4. Proper population order is followed
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from quantdb.models import (
    Addresses,
    Aspects,
    Units,
    ControlledTerms,
    DescriptorsInst,
    Objects,
    DescriptorsCat,
    DescriptorsQuant,
    ValuesInst,
    ObjDescInst,
    ObjDescCat,
    ObjDescQuant,
    ValuesCat,
    ValuesQuant,
)


class TestF006TablePopulation:
    """Test that f006.py properly implements the table population guide."""

    def test_root_tables_created(self):
        """Test that all root tables are created."""
        from ingestion.f006 import create_basic_descriptors

        # Mock session
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        mock_session.add = Mock()
        mock_session.commit = Mock()

        # Mock get_or_create to track what's created
        created_objects = []

        def mock_get_or_create(session, obj):
            created_objects.append(obj)
            # Set an ID for objects that need it
            if hasattr(obj, "id"):
                obj.id = len(created_objects)
            return obj

        with patch("ingestion.f006.get_or_create", side_effect=mock_get_or_create):
            components = create_basic_descriptors(mock_session)

        # Check that all root tables were created
        created_types = [type(obj).__name__ for obj in created_objects]

        # Root tables that should be created
        assert "Aspects" in created_types, "Aspects (root table) not created"
        assert "Units" in created_types, "Units (root table) not created"
        assert "DescriptorsInst" in created_types, "DescriptorsInst (root table) not created"
        assert "ControlledTerms" in created_types, "ControlledTerms (root table) not created"

        # Addresses should be created via session.add
        assert mock_session.add.called, "Addresses not created"
        assert any(isinstance(call[0][0], Addresses) for call in mock_session.add.call_args_list), "Addresses not created"

        # Verify specific Aspects were created
        aspects_created = [obj for obj in created_objects if isinstance(obj, Aspects)]
        aspect_labels = [a.label for a in aspects_created]
        assert "volume" in aspect_labels, "Volume aspect not created"
        assert "diameter" in aspect_labels, "Diameter aspect not created"

        # Verify specific Units were created
        units_created = [obj for obj in created_objects if isinstance(obj, Units)]
        unit_labels = [u.label for u in units_created]
        assert "mm3" in unit_labels, "mm3 unit not created"
        assert "um" in unit_labels, "um unit not created"

    def test_intermediate_tables_created(self):
        """Test that intermediate tables are created after root tables."""
        from ingestion.f006 import create_basic_descriptors

        # Mock session and get_or_create
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = None

        created_objects = []

        def mock_get_or_create(session, obj):
            created_objects.append(obj)
            if hasattr(obj, "id"):
                obj.id = len(created_objects)
            return obj

        with patch("ingestion.f006.get_or_create", side_effect=mock_get_or_create):
            components = create_basic_descriptors(mock_session)

        # Check intermediate tables
        created_types = [type(obj).__name__ for obj in created_objects]

        # Intermediate tables that should be created
        assert "DescriptorsCat" in created_types, "DescriptorsCat (intermediate table) not created"
        assert "DescriptorsQuant" in created_types, "DescriptorsQuant (intermediate table) not created"

        # Verify DescriptorsQuant have proper dependencies
        quant_descs = [obj for obj in created_objects if isinstance(obj, DescriptorsQuant)]
        for qd in quant_descs:
            assert qd.unit is not None, "DescriptorsQuant missing unit dependency"
            assert qd.aspect is not None, "DescriptorsQuant missing aspect dependency"
            assert qd.domain is not None, "DescriptorsQuant missing domain dependency"

    def test_obj_desc_mappings_created(self):
        """Test that ObjDesc* mapping tables are created."""
        from ingestion.f006 import create_obj_desc_mappings

        # Mock components
        mock_components = {"descriptors": {"nerve-volume": Mock(id=1)}, "modality_desc": Mock(id=2), "nerve_volume_desc": Mock(id=3), "const_addr": Mock(id=4), "tabular_addr": Mock(id=5)}

        # Mock package objects
        mock_packages = [Mock(id=f"package-{i}") for i in range(3)]

        # Mock session and get_or_create
        created_objects = []

        def mock_get_or_create(session, obj):
            created_objects.append(obj)
            return obj

        with patch("ingestion.f006.get_or_create", side_effect=mock_get_or_create):
            mappings = create_obj_desc_mappings(Mock(), mock_components, mock_packages)

        # Check that all ObjDesc* tables were created
        created_types = [type(obj).__name__ for obj in created_objects]

        assert created_types.count("ObjDescInst") == 3, "ObjDescInst not created for each package"
        assert created_types.count("ObjDescCat") == 3, "ObjDescCat not created for each package"
        assert created_types.count("ObjDescQuant") == 3, "ObjDescQuant not created for each package"

    def test_back_populate_tables_used_for_leaf_tables(self):
        """Test that back_populate_tables is used for leaf tables."""
        from ingestion.f006 import create_leaf_values

        # Mock all dependencies
        mock_metadata = {"data": [{"dataset_relative_path": "sub-001/sam-001/microct/file.jpx"}, {"dataset_relative_path": "sub-001/sam-002/microct/file.jpx"}]}

        mock_components = {"descriptors": {"nerve-volume": Mock(id=1)}, "modality_desc": Mock(id=2), "nerve_volume_desc": Mock(id=3), "terms": {"microct": Mock(id=4)}}

        mock_packages = [Mock(id=f"package-{i}") for i in range(2)]
        mock_instances = {"sub-001_sam-001": Mock(id=10), "sub-001_sam-002": Mock(id=11)}

        mock_mappings = {
            "obj_desc_inst": [Mock(object=f"package-{i}") for i in range(2)],
            "obj_desc_cat": [Mock(object=f"package-{i}") for i in range(2)],
            "obj_desc_quant": [Mock(object=f"package-{i}") for i in range(2)],
        }

        # Track calls to back_populate_tables
        back_populate_calls = []

        def mock_back_populate_tables(session, obj):
            back_populate_calls.append(obj)
            return obj

        with patch("ingestion.f006.back_populate_tables", side_effect=mock_back_populate_tables):
            leaf_values = create_leaf_values(Mock(), mock_metadata, mock_components, Mock(), mock_packages, mock_instances, mock_mappings)

        # Verify back_populate_tables was called for each leaf table entry
        assert len(back_populate_calls) == 4, "back_populate_tables not called for all leaf values"

        # Check that both ValuesCat and ValuesQuant were created
        values_cat_count = sum(1 for obj in back_populate_calls if isinstance(obj, ValuesCat))
        values_quant_count = sum(1 for obj in back_populate_calls if isinstance(obj, ValuesQuant))

        assert values_cat_count == 2, f"Expected 2 ValuesCat, got {values_cat_count}"
        assert values_quant_count == 2, f"Expected 2 ValuesQuant, got {values_quant_count}"

        # Verify relationships were set before calling back_populate_tables
        for obj in back_populate_calls:
            if isinstance(obj, ValuesCat):
                assert hasattr(obj, "controlled_terms"), "ValuesCat missing controlled_terms relationship"
                assert hasattr(obj, "descriptors_cat"), "ValuesCat missing descriptors_cat relationship"
                assert hasattr(obj, "obj_desc_cat"), "ValuesCat missing obj_desc_cat relationship"
            elif isinstance(obj, ValuesQuant):
                assert hasattr(obj, "descriptors_quant"), "ValuesQuant missing descriptors_quant relationship"
                assert hasattr(obj, "obj_desc_quant"), "ValuesQuant missing obj_desc_quant relationship"

    def test_population_order_follows_guide(self):
        """Test that tables are populated in the correct order."""
        from ingestion.f006 import run_f006_ingestion

        # Track the order of operations
        operation_order = []

        def track_operation(op_name):
            def decorator(func):
                def wrapper(*args, **kwargs):
                    operation_order.append(op_name)
                    return func(*args, **kwargs)

                return wrapper

            return decorator

        # Patch all the main functions to track order
        with patch("ingestion.f006.create_basic_descriptors", track_operation("create_basic_descriptors")(lambda *a: {})):
            with patch("ingestion.f006.ingest_objects_table", track_operation("ingest_objects")(lambda *a: (Mock(), []))):
                with patch("ingestion.f006.ingest_instances_table", track_operation("ingest_instances")(lambda *a: {})):
                    with patch("ingestion.f006.create_obj_desc_mappings", track_operation("create_mappings")(lambda *a: {"obj_desc_inst": [], "obj_desc_cat": [], "obj_desc_quant": []})):
                        with patch("ingestion.f006.create_leaf_values", track_operation("create_leaf_values")(lambda *a: {"values_cat": [], "values_quant": []})):
                            with patch("ingestion.f006.load_path_metadata", lambda: {"data": []}):
                                with patch("ingestion.f006.get_session", lambda **k: Mock()):
                                    run_f006_ingestion(commit=False)

        # Verify correct order
        expected_order = [
            "create_basic_descriptors",  # Root and intermediate tables
            "ingest_objects",  # Root table (Objects)
            "ingest_instances",  # Intermediate table (ValuesInst)
            "create_mappings",  # Intermediate tables (ObjDesc*)
            "create_leaf_values",  # Leaf tables with back_populate_tables
        ]

        assert operation_order == expected_order, f"Incorrect population order: {operation_order}"


if __name__ == "__main__":
    # Run tests
    test = TestF006TablePopulation()

    print("Testing root tables creation...")
    test.test_root_tables_created()
    print("✓ Root tables test passed")

    print("\nTesting intermediate tables creation...")
    test.test_intermediate_tables_created()
    print("✓ Intermediate tables test passed")

    print("\nTesting ObjDesc mappings creation...")
    test.test_obj_desc_mappings_created()
    print("✓ ObjDesc mappings test passed")

    print("\nTesting back_populate_tables usage...")
    test.test_back_populate_tables_used_for_leaf_tables()
    print("✓ back_populate_tables test passed")

    print("\nTesting population order...")
    test.test_population_order_follows_guide()
    print("✓ Population order test passed")

    print("\n✅ All tests passed! f006.py properly implements the table population guide.")
