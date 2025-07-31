"""
Simple CRUD functionality tests for ExportPresetManager
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from bson import ObjectId

from amocrm_exporter.web.export_presets import (
    ExportPreset,
    ExportPresetManager
)


def test_preset_manager_crud_operations():
    """Test basic CRUD operations work"""
    # Create mock storage
    mock_storage = Mock()
    mock_storage.db = Mock()
    mock_collection = Mock()
    mock_storage.db.export_presets = mock_collection

    # Create manager
    with patch.object(ExportPresetManager, '_ensure_indexes'):
        manager = ExportPresetManager(mock_storage)

    # Test CREATE - save new preset
    preset = ExportPreset(
        name="Test Preset",
        entity_type="deals",
        selected_fields=["id", "name"],
        field_order=["id", "name"]
    )

    # Mock no duplicate and successful insert
    mock_collection.find_one.return_value = None
    new_id = ObjectId()
    mock_collection.insert_one.return_value = Mock(inserted_id=new_id)

    preset_id = manager.save_preset(preset)
    assert preset_id == str(new_id)

    # Test READ - load preset
    mock_doc = {
        "_id": new_id,
        "name": "Test Preset",
        "entity_type": "deals",
        "selected_fields": ["id", "name"],
        "field_order": ["id", "name"],
        "custom_field_mappings": {},
        "filters": {},
        "created_at": "2024-01-01T10:00:00",
        "updated_at": "2024-01-01T10:00:00"
    }
    mock_collection.find_one.return_value = mock_doc

    loaded_preset = manager.load_preset(str(new_id))
    assert loaded_preset is not None
    assert loaded_preset.name == "Test Preset"

    # Test UPDATE - save existing preset
    loaded_preset.name = "Updated Preset"
    mock_collection.replace_one.return_value = Mock(matched_count=1)

    # Mock no duplicate for the update (should exclude current preset ID)
    mock_collection.find_one.return_value = None

    updated_id = manager.save_preset(loaded_preset)
    assert updated_id == str(new_id)

    # Test DELETE
    mock_collection.delete_one.return_value = Mock(deleted_count=1)
    result = manager.delete_preset(str(new_id))
    assert result is True


def test_preset_validation_works():
    """Test that preset validation catches errors"""
    mock_storage = Mock()
    mock_storage.db = Mock()
    mock_storage.db.export_presets = Mock()

    with patch.object(ExportPresetManager, '_ensure_indexes'):
        manager = ExportPresetManager(mock_storage)

    # Test invalid preset
    invalid_preset = ExportPreset(
        name="",  # Empty name should fail validation
        entity_type="deals",
        selected_fields=["id"],
        field_order=["id"]
    )

    with pytest.raises(ValueError, match="validation failed"):
        manager.save_preset(invalid_preset)


def test_duplicate_detection_works():
    """Test that duplicate name detection works"""
    mock_storage = Mock()
    mock_storage.db = Mock()
    mock_collection = Mock()
    mock_storage.db.export_presets = mock_collection

    with patch.object(ExportPresetManager, '_ensure_indexes'):
        manager = ExportPresetManager(mock_storage)

    preset = ExportPreset(
        name="Duplicate Name",
        entity_type="deals",
        selected_fields=["id"],
        field_order=["id"]
    )

    # Mock existing preset with same name
    mock_collection.find_one.return_value = {"name": "Duplicate Name"}

    with pytest.raises(ValueError, match="already exists"):
        manager.save_preset(preset)


def test_list_presets_functionality():
    """Test listing presets works"""
    mock_storage = Mock()
    mock_storage.db = Mock()
    mock_collection = Mock()
    mock_storage.db.export_presets = mock_collection

    with patch.object(ExportPresetManager, '_ensure_indexes'):
        manager = ExportPresetManager(mock_storage)

    # Mock empty result
    mock_cursor = Mock()
    mock_cursor.__iter__ = Mock(return_value=iter([]))
    mock_collection.find.return_value.sort.return_value = mock_cursor

    presets = manager.list_presets()
    assert isinstance(presets, list)
    assert len(presets) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])