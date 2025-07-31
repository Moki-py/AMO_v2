"""
Simple unit tests for Export Preset Management System
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from bson import ObjectId

from amocrm_exporter.web.export_presets import (
    ExportPreset,
    ExportPresetManager,
    EntityType
)


def test_export_preset_basic_creation():
    """Test basic ExportPreset creation"""
    preset = ExportPreset(
        name="Test Preset",
        entity_type="deals",
        selected_fields=["id", "name", "price"],
        field_order=["id", "name", "price"]
    )

    assert preset.name == "Test Preset"
    assert preset.entity_type == "deals"
    assert preset.selected_fields == ["id", "name", "price"]
    assert preset.field_order == ["id", "name", "price"]
    assert isinstance(preset.created_at, datetime)
    assert isinstance(preset.updated_at, datetime)


def test_export_preset_validation():
    """Test preset validation"""
    # Valid preset
    valid_preset = ExportPreset(
        name="Valid",
        entity_type="deals",
        selected_fields=["id", "name"],
        field_order=["id", "name"]
    )
    assert valid_preset.validate() == []

    # Invalid preset - empty name
    invalid_preset = ExportPreset(
        name="",
        entity_type="deals",
        selected_fields=["id"],
        field_order=["id"]
    )
    errors = invalid_preset.validate()
    assert len(errors) > 0
    assert any("name is required" in error for error in errors)


def test_export_preset_to_dict():
    """Test conversion to dictionary"""
    preset = ExportPreset(
        name="Dict Test",
        entity_type="deals",
        selected_fields=["id", "name"],
        field_order=["id", "name"],
        description="Test description"
    )

    preset_dict = preset.to_dict()

    assert preset_dict["name"] == "Dict Test"
    assert preset_dict["entity_type"] == "deals"
    assert preset_dict["selected_fields"] == ["id", "name"]
    assert isinstance(preset_dict["created_at"], str)  # Should be ISO format
    assert isinstance(preset_dict["updated_at"], str)


def test_export_preset_from_dict():
    """Test creation from dictionary"""
    object_id = ObjectId()
    preset_data = {
        "_id": object_id,
        "name": "From Dict Test",
        "entity_type": "contacts",
        "selected_fields": ["id", "email"],
        "field_order": ["id", "email"],
        "created_at": "2024-01-01T10:00:00",
        "updated_at": "2024-01-01T11:00:00"
    }

    preset = ExportPreset.from_dict(preset_data)

    assert preset.name == "From Dict Test"
    assert preset.entity_type == "contacts"
    assert isinstance(preset.created_at, datetime)
    assert isinstance(preset.updated_at, datetime)
    assert preset.preset_id == str(object_id)


def test_export_preset_custom_field_operations():
    """Test custom field mapping operations"""
    preset = ExportPreset(
        name="Custom Field Test",
        entity_type="deals",
        selected_fields=["id"],
        field_order=["id"]
    )

    # Add mapping
    preset.add_custom_field_mapping("123", "Test Field")
    assert preset.custom_field_mappings["123"] == "Test Field"
    assert preset.get_display_name("123") == "Test Field"
    assert preset.get_display_name("999") == "999"  # Fallback to field_id

    # Remove mapping
    preset.remove_custom_field_mapping("123")
    assert "123" not in preset.custom_field_mappings


def test_export_preset_manager_initialization():
    """Test preset manager initialization"""
    mock_storage = Mock()
    mock_storage.db = Mock()
    mock_storage.db.export_presets = Mock()

    with patch.object(ExportPresetManager, '_ensure_indexes'):
        manager = ExportPresetManager(mock_storage)
        assert manager.storage == mock_storage


def test_export_preset_manager_validate_data():
    """Test preset data validation"""
    mock_storage = Mock()
    mock_storage.db = Mock()
    mock_storage.db.export_presets = Mock()

    with patch.object(ExportPresetManager, '_ensure_indexes'):
        manager = ExportPresetManager(mock_storage)

    # Valid data
    valid_data = {
        "name": "Valid Preset",
        "entity_type": "deals",
        "selected_fields": ["id", "name"],
        "field_order": ["id", "name"]
    }
    errors = manager.validate_preset_data(valid_data)
    assert len(errors) == 0

    # Invalid data
    invalid_data = {
        "name": "",  # Empty name
        "entity_type": "deals",
        "selected_fields": [],  # No fields
        "field_order": []
    }
    errors = manager.validate_preset_data(invalid_data)
    assert len(errors) > 0


if __name__ == "__main__":
    pytest.main([__file__])