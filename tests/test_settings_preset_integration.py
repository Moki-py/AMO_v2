"""
Tests for ExportSettingsManager and ExportPresetManager integration
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock
from bson import ObjectId

from amocrm_exporter.web.export_settings import (
    ExportSettingsManager,
    ExportSettings,
    FieldInfo,
    EntityType
)
from amocrm_exporter.web.export_presets import ExportPreset


@pytest.fixture
def mock_storage():
    """Create mock storage"""
    storage = Mock()
    storage.db = Mock()
    storage.db.export_settings = Mock()
    storage.db.export_presets = Mock()
    return storage


@pytest.fixture
def settings_manager(mock_storage):
    """Create ExportSettingsManager with mocked dependencies"""
    with patch('amocrm_exporter.web.export_settings.DataEnricher'), \
         patch('amocrm_exporter.web.export_settings.ExportPresetManager') as mock_preset_manager:

        manager = ExportSettingsManager(mock_storage)
        manager.preset_manager = mock_preset_manager.return_value
        return manager


@pytest.fixture
def sample_settings():
    """Create sample export settings"""
    return ExportSettings(
        entity_type="deals",
        selected_fields=["id", "name", "price"],
        field_order=["id", "name", "price"],
        filters={},
        name="Test Settings",
        description="Test export settings"
    )


@pytest.fixture
def sample_preset():
    """Create sample export preset"""
    return ExportPreset(
        name="Test Preset",
        entity_type="deals",
        selected_fields=["id", "name", "price"],
        field_order=["id", "name", "price"],
        custom_field_mappings={"123": "Custom Field 1"},
        preset_id="preset_123"
    )


@pytest.mark.asyncio
async def test_create_preset_from_settings(settings_manager, sample_settings):
    """Test creating a preset from export settings"""
    # Mock preset manager save
    settings_manager.preset_manager.save_preset.return_value = "new_preset_id"

    # Create preset from settings
    preset_id = await settings_manager.create_preset_from_settings(
        sample_settings,
        "New Preset",
        {"123": "Custom Field"}
    )

    # Verify result
    assert preset_id == "new_preset_id"
    settings_manager.preset_manager.save_preset.assert_called_once()

    # Check the preset that was created
    call_args = settings_manager.preset_manager.save_preset.call_args[0][0]
    assert call_args.name == "New Preset"
    assert call_args.entity_type == "deals"
    assert call_args.selected_fields == ["id", "name", "price"]
    assert call_args.custom_field_mappings == {"123": "Custom Field"}


@pytest.mark.asyncio
async def test_apply_preset_to_settings(settings_manager, sample_preset):
    """Test converting a preset to export settings"""
    # Mock preset manager load
    settings_manager.preset_manager.load_preset.return_value = sample_preset

    # Apply preset to settings
    settings = await settings_manager.apply_preset_to_settings("preset_123")

    # Verify result
    assert settings is not None
    assert settings.entity_type == "deals"
    assert settings.selected_fields == ["id", "name", "price"]
    assert settings.field_order == ["id", "name", "price"]
    assert "preset" in settings.name.lower()


@pytest.mark.asyncio
async def test_apply_nonexistent_preset(settings_manager):
    """Test applying non-existent preset returns None"""
    # Mock preset manager load returning None
    settings_manager.preset_manager.load_preset.return_value = None

    # Apply preset to settings
    settings = await settings_manager.apply_preset_to_settings("nonexistent")

    # Verify result
    assert settings is None


@pytest.mark.asyncio
async def test_get_available_fields_with_preset_names(settings_manager, sample_preset):
    """Test getting fields with custom names from preset"""
    # Mock base field loading
    base_fields = [
        FieldInfo(
            field_id="id",
            field_name="ID",
            field_type="integer",
            is_custom=False
        ),
        FieldInfo(
            field_id="custom_field_123",
            field_name="custom_field_123",
            field_type="text",
            is_custom=True,
            custom_id="123"
        )
    ]

    with patch.object(settings_manager, 'get_available_fields', return_value=base_fields):
        # Mock preset loading
        settings_manager.preset_manager.load_preset.return_value = sample_preset

        # Get fields with preset names
        fields = await settings_manager.get_available_fields_with_preset_names(
            EntityType.DEALS,
            "preset_123"
        )

        # Verify custom field name was applied
        assert len(fields) == 2
        custom_field = next(f for f in fields if f.is_custom)
        assert custom_field.field_name == "Custom Field 1"  # From preset mapping
        assert custom_field.is_user_friendly is True


@pytest.mark.asyncio
async def test_get_available_fields_without_preset(settings_manager):
    """Test getting fields without preset returns base fields"""
    base_fields = [
        FieldInfo(
            field_id="id",
            field_name="ID",
            field_type="integer",
            is_custom=False
        )
    ]

    with patch.object(settings_manager, 'get_available_fields', return_value=base_fields):
        # Get fields without preset
        fields = await settings_manager.get_available_fields_with_preset_names(
            EntityType.DEALS
        )

        # Verify base fields returned
        assert len(fields) == 1
        assert fields[0].field_name == "ID"


@pytest.mark.asyncio
async def test_list_presets_for_entity(settings_manager):
    """Test listing presets for entity type"""
    # Mock preset manager
    expected_presets = [
        {"name": "Preset 1", "entity_type": "deals"},
        {"name": "Preset 2", "entity_type": "deals"}
    ]
    settings_manager.preset_manager.get_presets_by_entity.return_value = expected_presets

    # List presets
    presets = await settings_manager.list_presets_for_entity(EntityType.DEALS)

    # Verify result
    assert presets == expected_presets
    settings_manager.preset_manager.get_presets_by_entity.assert_called_with("deals")


@pytest.mark.asyncio
async def test_ensure_unique_field_names(settings_manager):
    """Test ensuring field names are unique"""
    # Mock field information
    field_info_list = [
        FieldInfo(
            field_id="id",
            field_name="ID",
            field_type="integer",
            is_custom=False
        ),
        FieldInfo(
            field_id="name",
            field_name="Name",
            field_type="text",
            is_custom=False
        ),
        FieldInfo(
            field_id="custom_field_123",
            field_name="Name",  # Duplicate name
            field_type="text",
            is_custom=True
        )
    ]

    with patch.object(settings_manager, 'get_available_fields', return_value=field_info_list):
        # Ensure unique names
        unique_names = await settings_manager.ensure_unique_field_names(
            ["id", "name", "custom_field_123"],
            EntityType.DEALS
        )

        # Verify uniqueness
        assert len(unique_names) == 3
        assert len(set(unique_names)) == 3  # All unique
        assert "ID" in unique_names
        assert "Name" in unique_names
        assert "Name_1" in unique_names  # Duplicate resolved


@pytest.mark.asyncio
async def test_validate_preset_compatibility_valid(settings_manager, sample_preset):
    """Test validating compatible preset"""
    # Mock preset loading
    settings_manager.preset_manager.load_preset.return_value = sample_preset

    # Mock available fields
    available_fields = [
        FieldInfo(field_id="id", field_name="ID", field_type="integer"),
        FieldInfo(field_id="name", field_name="Name", field_type="text"),
        FieldInfo(field_id="price", field_name="Price", field_type="number")
    ]

    with patch.object(settings_manager, 'get_available_fields', return_value=available_fields):
        # Validate compatibility
        result = await settings_manager.validate_preset_compatibility("preset_123", EntityType.DEALS)

        # Verify result
        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
        assert result["available_fields_count"] == 3
        assert result["preset_fields_count"] == 3


@pytest.mark.asyncio
async def test_validate_preset_compatibility_missing_fields(settings_manager, sample_preset):
    """Test validating preset with missing fields"""
    # Mock preset loading
    settings_manager.preset_manager.load_preset.return_value = sample_preset

    # Mock available fields (missing 'price')
    available_fields = [
        FieldInfo(field_id="id", field_name="ID", field_type="integer"),
        FieldInfo(field_id="name", field_name="Name", field_type="text")
    ]

    with patch.object(settings_manager, 'get_available_fields', return_value=available_fields):
        # Validate compatibility
        result = await settings_manager.validate_preset_compatibility("preset_123", EntityType.DEALS)

        # Verify result
        assert result["is_valid"] is False
        assert "price" in result["missing_fields"]
        assert len(result["warnings"]) > 0


def test_get_preset_manager(settings_manager):
    """Test getting preset manager instance"""
    preset_manager = settings_manager.get_preset_manager()
    assert preset_manager is not None
    assert preset_manager == settings_manager.preset_manager


if __name__ == "__main__":
    pytest.main([__file__, "-v"])