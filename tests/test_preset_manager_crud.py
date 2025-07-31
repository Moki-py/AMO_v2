"""
Integration tests for ExportPresetManager CRUD operations
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from bson import ObjectId

from amocrm_exporter.web.export_presets import (
    ExportPreset,
    ExportPresetManager,
    EntityType
)


class TestExportPresetManagerCRUD:
    """Test CRUD operations for ExportPresetManager"""

    @pytest.fixture
    def mock_storage(self):
        """Create mock storage with proper MongoDB collection"""
        storage = Mock()
        storage.db = Mock()

        # Create a mock collection that behaves more like MongoDB
        mock_collection = Mock()
        storage.db.export_presets = mock_collection

        return storage

    @pytest.fixture
    def preset_manager(self, mock_storage):
        """Create ExportPresetManager with mocked storage"""
        with patch.object(ExportPresetManager, '_ensure_indexes'):
            return ExportPresetManager(mock_storage)

    @pytest.fixture
    def sample_preset(self):
        """Create a sample preset for testing"""
        return ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id", "name", "price"],
            field_order=["id", "name", "price"],
            custom_field_mappings={"123": "Custom Field 1"},
            description="Test preset description"
        )

    def test_save_new_preset_success(self, preset_manager, mock_storage, sample_preset):
        """Test successfully saving a new preset"""
        # Mock no duplicate name
        mock_storage.db.export_presets.find_one.return_value = None

        # Mock successful insert
        new_id = ObjectId()
        mock_storage.db.export_presets.insert_one.return_value = Mock(inserted_id=new_id)

        # Save preset
        preset_id = preset_manager.save_preset(sample_preset)

        # Verify result
        assert preset_id == str(new_id)
        mock_storage.db.export_presets.insert_one.assert_called_once()

    def test_save_preset_duplicate_name_error(self, preset_manager, mock_storage, sample_preset):
        """Test error when saving preset with duplicate name"""
        # Mock existing preset with same name
        mock_storage.db.export_presets.find_one.return_value = {"name": "Test Preset"}

        # Should raise ValueError
        with pytest.raises(ValueError, match="already exists"):
            preset_manager.save_preset(sample_preset)

    def test_save_preset_validation_error(self, preset_manager):
        """Test error when saving invalid preset"""
        invalid_preset = ExportPreset(
            name="",  # Invalid empty name
            entity_type="deals",
            selected_fields=["id"],
            field_order=["id"]
        )

        with pytest.raises(ValueError, match="validation failed"):
            preset_manager.save_preset(invalid_preset)

    def test_load_existing_preset(self, preset_manager, mock_storage):
        """Test loading an existing preset"""
        preset_id = str(ObjectId())
        mock_doc = {
            "_id": ObjectId(preset_id),
            "name": "Test Preset",
            "entity_type": "deals",
            "selected_fields": ["id", "name"],
            "field_order": ["id", "name"],
            "custom_field_mappings": {"123": "Custom Field"},
            "filters": {},
            "description": "Test description",
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T10:00:00"
        }

        mock_storage.db.export_presets.find_one.return_value = mock_doc

        preset = preset_manager.load_preset(preset_id)

        assert preset is not None
        assert preset.name == "Test Preset"
        assert preset.entity_type == "deals"
        assert preset.preset_id == preset_id
        assert preset.custom_field_mappings == {"123": "Custom Field"}

    def test_load_nonexistent_preset(self, preset_manager, mock_storage):
        """Test loading non-existent preset returns None"""
        mock_storage.db.export_presets.find_one.return_value = None

        preset = preset_manager.load_preset("nonexistent_id")

        assert preset is None

    def test_list_all_presets(self, preset_manager, mock_storage):
        """Test listing all presets"""
        mock_docs = [
            {
                "_id": ObjectId(),
                "name": "Preset 1",
                "entity_type": "deals",
                "selected_fields": ["id"],
                "field_order": ["id"],
                "custom_field_mappings": {},
                "filters": {},
                "created_at": "2024-01-01T10:00:00",
                "updated_at": "2024-01-01T10:00:00"
            },
            {
                "_id": ObjectId(),
                "name": "Preset 2",
                "entity_type": "contacts",
                "selected_fields": ["id", "name"],
                "field_order": ["id", "name"],
                "custom_field_mappings": {},
                "filters": {},
                "created_at": "2024-01-01T11:00:00",
                "updated_at": "2024-01-01T11:00:00"
            }
        ]

        # Mock the MongoDB cursor chain
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(mock_docs))
        mock_storage.db.export_presets.find.return_value.sort.return_value = mock_cursor

        presets = preset_manager.list_presets()

        assert len(presets) == 2
        assert presets[0].name == "Preset 1"
        assert presets[1].name == "Preset 2"

    def test_list_presets_by_entity_type(self, preset_manager, mock_storage):
        """Test listing presets filtered by entity type"""
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([]))
        mock_storage.db.export_presets.find.return_value.sort.return_value = mock_cursor

        preset_manager.list_presets("deals")

        # Verify the query was called with entity type filter
        mock_storage.db.export_presets.find.assert_called_with({"entity_type": "deals"})

    def test_delete_existing_preset(self, preset_manager, mock_storage):
        """Test deleting an existing preset"""
        preset_id = str(ObjectId())
        mock_storage.db.export_presets.delete_one.return_value = Mock(deleted_count=1)

        result = preset_manager.delete_preset(preset_id)

        assert result is True
        mock_storage.db.export_presets.delete_one.assert_called_once()

    def test_delete_nonexistent_preset(self, preset_manager, mock_storage):
        """Test deleting non-existent preset returns False"""
        mock_storage.db.export_presets.delete_one.return_value = Mock(deleted_count=0)

        result = preset_manager.delete_preset("nonexistent_id")

        assert result is False

    def test_duplicate_preset_success(self, preset_manager, mock_storage):
        """Test successfully duplicating a preset"""
        original_id = str(ObjectId())
        original_doc = {
            "_id": ObjectId(original_id),
            "name": "Original",
            "entity_type": "deals",
            "selected_fields": ["id", "name"],
            "field_order": ["id", "name"],
            "custom_field_mappings": {"123": "Custom Field"},
            "filters": {},
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T10:00:00"
        }

        # Mock loading original preset
        mock_storage.db.export_presets.find_one.return_value = original_doc

        # Mock no duplicate name for new preset
        def mock_find_one_side_effect(query):
            if "_id" in query:
                return original_doc
            else:
                return None  # No duplicate name

        mock_storage.db.export_presets.find_one.side_effect = mock_find_one_side_effect

        # Mock saving duplicate
        new_id = ObjectId()
        mock_storage.db.export_presets.insert_one.return_value = Mock(inserted_id=new_id)

        duplicate_id = preset_manager.duplicate_preset(original_id, "Duplicate")

        assert duplicate_id == str(new_id)

    def test_duplicate_nonexistent_preset(self, preset_manager, mock_storage):
        """Test duplicating non-existent preset returns None"""
        mock_storage.db.export_presets.find_one.return_value = None

        result = preset_manager.duplicate_preset("nonexistent_id", "New Name")

        assert result is None

    def test_is_duplicate_name_detection(self, preset_manager, mock_storage):
        """Test duplicate name detection"""
        # Test with existing name
        mock_storage.db.export_presets.find_one.return_value = {"name": "Existing"}
        result = preset_manager._is_duplicate_name("Existing", "deals")
        assert result is True

        # Test with non-existing name
        mock_storage.db.export_presets.find_one.return_value = None
        result = preset_manager._is_duplicate_name("New Name", "deals")
        assert result is False

    def test_get_presets_by_entity(self, preset_manager, mock_storage):
        """Test getting presets by entity type as dictionaries"""
        mock_docs = [
            {
                "_id": ObjectId(),
                "name": "Deals Preset",
                "entity_type": "deals",
                "selected_fields": ["id", "name"],
                "field_order": ["id", "name"],
                "custom_field_mappings": {},
                "filters": {},
                "created_at": "2024-01-01T10:00:00",
                "updated_at": "2024-01-01T10:00:00"
            }
        ]

        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(mock_docs))
        mock_storage.db.export_presets.find.return_value.sort.return_value = mock_cursor

        presets_dict = preset_manager.get_presets_by_entity("deals")

        assert len(presets_dict) == 1
        assert isinstance(presets_dict[0], dict)
        assert presets_dict[0]["name"] == "Deals Preset"

    def test_get_preset_summary(self, preset_manager, mock_storage):
        """Test getting preset summary"""
        preset_id = str(ObjectId())
        mock_doc = {
            "_id": ObjectId(preset_id),
            "name": "Test Preset",
            "entity_type": "deals",
            "description": "Test description",
            "selected_fields": ["id", "name", "price"],
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T11:00:00"
        }

        mock_storage.db.export_presets.find_one.return_value = mock_doc

        summary = preset_manager.get_preset_summary(preset_id)

        assert summary is not None
        assert summary["name"] == "Test Preset"
        assert summary["field_count"] == 3
        assert summary["preset_id"] == preset_id

    def test_update_existing_preset(self, preset_manager, mock_storage, sample_preset):
        """Test updating an existing preset"""
        # Set preset ID to simulate existing preset
        sample_preset.preset_id = str(ObjectId())

        # Mock no duplicate name
        mock_storage.db.export_presets.find_one.return_value = None

        # Mock successful update
        mock_storage.db.export_presets.replace_one.return_value = Mock(matched_count=1)

        # Update preset
        preset_id = preset_manager.save_preset(sample_preset)

        # Verify result
        assert preset_id == sample_preset.preset_id
        mock_storage.db.export_presets.replace_one.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])