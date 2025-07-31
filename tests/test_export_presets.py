"""
Unit tests for Export Preset Management System
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


class TestExportPreset:
    """Test cases for ExportPreset data model"""

    def test_export_preset_creation(self):
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

    def test_export_preset_with_custom_fields(self):
        """Test ExportPreset with custom field mappings"""
        custom_mappings = {"123": "Custom Field 1", "456": "Custom Field 2"}

        preset = ExportPreset(
            name="Custom Fields Preset",
            entity_type="contacts",
            selected_fields=["id", "name", "custom_field_123"],
            field_order=["id", "name", "custom_field_123"],
            custom_field_mappings=custom_mappings
        )

        assert preset.custom_field_mappings == custom_mappings
        assert preset.get_display_name("123") == "Custom Field 1"
        assert preset.get_display_name("999") == "999"  # Fallback to field_id

    def test_export_preset_field_order_auto_correction(self):
        """Test that field_order is automatically corrected to match selected_fields"""
        preset = ExportPreset(
            name="Auto Correct Preset",
            entity_type="deals",
            selected_fields=["id", "name", "price", "status"],
            field_order=["id", "name"]  # Missing fields
        )

        # Should auto-add missing fields
        assert set(preset.field_order) == set(preset.selected_fields)
        assert "price" in preset.field_order
        assert "status" in preset.field_order

    def test_export_preset_invalid_entity_type(self):
        """Test that invalid entity type raises ValueError"""
        with pytest.raises(ValueError, match="Invalid entity type"):
            ExportPreset(
                name="Invalid Preset",
                entity_type="invalid_type",
                selected_fields=["id"],
                field_order=["id"]
            )

    def test_export_preset_to_dict(self):
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

    def test_export_preset_from_dict(self):
        """Test creation from dictionary"""
        original_id = ObjectId()
        preset_data = {
            "_id": original_id,
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
        assert preset.preset_id == str(original_id)

    def test_export_preset_duplicate(self):
        """Test preset duplication"""
        original = ExportPreset(
            name="Original",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["id", "name"],
            preset_id="original_id"
        )

        duplicate = original.duplicate("Duplicate")

        assert duplicate.name == "Duplicate"
        assert duplicate.entity_type == original.entity_type
        assert duplicate.selected_fields == original.selected_fields
        assert duplicate.preset_id is None  # Should reset ID
        assert duplicate.created_at != original.created_at  # Should be new

    def test_export_preset_update_fields(self):
        """Test updating field selection"""
        preset = ExportPreset(
            name="Update Test",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["id", "name"]
        )

        original_updated_at = preset.updated_at

        # Update fields
        preset.update_fields(["id", "name", "price"], ["name", "id", "price"])

        assert preset.selected_fields == ["id", "name", "price"]
        assert preset.field_order == ["name", "id", "price"]
        assert preset.updated_at > original_updated_at

    def test_export_preset_validation(self):
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
        assert any("name is required" in error for error in errors)

    def test_export_preset_custom_field_operations(self):
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

        # Remove mapping
        preset.remove_custom_field_mapping("123")
        assert "123" not in preset.custom_field_mappings


class TestExportPresetManager:
    """Test cases for ExportPresetManager"""

    @pytest.fixture
    def mock_storage(self):
        """Create mock storage"""
        storage = Mock()
        storage.db = Mock()
        return storage

    @pytest.fixture
    def mock_collection(self, mock_storage):
        """Create mock MongoDB collection"""
        collection = MagicMock()

        # Set up a default empty cursor for find operations
        def create_mock_cursor(docs=None):
            if docs is None:
                docs = []
            cursor = MagicMock()
            cursor.__iter__ = lambda self: iter(docs)
            cursor.sort = MagicMock(return_value=cursor)
            return cursor

        # Configure find() method with a default empty cursor
        default_cursor = create_mock_cursor([])
        collection.find.return_value.sort.return_value = default_cursor

        # Store a reference to make it easy for tests to override
        collection._mock_cursor = default_cursor
        collection._create_mock_cursor = create_mock_cursor

        # Configure find_one() to return None by default
        collection.find_one = MagicMock(return_value=None)

        # Configure insert_one() to return mock result
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = ObjectId()
        collection.insert_one = MagicMock(return_value=mock_insert_result)

        # Configure delete_one() to return proper mock result
        def mock_delete_one(*args, **kwargs):
            result = MagicMock()
            result.deleted_count = 1
            return result
        collection.delete_one = MagicMock(side_effect=mock_delete_one)

        # Configure replace_one() to return proper mock result
        def mock_replace_one(*args, **kwargs):
            result = MagicMock()
            result.matched_count = 1
            return result
        collection.replace_one = MagicMock(side_effect=mock_replace_one)

        # Configure count_documents() for the duplicate name check
        collection.count_documents = MagicMock(return_value=0)

        # Configure create_index to avoid errors
        collection.create_index = MagicMock()

        mock_storage.db.export_presets = collection
        setattr(mock_storage.db, 'export_presets', collection)
        return collection

    @pytest.fixture
    def preset_manager(self, mock_storage):
        """Create ExportPresetManager with mocked storage"""
        with patch.object(ExportPresetManager, '_ensure_indexes'):
            return ExportPresetManager(mock_storage)

    def test_preset_manager_initialization(self, mock_storage):
        """Test preset manager initialization"""
        with patch.object(ExportPresetManager, '_ensure_indexes') as mock_ensure:
            manager = ExportPresetManager(mock_storage)
            assert manager.storage == mock_storage
            mock_ensure.assert_called_once()

    def test_save_new_preset(self, preset_manager, mock_collection):
        """Test saving a new preset"""
        preset = ExportPreset(
            name="New Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["id", "name"]
        )

        # Mock MongoDB operations
        inserted_id = ObjectId()

        # Replace the insert_one method on the manager's collection
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = inserted_id
        preset_manager.presets_collection.insert_one = MagicMock(return_value=mock_insert_result)

        # Mock _is_duplicate_name to return False
        with patch.object(preset_manager, '_is_duplicate_name', return_value=False):
            preset_id = preset_manager.save_preset(preset)

        assert preset_id == str(inserted_id)
        preset_manager.presets_collection.insert_one.assert_called_once()

    def test_save_preset_duplicate_name(self, preset_manager, mock_collection):
        """Test saving preset with duplicate name raises error"""
        preset = ExportPreset(
            name="Duplicate",
            entity_type="deals",
            selected_fields=["id"],
            field_order=["id"]
        )

        with patch.object(preset_manager, '_is_duplicate_name', return_value=True):
            with pytest.raises(ValueError, match="already exists"):
                preset_manager.save_preset(preset)

    def test_load_existing_preset(self, preset_manager, mock_collection):
        """Test loading an existing preset"""
        preset_id = ObjectId()
        mock_doc = {
            "_id": preset_id,
            "name": "Test Preset",
            "entity_type": "deals",
            "selected_fields": ["id", "name"],
            "field_order": ["id", "name"],
            "custom_field_mappings": {},
            "filters": {},
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T10:00:00"
        }

        preset_manager.presets_collection.find_one.return_value = mock_doc

        preset = preset_manager.load_preset(str(preset_id))

        assert preset is not None
        assert preset.name == "Test Preset"
        assert preset.preset_id == str(preset_id)

    def test_load_nonexistent_preset(self, preset_manager, mock_collection):
        """Test loading non-existent preset returns None"""
        mock_collection.find_one.return_value = None

        preset = preset_manager.load_preset("nonexistent_id")

        assert preset is None

    def test_list_presets_all(self, preset_manager, mock_collection):
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

        # Create a proper iterable mock that can be used in for loops
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = lambda self: iter(mock_docs)
        preset_manager.presets_collection.find.return_value.sort.return_value = mock_cursor

        presets = preset_manager.list_presets()

        assert len(presets) == 2
        assert presets[0].name == "Preset 1"
        assert presets[1].name == "Preset 2"

    def test_list_presets_by_entity_type(self, preset_manager, mock_collection):
        """Test listing presets filtered by entity type"""
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = lambda self: iter([])
        preset_manager.presets_collection.find.return_value.sort.return_value = mock_cursor

        preset_manager.list_presets("deals")

        preset_manager.presets_collection.find.assert_called_with({"entity_type": "deals"})

    def test_delete_existing_preset(self, preset_manager, mock_collection):
        """Test deleting an existing preset"""
        preset_id = str(ObjectId())

        # Mock delete_one to return successful result
        mock_delete_result = MagicMock()
        mock_delete_result.deleted_count = 1
        preset_manager.presets_collection.delete_one = MagicMock(return_value=mock_delete_result)

        result = preset_manager.delete_preset(preset_id)

        assert result is True
        preset_manager.presets_collection.delete_one.assert_called_once()

    def test_delete_nonexistent_preset(self, preset_manager, mock_collection):
        """Test deleting non-existent preset returns False"""
        # Mock delete_one to return unsuccessful result
        mock_delete_result = MagicMock()
        mock_delete_result.deleted_count = 0
        preset_manager.presets_collection.delete_one = MagicMock(return_value=mock_delete_result)

        result = preset_manager.delete_preset("nonexistent_id")

        assert result is False

    def test_duplicate_preset(self, preset_manager, mock_collection):
        """Test duplicating a preset"""
        original_id = ObjectId()
        original_doc = {
            "_id": original_id,
            "name": "Original",
            "entity_type": "deals",
            "selected_fields": ["id", "name"],
            "field_order": ["id", "name"],
            "custom_field_mappings": {},
            "filters": {},
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T10:00:00"
        }

        # Mock loading original preset
        preset_manager.presets_collection.find_one.return_value = original_doc

        # Mock saving duplicate
        new_id = ObjectId()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = new_id
        preset_manager.presets_collection.insert_one = MagicMock(return_value=mock_insert_result)

        # Mock _is_duplicate_name to return False
        with patch.object(preset_manager, '_is_duplicate_name', return_value=False):
            duplicate_id = preset_manager.duplicate_preset(str(original_id), "Duplicate")

        assert duplicate_id == str(new_id)

    def test_is_duplicate_name(self, preset_manager, mock_collection):
        """Test duplicate name detection"""
        # Test with existing name
        preset_manager.presets_collection.find_one.return_value = {"name": "Existing"}
        result = preset_manager._is_duplicate_name("Existing", "deals")
        assert result is True

        # Test with non-existing name
        preset_manager.presets_collection.find_one.return_value = None
        result = preset_manager._is_duplicate_name("New Name", "deals")
        assert result is False

    def test_validate_preset_data(self, preset_manager):
        """Test preset data validation"""
        # Valid data
        valid_data = {
            "name": "Valid Preset",
            "entity_type": "deals",
            "selected_fields": ["id", "name"],
            "field_order": ["id", "name"]
        }
        errors = preset_manager.validate_preset_data(valid_data)
        assert len(errors) == 0

        # Invalid data
        invalid_data = {
            "name": "",  # Empty name
            "entity_type": "deals",
            "selected_fields": [],  # No fields
            "field_order": []
        }
        errors = preset_manager.validate_preset_data(invalid_data)
        assert len(errors) > 0

    def test_get_preset_summary(self, preset_manager, mock_collection):
        """Test getting preset summary"""
        preset_id = ObjectId()
        mock_doc = {
            "_id": preset_id,
            "name": "Test Preset",
            "entity_type": "deals",
            "description": "Test description",
            "selected_fields": ["id", "name", "price"],
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T11:00:00"
        }

        preset_manager.presets_collection.find_one.return_value = mock_doc

        summary = preset_manager.get_preset_summary(str(preset_id))

        assert summary is not None
        assert summary["name"] == "Test Preset"
        assert summary["field_count"] == 3
        assert summary["preset_id"] == str(preset_id)