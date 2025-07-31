"""
Unit tests for ExportPresetManager and ExportPreset
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from bson import ObjectId
from pymongo.errors import PyMongoError

from amocrm_exporter.web.export_presets import (
    ExportPreset,
    ExportPresetManager,
    EntityType
)
from amocrm_exporter.storage.storage import Storage


class TestExportPreset:
    """Test cases for ExportPreset dataclass"""

    def test_export_preset_creation(self):
        """Test basic ExportPreset creation"""
        preset = ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id", "name", "status"],
            field_order=["name", "id", "status"]
        )

        assert preset.name == "Test Preset"
        assert preset.entity_type == "deals"
        assert preset.selected_fields == ["id", "name", "status"]
        assert preset.field_order == ["name", "id", "status"]
        assert preset.custom_field_mappings == {}
        assert preset.filters == {}
        assert preset.created_at is not None
        assert preset.updated_at is not None
        assert preset.preset_id is None

    def test_export_preset_post_init_field_order_sync(self):
        """Test that __post_init__ syncs selected_fields and field_order"""
        # Test with missing fields in field_order
        preset = ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id", "name", "status", "created_at"],
            field_order=["name", "id"]  # Missing status and created_at
        )

        # Should add missing fields to field_order
        assert set(preset.field_order) == set(preset.selected_fields)
        assert "status" in preset.field_order
        assert "created_at" in preset.field_order

        # Test with extra fields in field_order
        preset2 = ExportPreset(
            name="Test Preset 2",
            entity_type="contacts",
            selected_fields=["id", "name"],
            field_order=["name", "id", "extra_field"]  # Extra field not in selected
        )

        # Should remove extra fields from field_order
        assert preset2.field_order == ["name", "id"]
        assert "extra_field" not in preset2.field_order

    def test_export_preset_invalid_entity_type(self):
        """Test that invalid entity type raises ValueError"""
        with pytest.raises(ValueError, match="Invalid entity type"):
            ExportPreset(
                name="Test Preset",
                entity_type="invalid_entity",
                selected_fields=["id"],
                field_order=["id"]
            )

    def test_to_dict_conversion(self):
        """Test conversion to dictionary"""
        created_time = datetime(2023, 1, 1, 12, 0, 0)
        updated_time = datetime(2023, 1, 2, 12, 0, 0)

        preset = ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["name", "id"],
            custom_field_mappings={"123": "Custom Field"},
            filters={"status": "active"},
            description="Test description",
            created_at=created_time,
            updated_at=updated_time,
            preset_id="507f1f77bcf86cd799439011"
        )

        result = preset.to_dict()

        assert result["name"] == "Test Preset"
        assert result["entity_type"] == "deals"
        assert result["selected_fields"] == ["id", "name"]
        assert result["field_order"] == ["name", "id"]
        assert result["custom_field_mappings"] == {"123": "Custom Field"}
        assert result["filters"] == {"status": "active"}
        assert result["description"] == "Test description"
        assert result["created_at"] == created_time.isoformat()
        assert result["updated_at"] == updated_time.isoformat()
        assert result["preset_id"] == "507f1f77bcf86cd799439011"

    def test_from_dict_conversion(self):
        """Test creation from dictionary"""
        data = {
            "name": "Test Preset",
            "entity_type": "contacts",
            "selected_fields": ["id", "name", "email"],
            "field_order": ["name", "email", "id"],
            "custom_field_mappings": {"456": "Phone Number"},
            "filters": {"type": "person"},
            "description": "Contact preset",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-02T12:00:00",
            "_id": ObjectId("507f1f77bcf86cd799439011")
        }

        preset = ExportPreset.from_dict(data)

        assert preset.name == "Test Preset"
        assert preset.entity_type == "contacts"
        assert preset.selected_fields == ["id", "name", "email"]
        assert preset.field_order == ["name", "email", "id"]
        assert preset.custom_field_mappings == {"456": "Phone Number"}
        assert preset.filters == {"type": "person"}
        assert preset.description == "Contact preset"
        assert preset.created_at == datetime(2023, 1, 1, 12, 0, 0)
        assert preset.updated_at == datetime(2023, 1, 2, 12, 0, 0)
        assert preset.preset_id == "507f1f77bcf86cd799439011"

    def test_duplicate_preset(self):
        """Test preset duplication"""
        original = ExportPreset(
            name="Original Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["name", "id"],
            custom_field_mappings={"123": "Custom Field"},
            preset_id="507f1f77bcf86cd799439011"
        )

        duplicate = original.duplicate("Duplicated Preset")

        assert duplicate.name == "Duplicated Preset"
        assert duplicate.entity_type == original.entity_type
        assert duplicate.selected_fields == original.selected_fields
        assert duplicate.field_order == original.field_order
        assert duplicate.custom_field_mappings == original.custom_field_mappings
        assert duplicate.preset_id is None  # Should be reset
        # Should be new timestamp (allow for same millisecond due to test speed)
        assert duplicate.created_at >= original.created_at

    def test_update_fields(self):
        """Test updating field selection and ordering"""
        preset = ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["name", "id"]
        )

        original_updated_at = preset.updated_at

        # Update with new fields and custom order
        preset.update_fields(
            selected_fields=["id", "name", "status", "created_at"],
            field_order=["status", "name", "created_at", "id"]
        )

        assert preset.selected_fields == ["id", "name", "status", "created_at"]
        assert preset.field_order == ["status", "name", "created_at", "id"]
        assert preset.updated_at > original_updated_at

    def test_update_fields_auto_order(self):
        """Test updating fields with automatic ordering"""
        preset = ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["name", "id"]
        )

        # Update without specifying field_order
        preset.update_fields(selected_fields=["id", "name", "status"])

        # Should maintain existing order and add new fields
        assert "name" in preset.field_order
        assert "id" in preset.field_order
        assert "status" in preset.field_order
        assert preset.field_order.index("name") < preset.field_order.index("id")

    def test_custom_field_mapping_operations(self):
        """Test custom field mapping add/remove operations"""
        preset = ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id"],
            field_order=["id"]
        )

        original_updated_at = preset.updated_at

        # Add custom field mapping
        preset.add_custom_field_mapping("123", "Custom Field 1")
        assert preset.custom_field_mappings["123"] == "Custom Field 1"
        assert preset.updated_at > original_updated_at

        updated_at_after_add = preset.updated_at

        # Update existing mapping
        preset.add_custom_field_mapping("123", "Updated Custom Field")
        assert preset.custom_field_mappings["123"] == "Updated Custom Field"
        assert preset.updated_at > updated_at_after_add

        # Remove mapping
        preset.remove_custom_field_mapping("123")
        assert "123" not in preset.custom_field_mappings

        # Remove non-existent mapping (should not error)
        preset.remove_custom_field_mapping("nonexistent")

    def test_get_display_name(self):
        """Test getting display names for fields"""
        preset = ExportPreset(
            name="Test Preset",
            entity_type="deals",
            selected_fields=["id", "123"],
            field_order=["id", "123"],
            custom_field_mappings={"123": "Custom Field Name"}
        )

        # Should return mapped name for custom field
        assert preset.get_display_name("123") == "Custom Field Name"

        # Should return field_id for unmapped field
        assert preset.get_display_name("id") == "id"
        assert preset.get_display_name("unmapped") == "unmapped"

    def test_validate_preset(self):
        """Test preset validation"""
        # Valid preset
        valid_preset = ExportPreset(
            name="Valid Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["name", "id"]
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

        # Invalid preset - no selected fields
        invalid_preset2 = ExportPreset(
            name="Test",
            entity_type="deals",
            selected_fields=[],
            field_order=[]
        )
        errors = invalid_preset2.validate()
        assert any("At least one field must be selected" in error for error in errors)


class TestExportPresetManager:
    """Test cases for ExportPresetManager"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_storage = Mock(spec=Storage)
        self.mock_db = Mock()
        self.mock_collection = Mock()

        self.mock_storage.db = self.mock_db
        self.mock_db.export_presets = self.mock_collection

        self.manager = ExportPresetManager(self.mock_storage)

    def test_initialization(self):
        """Test ExportPresetManager initialization"""
        assert self.manager.storage == self.mock_storage
        assert self.manager.db == self.mock_db
        assert self.manager.presets_collection == self.mock_collection

        # Should have called create_index for setting up indexes
        assert self.mock_collection.create_index.called

    def test_save_preset_new(self):
        """Test saving a new preset"""
        preset = ExportPreset(
            name="New Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["name", "id"]
        )

        # Mock successful insert
        mock_result = Mock()
        mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439011")
        self.mock_collection.insert_one.return_value = mock_result

        # Mock duplicate check
        with patch.object(self.manager, '_is_duplicate_name', return_value=False):
            result = self.manager.save_preset(preset)

            assert result == "507f1f77bcf86cd799439011"
            self.mock_collection.insert_one.assert_called_once()

    def test_save_preset_update_existing(self):
        """Test updating an existing preset"""
        preset = ExportPreset(
            name="Updated Preset",
            entity_type="deals",
            selected_fields=["id", "name", "status"],
            field_order=["name", "status", "id"],
            preset_id="507f1f77bcf86cd799439011"
        )

        # Mock successful update
        mock_result = Mock()
        mock_result.matched_count = 1
        self.mock_collection.replace_one.return_value = mock_result

        # Mock duplicate check
        with patch.object(self.manager, '_is_duplicate_name', return_value=False):
            result = self.manager.save_preset(preset)

            assert result == "507f1f77bcf86cd799439011"
            self.mock_collection.replace_one.assert_called_once()

    def test_save_preset_validation_error(self):
        """Test saving preset with validation errors"""
        invalid_preset = ExportPreset(
            name="",  # Invalid empty name
            entity_type="deals",
            selected_fields=[],  # Invalid empty fields
            field_order=[]
        )

        with pytest.raises(ValueError, match="Preset validation failed"):
            self.manager.save_preset(invalid_preset)

    def test_save_preset_duplicate_name(self):
        """Test saving preset with duplicate name"""
        preset = ExportPreset(
            name="Duplicate Name",
            entity_type="deals",
            selected_fields=["id"],
            field_order=["id"]
        )

        # Mock duplicate check returns True
        with patch.object(self.manager, '_is_duplicate_name', return_value=True):
            with pytest.raises(ValueError, match="already exists"):
                self.manager.save_preset(preset)

    def test_load_preset_success(self):
        """Test successfully loading a preset"""
        preset_id = "507f1f77bcf86cd799439011"
        mock_doc = {
            "_id": ObjectId(preset_id),
            "name": "Test Preset",
            "entity_type": "deals",
            "selected_fields": ["id", "name"],
            "field_order": ["name", "id"],
            "custom_field_mappings": {},
            "filters": {},
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:00:00"
        }

        self.mock_collection.find_one.return_value = mock_doc

        result = self.manager.load_preset(preset_id)

        assert result is not None
        assert isinstance(result, ExportPreset)
        assert result.name == "Test Preset"
        assert result.preset_id == preset_id
        self.mock_collection.find_one.assert_called_once_with({"_id": ObjectId(preset_id)})

    def test_load_preset_not_found(self):
        """Test loading non-existent preset"""
        preset_id = "507f1f77bcf86cd799439011"
        self.mock_collection.find_one.return_value = None

        result = self.manager.load_preset(preset_id)

        assert result is None
        self.mock_collection.find_one.assert_called_once_with({"_id": ObjectId(preset_id)})

    def test_list_presets_all(self):
        """Test listing all presets"""
        mock_docs = [
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "name": "Preset 1",
                "entity_type": "deals",
                "selected_fields": ["id"],
                "field_order": ["id"],
                "custom_field_mappings": {},
                "filters": {},
                "created_at": "2023-01-01T12:00:00",
                "updated_at": "2023-01-01T12:00:00"
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439012"),
                "name": "Preset 2",
                "entity_type": "contacts",
                "selected_fields": ["id", "name"],
                "field_order": ["name", "id"],
                "custom_field_mappings": {},
                "filters": {},
                "created_at": "2023-01-02T12:00:00",
                "updated_at": "2023-01-02T12:00:00"
            }
        ]

        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(mock_docs))
        self.mock_collection.find.return_value.sort.return_value = mock_cursor

        result = self.manager.list_presets()

        assert len(result) == 2
        assert all(isinstance(preset, ExportPreset) for preset in result)
        assert result[0].name == "Preset 1"
        assert result[1].name == "Preset 2"

        # Should query without filter and sort by updated_at descending
        self.mock_collection.find.assert_called_once_with({})
        self.mock_collection.find.return_value.sort.assert_called_once_with('updated_at', -1)

    def test_list_presets_filtered_by_entity(self):
        """Test listing presets filtered by entity type"""
        mock_docs = [
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "name": "Deals Preset",
                "entity_type": "deals",
                "selected_fields": ["id"],
                "field_order": ["id"],
                "custom_field_mappings": {},
                "filters": {},
                "created_at": "2023-01-01T12:00:00",
                "updated_at": "2023-01-01T12:00:00"
            }
        ]

        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(mock_docs))
        self.mock_collection.find.return_value.sort.return_value = mock_cursor

        result = self.manager.list_presets(entity_type="deals")

        assert len(result) == 1
        assert result[0].entity_type == "deals"

        # Should query with entity_type filter
        self.mock_collection.find.assert_called_once_with({"entity_type": "deals"})

    def test_list_presets_invalid_entity_type(self):
        """Test listing presets with invalid entity type"""
        with pytest.raises(ValueError, match="Invalid entity type"):
            self.manager.list_presets(entity_type="invalid_entity")

    def test_delete_preset_success(self):
        """Test successfully deleting a preset"""
        preset_id = "507f1f77bcf86cd799439011"

        mock_result = Mock()
        mock_result.deleted_count = 1
        self.mock_collection.delete_one.return_value = mock_result

        result = self.manager.delete_preset(preset_id)

        assert result is True
        self.mock_collection.delete_one.assert_called_once_with({"_id": ObjectId(preset_id)})

    def test_delete_preset_not_found(self):
        """Test deleting non-existent preset"""
        preset_id = "507f1f77bcf86cd799439011"

        mock_result = Mock()
        mock_result.deleted_count = 0
        self.mock_collection.delete_one.return_value = mock_result

        result = self.manager.delete_preset(preset_id)

        assert result is False
        self.mock_collection.delete_one.assert_called_once_with({"_id": ObjectId(preset_id)})

    def test_duplicate_preset_success(self):
        """Test successfully duplicating a preset"""
        original_preset = ExportPreset(
            name="Original Preset",
            entity_type="deals",
            selected_fields=["id", "name"],
            field_order=["name", "id"],
            preset_id="507f1f77bcf86cd799439011"
        )

        # Mock loading original preset
        with patch.object(self.manager, 'load_preset', return_value=original_preset), \
             patch.object(self.manager, 'save_preset', return_value="507f1f77bcf86cd799439012"):

            result = self.manager.duplicate_preset("507f1f77bcf86cd799439011", "Duplicated Preset")

            assert result == "507f1f77bcf86cd799439012"

    def test_duplicate_preset_original_not_found(self):
        """Test duplicating non-existent preset"""
        # Mock loading returns None
        with patch.object(self.manager, 'load_preset', return_value=None):
            result = self.manager.duplicate_preset("507f1f77bcf86cd799439011", "New Name")

            assert result is None

    def test_is_duplicate_name(self):
        """Test duplicate name checking"""
        # Mock finding existing preset with same name
        self.mock_collection.find_one.return_value = {"name": "Existing Preset"}

        result = self.manager._is_duplicate_name("Existing Preset", "deals")
        assert result is True

        # Mock no existing preset found
        self.mock_collection.find_one.return_value = None

        result = self.manager._is_duplicate_name("New Preset", "deals")
        assert result is False

    def test_is_duplicate_name_exclude_current(self):
        """Test duplicate name checking excluding current preset"""
        current_preset_id = "507f1f77bcf86cd799439011"

        # Should query with exclusion of current preset ID
        self.mock_collection.find_one.return_value = None

        self.manager._is_duplicate_name("Test Name", "deals", current_preset_id)

        expected_query = {
            "name": "Test Name",
            "entity_type": "deals",
            "_id": {"$ne": ObjectId(current_preset_id)}
        }
        self.mock_collection.find_one.assert_called_once_with(expected_query)

    def test_get_presets_by_entity(self):
        """Test getting presets by entity as dictionaries"""
        mock_presets = [
            ExportPreset(
                name="Preset 1",
                entity_type="deals",
                selected_fields=["id"],
                field_order=["id"]
            )
        ]

        with patch.object(self.manager, 'list_presets', return_value=mock_presets):
            result = self.manager.get_presets_by_entity("deals")

            assert len(result) == 1
            assert isinstance(result[0], dict)
            assert result[0]["name"] == "Preset 1"

    def test_validate_preset_data(self):
        """Test validating preset data without creating preset object"""
        # Valid data
        valid_data = {
            "name": "Test Preset",
            "entity_type": "deals",
            "selected_fields": ["id", "name"],
            "field_order": ["name", "id"]
        }

        errors = self.manager.validate_preset_data(valid_data)
        assert errors == []

        # Invalid data
        invalid_data = {
            "name": "",
            "entity_type": "deals",
            "selected_fields": [],
            "field_order": []
        }

        errors = self.manager.validate_preset_data(invalid_data)
        assert len(errors) > 0

    def test_get_preset_summary(self):
        """Test getting preset summary"""
        preset_id = "507f1f77bcf86cd799439011"
        mock_doc = {
            "_id": ObjectId(preset_id),
            "name": "Test Preset",
            "entity_type": "deals",
            "description": "Test description",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:00:00",
            "selected_fields": ["id", "name", "status"]
        }

        self.mock_collection.find_one.return_value = mock_doc

        result = self.manager.get_preset_summary(preset_id)

        assert result is not None
        assert result["preset_id"] == preset_id
        assert result["name"] == "Test Preset"
        assert result["entity_type"] == "deals"
        assert result["field_count"] == 3

        # Should use projection to limit returned fields
        expected_projection = {
            "name": 1,
            "entity_type": 1,
            "description": 1,
            "created_at": 1,
            "updated_at": 1,
            "selected_fields": 1
        }
        self.mock_collection.find_one.assert_called_once_with(
            {"_id": ObjectId(preset_id)},
            expected_projection
        )

    def test_get_preset_summary_not_found(self):
        """Test getting summary for non-existent preset"""
        preset_id = "507f1f77bcf86cd799439011"
        self.mock_collection.find_one.return_value = None

        result = self.manager.get_preset_summary(preset_id)

        assert result is None


class TestEntityType:
    """Test EntityType enum"""

    def test_entity_type_values(self):
        """Test that EntityType has expected values"""
        assert EntityType.DEALS.value == "deals"
        assert EntityType.CONTACTS.value == "contacts"
        assert EntityType.COMPANIES.value == "companies"
        assert EntityType.USERS.value == "users"
        assert EntityType.PIPELINES.value == "pipelines"

        # Events should be excluded per requirement 9.5
        with pytest.raises(AttributeError):
            EntityType.EVENTS

    def test_entity_type_list_values(self):
        """Test getting list of entity type values"""
        values = [e.value for e in EntityType]
        expected_values = ["deals", "contacts", "companies", "users", "pipelines"]

        assert set(values) == set(expected_values)
        assert "events" not in values  # Should be excluded


if __name__ == '__main__':
    pytest.main([__file__, '-v'])