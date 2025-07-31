"""
Tests for DataFilterEngine

Tests the filtering and organization capabilities for export data.
"""

import pytest
from datetime import datetime
from amocrm_exporter.utils.data_filter_engine import DataFilterEngine, DateFilter


class TestDataFilterEngine:
    """Test cases for DataFilterEngine"""

    def setup_method(self):
        """Set up test fixtures"""
        self.filter_engine = DataFilterEngine()

    def test_events_excluded_from_filter_entities_by_type(self):
        """Test that events are excluded from export operations"""
        # Arrange
        entities_data = {
            "deals": [{"id": 1, "name": "Deal 1"}],
            "contacts": [{"id": 1, "name": "Contact 1"}],
            "companies": [{"id": 1, "name": "Company 1"}],
            "events": [{"id": 1, "type": "call"}],  # Should be excluded
            "users": [{"id": 1, "name": "User 1"}],
            "pipelines": [{"id": 1, "name": "Pipeline 1"}]
        }

        # Act
        filtered_data = self.filter_engine.filter_entities_by_type(entities_data)

        # Assert
        assert "events" not in filtered_data, "Events should be excluded from export operations"
        assert "deals" in filtered_data, "Deals should be included"
        assert "contacts" in filtered_data, "Contacts should be included"
        assert "companies" in filtered_data, "Companies should be included (reference data)"
        assert "users" in filtered_data, "Users should be included (reference data)"
        assert "pipelines" in filtered_data, "Pipelines should be included (reference data)"

    def test_reference_entities_always_included(self):
        """Test that reference entities (companies, users, pipelines) are always included"""
        # Arrange
        entities_data = {
            "companies": [{"id": 1, "name": "Company 1"}],
            "users": [{"id": 1, "name": "User 1"}],
            "pipelines": [{"id": 1, "name": "Pipeline 1"}]
        }

        # Act
        filtered_data = self.filter_engine.filter_entities_by_type(entities_data)

        # Assert
        assert len(filtered_data["companies"]) == 1, "Companies should be included"
        assert len(filtered_data["users"]) == 1, "Users should be included"
        assert len(filtered_data["pipelines"]) == 1, "Pipelines should be included"

    def test_comprehensive_filter_excludes_events(self):
        """Test that comprehensive filtering excludes events"""
        # Arrange
        entities_data = {
            "deals": [{"id": 1, "name": "Deal 1", "updated_at": 1640995200}],  # 2022-01-01
            "contacts": [{"id": 1, "name": "Contact 1"}],
            "companies": [{"id": 1, "name": "Company 1"}],
            "events": [{"id": 1, "type": "call"}],  # Should be excluded
            "users": [{"id": 1, "name": "User 1"}],
            "pipelines": [{"id": 1, "name": "Pipeline 1"}]
        }

        # Act
        result = self.filter_engine.apply_comprehensive_filter(entities_data)

        # Assert
        assert "events" not in result.filtered_data, "Events should be excluded from comprehensive filter"
        assert result.filter_stats.get("events", 0) == 0, "Events count should be 0 in stats"

    def test_date_filter_functionality(self):
        """Test that date filtering works correctly for deals"""
        # Arrange
        deals = [
            {"id": 1, "name": "Deal 1", "updated_at": 1640995200},  # 2022-01-01
            {"id": 2, "name": "Deal 2", "updated_at": 1672531200},  # 2023-01-01
            {"id": 3, "name": "Deal 3", "updated_at": 1704067200},  # 2024-01-01
        ]

        date_filter = DateFilter(
            date_from="2022-06-01T00:00:00Z",
            date_to="2023-06-01T00:00:00Z"
        )

        # Act
        filtered_deals = self.filter_engine.apply_date_filter(deals, date_filter)

        # Assert
        assert len(filtered_deals) == 1, "Only one deal should match the date filter"
        assert filtered_deals[0]["id"] == 2, "Deal 2 should be the only match"

    def test_contact_filtering_based_on_deals(self):
        """Test that contacts are filtered based on deal relationships"""
        # Arrange
        deals = [
            {"id": 1, "name": "Deal 1", "contact_id": 1},
            {"id": 2, "name": "Deal 2", "contacts": [{"id": 2}]}
        ]

        all_contacts = [
            {"id": 1, "name": "Contact 1"},
            {"id": 2, "name": "Contact 2"},
            {"id": 3, "name": "Contact 3"}  # Should be filtered out
        ]

        # Act
        filtered_contacts = self.filter_engine.filter_related_contacts(deals, all_contacts)

        # Assert
        assert len(filtered_contacts) == 2, "Only contacts related to deals should be included"
        contact_ids = [c["id"] for c in filtered_contacts]
        assert 1 in contact_ids, "Contact 1 should be included (referenced in deal 1)"
        assert 2 in contact_ids, "Contact 2 should be included (referenced in deal 2)"
        assert 3 not in contact_ids, "Contact 3 should be excluded (not referenced)"

    def test_sheet_organization_excludes_events(self):
        """Test that sheet organization excludes events"""
        # Arrange
        filtered_data = {
            "deals": [{"id": 1, "name": "Deal 1"}],
            "contacts": [{"id": 1, "name": "Contact 1"}],
            "companies": [{"id": 1, "name": "Company 1"}],
            "users": [{"id": 1, "name": "User 1"}],
            "pipelines": [{"id": 1, "name": "Pipeline 1"}]
        }

        # Act
        sheets = self.filter_engine.organize_sheets(filtered_data)

        # Assert
        # Check that no sheet contains events data
        for sheet_name, sheet_data in sheets.items():
            assert "events" not in sheet_data.entity_types, f"Sheet {sheet_name} should not contain events"

        # Check that we have the expected sheets
        sheet_names = list(sheets.keys())
        deals_sheets = [name for name in sheet_names if "Deals" in name]
        contacts_sheets = [name for name in sheet_names if "Contacts" in name]
        reference_sheets = [name for name in sheet_names if "Reference" in name]

        assert len(deals_sheets) == 1, "Should have one deals sheet"
        assert len(contacts_sheets) == 1, "Should have one contacts sheet"
        assert len(reference_sheets) == 1, "Should have one reference sheet"

    def test_backward_compatibility_with_existing_configurations(self):
        """Test that the system maintains backward compatibility"""
        # This test ensures that existing configurations that might reference events
        # don't break the system, they just get filtered out

        # Arrange
        entities_data = {
            "deals": [{"id": 1, "name": "Deal 1"}],
            "contacts": [{"id": 1, "name": "Contact 1"}],
            "companies": [{"id": 1, "name": "Company 1"}],
            "events": [{"id": 1, "type": "call"}],  # Legacy data
            "users": [{"id": 1, "name": "User 1"}],
            "pipelines": [{"id": 1, "name": "Pipeline 1"}]
        }

        # Act - should not raise any exceptions
        result = self.filter_engine.apply_comprehensive_filter(entities_data)
        sheets = self.filter_engine.organize_sheets(result.filtered_data)

        # Assert
        assert "events" not in result.filtered_data, "Events should be filtered out"
        assert len(sheets) > 0, "Should still create sheets for other entities"

        # Verify that other entities are processed normally
        assert "deals" in result.filtered_data, "Deals should still be processed"
        assert "companies" in result.filtered_data, "Companies should still be processed"