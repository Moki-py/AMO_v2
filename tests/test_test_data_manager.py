"""
Tests for TestDataManager utility
"""

import pytest
import os
import json
import tempfile
from unittest.mock import Mock, patch

from test_data_manager import (
    TestDataManager,
    TestSpreadsheetInfo,
    MockAmoCRMEntity,
    create_test_dataset_small,
    create_test_dataset_medium,
    create_mock_google_sheets_service
)


class TestTestDataManager:
    """Test cases for TestDataManager"""

    def setup_method(self):
        """Set up test fixtures"""
        self.manager = TestDataManager(use_real_api=False)

    def teardown_method(self):
        """Clean up after tests"""
        self.manager.cleanup_test_resources()

    def test_initialization(self):
        """Test TestDataManager initialization"""
        assert self.manager.use_real_api is False
        assert len(self.manager.created_spreadsheets) == 0
        assert len(self.manager.temp_files) == 0
        assert len(self.manager.mock_data_cache) == 0

    def test_create_mock_spreadsheet(self):
        """Test creating mock spreadsheet"""
        spreadsheet = self.manager.create_test_spreadsheet(
            title="Test Spreadsheet",
            sheet_names=["Deals", "Contacts"]
        )

        assert isinstance(spreadsheet, TestSpreadsheetInfo)
        assert spreadsheet.title == "Test Spreadsheet"
        assert len(spreadsheet.spreadsheet_id) == 44  # Google Sheets ID length
        assert len(spreadsheet.sheets) == 2
        assert spreadsheet.sheets[0]['properties']['title'] == "Deals"
        assert spreadsheet.sheets[1]['properties']['title'] == "Contacts"
        assert spreadsheet.cleanup_required is False  # Mock doesn't need cleanup

    def test_generate_mock_deals_data(self):
        """Test generating mock deals data"""
        deals = self.manager.generate_mock_amocrm_data('deals', 5)

        assert len(deals) == 5
        for deal in deals:
            assert 'id' in deal
            assert 'name' in deal
            assert 'price' in deal
            assert 'status_id' in deal
            assert 'pipeline_id' in deal
            assert 'created_at' in deal
            assert 'updated_at' in deal
            assert deal['name'].startswith('Deal ')

    def test_generate_mock_contacts_data(self):
        """Test generating mock contacts data"""
        contacts = self.manager.generate_mock_amocrm_data('contacts', 3)

        assert len(contacts) == 3
        for contact in contacts:
            assert 'id' in contact
            assert 'name' in contact
            assert 'first_name' in contact
            assert 'last_name' in contact
            assert 'responsible_user_id' in contact
            assert contact['name'] == f"{contact['first_name']} {contact['last_name']}"

    def test_generate_mock_companies_data(self):
        """Test generating mock companies data"""
        companies = self.manager.generate_mock_amocrm_data('companies', 2)

        assert len(companies) == 2
        for company in companies:
            assert 'id' in company
            assert 'name' in company
            assert 'responsible_user_id' in company

    def test_generate_mock_users_data(self):
        """Test generating mock users data"""
        users = self.manager.generate_mock_amocrm_data('users', 3)

        assert len(users) == 3
        for user in users:
            assert 'id' in user
            assert 'name' in user
            assert 'email' in user
            assert 'is_active' in user
            assert 'is_admin' in user
            assert user['email'].endswith('@company.com')

    def test_generate_mock_pipelines_data(self):
        """Test generating mock pipelines data"""
        pipelines = self.manager.generate_mock_amocrm_data('pipelines', 2)

        assert len(pipelines) == 2
        for pipeline in pipelines:
            assert 'id' in pipeline
            assert 'name' in pipeline
            assert 'sort' in pipeline
            assert 'is_main' in pipeline

    def test_generate_custom_fields(self):
        """Test generating custom fields"""
        deals = self.manager.generate_mock_amocrm_data(
            'deals', 1, include_custom_fields=True, custom_field_count=3
        )

        deal = deals[0]
        assert 'custom_fields_values' in deal
        assert len(deal['custom_fields_values']) == 3

        for custom_field in deal['custom_fields_values']:
            assert 'field_id' in custom_field
            assert 'field_name' in custom_field
            assert 'field_type' in custom_field
            assert 'values' in custom_field
            assert len(custom_field['values']) > 0

    def test_generate_without_custom_fields(self):
        """Test generating data without custom fields"""
        deals = self.manager.generate_mock_amocrm_data(
            'deals', 2, include_custom_fields=False
        )

        for deal in deals:
            assert len(deal.get('custom_fields_values', [])) == 0

    def test_data_caching(self):
        """Test that generated data is cached"""
        # Generate data twice with same parameters
        deals1 = self.manager.generate_mock_amocrm_data('deals', 3)
        deals2 = self.manager.generate_mock_amocrm_data('deals', 3)

        # Should be identical due to caching
        assert deals1 == deals2
        assert len(self.manager.mock_data_cache) == 1

    def test_create_test_dataset(self):
        """Test creating complete test dataset"""
        dataset = self.manager.create_test_dataset({
            'deals': 5,
            'contacts': 3,
            'companies': 2
        })

        assert 'deals' in dataset
        assert 'contacts' in dataset
        assert 'companies' in dataset
        assert len(dataset['deals']) == 5
        assert len(dataset['contacts']) == 3
        assert len(dataset['companies']) == 2

    def test_create_test_configuration_files(self):
        """Test creating temporary configuration files"""
        config_files = self.manager.create_test_configuration_files()

        assert 'credentials' in config_files
        assert 'token' in config_files

        # Verify files exist and contain valid JSON
        credentials_path = config_files['credentials']
        token_path = config_files['token']

        assert os.path.exists(credentials_path)
        assert os.path.exists(token_path)

        with open(credentials_path, 'r') as f:
            credentials_data = json.load(f)
            assert 'installed' in credentials_data
            assert 'client_id' in credentials_data['installed']

        with open(token_path, 'r') as f:
            token_data = json.load(f)
            assert 'token' in token_data
            assert 'refresh_token' in token_data

        # Files should be tracked for cleanup
        assert credentials_path in self.manager.temp_files
        assert token_path in self.manager.temp_files

    def test_create_mock_google_sheets_service(self):
        """Test creating mock Google Sheets service"""
        mock_service = self.manager.create_mock_google_sheets_service()

        # Test spreadsheets().get() mock
        get_response = mock_service.spreadsheets().get(spreadsheetId="test_id").execute()
        assert 'spreadsheetId' in get_response
        assert 'properties' in get_response
        assert 'sheets' in get_response

        # Test spreadsheets().values().batchUpdate() mock
        batch_update_response = mock_service.spreadsheets().values().batchUpdate().execute()
        assert 'totalUpdatedCells' in batch_update_response
        assert 'totalUpdatedRows' in batch_update_response

        # Test spreadsheets().batchUpdate() mock
        sheet_update_response = mock_service.spreadsheets().batchUpdate().execute()
        assert 'replies' in sheet_update_response

    def test_get_test_spreadsheet_ids(self):
        """Test getting test spreadsheet IDs"""
        spreadsheet_ids = self.manager.get_test_spreadsheet_ids()

        assert 'deals' in spreadsheet_ids
        assert 'contacts' in spreadsheet_ids
        assert 'companies' in spreadsheet_ids
        assert 'events' in spreadsheet_ids

        # All IDs should be valid format (44 characters)
        for entity_type, spreadsheet_id in spreadsheet_ids.items():
            assert len(spreadsheet_id) == 44
            assert isinstance(spreadsheet_id, str)

    def test_create_test_export_presets(self):
        """Test creating test export presets"""
        presets = self.manager.create_test_export_presets()

        assert 'deals' in presets
        assert 'contacts' in presets
        assert 'companies' in presets

        deals_preset = presets['deals']
        assert deals_preset['name'] == 'Test Deals Preset'
        assert deals_preset['entity_type'] == 'deals'
        assert 'selected_fields' in deals_preset
        assert 'field_order' in deals_preset
        assert 'custom_field_mappings' in deals_preset
        assert len(deals_preset['selected_fields']) > 0

    def test_simulate_api_errors(self):
        """Test simulating various API errors"""
        # Test rate limit error
        rate_limit_error = self.manager.simulate_api_errors('rate_limit')
        assert hasattr(rate_limit_error, 'resp')
        assert rate_limit_error.resp.status == 429

        # Test permission error
        permission_error = self.manager.simulate_api_errors('permission')
        assert hasattr(permission_error, 'resp')
        assert permission_error.resp.status == 403

        # Test not found error
        not_found_error = self.manager.simulate_api_errors('not_found')
        assert hasattr(not_found_error, 'resp')
        assert not_found_error.resp.status == 404

        # Test network error
        network_error = self.manager.simulate_api_errors('network')
        assert isinstance(network_error, ConnectionError)

        # Test timeout error
        timeout_error = self.manager.simulate_api_errors('timeout')
        assert 'timeout' in str(type(timeout_error)).lower()

    def test_cleanup_test_resources(self):
        """Test cleaning up test resources"""
        # Create some resources
        config_files = self.manager.create_test_configuration_files()
        spreadsheet = self.manager.create_test_spreadsheet()
        dataset = self.manager.create_test_dataset({'deals': 2})

        # Verify resources exist
        assert len(self.manager.temp_files) == 2
        assert len(self.manager.created_spreadsheets) == 1
        assert len(self.manager.mock_data_cache) > 0

        # Cleanup
        self.manager.cleanup_test_resources()

        # Verify cleanup
        assert len(self.manager.temp_files) == 0
        assert len(self.manager.created_spreadsheets) == 0
        assert len(self.manager.mock_data_cache) == 0

        # Verify temp files are deleted
        for file_path in config_files.values():
            assert not os.path.exists(file_path)

    def test_context_manager(self):
        """Test using TestDataManager as context manager"""
        temp_files = []

        with TestDataManager() as manager:
            config_files = manager.create_test_configuration_files()
            temp_files.extend(config_files.values())

            # Verify files exist during context
            for file_path in temp_files:
                assert os.path.exists(file_path)

        # Verify files are cleaned up after context
        for file_path in temp_files:
            assert not os.path.exists(file_path)

    def test_realistic_spreadsheet_id_format(self):
        """Test that generated spreadsheet IDs have realistic format"""
        spreadsheet_id = self.manager._generate_realistic_spreadsheet_id()

        assert len(spreadsheet_id) == 44
        assert all(c.isalnum() or c in '-_' for c in spreadsheet_id)

    def test_events_data_generation(self):
        """Test generating events data (even though excluded from export)"""
        events = self.manager.generate_mock_amocrm_data('events', 3)

        assert len(events) == 3
        for event in events:
            assert 'id' in event
            assert 'type' in event
            assert 'entity_id' in event
            assert 'entity_type' in event
            assert 'created_by' in event
            assert event['type'] in ['call', 'meeting', 'email', 'task', 'note']


class TestConvenienceFunctions:
    """Test convenience functions"""

    def test_create_test_dataset_small(self):
        """Test small dataset creation"""
        dataset = create_test_dataset_small()

        assert 'deals' in dataset
        assert 'contacts' in dataset
        assert 'companies' in dataset
        assert 'users' in dataset
        assert 'pipelines' in dataset

        # Verify small sizes
        assert len(dataset['deals']) == 5
        assert len(dataset['contacts']) == 3
        assert len(dataset['companies']) == 2
        assert len(dataset['users']) == 2
        assert len(dataset['pipelines']) == 1

    def test_create_test_dataset_medium(self):
        """Test medium dataset creation"""
        dataset = create_test_dataset_medium()

        # Verify medium sizes
        assert len(dataset['deals']) == 50
        assert len(dataset['contacts']) == 30
        assert len(dataset['companies']) == 15
        assert len(dataset['users']) == 5
        assert len(dataset['pipelines']) == 3

    def test_create_mock_google_sheets_service_function(self):
        """Test mock service creation function"""
        mock_service = create_mock_google_sheets_service()

        # Should be a Mock object with expected methods
        assert hasattr(mock_service, 'spreadsheets')
        assert callable(mock_service.spreadsheets)

        # Test basic functionality
        response = mock_service.spreadsheets().get(spreadsheetId="test").execute()
        assert 'properties' in response


class TestMockAmoCRMEntity:
    """Test MockAmoCRMEntity dataclass"""

    def test_mock_entity_creation(self):
        """Test creating MockAmoCRMEntity"""
        entity = MockAmoCRMEntity(
            id=1,
            name="Test Entity",
            entity_type="deals",
            created_at=1640995200,
            updated_at=1640995300,
            custom_fields_values=[],
            additional_fields={"test": "value"}
        )

        assert entity.id == 1
        assert entity.name == "Test Entity"
        assert entity.entity_type == "deals"
        assert entity.created_at == 1640995200
        assert entity.updated_at == 1640995300
        assert entity.custom_fields_values == []
        assert entity.additional_fields == {"test": "value"}


class TestTestSpreadsheetInfo:
    """Test TestSpreadsheetInfo dataclass"""

    def test_spreadsheet_info_creation(self):
        """Test creating TestSpreadsheetInfo"""
        from datetime import datetime

        created_time = datetime.now()

        info = TestSpreadsheetInfo(
            spreadsheet_id="test_id_123456789012345678901234567890123456",
            title="Test Spreadsheet",
            url="https://docs.google.com/spreadsheets/d/test_id_123456789012345678901234567890123456",
            sheets=[{"properties": {"title": "Sheet1"}}],
            created_at=created_time,
            cleanup_required=True
        )

        assert info.spreadsheet_id == "test_id_123456789012345678901234567890123456"
        assert info.title == "Test Spreadsheet"
        assert info.url.startswith("https://docs.google.com/spreadsheets/d/")
        assert len(info.sheets) == 1
        assert info.created_at == created_time
        assert info.cleanup_required is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])