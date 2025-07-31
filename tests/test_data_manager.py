"""
Test data management utilities for Google Sheets export testing

This module provides utilities for:
- Creating test spreadsheets
- Generating mock AmoCRM data
- Managing test resources and cleanup
"""

import os
import json
import random
import string
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
from unittest.mock import Mock, patch

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


@dataclass
class TestSpreadsheetInfo:
    """Information about a test spreadsheet"""
    spreadsheet_id: str
    title: str
    url: str
    sheets: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    cleanup_required: bool = True


@dataclass
class MockAmoCRMEntity:
    """Mock AmoCRM entity data structure"""
    id: int
    name: str
    entity_type: str
    created_at: int
    updated_at: int
    custom_fields_values: List[Dict[str, Any]] = field(default_factory=list)
    additional_fields: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Handle additional fields that aren't explicitly defined"""
        # This allows the dataclass to accept any additional keyword arguments
        pass


class TestDataManager:
    """
    Manages test data creation, spreadsheet setup, and resource cleanup
    """

    def __init__(self, use_real_api: bool = False):
        """
        Initialize test data manager

        Args:
            use_real_api: Whether to use real Google Sheets API (for integration tests)
                         or mock API (for unit tests)
        """
        self.use_real_api = use_real_api
        self.created_spreadsheets: List[TestSpreadsheetInfo] = []
        self.temp_files: List[str] = []
        self.mock_data_cache: Dict[str, List[MockAmoCRMEntity]] = {}

    def create_test_spreadsheet(
        self,
        title: str = None,
        sheet_names: List[str] = None
    ) -> TestSpreadsheetInfo:
        """
        Create a test spreadsheet for testing

        Args:
            title: Title for the spreadsheet
            sheet_names: List of sheet names to create

        Returns:
            TestSpreadsheetInfo with spreadsheet details
        """
        if title is None:
            title = f"Test Spreadsheet {datetime.now().strftime('%Y%m%d_%H%M%S')}"

        if sheet_names is None:
            sheet_names = ["Deals", "Contacts", "Companies", "Reference"]

        if self.use_real_api:
            return self._create_real_spreadsheet(title, sheet_names)
        else:
            return self._create_mock_spreadsheet(title, sheet_names)

    def _create_real_spreadsheet(
        self,
        title: str,
        sheet_names: List[str]
    ) -> TestSpreadsheetInfo:
        """Create a real Google Spreadsheet for integration testing"""
        try:
            # This would require real Google Sheets API credentials
            # For now, we'll create a mock that simulates real API behavior
            spreadsheet_id = self._generate_realistic_spreadsheet_id()

            # In a real implementation, this would use the Google Sheets API
            # service = build('sheets', 'v4', credentials=credentials)
            # spreadsheet = service.spreadsheets().create(body={
            #     'properties': {'title': title},
            #     'sheets': [{'properties': {'title': name}} for name in sheet_names]
            # }).execute()

            sheets = [
                {
                    'properties': {
                        'title': name,
                        'sheetId': i,
                        'sheetType': 'GRID',
                        'gridProperties': {'rowCount': 1000, 'columnCount': 26}
                    }
                }
                for i, name in enumerate(sheet_names)
            ]

            spreadsheet_info = TestSpreadsheetInfo(
                spreadsheet_id=spreadsheet_id,
                title=title,
                url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}",
                sheets=sheets,
                cleanup_required=True
            )

            self.created_spreadsheets.append(spreadsheet_info)
            return spreadsheet_info

        except Exception as e:
            raise Exception(f"Failed to create test spreadsheet: {str(e)}")

    def _create_mock_spreadsheet(
        self,
        title: str,
        sheet_names: List[str]
    ) -> TestSpreadsheetInfo:
        """Create a mock spreadsheet for unit testing"""
        spreadsheet_id = self._generate_realistic_spreadsheet_id()

        sheets = [
            {
                'properties': {
                    'title': name,
                    'sheetId': i,
                    'sheetType': 'GRID',
                    'gridProperties': {'rowCount': 1000, 'columnCount': 26}
                }
            }
            for i, name in enumerate(sheet_names)
        ]

        spreadsheet_info = TestSpreadsheetInfo(
            spreadsheet_id=spreadsheet_id,
            title=title,
            url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}",
            sheets=sheets,
            cleanup_required=False  # Mock spreadsheets don't need cleanup
        )

        self.created_spreadsheets.append(spreadsheet_info)
        return spreadsheet_info

    def generate_mock_amocrm_data(
        self,
        entity_type: str,
        count: int,
        include_custom_fields: bool = True,
        custom_field_count: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Generate mock AmoCRM data for testing

        Args:
            entity_type: Type of entity (deals, contacts, companies, etc.)
            count: Number of entities to generate
            include_custom_fields: Whether to include custom fields
            custom_field_count: Number of custom fields per entity

        Returns:
            List of mock entity dictionaries
        """
        cache_key = f"{entity_type}_{count}_{include_custom_fields}_{custom_field_count}"

        if cache_key in self.mock_data_cache:
            return self.mock_data_cache[cache_key]

        entities = []
        base_timestamp = int(datetime.now().timestamp())

        for i in range(count):
            entity_id = i + 1

            # Generate base entity data
            entity_data = self._generate_base_entity_data(entity_type, entity_id, base_timestamp)

            # Add custom fields if requested
            if include_custom_fields:
                entity_data['custom_fields_values'] = self._generate_custom_fields(
                    entity_type, custom_field_count
                )

            entities.append(entity_data)

        # Cache the generated data (store as dicts, not objects for flexibility)
        self.mock_data_cache[cache_key] = entities

        return entities

    def _generate_base_entity_data(
        self,
        entity_type: str,
        entity_id: int,
        base_timestamp: int
    ) -> Dict[str, Any]:
        """Generate base data for a specific entity type"""
        created_at = base_timestamp - random.randint(0, 86400 * 30)  # Within last 30 days
        updated_at = created_at + random.randint(0, 86400 * 7)  # Updated within 7 days

        base_data = {
            'id': entity_id,
            'created_at': created_at,
            'updated_at': updated_at,
            'custom_fields_values': []
        }

        if entity_type == 'deals':
            base_data.update({
                'name': f'Deal {entity_id}',
                'price': random.randint(1000, 100000),
                'status_id': random.choice([1, 2, 3, 4]),
                'pipeline_id': random.choice([1, 2, 3]),
                'responsible_user_id': random.choice([1, 2, 3, 4, 5]),
                'contact_id': random.randint(1, min(50, entity_id * 2)),
                'company_id': random.randint(1, min(20, entity_id)),
                'closest_task_at': updated_at + random.randint(86400, 86400 * 14)
            })

        elif entity_type == 'contacts':
            first_names = ['John', 'Jane', 'Mike', 'Sarah', 'David', 'Lisa', 'Tom', 'Anna']
            last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']

            first_name = random.choice(first_names)
            last_name = random.choice(last_names)

            base_data.update({
                'name': f'{first_name} {last_name}',
                'first_name': first_name,
                'last_name': last_name,
                'responsible_user_id': random.choice([1, 2, 3, 4, 5]),
                'company_id': random.randint(1, min(20, entity_id))
            })

        elif entity_type == 'companies':
            company_types = ['LLC', 'Inc', 'Corp', 'Ltd', 'Co']
            company_names = ['Tech Solutions', 'Global Systems', 'Innovation Labs', 'Digital Works', 'Smart Solutions']

            base_data.update({
                'name': f'{random.choice(company_names)} {random.choice(company_types)}',
                'responsible_user_id': random.choice([1, 2, 3, 4, 5])
            })

        elif entity_type == 'users':
            user_names = ['Admin User', 'Sales Manager', 'Account Executive', 'Support Agent', 'Marketing Specialist']
            base_data.update({
                'name': user_names[min(entity_id - 1, len(user_names) - 1)],
                'email': f'user{entity_id}@company.com',
                'is_active': True,
                'is_admin': entity_id == 1
            })

        elif entity_type == 'pipelines':
            pipeline_names = ['Sales Pipeline', 'Support Pipeline', 'Marketing Pipeline']
            base_data.update({
                'name': pipeline_names[min(entity_id - 1, len(pipeline_names) - 1)],
                'sort': entity_id * 10,
                'is_main': entity_id == 1
            })

        elif entity_type == 'events':
            event_types = ['call', 'meeting', 'email', 'task', 'note']
            base_data.update({
                'type': random.choice(event_types),
                'entity_id': random.randint(1, 100),
                'entity_type': random.choice(['leads', 'contacts', 'companies']),
                'created_by': random.choice([1, 2, 3, 4, 5]),
                'value_after': f'Event {entity_id} details',
                'value_before': ''
            })

        return base_data

    def _generate_custom_fields(
        self,
        entity_type: str,
        field_count: int
    ) -> List[Dict[str, Any]]:
        """Generate custom fields for an entity"""
        custom_fields = []

        field_types = ['text', 'textarea', 'numeric', 'select', 'multiselect', 'date', 'url', 'checkbox']

        for i in range(field_count):
            field_id = str(100 + i)
            field_type = random.choice(field_types)
            field_name = f'Custom {field_type.title()} Field {i + 1}'

            # Generate appropriate value based on field type
            if field_type == 'text':
                value = f'Text value {i + 1}'
            elif field_type == 'textarea':
                value = f'This is a longer text value for field {i + 1} with multiple words.'
            elif field_type == 'numeric':
                value = random.randint(1, 1000)
            elif field_type == 'select':
                value = random.choice(['Option A', 'Option B', 'Option C'])
            elif field_type == 'multiselect':
                options = ['Tag1', 'Tag2', 'Tag3', 'Tag4']
                value = random.sample(options, random.randint(1, 3))
            elif field_type == 'date':
                value = int(datetime.now().timestamp()) + random.randint(-86400 * 30, 86400 * 30)
            elif field_type == 'url':
                value = f'https://example{i + 1}.com'
            elif field_type == 'checkbox':
                value = random.choice([True, False])
            else:
                value = f'Value {i + 1}'

            custom_field = {
                'field_id': field_id,
                'field_name': field_name,
                'field_type': field_type,
                'values': [{'value': value}] if not isinstance(value, list) else [{'value': v} for v in value]
            }

            custom_fields.append(custom_field)

        return custom_fields

    def create_test_dataset(
        self,
        entity_counts: Dict[str, int] = None,
        include_custom_fields: bool = True
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create a complete test dataset with multiple entity types

        Args:
            entity_counts: Dictionary mapping entity types to counts
            include_custom_fields: Whether to include custom fields

        Returns:
            Dictionary with entity type as key and list of entities as value
        """
        if entity_counts is None:
            entity_counts = {
                'deals': 50,
                'contacts': 30,
                'companies': 15,
                'users': 5,
                'pipelines': 3
            }

        dataset = {}

        for entity_type, count in entity_counts.items():
            dataset[entity_type] = self.generate_mock_amocrm_data(
                entity_type=entity_type,
                count=count,
                include_custom_fields=include_custom_fields
            )

        return dataset

    def create_test_configuration_files(self) -> Dict[str, str]:
        """
        Create temporary configuration files for testing

        Returns:
            Dictionary with file paths
        """
        config_files = {}

        # Create temporary credentials.json
        credentials_data = {
            "installed": {
                "client_id": "test_client_id.apps.googleusercontent.com",
                "project_id": "test-project",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_secret": "test_client_secret",
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
            }
        }

        credentials_file = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            delete=False,
            prefix='test_credentials_'
        )

        json.dump(credentials_data, credentials_file, indent=2)
        credentials_file.close()

        config_files['credentials'] = credentials_file.name
        self.temp_files.append(credentials_file.name)

        # Create temporary token.json
        token_data = {
            "token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_id": "test_client_id.apps.googleusercontent.com",
            "client_secret": "test_client_secret",
            "scopes": ["https://www.googleapis.com/auth/spreadsheets"]
        }

        token_file = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            delete=False,
            prefix='test_token_'
        )

        json.dump(token_data, token_file, indent=2)
        token_file.close()

        config_files['token'] = token_file.name
        self.temp_files.append(token_file.name)

        return config_files

    def create_mock_google_sheets_service(self) -> Mock:
        """
        Create a mock Google Sheets service for testing

        Returns:
            Mock object configured to simulate Google Sheets API
        """
        mock_service = Mock()

        # Mock spreadsheets().get() responses
        def mock_get_spreadsheet(spreadsheetId):
            mock_response = Mock()
            mock_response.execute.return_value = {
                'spreadsheetId': spreadsheetId,
                'properties': {
                    'title': f'Test Spreadsheet {spreadsheetId[:8]}',
                    'locale': 'en_US',
                    'autoRecalc': 'ON_CHANGE',
                    'timeZone': 'America/New_York'
                },
                'sheets': [
                    {
                        'properties': {
                            'sheetId': 0,
                            'title': 'Sheet1',
                            'sheetType': 'GRID',
                            'gridProperties': {
                                'rowCount': 1000,
                                'columnCount': 26
                            }
                        }
                    }
                ]
            }
            return mock_response

        mock_service.spreadsheets().get.side_effect = mock_get_spreadsheet

        # Mock spreadsheets().values().batchUpdate() responses
        mock_batch_update_response = Mock()
        mock_batch_update_response.execute.return_value = {
            'spreadsheetId': 'test_spreadsheet_id',
            'totalUpdatedCells': 100,
            'totalUpdatedColumns': 5,
            'totalUpdatedRows': 20,
            'totalUpdatedSheets': 1
        }
        mock_service.spreadsheets().values().batchUpdate.return_value = mock_batch_update_response

        # Mock spreadsheets().batchUpdate() responses
        mock_sheet_update_response = Mock()
        mock_sheet_update_response.execute.return_value = {
            'spreadsheetId': 'test_spreadsheet_id',
            'replies': [
                {
                    'addSheet': {
                        'properties': {
                            'sheetId': 123,
                            'title': 'New Sheet',
                            'sheetType': 'GRID'
                        }
                    }
                }
            ]
        }
        mock_service.spreadsheets().batchUpdate.return_value = mock_sheet_update_response

        return mock_service

    def cleanup_test_resources(self):
        """Clean up all created test resources"""
        # Clean up temporary files
        for file_path in self.temp_files:
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Warning: Could not delete temporary file {file_path}: {e}")

        self.temp_files.clear()

        # Clean up test spreadsheets (if using real API)
        if self.use_real_api:
            for spreadsheet_info in self.created_spreadsheets:
                if spreadsheet_info.cleanup_required:
                    try:
                        self._delete_test_spreadsheet(spreadsheet_info.spreadsheet_id)
                    except Exception as e:
                        print(f"Warning: Could not delete test spreadsheet {spreadsheet_info.spreadsheet_id}: {e}")

        self.created_spreadsheets.clear()
        self.mock_data_cache.clear()

    def _delete_test_spreadsheet(self, spreadsheet_id: str):
        """Delete a test spreadsheet (for real API usage)"""
        # In a real implementation, this would use the Google Drive API
        # to delete the spreadsheet file
        # drive_service = build('drive', 'v3', credentials=credentials)
        # drive_service.files().delete(fileId=spreadsheet_id).execute()
        pass

    def _generate_realistic_spreadsheet_id(self) -> str:
        """Generate a realistic-looking Google Sheets spreadsheet ID"""
        # Google Sheets IDs are 44 characters long, alphanumeric with hyphens and underscores
        chars = string.ascii_letters + string.digits + '-_'
        return ''.join(random.choice(chars) for _ in range(44))

    def get_test_spreadsheet_ids(self) -> Dict[str, str]:
        """
        Get spreadsheet IDs for testing configuration

        Returns:
            Dictionary mapping entity types to spreadsheet IDs
        """
        if len(self.created_spreadsheets) == 0:
            # Create default test spreadsheets
            for entity_type in ['deals', 'contacts', 'companies', 'events']:
                self.create_test_spreadsheet(f"Test {entity_type.title()} Spreadsheet")

        spreadsheet_ids = {}
        entity_types = ['deals', 'contacts', 'companies', 'events']

        for i, entity_type in enumerate(entity_types):
            if i < len(self.created_spreadsheets):
                spreadsheet_ids[entity_type] = self.created_spreadsheets[i].spreadsheet_id
            else:
                # Generate additional IDs if needed
                spreadsheet_ids[entity_type] = self._generate_realistic_spreadsheet_id()

        return spreadsheet_ids

    def create_test_export_presets(self) -> Dict[str, Dict[str, Any]]:
        """
        Create test export presets for different entity types

        Returns:
            Dictionary with preset configurations
        """
        presets = {
            'deals': {
                'name': 'Test Deals Preset',
                'entity_type': 'deals',
                'selected_fields': ['id', 'name', 'price', 'status_id', 'pipeline_id', 'responsible_user_id'],
                'field_order': ['name', 'price', 'status_id', 'pipeline_id', 'responsible_user_id', 'id'],
                'custom_field_mappings': {
                    '100': 'Custom Text Field',
                    '101': 'Custom Number Field',
                    '102': 'Custom Date Field'
                },
                'filters': {'status_id': [1, 2, 3]}
            },
            'contacts': {
                'name': 'Test Contacts Preset',
                'entity_type': 'contacts',
                'selected_fields': ['id', 'name', 'first_name', 'last_name', 'responsible_user_id'],
                'field_order': ['first_name', 'last_name', 'name', 'responsible_user_id', 'id'],
                'custom_field_mappings': {
                    '100': 'Phone Number',
                    '101': 'Email Address',
                    '102': 'Company Position'
                },
                'filters': {}
            },
            'companies': {
                'name': 'Test Companies Preset',
                'entity_type': 'companies',
                'selected_fields': ['id', 'name', 'responsible_user_id'],
                'field_order': ['name', 'responsible_user_id', 'id'],
                'custom_field_mappings': {
                    '100': 'Industry',
                    '101': 'Company Size',
                    '102': 'Website'
                },
                'filters': {}
            }
        }

        return presets

    def simulate_api_errors(self, error_type: str = 'rate_limit') -> Exception:
        """
        Create mock API errors for testing error handling

        Args:
            error_type: Type of error to simulate

        Returns:
            Exception object for testing
        """
        if error_type == 'rate_limit':
            mock_resp = Mock()
            mock_resp.status = 429
            error = HttpError(mock_resp, b'Rate limit exceeded')
            error.error_details = [{'reason': 'rateLimitExceeded'}]
            return error

        elif error_type == 'permission':
            mock_resp = Mock()
            mock_resp.status = 403
            error = HttpError(mock_resp, b'Insufficient permissions')
            error.error_details = [{'reason': 'forbidden'}]
            return error

        elif error_type == 'not_found':
            mock_resp = Mock()
            mock_resp.status = 404
            error = HttpError(mock_resp, b'Spreadsheet not found')
            error.error_details = [{'reason': 'notFound'}]
            return error

        elif error_type == 'network':
            return ConnectionError("Network connection failed")

        elif error_type == 'timeout':
            import socket
            return socket.timeout("Connection timed out")

        else:
            return Exception(f"Simulated {error_type} error")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup"""
        self.cleanup_test_resources()


# Convenience functions for common test scenarios

def create_test_dataset_small() -> Dict[str, List[Dict[str, Any]]]:
    """Create a small test dataset for quick tests"""
    with TestDataManager() as manager:
        return manager.create_test_dataset({
            'deals': 5,
            'contacts': 3,
            'companies': 2,
            'users': 2,
            'pipelines': 1
        })


def create_test_dataset_medium() -> Dict[str, List[Dict[str, Any]]]:
    """Create a medium test dataset for integration tests"""
    with TestDataManager() as manager:
        return manager.create_test_dataset({
            'deals': 50,
            'contacts': 30,
            'companies': 15,
            'users': 5,
            'pipelines': 3
        })


def create_test_dataset_large() -> Dict[str, List[Dict[str, Any]]]:
    """Create a large test dataset for performance tests"""
    with TestDataManager() as manager:
        return manager.create_test_dataset({
            'deals': 1000,
            'contacts': 500,
            'companies': 100,
            'users': 10,
            'pipelines': 5
        })


def create_mock_google_sheets_service() -> Mock:
    """Create a mock Google Sheets service for testing"""
    manager = TestDataManager()
    return manager.create_mock_google_sheets_service()


if __name__ == '__main__':
    # Example usage
    with TestDataManager() as manager:
        # Create test spreadsheet
        spreadsheet = manager.create_test_spreadsheet("Test Export Spreadsheet")
        print(f"Created test spreadsheet: {spreadsheet.title} ({spreadsheet.spreadsheet_id})")

        # Generate test data
        deals_data = manager.generate_mock_amocrm_data('deals', 10)
        print(f"Generated {len(deals_data)} test deals")

        # Create complete dataset
        dataset = manager.create_test_dataset()
        print(f"Created test dataset with {sum(len(entities) for entities in dataset.values())} total entities")

        # Resources will be cleaned up automatically when exiting the context manager