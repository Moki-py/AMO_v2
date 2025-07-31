"""
Unit tests for GoogleSheetsConfigManager
"""

import pytest
import os
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime

from amocrm_exporter.core.google_sheets_config import (
    GoogleSheetsConfigManager,
    ConfigValidationResult,
    SpreadsheetInfo,
    PermissionTestResult
)


class TestGoogleSheetsConfigManager:
    """Test cases for GoogleSheetsConfigManager"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config_manager = GoogleSheetsConfigManager()

    def test_initialization(self):
        """Test that GoogleSheetsConfigManager initializes correctly"""
        assert self.config_manager.credentials_path == 'credentials.json'
        assert self.config_manager.token_path == 'token.json'
        assert self.config_manager.creds is None
        assert len(self.config_manager.required_spreadsheet_configs) == 4

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_validate_configuration_missing_credentials(self, mock_file, mock_exists):
        """Test validation when credentials file is missing"""
        # Mock credentials file doesn't exist
        mock_exists.side_effect = lambda path: path != 'credentials.json'

        result = self.config_manager.validate_configuration()

        assert not result.is_valid
        assert result.has_errors
        assert any('credentials.json' in error for error in result.errors)
        assert 'not found' in result.errors[0]

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_validate_configuration_invalid_credentials_format(self, mock_file, mock_exists):
        """Test validation with invalid credentials format"""
        # Mock credentials file exists
        mock_exists.return_value = True

        # Mock invalid JSON content
        mock_file.return_value.read.return_value = '{"invalid": "format"}'

        result = self.config_manager.validate_configuration()

        assert not result.is_valid
        assert result.has_errors
        assert any('Invalid credentials format' in error for error in result.errors)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_validate_configuration_malformed_json(self, mock_file, mock_exists):
        """Test validation with malformed JSON"""
        # Mock credentials file exists
        mock_exists.return_value = True

        # Mock malformed JSON
        mock_file.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        result = self.config_manager.validate_configuration()

        assert not result.is_valid
        assert result.has_errors
        assert any('Invalid credentials.json file' in error for error in result.errors)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch.object(GoogleSheetsConfigManager, '_get_spreadsheet_ids')
    def test_validate_configuration_missing_spreadsheet_ids(self, mock_get_ids, mock_file, mock_exists):
        """Test validation with missing spreadsheet IDs"""
        # Mock credentials file exists and is valid
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = '{"installed": {"client_id": "test"}}'

        # Mock missing spreadsheet IDs
        mock_get_ids.return_value = {
            'leads': None,
            'contacts': 'valid_id_here_1234567890123456789012345678901234',
            'companies': None,
            'events': 'valid_id_here_1234567890123456789012345678901234'
        }

        result = self.config_manager.validate_configuration()

        assert not result.is_valid
        assert result.has_errors
        assert len(result.missing_configs) == 2  # leads and companies missing
        assert 'GOOGLE_SHEETS_LEADS_ID' in result.missing_configs
        assert 'GOOGLE_SHEETS_COMPANIES_ID' in result.missing_configs

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch.object(GoogleSheetsConfigManager, '_get_spreadsheet_ids')
    def test_validate_configuration_invalid_spreadsheet_id_format(self, mock_get_ids, mock_file, mock_exists):
        """Test validation with invalid spreadsheet ID format"""
        # Mock credentials file exists and is valid
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = '{"installed": {"client_id": "test"}}'

        # Mock invalid spreadsheet ID format
        mock_get_ids.return_value = {
            'leads': 'invalid_short_id',  # Too short
            'contacts': 'abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx',  # Valid 44 chars
            'companies': 'invalid@id#with$special%chars!',  # Invalid characters
            'events': 'abcd1234efgh5678ijkl9012mnop3456qrst7890xyza'  # Valid 44 chars
        }

        result = self.config_manager.validate_configuration()

        assert not result.is_valid
        assert result.has_errors
        # Should have errors for both invalid IDs
        invalid_format_errors = [error for error in result.errors if 'Invalid spreadsheet ID format' in error]
        assert len(invalid_format_errors) == 2

    def test_is_valid_spreadsheet_id(self):
        """Test spreadsheet ID validation logic"""
        # Valid ID (44 characters, alphanumeric with hyphens and underscores)
        valid_id = 'abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx'  # Exactly 44 chars
        assert self.config_manager._is_valid_spreadsheet_id(valid_id)

        # Invalid IDs
        assert not self.config_manager._is_valid_spreadsheet_id('')  # Empty
        assert not self.config_manager._is_valid_spreadsheet_id(None)  # None
        assert not self.config_manager._is_valid_spreadsheet_id('short')  # Too short
        assert not self.config_manager._is_valid_spreadsheet_id('a' * 45)  # Too long
        assert not self.config_manager._is_valid_spreadsheet_id('invalid@chars#here!' + 'a' * 27)  # Invalid chars

    @patch.object(GoogleSheetsConfigManager, '_get_credentials')
    @patch('amocrm_exporter.core.google_sheets_config.build')
    def test_get_spreadsheet_info_success(self, mock_build, mock_get_creds):
        """Test successful spreadsheet info retrieval"""
        # Mock credentials with proper universe_domain
        mock_creds = Mock()
        mock_creds.valid = True
        mock_creds.universe_domain = 'googleapis.com'
        self.config_manager.creds = mock_creds

        # Mock Google Sheets API response
        mock_service = Mock()
        mock_build.return_value = mock_service

        mock_spreadsheet_data = {
            'properties': {'title': 'Test Spreadsheet'},
            'sheets': [
                {
                    'properties': {
                        'title': 'Sheet1',
                        'sheetId': 0,
                        'sheetType': 'GRID',
                        'gridProperties': {'rowCount': 1000, 'columnCount': 26}
                    }
                }
            ]
        }
        mock_service.spreadsheets().get().execute.return_value = mock_spreadsheet_data

        # Mock permission test
        with patch.object(self.config_manager, 'test_permissions') as mock_test_perms:
            mock_test_perms.return_value = PermissionTestResult(
                can_read=True,
                can_write=True,
                can_create_sheets=True
            )

            spreadsheet_id = 'abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx'
            result = self.config_manager.get_spreadsheet_info(spreadsheet_id)

            assert isinstance(result, SpreadsheetInfo)
            assert result.spreadsheet_id == spreadsheet_id
            assert result.title == 'Test Spreadsheet'
            assert result.url == f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
            assert len(result.sheets) == 1
            assert result.sheets[0]['title'] == 'Sheet1'
            assert result.permissions['can_read'] is True
            assert result.permissions['can_write'] is True

    @patch.object(GoogleSheetsConfigManager, '_get_credentials')
    @patch('amocrm_exporter.core.google_sheets_config.build')
    def test_get_spreadsheet_info_not_found(self, mock_build, mock_get_creds):
        """Test spreadsheet info retrieval when spreadsheet not found"""
        # Mock credentials with proper universe_domain
        mock_creds = Mock()
        mock_creds.valid = True
        mock_creds.universe_domain = 'googleapis.com'
        self.config_manager.creds = mock_creds

        # Mock Google Sheets API 404 error
        from googleapiclient.errors import HttpError
        mock_service = Mock()
        mock_build.return_value = mock_service

        # Create mock HTTP error response
        mock_resp = Mock()
        mock_resp.status = 404
        http_error = HttpError(mock_resp, b'Not found')
        http_error.error_details = []

        mock_service.spreadsheets().get().execute.side_effect = http_error

        spreadsheet_id = 'abcd1234efgh5678ijkl9012mnop3456qrst7890xyza'
        with pytest.raises(Exception) as exc_info:
            self.config_manager.get_spreadsheet_info(spreadsheet_id)

        assert 'Spreadsheet not found' in str(exc_info.value)
        assert spreadsheet_id in str(exc_info.value)

    @patch.object(GoogleSheetsConfigManager, '_get_credentials')
    @patch('amocrm_exporter.core.google_sheets_config.build')
    def test_test_permissions_success(self, mock_build, mock_get_creds):
        """Test successful permission testing"""
        # Mock credentials with proper universe_domain
        mock_creds = Mock()
        mock_creds.valid = True
        mock_creds.universe_domain = 'googleapis.com'
        self.config_manager.creds = mock_creds

        # Mock Google Sheets API service
        mock_service = Mock()
        mock_build.return_value = mock_service

        # Mock successful API calls
        mock_service.spreadsheets().get().execute.return_value = {'properties': {'title': 'Test'}}
        mock_service.spreadsheets().values().get().execute.return_value = {'values': []}

        # Mock sheet creation and deletion for testing - the test_permissions method calls batchUpdate 3 times:
        # 1. Empty batch update to test write permissions
        # 2. Create sheet
        # 3. Delete sheet
        mock_service.spreadsheets().batchUpdate().execute.side_effect = [
            {'replies': []},  # Empty batch update for write test
            {'replies': [{'addSheet': {'properties': {'sheetId': 123}}}]},  # Create sheet
            {'replies': []}  # Delete sheet
        ]

        spreadsheet_id = 'abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx'
        result = self.config_manager.test_permissions(spreadsheet_id)

        assert isinstance(result, PermissionTestResult)
        assert result.can_read is True
        assert result.can_write is True
        assert result.can_create_sheets is True
        assert result.error_message is None

    @patch.object(GoogleSheetsConfigManager, '_get_credentials')
    @patch('amocrm_exporter.core.google_sheets_config.build')
    def test_test_permissions_read_only(self, mock_build, mock_get_creds):
        """Test permission testing with read-only access"""
        # Mock credentials with proper universe_domain
        mock_creds = Mock()
        mock_creds.valid = True
        mock_creds.universe_domain = 'googleapis.com'
        self.config_manager.creds = mock_creds

        # Mock Google Sheets API service
        mock_service = Mock()
        mock_build.return_value = mock_service

        # Mock successful read but failed write
        mock_service.spreadsheets().get().execute.return_value = {'properties': {'title': 'Test'}}
        mock_service.spreadsheets().values().get().execute.return_value = {'values': []}

        # Mock 403 error for write operations
        from googleapiclient.errors import HttpError
        mock_resp = Mock()
        mock_resp.status = 403
        http_error = HttpError(mock_resp, b'Forbidden')
        mock_service.spreadsheets().batchUpdate().execute.side_effect = http_error

        spreadsheet_id = 'abcd1234efgh5678ijkl9012mnop3456qrst7890xyza'
        result = self.config_manager.test_permissions(spreadsheet_id)

        assert isinstance(result, PermissionTestResult)
        assert result.can_read is True
        assert result.can_write is False
        assert result.can_create_sheets is False
        assert 'Write permission denied' in result.error_message

    def test_setup_guided_configuration(self):
        """Test guided configuration setup"""
        with patch.object(self.config_manager, '_get_spreadsheet_ids') as mock_get_ids, \
             patch.object(self.config_manager, 'validate_configuration') as mock_validate, \
             patch('os.path.exists') as mock_exists:

            # Mock missing credentials and some spreadsheet IDs
            mock_exists.return_value = False  # credentials.json doesn't exist
            mock_get_ids.return_value = {
                'leads': 'valid_id_here_1234567890123456789012345678901234',
                'contacts': None,
                'companies': None,
                'events': 'valid_id_here_1234567890123456789012345678901234'
            }

            mock_validation = ConfigValidationResult(
                is_valid=False,
                errors=['Missing credentials'],
                warnings=[],
                missing_configs=['GOOGLE_SHEETS_CONTACTS_ID', 'GOOGLE_SHEETS_COMPANIES_ID']
            )
            mock_validate.return_value = mock_validation

            result = self.config_manager.setup_guided_configuration()

            assert 'steps' in result
            assert 'current_status' in result
            assert 'next_actions' in result

            # Should have 4 steps
            assert len(result['steps']) == 4

            # First step should be incomplete (no credentials)
            assert result['steps'][0]['completed'] is False
            assert 'Google Cloud Console Setup' in result['steps'][0]['title']

            # Check progress calculation
            completed_steps = sum(1 for step in result['steps'] if step['completed'])
            expected_progress = (completed_steps / 4) * 100
            assert result['current_status']['progress_percentage'] == expected_progress

            # Should suggest creating credentials as next action
            assert any('credentials.json' in action for action in result['next_actions'])

    @patch.object(GoogleSheetsConfigManager, '_get_spreadsheet_ids')
    def test_get_configuration_summary(self, mock_get_ids):
        """Test configuration summary generation"""
        with patch('os.path.exists') as mock_exists, \
             patch.object(self.config_manager, 'validate_configuration') as mock_validate:

            # Mock file existence
            mock_exists.side_effect = lambda path: path == 'credentials.json'

            # Mock spreadsheet IDs
            mock_get_ids.return_value = {
                'leads': 'abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx',  # Valid 44 chars
                'contacts': None,
                'companies': 'invalid_id',  # Invalid (too short)
                'events': 'abcd1234efgh5678ijkl9012mnop3456qrst7890uvwyz'  # Valid 44 chars
            }

            # Mock validation result
            mock_validation = ConfigValidationResult(
                is_valid=False,
                errors=['Some error'],
                warnings=['Some warning'],
                missing_configs=[]
            )
            mock_validate.return_value = mock_validation

            result = self.config_manager.get_configuration_summary()

            assert result['credentials_file_exists'] is True
            assert result['token_file_exists'] is False

            # Check configured spreadsheets
            assert result['configured_spreadsheets']['leads']['configured'] is True
            assert result['configured_spreadsheets']['leads']['valid_format'] is True
            assert result['configured_spreadsheets']['contacts']['configured'] is False
            assert result['configured_spreadsheets']['companies']['configured'] is True
            assert result['configured_spreadsheets']['companies']['valid_format'] is False

            # Check validation status
            assert result['validation_status']['is_valid'] is False
            assert result['validation_status']['error_count'] == 1
            assert result['validation_status']['warning_count'] == 1

    @patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file')
    @patch('google.oauth2.credentials.Credentials.from_authorized_user_file')
    @patch('os.path.exists')
    def test_get_credentials_new_flow(self, mock_exists, mock_from_file, mock_flow):
        """Test getting credentials through OAuth flow"""
        # Mock credentials file exists, token doesn't
        mock_exists.side_effect = lambda path: path == 'credentials.json'

        # Mock OAuth flow
        mock_creds = Mock()
        mock_creds.valid = True
        mock_flow_instance = Mock()
        mock_flow_instance.run_local_server.return_value = mock_creds
        mock_flow.return_value = mock_flow_instance

        # Mock file writing
        with patch('builtins.open', mock_open()) as mock_file:
            self.config_manager._get_credentials()

            assert self.config_manager.creds == mock_creds
            from amocrm_exporter.core.google_sheets_config import SCOPES
            mock_flow.assert_called_once_with('credentials.json', SCOPES)
            mock_flow_instance.run_local_server.assert_called_once()

    @patch('google.oauth2.credentials.Credentials.from_authorized_user_file')
    @patch('os.path.exists')
    def test_get_credentials_refresh_existing(self, mock_exists, mock_from_file):
        """Test refreshing existing credentials"""
        # Mock both files exist
        mock_exists.return_value = True

        # Mock existing credentials that need refresh
        mock_creds = Mock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = 'refresh_token'
        mock_from_file.return_value = mock_creds

        # Mock successful refresh
        with patch('google.auth.transport.requests.Request') as mock_request:
            # Set up the refresh to be called
            def mock_refresh(request):
                mock_creds.valid = True
            mock_creds.refresh.side_effect = mock_refresh

            with patch('builtins.open', mock_open()) as mock_file:
                self.config_manager._get_credentials()

                assert self.config_manager.creds == mock_creds
                mock_creds.refresh.assert_called_once()


class TestConfigValidationResult:
    """Test ConfigValidationResult dataclass"""

    def test_has_errors_property(self):
        """Test has_errors property"""
        result_with_errors = ConfigValidationResult(
            is_valid=False,
            errors=['Error 1', 'Error 2'],
            warnings=[],
            missing_configs=[]
        )
        assert result_with_errors.has_errors is True

        result_without_errors = ConfigValidationResult(
            is_valid=True,
            errors=[],
            warnings=['Warning 1'],
            missing_configs=[]
        )
        assert result_without_errors.has_errors is False

    def test_has_warnings_property(self):
        """Test has_warnings property"""
        result_with_warnings = ConfigValidationResult(
            is_valid=True,
            errors=[],
            warnings=['Warning 1'],
            missing_configs=[]
        )
        assert result_with_warnings.has_warnings is True

        result_without_warnings = ConfigValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            missing_configs=[]
        )
        assert result_without_warnings.has_warnings is False


class TestSpreadsheetInfo:
    """Test SpreadsheetInfo dataclass"""

    def test_spreadsheet_info_creation(self):
        """Test SpreadsheetInfo creation and properties"""
        sheets_data = [
            {'title': 'Sheet1', 'sheet_id': 0, 'sheet_type': 'GRID'},
            {'title': 'Sheet2', 'sheet_id': 1, 'sheet_type': 'GRID'}
        ]

        permissions_data = {
            'can_read': True,
            'can_write': True,
            'can_create_sheets': False
        }

        info = SpreadsheetInfo(
            spreadsheet_id='test_id_1234567890123456789012345678901234',
            title='Test Spreadsheet',
            url='https://docs.google.com/spreadsheets/d/test_id_1234567890123456789012345678901234',
            sheets=sheets_data,
            permissions=permissions_data,
            last_modified=datetime.now()
        )

        assert info.spreadsheet_id == 'test_id_1234567890123456789012345678901234'
        assert info.title == 'Test Spreadsheet'
        assert len(info.sheets) == 2
        assert info.permissions['can_read'] is True
        assert info.permissions['can_write'] is True
        assert info.permissions['can_create_sheets'] is False
        assert info.last_modified is not None


class TestPermissionTestResult:
    """Test PermissionTestResult dataclass"""

    def test_permission_test_result_creation(self):
        """Test PermissionTestResult creation"""
        result = PermissionTestResult(
            can_read=True,
            can_write=False,
            can_create_sheets=False,
            error_message="Write permission denied"
        )

        assert result.can_read is True
        assert result.can_write is False
        assert result.can_create_sheets is False
        assert result.error_message == "Write permission denied"

    def test_permission_test_result_success(self):
        """Test PermissionTestResult for successful case"""
        result = PermissionTestResult(
            can_read=True,
            can_write=True,
            can_create_sheets=True
        )

        assert result.can_read is True
        assert result.can_write is True
        assert result.can_create_sheets is True
        assert result.error_message is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])