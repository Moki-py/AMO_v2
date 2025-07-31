"""
Tests for the Google Sheets configuration validation system
"""

import pytest
import os
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from amocrm_exporter.utils.config_validator import (
    ConfigurationValidator,
    DiagnosticResult,
    ConnectionTestResult,
    OAuthValidationResult
)
from amocrm_exporter.core.google_sheets_config import (
    GoogleSheetsConfigManager,
    ConfigValidationResult
)


class TestConfigurationValidator:
    """Test the comprehensive configuration validation system"""

    def setup_method(self):
        """Set up test fixtures"""
        self.validator = ConfigurationValidator()

    @patch('amocrm_exporter.utils.config_validator.GoogleSheetsConfigManager')
    def test_run_comprehensive_validation_success(self, mock_config_manager):
        """Test successful comprehensive validation"""
        # Mock the config manager
        mock_manager = Mock()
        mock_config_manager.return_value = mock_manager

        # Mock basic validation
        mock_manager.validate_configuration.return_value = ConfigValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            missing_configs=[]
        )

        # Create new validator with mocked manager
        validator = ConfigurationValidator()

        # Mock other methods
        validator._test_google_sheets_connection = Mock(return_value=ConnectionTestResult(
            can_connect=True,
            api_accessible=True,
            authentication_valid=True,
            spreadsheets_accessible={'leads': True, 'contacts': True},
            response_time_ms=150.0
        ))

        validator._validate_oauth_flow = Mock(return_value=OAuthValidationResult(
            credentials_file_valid=True,
            token_exists=True,
            token_valid=True,
            can_refresh=True,
            scopes_valid=True
        ))

        validator._run_diagnostic_tests = Mock(return_value=[
            DiagnosticResult(
                test_name="File Permissions",
                passed=True,
                message="All files have proper permissions"
            )
        ])

        # Run validation
        results = validator.run_comprehensive_validation()

        # Verify results
        assert results['overall_status'] == 'ready'
        assert results['basic_validation']['is_valid'] is True
        assert results['connection_test'].can_connect is True
        assert results['oauth_validation'].token_valid is True
        assert len(results['diagnostic_tests']) == 1
        assert results['diagnostic_tests'][0].passed is True

    def test_file_permissions_diagnostic(self):
        """Test file permissions diagnostic"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            credentials_path = os.path.join(temp_dir, 'credentials.json')
            token_path = os.path.join(temp_dir, 'token.json')

            with open(credentials_path, 'w') as f:
                json.dump({'installed': {'client_id': 'test'}}, f)

            with open(token_path, 'w') as f:
                json.dump({'token': 'test'}, f)

            # Mock the paths
            self.validator.config_manager.credentials_path = credentials_path
            self.validator.config_manager.token_path = token_path

            # Run test
            result = self.validator._test_file_permissions()

            # Verify
            assert result.passed is True
            assert "proper permissions" in result.message

    def test_file_permissions_diagnostic_missing_credentials(self):
        """Test file permissions diagnostic with missing credentials"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set non-existent credentials path
            self.validator.config_manager.credentials_path = os.path.join(temp_dir, 'missing.json')
            self.validator.config_manager.token_path = os.path.join(temp_dir, 'token.json')

            # Run test
            result = self.validator._test_file_permissions()

            # Verify
            assert result.passed is False
            assert "does not exist" in result.message
            assert "Download credentials.json" in result.fix_suggestions[0]

    @patch('requests.get')
    def test_network_connectivity_diagnostic_success(self, mock_get):
        """Test network connectivity diagnostic with successful connections"""
        # Mock successful responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_get.return_value = mock_response

        # Run test
        result = self.validator._test_network_connectivity()

        # Verify
        assert result.passed is True
        assert "accessible" in result.message
        assert mock_get.call_count == 3  # Three URLs tested

    @patch('requests.get')
    def test_network_connectivity_diagnostic_failure(self, mock_get):
        """Test network connectivity diagnostic with connection failures"""
        # Mock failed responses
        mock_get.side_effect = Exception("Connection failed")

        # Run test
        result = self.validator._test_network_connectivity()

        # Verify
        assert result.passed is False
        assert "Cannot access Google APIs" in result.message
        assert "Check internet connection" in result.fix_suggestions[0]

    def test_environment_variables_diagnostic_success(self):
        """Test environment variables diagnostic with valid configuration"""
        # Mock settings with valid values
        with patch('amocrm_exporter.utils.config_validator.settings') as mock_settings:
            mock_settings.google_sheets_leads_id = 'a' * 44  # Valid 44-char ID
            mock_settings.google_sheets_contacts_id = 'b' * 44
            mock_settings.google_sheets_companies_id = 'c' * 44
            mock_settings.google_sheets_events_id = 'd' * 44

            # Run test
            result = self.validator._test_environment_variables()

            # Verify
            assert result.passed is True
            assert "properly configured" in result.message

    def test_environment_variables_diagnostic_missing_vars(self):
        """Test environment variables diagnostic with missing variables"""
        # Mock settings with missing values
        with patch('amocrm_exporter.utils.config_validator.settings') as mock_settings:
            mock_settings.google_sheets_leads_id = None
            mock_settings.google_sheets_contacts_id = 'b' * 44
            mock_settings.google_sheets_companies_id = None
            mock_settings.google_sheets_events_id = 'd' * 44

            # Run test
            result = self.validator._test_environment_variables()

            # Verify
            assert result.passed is False
            assert "Missing environment variables" in result.message
            assert "Add missing variables" in result.fix_suggestions[0]

    def test_spreadsheet_formats_diagnostic_valid_ids(self):
        """Test spreadsheet format diagnostic with valid IDs"""
        # Mock valid spreadsheet IDs
        mock_ids = {
            'leads': 'a' * 44,
            'contacts': 'b' * 44,
            'companies': 'c' * 44,
            'events': 'd' * 44
        }

        self.validator.config_manager._get_spreadsheet_ids = Mock(return_value=mock_ids)
        self.validator.config_manager._is_valid_spreadsheet_id = Mock(return_value=True)

        # Mock credentials as invalid to skip accessibility test
        self.validator.config_manager.creds = None

        # Run test
        result = self.validator._test_spreadsheet_formats()

        # Verify
        assert result.passed is True
        assert "properly formatted" in result.message

    def test_spreadsheet_formats_diagnostic_invalid_ids(self):
        """Test spreadsheet format diagnostic with invalid IDs"""
        # Mock invalid spreadsheet IDs
        mock_ids = {
            'leads': 'invalid_id',
            'contacts': 'b' * 44,
            'companies': 'also_invalid',
            'events': 'd' * 44
        }

        self.validator.config_manager._get_spreadsheet_ids = Mock(return_value=mock_ids)

        def mock_is_valid(spreadsheet_id):
            return len(spreadsheet_id) == 44

        self.validator.config_manager._is_valid_spreadsheet_id = Mock(side_effect=mock_is_valid)

        # Mock credentials as invalid to skip accessibility test
        self.validator.config_manager.creds = None

        # Run test
        result = self.validator._test_spreadsheet_formats()

        # Verify
        assert result.passed is False
        assert "Invalid spreadsheet ID formats" in result.message
        assert "copied correctly" in result.fix_suggestions[0]

    def test_oauth_validation_valid_credentials(self):
        """Test OAuth validation with valid credentials"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create valid credentials file
            credentials_path = os.path.join(temp_dir, 'credentials.json')
            token_path = os.path.join(temp_dir, 'token.json')

            with open(credentials_path, 'w') as f:
                json.dump({
                    'installed': {
                        'client_id': 'test_client_id',
                        'client_secret': 'test_secret'
                    }
                }, f)

            with open(token_path, 'w') as f:
                json.dump({
                    'token': 'test_token',
                    'scopes': ['https://www.googleapis.com/auth/spreadsheets']
                }, f)

            # Mock the paths
            self.validator.config_manager.credentials_path = credentials_path
            self.validator.config_manager.token_path = token_path

            # Mock Credentials.from_authorized_user_file
            with patch('amocrm_exporter.utils.config_validator.Credentials') as mock_creds:
                mock_cred_instance = Mock()
                mock_cred_instance.valid = True
                mock_cred_instance.refresh_token = 'refresh_token'
                mock_creds.from_authorized_user_file.return_value = mock_cred_instance

                # Run test
                result = self.validator._validate_oauth_flow()

                # Verify
                assert result.credentials_file_valid is True
                assert result.token_exists is True
                assert result.token_valid is True
                assert result.can_refresh is True
                assert result.scopes_valid is True

    def test_oauth_validation_invalid_credentials(self):
        """Test OAuth validation with invalid credentials file"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create invalid credentials file
            credentials_path = os.path.join(temp_dir, 'credentials.json')

            with open(credentials_path, 'w') as f:
                json.dump({'invalid': 'structure'}, f)

            # Mock the paths
            self.validator.config_manager.credentials_path = credentials_path
            self.validator.config_manager.token_path = os.path.join(temp_dir, 'nonexistent.json')

            # Run test
            result = self.validator._validate_oauth_flow()

            # Verify
            assert result.credentials_file_valid is False
            assert result.token_exists is False
            assert result.token_valid is False

    def test_credential_file_format_diagnostic_valid(self):
        """Test credential file format diagnostic with valid file"""
        with tempfile.TemporaryDirectory() as temp_dir:
            credentials_path = os.path.join(temp_dir, 'credentials.json')

            with open(credentials_path, 'w') as f:
                json.dump({
                    'installed': {
                        'client_id': 'test_client_id',
                        'client_secret': 'test_secret',
                        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                        'token_uri': 'https://oauth2.googleapis.com/token'
                    }
                }, f)

            self.validator.config_manager.credentials_path = credentials_path

            # Run test
            result = self.validator._test_credential_file_format()

            # Verify
            assert result.passed is True
            assert "format is valid" in result.message

    def test_credential_file_format_diagnostic_invalid_json(self):
        """Test credential file format diagnostic with invalid JSON"""
        with tempfile.TemporaryDirectory() as temp_dir:
            credentials_path = os.path.join(temp_dir, 'credentials.json')

            with open(credentials_path, 'w') as f:
                f.write('invalid json content')

            self.validator.config_manager.credentials_path = credentials_path

            # Run test
            result = self.validator._test_credential_file_format()

            # Verify
            assert result.passed is False
            assert "Invalid JSON format" in result.message
            assert "Download a new credentials.json" in result.fix_suggestions[0]

    def test_credential_file_format_diagnostic_missing_file(self):
        """Test credential file format diagnostic with missing file"""
        self.validator.config_manager.credentials_path = '/nonexistent/path/credentials.json'

        # Run test
        result = self.validator._test_credential_file_format()

        # Verify
        assert result.passed is False
        assert "does not exist" in result.message
        assert "Download credentials.json" in result.fix_suggestions[0]

    def test_generate_recommendations_with_errors(self):
        """Test recommendation generation with various error conditions"""
        # Mock validation results with errors
        validation_results = {
            'basic_validation': {
                'has_errors': True,
                'errors': ['Missing spreadsheet ID', 'Invalid credentials']
            },
            'connection_test': ConnectionTestResult(
                can_connect=False,
                api_accessible=False,
                authentication_valid=False,
                spreadsheets_accessible={},
                error_details='Network timeout'
            ),
            'oauth_validation': OAuthValidationResult(
                credentials_file_valid=False,
                token_valid=False,
                token_exists=True,
                can_refresh=False,
                scopes_valid=False
            ),
            'diagnostic_tests': [
                DiagnosticResult(
                    test_name="File Permissions",
                    passed=False,
                    message="Permission denied",
                    fix_suggestions=["Check file permissions", "Run as administrator"]
                )
            ]
        }

        # Run test
        recommendations = self.validator._generate_recommendations(validation_results)

        # Verify
        assert len(recommendations) > 0
        assert any("Fix basic configuration errors" in rec for rec in recommendations)
        assert any("connection issues" in rec for rec in recommendations)
        assert any("Download valid credentials.json" in rec for rec in recommendations)

    def test_create_validation_summary(self):
        """Test validation summary creation"""
        # Mock validation results
        validation_results = {
            'basic_validation': {
                'is_valid': True,
                'errors': [],
                'warnings': ['Minor warning']
            },
            'connection_test': ConnectionTestResult(
                can_connect=True,
                api_accessible=True,
                authentication_valid=True,
                spreadsheets_accessible={}
            ),
            'oauth_validation': OAuthValidationResult(
                credentials_file_valid=True,
                token_valid=True,
                token_exists=True,
                can_refresh=True,
                scopes_valid=True
            ),
            'diagnostic_tests': [
                DiagnosticResult(test_name="Test1", passed=True, message="OK"),
                DiagnosticResult(test_name="Test2", passed=True, message="OK"),
                DiagnosticResult(test_name="Test3", passed=False, message="Failed")
            ]
        }

        # Run test
        summary = self.validator._create_validation_summary(validation_results)

        # Verify
        assert summary['total_tests'] == 6  # basic + connection + oauth + 3 diagnostics
        assert summary['passed_tests'] == 5
        assert summary['failed_tests'] == 1
        assert summary['warnings'] == 1
        assert summary['critical_issues'] == 0
        assert summary['configuration_complete'] is True
        assert summary['ready_for_export'] is False  # Because one test failed

    def test_determine_overall_status(self):
        """Test overall status determination"""
        # Test ready status
        results_ready = {
            'summary': {
                'ready_for_export': True,
                'configuration_complete': True,
                'critical_issues': 0
            }
        }
        assert self.validator._determine_overall_status(results_ready) == 'ready'

        # Test configured with warnings
        results_warnings = {
            'summary': {
                'ready_for_export': False,
                'configuration_complete': True,
                'critical_issues': 0
            }
        }
        assert self.validator._determine_overall_status(results_warnings) == 'configured_with_warnings'

        # Test critical issues
        results_critical = {
            'summary': {
                'ready_for_export': False,
                'configuration_complete': False,
                'critical_issues': 2
            }
        }
        assert self.validator._determine_overall_status(results_critical) == 'critical_issues'

    def test_get_diagnostic_report(self):
        """Test diagnostic report generation"""
        # Mock comprehensive validation
        self.validator.run_comprehensive_validation = Mock(return_value={
            'timestamp': '2024-01-01T12:00:00',
            'overall_status': 'ready',
            'summary': {
                'total_tests': 5,
                'passed_tests': 5,
                'failed_tests': 0,
                'warnings': 0,
                'critical_issues': 0,
                'ready_for_export': True
            },
            'basic_validation': {
                'is_valid': True,
                'errors': [],
                'warnings': []
            },
            'connection_test': ConnectionTestResult(
                can_connect=True,
                api_accessible=True,
                authentication_valid=True,
                spreadsheets_accessible={},
                response_time_ms=150
            ),
            'diagnostic_tests': [
                DiagnosticResult(
                    test_name='File Permissions',
                    passed=True,
                    message='All files accessible'
                )
            ],
            'recommendations': ['Configuration is working correctly']
        })

        # Run test
        report = self.validator.get_diagnostic_report()

        # Verify
        assert "GOOGLE SHEETS CONFIGURATION DIAGNOSTIC REPORT" in report
        assert "Overall Status: READY" in report
        assert "Total Tests: 5" in report
        assert "Passed: 5" in report
        assert "Ready for Export: Yes" in report
        assert "File Permissions: PASS" in report
        assert "Configuration is working correctly" in report


class TestConnectionTestResult:
    """Test the ConnectionTestResult dataclass"""

    def test_connection_test_result_creation(self):
        """Test creating a ConnectionTestResult"""
        result = ConnectionTestResult(
            can_connect=True,
            api_accessible=True,
            authentication_valid=True,
            spreadsheets_accessible={'leads': True, 'contacts': False},
            response_time_ms=250.5
        )

        assert result.can_connect is True
        assert result.api_accessible is True
        assert result.authentication_valid is True
        assert result.spreadsheets_accessible['leads'] is True
        assert result.spreadsheets_accessible['contacts'] is False
        assert result.response_time_ms == 250.5
        assert result.error_details is None


class TestOAuthValidationResult:
    """Test the OAuthValidationResult dataclass"""

    def test_oauth_validation_result_creation(self):
        """Test creating an OAuthValidationResult"""
        expires_at = datetime.now()

        result = OAuthValidationResult(
            credentials_file_valid=True,
            token_exists=True,
            token_valid=False,
            can_refresh=True,
            scopes_valid=True,
            error_message="Token expired",
            expires_at=expires_at
        )

        assert result.credentials_file_valid is True
        assert result.token_exists is True
        assert result.token_valid is False
        assert result.can_refresh is True
        assert result.scopes_valid is True
        assert result.error_message == "Token expired"
        assert result.expires_at == expires_at


class TestDiagnosticResult:
    """Test the DiagnosticResult dataclass"""

    def test_diagnostic_result_creation(self):
        """Test creating a DiagnosticResult"""
        details = {'test_data': 'value'}
        fix_suggestions = ['Fix suggestion 1', 'Fix suggestion 2']

        result = DiagnosticResult(
            test_name="Test Name",
            passed=False,
            message="Test failed",
            details=details,
            fix_suggestions=fix_suggestions
        )

        assert result.test_name == "Test Name"
        assert result.passed is False
        assert result.message == "Test failed"
        assert result.details == details
        assert result.fix_suggestions == fix_suggestions