"""
Tests for the Google Sheets setup wizard
"""

import pytest
import os
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from amocrm_exporter.utils.setup_wizard import (
    GoogleSheetsSetupWizard,
    SetupStep,
    SetupProgress
)


class TestGoogleSheetsSetupWizard:
    """Test the Google Sheets setup wizard"""

    def setup_method(self):
        """Set up test fixtures"""
        self.wizard = GoogleSheetsSetupWizard()

    def test_initialize_setup_steps(self):
        """Test that setup steps are properly initialized"""
        steps = self.wizard._initialize_setup_steps()

        assert len(steps) == 6
        assert all(isinstance(step, SetupStep) for step in steps)
        assert steps[0].step_number == 1
        assert steps[0].title == "Google Cloud Console Setup"
        assert steps[-1].step_number == 6
        assert steps[-1].title == "Final Validation"

    def test_get_step_details(self):
        """Test getting details for a specific step"""
        step = self.wizard.get_step_details(1)

        assert step is not None
        assert step.step_number == 1
        assert step.title == "Google Cloud Console Setup"
        assert len(step.instructions) > 0

    def test_get_step_details_invalid(self):
        """Test getting details for invalid step number"""
        step = self.wizard.get_step_details(999)
        assert step is None

    def test_get_all_steps(self):
        """Test getting all setup steps"""
        steps = self.wizard.get_all_steps()

        assert len(steps) == 6
        assert all(isinstance(step, SetupStep) for step in steps)
        assert steps[0].step_number == 1
        assert steps[-1].step_number == 6

    @patch('os.path.exists')
    def test_validate_google_cloud_setup_success(self, mock_exists):
        """Test successful Google Cloud setup validation"""
        mock_exists.return_value = True

        is_valid, error_message = self.wizard.validate_google_cloud_setup()

        assert is_valid is True
        assert error_message is None

    @patch('os.path.exists')
    def test_validate_google_cloud_setup_failure(self, mock_exists):
        """Test failed Google Cloud setup validation"""
        mock_exists.return_value = False

        is_valid, error_message = self.wizard.validate_google_cloud_setup()

        assert is_valid is False
        assert "Credentials file not found" in error_message

    def test_validate_oauth_credentials_success(self):
        """Test successful OAuth credentials validation"""
        with tempfile.TemporaryDirectory() as temp_dir:
            credentials_path = os.path.join(temp_dir, 'credentials.json')

            # Create valid credentials file
            with open(credentials_path, 'w') as f:
                json.dump({
                    'installed': {
                        'client_id': 'test_client_id',
                        'client_secret': 'test_secret',
                        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                        'token_uri': 'https://oauth2.googleapis.com/token'
                    }
                }, f)

            self.wizard.config_manager.credentials_path = credentials_path

            is_valid, error_message = self.wizard.validate_oauth_credentials()

            assert is_valid is True
            assert error_message is None

    def test_validate_oauth_credentials_missing_file(self):
        """Test OAuth credentials validation with missing file"""
        self.wizard.config_manager.credentials_path = '/nonexistent/credentials.json'

        is_valid, error_message = self.wizard.validate_oauth_credentials()

        assert is_valid is False
        assert "not found" in error_message

    def test_validate_oauth_credentials_invalid_format(self):
        """Test OAuth credentials validation with invalid format"""
        with tempfile.TemporaryDirectory() as temp_dir:
            credentials_path = os.path.join(temp_dir, 'credentials.json')

            # Create invalid credentials file
            with open(credentials_path, 'w') as f:
                json.dump({'invalid': 'structure'}, f)

            self.wizard.config_manager.credentials_path = credentials_path

            is_valid, error_message = self.wizard.validate_oauth_credentials()

            assert is_valid is False
            assert "Invalid credentials format" in error_message

    def test_validate_oauth_credentials_invalid_json(self):
        """Test OAuth credentials validation with invalid JSON"""
        with tempfile.TemporaryDirectory() as temp_dir:
            credentials_path = os.path.join(temp_dir, 'credentials.json')

            # Create file with invalid JSON
            with open(credentials_path, 'w') as f:
                f.write('invalid json content')

            self.wizard.config_manager.credentials_path = credentials_path

            is_valid, error_message = self.wizard.validate_oauth_credentials()

            assert is_valid is False
            assert "invalid JSON" in error_message

    def test_validate_spreadsheets_created_success(self):
        """Test successful spreadsheet validation"""
        # Mock valid spreadsheet IDs
        mock_ids = {
            'leads': 'a' * 44,
            'contacts': 'b' * 44,
            'companies': 'c' * 44,
            'events': 'd' * 44
        }

        self.wizard.config_manager._get_spreadsheet_ids = Mock(return_value=mock_ids)
        self.wizard.config_manager._is_valid_spreadsheet_id = Mock(return_value=True)

        is_valid, error_message = self.wizard.validate_spreadsheets_created()

        assert is_valid is True
        assert error_message is None

    def test_validate_spreadsheets_created_missing_ids(self):
        """Test spreadsheet validation with missing IDs"""
        # Mock missing spreadsheet IDs
        mock_ids = {
            'leads': None,
            'contacts': 'b' * 44,
            'companies': None,
            'events': 'd' * 44
        }

        self.wizard.config_manager._get_spreadsheet_ids = Mock(return_value=mock_ids)
        self.wizard.config_manager._is_valid_spreadsheet_id = Mock(return_value=True)

        is_valid, error_message = self.wizard.validate_spreadsheets_created()

        assert is_valid is False
        assert "Missing spreadsheet IDs" in error_message
        assert "leads" in error_message
        assert "companies" in error_message

    def test_validate_spreadsheets_created_invalid_format(self):
        """Test spreadsheet validation with invalid format"""
        # Mock invalid spreadsheet ID format
        mock_ids = {
            'leads': 'invalid_id',
            'contacts': 'b' * 44,
            'companies': 'c' * 44,
            'events': 'd' * 44
        }

        self.wizard.config_manager._get_spreadsheet_ids = Mock(return_value=mock_ids)

        def mock_is_valid(spreadsheet_id):
            return len(spreadsheet_id) == 44

        self.wizard.config_manager._is_valid_spreadsheet_id = Mock(side_effect=mock_is_valid)

        is_valid, error_message = self.wizard.validate_spreadsheets_created()

        assert is_valid is False
        assert "Invalid spreadsheet ID format" in error_message
        assert "leads" in error_message

    def test_validate_environment_variables_success(self):
        """Test successful environment variables validation"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory
            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                # Create .env file
                with open('.env', 'w') as f:
                    f.write('GOOGLE_SHEETS_LEADS_ID=' + 'a' * 44 + '\n')

                # Mock valid spreadsheet IDs
                mock_ids = {
                    'leads': 'a' * 44,
                    'contacts': 'b' * 44,
                    'companies': 'c' * 44,
                    'events': 'd' * 44
                }

                self.wizard.config_manager._get_spreadsheet_ids = Mock(return_value=mock_ids)
                self.wizard.config_manager._is_valid_spreadsheet_id = Mock(return_value=True)

                is_valid, error_message = self.wizard.validate_environment_variables()

                assert is_valid is True
                assert error_message is None

            finally:
                os.chdir(original_cwd)

    def test_validate_environment_variables_missing_env_file(self):
        """Test environment variables validation with missing .env file"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory (no .env file)
            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                is_valid, error_message = self.wizard.validate_environment_variables()

                assert is_valid is False
                assert ".env file not found" in error_message

            finally:
                os.chdir(original_cwd)

    @patch('amocrm_exporter.utils.setup_wizard.GoogleSheetsConfigManager')
    def test_validate_authentication_success(self, mock_config_manager_class):
        """Test successful authentication validation"""
        # Mock the config manager
        mock_manager = Mock()
        mock_config_manager_class.return_value = mock_manager

        # Mock successful credentials
        mock_creds = Mock()
        mock_creds.valid = True
        mock_manager.creds = mock_creds

        # Create new wizard with mocked manager
        wizard = GoogleSheetsSetupWizard()

        is_valid, error_message = wizard.validate_authentication()

        assert is_valid is True
        assert error_message is None

    @patch('amocrm_exporter.utils.setup_wizard.GoogleSheetsConfigManager')
    def test_validate_authentication_failure(self, mock_config_manager_class):
        """Test failed authentication validation"""
        # Mock the config manager
        mock_manager = Mock()
        mock_config_manager_class.return_value = mock_manager

        # Mock failed credentials
        mock_manager.creds = None

        # Create new wizard with mocked manager
        wizard = GoogleSheetsSetupWizard()

        is_valid, error_message = wizard.validate_authentication()

        assert is_valid is False
        assert "Authentication failed" in error_message

    def test_validate_complete_setup_success(self):
        """Test successful complete setup validation"""
        # Mock successful comprehensive validation
        self.wizard.validator.run_comprehensive_validation = Mock(return_value={
            'overall_status': 'ready'
        })

        is_valid, error_message = self.wizard.validate_complete_setup()

        assert is_valid is True
        assert error_message is None

    def test_validate_complete_setup_with_warnings(self):
        """Test complete setup validation with warnings"""
        # Mock validation with warnings
        self.wizard.validator.run_comprehensive_validation = Mock(return_value={
            'overall_status': 'configured_with_warnings',
            'basic_validation': {
                'warnings': ['Minor warning', 'Another warning']
            }
        })

        is_valid, error_message = self.wizard.validate_complete_setup()

        assert is_valid is True
        assert error_message is not None
        assert "warnings" in error_message

    def test_validate_complete_setup_failure(self):
        """Test failed complete setup validation"""
        # Mock failed validation
        self.wizard.validator.run_comprehensive_validation = Mock(return_value={
            'overall_status': 'critical_issues',
            'basic_validation': {
                'errors': ['Critical error', 'Another error']
            }
        })

        is_valid, error_message = self.wizard.validate_complete_setup()

        assert is_valid is False
        assert error_message is not None
        assert "validation failed" in error_message

    def test_get_setup_progress(self):
        """Test getting setup progress"""
        # Mock some validation methods
        self.wizard.validate_google_cloud_setup = Mock(return_value=(True, None))
        self.wizard.validate_oauth_credentials = Mock(return_value=(True, None))
        self.wizard.validate_spreadsheets_created = Mock(return_value=(False, "Missing IDs"))

        progress = self.wizard.get_setup_progress()

        assert isinstance(progress, SetupProgress)
        assert progress.total_steps == 6
        assert len(progress.completed_steps) >= 0
        assert progress.overall_progress_percentage >= 0
        assert progress.overall_progress_percentage <= 100
        assert progress.next_action is not None

    def test_get_troubleshooting_guide(self):
        """Test getting troubleshooting guide"""
        guide = self.wizard.get_troubleshooting_guide()

        assert 'common_problems' in guide
        assert 'diagnostic_commands' in guide
        assert 'helpful_links' in guide

        assert len(guide['common_problems']) > 0
        assert len(guide['diagnostic_commands']) > 0
        assert len(guide['helpful_links']) > 0

        # Check structure of common problems
        problem = guide['common_problems'][0]
        assert 'problem' in problem
        assert 'symptoms' in problem
        assert 'solutions' in problem

    def test_generate_setup_report(self):
        """Test generating setup report"""
        # Mock progress calculation
        self.wizard.get_setup_progress = Mock(return_value=SetupProgress(
            current_step=2,
            total_steps=6,
            completed_steps=[1],
            failed_steps=[],
            warnings=[],
            overall_progress_percentage=16.7,
            estimated_time_remaining_minutes=25,
            next_action="Complete Step 2"
        ))

        report = self.wizard.generate_setup_report()

        assert isinstance(report, str)
        assert "GOOGLE SHEETS SETUP WIZARD REPORT" in report
        assert "Overall Progress: 16.7%" in report
        assert "Completed Steps: 1/6" in report
        assert "Current Step: 2" in report

    def test_get_step_by_step_guide(self):
        """Test getting step-by-step guide"""
        guide = self.wizard.get_step_by_step_guide(1)

        assert 'step' in guide
        assert 'prerequisites' in guide
        assert 'instructions' in guide
        assert 'help_links' in guide
        assert 'validation' in guide
        assert 'estimated_time_minutes' in guide
        assert 'difficulty_level' in guide

        step = guide['step']
        assert step['number'] == 1
        assert step['title'] == "Google Cloud Console Setup"

    def test_get_step_by_step_guide_invalid(self):
        """Test getting step-by-step guide for invalid step"""
        guide = self.wizard.get_step_by_step_guide(999)

        assert 'error' in guide
        assert "not found" in guide['error']

    def test_estimate_step_time(self):
        """Test step time estimation"""
        # Test known step times
        assert self.wizard._estimate_step_time(1) == 10
        assert self.wizard._estimate_step_time(2) == 15
        assert self.wizard._estimate_step_time(3) == 10

        # Test unknown step (should default to 10)
        assert self.wizard._estimate_step_time(999) == 10

    def test_get_step_difficulty(self):
        """Test step difficulty assessment"""
        # Test known difficulties
        assert self.wizard._get_step_difficulty(1) == "Medium"
        assert self.wizard._get_step_difficulty(2) == "Hard"
        assert self.wizard._get_step_difficulty(3) == "Easy"

        # Test unknown step (should default to Medium)
        assert self.wizard._get_step_difficulty(999) == "Medium"

    @patch('webbrowser.open')
    def test_open_helpful_links_specific_step(self, mock_open):
        """Test opening helpful links for a specific step"""
        self.wizard.open_helpful_links(1)

        # Should open links for step 1
        assert mock_open.call_count > 0

    @patch('webbrowser.open')
    def test_open_helpful_links_general(self, mock_open):
        """Test opening general helpful links"""
        self.wizard.open_helpful_links()

        # Should open general helpful links
        assert mock_open.call_count > 0


class TestSetupStep:
    """Test the SetupStep dataclass"""

    def test_setup_step_creation(self):
        """Test creating a SetupStep"""
        step = SetupStep(
            step_number=1,
            title="Test Step",
            description="Test description",
            instructions=["Step 1", "Step 2"],
            validation_function="test_validation",
            completed=False,
            error_message=None,
            help_links=[{"title": "Help", "url": "http://example.com"}],
            prerequisites=[0]
        )

        assert step.step_number == 1
        assert step.title == "Test Step"
        assert step.description == "Test description"
        assert len(step.instructions) == 2
        assert step.validation_function == "test_validation"
        assert step.completed is False
        assert step.error_message is None
        assert len(step.help_links) == 1
        assert step.prerequisites == [0]


class TestSetupProgress:
    """Test the SetupProgress dataclass"""

    def test_setup_progress_creation(self):
        """Test creating a SetupProgress"""
        progress = SetupProgress(
            current_step=2,
            total_steps=6,
            completed_steps=[1],
            failed_steps=[],
            warnings=["Warning message"],
            overall_progress_percentage=16.7,
            estimated_time_remaining_minutes=25,
            next_action="Complete Step 2"
        )

        assert progress.current_step == 2
        assert progress.total_steps == 6
        assert progress.completed_steps == [1]
        assert progress.failed_steps == []
        assert progress.warnings == ["Warning message"]
        assert progress.overall_progress_percentage == 16.7
        assert progress.estimated_time_remaining_minutes == 25
        assert progress.next_action == "Complete Step 2"