"""
Guided setup assistance for Google Sheets configuration
"""

import os
import json
import webbrowser
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime

from ..core.config import settings
from ..core.logger import log_event
from ..core.google_sheets_config import GoogleSheetsConfigManager
from .config_validator import ConfigurationValidator


@dataclass
class SetupStep:
    """Represents a single setup step"""
    step_number: int
    title: str
    description: str
    instructions: List[str]
    validation_function: Optional[str] = None
    completed: bool = False
    error_message: Optional[str] = None
    help_links: Optional[List[Dict[str, str]]] = None
    prerequisites: Optional[List[int]] = None  # Step numbers that must be completed first


@dataclass
class SetupProgress:
    """Tracks overall setup progress"""
    current_step: int
    total_steps: int
    completed_steps: List[int]
    failed_steps: List[int]
    warnings: List[str]
    overall_progress_percentage: float
    estimated_time_remaining_minutes: int
    next_action: str


class GoogleSheetsSetupWizard:
    """Comprehensive setup wizard for Google Sheets configuration"""

    def __init__(self):
        self.config_manager = GoogleSheetsConfigManager()
        self.validator = ConfigurationValidator()
        self.setup_steps = self._initialize_setup_steps()
        self.progress = None

    def _initialize_setup_steps(self) -> List[SetupStep]:
        """Initialize all setup steps"""
        return [
            SetupStep(
                step_number=1,
                title="Google Cloud Console Setup",
                description="Create a Google Cloud project and enable the Google Sheets API",
                instructions=[
                    "Go to the Google Cloud Console (https://console.cloud.google.com/)",
                    "Create a new project or select an existing one",
                    "Navigate to 'APIs & Services' > 'Library'",
                    "Search for 'Google Sheets API' and click on it",
                    "Click 'Enable' to enable the API for your project",
                    "Wait for the API to be enabled (this may take a few minutes)"
                ],
                validation_function="validate_google_cloud_setup",
                help_links=[
                    {
                        "title": "Google Cloud Console",
                        "url": "https://console.cloud.google.com/"
                    },
                    {
                        "title": "Google Sheets API Documentation",
                        "url": "https://developers.google.com/sheets/api"
                    }
                ]
            ),

            SetupStep(
                step_number=2,
                title="OAuth Credentials Creation",
                description="Create OAuth 2.0 credentials for desktop application",
                instructions=[
                    "In the Google Cloud Console, go to 'APIs & Services' > 'Credentials'",
                    "Click 'Create Credentials' > 'OAuth client ID'",
                    "If prompted, configure the OAuth consent screen first:",
                    "  - Choose 'External' user type (unless you have a Google Workspace)",
                    "  - Fill in the required fields (App name, User support email, Developer email)",
                    "  - Add your email address as a test user in the 'Test users' section",
                    "For the OAuth client ID:",
                    "  - Choose 'Desktop app' as the application type",
                    "  - Give it a name (e.g., 'AmoCRM Exporter')",
                    "  - Click 'Create'",
                    "Download the JSON file and save it as 'credentials.json' in your project root directory"
                ],
                validation_function="validate_oauth_credentials",
                prerequisites=[1],
                help_links=[
                    {
                        "title": "OAuth 2.0 Setup Guide",
                        "url": "https://developers.google.com/identity/protocols/oauth2"
                    },
                    {
                        "title": "OAuth Consent Screen Setup",
                        "url": "https://support.google.com/cloud/answer/10311615"
                    }
                ]
            ),

            SetupStep(
                step_number=3,
                title="Create Google Sheets",
                description="Create separate Google Sheets for each entity type",
                instructions=[
                    "Go to Google Sheets (https://sheets.google.com/)",
                    "Create a new spreadsheet for Leads/Deals data",
                    "  - Name it something like 'AmoCRM Leads Export'",
                    "  - Copy the spreadsheet ID from the URL (the long string between '/d/' and '/edit')",
                    "Create a new spreadsheet for Contacts data",
                    "  - Name it something like 'AmoCRM Contacts Export'",
                    "  - Copy the spreadsheet ID from the URL",
                    "Create a new spreadsheet for Companies data",
                    "  - Name it something like 'AmoCRM Companies Export'",
                    "  - Copy the spreadsheet ID from the URL",
                    "Create a new spreadsheet for Events data (optional)",
                    "  - Name it something like 'AmoCRM Events Export'",
                    "  - Copy the spreadsheet ID from the URL",
                    "Make sure all spreadsheets are accessible to your Google account"
                ],
                validation_function="validate_spreadsheets_created",
                prerequisites=[2],
                help_links=[
                    {
                        "title": "Google Sheets",
                        "url": "https://sheets.google.com/"
                    },
                    {
                        "title": "How to Find Spreadsheet ID",
                        "url": "https://developers.google.com/sheets/api/guides/concepts#spreadsheet_id"
                    }
                ]
            ),

            SetupStep(
                step_number=4,
                title="Configure Environment Variables",
                description="Set up environment variables with your spreadsheet IDs",
                instructions=[
                    "Open your .env file (create one if it doesn't exist)",
                    "Add the following lines with your actual spreadsheet IDs:",
                    "  GOOGLE_SHEETS_LEADS_ID=your_leads_spreadsheet_id_here",
                    "  GOOGLE_SHEETS_CONTACTS_ID=your_contacts_spreadsheet_id_here",
                    "  GOOGLE_SHEETS_COMPANIES_ID=your_companies_spreadsheet_id_here",
                    "  GOOGLE_SHEETS_EVENTS_ID=your_events_spreadsheet_id_here",
                    "Replace 'your_*_spreadsheet_id_here' with the actual IDs you copied in step 3",
                    "Save the .env file",
                    "Restart the application to load the new environment variables"
                ],
                validation_function="validate_environment_variables",
                prerequisites=[3],
                help_links=[
                    {
                        "title": "Environment Variables Guide",
                        "url": "https://en.wikipedia.org/wiki/Environment_variable"
                    }
                ]
            ),

            SetupStep(
                step_number=5,
                title="Test Authentication",
                description="Authenticate with Google and test the connection",
                instructions=[
                    "Run the configuration validation tool",
                    "When prompted, authorize the application in your web browser",
                    "Grant the requested permissions for Google Sheets access",
                    "Verify that authentication was successful",
                    "Test the connection to your spreadsheets"
                ],
                validation_function="validate_authentication",
                prerequisites=[4],
                help_links=[
                    {
                        "title": "OAuth 2.0 Authorization",
                        "url": "https://developers.google.com/identity/protocols/oauth2/web-server"
                    }
                ]
            ),

            SetupStep(
                step_number=6,
                title="Final Validation",
                description="Run comprehensive validation to ensure everything is working",
                instructions=[
                    "Run the comprehensive configuration validation",
                    "Verify all tests pass",
                    "Test export functionality with a small dataset",
                    "Check that data appears correctly in your Google Sheets",
                    "Review any warnings or recommendations"
                ],
                validation_function="validate_complete_setup",
                prerequisites=[5]
            )
        ]

    def get_setup_progress(self) -> SetupProgress:
        """Get current setup progress"""
        if not self.progress:
            self.progress = self._calculate_progress()
        return self.progress

    def _calculate_progress(self) -> SetupProgress:
        """Calculate current setup progress"""
        completed_steps = []
        failed_steps = []
        warnings = []
        current_step = 1

        # Validate each step
        for step in self.setup_steps:
            try:
                if step.validation_function:
                    validation_method = getattr(self, step.validation_function)
                    is_completed, error_message = validation_method()

                    if is_completed:
                        completed_steps.append(step.step_number)
                        step.completed = True
                        step.error_message = None
                    else:
                        step.completed = False
                        step.error_message = error_message
                        if error_message:
                            failed_steps.append(step.step_number)

                        # Set current step to first incomplete step
                        if current_step == step.step_number - 1 or current_step == 1:
                            current_step = step.step_number
                        break
                else:
                    # Steps without validation are considered incomplete by default
                    step.completed = False
                    if current_step == step.step_number - 1 or current_step == 1:
                        current_step = step.step_number
                    break

            except Exception as e:
                step.completed = False
                step.error_message = f"Validation error: {str(e)}"
                failed_steps.append(step.step_number)
                warnings.append(f"Step {step.step_number} validation failed: {str(e)}")

        # Calculate progress percentage
        progress_percentage = (len(completed_steps) / len(self.setup_steps)) * 100

        # Estimate time remaining (rough estimate: 5 minutes per remaining step)
        remaining_steps = len(self.setup_steps) - len(completed_steps)
        estimated_time = remaining_steps * 5

        # Determine next action
        if len(completed_steps) == len(self.setup_steps):
            next_action = "Setup complete! You can now use Google Sheets export."
        elif failed_steps:
            failed_step = min(failed_steps)
            next_action = f"Fix issues in Step {failed_step}: {self.setup_steps[failed_step-1].title}"
        else:
            next_action = f"Complete Step {current_step}: {self.setup_steps[current_step-1].title}"

        return SetupProgress(
            current_step=current_step,
            total_steps=len(self.setup_steps),
            completed_steps=completed_steps,
            failed_steps=failed_steps,
            warnings=warnings,
            overall_progress_percentage=progress_percentage,
            estimated_time_remaining_minutes=estimated_time,
            next_action=next_action
        )

    def get_step_details(self, step_number: int) -> Optional[SetupStep]:
        """Get details for a specific step"""
        for step in self.setup_steps:
            if step.step_number == step_number:
                return step
        return None

    def get_all_steps(self) -> List[SetupStep]:
        """Get all setup steps with current status"""
        # Refresh progress to update step statuses
        self.progress = self._calculate_progress()
        return self.setup_steps

    def validate_google_cloud_setup(self) -> Tuple[bool, Optional[str]]:
        """Validate that Google Cloud Console setup is complete"""
        try:
            # We can't directly validate Google Cloud setup, but we can check
            # if the next step (credentials) is ready
            if os.path.exists(self.config_manager.credentials_path):
                return True, None
            else:
                return False, "Credentials file not found. Complete Google Cloud Console setup first."
        except Exception as e:
            return False, f"Error validating Google Cloud setup: {str(e)}"

    def validate_oauth_credentials(self) -> Tuple[bool, Optional[str]]:
        """Validate OAuth credentials file"""
        try:
            if not os.path.exists(self.config_manager.credentials_path):
                return False, "Credentials file 'credentials.json' not found in project root."

            # Validate credentials file format
            with open(self.config_manager.credentials_path, 'r') as f:
                try:
                    creds_data = json.load(f)
                    if 'installed' not in creds_data and 'web' not in creds_data:
                        return False, "Invalid credentials format. Must be for desktop or web application."

                    # Check for required fields
                    if 'installed' in creds_data:
                        installed = creds_data['installed']
                        required_fields = ['client_id', 'client_secret', 'auth_uri', 'token_uri']
                        missing_fields = [field for field in required_fields if field not in installed]
                        if missing_fields:
                            return False, f"Missing required fields in credentials: {', '.join(missing_fields)}"

                    return True, None

                except json.JSONDecodeError:
                    return False, "Credentials file contains invalid JSON."

        except Exception as e:
            return False, f"Error validating credentials: {str(e)}"

    def validate_spreadsheets_created(self) -> Tuple[bool, Optional[str]]:
        """Validate that spreadsheets are created and configured"""
        try:
            # Check if environment variables are set
            spreadsheet_ids = self.config_manager._get_spreadsheet_ids()
            missing_ids = []

            for entity_type, spreadsheet_id in spreadsheet_ids.items():
                if not spreadsheet_id:
                    missing_ids.append(entity_type)
                elif not self.config_manager._is_valid_spreadsheet_id(spreadsheet_id):
                    return False, f"Invalid spreadsheet ID format for {entity_type}: {spreadsheet_id}"

            if missing_ids:
                return False, f"Missing spreadsheet IDs for: {', '.join(missing_ids)}"

            return True, None

        except Exception as e:
            return False, f"Error validating spreadsheets: {str(e)}"

    def validate_environment_variables(self) -> Tuple[bool, Optional[str]]:
        """Validate environment variables configuration"""
        try:
            # Check if .env file exists
            env_file = Path('.env')
            if not env_file.exists():
                return False, ".env file not found. Create one with your spreadsheet IDs."

            # Validate spreadsheet IDs
            spreadsheet_ids = self.config_manager._get_spreadsheet_ids()
            issues = []

            for entity_type, spreadsheet_id in spreadsheet_ids.items():
                if not spreadsheet_id:
                    issues.append(f"Missing {entity_type.upper()}_ID")
                elif not self.config_manager._is_valid_spreadsheet_id(spreadsheet_id):
                    issues.append(f"Invalid format for {entity_type.upper()}_ID")

            if issues:
                return False, f"Environment variable issues: {'; '.join(issues)}"

            return True, None

        except Exception as e:
            return False, f"Error validating environment variables: {str(e)}"

    def validate_authentication(self) -> Tuple[bool, Optional[str]]:
        """Validate authentication setup"""
        try:
            # Try to get credentials
            self.config_manager._get_credentials()

            if not self.config_manager.creds or not self.config_manager.creds.valid:
                return False, "Authentication failed. Please complete the OAuth flow."

            return True, None

        except Exception as e:
            return False, f"Authentication error: {str(e)}"

    def validate_complete_setup(self) -> Tuple[bool, Optional[str]]:
        """Validate complete setup"""
        try:
            # Run comprehensive validation
            validation_results = self.validator.run_comprehensive_validation()

            if validation_results['overall_status'] == 'ready':
                return True, None
            elif validation_results['overall_status'] == 'configured_with_warnings':
                warnings = validation_results.get('basic_validation', {}).get('warnings', [])
                return True, f"Setup complete with warnings: {'; '.join(warnings[:2])}"
            else:
                errors = validation_results.get('basic_validation', {}).get('errors', [])
                return False, f"Setup validation failed: {'; '.join(errors[:2])}"

        except Exception as e:
            return False, f"Error during final validation: {str(e)}"

    def get_troubleshooting_guide(self) -> Dict[str, Any]:
        """Get comprehensive troubleshooting guide"""
        return {
            "common_problems": [
                {
                    "problem": "Credentials file not found",
                    "symptoms": [
                        "Error: 'credentials.json' not found",
                        "Authentication fails immediately"
                    ],
                    "solutions": [
                        "Download credentials.json from Google Cloud Console",
                        "Make sure the file is in your project root directory",
                        "Check that the file name is exactly 'credentials.json'"
                    ],
                    "help_links": [
                        {
                            "title": "Download OAuth Credentials",
                            "url": "https://developers.google.com/workspace/guides/create-credentials#oauth-client-id"
                        }
                    ]
                },
                {
                    "problem": "Invalid credentials format",
                    "symptoms": [
                        "Error: 'Invalid credentials format'",
                        "Missing required fields in credentials"
                    ],
                    "solutions": [
                        "Ensure you selected 'Desktop app' when creating OAuth client ID",
                        "Re-download the credentials file from Google Cloud Console",
                        "Don't edit the credentials.json file manually"
                    ]
                },
                {
                    "problem": "Spreadsheet access denied",
                    "symptoms": [
                        "Error: 'Access denied to spreadsheet'",
                        "HTTP 403 Forbidden errors"
                    ],
                    "solutions": [
                        "Make sure the spreadsheet is shared with your Google account",
                        "Verify you have 'Editor' permissions on the spreadsheet",
                        "Check that the spreadsheet ID is correct",
                        "Ensure the spreadsheet hasn't been deleted"
                    ]
                },
                {
                    "problem": "OAuth consent screen issues",
                    "symptoms": [
                        "Error: 'This app isn't verified'",
                        "OAuth flow fails or shows warnings"
                    ],
                    "solutions": [
                        "Add your email as a test user in the OAuth consent screen",
                        "Choose 'External' user type if you don't have Google Workspace",
                        "Fill in all required fields in the consent screen configuration",
                        "Click 'Advanced' and 'Go to [App Name] (unsafe)' if you see the unverified app warning"
                    ]
                },
                {
                    "problem": "Environment variables not loading",
                    "symptoms": [
                        "Spreadsheet IDs showing as None or empty",
                        "Configuration validation fails"
                    ],
                    "solutions": [
                        "Restart the application after updating .env file",
                        "Check that .env file is in the project root directory",
                        "Verify environment variable names match exactly",
                        "Make sure there are no spaces around the = sign in .env file"
                    ]
                },
                {
                    "problem": "API quota exceeded",
                    "symptoms": [
                        "HTTP 429 Too Many Requests errors",
                        "Quota exceeded messages"
                    ],
                    "solutions": [
                        "Wait a few minutes before retrying",
                        "Check your Google Cloud Console for quota limits",
                        "Consider requesting quota increases if needed",
                        "Reduce batch sizes in export operations"
                    ]
                }
            ],
            "diagnostic_commands": [
                {
                    "command": "python -m amocrm_exporter.cli.config_validator_cli --report",
                    "description": "Run comprehensive configuration validation"
                },
                {
                    "command": "python -m amocrm_exporter.cli.config_validator_cli --test connection",
                    "description": "Test Google Sheets API connection"
                },
                {
                    "command": "python -m amocrm_exporter.cli.config_validator_cli --test oauth",
                    "description": "Test OAuth authentication"
                }
            ],
            "helpful_links": [
                {
                    "title": "Google Sheets API Documentation",
                    "url": "https://developers.google.com/sheets/api",
                    "description": "Official API documentation"
                },
                {
                    "title": "OAuth 2.0 Setup Guide",
                    "url": "https://developers.google.com/identity/protocols/oauth2",
                    "description": "Complete OAuth setup guide"
                },
                {
                    "title": "Google Cloud Console",
                    "url": "https://console.cloud.google.com/",
                    "description": "Manage your Google Cloud projects"
                },
                {
                    "title": "Google Sheets",
                    "url": "https://sheets.google.com/",
                    "description": "Create and manage spreadsheets"
                }
            ]
        }

    def open_helpful_links(self, step_number: Optional[int] = None):
        """Open helpful links in the default web browser"""
        try:
            if step_number:
                step = self.get_step_details(step_number)
                if step and step.help_links:
                    for link in step.help_links:
                        webbrowser.open(link['url'])
                        log_event("setup_wizard", "info", f"Opened help link: {link['title']}")
            else:
                # Open general helpful links
                troubleshooting = self.get_troubleshooting_guide()
                for link in troubleshooting['helpful_links']:
                    webbrowser.open(link['url'])
                    log_event("setup_wizard", "info", f"Opened help link: {link['title']}")

        except Exception as e:
            log_event("setup_wizard", "error", f"Error opening help links: {str(e)}")

    def generate_setup_report(self) -> str:
        """Generate a comprehensive setup report"""
        progress = self.get_setup_progress()
        steps = self.get_all_steps()

        report_lines = [
            "=" * 60,
            "GOOGLE SHEETS SETUP WIZARD REPORT",
            "=" * 60,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Overall Progress: {progress.overall_progress_percentage:.1f}%",
            f"Completed Steps: {len(progress.completed_steps)}/{progress.total_steps}",
            f"Current Step: {progress.current_step}",
            f"Next Action: {progress.next_action}",
            ""
        ]

        if progress.estimated_time_remaining_minutes > 0:
            report_lines.append(f"Estimated Time Remaining: {progress.estimated_time_remaining_minutes} minutes")
            report_lines.append("")

        # Step details
        report_lines.append("SETUP STEPS:")
        for step in steps:
            status = "✓ COMPLETED" if step.completed else "○ PENDING"
            if step.error_message:
                status = "✗ FAILED"

            report_lines.append(f"  {step.step_number}. {step.title}: {status}")

            if step.error_message:
                report_lines.append(f"     Error: {step.error_message}")

            if not step.completed and step.step_number == progress.current_step:
                report_lines.append(f"     Description: {step.description}")
                report_lines.append("     Instructions:")
                for instruction in step.instructions[:3]:  # Show first 3 instructions
                    report_lines.append(f"       - {instruction}")
                if len(step.instructions) > 3:
                    report_lines.append(f"       ... and {len(step.instructions) - 3} more")

        report_lines.append("")

        # Warnings
        if progress.warnings:
            report_lines.append("WARNINGS:")
            for warning in progress.warnings:
                report_lines.append(f"  - {warning}")
            report_lines.append("")

        # Next steps
        if progress.failed_steps:
            report_lines.append("FAILED STEPS TO FIX:")
            for step_num in progress.failed_steps:
                step = self.get_step_details(step_num)
                if step:
                    report_lines.append(f"  {step_num}. {step.title}")
                    if step.error_message:
                        report_lines.append(f"     Error: {step.error_message}")
            report_lines.append("")

        report_lines.append("=" * 60)

        return "\n".join(report_lines)

    def get_step_by_step_guide(self, step_number: int) -> Dict[str, Any]:
        """Get detailed step-by-step guide for a specific step"""
        step = self.get_step_details(step_number)
        if not step:
            return {"error": f"Step {step_number} not found"}

        # Check prerequisites
        prerequisite_issues = []
        if step.prerequisites:
            for prereq_step in step.prerequisites:
                prereq = self.get_step_details(prereq_step)
                if prereq and not prereq.completed:
                    prerequisite_issues.append(f"Step {prereq_step}: {prereq.title}")

        return {
            "step": {
                "number": step.step_number,
                "title": step.title,
                "description": step.description,
                "completed": step.completed,
                "error_message": step.error_message
            },
            "prerequisites": {
                "required": step.prerequisites or [],
                "issues": prerequisite_issues
            },
            "instructions": step.instructions,
            "help_links": step.help_links or [],
            "validation": {
                "function": step.validation_function,
                "can_validate": step.validation_function is not None
            },
            "estimated_time_minutes": self._estimate_step_time(step_number),
            "difficulty_level": self._get_step_difficulty(step_number)
        }

    def _estimate_step_time(self, step_number: int) -> int:
        """Estimate time required for a step in minutes"""
        time_estimates = {
            1: 10,  # Google Cloud Console setup
            2: 15,  # OAuth credentials creation
            3: 10,  # Create spreadsheets
            4: 5,   # Configure environment variables
            5: 5,   # Test authentication
            6: 5    # Final validation
        }
        return time_estimates.get(step_number, 10)

    def _get_step_difficulty(self, step_number: int) -> str:
        """Get difficulty level for a step"""
        difficulty_levels = {
            1: "Medium",    # Google Cloud Console setup
            2: "Hard",      # OAuth credentials creation
            3: "Easy",      # Create spreadsheets
            4: "Easy",      # Configure environment variables
            5: "Easy",      # Test authentication
            6: "Easy"       # Final validation
        }
        return difficulty_levels.get(step_number, "Medium")