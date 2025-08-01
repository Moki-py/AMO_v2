"""
Enhanced Google Sheets configuration management system
"""

import os
import json
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .config import settings
from .logger import log_event


# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


@dataclass
class ConfigValidationResult:
    """Result of configuration validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    missing_configs: List[str]

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


@dataclass
class SpreadsheetInfo:
    """Information about a Google Spreadsheet"""
    spreadsheet_id: str
    title: str
    url: str
    sheets: List[Dict[str, Any]]
    permissions: Dict[str, bool]
    last_modified: Optional[datetime] = None


@dataclass
class PermissionTestResult:
    """Result of permission testing"""
    can_read: bool
    can_write: bool
    can_create_sheets: bool
    error_message: Optional[str] = None


class GoogleSheetsConfigManager:
    """Enhanced configuration manager for Google Sheets export functionality"""

    def __init__(self):
        self.credentials_path = 'credentials.json'
        self.token_path = 'token.json'
        self.creds: Optional[Credentials] = None

        # Required spreadsheet configurations
        self.required_spreadsheet_configs = {
            'leads': 'GOOGLE_SHEETS_LEADS_ID',
            'contacts': 'GOOGLE_SHEETS_CONTACTS_ID',
            'companies': 'GOOGLE_SHEETS_COMPANIES_ID',
            'events': 'GOOGLE_SHEETS_EVENTS_ID'
        }

    def validate_configuration(self) -> ConfigValidationResult:
        """
        Comprehensive validation of Google Sheets configuration

        Returns:
            ConfigValidationResult with validation status and details
        """
        errors = []
        warnings = []
        missing_configs = []

        log_event("sheets_config", "info", "Starting Google Sheets configuration validation")

        # 1. Check credentials file
        if not os.path.exists(self.credentials_path):
            errors.append(
                f"Google API credentials file '{self.credentials_path}' not found. "
                "Please follow the setup instructions to create this file."
            )
        else:
            # Validate credentials file format
            try:
                with open(self.credentials_path, 'r') as f:
                    creds_data = json.load(f)
                    if 'installed' not in creds_data and 'web' not in creds_data:
                        errors.append(
                            "Invalid credentials format. The credentials must be for a desktop or web application. "
                            "Please make sure you selected 'Desktop app' when creating the OAuth client ID."
                        )
            except json.JSONDecodeError:
                errors.append(
                    "Invalid credentials.json file. The file must be a valid JSON file. "
                    "Please download a new credentials file from the Google Cloud Console."
                )
            except Exception as e:
                errors.append(f"Error reading credentials file: {str(e)}")

        # 2. Check spreadsheet ID configurations
        spreadsheet_ids = {}  # Initialize to avoid potential unbound variable
        try:
            spreadsheet_ids = self._get_spreadsheet_ids()
            for entity_type, env_var in self.required_spreadsheet_configs.items():
                spreadsheet_id = spreadsheet_ids.get(entity_type)
                if not spreadsheet_id:
                    missing_configs.append(env_var)
                    errors.append(
                        f"Missing Google Sheets ID for {entity_type}. "
                        f"Please set {env_var} in your .env file."
                    )
                else:
                    # Validate spreadsheet ID format
                    if not self._is_valid_spreadsheet_id(spreadsheet_id):
                        errors.append(
                            f"Invalid spreadsheet ID format for {entity_type}: '{spreadsheet_id}'. "
                            "Spreadsheet IDs should be at least 5 characters long and contain only alphanumeric characters, "
                            "hyphens, and underscores."
                        )
        except Exception as e:
            errors.append(f"Error reading spreadsheet configuration: {str(e)}")

        # 3. Test OAuth token if credentials are available
        if not errors:  # Only test if basic config is valid
            try:
                self._get_credentials()
                if self.creds and self.creds.valid:
                    log_event("sheets_config", "info", "OAuth credentials are valid")
                else:
                    warnings.append(
                        "OAuth token is not available or expired. "
                        "You may need to re-authenticate when first using Google Sheets export."
                    )
            except Exception as e:
                warnings.append(f"Could not validate OAuth credentials: {str(e)}")

        # 4. Test spreadsheet accessibility if credentials are valid
        if not errors and self.creds and self.creds.valid:
            for entity_type, spreadsheet_id in spreadsheet_ids.items():
                if spreadsheet_id:
                    try:
                        spreadsheet_info = self.get_spreadsheet_info(spreadsheet_id)
                        log_event("sheets_config", "info",
                                f"Successfully accessed {entity_type} spreadsheet: {spreadsheet_info.title}")
                    except Exception as e:
                        error_msg = str(e)
                        # Улучшаем сообщения об ошибках
                        if "Access denied" in error_msg:
                            errors.append(
                                f"Cannot access {entity_type} spreadsheet (ID: {spreadsheet_id}): Access denied. "
                                "Please ensure the spreadsheet is shared with your Google account and you have Editor permissions."
                            )
                        elif "not found" in error_msg.lower():
                            errors.append(
                                f"Cannot access {entity_type} spreadsheet (ID: {spreadsheet_id}): Spreadsheet not found. "
                                "Please check the spreadsheet ID is correct and the spreadsheet exists."
                            )
                        else:
                            errors.append(
                                f"Cannot access {entity_type} spreadsheet (ID: {spreadsheet_id}): {error_msg}"
                            )

        is_valid = len(errors) == 0

        result = ConfigValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            missing_configs=missing_configs
        )

        # Log validation results
        if is_valid:
            log_event("sheets_config", "info", "Google Sheets configuration validation passed")
            if warnings:
                log_event("sheets_config", "warning", f"Validation warnings: {'; '.join(warnings)}")
        else:
            log_event("sheets_config", "error", f"Google Sheets configuration validation failed: {'; '.join(errors)}")

        return result

    def get_spreadsheet_info(self, spreadsheet_id: str) -> SpreadsheetInfo:
        """
        Get detailed information about a spreadsheet

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID

        Returns:
            SpreadsheetInfo object with spreadsheet details

        Raises:
            Exception: If spreadsheet cannot be accessed
        """
        if not self.creds or not self.creds.valid:
            self._get_credentials()

        try:
            service = build('sheets', 'v4', credentials=self.creds)

            # Get spreadsheet metadata
            spreadsheet_response = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

            # Validate response type
            if not isinstance(spreadsheet_response, dict):
                raise Exception(f"Invalid API response type: {type(spreadsheet_response)}, response: {spreadsheet_response}")

            spreadsheet = spreadsheet_response

            # Extract basic information
            if 'properties' not in spreadsheet:
                raise Exception(f"Missing 'properties' in spreadsheet response: {list(spreadsheet.keys())}")

            title = spreadsheet.get('properties', {}).get('title', 'Unknown')
            url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

            # Extract sheet information
            sheets = []
            for sheet in spreadsheet.get('sheets', []):
                sheet_props = sheet.get('properties', {})
                sheets.append({
                    'title': sheet_props.get('title', 'Unknown'),
                    'sheet_id': sheet_props.get('sheetId', 0),
                    'sheet_type': sheet_props.get('sheetType', 'GRID'),
                    'grid_properties': sheet_props.get('gridProperties', {})
                })

            # Test permissions
            permission_result = self.test_permissions(spreadsheet_id)
            permissions = {
                'can_read': permission_result.can_read,
                'can_write': permission_result.can_write,
                'can_create_sheets': permission_result.can_create_sheets
            }

            return SpreadsheetInfo(
                spreadsheet_id=spreadsheet_id,
                title=title,
                url=url,
                sheets=sheets,
                permissions=permissions
            )

        except HttpError as e:
            error_details = e.error_details[0] if e.error_details else {}
            # Handle case where error_details might be a string instead of a dict
            if isinstance(error_details, dict):
                reason = error_details.get('reason', 'unknown')
            else:
                reason = 'unknown'

            if e.resp.status == 403:
                if reason == 'forbidden':
                    raise Exception(
                        f"Access denied to spreadsheet. Please ensure:\n"
                        f"1. The spreadsheet ID is correct: {spreadsheet_id}\n"
                        f"2. The spreadsheet is shared with your Google account\n"
                        f"3. You have at least 'Editor' permissions"
                    )
                else:
                    raise Exception(f"Permission denied: {reason}")
            elif e.resp.status == 404:
                raise Exception(
                    f"Spreadsheet not found. Please check:\n"
                    f"1. The spreadsheet ID is correct: {spreadsheet_id}\n"
                    f"2. The spreadsheet exists and is not deleted\n"
                    f"3. You have access to the spreadsheet"
                )
            else:
                raise Exception(f"Google Sheets API error: {str(e)}")

        except Exception as e:
            raise Exception(f"Failed to access spreadsheet {spreadsheet_id}: {str(e)}")

    def test_permissions(self, spreadsheet_id: str) -> PermissionTestResult:
        """
        Test permissions for a specific spreadsheet

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID

        Returns:
            PermissionTestResult with permission details
        """
        if not self.creds or not self.creds.valid:
            self._get_credentials()

        can_read = False
        can_write = False
        can_create_sheets = False
        error_message = None

        try:
            service = build('sheets', 'v4', credentials=self.creds)

            # Test read permission
            try:
                service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                can_read = True
                log_event("sheets_config", "debug", f"Read permission confirmed for {spreadsheet_id}")
            except HttpError as e:
                error_message = f"Cannot read spreadsheet: {str(e)}"
                return PermissionTestResult(
                    can_read=False,
                    can_write=False,
                    can_create_sheets=False,
                    error_message=error_message
                )

            # Test write permission by attempting to read values (less intrusive than writing)
            try:
                # Try to get values from A1 - this requires read permission
                service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range='A1'
                ).execute()

                # If we can read, test write by attempting a batch update with no changes
                # This tests write permission without actually modifying data
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': []}  # Empty request list
                ).execute()
                can_write = True
                log_event("sheets_config", "debug", f"Write permission confirmed for {spreadsheet_id}")

            except HttpError as e:
                if e.resp.status == 403:
                    error_message = "Write permission denied. Please ensure you have 'Editor' access to the spreadsheet."
                else:
                    error_message = f"Cannot write to spreadsheet: {str(e)}"

            # Test sheet creation permission
            if can_write:
                try:
                    # Test by attempting to create a temporary sheet and immediately delete it
                    test_sheet_name = f"_test_sheet_{int(time.time())}"

                    # Create test sheet
                    create_request = {
                        'requests': [{
                            'addSheet': {
                                'properties': {
                                    'title': test_sheet_name
                                }
                            }
                        }]
                    }

                    response = service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=create_request
                    ).execute()

                    # Get the sheet ID from response
                    sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']

                    # Immediately delete the test sheet
                    delete_request = {
                        'requests': [{
                            'deleteSheet': {
                                'sheetId': sheet_id
                            }
                        }]
                    }

                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=delete_request
                    ).execute()

                    can_create_sheets = True
                    log_event("sheets_config", "debug", f"Sheet creation permission confirmed for {spreadsheet_id}")

                except HttpError as e:
                    if e.resp.status == 403:
                        # Can write but can't create sheets - this is still acceptable
                        log_event("sheets_config", "warning",
                                f"Cannot create new sheets in {spreadsheet_id}, but can write to existing sheets")
                    else:
                        log_event("sheets_config", "warning",
                                f"Error testing sheet creation for {spreadsheet_id}: {str(e)}")
                except Exception as e:
                    log_event("sheets_config", "warning",
                            f"Error testing sheet creation for {spreadsheet_id}: {str(e)}")

        except Exception as e:
            error_message = f"Permission test failed: {str(e)}"

        return PermissionTestResult(
            can_read=can_read,
            can_write=can_write,
            can_create_sheets=can_create_sheets,
            error_message=error_message
        )

    def setup_guided_configuration(self) -> Dict[str, Any]:
        """
        Provide guided setup assistance for Google Sheets configuration

        Returns:
            Dictionary with setup instructions and current status
        """
        setup_status = {
            'steps': [],
            'current_status': {},
            'next_actions': []
        }

        # Step 1: Google Cloud Console setup
        setup_status['steps'].append({
            'step': 1,
            'title': 'Google Cloud Console Setup',
            'description': 'Set up Google Cloud project and enable APIs',
            'instructions': [
                'Go to https://console.cloud.google.com/',
                'Create a new project or select an existing one',
                'Enable the Google Sheets API for your project',
                'Go to "APIs & Services" > "Credentials"',
                'Click "Create Credentials" > "OAuth client ID"',
                'Choose "Desktop app" as the application type',
                'Download the JSON file and save it as "credentials.json" in the project root'
            ],
            'completed': os.path.exists(self.credentials_path)
        })

        # Step 2: Spreadsheet creation and sharing
        spreadsheet_ids = self._get_spreadsheet_ids()
        missing_spreadsheets = []

        for entity_type, env_var in self.required_spreadsheet_configs.items():
            if not spreadsheet_ids.get(entity_type):
                missing_spreadsheets.append((entity_type, env_var))

        setup_status['steps'].append({
            'step': 2,
            'title': 'Create and Configure Spreadsheets',
            'description': 'Create Google Sheets for each entity type',
            'instructions': [
                'Create separate Google Sheets for each entity type:',
                '  - Leads/Deals spreadsheet',
                '  - Contacts spreadsheet',
                '  - Companies spreadsheet',
                '  - Events spreadsheet',
                'Share each spreadsheet with your Google account (Editor permissions)',
                'Copy the spreadsheet IDs from the URLs',
                'Add the IDs to your .env file using the required environment variables'
            ],
            'completed': len(missing_spreadsheets) == 0,
            'missing_configs': missing_spreadsheets
        })

        # Step 3: Environment configuration
        setup_status['steps'].append({
            'step': 3,
            'title': 'Environment Configuration',
            'description': 'Configure environment variables',
            'instructions': [
                'Update your .env file with the following variables:',
                *[f'  {env_var}=<your_spreadsheet_id>' for _, env_var in missing_spreadsheets],
                'Ensure all spreadsheet IDs are correctly formatted (44 characters)'
            ],
            'completed': len(missing_spreadsheets) == 0
        })

        # Step 4: Test configuration
        validation_result = self.validate_configuration()
        setup_status['steps'].append({
            'step': 4,
            'title': 'Test Configuration',
            'description': 'Validate the complete setup',
            'instructions': [
                'Run the configuration validation',
                'Authenticate with Google when prompted',
                'Verify all spreadsheets are accessible',
                'Test export functionality'
            ],
            'completed': validation_result.is_valid,
            'validation_errors': validation_result.errors,
            'validation_warnings': validation_result.warnings
        })

        # Determine current status and next actions
        completed_steps = sum(1 for step in setup_status['steps'] if step['completed'])
        setup_status['current_status'] = {
            'completed_steps': completed_steps,
            'total_steps': len(setup_status['steps']),
            'progress_percentage': (completed_steps / len(setup_status['steps'])) * 100
        }

        # Determine next actions
        if not os.path.exists(self.credentials_path):
            setup_status['next_actions'].append(
                'Create Google Cloud project and download credentials.json file'
            )
        elif missing_spreadsheets:
            setup_status['next_actions'].append(
                f'Configure missing spreadsheet IDs: {", ".join([env_var for _, env_var in missing_spreadsheets])}'
            )
        elif not validation_result.is_valid:
            setup_status['next_actions'].append(
                'Fix configuration errors: ' + '; '.join(validation_result.errors[:3])
            )
        else:
            setup_status['next_actions'].append('Configuration is complete! You can now use Google Sheets export.')

        return setup_status

    def _get_spreadsheet_ids(self) -> Dict[str, Optional[str]]:
        """Get configured spreadsheet IDs from settings"""
        spreadsheet_ids = {
            'leads': settings.google_sheets_leads_id,
            'contacts': settings.google_sheets_contacts_id,
            'companies': settings.google_sheets_companies_id,
            'events': settings.google_sheets_events_id
        }

        # Фильтруем пустые значения и приводим к строковому типу
        filtered_ids = {}
        for entity_type, spreadsheet_id in spreadsheet_ids.items():
            if spreadsheet_id and isinstance(spreadsheet_id, str) and spreadsheet_id.strip():
                filtered_ids[entity_type] = spreadsheet_id.strip()
            else:
                filtered_ids[entity_type] = None

        return filtered_ids

    def _is_valid_spreadsheet_id(self, spreadsheet_id: str) -> bool:
        """
        Validate spreadsheet ID format

        Args:
            spreadsheet_id: The spreadsheet ID to validate

        Returns:
            True if the ID format is valid
        """
        if not spreadsheet_id or not isinstance(spreadsheet_id, str):
            return False

        # Google Sheets IDs are typically 44 characters long
        # and contain alphanumeric characters, hyphens, and underscores
        # ВРЕМЕННО: Допускаем ID любой длины для совместимости
        if len(spreadsheet_id) < 5:  # Минимальная длина для разумности
            return False

        # Check for valid characters
        valid_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_')
        return all(c in valid_chars for c in spreadsheet_id)

    def _get_credentials(self) -> None:
        """Get or refresh Google API credentials"""
        if not os.path.exists(self.credentials_path):
            raise Exception(
                f"Google API credentials file '{self.credentials_path}' not found. "
                "Please follow the setup instructions to create this file."
            )

        # Load existing token if available
        if os.path.exists(self.token_path):
            try:
                self.creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)

                # Proactively refresh token if it's close to expiring (within 5 minutes)
                if self.creds and self.creds.valid and self.creds.expiry:
                    from datetime import datetime, timedelta
                    time_until_expiry = self.creds.expiry - datetime.utcnow()
                    if time_until_expiry < timedelta(minutes=5):
                        log_event("sheets_config", "info", f"Token expires in {time_until_expiry}, refreshing proactively")
                        if self.creds.refresh_token:
                            refresh_success = self._refresh_token_with_retry()
                            if not refresh_success:
                                log_event("sheets_config", "warning", "Proactive refresh failed, token may expire soon")
                        else:
                            log_event("sheets_config", "warning", "No refresh token available for proactive refresh")

            except Exception as e:
                log_event("sheets_config", "warning", f"Error loading existing token: {e}")
                # If token is invalid, delete it
                if os.path.exists(self.token_path):
                    os.remove(self.token_path)
                self.creds = None

        # Refresh or get new credentials
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                refresh_success = self._refresh_token_with_retry()
                if not refresh_success:
                    log_event("sheets_config", "warning", "Token refresh failed after retries, will attempt new OAuth flow")
                    # If refresh fails completely, delete the token and start fresh
                    if os.path.exists(self.token_path):
                        os.remove(self.token_path)
                    self.creds = None

            if not self.creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, SCOPES)
                    self.creds = flow.run_local_server(port=0)
                    log_event("sheets_config", "info", "Successfully obtained new OAuth token")
                except Exception as e:
                    raise Exception(
                        f"Error during OAuth flow: {str(e)}\n"
                        "Please make sure you have:\n"
                        "1. Enabled the Google Sheets API in your project\n"
                        "2. Created OAuth 2.0 credentials for a desktop application\n"
                        "3. Added your email as a test user in the OAuth consent screen"
                    )

            # Save the credentials for the next run
            if self.creds:
                try:
                    with open(self.token_path, 'w') as token:
                        token.write(self.creds.to_json())
                    log_event("sheets_config", "info", "Successfully saved OAuth token")
                except Exception as e:
                    log_event("sheets_config", "warning", f"Error saving token: {e}")
            else:
                log_event("sheets_config", "warning", "No credentials available to save")

    def _refresh_token_with_retry(self, max_retries: int = 3) -> bool:
        """
        Refresh OAuth token with retry logic and enhanced error handling

        Returns:
            bool: True if refresh succeeded, False if all retries failed
        """
        import time
        from google.auth.exceptions import RefreshError

        for attempt in range(max_retries):
            try:
                log_event("sheets_config", "info", f"Attempting token refresh (attempt {attempt + 1}/{max_retries})")

                # Ensure we have credentials to refresh
                if not self.creds:
                    log_event("sheets_config", "error", "No credentials available to refresh")
                    return False

                # Attempt to refresh the token
                self.creds.refresh(Request())

                # Validate that the refresh actually worked
                if self.creds.valid and not self.creds.expired:
                    log_event("sheets_config", "info", "Successfully refreshed OAuth token")

                    # Save the refreshed token immediately
                    try:
                        with open(self.token_path, 'w') as token:
                            token.write(self.creds.to_json())
                        log_event("sheets_config", "info", "Successfully saved refreshed OAuth token")
                    except Exception as save_error:
                        log_event("sheets_config", "warning", f"Token refreshed but failed to save: {save_error}")

                    return True
                else:
                    log_event("sheets_config", "warning", f"Token refresh appeared to succeed but token is still invalid (attempt {attempt + 1})")

            except RefreshError as e:
                log_event("sheets_config", "warning", f"OAuth refresh error (attempt {attempt + 1}): {e}")
                if "invalid_grant" in str(e).lower():
                    log_event("sheets_config", "error", "Refresh token is invalid or expired - need full re-authorization")
                    return False  # Don't retry for invalid_grant errors

            except Exception as e:
                log_event("sheets_config", "warning", f"Unexpected error during token refresh (attempt {attempt + 1}): {e}")

            # Wait before retrying (exponential backoff)
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 1s, 2s, 4s
                log_event("sheets_config", "info", f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)

        log_event("sheets_config", "error", f"Token refresh failed after {max_retries} attempts")
        return False

    def get_configuration_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current Google Sheets configuration

        Returns:
            Dictionary with configuration summary
        """
        summary = {
            'credentials_file_exists': os.path.exists(self.credentials_path),
            'token_file_exists': os.path.exists(self.token_path),
            'configured_spreadsheets': {},
            'validation_status': None
        }

        # Check configured spreadsheets
        spreadsheet_ids = self._get_spreadsheet_ids()
        for entity_type, spreadsheet_id in spreadsheet_ids.items():
            summary['configured_spreadsheets'][entity_type] = {
                'configured': bool(spreadsheet_id),
                'spreadsheet_id': spreadsheet_id,
                'valid_format': self._is_valid_spreadsheet_id(spreadsheet_id) if spreadsheet_id else False
            }

        # Get validation status
        try:
            validation_result = self.validate_configuration()
            summary['validation_status'] = {
                'is_valid': validation_result.is_valid,
                'error_count': len(validation_result.errors),
                'warning_count': len(validation_result.warnings),
                'errors': validation_result.errors,
                'warnings': validation_result.warnings
            }
        except Exception as e:
            summary['validation_status'] = {
                'is_valid': False,
                'error_count': 1,
                'warning_count': 0,
                'errors': [f"Validation failed: {str(e)}"],
                'warnings': []
            }

        return summary