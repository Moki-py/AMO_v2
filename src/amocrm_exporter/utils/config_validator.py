"""
Comprehensive configuration validation system for Google Sheets export
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

from ..core.config import settings
from ..core.logger import log_event
from ..core.google_sheets_config import GoogleSheetsConfigManager, ConfigValidationResult


@dataclass
class DiagnosticResult:
    """Result of diagnostic testing"""
    test_name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    fix_suggestions: Optional[List[str]] = None


@dataclass
class ConnectionTestResult:
    """Result of Google Sheets connection testing"""
    can_connect: bool
    api_accessible: bool
    authentication_valid: bool
    spreadsheets_accessible: Dict[str, bool]
    error_details: Optional[str] = None
    response_time_ms: Optional[float] = None


@dataclass
class OAuthValidationResult:
    """Result of OAuth flow validation"""
    credentials_file_valid: bool
    token_exists: bool
    token_valid: bool
    can_refresh: bool
    scopes_valid: bool
    error_message: Optional[str] = None
    expires_at: Optional[datetime] = None


class ConfigurationValidator:
    """Comprehensive configuration validation system for Google Sheets export"""

    def __init__(self):
        self.config_manager = GoogleSheetsConfigManager()
        self.diagnostic_tests = []

    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """
        Run comprehensive validation of Google Sheets configuration

        Returns:
            Dictionary with complete validation results
        """
        log_event("config_validator", "info", "Starting comprehensive Google Sheets configuration validation")

        validation_results = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'unknown',
            'basic_validation': None,
            'connection_test': None,
            'oauth_validation': None,
            'diagnostic_tests': [],
            'recommendations': [],
            'summary': {}
        }

        try:
            # 1. Basic configuration validation
            log_event("config_validator", "info", "Running basic configuration validation")
            validation_results['basic_validation'] = self._run_basic_validation()

            # 2. Google Sheets connection testing
            log_event("config_validator", "info", "Testing Google Sheets connection")
            validation_results['connection_test'] = self._test_google_sheets_connection()

            # 3. OAuth flow validation
            log_event("config_validator", "info", "Validating OAuth flow")
            validation_results['oauth_validation'] = self._validate_oauth_flow()

            # 4. Run diagnostic tests
            log_event("config_validator", "info", "Running diagnostic tests")
            validation_results['diagnostic_tests'] = self._run_diagnostic_tests()

            # 5. Generate recommendations
            validation_results['recommendations'] = self._generate_recommendations(validation_results)

            # 6. Create summary
            validation_results['summary'] = self._create_validation_summary(validation_results)

            # Determine overall status
            validation_results['overall_status'] = self._determine_overall_status(validation_results)

            log_event("config_validator", "info",
                     f"Comprehensive validation completed with status: {validation_results['overall_status']}")

        except Exception as e:
            log_event("config_validator", "error", f"Error during comprehensive validation: {str(e)}")
            validation_results['overall_status'] = 'error'
            validation_results['error'] = str(e)

        return validation_results

    def _run_basic_validation(self) -> Dict[str, Any]:
        """Run basic configuration validation using existing config manager"""
        try:
            result = self.config_manager.validate_configuration()
            return {
                'is_valid': result.is_valid,
                'errors': result.errors,
                'warnings': result.warnings,
                'missing_configs': result.missing_configs,
                'has_errors': result.has_errors,
                'has_warnings': result.has_warnings
            }
        except Exception as e:
            return {
                'is_valid': False,
                'errors': [f"Basic validation failed: {str(e)}"],
                'warnings': [],
                'missing_configs': [],
                'has_errors': True,
                'has_warnings': False
            }

    def _test_google_sheets_connection(self) -> ConnectionTestResult:
        """Test comprehensive Google Sheets connection"""
        start_time = time.time()

        try:
            # Test API accessibility
            if not self.config_manager.creds or not self.config_manager.creds.valid:
                try:
                    self.config_manager._get_credentials()
                except Exception as e:
                    return ConnectionTestResult(
                        can_connect=False,
                        api_accessible=False,
                        authentication_valid=False,
                        spreadsheets_accessible={},
                        error_details=f"Authentication failed: {str(e)}"
                    )

            # Test Google Sheets API connection
            try:
                service = build('sheets', 'v4', credentials=self.config_manager.creds)

                # Simple API test - get service info
                # This doesn't require a specific spreadsheet
                test_response = service.spreadsheets().get(
                    spreadsheetId='1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'  # Google's sample sheet
                ).execute()

                api_accessible = True
                log_event("config_validator", "debug", "Google Sheets API is accessible")

            except HttpError as e:
                if e.resp.status == 403:
                    # API is accessible but we don't have permission to the test sheet
                    api_accessible = True
                    log_event("config_validator", "debug", "Google Sheets API is accessible (permission denied to test sheet is expected)")
                else:
                    api_accessible = False
                    log_event("config_validator", "warning", f"Google Sheets API test failed: {str(e)}")
            except Exception as e:
                api_accessible = False
                log_event("config_validator", "error", f"Google Sheets API connection failed: {str(e)}")

            # Test configured spreadsheets
            spreadsheets_accessible = {}
            spreadsheet_ids = self.config_manager._get_spreadsheet_ids()

            for entity_type, spreadsheet_id in spreadsheet_ids.items():
                if spreadsheet_id:
                    try:
                        spreadsheet_info = self.config_manager.get_spreadsheet_info(spreadsheet_id)
                        spreadsheets_accessible[entity_type] = True
                        log_event("config_validator", "debug",
                                f"Successfully accessed {entity_type} spreadsheet: {spreadsheet_info.title}")
                    except Exception as e:
                        spreadsheets_accessible[entity_type] = False
                        log_event("config_validator", "warning",
                                f"Cannot access {entity_type} spreadsheet: {str(e)}")
                else:
                    spreadsheets_accessible[entity_type] = False

            response_time = (time.time() - start_time) * 1000  # Convert to milliseconds

            return ConnectionTestResult(
                can_connect=True,
                api_accessible=api_accessible,
                authentication_valid=self.config_manager.creds.valid,
                spreadsheets_accessible=spreadsheets_accessible,
                response_time_ms=response_time
            )

        except Exception as e:
            return ConnectionTestResult(
                can_connect=False,
                api_accessible=False,
                authentication_valid=False,
                spreadsheets_accessible={},
                error_details=str(e),
                response_time_ms=(time.time() - start_time) * 1000
            )

    def _validate_oauth_flow(self) -> OAuthValidationResult:
        """Validate OAuth flow and token management"""
        try:
            # Check credentials file
            credentials_file_valid = False
            if os.path.exists(self.config_manager.credentials_path):
                try:
                    with open(self.config_manager.credentials_path, 'r') as f:
                        creds_data = json.load(f)
                        if 'installed' in creds_data or 'web' in creds_data:
                            credentials_file_valid = True
                except Exception:
                    pass

            # Check token file
            token_exists = os.path.exists(self.config_manager.token_path)
            token_valid = False
            can_refresh = False
            expires_at = None

            if token_exists:
                try:
                    creds = Credentials.from_authorized_user_file(
                        self.config_manager.token_path,
                        ['https://www.googleapis.com/auth/spreadsheets']
                    )
                    token_valid = creds.valid
                    can_refresh = bool(creds.refresh_token)
                    if creds.expiry:
                        expires_at = creds.expiry
                except Exception as e:
                    log_event("config_validator", "warning", f"Error reading token file: {str(e)}")

            # Check scopes
            scopes_valid = True  # Assume valid unless we find otherwise
            if token_exists and token_valid:
                try:
                    with open(self.config_manager.token_path, 'r') as f:
                        token_data = json.load(f)
                        scopes = token_data.get('scopes', [])
                        required_scope = 'https://www.googleapis.com/auth/spreadsheets'
                        scopes_valid = required_scope in scopes
                except Exception:
                    scopes_valid = False

            return OAuthValidationResult(
                credentials_file_valid=credentials_file_valid,
                token_exists=token_exists,
                token_valid=token_valid,
                can_refresh=can_refresh,
                scopes_valid=scopes_valid,
                expires_at=expires_at
            )

        except Exception as e:
            return OAuthValidationResult(
                credentials_file_valid=False,
                token_exists=False,
                token_valid=False,
                can_refresh=False,
                scopes_valid=False,
                error_message=str(e)
            )

    def _run_diagnostic_tests(self) -> List[DiagnosticResult]:
        """Run comprehensive diagnostic tests for common configuration problems"""
        diagnostic_results = []

        # Test 1: File permissions
        diagnostic_results.append(self._test_file_permissions())

        # Test 2: Network connectivity
        diagnostic_results.append(self._test_network_connectivity())

        # Test 3: Environment variables
        diagnostic_results.append(self._test_environment_variables())

        # Test 4: Spreadsheet format validation
        diagnostic_results.append(self._test_spreadsheet_formats())

        # Test 5: API quotas and limits
        diagnostic_results.append(self._test_api_quotas())

        # Test 6: Credential file format
        diagnostic_results.append(self._test_credential_file_format())

        return diagnostic_results

    def _test_file_permissions(self) -> DiagnosticResult:
        """Test file system permissions for required files"""
        try:
            issues = []
            fix_suggestions = []

            # Test credentials file
            if os.path.exists(self.config_manager.credentials_path):
                if not os.access(self.config_manager.credentials_path, os.R_OK):
                    issues.append("Cannot read credentials.json file")
                    fix_suggestions.append("Check file permissions for credentials.json")
            else:
                issues.append("credentials.json file does not exist")
                fix_suggestions.append("Download credentials.json from Google Cloud Console")

            # Test token file directory
            token_dir = os.path.dirname(self.config_manager.token_path) or '.'
            if not os.access(token_dir, os.W_OK):
                issues.append("Cannot write to token file directory")
                fix_suggestions.append(f"Check write permissions for directory: {token_dir}")

            # Test token file if it exists
            if os.path.exists(self.config_manager.token_path):
                if not os.access(self.config_manager.token_path, os.R_OK | os.W_OK):
                    issues.append("Cannot read/write token.json file")
                    fix_suggestions.append("Check file permissions for token.json")

            if issues:
                return DiagnosticResult(
                    test_name="File Permissions",
                    passed=False,
                    message=f"File permission issues found: {'; '.join(issues)}",
                    fix_suggestions=fix_suggestions
                )
            else:
                return DiagnosticResult(
                    test_name="File Permissions",
                    passed=True,
                    message="All required files have proper permissions"
                )

        except Exception as e:
            return DiagnosticResult(
                test_name="File Permissions",
                passed=False,
                message=f"Error testing file permissions: {str(e)}",
                fix_suggestions=["Check file system permissions and access rights"]
            )

    def _test_network_connectivity(self) -> DiagnosticResult:
        """Test network connectivity to Google APIs"""
        try:
            import requests

            # Test connectivity to Google APIs
            test_urls = [
                'https://www.googleapis.com',
                'https://sheets.googleapis.com',
                'https://accounts.google.com'
            ]

            connectivity_results = {}
            for url in test_urls:
                try:
                    response = requests.get(url, timeout=10)
                    connectivity_results[url] = {
                        'accessible': response.status_code < 500,
                        'status_code': response.status_code,
                        'response_time': response.elapsed.total_seconds()
                    }
                except Exception as e:
                    connectivity_results[url] = {
                        'accessible': False,
                        'error': str(e)
                    }

            # Check if all URLs are accessible
            all_accessible = all(result.get('accessible', False) for result in connectivity_results.values())

            if all_accessible:
                avg_response_time = sum(
                    result.get('response_time', 0)
                    for result in connectivity_results.values()
                    if 'response_time' in result
                ) / len([r for r in connectivity_results.values() if 'response_time' in r])

                return DiagnosticResult(
                    test_name="Network Connectivity",
                    passed=True,
                    message=f"All Google APIs are accessible (avg response time: {avg_response_time:.2f}s)",
                    details=connectivity_results
                )
            else:
                failed_urls = [url for url, result in connectivity_results.items() if not result.get('accessible', False)]
                return DiagnosticResult(
                    test_name="Network Connectivity",
                    passed=False,
                    message=f"Cannot access Google APIs: {', '.join(failed_urls)}",
                    details=connectivity_results,
                    fix_suggestions=[
                        "Check internet connection",
                        "Verify firewall settings allow HTTPS traffic",
                        "Check if proxy settings are required"
                    ]
                )

        except ImportError:
            return DiagnosticResult(
                test_name="Network Connectivity",
                passed=False,
                message="Cannot test network connectivity (requests library not available)",
                fix_suggestions=["Install requests library: pip install requests"]
            )
        except Exception as e:
            return DiagnosticResult(
                test_name="Network Connectivity",
                passed=False,
                message=f"Network connectivity test failed: {str(e)}",
                fix_suggestions=["Check network connection and firewall settings"]
            )

    def _test_environment_variables(self) -> DiagnosticResult:
        """Test environment variable configuration"""
        try:
            issues = []
            warnings = []
            fix_suggestions = []

            # Check required Google Sheets environment variables
            required_vars = {
                'GOOGLE_SHEETS_LEADS_ID': settings.google_sheets_leads_id,
                'GOOGLE_SHEETS_CONTACTS_ID': settings.google_sheets_contacts_id,
                'GOOGLE_SHEETS_COMPANIES_ID': settings.google_sheets_companies_id,
                'GOOGLE_SHEETS_EVENTS_ID': settings.google_sheets_events_id
            }

            missing_vars = []
            invalid_format_vars = []

            for var_name, var_value in required_vars.items():
                if not var_value:
                    missing_vars.append(var_name)
                elif not self.config_manager._is_valid_spreadsheet_id(var_value):
                    invalid_format_vars.append((var_name, var_value))

            if missing_vars:
                issues.append(f"Missing environment variables: {', '.join(missing_vars)}")
                fix_suggestions.append("Add missing variables to your .env file")

            if invalid_format_vars:
                issues.append(f"Invalid spreadsheet ID format for: {', '.join([var for var, _ in invalid_format_vars])}")
                fix_suggestions.append("Verify spreadsheet IDs are 44 characters long and properly formatted")

            # Check .env file existence
            env_file_path = Path('.env')
            if not env_file_path.exists():
                warnings.append(".env file does not exist")
                fix_suggestions.append("Create .env file based on example.env")

            if issues:
                return DiagnosticResult(
                    test_name="Environment Variables",
                    passed=False,
                    message=f"Environment variable issues: {'; '.join(issues)}",
                    details={
                        'missing_vars': missing_vars,
                        'invalid_format_vars': invalid_format_vars,
                        'warnings': warnings
                    },
                    fix_suggestions=fix_suggestions
                )
            elif warnings:
                return DiagnosticResult(
                    test_name="Environment Variables",
                    passed=True,
                    message=f"Environment variables are valid (warnings: {'; '.join(warnings)})",
                    details={'warnings': warnings}
                )
            else:
                return DiagnosticResult(
                    test_name="Environment Variables",
                    passed=True,
                    message="All environment variables are properly configured"
                )

        except Exception as e:
            return DiagnosticResult(
                test_name="Environment Variables",
                passed=False,
                message=f"Error testing environment variables: {str(e)}",
                fix_suggestions=["Check .env file format and variable definitions"]
            )

    def _test_spreadsheet_formats(self) -> DiagnosticResult:
        """Test spreadsheet ID formats and accessibility"""
        try:
            spreadsheet_ids = self.config_manager._get_spreadsheet_ids()
            format_issues = []
            access_issues = []

            for entity_type, spreadsheet_id in spreadsheet_ids.items():
                if spreadsheet_id:
                    # Test format
                    if not self.config_manager._is_valid_spreadsheet_id(spreadsheet_id):
                        format_issues.append(f"{entity_type}: {spreadsheet_id}")

                    # Test accessibility if we have valid credentials
                    if self.config_manager.creds and self.config_manager.creds.valid:
                        try:
                            self.config_manager.get_spreadsheet_info(spreadsheet_id)
                        except Exception as e:
                            access_issues.append(f"{entity_type}: {str(e)[:100]}")

            issues = []
            fix_suggestions = []

            if format_issues:
                issues.append(f"Invalid spreadsheet ID formats: {', '.join(format_issues)}")
                fix_suggestions.append("Verify spreadsheet IDs are copied correctly from Google Sheets URLs")

            if access_issues:
                issues.append(f"Spreadsheet access issues: {', '.join(access_issues)}")
                fix_suggestions.extend([
                    "Ensure spreadsheets are shared with your Google account",
                    "Verify you have Editor permissions on all spreadsheets",
                    "Check that spreadsheet IDs are correct"
                ])

            if issues:
                return DiagnosticResult(
                    test_name="Spreadsheet Formats",
                    passed=False,
                    message=f"Spreadsheet issues found: {'; '.join(issues)}",
                    details={
                        'format_issues': format_issues,
                        'access_issues': access_issues
                    },
                    fix_suggestions=fix_suggestions
                )
            else:
                return DiagnosticResult(
                    test_name="Spreadsheet Formats",
                    passed=True,
                    message="All spreadsheet IDs are properly formatted and accessible"
                )

        except Exception as e:
            return DiagnosticResult(
                test_name="Spreadsheet Formats",
                passed=False,
                message=f"Error testing spreadsheet formats: {str(e)}",
                fix_suggestions=["Check spreadsheet configuration and accessibility"]
            )

    def _test_api_quotas(self) -> DiagnosticResult:
        """Test API quotas and rate limits"""
        try:
            if not self.config_manager.creds or not self.config_manager.creds.valid:
                return DiagnosticResult(
                    test_name="API Quotas",
                    passed=False,
                    message="Cannot test API quotas without valid credentials",
                    fix_suggestions=["Authenticate with Google first"]
                )

            # Make a few test API calls to check quota status
            service = build('sheets', 'v4', credentials=self.config_manager.creds)

            test_calls = 0
            successful_calls = 0
            quota_errors = []

            # Test with a few API calls
            spreadsheet_ids = [id for id in self.config_manager._get_spreadsheet_ids().values() if id]

            for spreadsheet_id in spreadsheet_ids[:3]:  # Test max 3 spreadsheets
                try:
                    test_calls += 1
                    service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                    successful_calls += 1
                except HttpError as e:
                    if e.resp.status == 429:  # Rate limit exceeded
                        quota_errors.append(f"Rate limit exceeded for {spreadsheet_id}")
                    elif e.resp.status == 403 and 'quota' in str(e).lower():
                        quota_errors.append(f"Quota exceeded for {spreadsheet_id}")
                except Exception:
                    pass  # Ignore other errors for quota testing

            if quota_errors:
                return DiagnosticResult(
                    test_name="API Quotas",
                    passed=False,
                    message=f"API quota issues detected: {'; '.join(quota_errors)}",
                    details={
                        'test_calls': test_calls,
                        'successful_calls': successful_calls,
                        'quota_errors': quota_errors
                    },
                    fix_suggestions=[
                        "Wait before making more API calls",
                        "Check Google Cloud Console for quota limits",
                        "Consider requesting quota increases if needed"
                    ]
                )
            else:
                return DiagnosticResult(
                    test_name="API Quotas",
                    passed=True,
                    message=f"API quotas are healthy ({successful_calls}/{test_calls} calls successful)",
                    details={
                        'test_calls': test_calls,
                        'successful_calls': successful_calls
                    }
                )

        except Exception as e:
            return DiagnosticResult(
                test_name="API Quotas",
                passed=False,
                message=f"Error testing API quotas: {str(e)}",
                fix_suggestions=["Check API access and quota configuration"]
            )

    def _test_credential_file_format(self) -> DiagnosticResult:
        """Test credential file format and content"""
        try:
            if not os.path.exists(self.config_manager.credentials_path):
                return DiagnosticResult(
                    test_name="Credential File Format",
                    passed=False,
                    message="Credentials file does not exist",
                    fix_suggestions=["Download credentials.json from Google Cloud Console"]
                )

            with open(self.config_manager.credentials_path, 'r') as f:
                try:
                    creds_data = json.load(f)
                except json.JSONDecodeError as e:
                    return DiagnosticResult(
                        test_name="Credential File Format",
                        passed=False,
                        message=f"Invalid JSON format in credentials file: {str(e)}",
                        fix_suggestions=["Download a new credentials.json file from Google Cloud Console"]
                    )

            # Check for required structure
            issues = []
            fix_suggestions = []

            if 'installed' not in creds_data and 'web' not in creds_data:
                issues.append("Missing 'installed' or 'web' section in credentials")
                fix_suggestions.append("Ensure you downloaded credentials for a Desktop or Web application")

            # Check for required fields in installed app credentials
            if 'installed' in creds_data:
                installed = creds_data['installed']
                required_fields = ['client_id', 'client_secret', 'auth_uri', 'token_uri']
                missing_fields = [field for field in required_fields if field not in installed]

                if missing_fields:
                    issues.append(f"Missing required fields in installed credentials: {', '.join(missing_fields)}")
                    fix_suggestions.append("Download a new credentials file from Google Cloud Console")

            if issues:
                return DiagnosticResult(
                    test_name="Credential File Format",
                    passed=False,
                    message=f"Credential file format issues: {'; '.join(issues)}",
                    fix_suggestions=fix_suggestions
                )
            else:
                return DiagnosticResult(
                    test_name="Credential File Format",
                    passed=True,
                    message="Credentials file format is valid"
                )

        except Exception as e:
            return DiagnosticResult(
                test_name="Credential File Format",
                passed=False,
                message=f"Error testing credential file format: {str(e)}",
                fix_suggestions=["Check credentials.json file and re-download if necessary"]
            )

    def _generate_recommendations(self, validation_results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on validation results"""
        recommendations = []

        # Basic validation recommendations
        basic_validation = validation_results.get('basic_validation', {})
        if basic_validation.get('has_errors'):
            recommendations.append("Fix basic configuration errors before proceeding")
            for error in basic_validation.get('errors', [])[:3]:  # Show first 3 errors
                recommendations.append(f"  - {error}")

        # Connection test recommendations
        connection_test = validation_results.get('connection_test')
        if connection_test and not connection_test.can_connect:
            recommendations.append("Resolve connection issues to Google Sheets API")
            if connection_test.error_details:
                recommendations.append(f"  - {connection_test.error_details}")

        # OAuth recommendations
        oauth_validation = validation_results.get('oauth_validation')
        if oauth_validation:
            if not oauth_validation.credentials_file_valid:
                recommendations.append("Download valid credentials.json from Google Cloud Console")
            if not oauth_validation.token_valid and oauth_validation.token_exists:
                recommendations.append("Re-authenticate with Google (token is expired or invalid)")

        # Diagnostic test recommendations
        diagnostic_tests = validation_results.get('diagnostic_tests', [])
        failed_tests = [test for test in diagnostic_tests if not test.passed]

        for test in failed_tests[:3]:  # Show first 3 failed tests
            if test.fix_suggestions:
                recommendations.append(f"Fix {test.test_name} issues:")
                for suggestion in test.fix_suggestions[:2]:  # Show first 2 suggestions
                    recommendations.append(f"  - {suggestion}")

        # General recommendations
        if not recommendations:
            recommendations.append("Configuration appears to be working correctly")
            recommendations.append("Consider running a test export to verify full functionality")

        return recommendations

    def _create_validation_summary(self, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create a summary of validation results"""
        summary = {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'warnings': 0,
            'critical_issues': 0,
            'configuration_complete': False,
            'ready_for_export': False
        }

        # Count basic validation
        basic_validation = validation_results.get('basic_validation', {})
        if basic_validation:
            summary['total_tests'] += 1
            if basic_validation.get('is_valid'):
                summary['passed_tests'] += 1
            else:
                summary['failed_tests'] += 1
                summary['critical_issues'] += len(basic_validation.get('errors', []))
            summary['warnings'] += len(basic_validation.get('warnings', []))

        # Count connection test
        connection_test = validation_results.get('connection_test')
        if connection_test:
            summary['total_tests'] += 1
            if connection_test.can_connect and connection_test.api_accessible:
                summary['passed_tests'] += 1
            else:
                summary['failed_tests'] += 1
                summary['critical_issues'] += 1

        # Count OAuth validation
        oauth_validation = validation_results.get('oauth_validation')
        if oauth_validation:
            summary['total_tests'] += 1
            if oauth_validation.credentials_file_valid and oauth_validation.token_valid:
                summary['passed_tests'] += 1
            else:
                summary['failed_tests'] += 1
                if not oauth_validation.credentials_file_valid:
                    summary['critical_issues'] += 1

        # Count diagnostic tests
        diagnostic_tests = validation_results.get('diagnostic_tests', [])
        for test in diagnostic_tests:
            summary['total_tests'] += 1
            if test.passed:
                summary['passed_tests'] += 1
            else:
                summary['failed_tests'] += 1

        # Determine readiness
        summary['configuration_complete'] = summary['critical_issues'] == 0
        summary['ready_for_export'] = (
            summary['configuration_complete'] and
            summary['failed_tests'] == 0 and
            connection_test and connection_test.can_connect
        )

        return summary

    def _determine_overall_status(self, validation_results: Dict[str, Any]) -> str:
        """Determine overall validation status"""
        summary = validation_results.get('summary', {})

        if summary.get('ready_for_export'):
            return 'ready'
        elif summary.get('configuration_complete'):
            return 'configured_with_warnings'
        elif summary.get('critical_issues', 0) > 0:
            return 'critical_issues'
        else:
            return 'needs_configuration'

    def get_diagnostic_report(self) -> str:
        """Generate a human-readable diagnostic report"""
        validation_results = self.run_comprehensive_validation()

        report_lines = [
            "=" * 60,
            "GOOGLE SHEETS CONFIGURATION DIAGNOSTIC REPORT",
            "=" * 60,
            f"Generated: {validation_results['timestamp']}",
            f"Overall Status: {validation_results['overall_status'].upper()}",
            ""
        ]

        # Summary section
        summary = validation_results.get('summary', {})
        report_lines.extend([
            "SUMMARY:",
            f"  Total Tests: {summary.get('total_tests', 0)}",
            f"  Passed: {summary.get('passed_tests', 0)}",
            f"  Failed: {summary.get('failed_tests', 0)}",
            f"  Warnings: {summary.get('warnings', 0)}",
            f"  Critical Issues: {summary.get('critical_issues', 0)}",
            f"  Ready for Export: {'Yes' if summary.get('ready_for_export') else 'No'}",
            ""
        ])

        # Basic validation section
        basic_validation = validation_results.get('basic_validation', {})
        if basic_validation:
            report_lines.extend([
                "BASIC CONFIGURATION:",
                f"  Status: {'PASS' if basic_validation.get('is_valid') else 'FAIL'}",
            ])

            if basic_validation.get('errors'):
                report_lines.append("  Errors:")
                for error in basic_validation['errors']:
                    report_lines.append(f"    - {error}")

            if basic_validation.get('warnings'):
                report_lines.append("  Warnings:")
                for warning in basic_validation['warnings']:
                    report_lines.append(f"    - {warning}")

            report_lines.append("")

        # Connection test section
        connection_test = validation_results.get('connection_test')
        if connection_test:
            report_lines.extend([
                "CONNECTION TEST:",
                f"  Can Connect: {'Yes' if connection_test.can_connect else 'No'}",
                f"  API Accessible: {'Yes' if connection_test.api_accessible else 'No'}",
                f"  Authentication Valid: {'Yes' if connection_test.authentication_valid else 'No'}",
            ])

            if connection_test.response_time_ms:
                report_lines.append(f"  Response Time: {connection_test.response_time_ms:.0f}ms")

            if connection_test.error_details:
                report_lines.append(f"  Error: {connection_test.error_details}")

            report_lines.append("")

        # Diagnostic tests section
        diagnostic_tests = validation_results.get('diagnostic_tests', [])
        if diagnostic_tests:
            report_lines.append("DIAGNOSTIC TESTS:")
            for test in diagnostic_tests:
                status = "PASS" if test.passed else "FAIL"
                report_lines.append(f"  {test.test_name}: {status}")
                report_lines.append(f"    {test.message}")
                if test.fix_suggestions and not test.passed:
                    report_lines.append("    Suggestions:")
                    for suggestion in test.fix_suggestions:
                        report_lines.append(f"      - {suggestion}")
            report_lines.append("")

        # Recommendations section
        recommendations = validation_results.get('recommendations', [])
        if recommendations:
            report_lines.append("RECOMMENDATIONS:")
            for recommendation in recommendations:
                report_lines.append(f"  {recommendation}")
            report_lines.append("")

        report_lines.append("=" * 60)

        return "\n".join(report_lines)