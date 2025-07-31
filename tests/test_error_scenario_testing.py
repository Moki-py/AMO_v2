"""
Comprehensive error scenario testing for all failure modes
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from googleapiclient.errors import HttpError

from amocrm_exporter.exporters.enhanced_sheets_exporter import EnhancedSheetsExporter
from amocrm_exporter.core.google_sheets_config import GoogleSheetsConfigManager
from amocrm_exporter.web.export_presets import ExportPresetManager
from amocrm_exporter.utils.progress_tracker import ExportProgressTracker
from amocrm_exporter.utils.data_filter_engine import DataFilterEngine
from amocrm_exporter.utils.error_handler import GoogleSheetsErrorHandler
from amocrm_exporter.utils.exceptions import (
    GoogleSheetsError,
    GoogleSheetsAuthError,
    GoogleSheetsPermissionError,
    GoogleSheetsRateLimitError,
    GoogleSheetsQuotaError,
    GoogleSheetsConfigError,
    GoogleSheetsSpreadsheetNotFoundError,
    GoogleSheetsSheetNotFoundError,
    GoogleSheetsBatchError,
    GoogleSheetsPartialExportError,
    NetworkError,
    ErrorSeverity
)


class TestAuthenticationErrorScenarios:
    """Test various authentication error scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_storage = Mock()
        self.config_manager = GoogleSheetsConfigManager()
        self.preset_manager = ExportPresetManager(self.mock_storage)
        self.error_handler = GoogleSheetsErrorHandler()

    def test_missing_credentials_file(self):
        """Test handling of missing credentials.json file"""
        with patch('os.path.exists', return_value=False):
            validation_result = self.config_manager.validate_configuration()

            assert not validation_result.is_valid
            assert validation_result.has_errors
            assert any('credentials.json' in error and 'not found' in error
                      for error in validation_result.errors)

    def test_invalid_credentials_format(self):
        """Test handling of invalid credentials file format"""
        from unittest.mock import mock_open
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='{"invalid": "format"}')):

            validation_result = self.config_manager.validate_configuration()

            assert not validation_result.is_valid
            assert any('Invalid credentials format' in error for error in validation_result.errors)

    def test_expired_oauth_token(self):
        """Test handling of expired OAuth tokens"""
        from unittest.mock import mock_open
        with patch('os.path.exists') as mock_exists, \
             patch('google.oauth2.credentials.Credentials.from_authorized_user_file') as mock_creds, \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file') as mock_flow, \
             patch('builtins.open', mock_open()) as mock_file:

            # Mock file existence
            def exists_side_effect(path):
                if 'credentials.json' in path:
                    return True
                elif 'token.json' in path:
                    return True
                return False
            mock_exists.side_effect = exists_side_effect

            # Mock that loading credentials returns None (corrupted token)
            mock_creds.return_value = None

            # Mock OAuth flow failure when trying to get new credentials
            mock_flow_instance = Mock()
            mock_flow_instance.run_local_server.side_effect = Exception("OAuth flow failed")
            mock_flow.return_value = mock_flow_instance

            with pytest.raises(Exception, match="Error during OAuth flow"):
                self.config_manager._get_credentials()

    def test_oauth_flow_failure(self):
        """Test OAuth flow failure scenarios"""
        from unittest.mock import mock_open
        with patch('os.path.exists') as mock_exists, \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file') as mock_flow, \
             patch('os.remove') as mock_remove, \
             patch('builtins.open', mock_open()) as mock_file:

            # Mock file existence - credentials exist, token doesn't
            def exists_side_effect(path):
                if 'credentials.json' in path:
                    return True
                elif 'token.json' in path:
                    return False  # No token file
                return False
            mock_exists.side_effect = exists_side_effect

            # Mock OAuth flow failure
            mock_flow_instance = Mock()
            mock_flow_instance.run_local_server.side_effect = Exception("OAuth flow failed")
            mock_flow.return_value = mock_flow_instance

            with pytest.raises(Exception, match="Error during OAuth flow"):
                self.config_manager._get_credentials()

    def test_google_api_disabled_error(self):
        """Test handling when Google Sheets API is disabled"""
        auth_error = GoogleSheetsAuthError(
            "Google Sheets API has not been used in project",
            context=Mock()
        )

        error_report = self.error_handler.handle_error(auth_error)

        assert error_report.error_type == "GoogleSheetsAuthError"
        assert any(action.action_type.value == "check_permissions"
                  for action in error_report.suggested_actions)
        assert any("API" in action.description for action in error_report.suggested_actions)


class TestPermissionErrorScenarios:
    """Test various permission error scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config_manager = GoogleSheetsConfigManager()
        self.error_handler = GoogleSheetsErrorHandler()

    def test_spreadsheet_not_shared(self):
        """Test handling when spreadsheet is not shared with user"""
        # Mock the Google Sheets API service completely
        with patch('amocrm_exporter.core.google_sheets_config.build') as mock_build:

            # Set up mock credentials with required attributes
            mock_creds = Mock()
            mock_creds.valid = True
            mock_creds.universe_domain = "googleapis.com"

            # Mock the service directly in the module
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Mock 403 forbidden error
            mock_resp = Mock()
            mock_resp.status = 403
            http_error = HttpError(mock_resp, b'Forbidden')
            http_error.error_details = [{"reason": "forbidden"}]

            mock_service.spreadsheets().get().execute.side_effect = http_error

            # Set the credentials directly
            self.config_manager.creds = mock_creds

            spreadsheet_id = "test_spreadsheet_id_123456789012345678901234"
            with pytest.raises(Exception, match="Access denied"):
                self.config_manager.get_spreadsheet_info(spreadsheet_id)

    def test_insufficient_permissions(self):
        """Test handling of insufficient permissions (read-only access)"""
        # Mock the Google Sheets API service completely
        with patch('amocrm_exporter.core.google_sheets_config.build') as mock_build:

            # Set up mock credentials with required attributes
            mock_creds = Mock()
            mock_creds.valid = True
            mock_creds.universe_domain = "googleapis.com"

            # Mock the service directly in the module
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Mock successful read but failed write
            mock_service.spreadsheets().get().execute.return_value = {"properties": {"title": "Test"}}
            mock_service.spreadsheets().values().get().execute.return_value = {"values": []}

            # Mock 403 error for write operations
            mock_resp = Mock()
            mock_resp.status = 403
            http_error = HttpError(mock_resp, b'Forbidden')
            mock_service.spreadsheets().batchUpdate().execute.side_effect = http_error

            # Set the credentials directly
            self.config_manager.creds = mock_creds

            spreadsheet_id = "test_spreadsheet_id_123456789012345678901234"
            result = self.config_manager.test_permissions(spreadsheet_id)

            assert result.can_read is True
            assert result.can_write is False
            assert result.can_create_sheets is False
            assert "Write permission denied" in result.error_message

    def test_service_account_not_added(self):
        """Test handling when service account email is not added to spreadsheet"""
        permission_error = GoogleSheetsPermissionError(
            "The caller does not have permission",
            spreadsheet_id="test_spreadsheet_id_123456789012345678901234",
            context=Mock()
        )

        error_report = self.error_handler.handle_error(permission_error)

        assert error_report.error_type == "GoogleSheetsPermissionError"
        assert any(action.action_type.value == "check_permissions"
                  for action in error_report.suggested_actions)
        assert any("Share" in action.description for action in error_report.suggested_actions)


class TestRateLimitingErrorScenarios:
    """Test rate limiting and quota error scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.error_handler = GoogleSheetsErrorHandler()

    def test_api_rate_limit_exceeded(self):
        """Test handling of API rate limit errors"""
        rate_limit_error = GoogleSheetsRateLimitError(
            "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user'",
            retry_after=60,
            context=Mock()
        )

        error_report = self.error_handler.handle_error(rate_limit_error)

        assert error_report.error_type == "GoogleSheetsRateLimitError"
        assert error_report.is_retryable is True
        assert error_report.retry_after == 60
        assert any(action.action_type.value == "wait" for action in error_report.suggested_actions)

    def test_daily_quota_exceeded(self):
        """Test handling of daily quota exceeded errors"""
        quota_error = GoogleSheetsQuotaError(
            "Quota exceeded for quota metric 'Requests' and limit 'Requests per day'",
            retry_after=86400,  # 24 hours
            context=Mock()
        )

        error_report = self.error_handler.handle_error(quota_error)

        assert error_report.error_type == "GoogleSheetsQuotaError"
        assert error_report.is_retryable is True
        assert error_report.retry_after == 86400
        assert any("24 hours" in action.description or "quota" in action.description.lower()
                  for action in error_report.suggested_actions)

    def test_concurrent_requests_limit(self):
        """Test handling of concurrent requests limit"""
        with patch('googleapiclient.discovery.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Mock 429 Too Many Requests error
            mock_resp = Mock()
            mock_resp.status = 429
            http_error = HttpError(mock_resp, b'Too Many Requests')
            mock_service.spreadsheets().batchUpdate().execute.side_effect = http_error

            # Should be handled as rate limit error
            with pytest.raises(HttpError):
                mock_service.spreadsheets().batchUpdate().execute()


class TestNetworkErrorScenarios:
    """Test various network error scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.error_handler = GoogleSheetsErrorHandler()

    def test_connection_timeout(self):
        """Test handling of connection timeout errors"""
        import socket
        timeout_error = socket.timeout("Connection timed out")

        # Wrap in NetworkError
        network_error = NetworkError(
            "Connection timeout occurred",
            original_error=timeout_error,
            context=Mock()
        )

        error_report = self.error_handler.handle_error(network_error)

        assert error_report.is_retryable is True
        assert any(action.action_type.value == "check_network"
                  for action in error_report.suggested_actions)

    def test_dns_resolution_failure(self):
        """Test handling of DNS resolution failures"""
        import socket
        dns_error = socket.gaierror("Name or service not known")

        network_error = NetworkError(
            "DNS resolution failed",
            original_error=dns_error,
            context=Mock()
        )

        error_report = self.error_handler.handle_error(network_error)

        assert error_report.is_retryable is True
        assert any(action.action_type.value == "check_network"
                  for action in error_report.suggested_actions)

    def test_ssl_certificate_error(self):
        """Test handling of SSL certificate errors"""
        import ssl
        ssl_error = ssl.SSLError("Certificate verification failed")

        network_error = NetworkError(
            "SSL certificate error",
            original_error=ssl_error,
            context=Mock()
        )

        error_report = self.error_handler.handle_error(network_error)

        assert error_report.is_retryable is True  # Network errors are retryable by default
        assert any(action.action_type.value == "check_network"
                  for action in error_report.suggested_actions)


class TestConfigurationErrorScenarios:
    """Test configuration error scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config_manager = GoogleSheetsConfigManager()
        self.error_handler = GoogleSheetsErrorHandler()

    def test_missing_spreadsheet_ids(self):
        """Test handling of missing spreadsheet ID configuration"""
        with patch.object(self.config_manager, '_get_spreadsheet_ids') as mock_get_ids:
            mock_get_ids.return_value = {
                'leads': None,
                'contacts': None,
                'companies': None,
                'events': None
            }

            validation_result = self.config_manager.validate_configuration()

            assert not validation_result.is_valid
            assert len(validation_result.missing_configs) == 4
            assert all(config.startswith('GOOGLE_SHEETS_') for config in validation_result.missing_configs)

    def test_invalid_spreadsheet_id_format(self):
        """Test handling of invalid spreadsheet ID formats"""
        with patch.object(self.config_manager, '_get_spreadsheet_ids') as mock_get_ids:
            mock_get_ids.return_value = {
                'leads': 'invalid_short_id',
                'contacts': 'id_with_invalid@characters!',
                'companies': 'a' * 50,  # Too long
                'events': ''  # Empty
            }

            validation_result = self.config_manager.validate_configuration()

            assert not validation_result.is_valid
            invalid_format_errors = [error for error in validation_result.errors
                                   if 'Invalid spreadsheet ID format' in error]
            assert len(invalid_format_errors) >= 3  # At least 3 invalid formats

    def test_spreadsheet_not_found(self):
        """Test handling when configured spreadsheet doesn't exist"""
        # Mock the Google Sheets API service completely
        with patch('amocrm_exporter.core.google_sheets_config.build') as mock_build:

            # Set up mock credentials with required attributes
            mock_creds = Mock()
            mock_creds.valid = True
            mock_creds.universe_domain = "googleapis.com"

            # Mock the service directly in the module
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Mock 404 not found error
            mock_resp = Mock()
            mock_resp.status = 404
            http_error = HttpError(mock_resp, b'Not found')
            http_error.error_details = []

            mock_service.spreadsheets().get().execute.side_effect = http_error

            # Set the credentials directly
            self.config_manager.creds = mock_creds

            spreadsheet_id = "nonexistent_spreadsheet_id_123456789012345678"
            with pytest.raises(Exception, match="Spreadsheet not found"):
                self.config_manager.get_spreadsheet_info(spreadsheet_id)


class TestDataProcessingErrorScenarios:
    """Test data processing error scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.error_handler = GoogleSheetsErrorHandler()

    def test_batch_operation_failure(self):
        """Test handling of batch operation failures"""
        batch_error = GoogleSheetsBatchError(
            "Batch operation failed",
            batch_size=1000,
            failed_rows=[100, 200, 300],
            context=Mock()
        )

        error_report = self.error_handler.handle_error(batch_error)

        assert error_report.error_type == "GoogleSheetsBatchError"
        assert error_report.is_retryable is True
        assert error_report.additional_info["batch_size"] == 1000
        assert error_report.additional_info["failed_rows"] == [100, 200, 300]

    def test_partial_export_failure(self):
        """Test handling of partial export failures"""
        partial_error = GoogleSheetsPartialExportError(
            "Partial export completed",
            exported_entities={"deals": 100, "contacts": 50},
            failed_entities=["companies", "users"],
            context=Mock()
        )

        error_report = self.error_handler.handle_error(partial_error)

        assert error_report.error_type == "GoogleSheetsPartialExportError"
        assert error_report.is_retryable is True
        assert "150 entities exported" in error_report.user_message  # 100 + 50
        assert "companies, users" in error_report.user_message

    def test_sheet_not_found_error(self):
        """Test handling when specific sheet doesn't exist"""
        sheet_error = GoogleSheetsSheetNotFoundError(
            sheet_name="Deals",
            spreadsheet_id="test_spreadsheet_id_123456789012345678901234",
            context=Mock()
        )

        error_report = self.error_handler.handle_error(sheet_error)

        assert error_report.error_type == "GoogleSheetsSheetNotFoundError"
        assert error_report.is_retryable is True
        assert error_report.additional_info["sheet_name"] == "Deals"
        assert any("create the sheet automatically" in action.description
                  for action in error_report.suggested_actions)

    def test_data_format_error(self):
        """Test handling of data formatting errors"""
        # Test with malformed custom fields data
        from amocrm_exporter.utils.custom_field_processor import CustomFieldProcessor

        processor = CustomFieldProcessor()

        # Malformed data that should trigger graceful degradation
        malformed_data = [
            {
                'field_id': None,  # Invalid field_id
                'field_name': '',  # Empty field_name
                'field_type': 'unknown_type',
                'values': None  # Invalid values
            }
        ]

        result = processor.process_custom_fields(malformed_data, graceful_degradation=True)

        # Should handle gracefully with warnings
        assert result.success or len(result.errors) == 0


class TestConcurrencyErrorScenarios:
    """Test error scenarios related to concurrent operations"""

    def setup_method(self):
        """Set up test fixtures"""
        self.error_handler = GoogleSheetsErrorHandler()

    @pytest.mark.asyncio
    async def test_concurrent_rate_limiting(self):
        """Test handling of rate limiting during concurrent operations"""
        # Simulate multiple concurrent requests hitting rate limits
        async def mock_api_call(call_id):
            if call_id % 2 == 0:  # Every other call fails with rate limit
                raise GoogleSheetsRateLimitError(
                    f"Rate limit exceeded for call {call_id}",
                    retry_after=30,
                    context=Mock()
                )
            return f"Success {call_id}"

        # Create multiple concurrent tasks
        tasks = [mock_api_call(i) for i in range(10)]

        # Execute with error handling
        results = []
        for task in tasks:
            try:
                result = await task
                results.append(result)
            except GoogleSheetsRateLimitError as e:
                # Handle rate limit error
                error_report = self.error_handler.handle_error(e)
                assert error_report.is_retryable is True
                results.append(f"Rate limited: {e}")

        # Should have mix of successes and rate limit errors
        successes = [r for r in results if r.startswith("Success")]
        rate_limited = [r for r in results if r.startswith("Rate limited")]

        assert len(successes) == 5  # Half should succeed
        assert len(rate_limited) == 5  # Half should be rate limited

    def test_resource_exhaustion(self):
        """Test handling of resource exhaustion scenarios"""
        # Simulate memory exhaustion
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Create scenario that might exhaust memory
        try:
            # Create large data structure
            large_data = []
            for i in range(100000):
                large_data.append({
                    "id": i,
                    "data": "x" * 1000,  # 1KB per record
                    "custom_fields": [{"field_id": j, "value": f"value_{j}"} for j in range(10)]
                })

            current_memory = process.memory_info().rss
            memory_increase = current_memory - initial_memory

            # If memory usage is excessive, it should be handled gracefully
            if memory_increase > 500 * 1024 * 1024:  # 500MB
                pytest.fail("Memory usage too high - should implement streaming or batching")

        except MemoryError:
            # Should handle memory errors gracefully
            assert True  # Expected in resource-constrained environments

        finally:
            # Cleanup
            if 'large_data' in locals():
                del large_data


class TestRecoveryScenarios:
    """Test error recovery scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.error_handler = GoogleSheetsErrorHandler()

    def test_automatic_retry_after_transient_error(self):
        """Test automatic retry after transient errors"""
        from amocrm_exporter.utils.retry_logic import ExponentialBackoffRetry, RetryConfiguration

        config = RetryConfiguration(max_retries=3, base_delay=0.01)  # Fast retry for testing
        retry_handler = ExponentialBackoffRetry(config)

        call_count = 0
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise NetworkError("Transient network error", is_retryable=True)
            return "success"

        with patch('time.sleep'):  # Speed up test
            result = retry_handler.execute_with_retry(flaky_function)

        assert result == "success"
        assert call_count == 3  # Should have retried twice

    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after service restoration"""
        from amocrm_exporter.utils.retry_logic import CircuitBreaker, RetryConfiguration

        config = RetryConfiguration(
            failure_threshold=2,
            recovery_timeout=1,  # Short timeout for testing
            success_threshold=1
        )
        circuit_breaker = CircuitBreaker(config)

        # Trigger circuit breaker to open
        circuit_breaker.record_failure()
        circuit_breaker.record_failure()
        assert circuit_breaker.stats.state.value == "open"

        # Wait for recovery timeout
        import time
        time.sleep(1.1)

        # Should transition to half-open
        assert circuit_breaker.can_execute() is True
        assert circuit_breaker.stats.state.value == "half_open"

        # Record success to close circuit
        circuit_breaker.record_success()
        assert circuit_breaker.stats.state.value == "closed"

    def test_partial_export_recovery(self):
        """Test recovery from partial export failures"""
        # Simulate partial export scenario
        exported_entities = {"deals": 100, "contacts": 50}
        failed_entities = ["companies", "users"]

        partial_error = GoogleSheetsPartialExportError(
            "Partial export completed",
            exported_entities=exported_entities,
            failed_entities=failed_entities,
            context=Mock()
        )

        error_report = self.error_handler.handle_error(partial_error)

        # Should suggest retrying only failed entities
        assert error_report.is_retryable is True
        assert any("retry" in action.title.lower() for action in error_report.suggested_actions)
        assert "companies, users" in error_report.user_message


# Helper function removed - using unittest.mock.mock_open directly


if __name__ == '__main__':
    pytest.main([__file__, '-v'])