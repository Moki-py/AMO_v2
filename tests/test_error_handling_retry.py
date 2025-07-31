"""
Unit tests for error handling and retry mechanisms
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from amocrm_exporter.utils.error_handler import (
    GoogleSheetsErrorHandler,
    ErrorReport,
    UserAction,
    UserActionType,
    handle_google_sheets_error
)
from amocrm_exporter.utils.retry_logic import (
    ExponentialBackoffRetry,
    RetryConfiguration,
    CircuitBreaker,
    CircuitBreakerState,
    with_retry,
    create_google_api_retry_config,
    create_network_retry_config
)
from amocrm_exporter.utils.exceptions import (
    GoogleSheetsError,
    GoogleSheetsAuthError,
    GoogleSheetsPermissionError,
    GoogleSheetsRateLimitError,
    GoogleSheetsConfigError,
    NetworkError,
    RateLimitError,
    ErrorSeverity,
    create_error_context
)


class TestGoogleSheetsErrorHandler:
    """Test cases for GoogleSheetsErrorHandler"""

    def setup_method(self):
        """Set up test fixtures"""
        self.error_handler = GoogleSheetsErrorHandler()

    def test_initialization(self):
        """Test error handler initialization"""
        assert self.error_handler._error_counter == 0
        assert self.error_handler.logger is not None

    def test_handle_auth_error(self):
        """Test handling authentication errors"""
        context = create_error_context("test_component", "test_operation")
        auth_error = GoogleSheetsAuthError(
            "Authentication failed",
            context=context
        )

        report = self.error_handler.handle_error(auth_error, context, "test_operation")

        assert isinstance(report, ErrorReport)
        assert report.error_type == "GoogleSheetsAuthError"
        assert report.severity == auth_error.severity
        assert "authentication failed" in report.user_message.lower()
        assert len(report.suggested_actions) > 0
        assert any(action.action_type == UserActionType.UPDATE_CREDENTIALS
                  for action in report.suggested_actions)

    def test_handle_permission_error(self):
        """Test handling permission errors"""
        context = create_error_context("test_component", "test_operation")
        spreadsheet_id = "test_spreadsheet_id_1234567890123456789012345"

        permission_error = GoogleSheetsPermissionError(
            "Insufficient permissions",
            spreadsheet_id=spreadsheet_id,
            context=context
        )

        report = self.error_handler.handle_error(permission_error, context, "test_operation")

        assert isinstance(report, ErrorReport)
        assert report.error_type == "GoogleSheetsPermissionError"
        assert "permissions" in report.user_message.lower()
        assert len(report.suggested_actions) > 0
        assert any(action.action_type == UserActionType.CHECK_PERMISSIONS
                  for action in report.suggested_actions)
        assert report.additional_info["spreadsheet_id"] == spreadsheet_id

    def test_handle_rate_limit_error(self):
        """Test handling rate limit errors"""
        context = create_error_context("test_component", "test_operation")
        rate_limit_error = GoogleSheetsRateLimitError(
            "Rate limit exceeded",
            retry_after=60,
            context=context
        )

        report = self.error_handler.handle_error(rate_limit_error, context, "test_operation")

        assert isinstance(report, ErrorReport)
        assert report.error_type == "GoogleSheetsRateLimitError"
        assert "rate limit" in report.user_message.lower()
        assert report.is_retryable is True
        assert report.retry_after == 60
        assert any(action.action_type == UserActionType.WAIT
                  for action in report.suggested_actions)

    def test_handle_config_error(self):
        """Test handling configuration errors"""
        context = create_error_context("test_component", "test_operation")
        config_error = GoogleSheetsConfigError(
            "Invalid configuration",
            config_key="GOOGLE_SHEETS_LEADS_ID",
            context=context
        )

        report = self.error_handler.handle_error(config_error, context, "test_operation")

        assert isinstance(report, ErrorReport)
        assert report.error_type == "GoogleSheetsConfigError"
        assert "configuration" in report.user_message.lower()
        assert report.is_retryable is False
        assert any(action.action_type == UserActionType.RECONFIGURE
                  for action in report.suggested_actions)
        assert report.additional_info.get("config_key") == "GOOGLE_SHEETS_LEADS_ID"

    def test_handle_generic_error(self):
        """Test handling generic Python exceptions"""
        context = create_error_context("test_component", "test_operation")
        generic_error = ValueError("Something went wrong")

        report = self.error_handler.handle_error(generic_error, context, "test_operation")

        assert isinstance(report, ErrorReport)
        assert report.error_type == "ValueError"
        assert report.severity == ErrorSeverity.HIGH
        assert "unexpected error" in report.user_message.lower()
        assert report.is_retryable is True
        assert len(report.suggested_actions) > 0

    def test_error_id_generation(self):
        """Test that unique error IDs are generated"""
        context = create_error_context("test_component", "test_operation")
        error = Exception("Test error")

        report1 = self.error_handler.handle_error(error, context)
        report2 = self.error_handler.handle_error(error, context)

        assert report1.error_id != report2.error_id
        assert report1.error_id.startswith("GSE_")
        assert report2.error_id.startswith("GSE_")

    def test_format_error_for_user(self):
        """Test formatting error report for user display"""
        context = create_error_context("test_component", "test_operation")
        error = GoogleSheetsAuthError("Auth failed", context=context)

        report = self.error_handler.handle_error(error, context)
        formatted = self.error_handler.format_error_for_user(report)

        assert report.error_id in formatted
        assert report.user_message in formatted
        assert "What you can do:" in formatted
        assert any(action.title in formatted for action in report.suggested_actions)

    def test_convenience_function(self):
        """Test the convenience function for error handling"""
        context = create_error_context("test_component", "test_operation")
        error = GoogleSheetsError("Test error", context=context)

        report = handle_google_sheets_error(error, context, "test_operation")

        assert isinstance(report, ErrorReport)
        assert report.error_type == "GoogleSheetsError"


class TestRetryConfiguration:
    """Test cases for RetryConfiguration"""

    def test_default_configuration(self):
        """Test default retry configuration"""
        config = RetryConfiguration()

        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
        assert config.circuit_breaker_enabled is True

    def test_custom_configuration(self):
        """Test custom retry configuration"""
        config = RetryConfiguration(
            max_retries=5,
            base_delay=2.0,
            max_delay=120.0,
            exponential_base=1.5,
            jitter=False
        )

        assert config.max_retries == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 120.0
        assert config.exponential_base == 1.5
        assert config.jitter is False

    def test_configuration_validation(self):
        """Test configuration parameter validation"""
        # Invalid max_retries
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            RetryConfiguration(max_retries=-1)

        # Invalid base_delay
        with pytest.raises(ValueError, match="base_delay must be positive"):
            RetryConfiguration(base_delay=0)

        # Invalid exponential_base
        with pytest.raises(ValueError, match="exponential_base must be greater than 1"):
            RetryConfiguration(exponential_base=1.0)

        # Invalid jitter_range
        with pytest.raises(ValueError, match="jitter_range must be between 0 and 1"):
            RetryConfiguration(jitter_range=1.5)


class TestCircuitBreaker:
    """Test cases for CircuitBreaker"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config = RetryConfiguration(
            failure_threshold=3,
            recovery_timeout=60,
            success_threshold=2
        )
        self.circuit_breaker = CircuitBreaker(self.config)

    def test_initialization(self):
        """Test circuit breaker initialization"""
        assert self.circuit_breaker.stats.state == CircuitBreakerState.CLOSED
        assert self.circuit_breaker.stats.failure_count == 0
        assert self.circuit_breaker.stats.success_count == 0

    def test_can_execute_closed_state(self):
        """Test can_execute in CLOSED state"""
        assert self.circuit_breaker.can_execute() is True

    def test_record_success_closed_state(self):
        """Test recording success in CLOSED state"""
        # Add some failures first
        self.circuit_breaker.stats.failure_count = 2

        self.circuit_breaker.record_success()

        assert self.circuit_breaker.stats.total_successes == 1
        assert self.circuit_breaker.stats.failure_count == 0  # Should reset
        assert self.circuit_breaker.stats.last_success_time is not None

    def test_record_failure_opens_circuit(self):
        """Test that failures open the circuit"""
        # Record failures up to threshold
        for _ in range(self.config.failure_threshold):
            self.circuit_breaker.record_failure()

        assert self.circuit_breaker.stats.state == CircuitBreakerState.OPEN
        assert self.circuit_breaker.stats.failure_count == self.config.failure_threshold

    def test_can_execute_open_state(self):
        """Test can_execute in OPEN state"""
        # Force circuit to OPEN state
        self.circuit_breaker.stats.state = CircuitBreakerState.OPEN
        self.circuit_breaker.stats.last_failure_time = time.time()

        assert self.circuit_breaker.can_execute() is False

    def test_transition_to_half_open(self):
        """Test transition from OPEN to HALF_OPEN after timeout"""
        # Force circuit to OPEN state with old failure time
        self.circuit_breaker.stats.state = CircuitBreakerState.OPEN
        self.circuit_breaker.stats.last_failure_time = time.time() - (self.config.recovery_timeout + 1)

        assert self.circuit_breaker.can_execute() is True
        assert self.circuit_breaker.stats.state == CircuitBreakerState.HALF_OPEN

    def test_half_open_to_closed_on_success(self):
        """Test transition from HALF_OPEN to CLOSED on successful calls"""
        # Set to HALF_OPEN state
        self.circuit_breaker.stats.state = CircuitBreakerState.HALF_OPEN

        # Record successful calls up to success threshold
        for _ in range(self.config.success_threshold):
            self.circuit_breaker.record_success()

        assert self.circuit_breaker.stats.state == CircuitBreakerState.CLOSED
        assert self.circuit_breaker.stats.failure_count == 0

    def test_half_open_to_open_on_failure(self):
        """Test transition from HALF_OPEN back to OPEN on failure"""
        # Set to HALF_OPEN state
        self.circuit_breaker.stats.state = CircuitBreakerState.HALF_OPEN

        self.circuit_breaker.record_failure()

        assert self.circuit_breaker.stats.state == CircuitBreakerState.OPEN

    def test_get_stats(self):
        """Test getting circuit breaker statistics"""
        self.circuit_breaker.record_success()
        self.circuit_breaker.record_failure()

        stats = self.circuit_breaker.get_stats()

        assert stats["state"] == CircuitBreakerState.CLOSED.value
        assert stats["total_requests"] == 2
        assert stats["total_successes"] == 1
        assert stats["total_failures"] == 1
        assert "last_success_time" in stats
        assert "last_failure_time" in stats

    def test_disabled_circuit_breaker(self):
        """Test circuit breaker when disabled"""
        config = RetryConfiguration(circuit_breaker_enabled=False)
        circuit_breaker = CircuitBreaker(config)

        # Should always allow execution when disabled
        assert circuit_breaker.can_execute() is True

        # Even after many failures
        for _ in range(10):
            circuit_breaker.record_failure()

        assert circuit_breaker.can_execute() is True


class TestExponentialBackoffRetry:
    """Test cases for ExponentialBackoffRetry"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config = RetryConfiguration(
            max_retries=3,
            base_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter=False  # Disable for predictable testing
        )
        self.retry_handler = ExponentialBackoffRetry(self.config)

    def test_initialization(self):
        """Test retry handler initialization"""
        assert self.retry_handler.config == self.config
        assert isinstance(self.retry_handler.circuit_breaker, CircuitBreaker)

    def test_calculate_delay(self):
        """Test delay calculation with exponential backoff"""
        # Test exponential progression
        delay_0 = self.retry_handler.calculate_delay(0)
        delay_1 = self.retry_handler.calculate_delay(1)
        delay_2 = self.retry_handler.calculate_delay(2)

        assert delay_0 == 1.0  # base_delay * (2^0) = 1.0
        assert delay_1 == 2.0  # base_delay * (2^1) = 2.0
        assert delay_2 == 4.0  # base_delay * (2^2) = 4.0

    def test_calculate_delay_with_max_limit(self):
        """Test delay calculation respects maximum delay"""
        # Large attempt number should be capped at max_delay
        delay = self.retry_handler.calculate_delay(10)
        assert delay == self.config.max_delay

    def test_calculate_delay_with_jitter(self):
        """Test delay calculation with jitter"""
        config_with_jitter = RetryConfiguration(
            base_delay=1.0,
            jitter=True,
            jitter_range=0.1
        )
        retry_handler = ExponentialBackoffRetry(config_with_jitter)

        # Calculate multiple delays and check they vary due to jitter
        delays = [retry_handler.calculate_delay(1) for _ in range(10)]

        # Should have some variation due to jitter
        assert len(set(delays)) > 1  # Not all delays should be identical
        assert all(delay >= 0 for delay in delays)  # All delays should be non-negative

    def test_is_retryable_exception(self):
        """Test retryable exception detection"""
        # Test BaseAmoException with retryable flag
        retryable_error = NetworkError("Network error", is_retryable=True)
        non_retryable_error = GoogleSheetsConfigError("Config error", is_retryable=False)

        assert self.retry_handler.is_retryable_exception(retryable_error) is True
        assert self.retry_handler.is_retryable_exception(non_retryable_error) is False

        # Test exceptions in retryable_exceptions list
        network_error = ConnectionError("Connection failed")
        assert self.retry_handler.is_retryable_exception(network_error) is True

        # Test non-retryable exception
        value_error = ValueError("Invalid value")
        assert self.retry_handler.is_retryable_exception(value_error) is False

    def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt"""
        mock_func = Mock(return_value="success")

        result = self.retry_handler.execute_with_retry(mock_func, "arg1", kwarg1="value1")

        assert result == "success"
        assert mock_func.call_count == 1
        mock_func.assert_called_with("arg1", kwarg1="value1")

    def test_execute_with_retry_success_after_retries(self):
        """Test successful execution after some retries"""
        mock_func = Mock()
        # Fail twice, then succeed
        mock_func.side_effect = [
            NetworkError("Network error 1", is_retryable=True),
            NetworkError("Network error 2", is_retryable=True),
            "success"
        ]

        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.retry_handler.execute_with_retry(mock_func)

        assert result == "success"
        assert mock_func.call_count == 3

    def test_execute_with_retry_max_retries_exceeded(self):
        """Test failure after max retries exceeded"""
        mock_func = Mock()
        # Always fail with retryable error
        mock_func.side_effect = NetworkError("Persistent network error", is_retryable=True)

        with patch('time.sleep'):  # Mock sleep to speed up test
            with pytest.raises(NetworkError):
                self.retry_handler.execute_with_retry(mock_func)

        # Should be called max_retries + 1 times (initial + retries)
        assert mock_func.call_count == self.config.max_retries + 1

    def test_execute_with_retry_non_retryable_error(self):
        """Test immediate failure for non-retryable errors"""
        mock_func = Mock()
        mock_func.side_effect = ValueError("Non-retryable error")

        with pytest.raises(ValueError):
            self.retry_handler.execute_with_retry(mock_func)

        # Should only be called once (no retries)
        assert mock_func.call_count == 1

    def test_execute_with_retry_rate_limit_delay(self):
        """Test that rate limit errors use their retry_after value"""
        mock_func = Mock()
        rate_limit_error = RateLimitError("Rate limited", retry_after=30, is_retryable=True)
        mock_func.side_effect = [rate_limit_error, "success"]

        with patch('time.sleep') as mock_sleep:
            result = self.retry_handler.execute_with_retry(mock_func)

        assert result == "success"
        # Should sleep for at least the retry_after time
        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        assert sleep_time >= 30

    def test_execute_with_retry_circuit_breaker_open(self):
        """Test that open circuit breaker prevents execution"""
        # Force circuit breaker to open state
        self.retry_handler.circuit_breaker.stats.state = CircuitBreakerState.OPEN
        self.retry_handler.circuit_breaker.stats.last_failure_time = time.time()

        mock_func = Mock()

        with pytest.raises(NetworkError, match="Circuit breaker is open"):
            self.retry_handler.execute_with_retry(mock_func)

        # Function should not be called when circuit is open
        assert mock_func.call_count == 0

    def test_get_circuit_breaker_stats(self):
        """Test getting circuit breaker statistics"""
        stats = self.retry_handler.get_circuit_breaker_stats()

        assert "state" in stats
        assert "total_requests" in stats
        assert "total_failures" in stats
        assert "total_successes" in stats


class TestRetryDecorator:
    """Test cases for @with_retry decorator"""

    def test_with_retry_decorator_success(self):
        """Test decorator with successful function"""
        @with_retry()
        def successful_function(x, y):
            return x + y

        result = successful_function(2, 3)
        assert result == 5

    def test_with_retry_decorator_with_retries(self):
        """Test decorator with function that fails then succeeds"""
        call_count = 0

        @with_retry(RetryConfiguration(max_retries=2, base_delay=0.01))
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise NetworkError("Network error", is_retryable=True)
            return "success"

        with patch('time.sleep'):  # Speed up test
            result = flaky_function()

        assert result == "success"
        assert call_count == 3

    def test_with_retry_decorator_access_stats(self):
        """Test accessing retry handler stats through decorated function"""
        @with_retry()
        def test_function():
            return "success"

        result = test_function()
        assert result == "success"

        # Should be able to access retry handler
        assert hasattr(test_function, '_retry_handler')
        stats = test_function._retry_handler.get_circuit_breaker_stats()
        assert "state" in stats


class TestRetryConfigurationPresets:
    """Test cases for retry configuration presets"""

    def test_google_api_retry_config(self):
        """Test Google API optimized retry configuration"""
        config = create_google_api_retry_config()

        assert config.max_retries == 5
        assert config.max_delay == 120.0
        assert config.circuit_breaker_enabled is True
        assert config.recovery_timeout == 300  # 5 minutes
        assert NetworkError in config.retryable_exceptions
        assert RateLimitError in config.retryable_exceptions

    def test_network_retry_config(self):
        """Test general network retry configuration"""
        config = create_network_retry_config()

        assert config.max_retries == 3
        assert config.base_delay == 2.0
        assert config.max_delay == 60.0
        assert config.circuit_breaker_enabled is True
        assert config.recovery_timeout == 120


if __name__ == '__main__':
    pytest.main([__file__, '-v'])