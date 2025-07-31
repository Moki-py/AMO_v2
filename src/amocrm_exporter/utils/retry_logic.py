"""
Retry logic and exponential backoff implementation for Google Sheets API
"""

import time
import random
import logging
from typing import Any, Callable, Optional, Type, Union, List
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps

from .exceptions import (
    BaseAmoException,
    NetworkError,
    ApiError,
    RateLimitError,
    ErrorContext,
    create_error_context
)


class CircuitBreakerState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class RetryConfiguration:
    """Configuration for retry logic with exponential backoff"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_range: float = 0.1  # ±10% jitter
    backoff_multiplier: float = 1.0

    # Circuit breaker configuration
    circuit_breaker_enabled: bool = True
    failure_threshold: int = 5  # Number of failures before opening circuit
    recovery_timeout: int = 60  # Seconds to wait before trying again
    success_threshold: int = 2  # Successful calls needed to close circuit

    # Retry conditions
    retryable_exceptions: List[Type[Exception]] = field(default_factory=lambda: [
        NetworkError,
        RateLimitError,
        ConnectionError,
        TimeoutError,
    ])

    def __post_init__(self):
        """Validate configuration parameters"""
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.max_delay <= 0:
            raise ValueError("max_delay must be positive")
        if self.exponential_base <= 1:
            raise ValueError("exponential_base must be greater than 1")
        if not 0 <= self.jitter_range <= 1:
            raise ValueError("jitter_range must be between 0 and 1")


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics"""
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    total_requests: int = 0
    total_failures: int = 0
    total_successes: int = 0


class CircuitBreaker:
    """Circuit breaker implementation for network connectivity issues"""

    def __init__(self, config: RetryConfiguration):
        self.config = config
        self.stats = CircuitBreakerStats()
        self.logger = logging.getLogger(__name__)

    def can_execute(self) -> bool:
        """Check if request can be executed based on circuit breaker state"""
        if not self.config.circuit_breaker_enabled:
            return True

        current_time = time.time()

        if self.stats.state == CircuitBreakerState.CLOSED:
            return True
        elif self.stats.state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has passed
            if (self.stats.last_failure_time and
                current_time - self.stats.last_failure_time >= self.config.recovery_timeout):
                self.stats.state = CircuitBreakerState.HALF_OPEN
                self.stats.success_count = 0
                self.logger.info("Circuit breaker transitioning to HALF_OPEN state")
                return True
            return False
        else:  # HALF_OPEN
            return True

    def record_success(self):
        """Record a successful operation"""
        current_time = time.time()
        self.stats.last_success_time = current_time
        self.stats.total_requests += 1
        self.stats.total_successes += 1

        if self.stats.state == CircuitBreakerState.HALF_OPEN:
            self.stats.success_count += 1
            if self.stats.success_count >= self.config.success_threshold:
                self.stats.state = CircuitBreakerState.CLOSED
                self.stats.failure_count = 0
                self.logger.info("Circuit breaker closed after successful recovery")
        elif self.stats.state == CircuitBreakerState.CLOSED:
            # Reset failure count on success
            self.stats.failure_count = 0

    def record_failure(self):
        """Record a failed operation"""
        current_time = time.time()
        self.stats.last_failure_time = current_time
        self.stats.total_requests += 1
        self.stats.total_failures += 1
        self.stats.failure_count += 1

        if self.stats.state == CircuitBreakerState.CLOSED:
            if self.stats.failure_count >= self.config.failure_threshold:
                self.stats.state = CircuitBreakerState.OPEN
                self.logger.warning(f"Circuit breaker opened after {self.stats.failure_count} failures")
        elif self.stats.state == CircuitBreakerState.HALF_OPEN:
            # Go back to open state
            self.stats.state = CircuitBreakerState.OPEN
            self.logger.warning("Circuit breaker reopened after failure in HALF_OPEN state")

    def get_stats(self) -> dict:
        """Get circuit breaker statistics"""
        return {
            "state": self.stats.state.value,
            "failure_count": self.stats.failure_count,
            "success_count": self.stats.success_count,
            "total_requests": self.stats.total_requests,
            "total_failures": self.stats.total_failures,
            "total_successes": self.stats.total_successes,
            "last_failure_time": self.stats.last_failure_time,
            "last_success_time": self.stats.last_success_time,
        }


class ExponentialBackoffRetry:
    """Exponential backoff retry mechanism with jitter and circuit breaker"""

    def __init__(self, config: RetryConfiguration):
        self.config = config
        self.circuit_breaker = CircuitBreaker(config)
        self.logger = logging.getLogger(__name__)

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt with exponential backoff and jitter"""
        # Calculate exponential delay
        delay = (self.config.base_delay *
                (self.config.exponential_base ** attempt) *
                self.config.backoff_multiplier)

        # Apply maximum delay limit
        delay = min(delay, self.config.max_delay)

        # Apply jitter if enabled
        if self.config.jitter:
            jitter_amount = delay * self.config.jitter_range
            jitter = random.uniform(-jitter_amount, jitter_amount)
            delay = max(0, delay + jitter)

        return delay

    def is_retryable_exception(self, exception: Exception) -> bool:
        """Check if exception is retryable based on configuration"""
        # Check if it's a BaseAmoException with retryable flag
        if isinstance(exception, BaseAmoException):
            return exception.is_retryable

        # Check if it's in the list of retryable exceptions
        return any(isinstance(exception, exc_type) for exc_type in self.config.retryable_exceptions)

    def execute_with_retry(
        self,
        func: Callable[..., Any],
        *args,
        context: Optional[ErrorContext] = None,
        **kwargs
    ) -> Any:
        """Execute function with retry logic and circuit breaker"""
        if context is None:
            context = create_error_context(
                component="retry_logic",
                operation=func.__name__ if hasattr(func, '__name__') else "unknown"
            )

        last_exception = None

        for attempt in range(self.config.max_retries + 1):
            # Check circuit breaker
            if not self.circuit_breaker.can_execute():
                raise NetworkError(
                    "Circuit breaker is open - service appears to be down",
                    context=context,
                    original_error=last_exception
                )

            try:
                # Execute the function
                result = func(*args, **kwargs)

                # Record success
                self.circuit_breaker.record_success()

                if attempt > 0:
                    self.logger.info(f"Operation succeeded after {attempt} retries")

                return result

            except Exception as e:
                last_exception = e

                # Record failure
                self.circuit_breaker.record_failure()

                # Check if this is the last attempt
                if attempt >= self.config.max_retries:
                    self.logger.error(f"Operation failed after {self.config.max_retries} retries: {e}")
                    raise

                # Check if exception is retryable
                if not self.is_retryable_exception(e):
                    self.logger.error(f"Non-retryable exception encountered: {e}")
                    raise

                # Calculate delay and wait
                delay = self.calculate_delay(attempt)

                # Handle rate limit specific delay
                if isinstance(e, RateLimitError) and e.retry_after:
                    delay = max(delay, e.retry_after)

                self.logger.warning(
                    f"Attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {delay:.2f} seconds..."
                )

                time.sleep(delay)

        # This should never be reached, but just in case
        raise last_exception or Exception("Unknown error in retry logic")

    def get_circuit_breaker_stats(self) -> dict:
        """Get circuit breaker statistics"""
        return self.circuit_breaker.get_stats()


def with_retry(config: Optional[RetryConfiguration] = None):
    """Decorator for adding retry logic to functions"""
    if config is None:
        config = RetryConfiguration()

    retry_handler = ExponentialBackoffRetry(config)

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            context = create_error_context(
                component="decorated_function",
                operation=func.__name__
            )
            return retry_handler.execute_with_retry(func, *args, context=context, **kwargs)

        # Attach retry handler for access to stats
        wrapper._retry_handler = retry_handler
        return wrapper

    return decorator


# Convenience functions for common retry configurations
def create_google_api_retry_config() -> RetryConfiguration:
    """Create retry configuration optimized for Google API calls"""
    return RetryConfiguration(
        max_retries=5,
        base_delay=1.0,
        max_delay=120.0,
        exponential_base=2.0,
        jitter=True,
        jitter_range=0.1,
        circuit_breaker_enabled=True,
        failure_threshold=3,
        recovery_timeout=300,  # 5 minutes
        success_threshold=2,
        retryable_exceptions=[
            NetworkError,
            RateLimitError,
            ConnectionError,
            TimeoutError,
            # Add Google API specific exceptions
        ]
    )


def create_network_retry_config() -> RetryConfiguration:
    """Create retry configuration for general network operations"""
    return RetryConfiguration(
        max_retries=3,
        base_delay=2.0,
        max_delay=60.0,
        exponential_base=2.0,
        jitter=True,
        circuit_breaker_enabled=True,
        failure_threshold=5,
        recovery_timeout=120,
        success_threshold=2
    )