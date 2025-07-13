"""
Resilience mechanisms for AmoCRM Data Exporter

This module provides:
- Retry mechanisms with exponential backoff
- Circuit breaker pattern
- Bulkhead isolation
- Timeout handling
- Graceful degradation
"""

import asyncio
import time
import random
import functools
from typing import Any, Callable, Dict, List, Optional, Union, TypeVar, Coroutine
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from exceptions import (
    BaseAmoException,
    NetworkError,
    ApiError,
    RateLimitError,
    DatabaseError,
    is_retryable_error,
    get_retry_delay,
    ErrorContext,
    create_error_context
)
from ..core.logger import log_event

T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])


class RetryStrategy(str, Enum):
    """Retry strategy types"""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    RANDOM = "random"


class CircuitState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Circuit is open, requests fail fast
    HALF_OPEN = "half_open"  # Testing if service is back


@dataclass
class RetryConfig:
    """Configuration for retry mechanism"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    jitter: bool = True
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retryable_exceptions: List[type] = field(default_factory=lambda: [
        NetworkError, ApiError, DatabaseError
    ])


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    expected_exception: type = BaseAmoException
    success_threshold: int = 2  # For half-open state


@dataclass
class TimeoutConfig:
    """Configuration for timeout handling"""
    timeout: float = 30.0
    cancel_on_timeout: bool = True


class RetryMechanism:
    """Retry mechanism with various strategies"""

    def __init__(self, config: RetryConfig):
        self.config = config
        self.stats = {
            'attempts': 0,
            'successes': 0,
            'failures': 0,
            'retries': 0
        }

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Check if should retry based on exception and attempt count"""
        if attempt >= self.config.max_attempts:
            return False

        # Check if exception is retryable
        if isinstance(exception, BaseAmoException):
            return exception.is_retryable

        # Check against configured retryable exceptions
        return any(isinstance(exception, exc_type)
                  for exc_type in self.config.retryable_exceptions)

    def get_delay(self, attempt: int, exception: Optional[Exception] = None) -> float:
        """Calculate retry delay based on strategy"""
        if isinstance(exception, BaseAmoException) and exception.retry_after:
            base_delay = float(exception.retry_after)
        else:
            base_delay = self.config.base_delay

        if self.config.strategy == RetryStrategy.FIXED:
            delay = base_delay
        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = base_delay * (self.config.backoff_factor ** attempt)
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = base_delay * (attempt + 1)
        elif self.config.strategy == RetryStrategy.RANDOM:
            delay = random.uniform(base_delay, base_delay * 2)
        else:
            delay = base_delay

        # Apply jitter if enabled
        if self.config.jitter:
            jitter = random.uniform(-0.1, 0.1) * delay
            delay += jitter

        # Cap at max delay
        delay = min(delay, self.config.max_delay)

        return max(0, delay)

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with retry logic"""
        last_exception = None

        for attempt in range(self.config.max_attempts):
            self.stats['attempts'] += 1

            try:
                result = func(*args, **kwargs)
                self.stats['successes'] += 1
                return result

            except Exception as e:
                last_exception = e
                self.stats['failures'] += 1

                if not self.should_retry(e, attempt):
                    break

                if attempt < self.config.max_attempts - 1:
                    delay = self.get_delay(attempt, e)
                    self.stats['retries'] += 1

                    log_event(
                        "retry", "info",
                        f"Retrying {func.__name__} after {delay:.2f}s "
                        f"(attempt {attempt + 1}/{self.config.max_attempts}): {e}"
                    )

                    time.sleep(delay)

        # All retries exhausted
        if isinstance(last_exception, BaseAmoException):
            raise last_exception
        else:
            raise BaseAmoException(
                f"Function {func.__name__} failed after {self.config.max_attempts} attempts",
                original_error=last_exception,
                context=create_error_context(operation="retry_exhausted")
            )

    async def execute_async(self, coro_func: Callable[..., Coroutine[Any, Any, T]], *args, **kwargs) -> T:
        """Execute async function with retry logic"""
        last_exception = None

        for attempt in range(self.config.max_attempts):
            self.stats['attempts'] += 1

            try:
                result = await coro_func(*args, **kwargs)
                self.stats['successes'] += 1
                return result

            except Exception as e:
                last_exception = e
                self.stats['failures'] += 1

                if not self.should_retry(e, attempt):
                    break

                if attempt < self.config.max_attempts - 1:
                    delay = self.get_delay(attempt, e)
                    self.stats['retries'] += 1

                    log_event(
                        "retry", "info",
                        f"Retrying {coro_func.__name__} after {delay:.2f}s "
                        f"(attempt {attempt + 1}/{self.config.max_attempts}): {e}"
                    )

                    await asyncio.sleep(delay)

        # All retries exhausted
        if isinstance(last_exception, BaseAmoException):
            raise last_exception
        else:
            raise BaseAmoException(
                f"Async function {coro_func.__name__} failed after {self.config.max_attempts} attempts",
                original_error=last_exception,
                context=create_error_context(operation="async_retry_exhausted")
            )

    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics"""
        return self.stats.copy()


class CircuitBreaker:
    """Circuit breaker implementation"""

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.lock = threading.Lock()

        # Statistics
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'circuit_opened': 0,
            'circuit_closed': 0,
            'fast_failures': 0
        }

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt to reset circuit"""
        if self.last_failure_time is None:
            return True

        time_since_failure = time.time() - self.last_failure_time
        return time_since_failure >= self.config.recovery_timeout

    def _record_success(self):
        """Record successful execution"""
        with self.lock:
            self.stats['total_requests'] += 1
            self.stats['successful_requests'] += 1

            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    self.stats['circuit_closed'] += 1
                    log_event("circuit_breaker", "info", f"Circuit {self.name} closed")

    def _record_failure(self, exception: Exception):
        """Record failed execution"""
        with self.lock:
            self.stats['total_requests'] += 1
            self.stats['failed_requests'] += 1

            # Only count configured exceptions as failures
            if isinstance(exception, self.config.expected_exception):
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.state == CircuitState.CLOSED:
                    if self.failure_count >= self.config.failure_threshold:
                        self.state = CircuitState.OPEN
                        self.stats['circuit_opened'] += 1
                        log_event("circuit_breaker", "warning",
                                f"Circuit {self.name} opened after {self.failure_count} failures")

                elif self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.OPEN
                    self.success_count = 0
                    log_event("circuit_breaker", "warning",
                            f"Circuit {self.name} reopened due to failure in half-open state")

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function through circuit breaker"""
        with self.lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    log_event("circuit_breaker", "info", f"Circuit {self.name} half-open")
                else:
                    self.stats['fast_failures'] += 1
                    raise BaseAmoException(
                        f"Circuit breaker {self.name} is open",
                        context=create_error_context(component="circuit_breaker")
                    )

        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure(e)
            raise

    async def call_async(self, coro_func: Callable[..., Coroutine[Any, Any, T]], *args, **kwargs) -> T:
        """Execute async function through circuit breaker"""
        with self.lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    log_event("circuit_breaker", "info", f"Circuit {self.name} half-open")
                else:
                    self.stats['fast_failures'] += 1
                    raise BaseAmoException(
                        f"Circuit breaker {self.name} is open",
                        context=create_error_context(component="circuit_breaker")
                    )

        try:
            result = await coro_func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure(e)
            raise

    def get_state(self) -> CircuitState:
        """Get current circuit state"""
        return self.state

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        with self.lock:
            return {
                **self.stats,
                'state': self.state.value,
                'failure_count': self.failure_count,
                'success_count': self.success_count,
                'last_failure_time': self.last_failure_time
            }

    def reset(self):
        """Manually reset circuit breaker"""
        with self.lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            log_event("circuit_breaker", "info", f"Circuit {self.name} manually reset")


class TimeoutHandler:
    """Timeout handler for operations"""

    def __init__(self, config: TimeoutConfig):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=10)

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with timeout"""
        future = self.executor.submit(func, *args, **kwargs)

        try:
            return future.result(timeout=self.config.timeout)
        except FutureTimeoutError:
            if self.config.cancel_on_timeout:
                future.cancel()

            raise BaseAmoException(
                f"Operation {func.__name__} timed out after {self.config.timeout}s",
                context=create_error_context(operation="timeout")
            )

    async def execute_async(self, coro_func: Callable[..., Coroutine[Any, Any, T]], *args, **kwargs) -> T:
        """Execute async function with timeout"""
        try:
            return await asyncio.wait_for(
                coro_func(*args, **kwargs),
                timeout=self.config.timeout
            )
        except asyncio.TimeoutError:
            raise BaseAmoException(
                f"Async operation {coro_func.__name__} timed out after {self.config.timeout}s",
                context=create_error_context(operation="async_timeout")
            )


class ResilienceManager:
    """Central manager for resilience mechanisms"""

    def __init__(self):
        self.retry_configs: Dict[str, RetryConfig] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.timeout_configs: Dict[str, TimeoutConfig] = {}
        self.timeout_handler = TimeoutHandler(TimeoutConfig())

    def add_retry_config(self, name: str, config: RetryConfig):
        """Add retry configuration"""
        self.retry_configs[name] = config

    def add_circuit_breaker(self, name: str, config: CircuitBreakerConfig):
        """Add circuit breaker"""
        self.circuit_breakers[name] = CircuitBreaker(name, config)

    def add_timeout_config(self, name: str, config: TimeoutConfig):
        """Add timeout configuration"""
        self.timeout_configs[name] = config

    def get_retry_mechanism(self, name: str) -> RetryMechanism:
        """Get retry mechanism"""
        config = self.retry_configs.get(name, RetryConfig())
        return RetryMechanism(config)

    def get_circuit_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit breaker"""
        return self.circuit_breakers.get(name)

    def get_timeout_handler(self, name: str) -> TimeoutHandler:
        """Get timeout handler"""
        config = self.timeout_configs.get(name, TimeoutConfig())
        return TimeoutHandler(config)

    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all resilience mechanisms"""
        return {
            'circuit_breakers': {
                name: cb.get_stats()
                for name, cb in self.circuit_breakers.items()
            }
        }


# Global resilience manager
resilience_manager = ResilienceManager()

# Default configurations
resilience_manager.add_retry_config("api", RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    strategy=RetryStrategy.EXPONENTIAL,
    retryable_exceptions=[NetworkError, ApiError, RateLimitError]
))

resilience_manager.add_circuit_breaker("api", CircuitBreakerConfig(
    failure_threshold=5,
    recovery_timeout=60.0,
    expected_exception=ApiError
))

resilience_manager.add_retry_config("database", RetryConfig(
    max_attempts=5,
    base_delay=0.5,
    max_delay=10.0,
    strategy=RetryStrategy.EXPONENTIAL,
    retryable_exceptions=[DatabaseError]
))

resilience_manager.add_circuit_breaker("database", CircuitBreakerConfig(
    failure_threshold=3,
    recovery_timeout=30.0,
    expected_exception=DatabaseError
))

resilience_manager.add_timeout_config("api", TimeoutConfig(timeout=30.0))
resilience_manager.add_timeout_config("database", TimeoutConfig(timeout=10.0))


# Decorators for easy usage
def retry(config_name: str = "api"):
    """Decorator to add retry logic to functions"""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_mechanism = resilience_manager.get_retry_mechanism(config_name)
            return retry_mechanism.execute(func, *args, **kwargs)
        return wrapper
    return decorator


def circuit_breaker(name: str = "api"):
    """Decorator to add circuit breaker to functions"""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cb = resilience_manager.get_circuit_breaker(name)
            if cb:
                return cb.call(func, *args, **kwargs)
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator


def timeout(config_name: str = "api"):
    """Decorator to add timeout to functions"""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            timeout_handler = resilience_manager.get_timeout_handler(config_name)
            return timeout_handler.execute(func, *args, **kwargs)
        return wrapper
    return decorator


def resilient(retry_config: str = "api", circuit_breaker_name: str = "api", timeout_config: str = "api"):
    """Decorator to add full resilience (retry + circuit breaker + timeout)"""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Create a combined function with all resilience mechanisms
            def resilient_func():
                cb = resilience_manager.get_circuit_breaker(circuit_breaker_name)
                timeout_handler = resilience_manager.get_timeout_handler(timeout_config)

                def timeout_func():
                    if cb:
                        return cb.call(func, *args, **kwargs)
                    else:
                        return func(*args, **kwargs)

                return timeout_handler.execute(timeout_func)

            retry_mechanism = resilience_manager.get_retry_mechanism(retry_config)
            return retry_mechanism.execute(resilient_func)

        return wrapper
    return decorator