"""
Intelligent retry scheduling system for failed batches
"""

import asyncio
import time
from typing import Dict, List, Optional, Callable, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import heapq
import threading
import logging

from .batch_processor import BatchResult, BatchStatus, BatchProcessingConfig
from .retry_logic import RetryConfiguration, ExponentialBackoffRetry
from .exceptions import RetrySchedulingError, create_error_context
from ..core.logger import log_event


class RetryPriority(str, Enum):
    """Priority levels for retry scheduling"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class RetryStrategy(str, Enum):
    """Different retry strategies"""
    IMMEDIATE = "immediate"          # Retry immediately
    EXPONENTIAL_BACKOFF = "exponential_backoff"  # Standard exponential backoff
    ADAPTIVE = "adaptive"            # Adapt based on failure patterns
    SCHEDULED = "scheduled"          # Retry at specific times
    CIRCUIT_BREAKER = "circuit_breaker"  # Use circuit breaker pattern


@dataclass
class RetryTask:
    """Represents a batch that needs to be retried"""
    batch_id: str
    original_batch_result: BatchResult
    retry_count: int = 0
    max_retries: int = 3
    priority: RetryPriority = RetryPriority.NORMAL
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    next_retry_time: float = 0.0
    created_at: float = field(default_factory=time.time)
    last_retry_time: Optional[float] = None
    failure_reasons: List[str] = field(default_factory=list)
    retry_data: Any = None  # Original data to retry
    processor_func: Optional[Callable] = None

    def __lt__(self, other):
        """For priority queue ordering"""
        # Higher priority and earlier retry time come first
        priority_order = {
            RetryPriority.CRITICAL: 0,
            RetryPriority.HIGH: 1,
            RetryPriority.NORMAL: 2,
            RetryPriority.LOW: 3
        }

        if priority_order[self.priority] != priority_order[other.priority]:
            return priority_order[self.priority] < priority_order[other.priority]

        return self.next_retry_time < other.next_retry_time


@dataclass
class RetrySchedulerConfig:
    """Configuration for the retry scheduler"""
    # Basic retry settings
    max_concurrent_retries: int = 3
    default_max_retries: int = 5
    retry_queue_size: int = 1000

    # Timing settings
    min_retry_delay: float = 1.0
    max_retry_delay: float = 300.0  # 5 minutes
    exponential_base: float = 2.0
    jitter_factor: float = 0.1

    # Adaptive settings
    enable_adaptive_retry: bool = True
    failure_pattern_window: int = 10  # Look at last N failures
    success_rate_threshold: float = 0.3  # Adjust strategy if success rate < 30%

    # Circuit breaker settings
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_time: float = 60.0

    # Scheduled retry settings
    scheduled_retry_intervals: List[float] = field(default_factory=lambda: [60, 300, 900, 3600])  # 1m, 5m, 15m, 1h

    # Priority settings
    priority_multipliers: Dict[RetryPriority, float] = field(default_factory=lambda: {
        RetryPriority.CRITICAL: 0.1,  # Retry much sooner
        RetryPriority.HIGH: 0.5,
        RetryPriority.NORMAL: 1.0,
        RetryPriority.LOW: 2.0  # Retry later
    })


@dataclass
class RetryStats:
    """Statistics for retry operations"""
    total_retries: int = 0
    successful_retries: int = 0
    failed_retries: int = 0
    abandoned_retries: int = 0
    avg_retry_delay: float = 0.0
    retry_success_rate: float = 0.0
    strategy_usage: Dict[RetryStrategy, int] = field(default_factory=dict)
    priority_distribution: Dict[RetryPriority, int] = field(default_factory=dict)


class FailurePatternAnalyzer:
    """Analyzes failure patterns to optimize retry strategies"""

    def __init__(self, config: RetrySchedulerConfig):
        self.config = config
        self.failure_history: List[Dict[str, Any]] = []
        self.success_history: List[Dict[str, Any]] = []
        self.logger = logging.getLogger(__name__)

    def record_failure(self, batch_id: str, error: str, retry_count: int) -> None:
        """Record a batch failure"""
        failure_record = {
            "batch_id": batch_id,
            "error": error,
            "retry_count": retry_count,
            "timestamp": time.time(),
            "error_type": self._classify_error(error)
        }

        self.failure_history.append(failure_record)

        # Keep only recent history
        if len(self.failure_history) > self.config.failure_pattern_window * 2:
            self.failure_history = self.failure_history[-self.config.failure_pattern_window:]

    def record_success(self, batch_id: str, retry_count: int) -> None:
        """Record a successful retry"""
        success_record = {
            "batch_id": batch_id,
            "retry_count": retry_count,
            "timestamp": time.time()
        }

        self.success_history.append(success_record)

        # Keep only recent history
        if len(self.success_history) > self.config.failure_pattern_window * 2:
            self.success_history = self.success_history[-self.config.failure_pattern_window:]

    def _classify_error(self, error: str) -> str:
        """Classify error type for pattern analysis"""
        error_lower = error.lower()

        if "rate limit" in error_lower or "quota" in error_lower:
            return "rate_limit"
        elif "timeout" in error_lower:
            return "timeout"
        elif "network" in error_lower or "connection" in error_lower:
            return "network"
        elif "permission" in error_lower or "auth" in error_lower:
            return "auth"
        elif "memory" in error_lower:
            return "memory"
        else:
            return "unknown"

    def analyze_patterns(self) -> Dict[str, Any]:
        """Analyze failure patterns and suggest optimal retry strategy"""
        if len(self.failure_history) < 3:
            return {"strategy": RetryStrategy.EXPONENTIAL_BACKOFF, "confidence": 0.0}

        recent_failures = self.failure_history[-self.config.failure_pattern_window:]
        recent_successes = self.success_history[-self.config.failure_pattern_window:]

        # Calculate success rate
        total_attempts = len(recent_failures) + len(recent_successes)
        success_rate = len(recent_successes) / total_attempts if total_attempts > 0 else 0.0

        # Analyze error types
        error_types = {}
        for failure in recent_failures:
            error_type = failure["error_type"]
            error_types[error_type] = error_types.get(error_type, 0) + 1

        # Determine optimal strategy
        dominant_error = max(error_types.items(), key=lambda x: x[1])[0] if error_types else "unknown"

        strategy = self._suggest_strategy_for_error_type(dominant_error, success_rate)
        confidence = min(len(recent_failures) / self.config.failure_pattern_window, 1.0)

        return {
            "strategy": strategy,
            "confidence": confidence,
            "success_rate": success_rate,
            "dominant_error": dominant_error,
            "error_distribution": error_types,
            "total_attempts": total_attempts
        }

    def _suggest_strategy_for_error_type(self, error_type: str, success_rate: float) -> RetryStrategy:
        """Suggest retry strategy based on error type and success rate"""
        if success_rate < self.config.success_rate_threshold:
            # Low success rate, use circuit breaker
            return RetryStrategy.CIRCUIT_BREAKER

        if error_type == "rate_limit":
            return RetryStrategy.SCHEDULED  # Use scheduled intervals for rate limits
        elif error_type == "network":
            return RetryStrategy.EXPONENTIAL_BACKOFF  # Standard backoff for network issues
        elif error_type == "timeout":
            return RetryStrategy.ADAPTIVE  # Adaptive for timeouts
        elif error_type == "auth":
            return RetryStrategy.IMMEDIATE  # Auth issues might be temporary
        elif error_type == "memory":
            return RetryStrategy.SCHEDULED  # Give time for memory to clear
        else:
            return RetryStrategy.EXPONENTIAL_BACKOFF  # Default strategy


class IntelligentRetryScheduler:
    """Main intelligent retry scheduler"""

    def __init__(self, config: Optional[RetrySchedulerConfig] = None):
        self.config = config or RetrySchedulerConfig()
        self.pattern_analyzer = FailurePatternAnalyzer(self.config)

        # Priority queue for retry tasks
        self.retry_queue: List[RetryTask] = []
        self.active_retries: Dict[str, RetryTask] = {}
        self.completed_retries: Dict[str, RetryTask] = {}

        # Thread safety
        self._lock = threading.Lock()
        self._running = False
        self._scheduler_task: Optional[asyncio.Task] = None

        # Statistics
        self.stats = RetryStats()

        # Circuit breaker state
        self.circuit_breaker_state: Dict[str, Dict[str, Any]] = {}

        self.logger = logging.getLogger(__name__)

    async def start_scheduler(self) -> None:
        """Start the retry scheduler"""
        if self._running:
            return

        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        log_event("retry_scheduler", "info", "Intelligent retry scheduler started")

    async def stop_scheduler(self) -> None:
        """Stop the retry scheduler"""
        self._running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        log_event("retry_scheduler", "info", "Intelligent retry scheduler stopped")

    def schedule_retry(
        self,
        batch_result: BatchResult,
        retry_data: Any,
        processor_func: Callable,
        priority: RetryPriority = RetryPriority.NORMAL,
        max_retries: Optional[int] = None
    ) -> str:
        """Schedule a batch for retry"""
        if max_retries is None:
            max_retries = self.config.default_max_retries

        # Analyze patterns to determine optimal strategy
        pattern_analysis = self.pattern_analyzer.analyze_patterns()
        strategy = pattern_analysis["strategy"]

        # Create retry task
        retry_task = RetryTask(
            batch_id=batch_result.batch_id,
            original_batch_result=batch_result,
            max_retries=max_retries,
            priority=priority,
            strategy=strategy,
            retry_data=retry_data,
            processor_func=processor_func,
            failure_reasons=batch_result.errors.copy()
        )

        # Calculate next retry time
        retry_task.next_retry_time = self._calculate_next_retry_time(retry_task)

        # Add to queue
        with self._lock:
            if len(self.retry_queue) >= self.config.retry_queue_size:
                # Remove lowest priority task
                self.retry_queue.sort()
                removed_task = self.retry_queue.pop()
                log_event("retry_scheduler", "warning",
                         f"Retry queue full, removed task {removed_task.batch_id}")
                self.stats.abandoned_retries += 1

            heapq.heappush(self.retry_queue, retry_task)

            # Update stats
            self.stats.priority_distribution[priority] = self.stats.priority_distribution.get(priority, 0) + 1
            self.stats.strategy_usage[strategy] = self.stats.strategy_usage.get(strategy, 0) + 1

        log_event("retry_scheduler", "info",
                 f"Scheduled retry for batch {batch_result.batch_id} "
                 f"(strategy: {strategy}, priority: {priority}, "
                 f"next retry: {retry_task.next_retry_time - time.time():.1f}s)")

        return retry_task.batch_id

    def _calculate_next_retry_time(self, retry_task: RetryTask) -> float:
        """Calculate when to retry based on strategy"""
        current_time = time.time()
        base_delay = self.config.min_retry_delay

        if retry_task.strategy == RetryStrategy.IMMEDIATE:
            delay = 0.0

        elif retry_task.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = min(
                base_delay * (self.config.exponential_base ** retry_task.retry_count),
                self.config.max_retry_delay
            )

        elif retry_task.strategy == RetryStrategy.ADAPTIVE:
            # Adaptive delay based on failure patterns
            pattern_analysis = self.pattern_analyzer.analyze_patterns()
            success_rate = pattern_analysis.get("success_rate", 0.5)

            # Lower success rate = longer delay
            adaptive_multiplier = 2.0 - success_rate  # Range: 1.0 to 2.0
            delay = min(
                base_delay * (self.config.exponential_base ** retry_task.retry_count) * adaptive_multiplier,
                self.config.max_retry_delay
            )

        elif retry_task.strategy == RetryStrategy.SCHEDULED:
            # Use predefined intervals
            if retry_task.retry_count < len(self.config.scheduled_retry_intervals):
                delay = self.config.scheduled_retry_intervals[retry_task.retry_count]
            else:
                delay = self.config.scheduled_retry_intervals[-1]  # Use last interval

        elif retry_task.strategy == RetryStrategy.CIRCUIT_BREAKER:
            # Check circuit breaker state
            error_type = self._get_dominant_error_type(retry_task.failure_reasons)
            if self._is_circuit_open(error_type):
                delay = self.config.circuit_breaker_recovery_time
            else:
                delay = base_delay * (self.config.exponential_base ** retry_task.retry_count)

        else:
            delay = base_delay * (self.config.exponential_base ** retry_task.retry_count)

        # Apply priority multiplier
        priority_multiplier = self.config.priority_multipliers.get(retry_task.priority, 1.0)
        delay *= priority_multiplier

        # Apply jitter
        if self.config.jitter_factor > 0:
            jitter = delay * self.config.jitter_factor * (2 * (time.time() % 1) - 1)  # ±jitter_factor
            delay = max(0, delay + jitter)

        # Ensure within bounds
        delay = max(self.config.min_retry_delay, min(delay, self.config.max_retry_delay))

        return current_time + delay

    def _get_dominant_error_type(self, errors: List[str]) -> str:
        """Get the dominant error type from a list of errors"""
        if not errors:
            return "unknown"

        # Use the first error for simplicity
        return self.pattern_analyzer._classify_error(errors[0])

    def _is_circuit_open(self, error_type: str) -> bool:
        """Check if circuit breaker is open for given error type"""
        if error_type not in self.circuit_breaker_state:
            self.circuit_breaker_state[error_type] = {
                "failure_count": 0,
                "last_failure_time": 0,
                "is_open": False
            }

        state = self.circuit_breaker_state[error_type]
        current_time = time.time()

        # Check if circuit should be closed (recovery time passed)
        if (state["is_open"] and
            current_time - state["last_failure_time"] >= self.config.circuit_breaker_recovery_time):
            state["is_open"] = False
            state["failure_count"] = 0
            log_event("retry_scheduler", "info", f"Circuit breaker closed for {error_type}")

        return state["is_open"]

    def _update_circuit_breaker(self, error_type: str, success: bool) -> None:
        """Update circuit breaker state"""
        if error_type not in self.circuit_breaker_state:
            self.circuit_breaker_state[error_type] = {
                "failure_count": 0,
                "last_failure_time": 0,
                "is_open": False
            }

        state = self.circuit_breaker_state[error_type]

        if success:
            state["failure_count"] = 0
        else:
            state["failure_count"] += 1
            state["last_failure_time"] = time.time()

            if state["failure_count"] >= self.config.circuit_breaker_failure_threshold:
                state["is_open"] = True
                log_event("retry_scheduler", "warning",
                         f"Circuit breaker opened for {error_type} after {state['failure_count']} failures")

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop"""
        while self._running:
            try:
                await self._process_retry_queue()
                await asyncio.sleep(1.0)  # Check every second
            except Exception as e:
                log_event("retry_scheduler", "error", f"Error in scheduler loop: {e}")
                await asyncio.sleep(5.0)  # Wait longer on error

    async def _process_retry_queue(self) -> None:
        """Process the retry queue"""
        current_time = time.time()
        tasks_to_retry = []

        with self._lock:
            # Check if we can process more retries
            if len(self.active_retries) >= self.config.max_concurrent_retries:
                return

            # Find tasks ready for retry
            while (self.retry_queue and
                   len(tasks_to_retry) + len(self.active_retries) < self.config.max_concurrent_retries):

                if self.retry_queue[0].next_retry_time <= current_time:
                    task = heapq.heappop(self.retry_queue)
                    tasks_to_retry.append(task)
                else:
                    break  # No more tasks ready

        # Process ready tasks
        for task in tasks_to_retry:
            await self._execute_retry(task)

    async def _execute_retry(self, retry_task: RetryTask) -> None:
        """Execute a retry task"""
        retry_task.retry_count += 1
        retry_task.last_retry_time = time.time()

        # Add to active retries
        with self._lock:
            self.active_retries[retry_task.batch_id] = retry_task

        log_event("retry_scheduler", "info",
                 f"Executing retry {retry_task.retry_count}/{retry_task.max_retries} "
                 f"for batch {retry_task.batch_id}")

        try:
            # Execute the retry
            result = await asyncio.get_event_loop().run_in_executor(
                None, retry_task.processor_func, retry_task.retry_data
            )

            # Retry succeeded
            self._handle_retry_success(retry_task)

        except Exception as e:
            # Retry failed
            await self._handle_retry_failure(retry_task, str(e))

        finally:
            # Remove from active retries
            with self._lock:
                self.active_retries.pop(retry_task.batch_id, None)

    def _handle_retry_success(self, retry_task: RetryTask) -> None:
        """Handle successful retry"""
        log_event("retry_scheduler", "info",
                 f"Retry succeeded for batch {retry_task.batch_id} "
                 f"after {retry_task.retry_count} attempts")

        # Record success
        self.pattern_analyzer.record_success(retry_task.batch_id, retry_task.retry_count)

        # Update circuit breaker
        error_type = self._get_dominant_error_type(retry_task.failure_reasons)
        self._update_circuit_breaker(error_type, success=True)

        # Update stats
        self.stats.total_retries += 1
        self.stats.successful_retries += 1
        self._update_success_rate()

        # Move to completed
        with self._lock:
            self.completed_retries[retry_task.batch_id] = retry_task

    async def _handle_retry_failure(self, retry_task: RetryTask, error: str) -> None:
        """Handle failed retry"""
        retry_task.failure_reasons.append(error)

        # Record failure
        self.pattern_analyzer.record_failure(retry_task.batch_id, error, retry_task.retry_count)

        # Update circuit breaker
        error_type = self._get_dominant_error_type([error])
        self._update_circuit_breaker(error_type, success=False)

        # Check if we should retry again
        if retry_task.retry_count < retry_task.max_retries:
            # Schedule another retry
            retry_task.next_retry_time = self._calculate_next_retry_time(retry_task)

            with self._lock:
                heapq.heappush(self.retry_queue, retry_task)

            log_event("retry_scheduler", "warning",
                     f"Retry {retry_task.retry_count} failed for batch {retry_task.batch_id}: {error}. "
                     f"Next retry in {retry_task.next_retry_time - time.time():.1f}s")
        else:
            # Max retries reached, abandon
            log_event("retry_scheduler", "error",
                     f"Abandoning batch {retry_task.batch_id} after {retry_task.retry_count} failed retries")

            self.stats.abandoned_retries += 1

            with self._lock:
                self.completed_retries[retry_task.batch_id] = retry_task

        # Update stats
        self.stats.total_retries += 1
        self.stats.failed_retries += 1
        self._update_success_rate()

    def _update_success_rate(self) -> None:
        """Update retry success rate"""
        if self.stats.total_retries > 0:
            self.stats.retry_success_rate = self.stats.successful_retries / self.stats.total_retries

    def get_retry_stats(self) -> Dict[str, Any]:
        """Get comprehensive retry statistics"""
        with self._lock:
            active_count = len(self.active_retries)
            queued_count = len(self.retry_queue)
            completed_count = len(self.completed_retries)

        pattern_analysis = self.pattern_analyzer.analyze_patterns()

        return {
            "stats": {
                "total_retries": self.stats.total_retries,
                "successful_retries": self.stats.successful_retries,
                "failed_retries": self.stats.failed_retries,
                "abandoned_retries": self.stats.abandoned_retries,
                "retry_success_rate": self.stats.retry_success_rate,
                "strategy_usage": dict(self.stats.strategy_usage),
                "priority_distribution": dict(self.stats.priority_distribution)
            },
            "queue_status": {
                "active_retries": active_count,
                "queued_retries": queued_count,
                "completed_retries": completed_count,
                "queue_capacity": self.config.retry_queue_size
            },
            "pattern_analysis": pattern_analysis,
            "circuit_breaker_states": dict(self.circuit_breaker_state)
        }

    def get_active_retries(self) -> Dict[str, Dict[str, Any]]:
        """Get information about active retries"""
        with self._lock:
            return {
                batch_id: {
                    "retry_count": task.retry_count,
                    "max_retries": task.max_retries,
                    "priority": task.priority.value,
                    "strategy": task.strategy.value,
                    "last_retry_time": task.last_retry_time,
                    "failure_reasons": task.failure_reasons[-3:]  # Last 3 errors
                }
                for batch_id, task in self.active_retries.items()
            }

    def cancel_retry(self, batch_id: str) -> bool:
        """Cancel a scheduled retry"""
        with self._lock:
            # Remove from queue
            self.retry_queue = [task for task in self.retry_queue if task.batch_id != batch_id]
            heapq.heapify(self.retry_queue)

            # Remove from active retries (if possible)
            if batch_id in self.active_retries:
                task = self.active_retries.pop(batch_id)
                task.failure_reasons.append("Cancelled by user")
                self.completed_retries[batch_id] = task
                return True

        return False

    def clear_completed_retries(self) -> int:
        """Clear completed retries and return count"""
        with self._lock:
            count = len(self.completed_retries)
            self.completed_retries.clear()
            return count