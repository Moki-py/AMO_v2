"""
Concurrent export management system with resource management and throttling
"""

import asyncio
import time
from typing import Dict, List, Optional, Callable, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
from datetime import datetime, timedelta

from .batch_processor import BatchProcessingConfig, ConfigurableBatchProcessor
from .intelligent_retry_scheduler import IntelligentRetryScheduler, RetrySchedulerConfig
from .exceptions import ConcurrentExportError, create_error_context
from ..core.logger import log_event


class ExportPriority(str, Enum):
    """Priority levels for export operations"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class ExportStatus(str, Enum):
    """Status of export operations"""
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ResourceLimits:
    """Resource limits for concurrent operations"""
    max_concurrent_exports: int = 3
    max_memory_usage_mb: float = 2048.0  # 2GB
    max_cpu_usage_percent: float = 80.0
    max_api_calls_per_minute: int = 1000
    max_api_calls_per_hour: int = 10000

    # Per-export limits
    max_export_duration_minutes: int = 60
    max_export_memory_mb: float = 512.0

    # Queue limits
    max_queue_size: int = 50
    max_priority_queue_size: int = 10


@dataclass
class QuotaManager:
    """Manages API quota and rate limiting"""
    calls_per_minute: int = 0
    calls_per_hour: int = 0
    minute_window_start: float = field(default_factory=time.time)
    hour_window_start: float = field(default_factory=time.time)

    # Quota tracking
    remaining_minute_quota: int = 1000
    remaining_hour_quota: int = 10000

    # Adaptive batch sizing
    current_batch_size_multiplier: float = 1.0
    last_rate_limit_time: Optional[float] = None
    consecutive_rate_limits: int = 0


@dataclass
class ExportJob:
    """Represents an export job in the queue"""
    export_id: str
    entity_types: List[str]
    priority: ExportPriority
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    status: ExportStatus = ExportStatus.QUEUED

    # Export configuration
    export_config: Dict[str, Any] = field(default_factory=dict)
    batch_config: Optional[BatchProcessingConfig] = None

    # Progress tracking
    progress_percentage: float = 0.0
    current_entity: Optional[str] = None
    processed_entities: Set[str] = field(default_factory=set)

    # Resource usage
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    api_calls_made: int = 0

    # Results and errors
    results: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Callbacks
    progress_callback: Optional[Callable] = None
    completion_callback: Optional[Callable] = None

    def __lt__(self, other):
        """For priority queue ordering"""
        priority_order = {
            ExportPriority.CRITICAL: 0,
            ExportPriority.HIGH: 1,
            ExportPriority.NORMAL: 2,
            ExportPriority.LOW: 3
        }

        if priority_order[self.priority] != priority_order[other.priority]:
            return priority_order[self.priority] < priority_order[other.priority]

        return self.created_at < other.created_at


class ResourceMonitor:
    """Monitors system resources for concurrent export management"""

    def __init__(self, limits: ResourceLimits):
        self.limits = limits
        self.process = psutil.Process()
        self.logger = logging.getLogger(__name__)

        # Resource history for trend analysis
        self.memory_history: List[Tuple[float, float]] = []  # (timestamp, memory_mb)
        self.cpu_history: List[Tuple[float, float]] = []     # (timestamp, cpu_percent)

        # Resource alerts
        self.memory_alert_threshold = limits.max_memory_usage_mb * 0.9
        self.cpu_alert_threshold = limits.max_cpu_usage_percent * 0.9

    def get_current_resource_usage(self) -> Dict[str, float]:
        """Get current system resource usage"""
        try:
            # Memory usage
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024

            # CPU usage (average over short interval)
            cpu_percent = self.process.cpu_percent(interval=0.1)

            # System-wide memory
            system_memory = psutil.virtual_memory()
            system_memory_percent = system_memory.percent

            # Update history
            current_time = time.time()
            self.memory_history.append((current_time, memory_mb))
            self.cpu_history.append((current_time, cpu_percent))

            # Keep only recent history (last 5 minutes)
            cutoff_time = current_time - 300
            self.memory_history = [(t, m) for t, m in self.memory_history if t > cutoff_time]
            self.cpu_history = [(t, c) for t, c in self.cpu_history if t > cutoff_time]

            return {
                "process_memory_mb": memory_mb,
                "process_cpu_percent": cpu_percent,
                "system_memory_percent": system_memory_percent,
                "system_memory_available_mb": system_memory.available / 1024 / 1024
            }

        except Exception as e:
            self.logger.warning(f"Failed to get resource usage: {e}")
            return {
                "process_memory_mb": 0.0,
                "process_cpu_percent": 0.0,
                "system_memory_percent": 0.0,
                "system_memory_available_mb": 0.0
            }

    def check_resource_limits(self) -> Tuple[bool, List[str]]:
        """Check if current resource usage is within limits"""
        usage = self.get_current_resource_usage()
        violations = []

        # Check memory limit
        if usage["process_memory_mb"] > self.limits.max_memory_usage_mb:
            violations.append(f"Memory usage ({usage['process_memory_mb']:.1f}MB) exceeds limit ({self.limits.max_memory_usage_mb}MB)")

        # Check CPU limit
        if usage["process_cpu_percent"] > self.limits.max_cpu_usage_percent:
            violations.append(f"CPU usage ({usage['process_cpu_percent']:.1f}%) exceeds limit ({self.limits.max_cpu_usage_percent}%)")

        # Check system memory
        if usage["system_memory_percent"] > 90:  # System-wide memory critical
            violations.append(f"System memory usage ({usage['system_memory_percent']:.1f}%) is critical")

        return len(violations) == 0, violations

    def should_throttle_operations(self) -> Tuple[bool, str]:
        """Determine if operations should be throttled"""
        usage = self.get_current_resource_usage()

        # Check for memory pressure
        if usage["process_memory_mb"] > self.memory_alert_threshold:
            return True, f"Memory pressure: {usage['process_memory_mb']:.1f}MB"

        # Check for CPU pressure
        if usage["process_cpu_percent"] > self.cpu_alert_threshold:
            return True, f"CPU pressure: {usage['process_cpu_percent']:.1f}%"

        # Check system memory
        if usage["system_memory_percent"] > 85:
            return True, f"System memory pressure: {usage['system_memory_percent']:.1f}%"

        return False, ""

    def get_resource_stats(self) -> Dict[str, Any]:
        """Get comprehensive resource statistics"""
        current_usage = self.get_current_resource_usage()

        # Calculate averages from history
        if self.memory_history:
            avg_memory = sum(m for _, m in self.memory_history) / len(self.memory_history)
            peak_memory = max(m for _, m in self.memory_history)
        else:
            avg_memory = peak_memory = 0.0

        if self.cpu_history:
            avg_cpu = sum(c for _, c in self.cpu_history) / len(self.cpu_history)
            peak_cpu = max(c for _, c in self.cpu_history)
        else:
            avg_cpu = peak_cpu = 0.0

        within_limits, violations = self.check_resource_limits()
        should_throttle, throttle_reason = self.should_throttle_operations()

        return {
            "current": current_usage,
            "limits": {
                "max_memory_mb": self.limits.max_memory_usage_mb,
                "max_cpu_percent": self.limits.max_cpu_usage_percent
            },
            "averages": {
                "avg_memory_mb": avg_memory,
                "avg_cpu_percent": avg_cpu,
                "peak_memory_mb": peak_memory,
                "peak_cpu_percent": peak_cpu
            },
            "status": {
                "within_limits": within_limits,
                "violations": violations,
                "should_throttle": should_throttle,
                "throttle_reason": throttle_reason
            },
            "history_samples": {
                "memory_samples": len(self.memory_history),
                "cpu_samples": len(self.cpu_history)
            }
        }


class APIQuotaManager:
    """Manages API quota and implements intelligent throttling"""

    def __init__(self, limits: ResourceLimits):
        self.limits = limits
        self.quota = QuotaManager()
        self.logger = logging.getLogger(__name__)
        self._lock = threading.Lock()

        # Rate limiting history
        self.rate_limit_events: List[Tuple[float, str]] = []  # (timestamp, reason)

        # Adaptive batch sizing
        self.batch_size_history: List[Tuple[float, int, bool]] = []  # (timestamp, batch_size, success)

    def record_api_call(self, calls_made: int = 1) -> None:
        """Record API calls made"""
        with self._lock:
            current_time = time.time()

            # Reset minute window if needed
            if current_time - self.quota.minute_window_start >= 60:
                self.quota.calls_per_minute = 0
                self.quota.minute_window_start = current_time
                self.quota.remaining_minute_quota = self.limits.max_api_calls_per_minute

            # Reset hour window if needed
            if current_time - self.quota.hour_window_start >= 3600:
                self.quota.calls_per_hour = 0
                self.quota.hour_window_start = current_time
                self.quota.remaining_hour_quota = self.limits.max_api_calls_per_hour

            # Update counters
            self.quota.calls_per_minute += calls_made
            self.quota.calls_per_hour += calls_made
            self.quota.remaining_minute_quota = max(0, self.limits.max_api_calls_per_minute - self.quota.calls_per_minute)
            self.quota.remaining_hour_quota = max(0, self.limits.max_api_calls_per_hour - self.quota.calls_per_hour)

    def can_make_api_calls(self, calls_needed: int = 1) -> Tuple[bool, str]:
        """Check if API calls can be made within quota"""
        with self._lock:
            # Check minute quota
            if self.quota.calls_per_minute + calls_needed > self.limits.max_api_calls_per_minute:
                return False, f"Minute quota exceeded: {self.quota.calls_per_minute}/{self.limits.max_api_calls_per_minute}"

            # Check hour quota
            if self.quota.calls_per_hour + calls_needed > self.limits.max_api_calls_per_hour:
                return False, f"Hour quota exceeded: {self.quota.calls_per_hour}/{self.limits.max_api_calls_per_hour}"

            return True, ""

    def record_rate_limit(self, reason: str = "api_rate_limit") -> None:
        """Record a rate limit event"""
        with self._lock:
            current_time = time.time()
            self.quota.last_rate_limit_time = current_time
            self.quota.consecutive_rate_limits += 1

            # Record event
            self.rate_limit_events.append((current_time, reason))

            # Keep only recent events (last hour)
            cutoff_time = current_time - 3600
            self.rate_limit_events = [(t, r) for t, r in self.rate_limit_events if t > cutoff_time]

            # Adjust batch size multiplier
            self.quota.current_batch_size_multiplier = max(0.1, self.quota.current_batch_size_multiplier * 0.8)

            log_event("quota_manager", "warning",
                     f"Rate limit recorded: {reason}. Consecutive: {self.quota.consecutive_rate_limits}, "
                     f"Batch multiplier: {self.quota.current_batch_size_multiplier:.2f}")

    def record_successful_batch(self, batch_size: int) -> None:
        """Record a successful batch operation"""
        with self._lock:
            current_time = time.time()
            self.batch_size_history.append((current_time, batch_size, True))

            # Reset consecutive rate limits on success
            if self.quota.consecutive_rate_limits > 0:
                self.quota.consecutive_rate_limits = max(0, self.quota.consecutive_rate_limits - 1)

                # Gradually increase batch size multiplier
                if self.quota.consecutive_rate_limits == 0:
                    self.quota.current_batch_size_multiplier = min(1.0, self.quota.current_batch_size_multiplier * 1.1)

            # Keep only recent history
            cutoff_time = current_time - 3600
            self.batch_size_history = [(t, s, success) for t, s, success in self.batch_size_history if t > cutoff_time]

    def get_optimal_batch_size(self, base_batch_size: int) -> int:
        """Get optimal batch size based on quota and rate limiting history"""
        with self._lock:
            # Apply current multiplier
            optimal_size = int(base_batch_size * self.quota.current_batch_size_multiplier)

            # Consider remaining quota
            minute_quota_factor = self.quota.remaining_minute_quota / self.limits.max_api_calls_per_minute
            hour_quota_factor = self.quota.remaining_hour_quota / self.limits.max_api_calls_per_hour

            quota_factor = min(minute_quota_factor, hour_quota_factor)
            if quota_factor < 0.2:  # Less than 20% quota remaining
                optimal_size = int(optimal_size * quota_factor * 5)  # Reduce batch size significantly

            # Ensure minimum batch size
            return max(10, optimal_size)

    def get_quota_stats(self) -> Dict[str, Any]:
        """Get comprehensive quota statistics"""
        with self._lock:
            current_time = time.time()

            # Calculate rate limit frequency
            recent_rate_limits = [t for t, _ in self.rate_limit_events if current_time - t <= 3600]
            rate_limit_frequency = len(recent_rate_limits) / 60 if recent_rate_limits else 0  # per minute

            # Calculate success rate
            recent_batches = [success for t, _, success in self.batch_size_history if current_time - t <= 3600]
            success_rate = sum(recent_batches) / len(recent_batches) if recent_batches else 1.0

            return {
                "current_quota": {
                    "calls_per_minute": self.quota.calls_per_minute,
                    "calls_per_hour": self.quota.calls_per_hour,
                    "remaining_minute": self.quota.remaining_minute_quota,
                    "remaining_hour": self.quota.remaining_hour_quota
                },
                "limits": {
                    "max_per_minute": self.limits.max_api_calls_per_minute,
                    "max_per_hour": self.limits.max_api_calls_per_hour
                },
                "rate_limiting": {
                    "consecutive_rate_limits": self.quota.consecutive_rate_limits,
                    "last_rate_limit": self.quota.last_rate_limit_time,
                    "rate_limit_frequency_per_minute": rate_limit_frequency,
                    "total_rate_limit_events": len(self.rate_limit_events)
                },
                "batch_optimization": {
                    "current_multiplier": self.quota.current_batch_size_multiplier,
                    "success_rate": success_rate,
                    "total_batches": len(self.batch_size_history)
                }
            }


class ConcurrentExportManager:
    """Main concurrent export management system"""

    def __init__(self, limits: Optional[ResourceLimits] = None):
        self.limits = limits or ResourceLimits()
        self.resource_monitor = ResourceMonitor(self.limits)
        self.quota_manager = APIQuotaManager(self.limits)

        # Export queue and management
        self.export_queue: List[ExportJob] = []
        self.running_exports: Dict[str, ExportJob] = {}
        self.completed_exports: Dict[str, ExportJob] = {}

        # Thread pool for concurrent execution
        self.executor = ThreadPoolExecutor(max_workers=self.limits.max_concurrent_exports)

        # Async management
        self._running = False
        self._manager_task: Optional[asyncio.Task] = None

        # Thread safety
        self._lock = threading.Lock()

        # Statistics
        self.stats = {
            "total_exports": 0,
            "successful_exports": 0,
            "failed_exports": 0,
            "cancelled_exports": 0,
            "total_processing_time": 0.0,
            "avg_queue_wait_time": 0.0
        }

        self.logger = logging.getLogger(__name__)

    async def start_manager(self) -> None:
        """Start the concurrent export manager"""
        if self._running:
            return

        self._running = True
        self._manager_task = asyncio.create_task(self._management_loop())
        log_event("concurrent_export", "info", "Concurrent export manager started")

    async def stop_manager(self) -> None:
        """Stop the concurrent export manager"""
        self._running = False

        if self._manager_task:
            self._manager_task.cancel()
            try:
                await self._manager_task
            except asyncio.CancelledError:
                pass

        # Shutdown executor
        self.executor.shutdown(wait=True)

        log_event("concurrent_export", "info", "Concurrent export manager stopped")

    def queue_export(
        self,
        export_id: str,
        entity_types: List[str],
        export_config: Dict[str, Any],
        priority: ExportPriority = ExportPriority.NORMAL,
        batch_config: Optional[BatchProcessingConfig] = None,
        progress_callback: Optional[Callable] = None,
        completion_callback: Optional[Callable] = None
    ) -> bool:
        """Queue an export for processing"""

        with self._lock:
            # Check queue size limits
            if len(self.export_queue) >= self.limits.max_queue_size:
                log_event("concurrent_export", "warning", f"Export queue full, rejecting export {export_id}")
                return False

            # Check priority queue limits
            priority_count = sum(1 for job in self.export_queue if job.priority in [ExportPriority.HIGH, ExportPriority.CRITICAL])
            if priority in [ExportPriority.HIGH, ExportPriority.CRITICAL] and priority_count >= self.limits.max_priority_queue_size:
                log_event("concurrent_export", "warning", f"Priority queue full, rejecting high-priority export {export_id}")
                return False

            # Create export job
            export_job = ExportJob(
                export_id=export_id,
                entity_types=entity_types,
                priority=priority,
                export_config=export_config,
                batch_config=batch_config,
                progress_callback=progress_callback,
                completion_callback=completion_callback
            )

            # Add to queue (maintain priority order)
            self.export_queue.append(export_job)
            self.export_queue.sort()  # Sort by priority and creation time

            self.stats["total_exports"] += 1

            log_event("concurrent_export", "info",
                     f"Queued export {export_id} with priority {priority.value} "
                     f"(queue size: {len(self.export_queue)})")

            return True

    async def _management_loop(self) -> None:
        """Main management loop"""
        while self._running:
            try:
                await self._process_export_queue()
                await self._monitor_running_exports()
                await self._cleanup_completed_exports()
                await asyncio.sleep(1.0)  # Check every second
            except Exception as e:
                log_event("concurrent_export", "error", f"Error in management loop: {e}")
                await asyncio.sleep(5.0)

    async def _process_export_queue(self) -> None:
        """Process the export queue"""
        with self._lock:
            # Check if we can start more exports
            if len(self.running_exports) >= self.limits.max_concurrent_exports:
                return

            # Check resource limits
            within_limits, violations = self.resource_monitor.check_resource_limits()
            if not within_limits:
                log_event("concurrent_export", "warning", f"Resource limits exceeded: {violations}")
                return

            # Check for throttling
            should_throttle, throttle_reason = self.resource_monitor.should_throttle_operations()
            if should_throttle:
                log_event("concurrent_export", "info", f"Throttling operations: {throttle_reason}")
                return

            # Find next export to start
            if not self.export_queue:
                return

            next_export = self.export_queue.pop(0)

            # Move to running exports
            next_export.status = ExportStatus.RUNNING
            next_export.started_at = time.time()
            self.running_exports[next_export.export_id] = next_export

        # Start the export (outside of lock)
        await self._start_export(next_export)

    async def _start_export(self, export_job: ExportJob) -> None:
        """Start an individual export"""
        log_event("concurrent_export", "info", f"Starting export {export_job.export_id}")

        try:
            # Create batch processor with optimal configuration
            batch_config = export_job.batch_config or self._create_optimal_batch_config()
            batch_processor = ConfigurableBatchProcessor(batch_config)

            # Create retry scheduler
            retry_scheduler = IntelligentRetryScheduler()
            await retry_scheduler.start_scheduler()

            # Submit to thread pool
            future = self.executor.submit(
                self._execute_export,
                export_job,
                batch_processor,
                retry_scheduler
            )

            # Monitor the future
            asyncio.create_task(self._monitor_export_future(export_job, future, retry_scheduler))

        except Exception as e:
            await self._handle_export_error(export_job, str(e))

    def _execute_export(
        self,
        export_job: ExportJob,
        batch_processor: ConfigurableBatchProcessor,
        retry_scheduler: IntelligentRetryScheduler
    ) -> Dict[str, Any]:
        """Execute export in thread pool"""
        try:
            # This would be replaced with actual export logic
            # For now, simulate export processing

            results = {}
            total_entities = len(export_job.entity_types)

            for i, entity_type in enumerate(export_job.entity_types):
                # Check if export was cancelled
                if export_job.status == ExportStatus.CANCELLED:
                    break

                # Update progress
                export_job.current_entity = entity_type
                export_job.progress_percentage = (i / total_entities) * 100

                # Record API calls
                api_calls = 10  # Simulate API calls
                self.quota_manager.record_api_call(api_calls)
                export_job.api_calls_made += api_calls

                # Update resource usage
                resource_usage = self.resource_monitor.get_current_resource_usage()
                export_job.memory_usage_mb = resource_usage["process_memory_mb"]
                export_job.cpu_usage_percent = resource_usage["process_cpu_percent"]

                # Simulate processing time
                time.sleep(1)

                # Mark entity as processed
                export_job.processed_entities.add(entity_type)
                results[entity_type] = f"processed_{entity_type}"

                # Call progress callback if provided
                if export_job.progress_callback:
                    export_job.progress_callback(export_job.export_id, i + 1, total_entities)

            export_job.results = results
            return results

        except Exception as e:
            export_job.errors.append(str(e))
            raise

    async def _monitor_export_future(
        self,
        export_job: ExportJob,
        future,
        retry_scheduler: IntelligentRetryScheduler
    ) -> None:
        """Monitor export future and handle completion"""
        try:
            # Wait for completion
            result = await asyncio.get_event_loop().run_in_executor(None, future.result)

            # Handle successful completion
            await self._handle_export_completion(export_job, result)

        except Exception as e:
            # Handle export failure
            await self._handle_export_error(export_job, str(e))

        finally:
            # Cleanup retry scheduler
            await retry_scheduler.stop_scheduler()

    async def _handle_export_completion(self, export_job: ExportJob, result: Dict[str, Any]) -> None:
        """Handle successful export completion"""
        export_job.status = ExportStatus.COMPLETED
        export_job.completed_at = time.time()
        export_job.progress_percentage = 100.0
        export_job.results = result

        # Move from running to completed
        with self._lock:
            self.running_exports.pop(export_job.export_id, None)
            self.completed_exports[export_job.export_id] = export_job
            self.stats["successful_exports"] += 1

            # Update processing time
            if export_job.started_at:
                processing_time = export_job.completed_at - export_job.started_at
                self.stats["total_processing_time"] += processing_time

        # Call completion callback
        if export_job.completion_callback:
            try:
                export_job.completion_callback(export_job.export_id, True, result)
            except Exception as e:
                log_event("concurrent_export", "warning", f"Error in completion callback: {e}")

        log_event("concurrent_export", "info",
                 f"Export {export_job.export_id} completed successfully "
                 f"({len(export_job.processed_entities)} entities processed)")

    async def _handle_export_error(self, export_job: ExportJob, error: str) -> None:
        """Handle export failure"""
        export_job.status = ExportStatus.FAILED
        export_job.completed_at = time.time()
        export_job.errors.append(error)

        # Move from running to completed
        with self._lock:
            self.running_exports.pop(export_job.export_id, None)
            self.completed_exports[export_job.export_id] = export_job
            self.stats["failed_exports"] += 1

        # Call completion callback
        if export_job.completion_callback:
            try:
                export_job.completion_callback(export_job.export_id, False, {"error": error})
            except Exception as e:
                log_event("concurrent_export", "warning", f"Error in completion callback: {e}")

        log_event("concurrent_export", "error", f"Export {export_job.export_id} failed: {error}")

    async def _monitor_running_exports(self) -> None:
        """Monitor running exports for timeouts and resource usage"""
        current_time = time.time()
        exports_to_cancel = []

        with self._lock:
            for export_id, export_job in self.running_exports.items():
                # Check for timeout
                if (export_job.started_at and
                    current_time - export_job.started_at > self.limits.max_export_duration_minutes * 60):
                    exports_to_cancel.append((export_id, "timeout"))

                # Check for memory limit
                if export_job.memory_usage_mb > self.limits.max_export_memory_mb:
                    exports_to_cancel.append((export_id, f"memory_limit_{export_job.memory_usage_mb:.1f}MB"))

        # Cancel problematic exports
        for export_id, reason in exports_to_cancel:
            await self._cancel_export(export_id, reason)

    async def _cleanup_completed_exports(self) -> None:
        """Cleanup old completed exports"""
        current_time = time.time()
        cleanup_age = 3600  # 1 hour

        with self._lock:
            exports_to_remove = [
                export_id for export_id, export_job in self.completed_exports.items()
                if (export_job.completed_at and
                    current_time - export_job.completed_at > cleanup_age)
            ]

            for export_id in exports_to_remove:
                del self.completed_exports[export_id]

            if exports_to_remove:
                log_event("concurrent_export", "info", f"Cleaned up {len(exports_to_remove)} old exports")

    def _create_optimal_batch_config(self) -> BatchProcessingConfig:
        """Create optimal batch configuration based on current conditions"""
        # Get current resource usage
        resource_stats = self.resource_monitor.get_resource_stats()
        quota_stats = self.quota_manager.get_quota_stats()

        # Determine optimal configuration
        if resource_stats["status"]["should_throttle"]:
            # Use memory-constrained config
            from .batch_processor import create_memory_constrained_config
            config = create_memory_constrained_config()
        elif quota_stats["rate_limiting"]["consecutive_rate_limits"] > 2:
            # Use API-optimized config
            from .batch_processor import create_api_optimized_config
            config = create_api_optimized_config()
        else:
            # Use large dataset config
            from .batch_processor import create_large_dataset_config
            config = create_large_dataset_config()

        # Apply quota-based batch size adjustment
        base_batch_size = config.initial_batch_size
        optimal_batch_size = self.quota_manager.get_optimal_batch_size(base_batch_size)
        config.initial_batch_size = optimal_batch_size
        config.max_batch_size = min(config.max_batch_size, optimal_batch_size * 2)

        return config

    async def cancel_export(self, export_id: str) -> bool:
        """Cancel an export"""
        return await self._cancel_export(export_id, "user_requested")

    async def _cancel_export(self, export_id: str, reason: str) -> bool:
        """Internal cancel export method"""
        with self._lock:
            # Check if export is queued
            for i, export_job in enumerate(self.export_queue):
                if export_job.export_id == export_id:
                    export_job.status = ExportStatus.CANCELLED
                    export_job.errors.append(f"Cancelled: {reason}")
                    self.export_queue.pop(i)
                    self.completed_exports[export_id] = export_job
                    self.stats["cancelled_exports"] += 1
                    log_event("concurrent_export", "info", f"Cancelled queued export {export_id}: {reason}")
                    return True

            # Check if export is running
            if export_id in self.running_exports:
                export_job = self.running_exports[export_id]
                export_job.status = ExportStatus.CANCELLED
                export_job.errors.append(f"Cancelled: {reason}")
                # Note: The actual cancellation will be handled by the monitoring loop
                log_event("concurrent_export", "info", f"Marked running export {export_id} for cancellation: {reason}")
                return True

        return False

    def get_export_status(self, export_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific export"""
        with self._lock:
            # Check queued exports
            for export_job in self.export_queue:
                if export_job.export_id == export_id:
                    return self._export_job_to_dict(export_job)

            # Check running exports
            if export_id in self.running_exports:
                return self._export_job_to_dict(self.running_exports[export_id])

            # Check completed exports
            if export_id in self.completed_exports:
                return self._export_job_to_dict(self.completed_exports[export_id])

        return None

    def _export_job_to_dict(self, export_job: ExportJob) -> Dict[str, Any]:
        """Convert export job to dictionary"""
        return {
            "export_id": export_job.export_id,
            "entity_types": export_job.entity_types,
            "priority": export_job.priority.value,
            "status": export_job.status.value,
            "created_at": export_job.created_at,
            "started_at": export_job.started_at,
            "completed_at": export_job.completed_at,
            "progress_percentage": export_job.progress_percentage,
            "current_entity": export_job.current_entity,
            "processed_entities": list(export_job.processed_entities),
            "memory_usage_mb": export_job.memory_usage_mb,
            "cpu_usage_percent": export_job.cpu_usage_percent,
            "api_calls_made": export_job.api_calls_made,
            "errors": export_job.errors,
            "warnings": export_job.warnings,
            "results": export_job.results
        }

    def get_manager_stats(self) -> Dict[str, Any]:
        """Get comprehensive manager statistics"""
        with self._lock:
            queue_stats = {
                "queued_exports": len(self.export_queue),
                "running_exports": len(self.running_exports),
                "completed_exports": len(self.completed_exports),
                "queue_capacity": self.limits.max_queue_size,
                "priority_queue_usage": sum(1 for job in self.export_queue if job.priority in [ExportPriority.HIGH, ExportPriority.CRITICAL])
            }

            # Calculate average queue wait time
            current_time = time.time()
            wait_times = [current_time - job.created_at for job in self.export_queue]
            avg_wait_time = sum(wait_times) / len(wait_times) if wait_times else 0.0

            overall_stats = self.stats.copy()
            overall_stats["avg_queue_wait_time"] = avg_wait_time

        resource_stats = self.resource_monitor.get_resource_stats()
        quota_stats = self.quota_manager.get_quota_stats()

        return {
            "queue": queue_stats,
            "overall": overall_stats,
            "resources": resource_stats,
            "quota": quota_stats,
            "limits": {
                "max_concurrent_exports": self.limits.max_concurrent_exports,
                "max_queue_size": self.limits.max_queue_size,
                "max_memory_mb": self.limits.max_memory_usage_mb,
                "max_cpu_percent": self.limits.max_cpu_usage_percent
            }
        }

    def get_all_exports(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all exports by status"""
        with self._lock:
            return {
                "queued": [self._export_job_to_dict(job) for job in self.export_queue],
                "running": [self._export_job_to_dict(job) for job in self.running_exports.values()],
                "completed": [self._export_job_to_dict(job) for job in self.completed_exports.values()]
            }

    def update_resource_limits(self, new_limits: ResourceLimits) -> None:
        """Update resource limits"""
        self.limits = new_limits
        self.resource_monitor.limits = new_limits
        self.quota_manager.limits = new_limits
        log_event("concurrent_export", "info", "Resource limits updated")

    def clear_completed_exports(self) -> int:
        """Clear completed exports and return count"""
        with self._lock:
            count = len(self.completed_exports)
            self.completed_exports.clear()
            return count