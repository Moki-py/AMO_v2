"""
Configurable batch processing system for large datasets with dynamic adjustment
"""

import asyncio
import time
import psutil
from typing import Any, Dict, List, Optional, Callable, Iterator, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from .retry_logic import RetryConfiguration, ExponentialBackoffRetry
from .exceptions import BatchProcessingError, create_error_context
from ..core.logger import log_event


class BatchStatus(str, Enum):
    """Batch processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class BatchMetrics:
    """Metrics for batch processing performance"""
    batch_size: int
    processing_time: float
    api_response_time: float
    memory_usage_mb: float
    success_rate: float
    retry_count: int
    error_count: int


@dataclass
class BatchProcessingConfig:
    """Configuration for batch processing system"""
    # Basic batch settings
    initial_batch_size: int = 1000
    min_batch_size: int = 100
    max_batch_size: int = 10000

    # Dynamic adjustment settings
    enable_dynamic_adjustment: bool = True
    target_processing_time: float = 30.0  # Target time per batch in seconds
    adjustment_factor: float = 0.2  # How aggressively to adjust batch size

    # Memory monitoring
    enable_memory_monitoring: bool = True
    max_memory_usage_mb: float = 1024.0  # 1GB default
    memory_check_interval: int = 10  # Check every N batches

    # Performance thresholds
    slow_batch_threshold: float = 60.0  # Consider batch slow if > 60s
    fast_batch_threshold: float = 10.0  # Consider batch fast if < 10s

    # Retry configuration
    retry_config: RetryConfiguration = field(default_factory=lambda: RetryConfiguration(
        max_retries=3,
        base_delay=2.0,
        max_delay=60.0,
        exponential_base=2.0,
        jitter=True
    ))

    # Streaming settings
    enable_streaming: bool = True
    stream_chunk_size: int = 100

    # Concurrency settings
    max_concurrent_batches: int = 3
    enable_parallel_processing: bool = False


@dataclass
class BatchResult:
    """Result of batch processing"""
    batch_id: str
    status: BatchStatus
    processed_items: int
    total_items: int
    processing_time: float
    memory_usage_mb: float
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metrics: Optional[BatchMetrics] = None


class DynamicBatchSizeAdjuster:
    """Handles dynamic batch size adjustment based on performance metrics"""

    def __init__(self, config: BatchProcessingConfig):
        self.config = config
        self.current_batch_size = config.initial_batch_size
        self.performance_history: List[BatchMetrics] = []
        self.adjustment_history: List[Tuple[int, float, str]] = []  # (size, time, reason)
        self.logger = logging.getLogger(__name__)

    def adjust_batch_size(self, metrics: BatchMetrics) -> int:
        """Adjust batch size based on performance metrics"""
        if not self.config.enable_dynamic_adjustment:
            return self.current_batch_size

        # Store metrics for analysis
        self.performance_history.append(metrics)

        # Keep only recent history (last 10 batches)
        if len(self.performance_history) > 10:
            self.performance_history = self.performance_history[-10:]

        old_size = self.current_batch_size
        adjustment_reason = "no_change"

        # Adjust based on processing time
        if metrics.processing_time > self.config.slow_batch_threshold:
            # Batch is too slow, reduce size
            reduction_factor = 1 - self.config.adjustment_factor
            self.current_batch_size = max(
                int(self.current_batch_size * reduction_factor),
                self.config.min_batch_size
            )
            adjustment_reason = f"slow_batch_{metrics.processing_time:.1f}s"

        elif metrics.processing_time < self.config.fast_batch_threshold:
            # Batch is fast, can increase size
            increase_factor = 1 + self.config.adjustment_factor
            self.current_batch_size = min(
                int(self.current_batch_size * increase_factor),
                self.config.max_batch_size
            )
            adjustment_reason = f"fast_batch_{metrics.processing_time:.1f}s"

        # Adjust based on memory usage
        if metrics.memory_usage_mb > self.config.max_memory_usage_mb * 0.8:
            # High memory usage, reduce batch size
            memory_reduction = 1 - (self.config.adjustment_factor * 1.5)
            self.current_batch_size = max(
                int(self.current_batch_size * memory_reduction),
                self.config.min_batch_size
            )
            adjustment_reason = f"high_memory_{metrics.memory_usage_mb:.1f}MB"

        # Adjust based on API response time
        if metrics.api_response_time > 30.0:  # API is slow
            api_reduction = 1 - (self.config.adjustment_factor * 0.5)
            self.current_batch_size = max(
                int(self.current_batch_size * api_reduction),
                self.config.min_batch_size
            )
            adjustment_reason = f"slow_api_{metrics.api_response_time:.1f}s"

        # Log adjustment if size changed
        if self.current_batch_size != old_size:
            self.adjustment_history.append((self.current_batch_size, metrics.processing_time, adjustment_reason))
            self.logger.info(
                f"Batch size adjusted: {old_size} -> {self.current_batch_size} "
                f"(reason: {adjustment_reason})"
            )

        return self.current_batch_size

    def get_optimal_batch_size(self) -> int:
        """Get current optimal batch size"""
        return self.current_batch_size

    def get_adjustment_stats(self) -> Dict[str, Any]:
        """Get statistics about batch size adjustments"""
        if not self.performance_history:
            return {"no_data": True}

        recent_metrics = self.performance_history[-5:] if len(self.performance_history) >= 5 else self.performance_history

        return {
            "current_batch_size": self.current_batch_size,
            "initial_batch_size": self.config.initial_batch_size,
            "avg_processing_time": sum(m.processing_time for m in recent_metrics) / len(recent_metrics),
            "avg_memory_usage": sum(m.memory_usage_mb for m in recent_metrics) / len(recent_metrics),
            "avg_api_response_time": sum(m.api_response_time for m in recent_metrics) / len(recent_metrics),
            "total_adjustments": len(self.adjustment_history),
            "recent_adjustments": self.adjustment_history[-5:] if self.adjustment_history else []
        }


class MemoryMonitor:
    """Monitors memory usage during batch processing"""

    def __init__(self, config: BatchProcessingConfig):
        self.config = config
        self.process = psutil.Process()
        self.peak_memory_mb = 0.0
        self.memory_samples: List[float] = []
        self.logger = logging.getLogger(__name__)

    def get_current_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB

            # Update peak memory
            if memory_mb > self.peak_memory_mb:
                self.peak_memory_mb = memory_mb

            # Store sample
            self.memory_samples.append(memory_mb)
            if len(self.memory_samples) > 100:  # Keep last 100 samples
                self.memory_samples = self.memory_samples[-100:]

            return memory_mb
        except Exception as e:
            self.logger.warning(f"Failed to get memory usage: {e}")
            return 0.0

    def check_memory_threshold(self) -> bool:
        """Check if memory usage is within acceptable limits"""
        if not self.config.enable_memory_monitoring:
            return True

        current_memory = self.get_current_memory_usage()
        return current_memory <= self.config.max_memory_usage_mb

    def get_memory_stats(self) -> Dict[str, float]:
        """Get memory usage statistics"""
        if not self.memory_samples:
            return {"no_data": True}

        return {
            "current_mb": self.get_current_memory_usage(),
            "peak_mb": self.peak_memory_mb,
            "avg_mb": sum(self.memory_samples) / len(self.memory_samples),
            "max_threshold_mb": self.config.max_memory_usage_mb,
            "samples_count": len(self.memory_samples)
        }


class StreamingProcessor:
    """Handles streaming processing for memory efficiency"""

    def __init__(self, config: BatchProcessingConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def create_streaming_batches(self, data: List[Any]) -> Iterator[List[Any]]:
        """Create streaming batches from data"""
        if not self.config.enable_streaming:
            # Return all data as single batch
            yield data
            return

        chunk_size = self.config.stream_chunk_size
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def process_streaming_batch(
        self,
        batch_data: List[Any],
        processor_func: Callable[[List[Any]], Any],
        batch_id: str
    ) -> Any:
        """Process a streaming batch with memory monitoring"""
        try:
            self.logger.debug(f"Processing streaming batch {batch_id} with {len(batch_data)} items")

            # Process the batch
            result = processor_func(batch_data)

            # Clear batch data from memory immediately
            batch_data.clear()

            return result

        except Exception as e:
            self.logger.error(f"Error processing streaming batch {batch_id}: {e}")
            raise


class ConfigurableBatchProcessor:
    """Main configurable batch processing system"""

    def __init__(self, config: Optional[BatchProcessingConfig] = None):
        self.config = config or BatchProcessingConfig()
        self.batch_adjuster = DynamicBatchSizeAdjuster(self.config)
        self.memory_monitor = MemoryMonitor(self.config)
        self.streaming_processor = StreamingProcessor(self.config)
        self.retry_handler = ExponentialBackoffRetry(self.config.retry_config)

        self.logger = logging.getLogger(__name__)
        self.processing_stats = {
            "total_batches": 0,
            "successful_batches": 0,
            "failed_batches": 0,
            "total_items_processed": 0,
            "total_processing_time": 0.0,
            "total_retries": 0
        }

        # Thread safety
        self._lock = threading.Lock()
        self._active_batches: Dict[str, BatchResult] = {}

    async def process_data_in_batches(
        self,
        data: List[Any],
        processor_func: Callable[[List[Any]], Any],
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        batch_id_prefix: str = "batch"
    ) -> List[BatchResult]:
        """
        Process data in configurable batches with dynamic adjustment

        Args:
            data: List of items to process
            processor_func: Function to process each batch
            progress_callback: Optional callback for progress updates
            batch_id_prefix: Prefix for batch IDs

        Returns:
            List of batch results
        """
        if not data:
            return []

        log_event("batch_processor", "info", f"Starting batch processing of {len(data)} items")

        results: List[BatchResult] = []
        current_batch_size = self.batch_adjuster.get_optimal_batch_size()

        # Process data in batches
        for batch_index in range(0, len(data), current_batch_size):
            batch_data = data[batch_index:batch_index + current_batch_size]
            batch_id = f"{batch_id_prefix}_{batch_index // current_batch_size + 1}"

            # Process single batch
            batch_result = await self._process_single_batch(
                batch_data, processor_func, batch_id, batch_index, len(data)
            )

            results.append(batch_result)

            # Update progress
            if progress_callback:
                processed_items = min(batch_index + current_batch_size, len(data))
                progress_callback(batch_id, processed_items, len(data))

            # Adjust batch size based on performance
            if batch_result.metrics:
                current_batch_size = self.batch_adjuster.adjust_batch_size(batch_result.metrics)

            # Check memory usage periodically
            if batch_index % (self.config.memory_check_interval * current_batch_size) == 0:
                if not self.memory_monitor.check_memory_threshold():
                    log_event("batch_processor", "warning", "Memory threshold exceeded, reducing batch size")
                    current_batch_size = max(current_batch_size // 2, self.config.min_batch_size)

        # Update overall stats
        self._update_processing_stats(results)

        log_event("batch_processor", "info",
                 f"Batch processing completed: {len(results)} batches, "
                 f"{sum(r.processed_items for r in results)} items processed")

        return results

    async def _process_single_batch(
        self,
        batch_data: List[Any],
        processor_func: Callable[[List[Any]], Any],
        batch_id: str,
        batch_start_index: int,
        total_items: int
    ) -> BatchResult:
        """Process a single batch with retry logic and metrics collection"""
        start_time = time.time()
        start_memory = self.memory_monitor.get_current_memory_usage()

        # Initialize batch result
        batch_result = BatchResult(
            batch_id=batch_id,
            status=BatchStatus.PENDING,
            processed_items=0,
            total_items=len(batch_data),
            processing_time=0.0,
            memory_usage_mb=start_memory
        )

        # Register active batch
        with self._lock:
            self._active_batches[batch_id] = batch_result

        try:
            batch_result.status = BatchStatus.PROCESSING
            log_event("batch_processor", "info", f"Processing batch {batch_id} with {len(batch_data)} items")

            # Execute with retry logic
            context = create_error_context(
                component="batch_processor",
                operation=f"process_batch_{batch_id}",
                details={"batch_size": len(batch_data), "batch_index": batch_start_index}
            )

            api_start_time = time.time()
            result = self.retry_handler.execute_with_retry(
                processor_func, batch_data, context=context
            )
            api_end_time = time.time()

            # Calculate metrics
            end_time = time.time()
            end_memory = self.memory_monitor.get_current_memory_usage()

            processing_time = end_time - start_time
            api_response_time = api_end_time - api_start_time
            memory_usage = max(end_memory, start_memory)

            # Create metrics
            metrics = BatchMetrics(
                batch_size=len(batch_data),
                processing_time=processing_time,
                api_response_time=api_response_time,
                memory_usage_mb=memory_usage,
                success_rate=1.0,
                retry_count=0,  # TODO: Get from retry handler
                error_count=0
            )

            # Update batch result
            batch_result.status = BatchStatus.COMPLETED
            batch_result.processed_items = len(batch_data)
            batch_result.processing_time = processing_time
            batch_result.memory_usage_mb = memory_usage
            batch_result.metrics = metrics

            log_event("batch_processor", "info",
                     f"Batch {batch_id} completed in {processing_time:.2f}s "
                     f"(API: {api_response_time:.2f}s, Memory: {memory_usage:.1f}MB)")

            return batch_result

        except Exception as e:
            # Handle batch failure
            end_time = time.time()
            processing_time = end_time - start_time

            error_msg = str(e)
            batch_result.status = BatchStatus.FAILED
            batch_result.processing_time = processing_time
            batch_result.errors.append(error_msg)

            log_event("batch_processor", "error", f"Batch {batch_id} failed after {processing_time:.2f}s: {error_msg}")

            # Create failure metrics
            metrics = BatchMetrics(
                batch_size=len(batch_data),
                processing_time=processing_time,
                api_response_time=0.0,
                memory_usage_mb=self.memory_monitor.get_current_memory_usage(),
                success_rate=0.0,
                retry_count=0,
                error_count=1
            )
            batch_result.metrics = metrics

            return batch_result

        finally:
            # Remove from active batches
            with self._lock:
                self._active_batches.pop(batch_id, None)

    def _update_processing_stats(self, results: List[BatchResult]) -> None:
        """Update overall processing statistics"""
        with self._lock:
            self.processing_stats["total_batches"] += len(results)

            for result in results:
                if result.status == BatchStatus.COMPLETED:
                    self.processing_stats["successful_batches"] += 1
                else:
                    self.processing_stats["failed_batches"] += 1

                self.processing_stats["total_items_processed"] += result.processed_items
                self.processing_stats["total_processing_time"] += result.processing_time

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics"""
        with self._lock:
            stats = self.processing_stats.copy()

        # Add batch adjuster stats
        stats["batch_adjustment"] = self.batch_adjuster.get_adjustment_stats()

        # Add memory stats
        stats["memory"] = self.memory_monitor.get_memory_stats()

        # Add retry stats
        stats["circuit_breaker"] = self.retry_handler.get_circuit_breaker_stats()

        # Add active batches info
        with self._lock:
            stats["active_batches"] = len(self._active_batches)
            stats["active_batch_ids"] = list(self._active_batches.keys())

        return stats

    def get_active_batches(self) -> Dict[str, BatchResult]:
        """Get currently active batches"""
        with self._lock:
            return self._active_batches.copy()

    def cancel_batch(self, batch_id: str) -> bool:
        """Cancel a specific batch (if possible)"""
        with self._lock:
            if batch_id in self._active_batches:
                batch_result = self._active_batches[batch_id]
                batch_result.status = BatchStatus.FAILED
                batch_result.errors.append("Batch cancelled by user")
                return True
        return False

    def reset_stats(self) -> None:
        """Reset processing statistics"""
        with self._lock:
            self.processing_stats = {
                "total_batches": 0,
                "successful_batches": 0,
                "failed_batches": 0,
                "total_items_processed": 0,
                "total_processing_time": 0.0,
                "total_retries": 0
            }

        # Reset component stats
        self.batch_adjuster.performance_history.clear()
        self.batch_adjuster.adjustment_history.clear()
        self.memory_monitor.memory_samples.clear()
        self.memory_monitor.peak_memory_mb = 0.0


# Convenience functions for creating common configurations
def create_large_dataset_config() -> BatchProcessingConfig:
    """Create configuration optimized for large datasets"""
    return BatchProcessingConfig(
        initial_batch_size=2000,
        min_batch_size=500,
        max_batch_size=20000,
        enable_dynamic_adjustment=True,
        target_processing_time=45.0,
        adjustment_factor=0.3,
        enable_memory_monitoring=True,
        max_memory_usage_mb=2048.0,  # 2GB
        enable_streaming=True,
        stream_chunk_size=200,
        max_concurrent_batches=5
    )


def create_memory_constrained_config() -> BatchProcessingConfig:
    """Create configuration for memory-constrained environments"""
    return BatchProcessingConfig(
        initial_batch_size=500,
        min_batch_size=100,
        max_batch_size=2000,
        enable_dynamic_adjustment=True,
        target_processing_time=20.0,
        adjustment_factor=0.4,
        enable_memory_monitoring=True,
        max_memory_usage_mb=512.0,  # 512MB
        memory_check_interval=5,
        enable_streaming=True,
        stream_chunk_size=50,
        max_concurrent_batches=2
    )


def create_api_optimized_config() -> BatchProcessingConfig:
    """Create configuration optimized for API rate limits"""
    return BatchProcessingConfig(
        initial_batch_size=1000,
        min_batch_size=200,
        max_batch_size=5000,
        enable_dynamic_adjustment=True,
        target_processing_time=30.0,
        adjustment_factor=0.2,
        slow_batch_threshold=45.0,
        fast_batch_threshold=15.0,
        retry_config=RetryConfiguration(
            max_retries=5,
            base_delay=2.0,
            max_delay=120.0,
            exponential_base=2.0,
            jitter=True
        )
    )