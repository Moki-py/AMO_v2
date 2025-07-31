"""
Tests for batch processing optimization system
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, patch, MagicMock

from amocrm_exporter.utils.batch_processor import (
    ConfigurableBatchProcessor, BatchProcessingConfig, BatchStatus,
    DynamicBatchSizeAdjuster, MemoryMonitor, create_large_dataset_config
)
from amocrm_exporter.utils.intelligent_retry_scheduler import (
    IntelligentRetryScheduler, RetrySchedulerConfig, RetryPriority, RetryStrategy
)
from amocrm_exporter.utils.concurrent_export_manager import (
    ConcurrentExportManager, ResourceLimits, ExportPriority, ExportStatus
)


class TestConfigurableBatchProcessor:
    """Test the configurable batch processor"""

    def test_batch_processor_initialization(self):
        """Test batch processor initializes correctly"""
        config = BatchProcessingConfig(
            initial_batch_size=500,
            min_batch_size=100,
            max_batch_size=2000
        )
        processor = ConfigurableBatchProcessor(config)

        assert processor.config.initial_batch_size == 500
        assert processor.batch_adjuster.current_batch_size == 500
        assert processor.memory_monitor.config.max_memory_usage_mb == config.max_memory_usage_mb

    @pytest.mark.asyncio
    async def test_batch_processing_with_simple_data(self):
        """Test batch processing with simple data"""
        config = BatchProcessingConfig(
            initial_batch_size=10,
            min_batch_size=5,
            max_batch_size=20,
            enable_dynamic_adjustment=False  # Disable for predictable testing
        )
        processor = ConfigurableBatchProcessor(config)

        # Create test data
        test_data = list(range(25))  # 25 items

        # Mock processor function
        def mock_processor(batch_data):
            return {"processed": len(batch_data), "success": True}

        # Process data
        results = await processor.process_data_in_batches(
            data=test_data,
            processor_func=mock_processor,
            batch_id_prefix="test"
        )

        # Verify results
        assert len(results) == 3  # 25 items / 10 batch_size = 3 batches
        assert all(result.status == BatchStatus.COMPLETED for result in results)
        assert sum(result.processed_items for result in results) == 25

    def test_dynamic_batch_size_adjustment(self):
        """Test dynamic batch size adjustment"""
        config = BatchProcessingConfig(
            initial_batch_size=1000,
            min_batch_size=100,
            max_batch_size=5000,
            slow_batch_threshold=30.0,
            fast_batch_threshold=10.0
        )
        adjuster = DynamicBatchSizeAdjuster(config)

        # Test slow batch adjustment
        from amocrm_exporter.utils.batch_processor import BatchMetrics
        slow_metrics = BatchMetrics(
            batch_size=1000,
            processing_time=45.0,  # Slow
            api_response_time=20.0,
            memory_usage_mb=500.0,
            success_rate=1.0,
            retry_count=0,
            error_count=0
        )

        new_size = adjuster.adjust_batch_size(slow_metrics)
        assert new_size < 1000  # Should reduce batch size

        # Test fast batch adjustment
        fast_metrics = BatchMetrics(
            batch_size=new_size,
            processing_time=5.0,  # Fast
            api_response_time=2.0,
            memory_usage_mb=200.0,
            success_rate=1.0,
            retry_count=0,
            error_count=0
        )

        newer_size = adjuster.adjust_batch_size(fast_metrics)
        assert newer_size > new_size  # Should increase batch size

    @patch('psutil.Process')
    def test_memory_monitor(self, mock_process):
        """Test memory monitoring"""
        # Mock memory info
        mock_memory_info = Mock()
        mock_memory_info.rss = 512 * 1024 * 1024  # 512 MB in bytes
        mock_process.return_value.memory_info.return_value = mock_memory_info

        config = BatchProcessingConfig(
            max_memory_usage_mb=1024.0,
            enable_memory_monitoring=True
        )
        monitor = MemoryMonitor(config)

        # Test memory usage
        memory_mb = monitor.get_current_memory_usage()
        assert memory_mb == 512.0

        # Test threshold check
        assert monitor.check_memory_threshold() is True  # 512 < 1024

        # Test with high memory usage
        mock_memory_info.rss = 1536 * 1024 * 1024  # 1536 MB
        assert monitor.check_memory_threshold() is False  # 1536 > 1024


class TestIntelligentRetryScheduler:
    """Test the intelligent retry scheduler"""

    def test_retry_scheduler_initialization(self):
        """Test retry scheduler initializes correctly"""
        config = RetrySchedulerConfig(
            max_concurrent_retries=5,
            default_max_retries=3
        )
        scheduler = IntelligentRetryScheduler(config)

        assert scheduler.config.max_concurrent_retries == 5
        assert scheduler.config.default_max_retries == 3
        assert len(scheduler.retry_queue) == 0

    def test_failure_pattern_analysis(self):
        """Test failure pattern analysis"""
        config = RetrySchedulerConfig()
        scheduler = IntelligentRetryScheduler(config)

        # Record some failures
        scheduler.pattern_analyzer.record_failure("batch1", "rate limit exceeded", 1)
        scheduler.pattern_analyzer.record_failure("batch2", "network timeout", 1)
        scheduler.pattern_analyzer.record_failure("batch3", "rate limit exceeded", 1)

        # Analyze patterns
        analysis = scheduler.pattern_analyzer.analyze_patterns()

        assert analysis["dominant_error"] == "rate_limit"
        # The strategy might be circuit_breaker due to low success rate (no successes recorded)
        assert analysis["strategy"] in [RetryStrategy.SCHEDULED, RetryStrategy.CIRCUIT_BREAKER]
        assert "error_distribution" in analysis

    @pytest.mark.asyncio
    async def test_retry_scheduling(self):
        """Test retry scheduling functionality"""
        config = RetrySchedulerConfig(
            max_concurrent_retries=2,
            min_retry_delay=0.1  # Short delay for testing
        )
        scheduler = IntelligentRetryScheduler(config)

        # Create mock batch result
        from amocrm_exporter.utils.batch_processor import BatchResult
        batch_result = BatchResult(
            batch_id="test_batch",
            status=BatchStatus.FAILED,
            processed_items=0,
            total_items=100,
            processing_time=10.0,
            memory_usage_mb=200.0,
            errors=["Test error"]
        )

        # Mock processor function
        def mock_processor(data):
            return {"success": True}

        # Schedule retry
        retry_id = scheduler.schedule_retry(
            batch_result=batch_result,
            retry_data=[1, 2, 3],
            processor_func=mock_processor,
            priority=RetryPriority.HIGH
        )

        assert retry_id == "test_batch"
        assert len(scheduler.retry_queue) == 1

        # Test stats
        stats = scheduler.get_retry_stats()
        assert stats["queue_status"]["queued_retries"] == 1


class TestConcurrentExportManager:
    """Test the concurrent export manager"""

    def test_concurrent_manager_initialization(self):
        """Test concurrent manager initializes correctly"""
        limits = ResourceLimits(
            max_concurrent_exports=3,
            max_memory_usage_mb=2048.0,
            max_api_calls_per_minute=1000
        )
        manager = ConcurrentExportManager(limits)

        assert manager.limits.max_concurrent_exports == 3
        assert manager.limits.max_memory_usage_mb == 2048.0
        assert len(manager.export_queue) == 0
        assert len(manager.running_exports) == 0

    def test_export_queueing(self):
        """Test export queueing functionality"""
        manager = ConcurrentExportManager()

        # Queue an export
        success = manager.queue_export(
            export_id="test_export_1",
            entity_types=["leads", "contacts"],
            export_config={"test": "config"},
            priority=ExportPriority.HIGH
        )

        assert success is True
        assert len(manager.export_queue) == 1
        assert manager.export_queue[0].export_id == "test_export_1"
        assert manager.export_queue[0].priority == ExportPriority.HIGH

    def test_export_priority_ordering(self):
        """Test export priority ordering"""
        manager = ConcurrentExportManager()

        # Queue exports with different priorities
        manager.queue_export("low_priority", ["leads"], {}, ExportPriority.LOW)
        manager.queue_export("high_priority", ["contacts"], {}, ExportPriority.HIGH)
        manager.queue_export("normal_priority", ["companies"], {}, ExportPriority.NORMAL)
        manager.queue_export("critical_priority", ["events"], {}, ExportPriority.CRITICAL)

        # Check ordering (should be sorted by priority)
        priorities = [job.priority for job in manager.export_queue]
        expected_order = [ExportPriority.CRITICAL, ExportPriority.HIGH, ExportPriority.NORMAL, ExportPriority.LOW]
        assert priorities == expected_order

    @patch('psutil.Process')
    def test_resource_monitoring(self, mock_process):
        """Test resource monitoring"""
        # Mock system resources
        mock_memory_info = Mock()
        mock_memory_info.rss = 1024 * 1024 * 1024  # 1GB
        mock_process.return_value.memory_info.return_value = mock_memory_info
        mock_process.return_value.cpu_percent.return_value = 50.0

        limits = ResourceLimits(max_memory_usage_mb=2048.0, max_cpu_usage_percent=80.0)
        manager = ConcurrentExportManager(limits)

        # Test resource usage
        usage = manager.resource_monitor.get_current_resource_usage()
        assert usage["process_memory_mb"] == 1024.0
        assert usage["process_cpu_percent"] == 50.0

        # Test resource limits
        within_limits, violations = manager.resource_monitor.check_resource_limits()
        assert within_limits is True
        assert len(violations) == 0

    def test_api_quota_management(self):
        """Test API quota management"""
        limits = ResourceLimits(
            max_api_calls_per_minute=100,
            max_api_calls_per_hour=1000
        )
        manager = ConcurrentExportManager(limits)

        # Test initial quota
        can_make_calls, reason = manager.quota_manager.can_make_api_calls(10)
        assert can_make_calls is True

        # Record API calls
        manager.quota_manager.record_api_call(50)

        # Test quota after calls
        can_make_calls, reason = manager.quota_manager.can_make_api_calls(60)
        assert can_make_calls is False  # 50 + 60 > 100
        assert "quota exceeded" in reason.lower()

    def test_optimal_batch_size_calculation(self):
        """Test optimal batch size calculation"""
        manager = ConcurrentExportManager()

        # Test with normal conditions
        optimal_size = manager.quota_manager.get_optimal_batch_size(1000)
        assert optimal_size > 0

        # Test after rate limiting
        manager.quota_manager.record_rate_limit("api_rate_limit")
        reduced_size = manager.quota_manager.get_optimal_batch_size(1000)
        assert reduced_size < optimal_size  # Should be reduced after rate limit

    def test_manager_statistics(self):
        """Test manager statistics collection"""
        manager = ConcurrentExportManager()

        # Queue some exports
        manager.queue_export("export1", ["leads"], {}, ExportPriority.NORMAL)
        manager.queue_export("export2", ["contacts"], {}, ExportPriority.HIGH)

        # Get stats
        stats = manager.get_manager_stats()

        assert "queue" in stats
        assert "overall" in stats
        assert "resources" in stats
        assert "quota" in stats
        assert stats["queue"]["queued_exports"] == 2

    def test_export_status_tracking(self):
        """Test export status tracking"""
        manager = ConcurrentExportManager()

        # Queue an export
        manager.queue_export("test_export", ["leads"], {"test": "config"})

        # Get status
        status = manager.get_export_status("test_export")
        assert status is not None
        assert status["export_id"] == "test_export"
        assert status["status"] == ExportStatus.QUEUED.value
        assert status["entity_types"] == ["leads"]


class TestIntegration:
    """Integration tests for the complete batch processing system"""

    @pytest.mark.asyncio
    async def test_end_to_end_batch_processing(self):
        """Test end-to-end batch processing workflow"""
        # Create configuration for testing
        config = BatchProcessingConfig(
            initial_batch_size=5,
            min_batch_size=2,
            max_batch_size=10,
            enable_dynamic_adjustment=False
        )

        processor = ConfigurableBatchProcessor(config)

        # Create test data
        test_data = [{"id": i, "value": f"item_{i}"} for i in range(12)]

        # Mock processor function that occasionally fails
        call_count = 0
        def mock_processor_with_failures(batch_data):
            nonlocal call_count
            call_count += 1

            # Fail the second batch to test retry logic
            if call_count == 2:
                raise Exception("Simulated batch failure")

            return {
                "processed_count": len(batch_data),
                "batch_number": call_count,
                "success": True
            }

        # Process with progress tracking
        progress_updates = []
        def progress_callback(batch_id, processed, total):
            progress_updates.append((batch_id, processed, total))

        # Process data
        results = await processor.process_data_in_batches(
            data=test_data,
            processor_func=mock_processor_with_failures,
            progress_callback=progress_callback,
            batch_id_prefix="integration_test"
        )

        # Verify results
        assert len(results) >= 2  # At least 2 batches (12 items / 5 batch_size)
        successful_results = [r for r in results if r.status == BatchStatus.COMPLETED]
        failed_results = [r for r in results if r.status == BatchStatus.FAILED]

        # Should have some successful and some failed batches
        assert len(successful_results) > 0
        assert len(failed_results) > 0  # The second batch should fail

        # Verify progress updates were called
        assert len(progress_updates) > 0

        # Verify processing stats
        stats = processor.get_processing_stats()
        assert stats["total_batches"] == len(results)
        assert stats["successful_batches"] == len(successful_results)
        assert stats["failed_batches"] == len(failed_results)

    def test_configuration_presets(self):
        """Test different configuration presets"""
        # Test large dataset config
        large_config = create_large_dataset_config()
        assert large_config.initial_batch_size >= 2000
        assert large_config.max_memory_usage_mb >= 2048.0
        assert large_config.enable_streaming is True

        # Test memory constrained config
        from amocrm_exporter.utils.batch_processor import create_memory_constrained_config
        memory_config = create_memory_constrained_config()
        assert memory_config.initial_batch_size <= 500
        assert memory_config.max_memory_usage_mb <= 512.0
        assert memory_config.memory_check_interval <= 5

        # Test API optimized config
        from amocrm_exporter.utils.batch_processor import create_api_optimized_config
        api_config = create_api_optimized_config()
        assert api_config.retry_config.max_retries >= 5
        assert api_config.retry_config.max_delay >= 120.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])