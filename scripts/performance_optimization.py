#!/usr/bin/env python3
"""
Performance optimization and validation script for Google Sheets export system

This script:
1. Validates system performance under various conditions
2. Optimizes configuration parameters
3. Provides performance recommendations
4. Tests memory usage and processing efficiency
"""

import asyncio
import time
import psutil
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import json

# Import system components
from src.amocrm_exporter.storage.storage import Storage
from src.amocrm_exporter.exporters.sheets_exporter import SheetsExporter
from src.amocrm_exporter.web.export_presets import ExportPresetManager, ExportPreset
from src.amocrm_exporter.utils.progress_tracker import ExportProgressTracker
from src.amocrm_exporter.core.logger import log_event


@dataclass
class PerformanceMetrics:
    """Performance metrics for analysis"""
    operation: str
    duration: float
    memory_before: float
    memory_after: float
    memory_peak: float
    cpu_usage: float
    records_processed: int
    throughput: float  # records per second
    errors: List[str]
    warnings: List[str]


class PerformanceOptimizer:
    """
    Performance optimization and validation system
    """

    def __init__(self):
        """Initialize the performance optimizer"""
        self.storage = Storage()
        self.metrics: List[PerformanceMetrics] = []
        self.process = psutil.Process()

        print("🚀 Performance Optimization System Initialized")
        print("=" * 60)

    def measure_memory(self) -> float:
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024

    def measure_cpu(self) -> float:
        """Get current CPU usage percentage"""
        return self.process.cpu_percent()

    async def benchmark_preset_operations(self) -> PerformanceMetrics:
        """Benchmark preset management operations"""
        print("\n📊 Benchmarking Preset Operations...")

        start_time = time.time()
        memory_before = self.measure_memory()
        cpu_before = self.measure_cpu()
        errors = []
        warnings = []

        try:
            preset_manager = ExportPresetManager(self.storage)

            # Create test presets
            test_presets = []
            for i in range(100):  # Create 100 presets for testing
                preset = ExportPreset(
                    name=f"Performance Test Preset {i}",
                    entity_type="deals",
                    description=f"Test preset {i} for performance benchmarking",
                    selected_fields=["id", "name", "price", "status_id", "created_at"],
                    field_order=["name", "price", "id", "status_id", "created_at"],
                    custom_field_mappings={f"custom_field_{i}": f"Custom Field {i}"}
                )
                test_presets.append(preset)

            # Benchmark preset creation
            creation_start = time.time()
            created_ids = []
            for preset in test_presets:
                try:
                    preset_id = preset_manager.save_preset(preset)
                    created_ids.append(preset_id)
                except Exception as e:
                    errors.append(f"Preset creation error: {e}")

            creation_time = time.time() - creation_start
            print(f"  ✓ Created {len(created_ids)} presets in {creation_time:.2f}s")
            print(f"    Rate: {len(created_ids) / creation_time:.1f} presets/second")

            # Benchmark preset loading
            loading_start = time.time()
            loaded_count = 0
            for preset_id in created_ids[:50]:  # Load first 50
                try:
                    loaded_preset = preset_manager.load_preset(preset_id)
                    if loaded_preset:
                        loaded_count += 1
                except Exception as e:
                    errors.append(f"Preset loading error: {e}")

            loading_time = time.time() - loading_start
            print(f"  ✓ Loaded {loaded_count} presets in {loading_time:.2f}s")
            print(f"    Rate: {loaded_count / loading_time:.1f} presets/second")

            # Benchmark preset listing
            listing_start = time.time()
            all_presets = preset_manager.list_presets()
            listing_time = time.time() - listing_start
            print(f"  ✓ Listed {len(all_presets)} presets in {listing_time:.3f}s")

            # Cleanup test presets
            cleanup_start = time.time()
            deleted_count = 0
            for preset_id in created_ids:
                try:
                    if preset_manager.delete_preset(preset_id):
                        deleted_count += 1
                except Exception as e:
                    errors.append(f"Preset deletion error: {e}")

            cleanup_time = time.time() - cleanup_start
            print(f"  ✓ Deleted {deleted_count} presets in {cleanup_time:.2f}s")

        except Exception as e:
            errors.append(f"Benchmark error: {e}")

        end_time = time.time()
        memory_after = self.measure_memory()
        memory_peak = max(memory_before, memory_after)
        cpu_after = self.measure_cpu()

        metrics = PerformanceMetrics(
            operation="preset_operations",
            duration=end_time - start_time,
            memory_before=memory_before,
            memory_after=memory_after,
            memory_peak=memory_peak,
            cpu_usage=max(cpu_before, cpu_after),
            records_processed=len(test_presets),
            throughput=len(test_presets) / (end_time - start_time),
            errors=errors,
            warnings=warnings
        )

        self.metrics.append(metrics)
        return metrics

    async def benchmark_progress_tracking(self) -> PerformanceMetrics:
        """Benchmark progress tracking system performance"""
        print("\n📈 Benchmarking Progress Tracking...")

        start_time = time.time()
        memory_before = self.measure_memory()
        cpu_before = self.measure_cpu()
        errors = []
        warnings = []

        try:
            progress_tracker = ExportProgressTracker(self.storage)

            # Simulate multiple concurrent exports
            export_ids = []
            for i in range(10):  # 10 concurrent exports
                export_id = progress_tracker.start_export(
                    entity_types=["deals", "contacts", "companies"]
                )
                export_ids.append(export_id)

            print(f"  ✓ Started {len(export_ids)} concurrent export tracking sessions")

            # Simulate progress updates
            update_count = 0
            for export_id in export_ids:
                for entity_type in ["deals", "contacts", "companies"]:
                    for progress in range(0, 101, 10):  # 0%, 10%, 20%, ..., 100%
                        try:
                            progress_tracker.update_entity_progress(
                                export_id=export_id,
                                entity_type=entity_type,
                                processed=progress,
                                total=100
                            )
                            update_count += 1
                        except Exception as e:
                            errors.append(f"Progress update error: {e}")

            print(f"  ✓ Processed {update_count} progress updates")

            # Complete all exports
            completed_count = 0
            for export_id in export_ids:
                try:
                    progress_tracker.complete_export(export_id, success=True)
                    completed_count += 1
                except Exception as e:
                    errors.append(f"Export completion error: {e}")

            print(f"  ✓ Completed {completed_count} export sessions")

        except Exception as e:
            errors.append(f"Progress tracking benchmark error: {e}")

        end_time = time.time()
        memory_after = self.measure_memory()
        memory_peak = max(memory_before, memory_after)
        cpu_after = self.measure_cpu()

        metrics = PerformanceMetrics(
            operation="progress_tracking",
            duration=end_time - start_time,
            memory_before=memory_before,
            memory_after=memory_after,
            memory_peak=memory_peak,
            cpu_usage=max(cpu_before, cpu_after),
            records_processed=update_count,
            throughput=update_count / (end_time - start_time),
            errors=errors,
            warnings=warnings
        )

        self.metrics.append(metrics)
        return metrics

    async def benchmark_data_processing(self) -> PerformanceMetrics:
        """Benchmark data processing performance"""
        print("\n⚙️ Benchmarking Data Processing...")

        start_time = time.time()
        memory_before = self.measure_memory()
        cpu_before = self.measure_cpu()
        errors = []
        warnings = []

        try:
            # Create mock exporter for testing
            exporter = SheetsExporter(self.storage)

            # Generate test data
            test_data_sizes = [100, 1000, 5000, 10000]
            processing_rates = []

            for data_size in test_data_sizes:
                print(f"  Testing with {data_size} records...")

                # Generate test data
                test_data = []
                for i in range(data_size):
                    test_data.append({
                        "id": i,
                        "name": f"Test Record {i}",
                        "price": i * 100,
                        "created_at": int(time.time()) - (i * 3600),
                        "custom_fields_values": [
                            {
                                "field_id": "123",
                                "field_name": "Test Field",
                                "field_type": "text",
                                "values": [{"value": f"Value {i}"}]
                            }
                        ]
                    })

                # Benchmark data processing
                process_start = time.time()
                try:
                    processed_data = exporter._process_data_with_enhanced_custom_fields(test_data)
                    process_time = time.time() - process_start

                    if processed_data:
                        rate = len(processed_data) / process_time
                        processing_rates.append(rate)
                        print(f"    ✓ Processed {len(processed_data)} records in {process_time:.2f}s")
                        print(f"      Rate: {rate:.1f} records/second")
                    else:
                        warnings.append(f"No processed data returned for {data_size} records")

                except Exception as e:
                    errors.append(f"Data processing error for {data_size} records: {e}")

            # Calculate average processing rate
            avg_rate = sum(processing_rates) / len(processing_rates) if processing_rates else 0
            print(f"  📊 Average processing rate: {avg_rate:.1f} records/second")

        except Exception as e:
            errors.append(f"Data processing benchmark error: {e}")

        end_time = time.time()
        memory_after = self.measure_memory()
        memory_peak = max(memory_before, memory_after)
        cpu_after = self.measure_cpu()

        total_records = sum(test_data_sizes)

        metrics = PerformanceMetrics(
            operation="data_processing",
            duration=end_time - start_time,
            memory_before=memory_before,
            memory_after=memory_after,
            memory_peak=memory_peak,
            cpu_usage=max(cpu_before, cpu_after),
            records_processed=total_records,
            throughput=total_records / (end_time - start_time),
            errors=errors,
            warnings=warnings
        )

        self.metrics.append(metrics)
        return metrics

    async def benchmark_memory_efficiency(self) -> PerformanceMetrics:
        """Benchmark memory efficiency and garbage collection"""
        print("\n🧠 Benchmarking Memory Efficiency...")

        start_time = time.time()
        memory_before = self.measure_memory()
        cpu_before = self.measure_cpu()
        errors = []
        warnings = []

        try:
            # Test memory usage with large datasets
            large_datasets = []
            memory_measurements = []

            for size in [1000, 5000, 10000, 20000]:
                print(f"  Creating dataset with {size} records...")

                # Create large dataset
                dataset = []
                for i in range(size):
                    record = {
                        "id": i,
                        "name": f"Large Record {i}",
                        "data": "x" * 1000,  # 1KB of data per record
                        "custom_fields": {f"field_{j}": f"value_{j}" for j in range(10)}
                    }
                    dataset.append(record)

                large_datasets.append(dataset)
                current_memory = self.measure_memory()
                memory_measurements.append(current_memory)

                print(f"    Memory usage: {current_memory:.1f} MB")

            # Test garbage collection efficiency
            print("  Testing garbage collection...")
            gc_start_memory = self.measure_memory()

            # Clear datasets
            large_datasets.clear()

            # Force garbage collection
            collected = gc.collect()
            gc_end_memory = self.measure_memory()

            memory_freed = gc_start_memory - gc_end_memory
            print(f"    Garbage collected: {collected} objects")
            print(f"    Memory freed: {memory_freed:.1f} MB")

            if memory_freed < 10:  # Less than 10MB freed
                warnings.append("Low memory recovery after garbage collection")

        except Exception as e:
            errors.append(f"Memory efficiency benchmark error: {e}")

        end_time = time.time()
        memory_after = self.measure_memory()
        memory_peak = max(memory_measurements) if memory_measurements else memory_after
        cpu_after = self.measure_cpu()

        metrics = PerformanceMetrics(
            operation="memory_efficiency",
            duration=end_time - start_time,
            memory_before=memory_before,
            memory_after=memory_after,
            memory_peak=memory_peak,
            cpu_usage=max(cpu_before, cpu_after),
            records_processed=sum([1000, 5000, 10000, 20000]),
            throughput=0,  # Not applicable for memory test
            errors=errors,
            warnings=warnings
        )

        self.metrics.append(metrics)
        return metrics

    def analyze_performance_metrics(self) -> Dict[str, Any]:
        """Analyze collected performance metrics and provide recommendations"""
        print("\n📋 Performance Analysis Report")
        print("=" * 60)

        if not self.metrics:
            print("No performance metrics collected.")
            return {}

        analysis = {
            "summary": {},
            "recommendations": [],
            "warnings": [],
            "optimizations": []
        }

        # Analyze each operation
        for metric in self.metrics:
            print(f"\n{metric.operation.upper()} PERFORMANCE:")
            print(f"  Duration: {metric.duration:.2f} seconds")
            print(f"  Memory Usage: {metric.memory_before:.1f} → {metric.memory_after:.1f} MB")
            print(f"  Memory Peak: {metric.memory_peak:.1f} MB")
            print(f"  CPU Usage: {metric.cpu_usage:.1f}%")
            print(f"  Records Processed: {metric.records_processed}")
            if metric.throughput > 0:
                print(f"  Throughput: {metric.throughput:.1f} records/second")

            if metric.errors:
                print(f"  ❌ Errors ({len(metric.errors)}):")
                for error in metric.errors[:3]:  # Show first 3 errors
                    print(f"    • {error}")
                if len(metric.errors) > 3:
                    print(f"    ... and {len(metric.errors) - 3} more errors")

            if metric.warnings:
                print(f"  ⚠️ Warnings ({len(metric.warnings)}):")
                for warning in metric.warnings[:3]:  # Show first 3 warnings
                    print(f"    • {warning}")
                if len(metric.warnings) > 3:
                    print(f"    ... and {len(metric.warnings) - 3} more warnings")

            # Store in analysis
            analysis["summary"][metric.operation] = {
                "duration": metric.duration,
                "throughput": metric.throughput,
                "memory_peak": metric.memory_peak,
                "error_count": len(metric.errors),
                "warning_count": len(metric.warnings)
            }

        # Generate recommendations
        print(f"\n🎯 PERFORMANCE RECOMMENDATIONS:")

        # Memory recommendations
        max_memory = max(m.memory_peak for m in self.metrics)
        if max_memory > 500:  # More than 500MB
            recommendation = f"High memory usage detected ({max_memory:.1f} MB). Consider implementing streaming processing for large datasets."
            analysis["recommendations"].append(recommendation)
            print(f"  • {recommendation}")

        # Throughput recommendations
        processing_metrics = [m for m in self.metrics if m.operation == "data_processing"]
        if processing_metrics:
            avg_throughput = sum(m.throughput for m in processing_metrics) / len(processing_metrics)
            if avg_throughput < 100:  # Less than 100 records/second
                recommendation = f"Low processing throughput ({avg_throughput:.1f} records/sec). Consider optimizing data processing algorithms."
                analysis["recommendations"].append(recommendation)
                print(f"  • {recommendation}")

        # Error rate recommendations
        total_errors = sum(len(m.errors) for m in self.metrics)
        if total_errors > 0:
            recommendation = f"Errors detected ({total_errors} total). Review error handling and system stability."
            analysis["recommendations"].append(recommendation)
            print(f"  • {recommendation}")

        # Configuration optimizations
        print(f"\n⚙️ CONFIGURATION OPTIMIZATIONS:")

        # Batch size optimization
        if processing_metrics:
            avg_throughput = sum(m.throughput for m in processing_metrics) / len(processing_metrics)
            if avg_throughput > 500:
                optimization = "High throughput detected. Consider increasing batch sizes for better efficiency."
                analysis["optimizations"].append(optimization)
                print(f"  • {optimization}")
            elif avg_throughput < 50:
                optimization = "Low throughput detected. Consider reducing batch sizes or optimizing processing logic."
                analysis["optimizations"].append(optimization)
                print(f"  • {optimization}")

        # Memory optimization
        if max_memory > 1000:  # More than 1GB
            optimization = "Very high memory usage. Implement memory-efficient streaming and consider increasing available system memory."
            analysis["optimizations"].append(optimization)
            print(f"  • {optimization}")

        return analysis

    async def run_comprehensive_benchmark(self) -> Dict[str, Any]:
        """Run comprehensive performance benchmark"""
        print("🚀 Starting Comprehensive Performance Benchmark")
        print("=" * 60)

        start_time = time.time()

        try:
            # Run all benchmarks
            await self.benchmark_preset_operations()
            await self.benchmark_progress_tracking()
            await self.benchmark_data_processing()
            await self.benchmark_memory_efficiency()

            # Analyze results
            analysis = self.analyze_performance_metrics()

            total_time = time.time() - start_time
            print(f"\n✅ Benchmark completed in {total_time:.2f} seconds")

            # Save results
            results = {
                "timestamp": datetime.now().isoformat(),
                "total_duration": total_time,
                "metrics": [
                    {
                        "operation": m.operation,
                        "duration": m.duration,
                        "throughput": m.throughput,
                        "memory_peak": m.memory_peak,
                        "error_count": len(m.errors),
                        "warning_count": len(m.warnings)
                    }
                    for m in self.metrics
                ],
                "analysis": analysis
            }

            # Save to file
            with open("performance_benchmark_results.json", "w") as f:
                json.dump(results, f, indent=2)

            print(f"📊 Results saved to performance_benchmark_results.json")

            return results

        except Exception as e:
            print(f"❌ Benchmark failed: {e}")
            raise


async def main():
    """Main function to run performance optimization"""
    print("AmoCRM Export System Performance Optimization")
    print("=" * 60)

    optimizer = PerformanceOptimizer()

    try:
        # Run comprehensive benchmark
        results = await optimizer.run_comprehensive_benchmark()

        # Print final summary
        print(f"\n🎉 Performance Optimization Complete!")
        print(f"Total operations benchmarked: {len(optimizer.metrics)}")
        print(f"Recommendations generated: {len(results['analysis']['recommendations'])}")
        print(f"Optimizations suggested: {len(results['analysis']['optimizations'])}")

        return results

    except Exception as e:
        print(f"❌ Performance optimization failed: {e}")
        return None


if __name__ == "__main__":
    # Run the performance optimization
    results = asyncio.run(main())

    if results:
        print("\n✅ Performance optimization completed successfully!")
        print("Check performance_benchmark_results.json for detailed results.")
    else:
        print("\n❌ Performance optimization failed!")
        exit(1)