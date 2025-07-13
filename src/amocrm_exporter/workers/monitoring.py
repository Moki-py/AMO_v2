"""
Enhanced monitoring system for AmoCRM Data Exporter

This module provides:
- Health checks for all components
- Metrics collection and aggregation
- Performance monitoring
- Alert conditions
- Dashboard data preparation
"""

import time
import asyncio
import threading
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict, deque
import statistics
import json

from entity_types import EntityType, get_all_entity_types
from exceptions import (
    BaseAmoException,
    HealthCheckError,
    SystemError,
    ErrorSeverity,
    create_error_context
)
from ..core.logger import log_event


class HealthStatus(str, Enum):
    """Health check status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class MetricType(str, Enum):
    """Metric type"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class HealthCheck:
    """Health check definition"""
    name: str
    description: str
    check_func: Callable[[], bool]
    timeout: float = 5.0
    critical: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class HealthCheckResult:
    """Health check result"""
    name: str
    status: HealthStatus
    message: str
    timestamp: datetime
    response_time: float
    critical: bool = False
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "response_time": self.response_time,
            "critical": self.critical,
            "details": self.details
        }


@dataclass
class Metric:
    """Metric definition"""
    name: str
    type: MetricType
    description: str
    labels: Dict[str, str] = field(default_factory=dict)
    value: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "type": self.type.value,
            "description": self.description,
            "labels": self.labels,
            "value": self.value,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class AlertCondition:
    """Alert condition definition"""
    name: str
    metric_name: str
    operator: str  # >, <, >=, <=, ==, !=
    threshold: float
    duration: int  # seconds
    severity: ErrorSeverity
    description: str
    enabled: bool = True
    cooldown: int = 300  # seconds between alerts


@dataclass
class AlertEvent:
    """Alert event"""
    condition: AlertCondition
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "condition_name": self.condition.name,
            "metric_name": self.condition.metric_name,
            "severity": self.condition.severity.value,
            "triggered_at": self.triggered_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "message": self.message,
            "description": self.condition.description
        }


class MetricsCollector:
    """Metrics collector and aggregator"""

    def __init__(self, retention_period: int = 3600):
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.retention_period = retention_period
        self.lock = threading.Lock()

    def record_metric(self, metric: Metric):
        """Record a metric value"""
        with self.lock:
            metric_key = f"{metric.name}:{json.dumps(metric.labels, sort_keys=True)}"
            self.metrics[metric_key].append(metric)

    def get_metric_history(self, name: str, labels: Optional[Dict[str, str]] = None,
                          duration: Optional[int] = None) -> List[Metric]:
        """Get metric history"""
        with self.lock:
            labels = labels or {}
            metric_key = f"{name}:{json.dumps(labels, sort_keys=True)}"

            if metric_key not in self.metrics:
                return []

            metrics = list(self.metrics[metric_key])

            if duration:
                cutoff_time = datetime.now() - timedelta(seconds=duration)
                metrics = [m for m in metrics if m.timestamp >= cutoff_time]

            return metrics

    def get_current_value(self, name: str, labels: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Get current metric value"""
        history = self.get_metric_history(name, labels)
        return history[-1].value if history else None

    def get_metric_stats(self, name: str, labels: Optional[Dict[str, str]] = None,
                        duration: Optional[int] = None) -> Dict[str, Any]:
        """Get metric statistics"""
        history = self.get_metric_history(name, labels, duration)

        if not history:
            return {}

        values = [m.value for m in history]

        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": statistics.mean(values),
            "median": statistics.median(values),
            "std": statistics.stdev(values) if len(values) > 1 else 0,
            "first": values[0],
            "last": values[-1],
            "sum": sum(values)
        }

    def cleanup_old_metrics(self):
        """Clean up old metrics"""
        with self.lock:
            cutoff_time = datetime.now() - timedelta(seconds=self.retention_period)

            for metric_key in list(self.metrics.keys()):
                metrics = self.metrics[metric_key]
                # Remove old metrics
                while metrics and metrics[0].timestamp < cutoff_time:
                    metrics.popleft()

                # Remove empty queues
                if not metrics:
                    del self.metrics[metric_key]


class HealthChecker:
    """Health check manager"""

    def __init__(self):
        self.health_checks: Dict[str, HealthCheck] = {}
        self.results: Dict[str, HealthCheckResult] = {}
        self.last_run: Optional[datetime] = None
        self.lock = threading.Lock()

    def register_health_check(self, health_check: HealthCheck):
        """Register a health check"""
        with self.lock:
            self.health_checks[health_check.name] = health_check

    def run_health_check(self, name: str) -> HealthCheckResult:
        """Run a specific health check"""
        if name not in self.health_checks:
            raise HealthCheckError(f"Health check {name} not found")

        health_check = self.health_checks[name]
        start_time = time.time()

        try:
            # Run health check with timeout
            result = asyncio.wait_for(
                asyncio.to_thread(health_check.check_func),
                timeout=health_check.timeout
            )

            if asyncio.iscoroutine(result):
                result = asyncio.run(result)

            response_time = time.time() - start_time

            if result:
                status = HealthStatus.HEALTHY
                message = f"Health check {name} passed"
            else:
                status = HealthStatus.UNHEALTHY
                message = f"Health check {name} failed"

            return HealthCheckResult(
                name=name,
                status=status,
                message=message,
                timestamp=datetime.now(),
                response_time=response_time,
                critical=health_check.critical
            )

        except asyncio.TimeoutError:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check {name} timed out after {health_check.timeout}s",
                timestamp=datetime.now(),
                response_time=health_check.timeout,
                critical=health_check.critical
            )
        except Exception as e:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check {name} failed: {str(e)}",
                timestamp=datetime.now(),
                response_time=time.time() - start_time,
                critical=health_check.critical
            )

    def run_all_health_checks(self) -> Dict[str, HealthCheckResult]:
        """Run all health checks"""
        results = {}

        with self.lock:
            for name in self.health_checks:
                try:
                    result = self.run_health_check(name)
                    results[name] = result
                    self.results[name] = result
                except Exception as e:
                    log_event("health_check", "error", f"Error running health check {name}: {e}")
                    results[name] = HealthCheckResult(
                        name=name,
                        status=HealthStatus.UNKNOWN,
                        message=f"Error running health check: {str(e)}",
                        timestamp=datetime.now(),
                        response_time=0.0,
                        critical=self.health_checks[name].critical
                    )

        self.last_run = datetime.now()
        return results

    def get_overall_health(self) -> Dict[str, Any]:
        """Get overall system health"""
        if not self.results:
            return {
                "status": HealthStatus.UNKNOWN.value,
                "message": "No health checks have been run",
                "last_run": None
            }

        critical_failed = []
        non_critical_failed = []

        for name, result in self.results.items():
            if result.status == HealthStatus.UNHEALTHY:
                if result.critical:
                    critical_failed.append(name)
                else:
                    non_critical_failed.append(name)

        if critical_failed:
            status = HealthStatus.UNHEALTHY
            message = f"Critical health checks failed: {', '.join(critical_failed)}"
        elif non_critical_failed:
            status = HealthStatus.DEGRADED
            message = f"Non-critical health checks failed: {', '.join(non_critical_failed)}"
        else:
            status = HealthStatus.HEALTHY
            message = "All health checks passed"

        return {
            "status": status.value,
            "message": message,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "total_checks": len(self.results),
            "healthy_checks": len([r for r in self.results.values() if r.status == HealthStatus.HEALTHY]),
            "failed_checks": len([r for r in self.results.values() if r.status == HealthStatus.UNHEALTHY]),
            "critical_failed": critical_failed,
            "non_critical_failed": non_critical_failed
        }


class AlertManager:
    """Alert manager for monitoring conditions"""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.conditions: Dict[str, AlertCondition] = {}
        self.active_alerts: Dict[str, AlertEvent] = {}
        self.alert_history: List[AlertEvent] = []
        self.last_alert_time: Dict[str, datetime] = {}
        self.lock = threading.Lock()

    def add_condition(self, condition: AlertCondition):
        """Add alert condition"""
        with self.lock:
            self.conditions[condition.name] = condition

    def check_conditions(self) -> List[AlertEvent]:
        """Check all alert conditions"""
        new_alerts = []

        with self.lock:
            for name, condition in self.conditions.items():
                if not condition.enabled:
                    continue

                # Check cooldown
                if name in self.last_alert_time:
                    time_since_last = (datetime.now() - self.last_alert_time[name]).total_seconds()
                    if time_since_last < condition.cooldown:
                        continue

                # Get current metric value
                current_value = self.metrics_collector.get_current_value(condition.metric_name)

                if current_value is None:
                    continue

                # Check condition
                triggered = self._evaluate_condition(current_value, condition)

                if triggered and name not in self.active_alerts:
                    # New alert
                    alert = AlertEvent(
                        condition=condition,
                        triggered_at=datetime.now(),
                        message=f"Alert triggered: {condition.metric_name} {condition.operator} {condition.threshold} (current: {current_value})"
                    )

                    self.active_alerts[name] = alert
                    self.alert_history.append(alert)
                    self.last_alert_time[name] = datetime.now()
                    new_alerts.append(alert)

                    log_event("alert", condition.severity.value, alert.message)

                elif not triggered and name in self.active_alerts:
                    # Resolve alert
                    alert = self.active_alerts[name]
                    alert.resolved_at = datetime.now()
                    del self.active_alerts[name]

                    log_event("alert", "info", f"Alert resolved: {condition.name}")

        return new_alerts

    def _evaluate_condition(self, value: float, condition: AlertCondition) -> bool:
        """Evaluate alert condition"""
        if condition.operator == ">":
            return value > condition.threshold
        elif condition.operator == "<":
            return value < condition.threshold
        elif condition.operator == ">=":
            return value >= condition.threshold
        elif condition.operator == "<=":
            return value <= condition.threshold
        elif condition.operator == "==":
            return value == condition.threshold
        elif condition.operator == "!=":
            return value != condition.threshold
        else:
            log_event("alert", "error", f"Unknown operator: {condition.operator}")
            return False

    def get_active_alerts(self) -> List[AlertEvent]:
        """Get active alerts"""
        with self.lock:
            return list(self.active_alerts.values())

    def get_alert_history(self, limit: int = 100) -> List[AlertEvent]:
        """Get alert history"""
        with self.lock:
            return self.alert_history[-limit:]


class MonitoringSystem:
    """Central monitoring system"""

    def __init__(self):
        self.metrics_collector = MetricsCollector()
        self.health_checker = HealthChecker()
        self.alert_manager = AlertManager(self.metrics_collector)
        self.running = False
        self.background_thread: Optional[threading.Thread] = None
        self.check_interval = 30  # seconds

        # Setup default health checks
        self._setup_default_health_checks()

        # Setup default alert conditions
        self._setup_default_alert_conditions()

    def _setup_default_health_checks(self):
        """Setup default health checks"""

        # MongoDB health check
        def check_mongodb():
            try:
                from ..storage.storage import Storage
                storage = Storage()
                return storage.db is not None
            except Exception:
                return False

        self.health_checker.register_health_check(HealthCheck(
            name="mongodb",
            description="MongoDB database connectivity",
            check_func=check_mongodb,
            timeout=5.0,
            critical=True
        ))

        # Redis health check
        def check_redis():
            try:
                from cache_manager import get_cache_manager
                cache = get_cache_manager()
                return cache.is_connected
            except Exception:
                return False

        self.health_checker.register_health_check(HealthCheck(
            name="redis",
            description="Redis cache connectivity",
            check_func=check_redis,
            timeout=5.0,
            critical=False
        ))

        # RabbitMQ health check
        def check_rabbitmq():
            try:
                from message_broker import broker
                return hasattr(broker, "_connection") and broker._connection is not None
            except Exception:
                return False

        self.health_checker.register_health_check(HealthCheck(
            name="rabbitmq",
            description="RabbitMQ message broker connectivity",
            check_func=check_rabbitmq,
            timeout=5.0,
            critical=True
        ))

    def _setup_default_alert_conditions(self):
        """Setup default alert conditions"""

        # High error rate
        self.alert_manager.add_condition(AlertCondition(
            name="high_error_rate",
            metric_name="error_rate",
            operator=">",
            threshold=0.1,  # 10% error rate
            duration=300,   # 5 minutes
            severity=ErrorSeverity.HIGH,
            description="High error rate detected"
        ))

        # Slow response time
        self.alert_manager.add_condition(AlertCondition(
            name="slow_response_time",
            metric_name="response_time_avg",
            operator=">",
            threshold=5.0,  # 5 seconds
            duration=300,   # 5 minutes
            severity=ErrorSeverity.MEDIUM,
            description="Slow response time detected"
        ))

        # High memory usage
        self.alert_manager.add_condition(AlertCondition(
            name="high_memory_usage",
            metric_name="memory_usage_percent",
            operator=">",
            threshold=85.0,  # 85%
            duration=300,    # 5 minutes
            severity=ErrorSeverity.HIGH,
            description="High memory usage detected"
        ))

        # Export failure
        self.alert_manager.add_condition(AlertCondition(
            name="export_failure",
            metric_name="export_failures",
            operator=">",
            threshold=0,
            duration=60,    # 1 minute
            severity=ErrorSeverity.HIGH,
            description="Export failure detected"
        ))

    def start(self):
        """Start monitoring system"""
        if self.running:
            return

        self.running = True
        self.background_thread = threading.Thread(target=self._background_loop, daemon=True)
        self.background_thread.start()

        log_event("monitoring", "info", "Monitoring system started")

    def stop(self):
        """Stop monitoring system"""
        self.running = False
        if self.background_thread:
            self.background_thread.join(timeout=10)

        log_event("monitoring", "info", "Monitoring system stopped")

    def _background_loop(self):
        """Background monitoring loop"""
        while self.running:
            try:
                # Run health checks
                self.health_checker.run_all_health_checks()

                # Check alert conditions
                self.alert_manager.check_conditions()

                # Cleanup old metrics
                self.metrics_collector.cleanup_old_metrics()

                # Record system metrics
                self._record_system_metrics()

            except Exception as e:
                log_event("monitoring", "error", f"Error in monitoring loop: {e}")

            time.sleep(self.check_interval)

    def _record_system_metrics(self):
        """Record system metrics"""
        try:
            import psutil

            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.record_metric("cpu_usage_percent", cpu_percent)

            # Memory usage
            memory = psutil.virtual_memory()
            self.record_metric("memory_usage_percent", memory.percent)
            self.record_metric("memory_usage_bytes", memory.used)

            # Disk usage
            disk = psutil.disk_usage('/')
            self.record_metric("disk_usage_percent", disk.percent)
            self.record_metric("disk_usage_bytes", disk.used)

        except ImportError:
            # psutil not available
            pass
        except Exception as e:
            log_event("monitoring", "error", f"Error recording system metrics: {e}")

    def record_metric(self, name: str, value: float, labels: Optional[Dict[str, str]] = None,
                     metric_type: MetricType = MetricType.GAUGE):
        """Record a metric"""
        metric = Metric(
            name=name,
            type=metric_type,
            value=value,
            labels=labels or {},
            description=f"Metric {name}"
        )
        self.metrics_collector.record_metric(metric)

    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status"""
        return self.health_checker.get_overall_health()

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        # Get common metrics
        metrics = {}

        # System metrics
        for metric_name in ["cpu_usage_percent", "memory_usage_percent", "disk_usage_percent"]:
            stats = self.metrics_collector.get_metric_stats(metric_name, duration=300)
            if stats:
                metrics[metric_name] = stats

        # Application metrics
        for metric_name in ["error_rate", "response_time_avg", "export_failures"]:
            stats = self.metrics_collector.get_metric_stats(metric_name, duration=300)
            if stats:
                metrics[metric_name] = stats

        return metrics

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for monitoring dashboard"""
        return {
            "health": self.get_health_status(),
            "metrics": self.get_metrics_summary(),
            "active_alerts": [alert.to_dict() for alert in self.alert_manager.get_active_alerts()],
            "recent_alerts": [alert.to_dict() for alert in self.alert_manager.get_alert_history(10)],
            "timestamp": datetime.now().isoformat()
        }


# Global monitoring system
monitoring_system = MonitoringSystem()

# Helper functions
def start_monitoring():
    """Start the monitoring system"""
    monitoring_system.start()


def stop_monitoring():
    """Stop the monitoring system"""
    monitoring_system.stop()


def record_metric(name: str, value: float, labels: Optional[Dict[str, str]] = None):
    """Record a metric"""
    monitoring_system.record_metric(name, value, labels)


def get_health_status() -> Dict[str, Any]:
    """Get system health status"""
    return monitoring_system.get_health_status()


def get_dashboard_data() -> Dict[str, Any]:
    """Get dashboard data"""
    return monitoring_system.get_dashboard_data()