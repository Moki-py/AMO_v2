"""
Progress update mechanisms for real-time web UI updates and detailed reporting
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Callable
from dataclasses import dataclass
from enum import Enum

from .progress_tracker import ExportProgress, EntityProgress, ExportStatus
from ..core.logger import log_event


class NotificationLevel(str, Enum):
    """Notification levels for progress updates"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


@dataclass
class ProgressNotification:
    """A progress notification message"""
    export_id: str
    level: NotificationLevel
    message: str
    entity_type: Optional[str] = None
    timestamp: datetime = None
    details: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class EntityProgressReport:
    """Detailed progress report for an entity"""
    entity_type: str
    status: str
    progress_percentage: float
    processed: int
    total: int
    current_batch: int
    total_batches: int
    processing_rate: float
    estimated_completion: Optional[str]
    duration: Optional[float]
    error_message: Optional[str]
    suggested_actions: List[str]


@dataclass
class ExportProgressReport:
    """Comprehensive export progress report"""
    export_id: str
    status: str
    overall_progress: float
    start_time: Optional[str]
    estimated_completion: Optional[str]
    duration: Optional[float]
    entities: Dict[str, EntityProgressReport]
    spreadsheet_urls: Dict[str, str]
    notifications: List[ProgressNotification]
    summary_stats: Dict[str, Any]
    suggested_actions: List[str]


class ProgressUpdateMechanism:
    """
    Manages progress update mechanisms for real-time web UI updates
    and detailed progress reporting by entity type
    """

    def __init__(self):
        """Initialize the progress update mechanism"""
        self.websocket_connections: Set[Any] = set()
        self.notification_callbacks: List[Callable[[ProgressNotification], None]] = []
        self.progress_callbacks: List[Callable[[str, ExportProgressReport], None]] = []
        self.notifications_history: Dict[str, List[ProgressNotification]] = {}
        self.max_notifications_per_export = 100

    def add_websocket_connection(self, websocket) -> None:
        """Add a WebSocket connection for real-time updates"""
        self.websocket_connections.add(websocket)
        log_event("progress_notifier", "debug", f"Added WebSocket connection, total: {len(self.websocket_connections)}")

    def remove_websocket_connection(self, websocket) -> None:
        """Remove a WebSocket connection"""
        self.websocket_connections.discard(websocket)
        log_event("progress_notifier", "debug", f"Removed WebSocket connection, total: {len(self.websocket_connections)}")

    def add_notification_callback(self, callback: Callable[[ProgressNotification], None]) -> None:
        """Add a callback for notifications"""
        self.notification_callbacks.append(callback)

    def remove_notification_callback(self, callback: Callable[[ProgressNotification], None]) -> None:
        """Remove a notification callback"""
        if callback in self.notification_callbacks:
            self.notification_callbacks.remove(callback)

    def add_progress_callback(self, callback: Callable[[str, ExportProgressReport], None]) -> None:
        """Add a callback for progress reports"""
        self.progress_callbacks.append(callback)

    def remove_progress_callback(self, callback: Callable[[str, ExportProgressReport], None]) -> None:
        """Remove a progress callback"""
        if callback in self.progress_callbacks:
            self.progress_callbacks.remove(callback)

    async def send_progress_update(self, export_id: str, progress: ExportProgress) -> None:
        """
        Send comprehensive progress update to all connected clients

        Args:
            export_id: Export operation ID
            progress: Current progress information
        """
        try:
            # Generate detailed progress report
            report = self._generate_progress_report(export_id, progress)

            # Send to WebSocket connections
            await self._send_websocket_update(report)

            # Call progress callbacks
            for callback in self.progress_callbacks:
                try:
                    callback(export_id, report)
                except Exception as e:
                    log_event("progress_notifier", "error", f"Error in progress callback: {e}")

            log_event("progress_notifier", "debug", f"Sent progress update for export {export_id}")

        except Exception as e:
            log_event("progress_notifier", "error", f"Error sending progress update: {e}")

    async def send_notification(self, notification: ProgressNotification) -> None:
        """
        Send a notification to all connected clients

        Args:
            notification: Notification to send
        """
        try:
            # Store notification in history
            if notification.export_id not in self.notifications_history:
                self.notifications_history[notification.export_id] = []

            self.notifications_history[notification.export_id].append(notification)

            # Limit notification history size
            if len(self.notifications_history[notification.export_id]) > self.max_notifications_per_export:
                self.notifications_history[notification.export_id] = \
                    self.notifications_history[notification.export_id][-self.max_notifications_per_export:]

            # Send to WebSocket connections
            await self._send_websocket_notification(notification)

            # Call notification callbacks
            for callback in self.notification_callbacks:
                try:
                    callback(notification)
                except Exception as e:
                    log_event("progress_notifier", "error", f"Error in notification callback: {e}")

            log_event("progress_notifier", "debug", f"Sent notification: {notification.message}")

        except Exception as e:
            log_event("progress_notifier", "error", f"Error sending notification: {e}")

    async def notify_export_started(self, export_id: str, entity_types: List[str]) -> None:
        """Notify that an export has started"""
        notification = ProgressNotification(
            export_id=export_id,
            level=NotificationLevel.INFO,
            message=f"Started Google Sheets export for {len(entity_types)} entity types",
            details={"entity_types": entity_types}
        )
        await self.send_notification(notification)

    async def notify_entity_started(self, export_id: str, entity_type: str, total_items: int) -> None:
        """Notify that processing of an entity type has started"""
        notification = ProgressNotification(
            export_id=export_id,
            level=NotificationLevel.INFO,
            message=f"Started processing {entity_type} ({total_items:,} items)",
            entity_type=entity_type,
            details={"total_items": total_items}
        )
        await self.send_notification(notification)

    async def notify_entity_progress(
        self,
        export_id: str,
        entity_type: str,
        processed: int,
        total: int,
        current_batch: int,
        total_batches: int
    ) -> None:
        """Notify of entity processing progress"""
        progress_pct = (processed / total * 100) if total > 0 else 0

        notification = ProgressNotification(
            export_id=export_id,
            level=NotificationLevel.DEBUG,
            message=f"{entity_type}: Processed {processed:,}/{total:,} items ({progress_pct:.1f}%) - Batch {current_batch}/{total_batches}",
            entity_type=entity_type,
            details={
                "processed": processed,
                "total": total,
                "progress_percentage": progress_pct,
                "current_batch": current_batch,
                "total_batches": total_batches
            }
        )
        await self.send_notification(notification)

    async def notify_entity_completed(
        self,
        export_id: str,
        entity_type: str,
        processed: int,
        duration: float,
        spreadsheet_url: Optional[str] = None
    ) -> None:
        """Notify that an entity has been completed"""
        rate = processed / duration if duration > 0 else 0

        message = f"Completed {entity_type}: {processed:,} items in {duration:.1f}s ({rate:.1f} items/sec)"
        if spreadsheet_url:
            message += f" - View at: {spreadsheet_url}"

        notification = ProgressNotification(
            export_id=export_id,
            level=NotificationLevel.SUCCESS,
            message=message,
            entity_type=entity_type,
            details={
                "processed": processed,
                "duration": duration,
                "processing_rate": rate,
                "spreadsheet_url": spreadsheet_url
            }
        )
        await self.send_notification(notification)

    async def notify_entity_error(
        self,
        export_id: str,
        entity_type: str,
        error_message: str,
        suggested_actions: List[str] = None
    ) -> None:
        """Notify of an entity processing error"""
        if not suggested_actions:
            suggested_actions = self._generate_error_suggestions(error_message)

        notification = ProgressNotification(
            export_id=export_id,
            level=NotificationLevel.ERROR,
            message=f"Error processing {entity_type}: {error_message}",
            entity_type=entity_type,
            details={
                "error_message": error_message,
                "suggested_actions": suggested_actions
            }
        )
        await self.send_notification(notification)

    async def notify_export_completed(
        self,
        export_id: str,
        success: bool,
        total_processed: int,
        duration: float,
        spreadsheet_urls: Dict[str, str]
    ) -> None:
        """Notify that the entire export has completed"""
        if success:
            message = f"Export completed successfully: {total_processed:,} items in {duration:.1f}s"
            level = NotificationLevel.SUCCESS
        else:
            message = f"Export completed with errors after {duration:.1f}s"
            level = NotificationLevel.WARNING

        notification = ProgressNotification(
            export_id=export_id,
            level=level,
            message=message,
            details={
                "success": success,
                "total_processed": total_processed,
                "duration": duration,
                "spreadsheet_urls": spreadsheet_urls,
                "spreadsheet_count": len(spreadsheet_urls)
            }
        )
        await self.send_notification(notification)

    async def notify_rate_limit_warning(self, export_id: str, entity_type: str, retry_after: int) -> None:
        """Notify of rate limiting"""
        notification = ProgressNotification(
            export_id=export_id,
            level=NotificationLevel.WARNING,
            message=f"Rate limit reached for {entity_type}. Retrying in {retry_after} seconds...",
            entity_type=entity_type,
            details={"retry_after": retry_after}
        )
        await self.send_notification(notification)

    async def notify_batch_retry(self, export_id: str, entity_type: str, batch_num: int, attempt: int) -> None:
        """Notify of batch retry"""
        notification = ProgressNotification(
            export_id=export_id,
            level=NotificationLevel.WARNING,
            message=f"Retrying {entity_type} batch {batch_num} (attempt {attempt})",
            entity_type=entity_type,
            details={"batch_number": batch_num, "attempt": attempt}
        )
        await self.send_notification(notification)

    def get_notifications_history(self, export_id: str) -> List[ProgressNotification]:
        """Get notification history for an export"""
        return self.notifications_history.get(export_id, [])

    def clear_notifications_history(self, export_id: str) -> None:
        """Clear notification history for an export"""
        if export_id in self.notifications_history:
            del self.notifications_history[export_id]

    def _generate_progress_report(self, export_id: str, progress: ExportProgress) -> ExportProgressReport:
        """Generate a comprehensive progress report"""
        # Generate entity reports
        entity_reports = {}
        for entity_type, entity_progress in progress.entities.items():
            entity_reports[entity_type] = EntityProgressReport(
                entity_type=entity_type,
                status=entity_progress.status.value,
                progress_percentage=entity_progress.progress_percentage,
                processed=entity_progress.processed,
                total=entity_progress.total,
                current_batch=entity_progress.current_batch,
                total_batches=entity_progress.total_batches,
                processing_rate=entity_progress.processing_rate,
                estimated_completion=entity_progress.estimated_completion.isoformat() if entity_progress.estimated_completion else None,
                duration=entity_progress.duration.total_seconds() if entity_progress.duration else None,
                error_message=entity_progress.error_message,
                suggested_actions=self._generate_entity_suggestions(entity_progress)
            )

        # Calculate summary statistics
        summary_stats = self._calculate_summary_stats(progress)

        # Generate overall suggested actions
        suggested_actions = self._generate_overall_suggestions(progress)

        # Get recent notifications
        recent_notifications = self.get_notifications_history(export_id)[-10:]  # Last 10 notifications

        return ExportProgressReport(
            export_id=export_id,
            status=progress.status.value,
            overall_progress=progress.overall_progress_percentage,
            start_time=progress.start_time.isoformat() if progress.start_time else None,
            estimated_completion=progress.estimated_completion.isoformat() if progress.estimated_completion else None,
            duration=progress.duration.total_seconds() if progress.duration else None,
            entities=entity_reports,
            spreadsheet_urls=progress.spreadsheet_urls,
            notifications=recent_notifications,
            summary_stats=summary_stats,
            suggested_actions=suggested_actions
        )

    def _calculate_summary_stats(self, progress: ExportProgress) -> Dict[str, Any]:
        """Calculate summary statistics for the export"""
        total_processed = sum(entity.processed for entity in progress.entities.values())
        total_items = sum(entity.total for entity in progress.entities.values())

        # Calculate average processing rate
        active_entities = [e for e in progress.entities.values() if e.processing_rate > 0]
        avg_processing_rate = sum(e.processing_rate for e in active_entities) / len(active_entities) if active_entities else 0

        # Count entities by status
        status_counts = {}
        for entity in progress.entities.values():
            status = entity.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "total_processed": total_processed,
            "total_items": total_items,
            "overall_progress_percentage": (total_processed / total_items * 100) if total_items > 0 else 0,
            "average_processing_rate": avg_processing_rate,
            "entities_by_status": status_counts,
            "completed_entities": progress.completed_entities,
            "total_entities": progress.total_entities,
            "spreadsheet_count": len(progress.spreadsheet_urls)
        }

    def _generate_entity_suggestions(self, entity_progress: EntityProgress) -> List[str]:
        """Generate suggested actions for an entity based on its status"""
        suggestions = []

        if entity_progress.status == ExportStatus.FAILED:
            if entity_progress.error_message:
                suggestions.extend(self._generate_error_suggestions(entity_progress.error_message))
            else:
                suggestions.append("Check the logs for detailed error information")
                suggestions.append("Try restarting the export for this entity type")

        elif entity_progress.status == ExportStatus.IN_PROGRESS:
            if entity_progress.processing_rate < 1.0:  # Less than 1 item per second
                suggestions.append("Processing is slow - check network connectivity")
                suggestions.append("Consider reducing batch size if memory usage is high")

            if entity_progress.current_batch > 0 and entity_progress.processed == 0:
                suggestions.append("No items processed yet - check data availability")

        elif entity_progress.status == ExportStatus.PENDING:
            suggestions.append("Waiting to start - check if other exports are running")

        return suggestions

    def _generate_overall_suggestions(self, progress: ExportProgress) -> List[str]:
        """Generate overall suggested actions for the export"""
        suggestions = []

        if progress.status == ExportStatus.FAILED:
            suggestions.append("Review error messages for each entity type")
            suggestions.append("Check Google Sheets API quotas and permissions")
            suggestions.append("Verify network connectivity")
            suggestions.append("Consider restarting failed entity exports individually")

        elif progress.status == ExportStatus.IN_PROGRESS:
            # Check for slow progress
            if progress.overall_progress_percentage < 10 and progress.duration and progress.duration.total_seconds() > 300:
                suggestions.append("Export is progressing slowly - check system resources")
                suggestions.append("Consider reducing concurrent entity exports")

            # Check for stalled entities
            stalled_entities = [
                entity_type for entity_type, entity in progress.entities.items()
                if entity.status == ExportStatus.IN_PROGRESS and entity.processing_rate == 0
            ]
            if stalled_entities:
                suggestions.append(f"Some entities appear stalled: {', '.join(stalled_entities)}")
                suggestions.append("Check logs for these entities or consider restarting them")

        elif progress.status == ExportStatus.COMPLETED:
            suggestions.append("Export completed successfully!")
            if progress.spreadsheet_urls:
                suggestions.append("Click on the spreadsheet links to view your exported data")

        return suggestions

    def _generate_error_suggestions(self, error_message: str) -> List[str]:
        """Generate suggested actions based on error message"""
        suggestions = []
        error_lower = error_message.lower()

        if "rate limit" in error_lower or "quota" in error_lower:
            suggestions.append("Wait for rate limits to reset (usually 1 minute)")
            suggestions.append("Reduce batch size to avoid hitting rate limits")
            suggestions.append("Check Google Sheets API quota usage in Google Cloud Console")

        elif "permission" in error_lower or "access" in error_lower:
            suggestions.append("Check that the service account has edit access to the spreadsheet")
            suggestions.append("Verify the spreadsheet ID is correct")
            suggestions.append("Ensure the spreadsheet is not deleted or moved")

        elif "network" in error_lower or "connection" in error_lower:
            suggestions.append("Check internet connectivity")
            suggestions.append("Verify firewall settings allow Google Sheets API access")
            suggestions.append("Try again in a few minutes")

        elif "authentication" in error_lower or "credential" in error_lower:
            suggestions.append("Check that credentials.json file is present and valid")
            suggestions.append("Verify the service account key is not expired")
            suggestions.append("Re-download credentials from Google Cloud Console if needed")

        elif "not found" in error_lower:
            suggestions.append("Verify the spreadsheet ID exists and is accessible")
            suggestions.append("Check that the spreadsheet hasn't been deleted")
            suggestions.append("Ensure you have the correct spreadsheet URL")

        else:
            suggestions.append("Check the detailed error logs for more information")
            suggestions.append("Try restarting the export operation")
            suggestions.append("Contact support if the issue persists")

        return suggestions

    async def _send_websocket_update(self, report: ExportProgressReport) -> None:
        """Send progress report to WebSocket connections"""
        if not self.websocket_connections:
            return

        # Convert report to JSON-serializable format
        update_message = {
            "type": "progress_report",
            "export_id": report.export_id,
            "status": report.status,
            "overall_progress": report.overall_progress,
            "start_time": report.start_time,
            "estimated_completion": report.estimated_completion,
            "duration": report.duration,
            "entities": {
                entity_type: {
                    "status": entity.status,
                    "progress_percentage": entity.progress_percentage,
                    "processed": entity.processed,
                    "total": entity.total,
                    "current_batch": entity.current_batch,
                    "total_batches": entity.total_batches,
                    "processing_rate": entity.processing_rate,
                    "estimated_completion": entity.estimated_completion,
                    "duration": entity.duration,
                    "error_message": entity.error_message,
                    "suggested_actions": entity.suggested_actions
                }
                for entity_type, entity in report.entities.items()
            },
            "spreadsheet_urls": report.spreadsheet_urls,
            "summary_stats": report.summary_stats,
            "suggested_actions": report.suggested_actions,
            "timestamp": datetime.now().isoformat()
        }

        # Send to all connected WebSockets
        disconnected_connections = set()
        for websocket in self.websocket_connections.copy():
            try:
                await websocket.send_text(json.dumps(update_message))
            except Exception as e:
                log_event("progress_notifier", "debug", f"WebSocket connection error: {e}")
                disconnected_connections.add(websocket)

        # Remove disconnected connections
        for websocket in disconnected_connections:
            self.websocket_connections.discard(websocket)

    async def _send_websocket_notification(self, notification: ProgressNotification) -> None:
        """Send notification to WebSocket connections"""
        if not self.websocket_connections:
            return

        # Convert notification to JSON-serializable format
        notification_message = {
            "type": "notification",
            "export_id": notification.export_id,
            "level": notification.level.value,
            "message": notification.message,
            "entity_type": notification.entity_type,
            "timestamp": notification.timestamp.isoformat(),
            "details": notification.details
        }

        # Send to all connected WebSockets
        disconnected_connections = set()
        for websocket in self.websocket_connections.copy():
            try:
                await websocket.send_text(json.dumps(notification_message))
            except Exception as e:
                log_event("progress_notifier", "debug", f"WebSocket connection error: {e}")
                disconnected_connections.add(websocket)

        # Remove disconnected connections
        for websocket in disconnected_connections:
            self.websocket_connections.discard(websocket)


# Global instance for use across the application
progress_notifier = ProgressUpdateMechanism()