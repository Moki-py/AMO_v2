"""
Real-time progress tracking system for Google Sheets export operations
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

from ..storage.storage import Storage
from ..core.logger import log_event


class ExportStatus(str, Enum):
    """Export status enumeration"""
    PENDING = "pending"
    STARTING = "starting"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class EntityType(str, Enum):
    """Entity types for export"""
    DEALS = "deals"
    CONTACTS = "contacts"
    COMPANIES = "companies"
    EVENTS = "events"
    USERS = "users"
    PIPELINES = "pipelines"


@dataclass
class EntityProgress:
    """Progress information for a specific entity type"""
    entity_type: str
    processed: int = 0
    total: int = 0
    current_batch: int = 0
    total_batches: int = 0
    status: ExportStatus = ExportStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    estimated_completion: Optional[datetime] = None

    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage"""
        if self.total == 0:
            return 0.0
        return min(100.0, (self.processed / self.total) * 100.0)

    @property
    def duration(self) -> Optional[timedelta]:
        """Calculate duration of processing"""
        if not self.start_time:
            return None
        end_time = self.end_time or datetime.now()
        return end_time - self.start_time

    @property
    def processing_rate(self) -> float:
        """Calculate processing rate (items per second)"""
        if not self.start_time or self.processed == 0:
            return 0.0
        duration = self.duration
        if not duration or duration.total_seconds() == 0:
            return 0.0
        return self.processed / duration.total_seconds()


@dataclass
class ExportProgress:
    """Overall export progress information"""
    export_id: str
    status: ExportStatus = ExportStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    entities: Dict[str, EntityProgress] = None
    total_entities: int = 0
    completed_entities: int = 0
    error_message: Optional[str] = None
    spreadsheet_urls: Dict[str, str] = None

    def __post_init__(self):
        if self.entities is None:
            self.entities = {}
        if self.spreadsheet_urls is None:
            self.spreadsheet_urls = {}

    @property
    def overall_progress_percentage(self) -> float:
        """Calculate overall progress percentage"""
        if not self.entities:
            return 0.0

        total_progress = sum(entity.progress_percentage for entity in self.entities.values())
        return total_progress / len(self.entities) if self.entities else 0.0

    @property
    def duration(self) -> Optional[timedelta]:
        """Calculate total export duration"""
        if not self.start_time:
            return None
        end_time = self.end_time or datetime.now()
        return end_time - self.start_time

    @property
    def estimated_completion(self) -> Optional[datetime]:
        """Estimate completion time based on current progress"""
        if self.status in [ExportStatus.COMPLETED, ExportStatus.FAILED, ExportStatus.CANCELLED]:
            return self.end_time

        if not self.start_time or self.overall_progress_percentage == 0:
            return None

        duration = self.duration
        if not duration:
            return None

        # Calculate estimated total time based on current progress
        progress_ratio = self.overall_progress_percentage / 100.0
        if progress_ratio == 0:
            return None

        estimated_total_seconds = duration.total_seconds() / progress_ratio
        return self.start_time + timedelta(seconds=estimated_total_seconds)


class ExportProgressTracker:
    """
    Tracks progress of Google Sheets export operations with real-time updates
    and WebSocket integration capabilities
    """

    def __init__(self, storage: Storage):
        """Initialize the progress tracker"""
        self.storage = storage
        self.active_exports: Dict[str, ExportProgress] = {}
        self.progress_callbacks: List[Callable[[str, ExportProgress], None]] = []
        self.websocket_connections: Set[Any] = set()  # WebSocket connections for real-time updates

        # Initialize MongoDB collection for progress persistence
        if hasattr(storage, 'db') and storage.db:
            self.progress_collection = storage.db['export_progress']
            self._ensure_progress_indexes()
        else:
            self.progress_collection = None
            log_event("progress", "warning", "MongoDB not available, progress will not be persisted")

    def _ensure_progress_indexes(self):
        """Ensure indexes exist for progress collection"""
        if not self.progress_collection:
            return

        try:
            # Create index on export_id for fast lookups
            self.progress_collection.create_index([("export_id", 1)], unique=True)
            # Create index on status for filtering
            self.progress_collection.create_index([("status", 1)])
            # Create index on start_time for cleanup
            self.progress_collection.create_index([("start_time", 1)])
            log_event("progress", "info", "Created indexes for progress collection")
        except Exception as e:
            log_event("progress", "error", f"Error creating progress indexes: {e}")

    def start_export(self, export_id: Optional[str] = None, entity_types: List[str] = None) -> str:
        """
        Start tracking a new export operation

        Args:
            export_id: Optional export ID, will generate one if not provided
            entity_types: List of entity types to be exported

        Returns:
            export_id: The ID of the started export
        """
        if not export_id:
            export_id = str(uuid.uuid4())

        if not entity_types:
            entity_types = [e.value for e in EntityType]

        # Create progress tracking object
        progress = ExportProgress(
            export_id=export_id,
            status=ExportStatus.STARTING,
            start_time=datetime.now(),
            total_entities=len(entity_types)
        )

        # Initialize entity progress
        for entity_type in entity_types:
            progress.entities[entity_type] = EntityProgress(
                entity_type=entity_type,
                status=ExportStatus.PENDING
            )

        # Store in active exports
        self.active_exports[export_id] = progress

        # Persist to database
        self._persist_progress(progress)

        # Notify callbacks
        self._notify_progress_update(export_id, progress)

        log_event("progress", "info", f"Started tracking export {export_id} with {len(entity_types)} entities")
        return export_id

    def update_entity_progress(
        self,
        export_id: str,
        entity_type: str,
        processed: int,
        total: int,
        current_batch: int = 0,
        total_batches: int = 0,
        status: Optional[ExportStatus] = None
    ) -> None:
        """
        Update progress for a specific entity type

        Args:
            export_id: Export operation ID
            entity_type: Type of entity being processed
            processed: Number of items processed
            total: Total number of items to process
            current_batch: Current batch number
            total_batches: Total number of batches
            status: Optional status update
        """
        if export_id not in self.active_exports:
            log_event("progress", "warning", f"Export {export_id} not found in active exports")
            return

        progress = self.active_exports[export_id]

        if entity_type not in progress.entities:
            # Add new entity if not exists
            progress.entities[entity_type] = EntityProgress(entity_type=entity_type)

        entity_progress = progress.entities[entity_type]

        # Update entity progress
        entity_progress.processed = processed
        entity_progress.total = total
        entity_progress.current_batch = current_batch
        entity_progress.total_batches = total_batches

        if status:
            entity_progress.status = status
            if status == ExportStatus.IN_PROGRESS and not entity_progress.start_time:
                entity_progress.start_time = datetime.now()
            elif status in [ExportStatus.COMPLETED, ExportStatus.FAILED, ExportStatus.CANCELLED]:
                entity_progress.end_time = datetime.now()

        # Calculate estimated completion time
        if entity_progress.processing_rate > 0 and processed < total:
            remaining_items = total - processed
            estimated_seconds = remaining_items / entity_progress.processing_rate
            entity_progress.estimated_completion = datetime.now() + timedelta(seconds=estimated_seconds)

        # Update overall export status
        self._update_overall_status(progress)

        # Persist to database
        self._persist_progress(progress)

        # Notify callbacks
        self._notify_progress_update(export_id, progress)

        log_event(
            "progress", "debug",
            f"Updated {entity_type} progress: {processed}/{total} ({entity_progress.progress_percentage:.1f}%)"
        )

    def complete_entity(self, export_id: str, entity_type: str, spreadsheet_url: Optional[str] = None) -> None:
        """
        Mark an entity as completed

        Args:
            export_id: Export operation ID
            entity_type: Type of entity completed
            spreadsheet_url: URL of the exported spreadsheet
        """
        if export_id not in self.active_exports:
            return

        progress = self.active_exports[export_id]

        if entity_type in progress.entities:
            entity_progress = progress.entities[entity_type]
            entity_progress.status = ExportStatus.COMPLETED
            entity_progress.end_time = datetime.now()

            # Set processed to total if not already done
            if entity_progress.processed < entity_progress.total:
                entity_progress.processed = entity_progress.total

        # Store spreadsheet URL
        if spreadsheet_url:
            progress.spreadsheet_urls[entity_type] = spreadsheet_url

        # Update overall status
        self._update_overall_status(progress)

        # Persist and notify
        self._persist_progress(progress)
        self._notify_progress_update(export_id, progress)

        log_event("progress", "info", f"Completed {entity_type} export for {export_id}")

    def handle_entity_error(self, export_id: str, entity_type: str, error_message: str) -> None:
        """
        Handle error for a specific entity

        Args:
            export_id: Export operation ID
            entity_type: Type of entity that failed
            error_message: Error message
        """
        if export_id not in self.active_exports:
            return

        progress = self.active_exports[export_id]

        if entity_type in progress.entities:
            entity_progress = progress.entities[entity_type]
            entity_progress.status = ExportStatus.FAILED
            entity_progress.error_message = error_message
            entity_progress.end_time = datetime.now()

        # Update overall status
        self._update_overall_status(progress)

        # Persist and notify
        self._persist_progress(progress)
        self._notify_progress_update(export_id, progress)

        log_event("progress", "error", f"Error in {entity_type} export for {export_id}: {error_message}")

    def complete_export(self, export_id: str, success: bool = True, error_message: Optional[str] = None) -> None:
        """
        Complete an export operation

        Args:
            export_id: Export operation ID
            success: Whether the export was successful
            error_message: Optional error message if failed
        """
        if export_id not in self.active_exports:
            return

        progress = self.active_exports[export_id]
        progress.end_time = datetime.now()

        if success:
            progress.status = ExportStatus.COMPLETED
        else:
            progress.status = ExportStatus.FAILED
            progress.error_message = error_message

        # Persist final state
        self._persist_progress(progress)

        # Notify callbacks
        self._notify_progress_update(export_id, progress)

        # Remove from active exports after a delay to allow final updates
        asyncio.create_task(self._cleanup_completed_export(export_id))

        log_event("progress", "info", f"Export {export_id} completed with status: {progress.status}")

    def cancel_export(self, export_id: str) -> bool:
        """
        Cancel an export operation

        Args:
            export_id: Export operation ID

        Returns:
            bool: True if export was cancelled, False if not found
        """
        if export_id not in self.active_exports:
            return False

        progress = self.active_exports[export_id]
        progress.status = ExportStatus.CANCELLED
        progress.end_time = datetime.now()

        # Cancel all entity exports
        for entity_progress in progress.entities.values():
            if entity_progress.status in [ExportStatus.PENDING, ExportStatus.IN_PROGRESS]:
                entity_progress.status = ExportStatus.CANCELLED
                entity_progress.end_time = datetime.now()

        # Persist and notify
        self._persist_progress(progress)
        self._notify_progress_update(export_id, progress)

        log_event("progress", "info", f"Cancelled export {export_id}")
        return True

    def get_export_progress(self, export_id: str) -> Optional[ExportProgress]:
        """
        Get current progress for an export

        Args:
            export_id: Export operation ID

        Returns:
            ExportProgress or None if not found
        """
        # Check active exports first
        if export_id in self.active_exports:
            return self.active_exports[export_id]

        # Try to load from database
        return self._load_progress_from_db(export_id)

    def get_all_active_exports(self) -> Dict[str, ExportProgress]:
        """Get all currently active exports"""
        return self.active_exports.copy()

    def add_progress_callback(self, callback: Callable[[str, ExportProgress], None]) -> None:
        """
        Add a callback function to be called on progress updates

        Args:
            callback: Function that takes (export_id, progress) as arguments
        """
        self.progress_callbacks.append(callback)

    def remove_progress_callback(self, callback: Callable[[str, ExportProgress], None]) -> None:
        """Remove a progress callback"""
        if callback in self.progress_callbacks:
            self.progress_callbacks.remove(callback)

    def add_websocket_connection(self, websocket) -> None:
        """Add a WebSocket connection for real-time updates"""
        self.websocket_connections.add(websocket)
        log_event("progress", "debug", f"Added WebSocket connection, total: {len(self.websocket_connections)}")

    def remove_websocket_connection(self, websocket) -> None:
        """Remove a WebSocket connection"""
        self.websocket_connections.discard(websocket)
        log_event("progress", "debug", f"Removed WebSocket connection, total: {len(self.websocket_connections)}")

    def _update_overall_status(self, progress: ExportProgress) -> None:
        """Update overall export status based on entity statuses"""
        if not progress.entities:
            return

        entity_statuses = [entity.status for entity in progress.entities.values()]

        # Count entities by status
        completed_count = sum(1 for status in entity_statuses if status == ExportStatus.COMPLETED)
        failed_count = sum(1 for status in entity_statuses if status == ExportStatus.FAILED)
        cancelled_count = sum(1 for status in entity_statuses if status == ExportStatus.CANCELLED)
        in_progress_count = sum(1 for status in entity_statuses if status == ExportStatus.IN_PROGRESS)

        progress.completed_entities = completed_count

        # Determine overall status
        if cancelled_count > 0:
            progress.status = ExportStatus.CANCELLED
        elif failed_count == len(entity_statuses):
            progress.status = ExportStatus.FAILED
        elif completed_count == len(entity_statuses):
            progress.status = ExportStatus.COMPLETED
        elif in_progress_count > 0 or completed_count > 0:
            progress.status = ExportStatus.IN_PROGRESS
        else:
            progress.status = ExportStatus.PENDING

    def _persist_progress(self, progress: ExportProgress) -> None:
        """Persist progress to database"""
        if not self.progress_collection:
            return

        try:
            # Convert to dict for MongoDB storage
            progress_dict = asdict(progress)
            progress_dict['_id'] = progress.export_id
            progress_dict['updated_at'] = datetime.now().isoformat()

            # Convert datetime objects to ISO strings
            if progress_dict['start_time']:
                progress_dict['start_time'] = progress.start_time.isoformat()
            if progress_dict['end_time']:
                progress_dict['end_time'] = progress.end_time.isoformat()

            # Convert entity datetime objects
            for entity_type, entity_data in progress_dict['entities'].items():
                if entity_data.get('start_time'):
                    entity_data['start_time'] = progress.entities[entity_type].start_time.isoformat()
                if entity_data.get('end_time'):
                    entity_data['end_time'] = progress.entities[entity_type].end_time.isoformat()
                if entity_data.get('estimated_completion'):
                    entity_data['estimated_completion'] = progress.entities[entity_type].estimated_completion.isoformat()

            # Upsert to database
            self.progress_collection.replace_one(
                {'_id': progress.export_id},
                progress_dict,
                upsert=True
            )

        except Exception as e:
            log_event("progress", "error", f"Error persisting progress: {e}")

    def _load_progress_from_db(self, export_id: str) -> Optional[ExportProgress]:
        """Load progress from database"""
        if not self.progress_collection:
            return None

        try:
            progress_dict = self.progress_collection.find_one({'_id': export_id})
            if not progress_dict:
                return None

            # Remove MongoDB _id
            del progress_dict['_id']

            # Convert ISO strings back to datetime objects
            if progress_dict.get('start_time'):
                progress_dict['start_time'] = datetime.fromisoformat(progress_dict['start_time'])
            if progress_dict.get('end_time'):
                progress_dict['end_time'] = datetime.fromisoformat(progress_dict['end_time'])

            # Convert entity datetime objects
            entities = {}
            for entity_type, entity_data in progress_dict.get('entities', {}).items():
                if entity_data.get('start_time'):
                    entity_data['start_time'] = datetime.fromisoformat(entity_data['start_time'])
                if entity_data.get('end_time'):
                    entity_data['end_time'] = datetime.fromisoformat(entity_data['end_time'])
                if entity_data.get('estimated_completion'):
                    entity_data['estimated_completion'] = datetime.fromisoformat(entity_data['estimated_completion'])

                entities[entity_type] = EntityProgress(**entity_data)

            progress_dict['entities'] = entities

            return ExportProgress(**progress_dict)

        except Exception as e:
            log_event("progress", "error", f"Error loading progress from database: {e}")
            return None

    def _notify_progress_update(self, export_id: str, progress: ExportProgress) -> None:
        """Notify all callbacks and WebSocket connections of progress update"""
        # Call registered callbacks
        for callback in self.progress_callbacks:
            try:
                callback(export_id, progress)
            except Exception as e:
                log_event("progress", "error", f"Error in progress callback: {e}")

        # Send WebSocket updates
        if self.websocket_connections:
            asyncio.create_task(self._send_websocket_updates(export_id, progress))

    async def _send_websocket_updates(self, export_id: str, progress: ExportProgress) -> None:
        """Send progress updates to WebSocket connections"""
        if not self.websocket_connections:
            return

        # Prepare update message
        update_message = {
            'type': 'progress_update',
            'export_id': export_id,
            'status': progress.status.value,
            'overall_progress': progress.overall_progress_percentage,
            'entities': {
                entity_type: {
                    'status': entity.status.value,
                    'progress': entity.progress_percentage,
                    'processed': entity.processed,
                    'total': entity.total,
                    'current_batch': entity.current_batch,
                    'total_batches': entity.total_batches,
                    'processing_rate': entity.processing_rate,
                    'estimated_completion': entity.estimated_completion.isoformat() if entity.estimated_completion else None
                }
                for entity_type, entity in progress.entities.items()
            },
            'spreadsheet_urls': progress.spreadsheet_urls,
            'estimated_completion': progress.estimated_completion.isoformat() if progress.estimated_completion else None,
            'duration': progress.duration.total_seconds() if progress.duration else None
        }

        # Send to all connected WebSockets
        disconnected_connections = set()
        for websocket in self.websocket_connections.copy():
            try:
                await websocket.send_text(json.dumps(update_message))
            except Exception as e:
                log_event("progress", "debug", f"WebSocket connection error: {e}")
                disconnected_connections.add(websocket)

        # Remove disconnected connections
        for websocket in disconnected_connections:
            self.websocket_connections.discard(websocket)

    async def _cleanup_completed_export(self, export_id: str, delay_seconds: int = 300) -> None:
        """
        Clean up completed export from active exports after delay

        Args:
            export_id: Export operation ID
            delay_seconds: Delay before cleanup in seconds
        """
        await asyncio.sleep(delay_seconds)

        if export_id in self.active_exports:
            progress = self.active_exports[export_id]
            if progress.status in [ExportStatus.COMPLETED, ExportStatus.FAILED, ExportStatus.CANCELLED]:
                del self.active_exports[export_id]
                log_event("progress", "debug", f"Cleaned up completed export {export_id}")

    def cleanup_old_progress_records(self, days_old: int = 30) -> int:
        """
        Clean up old progress records from database

        Args:
            days_old: Remove records older than this many days

        Returns:
            Number of records removed
        """
        if not self.progress_collection:
            return 0

        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            result = self.progress_collection.delete_many({
                'start_time': {'$lt': cutoff_date.isoformat()}
            })

            deleted_count = result.deleted_count
            if deleted_count > 0:
                log_event("progress", "info", f"Cleaned up {deleted_count} old progress records")

            return deleted_count

        except Exception as e:
            log_event("progress", "error", f"Error cleaning up old progress records: {e}")
            return 0