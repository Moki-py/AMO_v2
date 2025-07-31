"""
Export recovery and partial export handling for Google Sheets
"""

import json
import time
from typing import Dict, Any, Optional, List, Set, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

from .exceptions import (
    GoogleSheetsPartialExportError,
    GoogleSheetsBatchError,
    ErrorContext,
    create_error_context
)
from ..core.logger import log_event


class ExportStatus(str, Enum):
    """Export operation status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"
    CANCELLED = "cancelled"


class EntityExportStatus(str, Enum):
    """Individual entity export status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class BatchProgress:
    """Progress tracking for batch operations"""
    batch_id: str
    entity_type: str
    total_records: int
    processed_records: int = 0
    failed_records: int = 0
    batch_size: int = 1000
    current_batch: int = 0
    total_batches: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: EntityExportStatus = EntityExportStatus.PENDING
    error_message: Optional[str] = None

    def __post_init__(self):
        if self.total_batches == 0 and self.total_records > 0:
            self.total_batches = (self.total_records + self.batch_size - 1) // self.batch_size


@dataclass
class ExportState:
    """Complete export operation state"""
    export_id: str
    operation_type: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: ExportStatus = ExportStatus.PENDING
    entity_progress: Dict[str, BatchProgress] = field(default_factory=dict)
    exported_urls: Dict[str, str] = field(default_factory=dict)
    failed_entities: List[str] = field(default_factory=list)
    error_messages: List[str] = field(default_factory=list)
    configuration: Dict[str, Any] = field(default_factory=dict)
    resumable: bool = True

    def get_total_progress(self) -> Tuple[int, int]:
        """Get overall progress (processed, total)"""
        total_processed = sum(p.processed_records for p in self.entity_progress.values())
        total_records = sum(p.total_records for p in self.entity_progress.values())
        return total_processed, total_records

    def get_completion_percentage(self) -> float:
        """Get completion percentage"""
        processed, total = self.get_total_progress()
        return (processed / total * 100) if total > 0 else 0.0

    def is_complete(self) -> bool:
        """Check if export is complete"""
        return all(
            p.status in [EntityExportStatus.COMPLETED, EntityExportStatus.SKIPPED]
            for p in self.entity_progress.values()
        )

    def has_failures(self) -> bool:
        """Check if export has any failures"""
        return any(
            p.status == EntityExportStatus.FAILED
            for p in self.entity_progress.values()
        )


class ExportRecoveryManager:
    """Manages export recovery and partial export handling"""

    def __init__(self, state_dir: str = "data/export_states"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.current_exports: Dict[str, ExportState] = {}

    def create_export_state(
        self,
        export_id: str,
        operation_type: str,
        entity_counts: Dict[str, int],
        configuration: Dict[str, Any]
    ) -> ExportState:
        """Create new export state"""
        export_state = ExportState(
            export_id=export_id,
            operation_type=operation_type,
            start_time=datetime.now(),
            configuration=configuration
        )

        # Initialize entity progress
        for entity_type, count in entity_counts.items():
            batch_progress = BatchProgress(
                batch_id=f"{export_id}_{entity_type}",
                entity_type=entity_type,
                total_records=count
            )
            export_state.entity_progress[entity_type] = batch_progress

        self.current_exports[export_id] = export_state
        self._save_export_state(export_state)

        log_event("export_recovery", "info", f"Created export state for {export_id}")
        return export_state

    def update_entity_progress(
        self,
        export_id: str,
        entity_type: str,
        processed_records: int,
        status: EntityExportStatus,
        error_message: Optional[str] = None
    ) -> None:
        """Update progress for specific entity"""
        if export_id not in self.current_exports:
            log_event("export_recovery", "warning", f"Export state not found: {export_id}")
            return

        export_state = self.current_exports[export_id]

        if entity_type in export_state.entity_progress:
            progress = export_state.entity_progress[entity_type]
            progress.processed_records = processed_records
            progress.status = status

            if error_message:
                progress.error_message = error_message

            if status == EntityExportStatus.IN_PROGRESS and not progress.start_time:
                progress.start_time = datetime.now()
            elif status in [EntityExportStatus.COMPLETED, EntityExportStatus.FAILED]:
                progress.end_time = datetime.now()

            self._save_export_state(export_state)

            log_event(
                "export_recovery",
                "info",
                f"Updated {entity_type} progress: {processed_records}/{progress.total_records} ({status.value})"
            )

    def mark_entity_completed(
        self,
        export_id: str,
        entity_type: str,
        spreadsheet_url: Optional[str] = None
    ) -> None:
        """Mark entity export as completed"""
        if export_id not in self.current_exports:
            return

        export_state = self.current_exports[export_id]

        if entity_type in export_state.entity_progress:
            progress = export_state.entity_progress[entity_type]
            progress.status = EntityExportStatus.COMPLETED
            progress.end_time = datetime.now()
            progress.processed_records = progress.total_records

            if spreadsheet_url:
                export_state.exported_urls[entity_type] = spreadsheet_url

            self._update_overall_status(export_state)
            self._save_export_state(export_state)

            log_event("export_recovery", "info", f"Marked {entity_type} as completed")

    def mark_entity_failed(
        self,
        export_id: str,
        entity_type: str,
        error_message: str
    ) -> None:
        """Mark entity export as failed"""
        if export_id not in self.current_exports:
            return

        export_state = self.current_exports[export_id]

        if entity_type in export_state.entity_progress:
            progress = export_state.entity_progress[entity_type]
            progress.status = EntityExportStatus.FAILED
            progress.end_time = datetime.now()
            progress.error_message = error_message

            if entity_type not in export_state.failed_entities:
                export_state.failed_entities.append(entity_type)

            if error_message not in export_state.error_messages:
                export_state.error_messages.append(error_message)

            self._update_overall_status(export_state)
            self._save_export_state(export_state)

            log_event("export_recovery", "error", f"Marked {entity_type} as failed: {error_message}")

    def get_resumable_entities(self, export_id: str) -> List[str]:
        """Get list of entities that can be resumed"""
        if export_id not in self.current_exports:
            return []

        export_state = self.current_exports[export_id]
        resumable_entities = []

        for entity_type, progress in export_state.entity_progress.items():
            if progress.status in [EntityExportStatus.PENDING, EntityExportStatus.FAILED]:
                resumable_entities.append(entity_type)

        return resumable_entities

    def create_partial_export_report(self, export_id: str) -> Dict[str, Any]:
        """Create detailed partial export report"""
        if export_id not in self.current_exports:
            return {}

        export_state = self.current_exports[export_id]

        completed_entities = {}
        failed_entities = {}
        pending_entities = {}

        for entity_type, progress in export_state.entity_progress.items():
            if progress.status == EntityExportStatus.COMPLETED:
                completed_entities[entity_type] = {
                    "records": progress.total_records,
                    "url": export_state.exported_urls.get(entity_type),
                    "duration": self._calculate_duration(progress)
                }
            elif progress.status == EntityExportStatus.FAILED:
                failed_entities[entity_type] = {
                    "records": progress.total_records,
                    "processed": progress.processed_records,
                    "error": progress.error_message
                }
            else:
                pending_entities[entity_type] = {
                    "records": progress.total_records,
                    "processed": progress.processed_records,
                    "status": progress.status.value
                }

        total_processed, total_records = export_state.get_total_progress()

        return {
            "export_id": export_id,
            "status": export_state.status.value,
            "completion_percentage": export_state.get_completion_percentage(),
            "total_records": total_records,
            "processed_records": total_processed,
            "completed_entities": completed_entities,
            "failed_entities": failed_entities,
            "pending_entities": pending_entities,
            "error_messages": export_state.error_messages,
            "start_time": export_state.start_time.isoformat(),
            "end_time": export_state.end_time.isoformat() if export_state.end_time else None,
            "is_resumable": export_state.resumable and len(self.get_resumable_entities(export_id)) > 0
        }

    def resume_export(self, export_id: str) -> Optional[ExportState]:
        """Resume a failed or partial export"""
        # Try to load from disk if not in memory
        if export_id not in self.current_exports:
            export_state = self._load_export_state(export_id)
            if export_state:
                self.current_exports[export_id] = export_state

        if export_id not in self.current_exports:
            log_event("export_recovery", "error", f"Cannot resume export {export_id}: state not found")
            return None

        export_state = self.current_exports[export_id]

        if not export_state.resumable:
            log_event("export_recovery", "error", f"Export {export_id} is not resumable")
            return None

        resumable_entities = self.get_resumable_entities(export_id)
        if not resumable_entities:
            log_event("export_recovery", "info", f"No entities to resume for export {export_id}")
            return export_state

        # Reset failed entities to pending
        for entity_type in resumable_entities:
            if entity_type in export_state.entity_progress:
                progress = export_state.entity_progress[entity_type]
                if progress.status == EntityExportStatus.FAILED:
                    progress.status = EntityExportStatus.PENDING
                    progress.error_message = None
                    progress.start_time = None
                    progress.end_time = None

        # Remove from failed entities list
        export_state.failed_entities = [
            e for e in export_state.failed_entities
            if e not in resumable_entities
        ]

        export_state.status = ExportStatus.IN_PROGRESS
        self._save_export_state(export_state)

        log_event("export_recovery", "info", f"Resumed export {export_id} with entities: {resumable_entities}")
        return export_state

    def handle_batch_failure(
        self,
        export_id: str,
        entity_type: str,
        batch_number: int,
        error: Exception,
        reduce_batch_size: bool = True
    ) -> bool:
        """Handle batch operation failure with recovery"""
        if export_id not in self.current_exports:
            return False

        export_state = self.current_exports[export_id]

        if entity_type not in export_state.entity_progress:
            return False

        progress = export_state.entity_progress[entity_type]

        # Reduce batch size if requested and possible
        if reduce_batch_size and progress.batch_size > 100:
            old_batch_size = progress.batch_size
            progress.batch_size = max(100, progress.batch_size // 2)
            progress.total_batches = (progress.total_records + progress.batch_size - 1) // progress.batch_size

            log_event(
                "export_recovery",
                "info",
                f"Reduced batch size for {entity_type} from {old_batch_size} to {progress.batch_size}"
            )

        # Log the failure
        error_msg = f"Batch {batch_number} failed: {str(error)}"
        progress.error_message = error_msg

        self._save_export_state(export_state)

        log_event("export_recovery", "warning", f"Batch failure handled for {entity_type}: {error_msg}")
        return True

    def cleanup_completed_exports(self, max_age_days: int = 7) -> None:
        """Clean up old completed export states"""
        cutoff_time = datetime.now() - timedelta(days=max_age_days)

        # Clean up in-memory exports
        to_remove = []
        for export_id, export_state in self.current_exports.items():
            if (export_state.status == ExportStatus.COMPLETED and
                export_state.end_time and
                export_state.end_time < cutoff_time):
                to_remove.append(export_id)

        for export_id in to_remove:
            del self.current_exports[export_id]
            log_event("export_recovery", "info", f"Cleaned up completed export {export_id}")

        # Clean up disk files
        for state_file in self.state_dir.glob("*.json"):
            if state_file.stat().st_mtime < cutoff_time.timestamp():
                try:
                    state_file.unlink()
                    log_event("export_recovery", "info", f"Deleted old state file {state_file.name}")
                except Exception as e:
                    log_event("export_recovery", "error", f"Failed to delete state file {state_file.name}: {e}")

    def _update_overall_status(self, export_state: ExportState) -> None:
        """Update overall export status based on entity statuses"""
        if export_state.is_complete():
            if export_state.has_failures():
                export_state.status = ExportStatus.PARTIAL
            else:
                export_state.status = ExportStatus.COMPLETED
            export_state.end_time = datetime.now()
        elif export_state.has_failures():
            # Check if all entities are either completed or failed
            all_done = all(
                p.status in [EntityExportStatus.COMPLETED, EntityExportStatus.FAILED, EntityExportStatus.SKIPPED]
                for p in export_state.entity_progress.values()
            )
            if all_done:
                export_state.status = ExportStatus.PARTIAL
                export_state.end_time = datetime.now()

    def _calculate_duration(self, progress: BatchProgress) -> Optional[str]:
        """Calculate duration for batch progress"""
        if progress.start_time and progress.end_time:
            duration = progress.end_time - progress.start_time
            return str(duration)
        return None

    def _save_export_state(self, export_state: ExportState) -> None:
        """Save export state to disk"""
        try:
            state_file = self.state_dir / f"{export_state.export_id}.json"

            # Convert to dict and handle datetime serialization
            state_dict = asdict(export_state)
            state_dict['start_time'] = export_state.start_time.isoformat()
            if export_state.end_time:
                state_dict['end_time'] = export_state.end_time.isoformat()

            # Handle datetime fields in entity progress
            for entity_type, progress_dict in state_dict['entity_progress'].items():
                progress = export_state.entity_progress[entity_type]
                if progress.start_time:
                    progress_dict['start_time'] = progress.start_time.isoformat()
                if progress.end_time:
                    progress_dict['end_time'] = progress.end_time.isoformat()

            with open(state_file, 'w') as f:
                json.dump(state_dict, f, indent=2)

        except Exception as e:
            log_event("export_recovery", "error", f"Failed to save export state {export_state.export_id}: {e}")

    def _load_export_state(self, export_id: str) -> Optional[ExportState]:
        """Load export state from disk"""
        try:
            state_file = self.state_dir / f"{export_id}.json"

            if not state_file.exists():
                return None

            with open(state_file, 'r') as f:
                state_dict = json.load(f)

            # Convert datetime strings back to datetime objects
            state_dict['start_time'] = datetime.fromisoformat(state_dict['start_time'])
            if state_dict.get('end_time'):
                state_dict['end_time'] = datetime.fromisoformat(state_dict['end_time'])

            # Handle entity progress datetime fields
            entity_progress = {}
            for entity_type, progress_dict in state_dict['entity_progress'].items():
                if progress_dict.get('start_time'):
                    progress_dict['start_time'] = datetime.fromisoformat(progress_dict['start_time'])
                if progress_dict.get('end_time'):
                    progress_dict['end_time'] = datetime.fromisoformat(progress_dict['end_time'])

                entity_progress[entity_type] = BatchProgress(**progress_dict)

            state_dict['entity_progress'] = entity_progress

            # Convert enum strings back to enums
            state_dict['status'] = ExportStatus(state_dict['status'])

            return ExportState(**state_dict)

        except Exception as e:
            log_event("export_recovery", "error", f"Failed to load export state {export_id}: {e}")
            return None


# Global recovery manager instance
recovery_manager = ExportRecoveryManager()


def create_export_recovery_context(
    export_id: str,
    operation_type: str,
    entity_counts: Dict[str, int],
    configuration: Dict[str, Any]
) -> ExportState:
    """Convenience function to create export recovery context"""
    return recovery_manager.create_export_state(export_id, operation_type, entity_counts, configuration)


def handle_partial_export_error(
    export_id: str,
    completed_entities: Dict[str, int],
    failed_entities: List[str],
    error_messages: List[str]
) -> GoogleSheetsPartialExportError:
    """Create and handle partial export error"""
    context = create_error_context(
        component="export_recovery",
        operation="partial_export_handling",
        additional_data={
            "export_id": export_id,
            "completed_count": len(completed_entities),
            "failed_count": len(failed_entities)
        }
    )

    error_message = f"Export {export_id} completed partially. {len(completed_entities)} entities succeeded, {len(failed_entities)} failed."

    return GoogleSheetsPartialExportError(
        message=error_message,
        exported_entities=completed_entities,
        failed_entities=failed_entities,
        context=context
    )