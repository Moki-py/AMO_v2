"""
Parallel exporter for AmoCRM data
"""

import threading
import time
from datetime import datetime
from typing import Any, Callable

from ..core.api import AmoCRMAPI
from ..storage.storage import Storage
from ..storage.state_manager import StateManager
from ..core.logger import log_event


class ParallelExporter:
    """Handles parallel data export from AmoCRM"""

    def __init__(self, max_workers: int = 4):
        """Initialize the parallel exporter"""
        self.max_workers = max_workers
        self.api = AmoCRMAPI()
        self.storage = Storage()
        from ..core import logger
        logger.init_storage(self.storage)
        self.state_manager = StateManager()
        self.threads: dict[str, threading.Thread] = {}
        self.stop_flags: dict[str, bool] = {}

        # Verify running exports from previous session
        self._validate_running_exports()

    def _validate_running_exports(self):
        """Validate that exports marked as running in state can actually be found"""
        valid_entity_types = ["leads", "contacts", "companies", "events", "users", "pipelines"]
        # Validate exports against known entity types
        self.state_manager.verify_running_exports(valid_entity_types)

        # Clean up any thread references as they aren't valid after restart
        self.threads = {}

        # Check recovery status and attempt recovery if needed
        recovery_status = self.state_manager.get_recovery_status()
        if not recovery_status["state_valid"]:
            log_event("exporter", "warning", "Invalid state detected, attempting recovery")
            if self.state_manager.emergency_recovery():
                log_event("exporter", "info", "Emergency recovery completed successfully")
            else:
                log_event("exporter", "error", "Emergency recovery failed")

        # Clean up stale exports based on heartbeat
        stale_exports = self.state_manager.cleanup_stale_exports(timeout_minutes=10)
        if stale_exports:
            log_event("exporter", "info", f"Cleaned up stale exports: {stale_exports}")

        # Synchronize memory state with MongoDB state
        self._synchronize_state_with_db()

    def _synchronize_state_with_db(self):
        """Synchronize in-memory state with MongoDB state"""
        try:
            # Get current state from MongoDB
            db_running_exports = self.state_manager.get_running_exports()

            # Check each running export in DB
            for entity_type in db_running_exports:
                # Check if we have a thread for this export
                if entity_type in self.threads:
                    thread = self.threads[entity_type]
                    if not thread.is_alive():
                        # Thread is dead but marked as running - clean up
                        log_event("exporter", "warning", f"Thread for {entity_type} is dead, cleaning up")
                        self.state_manager.mark_export_stopped(entity_type)
                        del self.threads[entity_type]
                else:
                    # No thread but marked as running - this is from previous session
                    log_event("exporter", "info", f"Export {entity_type} was running in previous session")

            # Clean up thread references for exports not in DB
            threads_to_remove = []
            for entity_type, thread in self.threads.items():
                if entity_type not in db_running_exports:
                    if thread.is_alive():
                        log_event("exporter", "warning", f"Thread {entity_type} is alive but not in DB, stopping")
                        self.stop_flags[entity_type] = True
                    threads_to_remove.append(entity_type)

            # Remove cleaned up threads
            for entity_type in threads_to_remove:
                del self.threads[entity_type]

        except Exception as e:
            log_event("exporter", "error", f"Error synchronizing state with DB: {e}")

    def export_deals(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Export deals in a separate thread"""
        self._start_export_thread(
            "leads",
            self._export_deals_worker,
            force_restart,
            batch_save,
            batch_size,
            date_from,
            date_to,
        )

    def export_contacts(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Export contacts in a separate thread"""
        self._start_export_thread(
            "contacts",
            self._export_contacts_worker,
            force_restart,
            batch_save,
            batch_size,
            date_from,
            date_to,
        )

    def export_companies(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Export companies in a separate thread"""
        self._start_export_thread(
            "companies",
            self._export_companies_worker,
            force_restart,
            batch_save,
            batch_size,
            date_from,
            date_to,
        )

    def export_events(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Export events in a separate thread"""
        self._start_export_thread(
            "events",
            self._export_events_worker,
            force_restart,
            batch_save,
            batch_size,
            date_from,
            date_to,
        )

    def export_users(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Export users in a separate thread"""
        self._start_export_thread(
            "users",
            self._export_users_worker,
            force_restart,
            batch_save,
            batch_size,
            date_from,
            date_to,
        )

    def export_pipelines(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Export pipelines in a separate thread"""
        self._start_export_thread(
            "pipelines",
            self._export_pipelines_worker,
            force_restart,
            batch_save,
            batch_size,
            date_from,
            date_to,
        )

    def export_all(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Export all entity types in parallel"""
        self.export_deals(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
            date_from=date_from,
            date_to=date_to,
        )
        self.export_contacts(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
            date_from=date_from,
            date_to=date_to,
        )
        self.export_companies(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
            date_from=date_from,
            date_to=date_to,
        )
        self.export_events(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
            date_from=date_from,
            date_to=date_to,
        )
        self.export_users(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
            date_from=date_from,
            date_to=date_to,
        )
        self.export_pipelines(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
            date_from=date_from,
            date_to=date_to,
        )

    def _start_export_thread(
        self,
        entity_type: str,
        worker_func: Callable,
        force_restart: bool,
        batch_save: bool,
        batch_size: int,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Start a new export thread if one is not already running"""
        # Check if export is already running - use direct MongoDB check
        if self.state_manager.is_export_running_in_db(entity_type):
            # Check if the thread actually exists
            if entity_type in self.threads and self.threads[entity_type].is_alive():
                log_event(
                    "exporter",
                    "warning",
                    f"{entity_type} export is already running",
                )
                return
            else:
                # Thread reference doesn't exist or thread is dead, but state says it's running
                # This can happen after server restart - fix the state
                log_event(
                    "exporter",
                    "warning",
                    f"{entity_type} marked as running but no thread exists - fixing state",
                )
                # Stop it in the state so we can restart it properly
                self.state_manager.mark_export_stopped(entity_type)

        # Reset export state if forced restart
        if force_restart:
            self.state_manager.reset_export_state(entity_type)

        # Initialize stop flag
        self.stop_flags[entity_type] = False

        # Mark export as running
        self.state_manager.mark_export_running(entity_type)

        # Start the export thread
        thread = threading.Thread(
            target=worker_func,
            args=(batch_save, batch_size, date_from, date_to),
            name=f"{entity_type}_export_thread",
        )
        thread.daemon = True
        thread.start()

        # Store the thread reference
        self.threads[entity_type] = thread

        log_event("exporter", "info", f"Started {entity_type} export thread")

    def _export_deals_worker(
        self, batch_save: bool = True, batch_size: int = 10, date_from: str | None = None, date_to: str | None = None
    ):
        """Worker function for exporting deals"""
        try:
            log_event("exporter", "warning", "Starting deals export (test)")
            # Send initial heartbeat
            self.state_manager.send_heartbeat("leads",
                thread_id=threading.current_thread().name,
                metadata={"batch_save": batch_save, "batch_size": batch_size}
            )

            self._export_entities_worker(
                "leads", self.api.get_deals_page, batch_save, batch_size, date_from, date_to
            )
        except Exception as e:
            log_event(
                "exporter", "error", f"Error in deals export worker: {e}"
            )
        finally:
            self.state_manager.mark_export_stopped("leads")
            # Clean up thread reference
            if "leads" in self.threads:
                del self.threads["leads"]

    def _export_contacts_worker(
        self, batch_save: bool = True, batch_size: int = 10, date_from: str | None = None, date_to: str | None = None
    ):
        """Worker function for exporting contacts"""
        try:
            # Send initial heartbeat
            self.state_manager.send_heartbeat("contacts",
                thread_id=threading.current_thread().name,
                metadata={"batch_save": batch_save, "batch_size": batch_size}
            )

            self._export_entities_worker(
                "contacts", self.api.get_contacts_page, batch_save, batch_size, date_from, date_to
            )
        except Exception as e:
            log_event(
                "exporter", "error", f"Error in contacts export worker: {e}"
            )
        finally:
            self.state_manager.mark_export_stopped("contacts")
            # Clean up thread reference
            if "contacts" in self.threads:
                del self.threads["contacts"]

    def _export_companies_worker(
        self, batch_save: bool = True, batch_size: int = 10, date_from: str | None = None, date_to: str | None = None
    ):
        """Worker function for exporting companies"""
        try:
            self._export_entities_worker(
                "companies",
                self.api.get_companies_page,
                batch_save,
                batch_size,
                date_from,
                date_to,
            )
        except Exception as e:
            log_event(
                "exporter", "error", f"Error in companies export worker: {e}"
            )
        finally:
            self.state_manager.mark_export_stopped("companies")
            # Clean up thread reference
            if "companies" in self.threads:
                del self.threads["companies"]

    def _export_events_worker(
        self, batch_save: bool = True, batch_size: int = 10, date_from: str | None = None, date_to: str | None = None
    ):
        """Worker function for exporting events"""
        try:
            self._export_entities_worker(
                "events", self.api.get_events_page, batch_save, batch_size, date_from, date_to
            )
        except Exception as e:
            log_event(
                "exporter", "error", f"Error in events export worker: {e}"
            )
        finally:
            self.state_manager.mark_export_stopped("events")
            # Clean up thread reference
            if "events" in self.threads:
                del self.threads["events"]

    def _export_users_worker(
        self, batch_save: bool = True, batch_size: int = 10, date_from: str | None = None, date_to: str | None = None
    ):
        """Worker function for exporting users"""
        try:
            self._export_entities_worker(
                "users", self.api.get_users_page, batch_save, batch_size, date_from, date_to
            )
        except Exception as e:
            log_event(
                "exporter", "error", f"Error in users export worker: {e}"
            )
        finally:
            self.state_manager.mark_export_stopped("users")
            # Clean up thread reference
            if "users" in self.threads:
                del self.threads["users"]

    def _export_pipelines_worker(
        self, batch_save: bool = True, batch_size: int = 10, date_from: str | None = None, date_to: str | None = None
    ):
        """Worker function for exporting pipelines"""
        try:
            self._export_entities_worker(
                "pipelines", self.api.get_pipelines_page, batch_save, batch_size, date_from, date_to
            )
        except Exception as e:
            log_event(
                "exporter", "error", f"Error in pipelines export worker: {e}"
            )
        finally:
            self.state_manager.mark_export_stopped("pipelines")
            # Clean up thread reference
            if "pipelines" in self.threads:
                del self.threads["pipelines"]

    def _export_entities_worker(
        self,
        entity_type: str,
        page_getter: Callable,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Generic worker function for exporting entities"""

        # Get the last processed page from state
        start_page = self.state_manager.get_last_page(entity_type)
        current_page = start_page + 1 if start_page > 0 else 1

        # Prepare storage
        all_entities = (
            self.storage.get_entities(entity_type) if batch_save else []
        )
        batch_count = 0
        pages_since_last_save = 0  # Track pages for more frequent state saves

        log_event(
            "exporter",
            "info",
            f"Starting {entity_type} export from page {current_page}",
        )

        # Process pages until no more data or stop flag
        has_more = True
        last_heartbeat_time = datetime.now()

        while has_more and not self.stop_flags.get(entity_type, False):
            try:
                # Send heartbeat every 2 minutes
                current_time = datetime.now()
                if (current_time - last_heartbeat_time).total_seconds() >= 120:  # 2 minutes
                    self.state_manager.send_heartbeat(entity_type,
                        thread_id=threading.current_thread().name,
                        metadata={
                            "current_page": current_page,
                            "batch_save": batch_save,
                            "batch_size": batch_size,
                            "entities_processed": len(all_entities) if batch_save else 0
                        }
                    )
                    last_heartbeat_time = current_time

                # Get entities for current page
                entities, has_more = page_getter(current_page, date_from, date_to)

                # Filter out already exported entities to prevent duplicates
                filtered_entities = self.state_manager.filter_already_exported(
                    entity_type, entities
                )

                # Track entities that were actually processed
                current_page_exported_ids = []
                if filtered_entities:
                    entity_ids = [entity["id"] for entity in filtered_entities if "id" in entity]
                    current_page_exported_ids = entity_ids

                    # If batch save is enabled, add to batch
                    if batch_save:
                        all_entities.extend(filtered_entities)
                        batch_count += 1

                        # Save batch if reached batch size or no more data
                        if batch_count >= batch_size or not has_more:
                            # Create recovery point before critical operation
                            self.state_manager.create_recovery_point({
                                "operation": "batch_save",
                                "entity_type": entity_type,
                                "entities_count": len(all_entities),
                                "current_page": current_page
                            })

                            self.storage.append_entities(entity_type, all_entities)

                            # Mark entities as exported
                            if entity_ids:
                                all_entity_ids = [e["id"] for e in all_entities if "id" in e]
                                self.state_manager.add_exported_ids(entity_type, all_entity_ids)

                            log_event(
                                "exporter",
                                "info",
                                f"Saved {len(all_entities)} {entity_type} after "
                                f"processing {batch_count} pages",
                            )
                            all_entities = []  # Clear the batch
                            batch_count = 0
                    else:
                        # Otherwise, save directly
                        self.storage.append_entities(entity_type, filtered_entities)

                        # Mark entities as exported
                        if entity_ids:
                            self.state_manager.add_exported_ids(entity_type, entity_ids)

                # Update state every 2-3 pages for better resume capability
                pages_since_last_save += 1
                if pages_since_last_save >= 2 or not has_more:
                    # Use atomic update with exported IDs if available
                    self.state_manager.update_export_progress(
                        entity_type, current_page, not has_more, current_page_exported_ids
                    )
                    pages_since_last_save = 0

                log_event(
                    "exporter",
                    "info",
                    f"Processed {entity_type} page {current_page} with "
                    f"{len(entities)} items ({len(filtered_entities)} new)",
                )

                # Move to next page
                current_page += 1

            except Exception as e:
                log_event(
                    "exporter",
                    "error",
                    f"Error processing {entity_type} page {current_page}: {e}",
                )

                # Wait a bit before retrying
                time.sleep(5)

        # Ensure final state is saved
        if has_more:
            log_event(
                "exporter",
                "info",
                f"{entity_type} export stopped at page {current_page}",
            )
        else:
            log_event(
                "exporter",
                "info",
                f"{entity_type} export completed at page {current_page - 1}",
            )
            self.state_manager.update_export_progress(
                entity_type, current_page - 1, True
            )

    def stop_export(self, entity_type: str):
        """Stop an export thread"""
        if entity_type in self.stop_flags:
            self.stop_flags[entity_type] = True
            log_event("exporter", "info", f"Stopping {entity_type} export...")

    def stop_all_exports(self):
        """Stop all export threads"""
        for entity_type in self.stop_flags:
            self.stop_flags[entity_type] = True
        log_event("exporter", "info", "Stopping all exports...")

    def is_export_running(self, entity_type: str) -> bool:
        """Check if an export is running"""
        return self.state_manager.is_export_running(entity_type)

    def get_running_exports(self) -> list[str]:
        """Get a list of currently running exports"""
        return self.state_manager.get_running_exports()

    def get_export_status(self) -> dict[str, Any]:
        """Get the status of all exports"""
        status = {}
        for entity_type in ["leads", "contacts", "companies", "events", "users", "pipelines"]:
            status[entity_type] = {
                "running": self.state_manager.is_export_running(entity_type),
                "completed": self.state_manager.is_export_completed(
                    entity_type
                ),
                "last_page": self.state_manager.get_last_page(entity_type),
            }
        return status

    def _normalize_entity_type(self, entity_type: str) -> str:
        """Normalize entity type to internal representation"""
        if entity_type in ["deals", "leads"]:
            return "leads"
        return entity_type

    def restart_export(self, entity_type: str):
        """Force restart an export regardless of its current state"""
        entity_type = self._normalize_entity_type(entity_type)
        # First stop any running export
        if entity_type in self.stop_flags:
            self.stop_flags[entity_type] = True
            log_event("exporter", "info", f"Stopping {entity_type} export for restart...")

        # Clear the thread reference if it exists
        if entity_type in self.threads:
            del self.threads[entity_type]

        # Make sure it's marked as stopped in the state
        self.state_manager.mark_export_stopped(entity_type)

        # Now restart based on entity type
        export_methods = {
            "leads": self.export_deals,
            "contacts": self.export_contacts,
            "companies": self.export_companies,
            "events": self.export_events,
            "users": self.export_users,
            "pipelines": self.export_pipelines
        }

        if entity_type in export_methods:
            log_event("exporter", "info", f"Restarting {entity_type} export")
            export_methods[entity_type](force_restart=True)
        else:
            log_event("exporter", "error", f"Unknown entity type for restart: {entity_type}")

    def resume_export(self, entity_type: str):
        """Resume an export from the last saved page without resetting state"""
        entity_type = self._normalize_entity_type(entity_type)
        # First stop any running export (if any)
        if entity_type in self.stop_flags:
            self.stop_flags[entity_type] = True
            log_event("exporter", "info", f"Stopping {entity_type} export for resume...")

        # Clear the thread reference if it exists
        if entity_type in self.threads:
            del self.threads[entity_type]

        # Make sure it's marked as stopped in the state
        self.state_manager.mark_export_stopped(entity_type)

        # Now resume based on entity type (without force_restart)
        export_methods = {
            "leads": self.export_deals,
            "contacts": self.export_contacts,
            "companies": self.export_companies,
            "events": self.export_events,
            "users": self.export_users,
            "pipelines": self.export_pipelines
        }

        if entity_type in export_methods:
            log_event("exporter", "info", f"Resuming {entity_type} export")
            export_methods[entity_type](force_restart=False)
        else:
            log_event("exporter", "error", f"Unknown entity type for resume: {entity_type}")

    def create_manual_backup(self, description: str = "Manual backup") -> bool:
        """Create a manual backup of the current state"""
        return self.state_manager.create_backup("manual", {"description": description})

    def get_backup_list(self, backup_type: str | None = None, limit: int = 20):
        """Get list of available backups"""
        return self.state_manager.get_backup_list(backup_type, limit)

    def restore_from_backup(self, backup_id: str | None = None, backup_type: str | None = None) -> bool:
        """Restore state from a backup"""
        # Stop all running exports before restore
        self.stop_all_exports()

        # Wait for exports to stop
        import time
        time.sleep(2)

        # Perform restore
        success = self.state_manager.restore_from_backup(backup_id, backup_type)

        if success:
            log_event("exporter", "info", "State restored successfully, reinitializing")
            # Reinitialize after restore
            self._validate_running_exports()

        return success

    def get_recovery_status(self):
        """Get current recovery status"""
        return self.state_manager.get_recovery_status()

    def emergency_recovery(self) -> bool:
        """Perform emergency recovery"""
        # Stop all exports first
        self.stop_all_exports()

        # Perform emergency recovery
        success = self.state_manager.emergency_recovery()

        if success:
            log_event("exporter", "info", "Emergency recovery completed, reinitializing")
            # Reinitialize after recovery
            self._validate_running_exports()

        return success

    def validate_and_repair_state(self) -> bool:
        """Validate state and attempt repair if needed"""
        validation_result = self.state_manager.validate_state_integrity()

        if not validation_result["valid"]:
            log_event("exporter", "warning", f"State validation failed: {validation_result['errors']}")
            return self.state_manager.repair_state()
        elif validation_result["warnings"]:
            log_event("exporter", "info", f"State validation warnings: {validation_result['warnings']}")

        return True

    def get_heartbeat_status(self, entity_type: str | None = None):
        """Get heartbeat status for entity type or all entities"""
        if entity_type:
            return self.state_manager.get_heartbeat_status(entity_type)
        else:
            return self.state_manager.get_all_heartbeats()

    def check_stale_exports(self, timeout_minutes: int = 10):
        """Check for stale exports and return list"""
        return self.state_manager.check_stale_exports(timeout_minutes)

    def cleanup_stale_exports(self, timeout_minutes: int = 10):
        """Clean up stale exports"""
        return self.state_manager.cleanup_stale_exports(timeout_minutes)


def main():
    """Main entry point for parallel exporter CLI"""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description='AmoCRM Parallel Data Exporter')
    parser.add_argument('--entity', choices=['deals', 'contacts', 'companies', 'events', 'users', 'pipelines', 'all'],
                       default='all', help='Entity type to export')
    parser.add_argument('--force-restart', action='store_true', help='Force restart export')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for processing')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of worker threads')
    parser.add_argument('--date-from', help='Export data from this date (YYYY-MM-DD)')
    parser.add_argument('--date-to', help='Export data to this date (YYYY-MM-DD)')

    args = parser.parse_args()

    try:
        exporter = ParallelExporter(max_workers=args.max_workers)

        if args.entity == 'all':
            exporter.export_all(
                force_restart=args.force_restart,
                batch_size=args.batch_size,
                date_from=args.date_from,
                date_to=args.date_to
            )
        elif args.entity == 'deals':
            exporter.export_deals(
                force_restart=args.force_restart,
                batch_size=args.batch_size,
                date_from=args.date_from,
                date_to=args.date_to
            )
        elif args.entity == 'contacts':
            exporter.export_contacts(
                force_restart=args.force_restart,
                batch_size=args.batch_size,
                date_from=args.date_from,
                date_to=args.date_to
            )
        elif args.entity == 'companies':
            exporter.export_companies(
                force_restart=args.force_restart,
                batch_size=args.batch_size,
                date_from=args.date_from,
                date_to=args.date_to
            )
        elif args.entity == 'events':
            exporter.export_events(
                force_restart=args.force_restart,
                batch_size=args.batch_size,
                date_from=args.date_from,
                date_to=args.date_to
            )
        elif args.entity == 'users':
            exporter.export_users(
                force_restart=args.force_restart,
                batch_size=args.batch_size,
                date_from=args.date_from,
                date_to=args.date_to
            )
        elif args.entity == 'pipelines':
            exporter.export_pipelines(
                force_restart=args.force_restart,
                batch_size=args.batch_size,
                date_from=args.date_from,
                date_to=args.date_to
            )

        print(f"Экспорт {args.entity} завершен успешно!")

    except Exception as e:
        print(f"Ошибка при экспорте: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
