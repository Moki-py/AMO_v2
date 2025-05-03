"""
Parallel exporter for AmoCRM data
"""

import threading
import time
from typing import Any, Callable

from api import AmoCRMAPI
from storage import Storage
from state_manager import StateManager
from logger import log_event


class ParallelExporter:
    """Handles parallel data export from AmoCRM"""

    def __init__(self, max_workers: int = 4):
        """Initialize the parallel exporter"""
        self.max_workers = max_workers
        self.api = AmoCRMAPI()
        self.storage = Storage()
        import logger
        logger.init_storage(self.storage)
        self.state_manager = StateManager()
        self.threads = {}
        self.stop_flags = {}

        # Verify running exports from previous session
        self._validate_running_exports()

    def _validate_running_exports(self):
        """Validate that exports marked as running in state can actually be found"""
        valid_entity_types = ["leads", "contacts", "companies", "events"]
        # Validate exports against known entity types
        self.state_manager.verify_running_exports(valid_entity_types)

        # Clean up any thread references as they aren't valid after restart
        self.threads = {}

    def export_deals(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
    ):
        """Export deals in a separate thread"""
        self._start_export_thread(
            "leads",
            self._export_deals_worker,
            force_restart,
            batch_save,
            batch_size,
        )

    def export_contacts(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
    ):
        """Export contacts in a separate thread"""
        self._start_export_thread(
            "contacts",
            self._export_contacts_worker,
            force_restart,
            batch_save,
            batch_size,
        )

    def export_companies(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
    ):
        """Export companies in a separate thread"""
        self._start_export_thread(
            "companies",
            self._export_companies_worker,
            force_restart,
            batch_save,
            batch_size,
        )

    def export_events(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
    ):
        """Export events in a separate thread"""
        self._start_export_thread(
            "events",
            self._export_events_worker,
            force_restart,
            batch_save,
            batch_size,
        )

    def export_all(
        self,
        force_restart: bool = False,
        batch_save: bool = True,
        batch_size: int = 10,
    ):
        """Export all entity types in parallel"""
        self.export_deals(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
        )
        self.export_contacts(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
        )
        self.export_companies(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
        )
        self.export_events(
            force_restart=force_restart,
            batch_save=batch_save,
            batch_size=batch_size,
        )

    def _start_export_thread(
        self,
        entity_type: str,
        worker_func: Callable,
        force_restart: bool,
        batch_save: bool,
        batch_size: int,
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
            args=(batch_save, batch_size),
            name=f"{entity_type}_export_thread",
        )
        thread.daemon = True
        thread.start()

        # Store the thread reference
        self.threads[entity_type] = thread

        log_event("exporter", "info", f"Started {entity_type} export thread")

    def _export_deals_worker(
        self, batch_save: bool = True, batch_size: int = 10
    ):
        """Worker function for exporting deals"""
        try:
            log_event("exporter", "warning", "Starting deals export (test)")
            self._export_entities_worker(
                "leads", self.api.get_deals_page, batch_save, batch_size
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
        self, batch_save: bool = True, batch_size: int = 10
    ):
        """Worker function for exporting contacts"""
        try:
            self._export_entities_worker(
                "contacts", self.api.get_contacts_page, batch_save, batch_size
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
        self, batch_save: bool = True, batch_size: int = 10
    ):
        """Worker function for exporting companies"""
        try:
            self._export_entities_worker(
                "companies",
                self.api.get_companies_page,
                batch_save,
                batch_size,
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
        self, batch_save: bool = True, batch_size: int = 10
    ):
        """Worker function for exporting events"""
        try:
            self._export_entities_worker(
                "events", self.api.get_events_page, batch_save, batch_size
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

    def _export_entities_worker(
        self,
        entity_type: str,
        page_getter: Callable,
        batch_save: bool = True,
        batch_size: int = 10,
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

        log_event(
            "exporter",
            "info",
            f"Starting {entity_type} export from page {current_page}",
        )

        # Process pages until no more data or stop flag
        has_more = True
        while has_more and not self.stop_flags.get(entity_type, False):
            try:
                # Get entities for current page
                entities, has_more = page_getter(current_page)

                # Update state
                self.state_manager.update_export_progress(
                    entity_type, current_page, not has_more
                )

                # If batch save is enabled, add to batch
                if batch_save:
                    all_entities.extend(entities)
                    batch_count += 1

                    # Save batch if reached batch size or no more data
                    if batch_count >= batch_size or not has_more:
                        self.storage.append_entities(entity_type, all_entities)
                        log_event(
                            "exporter",
                            "info",
                            f"Saved {len(all_entities)} {entity_type} after "
                            f"processing {batch_count} pages",
                        )
                        batch_count = 0
                else:
                    # Otherwise, save directly
                    self.storage.append_entities(entity_type, entities)

                log_event(
                    "exporter",
                    "info",
                    f"Processed {entity_type} page {current_page} with "
                    f"{len(entities)} items",
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
        for entity_type in ["leads", "contacts", "companies", "events"]:
            status[entity_type] = {
                "running": self.state_manager.is_export_running(entity_type),
                "completed": self.state_manager.is_export_completed(
                    entity_type
                ),
                "last_page": self.state_manager.get_last_page(entity_type),
            }
        return status

    def restart_export(self, entity_type: str):
        """Force restart an export regardless of its current state"""
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
            "events": self.export_events
        }

        if entity_type in export_methods:
            log_event("exporter", "info", f"Restarting {entity_type} export")
            export_methods[entity_type](force_restart=True)
        else:
            log_event("exporter", "error", f"Unknown entity type for restart: {entity_type}")
