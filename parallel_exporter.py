"""
Parallel exporter for AmoCRM data
"""
import threading
import time
from typing import Dict, List, Any, Callable, Optional
from datetime import datetime

import config
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
        self.state_manager = StateManager()
        self.threads = {}
        self.stop_flags = {}

    def export_deals(self, force_restart: bool = False, batch_save: bool = True, batch_size: int = 10):
        """Export deals in a separate thread"""
        self._start_export_thread('leads', self._export_deals_worker, force_restart, batch_save, batch_size)

    def export_contacts(self, force_restart: bool = False, batch_save: bool = True, batch_size: int = 10):
        """Export contacts in a separate thread"""
        self._start_export_thread('contacts', self._export_contacts_worker, force_restart, batch_save, batch_size)

    def export_companies(self, force_restart: bool = False, batch_save: bool = True, batch_size: int = 10):
        """Export companies in a separate thread"""
        self._start_export_thread('companies', self._export_companies_worker, force_restart, batch_save, batch_size)

    def export_events(self, force_restart: bool = False, batch_save: bool = True, batch_size: int = 10):
        """Export events in a separate thread"""
        self._start_export_thread('events', self._export_events_worker, force_restart, batch_save, batch_size)

    def export_all(self, force_restart: bool = False, batch_save: bool = True, batch_size: int = 10):
        """Export all entity types in parallel"""
        self.export_deals(force_restart, batch_save, batch_size)
        self.export_contacts(force_restart, batch_save, batch_size)
        self.export_companies(force_restart, batch_save, batch_size)
        self.export_events(force_restart, batch_save, batch_size)

    def _start_export_thread(self, entity_type: str, worker_func: Callable,
                            force_restart: bool, batch_save: bool, batch_size: int):
        """Start a new export thread if one is not already running"""
        # Check if export is already running
        if self.state_manager.is_export_running(entity_type):
            log_event('exporter', 'warning', f'{entity_type} export is already running')
            return

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
            name=f"{entity_type}_export_thread"
        )
        thread.daemon = True
        thread.start()

        # Store the thread reference
        self.threads[entity_type] = thread

        log_event('exporter', 'info', f'Started {entity_type} export thread')

    def _export_deals_worker(self, batch_save: bool = True, batch_size: int = 10):
        """Worker function for exporting deals"""
        try:
            self._export_entities_worker('leads', self.api.get_deals_page, batch_save, batch_size)
        except Exception as e:
            log_event('exporter', 'error', f'Error in deals export worker: {e}')
        finally:
            self.state_manager.mark_export_stopped('leads')

    def _export_contacts_worker(self, batch_save: bool = True, batch_size: int = 10):
        """Worker function for exporting contacts"""
        try:
            self._export_entities_worker('contacts', self.api.get_contacts_page, batch_save, batch_size)
        except Exception as e:
            log_event('exporter', 'error', f'Error in contacts export worker: {e}')
        finally:
            self.state_manager.mark_export_stopped('contacts')

    def _export_companies_worker(self, batch_save: bool = True, batch_size: int = 10):
        """Worker function for exporting companies"""
        try:
            self._export_entities_worker('companies', self.api.get_companies_page, batch_save, batch_size)
        except Exception as e:
            log_event('exporter', 'error', f'Error in companies export worker: {e}')
        finally:
            self.state_manager.mark_export_stopped('companies')

    def _export_events_worker(self, batch_save: bool = True, batch_size: int = 10):
        """Worker function for exporting events"""
        try:
            self._export_entities_worker('events', self.api.get_events_page, batch_save, batch_size)
        except Exception as e:
            log_event('exporter', 'error', f'Error in events export worker: {e}')
        finally:
            self.state_manager.mark_export_stopped('events')

    def _export_entities_worker(self, entity_type: str, page_getter: Callable,
                               batch_save: bool = True, batch_size: int = 10):
        """Generic worker function for exporting entities"""
        # Get the last processed page from state
        start_page = self.state_manager.get_last_page(entity_type)
        current_page = start_page + 1 if start_page > 0 else 1

        # Prepare storage
        all_entities = self.storage.get_entities(entity_type) if batch_save else []
        batch_count = 0

        log_event('exporter', 'info',
                 f'Starting {entity_type} export from page {current_page}')

        # Process pages until no more data or stop flag
        has_more = True
        while has_more and not self.stop_flags.get(entity_type, False):
            try:
                # Get entities for current page
                entities, has_more = page_getter(current_page)

                # Update state
                self.state_manager.update_export_progress(entity_type, current_page, not has_more)

                # If batch save is enabled, add to batch
                if batch_save:
                    all_entities.extend(entities)
                    batch_count += 1

                    # Save batch if reached batch size or no more data
                    if batch_count >= batch_size or not has_more:
                        self.storage.save_entities(entity_type, all_entities)
                        log_event('exporter', 'info',
                                 f'Saved {len(all_entities)} {entity_type} after processing {batch_count} pages')
                        batch_count = 0
                else:
                    # Otherwise, save directly
                    self.storage.save_entities(entity_type, entities)

                log_event('exporter', 'info',
                         f'Processed {entity_type} page {current_page} with {len(entities)} items')

                # Move to next page
                current_page += 1

            except Exception as e:
                log_event('exporter', 'error',
                         f'Error processing {entity_type} page {current_page}: {e}')

                # Wait a bit before retrying
                time.sleep(5)

        # Ensure final state is saved
        if has_more:
            log_event('exporter', 'info', f'{entity_type} export stopped at page {current_page}')
        else:
            log_event('exporter', 'info', f'{entity_type} export completed at page {current_page - 1}')
            self.state_manager.update_export_progress(entity_type, current_page - 1, True)

    def stop_export(self, entity_type: str):
        """Stop an export thread"""
        if entity_type in self.stop_flags:
            self.stop_flags[entity_type] = True
            log_event('exporter', 'info', f'Stopping {entity_type} export...')

    def stop_all_exports(self):
        """Stop all export threads"""
        for entity_type in self.stop_flags:
            self.stop_flags[entity_type] = True
        log_event('exporter', 'info', 'Stopping all exports...')

    def is_export_running(self, entity_type: str) -> bool:
        """Check if an export is running"""
        return self.state_manager.is_export_running(entity_type)

    def get_running_exports(self) -> List[str]:
        """Get a list of currently running exports"""
        return self.state_manager.get_running_exports()

    def get_export_status(self) -> Dict[str, Any]:
        """Get the status of all exports"""
        status = {}
        for entity_type in ['leads', 'contacts', 'companies', 'events']:
            status[entity_type] = {
                'running': self.state_manager.is_export_running(entity_type),
                'completed': self.state_manager.is_export_completed(entity_type),
                'last_page': self.state_manager.get_last_page(entity_type)
            }
        return status