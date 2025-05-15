"""
Worker system for handling export tasks
"""

import time
import threading
import queue
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime

from api import AmoCRMAPI
from storage import Storage
from state_manager import StateManager
from logger import log_event


class ExportTask:
    """Represents a task for exporting entities from AmoCRM"""

    def __init__(
        self,
        entity_type: str,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        force_restart: bool = False,
        priority: int = 1
    ):
        """Initialize an export task"""
        self.entity_type = entity_type
        self.batch_save = batch_save
        self.batch_size = batch_size
        self.date_from = date_from
        self.date_to = date_to
        self.force_restart = force_restart
        self.priority = priority
        self.created_at = datetime.now()
        self.status = "pending"
        self.task_id = f"{entity_type}_{int(time.time())}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary for storage"""
        return {
            "task_id": self.task_id,
            "entity_type": self.entity_type,
            "batch_save": self.batch_save,
            "batch_size": self.batch_size,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "force_restart": self.force_restart,
            "priority": self.priority,
            "created_at": self.created_at.isoformat(),
            "status": self.status
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExportTask':
        """Create task from dictionary"""
        task = cls(
            entity_type=data["entity_type"],
            batch_save=data["batch_save"],
            batch_size=data["batch_size"],
            date_from=data.get("date_from"),
            date_to=data.get("date_to"),
            force_restart=data["force_restart"],
            priority=data["priority"]
        )
        task.task_id = data["task_id"]
        task.created_at = datetime.fromisoformat(data["created_at"])
        task.status = data["status"]
        return task


class TaskQueue:
    """Queue for managing export tasks"""

    def __init__(self, state_manager: StateManager):
        """Initialize the task queue"""
        self.queue = queue.PriorityQueue()
        self.task_map = {}  # Maps task_id to task object
        self.state_manager = state_manager
        self.lock = threading.Lock()

        # Load tasks from database
        self._load_tasks_from_db()

    def _load_tasks_from_db(self):
        """Load pending tasks from database"""
        tasks = self.state_manager.get_pending_tasks()

        for task_data in tasks:
            task = ExportTask.from_dict(task_data)
            self.task_map[task.task_id] = task
            # Add to priority queue with priority and creation time as sort key
            self.queue.put((task.priority, task.created_at.timestamp(), task))

        log_event("worker", "info", f"Loaded {len(tasks)} pending tasks from database")

    def add_task(self, task: ExportTask) -> str:
        """Add a task to the queue"""
        with self.lock:
            # Store task in map
            self.task_map[task.task_id] = task

            # Add to priority queue with priority and creation time as sort key
            self.queue.put((task.priority, task.created_at.timestamp(), task))

            # Persist to database
            self.state_manager.save_task(task.to_dict())

            log_event("worker", "info", f"Added task {task.task_id} to queue")

            return task.task_id

    def get_task(self) -> Optional[ExportTask]:
        """Get the next task from the queue"""
        try:
            # Get task from queue
            _, _, task = self.queue.get(block=False)

            # Update task status
            task.status = "processing"
            self.state_manager.update_task_status(task.task_id, "processing")

            return task
        except queue.Empty:
            return None

    def complete_task(self, task_id: str, success: bool = True):
        """Mark a task as completed"""
        with self.lock:
            if task_id in self.task_map:
                task = self.task_map[task_id]
                task.status = "completed" if success else "failed"

                # Update in database
                self.state_manager.update_task_status(task_id, task.status)

                # Remove from task map
                del self.task_map[task_id]

                log_event("worker", "info", f"Marked task {task_id} as {task.status}")

    def cancel_task(self, task_id: str):
        """Cancel a pending task"""
        with self.lock:
            if task_id in self.task_map:
                task = self.task_map[task_id]
                task.status = "cancelled"

                # Update in database
                self.state_manager.update_task_status(task_id, "cancelled")

                # Note: Task will still be in priority queue but will be ignored when retrieved
                # as it's marked cancelled in the task map

                log_event("worker", "info", f"Cancelled task {task_id}")

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks including completed ones from database"""
        return self.state_manager.get_all_tasks()

    def get_task_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific task by ID"""
        return self.state_manager.get_task_by_id(task_id)


class Worker:
    """Worker for processing export tasks"""

    def __init__(self, worker_id: str, task_queue: TaskQueue, max_retries: int = 3):
        """Initialize a worker"""
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.api = AmoCRMAPI()
        self.storage = Storage()
        self.state_manager = task_queue.state_manager
        self.max_retries = max_retries
        self.current_task = None
        self.stop_requested = False
        self.thread = None

    def start(self):
        """Start the worker in a new thread"""
        if self.thread is None or not self.thread.is_alive():
            self.stop_requested = False
            self.thread = threading.Thread(target=self._worker_loop, name=f"worker_{self.worker_id}")
            self.thread.daemon = True
            self.thread.start()
            log_event("worker", "info", f"Started worker {self.worker_id}")

    def stop(self):
        """Request the worker to stop"""
        self.stop_requested = True
        log_event("worker", "info", f"Requested stop for worker {self.worker_id}")

    def is_running(self) -> bool:
        """Check if the worker is running"""
        return self.thread is not None and self.thread.is_alive()

    def _worker_loop(self):
        """Main worker loop"""
        log_event("worker", "info", f"Worker {self.worker_id} started")

        while not self.stop_requested:
            # Get next task
            task = self.task_queue.get_task()

            if task is None:
                # No tasks available, sleep and try again
                time.sleep(1)
                continue

            # Skip cancelled tasks
            if task.status == "cancelled":
                self.task_queue.queue.task_done()
                continue

            self.current_task = task
            log_event("worker", "info", f"Worker {self.worker_id} processing task {task.task_id} ({task.entity_type})")

            try:
                # Process the task
                success = self._process_task(task)

                # Mark task as complete
                self.task_queue.complete_task(task.task_id, success)

            except Exception as e:
                log_event("worker", "error", f"Error processing task {task.task_id}: {e}")
                self.task_queue.complete_task(task.task_id, False)

            # Mark task as done in queue
            self.task_queue.queue.task_done()
            self.current_task = None

        log_event("worker", "info", f"Worker {self.worker_id} stopped")

    def _process_task(self, task: ExportTask) -> bool:
        """Process an export task"""
        entity_type = task.entity_type

        # Reset export state if forced restart
        if task.force_restart:
            self.state_manager.reset_export_state(entity_type)

        # Process export based on entity type
        entity_processors = {
            "leads": self._export_deals,
            "contacts": self._export_contacts,
            "companies": self._export_companies,
            "events": self._export_events
        }

        if entity_type in entity_processors:
            return entity_processors[entity_type](
                task.batch_save,
                task.batch_size,
                task.date_from,
                task.date_to
            )
        else:
            log_event("worker", "error", f"Unknown entity type: {entity_type}")
            return False

    def _export_deals(self, batch_save: bool, batch_size: int, date_from: Optional[str], date_to: Optional[str]) -> bool:
        """Export deals"""
        return self._export_entities(
            "leads",
            self.api.get_deals_page,
            batch_save,
            batch_size,
            date_from,
            date_to
        )

    def _export_contacts(self, batch_save: bool, batch_size: int, date_from: Optional[str], date_to: Optional[str]) -> bool:
        """Export contacts"""
        return self._export_entities(
            "contacts",
            self.api.get_contacts_page,
            batch_save,
            batch_size,
            date_from,
            date_to
        )

    def _export_companies(self, batch_save: bool, batch_size: int, date_from: Optional[str], date_to: Optional[str]) -> bool:
        """Export companies"""
        return self._export_entities(
            "companies",
            self.api.get_companies_page,
            batch_save,
            batch_size,
            date_from,
            date_to
        )

    def _export_events(self, batch_save: bool, batch_size: int, date_from: Optional[str], date_to: Optional[str]) -> bool:
        """Export events"""
        return self._export_entities(
            "events",
            self.api.get_events_page,
            batch_save,
            batch_size,
            date_from,
            date_to
        )

    def _export_entities(
        self,
        entity_type: str,
        page_getter: Callable,
        batch_save: bool = True,
        batch_size: int = 10,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> bool:
        """Worker function for exporting entities of any type"""
        log_event("worker", "info", f"Starting {entity_type} export")

        # Get the last exported page from state
        start_page = self.state_manager.get_last_page(entity_type) + 1

        current_page = start_page
        batch = []
        retry_count = 0

        try:
            while not self.stop_requested:
                try:
                    # Get a page of entities
                    entities, has_more = page_getter(current_page, date_from, date_to)

                    # Reset retry counter on successful fetch
                    retry_count = 0

                    # Add to batch
                    batch.extend(entities)

                    # Save batch if it's full or if we should save on each page
                    if len(batch) >= batch_size or not batch_save:
                        if batch:
                            self.storage.save_entities(entity_type, batch)
                            batch = []

                    # Update progress in state
                    self.state_manager.update_export_progress(
                        entity_type, current_page, not has_more
                    )

                    # Stop if no more pages
                    if not has_more:
                        break

                    # Move to next page
                    current_page += 1

                except Exception as e:
                    log_event(
                        "worker", "error",
                        f"Error exporting {entity_type} page {current_page}: {e}"
                    )

                    # Increment retry counter
                    retry_count += 1

                    # If max retries reached, abort export
                    if retry_count > self.max_retries:
                        log_event(
                            "worker", "error",
                            f"Max retries reached for {entity_type} export, aborting"
                        )
                        return False

                    # Wait before retrying
                    time.sleep(5)

            # Save any remaining entities in batch
            if batch and not self.stop_requested:
                self.storage.save_entities(entity_type, batch)

            if self.stop_requested:
                log_event("worker", "info", f"{entity_type} export stopped by request")
                return False

            log_event("worker", "info", f"Completed {entity_type} export")
            return True

        except Exception as e:
            log_event("worker", "error", f"Error in {entity_type} export: {e}")
            return False


class WorkerPool:
    """Pool of workers for processing export tasks"""

    def __init__(self, state_manager: StateManager, num_workers: int = 4):
        """Initialize the worker pool"""
        self.state_manager = state_manager
        self.task_queue = TaskQueue(state_manager)
        self.num_workers = num_workers
        self.workers = {}
        self.started = False

    def start(self):
        """Start all workers in the pool"""
        if not self.started:
            for i in range(self.num_workers):
                worker_id = f"worker_{i+1}"
                worker = Worker(worker_id, self.task_queue)
                self.workers[worker_id] = worker
                worker.start()

            self.started = True
            log_event("worker", "info", f"Started worker pool with {self.num_workers} workers")

    def stop(self):
        """Stop all workers in the pool"""
        if self.started:
            for worker in self.workers.values():
                worker.stop()

            self.started = False
            log_event("worker", "info", "Stopped worker pool")

    def add_task(self, task: ExportTask) -> str:
        """Add a task to the queue"""
        return self.task_queue.add_task(task)

    def cancel_task(self, task_id: str):
        """Cancel a pending task"""
        self.task_queue.cancel_task(task_id)

    def get_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks"""
        return self.task_queue.get_all_tasks()

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific task by ID"""
        return self.task_queue.get_task_by_id(task_id)

    def get_worker_status(self) -> List[Dict[str, Any]]:
        """Get status of all workers"""
        status = []

        for worker_id, worker in self.workers.items():
            worker_status = {
                "worker_id": worker_id,
                "is_running": worker.is_running(),
                "current_task": None
            }

            if worker.current_task:
                worker_status["current_task"] = {
                    "task_id": worker.current_task.task_id,
                    "entity_type": worker.current_task.entity_type,
                    "status": worker.current_task.status
                }

            status.append(worker_status)

        return status