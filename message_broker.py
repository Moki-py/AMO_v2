"""
Message broker for AMO export tasks using FastStream with RabbitMQ
"""

import os
import asyncio
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

from faststream import FastStream, Logger
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, ExchangeType
from faststream import Context

import config
from api import AmoCRMAPI
from storage import Storage
from state_manager import StateManager
from logger import log_event


# Define message schemas for export tasks
class ExportTask(BaseModel):
    """Schema for export task messages"""
    entity_type: str
    batch_save: bool = True
    batch_size: int = 10
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    force_restart: bool = False
    priority: int = 1
    task_id: str = Field(default_factory=lambda: f"{datetime.now().timestamp()}")
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    status: str = "pending"


class TaskStatus(BaseModel):
    """Schema for task status updates"""
    task_id: str
    status: str
    entity_type: str
    current_page: Optional[int] = None
    details: Optional[str] = None
    updated_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class WorkerStatus(BaseModel):
    """Schema for worker status updates"""
    worker_id: str
    status: str = "idle"  # idle, busy, error
    current_task_id: Optional[str] = None
    updated_at: str = Field(default_factory=lambda: datetime.now().isoformat())


# Get RabbitMQ URL from environment variable or use default
rabbitmq_url = os.environ.get(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/"
)

# Get resource constraints from environment
max_retries = int(os.environ.get("MAX_RETRIES", "3"))
batch_buffer_size = int(os.environ.get("BATCH_BUFFER_SIZE", "10"))
retry_delay = int(os.environ.get("RETRY_DELAY", "5"))

# Initialize broker with optimization settings
broker = RabbitBroker(
    rabbitmq_url,
    # Reduce prefetch count to limit memory usage
    prefetch_count=5,
    # Add heartbeat to keep connection alive but not too frequent
    heartbeat=60,
)

# Create FastStream app for CLI usage
app = FastStream(broker)

# Define exchanges
task_exchange = RabbitExchange(
    "amo.tasks",
    type=ExchangeType.DIRECT,
    auto_delete=False
)

status_exchange = RabbitExchange(
    "amo.status",
    type=ExchangeType.FANOUT,
    auto_delete=False
)

# Define queues
task_queue = RabbitQueue(
    "amo.tasks.queue",
    exchange=task_exchange,
    routing_key="export_task",
    durable=True
)

status_queue = RabbitQueue(
    "amo.status.queue",
    exchange=status_exchange,
    durable=True
)


# Declare exchanges and queues
@broker.on_startup
async def startup():
    """Setup exchanges and queues on startup"""
    log_event("broker", "info", "Setting up RabbitMQ exchanges and queues")

    # Declare exchanges
    await broker.declare_exchange(task_exchange)
    await broker.declare_exchange(status_exchange)

    # Declare queues
    await broker.declare_queue(task_queue)
    await broker.declare_queue(status_queue)

    log_event("broker", "info", "RabbitMQ setup completed")


# Initialize services for task handlers
class Services:
    """Shared services for task handlers"""
    api: Optional[AmoCRMAPI] = None
    storage: Optional[Storage] = None
    state_manager: Optional[StateManager] = None


services = Services()


@broker.on_startup
async def init_services():
    """Initialize services on startup"""
    log_event("broker", "info", "Initializing services")
    services.api = AmoCRMAPI()
    services.storage = Storage()
    services.state_manager = StateManager()
    log_event("broker", "info", "Services initialized")


@broker.on_shutdown
async def shutdown_services():
    """Clean up services on shutdown"""
    log_event("broker", "info", "Shutting down services")

    # Close any open connections
    if services.api:
        await services.api.close_session()

    # Update worker status to show it's shutting down
    try:
        # Generate a worker ID based on current process
        worker_id = f"worker_{os.getpid()}"
        await publish_worker_status(WorkerStatus(
            worker_id=worker_id,
            status="shutdown"
        ))
    except Exception as e:
        log_event("broker", "error", f"Error updating worker status on shutdown: {e}")

    log_event("broker", "info", "Services shut down")


# Task handler for export tasks with resource optimization
@broker.subscriber(task_queue)
async def handle_export_task(
    task: ExportTask,
    logger: Logger,
    context: Context
) -> None:
    """Handle export task messages"""
    worker_id = f"worker_{id(context)}"
    logger.info(f"Worker {worker_id} received task {task.task_id}")

    # Update task status to processing
    await publish_status_update(TaskStatus(
        task_id=task.task_id,
        status="processing",
        entity_type=task.entity_type
    ))

    # Update worker status
    await publish_worker_status(WorkerStatus(
        worker_id=worker_id,
        status="busy",
        current_task_id=task.task_id
    ))

    # Reset export state if forced restart
    if task.force_restart and services.state_manager:
        services.state_manager.reset_export_state(task.entity_type)

    # Track progress
    start_page = 1
    if services.state_manager:
        start_page = services.state_manager.get_last_page(task.entity_type) + 1

    current_page = start_page
    batch = []
    retry_count = 0

    # Use environment-based retry count
    max_retry_count = max_retries

    try:
        # Get the appropriate page getter method for the entity type
        page_getter = None
        if services.api:
            if task.entity_type == "leads":
                page_getter = services.api.get_deals_page
            elif task.entity_type == "contacts":
                page_getter = services.api.get_contacts_page
            elif task.entity_type == "companies":
                page_getter = services.api.get_companies_page
            elif task.entity_type == "events":
                page_getter = services.api.get_events_page

        if not page_getter:
            raise ValueError(f"Unknown entity type: {task.entity_type}")

        # Start exporting data
        logger.info(f"Starting export of {task.entity_type} from page {current_page}")

        while True:
            try:
                # Get a page of entities
                entities, has_more = page_getter(current_page, task.date_from, task.date_to)

                # Reset retry counter on successful fetch
                retry_count = 0

                # Add to batch
                batch.extend(entities)

                # Save batch if it's full or if we should save on each page
                if len(batch) >= task.batch_size or not task.batch_save:
                    if batch and services.storage:
                        services.storage.save_entities(task.entity_type, batch)
                        batch = []

                # Update progress in state and publish status update
                if services.state_manager:
                    services.state_manager.update_export_progress(
                        task.entity_type, current_page, not has_more
                    )

                await publish_status_update(TaskStatus(
                    task_id=task.task_id,
                    status="processing",
                    entity_type=task.entity_type,
                    current_page=current_page
                ))

                # Stop if no more pages
                if not has_more:
                    break

                # Move to next page
                current_page += 1

            except Exception as e:
                # Log error and retry
                error_msg = f"Error exporting {task.entity_type} page {current_page}: {str(e)}"
                logger.error(error_msg)

                # Update status with error details
                await publish_status_update(TaskStatus(
                    task_id=task.task_id,
                    status="error",
                    entity_type=task.entity_type,
                    current_page=current_page,
                    details=error_msg
                ))

                # Increment retry counter
                retry_count += 1

                # If max retries reached, abort export
                if retry_count > max_retry_count:
                    raise ValueError(f"Max retries reached for {task.entity_type} export, aborting")

                # Wait before retrying - use configured delay
                await asyncio.sleep(retry_delay)

        # Save any remaining entities in batch
        if batch and services.storage:
            services.storage.save_entities(task.entity_type, batch)

        # Update task status to completed
        await publish_status_update(TaskStatus(
            task_id=task.task_id,
            status="completed",
            entity_type=task.entity_type
        ))

        logger.info(f"Completed export of {task.entity_type}")

    except Exception as e:
        error_msg = f"Error in {task.entity_type} export: {str(e)}"
        logger.error(error_msg)

        # Update task status to failed
        await publish_status_update(TaskStatus(
            task_id=task.task_id,
            status="failed",
            entity_type=task.entity_type,
            details=error_msg
        ))

    finally:
        # Update worker status to idle
        await publish_worker_status(WorkerStatus(
            worker_id=worker_id,
            status="idle"
        ))


# Status update handler
@broker.subscriber(status_queue)
async def handle_status_update(status: TaskStatus, logger: Logger) -> None:
    """Handle task status updates"""
    logger.info(f"Task {status.task_id} status update: {status.status}")

    # Save task status to database if needed
    if services.state_manager:
        services.state_manager.update_task_status(status.task_id, status.status)

    # Log the status update
    log_event("broker", "info", f"Task {status.task_id} ({status.entity_type}) status: {status.status}")


# Worker status handler
@broker.subscriber(status_queue)
async def handle_worker_status(status: WorkerStatus, logger: Logger) -> None:
    """Handle worker status updates"""
    logger.info(f"Worker {status.worker_id} status update: {status.status}")

    # Log the status update
    log_event("broker", "info", f"Worker {status.worker_id} status: {status.status}")


# Publisher methods
async def publish_export_task(task: ExportTask) -> None:
    """Publish an export task to the task exchange"""
    await broker.publish(
        message=task,
        exchange=task_exchange,
        routing_key="export_task"
    )
    log_event("broker", "info", f"Published export task {task.task_id} for {task.entity_type}")


async def publish_status_update(status: TaskStatus) -> None:
    """Publish a task status update to the status exchange"""
    await broker.publish(
        message=status,
        exchange=status_exchange
    )


async def publish_worker_status(status: WorkerStatus) -> None:
    """Publish a worker status update to the status exchange"""
    await broker.publish(
        message=status,
        exchange=status_exchange
    )


# Export task creator functions
async def create_export_task(
    entity_type: str,
    batch_save: bool = True,
    batch_size: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    force_restart: bool = False,
    priority: int = 1
) -> str:
    """Create and publish an export task"""
    task = ExportTask(
        entity_type=entity_type,
        batch_save=batch_save,
        batch_size=batch_size,
        date_from=date_from,
        date_to=date_to,
        force_restart=force_restart,
        priority=priority
    )

    await publish_export_task(task)

    # Save task to state manager
    if services.state_manager:
        try:
            # Use model_dump() for Pydantic v2 compatibility
            if hasattr(task, 'model_dump'):
                task_data = task.model_dump()
            else:
                # Fallback for Pydantic v1 compatibility
                task_data = task.dict()
            services.state_manager.save_task(task_data)
        except Exception as e:
            log_event("broker", "error", f"Error saving task: {e}")

    return task.task_id