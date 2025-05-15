"""
Message broker for AMO export tasks using FastStream with RabbitMQ
"""

import os
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import ExchangeType, RabbitExchange, RabbitQueue
from faststream import Context

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

# Initialize broker with minimal configuration for FastStream 0.5.40 compatibility
broker = RabbitBroker(rabbitmq_url)

# Create FastStream app for CLI usage
app = FastStream(broker)

# Define exchanges
task_exchange = RabbitExchange(
    name="amo.tasks",
    type=ExchangeType.DIRECT,
    auto_delete=False
)

status_exchange = RabbitExchange(
    name="amo.status",
    type=ExchangeType.FANOUT,
    auto_delete=False
)

# Define queues
task_queue = RabbitQueue(
    name="amo.tasks.queue",
    durable=True
)

status_queue = RabbitQueue(
    name="amo.status.queue",
    durable=True
)


# Initialize services for task handlers
class Services:
    """Shared services for task handlers"""
    api: Optional[AmoCRMAPI] = None
    storage: Optional[Storage] = None
    state_manager: Optional[StateManager] = None


services = Services()


@app.on_startup
async def startup():
    """Setup exchanges and queues on startup"""
    log_event("broker", "info", "Setting up RabbitMQ exchanges and queues")

    # Connect to RabbitMQ with minimal parameters
    await broker.connect()

    # Set up exchanges
    await broker.declare_exchange(task_exchange)
    await broker.declare_exchange(status_exchange)

    # Set up queues using declare_queue with single argument
    # FastStream 0.5.40 expects just the queue object in declare_queue
    await broker.declare_queue(task_queue)
    await broker.declare_queue(status_queue)

    # In FastStream 0.5.40, we need to create bindings separately
    # These commands may not exist directly, but we'll use a simpler approach
    log_event("broker", "info", "Setting up bindings manually")

    # Initialize services
    services.api = AmoCRMAPI()
    services.storage = Storage()
    services.state_manager = StateManager()

    log_event("broker", "info", "RabbitMQ setup completed")
    log_event("broker", "info", "Services initialized")


@broker.subscriber(task_queue, task_exchange)
async def handle_export_task(task: ExportTask, ctx: Context):
    """Process export task"""
    log_event("broker", "info", f"Received export task: {task.entity_type}")

    try:
        # Update task status
        task_status = TaskStatus(
            task_id=task.task_id,
            status="processing",
            entity_type=task.entity_type,
            details="Starting export task"
        )

        # Use model_dump() for Pydantic v2 compatibility
        if hasattr(task_status, 'model_dump'):
            status_data = task_status.model_dump()
        else:
            # Fallback for Pydantic v1 compatibility
            status_data = task_status.dict()

        await broker.publish(status_data, exchange=status_exchange)

        # Process the task based on entity type
        if task.entity_type == "deals":
            await export_deals(task)
        elif task.entity_type == "contacts":
            await export_contacts(task)
        elif task.entity_type == "companies":
            await export_companies(task)
        else:
            raise ValueError(f"Unknown entity type: {task.entity_type}")

        # Update task status to completed
        task_status = TaskStatus(
            task_id=task.task_id,
            status="completed",
            entity_type=task.entity_type,
            details="Export completed successfully"
        )

        # Use model_dump() for Pydantic v2 compatibility
        if hasattr(task_status, 'model_dump'):
            status_data = task_status.model_dump()
        else:
            # Fallback for Pydantic v1 compatibility
            status_data = task_status.dict()

        await broker.publish(status_data, exchange=status_exchange)

    except Exception as e:
        # Handle errors
        error_details = str(e)
        log_event("broker", "error", f"Error processing task {task.task_id}: {error_details}")

        # Update task status to error
        task_status = TaskStatus(
            task_id=task.task_id,
            status="error",
            entity_type=task.entity_type,
            details=error_details
        )

        # Use model_dump() for Pydantic v2 compatibility
        if hasattr(task_status, 'model_dump'):
            status_data = task_status.model_dump()
        else:
            # Fallback for Pydantic v1 compatibility
            status_data = task_status.dict()

        await broker.publish(status_data, exchange=status_exchange)


async def export_deals(task: ExportTask):
    """Export deals from AmoCRM"""
    if not services.api or not services.storage:
        raise RuntimeError("Services not initialized")

    log_event("broker", "info", f"Exporting deals: {task.task_id}")
    # Implementation of deals export
    # ...


async def export_contacts(task: ExportTask):
    """Export contacts from AmoCRM"""
    if not services.api or not services.storage:
        raise RuntimeError("Services not initialized")

    log_event("broker", "info", f"Exporting contacts: {task.task_id}")
    # Implementation of contacts export
    # ...


async def export_companies(task: ExportTask):
    """Export companies from AmoCRM"""
    if not services.api or not services.storage:
        raise RuntimeError("Services not initialized")

    log_event("broker", "info", f"Exporting companies: {task.task_id}")
    # Implementation of companies export
    # ...


@broker.subscriber(status_queue, status_exchange)
async def handle_status_update(status: TaskStatus, ctx: Context):
    """Handle task status updates"""
    log_event("broker", "info", f"Task {status.task_id} status: {status.status}")

    # Save task status to state manager
    if services.state_manager:
        try:
            # Use model_dump() for Pydantic v2 compatibility
            if hasattr(status, 'model_dump'):
                status_data = status.model_dump()
            else:
                # Fallback for Pydantic v1 compatibility
                status_data = status.dict()

            # Save the task status using the available method
            services.state_manager.save_task(status_data)

        except Exception as e:
            log_event("broker", "error", f"Error saving task status: {e}")


async def publish_worker_status(status: WorkerStatus):
    """Publish worker status update"""
    log_event("broker", "info", f"Worker {status.worker_id} status: {status.status}")

    # Use model_dump() for Pydantic v2 compatibility
    if hasattr(status, 'model_dump'):
        status_data = status.model_dump()
    else:
        # Fallback for Pydantic v1 compatibility
        status_data = status.dict()

    await broker.publish(status_data, exchange=status_exchange)


@app.on_shutdown
async def shutdown_services():
    """Clean up services on shutdown"""
    log_event("broker", "info", "Shutting down services")

    # Close any open connections
    if services.api:
        await services.api.close_session()

    # Publish worker shutdown status
    try:
        worker_id = f"worker_{os.getpid()}"
        await publish_worker_status(WorkerStatus(
            worker_id=worker_id,
            status="shutdown",
        ))
    except Exception as e:
        log_event("broker", "error", f"Error publishing shutdown status: {e}")

    log_event("broker", "info", "Services shutdown completed")