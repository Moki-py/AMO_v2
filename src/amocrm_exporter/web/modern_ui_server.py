"""
Modern web interface for AmoCRM exporter using FastAPI
"""

import webbrowser
import hmac
import hashlib
import os
from enum import Enum
from typing import Callable, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient

from fastapi import FastAPI, HTTPException, Request, Header, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from ..core.logger import log_event
from ..storage.storage import Storage
from ..exporters.parallel_exporter import ParallelExporter
from ..exporters.excel_exporter import ExcelExporter
from ..exporters.sheets_exporter import SheetsExporter
from .export_settings import ExportSettingsManager, EntityType as ExportEntityType, ExportSettings, FieldInfo
from ..core import logger
from ..core import config
from ..storage.state_manager import StateManager
from ..workers.message_broker import create_export_task, broker
from ..processors.flattening_processor import get_flattening_processor


class ActionType(str, Enum):
    """Types of actions that can be performed"""

    STATS = "stats"
    LOGS = "logs"
    FETCH = "fetch"


class EntityType(str, Enum):
    """Types of entities that can be exported"""

    ALL = "all"
    DEALS = "deals"  # Keep as "deals" for external API
    CONTACTS = "contacts"
    COMPANIES = "companies"
    EVENTS = "events"
    USERS = "users"
    PIPELINES = "pipelines"

    @staticmethod
    def normalize(entity_type: str) -> str:
        """Normalize entity type to internal representation"""
        if entity_type in ["deals", "leads"]:
            return "leads"
        return entity_type

    def __str__(self) -> str:
        """Convert to string, normalizing deals/leads"""
        if self.value == "deals":
            return "leads"
        return self.value


# Create global instances
storage = Storage()
logger.init_storage(storage)
exporter = ParallelExporter()
excel_exporter = ExcelExporter(storage)
sheets_exporter = SheetsExporter(storage)
export_settings_manager = ExportSettingsManager(storage)
flattening_processor = get_flattening_processor(storage)

# Auto-continue exports that were still marked as running
def continue_running_exports():
    """Check for and continue any exports that were running when server was stopped"""
    running_exports = exporter.get_running_exports()
    if running_exports:
        log_event("server", "info", f"Found running exports from previous session: {running_exports}")
        for entity_type in running_exports:
            try:
                # Use the restart function which handles cleanup properly
                exporter.restart_export(entity_type)
            except Exception as e:
                log_event("server", "error", f"Error continuing export for {entity_type}: {e}")
    else:
        log_event("server", "info", "No running exports from previous session found")

# Try to continue any previously running exports
continue_running_exports()

# Create FastAPI app
app = FastAPI(
    title="AmoCRM Data Exporter",
    description="Modern web interface for AmoCRM data export",
    version="1.0.0",
)

# Add CORS middleware to handle preflight OPTIONS requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup templates
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# Initialize worker pool
worker_pool = None

@app.on_event("startup")
async def startup_event():
    """Initialize components on startup"""
    # No need to manually start the broker - it will be handled by FastStream
    log_event("api", "info", "Connected to RabbitMQ message broker")

    # Start flattening processor
    try:
        flattening_processor.start()
        log_event("api", "info", "Started flattening processor")
    except Exception as e:
        log_event("api", "error", f"Failed to start flattening processor: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    # Stop flattening processor
    try:
        flattening_processor.stop()
        log_event("api", "info", "Stopped flattening processor")
    except Exception as e:
        log_event("api", "error", f"Error stopping flattening processor: {e}")

    # Close the broker connection
    await broker.close()
    log_event("api", "info", "Closed connection to message broker")

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    # Check connections to critical services
    health_status = {
        "status": "healthy",
        "services": {}
    }

    # Check RabbitMQ connection
    try:
        # For FastStream 0.5.40, check if broker is connected
        # using the _connection attribute which is set when connected
        if hasattr(broker, "_connection") and broker._connection:
            health_status["services"]["rabbitmq"] = "connected"
        else:
            health_status["services"]["rabbitmq"] = "disconnected"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["rabbitmq"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"

    # Check MongoDB connection
    try:
        if storage.db is not None:
            health_status["services"]["mongodb"] = "connected"
        else:
            health_status["services"]["mongodb"] = "disconnected"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["mongodb"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"

    return health_status

@app.get("/", response_class=HTMLResponse)
async def get_root(request: Request):
    """Render the main UI"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/export-settings", response_class=HTMLResponse)
async def render_export_settings_page(request: Request):
    """Render the export settings UI"""
    return templates.TemplateResponse("export-settings.html", {"request": request})


@app.get("/stats")
async def stats() -> dict:
    """Return statistics"""
    return get_stats()


@app.get("/logs")
async def logs(entity: str | None = None, level: str | None = None) -> dict:
    """Return recent logs with optional filtering by entity type and log level"""
    try:
        logs = logger.get_recent_logs(count=30, entity=entity, level=level)
        return {"logs": logs}
    except Exception as e:
        log_event("server", "error", f"Error retrieving logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fetch/all")
async def fetch_all_handler(
    date_from: str = Query(None),
    date_to: str = Query(None)
) -> dict:
    await fetch_all(date_from, date_to)
    return {"success": True}


@app.post("/fetch/{entity}")
async def fetch_entity_handler(
    entity: EntityType,
    date_from: str = Query(None),
    date_to: str = Query(None)
) -> dict:
    if entity not in [
        EntityType.DEALS,
        EntityType.CONTACTS,
        EntityType.COMPANIES,
        EntityType.EVENTS,
        EntityType.USERS,
        EntityType.PIPELINES
    ]:
        raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity}")
    try:
        await fetch_entity(entity, date_from, date_to)
        return {"success": True}
    except Exception as e:
        log_event("server", "error", f"Error in fetch_entity_handler: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/state/clear-running")
async def clear_running_exports() -> dict:
    """Clear all running exports to allow server restart"""
    try:
        exporter.state_manager.clear_running_exports()
        log_event("server", "info", "Cleared all running exports")
        return {"success": True, "message": "All running exports cleared"}
    except Exception as e:
        log_event("server", "error", f"Error clearing running exports: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/state/reset")
async def reset_all_state() -> dict:
    """Reset all export state including running exports"""
    try:
        exporter.state_manager.reset_all_state()
        log_event("server", "info", "Reset all export state")
        return {"success": True, "message": "All export state has been reset"}
    except Exception as e:
        log_event("server", "error", f"Error resetting state: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/export-status")
async def export_status() -> dict:
    """Return the status of all exports"""
    return {"status": exporter.get_export_status()}


@app.post("/export/restart/{entity}")
async def restart_export_handler(entity: EntityType) -> dict:
    """Forcibly restart an export regardless of its current state"""
    try:
        if entity == EntityType.ALL:
            for e in [EntityType.DEALS, EntityType.CONTACTS, EntityType.COMPANIES, EntityType.EVENTS, EntityType.USERS, EntityType.PIPELINES]:
                exporter.restart_export(e.value)
            log_event("server", "info", "Restarting all exports")
            return {"success": True, "message": "All exports are being restarted"}
        else:
            exporter.restart_export(entity.value)
            log_event("server", "info", f"Restarting {entity.value} export")
            return {"success": True, "message": f"{entity.value} export is being restarted"}
    except Exception as e:
        log_event("server", "error", f"Error restarting export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/export/stop/{entity}")
async def stop_export_handler(entity: EntityType) -> dict:
    """Stop a running export"""
    try:
        if entity == EntityType.ALL:
            exporter.stop_all_exports()
            log_event("server", "info", "Stopping all exports")
            return {"success": True, "message": "All exports are being stopped"}
        else:
            exporter.stop_export(entity.value)
            log_event("server", "info", f"Stopping {entity.value} export")
            return {"success": True, "message": f"{entity.value} export is being stopped"}
    except Exception as e:
        log_event("server", "error", f"Error stopping export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/export/resume/{entity}")
async def resume_export_handler(entity: EntityType) -> dict:
    """Resume an export from the last saved page without resetting state"""
    try:
        if entity == EntityType.ALL:
            for e in [EntityType.DEALS, EntityType.CONTACTS, EntityType.COMPANIES, EntityType.EVENTS, EntityType.USERS, EntityType.PIPELINES]:
                exporter.resume_export(e.value)
            log_event("server", "info", "Resuming all exports")
            return {"success": True, "message": "All exports are being resumed"}
        else:
            exporter.resume_export(entity.value)
            log_event("server", "info", f"Resuming {entity.value} export")
            return {"success": True, "message": f"{entity.value} export is being resumed"}
    except Exception as e:
        log_event("server", "error", f"Error resuming export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/export/excel")
async def export_excel_handler(
    date_from: str = Query(None),
    date_to: str = Query(None)
):
    try:
        excel_file = excel_exporter.export_all_to_excel(
            date_from=date_from, date_to=date_to
        )
        log_event(
            "server", "info", f"Excel export generated: {excel_file}"
        )
        return FileResponse(
            path=excel_file,
            filename=os.path.basename(excel_file),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        log_event("server", "error", f"Error generating Excel export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/export/sheets")
async def export_sheets_handler(
    date_from: str = Query(None),
    date_to: str = Query(None)
):
    try:
        sheets_url = sheets_exporter.export_all_to_sheets(
            date_from=date_from, date_to=date_to
        )
        log_event(
            "server", "info", f"Google Sheets export generated: {sheets_url}"
        )
        return {"url": sheets_url}
    except Exception as e:
        log_event("server", "error", f"Error generating Google Sheets export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_stats() -> dict:
    """Get current statistics"""
    try:
        deals = len(storage.get_entities("leads"))
        contacts = len(storage.get_entities("contacts"))
        companies = len(storage.get_entities("companies"))
        events = len(storage.get_entities("events"))
        users = len(storage.get_entities("users"))
        pipelines = len(storage.get_entities("pipelines"))

        return {
            "deals": deals,
            "contacts": contacts,
            "companies": companies,
            "events": events,
            "users": users,
            "pipelines": pipelines,
        }
    except Exception as e:
        log_event("server", "error", f"Error getting stats: {e}")
        return {"deals": 0, "contacts": 0, "companies": 0, "events": 0, "users": 0, "pipelines": 0}


async def fetch_all(date_from=None, date_to=None):
    try:
        exporter.export_all(date_from=date_from, date_to=date_to)
        log_event("server", "info", "Started export of all data")
    except Exception as e:
        log_event("server", "error", f"Error starting export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def fetch_entity(entity: EntityType, date_from=None, date_to=None):
    try:
        export_methods: dict[EntityType, Callable] = {
            EntityType.DEALS: exporter.export_deals,
            EntityType.CONTACTS: exporter.export_contacts,
            EntityType.COMPANIES: exporter.export_companies,
            EntityType.EVENTS: exporter.export_events,
            EntityType.USERS: exporter.export_users,
            EntityType.PIPELINES: exporter.export_pipelines,
        }
        if entity in export_methods:
            export_methods[entity](date_from=date_from, date_to=date_to)
            log_event(
                "server", "info", f"Started export of {entity.value}"
            )
        else:
            log_event(
                "server", "error", f"Invalid entity type: {entity.value}"
            )
            raise HTTPException(
                status_code=400, detail=f"Invalid entity type: {entity.value}"
            )
    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error starting export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_mongo_webhook_collection():
    client = MongoClient(config.settings.mongodb_uri)
    db = client[config.settings.mongodb_db]
    return db["webhook_events"]


def verify_webhook_signature(signature: Optional[str], body: bytes) -> bool:
    if not hasattr(config.settings, 'webhook_secret') or not config.settings.webhook_secret or not signature:
        return False
    expected_signature = hmac.new(
        config.settings.webhook_secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected_signature, signature)

async def store_webhook_event(event_data: Dict[str, Any]) -> bool:
    try:
        event_data["_received_at"] = datetime.now().isoformat()
        collection = get_mongo_webhook_collection()
        collection.insert_one(event_data)
        return True
    except Exception as e:
        log_event("webhook", "error", f"Error storing webhook event in MongoDB: {e}")
        return False

@app.post("/webhook")
async def webhook_handler(request: Request, x_signature: Optional[str] = Header(None)):
    body = await request.body()
    if hasattr(config.settings, 'webhook_secret') and config.settings.webhook_secret:
        if not verify_webhook_signature(x_signature, body):
            log_event("webhook", "warning", "Invalid webhook signature")
            return JSONResponse(status_code=401, content={"error": "Invalid signature"})
    try:
        webhook_data = await request.json()
        event_type = webhook_data.get("event_type")
        log_event("webhook", "info", f"Received webhook: {event_type}", details=webhook_data)
        await store_webhook_event(webhook_data)
        # Handle all main entity types
        if event_type in ("update_lead", "add_lead", "delete_lead"):
            exporter.export_deals()
            log_event("webhook", "info", "Triggered deals export due to webhook")
        elif event_type in ("update_contact", "add_contact", "delete_contact"):
            exporter.export_contacts()
            log_event("webhook", "info", "Triggered contacts export due to webhook")
        elif event_type in ("update_company", "add_company", "delete_company"):
            exporter.export_companies()
            log_event("webhook", "info", "Triggered companies export due to webhook")
        elif event_type in ("update_event", "add_event", "delete_event"):
            exporter.export_events()
            log_event("webhook", "info", "Triggered events export due to webhook")
        # You can add more entity types here as needed
        return {"success": True, "event_type": event_type}
    except Exception as e:
        log_event("webhook", "error", f"Error processing webhook: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/webhooks")
async def get_webhook_events():
    try:
        collection = get_mongo_webhook_collection()
        events = list(collection.find({}, {"_id": 0}))
    except Exception as e:
        log_event("webhook", "error", f"Error reading webhook events from MongoDB: {e}")
        events = []
    return {"events": events, "count": len(events)}

# Worker API routes
@app.get("/api/tasks")
async def get_tasks():
    """Get all export tasks"""
    state_manager = StateManager()
    return {"tasks": state_manager.get_all_tasks()}

@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str):
    """Get details of a specific task"""
    state_manager = StateManager()
    task = state_manager.get_task_by_id(task_id)
    if task:
        return {"task": task}
    return {"error": "Task not found"}

@app.post("/api/tasks/export_deals")
async def create_deals_export_task(
    force_restart: bool = False,
    batch_save: bool = True,
    batch_size: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    priority: int = 1
):
    """Create a task to export deals"""
    task_id = await create_export_task(
        entity_type="leads",
        batch_save=batch_save,
        batch_size=batch_size,
        date_from=date_from,
        date_to=date_to,
        force_restart=force_restart,
        priority=priority
    )
    return {"task_id": task_id}

@app.post("/api/tasks/export_contacts")
async def create_contacts_export_task(
    force_restart: bool = False,
    batch_save: bool = True,
    batch_size: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    priority: int = 1
):
    """Create a task to export contacts"""
    task_id = await create_export_task(
        entity_type="contacts",
        batch_save=batch_save,
        batch_size=batch_size,
        date_from=date_from,
        date_to=date_to,
        force_restart=force_restart,
        priority=priority
    )
    return {"task_id": task_id}

@app.post("/api/tasks/export_companies")
async def create_companies_export_task(
    force_restart: bool = False,
    batch_save: bool = True,
    batch_size: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    priority: int = 1
):
    """Create a task to export companies"""
    task_id = await create_export_task(
        entity_type="companies",
        batch_save=batch_save,
        batch_size=batch_size,
        date_from=date_from,
        date_to=date_to,
        force_restart=force_restart,
        priority=priority
    )
    return {"task_id": task_id}

@app.post("/api/tasks/export_events")
async def create_events_export_task(
    force_restart: bool = False,
    batch_save: bool = True,
    batch_size: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    priority: int = 1
):
    """Create a task to export events"""
    task_id = await create_export_task(
        entity_type="events",
        batch_save=batch_save,
        batch_size=batch_size,
        date_from=date_from,
        date_to=date_to,
        force_restart=force_restart,
        priority=priority
    )
    return {"task_id": task_id}

@app.post("/api/tasks/export_all")
async def create_all_export_tasks(
    force_restart: bool = False,
    batch_save: bool = True,
    batch_size: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    priority: int = 1
):
    """Create tasks to export all entity types"""
    task_ids = []

    # Create a task for each entity type
    for entity_type in ["leads", "contacts", "companies", "events"]:
        task_id = await create_export_task(
            entity_type=entity_type,
            batch_save=batch_save,
            batch_size=batch_size,
            date_from=date_from,
            date_to=date_to,
            force_restart=force_restart,
            priority=priority
        )
        task_ids.append(task_id)

    return {"task_ids": task_ids}

@app.delete("/api/tasks/{task_id}")
async def cancel_task(task_id: str):
    """Cancel a pending task"""
    state_manager = StateManager()
    state_manager.update_task_status(task_id, "cancelled")
    return {"status": "Task cancelled"}

@app.get("/api/workers")
async def get_worker_status():
    """Get status of worker processes"""
    try:
        return {"status": "running", "workers": []}
    except Exception as e:
        log_event("server", "error", f"Error getting worker status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Export Settings API Endpoints

@app.get("/api/export-settings/fields/{entity_type}")
async def get_available_fields(entity_type: str, force_refresh: bool = False):
    """Get available fields for an entity type"""
    try:
        # Convert to ExportEntityType - support all entity types
        entity_type_map = {
            "deals": ExportEntityType.DEALS,
            "leads": ExportEntityType.DEALS,  # Alias for deals
            "contacts": ExportEntityType.CONTACTS,
            "companies": ExportEntityType.COMPANIES,
            "events": ExportEntityType.EVENTS,
            "users": ExportEntityType.USERS,
            "pipelines": ExportEntityType.PIPELINES
        }

        export_entity_type = entity_type_map.get(entity_type.lower())
        if not export_entity_type:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        fields = await export_settings_manager.get_available_fields(export_entity_type, force_refresh)
        return {
            "entity_type": entity_type,
            "fields": [field.to_dict() for field in fields],
            "total_count": len(fields)
        }
    except Exception as e:
        log_event("export_settings", "error", f"Error getting fields for {entity_type}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export-settings/preview/{entity_type}/{field_name}")
async def get_field_preview(entity_type: str, field_name: str, limit: int = 10):
    """Get preview data for a specific field"""
    try:
        # Convert to ExportEntityType - support all entity types
        entity_type_map = {
            "deals": ExportEntityType.DEALS,
            "leads": ExportEntityType.DEALS,  # Alias for deals
            "contacts": ExportEntityType.CONTACTS,
            "companies": ExportEntityType.COMPANIES,
            "events": ExportEntityType.EVENTS,
            "users": ExportEntityType.USERS,
            "pipelines": ExportEntityType.PIPELINES
        }

        export_entity_type = entity_type_map.get(entity_type.lower())
        if not export_entity_type:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        preview_data = await export_settings_manager.get_field_preview_data(
            export_entity_type, field_name, limit
        )

        return {
            "entity_type": entity_type,
            "field_name": field_name,
            "preview_data": preview_data
        }
    except Exception as e:
        log_event("export_settings", "error", f"Error getting preview for {field_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/export-settings")
async def save_export_settings(settings_data: dict):
    """Save export settings"""
    try:
        # Validate required fields
        required_fields = ['entity_type', 'selected_fields', 'field_order', 'name']
        for field in required_fields:
            if field not in settings_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

        # Create ExportSettings object
        export_settings = ExportSettings(
            entity_type=settings_data['entity_type'],
            selected_fields=settings_data['selected_fields'],
            field_order=settings_data['field_order'],
            filters=settings_data.get('filters', {}),
            name=settings_data['name'],
            description=settings_data.get('description')
        )

        settings_id = await export_settings_manager.save_export_settings(export_settings)

        return {
            "success": True,
            "settings_id": settings_id,
            "message": "Export settings saved successfully"
        }
    except Exception as e:
        log_event("export_settings", "error", f"Error saving export settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export-settings")
async def list_export_settings(entity_type: Optional[str] = None):
    """List all saved export settings"""
    try:
        export_entity_type = None
        if entity_type:
            entity_type_map = {
                "deals": ExportEntityType.DEALS,
                "leads": ExportEntityType.DEALS,  # Alias for deals
                "contacts": ExportEntityType.CONTACTS,
                "companies": ExportEntityType.COMPANIES,
                "events": ExportEntityType.EVENTS,
                "users": ExportEntityType.USERS,
                "pipelines": ExportEntityType.PIPELINES
            }

            export_entity_type = entity_type_map.get(entity_type.lower())
            if not export_entity_type:
                raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        settings_list = await export_settings_manager.list_export_settings(export_entity_type)

        return {
            "settings": settings_list,
            "total_count": len(settings_list)
        }
    except Exception as e:
        log_event("export_settings", "error", f"Error listing export settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export-settings/{settings_id}")
async def get_export_settings(settings_id: str):
    """Get specific export settings by ID"""
    try:
        settings = await export_settings_manager.load_export_settings(settings_id)
        if not settings:
            raise HTTPException(status_code=404, detail="Export settings not found")

        return {
            "settings": settings.to_dict(),
            "settings_id": settings_id
        }
    except HTTPException:
        raise
    except Exception as e:
        log_event("export_settings", "error", f"Error getting export settings {settings_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/export-settings/{settings_id}")
async def update_export_settings(settings_id: str, settings_data: dict):
    """Update existing export settings"""
    try:
        # Load existing settings
        existing_settings = await export_settings_manager.load_export_settings(settings_id)
        if not existing_settings:
            raise HTTPException(status_code=404, detail="Export settings not found")

        # Update with new data
        updated_settings = ExportSettings(
            entity_type=settings_data.get('entity_type', existing_settings.entity_type),
            selected_fields=settings_data.get('selected_fields', existing_settings.selected_fields),
            field_order=settings_data.get('field_order', existing_settings.field_order),
            filters=settings_data.get('filters', existing_settings.filters),
            name=settings_data.get('name', existing_settings.name),
            description=settings_data.get('description', existing_settings.description),
            created_at=existing_settings.created_at
        )

        # Delete old settings and save new ones
        await export_settings_manager.delete_export_settings(settings_id)
        new_settings_id = await export_settings_manager.save_export_settings(updated_settings)

        return {
            "success": True,
            "settings_id": new_settings_id,
            "message": "Export settings updated successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        log_event("export_settings", "error", f"Error updating export settings {settings_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/export-settings/{settings_id}")
async def delete_export_settings(settings_id: str):
    """Delete export settings"""
    try:
        success = await export_settings_manager.delete_export_settings(settings_id)
        if not success:
            raise HTTPException(status_code=404, detail="Export settings not found")

        return {
            "success": True,
            "message": "Export settings deleted successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        log_event("export_settings", "error", f"Error deleting export settings {settings_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/export-settings/clear-cache")
async def clear_export_settings_cache():
    """Clear export settings cache"""
    try:
        await export_settings_manager.clear_cache()
        return {
            "success": True,
            "message": "Cache cleared successfully"
        }
    except Exception as e:
        log_event("export_settings", "error", f"Error clearing cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Flattening processor endpoints
@app.get("/api/flattening/status")
async def get_flattening_status():
    """Get current flattening processor status"""
    try:
        status = flattening_processor.get_flattening_status()
        return status
    except Exception as e:
        log_event("flattening", "error", f"Error getting flattening status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flattening/start")
async def start_flattening():
    """Start the flattening processor"""
    try:
        flattening_processor.start()
        return {"success": True, "message": "Flattening processor started"}
    except Exception as e:
        log_event("flattening", "error", f"Error starting flattening processor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flattening/stop")
async def stop_flattening():
    """Stop the flattening processor"""
    try:
        flattening_processor.stop()
        return {"success": True, "message": "Flattening processor stopped"}
    except Exception as e:
        log_event("flattening", "error", f"Error stopping flattening processor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flattening/sync/{entity_type}")
async def force_sync_flattening(entity_type: str):
    """Force immediate synchronization for a specific entity type"""
    try:
        # Normalize entity type and validate
        valid_entity_types = ["leads", "deals", "contacts", "companies", "events", "users", "pipelines"]
        if entity_type.lower() not in valid_entity_types:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        # Normalize deals -> leads for internal consistency
        normalized_entity_type = "leads" if entity_type.lower() == "deals" else entity_type.lower()

        result = flattening_processor.force_sync_entity_type(normalized_entity_type)
        return result
    except Exception as e:
        log_event("flattening", "error", f"Error force syncing {entity_type}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flattening/cleanup")
async def cleanup_flattened_data():
    """Clean up orphaned flattened data"""
    try:
        result = flattening_processor.cleanup_old_flattened_data()
        return result
    except Exception as e:
        log_event("flattening", "error", f"Error cleaning up flattened data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/flattening/search/{entity_type}")
async def search_flattened_data(entity_type: str, q: str = Query(...), limit: int = Query(20)):
    """Search in flattened data"""
    try:
        # Normalize entity type and validate
        valid_entity_types = ["leads", "deals", "contacts", "companies", "events", "users", "pipelines"]
        if entity_type.lower() not in valid_entity_types:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        # Normalize deals -> leads for internal consistency
        normalized_entity_type = "leads" if entity_type.lower() == "deals" else entity_type.lower()

        results = storage.search_flattened_entities(normalized_entity_type, q, limit)
        return {"results": results, "count": len(results)}
    except Exception as e:
        log_event("flattening", "error", f"Error searching flattened data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/flattening/statistics/{entity_type}")
async def get_flattened_statistics(entity_type: str):
    """Get field statistics for flattened data"""
    try:
        # Normalize entity type and validate
        valid_entity_types = ["leads", "deals", "contacts", "companies", "events", "users", "pipelines"]
        if entity_type.lower() not in valid_entity_types:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        # Normalize deals -> leads for internal consistency
        normalized_entity_type = "leads" if entity_type.lower() == "deals" else entity_type.lower()

        stats = storage.get_flattened_field_statistics(normalized_entity_type)
        return {"statistics": stats}
    except Exception as e:
        log_event("flattening", "error", f"Error getting flattened statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Performance monitoring endpoints

@app.get("/api/performance/stats")
async def get_performance_stats():
    """Get MongoDB operation performance statistics"""
    try:
        stats = storage.performance_monitor.get_operation_stats()
        return {"status": "success", "stats": stats}
    except Exception as e:
        log_event("api", "error", f"Error getting performance stats: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/stats/{operation_name}")
async def get_operation_performance_stats(operation_name: str):
    """Get performance statistics for a specific operation"""
    try:
        stats = storage.performance_monitor.get_operation_stats(operation_name)
        return {"status": "success", "operation": operation_name, "stats": stats}
    except Exception as e:
        log_event("api", "error", f"Error getting performance stats for {operation_name}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/slow-operations")
async def get_slow_operations(limit: int = 10):
    """Get the slowest recent operations"""
    try:
        slow_ops = storage.performance_monitor.get_slow_operations(limit)
        return {"status": "success", "slow_operations": slow_ops}
    except Exception as e:
        log_event("api", "error", f"Error getting slow operations: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/collection-stats/{entity_type}")
async def get_collection_stats(entity_type: str):
    """Get collection statistics for performance monitoring"""
    try:
        stats = storage.get_collection_stats(entity_type)
        return {"status": "success", "stats": stats}
    except Exception as e:
        log_event("api", "error", f"Error getting collection stats: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/performance/analyze-query")
async def analyze_query_performance(request: Request):
    """Analyze query performance and get optimization suggestions"""
    try:
        data = await request.json()
        entity_type = data.get("entity_type")
        query = data.get("query", {})

        if not entity_type:
            return {"status": "error", "message": "entity_type is required"}

        analysis = storage.optimize_query_performance(entity_type, query)
        return {"status": "success", "analysis": analysis}
    except Exception as e:
        log_event("api", "error", f"Error analyzing query performance: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/slow-queries")
async def get_slow_queries_analysis(threshold_ms: int = 1000):
    """Get analysis of slow queries with optimization suggestions"""
    try:
        analysis = storage.get_slow_queries_analysis(threshold_ms)
        return {"status": "success", "slow_queries": analysis}
    except Exception as e:
        log_event("api", "error", f"Error getting slow queries analysis: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/entities/{entity_type}/paginated")
async def get_entities_paginated(entity_type: str, page: int = 1, page_size: int = 100,
                                sort_field: str = None, sort_order: int = 1):
    """Get entities with pagination support"""
    try:
        # Validate entity type
        valid_entity_types = ["leads", "deals", "contacts", "companies", "events", "users", "pipelines"]
        if entity_type.lower() not in valid_entity_types:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        # Normalize deals -> leads
        normalized_entity_type = "leads" if entity_type.lower() == "deals" else entity_type.lower()

        # Validate page_size
        if page_size > 1000:
            page_size = 1000

        result = storage.get_entities_paginated(
            normalized_entity_type,
            page=page,
            page_size=page_size,
            sort_field=sort_field,
            sort_order=sort_order
        )

        return {"status": "success", **result}
    except Exception as e:
        log_event("api", "error", f"Error getting paginated entities: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/performance/reset-stats")
async def reset_performance_stats():
    """Reset performance statistics"""
    try:
        storage.performance_monitor.operation_stats.clear()
        log_event("api", "info", "Performance statistics reset")
        return {"status": "success", "message": "Performance statistics reset"}
    except Exception as e:
        log_event("api", "error", f"Error resetting performance stats: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/threshold")
async def get_slow_query_threshold():
    """Get current slow query threshold"""
    try:
        threshold = storage.performance_monitor.slow_query_threshold
        return {"status": "success", "threshold": threshold}
    except Exception as e:
        log_event("api", "error", f"Error getting threshold: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/performance/threshold")
async def set_slow_query_threshold(request: Request):
    """Set slow query threshold"""
    try:
        data = await request.json()
        threshold = float(data.get("threshold", 1.0))

        if threshold < 0:
            return {"status": "error", "message": "Threshold must be positive"}

        storage.performance_monitor.slow_query_threshold = threshold
        log_event("api", "info", f"Slow query threshold set to {threshold}s")
        return {"status": "success", "threshold": threshold}
    except Exception as e:
        log_event("api", "error", f"Error setting threshold: {e}")
        return {"status": "error", "message": str(e)}

# Benchmark testing endpoints

@app.post("/api/benchmark/run")
async def run_benchmark_tests(request: Request):
    """Run benchmark tests for system performance evaluation"""
    try:
        data = await request.json() if hasattr(request, 'json') else {}
        test_categories = data.get("categories", ["all"])

        # Import and run benchmark tests
        try:
            # from benchmark_tests import BenchmarkSuite
            # TODO: Implement benchmark tests module
            # suite = BenchmarkSuite()

            if "all" in test_categories:
                results = suite.run_full_benchmark_suite()
            else:
                # Run specific categories
                results = {
                    'timestamp': datetime.now().isoformat(),
                    'system_info': suite._get_system_info(),
                    'benchmarks': {}
                }

                if "storage" in test_categories:
                    results['benchmarks']['storage'] = suite.benchmark_storage_operations()
                if "sampling" in test_categories:
                    results['benchmarks']['sampling'] = suite.benchmark_sampling_operations()
                if "enrichment" in test_categories:
                    results['benchmarks']['enrichment'] = suite.benchmark_data_enrichment()
                if "export_settings" in test_categories:
                    results['benchmarks']['export_settings'] = suite.benchmark_export_settings()
                if "flattened_data" in test_categories:
                    results['benchmarks']['flattened_data'] = suite.benchmark_flattened_data_operations()

            # Generate report
            report = suite.generate_benchmark_report(results)

            log_event("api", "info", "Benchmark tests completed successfully")
            return {
                "status": "success",
                "results": results,
                "report": report
            }

        except ImportError as e:
            return {"status": "error", "message": f"Benchmark module not available: {e}"}

    except Exception as e:
        log_event("api", "error", f"Error running benchmark tests: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/benchmark/available-categories")
async def get_benchmark_categories():
    """Get available benchmark test categories"""
    return {
        "status": "success",
        "categories": [
            {
                "id": "storage",
                "name": "Storage Operations",
                "description": "MongoDB CRUD operations performance"
            },
            {
                "id": "sampling",
                "name": "Data Sampling",
                "description": "Field statistics and smart sampling performance"
            },
            {
                "id": "enrichment",
                "name": "Data Enrichment",
                "description": "User and pipeline enrichment performance"
            },
            {
                "id": "export_settings",
                "name": "Export Settings",
                "description": "Export configuration and field analysis performance"
            },
            {
                "id": "flattened_data",
                "name": "Flattened Data",
                "description": "Custom fields flattening and search performance"
            },
            {
                "id": "all",
                "name": "Full Suite",
                "description": "Complete benchmark test suite"
            }
        ]
    }

# Flattening processor management endpoints


def run_server(host: str = "0.0.0.0", port: int = 8000):
    """Run the FastAPI server"""
    try:
        # Log server start
        log_event("server", "info", f"Starting server on {host}:{port}")

        # Open browser
        webbrowser.open(f"http://localhost:{port}")

        # Start server
        uvicorn.run(app, host=host, port=port)
    except Exception as e:
        log_event("server", "error", f"Server error: {e}")
        raise


if __name__ == "__main__":
    run_server()
