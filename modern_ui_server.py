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
from pymongo import MongoClient

from fastapi import FastAPI, HTTPException, Request, Header, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from logger import log_event
from storage import Storage
from parallel_exporter import ParallelExporter
from excel_exporter import ExcelExporter
from sheets_exporter import SheetsExporter
import logger
import config
from state_manager import StateManager
from message_broker import create_export_task, broker


class ActionType(str, Enum):
    """Types of actions that can be performed"""

    STATS = "stats"
    LOGS = "logs"
    FETCH = "fetch"


class EntityType(str, Enum):
    """Types of entities that can be exported"""

    ALL = "all"
    DEALS = "deals"
    CONTACTS = "contacts"
    COMPANIES = "companies"
    EVENTS = "events"


# Create global instances
storage = Storage()
logger.init_storage(storage)
exporter = ParallelExporter()
excel_exporter = ExcelExporter(storage)
sheets_exporter = SheetsExporter(storage)

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


# Setup templates
templates = Jinja2Templates(directory="templates")

# Initialize worker pool
worker_pool = None

@app.on_event("startup")
async def startup_event():
    """Initialize components on startup"""
    # No need to manually start the broker - it will be handled by FastStream
    log_event("api", "info", "Connected to RabbitMQ message broker")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    # Close the broker connection
    await broker.close()
    log_event("api", "info", "Closed connection to message broker")

@app.get("/", response_class=HTMLResponse)
async def get_root(request: Request):
    """Render the main UI"""
    return templates.TemplateResponse("index.html", {"request": request})


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
    ]:
        raise HTTPException(status_code=400, detail="Invalid entity type")
    await fetch_entity(entity, date_from, date_to)
    return {"success": True}


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
            for e in [EntityType.DEALS, EntityType.CONTACTS, EntityType.COMPANIES, EntityType.EVENTS]:
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
            for e in [EntityType.DEALS, EntityType.CONTACTS, EntityType.COMPANIES, EntityType.EVENTS]:
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

        return {
            "deals": deals,
            "contacts": contacts,
            "companies": companies,
            "events": events,
        }
    except Exception as e:
        log_event("server", "error", f"Error getting stats: {e}")
        return {"deals": 0, "contacts": 0, "companies": 0, "events": 0}


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
    """Get status of all workers"""
    # This will need to be implemented differently with RabbitMQ
    # For now, just return an empty list
    return {"workers": []}

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
