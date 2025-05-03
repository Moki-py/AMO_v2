"""
Modern web interface for AmoCRM exporter using FastAPI
"""

import webbrowser
from enum import Enum
from typing import Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from logger import log_event
from storage import Storage
from parallel_exporter import ParallelExporter
import logger


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


@app.get("/", response_class=HTMLResponse)
async def get_root(request: Request):
    """Render the main UI"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/stats")
async def stats() -> dict:
    """Return statistics"""
    return get_stats()


@app.get("/logs")
async def logs() -> dict:
    """Return recent logs"""
    return {"logs": logger.get_recent_logs(30)}


@app.post("/fetch/all")
async def fetch_all_handler() -> dict:
    """Trigger export of all entities"""
    await fetch_all()
    return {"success": True}


@app.post("/fetch/{entity}")
async def fetch_entity_handler(entity: EntityType) -> dict:
    """Trigger export of a specific entity"""
    if entity not in [
        EntityType.DEALS,
        EntityType.CONTACTS,
        EntityType.COMPANIES,
        EntityType.EVENTS,
    ]:
        raise HTTPException(status_code=400, detail="Invalid entity type")
    await fetch_entity(entity)
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


async def fetch_all():
    """Fetch all data"""
    try:
        exporter.export_all()
        log_event("server", "info", "Started export of all data")
    except Exception as e:
        log_event("server", "error", f"Error starting export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def fetch_entity(entity: EntityType):
    """Fetch specific entity type"""
    try:
        # Map entity types to export methods
        export_methods: dict[EntityType, Callable] = {
            EntityType.DEALS: exporter.export_deals,
            EntityType.CONTACTS: exporter.export_contacts,
            EntityType.COMPANIES: exporter.export_companies,
            EntityType.EVENTS: exporter.export_events,
        }

        if entity in export_methods:
            export_methods[entity]()
            log_event("server", "info", f"Started export of {entity.value}")
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
