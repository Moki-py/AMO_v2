"""
Modern web interface for AmoCRM exporter using FastAPI
"""

import os
import json
from datetime import datetime
import webbrowser
from enum import Enum
from typing import Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

import config
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


# Create FastAPI app
app = FastAPI(
    title="AmoCRM Data Exporter",
    description="Modern web interface for AmoCRM data export",
    version="1.0.0",
)


# Setup templates
templates = Jinja2Templates(directory="templates")


def get_recent_logs(count: int = 100) -> list[dict]:
    """Get the most recent logs from the log file"""
    log_file = config.settings.log_file
    logs = []

    if os.path.exists(log_file):
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
                # Get the last N logs
                logs = logs[-count:] if len(logs) > count else logs
        except Exception as e:
            logs = [
                {
                    "timestamp": datetime.now().isoformat(),
                    "level": "error",
                    "message": f"Error loading logs: {e}",
                }
            ]

    return logs


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Serve the main page"""
    try:
        with open("templates/index.html", "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(content=html)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading template: {e}"
        )


@app.get("/stats")
async def stats() -> dict:
    """Return statistics"""
    return get_stats()


@app.get("/logs")
async def logs() -> dict:
    """Return recent logs"""
    return {"logs": get_recent_logs(30)}


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
