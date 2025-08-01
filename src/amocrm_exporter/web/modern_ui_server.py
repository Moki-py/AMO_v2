"""
Modern web interface for AmoCRM exporter using FastAPI
"""

import asyncio
import webbrowser
import hmac
import hashlib
import os
import json
from enum import Enum
from typing import Callable, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient

from fastapi import FastAPI, HTTPException, Request, Header, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from ..core.logger import log_event
from ..storage.storage import Storage
from ..exporters.parallel_exporter import ParallelExporter
from ..exporters.excel_exporter import ExcelExporter
from ..exporters.sheets_exporter import SheetsExporter
from ..exporters.enhanced_sheets_exporter import EnhancedSheetsExporter
from ..utils.progress_tracker import ExportProgressTracker
from ..utils.progress_notifier import progress_notifier
from .export_settings import ExportSettingsManager, EntityType as ExportEntityType, ExportSettings, FieldInfo
from .config_validation_routes import router as config_validation_router
from ..core import logger
from ..core import config
from ..core.google_sheets_config import GoogleSheetsConfigManager
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
    # EVENTS = "events"  # Excluded from export operations per requirement 9.5
    USERS = "users"
    PIPELINES = "pipelines"
    CUSTOM_FIELDS = "custom_fields"

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

# Initialize progress tracker first
progress_tracker = ExportProgressTracker(storage)

# Initialize Google Sheets components with error handling to prevent server crash
sheets_exporter = None
enhanced_sheets_exporter = None
google_sheets_config = None

try:
    sheets_exporter = SheetsExporter(storage, progress_tracker=progress_tracker)
    enhanced_sheets_exporter = EnhancedSheetsExporter(storage, progress_tracker=progress_tracker)
    google_sheets_config = GoogleSheetsConfigManager()
    print("✅ Google Sheets экспортеры инициализированы успешно")
except Exception as e:
    print(f"⚠️ Ошибка инициализации Google Sheets экспортеров: {e}")
    print("🔧 Сервер запустится без функций Google Sheets экспорта")
    # Создать заглушки для API endpoints
    sheets_exporter = None
    enhanced_sheets_exporter = None
    google_sheets_config = GoogleSheetsConfigManager()  # Config manager должен работать без валидации

# Connect progress tracker to progress notifier for WebSocket updates
def progress_callback(export_id: str, progress) -> None:
    """Bridge progress tracker updates to progress notifier"""
    try:
        # Check if there's an active event loop before creating a task
        try:
            loop = asyncio.get_running_loop()
            # Convert progress to report format and notify via WebSocket
            asyncio.create_task(progress_notifier.send_progress_update(export_id, progress))
        except RuntimeError:
            # No running event loop, skip WebSocket updates
            log_event("server", "debug", f"Progress update skipped - no event loop running for export {export_id}")
    except Exception as e:
        log_event("server", "error", f"Error bridging progress update: {e}")

progress_tracker.add_progress_callback(progress_callback)
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

# Include configuration validation routes
app.include_router(config_validation_router)

# Initialize worker pool
worker_pool = None

@app.on_event("startup")
async def startup_event() -> None:
    """Initialize components on startup"""
    # No need to manually start the broker - it will be handled by FastStream
    log_event("api", "info", "Connected to RabbitMQ message broker")

    # Validate Google Sheets configuration
    try:
        validation_result = google_sheets_config.validate_configuration()
        if validation_result.is_valid:
            log_event("api", "info", "Google Sheets configuration validation passed")
            if validation_result.has_warnings:
                for warning in validation_result.warnings:
                    log_event("api", "warning", f"Google Sheets config warning: {warning}")
        else:
            log_event("api", "warning", "Google Sheets configuration validation failed")
            for error in validation_result.errors:
                log_event("api", "error", f"Google Sheets config error: {error}")
            log_event("api", "info", "Google Sheets export functionality will be disabled until configuration is fixed")
    except Exception as e:
        log_event("api", "error", f"Error during Google Sheets configuration validation: {e}")

    # Start flattening processor
    try:
        flattening_processor.start()
        log_event("api", "info", "Started flattening processor")
    except Exception as e:
        log_event("api", "error", f"Failed to start flattening processor: {e}")


@app.on_event("shutdown")
async def shutdown_event() -> None:
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
async def health_check() -> dict:
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
async def get_root(request: Request) -> HTMLResponse:
    """Render the main UI"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/export-settings", response_class=HTMLResponse)
async def render_export_settings_page(request: Request) -> HTMLResponse:
    """Render the export settings UI"""
    return templates.TemplateResponse("export-settings.html", {"request": request})


@app.get("/progress", response_class=HTMLResponse)
async def render_progress_page(request: Request) -> HTMLResponse:
    """Render the export progress tracking UI"""
    return templates.TemplateResponse("progress.html", {"request": request})


@app.get("/config-validation", response_class=HTMLResponse)
async def render_config_validation_page(request: Request) -> HTMLResponse:
    """Render the Google Sheets configuration validation UI"""
    return templates.TemplateResponse("config_validation.html", {"request": request})





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
        # EntityType.EVENTS,  # Disabled per requirement 9.5
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
            for e in [EntityType.DEALS, EntityType.CONTACTS, EntityType.COMPANIES, EntityType.USERS, EntityType.PIPELINES]:  # Events excluded per requirement 9.5
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
            for e in [EntityType.DEALS, EntityType.CONTACTS, EntityType.COMPANIES, EntityType.USERS, EntityType.PIPELINES]:  # Events excluded per requirement 9.5
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
) -> FileResponse:
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
) -> dict:
    try:
        # Check if Google Sheets exporter is available
        if sheets_exporter is None:
            raise HTTPException(
                status_code=503,
                detail="Google Sheets export is not available. Please check your Google Sheets configuration."
            )

        # Check configuration before attempting export
        validation_result = google_sheets_config.validate_configuration()
        if not validation_result.is_valid:
            raise HTTPException(
                status_code=400,
                detail=f"Google Sheets configuration is invalid: {'; '.join(validation_result.errors)}"
            )

        sheets_url = sheets_exporter.export_all_to_sheets(
            date_from=date_from, date_to=date_to
        )
        log_event(
            "server", "info", f"Google Sheets export generated: {sheets_url}"
        )
        return {"url": sheets_url}
    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error generating Google Sheets export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/export/sheets/enhanced")
async def export_sheets_enhanced_handler(
    request: Request,
    date_from: str = Query(None),
    date_to: str = Query(None)
) -> dict:
    """Start an enhanced Google Sheets export with progress tracking"""
    try:
        # Check if Google Sheets exporter is available
        if enhanced_sheets_exporter is None:
            raise HTTPException(
                status_code=503,
                detail="Enhanced Google Sheets export is not available. Please check your Google Sheets configuration."
            )

        # Check configuration before attempting export
        validation_result = google_sheets_config.validate_configuration()
        if not validation_result.is_valid:
            raise HTTPException(
                status_code=400,
                detail=f"Google Sheets configuration is invalid: {'; '.join(validation_result.errors)}"
            )

        # Parse request body for export configuration
        export_config = {}
        if request.headers.get('content-type') == 'application/json':
            try:
                export_config = await request.json()
            except:
                pass

        # Determine entity types based on config or use default
        entity_types = ["leads", "contacts", "companies"]  # Events excluded per requirement 9.5

        # If specific entity type is provided, export only that
        if "entity_type" in export_config:
            entity_types = [export_config["entity_type"]]

        # Start enhanced export with progress tracking
        export_id = progress_tracker.start_export(
            entity_types=entity_types
        )

        # Start the export in the background
        import asyncio
        # For now, use standard export - custom field export can be implemented later
        asyncio.create_task(
            enhanced_sheets_exporter.export_all_to_sheets_with_progress(
                date_from=date_from,
                date_to=date_to,
                export_id=export_id
            )
        )

        log_event("server", "info", f"Started enhanced Google Sheets export: {export_id}")
        return {
            "export_id": export_id,
            "message": "Export started with progress tracking",
            "status": "started"
        }

    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error starting enhanced Google Sheets export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/progress/{export_id}")
async def get_export_progress(export_id: str) -> dict:
    """Get current progress for a specific export"""
    try:
        progress = progress_tracker.get_export_progress(export_id)
        if not progress:
            raise HTTPException(status_code=404, detail="Export not found")

        # Convert progress to JSON-serializable format
        return {
            "export_id": progress.export_id,
            "status": progress.status.value,
            "overall_progress": progress.overall_progress_percentage,
            "start_time": progress.start_time.isoformat() if progress.start_time else None,
            "end_time": progress.end_time.isoformat() if progress.end_time else None,
            "estimated_completion": progress.estimated_completion.isoformat() if progress.estimated_completion else None,
            "duration": progress.duration.total_seconds() if progress.duration else None,
            "total_entities": progress.total_entities,
            "completed_entities": progress.completed_entities,
            "entities": {
                entity_type: {
                    "status": entity.status.value,
                    "progress_percentage": entity.progress_percentage,
                    "processed": entity.processed,
                    "total": entity.total,
                    "current_batch": entity.current_batch,
                    "total_batches": entity.total_batches,
                    "processing_rate": entity.processing_rate,
                    "estimated_completion": entity.estimated_completion.isoformat() if entity.estimated_completion else None,
                    "duration": entity.duration.total_seconds() if entity.duration else None,
                    "error_message": entity.error_message
                }
                for entity_type, entity in progress.entities.items()
            },
            "spreadsheet_urls": progress.spreadsheet_urls,
            "error_message": progress.error_message
        }

    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error getting export progress: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Export Preset Management Routes

@app.get("/api/presets")
async def list_presets(entity_type: str = Query(None)) -> dict:
    """List all export presets, optionally filtered by entity type"""
    try:
        presets = export_settings_manager.preset_manager.list_presets(entity_type)
        return {
            "presets": [preset.to_dict() for preset in presets],
            "count": len(presets)
        }
    except Exception as e:
        log_event("server", "error", f"Error listing presets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/presets/{preset_id}")
async def get_preset(preset_id: str) -> dict:
    """Get a specific export preset by ID"""
    try:
        preset = export_settings_manager.preset_manager.load_preset(preset_id)
        if not preset:
            raise HTTPException(status_code=404, detail="Preset not found")

        return {"preset": preset.to_dict()}
    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error getting preset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/presets")
async def create_preset(request: Request) -> dict:
    """Create a new export preset"""
    try:
        preset_data = await request.json()

        # Validate preset data
        from ..web.export_presets import ExportPreset
        preset = ExportPreset.from_dict(preset_data)

        # Save preset
        preset_id = export_settings_manager.preset_manager.save_preset(preset)

        log_event("server", "info", f"Created new preset: {preset.name} ({preset_id})")
        return {
            "preset_id": preset_id,
            "message": "Preset created successfully"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log_event("server", "error", f"Error creating preset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/presets/{preset_id}")
async def update_preset(preset_id: str, request: Request) -> dict:
    """Update an existing export preset"""
    try:
        preset_data = await request.json()
        preset_data['preset_id'] = preset_id

        # Validate and update preset
        from ..web.export_presets import ExportPreset
        preset = ExportPreset.from_dict(preset_data)

        # Save updated preset
        saved_preset_id = export_settings_manager.preset_manager.save_preset(preset)

        log_event("server", "info", f"Updated preset: {preset.name} ({saved_preset_id})")
        return {
            "preset_id": saved_preset_id,
            "message": "Preset updated successfully"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log_event("server", "error", f"Error updating preset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/presets/{preset_id}")
async def delete_preset(preset_id: str) -> dict:
    """Delete an export preset"""
    try:
        success = export_settings_manager.preset_manager.delete_preset(preset_id)
        if not success:
            raise HTTPException(status_code=404, detail="Preset not found")

        log_event("server", "info", f"Deleted preset: {preset_id}")
        return {"message": "Preset deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error deleting preset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/presets/{preset_id}/duplicate")
async def duplicate_preset(preset_id: str, request: Request) -> dict:
    """Duplicate an existing preset with a new name"""
    try:
        data = await request.json()
        new_name = data.get('name')
        if not new_name:
            raise HTTPException(status_code=400, detail="New name is required")

        new_preset_id = export_settings_manager.preset_manager.duplicate_preset(preset_id, new_name)
        if not new_preset_id:
            raise HTTPException(status_code=404, detail="Original preset not found")

        log_event("server", "info", f"Duplicated preset {preset_id} as {new_name} ({new_preset_id})")
        return {
            "preset_id": new_preset_id,
            "message": "Preset duplicated successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error duplicating preset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/export/sheets/with-presets")
async def export_sheets_with_presets_handler(request: Request) -> dict:
    """Start a Google Sheets export using saved presets"""
    try:
        data = await request.json()
        preset_ids = data.get('preset_ids', {})  # entity_type -> preset_id mapping
        date_from = data.get('date_from')
        date_to = data.get('date_to')

        if not preset_ids:
            raise HTTPException(status_code=400, detail="At least one preset must be specified")

        # Check if Google Sheets exporter is available
        if sheets_exporter is None:
            raise HTTPException(
                status_code=503,
                detail="Google Sheets export is not available. Please check your Google Sheets configuration."
            )

        # Check configuration before attempting export
        validation_result = google_sheets_config.validate_configuration()
        if not validation_result.is_valid:
            raise HTTPException(
                status_code=400,
                detail=f"Google Sheets configuration is invalid: {'; '.join(validation_result.errors)}"
            )

        # Load presets
        presets = {}
        for entity_type, preset_id in preset_ids.items():
            preset = export_settings_manager.preset_manager.load_preset(preset_id)
            if not preset:
                raise HTTPException(status_code=404, detail=f"Preset not found: {preset_id}")
            presets[entity_type] = preset

        # Start export with presets
        sheets_urls = await sheets_exporter.export_with_presets(
            presets=presets,
            date_from=date_from,
            date_to=date_to
        )

        log_event("server", "info", f"Google Sheets export with presets completed: {sheets_urls}")
        return {
            "urls": sheets_urls,
            "message": "Export completed successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error in preset-based export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Export Settings API Routes (needed for preset management)

@app.get("/api/export-settings/fields/{entity_type}")
async def get_entity_fields(entity_type: str) -> dict:
    """Get available fields for an entity type"""
    try:
        # Convert entity type to ExportEntityType enum
        if entity_type not in [e.value for e in ExportEntityType]:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        entity_enum = ExportEntityType(entity_type)
        fields = await export_settings_manager.get_available_fields(entity_enum)

        return {
            "fields": [field.to_dict() for field in fields],
            "count": len(fields)
        }
    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error getting entity fields: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/progress")
async def get_all_active_exports() -> dict:
    """Get all currently active exports"""
    try:
        active_exports = progress_tracker.get_all_active_exports()

        # Convert to JSON-serializable format
        exports_data = {}
        for export_id, progress in active_exports.items():
            exports_data[export_id] = {
                "export_id": progress.export_id,
                "status": progress.status.value,
                "overall_progress": progress.overall_progress_percentage,
                "start_time": progress.start_time.isoformat() if progress.start_time else None,
                "estimated_completion": progress.estimated_completion.isoformat() if progress.estimated_completion else None,
                "total_entities": progress.total_entities,
                "completed_entities": progress.completed_entities,
                "entity_count": len(progress.entities)
            }

        return {
            "active_exports": exports_data,
            "count": len(active_exports)
        }

    except Exception as e:
        log_event("server", "error", f"Error getting active exports: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/progress/{export_id}/cancel")
async def cancel_export(export_id: str) -> dict:
    """Cancel a running export"""
    try:
        success = progress_tracker.cancel_export(export_id)
        if not success:
            raise HTTPException(status_code=404, detail="Export not found or already completed")

        log_event("server", "info", f"Cancelled export: {export_id}")
        return {
            "export_id": export_id,
            "status": "cancelled",
            "message": "Export has been cancelled"
        }

    except HTTPException:
        raise
    except Exception as e:
        log_event("server", "error", f"Error cancelling export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/progress")
async def websocket_progress_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time progress updates"""
    await websocket.accept()

    # Add connection to progress notifier
    progress_notifier.add_websocket_connection(websocket)

    try:
        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Wait for messages (ping/pong or client requests)
                data = await websocket.receive_text()

                # Handle client messages if needed
                try:
                    message = json.loads(data)
                    if message.get("type") == "ping":
                        await websocket.send_text(json.dumps({"type": "pong"}))
                except json.JSONDecodeError:
                    # Ignore invalid JSON
                    pass

            except WebSocketDisconnect:
                break
            except Exception as e:
                log_event("server", "debug", f"WebSocket error: {e}")
                break

    finally:
        # Remove connection from progress notifier
        progress_notifier.remove_websocket_connection(websocket)
        log_event("server", "debug", "WebSocket connection closed")


@app.get("/api/progress/{export_id}/notifications")
async def get_export_notifications(export_id: str, limit: int = Query(50)) -> dict:
    """Get notification history for an export"""
    try:
        notifications = progress_notifier.get_notifications_history(export_id)

        # Limit and convert to JSON-serializable format
        limited_notifications = notifications[-limit:] if len(notifications) > limit else notifications

        notifications_data = []
        for notification in limited_notifications:
            notifications_data.append({
                "export_id": notification.export_id,
                "level": notification.level.value,
                "message": notification.message,
                "entity_type": notification.entity_type,
                "timestamp": notification.timestamp.isoformat(),
                "details": notification.details
            })

        return {
            "export_id": export_id,
            "notifications": notifications_data,
            "total_count": len(notifications),
            "returned_count": len(notifications_data)
        }

    except Exception as e:
        log_event("server", "error", f"Error getting export notifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_stats() -> dict:
    """Get current statistics"""
    try:
        deals = storage.get_entity_count("leads")
        contacts = storage.get_entity_count("contacts")
        companies = storage.get_entity_count("companies")
        events = 0  # Events excluded from export operations per requirement 9.5
        users = storage.get_entity_count("users")
        pipelines = storage.get_entity_count("pipelines")

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


async def fetch_all(date_from=None, date_to=None) -> None:
    try:
        exporter.export_all(date_from=date_from, date_to=date_to)
        log_event("server", "info", "Started export of all data")
    except Exception as e:
        log_event("server", "error", f"Error starting export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def fetch_entity(entity: EntityType, date_from=None, date_to=None) -> None:
    try:
        export_methods: dict[EntityType, Callable] = {
            EntityType.DEALS: exporter.export_deals,
            EntityType.CONTACTS: exporter.export_contacts,
            EntityType.COMPANIES: exporter.export_companies,
            # EntityType.EVENTS: exporter.export_events,  # Excluded per requirement 9.5
            EntityType.USERS: exporter.export_users,
            EntityType.PIPELINES: exporter.export_pipelines,
            EntityType.CUSTOM_FIELDS: exporter.export_custom_fields,
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
            # Events export disabled per requirement 9.5
            log_event("webhook", "info", "Events export skipped (disabled per requirement 9.5)")
        # You can add more entity types here as needed
        return {"success": True, "event_type": event_type}
    except Exception as e:
        log_event("webhook", "error", f"Error processing webhook: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/webhooks")
async def get_webhook_events() -> dict:
    try:
        collection = get_mongo_webhook_collection()
        events = list(collection.find({}, {"_id": 0}))
    except Exception as e:
        log_event("webhook", "error", f"Error reading webhook events from MongoDB: {e}")
        events = []
    return {"events": events, "count": len(events)}

# Worker API routes
@app.get("/api/tasks")
async def get_tasks() -> dict:
    """Get all export tasks"""
    state_manager = StateManager()
    return {"tasks": state_manager.get_all_tasks()}

@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str) -> dict:
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
) -> dict:
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
) -> dict:
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
) -> dict:
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
) -> dict:
    """Events export disabled per requirement 9.5"""
    return {"error": "Events export is disabled per requirement 9.5", "task_id": None}

@app.post("/api/tasks/export_all")
async def create_all_export_tasks(
    force_restart: bool = False,
    batch_save: bool = True,
    batch_size: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    priority: int = 1
) -> dict:
    """Create tasks to export all entity types"""
    task_ids = []

    # Create a task for each entity type (events excluded per requirement 9.5)
    for entity_type in ["leads", "contacts", "companies"]:
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
async def cancel_task(task_id: str) -> dict:
    """Cancel a pending task"""
    state_manager = StateManager()
    state_manager.update_task_status(task_id, "cancelled")
    return {"status": "Task cancelled"}

@app.get("/api/workers")
async def get_worker_status() -> dict:
    """Get status of worker processes"""
    try:
        return {"status": "running", "workers": []}
    except Exception as e:
        log_event("server", "error", f"Error getting worker status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Export Settings API Endpoints

@app.get("/api/export-settings/fields/{entity_type}")
async def get_available_fields(entity_type: str, force_refresh: bool = False, include_unnamed_fields: bool = False) -> dict:
    """Get available fields for an entity type"""
    try:
        # Convert to ExportEntityType - support all entity types (events excluded per requirement 9.5)
        entity_type_map = {
            "deals": ExportEntityType.DEALS,
            "leads": ExportEntityType.DEALS,  # Alias for deals
            "contacts": ExportEntityType.CONTACTS,
            "companies": ExportEntityType.COMPANIES,
            # "events": ExportEntityType.EVENTS,  # Excluded per requirement 9.5
            "users": ExportEntityType.USERS,
            "pipelines": ExportEntityType.PIPELINES
        }

        export_entity_type = entity_type_map.get(entity_type.lower())
        if not export_entity_type:
            raise HTTPException(status_code=400, detail=f"Invalid entity type: {entity_type}")

        fields = await export_settings_manager.get_available_fields(
            export_entity_type,
            force_refresh=force_refresh,
            include_unnamed_fields=include_unnamed_fields
        )

        # Count friendly vs technical fields for response metadata
        friendly_count = sum(1 for f in fields if f.is_user_friendly)
        technical_count = len(fields) - friendly_count

        return {
            "entity_type": entity_type,
            "fields": [field.to_dict() for field in fields],
            "total_count": len(fields),
            "friendly_count": friendly_count,
            "technical_count": technical_count,
            "include_unnamed_fields": include_unnamed_fields
        }
    except Exception as e:
        log_event("export_settings", "error", f"Error getting fields for {entity_type}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export-settings/preview/{entity_type}/{field_name}")
async def get_field_preview(entity_type: str, field_name: str, limit: int = 10) -> dict:
    """Get preview data for a specific field"""
    try:
        # Convert to ExportEntityType - support all entity types (events excluded per requirement 9.5)
        entity_type_map = {
            "deals": ExportEntityType.DEALS,
            "leads": ExportEntityType.DEALS,  # Alias for deals
            "contacts": ExportEntityType.CONTACTS,
            "companies": ExportEntityType.COMPANIES,
            # "events": ExportEntityType.EVENTS,  # Excluded per requirement 9.5
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
async def save_export_settings(settings_data: dict) -> dict:
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
async def list_export_settings(entity_type: Optional[str] = None) -> dict:
    """List all saved export settings"""
    try:
        export_entity_type = None
        if entity_type:
            entity_type_map = {
                "deals": ExportEntityType.DEALS,
                "leads": ExportEntityType.DEALS,  # Alias for deals
                "contacts": ExportEntityType.CONTACTS,
                "companies": ExportEntityType.COMPANIES,
                # "events": ExportEntityType.EVENTS,  # Excluded per requirement 9.5
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
async def get_export_settings(settings_id: str) -> dict:
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
async def update_export_settings(settings_id: str, settings_data: dict) -> dict:
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
async def delete_export_settings(settings_id: str) -> dict:
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
async def clear_export_settings_cache() -> dict:
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
async def get_flattening_status() -> dict:
    """Get current flattening processor status"""
    try:
        status = flattening_processor.get_flattening_status()
        return status
    except Exception as e:
        log_event("flattening", "error", f"Error getting flattening status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flattening/start")
async def start_flattening() -> dict:
    """Start the flattening processor"""
    try:
        flattening_processor.start()
        return {"success": True, "message": "Flattening processor started"}
    except Exception as e:
        log_event("flattening", "error", f"Error starting flattening processor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flattening/stop")
async def stop_flattening() -> dict:
    """Stop the flattening processor"""
    try:
        flattening_processor.stop()
        return {"success": True, "message": "Flattening processor stopped"}
    except Exception as e:
        log_event("flattening", "error", f"Error stopping flattening processor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/flattening/sync/{entity_type}")
async def force_sync_flattening(entity_type: str) -> dict:
    """Force immediate synchronization for a specific entity type"""
    try:
        # Normalize entity type and validate (events excluded per requirement 9.5)
        valid_entity_types = ["leads", "deals", "contacts", "companies", "users", "pipelines"]
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
async def cleanup_flattened_data() -> dict:
    """Clean up orphaned flattened data"""
    try:
        result = flattening_processor.cleanup_old_flattened_data()
        return result
    except Exception as e:
        log_event("flattening", "error", f"Error cleaning up flattened data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/flattening/search/{entity_type}")
async def search_flattened_data(entity_type: str, q: str = Query(...), limit: int = Query(20)) -> dict:
    """Search in flattened data"""
    try:
        # Normalize entity type and validate (events excluded per requirement 9.5)
        valid_entity_types = ["leads", "deals", "contacts", "companies", "users", "pipelines"]
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
async def get_flattened_statistics(entity_type: str) -> dict:
    """Get field statistics for flattened data"""
    try:
        # Normalize entity type and validate (events excluded per requirement 9.5)
        valid_entity_types = ["leads", "deals", "contacts", "companies", "users", "pipelines"]
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
async def get_performance_stats() -> dict:
    """Get MongoDB operation performance statistics"""
    try:
        stats = storage.performance_monitor.get_operation_stats()
        return {"status": "success", "stats": stats}
    except Exception as e:
        log_event("api", "error", f"Error getting performance stats: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/stats/{operation_name}")
async def get_operation_performance_stats(operation_name: str) -> dict:
    """Get performance statistics for a specific operation"""
    try:
        stats = storage.performance_monitor.get_operation_stats(operation_name)
        return {"status": "success", "operation": operation_name, "stats": stats}
    except Exception as e:
        log_event("api", "error", f"Error getting performance stats for {operation_name}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/slow-operations")
async def get_slow_operations(limit: int = 10) -> dict:
    """Get the slowest recent operations"""
    try:
        slow_ops = storage.performance_monitor.get_slow_operations(limit)
        return {"status": "success", "slow_operations": slow_ops}
    except Exception as e:
        log_event("api", "error", f"Error getting slow operations: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/collection-stats/{entity_type}")
async def get_collection_stats(entity_type: str) -> dict:
    """Get collection statistics for performance monitoring"""
    try:
        stats = storage.get_collection_stats(entity_type)
        return {"status": "success", "stats": stats}
    except Exception as e:
        log_event("api", "error", f"Error getting collection stats: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/performance/analyze-query")
async def analyze_query_performance(request: Request) -> dict:
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
async def get_slow_queries_analysis(threshold_ms: int = 1000) -> dict:
    """Get analysis of slow queries with optimization suggestions"""
    try:
        analysis = storage.get_slow_queries_analysis(threshold_ms)
        return {"status": "success", "slow_queries": analysis}
    except Exception as e:
        log_event("api", "error", f"Error getting slow queries analysis: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/entities/{entity_type}/paginated")
async def get_entities_paginated(entity_type: str, page: int = 1, page_size: int = 100,
                                sort_field: str = None, sort_order: int = 1) -> dict:
    """Get entities with pagination support"""
    try:
        # Validate entity type (events excluded per requirement 9.5)
        valid_entity_types = ["leads", "deals", "contacts", "companies", "users", "pipelines"]
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
async def reset_performance_stats() -> dict:
    """Reset performance statistics"""
    try:
        storage.performance_monitor.operation_stats.clear()
        log_event("api", "info", "Performance statistics reset")
        return {"status": "success", "message": "Performance statistics reset"}
    except Exception as e:
        log_event("api", "error", f"Error resetting performance stats: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/performance/threshold")
async def get_slow_query_threshold() -> dict:
    """Get current slow query threshold"""
    try:
        threshold = storage.performance_monitor.slow_query_threshold
        return {"status": "success", "threshold": threshold}
    except Exception as e:
        log_event("api", "error", f"Error getting threshold: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/performance/threshold")
async def set_slow_query_threshold(request: Request) -> dict:
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
async def run_benchmark_tests(request: Request) -> dict:
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
async def get_benchmark_categories() -> dict:
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

# Google Sheets Configuration API Endpoints

@app.get("/api/google-sheets/config/validate")
async def validate_google_sheets_config() -> dict:
    """Validate Google Sheets configuration"""
    try:
        validation_result = google_sheets_config.validate_configuration()
        return {
            "is_valid": validation_result.is_valid,
            "errors": validation_result.errors,
            "warnings": validation_result.warnings,
            "missing_configs": validation_result.missing_configs
        }
    except Exception as e:
        log_event("sheets_config", "error", f"Error validating Google Sheets config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/google-sheets/config/summary")
async def get_google_sheets_config_summary() -> dict:
    """Get Google Sheets configuration summary"""
    try:
        summary = google_sheets_config.get_configuration_summary()
        return summary
    except Exception as e:
        log_event("sheets_config", "error", f"Error getting Google Sheets config summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/google-sheets/config/setup-guide")
async def get_google_sheets_setup_guide() -> dict:
    """Get guided setup instructions for Google Sheets configuration"""
    try:
        setup_guide = google_sheets_config.setup_guided_configuration()
        return setup_guide
    except Exception as e:
        log_event("sheets_config", "error", f"Error getting Google Sheets setup guide: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/google-sheets/spreadsheet/{spreadsheet_id}/info")
async def get_spreadsheet_info(spreadsheet_id: str) -> dict:
    """Get information about a specific spreadsheet"""
    try:
        spreadsheet_info = google_sheets_config.get_spreadsheet_info(spreadsheet_id)
        return {
            "spreadsheet_id": spreadsheet_info.spreadsheet_id,
            "title": spreadsheet_info.title,
            "url": spreadsheet_info.url,
            "sheets": spreadsheet_info.sheets,
            "permissions": spreadsheet_info.permissions,
            "last_modified": spreadsheet_info.last_modified.isoformat() if spreadsheet_info.last_modified else None
        }
    except Exception as e:
        log_event("sheets_config", "error", f"Error getting spreadsheet info for {spreadsheet_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/google-sheets/spreadsheet/{spreadsheet_id}/test-permissions")
async def test_spreadsheet_permissions(spreadsheet_id: str) -> dict:
    """Test permissions for a specific spreadsheet"""
    try:
        permission_result = google_sheets_config.test_permissions(spreadsheet_id)
        return {
            "spreadsheet_id": spreadsheet_id,
            "can_read": permission_result.can_read,
            "can_write": permission_result.can_write,
            "can_create_sheets": permission_result.can_create_sheets,
            "error_message": permission_result.error_message
        }
    except Exception as e:
        log_event("sheets_config", "error", f"Error testing permissions for {spreadsheet_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def run_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the FastAPI server"""
    try:
        # Log server start
        log_event("server", "info", f"Starting server on {host}:{port}")

        # Open browser
        webbrowser.open(f"http://localhost:{port}")

                    # Start server with socket reuse and multi-client support
        uvicorn.run(
            app, 
            host=host, 
            port=port,
            # Enable socket reuse for multiple clients and quick restarts
            access_log=True,
            # Configure server socket options for better connection handling
            backlog=2048,  # Increase backlog for better connection handling
            # Add timeout configurations
            timeout_keep_alive=5,
            timeout_graceful_shutdown=5,
            # WebSocket configuration for multi-client support
            ws_ping_interval=20,
            ws_ping_timeout=20,
            ws_max_size=16777216,  # 16MB for WebSocket messages
            # Allow reuse of address to prevent "address already in use" errors
            # This is handled at the OS level in uvicorn
        )
    except Exception as e:
        log_event("server", "error", f"Server error: {e}")
        raise


if __name__ == "__main__":
    run_server()
