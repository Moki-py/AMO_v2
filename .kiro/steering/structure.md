# Project Structure & Organization

## Source Code Layout

The project follows modern Python src-layout with all code under `src/amocrm_exporter/`:

```
src/amocrm_exporter/
├── core/                      # Core system modules
│   ├── config.py              # Configuration management
│   ├── api.py                 # AmoCRM API client
│   ├── auth.py                # Authentication handling
│   └── logger.py              # Logging system
├── exporters/                 # Data export modules
│   ├── parallel_exporter.py   # Main parallel export logic
│   ├── excel_exporter.py      # Excel export functionality
│   └── sheets_exporter.py     # Google Sheets integration
├── processors/                # Data processing
│   ├── batch_processor.py     # Batch operations
│   ├── flattening_processor.py # Custom fields flattening
│   └── precomputed_statistics.py # Statistics computation
├── enrichment/                # Data enrichment
│   ├── data_enrichment.py     # User/pipeline enrichment
│   └── smart_field_detector.py # Field detection logic
├── storage/                   # Storage layer
│   ├── storage.py             # MongoDB/JSON operations
│   ├── cache_manager.py       # Redis caching
│   └── state_manager.py       # Export state management
├── workers/                   # Background processing
│   ├── worker.py              # Main worker implementation
│   ├── message_broker.py      # RabbitMQ integration
│   └── monitoring.py          # Performance monitoring
├── utils/                     # Utilities
│   ├── exceptions.py          # Custom exceptions
│   ├── entity_types.py        # AmoCRM entity definitions
│   └── rate_limiter.py        # API rate limiting
└── web/                       # Web interface
    ├── modern_ui_server.py    # FastAPI application
    ├── export_settings.py     # Export configuration
    └── templates/             # HTML templates
```

## Key Directories

- **`data/`** - Runtime data storage (JSON files, created automatically)
- **`exports/`** - Generated export files (Excel, etc.)
- **`mongo_data/`** - MongoDB data files (when running locally)
- **`logs/`** - Application logs by service
- **`scripts/`** - Utility scripts and fixes
- **`tests/`** - Test suite
- **`docs/`** - Documentation files

## Entry Points

- **`main.py`** - Direct export execution
- **`web_server.py`** - Web interface launcher
- **`run_modern_ui.sh/.bat`** - Platform-specific launchers

## Configuration Files

- **`.env`** - Environment variables (AmoCRM tokens, MongoDB URL)
- **`pyproject.toml`** - Modern Python project configuration
- **`requirements.txt`** - Python dependencies
- **`docker-compose.yml`** - Full stack deployment
- **`Dockerfile`** - Container image definition

## Architectural Patterns

- **Modular design** - Clear separation of concerns
- **Dependency injection** - Configuration-driven components
- **Worker pattern** - Background task processing
- **Repository pattern** - Storage abstraction layer
- **Factory pattern** - Entity type handling
- **Observer pattern** - Progress monitoring and logging