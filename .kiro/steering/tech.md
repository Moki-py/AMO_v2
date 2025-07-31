# Technology Stack & Build System

## Core Technologies

- **Python 3.11+** with modern packaging (pyproject.toml)
- **FastAPI** for web interface and REST API
- **MongoDB** primary storage with **JSON files** as fallback
- **RabbitMQ + FastStream** for distributed worker system
- **Redis** for caching
- **Docker + Docker Compose** for containerized deployment

## Key Dependencies

- `fastapi`, `uvicorn` - Web framework and ASGI server
- `pymongo` - MongoDB integration
- `faststream[rabbit,cli]` - Message queue framework
- `requests` - AmoCRM API client
- `pandas`, `openpyxl` - Data processing and Excel export
- `google-api-python-client` - Google Sheets integration
- `pydantic-settings` - Configuration management

## Common Commands

### Development Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"
```

### Running the Application
```bash
# Web interface (recommended)
python web_server.py
# or
bash run_modern_ui.sh  # Linux/Mac
run_modern_ui.bat      # Windows

# CLI commands (after pip install -e .)
amocrm-web          # Web interface
amocrm-exporter     # Direct export
amocrm-worker       # Worker process

# Direct export
python main.py
```

### Docker Deployment
```bash
# Full stack with RabbitMQ, MongoDB, workers
docker-compose up -d

# Local development (MongoDB only)
docker-compose -f local.docker-compose.yml up -d

# Resource-constrained deployment
bash run_resource_constrained.sh
```

### Testing & Quality
```bash
# Run tests
pytest

# Code formatting
black src/
isort src/

# Type checking
mypy src/
```

## Build System

Uses modern Python packaging with `pyproject.toml`. The project follows src-layout structure with all code under `src/amocrm_exporter/`.