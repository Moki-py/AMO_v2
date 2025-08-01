#!/usr/bin/env python3
"""
Скрипт запуска веб-интерфейса AmoCRM Data Exporter
"""

import sys
from pathlib import Path

# Добавляем src в путь для импорта
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    import uvicorn
    from amocrm_exporter.web.modern_ui_server import app

    if __name__ == "__main__":
        print("Запуск веб-интерфейса AmoCRM Data Exporter на http://127.0.0.1:8001")
        # Configure uvicorn with socket reuse and WebSocket support for multiple clients
        uvicorn.run(
            app, 
            host="127.0.0.1", 
            port=8001,
            # Enable socket reuse to allow multiple clients and quick restarts
            access_log=True,
            # Configure server socket options
            backlog=2048,  # Increase backlog for better connection handling
            # Add timeout configurations
            timeout_keep_alive=5,
            timeout_graceful_shutdown=5,
            # WebSocket configuration for multi-client support
            ws_ping_interval=20,
            ws_ping_timeout=20,
            ws_max_size=16777216  # 16MB for WebSocket messages
        )
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Убедитесь, что все зависимости установлены:")
    print("pip install -r requirements.txt")
    sys.exit(1)