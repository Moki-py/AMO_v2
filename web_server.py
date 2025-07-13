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
        print("Запуск веб-интерфейса AmoCRM Data Exporter на http://127.0.0.1:8000")
        uvicorn.run(app, host="127.0.0.1", port=8000)
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Убедитесь, что все зависимости установлены:")
    print("pip install -r requirements.txt")
    sys.exit(1)