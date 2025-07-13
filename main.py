#!/usr/bin/env python3
"""
Основной скрипт запуска AmoCRM Data Exporter
"""

import sys
from pathlib import Path

# Добавляем src в путь для импорта
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from amocrm_exporter.exporters.parallel_exporter import main
    if __name__ == "__main__":
        main()
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Убедитесь, что все зависимости установлены:")
    print("pip install -r requirements.txt")
    sys.exit(1)